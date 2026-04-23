from __future__ import annotations

from typing import Any

import hmac

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette import status

from app.core.database import get_db
from app.hotdock.services.audit import record_audit_log
from app.hotdock.services.auth import (
    attach_auth_context,
    build_login_redirect,
    default_workspace_for_user,
    get_flash,
    sanitize_next_path,
    set_flash,
)
from app.hotdock.services.context import build_app_context
from app.hotdock.services.github import (
    DETAIL_SYNC_COMPLETED,
    DETAIL_SYNC_ERROR,
    DETAIL_SYNC_NOT_STARTED,
    DETAIL_SYNC_SYNCING,
    REPOSITORY_SELECTION_ACTIVE,
    REPOSITORY_SELECTION_INACCESSIBLE,
    REPOSITORY_SELECTION_INACTIVE,
    REPOSITORY_SELECTION_UNSELECTED,
    activate_workspace_repository_selection,
    active_repository_limit,
    manually_register_branch_snapshot,
    manual_sync_workspace_installation_repositories,
)
from app.hotdock.services.workspaces import (
    build_workspace_navigation,
    resolve_workspace_access,
    workspace_billing_data,
    workspace_dashboard_data,
    workspace_members_data,
    workspace_settings_data,
)
from app.models.branch import Branch
from app.models.branch_file import BranchFile
from app.models.file_collision import FileCollision
from app.models.file_collision_branch import FileCollisionBranch
from app.models.github_installation import GithubInstallation
from app.models.repository import Repository
from app.models.workspace import Workspace

router = APIRouter()
MANUAL_BRANCH_RESULT_SESSION_KEY = "manual_branch_result"


def render_app(template_name: str, context: dict[str, Any]):
    request = context["request"]
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


def workspace_page_context(
    request: Request,
    db: Session,
    *,
    workspace: Workspace,
    active_nav: str,
    page_title: str,
    page_heading: str,
    page_description: str,
    breadcrumbs: list[dict[str, str]],
    current_membership,
) -> dict[str, Any]:
    auth = attach_auth_context(request, db)
    context = build_app_context(
        request,
        page_title=page_title,
        page_description=page_description,
        page_heading=page_heading,
        active_nav=active_nav,
        body_class="page-app page-workspace",
        breadcrumbs=breadcrumbs,
    )
    context.update(
        {
            "app_navigation": build_workspace_navigation(workspace.slug, current_membership.role if current_membership else None),
            "app_workspace": workspace.name,
            "current_user": auth.user,
            "current_membership": current_membership,
            "workspace": workspace,
            "flash": get_flash(request),
            "csrf_token": auth.csrf_token,
            "sidebar_bookmarks": {"items": [], "remaining_count": 0},
        }
    )
    return context


@router.get("/dashboard", name="hotdock-dashboard")
async def dashboard_root(request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(url=build_login_redirect("/dashboard"), status_code=status.HTTP_303_SEE_OTHER)
    workspace = default_workspace_for_user(db, auth.user.id)
    if workspace is None:
        return RedirectResponse(url="/workspaces/new", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=f"/workspaces/{workspace.slug}/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/workspaces/{workspace_slug}/dashboard", name="hotdock-workspace-dashboard")
async def workspace_dashboard(workspace_slug: str, request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/dashboard"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="viewer")
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-dashboard",
        page_title=f"{access.workspace.name} | ダッシュボード | Hotdock",
        page_heading="ダッシュボード",
        page_description="競合と監視状況の概要",
        breadcrumbs=[
            {"label": "ダッシュボード", "href": f"/workspaces/{workspace_slug}/dashboard"},
        ],
        current_membership=access.membership,
    )
    context["dashboard"] = workspace_dashboard_data(db, access.workspace)
    return render_app("hotdock/app/workspace_dashboard.html", context)


@router.get("/workspaces/{workspace_slug}/repositories", name="hotdock-workspace-repositories")
async def workspace_repositories(workspace_slug: str, request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/repositories"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="viewer")
    catalog_sync_error = None
    github_settings_href = f"/settings/integrations/github?workspace={workspace_slug}"
    claimed_installations = db.scalars(
        select(GithubInstallation).where(GithubInstallation.claimed_workspace_id == access.workspace.id)
    ).all()
    if claimed_installations and access.membership.role in {"owner", "admin"}:
        try:
            await manual_sync_workspace_installation_repositories(
                db,
                request,
                workspace=access.workspace,
                actor=auth.user,
                record_audit_event=False,
            )
        except Exception as exc:
            catalog_sync_error = str(getattr(exc, "detail", None) or exc)
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-repositories",
        page_title=f"{access.workspace.name} | Repositories | Hotdock",
        page_heading="Repositories",
        page_description="監視対象にする repository を選択します",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Repositories", "href": f"/workspaces/{workspace_slug}/repositories"},
        ],
        current_membership=access.membership,
    )
    repositories = db.scalars(
        select(Repository).where(Repository.workspace_id == access.workspace.id, Repository.deleted_at.is_(None))
    ).all()
    repositories.sort(
        key=lambda repository: (
            0 if repository.selection_status == REPOSITORY_SELECTION_ACTIVE else 1,
            0 if repository.is_available else 1,
            repository.display_name.lower(),
        )
    )
    active_repository = next((repository for repository in repositories if repository.selection_status == REPOSITORY_SELECTION_ACTIVE), None)
    has_claimed_installations = bool(claimed_installations)
    catalog_state = "ready"
    if not has_claimed_installations:
        catalog_state = "unconnected"
    elif not repositories:
        catalog_state = "empty"
    sync_warning = None
    if catalog_sync_error and has_claimed_installations:
        sync_warning = "候補 repository をまだ取得できませんでした。GitHub App の接続状態を確認して、もう一度お試しください。"
    repositories_view = []
    for repository in repositories:
        if not repository.is_available or repository.selection_status == REPOSITORY_SELECTION_INACCESSIBLE:
            status_label = "エラー"
            status_class = "is-conflict"
            helper_text = repository.inaccessible_reason or "GitHub App から現在アクセスできません"
        elif repository.detail_sync_status == DETAIL_SYNC_ERROR:
            status_label = "エラー"
            status_class = "is-conflict"
            helper_text = repository.detail_sync_error_message or "同期に失敗しました"
        elif repository.detail_sync_status == DETAIL_SYNC_SYNCING:
            status_label = "同期中"
            status_class = "is-stale"
            helper_text = "同期を進めています"
        elif repository.selection_status == REPOSITORY_SELECTION_ACTIVE:
            status_label = "監視中"
            status_class = "is-available"
            helper_text = "現在の監視対象です"
        elif repository.selection_status == REPOSITORY_SELECTION_INACTIVE:
            status_label = "未監視"
            status_class = "is-stale"
            helper_text = "以前の監視対象です"
        else:
            status_label = "未選択"
            status_class = ""
            helper_text = "候補から選択できます"
        repositories_view.append(
            {
                "id": repository.id,
                "display_name": repository.display_name,
                "full_name": repository.full_name,
                "visibility": repository.visibility,
                "default_branch": repository.default_branch or "-",
                "status_label": status_label,
                "status_class": status_class,
                "helper_text": helper_text,
                "last_synced_at": repository.last_synced_at or "-",
                "can_activate": repository.selection_status in [REPOSITORY_SELECTION_UNSELECTED, REPOSITORY_SELECTION_INACTIVE]
                and repository.is_available,
                "is_active": repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
            }
        )
    if catalog_state == "unconnected":
        state_banner = {
            "tone": "is-conflict",
            "title": "GitHub App が未接続です",
            "body": "",
            "supporting": None,
            "action_label": "GitHub App を連携",
            "action_href": github_settings_href,
        }
        empty_state = {
            "title": "候補 repository はまだありません",
            "body": "GitHub App を連携すると、利用可能な repository 候補がここに表示されます",
            "action_label": "GitHub App を連携",
            "action_href": github_settings_href,
            "action_variant": "text",
        }
    elif catalog_state == "empty":
        state_banner = {
            "tone": "is-stale",
            "title": "候補 repository はまだ同期されていません",
            "body": "GitHub App は接続済みです。候補 repository を取り込むと監視対象を選べます",
            "supporting": "再同期すると候補 repository がここに表示されます",
            "action_label": "repository を再同期",
            "action_href": None,
        }
        empty_state = {
            "title": "候補 repository はまだありません",
            "body": "GitHub App は接続済みです。再同期すると利用可能な repository 候補を取得できます",
            "action_label": "repository を再同期",
            "action_href": None,
            "action_variant": "secondary",
        }
    else:
        state_banner = {
            "tone": "is-available" if active_repository else "is-stale",
            "title": "監視対象にする repository を選択します",
            "body": "候補から 1 件選ぶと、その repository を起点に branch と conflict の監視が始まります",
            "supporting": f"監視上限は {active_repository_limit()} 件です",
            "action_label": "GitHub App を確認",
            "action_href": github_settings_href,
        }
        empty_state = None
    context["repositories_page"] = {
        "catalog_state": catalog_state,
        "sync_warning": sync_warning,
        "state_banner": state_banner,
        "summary_items": [
            {"label": "接続状態", "value": "未接続" if not has_claimed_installations else "接続済み", "class": "is-conflict" if not has_claimed_installations else "is-available"},
            {"label": "候補数", "value": str(len(repositories)), "class": ""},
            {"label": "監視対象", "value": active_repository.display_name if active_repository else "未選択", "class": "is-available" if active_repository else "is-stale"},
        ],
        "steps": [
            {
                "title": "GitHub App を連携",
                "description": "repository 候補を取り込むための最初の設定です",
                "status_label": "未完了" if not has_claimed_installations else "完了",
                "status_class": "is-stale" if not has_claimed_installations else "is-available",
                "action_label": "連携する" if not has_claimed_installations else None,
                "action_href": github_settings_href if not has_claimed_installations else None,
                "is_current": not has_claimed_installations,
            },
            {
                "title": "監視対象 repository を選択",
                "description": "連携後に候補から 1 件選択できます" if not active_repository else "監視対象を選択済みです",
                "status_label": "未開始" if not active_repository else "完了",
                "status_class": "" if not active_repository else "is-available",
                "action_label": None,
                "action_href": None,
                "is_current": has_claimed_installations and not active_repository,
            },
            {
                "title": "branch を観測または手動登録",
                "description": "repository 選択後に push または手動登録で進めます",
                "status_label": "未開始" if not active_repository else "未開始",
                "status_class": "",
                "action_label": None,
                "action_href": None,
                "is_current": False,
            },
        ],
        "empty_state": empty_state,
        "github_settings_href": github_settings_href,
        "active_repository": active_repository,
        "repositories": repositories_view,
        "repository_limit": active_repository_limit(),
    }
    return render_app("hotdock/app/workspace_repositories.html", context)


@router.post("/workspaces/{workspace_slug}/repositories/sync", name="hotdock-workspace-repositories-sync")
async def workspace_repositories_sync(
    workspace_slug: str,
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(...),
):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/repositories"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(
            url=f"/workspaces/{workspace_slug}/repositories",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="admin")
    try:
        result = await manual_sync_workspace_installation_repositories(
            db,
            request,
            workspace=access.workspace,
            actor=auth.user,
        )
    except Exception as exc:
        message = "repository 同期に失敗しました。"
        if getattr(exc, "detail", None) == "No claimed installations":
            message = "先に GitHub App installation を claim してください。"
        else:
            message = "GitHub 側の repository 候補一覧を取得できませんでした。installation 状態を確認して再試行してください。"
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=auth.user.id,
            workspace_id=access.workspace.id,
            target_type="workspace",
            target_id=access.workspace.id,
            action="workspace_repository_sync_failed",
            metadata={
                "workspace_slug": access.workspace.slug,
                "error": str(getattr(exc, "detail", None) or exc),
            },
        )
        db.commit()
        set_flash(request, "error", message)
        return RedirectResponse(
            url=f"/workspaces/{workspace_slug}/repositories",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    if result["repositories_synced"] > 0:
        set_flash(
            request,
            "success",
            f"{result['repositories_synced']} 件の repository 候補を反映しました。",
        )
    elif result["skipped_installations"] > 0:
        set_flash(
            request,
            "error",
            "GitHub App credentials または installation 状態を確認できず、repository を取得できませんでした。GitHub App 設定と installation 状態を確認してください。",
        )
    else:
        set_flash(request, "error", "repository 候補は取得できませんでした。GitHub 側の repository 権限と installation 状態を確認してください。")
    return RedirectResponse(
        url=f"/workspaces/{workspace_slug}/repositories",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/workspaces/{workspace_slug}/repositories/{repository_id}/activate", name="hotdock-workspace-repository-activate")
async def workspace_repository_activate(
    workspace_slug: str,
    repository_id: str,
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(...),
):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/repositories"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/repositories", status_code=status.HTTP_303_SEE_OTHER)

    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="admin")
    repository = db.scalar(
        select(Repository).where(
            Repository.id == repository_id,
            Repository.workspace_id == access.workspace.id,
            Repository.deleted_at.is_(None),
        )
    )
    if repository is None:
        set_flash(request, "error", "repository が見つかりません。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/repositories", status_code=status.HTTP_303_SEE_OTHER)

    try:
        repository = activate_workspace_repository_selection(db, workspace=access.workspace, repository=repository)
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=auth.user.id,
            workspace_id=access.workspace.id,
            target_type="repository",
            target_id=repository.id,
            action="workspace_repository_activated",
            metadata={"github_repository_id": repository.github_repository_id, "activation_mode": "webhook_driven"},
        )
        db.commit()
        set_flash(request, "success", "監視対象を切り替えました。以後はこの repository への push webhook を受けた branch だけを表示します。")
    except Exception as exc:
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=auth.user.id,
            workspace_id=access.workspace.id,
            target_type="repository",
            target_id=repository.id,
            action="workspace_repository_activate_failed",
            metadata={"error": str(getattr(exc, "detail", None) or exc)},
        )
        db.commit()
        set_flash(request, "error", str(getattr(exc, "detail", None) or "repository の切り替えに失敗しました。"))
    return RedirectResponse(url=f"/workspaces/{workspace_slug}/repositories", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/workspaces/{workspace_slug}/repositories/{repository_id}/branches/manual-register", name="hotdock-workspace-branch-manual-register")
async def workspace_branch_manual_register(
    workspace_slug: str,
    repository_id: str,
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(...),
    manual_branch_input: str = Form(...),
    next_path: str | None = Form(default=None),
):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/repositories"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/repositories", status_code=status.HTTP_303_SEE_OTHER)

    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="admin")
    repository = db.scalar(
        select(Repository).where(
            Repository.id == repository_id,
            Repository.workspace_id == access.workspace.id,
            Repository.deleted_at.is_(None),
        )
    )
    if repository is None:
        set_flash(request, "error", "repository が見つかりません。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/repositories", status_code=status.HTTP_303_SEE_OTHER)

    redirect_url = sanitize_next_path(next_path, f"/workspaces/{workspace_slug}/branches")

    try:
        result = await manually_register_branch_snapshot(
            db,
            request,
            workspace=access.workspace,
            repository=repository,
            actor=auth.user,
            raw_text=manual_branch_input,
        )
        request.session[MANUAL_BRANCH_RESULT_SESSION_KEY] = {
            "status": "success",
            "branch_name": result["branch_name"],
            "created": bool(result["created"]),
            "reactivated": bool(result["reactivated"]),
            "parsed_file_count": int(result["parsed_file_count"]),
            "applied_file_count": int(result["applied_file_count"]),
            "collision_recomputed": bool(result["collision_recomputed"]),
            "observed_via": result["observed_via"],
            "touch_seed_source": result["touch_seed_source"],
            "rescued_touch_seed": bool(result["rescued_touch_seed"]),
        }
        success_parts = [
            "ブランチを手動登録しました。",
            f"touched files を {result['applied_file_count']} 件反映しました。",
        ]
        if result["reactivated"]:
            success_parts.append("既存ブランチを再活性化しました。")
        if result["rescued_touch_seed"]:
            success_parts.append("手動登録により touched files を確定しました。")
        success_parts.append("このブランチは衝突判定対象になりました。")
        set_flash(
            request,
            "success",
            " ".join(success_parts),
        )
    except Exception as exc:
        detail = str(getattr(exc, "detail", None) or "手動登録に失敗しました。")
        request.session[MANUAL_BRANCH_RESULT_SESSION_KEY] = {
            "status": "error",
            "message": detail,
        }
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=auth.user.id,
            workspace_id=access.workspace.id,
            target_type="repository",
            target_id=repository.id,
            action="workspace_branch_manual_register_failed",
            metadata={
                "branch_name": (manual_branch_input.splitlines()[0] if manual_branch_input else None),
                "error": detail,
                "source": "manual_diff",
            },
        )
        db.commit()
        set_flash(request, "error", detail)
    return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/workspaces/{workspace_slug}/branches", name="hotdock-workspace-branches")
async def workspace_branches(workspace_slug: str, request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/branches"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="viewer")
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-branches",
        page_title=f"{access.workspace.name} | Branches | Hotdock",
        page_heading="Branches",
        page_description="workspace branch 一覧。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Branches", "href": f"/workspaces/{workspace_slug}/branches"},
        ],
        current_membership=access.membership,
    )
    repositories = {
        repository.id: repository
        for repository in db.scalars(
            select(Repository).where(
                Repository.workspace_id == access.workspace.id,
                Repository.deleted_at.is_(None),
                Repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
                Repository.is_active.is_(True),
                Repository.is_available.is_(True),
            )
        ).all()
    }
    active_repository = next(iter(repositories.values()), None)
    active_repository_ids = list(repositories.keys())
    branches = []
    if active_repository_ids:
        branches = db.scalars(
            select(Branch)
            .where(Branch.workspace_id == access.workspace.id, Branch.repository_id.in_(active_repository_ids))
            .order_by(Branch.last_push_at.desc().nullslast())
        ).all()
    branch_ids = [branch.id for branch in branches]
    branch_files = []
    if branch_ids:
        branch_files = db.scalars(
            select(BranchFile)
            .where(BranchFile.branch_id.in_(branch_ids))
            .order_by(BranchFile.last_seen_at.desc().nullslast(), BranchFile.updated_at.desc())
        ).all()
    files_by_branch: dict[str, list[BranchFile]] = {}
    for file_item in branch_files:
        files_by_branch.setdefault(file_item.branch_id, []).append(file_item)
    context["branches"] = [
        {
            "id": branch.id,
            "repository_id": branch.repository_id,
            "details_id": f"branch-details-{branch.id}",
            "name": branch.name,
            "last_push_at": branch.last_push_at,
            "current_head_sha": branch.current_head_sha or branch.last_commit_sha,
            "touched_files_count": branch.touched_files_count,
            "conflict_files_count": branch.conflict_files_count,
            "branch_status": branch.branch_status,
            "touch_seed_status": branch.touch_seed_status,
            "touch_seed_warning": branch.touch_seed_warning,
            "touch_seed_error_message": branch.touch_seed_error_message,
            "has_authoritative_compare_history": branch.has_authoritative_compare_history,
            "observed_via": branch.observed_via,
            "touch_seed_source": branch.touch_seed_source,
            "is_deleted": branch.is_deleted,
            "repository_name": repositories.get(branch.repository_id).display_name if repositories.get(branch.repository_id) else "-",
            "repository_selection_status": repositories.get(branch.repository_id).selection_status if repositories.get(branch.repository_id) else None,
            "repository_is_available": repositories.get(branch.repository_id).is_available if repositories.get(branch.repository_id) else False,
            "files": files_by_branch.get(branch.id, []),
        }
        for branch in branches
    ]
    context["active_repository"] = active_repository
    context["manual_branch_command_example"] = 'BRANCH="feature/login-form"\necho "BRANCH:$BRANCH"\ngit diff --name-status origin/master..."$BRANCH"'
    context["manual_branch_output_example"] = "BRANCH:feature/login-form\nM\tapp/controllers/login_controller.rb\nA\tapp/views/login/new.html.erb\nR100\tapp/models/user_old.rb\tapp/models/user.rb\nD\tapp/tmp/old_login.txt"
    manual_branch_result = request.session.pop(MANUAL_BRANCH_RESULT_SESSION_KEY, None)
    context["manual_branch_result"] = manual_branch_result if isinstance(manual_branch_result, dict) else None
    return render_app("hotdock/app/workspace_branches.html", context)


@router.get("/workspaces/{workspace_slug}/branches/{branch_id}", name="hotdock-workspace-branch-detail")
async def workspace_branch_detail(workspace_slug: str, branch_id: str, request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/branches/{branch_id}"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="viewer")
    branch = db.scalar(
        select(Branch).where(
            Branch.id == branch_id,
            Branch.workspace_id == access.workspace.id,
        )
    )
    if branch is None:
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/branches", status_code=status.HTTP_303_SEE_OTHER)
    repository = db.get(Repository, branch.repository_id)
    if (
        repository is None
        or repository.workspace_id != access.workspace.id
        or repository.deleted_at is not None
        or not repository.is_active
        or not repository.is_available
        or repository.selection_status != REPOSITORY_SELECTION_ACTIVE
    ):
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/branches", status_code=status.HTTP_303_SEE_OTHER)
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-branches",
        page_title=f"{branch.name} | Branch | Hotdock",
        page_heading="Branch Detail",
        page_description="branch ごとの touched files と collision 状態。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Branches", "href": f"/workspaces/{workspace_slug}/branches"},
            {"label": branch.name, "href": f"/workspaces/{workspace_slug}/branches/{branch_id}"},
        ],
        current_membership=access.membership,
    )
    files = db.scalars(
        select(BranchFile).where(BranchFile.branch_id == branch.id).order_by(BranchFile.is_active.desc(), BranchFile.last_seen_at.desc().nullslast())
    ).all()
    open_collisions = db.scalars(
        select(FileCollisionBranch)
        .join(FileCollision, FileCollision.id == FileCollisionBranch.collision_id)
        .where(
            FileCollision.collision_status == "open",
            FileCollisionBranch.branch_id == branch.id,
        )
        .order_by(FileCollision.updated_at.desc())
    ).all()
    context.update(
        {
            "branch_detail": branch,
            "branch_repository": repository,
            "branch_files": files,
            "branch_collisions": open_collisions,
        }
    )
    return render_app("hotdock/app/workspace_branch_detail.html", context)


@router.get("/workspaces/{workspace_slug}/conflicts", name="hotdock-workspace-conflicts")
async def workspace_conflicts(
    workspace_slug: str,
    request: Request,
    db: Session = Depends(get_db),
    path: str | None = Query(default=None),
    repository_id: str | None = Query(default=None),
):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/conflicts"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="viewer")
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-conflicts",
        page_title=f"{access.workspace.name} | Conflicts | Hotdock",
        page_heading="Conflicts",
        page_description="workspace conflict 一覧。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Conflicts", "href": f"/workspaces/{workspace_slug}/conflicts"},
        ],
        current_membership=access.membership,
    )
    repositories = {repository.id: repository for repository in db.scalars(select(Repository).where(Repository.workspace_id == access.workspace.id)).all()}
    branches = {branch.id: branch for branch in db.scalars(select(Branch).where(Branch.workspace_id == access.workspace.id)).all()}
    collision_rows = db.scalars(
        select(FileCollision)
        .join(Repository, Repository.id == FileCollision.repository_id)
        .where(
            Repository.workspace_id == access.workspace.id,
            Repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
            Repository.is_active.is_(True),
            Repository.is_available.is_(True),
            FileCollision.collision_status == "open",
        )
        .order_by(FileCollision.last_detected_at.desc())
    ).all()
    if path:
        collision_rows = [collision for collision in collision_rows if collision.normalized_path == path]
    if repository_id:
        collision_rows = [collision for collision in collision_rows if collision.repository_id == repository_id]
    context["conflicts"] = [
        {
            "repository_name": repositories.get(collision.repository_id).display_name if repositories.get(collision.repository_id) else "-",
            "repository_id": collision.repository_id,
            "file_path": collision.normalized_path,
            "conflict_status": collision.collision_status,
            "active_branch_count": collision.active_branch_count,
            "first_detected_at": collision.first_detected_at,
            "last_detected_at": collision.last_detected_at,
            "branches": [
                {
                    "name": branches.get(collision_branch.branch_id).name if branches.get(collision_branch.branch_id) else "-",
                    "last_change_type": collision_branch.last_change_type,
                }
                for collision_branch in db.scalars(
                    select(FileCollisionBranch).where(FileCollisionBranch.collision_id == collision.id).order_by(FileCollisionBranch.updated_at.desc())
                ).all()
            ],
        }
        for collision in collision_rows
    ]
    context["conflicts_filter_path"] = path
    context["conflicts_filter_repository_id"] = repository_id
    return render_app("hotdock/app/workspace_conflicts.html", context)


@router.get("/workspaces/{workspace_slug}/members", name="hotdock-workspace-members")
async def workspace_members(workspace_slug: str, request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/members"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="viewer")
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-members",
        page_title=f"{access.workspace.name} | Members | Hotdock",
        page_heading="Members",
        page_description="workspace member 管理。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Members", "href": f"/workspaces/{workspace_slug}/members"},
        ],
        current_membership=access.membership,
    )
    context["members_data"] = workspace_members_data(db, access.workspace)
    return render_app("hotdock/app/workspace_members.html", context)


@router.get("/workspaces/{workspace_slug}/settings", name="hotdock-workspace-settings")
async def workspace_settings(workspace_slug: str, request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/settings"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="admin")
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-settings",
        page_title=f"{access.workspace.name} | Settings | Hotdock",
        page_heading="Workspace Settings",
        page_description="workspace 単位の設定ページ。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Settings", "href": f"/workspaces/{workspace_slug}/settings"},
        ],
        current_membership=access.membership,
    )
    context["settings_sections"] = workspace_settings_data(access.workspace)
    return render_app("hotdock/app/workspace_settings.html", context)


@router.get("/workspaces/{workspace_slug}/billing", name="hotdock-workspace-billing")
async def workspace_billing(workspace_slug: str, request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(
            url=build_login_redirect(f"/workspaces/{workspace_slug}/billing"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="owner")
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-billing",
        page_title=f"{access.workspace.name} | Billing | Hotdock",
        page_heading="Workspace Billing",
        page_description="workspace 単位の請求情報ページ。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Billing", "href": f"/workspaces/{workspace_slug}/billing"},
        ],
        current_membership=access.membership,
    )
    context["billing_overview"] = workspace_billing_data(access.workspace)
    return render_app("hotdock/app/workspace_billing.html", context)
