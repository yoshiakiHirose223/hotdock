from __future__ import annotations

from typing import Any

import hmac

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette import status

from app.core.database import get_db
from app.hotdock.services.auth import (
    attach_auth_context,
    build_login_redirect,
    default_workspace_for_user,
    get_flash,
    set_flash,
)
from app.hotdock.services.context import build_app_context
from app.hotdock.services.github import manual_sync_workspace_installation_repositories
from app.hotdock.services.workspaces import (
    build_workspace_navigation,
    resolve_workspace_access,
    workspace_dashboard_data,
    workspace_members_data,
)
from app.models.branch import Branch
from app.models.conflict import Conflict
from app.models.repository import Repository
from app.models.workspace import Workspace

router = APIRouter()


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
            "app_navigation": build_workspace_navigation(workspace.slug),
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
        page_title=f"{access.workspace.name} | Dashboard | Hotdock",
        page_heading="Workspace Dashboard",
        page_description="workspace 単位のダッシュボード。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
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
    context = workspace_page_context(
        request,
        db,
        workspace=access.workspace,
        active_nav="workspace-repositories",
        page_title=f"{access.workspace.name} | Repositories | Hotdock",
        page_heading="Repositories",
        page_description="workspace repository 一覧。",
        breadcrumbs=[
            {"label": "Dashboard", "href": f"/workspaces/{workspace_slug}/dashboard"},
            {"label": "Repositories", "href": f"/workspaces/{workspace_slug}/repositories"},
        ],
        current_membership=access.membership,
    )
    repositories = db.scalars(
        select(Repository).where(Repository.workspace_id == access.workspace.id, Repository.deleted_at.is_(None))
    ).all()
    context["repositories"] = repositories
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
            message = "GitHub 側の認証または installation 状態を確認してください。"
        set_flash(request, "error", message)
        return RedirectResponse(
            url=f"/workspaces/{workspace_slug}/repositories",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    if result["repositories_synced"] > 0:
        set_flash(
            request,
            "success",
            f"{result['repositories_synced']} 件の repository と {result['branches_synced']} 件の branch を反映しました。",
        )
    elif result["skipped_installations"] > 0:
        set_flash(
            request,
            "error",
            "GitHub 認証済みユーザーのトークンが見つからず、repository を取得できませんでした。GitHub App claim を行ったアカウントで再ログインしてください。",
        )
    else:
        set_flash(request, "error", "repository は取得できませんでした。GitHub 側の repository 権限と installation 状態を確認してください。")
    return RedirectResponse(
        url=f"/workspaces/{workspace_slug}/repositories",
        status_code=status.HTTP_303_SEE_OTHER,
    )


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
    repositories = {repository.id: repository for repository in db.scalars(select(Repository).where(Repository.workspace_id == access.workspace.id)).all()}
    branches = db.scalars(select(Branch).where(Branch.workspace_id == access.workspace.id)).all()
    context["branches"] = [
        {
            "name": branch.name,
            "last_push_at": branch.last_push_at,
            "touched_files_count": branch.touched_files_count,
            "conflict_files_count": branch.conflict_files_count,
            "repository_name": repositories.get(branch.repository_id).display_name if repositories.get(branch.repository_id) else "-",
        }
        for branch in branches
    ]
    return render_app("hotdock/app/workspace_branches.html", context)


@router.get("/workspaces/{workspace_slug}/conflicts", name="hotdock-workspace-conflicts")
async def workspace_conflicts(workspace_slug: str, request: Request, db: Session = Depends(get_db)):
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
    conflicts = db.scalars(
        select(Conflict).where(Conflict.workspace_id == access.workspace.id, Conflict.conflict_status == "open")
    ).all()
    context["conflicts"] = [
        {
            "repository_name": repositories.get(conflict.repository_id).display_name if repositories.get(conflict.repository_id) else "-",
            "branch_name": branches.get(conflict.primary_branch_id).name if branches.get(conflict.primary_branch_id) else "-",
            "file_path": conflict.file_path,
            "conflict_status": conflict.conflict_status,
            "last_detected_at": conflict.last_detected_at,
        }
        for conflict in conflicts
    ]
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
