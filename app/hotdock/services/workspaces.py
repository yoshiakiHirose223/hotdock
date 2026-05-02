from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import quote

from fastapi import HTTPException, Request
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.hotdock.services.audit import record_audit_log
from app.hotdock.services.auth import deny_workspace_access, require_role
from app.hotdock.services.security import future_invitation_expiry, generate_token, hash_password, hash_token, utcnow
from app.models.branch_event import BranchEvent
from app.models.branch_file import BranchFile
from app.models.conflict import Conflict
from app.models.file_collision_branch import FileCollisionBranch
from app.models.github_install_intent import GithubInstallIntent
from app.models.user import User
from app.models.workspace import Workspace
from app.models.workspace_invitation import WorkspaceInvitation
from app.models.workspace_member import WorkspaceMember
from app.models.github_installation import GithubInstallation
from app.models.github_installation_repository import GithubInstallationRepository
from app.models.github_webhook_event import GithubWebhookEvent
from app.models.github_pending_claim import GithubPendingClaim
from app.models.repository import Repository
from app.models.branch import Branch
from app.models.file_collision import FileCollision
from app.models.audit_log import AuditLog
from app.hotdock.services.github import (
    REPOSITORY_SELECTION_ACTIVE,
    REPOSITORY_SELECTION_INACCESSIBLE,
    REPOSITORY_SELECTION_INACTIVE,
    active_repository_limit,
)


@dataclass
class WorkspaceAccess:
    workspace: Workspace
    membership: WorkspaceMember


def slugify_workspace(value: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    slug = "-".join(filter(None, slug.split("-")))
    return slug[:80]


def create_user(db: Session, *, email: str, password: str, display_name: str) -> User:
    user = User(
        email=email.lower().strip(),
        password_hash=hash_password(password),
        display_name=display_name.strip(),
        status="active",
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def create_workspace(db: Session, request: Request, *, user: User, name: str, slug: str | None = None) -> Workspace:
    resolved_slug = slugify_workspace(slug or name)
    if not resolved_slug:
        raise HTTPException(status_code=400, detail="Workspace slug is required")
    if db.scalar(select(Workspace).where(Workspace.slug == resolved_slug)) is not None:
        raise HTTPException(status_code=400, detail="Workspace slug already exists")

    workspace = Workspace(name=name.strip(), slug=resolved_slug, created_by_user_id=user.id, status="active")
    db.add(workspace)
    db.flush()
    member = WorkspaceMember(
        workspace_id=workspace.id,
        user_id=user.id,
        role="owner",
        status="active",
        joined_at=utcnow(),
    )
    db.add(member)
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=workspace.id,
        target_type="workspace",
        target_id=workspace.id,
        action="workspace_create",
        metadata={"workspace_slug": workspace.slug},
    )
    db.commit()
    db.refresh(workspace)
    return workspace


def resolve_workspace_access(db: Session, request: Request, *, user: User | None, workspace_slug: str, required_role: str = "viewer") -> WorkspaceAccess:
    workspace = db.scalar(select(Workspace).where(Workspace.slug == workspace_slug, Workspace.deleted_at.is_(None)))
    if workspace is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    if user is None:
        deny_workspace_access(db, request, workspace_slug, None)
        raise HTTPException(status_code=403, detail="Forbidden")

    membership = db.scalar(
        select(WorkspaceMember).where(
            WorkspaceMember.workspace_id == workspace.id,
            WorkspaceMember.user_id == user.id,
            WorkspaceMember.status == "active",
            WorkspaceMember.revoked_at.is_(None),
        )
    )
    if membership is None:
        deny_workspace_access(db, request, workspace_slug, user.id)
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        require_role(membership, required_role)
    except HTTPException:
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=user.id,
            workspace_id=workspace.id,
            target_type="workspace",
            target_id=workspace.id,
            action="workspace_role_access_denied",
            metadata={"workspace_slug": workspace.slug, "required_role": required_role, "actual_role": membership.role},
        )
        db.commit()
        raise
    return WorkspaceAccess(workspace=workspace, membership=membership)


def invite_workspace_member(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    inviter: User,
    inviter_membership: WorkspaceMember,
    email: str,
    role: str,
) -> tuple[WorkspaceInvitation, str]:
    require_role(inviter_membership, "owner")
    normalized_email = email.lower().strip()
    if role not in {"admin", "member", "viewer"}:
        raise HTTPException(status_code=400, detail="Invalid role")

    token = generate_token()
    invitation = WorkspaceInvitation(
        workspace_id=workspace.id,
        email=normalized_email,
        role=role,
        invitation_token_hash=hash_token(token),
        invited_by_user_id=inviter.id,
        expires_at=future_invitation_expiry(),
        status="pending",
    )
    db.add(invitation)
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=inviter.id,
        workspace_id=workspace.id,
        target_type="workspace_member",
        target_id=normalized_email,
        action="workspace_member_invite",
        metadata={"role": role},
    )
    db.commit()
    db.refresh(invitation)
    return invitation, token


def accept_workspace_invitation(
    db: Session,
    request: Request,
    *,
    invitation: WorkspaceInvitation,
    user: User,
) -> WorkspaceMember:
    if invitation.status != "pending" or invitation.revoked_at is not None or invitation.expires_at <= utcnow():
        raise HTTPException(status_code=410, detail="Invitation expired")
    if invitation.email != user.email.lower().strip():
        raise HTTPException(status_code=403, detail="Invitation email does not match current user")

    membership = db.scalar(
        select(WorkspaceMember).where(
            WorkspaceMember.workspace_id == invitation.workspace_id,
            WorkspaceMember.user_id == user.id,
        )
    )
    if membership is None:
        membership = WorkspaceMember(
            workspace_id=invitation.workspace_id,
            user_id=user.id,
            role=invitation.role,
            status="active",
            invited_by_user_id=invitation.invited_by_user_id,
            invited_email=invitation.email,
            joined_at=utcnow(),
        )
        db.add(membership)
    else:
        membership.role = invitation.role
        membership.status = "active"
        membership.revoked_at = None
        membership.joined_at = membership.joined_at or utcnow()

    invitation.accepted_by_user_id = user.id
    invitation.accepted_at = utcnow()
    invitation.status = "accepted"
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=invitation.workspace_id,
        target_type="workspace_member",
        target_id=user.id,
        action="workspace_member_accept",
        metadata={"role": invitation.role},
    )
    db.commit()
    db.refresh(membership)
    return membership


def revoke_workspace_member(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    actor: User,
    actor_membership: WorkspaceMember,
    member: WorkspaceMember,
) -> None:
    require_role(actor_membership, "owner")
    _ensure_workspace_member_belongs_to_workspace(member, workspace)
    _assert_owner_lifecycle_allows_change(
        db,
        request,
        workspace=workspace,
        actor=actor,
        member=member,
        next_role=None,
        action="revoke",
    )
    member.status = "revoked"
    member.revoked_at = utcnow()
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=actor.id,
        workspace_id=workspace.id,
        target_type="workspace_member",
        target_id=member.user_id,
        action="workspace_member_revoke",
        metadata={"role": member.role},
    )
    db.commit()


def update_workspace_member_role(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    actor: User,
    actor_membership: WorkspaceMember,
    member: WorkspaceMember,
    new_role: str,
) -> WorkspaceMember:
    require_role(actor_membership, "owner")
    _ensure_workspace_member_belongs_to_workspace(member, workspace)
    if new_role not in {"owner", "admin", "member", "viewer"}:
        raise HTTPException(status_code=400, detail="Invalid role")
    if member.status != "active" or member.revoked_at is not None:
        raise HTTPException(status_code=400, detail="Only active members can change roles")
    if member.role == new_role:
        return member

    _assert_owner_lifecycle_allows_change(
        db,
        request,
        workspace=workspace,
        actor=actor,
        member=member,
        next_role=new_role,
        action="role_change",
    )

    previous_role = member.role
    member.role = new_role
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=actor.id,
        workspace_id=workspace.id,
        target_type="workspace_member",
        target_id=member.user_id,
        action="workspace_member_role_changed",
        metadata={"previous_role": previous_role, "new_role": new_role},
    )
    db.commit()
    db.refresh(member)
    return member


def leave_workspace(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    member: WorkspaceMember,
    actor: User,
) -> None:
    try:
        locked_workspace = db.scalar(
            select(Workspace).where(Workspace.id == workspace.id, Workspace.deleted_at.is_(None)).with_for_update()
        )
        if locked_workspace is None:
            raise HTTPException(status_code=404, detail="Workspace not found")
        locked_member = db.scalar(
            select(WorkspaceMember).where(WorkspaceMember.id == member.id).with_for_update()
        )
        if locked_member is None:
            raise HTTPException(status_code=404, detail="Membership not found")

        _ensure_workspace_member_belongs_to_workspace(locked_member, locked_workspace)
        if locked_member.user_id != actor.id:
            raise HTTPException(status_code=403, detail="You can only leave your own membership")
        if locked_member.status != "active" or locked_member.revoked_at is not None:
            raise HTTPException(status_code=400, detail="Membership is already inactive")

        _assert_owner_lifecycle_allows_change(
            db,
            request,
            workspace=locked_workspace,
            actor=actor,
            member=locked_member,
            next_role=None,
            action="leave",
        )

        db.execute(
            delete(GithubPendingClaim).where(
                GithubPendingClaim.workspace_id == locked_workspace.id,
                GithubPendingClaim.user_id == actor.id,
            )
        )
        db.execute(
            delete(WorkspaceInvitation).where(
                WorkspaceInvitation.workspace_id == locked_workspace.id,
                (WorkspaceInvitation.email == actor.email.lower().strip()) | (WorkspaceInvitation.accepted_by_user_id == actor.id),
            )
        )
        db.execute(delete(WorkspaceMember).where(WorkspaceMember.id == locked_member.id))
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=actor.id,
            workspace_id=locked_workspace.id,
            target_type="workspace_member",
            target_id=actor.id,
            action="workspace_member_leave",
            metadata={
                "result": "success",
                "actor_email": actor.email,
                "target_workspace_id": locked_workspace.id,
                "target_workspace_name": locked_workspace.name,
                "role": locked_member.role,
            },
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        if not isinstance(exc, HTTPException):
            record_audit_log(
                db,
                request,
                actor_type="user",
                actor_id=actor.id,
                workspace_id=workspace.id,
                target_type="workspace_member",
                target_id=actor.id,
                action="workspace_member_leave",
                metadata={
                    "result": "failed",
                    "actor_email": actor.email,
                    "target_workspace_id": workspace.id,
                    "target_workspace_name": workspace.name,
                    "role": member.role,
                    "error": str(exc),
                },
            )
            db.commit()
        raise


def delete_workspace_and_related_data(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    actor: User,
    actor_membership: WorkspaceMember,
) -> None:
    require_role(actor_membership, "owner")
    if actor_membership.workspace_id != workspace.id or actor_membership.user_id != actor.id:
        raise HTTPException(status_code=403, detail="Workspace delete is not allowed")

    try:
        locked_workspace = db.scalar(
            select(Workspace).where(Workspace.id == workspace.id, Workspace.deleted_at.is_(None)).with_for_update()
        )
        if locked_workspace is None:
            raise HTTPException(status_code=404, detail="Workspace not found")
        locked_actor_membership = db.scalar(
            select(WorkspaceMember).where(WorkspaceMember.id == actor_membership.id).with_for_update()
        )
        if locked_actor_membership is None:
            raise HTTPException(status_code=403, detail="Workspace delete is not allowed")
        now = utcnow()
        repository_ids = db.scalars(
            select(Repository.id).where(Repository.workspace_id == locked_workspace.id)
        ).all()
        branch_ids = db.scalars(
            select(Branch.id).where(Branch.workspace_id == locked_workspace.id)
        ).all()
        collision_ids = db.scalars(
            select(FileCollision.id).where(FileCollision.repository_id.in_(repository_ids) if repository_ids else False)
        ).all() if repository_ids else []

        db.scalars(
            select(GithubPendingClaim.id).where(GithubPendingClaim.workspace_id == locked_workspace.id).with_for_update()
        ).all()

        installations = db.scalars(
            select(GithubInstallation).where(GithubInstallation.claimed_workspace_id == locked_workspace.id).with_for_update()
        ).all()
        installation_ref_ids = [installation.id for installation in installations]

        for installation in installations:
            installation.claimed_workspace_id = None
            installation.claimed_by_user_id = None
            installation.installation_status = "unlinked"
            installation.unlink_requested_at = now
            installation.unlinked_at = now
            installation.unlinked_by_user_id = actor.id

        if installation_ref_ids:
            db.execute(
                delete(GithubInstallationRepository).where(
                    GithubInstallationRepository.workspace_id == locked_workspace.id
                )
            )

        if collision_ids:
            db.execute(delete(FileCollisionBranch).where(FileCollisionBranch.collision_id.in_(collision_ids)))
        if repository_ids:
            db.execute(delete(FileCollision).where(FileCollision.repository_id.in_(repository_ids)))
            db.execute(delete(Conflict).where(Conflict.repository_id.in_(repository_ids)))
            db.execute(delete(BranchEvent).where(BranchEvent.repository_id.in_(repository_ids)))
        if branch_ids:
            db.execute(delete(BranchFile).where(BranchFile.branch_id.in_(branch_ids)))
            db.execute(delete(Branch).where(Branch.id.in_(branch_ids)))
        if repository_ids:
            db.execute(delete(Repository).where(Repository.id.in_(repository_ids)))

        db.execute(delete(GithubPendingClaim).where(GithubPendingClaim.workspace_id == locked_workspace.id))
        db.execute(delete(WorkspaceInvitation).where(WorkspaceInvitation.workspace_id == locked_workspace.id))
        db.execute(delete(GithubInstallIntent).where(GithubInstallIntent.workspace_slug == locked_workspace.slug))
        db.execute(delete(WorkspaceMember).where(WorkspaceMember.workspace_id == locked_workspace.id))
        db.execute(delete(Workspace).where(Workspace.id == locked_workspace.id))

        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=actor.id,
            workspace_id=locked_workspace.id,
            target_type="workspace",
            target_id=locked_workspace.id,
            action="workspace_deleted",
            metadata={
                "result": "success",
                "actor_email": actor.email,
                "target_workspace_id": locked_workspace.id,
                "target_workspace_name": locked_workspace.name,
                "actor_role": locked_actor_membership.role,
                "repository_count": len(repository_ids),
                "branch_count": len(branch_ids),
                "linked_installation_count": len(installations),
            },
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        if not isinstance(exc, HTTPException):
            record_audit_log(
                db,
                request,
                actor_type="user",
                actor_id=actor.id,
                workspace_id=workspace.id,
                target_type="workspace",
                target_id=workspace.id,
                action="workspace_deleted",
                metadata={
                    "result": "failed",
                    "actor_email": actor.email,
                    "target_workspace_id": workspace.id,
                    "target_workspace_name": workspace.name,
                    "actor_role": actor_membership.role,
                    "error": str(exc),
                },
            )
            db.commit()
        raise


def list_user_workspaces(db: Session, user: User) -> list[WorkspaceAccess]:
    memberships = db.scalars(
        select(WorkspaceMember).where(
            WorkspaceMember.user_id == user.id,
            WorkspaceMember.status == "active",
            WorkspaceMember.revoked_at.is_(None),
        )
    ).all()
    results: list[WorkspaceAccess] = []
    for membership in memberships:
        workspace = db.get(Workspace, membership.workspace_id)
        if workspace is not None and workspace.deleted_at is None:
            results.append(WorkspaceAccess(workspace=workspace, membership=membership))
    return results


def build_workspace_navigation(workspace_slug: str, current_role: str | None = None) -> list[dict[str, object]]:
    base = f"/workspaces/{workspace_slug}"
    overview_items = [
        {"label": "ダッシュボード", "href": f"{base}/dashboard", "key": "workspace-dashboard", "icon": "dashboard"},
    ]
    monitoring_items = [
        {"label": "ファイルツリー", "href": f"{base}/file-tree", "key": "workspace-file-tree", "icon": "account_tree"},
        {"label": "リポジトリ", "href": f"{base}/repositories", "key": "workspace-repositories", "icon": "source"},
        {"label": "ブランチ", "href": f"{base}/branches", "key": "workspace-branches", "icon": "fork_right"},
        {"label": "競合", "href": f"{base}/conflicts", "key": "workspace-conflicts", "icon": "warning"},
    ]
    workspace_items = [
        {"label": "メンバー", "href": f"{base}/members", "key": "workspace-members", "icon": "groups"},
        {"label": "設定", "href": f"{base}/settings", "key": "workspace-settings", "icon": "settings"},
        {"label": "請求", "href": f"{base}/billing", "key": "workspace-billing", "icon": "payments"},
        {"label": "GitHub", "href": f"/settings/integrations/github?workspace={workspace_slug}", "key": "workspace-github", "icon": "integration_instructions"},
    ]
    if current_role not in {"owner", "admin"}:
        workspace_items = [
            item for item in workspace_items if item["key"] not in {"workspace-settings", "workspace-github"}
        ]
    if current_role != "owner":
        workspace_items = [item for item in workspace_items if item["key"] != "workspace-billing"]
    return [
        {"title": "概要", "items": overview_items},
        {"title": "監視", "items": monitoring_items},
        {"title": "ワークスペース", "items": workspace_items},
    ]


def _format_timestamp(value) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M")


def _repository_row_state(repository: Repository) -> tuple[str, str, str]:
    if not repository.is_available or repository.selection_status == REPOSITORY_SELECTION_INACCESSIBLE:
        return "エラー", "is-conflict", "GitHub App から現在アクセスできません"
    if repository.detail_sync_status == "error":
        return "エラー", "is-conflict", repository.detail_sync_error_message or "同期に失敗しました"
    if repository.detail_sync_status == "syncing":
        return "同期中", "is-stale", "同期を進めています"
    if repository.selection_status == REPOSITORY_SELECTION_ACTIVE:
        return "監視中", "is-available", "現在の監視対象です"
    if repository.selection_status == REPOSITORY_SELECTION_INACTIVE:
        return "未監視", "is-stale", "以前の監視対象です"
    return "未選択", "", "候補から選択できます"


def _installation_status_badge(installation: GithubInstallation) -> tuple[str, str, str]:
    if installation.installation_status == "active":
        return "接続中", "is-available", "GitHub App は正常に接続されています"
    if installation.installation_status == "suspended":
        return "警告", "is-stale", "GitHub 側の状態を確認してください"
    if installation.installation_status in {"uninstalled", "unlinked"}:
        return "未接続", "is-conflict", "再連携が必要です"
    return "確認中", "", "接続状態を確認しています"


def _sync_health_summary(
    *,
    installations: list[GithubInstallation],
    repositories: list[Repository],
    active_repositories: list[Repository],
    conflicts: list[FileCollision],
) -> dict[str, object]:
    latest_sync_at = max((repository.last_synced_at for repository in repositories if repository.last_synced_at), default=None)
    latest_webhook_at = max(
        (installation.last_webhook_event_at for installation in installations if installation.last_webhook_event_at),
        default=None,
    )
    has_error = any(
        (not repository.is_available)
        or repository.selection_status == REPOSITORY_SELECTION_INACCESSIBLE
        or repository.detail_sync_status == "error"
        for repository in repositories
    )
    is_syncing = any(repository.detail_sync_status == "syncing" for repository in repositories)
    if not installations:
        return {
            "status": "未接続",
            "tone": "is-conflict",
            "subtitle": "GitHub App を連携してください",
            "details": [
                f"最終同期 {_format_timestamp(latest_sync_at)}",
                "最終 webhook -",
                "repository 未接続",
                "branch 未設定",
            ],
            "latest_sync_at": _format_timestamp(latest_sync_at),
        }
    if has_error or conflicts:
        status = "警告"
        tone = "is-conflict"
        subtitle = "確認が必要な項目があります"
    elif not active_repositories:
        status = "要設定"
        tone = "is-stale"
        subtitle = "監視対象 repository を選択してください"
    elif is_syncing:
        status = "同期中"
        tone = "is-stale"
        subtitle = "状態を更新しています"
    else:
        status = "正常"
        tone = "is-available"
        subtitle = "監視は正常です"
    return {
        "status": status,
        "tone": tone,
        "subtitle": subtitle,
        "details": [
            f"最終同期 {_format_timestamp(latest_sync_at)}",
            f"最終 webhook {_format_timestamp(latest_webhook_at)}",
            f"repository {'正常' if repositories and not has_error else '要確認' if has_error else '未接続'}",
            f"branch {'監視中' if active_repositories else '未設定'}",
        ],
        "latest_sync_at": _format_timestamp(latest_sync_at),
    }


def _dashboard_action(label: str, href: str, variant: str = "secondary") -> dict[str, str]:
    return {"label": label, "href": href, "variant": variant}


def _recent_event_label(event: AuditLog) -> tuple[str, str]:
    mapping = {
        "file_collision_detected": ("競合候補を検知", "競合一覧で確認できます"),
        "file_collision_resolved": ("競合候補を解消", "最新状態へ更新しました"),
        "workspace_repository_activated": ("監視対象 repository を更新", "監視対象を切り替えました"),
        "workspace_repository_sync_requested": ("repository を再同期", "候補一覧を更新しています"),
        "workspace_branch_manual_registered": ("ブランチを手動登録", "touched files を反映しました"),
        "claim_succeeded": ("GitHub App を接続", "repository の取り込み準備が整いました"),
        "branch_compare_failed": ("branch 同期で警告", "次回以降の更新で再試行されます"),
    }
    return mapping.get(event.action, ("最近の更新", event.action.replace("_", " ")))


_DIRECTORY_ACTIVITY_STATUS_PRIORITY = {
    "conflict": 0,
    "recent_7": 1,
    "recent_28": 2,
    "stale": 3,
    "unknown": 4,
}


def _directory_activity_status(*, last_updated_at, is_conflict: bool, now) -> str:
    if is_conflict:
        return "conflict"
    if last_updated_at is None:
        return "unknown"
    age = now - last_updated_at
    if age <= timedelta(days=7):
        return "recent_7"
    if age <= timedelta(days=28):
        return "recent_28"
    return "stale"


def _directory_activity_status_label(status: str) -> str:
    return {
        "conflict": "競合",
        "recent_7": "7日以内に更新",
        "recent_28": "28日以内に更新",
        "stale": "長期間更新なし",
        "unknown": "不明",
    }.get(status, "不明")


def _directory_activity_status_class(status: str) -> str:
    return f"is-{status.replace('_', '-')}"


def _directory_activity_source_label(source_labels: set[str]) -> str:
    if not source_labels:
        return "不明"
    if source_labels == {"手動登録"}:
        return "手動登録"
    if source_labels == {"Webhook"}:
        return "Webhook"
    return "混在"


def _directory_activity_relative_time(value, now) -> str:
    if value is None:
        return "不明"
    delta = now - value
    total_seconds = max(int(delta.total_seconds()), 0)
    if total_seconds < 60:
        return "たった今"
    minutes = total_seconds // 60
    if minutes < 60:
        return f"{minutes}分前"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}時間前"
    days = hours // 24
    if days < 30:
        return f"{days}日前"
    return f"{days // 30}か月前"


def _dashboard_recent_or_timestamp(value, now) -> str:
    if value is None:
        return "-"
    if now - value < timedelta(days=1):
        return _directory_activity_relative_time(value, now)
    return value.strftime("%Y-%m-%d %H:%M")


def _dashboard_payload_actor(payload: dict | None) -> str | None:
    if not payload:
        return None
    sender = payload.get("sender") or {}
    pusher = payload.get("pusher") or {}
    return sender.get("login") or pusher.get("name") or pusher.get("email")


def _dashboard_payload_branch_name(payload: dict | None) -> str | None:
    if not payload:
        return None
    ref = str(payload.get("ref") or "")
    if ref.startswith("refs/heads/"):
        return ref.removeprefix("refs/heads/")
    return None


def _dashboard_payload_changed_file_count(payload: dict | None) -> int:
    if not payload:
        return 0
    paths: set[str] = set()
    for commit in payload.get("commits") or []:
        for key in ("added", "modified", "removed"):
            for path in commit.get(key) or []:
                if path:
                    paths.add(str(path))
    return len(paths)


def _dashboard_related_branch_names(
    db: Session,
    *,
    repository_id: str,
    normalized_path: str,
) -> list[str]:
    rows = db.execute(
        select(Branch.name)
        .join(BranchFile, BranchFile.branch_id == Branch.id)
        .where(
            Branch.repository_id == repository_id,
            Branch.is_active.is_(True),
            Branch.is_deleted.is_(False),
            BranchFile.normalized_path == normalized_path,
            BranchFile.is_active.is_(True),
        )
        .order_by(Branch.name.asc())
    ).all()
    names: list[str] = []
    for (name,) in rows:
        if name not in names:
            names.append(name)
    return names


def _dashboard_branch_list_label(names: list[str], *, limit: int = 3) -> str:
    if not names:
        return "-"
    if len(names) <= limit:
        return ", ".join(names)
    return f"{', '.join(names[:limit])} ほか{len(names) - limit}件"


def _build_directory_activity_data(
    db: Session,
    *,
    workspace: Workspace,
    branches: list[Branch],
    branches_href: str,
    conflicts_href: str,
) -> dict[str, object]:
    now = utcnow()
    open_collisions = db.scalars(
        select(FileCollision)
        .join(Repository, Repository.id == FileCollision.repository_id)
        .where(
            Repository.workspace_id == workspace.id,
            Repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
            Repository.is_active.is_(True),
            Repository.is_available.is_(True),
            FileCollision.collision_status == "open",
        )
    ).all()
    open_collision_keys = {(collision.repository_id, collision.normalized_path) for collision in open_collisions}
    branch_file_rows = db.execute(
        select(BranchFile, Branch, Repository)
        .join(Branch, Branch.id == BranchFile.branch_id)
        .join(Repository, Repository.id == Branch.repository_id)
        .where(
            Repository.workspace_id == workspace.id,
            Repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
            Repository.is_active.is_(True),
            Repository.is_available.is_(True),
            Branch.is_active.is_(True),
            Branch.is_deleted.is_(False),
            BranchFile.is_active.is_(True),
        )
    ).all()

    file_entries: dict[tuple[str, str], dict[str, object]] = {}
    for branch_file, branch, repository in branch_file_rows:
        normalized_path = branch_file.normalized_path or branch_file.path
        if not normalized_path:
            continue
        key = (repository.id, normalized_path)
        source_label = "手動登録" if branch.observed_via == "manual" or branch_file.source_kind == "manual_input" else "Webhook"
        observed_at = branch_file.last_seen_at or branch_file.observed_at or branch.last_push_at or branch.updated_at
        branch_href = f"{branches_href}?branch={quote(branch.name)}"
        entry = file_entries.get(key)
        if entry is None:
            entry = {
                "repository_id": repository.id,
                "repository_name": repository.display_name,
                "repository_full_name": repository.full_name,
                "path": branch_file.path,
                "normalized_path": normalized_path,
                "last_updated_at": observed_at,
                "source_labels": {source_label},
                "branch_map": {},
                "is_conflict": key in open_collision_keys or bool(branch_file.is_conflict),
            }
            file_entries[key] = entry
        else:
            entry["source_labels"].add(source_label)
            entry["is_conflict"] = bool(entry["is_conflict"]) or key in open_collision_keys or bool(branch_file.is_conflict)
            current_last_updated = entry.get("last_updated_at")
            if observed_at and (current_last_updated is None or observed_at > current_last_updated):
                entry["last_updated_at"] = observed_at
                entry["path"] = branch_file.path
        branch_seen_at = observed_at
        branch_map = entry["branch_map"]
        existing_branch = branch_map.get(branch.id)
        if existing_branch is None or (
            branch_seen_at is not None
            and (existing_branch.get("last_updated_at") is None or branch_seen_at > existing_branch.get("last_updated_at"))
        ):
            branch_map[branch.id] = {
                "id": branch.id,
                "name": branch.name,
                "href": branch_href,
                "last_updated_at": branch_seen_at,
            }

    branch_options = [
        {"value": branch.name, "label": branch.name}
        for branch in sorted(branches, key=lambda item: item.name.lower())
        if not branch.is_deleted and branch.is_active
    ]

    if not file_entries:
        payload = {
            "filters": {
                "branches": branch_options,
                "statuses": [
                    {"value": "conflict", "label": "競合"},
                    {"value": "recent_7", "label": "7日以内に更新"},
                    {"value": "recent_28", "label": "28日以内に更新"},
                    {"value": "stale", "label": "長期間更新なし"},
                    {"value": "unknown", "label": "不明"},
                ],
                "directories": [],
            },
            "root_ids": [],
            "nodes": [],
            "default_expanded": [],
            "initial_selected_id": None,
        }
        return {
            "is_empty": True,
            "summary_cards": [
                {"label": "競合", "value": "0", "meta": "競合中のパス", "class": "is-conflict"},
                {"label": "7日以内更新", "value": "0", "meta": "最近更新されたパス", "class": ""},
                {"label": "28日以内更新", "value": "0", "meta": "観測済みのパス", "class": ""},
                {"label": "長期間更新なし", "value": "0", "meta": "28日以上更新なし", "class": ""},
                {"label": "手動追跡", "value": "0", "meta": "手動登録ソース", "class": ""},
            ],
            "empty_title": "まだ観測済みファイルはありません",
            "empty_body": "Push/Webhook または手動登録で検知されたファイルが表示されます",
            "empty_action": _dashboard_action("ブランチへ移動", branches_href),
            "legend_items": [
                {"label": "赤 = 競合", "class": "is-conflict"},
                {"label": "緑 = 7日以内に更新", "class": "is-recent-7"},
                {"label": "黄 = 28日以内に更新", "class": "is-recent-28"},
                {"label": "灰 = 長期間更新なし", "class": "is-stale"},
            ],
            "note": "この画面は観測済みファイルのみ表示対象です。push/webhook または手動登録で検知されたもののみ対象です。",
            "json": json.dumps(payload, ensure_ascii=False).replace("</", "<\\/"),
        }

    def _branch_sort_key(item: dict[str, object]):
        last_updated_at = item.get("last_updated_at")
        return (
            0 if last_updated_at else 1,
            -(last_updated_at.timestamp()) if last_updated_at else 0,
            str(item["name"]).lower(),
        )

    directory_options_map: dict[str, str] = {}
    file_nodes: list[dict[str, object]] = []
    for entry in file_entries.values():
        normalized_path = str(entry["normalized_path"])
        path_parts = [segment for segment in normalized_path.split("/") if segment]
        top_directory = path_parts[0] if path_parts else "__root__"
        directory_value = f"{entry['repository_id']}::{top_directory}"
        directory_label = (
            f"{entry['repository_name']} / {top_directory}"
            if top_directory != "__root__"
            else f"{entry['repository_name']} / root"
        )
        directory_options_map[directory_value] = directory_label
        branch_items = sorted(entry["branch_map"].values(), key=_branch_sort_key)
        status = _directory_activity_status(
            last_updated_at=entry["last_updated_at"],
            is_conflict=bool(entry["is_conflict"]),
            now=now,
        )
        full_path = f"{entry['repository_full_name']} / {normalized_path}"
        file_nodes.append(
            {
                "id": f"file:{entry['repository_id']}:{normalized_path}",
                "type": "file",
                "name": path_parts[-1] if path_parts else normalized_path,
                "path": normalized_path,
                "full_path": full_path,
                "repository_id": entry["repository_id"],
                "repository_name": entry["repository_name"],
                "repository_full_name": entry["repository_full_name"],
                "directory_value": directory_value,
                "status": status,
                "status_label": _directory_activity_status_label(status),
                "status_class": _directory_activity_status_class(status),
                "last_active_at": entry["last_updated_at"],
                "last_active_label": _directory_activity_relative_time(entry["last_updated_at"], now),
                "source_label": _directory_activity_source_label(entry["source_labels"]),
                "source_labels": set(entry["source_labels"]),
                "is_manual": "手動登録" in entry["source_labels"],
                "is_conflict": bool(entry["is_conflict"]),
                "conflict_count": 1 if status == "conflict" else 0,
                "recent_count": 1 if status == "recent_7" else 0,
                "file_count": 1,
                "branch_items": branch_items,
                "branch_names": [str(item["name"]) for item in branch_items],
                "branch_preview": branch_items[:2],
                "branch_overflow": max(len(branch_items) - 2, 0),
                "child_ids": [],
                "search_text": " ".join(
                    [
                        full_path.lower(),
                        " ".join(str(item["name"]).lower() for item in branch_items),
                        str(entry["repository_full_name"]).lower(),
                    ]
                ),
                "copy_text": full_path,
                "conflict_href": (
                    f"{conflicts_href}?path={quote(normalized_path)}&repository_id={quote(str(entry['repository_id']))}"
                    if bool(entry["is_conflict"])
                    else None
                ),
            }
        )

    nodes: dict[str, dict[str, object]] = {}
    root_ids: list[str] = []

    def ensure_directory_node(
        *,
        node_id: str,
        name: str,
        path: str,
        full_path: str,
        repository_id: str,
        repository_name: str,
        repository_full_name: str,
        parent_id: str | None,
        directory_value: str,
    ) -> dict[str, object]:
        existing = nodes.get(node_id)
        if existing is not None:
            return existing
        node = {
            "id": node_id,
            "type": "directory",
            "name": name,
            "path": path,
            "full_path": full_path,
            "repository_id": repository_id,
            "repository_name": repository_name,
            "repository_full_name": repository_full_name,
            "parent_id": parent_id,
            "directory_value": directory_value,
            "child_ids": [],
            "branch_map": {},
            "branch_names": [],
            "branch_preview": [],
            "branch_overflow": 0,
            "source_labels": set(),
            "source_label": "不明",
            "status": "unknown",
            "status_label": _directory_activity_status_label("unknown"),
            "status_class": _directory_activity_status_class("unknown"),
            "last_active_at": None,
            "last_active_label": "不明",
            "is_conflict": False,
            "conflict_count": 0,
            "recent_count": 0,
            "file_count": 0,
            "search_text": full_path.lower(),
            "copy_text": full_path,
            "conflict_href": None,
        }
        nodes[node_id] = node
        if parent_id is None:
            root_ids.append(node_id)
        else:
            parent = nodes[parent_id]
            if node_id not in parent["child_ids"]:
                parent["child_ids"].append(node_id)
        return node

    def merge_branch_maps(target: dict[str, dict[str, object]], source: dict[str, dict[str, object]]) -> None:
        for branch_id, branch_item in source.items():
            existing = target.get(branch_id)
            if existing is None or (
                branch_item.get("last_updated_at") is not None
                and (existing.get("last_updated_at") is None or branch_item["last_updated_at"] > existing["last_updated_at"])
            ):
                target[branch_id] = branch_item

    for file_node in file_nodes:
        repository_id = str(file_node["repository_id"])
        repository_name = str(file_node["repository_name"])
        repository_full_name = str(file_node["repository_full_name"])
        root_directory_value = f"{repository_id}::__root__"
        root_id = f"directory:{repository_id}:__root__"
        ensure_directory_node(
            node_id=root_id,
            name=repository_name,
            path=repository_name,
            full_path=repository_full_name,
            repository_id=repository_id,
            repository_name=repository_name,
            repository_full_name=repository_full_name,
            parent_id=None,
            directory_value=root_directory_value,
        )
        current_parent_id = root_id
        path_segments = [segment for segment in str(file_node["path"]).split("/") if segment]
        directory_segments = path_segments[:-1]
        current_prefix: list[str] = []
        for segment in directory_segments:
            current_prefix.append(segment)
            directory_path = "/".join(current_prefix)
            directory_id = f"directory:{repository_id}:{directory_path}"
            ensure_directory_node(
                node_id=directory_id,
                name=segment,
                path=directory_path,
                full_path=f"{repository_full_name} / {directory_path}",
                repository_id=repository_id,
                repository_name=repository_name,
                repository_full_name=repository_full_name,
                parent_id=current_parent_id,
                directory_value=f"{repository_id}::{current_prefix[0]}",
            )
            current_parent_id = directory_id

        file_node["parent_id"] = current_parent_id
        nodes[file_node["id"]] = file_node
        parent = nodes[current_parent_id]
        if file_node["id"] not in parent["child_ids"]:
            parent["child_ids"].append(file_node["id"])

    def finalize_node(node_id: str) -> dict[str, object]:
        node = nodes[node_id]
        if node["type"] == "file":
            branch_map = {item["id"]: item for item in node["branch_items"]}
            node["branch_map"] = branch_map
            return node

        conflict_href = None
        for child_id in list(node["child_ids"]):
            child = finalize_node(child_id)
            node["file_count"] += int(child["file_count"])
            node["conflict_count"] += int(child["conflict_count"])
            node["recent_count"] += int(child["recent_count"])
            node["is_conflict"] = bool(node["is_conflict"]) or bool(child["is_conflict"])
            node["source_labels"].update(child["source_labels"])
            merge_branch_maps(node["branch_map"], child["branch_map"])
            if child["last_active_at"] is not None and (
                node["last_active_at"] is None or child["last_active_at"] > node["last_active_at"]
            ):
                node["last_active_at"] = child["last_active_at"]
            if child["conflict_href"] and conflict_href is None:
                conflict_href = child["conflict_href"]

        if node["file_count"]:
            child_statuses = [nodes[child_id]["status"] for child_id in node["child_ids"]]
            node["status"] = min(
                child_statuses,
                key=lambda status: _DIRECTORY_ACTIVITY_STATUS_PRIORITY.get(status, 99),
            )
            node["status_label"] = _directory_activity_status_label(node["status"])
            node["status_class"] = _directory_activity_status_class(node["status"])
            node["last_active_label"] = _directory_activity_relative_time(node["last_active_at"], now)
        node["source_label"] = _directory_activity_source_label(node["source_labels"])
        node["conflict_href"] = conflict_href
        branch_items = sorted(node["branch_map"].values(), key=_branch_sort_key)
        node["branch_names"] = [str(item["name"]) for item in branch_items]
        node["branch_preview"] = branch_items[:2]
        node["branch_overflow"] = max(len(branch_items) - 2, 0)
        node["search_text"] = " ".join(
            [
                str(node["search_text"]),
                " ".join(name.lower() for name in node["branch_names"]),
            ]
        ).strip()
        node["child_ids"].sort(
            key=lambda child_id: (
                0 if nodes[child_id]["type"] == "directory" else 1,
                _DIRECTORY_ACTIVITY_STATUS_PRIORITY.get(str(nodes[child_id]["status"]), 99),
                -(nodes[child_id]["last_active_at"].timestamp()) if nodes[child_id]["last_active_at"] else 0,
                str(nodes[child_id]["name"]).lower(),
            )
        )
        return node

    for root_id in root_ids:
        finalize_node(root_id)

    root_ids.sort(
        key=lambda node_id: (
            _DIRECTORY_ACTIVITY_STATUS_PRIORITY.get(str(nodes[node_id]["status"]), 99),
            -(nodes[node_id]["last_active_at"].timestamp()) if nodes[node_id]["last_active_at"] else 0,
            str(nodes[node_id]["name"]).lower(),
        )
    )

    serialized_nodes: list[dict[str, object]] = []
    summary_counts = {"conflict": 0, "recent_7": 0, "recent_28": 0, "stale": 0, "manual": 0}
    for node in nodes.values():
        if node["type"] == "file":
            summary_counts[node["status"]] = summary_counts.get(node["status"], 0) + 1
            if node["is_manual"]:
                summary_counts["manual"] += 1
        serialized_nodes.append(
            {
                "id": node["id"],
                "type": node["type"],
                "name": node["name"],
                "path": node["path"],
                "full_path": node["full_path"],
                "parent_id": node.get("parent_id"),
                "child_ids": node["child_ids"],
                "status": node["status"],
                "status_label": node["status_label"],
                "status_class": node["status_class"],
                "last_active_label": node["last_active_label"],
                "last_active_at": node["last_active_at"].isoformat() if node["last_active_at"] else "",
                "source_label": node["source_label"],
                "branch_names": node["branch_names"],
                "branch_preview": [
                    {
                        "id": item["id"],
                        "name": item["name"],
                        "href": item["href"],
                    }
                    for item in node["branch_preview"]
                ],
                "branch_overflow": node["branch_overflow"],
                "branches": [
                    {
                        "id": item["id"],
                        "name": item["name"],
                        "href": item["href"],
                        "last_active_label": _directory_activity_relative_time(item["last_updated_at"], now),
                    }
                    for item in sorted(node["branch_map"].values(), key=_branch_sort_key)[:8]
                ],
                "repository_id": node["repository_id"],
                "repository_name": node["repository_name"],
                "repository_full_name": node["repository_full_name"],
                "directory_value": node["directory_value"],
                "search_text": node["search_text"],
                "is_conflict": bool(node["is_conflict"]),
                "conflict_count": node["conflict_count"],
                "recent_count": node["recent_count"],
                "file_count": node["file_count"],
                "copy_text": node["copy_text"],
                "conflict_href": node["conflict_href"],
            }
        )

    prioritized_files = [
        node
        for node in serialized_nodes
        if node["type"] == "file"
    ]
    prioritized_files.sort(
        key=lambda node: (
            _DIRECTORY_ACTIVITY_STATUS_PRIORITY.get(str(node["status"]), 99),
            node["last_active_at"] != "",
            node["last_active_at"],
            node["full_path"],
        ),
        reverse=False,
    )
    initial_selected_id = prioritized_files[0]["id"] if prioritized_files else (root_ids[0] if root_ids else None)

    payload = {
        "filters": {
            "branches": branch_options,
            "statuses": [
                {"value": "conflict", "label": "競合"},
                {"value": "recent_7", "label": "7日以内に更新"},
                {"value": "recent_28", "label": "28日以内に更新"},
                {"value": "stale", "label": "長期間更新なし"},
                {"value": "unknown", "label": "不明"},
            ],
            "directories": [
                {"value": value, "label": label}
                for value, label in sorted(directory_options_map.items(), key=lambda item: item[1].lower())
            ],
        },
        "root_ids": root_ids,
        "nodes": serialized_nodes,
        "default_expanded": root_ids,
        "initial_selected_id": initial_selected_id,
    }

    return {
        "is_empty": False,
        "summary_cards": [
            {
                "label": "競合",
                "value": str(summary_counts.get("conflict", 0)),
                "meta": "競合中のパス",
                "class": "is-conflict",
            },
            {
                "label": "7日以内更新",
                "value": str(summary_counts.get("recent_7", 0)),
                "meta": "最近更新されたパス",
                "class": "",
            },
            {
                "label": "28日以内更新",
                "value": str(summary_counts.get("recent_28", 0)),
                "meta": "観測済みのパス",
                "class": "",
            },
            {
                "label": "長期間更新なし",
                "value": str(summary_counts.get("stale", 0)),
                "meta": "28日以上更新なし",
                "class": "",
            },
            {
                "label": "手動追跡",
                "value": str(summary_counts.get("manual", 0)),
                "meta": "手動登録ソース",
                "class": "",
            },
        ],
        "empty_title": "まだ観測済みファイルはありません",
        "empty_body": "Push/Webhook または手動登録で検知されたファイルが表示されます",
        "empty_action": _dashboard_action("ブランチへ移動", branches_href),
        "legend_items": [
            {"label": "赤 = 競合", "class": "is-conflict"},
            {"label": "緑 = 7日以内に更新", "class": "is-recent-7"},
            {"label": "黄 = 28日以内に更新", "class": "is-recent-28"},
            {"label": "灰 = 長期間更新なし", "class": "is-stale"},
        ],
        "note": "この画面は観測済みファイルのみ表示対象です。push/webhook または手動登録で検知されたもののみ対象です。",
        "json": json.dumps(payload, ensure_ascii=False).replace("</", "<\\/"),
    }


_FILE_TREE_EXCLUDED_SEGMENTS = {
    ".git",
    "node_modules",
    "vendor",
    "__pycache__",
    ".venv",
    "dist",
    "build",
    "coverage",
}

_FILE_TREE_STATUS_PRIORITY = {
    "conflict": 0,
    "overlap": 1,
    "recent": 2,
    "watch": 3,
    "manual": 4,
    "stale": 5,
    "unknown": 6,
}


def _file_tree_is_excluded_path(path: str | None) -> bool:
    normalized = str(path or "").strip().strip("/")
    if not normalized:
        return False
    parts = [segment.lower() for segment in normalized.split("/") if segment]
    return any(segment in _FILE_TREE_EXCLUDED_SEGMENTS for segment in parts)


def _file_tree_source_label(source_labels: set[str]) -> str:
    if not source_labels:
        return "不明"
    if source_labels == {"手動登録"}:
        return "手動追跡"
    if source_labels == {"Webhook"}:
        return "Webhookで検出"
    return "Webhookと手動登録"


def _file_tree_change_label(change_type: str | None) -> str:
    normalized = str(change_type or "").strip().lower()
    if not normalized:
        return "変更あり"
    if normalized in {"a", "added"}:
        return "追加"
    if normalized in {"m", "modified", "change", "changed"}:
        return "変更"
    if normalized in {"d", "deleted", "removed", "remove"}:
        return "削除"
    if normalized.startswith("r") or normalized in {"renamed", "rename"}:
        return "リネーム"
    return "変更あり"


def _file_tree_icon_key(path: str) -> str:
    filename = path.rsplit("/", 1)[-1]
    lowered = filename.lower()
    if lowered == "dockerfile":
        return "docker"
    if lowered == "package.json":
        return "json"
    if lowered == "requirements.txt":
        return "text"
    if lowered == "docker-compose.yml":
        return "settings"
    if "." not in lowered:
        return "file"
    extension = lowered.rsplit(".", 1)[-1]
    if extension in {"py", "js", "ts", "tsx", "jsx"}:
        return "code"
    if extension == "json":
        return "json"
    if extension == "html":
        return "html"
    if extension in {"css", "scss"}:
        return "css"
    if extension in {"yml", "yaml", "toml"}:
        return "settings"
    if extension == "env":
        return "key"
    if extension in {"md", "txt"}:
        return "text"
    if extension == "sql":
        return "storage"
    if extension in {"png", "jpg", "jpeg", "webp", "svg"}:
        return "image"
    if extension == "lock":
        return "lock"
    return "file"


def _file_tree_status(
    *,
    last_updated_at,
    is_conflict: bool,
    is_overlap: bool,
    is_manual_only: bool,
    now,
) -> str:
    if is_conflict:
        return "conflict"
    if is_overlap:
        return "overlap"
    if last_updated_at is None:
        return "unknown"
    age = now - last_updated_at
    if age <= timedelta(days=7):
        return "recent"
    if is_manual_only:
        return "manual"
    if age <= timedelta(days=28):
        return "watch"
    return "stale"


def _file_tree_status_label(status: str) -> str:
    return {
        "conflict": "競合中",
        "overlap": "重複編集",
        "recent": "最近更新",
        "watch": "変更あり",
        "manual": "手動追跡",
        "stale": "長期間更新なし",
        "unknown": "不明",
    }.get(status, "不明")


def _file_tree_status_class(status: str) -> str:
    return f"is-{status.replace('_', '-')}"


def _file_tree_branch_card_status(
    *,
    file_status: str,
    is_manual: bool,
) -> tuple[str, str]:
    if file_status == "conflict":
        return "conflict", "競合中"
    if file_status == "overlap":
        return "overlap", "重複編集"
    if is_manual:
        return "manual", "手動追跡"
    if file_status in {"recent", "watch"}:
        return "changed", "変更あり"
    return "safe", "問題なし"


def _file_tree_status_banner(*, status: str, branch_count: int) -> dict[str, str]:
    if status == "conflict":
        return {
            "tone": "is-conflict",
            "title": "競合の可能性があります",
            "body": f"{branch_count}件のブランチがこのファイルを変更しています。",
        }
    if status == "overlap":
        return {
            "tone": "is-overlap",
            "title": "重複編集があります",
            "body": "複数のブランチがこのファイルを変更しています。",
        }
    return {
        "tone": "is-safe",
        "title": "現在、このファイルに競合は検出されていません",
        "body": "関連ブランチと最新の変更状況を確認できます。",
    }


def workspace_file_tree_data(db: Session, workspace: Workspace) -> dict[str, object]:
    now = utcnow()
    github_settings_href = f"/settings/integrations/github?workspace={workspace.slug}"
    repositories_href = f"/workspaces/{workspace.slug}/repositories"
    branches_href = f"/workspaces/{workspace.slug}/branches"
    conflicts_href = f"/workspaces/{workspace.slug}/conflicts"

    installations = db.scalars(
        select(GithubInstallation).where(GithubInstallation.claimed_workspace_id == workspace.id)
    ).all()
    repositories = db.scalars(
        select(Repository).where(
            Repository.workspace_id == workspace.id,
            Repository.deleted_at.is_(None),
        )
    ).all()
    repositories.sort(
        key=lambda repository: (
            0 if repository.selection_status == REPOSITORY_SELECTION_ACTIVE else 1,
            0 if repository.is_available else 1,
            repository.display_name.lower(),
        )
    )
    active_repositories = [
        repository
        for repository in repositories
        if repository.selection_status == REPOSITORY_SELECTION_ACTIVE
        and repository.is_active
        and repository.is_available
    ]
    active_repository_ids = [repository.id for repository in active_repositories]
    branches = db.scalars(
        select(Branch).where(
            Branch.workspace_id == workspace.id,
            Branch.repository_id.in_(active_repository_ids) if active_repository_ids else False,
            Branch.is_active.is_(True),
            Branch.is_deleted.is_(False),
        )
    ).all()
    open_collisions = db.scalars(
        select(FileCollision)
        .join(Repository, Repository.id == FileCollision.repository_id)
        .where(
            Repository.workspace_id == workspace.id,
            Repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
            Repository.is_active.is_(True),
            Repository.is_available.is_(True),
            FileCollision.collision_status == "open",
        )
    ).all()
    open_collision_keys = {(collision.repository_id, collision.normalized_path) for collision in open_collisions}
    branch_file_rows = db.execute(
        select(BranchFile, Branch, Repository)
        .join(Branch, Branch.id == BranchFile.branch_id)
        .join(Repository, Repository.id == Branch.repository_id)
        .where(
            Repository.workspace_id == workspace.id,
            Repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
            Repository.is_active.is_(True),
            Repository.is_available.is_(True),
            Branch.is_active.is_(True),
            Branch.is_deleted.is_(False),
            BranchFile.is_active.is_(True),
        )
    ).all()

    file_entries: dict[tuple[str, str], dict[str, object]] = {}
    for branch_file, branch, repository in branch_file_rows:
        normalized_path = branch_file.normalized_path or branch_file.path
        if not normalized_path or _file_tree_is_excluded_path(normalized_path):
            continue
        key = (repository.id, normalized_path)
        source_label = "手動登録" if branch.observed_via == "manual" or branch_file.source_kind == "manual_input" else "Webhook"
        observed_at = branch_file.last_seen_at or branch_file.observed_at or branch.last_push_at or branch.updated_at
        branch_detail_href = f"/workspaces/{workspace.slug}/branches/{branch.id}"
        branch_diff_href = f"{branches_href}?branch={quote(branch.name, safe='')}"
        entry = file_entries.get(key)
        if entry is None:
            entry = {
                "repository_id": repository.id,
                "repository_name": repository.display_name,
                "repository_full_name": repository.full_name,
                "path": branch_file.path,
                "normalized_path": normalized_path,
                "last_updated_at": observed_at,
                "source_labels": {source_label},
                "branch_map": {},
                "is_conflict": key in open_collision_keys or bool(branch_file.is_conflict),
            }
            file_entries[key] = entry
        else:
            entry["source_labels"].add(source_label)
            entry["is_conflict"] = bool(entry["is_conflict"]) or key in open_collision_keys or bool(branch_file.is_conflict)
            current_last_updated = entry.get("last_updated_at")
            if observed_at and (current_last_updated is None or observed_at > current_last_updated):
                entry["last_updated_at"] = observed_at
                entry["path"] = branch_file.path

        entry["branch_map"][branch.id] = {
            "id": branch.id,
            "name": branch.name,
            "detail_href": branch_detail_href,
            "diff_href": branch_diff_href,
            "repository_name": repository.display_name,
            "last_updated_at": observed_at,
            "source_label": "手動追跡" if source_label == "手動登録" else "Webhookで検出",
            "change_label": _file_tree_change_label(
                branch_file.last_change_type or branch_file.change_type or branch_file.first_seen_change_type
            ),
            "is_manual": source_label == "手動登録",
        }

    empty_state = {
        "title": "まだ観測済みファイルはありません",
        "body": "Push/Webhook または手動登録で検知されたファイルが表示されます。",
        "action": {"label": "ブランチへ移動", "href": branches_href},
    }
    if not installations:
        empty_state = {
            "title": "GitHub App を連携するとファイルツリーが表示されます",
            "body": "",
            "action": {"label": "GitHub を開く", "href": github_settings_href},
        }
    elif not active_repositories:
        empty_state = {
            "title": "監視対象の repository がまだ選ばれていません",
            "body": "リポジトリ画面で監視対象を選ぶと、観測済みファイルをここでたどれます。",
            "action": {"label": "リポジトリへ移動", "href": repositories_href},
        }
    elif not branches:
        empty_state = {
            "title": "まだ観測済みのブランチはありません",
            "body": "Push/Webhook または手動登録でブランチを取り込むと、ファイルツリーが表示されます。",
            "action": {"label": "ブランチへ移動", "href": branches_href},
        }

    if not file_entries:
        payload = {
            "root_ids": [],
            "nodes": [],
            "default_expanded": [],
            "initial_selected_id": None,
            "branch_filter_options": [
                {"value": "all", "label": "すべてのアクティブブランチ"},
                {"value": "conflict", "label": "競合中のみ"},
                {"value": "overlap", "label": "重複編集のみ"},
                {"value": "changed", "label": "最近更新のみ"},
                {"value": "manual", "label": "手動追跡のみ"},
            ],
        }
        return {
            "is_empty": True,
            "empty_state": empty_state,
            "note": "表示対象は Hotdock が観測済みのファイルのみです。GitHub リポジトリ全体の全ファイルは表示しません。",
            "json": json.dumps(payload, ensure_ascii=False).replace("</", "<\\/"),
        }

    def _branch_sort_key(item: dict[str, object]):
        last_updated_at = item.get("last_updated_at")
        return (
            0 if last_updated_at else 1,
            -(last_updated_at.timestamp()) if last_updated_at else 0,
            str(item["name"]).lower(),
        )

    file_nodes: list[dict[str, object]] = []
    for entry in file_entries.values():
        normalized_path = str(entry["normalized_path"])
        branch_items = sorted(entry["branch_map"].values(), key=_branch_sort_key)
        branch_count = len(branch_items)
        status = _file_tree_status(
            last_updated_at=entry["last_updated_at"],
            is_conflict=bool(entry["is_conflict"]),
            is_overlap=branch_count > 1 and not bool(entry["is_conflict"]),
            is_manual_only=entry["source_labels"] == {"手動登録"},
            now=now,
        )
        branch_cards = []
        for item in branch_items:
            status_key, status_label = _file_tree_branch_card_status(
                file_status=status,
                is_manual=bool(item["is_manual"]),
            )
            branch_cards.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "detail_href": item["detail_href"],
                    "diff_href": item["diff_href"],
                    "repository_name": item["repository_name"],
                    "last_updated_label": _directory_activity_relative_time(item["last_updated_at"], now),
                    "change_label": item["change_label"],
                    "source_label": item["source_label"],
                    "status_key": status_key,
                    "status_label": status_label,
                    "status_class": _file_tree_status_class(status_key),
                }
            )
        path_parts = [segment for segment in normalized_path.split("/") if segment]
        full_path = f"{entry['repository_full_name']} / {normalized_path}"
        file_nodes.append(
            {
                "id": f"file:{entry['repository_id']}:{normalized_path}",
                "kind": "file",
                "name": path_parts[-1] if path_parts else normalized_path,
                "path": normalized_path,
                "full_path": full_path,
                "repository_id": entry["repository_id"],
                "repository_name": entry["repository_name"],
                "repository_full_name": entry["repository_full_name"],
                "icon_key": _file_tree_icon_key(normalized_path),
                "child_ids": [],
                "status": status,
                "status_label": _file_tree_status_label(status),
                "status_class": _file_tree_status_class(status),
                "last_active_at": entry["last_updated_at"],
                "last_active_label": _directory_activity_relative_time(entry["last_updated_at"], now),
                "source_labels": set(entry["source_labels"]),
                "source_label": _file_tree_source_label(entry["source_labels"]),
                "branch_map": dict(entry["branch_map"]),
                "branch_names": [str(item["name"]) for item in branch_items],
                "branch_cards": branch_cards,
                "branch_count": branch_count,
                "branch_preview": branch_cards[:2],
                "branch_overflow": max(branch_count - 2, 0),
                "file_count": 1,
                "conflict_count": 1 if status == "conflict" else 0,
                "overlap_count": 1 if status == "overlap" else 0,
                "recent_count": 1 if status in {"recent", "watch"} else 0,
                "manual_count": 1 if "手動登録" in entry["source_labels"] else 0,
                "search_text": " ".join(
                    [
                        full_path.lower(),
                        normalized_path.lower(),
                        " ".join(str(item["name"]).lower() for item in branch_items),
                    ]
                ),
                "problem_files": [],
                "status_banner": _file_tree_status_banner(status=status, branch_count=branch_count),
                "conflict_href": (
                    f"{conflicts_href}?path={quote(normalized_path)}&repository_id={quote(str(entry['repository_id']))}"
                    if status == "conflict"
                    else None
                ),
            }
        )

    nodes: dict[str, dict[str, object]] = {}
    root_ids: list[str] = []

    def ensure_node(
        *,
        node_id: str,
        kind: str,
        name: str,
        path: str,
        full_path: str,
        repository_id: str,
        repository_name: str,
        repository_full_name: str,
        parent_id: str | None,
    ) -> dict[str, object]:
        existing = nodes.get(node_id)
        if existing is not None:
            return existing
        node = {
            "id": node_id,
            "kind": kind,
            "name": name,
            "path": path,
            "full_path": full_path,
            "repository_id": repository_id,
            "repository_name": repository_name,
            "repository_full_name": repository_full_name,
            "parent_id": parent_id,
            "child_ids": [],
            "branch_map": {},
            "branch_names": [],
            "branch_count": 0,
            "branch_preview": [],
            "branch_overflow": 0,
            "branch_cards": [],
            "source_labels": set(),
            "source_label": "不明",
            "status": "unknown",
            "status_label": _file_tree_status_label("unknown"),
            "status_class": _file_tree_status_class("unknown"),
            "last_active_at": None,
            "last_active_label": "不明",
            "conflict_count": 0,
            "overlap_count": 0,
            "recent_count": 0,
            "manual_count": 0,
            "file_count": 0,
            "search_text": full_path.lower(),
            "problem_files": [],
            "conflict_href": None,
            "icon_key": "folder",
        }
        nodes[node_id] = node
        if parent_id is None:
            root_ids.append(node_id)
        else:
            parent = nodes[parent_id]
            if node_id not in parent["child_ids"]:
                parent["child_ids"].append(node_id)
        return node

    def merge_branch_maps(target: dict[str, dict[str, object]], source: dict[str, dict[str, object]]) -> None:
        for branch_id, branch_item in source.items():
            existing = target.get(branch_id)
            if existing is None or (
                branch_item.get("last_updated_at") is not None
                and (existing.get("last_updated_at") is None or branch_item["last_updated_at"] > existing["last_updated_at"])
            ):
                target[branch_id] = branch_item

    for file_node in file_nodes:
        repository_id = str(file_node["repository_id"])
        repository_name = str(file_node["repository_name"])
        repository_full_name = str(file_node["repository_full_name"])
        root_id = f"repository:{repository_id}"
        ensure_node(
            node_id=root_id,
            kind="repository",
            name=repository_name,
            path=repository_name,
            full_path=repository_full_name,
            repository_id=repository_id,
            repository_name=repository_name,
            repository_full_name=repository_full_name,
            parent_id=None,
        )

        current_parent_id = root_id
        path_segments = [segment for segment in str(file_node["path"]).split("/") if segment]
        directory_segments = path_segments[:-1]
        current_prefix: list[str] = []
        for segment in directory_segments:
            current_prefix.append(segment)
            directory_path = "/".join(current_prefix)
            directory_id = f"directory:{repository_id}:{directory_path}"
            ensure_node(
                node_id=directory_id,
                kind="directory",
                name=segment,
                path=directory_path,
                full_path=f"{repository_full_name} / {directory_path}",
                repository_id=repository_id,
                repository_name=repository_name,
                repository_full_name=repository_full_name,
                parent_id=current_parent_id,
            )
            current_parent_id = directory_id

        file_node["parent_id"] = current_parent_id
        nodes[file_node["id"]] = file_node
        parent = nodes[current_parent_id]
        if file_node["id"] not in parent["child_ids"]:
            parent["child_ids"].append(file_node["id"])

    def finalize_node(node_id: str) -> dict[str, object]:
        node = nodes[node_id]
        if node["kind"] == "file":
            return node

        problem_files: list[dict[str, object]] = []
        conflict_href = None
        for child_id in list(node["child_ids"]):
            child = finalize_node(child_id)
            node["file_count"] += int(child["file_count"])
            node["conflict_count"] += int(child["conflict_count"])
            node["overlap_count"] += int(child["overlap_count"])
            node["recent_count"] += int(child["recent_count"])
            node["manual_count"] += int(child["manual_count"])
            node["source_labels"].update(child["source_labels"])
            merge_branch_maps(node["branch_map"], child["branch_map"])
            if child["last_active_at"] is not None and (
                node["last_active_at"] is None or child["last_active_at"] > node["last_active_at"]
            ):
                node["last_active_at"] = child["last_active_at"]
            if child.get("conflict_href") and conflict_href is None:
                conflict_href = child["conflict_href"]
            if child["kind"] == "file" and child["status"] in {"conflict", "overlap", "recent", "watch"}:
                problem_files.append(
                    {
                        "id": child["id"],
                        "path": child["path"],
                        "name": child["name"],
                        "status": child["status"],
                        "status_label": child["status_label"],
                        "branch_count": child["branch_count"],
                    }
                )
            problem_files.extend(child.get("problem_files", []))

        if node["file_count"]:
            child_statuses = [nodes[child_id]["status"] for child_id in node["child_ids"]]
            node["status"] = min(
                child_statuses,
                key=lambda status: _FILE_TREE_STATUS_PRIORITY.get(status, 99),
            )
            node["status_label"] = _file_tree_status_label(node["status"])
            node["status_class"] = _file_tree_status_class(node["status"])
            node["last_active_label"] = _directory_activity_relative_time(node["last_active_at"], now)
        node["source_label"] = _file_tree_source_label(node["source_labels"])
        node["conflict_href"] = conflict_href
        branch_items = sorted(node["branch_map"].values(), key=_branch_sort_key)
        node["branch_names"] = [str(item["name"]) for item in branch_items]
        node["branch_count"] = len(branch_items)
        node["branch_preview"] = branch_items[:2]
        node["branch_overflow"] = max(len(branch_items) - 2, 0)
        node["branch_cards"] = [
            {
                "id": item["id"],
                "name": item["name"],
                "detail_href": item["detail_href"],
                "diff_href": item["diff_href"],
                "repository_name": item["repository_name"],
                "last_updated_label": _directory_activity_relative_time(item["last_updated_at"], now),
                "change_label": item["change_label"],
                "source_label": item["source_label"],
                "status_key": (
                    "conflict" if node["status"] == "conflict" else
                    "overlap" if node["status"] == "overlap" else
                    "manual" if item["is_manual"] else
                    "changed" if node["status"] in {"recent", "watch"} else
                    "safe"
                ),
                "status_label": (
                    "競合中" if node["status"] == "conflict" else
                    "重複編集" if node["status"] == "overlap" else
                    "手動追跡" if item["is_manual"] else
                    "変更あり" if node["status"] in {"recent", "watch"} else
                    "問題なし"
                ),
                "status_class": _file_tree_status_class(
                    "conflict" if node["status"] == "conflict" else
                    "overlap" if node["status"] == "overlap" else
                    "manual" if item["is_manual"] else
                    "changed" if node["status"] in {"recent", "watch"} else
                    "safe"
                ),
            }
            for item in branch_items
        ]
        node["search_text"] = " ".join(
            [
                str(node["search_text"]),
                " ".join(name.lower() for name in node["branch_names"]),
            ]
        ).strip()
        node["child_ids"].sort(
            key=lambda child_id: (
                0 if nodes[child_id]["kind"] in {"repository", "directory"} else 1,
                _FILE_TREE_STATUS_PRIORITY.get(str(nodes[child_id]["status"]), 99),
                str(nodes[child_id]["name"]).lower(),
            )
        )
        problem_files.sort(
            key=lambda item: (
                _FILE_TREE_STATUS_PRIORITY.get(str(item["status"]), 99),
                -int(item["branch_count"]),
                str(item["path"]).lower(),
            )
        )
        deduped_problem_files: list[dict[str, object]] = []
        seen_problem_ids: set[str] = set()
        for item in problem_files:
            if item["id"] in seen_problem_ids:
                continue
            seen_problem_ids.add(item["id"])
            deduped_problem_files.append(item)
        node["problem_files"] = deduped_problem_files[:8]
        return node

    for root_id in root_ids:
        finalize_node(root_id)

    root_ids.sort(
        key=lambda node_id: (
            _FILE_TREE_STATUS_PRIORITY.get(str(nodes[node_id]["status"]), 99),
            str(nodes[node_id]["name"]).lower(),
        )
    )

    serialized_nodes: list[dict[str, object]] = []
    for node in nodes.values():
        serialized_nodes.append(
            {
                "id": node["id"],
                "kind": node["kind"],
                "name": node["name"],
                "path": node["path"],
                "full_path": node["full_path"],
                "parent_id": node.get("parent_id"),
                "child_ids": node["child_ids"],
                "icon_key": node.get("icon_key", "folder"),
                "status": node["status"],
                "status_label": node["status_label"],
                "status_class": node["status_class"],
                "last_active_label": node["last_active_label"],
                "last_active_at": node["last_active_at"].isoformat() if node["last_active_at"] else "",
                "source_label": node["source_label"],
                "branch_names": node["branch_names"],
                "branch_count": node["branch_count"],
                "branch_preview": [
                    {
                        "id": item["id"],
                        "name": item["name"],
                        "detail_href": item.get("detail_href"),
                        "diff_href": item.get("diff_href"),
                    }
                    for item in node["branch_preview"]
                ],
                "branch_overflow": node["branch_overflow"],
                "branch_cards": node["branch_cards"],
                "repository_id": node["repository_id"],
                "repository_name": node["repository_name"],
                "repository_full_name": node["repository_full_name"],
                "search_text": node["search_text"],
                "conflict_count": node["conflict_count"],
                "overlap_count": node["overlap_count"],
                "recent_count": node["recent_count"],
                "manual_count": node["manual_count"],
                "file_count": node["file_count"],
                "problem_files": node["problem_files"],
                "conflict_href": node["conflict_href"],
                "status_banner": node.get("status_banner"),
            }
        )

    default_expanded = set(root_ids)

    payload = {
        "root_ids": root_ids,
        "nodes": serialized_nodes,
        "default_expanded": sorted(default_expanded),
        "initial_selected_id": None,
        "branch_filter_options": [
            {"value": "all", "label": "すべてのアクティブブランチ"},
            {"value": "conflict", "label": "競合中のみ"},
            {"value": "overlap", "label": "重複編集のみ"},
            {"value": "changed", "label": "最近更新のみ"},
            {"value": "manual", "label": "手動追跡のみ"},
        ],
    }
    return {
        "is_empty": False,
        "empty_state": empty_state,
        "note": "表示対象は Hotdock が観測済みのファイルのみです。push / webhook または手動登録で検知されたものを表示します。",
        "json": json.dumps(payload, ensure_ascii=False).replace("</", "<\\/"),
    }


def workspace_dashboard_data(db: Session, workspace: Workspace) -> dict[str, object]:
    now = utcnow()
    installations = db.scalars(
        select(GithubInstallation).where(GithubInstallation.claimed_workspace_id == workspace.id)
    ).all()
    repositories = db.scalars(
        select(Repository).where(Repository.workspace_id == workspace.id, Repository.deleted_at.is_(None))
    ).all()
    repositories.sort(
        key=lambda repository: (
            0 if repository.selection_status == REPOSITORY_SELECTION_ACTIVE else 1,
            0 if repository.is_available else 1,
            repository.display_name.lower(),
        )
    )
    active_repositories = [repository for repository in repositories if repository.selection_status == REPOSITORY_SELECTION_ACTIVE]
    active_repository_ids = [repository.id for repository in active_repositories]
    branches = db.scalars(
        select(Branch).where(
            Branch.workspace_id == workspace.id,
            Branch.repository_id.in_(active_repository_ids) if active_repository_ids else False,
        )
    ).all()
    tracked_branches = [branch for branch in branches if branch.is_active and not branch.is_deleted]
    branch_ids = [branch.id for branch in branches]
    conflicts = db.scalars(
        select(FileCollision)
        .join(Repository, Repository.id == FileCollision.repository_id)
        .where(
            Repository.workspace_id == workspace.id,
            Repository.selection_status == REPOSITORY_SELECTION_ACTIVE,
            Repository.is_active.is_(True),
            Repository.is_available.is_(True),
            FileCollision.collision_status == "open",
        )
    ).all()
    github_settings_href = f"/settings/integrations/github?workspace={workspace.slug}"
    conflicts_href = f"/workspaces/{workspace.slug}/conflicts"

    branch_file_counts = (
        {
            branch_id: count
            for branch_id, count in db.execute(
                select(BranchFile.branch_id, func.count(BranchFile.id))
                .where(
                    BranchFile.branch_id.in_(branch_ids) if branch_ids else False,
                    BranchFile.is_active.is_(True),
                )
                .group_by(BranchFile.branch_id)
            ).all()
        }
        if branch_ids
        else {}
    )
    branch_conflict_counts = (
        {
            branch_id: count
            for branch_id, count in db.execute(
                select(BranchFile.branch_id, func.count(BranchFile.id))
                .where(
                    BranchFile.branch_id.in_(branch_ids) if branch_ids else False,
                    BranchFile.is_active.is_(True),
                    BranchFile.is_conflict.is_(True),
                )
                .group_by(BranchFile.branch_id)
            ).all()
        }
        if branch_ids
        else {}
    )

    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    monthly_push_count = db.scalar(
        select(func.count(BranchEvent.id)).where(
            BranchEvent.repository_id.in_(active_repository_ids) if active_repository_ids else False,
            BranchEvent.event_type == "push",
            BranchEvent.occurred_at >= month_start,
        )
    ) or 0

    recent_push_events = db.scalars(
        select(BranchEvent)
        .where(
            BranchEvent.repository_id.in_(active_repository_ids) if active_repository_ids else False,
            BranchEvent.event_type == "push",
        )
        .order_by(BranchEvent.occurred_at.desc())
        .limit(5)
    ).all()
    push_delivery_ids = [event.webhook_delivery_id for event in recent_push_events if event.webhook_delivery_id]
    webhook_events = db.scalars(
        select(GithubWebhookEvent).where(
            GithubWebhookEvent.delivery_id.in_(push_delivery_ids) if push_delivery_ids else False,
        )
    ).all()
    webhook_by_delivery = {event.delivery_id: event for event in webhook_events}
    latest_push_received_at = max(
        (
            (webhook_by_delivery.get(event.webhook_delivery_id).received_at if webhook_by_delivery.get(event.webhook_delivery_id) else event.occurred_at)
            for event in recent_push_events
        ),
        default=None,
    )

    all_branch_map = {branch.id: branch for branch in branches}
    repository_map = {repository.id: repository for repository in active_repositories}
    push_events: list[dict[str, str]] = []
    for event in recent_push_events:
        webhook_event = webhook_by_delivery.get(event.webhook_delivery_id)
        payload = webhook_event.payload if webhook_event else {}
        branch = all_branch_map.get(event.branch_id) if event.branch_id else None
        repository = repository_map.get(event.repository_id)
        branch_name = branch.name if branch else (_dashboard_payload_branch_name(payload) or "不明")
        changed_files = _dashboard_payload_changed_file_count(payload) or branch_file_counts.get(
            event.branch_id or "",
            branch.touched_files_count if branch else 0,
        )
        conflict_count = branch_conflict_counts.get(event.branch_id or "", branch.conflict_files_count if branch else 0)
        event_time = webhook_event.received_at if webhook_event else event.occurred_at
        push_events.append(
            {
                "branch_name": branch_name,
                "repository_name": repository.display_name if repository else "未設定",
                "changed_files_label": f"変更ファイル {changed_files}",
                "time_label": _dashboard_recent_or_timestamp(event_time, now),
                "conflict_label": "競合あり" if conflict_count > 0 else "競合なし",
                "conflict_class": "is-conflict" if conflict_count > 0 else "is-safe",
                "actor_label": _dashboard_payload_actor(payload) or "作業者不明",
            }
        )

    repo_by_full_name = {repository.full_name: repository for repository in repositories}
    recent_conflict_audits = db.scalars(
        select(AuditLog)
        .where(
            AuditLog.workspace_id == workspace.id,
            AuditLog.action.in_(["file_collision_detected", "file_collision_resolved"]),
        )
        .order_by(AuditLog.created_at.desc())
        .limit(5)
    ).all()
    conflict_events: list[dict[str, str]] = []
    for event in recent_conflict_audits:
        metadata = event.event_metadata or {}
        path = str(metadata.get("path") or "-")
        repository_full_name = str(metadata.get("repository") or "-")
        repository = repo_by_full_name.get(repository_full_name)
        branch_names = (
            _dashboard_related_branch_names(
                db,
                repository_id=repository.id,
                normalized_path=path,
            )
            if repository is not None and path != "-"
            else []
        )
        is_detected = event.action == "file_collision_detected"
        conflict_events.append(
            {
                "status_label": "競合発生" if is_detected else "競合解消",
                "status_class": "is-conflict" if is_detected else "is-resolved",
                "path_label": path,
                "repository_label": repository.display_name if repository else repository_full_name,
                "branches_label": _dashboard_branch_list_label(branch_names),
                "detail_label": "通知: 未設定" if is_detected else "解消: 最新状態で重複なし",
                "time_label": _dashboard_recent_or_timestamp(event.created_at, now),
            }
        )

    latest_conflict_at = max((collision.last_detected_at for collision in conflicts if collision.last_detected_at), default=None)

    return {
        "is_unconnected": not installations,
        "push_card": {
            "label": "今月のpush検知",
            "value": monthly_push_count,
            "limit": 500,
            "meta": f"最終受信: {_dashboard_recent_or_timestamp(latest_push_received_at, now)}",
            "icon": "stat_3",
            "tone": "",
        },
        "tracked_branches_card": {
            "label": "監視中ブランチ",
            "value": len(tracked_branches),
            "meta": "",
            "icon": "account_tree",
            "tone": "",
        },
        "conflicts_card": {
            "label": "未解消競合",
            "value": len(conflicts),
            "meta": f"最終発生: {_dashboard_recent_or_timestamp(latest_conflict_at, now)}",
            "icon": "warning",
            "tone": "is-conflict",
        },
        "conflict_events": conflict_events,
        "push_events": push_events,
        "conflicts_href": conflicts_href,
        "github_settings_href": github_settings_href,
    }


def workspace_members_data(db: Session, workspace: Workspace) -> dict[str, object]:
    members = db.scalars(
        select(WorkspaceMember).where(WorkspaceMember.workspace_id == workspace.id)
    ).all()
    invitations = db.scalars(
        select(WorkspaceInvitation).where(
            WorkspaceInvitation.workspace_id == workspace.id,
            WorkspaceInvitation.status == "pending",
            WorkspaceInvitation.revoked_at.is_(None),
        )
    ).all()
    return {
        "members": members,
        "pending_invitations": invitations,
    }


def workspace_settings_data(workspace: Workspace) -> list[dict[str, object]]:
    return [
        {
            "title": "Workspace 設定",
            "fields": [
                {"label": "Workspace 名", "value": workspace.name},
                {"label": "Slug", "value": workspace.slug},
                {"label": "Status", "value": workspace.status},
            ],
        },
        {
            "title": "権限運用",
            "fields": [
                {"label": "owner", "value": "請求、owner 管理、メンバー招待/削除、GitHub claim を担当"},
                {"label": "admin", "value": "GitHub 連携設定、repository 同期、workspace 設定閲覧"},
                {"label": "member/viewer", "value": "閲覧中心。member は将来の運用設定拡張枠"},
            ],
        },
    ]


def workspace_billing_data(workspace: Workspace) -> dict[str, str]:
    return {
        "plan": "準備中",
        "usage": f"{workspace.name} の billing 機能は後続実装です",
        "renewal": "未接続",
        "placeholder": "請求情報は owner のみ閲覧可能です。Stripe 等の接続前でも未認可公開にはしません。",
    }


def _ensure_workspace_member_belongs_to_workspace(member: WorkspaceMember, workspace: Workspace) -> None:
    if member.workspace_id != workspace.id:
        raise HTTPException(status_code=404, detail="Member not found")


def _active_owner_count(db: Session, workspace_id: str) -> int:
    return len(
        db.scalars(
            select(WorkspaceMember).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.role == "owner",
                WorkspaceMember.status == "active",
                WorkspaceMember.revoked_at.is_(None),
            )
        ).all()
    )


def _assert_owner_lifecycle_allows_change(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    actor: User,
    member: WorkspaceMember,
    next_role: str | None,
    action: str,
) -> None:
    active_owner_count = _active_owner_count(db, workspace.id)
    removes_owner_privilege = member.role == "owner" and next_role != "owner"
    if not removes_owner_privilege:
        return
    if active_owner_count <= 1:
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=actor.id,
            workspace_id=workspace.id,
            target_type="workspace_member",
            target_id=member.user_id,
            action="workspace_owner_guard_denied",
            metadata={"reason": "last_owner", "attempted_action": action, "next_role": next_role},
        )
        db.commit()
        raise HTTPException(status_code=409, detail="Last owner cannot be removed or demoted")
