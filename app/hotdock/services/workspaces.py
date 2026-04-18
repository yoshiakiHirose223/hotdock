from __future__ import annotations

from dataclasses import dataclass

from fastapi import HTTPException, Request
from sqlalchemy import delete, select
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
from app.models.github_pending_claim import GithubPendingClaim
from app.models.repository import Repository
from app.models.branch import Branch
from app.models.file_collision import FileCollision
from app.hotdock.services.github import REPOSITORY_SELECTION_ACTIVE, active_repository_limit


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


def build_workspace_navigation(workspace_slug: str, current_role: str | None = None) -> list[dict[str, str]]:
    base = f"/workspaces/{workspace_slug}"
    navigation = [
        {"label": "Dashboard", "href": f"{base}/dashboard", "key": "workspace-dashboard"},
        {"label": "Repositories", "href": f"{base}/repositories", "key": "workspace-repositories"},
        {"label": "Branches", "href": f"{base}/branches", "key": "workspace-branches"},
        {"label": "Conflicts", "href": f"{base}/conflicts", "key": "workspace-conflicts"},
        {"label": "Members", "href": f"{base}/members", "key": "workspace-members"},
        {"label": "Settings", "href": f"{base}/settings", "key": "workspace-settings"},
        {"label": "Billing", "href": f"{base}/billing", "key": "workspace-billing"},
        {"label": "GitHub", "href": f"/settings/integrations/github?workspace={workspace_slug}", "key": "workspace-github"},
    ]
    if current_role not in {"owner", "admin"}:
        navigation = [item for item in navigation if item["key"] not in {"workspace-settings", "workspace-github"}]
    if current_role != "owner":
        navigation = [item for item in navigation if item["key"] != "workspace-billing"]
    return navigation


def workspace_dashboard_data(db: Session, workspace: Workspace) -> dict[str, object]:
    installations = db.scalars(
        select(GithubInstallation).where(GithubInstallation.claimed_workspace_id == workspace.id)
    ).all()
    repositories = db.scalars(
        select(Repository).where(Repository.workspace_id == workspace.id, Repository.deleted_at.is_(None))
    ).all()
    active_repositories = [repository for repository in repositories if repository.selection_status == REPOSITORY_SELECTION_ACTIVE]
    active_repository_ids = [repository.id for repository in active_repositories]
    branches = db.scalars(
        select(Branch).where(Branch.workspace_id == workspace.id, Branch.repository_id.in_(active_repository_ids) if active_repository_ids else False)
    ).all()
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
    members = db.scalars(
        select(WorkspaceMember).where(
            WorkspaceMember.workspace_id == workspace.id,
            WorkspaceMember.status == "active",
            WorkspaceMember.revoked_at.is_(None),
        )
    ).all()
    return {
        "summary_cards": [
            {"label": "Installations", "value": str(len(installations)), "meta": "GitHub App 連携数"},
            {"label": "Repository Candidates", "value": str(len(repositories)), "meta": "利用可能な候補一覧"},
            {"label": "Active Repositories", "value": str(len(active_repositories)), "meta": f"現在の監視対象 / 上限 {active_repository_limit()}"},
            {"label": "Branches", "value": str(len(branches)), "meta": "同期済み branch"},
            {"label": "Open conflicts", "value": str(len(conflicts)), "meta": "競合候補"},
        ],
        "recent_repositories": repositories[:5],
        "active_repositories": active_repositories,
        "member_count": len(members),
        "installations": installations,
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
