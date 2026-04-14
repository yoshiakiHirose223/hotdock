from __future__ import annotations

from dataclasses import dataclass

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.hotdock.services.audit import record_audit_log
from app.hotdock.services.auth import deny_workspace_access, require_role
from app.hotdock.services.security import future_invitation_expiry, generate_token, hash_password, hash_token, utcnow
from app.models.user import User
from app.models.workspace import Workspace
from app.models.workspace_invitation import WorkspaceInvitation
from app.models.workspace_member import WorkspaceMember
from app.models.github_installation import GithubInstallation
from app.models.repository import Repository
from app.models.branch import Branch
from app.models.conflict import Conflict


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

    require_role(membership, required_role)
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


def build_workspace_navigation(workspace_slug: str) -> list[dict[str, str]]:
    base = f"/workspaces/{workspace_slug}"
    return [
        {"label": "Dashboard", "href": f"{base}/dashboard", "key": "workspace-dashboard"},
        {"label": "Repositories", "href": f"{base}/repositories", "key": "workspace-repositories"},
        {"label": "Branches", "href": f"{base}/branches", "key": "workspace-branches"},
        {"label": "Conflicts", "href": f"{base}/conflicts", "key": "workspace-conflicts"},
        {"label": "Members", "href": f"{base}/members", "key": "workspace-members"},
        {"label": "GitHub", "href": f"/settings/integrations/github?workspace={workspace_slug}", "key": "workspace-github"},
    ]


def workspace_dashboard_data(db: Session, workspace: Workspace) -> dict[str, object]:
    installations = db.scalars(
        select(GithubInstallation).where(GithubInstallation.claimed_workspace_id == workspace.id)
    ).all()
    repositories = db.scalars(
        select(Repository).where(Repository.workspace_id == workspace.id, Repository.deleted_at.is_(None))
    ).all()
    branches = db.scalars(select(Branch).where(Branch.workspace_id == workspace.id)).all()
    conflicts = db.scalars(select(Conflict).where(Conflict.workspace_id == workspace.id, Conflict.conflict_status == "open")).all()
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
            {"label": "Repositories", "value": str(len(repositories)), "meta": "監視対象 repository"},
            {"label": "Branches", "value": str(len(branches)), "meta": "同期済み branch"},
            {"label": "Open conflicts", "value": str(len(conflicts)), "meta": "競合候補"},
        ],
        "recent_repositories": repositories[:5],
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
