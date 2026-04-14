from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import httpx
from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.hotdock.services.audit import record_audit_log
from app.hotdock.services.security import future_claim_expiry, generate_token, hash_token, utcnow, verify_token_hash
from app.models.branch import Branch
from app.models.branch_file import BranchFile
from app.models.conflict import Conflict
from app.models.github_installation import GithubInstallation
from app.models.github_installation_repository import GithubInstallationRepository
from app.models.github_pending_claim import GithubPendingClaim
from app.models.github_user_link import GithubUserLink
from app.models.github_webhook_event import GithubWebhookEvent
from app.models.repository import Repository
from app.models.user import User
from app.models.workspace import Workspace

settings = get_settings()


@dataclass
class PendingClaimResult:
    pending_claim: GithubPendingClaim
    claim_token: str


class GithubOAuthClient:
    async def exchange_code(self, code: str) -> dict[str, Any]:
        if settings.github_mock_oauth_enabled:
            return {
                "access_token": f"mock-access-{code}",
                "refresh_token": f"mock-refresh-{code}",
                "expires_in": 3600,
                "refresh_token_expires_in": 86400,
                "scope": "",
                "token_type": "bearer",
            }
        if not settings.github_app_client_id or not settings.github_app_client_secret:
            raise HTTPException(status_code=503, detail="GitHub OAuth is not configured")
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{settings.github_oauth_base_url}/login/oauth/access_token",
                headers={"Accept": "application/json"},
                data={
                    "client_id": settings.github_app_client_id,
                    "client_secret": settings.github_app_client_secret,
                    "code": code,
                },
            )
        response.raise_for_status()
        return response.json()

    async def fetch_user(self, access_token: str) -> dict[str, Any]:
        if settings.github_mock_oauth_enabled and access_token.startswith("mock-access-"):
            return {"id": 2001, "login": "mock-org-admin"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{settings.github_api_base_url}/user",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {access_token}",
                },
            )
        response.raise_for_status()
        return response.json()

    async def fetch_user_installations(self, access_token: str) -> list[dict[str, Any]]:
        if settings.github_mock_oauth_enabled and access_token.startswith("mock-access-"):
            return [{"id": 1001}, {"id": 1002}, {"id": 1003}]
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{settings.github_api_base_url}/user/installations",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {access_token}",
                },
            )
        response.raise_for_status()
        payload = response.json()
        return payload.get("installations", [])


def create_pending_github_claim(
    db: Session,
    request: Request,
    *,
    installation_id: int,
    initiated_via: str,
    setup_payload: dict[str, Any],
) -> PendingClaimResult:
    existing = db.scalar(
        select(GithubPendingClaim)
        .where(
            GithubPendingClaim.installation_id == installation_id,
            GithubPendingClaim.status.in_(["pending", "workspace_selected", "awaiting_github_auth"]),
            GithubPendingClaim.expires_at > utcnow(),
        )
        .order_by(GithubPendingClaim.created_at.desc())
    )
    if existing is not None:
        token = generate_token()
        existing.claim_token_hash = hash_token(token)
        existing.updated_at = utcnow()
        db.commit()
        return PendingClaimResult(pending_claim=existing, claim_token=token)

    claim_token = generate_token()
    pending = GithubPendingClaim(
        claim_token_hash=hash_token(claim_token),
        installation_id=installation_id,
        setup_nonce=generate_token(24),
        initiated_via=initiated_via,
        status="pending",
        expires_at=future_claim_expiry(),
        setup_payload=setup_payload,
    )
    db.add(pending)
    installation = db.scalar(select(GithubInstallation).where(GithubInstallation.installation_id == installation_id))
    if installation is None:
        installation = GithubInstallation(installation_id=installation_id, installation_status="claim_pending")
        db.add(installation)
    record_audit_log(
        db,
        request,
        actor_type="anonymous",
        actor_id=None,
        workspace_id=None,
        target_type="pending_claim",
        target_id=str(installation_id),
        action="pending_claim_created",
        metadata={"installation_id": installation_id, "initiated_via": initiated_via},
    )
    db.commit()
    db.refresh(pending)
    return PendingClaimResult(pending_claim=pending, claim_token=claim_token)


def load_pending_claim_by_token(db: Session, token: str) -> GithubPendingClaim | None:
    token_hash = hash_token(token)
    pending = db.scalar(select(GithubPendingClaim).where(GithubPendingClaim.claim_token_hash == token_hash))
    if pending is None:
        return None
    if pending.expires_at <= utcnow() and pending.status not in {"succeeded", "expired"}:
        pending.status = "expired"
        db.commit()
    return pending


def select_claim_workspace(
    db: Session,
    request: Request,
    *,
    pending_claim: GithubPendingClaim,
    user: User,
    workspace: Workspace,
) -> GithubPendingClaim:
    installation = db.scalar(select(GithubInstallation).where(GithubInstallation.installation_id == pending_claim.installation_id))
    if installation and installation.claimed_workspace_id and installation.claimed_workspace_id != workspace.id:
        raise HTTPException(status_code=409, detail="Installation already claimed")
    pending_claim.workspace_id = workspace.id
    pending_claim.user_id = user.id
    pending_claim.status = "workspace_selected"
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=workspace.id,
        target_type="pending_claim",
        target_id=pending_claim.id,
        action="claim_started",
        metadata={"installation_id": pending_claim.installation_id},
    )
    db.commit()
    db.refresh(pending_claim)
    return pending_claim


def set_pending_oauth_state(db: Session, pending_claim: GithubPendingClaim, state_token: str) -> None:
    pending_claim.oauth_state_hash = hash_token(state_token)
    pending_claim.status = "awaiting_github_auth"
    db.commit()


def verify_pending_oauth_state(pending_claim: GithubPendingClaim, state_token: str) -> bool:
    if not pending_claim.oauth_state_hash:
        return False
    return verify_token_hash(state_token, pending_claim.oauth_state_hash)


async def complete_github_claim(
    db: Session,
    request: Request,
    *,
    pending_claim: GithubPendingClaim,
    user: User,
    access_token: str,
    token_payload: dict[str, Any],
) -> GithubInstallation:
    if pending_claim.expires_at <= utcnow():
        pending_claim.status = "expired"
        db.commit()
        raise HTTPException(status_code=410, detail="Claim expired")
    if pending_claim.workspace_id is None:
        raise HTTPException(status_code=400, detail="Workspace is not selected")

    oauth_client = GithubOAuthClient()
    github_user = await oauth_client.fetch_user(access_token)
    user_installations = await oauth_client.fetch_user_installations(access_token)
    if not any(item.get("id") == pending_claim.installation_id for item in user_installations):
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=user.id,
            workspace_id=pending_claim.workspace_id,
            target_type="pending_claim",
            target_id=pending_claim.id,
            action="claim_failed",
            metadata={"reason": "installation_not_visible_to_user"},
        )
        db.commit()
        raise HTTPException(status_code=403, detail="GitHub user is not authorized for this installation")

    installation = db.scalar(
        select(GithubInstallation).where(GithubInstallation.installation_id == pending_claim.installation_id)
    )
    if installation is None:
        installation = GithubInstallation(installation_id=pending_claim.installation_id, installation_status="pending")
        db.add(installation)
        db.flush()

    if installation.claimed_workspace_id and installation.claimed_workspace_id != pending_claim.workspace_id:
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=user.id,
            workspace_id=pending_claim.workspace_id,
            target_type="github_installation",
            target_id=str(installation.installation_id),
            action="claim_failed",
            metadata={"reason": "already_claimed_other_workspace"},
        )
        db.commit()
        raise HTTPException(status_code=409, detail="Installation already claimed")

    github_link = db.scalar(
        select(GithubUserLink).where(GithubUserLink.user_id == user.id, GithubUserLink.github_user_id == github_user["id"])
    )
    if github_link is None:
        github_link = GithubUserLink(
            user_id=user.id,
            github_user_id=github_user["id"],
            github_login=github_user["login"],
            access_token_encrypted=access_token,
            refresh_token_encrypted=token_payload.get("refresh_token"),
            scope_snapshot=token_payload.get("scope", "").split(",") if token_payload.get("scope") else [],
        )
        db.add(github_link)
    else:
        github_link.github_login = github_user["login"]
        github_link.access_token_encrypted = access_token
        github_link.refresh_token_encrypted = token_payload.get("refresh_token")

    installation.claimed_workspace_id = pending_claim.workspace_id
    installation.claimed_by_user_id = user.id
    installation.claimed_at = utcnow()
    installation.installation_status = "active"
    pending_claim.github_user_id = github_user["id"]
    pending_claim.github_user_login = github_user["login"]
    pending_claim.user_id = user.id
    pending_claim.status = "succeeded"
    pending_claim.consumed_at = utcnow()
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=pending_claim.workspace_id,
        target_type="github_installation",
        target_id=str(installation.installation_id),
        action="claim_succeeded",
        metadata={"github_user_id": github_user["id"], "github_login": github_user["login"]},
    )
    db.commit()
    db.refresh(installation)
    return installation


def verify_github_webhook_signature(body: bytes, signature_header: str | None) -> bool:
    if not settings.github_app_webhook_secret or not signature_header:
        return False
    expected = "sha256=" + hmac.new(
        settings.github_app_webhook_secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


def record_webhook_event(
    db: Session,
    *,
    delivery_id: str,
    event_name: str,
    action_name: str | None,
    installation_id: int | None,
    payload: dict[str, Any],
    signature_valid: bool,
) -> GithubWebhookEvent | None:
    existing = db.scalar(select(GithubWebhookEvent).where(GithubWebhookEvent.delivery_id == delivery_id))
    if existing is not None:
        existing.processing_status = "replayed"
        db.commit()
        return None
    event = GithubWebhookEvent(
        delivery_id=delivery_id,
        event_name=event_name,
        action_name=action_name,
        installation_id=installation_id,
        signature_valid=signature_valid,
        payload=payload,
        payload_sha256=hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest(),
        processing_status="received",
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def sync_installation_event(db: Session, payload: dict[str, Any]) -> GithubInstallation:
    installation_payload = payload.get("installation") or {}
    installation_id = installation_payload.get("id")
    if installation_id is None:
        raise HTTPException(status_code=400, detail="installation id missing")
    installation = db.scalar(select(GithubInstallation).where(GithubInstallation.installation_id == installation_id))
    if installation is None:
        installation = GithubInstallation(installation_id=installation_id)
        db.add(installation)
        db.flush()
    account = installation_payload.get("account") or {}
    installation.github_account_id = account.get("id")
    installation.github_account_login = account.get("login")
    installation.github_account_type = account.get("type")
    installation.target_type = installation_payload.get("target_type") or account.get("type")
    installation.permissions_snapshot = installation_payload.get("permissions") or {}
    installation.events_snapshot = installation_payload.get("events") or []
    action = payload.get("action") or ""
    installation.last_webhook_event_at = utcnow()
    if action == "deleted":
        installation.installation_status = "uninstalled"
        installation.uninstalled_at = utcnow()
    elif action == "suspend":
        installation.installation_status = "suspended"
        installation.suspended_at = utcnow()
    elif action == "unsuspend":
        installation.installation_status = "active"
        installation.suspended_at = None
    else:
        installation.installation_status = "active"
    db.commit()
    db.refresh(installation)
    return installation


def sync_installation_repositories_event(db: Session, payload: dict[str, Any]) -> None:
    installation_payload = payload.get("installation") or {}
    installation = db.scalar(
        select(GithubInstallation).where(GithubInstallation.installation_id == installation_payload.get("id"))
    )
    if installation is None:
        return
    workspace_id = installation.claimed_workspace_id
    for repository_payload in payload.get("repositories_added") or []:
        record = db.scalar(
            select(GithubInstallationRepository).where(
                GithubInstallationRepository.installation_ref_id == installation.id,
                GithubInstallationRepository.github_repository_id == repository_payload["id"],
            )
        )
        if record is None:
            record = GithubInstallationRepository(
                installation_ref_id=installation.id,
                workspace_id=workspace_id,
                github_repository_id=repository_payload["id"],
                full_name=repository_payload["full_name"],
                name=repository_payload["name"],
                private=repository_payload.get("private", True),
                default_branch=repository_payload.get("default_branch"),
                status="active",
            )
            db.add(record)
        else:
            record.workspace_id = workspace_id
            record.status = "active"
            record.removed_at = None
    for repository_payload in payload.get("repositories_removed") or []:
        record = db.scalar(
            select(GithubInstallationRepository).where(
                GithubInstallationRepository.installation_ref_id == installation.id,
                GithubInstallationRepository.github_repository_id == repository_payload["id"],
            )
        )
        if record is not None:
            record.status = "removed"
            record.removed_at = utcnow()
    db.commit()


def sync_claimed_installation_repositories(db: Session, installation: GithubInstallation) -> None:
    if installation.claimed_workspace_id is None:
        return
    installation_repositories = db.scalars(
        select(GithubInstallationRepository).where(
            GithubInstallationRepository.installation_ref_id == installation.id,
            GithubInstallationRepository.status == "active",
        )
    ).all()
    for installation_repository in installation_repositories:
        repository = db.scalar(
            select(Repository).where(
                Repository.workspace_id == installation.claimed_workspace_id,
                Repository.github_repository_id == installation_repository.github_repository_id,
            )
        )
        if repository is None:
            repository = Repository(
                workspace_id=installation.claimed_workspace_id,
                github_installation_id=installation.id,
                github_repository_id=installation_repository.github_repository_id,
                full_name=installation_repository.full_name,
                display_name=installation_repository.name,
                provider="github",
                visibility="private" if installation_repository.private else "public",
                sync_status="active",
                last_synced_at=utcnow(),
            )
            db.add(repository)
        else:
            repository.sync_status = "active"
            repository.last_synced_at = utcnow()
    db.commit()


def record_push_event(db: Session, payload: dict[str, Any]) -> None:
    installation_id = (payload.get("installation") or {}).get("id")
    if installation_id is None:
        return
    installation = db.scalar(select(GithubInstallation).where(GithubInstallation.installation_id == installation_id))
    if installation is None or installation.claimed_workspace_id is None:
        return

    repository_payload = payload.get("repository") or {}
    repository = db.scalar(
        select(Repository).where(
            Repository.workspace_id == installation.claimed_workspace_id,
            Repository.github_repository_id == repository_payload.get("id"),
        )
    )
    if repository is None:
        repository = Repository(
            workspace_id=installation.claimed_workspace_id,
            github_installation_id=installation.id,
            github_repository_id=repository_payload["id"],
            full_name=repository_payload["full_name"],
            display_name=repository_payload["name"],
            provider="github",
            visibility="private" if repository_payload.get("private", True) else "public",
            sync_status="active",
            last_synced_at=utcnow(),
        )
        db.add(repository)
        db.flush()

    ref = payload.get("ref", "")
    branch_name = ref.split("/")[-1] if ref else "unknown"
    branch = db.scalar(
        select(Branch).where(Branch.repository_id == repository.id, Branch.name == branch_name)
    )
    if branch is None:
        branch = Branch(workspace_id=installation.claimed_workspace_id, repository_id=repository.id, name=branch_name)
        db.add(branch)
        db.flush()

    files: set[tuple[str, str]] = set()
    for commit in payload.get("commits") or []:
        for path in commit.get("added") or []:
            files.add((path, "added"))
        for path in commit.get("modified") or []:
            files.add((path, "modified"))
        for path in commit.get("removed") or []:
            files.add((path, "removed"))

    last_push_at = utcnow()
    branch.last_push_at = last_push_at
    branch.last_commit_sha = payload.get("after")

    existing_files = {
        item.path: item
        for item in db.scalars(select(BranchFile).where(BranchFile.branch_id == branch.id)).all()
    }
    conflict_count = 0
    for path, change_type in files:
        is_conflict = "conflict" in path.lower()
        file_record = existing_files.get(path)
        if file_record is None:
            file_record = BranchFile(
                workspace_id=installation.claimed_workspace_id,
                branch_id=branch.id,
                path=path,
                change_type=change_type,
                is_conflict=is_conflict,
                observed_at=last_push_at,
            )
            db.add(file_record)
        else:
            file_record.change_type = change_type
            file_record.is_conflict = is_conflict
            file_record.observed_at = last_push_at
        if is_conflict:
            conflict_count += 1
            conflict = db.scalar(
                select(Conflict).where(
                    Conflict.workspace_id == installation.claimed_workspace_id,
                    Conflict.repository_id == repository.id,
                    Conflict.primary_branch_id == branch.id,
                    Conflict.file_path == path,
                    Conflict.conflict_status == "open",
                )
            )
            if conflict is None:
                conflict = Conflict(
                    workspace_id=installation.claimed_workspace_id,
                    repository_id=repository.id,
                    primary_branch_id=branch.id,
                    file_path=path,
                    conflict_status="open",
                    first_detected_at=last_push_at,
                    last_detected_at=last_push_at,
                )
                db.add(conflict)
            else:
                conflict.last_detected_at = last_push_at

    branch.touched_files_count = len(files)
    branch.conflict_files_count = conflict_count
    branch.branch_status = "has_conflict" if conflict_count else "normal"
    repository.last_synced_at = last_push_at
    db.commit()
