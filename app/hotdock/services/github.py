from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import jwt
from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
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


def _claim_metadata(pending_claim: GithubPendingClaim) -> dict[str, Any]:
    return dict(pending_claim.setup_payload or {})


def _update_claim_metadata(pending_claim: GithubPendingClaim, **updates: Any) -> None:
    metadata = _claim_metadata(pending_claim)
    for key, value in updates.items():
        if value is None and key in metadata:
            metadata.pop(key, None)
        elif value is not None:
            metadata[key] = value
    pending_claim.setup_payload = metadata


def _installation_activity_at(installation: GithubInstallation) -> datetime:
    return installation.last_webhook_event_at or installation.updated_at or installation.created_at or utcnow()


def pending_claim_has_verified_github_identity(pending_claim: GithubPendingClaim) -> bool:
    metadata = _claim_metadata(pending_claim)
    return (
        pending_claim.github_user_id is not None
        and bool(pending_claim.github_user_login)
        and bool(metadata.get("authorization_verified_at"))
    )


class GithubAppClient:
    def _require_app_credentials(self) -> tuple[str, str]:
        if settings.github_mock_oauth_enabled:
            return "mock-app-id", "mock-private-key"
        if not settings.github_app_id or not settings.github_private_key_pem:
            raise HTTPException(status_code=503, detail="GitHub App is not configured")
        return settings.github_app_id, settings.github_private_key_pem

    def generate_app_jwt(self) -> str:
        app_id, private_key = self._require_app_credentials()
        if settings.github_mock_oauth_enabled:
            return "mock-app-jwt"
        now = datetime.now(UTC)
        payload = {
            "iat": int((now - timedelta(seconds=60)).timestamp()),
            "exp": int((now + timedelta(minutes=9)).timestamp()),
            "iss": app_id,
        }
        return jwt.encode(payload, private_key, algorithm="RS256")

    async def fetch_installation(self, installation_id: int) -> dict[str, Any]:
        if settings.github_mock_oauth_enabled:
            return {
                "id": installation_id,
                "target_type": "Organization",
                "account": {
                    "id": installation_id + 9000,
                    "login": f"mock-account-{installation_id}",
                    "type": "Organization",
                },
                "permissions": {"contents": "read_only", "metadata": "read_only"},
                "events": ["installation", "installation_repositories", "push"],
            }
        app_jwt = self.generate_app_jwt()
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{settings.github_api_base_url}/app/installations/{installation_id}",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {app_jwt}",
                },
            )
        response.raise_for_status()
        return response.json()

    async def create_installation_token(self, installation_id: int) -> dict[str, Any]:
        if settings.github_mock_oauth_enabled:
            return {
                "token": f"mock-installation-token-{installation_id}",
                "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            }
        app_jwt = self.generate_app_jwt()
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{settings.github_api_base_url}/app/installations/{installation_id}/access_tokens",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {app_jwt}",
                },
            )
        response.raise_for_status()
        return response.json()

    async def fetch_installation_repositories(self, installation_token: str) -> list[dict[str, Any]]:
        if settings.github_mock_oauth_enabled and installation_token.startswith("mock-installation-token-"):
            installation_id = int(installation_token.removeprefix("mock-installation-token-"))
            return [
                {
                    "id": installation_id * 10 + 1,
                    "full_name": f"mock-org/repository-{installation_id}",
                    "name": f"repository-{installation_id}",
                    "private": True,
                    "default_branch": "main",
                }
            ]
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{settings.github_api_base_url}/installation/repositories",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {installation_token}",
                },
            )
        response.raise_for_status()
        payload = response.json()
        return payload.get("repositories", [])

    async def fetch_repository_branches(self, installation_token: str, repository_full_name: str) -> list[dict[str, Any]]:
        if settings.github_mock_oauth_enabled and installation_token.startswith("mock-installation-token-"):
            return [
                {"name": "main", "commit": {"sha": f"mock-{repository_full_name.replace('/', '-')}-main"}},
                {"name": "develop", "commit": {"sha": f"mock-{repository_full_name.replace('/', '-')}-develop"}},
            ]
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{settings.github_api_base_url}/repos/{repository_full_name}/branches",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {installation_token}",
                },
                params={"per_page": 100},
            )
        response.raise_for_status()
        return response.json()


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


async def resolve_callback_installation(
    db: Session,
    *,
    access_token: str,
    installation_id: int | None = None,
    issued_at_ts: int | None = None,
    preferred_workspace_id: str | None = None,
) -> tuple[dict[str, Any], GithubInstallation]:
    oauth_client = GithubOAuthClient()
    app_client = GithubAppClient()

    github_user = await oauth_client.fetch_user(access_token)
    visible_installations = await oauth_client.fetch_user_installations(access_token)
    visible_ids = [item.get("id") for item in visible_installations if item.get("id")]
    if not visible_ids:
        raise HTTPException(status_code=403, detail="GitHub user has no visible installations")

    if installation_id is not None:
        if installation_id not in visible_ids:
            raise HTTPException(status_code=403, detail="GitHub user is not authorized for this installation")
        installation = db.scalar(
            select(GithubInstallation).where(GithubInstallation.installation_id == installation_id)
        )
        if installation is None:
            installation_payload = await app_client.fetch_installation(installation_id)
            installation = sync_installation_from_api_payload(db, installation_payload)
        if installation.installation_status == "uninstalled":
            raise HTTPException(status_code=404, detail="Installation is already uninstalled")
        return github_user, installation

    local_installations = db.scalars(
        select(GithubInstallation).where(GithubInstallation.installation_id.in_(visible_ids))
    ).all()
    local_by_id = {installation.installation_id: installation for installation in local_installations}

    for installation_id in visible_ids:
        if installation_id in local_by_id:
            continue
        try:
            installation_payload = await app_client.fetch_installation(installation_id)
        except httpx.HTTPError:
            continue
        local_by_id[installation_id] = sync_installation_from_api_payload(db, installation_payload)

    candidates = [
        installation
        for installation_id in visible_ids
        for installation in [local_by_id.get(installation_id)]
        if installation is not None and installation.installation_status != "uninstalled"
    ]
    if not candidates:
        raise HTTPException(status_code=404, detail="No installable GitHub installations were found")

    if issued_at_ts:
        threshold = datetime.fromtimestamp(max(0, issued_at_ts - 600), tz=UTC).replace(tzinfo=None)
        recent_candidates = [installation for installation in candidates if _installation_activity_at(installation) >= threshold]
        if recent_candidates:
            candidates = recent_candidates

    if preferred_workspace_id:
        preferred_candidates = [
            installation for installation in candidates if installation.claimed_workspace_id == preferred_workspace_id
        ]
        if preferred_candidates:
            candidates = preferred_candidates
    else:
        unclaimed_candidates = [installation for installation in candidates if installation.claimed_workspace_id is None]
        if unclaimed_candidates:
            candidates = unclaimed_candidates

    candidates.sort(key=_installation_activity_at, reverse=True)
    if len(candidates) != 1:
        raise HTTPException(status_code=409, detail="Installation could not be uniquely determined")
    return github_user, candidates[0]


def _upsert_installation_snapshot(
    db: Session,
    *,
    installation_id: int,
    installation_payload: dict[str, Any] | None,
    default_status: str,
) -> GithubInstallation:
    installation = db.scalar(select(GithubInstallation).where(GithubInstallation.installation_id == installation_id))
    if installation is None:
        installation = GithubInstallation(installation_id=installation_id, installation_status=default_status)
        db.add(installation)
        db.flush()
    if installation_payload:
        account = installation_payload.get("account") or {}
        installation.github_account_id = account.get("id") or installation.github_account_id
        installation.github_account_login = account.get("login") or installation.github_account_login
        installation.github_account_type = account.get("type") or installation.github_account_type
        installation.target_type = installation_payload.get("target_type") or account.get("type") or installation.target_type
        installation.permissions_snapshot = installation_payload.get("permissions") or installation.permissions_snapshot or {}
        installation.events_snapshot = installation_payload.get("events") or installation.events_snapshot or []
    return installation

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
            GithubPendingClaim.status.in_(["pending", "workspace_selected", "awaiting_github_auth", "github_authorized"]),
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
        setup_nonce=setup_payload.get("install_state") or generate_token(24),
        initiated_via=initiated_via,
        status="pending",
        expires_at=future_claim_expiry(),
        setup_payload=setup_payload,
    )
    db.add(pending)
    _upsert_installation_snapshot(
        db,
        installation_id=installation_id,
        installation_payload=setup_payload.get("installation"),
        default_status="claim_pending",
    )
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


def create_callback_pending_claim(
    db: Session,
    request: Request,
    *,
    installation: GithubInstallation,
    github_user: dict[str, Any],
    source: str,
    callback_state: str,
    install_intent: dict[str, Any] | None,
) -> PendingClaimResult:
    result = create_pending_github_claim(
        db,
        request,
        installation_id=installation.installation_id,
        initiated_via=source,
        setup_payload={
            "source": source,
            "callback_state_hash": hash_token(callback_state),
            "authorization_verified_at": utcnow().isoformat(),
            "github_account_id": installation.github_account_id,
            "github_account_login": installation.github_account_login,
            "github_account_type": installation.github_account_type,
            "install_intent": install_intent or {},
        },
    )
    pending_claim = db.scalar(select(GithubPendingClaim).where(GithubPendingClaim.id == result.pending_claim.id).with_for_update())
    if pending_claim is None:
        raise HTTPException(status_code=500, detail="Pending claim could not be reloaded")
    pending_claim.workspace_id = None
    pending_claim.user_id = None
    pending_claim.consumed_at = None
    pending_claim.expires_at = future_claim_expiry()
    pending_claim.github_user_id = github_user["id"]
    pending_claim.github_user_login = github_user["login"]
    pending_claim.status = "github_authorized"
    pending_claim.oauth_state_hash = None
    db.commit()
    db.refresh(pending_claim)
    return PendingClaimResult(pending_claim=pending_claim, claim_token=result.claim_token)


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
    locked_claim = db.scalar(
        select(GithubPendingClaim).where(GithubPendingClaim.id == pending_claim.id).with_for_update()
    )
    if locked_claim is None:
        raise HTTPException(status_code=404, detail="Claim not found")
    if locked_claim.expires_at <= utcnow():
        locked_claim.status = "expired"
        db.commit()
        raise HTTPException(status_code=410, detail="Claim expired")
    if locked_claim.consumed_at is not None or locked_claim.status == "succeeded":
        raise HTTPException(status_code=409, detail="Claim already completed")
    if locked_claim.user_id and locked_claim.user_id != user.id:
        raise HTTPException(status_code=403, detail="Claim belongs to another user")

    installation = db.scalar(
        select(GithubInstallation)
        .where(GithubInstallation.installation_id == locked_claim.installation_id)
        .with_for_update()
    )
    if installation and installation.claimed_workspace_id and installation.claimed_workspace_id != workspace.id:
        raise HTTPException(status_code=409, detail="Installation already claimed")
    locked_claim.workspace_id = workspace.id
    locked_claim.user_id = user.id
    locked_claim.status = "workspace_selected"
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=workspace.id,
        target_type="pending_claim",
        target_id=locked_claim.id,
        action="claim_started",
        metadata={"installation_id": locked_claim.installation_id},
    )
    db.commit()
    db.refresh(locked_claim)
    return locked_claim


def set_pending_oauth_state(db: Session, pending_claim: GithubPendingClaim, state_token: str) -> None:
    locked_claim = db.scalar(
        select(GithubPendingClaim).where(GithubPendingClaim.id == pending_claim.id).with_for_update()
    )
    if locked_claim is None:
        raise HTTPException(status_code=404, detail="Claim not found")
    if locked_claim.expires_at <= utcnow():
        locked_claim.status = "expired"
        db.commit()
        raise HTTPException(status_code=410, detail="Claim expired")
    if locked_claim.consumed_at is not None or locked_claim.status == "succeeded":
        raise HTTPException(status_code=409, detail="Claim already completed")
    if locked_claim.status not in {"workspace_selected", "awaiting_github_auth"}:
        raise HTTPException(status_code=409, detail="Claim is not ready for GitHub authorization")
    locked_claim.oauth_state_hash = hash_token(state_token)
    locked_claim.status = "awaiting_github_auth"
    db.commit()


def verify_pending_oauth_state(pending_claim: GithubPendingClaim, state_token: str) -> bool:
    if not pending_claim.oauth_state_hash:
        return False
    return verify_token_hash(state_token, pending_claim.oauth_state_hash)


def finalize_github_claim(
    db: Session,
    request: Request,
    *,
    pending_claim: GithubPendingClaim,
    user: User,
) -> GithubInstallation:
    locked_claim = db.scalar(
        select(GithubPendingClaim).where(GithubPendingClaim.id == pending_claim.id).with_for_update()
    )
    if locked_claim is None:
        raise HTTPException(status_code=404, detail="Claim not found")
    if locked_claim.expires_at <= utcnow():
        locked_claim.status = "expired"
        db.commit()
        raise HTTPException(status_code=410, detail="Claim expired")
    if locked_claim.workspace_id is None:
        raise HTTPException(status_code=400, detail="Workspace is not selected")
    if locked_claim.user_id and locked_claim.user_id != user.id:
        raise HTTPException(status_code=403, detail="Claim belongs to another user")
    if not pending_claim_has_verified_github_identity(locked_claim):
        raise HTTPException(status_code=409, detail="GitHub authorization is not attached to this claim")

    installation = db.scalar(
        select(GithubInstallation)
        .where(GithubInstallation.installation_id == locked_claim.installation_id)
        .with_for_update()
    )
    if installation is None:
        installation = GithubInstallation(installation_id=locked_claim.installation_id, installation_status="pending")
        db.add(installation)
        db.flush()

    if locked_claim.status == "succeeded" and locked_claim.consumed_at is not None:
        if installation.claimed_workspace_id == locked_claim.workspace_id:
            return installation
        raise HTTPException(status_code=409, detail="Claim already completed")

    if installation.claimed_workspace_id and installation.claimed_workspace_id != locked_claim.workspace_id:
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=user.id,
            workspace_id=locked_claim.workspace_id,
            target_type="github_installation",
            target_id=str(installation.installation_id),
            action="claim_failed",
            metadata={"reason": "already_claimed_other_workspace"},
        )
        db.commit()
        raise HTTPException(status_code=409, detail="Installation already claimed")

    github_link = db.scalar(
        select(GithubUserLink).where(
            GithubUserLink.user_id == user.id,
            GithubUserLink.github_user_id == locked_claim.github_user_id,
        )
    )
    if github_link is None:
        github_link = GithubUserLink(
            user_id=user.id,
            github_user_id=locked_claim.github_user_id,
            github_login=locked_claim.github_user_login or "unknown",
            access_token_encrypted=None,
            refresh_token_encrypted=None,
            token_expires_at=None,
            scope_snapshot=[],
        )
        db.add(github_link)
    else:
        github_link.github_login = locked_claim.github_user_login or github_link.github_login
        github_link.access_token_encrypted = None
        github_link.refresh_token_encrypted = None
        github_link.token_expires_at = None

    installation.claimed_workspace_id = locked_claim.workspace_id
    installation.claimed_by_user_id = user.id
    installation.claimed_at = utcnow()
    installation.installation_status = "active"
    locked_claim.user_id = user.id
    locked_claim.status = "succeeded"
    locked_claim.consumed_at = utcnow()
    _update_claim_metadata(
        locked_claim,
        claimed_workspace_id=locked_claim.workspace_id,
        claimed_by_user_id=user.id,
        claim_completed_at=utcnow().isoformat(),
    )
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=locked_claim.workspace_id,
        target_type="github_installation",
        target_id=str(installation.installation_id),
        action="claim_succeeded",
        metadata={"github_user_id": locked_claim.github_user_id, "github_login": locked_claim.github_user_login},
    )
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Claim could not be completed safely") from exc
    db.refresh(installation)
    return installation


async def complete_github_claim(
    db: Session,
    request: Request,
    *,
    pending_claim: GithubPendingClaim,
    user: User,
    access_token: str,
    token_payload: dict[str, Any],
) -> GithubInstallation:
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
    locked_claim = db.scalar(
        select(GithubPendingClaim).where(GithubPendingClaim.id == pending_claim.id).with_for_update()
    )
    if locked_claim is None:
        raise HTTPException(status_code=404, detail="Claim not found")
    locked_claim.github_user_id = github_user["id"]
    locked_claim.github_user_login = github_user["login"]
    locked_claim.user_id = user.id
    locked_claim.status = "github_authorized" if locked_claim.workspace_id is None else "workspace_selected"
    locked_claim.oauth_state_hash = None
    _update_claim_metadata(
        locked_claim,
        authorization_source="legacy_explicit_authorize",
        authorization_verified_at=utcnow().isoformat(),
        oauth_scope=token_payload.get("scope", ""),
    )
    db.commit()
    return finalize_github_claim(db, request, pending_claim=locked_claim, user=user)


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
    payload_sha256: str,
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
        payload_sha256=payload_sha256,
        processing_status="received",
    )
    db.add(event)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.scalar(select(GithubWebhookEvent).where(GithubWebhookEvent.delivery_id == delivery_id))
        if existing is not None:
            existing.processing_status = "replayed"
            db.commit()
        return None
    db.refresh(event)
    return event


def mark_webhook_event_failed(db: Session, event_id: str, error_message: str) -> None:
    event = db.get(GithubWebhookEvent, event_id)
    if event is None:
        return
    event.processing_status = "failed"
    event.error_message = error_message[:2000]
    db.commit()


def sync_installation_from_api_payload(db: Session, installation_payload: dict[str, Any]) -> GithubInstallation:
    installation_id = installation_payload.get("id")
    if installation_id is None:
        raise HTTPException(status_code=400, detail="installation id missing")
    installation = _upsert_installation_snapshot(
        db,
        installation_id=installation_id,
        installation_payload=installation_payload,
        default_status="claim_pending",
    )
    installation.last_webhook_event_at = installation.last_webhook_event_at or utcnow()
    if installation.installation_status in {"pending", "claim_pending"}:
        installation.installation_status = "claim_pending"
    db.commit()
    db.refresh(installation)
    return installation


def sync_installation_event(db: Session, payload: dict[str, Any]) -> GithubInstallation:
    installation_payload = payload.get("installation") or {}
    installation_id = installation_payload.get("id")
    if installation_id is None:
        raise HTTPException(status_code=400, detail="installation id missing")
    installation = _upsert_installation_snapshot(
        db,
        installation_id=installation_id,
        installation_payload=installation_payload,
        default_status="pending",
    )
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
    installation_id = installation_payload.get("id")
    if installation_id is None:
        return
    installation = db.scalar(
        select(GithubInstallation).where(GithubInstallation.installation_id == installation_id)
    )
    if installation is None:
        installation = GithubInstallation(
            installation_id=installation_id,
            installation_status="pending",
        )
        db.add(installation)
        db.flush()
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
    if installation.claimed_workspace_id is not None:
        sync_claimed_installation_repositories(db, installation)


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


def _upsert_installation_repositories_from_api(
    db: Session,
    *,
    installation: GithubInstallation,
    repositories_payload: list[dict[str, Any]],
) -> int:
    seen_repository_ids: set[int] = set()
    synced_count = 0
    for repository_payload in repositories_payload:
        github_repository_id = repository_payload.get("id")
        if github_repository_id is None:
            continue
        seen_repository_ids.add(github_repository_id)
        record = db.scalar(
            select(GithubInstallationRepository).where(
                GithubInstallationRepository.installation_ref_id == installation.id,
                GithubInstallationRepository.github_repository_id == github_repository_id,
            )
        )
        if record is None:
            record = GithubInstallationRepository(
                installation_ref_id=installation.id,
                workspace_id=installation.claimed_workspace_id,
                github_repository_id=github_repository_id,
                full_name=repository_payload.get("full_name") or repository_payload.get("name") or str(github_repository_id),
                name=repository_payload.get("name") or repository_payload.get("full_name") or str(github_repository_id),
                private=repository_payload.get("private", True),
                default_branch=repository_payload.get("default_branch"),
                status="active",
            )
            db.add(record)
        else:
            record.workspace_id = installation.claimed_workspace_id
            record.full_name = repository_payload.get("full_name") or record.full_name
            record.name = repository_payload.get("name") or record.name
            record.private = repository_payload.get("private", record.private)
            record.default_branch = repository_payload.get("default_branch") or record.default_branch
            record.status = "active"
            record.removed_at = None
        synced_count += 1

    existing_records = db.scalars(
        select(GithubInstallationRepository).where(
            GithubInstallationRepository.installation_ref_id == installation.id,
        )
    ).all()
    for record in existing_records:
        if record.github_repository_id not in seen_repository_ids:
            record.status = "removed"
            record.removed_at = utcnow()

    db.commit()
    sync_claimed_installation_repositories(db, installation)
    return synced_count


def _upsert_repository_branches_from_api(
    db: Session,
    *,
    workspace_id: str,
    repository: Repository,
    branches_payload: list[dict[str, Any]],
) -> int:
    seen_branch_names: set[str] = set()
    synced_count = 0

    for branch_payload in branches_payload:
        branch_name = branch_payload.get("name")
        if not branch_name:
            continue
        seen_branch_names.add(branch_name)
        branch = db.scalar(
            select(Branch).where(
                Branch.repository_id == repository.id,
                Branch.name == branch_name,
            )
        )
        commit = branch_payload.get("commit") or {}
        if branch is None:
            branch = Branch(
                workspace_id=workspace_id,
                repository_id=repository.id,
                name=branch_name,
                last_commit_sha=commit.get("sha"),
                touched_files_count=0,
                conflict_files_count=0,
                branch_status="normal",
                is_active=True,
            )
            db.add(branch)
        else:
            branch.last_commit_sha = commit.get("sha") or branch.last_commit_sha
            branch.is_active = True
            if branch.conflict_files_count == 0:
                branch.branch_status = "normal"
        synced_count += 1

    existing_branches = db.scalars(
        select(Branch).where(Branch.repository_id == repository.id)
    ).all()
    for branch in existing_branches:
        if branch.name not in seen_branch_names:
            branch.is_active = False

    db.commit()
    return synced_count

async def manual_sync_workspace_installation_repositories(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    actor: User,
) -> dict[str, int]:
    installations = db.scalars(
        select(GithubInstallation).where(
            GithubInstallation.claimed_workspace_id == workspace.id,
            GithubInstallation.installation_status.not_in(["uninstalled", "suspended"]),
        )
    ).all()
    if not installations:
        raise HTTPException(status_code=400, detail="No claimed installations")

    app_client = GithubAppClient()
    installations_synced = 0
    repositories_synced = 0
    branches_synced = 0
    skipped_installations = 0

    for installation in installations:
        try:
            token_payload = await app_client.create_installation_token(installation.installation_id)
            installation_token = token_payload["token"]
        except (httpx.HTTPError, HTTPException, KeyError):
            skipped_installations += 1
            continue

        cached_installation_repositories = db.scalars(
            select(GithubInstallationRepository).where(
                GithubInstallationRepository.installation_ref_id == installation.id,
                GithubInstallationRepository.status == "active",
            )
        ).all()

        if cached_installation_repositories:
            sync_claimed_installation_repositories(db, installation)
            installations_synced += 1
            repositories_synced += len(cached_installation_repositories)
            workspace_repositories = db.scalars(
                select(Repository).where(
                    Repository.workspace_id == workspace.id,
                    Repository.github_installation_id == installation.id,
                    Repository.deleted_at.is_(None),
                )
            ).all()
            for repository in workspace_repositories:
                try:
                    branches_payload = await app_client.fetch_repository_branches(installation_token, repository.full_name)
                except httpx.HTTPError:
                    continue
                branches_synced += _upsert_repository_branches_from_api(
                    db,
                    workspace_id=workspace.id,
                    repository=repository,
                    branches_payload=branches_payload,
                )
            continue

        try:
            repositories_payload = await app_client.fetch_installation_repositories(installation_token)
        except httpx.HTTPError:
            skipped_installations += 1
            continue

        repositories_synced += _upsert_installation_repositories_from_api(
            db,
            installation=installation,
            repositories_payload=repositories_payload,
        )
        workspace_repositories = db.scalars(
            select(Repository).where(
                Repository.workspace_id == workspace.id,
                Repository.github_installation_id == installation.id,
                Repository.deleted_at.is_(None),
            )
        ).all()
        for repository in workspace_repositories:
            try:
                branches_payload = await app_client.fetch_repository_branches(installation_token, repository.full_name)
            except httpx.HTTPError:
                continue
            branches_synced += _upsert_repository_branches_from_api(
                db,
                workspace_id=workspace.id,
                repository=repository,
                branches_payload=branches_payload,
            )
        installations_synced += 1

    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=actor.id,
        workspace_id=workspace.id,
        target_type="workspace",
        target_id=workspace.id,
        action="workspace_repository_sync_requested",
        metadata={
            "installations_synced": installations_synced,
            "repositories_synced": repositories_synced,
            "branches_synced": branches_synced,
            "skipped_installations": skipped_installations,
        },
    )
    db.commit()
    return {
        "installations_synced": installations_synced,
        "repositories_synced": repositories_synced,
        "branches_synced": branches_synced,
        "skipped_installations": skipped_installations,
    }


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
