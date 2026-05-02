from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx
import jwt
from fastapi import HTTPException, Request
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.hotdock.services.audit import AUDIT_STREAM_PATH, record_audit_log
from app.hotdock.services.security import future_claim_expiry, generate_token, hash_token, utcnow, verify_token_hash
from app.models import Base
from app.models.branch import Branch
from app.models.branch_event import BranchEvent
from app.models.branch_file import BranchFile
from app.models.file_collision import FileCollision
from app.models.file_collision_branch import FileCollisionBranch
from app.models.github_installation import GithubInstallation
from app.models.github_install_intent import GithubInstallIntent
from app.models.github_installation_repository import GithubInstallationRepository
from app.models.github_pending_claim import GithubPendingClaim
from app.models.github_user_link import GithubUserLink
from app.models.github_webhook_event import GithubWebhookEvent
from app.models.repository import Repository
from app.models.user import User
from app.models.workspace import Workspace

settings = get_settings()

MAX_ACTIVE_REPOSITORIES_PER_WORKSPACE = 1
REPOSITORY_SELECTION_UNSELECTED = "unselected"
REPOSITORY_SELECTION_ACTIVE = "active"
REPOSITORY_SELECTION_INACTIVE = "inactive"
REPOSITORY_SELECTION_INACCESSIBLE = "inaccessible"
DETAIL_SYNC_NOT_STARTED = "not_started"
DETAIL_SYNC_SYNCING = "syncing"
DETAIL_SYNC_COMPLETED = "completed"
DETAIL_SYNC_ERROR = "error"
BRANCH_TOUCH_SEED_STATUS_PAYLOAD = "seeded_from_payload"
BRANCH_TOUCH_SEED_STATUS_PARTIAL = "partial"
BRANCH_TOUCH_SEED_STATUS_API_ERROR = "api_error"
MANUAL_BRANCH_INPUT_MAX_CHARS = 100_000
MANUAL_BRANCH_INPUT_MAX_LINES = 5_000
MANUAL_BRANCH_PREFIX = "BRANCH:"


@dataclass
class PendingClaimResult:
    pending_claim: GithubPendingClaim
    claim_token: str


ARTICLE_TABLE_NAMES = {
    "articles",
    "blog_articles",
    "blog_posts",
    "posts",
}


def delete_all_non_article_data(
    db: Session,
    request: Request,
    *,
    actor_type: str,
    actor_id: str | None,
    actor_label: str | None,
) -> dict[str, int]:
    try:
        table_counts: dict[str, int] = {}
        deleted_tables: list[str] = []
        for table in reversed(Base.metadata.sorted_tables):
            if table.name in ARTICLE_TABLE_NAMES:
                continue
            count = db.execute(select(func.count()).select_from(table)).scalar_one()
            table_counts[table.name] = int(count)
            db.execute(table.delete())
            deleted_tables.append(table.name)

        db.commit()
        audit_stream_deleted = 0
        try:
            if AUDIT_STREAM_PATH.exists():
                AUDIT_STREAM_PATH.unlink()
                audit_stream_deleted = 1
        except OSError:
            audit_stream_deleted = 0
        return {
            "deleted_tables": len(deleted_tables),
            "deleted_rows": sum(table_counts.values()),
            "table_counts": table_counts,
            "deleted_table_names": deleted_tables,
            "deleted_audit_streams": audit_stream_deleted,
            "actor_type": actor_type,
            "actor_id": actor_id,
            "actor_label": actor_label,
        }
    except Exception as exc:
        db.rollback()
        raise


def future_install_intent_expiry() -> datetime:
    return utcnow() + timedelta(seconds=settings.github_install_intent_ttl_seconds)


def _collision_payload_actor(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    sender = payload.get("sender") or {}
    pusher = payload.get("pusher") or {}
    return sender.get("login") or pusher.get("name") or pusher.get("email")


def _collision_payload_commit_message(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    head_commit = payload.get("head_commit") or {}
    if head_commit.get("message"):
        return str(head_commit["message"])
    commits = payload.get("commits") or []
    if commits:
        message = commits[-1].get("message")
        if message:
            return str(message)
    return None


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _build_file_collision_snapshot(
    db: Session,
    *,
    active_files: list[BranchFile],
    occurred_at: datetime,
) -> tuple[dict[str, Any], str]:
    branch_ids = sorted({file_item.branch_id for file_item in active_files if file_item.branch_id})
    branches = {
        branch.id: branch
        for branch in db.scalars(select(Branch).where(Branch.id.in_(branch_ids))).all()
    } if branch_ids else {}
    delivery_ids = [
        branch.last_delivery_id
        for branch in branches.values()
        if branch.last_delivery_id
    ]
    webhook_by_delivery = {
        event.delivery_id: event
        for event in db.scalars(
            select(GithubWebhookEvent).where(GithubWebhookEvent.delivery_id.in_(delivery_ids))
        ).all()
    } if delivery_ids else {}

    snapshot_branches: list[dict[str, Any]] = []
    signature_rows: list[dict[str, Any]] = []
    for file_item in sorted(
        active_files,
        key=lambda item: (
            branches.get(item.branch_id).name if branches.get(item.branch_id) else "",
            item.path or "",
        ),
    ):
        branch = branches.get(file_item.branch_id)
        webhook_event = webhook_by_delivery.get(branch.last_delivery_id) if branch and branch.last_delivery_id else None
        payload = webhook_event.payload if webhook_event else {}
        last_updated_at = file_item.last_seen_at or (branch.last_push_at if branch else None) or occurred_at
        actor_label = _collision_payload_actor(payload) or ("手動登録" if branch and branch.observed_via == "manual" else None) or "作業者不明"
        commit_message = _collision_payload_commit_message(payload) or ("手動登録" if branch and branch.observed_via == "manual" else None)
        change_type = file_item.last_change_type or file_item.change_type or "modified"
        snapshot_branches.append(
            {
                "branch_id": file_item.branch_id,
                "branch_name": branch.name if branch else "-",
                "path": file_item.path,
                "change_type": change_type,
                "last_push_actor": actor_label,
                "last_updated_at": _serialize_datetime(last_updated_at),
                "observed_via_label": "手動追跡" if branch and branch.observed_via == "manual" else "Webhookで検出",
                "commit_message": commit_message,
            }
        )
        signature_rows.append(
            {
                "branch_id": file_item.branch_id,
                "path": file_item.path,
                "change_type": change_type,
                "last_updated_at": _serialize_datetime(last_updated_at),
                "head_sha": branch.current_head_sha if branch else None,
                "last_commit_sha": branch.last_commit_sha if branch else None,
            }
        )

    latest_entry = max(
        snapshot_branches,
        key=lambda item: item.get("last_updated_at") or "",
        default=None,
    )
    snapshot = {
        "branch_ids": [item["branch_id"] for item in snapshot_branches if item.get("branch_id")],
        "branches": snapshot_branches,
        "latest_actor": latest_entry.get("last_push_actor") if latest_entry else None,
        "latest_updated_at": latest_entry.get("last_updated_at") if latest_entry else None,
        "latest_commit_message": latest_entry.get("commit_message") if latest_entry else None,
    }
    signature = hashlib.sha1(
        json.dumps(signature_rows, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return snapshot, signature


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


def active_repository_limit() -> int:
    return MAX_ACTIVE_REPOSITORIES_PER_WORKSPACE


def repository_detail_sync_allowed(repository: Repository) -> bool:
    return (
        repository.is_active
        and repository.is_available
        and repository.selection_status == REPOSITORY_SELECTION_ACTIVE
        and repository.deleted_at is None
    )


def repository_selection_allowed(repository: Repository) -> bool:
    return (
        repository.deleted_at is None
        and repository.is_available
        and repository.selection_status != REPOSITORY_SELECTION_INACCESSIBLE
    )


def _normalize_path(path: str) -> str:
    return path


def _all_zero_sha(sha: str | None) -> bool:
    return bool(sha) and set(sha) == {"0"}


def _build_initial_branch_changes_from_push_commits(payload: dict[str, Any]) -> dict[str, Any]:
    commits = payload.get("commits")
    if not isinstance(commits, list) or not commits:
        return {
            "changes": [],
            "status": BRANCH_TOUCH_SEED_STATUS_API_ERROR,
            "error_message": "初回 push の commits payload にファイル一覧が含まれていないため、touched files を復元できませんでした。",
        }

    change_map: dict[str, dict[str, str | None]] = {}
    partial = False
    warnings: list[str] = []
    size_value = payload.get("size")
    if isinstance(size_value, int) and size_value > len(commits):
        partial = True
        warnings.append("push payload の commits 一覧が省略されている可能性があります。")
    if len(commits) >= 2048:
        partial = True
        warnings.append("push payload の commits 件数が上限に達しているため、初回 seed が不完全な可能性があります。")

    for commit in commits:
        if not isinstance(commit, dict):
            partial = True
            continue
        for payload_key, change_type in (("added", "added"), ("modified", "modified"), ("removed", "removed")):
            paths = commit.get(payload_key)
            if paths is None:
                continue
            if not isinstance(paths, list):
                partial = True
                warnings.append(f"commit payload の {payload_key} 形式が不正でした。")
                continue
            for raw_path in paths:
                path = str(raw_path or "").strip()
                if not path:
                    partial = True
                    continue
                normalized_path = _normalize_path(path)
                change_map[normalized_path] = {
                    "path": path,
                    "normalized_path": normalized_path,
                    "change_type": change_type,
                    "previous_path": None,
                }

    if not change_map:
        return {
            "changes": [],
            "status": BRANCH_TOUCH_SEED_STATUS_API_ERROR,
            "error_message": "初回 push payload から touched files を取り出せませんでした。",
        }

    warning_text = " ".join(dict.fromkeys(warnings)) if warnings else None
    return {
        "changes": list(change_map.values()),
        "status": BRANCH_TOUCH_SEED_STATUS_PARTIAL if partial else BRANCH_TOUCH_SEED_STATUS_PAYLOAD,
        "error_message": warning_text,
    }


def _set_branch_touch_seed_state(
    branch: Branch,
    *,
    source: str | None,
    seeded_at: datetime | None,
    status_value: str | None,
    error_message: str | None,
) -> None:
    branch.touch_seed_source = source
    branch.touch_seeded_at = seeded_at
    branch.touch_seed_status = status_value
    branch.touch_seed_warning = error_message
    branch.touch_seed_error_message = error_message


def _extract_branch_name(ref: str | None) -> str | None:
    if not ref or not ref.startswith("refs/heads/"):
        return None
    return ref.removeprefix("refs/heads/")


def _manual_change_type(status_token: str) -> str:
    if status_token == "A":
        return "added"
    if status_token == "M":
        return "modified"
    if status_token == "D":
        return "removed"
    if status_token.startswith("R"):
        return "renamed"
    raise HTTPException(status_code=422, detail=f"未対応のステータスです: {status_token}")


def _split_diff_columns(line: str) -> list[str]:
    if "\t" in line:
        return [column.strip() for column in line.split("\t") if column.strip()]
    return [column.strip() for column in re.split(r"\s+", line.strip()) if column.strip()]


def parse_manual_branch_registration_input(raw_text: str) -> tuple[str, list[dict[str, str | None]]]:
    if len(raw_text) > MANUAL_BRANCH_INPUT_MAX_CHARS:
        raise HTTPException(status_code=422, detail=f"入力は {MANUAL_BRANCH_INPUT_MAX_CHARS} 文字以内にしてください")

    lines = raw_text.splitlines()
    if len(lines) > MANUAL_BRANCH_INPUT_MAX_LINES:
        raise HTTPException(status_code=422, detail=f"入力行数は {MANUAL_BRANCH_INPUT_MAX_LINES} 行以内にしてください")

    if not lines:
        raise HTTPException(status_code=422, detail="1行目は BRANCH:<branch_name> 形式で入力してください")

    branch_line = lines[0].strip()
    if not branch_line.startswith(MANUAL_BRANCH_PREFIX):
        raise HTTPException(status_code=422, detail="1行目は BRANCH:<branch_name> 形式で入力してください")
    branch_name = branch_line.removeprefix(MANUAL_BRANCH_PREFIX).strip()
    if not branch_name:
        raise HTTPException(status_code=422, detail="1行目は BRANCH:<branch_name> 形式で入力してください")

    changes: list[dict[str, str | None]] = []
    for line_number, raw_line in enumerate(lines[1:], start=2):
        line = raw_line.strip()
        if not line:
            continue
        columns = _split_diff_columns(line)
        if not columns:
            continue
        status_token = columns[0]
        change_type = _manual_change_type(status_token)
        if change_type == "renamed":
            if len(columns) != 3:
                raise HTTPException(status_code=422, detail=f"{line_number}行目の rename 行は old path と new path の2つが必要です")
            previous_path, path = columns[1], columns[2]
            if not previous_path or not path:
                raise HTTPException(status_code=422, detail=f"{line_number}行目の rename 行は old path と new path の2つが必要です")
            changes.append(
                {
                    "path": path,
                    "normalized_path": _normalize_path(path),
                    "change_type": change_type,
                    "previous_path": previous_path,
                }
            )
            continue
        if len(columns) != 2:
            raise HTTPException(status_code=422, detail=f"{line_number}行目の diff 形式が不正です")
        path = columns[1]
        if not path:
            raise HTTPException(status_code=422, detail=f"{line_number}行目の path が空です")
        changes.append(
            {
                "path": path,
                "normalized_path": _normalize_path(path),
                "change_type": change_type,
                "previous_path": None,
            }
        )

    if not changes:
        raise HTTPException(status_code=422, detail="2行目以降に git diff --name-status の出力を貼り付けてください")

    return branch_name, changes


def _payload_occurred_at(payload: dict[str, Any]) -> datetime:
    repository_payload = payload.get("repository") or {}
    pushed_at = repository_payload.get("pushed_at")
    if isinstance(pushed_at, (int, float)):
        return datetime.fromtimestamp(pushed_at, tz=UTC).replace(tzinfo=None)
    head_commit = payload.get("head_commit") or {}
    timestamp = head_commit.get("timestamp")
    if isinstance(timestamp, str):
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)
        except ValueError:
            pass
    return utcnow()


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

    async def fetch_repository_branch(self, installation_token: str, repository_full_name: str, branch_name: str) -> dict[str, Any]:
        if settings.github_mock_oauth_enabled and installation_token.startswith("mock-installation-token-"):
            if not branch_name.strip():
                raise HTTPException(status_code=404, detail="Branch not found")
            repo_key = repository_full_name.replace("/", "-")
            return {"name": branch_name, "commit": {"sha": f"mock-{repo_key}-{branch_name.replace('/', '-')}"}} 
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{settings.github_api_base_url}/repos/{repository_full_name}/branches/{quote(branch_name, safe='')}",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {installation_token}",
                },
            )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="Branch not found")
        response.raise_for_status()
        return response.json()

    async def compare_commits(self, installation_token: str, repository_full_name: str, base_sha: str, head_sha: str) -> dict[str, Any]:
        if settings.github_mock_oauth_enabled and installation_token.startswith("mock-installation-token-"):
            repo_key = repository_full_name.replace("/", "-")
            return {
                "merge_base_commit": {"sha": base_sha or f"mock-base-{repo_key}"},
                "total_commits": 1,
                "commits": [{"sha": head_sha or f"mock-head-{repo_key}"}],
                "files": [
                    {
                        "filename": f"src/{repo_key}/service.py",
                        "status": "modified",
                    },
                    {
                        "filename": f"templates/{repo_key}/index.html",
                        "status": "added",
                    },
                ],
            }
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                f"{settings.github_api_base_url}/repos/{repository_full_name}/compare/{base_sha}...{head_sha}",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {installation_token}",
                },
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


async def resolve_callback_installation_unverified(
    db: Session,
    *,
    access_token: str,
    installation_id: int,
) -> tuple[dict[str, Any], GithubInstallation]:
    oauth_client = GithubOAuthClient()
    app_client = GithubAppClient()

    github_user = await oauth_client.fetch_user(access_token)
    installation = db.scalar(
        select(GithubInstallation).where(GithubInstallation.installation_id == installation_id)
    )
    if installation is None or installation.installation_status == "uninstalled":
        installation_payload = await app_client.fetch_installation(installation_id)
        installation = sync_installation_from_api_payload(db, installation_payload)
    if installation.installation_status == "uninstalled":
        raise HTTPException(status_code=404, detail="Installation is already uninstalled")
    return github_user, installation


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


def create_github_install_intent(
    db: Session,
    request: Request,
    *,
    state_token: str,
    workspace_slug: str | None,
    user_id: str | None,
    source: str,
) -> GithubInstallIntent:
    intent = GithubInstallIntent(
        state_token_hash=hash_token(state_token),
        workspace_slug=workspace_slug,
        user_id=user_id,
        source=source,
        expires_at=future_install_intent_expiry(),
    )
    db.add(intent)
    record_audit_log(
        db,
        request,
        actor_type="user" if user_id else "anonymous",
        actor_id=user_id,
        workspace_id=None,
        target_type="github_install_intent",
        target_id=intent.id,
        action="github_install_intent_created",
        metadata={"workspace_slug": workspace_slug, "source": source},
    )
    db.commit()
    db.refresh(intent)
    return intent


def load_github_install_intent_by_state(db: Session, state_token: str) -> GithubInstallIntent | None:
    return db.scalar(
        select(GithubInstallIntent).where(GithubInstallIntent.state_token_hash == hash_token(state_token))
    )


def consume_github_install_intent(
    db: Session,
    *,
    intent: GithubInstallIntent,
) -> GithubInstallIntent:
    locked_intent = db.scalar(
        select(GithubInstallIntent).where(GithubInstallIntent.id == intent.id).with_for_update()
    )
    if locked_intent is None:
        raise HTTPException(status_code=404, detail="Install intent not found")
    if locked_intent.expires_at <= utcnow():
        raise HTTPException(status_code=410, detail="Install intent expired")
    if locked_intent.consumed_at is not None:
        raise HTTPException(status_code=409, detail="Install intent already consumed")
    locked_intent.consumed_at = utcnow()
    db.commit()
    db.refresh(locked_intent)
    return locked_intent


def create_callback_pending_claim(
    db: Session,
    request: Request,
    *,
    installation: GithubInstallation,
    github_user: dict[str, Any],
    source: str,
    callback_state: str,
    install_intent: dict[str, Any] | None,
    verified_identity: bool = True,
) -> PendingClaimResult:
    result = create_pending_github_claim(
        db,
        request,
        installation_id=installation.installation_id,
        initiated_via=source,
        setup_payload={
            "source": source,
            "callback_state_hash": hash_token(callback_state) if callback_state else None,
            "authorization_verified_at": utcnow().isoformat() if verified_identity else None,
            "github_account_id": installation.github_account_id,
            "github_account_login": installation.github_account_login,
            "github_account_type": installation.github_account_type,
            "install_intent": install_intent or {},
            "identity_verification": "verified" if verified_identity else "callback_pending_recheck",
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
    pending_claim.status = "github_authorized" if verified_identity else "pending"
    pending_claim.oauth_state_hash = None
    pending_claim.state_verified_at = utcnow() if verified_identity else None
    pending_claim.callback_source = source
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
    elif pending.status not in {"expired"}:
        pending.last_resume_at = utcnow()
        db.commit()
    return pending


def reissue_pending_claim_token(db: Session, pending_claim: GithubPendingClaim) -> str:
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
    token = generate_token()
    locked_claim.claim_token_hash = hash_token(token)
    locked_claim.last_resume_at = utcnow()
    db.commit()
    return token


def find_resumable_pending_claims_for_github_user(
    db: Session,
    *,
    github_user_id: int,
    user_id: str | None = None,
) -> list[GithubPendingClaim]:
    claims = db.scalars(
        select(GithubPendingClaim)
        .where(
            GithubPendingClaim.github_user_id == github_user_id,
            GithubPendingClaim.status.in_(["pending", "workspace_selected", "awaiting_github_auth", "github_authorized"]),
            GithubPendingClaim.consumed_at.is_(None),
            GithubPendingClaim.expires_at > utcnow(),
        )
        .order_by(GithubPendingClaim.updated_at.desc(), GithubPendingClaim.created_at.desc())
    ).all()
    if user_id is None:
        return claims
    return [claim for claim in claims if claim.user_id in {None, user_id}]


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
    workspace = db.scalar(
        select(Workspace).where(
            Workspace.id == locked_claim.workspace_id,
            Workspace.deleted_at.is_(None),
        ).with_for_update()
    )
    if workspace is None:
        raise HTTPException(status_code=409, detail="Workspace is no longer available")

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
    installation.unlink_requested_at = None
    installation.unlinked_at = None
    installation.unlinked_by_user_id = None
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


def unlink_github_installation(
    db: Session,
    request: Request,
    *,
    installation: GithubInstallation,
    workspace: Workspace,
    actor: User,
) -> GithubInstallation:
    locked_installation = db.scalar(
        select(GithubInstallation)
        .where(GithubInstallation.id == installation.id)
        .with_for_update()
    )
    if locked_installation is None:
        raise HTTPException(status_code=404, detail="Installation not found")
    if locked_installation.claimed_workspace_id != workspace.id:
        raise HTTPException(status_code=404, detail="Installation not linked to this workspace")
    if locked_installation.installation_status == "unlinked":
        return locked_installation

    now = utcnow()
    locked_installation.unlink_requested_at = now
    locked_installation.unlinked_at = now
    locked_installation.unlinked_by_user_id = actor.id
    locked_installation.installation_status = "unlinked"
    locked_installation.claimed_workspace_id = None
    locked_installation.claimed_by_user_id = None

    installation_repositories = db.scalars(
        select(GithubInstallationRepository).where(
            GithubInstallationRepository.installation_ref_id == locked_installation.id,
        )
    ).all()
    for installation_repository in installation_repositories:
        installation_repository.workspace_id = None

    repositories = db.scalars(
        select(Repository).where(
            Repository.workspace_id == workspace.id,
            Repository.github_installation_id == locked_installation.id,
            Repository.deleted_at.is_(None),
        )
    ).all()
    for repository in repositories:
        repository.is_active = False
        repository.is_available = False
        repository.selection_status = REPOSITORY_SELECTION_INACCESSIBLE
        repository.inaccessible_reason = "permission_lost"
        repository.deactivated_at = repository.deactivated_at or now
        repository.sync_status = "unlinked"
        repository.last_synced_at = now

    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=actor.id,
        workspace_id=workspace.id,
        target_type="github_installation",
        target_id=str(locked_installation.installation_id),
        action="installation_unlinked",
        metadata={"installation_id": locked_installation.installation_id},
    )
    db.commit()
    db.refresh(locked_installation)
    return locked_installation


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
            record.full_name = repository_payload.get("full_name") or record.full_name
            record.name = repository_payload.get("name") or record.name
            record.private = repository_payload.get("private", record.private)
            record.default_branch = repository_payload.get("default_branch") or record.default_branch
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
    workspace_id = installation.claimed_workspace_id
    now = utcnow()
    if installation.installation_status in {"uninstalled", "suspended", "unlinked"}:
        repositories = db.scalars(
            select(Repository).where(
                Repository.workspace_id == workspace_id,
                Repository.github_installation_id == installation.id,
                Repository.deleted_at.is_(None),
            )
        ).all()
        for repository in repositories:
            if repository.selection_status == REPOSITORY_SELECTION_ACTIVE and repository.deactivated_at is None:
                repository.deactivated_at = now
            repository.is_available = False
            repository.is_active = False
            repository.selection_status = REPOSITORY_SELECTION_INACCESSIBLE
            repository.inaccessible_reason = "permission_lost"
            repository.sync_status = "inaccessible"
            repository.last_synced_at = now
        db.commit()
        return

    installation_repositories = db.scalars(
        select(GithubInstallationRepository).where(
            GithubInstallationRepository.installation_ref_id == installation.id,
            GithubInstallationRepository.status == "active",
        )
    ).all()
    seen_repository_ids = {row.github_repository_id for row in installation_repositories}

    for installation_repository in installation_repositories:
        repository = db.scalar(
            select(Repository).where(
                Repository.workspace_id == workspace_id,
                Repository.github_repository_id == installation_repository.github_repository_id,
            )
        )
        if repository is None:
            repository = Repository(
                workspace_id=workspace_id,
                github_installation_id=installation.id,
                github_repository_id=installation_repository.github_repository_id,
                full_name=installation_repository.full_name,
                display_name=installation_repository.name,
                default_branch=installation_repository.default_branch,
                provider="github",
                visibility="private" if installation_repository.private else "public",
                is_available=True,
                is_active=False,
                selection_status=REPOSITORY_SELECTION_UNSELECTED,
                sync_status="catalog_synced",
                detail_sync_status=DETAIL_SYNC_NOT_STARTED,
                last_synced_at=now,
            )
            db.add(repository)
        else:
            repository.github_installation_id = installation.id
            repository.full_name = installation_repository.full_name
            repository.display_name = installation_repository.name
            repository.default_branch = installation_repository.default_branch or repository.default_branch
            repository.visibility = "private" if installation_repository.private else "public"
            repository.is_available = True
            repository.inaccessible_reason = None
            if repository.selection_status == REPOSITORY_SELECTION_INACCESSIBLE:
                repository.selection_status = REPOSITORY_SELECTION_INACTIVE
            elif not repository.selection_status:
                repository.selection_status = REPOSITORY_SELECTION_UNSELECTED
            repository.is_active = repository.selection_status == REPOSITORY_SELECTION_ACTIVE
            repository.sync_status = "catalog_synced"
            repository.last_synced_at = now

        if repository.selection_status == REPOSITORY_SELECTION_ACTIVE:
            repository.is_active = True
        elif repository.selection_status in {REPOSITORY_SELECTION_UNSELECTED, REPOSITORY_SELECTION_INACTIVE, REPOSITORY_SELECTION_INACCESSIBLE}:
            repository.is_active = False

    existing_repositories = db.scalars(
        select(Repository).where(
            Repository.workspace_id == workspace_id,
            Repository.github_installation_id == installation.id,
            Repository.deleted_at.is_(None),
        )
    ).all()
    for repository in existing_repositories:
        if repository.github_repository_id in seen_repository_ids:
            continue
        was_active = repository.selection_status == REPOSITORY_SELECTION_ACTIVE
        repository.is_available = False
        repository.selection_status = REPOSITORY_SELECTION_INACCESSIBLE
        repository.inaccessible_reason = "removed_from_installation"
        repository.is_active = False
        repository.sync_status = "inaccessible"
        if was_active and repository.deactivated_at is None:
            repository.deactivated_at = now
        repository.last_synced_at = now
    db.commit()


def _upsert_installation_repositories_from_api(
    db: Session,
    *,
    installation: GithubInstallation,
    repositories_payload: list[dict[str, Any]],
) -> int:
    seen_repository_ids: set[int] = set()
    synced_count = 0
    now = utcnow()
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
            record.removed_at = now

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
                current_head_sha=commit.get("sha"),
                last_after_sha=commit.get("sha"),
                last_commit_sha=commit.get("sha"),
                touched_files_count=0,
                conflict_files_count=0,
                branch_status="normal",
                is_active=True,
                is_deleted=False,
            )
            db.add(branch)
        else:
            branch.current_head_sha = commit.get("sha") or branch.current_head_sha
            branch.last_after_sha = commit.get("sha") or branch.last_after_sha
            branch.last_commit_sha = commit.get("sha") or branch.last_commit_sha
            branch.is_active = True
            branch.is_deleted = False
            if branch.conflict_files_count == 0:
                branch.branch_status = "normal"
        synced_count += 1

    existing_branches = db.scalars(
        select(Branch).where(Branch.repository_id == repository.id)
    ).all()
    for branch in existing_branches:
        if branch.name not in seen_branch_names:
            branch.is_active = False
            branch.is_deleted = True

    db.commit()
    return synced_count


def _mark_repository_detail_sync_state(
    db: Session,
    *,
    repository: Repository,
    status_value: str,
    error_message: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> None:
    repository.detail_sync_status = status_value
    repository.detail_sync_error_message = error_message[:2048] if error_message else None
    if started_at is not None:
        repository.last_detail_sync_started_at = started_at
    if completed_at is not None:
        repository.last_detail_sync_completed_at = completed_at


def _locked_workspace_repositories(db: Session, workspace_id: str) -> list[Repository]:
    return db.scalars(
        select(Repository)
        .where(Repository.workspace_id == workspace_id, Repository.deleted_at.is_(None))
        .with_for_update()
    ).all()


def activate_workspace_repository_selection(
    db: Session,
    *,
    workspace: Workspace,
    repository: Repository,
) -> Repository:
    if repository.workspace_id != workspace.id or repository.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Repository not found")
    if not repository_selection_allowed(repository):
        raise HTTPException(status_code=409, detail="Repository is not selectable")

    now = utcnow()
    repositories = _locked_workspace_repositories(db, workspace.id)
    active_repositories = [
        item
        for item in repositories
        if item.selection_status == REPOSITORY_SELECTION_ACTIVE and item.id != repository.id
    ]

    for current in active_repositories:
        current.is_active = False
        current.selection_status = REPOSITORY_SELECTION_INACTIVE
        current.deactivated_at = now
        current.sync_status = "inactive"

    target = next((item for item in repositories if item.id == repository.id), repository)
    target.is_available = True
    target.is_active = True
    target.selection_status = REPOSITORY_SELECTION_ACTIVE
    target.activated_at = now
    target.inaccessible_reason = None
    target.sync_status = "active"
    if not target.detail_sync_status:
        _mark_repository_detail_sync_state(
            db,
            repository=target,
            status_value=DETAIL_SYNC_NOT_STARTED,
            error_message=None,
        )
    else:
        target.detail_sync_error_message = None
    db.commit()
    db.refresh(target)
    return target


class GenerateInstallationTokenService:
    async def __call__(self, installation: GithubInstallation) -> str:
        payload = await GithubAppClient().create_installation_token(installation.installation_id)
        token = payload.get("token")
        if not token:
            raise HTTPException(status_code=503, detail="installation token missing")
        return token


class CompareBranchRangeService:
    async def __call__(self, installation_token: str, repository: Repository, before_sha: str, after_sha: str) -> dict[str, Any]:
        if not repository_detail_sync_allowed(repository):
            raise HTTPException(status_code=409, detail="Repository is not active for compare")
        return await GithubAppClient().compare_commits(
            installation_token,
            repository.full_name,
            before_sha,
            after_sha,
        )


class UpsertBranchStateService:
    def __call__(
        self,
        db: Session,
        *,
        repository: Repository,
        branch_name: str,
        before_sha: str,
        after_sha: str,
        delivery_id: str,
        occurred_at: datetime,
        created: bool,
        deleted: bool,
        forced: bool,
    ) -> Branch:
        branch = db.scalar(
            select(Branch).where(Branch.repository_id == repository.id, Branch.name == branch_name)
        )
        if branch is None:
            branch = Branch(
                workspace_id=repository.workspace_id,
                repository_id=repository.id,
                name=branch_name,
                touched_files_count=0,
                conflict_files_count=0,
                branch_status="normal",
                is_active=not deleted,
                is_deleted=deleted,
                touch_seed_status=None,
                touch_seed_warning=None,
                touch_seed_error_message=None,
                has_authoritative_compare_history=False,
            )
            db.add(branch)
            db.flush()
        branch.last_push_at = occurred_at
        branch.last_before_sha = before_sha
        branch.last_after_sha = after_sha
        branch.last_delivery_id = delivery_id
        branch.was_created_observed = branch.was_created_observed or bool(created)
        branch.was_force_pushed_observed = branch.was_force_pushed_observed or bool(forced)
        branch.observed_via = "webhook"
        branch.has_webhook_history = True
        branch.is_deleted = bool(deleted)
        branch.is_active = not deleted
        if not deleted:
            branch.current_head_sha = after_sha
            branch.last_commit_sha = after_sha
            if branch.is_ignored_from_conflicts:
                branch.branch_status = "ignored"
            elif branch.conflict_files_count == 0:
                branch.branch_status = "normal"
        else:
            branch.branch_status = "deleted"
        return branch


def _apply_branch_file_changes(
    db: Session,
    *,
    repository: Repository,
    branch: Branch,
    changes: list[dict[str, str | None]],
    head_sha: str,
    occurred_at: datetime,
    source_kind: str,
    reset_existing_active: bool = False,
) -> dict[str, Any]:
    impacted_paths: set[str] = set()
    if reset_existing_active:
        existing_active_files = db.scalars(
            select(BranchFile).where(
                BranchFile.branch_id == branch.id,
                BranchFile.is_active.is_(True),
            )
        ).all()
        for file_item in existing_active_files:
            impacted_paths.add(file_item.normalized_path or _normalize_path(file_item.path))
            file_item.is_active = False
            file_item.is_conflict = False
            file_item.last_seen_at = occurred_at
            file_item.observed_at = occurred_at

    for change in changes:
        path = str(change.get("path") or "").strip()
        if not path:
            continue
        change_type = str(change.get("change_type") or "modified")
        previous_path = str(change.get("previous_path") or "").strip() or None
        normalized_path = str(change.get("normalized_path") or "").strip() or _normalize_path(path)
        impacted_paths.add(normalized_path)
        if previous_path:
            impacted_paths.add(_normalize_path(previous_path))

        if change_type == "renamed" and previous_path:
            previous_record = db.scalar(
                select(BranchFile).where(
                    BranchFile.branch_id == branch.id,
                    BranchFile.path == previous_path,
                )
            )
            if previous_record is not None:
                previous_record.is_active = False
                previous_record.is_conflict = False
                previous_record.last_seen_at = occurred_at
                previous_record.observed_at = occurred_at

        file_record = db.scalar(
            select(BranchFile).where(
                BranchFile.branch_id == branch.id,
                BranchFile.path == path,
            )
        )
        if file_record is None:
            file_record = BranchFile(
                workspace_id=repository.workspace_id,
                repository_id=repository.id,
                branch_id=branch.id,
                path=path,
                normalized_path=normalized_path,
                first_seen_change_type=change_type,
                change_type=change_type,
                last_change_type=change_type,
                previous_path=previous_path,
                last_seen_commit_sha=head_sha,
                first_seen_at=occurred_at,
                last_seen_at=occurred_at,
                source_kind=source_kind,
                is_active=True,
                is_conflict=False,
                observed_at=occurred_at,
            )
            db.add(file_record)
        else:
            file_record.repository_id = repository.id
            file_record.path = path
            file_record.normalized_path = normalized_path
            if not file_record.first_seen_change_type:
                file_record.first_seen_change_type = file_record.change_type or change_type
            if not file_record.first_seen_at:
                file_record.first_seen_at = file_record.observed_at or occurred_at
            if not file_record.change_type:
                file_record.change_type = file_record.first_seen_change_type or change_type
            file_record.last_change_type = change_type
            file_record.previous_path = previous_path
            file_record.last_seen_commit_sha = head_sha
            file_record.last_seen_at = occurred_at
            file_record.source_kind = source_kind
            file_record.is_active = True
            file_record.is_conflict = False
            file_record.observed_at = occurred_at

    # Sessions are configured with autoflush=False, so the writes above must be
    # flushed before downstream count/collision queries can see the current
    # path set for this branch.
    db.flush()

    branch.touched_files_count = db.scalar(
        select(func.count(BranchFile.id)).where(
            BranchFile.branch_id == branch.id,
            BranchFile.is_active.is_(True),
        )
    ) or 0
    return {
        "impacted_paths": impacted_paths,
        "files_seen": len(changes),
        "active_file_count": branch.touched_files_count,
        "head_commit_sha": head_sha,
    }


class UpsertBranchFilesFromCompareService:
    def __call__(
        self,
        db: Session,
        *,
        repository: Repository,
        branch: Branch,
        compare_payload: dict[str, Any],
        head_sha: str,
        occurred_at: datetime,
    ) -> dict[str, Any]:
        changed_files = [
            {
                "path": file_payload.get("filename"),
                "change_type": file_payload.get("status") or "modified",
                "previous_path": file_payload.get("previous_filename"),
            }
            for file_payload in (compare_payload.get("files") or [])
            if file_payload.get("filename")
        ]
        result = _apply_branch_file_changes(
            db,
            repository=repository,
            branch=branch,
            changes=changed_files,
            head_sha=head_sha,
            occurred_at=occurred_at,
            source_kind="compare",
            reset_existing_active=False,
        )
        result["merge_base_sha"] = ((compare_payload.get("merge_base_commit") or {}).get("sha"))
        return result


class UpsertBranchFilesFromPushPayloadSeedService:
    def __call__(
        self,
        db: Session,
        *,
        repository: Repository,
        branch: Branch,
        payload: dict[str, Any],
        head_sha: str,
        occurred_at: datetime,
    ) -> dict[str, Any]:
        seed_result = _build_initial_branch_changes_from_push_commits(payload)
        apply_result = _apply_branch_file_changes(
            db,
            repository=repository,
            branch=branch,
            changes=seed_result["changes"],
            head_sha=head_sha,
            occurred_at=occurred_at,
            source_kind="initial_payload_seed",
            reset_existing_active=False,
        )
        apply_result["seed_status"] = seed_result["status"]
        apply_result["error_message"] = seed_result["error_message"]
        return apply_result


async def manually_register_branch_snapshot(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    repository: Repository,
    actor: User,
    raw_text: str,
) -> dict[str, Any]:
    if repository.workspace_id != workspace.id or repository.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Repository not found")
    if not repository_detail_sync_allowed(repository):
        raise HTTPException(status_code=409, detail="現在この repository は監視対象ではありません")

    branch_name, changes = parse_manual_branch_registration_input(raw_text)
    installation = db.get(GithubInstallation, repository.github_installation_id)
    if installation is None or installation.claimed_workspace_id != workspace.id:
        raise HTTPException(status_code=409, detail="Repository installation is not available")
    if installation.installation_status in {"uninstalled", "suspended", "unlinked"}:
        raise HTTPException(status_code=409, detail="Installation is not active")

    installation_token = await GenerateInstallationTokenService()(installation)
    try:
        branch_payload = await GithubAppClient().fetch_repository_branch(
            installation_token,
            repository.full_name,
            branch_name,
        )
    except HTTPException as exc:
        if exc.status_code == 404:
            raise HTTPException(status_code=422, detail="指定されたブランチは GitHub 上に存在しません")
        raise
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="GitHub 上のブランチ確認に失敗しました") from exc

    occurred_at = utcnow()
    head_sha = str(((branch_payload.get("commit") or {}).get("sha")) or "")
    branch = db.scalar(
        select(Branch).where(
            Branch.repository_id == repository.id,
            Branch.name == branch_name,
        ).with_for_update()
    )
    created = False
    reactivated = False
    rescued_touch_seed = False
    if branch is None:
        branch = Branch(
            workspace_id=workspace.id,
            repository_id=repository.id,
            name=branch_name,
            touched_files_count=0,
            conflict_files_count=0,
            branch_status="tracked",
            is_active=True,
            is_deleted=False,
            observed_via="manual",
            touch_seed_source="manual_diff",
            touch_seeded_at=occurred_at,
            touch_seed_status=None,
            touch_seed_warning=None,
            touch_seed_error_message=None,
            has_authoritative_compare_history=False,
            has_webhook_history=False,
            current_head_sha=head_sha or None,
            last_commit_sha=head_sha or None,
            last_after_sha=head_sha or None,
            last_push_at=occurred_at,
        )
        db.add(branch)
        db.flush()
        created = True
    else:
        rescued_touch_seed = (
            branch.touch_seed_status in {BRANCH_TOUCH_SEED_STATUS_API_ERROR, BRANCH_TOUCH_SEED_STATUS_PARTIAL, BRANCH_TOUCH_SEED_STATUS_PAYLOAD}
            or (branch.has_webhook_history and not branch.has_authoritative_compare_history)
        )
        reactivated = branch.is_deleted or not branch.is_active
        branch.is_active = True
        branch.is_deleted = False
        branch.branch_status = "tracked"
        branch.observed_via = "manual"
        branch.touch_seed_source = "manual_diff"
        branch.touch_seeded_at = occurred_at
        branch.touch_seed_status = None
        branch.touch_seed_warning = None
        branch.touch_seed_error_message = None
        branch.has_authoritative_compare_history = False
        branch.current_head_sha = head_sha or branch.current_head_sha
        branch.last_commit_sha = head_sha or branch.last_commit_sha
        branch.last_after_sha = head_sha or branch.last_after_sha
        branch.last_push_at = occurred_at

    apply_result = _apply_branch_file_changes(
        db,
        repository=repository,
        branch=branch,
        changes=changes,
        head_sha=head_sha,
        occurred_at=occurred_at,
        source_kind="manual_input",
        reset_existing_active=True,
    )
    collision_result = RecalculateFileCollisionsService()(
        db,
        repository=repository,
        impacted_paths=apply_result["impacted_paths"],
        occurred_at=occurred_at,
    )
    repository.last_synced_at = occurred_at
    repository.sync_status = "active"
    _mark_repository_detail_sync_state(
        db,
        repository=repository,
        status_value=DETAIL_SYNC_COMPLETED,
        error_message=None,
        completed_at=occurred_at,
    )
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=actor.id,
        workspace_id=workspace.id,
        target_type="branch",
        target_id=branch.id,
        action="workspace_branch_manual_registered",
        metadata={
            "repository_id": repository.id,
            "branch_name": branch_name,
            "file_count": apply_result["active_file_count"],
            "created": created,
            "reactivated": reactivated,
            "observed_via": "manual",
            "source": "manual_diff",
            "success": True,
        },
    )
    db.commit()
    return {
        "branch_name": branch_name,
        "created": created,
        "reactivated": reactivated,
        "parsed_file_count": len(changes),
        "applied_file_count": apply_result["active_file_count"],
        "collision_recomputed": True,
        "new_collisions": len(collision_result["new_collisions"]),
        "resolved_collisions": len(collision_result["resolved_collisions"]),
        "observed_via": "manual",
        "touch_seed_source": "manual_diff",
        "rescued_touch_seed": rescued_touch_seed,
    }


class RecalculateFileCollisionsService:
    def __call__(
        self,
        db: Session,
        *,
        repository: Repository,
        impacted_paths: set[str],
        occurred_at: datetime,
    ) -> dict[str, Any]:
        new_collisions: list[str] = []
        resolved_collisions: list[str] = []
        affected_branch_ids: set[str] = set()

        for normalized_path in impacted_paths:
            active_files = db.scalars(
                select(BranchFile)
                .join(Branch, Branch.id == BranchFile.branch_id)
                .where(
                    BranchFile.repository_id == repository.id,
                    BranchFile.normalized_path == normalized_path,
                    BranchFile.is_active.is_(True),
                    BranchFile.is_ignored_from_conflicts.is_(False),
                    Branch.is_deleted.is_(False),
                    Branch.is_active.is_(True),
                    Branch.is_ignored_from_conflicts.is_(False),
                )
            ).all()
            active_files = [file_item for file_item in active_files if file_item.branch_id]
            active_branch_ids = {file_item.branch_id for file_item in active_files}
            affected_branch_ids.update(active_branch_ids)

            collision = db.scalar(
                select(FileCollision).where(
                    FileCollision.repository_id == repository.id,
                    FileCollision.normalized_path == normalized_path,
                )
            )
            previous_branch_ids: set[str] = set()
            if collision is not None:
                previous_branch_ids = {
                    row.branch_id
                    for row in db.scalars(
                        select(FileCollisionBranch).where(FileCollisionBranch.collision_id == collision.id)
                    ).all()
                }
                affected_branch_ids.update(previous_branch_ids)

            if len(active_branch_ids) >= 2:
                snapshot_payload, state_signature = _build_file_collision_snapshot(
                    db,
                    active_files=active_files,
                    occurred_at=occurred_at,
                )
                if collision is None:
                    collision = FileCollision(
                        repository_id=repository.id,
                        normalized_path=normalized_path,
                        active_branch_count=len(active_branch_ids),
                        collision_status="open",
                        state_signature=state_signature,
                        branch_snapshot_json=json.dumps(snapshot_payload, ensure_ascii=False),
                        first_detected_at=occurred_at,
                        last_detected_at=occurred_at,
                    )
                    db.add(collision)
                    db.flush()
                    new_collisions.append(normalized_path)
                else:
                    if collision.collision_status != "open":
                        collision.collision_status = "open"
                        collision.first_detected_at = collision.first_detected_at or occurred_at
                        collision.resolved_at = None
                        collision.acknowledged_at = None
                        collision.acknowledged_by_user_id = None
                        collision.acknowledged_signature = None
                        new_collisions.append(normalized_path)
                    elif collision.active_branch_count != len(active_branch_ids):
                        new_collisions.append(normalized_path)
                    collision.active_branch_count = len(active_branch_ids)
                    collision.state_signature = state_signature
                    collision.branch_snapshot_json = json.dumps(snapshot_payload, ensure_ascii=False)
                    collision.last_detected_at = occurred_at
                    collision.resolved_at = None

                collision.state_signature = state_signature
                collision.branch_snapshot_json = json.dumps(snapshot_payload, ensure_ascii=False)

                existing_rows = {
                    row.branch_id: row
                    for row in db.scalars(
                        select(FileCollisionBranch).where(FileCollisionBranch.collision_id == collision.id)
                    ).all()
                }
                for file_item in active_files:
                    row = existing_rows.pop(file_item.branch_id, None)
                    if row is None:
                        row = FileCollisionBranch(
                            collision_id=collision.id,
                            branch_id=file_item.branch_id,
                            path=file_item.path,
                            last_change_type=file_item.last_change_type or file_item.change_type,
                            updated_at=occurred_at,
                        )
                        db.add(row)
                    else:
                        row.path = file_item.path
                        row.last_change_type = file_item.last_change_type or file_item.change_type
                        row.updated_at = occurred_at
                if existing_rows:
                    db.execute(
                        delete(FileCollisionBranch).where(
                            FileCollisionBranch.collision_id == collision.id,
                            FileCollisionBranch.branch_id.in_(list(existing_rows.keys())),
                        )
                    )
            elif collision is not None:
                if collision.collision_status == "open":
                    collision.collision_status = "resolved"
                    collision.resolved_at = occurred_at
                    collision.last_detected_at = occurred_at
                    collision.active_branch_count = len(active_branch_ids)
                    resolved_collisions.append(normalized_path)
                else:
                    collision.active_branch_count = len(active_branch_ids)
                db.execute(delete(FileCollisionBranch).where(FileCollisionBranch.collision_id == collision.id))

        open_collision_paths = {
            item.normalized_path
            for item in db.scalars(
                select(FileCollision).where(
                    FileCollision.repository_id == repository.id,
                    FileCollision.normalized_path.in_(list(impacted_paths)),
                    FileCollision.collision_status == "open",
                )
            ).all()
        }
        active_impacted_files = db.scalars(
            select(BranchFile).where(
                BranchFile.repository_id == repository.id,
                BranchFile.normalized_path.in_(list(impacted_paths)),
            )
        ).all()
        affected_branches = {
            branch.id: branch
            for branch in db.scalars(select(Branch).where(Branch.id.in_({file_item.branch_id for file_item in active_impacted_files if file_item.branch_id}))).all()
        } if active_impacted_files else {}
        for file_item in active_impacted_files:
            branch = affected_branches.get(file_item.branch_id)
            file_item.is_conflict = (
                file_item.is_active
                and not file_item.is_ignored_from_conflicts
                and (not bool(branch.is_ignored_from_conflicts) if branch is not None else True)
                and file_item.normalized_path in open_collision_paths
            )

        # This project runs sessions with autoflush=False, so collision row
        # inserts/deletes above must be flushed before the per-branch count
        # queries below can observe the current open-collision state.
        db.flush()

        for branch_id in affected_branch_ids:
            branch = db.get(Branch, branch_id)
            if branch is None:
                continue
            branch.touched_files_count = db.scalar(
                select(func.count(BranchFile.id)).where(
                    BranchFile.branch_id == branch.id,
                    BranchFile.is_active.is_(True),
                )
            ) or 0
            branch.conflict_files_count = db.scalar(
                select(func.count(FileCollisionBranch.branch_id)).join(
                    FileCollision,
                    FileCollision.id == FileCollisionBranch.collision_id,
                ).where(
                    FileCollisionBranch.branch_id == branch.id,
                    FileCollision.collision_status == "open",
                )
            ) or 0
            if branch.is_deleted:
                branch.branch_status = "deleted"
            elif branch.is_ignored_from_conflicts:
                branch.branch_status = "ignored"
            elif branch.conflict_files_count > 0:
                branch.branch_status = "has_conflict"
            elif branch.touch_seed_status == BRANCH_TOUCH_SEED_STATUS_API_ERROR:
                branch.branch_status = "api_error"
            else:
                branch.branch_status = "normal"

        return {
            "new_collisions": new_collisions,
            "resolved_collisions": resolved_collisions,
        }


class NotifyFileCollisionsService:
    def __call__(
        self,
        db: Session,
        request: Request,
        *,
        repository: Repository,
        new_collisions: list[str],
        resolved_collisions: list[str],
    ) -> None:
        for normalized_path in new_collisions:
            record_audit_log(
                db,
                request,
                actor_type="system",
                actor_id=None,
                workspace_id=repository.workspace_id,
                target_type="repository",
                target_id=repository.id,
                action="file_collision_detected",
                metadata={"path": normalized_path, "repository": repository.full_name},
            )
        for normalized_path in resolved_collisions:
            record_audit_log(
                db,
                request,
                actor_type="system",
                actor_id=None,
                workspace_id=repository.workspace_id,
                target_type="repository",
                target_id=repository.id,
                action="file_collision_resolved",
                metadata={"path": normalized_path, "repository": repository.full_name},
            )


class HandleGithubPushWebhookService:
    async def __call__(self, db: Session, request: Request, *, delivery_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        installation_id = (payload.get("installation") or {}).get("id")
        if installation_id is None:
            return {"status": "ignored", "reason": "missing_installation_id"}

        installation = db.scalar(select(GithubInstallation).where(GithubInstallation.installation_id == installation_id))
        if installation is not None and installation.installation_status == "unlinked":
            return {"status": "ignored", "reason": "installation_unlinked"}
        if installation is None or installation.claimed_workspace_id is None:
            return {"status": "ignored", "reason": "installation_unclaimed"}
        if installation.installation_status in {"uninstalled", "suspended"}:
            return {"status": "ignored", "reason": "installation_inactive"}

        repository_payload = payload.get("repository") or {}
        repository = db.scalar(
            select(Repository).where(
                Repository.workspace_id == installation.claimed_workspace_id,
                Repository.github_repository_id == repository_payload.get("id"),
                Repository.deleted_at.is_(None),
            )
        )
        if repository is None:
            return {"status": "ignored", "reason": "repository_not_found"}
        if not repository.is_available:
            return {"status": "ignored", "reason": "repository_not_available"}
        if not repository.is_active or repository.selection_status != REPOSITORY_SELECTION_ACTIVE:
            return {"status": "ignored", "reason": "repository_not_active"}

        branch_name = _extract_branch_name(payload.get("ref"))
        if not branch_name:
            return {"status": "ignored", "reason": "ref_not_branch"}

        occurred_at = _payload_occurred_at(payload)
        before_sha = str(payload.get("before") or "")
        after_sha = str(payload.get("after") or "")
        created = bool(payload.get("created"))
        deleted = bool(payload.get("deleted"))
        forced = bool(payload.get("forced"))

        branch = UpsertBranchStateService()(
            db,
            repository=repository,
            branch_name=branch_name,
            before_sha=before_sha,
            after_sha=after_sha,
            delivery_id=delivery_id,
            occurred_at=occurred_at,
            created=created,
            deleted=deleted,
            forced=forced,
        )

        branch_event = BranchEvent(
            repository_id=repository.id,
            branch_id=branch.id,
            webhook_delivery_id=delivery_id,
            event_type="push",
            before_sha=before_sha or None,
            after_sha=after_sha or None,
            created=created,
            deleted=deleted,
            forced=forced,
            occurred_at=occurred_at,
        )
        db.add(branch_event)
        db.flush()
        repository.sync_status = "webhook_processing"
        _mark_repository_detail_sync_state(
            db,
            repository=repository,
            status_value=DETAIL_SYNC_SYNCING,
            error_message=None,
            started_at=occurred_at,
        )

        if deleted:
            branch_event.reason = "branch_deleted"
            impacted_paths = {
                item.normalized_path or _normalize_path(item.path)
                for item in db.scalars(
                    select(BranchFile).where(
                        BranchFile.branch_id == branch.id,
                        BranchFile.is_active.is_(True),
                    )
                ).all()
            }
            branch_files = db.scalars(select(BranchFile).where(BranchFile.branch_id == branch.id)).all()
            for file_item in branch_files:
                file_item.is_active = False
                file_item.is_conflict = False
                file_item.last_seen_at = occurred_at
                file_item.observed_at = occurred_at
            branch.touched_files_count = 0
            branch.conflict_files_count = 0
            branch.branch_status = "deleted"
            RecalculateFileCollisionsService()(db, repository=repository, impacted_paths=impacted_paths, occurred_at=occurred_at)
            repository.last_synced_at = occurred_at
            repository.sync_status = "active"
            _mark_repository_detail_sync_state(
                db,
                repository=repository,
                status_value=DETAIL_SYNC_COMPLETED,
                error_message=None,
                completed_at=occurred_at,
            )
            db.commit()
            return {"status": "processed", "reason": "branch_deleted", "compare_requested": False}

        if branch.last_processed_compare_head == after_sha or branch.current_head_sha == after_sha and branch.last_delivery_id != delivery_id:
            branch_event.compare_requested = False
            branch_event.compare_completed = False
            branch_event.reason = "after_sha_already_processed"
            db.commit()
            return {"status": "ignored", "reason": "after_sha_already_processed"}

        if created or _all_zero_sha(before_sha):
            branch_event.compare_requested = False
            branch_event.compare_completed = False
            seed_result = UpsertBranchFilesFromPushPayloadSeedService()(
                db,
                repository=repository,
                branch=branch,
                payload=payload,
                head_sha=after_sha,
                occurred_at=occurred_at,
            )
            branch.has_authoritative_compare_history = False
            if seed_result["seed_status"] == BRANCH_TOUCH_SEED_STATUS_API_ERROR:
                branch.branch_status = "api_error"
                _set_branch_touch_seed_state(
                    branch,
                    source="payload_commits",
                    seeded_at=occurred_at,
                    status_value=BRANCH_TOUCH_SEED_STATUS_API_ERROR,
                    error_message=seed_result["error_message"],
                )
                branch_event.reason = "initial_branch_push_seeded_from_payload_commits_partial"
            elif seed_result["seed_status"] == BRANCH_TOUCH_SEED_STATUS_PARTIAL:
                _set_branch_touch_seed_state(
                    branch,
                    source="payload_commits",
                    seeded_at=occurred_at,
                    status_value=BRANCH_TOUCH_SEED_STATUS_PARTIAL,
                    error_message=seed_result["error_message"],
                )
                branch_event.reason = "initial_branch_push_seeded_from_payload_commits_partial"
            else:
                _set_branch_touch_seed_state(
                    branch,
                    source="payload_commits",
                    seeded_at=occurred_at,
                    status_value=BRANCH_TOUCH_SEED_STATUS_PAYLOAD,
                    error_message=None,
                )
                branch_event.reason = "initial_branch_push_seeded_from_payload_commits"
            collision_result = RecalculateFileCollisionsService()(
                db,
                repository=repository,
                impacted_paths=seed_result["impacted_paths"],
                occurred_at=occurred_at,
            )
            NotifyFileCollisionsService()(
                db,
                request,
                repository=repository,
                new_collisions=collision_result["new_collisions"],
                resolved_collisions=collision_result["resolved_collisions"],
            )
            repository.last_synced_at = occurred_at
            repository.sync_status = "active"
            _mark_repository_detail_sync_state(
                db,
                repository=repository,
                status_value=DETAIL_SYNC_COMPLETED,
                error_message=seed_result["error_message"] if seed_result["seed_status"] != BRANCH_TOUCH_SEED_STATUS_PAYLOAD else None,
                completed_at=occurred_at,
            )
            db.commit()
            return {
                "status": "processed",
                "reason": branch_event.reason,
                "files_seen": seed_result["files_seen"],
                "new_collisions": len(collision_result["new_collisions"]),
                "resolved_collisions": len(collision_result["resolved_collisions"]),
            }

        branch_event.compare_requested = True
        try:
            installation_token = await GenerateInstallationTokenService()(installation)
            compare_payload = await CompareBranchRangeService()(installation_token, repository, before_sha, after_sha)
            compare_head_sha = after_sha or ((compare_payload.get("commits") or [{}])[-1].get("sha"))
            compare_result = UpsertBranchFilesFromCompareService()(
                db,
                repository=repository,
                branch=branch,
                compare_payload=compare_payload,
                head_sha=compare_head_sha,
                occurred_at=occurred_at,
            )
            branch.last_processed_compare_base = compare_result["merge_base_sha"] or before_sha or None
            branch.last_processed_compare_head = compare_head_sha
            branch.current_head_sha = compare_head_sha
            branch.last_commit_sha = compare_head_sha
            branch.has_authoritative_compare_history = True
            branch.touch_seed_status = None
            branch.touch_seed_warning = None
            branch.touch_seed_error_message = None
            branch_event.reason = "compare_completed"
            collision_result = RecalculateFileCollisionsService()(
                db,
                repository=repository,
                impacted_paths=compare_result["impacted_paths"],
                occurred_at=occurred_at,
            )
            NotifyFileCollisionsService()(
                db,
                request,
                repository=repository,
                new_collisions=collision_result["new_collisions"],
                resolved_collisions=collision_result["resolved_collisions"],
            )
            branch_event.compare_completed = True
            repository.last_synced_at = occurred_at
            repository.sync_status = "active"
            _mark_repository_detail_sync_state(
                db,
                repository=repository,
                status_value=DETAIL_SYNC_COMPLETED,
                error_message=None,
                completed_at=occurred_at,
            )
            db.commit()
            return {
                "status": "processed",
                "reason": "compare_completed",
                "files_seen": compare_result["files_seen"],
                "new_collisions": len(collision_result["new_collisions"]),
                "resolved_collisions": len(collision_result["resolved_collisions"]),
            }
        except (httpx.HTTPError, HTTPException) as exc:
            branch_event.compare_error = True
            branch_event.compare_error_message = str(getattr(exc, "detail", None) or exc)
            branch_event.reason = "compare_failed"
            branch.branch_status = "compare_error"
            repository.last_synced_at = occurred_at
            repository.sync_status = "detail_sync_error"
            _mark_repository_detail_sync_state(
                db,
                repository=repository,
                status_value=DETAIL_SYNC_ERROR,
                error_message=branch_event.compare_error_message,
            )
            record_audit_log(
                db,
                request,
                actor_type="github_app",
                actor_id=None,
                workspace_id=repository.workspace_id,
                target_type="branch_event",
                target_id=branch_event.id,
                action="branch_compare_failed",
                metadata={
                    "repository": repository.full_name,
                    "branch": branch.name,
                    "before": before_sha,
                    "after": after_sha,
                    "delivery_id": delivery_id,
                },
            )
            db.commit()
            return {"status": "accepted_with_error", "reason": branch_event.compare_error_message}

async def manual_sync_workspace_installation_repositories(
    db: Session,
    request: Request,
    *,
    workspace: Workspace,
    actor: User,
    record_audit_event: bool = True,
) -> dict[str, int]:
    installations = db.scalars(
        select(GithubInstallation).where(
            GithubInstallation.claimed_workspace_id == workspace.id,
            GithubInstallation.installation_status.not_in(["uninstalled", "suspended", "unlinked"]),
        )
    ).all()
    if not installations:
        raise HTTPException(status_code=400, detail="No claimed installations")

    app_client = GithubAppClient()
    installations_synced = 0
    repositories_synced = 0
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
        installations_synced += 1

    if record_audit_event:
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
                "branches_synced": 0,
                "skipped_installations": skipped_installations,
            },
        )
    db.commit()
    return {
        "installations_synced": installations_synced,
        "repositories_synced": repositories_synced,
        "branches_synced": 0,
        "skipped_installations": skipped_installations,
    }


async def record_push_event(db: Session, request: Request, *, delivery_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return await HandleGithubPushWebhookService()(db, request, delivery_id=delivery_id, payload=payload)
