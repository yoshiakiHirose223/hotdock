from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from time import perf_counter_ns
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.tools.conflict_watch_models import (
    ConflictWatchBranch,
    ConflictWatchBranchCommit,
    ConflictWatchBranchCommitFile,
    ConflictWatchBranchFile,
    ConflictWatchBranchFileIgnore,
    ConflictWatchConflict,
    ConflictWatchConflictBranch,
    ConflictWatchIgnoreRule,
    ConflictWatchNotification,
    ConflictWatchRepository,
    ConflictWatchSecurityLog,
    ConflictWatchSetting,
    ConflictWatchWebhookEvent,
)

DEFAULT_IGNORE_PATTERNS = [
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "composer.lock",
    "dist/**",
    "build/**",
    "node_modules/**",
    "vendor/**",
    "tmp/**",
    "log/**",
    "*.png",
    "*.jpg",
    "*.jpeg",
    "*.gif",
    "*.webp",
    "*.pdf",
    "*.zip",
]

BRANCH_STATUS_ORDER = {
    "active": 4,
    "quiet": 3,
    "stale": 2,
    "merged_to_main_or_master": 1,
    "branch_excluded": 1,
}

ZERO_GIT_SHA = "0000000000000000000000000000000000000000"
MAINLINE_BRANCH_NAMES = {"main", "master"}


@dataclass(slots=True)
class ServiceMessage:
    message: str
    tone: str = "success"


@dataclass(slots=True)
class ObservedCommitFileChange:
    file_path: str
    normalized_file_path: str
    change_type: str
    previous_path: str | None = None


@dataclass(slots=True)
class ObservedCommit:
    commit_sha: str
    observed_at: datetime
    changes: list[ObservedCommitFileChange]
    message: str | None = None


class ConflictWatchService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.settings.conflict_watch_payloads_dir.mkdir(parents=True, exist_ok=True)
        self.settings.conflict_watch_processing_logs_dir.mkdir(parents=True, exist_ok=True)

    def now(self) -> datetime:
        return datetime.now(UTC)

    def _iso(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    def _coerce_datetime(self, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value), tz=UTC)
            except (OverflowError, OSError, ValueError):
                return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromtimestamp(float(text), tz=UTC)
        except (OverflowError, OSError, ValueError):
            pass
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    def normalize_path(self, file_path: str | None) -> str:
        if file_path is None:
            return ""
        normalized = str(file_path).strip().replace("\\", "/")
        normalized = re.sub(r"^\./", "", normalized)
        normalized = re.sub(r"/{2,}", "/", normalized)
        normalized = re.sub(r"/$", "", normalized)
        return normalized

    def _pattern_to_regex(self, pattern: str) -> re.Pattern[str] | None:
        normalized = self.normalize_path(pattern)
        if not normalized:
            return None
        placeholder = "__double_star__"
        escaped = re.escape(normalized)
        escaped = escaped.replace(r"\*\*", placeholder)
        escaped = escaped.replace(r"\*", "[^/]*")
        escaped = escaped.replace(placeholder, ".*")
        return re.compile(f"^{escaped}$", re.IGNORECASE)

    def _is_ignored_file(self, path: str, rules: list[ConflictWatchIgnoreRule]) -> bool:
        for rule in rules:
            if not rule.is_active:
                continue
            regex = self._pattern_to_regex(rule.pattern)
            if regex and regex.match(path):
                return True
        return False

    def _branch_file_ignore_lookup(
        self,
        branch_file_ignores: list[ConflictWatchBranchFileIgnore],
    ) -> dict[tuple[int, str], ConflictWatchBranchFileIgnore]:
        lookup: dict[tuple[int, str], ConflictWatchBranchFileIgnore] = {}
        for item in branch_file_ignores:
            if not item.is_active:
                continue
            lookup[(item.branch_id, item.normalized_file_path)] = item
        return lookup

    def _make_conflict_key(self, repository_id: int, normalized_file_path: str) -> str:
        return f"{repository_id}::{normalized_file_path}"

    def _push_history(self, conflict: ConflictWatchConflict, label: str, note: str, happened_at: datetime) -> None:
        history = list(conflict.history or [])
        last_entry = history[-1] if history else None
        if last_entry and last_entry.get("label") == label and last_entry.get("note") == note:
            return
        history.append({
            "happenedAt": self._iso(happened_at),
            "label": label,
            "note": note,
        })
        conflict.history = history

    def _get_or_create_settings(self, db: Session) -> ConflictWatchSetting:
        settings_rows = db.scalars(
            select(ConflictWatchSetting).order_by(ConflictWatchSetting.id.asc())
        ).all()
        if settings_rows:
            primary = settings_rows[0]
            if len(settings_rows) > 1:
                latest = settings_rows[-1]
                primary.stale_days = latest.stale_days
                primary.long_unresolved_days = latest.long_unresolved_days
                primary.raw_payload_retention_days = latest.raw_payload_retention_days
                primary.processing_trace_enabled = latest.processing_trace_enabled
                primary.force_push_note_enabled = latest.force_push_note_enabled
                primary.suppress_notice_notifications = latest.suppress_notice_notifications
                primary.notification_destination = latest.notification_destination
                primary.slack_webhook_url = latest.slack_webhook_url
                primary.github_webhook_endpoint = latest.github_webhook_endpoint
                primary.backlog_webhook_endpoint = latest.backlog_webhook_endpoint
                primary.github_webhook_secret = latest.github_webhook_secret
                primary.backlog_webhook_secret = latest.backlog_webhook_secret
                for redundant in settings_rows[1:]:
                    db.delete(redundant)
                db.flush()
            if primary.stale_days == 30:
                primary.stale_days = 15
                db.flush()
            return primary
        settings_row = ConflictWatchSetting()
        db.add(settings_row)
        db.flush()
        return settings_row

    def _repository_rules(self, repository: ConflictWatchRepository) -> list[ConflictWatchIgnoreRule]:
        return sorted(repository.ignore_rules, key=lambda rule: rule.id)

    def _clone_default_ignore_rules(self, db: Session, repository: ConflictWatchRepository, now: datetime) -> None:
        if repository.ignore_rules:
            return
        for pattern in DEFAULT_IGNORE_PATTERNS:
            db.add(
                ConflictWatchIgnoreRule(
                    repository_id=repository.id,
                    rule_type="path_pattern",
                    pattern=pattern,
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                )
            )

    def _get_repository(self, db: Session, repository_id: int) -> ConflictWatchRepository:
        repository = db.scalar(
            select(ConflictWatchRepository)
            .where(ConflictWatchRepository.id == repository_id)
            .options(
                selectinload(ConflictWatchRepository.branches).selectinload(ConflictWatchBranch.branch_files),
                selectinload(ConflictWatchRepository.ignore_rules),
                selectinload(ConflictWatchRepository.conflicts).selectinload(ConflictWatchConflict.conflict_branches),
                selectinload(ConflictWatchRepository.conflicts).selectinload(ConflictWatchConflict.notifications),
                selectinload(ConflictWatchRepository.webhook_events),
            )
        )
        if not repository:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Repository not found")
        return repository

    def _ensure_repository(self, db: Session, provider_type: str, external_repo_id: str, repository_name: str) -> ConflictWatchRepository:
        repository = db.scalar(
            select(ConflictWatchRepository).where(
                ConflictWatchRepository.provider_type == provider_type,
                ConflictWatchRepository.external_repo_id == external_repo_id,
            )
        )
        now = self.now()
        if repository:
            if repository.repository_name != repository_name:
                repository.repository_name = repository_name
                repository.updated_at = now
            return repository
        repository = ConflictWatchRepository(
            provider_type=provider_type,
            external_repo_id=external_repo_id,
            repository_name=repository_name,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        db.add(repository)
        db.flush()
        self._clone_default_ignore_rules(db, repository, now)
        return repository

    def _is_mainline_branch_name(self, branch_name: str | None) -> bool:
        return str(branch_name or "").strip() in MAINLINE_BRANCH_NAMES

    def _compute_branch_status(self, branch: ConflictWatchBranch, stale_days: int, now: datetime) -> str:
        if branch.monitoring_closed_reason == "merged_to_main_or_master":
            return "merged_to_main_or_master"
        if branch.is_branch_excluded:
            return "branch_excluded"
        if branch.last_seen_at is None:
            return "quiet"
        days = (now - branch.last_seen_at).days
        if days >= stale_days:
            return "stale"
        if days >= 7 or not branch.is_monitored:
            return "quiet"
        return "active"

    def _compute_branch_confidence(self, branch: ConflictWatchBranch, stale_days: int, now: datetime) -> str:
        if branch.monitoring_closed_reason == "merged_to_main_or_master":
            return "high"
        if branch.possibly_inconsistent or branch.is_deleted:
            return "low"
        if branch.last_seen_at is None:
            return "medium"
        days = (now - branch.last_seen_at).days
        if days >= stale_days:
            return "low"
        if days >= 7 or branch.is_branch_excluded:
            return "medium"
        return "high"

    def _remove_branch(self, db: Session, branch: ConflictWatchBranch) -> None:
        db.query(ConflictWatchBranchFileIgnore).filter(
            ConflictWatchBranchFileIgnore.branch_id == branch.id,
        ).delete()
        db.query(ConflictWatchBranchFile).filter(
            ConflictWatchBranchFile.branch_id == branch.id,
        ).delete()
        db.query(ConflictWatchBranchCommitFile).filter(
            ConflictWatchBranchCommitFile.branch_id == branch.id,
        ).delete()
        db.query(ConflictWatchBranchCommit).filter(
            ConflictWatchBranchCommit.branch_id == branch.id,
        ).delete()
        db.delete(branch)
        db.flush()

    def _get_or_create_branch(
        self,
        db: Session,
        repository: ConflictWatchRepository,
        branch_name: str,
        *,
        now: datetime,
    ) -> ConflictWatchBranch:
        branch = db.scalar(
            select(ConflictWatchBranch).where(
                ConflictWatchBranch.repository_id == repository.id,
                ConflictWatchBranch.branch_name == branch_name,
            )
        )
        if branch:
            return branch
        branch = ConflictWatchBranch(
            repository_id=repository.id,
            branch_name=branch_name,
            is_monitored=True,
            status="active",
            last_push_at=None,
            latest_after_sha=None,
            last_seen_at=None,
            is_deleted=False,
            is_branch_excluded=False,
            possibly_inconsistent=False,
            confidence="high",
            memo="",
            monitoring_closed_reason=None,
            monitoring_closed_at=None,
            created_at=now,
            updated_at=now,
        )
        db.add(branch)
        db.flush()
        return branch

    def _build_file_changes(
        self,
        *,
        added: list[str] | None = None,
        modified: list[str] | None = None,
        removed: list[str] | None = None,
        renamed: list[dict[str, str]] | None = None,
    ) -> list[ObservedCommitFileChange]:
        changes: list[ObservedCommitFileChange] = []
        seen: set[tuple[str, str, str | None]] = set()

        def append_change(path: str, change_type: str, previous_path: str | None = None) -> None:
            normalized = self.normalize_path(path)
            normalized_previous = self.normalize_path(previous_path) if previous_path else None
            if not normalized:
                return
            key = (normalized, change_type, normalized_previous)
            if key in seen:
                return
            seen.add(key)
            changes.append(
                ObservedCommitFileChange(
                    file_path=normalized,
                    normalized_file_path=normalized,
                    change_type=change_type,
                    previous_path=normalized_previous,
                )
            )

        for file_path in added or []:
            append_change(file_path, "added")
        for file_path in modified or []:
            append_change(file_path, "modified")
        for file_path in removed or []:
            append_change(file_path, "removed")
        for item in renamed or []:
            if not isinstance(item, dict):
                continue
            append_change(item.get("newPath") or item.get("to") or "", "renamed", item.get("oldPath") or item.get("from"))
        return changes

    def _looks_like_single_rename_pair(
        self,
        added_path: str,
        removed_path: str,
        *,
        modified_paths: list[str],
    ) -> bool:
        if modified_paths:
            return False
        if added_path == removed_path:
            return False
        added = PurePosixPath(added_path)
        removed = PurePosixPath(removed_path)
        if added.suffix.lower() != removed.suffix.lower():
            return False
        same_parent = added.parent == removed.parent
        same_stem = added.stem.lower() == removed.stem.lower()
        return same_parent or same_stem

    def _extract_github_commit_change_sets(
        self,
        commit: dict[str, Any],
    ) -> tuple[list[str], list[str], list[str], list[dict[str, str]]]:
        added = [self.normalize_path(item) for item in list(commit.get("added", []) or []) if self.normalize_path(item)]
        modified = [self.normalize_path(item) for item in list(commit.get("modified", []) or []) if self.normalize_path(item)]
        removed = [self.normalize_path(item) for item in list(commit.get("removed", []) or []) if self.normalize_path(item)]
        renamed: list[dict[str, str]] = []

        if len(added) == 1 and len(removed) == 1 and self._looks_like_single_rename_pair(
            added[0],
            removed[0],
            modified_paths=modified,
        ):
            renamed.append({"oldPath": removed[0], "newPath": added[0]})
            added = []
            removed = []

        return added, modified, removed, renamed

    def _make_synthetic_commit_sha(self, event: ConflictWatchWebhookEvent, suffix: str = "aggregate") -> str:
        after_sha = self.normalize_path(event.after_sha or "")
        if after_sha:
            return after_sha
        return f"synthetic:{event.provider_type}:{event.delivery_id}:{suffix}"

    def _event_to_single_observed_commit(self, event: ConflictWatchWebhookEvent) -> list[ObservedCommit]:
        changes = self._build_file_changes(
            added=list(event.files_added or []),
            modified=list(event.files_modified or []),
            removed=list(event.files_removed or []),
            renamed=list(event.files_renamed or []),
        )
        if not changes:
            return []
        return [
            ObservedCommit(
                commit_sha=self._make_synthetic_commit_sha(event),
                observed_at=event.pushed_at or self.now(),
                changes=changes,
            )
        ]

    def _extract_github_commit_observations(
        self,
        payload: dict[str, Any],
        *,
        delivery_id: str,
        pushed_at: datetime,
    ) -> list[ObservedCommit]:
        observed: list[ObservedCommit] = []
        commits = payload.get("commits", []) or []
        head_commit = payload.get("head_commit")
        source_commits = commits
        if not source_commits and isinstance(head_commit, dict):
            source_commits = [head_commit]

        for index, commit in enumerate(source_commits):
            if not isinstance(commit, dict):
                continue
            commit_sha = str(commit.get("id") or "").strip() or f"synthetic:github:{delivery_id}:{index}"
            added, modified, removed, renamed = self._extract_github_commit_change_sets(commit)
            changes = self._build_file_changes(
                added=added,
                modified=modified,
                removed=removed,
                renamed=renamed,
            )
            observed.append(
                ObservedCommit(
                    commit_sha=commit_sha,
                    observed_at=pushed_at,
                    changes=changes,
                    message=str(commit.get("message") or "").strip() or None,
                )
            )
        return observed

    def _extract_github_event_file_sets(
        self,
        payload: dict[str, Any],
    ) -> tuple[list[str], list[str], list[str], list[dict[str, str]]]:
        files_added: list[str] = []
        files_modified: list[str] = []
        files_removed: list[str] = []
        files_renamed: list[dict[str, str]] = []
        commits = payload.get("commits", []) or []
        head_commit = payload.get("head_commit")
        source_commits = commits or ([head_commit] if isinstance(head_commit, dict) else [])

        def append_unique(values: list[str], target: list[str]) -> None:
            for value in values:
                if value and value not in target:
                    target.append(value)

        for commit in source_commits:
            if not isinstance(commit, dict):
                continue
            added, modified, removed, renamed = self._extract_github_commit_change_sets(commit)
            append_unique(added, files_added)
            append_unique(modified, files_modified)
            append_unique(removed, files_removed)
            for item in renamed:
                if item not in files_renamed:
                    files_renamed.append(item)
        return files_added, files_modified, files_removed, files_renamed

    def _extract_github_pushed_at(self, payload: dict[str, Any]) -> datetime | None:
        repository = payload.get("repository")
        if isinstance(repository, dict):
            pushed_at = self._coerce_datetime(repository.get("pushed_at"))
            if pushed_at is not None:
                return pushed_at
        head_commit = payload.get("head_commit")
        if isinstance(head_commit, dict):
            commit_timestamp = self._coerce_datetime(head_commit.get("timestamp"))
            if commit_timestamp is not None:
                return commit_timestamp
        return None

    def _extract_github_pull_request_merge_payload(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        action = str(payload.get("action") or "").strip()
        pull_request = payload.get("pull_request")
        if action != "closed" or not isinstance(pull_request, dict):
            return None
        if not bool(pull_request.get("merged")):
            return None
        base = pull_request.get("base") or {}
        head = pull_request.get("head") or {}
        base_ref = str(base.get("ref") or "").strip()
        head_ref = str(head.get("ref") or "").strip()
        if not self._is_mainline_branch_name(base_ref) or not head_ref:
            return None
        repository = payload.get("repository") if isinstance(payload.get("repository"), dict) else {}
        repository_external_id = str(
            repository.get("full_name")
            or ((base.get("repo") or {}).get("full_name") if isinstance(base.get("repo"), dict) else "")
            or ((head.get("repo") or {}).get("full_name") if isinstance(head.get("repo"), dict) else "")
            or ""
        ).strip()
        repository_name = str(
            repository.get("name")
            or ((base.get("repo") or {}).get("name") if isinstance(base.get("repo"), dict) else "")
            or ((head.get("repo") or {}).get("name") if isinstance(head.get("repo"), dict) else "")
            or repository_external_id
        ).strip() or repository_external_id
        merged_at = self._coerce_datetime(pull_request.get("merged_at")) or self.now()
        return {
            "repositoryExternalId": repository_external_id,
            "repositoryName": repository_name,
            "headRef": head_ref,
            "baseRef": base_ref,
            "headSha": str(head.get("sha") or "").strip() or None,
            "baseSha": str(base.get("sha") or "").strip() or None,
            "mergedAt": merged_at,
            "mergedBy": (
                (pull_request.get("merged_by") or {}).get("login")
                or (payload.get("sender") or {}).get("login")
                or None
            ),
            "pullRequestNumber": pull_request.get("number"),
        }

    def _next_branch_sequence_no(self, db: Session, branch: ConflictWatchBranch) -> int:
        current_max = db.scalar(
            select(func.max(ConflictWatchBranchCommit.sequence_no)).where(
                ConflictWatchBranchCommit.repository_id == branch.repository_id,
                ConflictWatchBranchCommit.branch_id == branch.id,
            )
        )
        return int(current_max or 0) + 1

    def _set_branch_commit_state(
        self,
        branch_commit: ConflictWatchBranchCommit,
        *,
        is_active: bool,
        observed_at: datetime,
        event_id: int | None,
    ) -> None:
        branch_commit.is_active = is_active
        branch_commit.observed_at = observed_at
        branch_commit.last_seen_at = observed_at
        branch_commit.observed_via_event_id = event_id
        branch_commit.updated_at = self.now()

    def _set_branch_commit_files_state(
        self,
        db: Session,
        branch_commit_id: int,
        *,
        is_active: bool,
        observed_at: datetime,
    ) -> None:
        rows = db.scalars(
            select(ConflictWatchBranchCommitFile).where(
                ConflictWatchBranchCommitFile.branch_commit_id == branch_commit_id,
            )
        ).all()
        now = self.now()
        for row in rows:
            row.is_active = is_active
            row.observed_at = observed_at
            row.updated_at = now

    def _append_observed_commits(
        self,
        db: Session,
        branch: ConflictWatchBranch,
        event: ConflictWatchWebhookEvent,
        observed_commits: list[ObservedCommit],
        *,
        is_active: bool,
    ) -> None:
        sequence_no = self._next_branch_sequence_no(db, branch)
        now = self.now()
        for observed_commit in observed_commits:
            branch_commit = db.scalar(
                select(ConflictWatchBranchCommit).where(
                    ConflictWatchBranchCommit.repository_id == branch.repository_id,
                    ConflictWatchBranchCommit.branch_id == branch.id,
                    ConflictWatchBranchCommit.commit_sha == observed_commit.commit_sha,
                )
            )
            if branch_commit is None:
                branch_commit = ConflictWatchBranchCommit(
                    repository_id=branch.repository_id,
                    branch_id=branch.id,
                    commit_sha=observed_commit.commit_sha,
                    sequence_no=sequence_no,
                    observed_via_event_id=event.id,
                    observed_at=observed_commit.observed_at,
                    is_active=is_active,
                    first_seen_at=observed_commit.observed_at,
                    last_seen_at=observed_commit.observed_at,
                    created_at=now,
                    updated_at=now,
                )
                db.add(branch_commit)
                db.flush()
                sequence_no += 1
            else:
                self._set_branch_commit_state(
                    branch_commit,
                    is_active=is_active,
                    observed_at=observed_commit.observed_at,
                    event_id=event.id,
                )

            for change in observed_commit.changes:
                commit_file = db.scalar(
                    select(ConflictWatchBranchCommitFile).where(
                        ConflictWatchBranchCommitFile.repository_id == branch.repository_id,
                        ConflictWatchBranchCommitFile.branch_id == branch.id,
                        ConflictWatchBranchCommitFile.commit_sha == branch_commit.commit_sha,
                        ConflictWatchBranchCommitFile.normalized_file_path == change.normalized_file_path,
                        ConflictWatchBranchCommitFile.change_type == change.change_type,
                    )
                )
                if commit_file is None:
                    commit_file = ConflictWatchBranchCommitFile(
                        repository_id=branch.repository_id,
                        branch_id=branch.id,
                        branch_commit_id=branch_commit.id,
                        commit_sha=branch_commit.commit_sha,
                        file_path=change.file_path,
                        normalized_file_path=change.normalized_file_path,
                        change_type=change.change_type,
                        previous_path=change.previous_path,
                        observed_at=observed_commit.observed_at,
                        is_active=is_active,
                        created_at=now,
                        updated_at=now,
                    )
                    db.add(commit_file)
                else:
                    commit_file.branch_commit_id = branch_commit.id
                    commit_file.file_path = change.file_path
                    commit_file.previous_path = change.previous_path
                    commit_file.observed_at = observed_commit.observed_at
                    commit_file.is_active = is_active
                    commit_file.updated_at = now
        db.flush()

    def _apply_observed_touched_files_to_branch_cache(
        self,
        db: Session,
        branch: ConflictWatchBranch,
        observed_commits: list[ObservedCommit],
        *,
        trace: dict[str, Any] | None = None,
    ) -> None:
        if not observed_commits:
            self._trace_step(
                trace,
                "branch_cache_updated",
                branchName=branch.branch_name,
                touchedFileCount=0,
                insertedCount=0,
                updatedCount=0,
                commitReplayCount=0,
            )
            return

        current_state: dict[str, dict[str, Any]] = {}
        for observed_commit in observed_commits:
            for change in observed_commit.changes:
                if change.change_type == "renamed" and change.previous_path:
                    previous_path = self.normalize_path(change.previous_path)
                    if previous_path:
                        current_state[previous_path] = {
                            "file_path": previous_path,
                            "normalized_file_path": previous_path,
                            "change_type": "removed",
                            "previous_path": change.normalized_file_path,
                            "observed_at": observed_commit.observed_at,
                        }
                current_state[change.normalized_file_path] = {
                    "file_path": change.file_path,
                    "normalized_file_path": change.normalized_file_path,
                    "change_type": change.change_type,
                    "previous_path": change.previous_path,
                    "observed_at": observed_commit.observed_at,
                }

        existing_rows = db.scalars(
            select(ConflictWatchBranchFile).where(
                ConflictWatchBranchFile.repository_id == branch.repository_id,
                ConflictWatchBranchFile.branch_id == branch.id,
            )
        ).all()
        existing_by_path = {row.normalized_file_path: row for row in existing_rows}
        now = self.now()
        inserted_count = 0
        updated_count = 0

        for normalized_file_path, item in current_state.items():
            existing = existing_by_path.pop(normalized_file_path, None)
            if existing:
                existing.file_path = str(item["file_path"])
                existing.change_type = str(item["change_type"])
                existing.previous_path = str(item["previous_path"]) if item["previous_path"] else None
                existing.last_seen_at = item["observed_at"]
                existing.updated_at = now
                updated_count += 1
                continue
            db.add(
                ConflictWatchBranchFile(
                    repository_id=branch.repository_id,
                    branch_id=branch.id,
                    file_path=str(item["file_path"]),
                    normalized_file_path=normalized_file_path,
                    change_type=str(item["change_type"]),
                    previous_path=str(item["previous_path"]) if item["previous_path"] else None,
                    first_seen_at=item["observed_at"],
                    last_seen_at=item["observed_at"],
                    updated_at=now,
                )
            )
            inserted_count += 1
        db.flush()

        active_ignores = db.scalars(
            select(ConflictWatchBranchFileIgnore).where(
                ConflictWatchBranchFileIgnore.branch_id == branch.id,
            )
        ).all()
        refreshed_branch_files = db.scalars(
            select(ConflictWatchBranchFile).where(
                ConflictWatchBranchFile.branch_id == branch.id,
            )
        ).all()
        branch_file_by_path = {row.normalized_file_path: row for row in refreshed_branch_files}
        for ignore_entry in active_ignores:
            branch_file = branch_file_by_path.get(ignore_entry.normalized_file_path)
            if branch_file is None:
                db.delete(ignore_entry)
                continue
            ignore_entry.branch_file_id = branch_file.id
            ignore_entry.updated_at = now
        db.flush()
        self._trace_step(
            trace,
            "branch_cache_updated",
            branchName=branch.branch_name,
            touchedFileCount=len(current_state),
            insertedCount=inserted_count,
            updatedCount=updated_count,
            commitReplayCount=0,
        )

    def _clear_branch_cache(self, db: Session, branch: ConflictWatchBranch) -> None:
        db.query(ConflictWatchBranchFileIgnore).filter(
            ConflictWatchBranchFileIgnore.branch_id == branch.id,
        ).delete()
        db.query(ConflictWatchBranchFile).filter(
            ConflictWatchBranchFile.branch_id == branch.id,
        ).delete()
        db.flush()

    def _mark_branch_history_inactive(self, db: Session, branch: ConflictWatchBranch, *, observed_at: datetime, event_id: int | None) -> None:
        branch_commits = db.scalars(
            select(ConflictWatchBranchCommit).where(
                ConflictWatchBranchCommit.repository_id == branch.repository_id,
                ConflictWatchBranchCommit.branch_id == branch.id,
            )
        ).all()
        for branch_commit in branch_commits:
            self._set_branch_commit_state(
                branch_commit,
                is_active=False,
                observed_at=observed_at,
                event_id=event_id,
            )
            self._set_branch_commit_files_state(
                db,
                branch_commit.id,
                is_active=False,
                observed_at=observed_at,
            )
        db.flush()

    def _clear_mainline_branch_tracking(
        self,
        db: Session,
        repository: ConflictWatchRepository,
        branch_name: str,
        *,
        trace: dict[str, Any] | None = None,
    ) -> bool:
        branch = self._get_branch_by_name(db, repository.id, branch_name)
        if branch is None:
            return False
        self._remove_branch(db, branch)
        self._trace_step(
            trace,
            "mainline_branch_tracking_cleared",
            repositoryId=repository.id,
            repositoryName=repository.repository_name,
            branchName=branch_name,
        )
        return True

    def _get_branch_by_name(
        self,
        db: Session,
        repository_id: int,
        branch_name: str,
    ) -> ConflictWatchBranch | None:
        return db.scalar(
            select(ConflictWatchBranch).where(
                ConflictWatchBranch.repository_id == repository_id,
                ConflictWatchBranch.branch_name == branch_name,
            )
        )

    def _get_branch_latest_observed_commit_sha(
        self,
        db: Session,
        branch: ConflictWatchBranch,
    ) -> str | None:
        latest_after_sha = str(branch.latest_after_sha or "").strip()
        if latest_after_sha and latest_after_sha != ZERO_GIT_SHA:
            return latest_after_sha
        latest_commit = db.scalar(
            select(ConflictWatchBranchCommit.commit_sha)
            .where(
                ConflictWatchBranchCommit.repository_id == branch.repository_id,
                ConflictWatchBranchCommit.branch_id == branch.id,
            )
            .order_by(ConflictWatchBranchCommit.sequence_no.desc())
            .limit(1)
        )
        normalized = str(latest_commit or "").strip()
        return normalized or None

    def _build_merged_resolution_context(
        self,
        *,
        branch_names: list[str],
        detected_by: str,
        mainline_branch: str,
        delivery_id: str | None = None,
        after_sha: str | None = None,
    ) -> dict[str, Any]:
        ordered_branch_names = sorted({name for name in branch_names if name})
        context_kwargs: dict[str, Any] = {
            "branchName": ordered_branch_names[0] if len(ordered_branch_names) == 1 else None,
            "branchNames": ordered_branch_names or None,
            "detectedBy": detected_by,
            "mainlineBranch": mainline_branch,
            "deliveryId": delivery_id,
            "afterSha": after_sha,
        }
        return self._build_resolution_context("merged_to_main_or_master", **context_kwargs)

    def _mark_branch_merged_to_main_or_master(
        self,
        db: Session,
        branch: ConflictWatchBranch,
        *,
        detected_at: datetime,
        detected_by: str,
        trace: dict[str, Any] | None = None,
    ) -> bool:
        if branch.monitoring_closed_reason == "merged_to_main_or_master":
            self._trace_step(
                trace,
                "branch_merge_already_marked",
                branchName=branch.branch_name,
                detectedBy=branch.merged_detected_by,
            )
            return False
        branch.is_monitored = False
        branch.is_deleted = False
        branch.possibly_inconsistent = False
        branch.monitoring_closed_reason = "merged_to_main_or_master"
        branch.monitoring_closed_at = detected_at
        branch.merged_detected_by = detected_by
        branch.updated_at = self.now()
        self._trace_step(
            trace,
            "branch_marked_merged",
            branchName=branch.branch_name,
            detectedBy=detected_by,
            mergedAt=self._iso(detected_at),
        )
        db.flush()
        return True

    def _resolve_conflicts_for_merged_branches(
        self,
        db: Session,
        branches: list[ConflictWatchBranch],
        *,
        resolution_context: dict[str, Any],
    ) -> int:
        branch_ids = {branch.id for branch in branches}
        if not branch_ids:
            return 0
        now = self.now()
        branch_by_id = {branch.id: branch for branch in branches}
        conflicts = db.scalars(
            select(ConflictWatchConflict)
            .options(selectinload(ConflictWatchConflict.conflict_branches))
            .where(ConflictWatchConflict.status.in_(["warning", "notice"]))
        ).all()
        resolved_count = 0
        for conflict in conflicts:
            if not any(link.branch_id in branch_ids for link in conflict.conflict_branches):
                continue
            entries: list[tuple[ConflictWatchBranch, ConflictWatchBranchFile]] = []
            for link in conflict.conflict_branches:
                branch = branch_by_id.get(link.branch_id) or db.get(ConflictWatchBranch, link.branch_id)
                if branch is None:
                    continue
                branch_file = db.scalar(
                    select(ConflictWatchBranchFile).where(
                        ConflictWatchBranchFile.repository_id == conflict.repository_id,
                        ConflictWatchBranchFile.branch_id == branch.id,
                        ConflictWatchBranchFile.normalized_file_path == conflict.normalized_file_path,
                    )
                )
                if branch_file is None:
                    continue
                entries.append((branch, branch_file))
            if entries:
                conflict.last_related_branches = self._snapshot_conflict_branches(entries)
            detail = self._coerce_resolution_context("merged_to_main_or_master", resolution_context)
            conflict.status = "resolved"
            conflict.resolved_at = now
            conflict.resolved_reason = detail["reason"]
            conflict.resolved_context = detail
            conflict.updated_at = now
            conflict.confidence = "low"
            self._push_history(conflict, "resolved", str(detail["summary"]), now)
            self._update_conflict_links(db, conflict, [])
            resolved_count += 1
        db.flush()
        return resolved_count

    def _detect_branches_merged_by_mainline_push(
        self,
        db: Session,
        repository: ConflictWatchRepository,
        *,
        mainline_branch_name: str,
        payload: dict[str, Any],
        trace: dict[str, Any] | None = None,
    ) -> list[ConflictWatchBranch]:
        commit_sha_candidates: set[str] = set()
        after_sha = str(payload.get("after") or "").strip()
        if after_sha and after_sha != ZERO_GIT_SHA:
            commit_sha_candidates.add(after_sha)
        head_commit = payload.get("head_commit")
        if isinstance(head_commit, dict):
            head_sha = str(head_commit.get("id") or "").strip()
            if head_sha:
                commit_sha_candidates.add(head_sha)
        for commit in payload.get("commits", []) or []:
            if not isinstance(commit, dict):
                continue
            commit_sha = str(commit.get("id") or "").strip()
            if commit_sha:
                commit_sha_candidates.add(commit_sha)

        candidate_branches = db.scalars(
            select(ConflictWatchBranch).where(
                ConflictWatchBranch.repository_id == repository.id,
            )
        ).all()
        self._trace_step(
            trace,
            "mainline_push_merge_detection_started",
            mainlineBranch=mainline_branch_name,
            candidateBranchCount=len(candidate_branches),
            commitCandidateCount=len(commit_sha_candidates),
        )
        if not commit_sha_candidates:
            self._trace_step(
                trace,
                "mainline_push_merge_detection_completed",
                mergedBranchNames=[],
                scannedBranchCount=0,
                reason="no_commit_candidates",
            )
            return []

        merged_branches: list[ConflictWatchBranch] = []
        scanned_count = 0
        for branch in candidate_branches:
            if branch.is_deleted or not branch.is_monitored:
                continue
            if self._is_mainline_branch_name(branch.branch_name):
                continue
            latest_commit_sha = self._get_branch_latest_observed_commit_sha(db, branch)
            scanned_count += 1
            if not latest_commit_sha or latest_commit_sha not in commit_sha_candidates:
                continue
            self._trace_step(
                trace,
                "mainline_push_merge_candidate_matched",
                branchName=branch.branch_name,
                matchedCommitSha=latest_commit_sha,
                mainlineBranch=mainline_branch_name,
            )
            merged_branches.append(branch)

        self._trace_step(
            trace,
            "mainline_push_merge_detection_completed",
            mergedBranchNames=[branch.branch_name for branch in merged_branches],
            scannedBranchCount=scanned_count,
        )
        return merged_branches

    def _compute_conflict_confidence(self, branches: list[ConflictWatchBranch]) -> str:
        if not branches:
            return "low"
        if any(branch.confidence == "low" for branch in branches):
            return "low"
        if any(branch.confidence == "medium" for branch in branches):
            return "medium"
        return "high"

    def _snapshot_conflict_branches(
        self,
        entries: list[tuple[ConflictWatchBranch, ConflictWatchBranchFile]],
    ) -> list[dict[str, Any]]:
        snapshots: list[dict[str, Any]] = []
        for branch, branch_file in entries:
            snapshots.append({
                "branchId": branch.id,
                "branchName": branch.branch_name,
                "status": branch.status,
                "lastPushAt": self._iso(branch.last_push_at),
                "lastSeenAt": self._iso(branch.last_seen_at),
                "changeType": branch_file.change_type,
                "previousPath": branch_file.previous_path,
            })
        return snapshots

    def _build_resolution_context(self, reason: str | None, **kwargs: Any) -> dict[str, Any]:
        resolved_reason = reason or "other_observed_resolution"
        context: dict[str, Any] = {"reason": resolved_reason}
        for key, value in kwargs.items():
            if value in (None, "", [], {}):
                continue
            context[key] = value
        context["summary"] = self._format_resolution_summary(context)
        return context

    def _coerce_resolution_context(
        self,
        reason: str | None,
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not context:
            return self._build_resolution_context(reason)
        if "reason" not in context and "summary" not in context:
            return self._build_resolution_context(reason, **context)

        detail: dict[str, Any] = {}
        for key, value in context.items():
            if value in (None, "", [], {}):
                continue
            detail[key] = value
        detail["reason"] = str(detail.get("reason") or reason or "other_observed_resolution")
        detail["summary"] = self._format_resolution_summary(detail)
        return detail

    def _format_branch_names(self, entries: list[tuple[ConflictWatchBranch, ConflictWatchBranchFile]]) -> str:
        names = sorted({branch.branch_name for branch, _ in entries})
        return ", ".join(names)

    def _format_resolution_summary(self, context: dict[str, Any]) -> str:
        reason = str(context.get("reason") or "other_observed_resolution")
        branch_name = context.get("branchName")
        branch_names = context.get("branchNames") or []
        normalized_file_path = context.get("normalizedFilePath")
        pattern = context.get("pattern")
        delivery_id = context.get("deliveryId")
        after_sha = context.get("afterSha")
        detected_by = context.get("detectedBy")
        mainline_branch = context.get("mainlineBranch")

        if reason == "webhook_branch_deleted":
            details = [f"branch: {branch_name}"] if branch_name else []
            if delivery_id:
                details.append(f"delivery_id: {delivery_id}")
            return f"Webhook で branch 削除が来て解消 ({', '.join(details)})" if details else "Webhook で branch 削除が来て解消"
        if reason == "webhook_observed_resolution":
            details = [f"branch: {branch_name}"] if branch_name else []
            if delivery_id:
                details.append(f"delivery_id: {delivery_id}")
            if after_sha:
                details.append(f"after: {after_sha}")
            return f"push 再計算で解消 ({', '.join(details)})" if details else "push 再計算で解消"
        if reason == "branch_excluded":
            return f"branch を除外して解消 (branch: {branch_name})" if branch_name else "branch を除外して解消"
        if reason == "branch_included":
            return f"除外解除後の再計算で解消 (branch: {branch_name})" if branch_name else "除外解除後の再計算で解消"
        if reason == "merged_to_main_or_master":
            details: list[str] = []
            if branch_name:
                details.append(f"branch: {branch_name}")
            elif isinstance(branch_names, list) and branch_names:
                details.append(f"branches: {', '.join(str(item) for item in branch_names)}")
            if mainline_branch:
                details.append(f"mainline: {mainline_branch}")
            if detected_by:
                details.append(f"detected_by: {detected_by}")
            return f"main/master へマージされて解消 ({', '.join(details)})" if details else "main/master へマージされて解消"
        if reason == "branch_deleted":
            return f"手動で branch 削除して解消 (branch: {branch_name})" if branch_name else "手動で branch 削除して解消"
        if reason == "manual_reset":
            return f"手動リセットで解消 (branch: {branch_name})" if branch_name else "手動リセットで解消"
        if reason == "branch_file_ignored":
            details = [f"branch: {branch_name}"] if branch_name else []
            if normalized_file_path:
                details.append(f"file: {normalized_file_path}")
            return f"branch-file ignore で解消 ({', '.join(details)})" if details else "branch-file ignore で解消"
        if reason == "branch_file_ignore_removed":
            details = [f"branch: {branch_name}"] if branch_name else []
            if normalized_file_path:
                details.append(f"file: {normalized_file_path}")
            return f"ignore 解除後の再計算で解消 ({', '.join(details)})" if details else "ignore 解除後の再計算で解消"
        if reason == "ignore_rule_added":
            return f"repository ignore rule 追加で解消 (pattern: {pattern})" if pattern else "repository ignore rule 追加で解消"
        if reason == "ignore_rule_enabled":
            return f"repository ignore rule 有効化で解消 (pattern: {pattern})" if pattern else "repository ignore rule 有効化で解消"
        if reason == "ignore_rule_disabled":
            return f"repository ignore rule 無効化後の再計算で解消 (pattern: {pattern})" if pattern else "repository ignore rule 無効化後の再計算で解消"
        if reason == "manual_resolved":
            return "手動で resolved に変更"
        return "上記に当てはまらない再計算の fallback"

    def _webhook_resolution_context(self, event: ConflictWatchWebhookEvent) -> tuple[str, dict[str, Any]]:
        reason = "webhook_branch_deleted" if event.is_deleted is True else "webhook_observed_resolution"
        context = self._build_resolution_context(
            reason,
            providerType=event.provider_type,
            deliveryId=event.delivery_id,
            branchName=event.branch_name,
            afterSha=event.after_sha,
            pusher=event.pusher,
        )
        return reason, context

    def _append_notification(
        self,
        db: Session,
        settings_row: ConflictWatchSetting,
        conflict: ConflictWatchConflict,
        notification_type: str,
        sent_at: datetime,
        status_value: str = "sent",
        error_message: str | None = None,
    ) -> None:
        notification = ConflictWatchNotification(
            conflict_id=conflict.id,
            notification_type=notification_type,
            destination_type="slack",
            destination_value=settings_row.notification_destination,
            sent_at=sent_at,
            status=status_value,
            error_message=error_message,
        )
        db.add(notification)
        db.flush()
        send_status, send_error = self._send_slack_notification(db, settings_row, conflict, notification)
        notification.status = send_status
        notification.error_message = send_error

    def _build_notification_text(
        self,
        db: Session,
        settings_row: ConflictWatchSetting,
        conflict: ConflictWatchConflict,
        notification: ConflictWatchNotification,
    ) -> str:
        repository = db.get(ConflictWatchRepository, conflict.repository_id)
        branches = db.scalars(
            select(ConflictWatchBranch)
            .join(ConflictWatchConflictBranch, ConflictWatchConflictBranch.branch_id == ConflictWatchBranch.id)
            .where(ConflictWatchConflictBranch.conflict_id == conflict.id)
            .order_by(ConflictWatchBranch.branch_name.asc())
        ).all()
        branch_files = db.scalars(
            select(ConflictWatchBranchFile).where(
                ConflictWatchBranchFile.repository_id == conflict.repository_id,
                ConflictWatchBranchFile.normalized_file_path == conflict.normalized_file_path,
            )
        ).all()
        branch_file_map = {branch_file.branch_id: branch_file for branch_file in branch_files}
        branch_lines = []
        for branch in branches:
            branch_file = branch_file_map.get(branch.id)
            note = []
            if branch_file:
                note.append(f"change_type={branch_file.change_type}")
            if branch.possibly_inconsistent and settings_row.force_push_note_enabled:
                note.append("possibly_inconsistent=true")
            branch_lines.append(f"- {branch.branch_name} ({', '.join(note) if note else 'observed'})")
        return "\n".join(
            [
                f"[Conflict Watch] {notification.notification_type}",
                f"repository: {repository.repository_name if repository else conflict.repository_id}",
                f"conflict_key: {conflict.conflict_key}",
                f"path: {conflict.normalized_file_path}",
                f"status: {conflict.status}",
                f"first_detected_at: {self._iso(conflict.first_detected_at)}",
                f"last_detected_at: {self._iso(conflict.last_detected_at)}",
                f"reopened_at: {self._iso(conflict.reopened_at)}",
                f"memo: {conflict.memo or '-'}",
                "branches:",
                *branch_lines,
                f"detail_url: {self.settings.site_url.rstrip('/')}/tools/conflict-watch",
            ]
        )

    def _send_slack_notification(
        self,
        db: Session,
        settings_row: ConflictWatchSetting,
        conflict: ConflictWatchConflict,
        notification: ConflictWatchNotification,
    ) -> tuple[str, str | None]:
        self._build_notification_text(db, settings_row, conflict, notification)
        return "sent", None

    def _update_conflict_links(self, db: Session, conflict: ConflictWatchConflict, branch_ids: list[int]) -> None:
        existing = {link.branch_id: link for link in conflict.conflict_branches}
        for branch_id in list(existing):
            if branch_id not in branch_ids:
                db.delete(existing[branch_id])
        for branch_id in branch_ids:
            if branch_id in existing:
                existing[branch_id].updated_at = self.now()
                continue
            db.add(ConflictWatchConflictBranch(conflict_id=conflict.id, branch_id=branch_id))

    def _refresh_raw_payload_retention(self, db: Session, settings_row: ConflictWatchSetting) -> None:
        now = self.now()
        expire_before = now - timedelta(days=settings_row.raw_payload_retention_days)
        expired_events = db.scalars(
            select(ConflictWatchWebhookEvent).where(
                ConflictWatchWebhookEvent.received_at < expire_before,
            )
        ).all()
        for event in expired_events:
            removed_any = False
            if event.raw_payload_ref:
                payload_path = self.settings.base_dir / event.raw_payload_ref
                if payload_path.exists():
                    payload_path.unlink()
                    removed_any = True
            event.raw_payload_ref = None
            trace_path = self._processing_trace_path(event.provider_type, event.delivery_id)
            if trace_path.exists():
                trace_path.unlink()
                removed_any = True
            if removed_any:
                event.raw_payload_expired_at = now

    def _reconcile_all(
        self,
        db: Session,
        resolution_reason: str | None = None,
        resolution_context: dict[str, Any] | None = None,
        suppress_notifications: bool = False,
        trace: dict[str, Any] | None = None,
        trace_repository_id: int | None = None,
    ) -> None:
        settings_row = self._get_or_create_settings(db)
        self._refresh_raw_payload_retention(db, settings_row)
        now = self.now()

        branches = db.scalars(select(ConflictWatchBranch).options(selectinload(ConflictWatchBranch.branch_files))).all()
        for branch in branches:
            branch.status = self._compute_branch_status(branch, settings_row.stale_days, now)
            branch.confidence = self._compute_branch_confidence(branch, settings_row.stale_days, now)

        repositories = db.scalars(
            select(ConflictWatchRepository).options(
                selectinload(ConflictWatchRepository.branches).selectinload(ConflictWatchBranch.branch_files),
                selectinload(ConflictWatchRepository.ignore_rules),
                selectinload(ConflictWatchRepository.conflicts).selectinload(ConflictWatchConflict.conflict_branches),
                selectinload(ConflictWatchRepository.conflicts).selectinload(ConflictWatchConflict.notifications),
            )
        ).all()
        branch_file_ignores = db.scalars(select(ConflictWatchBranchFileIgnore)).all()
        branch_file_ignore_lookup = self._branch_file_ignore_lookup(branch_file_ignores)

        for repository in repositories:
            rules = self._repository_rules(repository)
            active_groups: dict[str, list[tuple[ConflictWatchBranch, ConflictWatchBranchFile]]] = {}
            inconsistent_paths: set[str] = set()
            for branch in repository.branches:
                if branch.is_deleted or not branch.is_monitored or branch.is_branch_excluded:
                    continue
                if branch.possibly_inconsistent:
                    for branch_file in branch.branch_files:
                        inconsistent_paths.add(branch_file.normalized_file_path)
                    continue
                for branch_file in branch.branch_files:
                    if self._is_ignored_file(branch_file.normalized_file_path, rules):
                        continue
                    if (branch.id, branch_file.normalized_file_path) in branch_file_ignore_lookup:
                        continue
                    active_groups.setdefault(branch_file.normalized_file_path, []).append((branch, branch_file))

            existing_map = {conflict.conflict_key: conflict for conflict in repository.conflicts}
            processed_keys: set[str] = set()

            for normalized_file_path, entries in active_groups.items():
                branch_ids = sorted({branch.id for branch, _ in entries})
                conflict_key = self._make_conflict_key(repository.id, normalized_file_path)
                if len(branch_ids) < 2:
                    continue
                processed_keys.add(conflict_key)
                existing = existing_map.get(conflict_key)
                active_branches = sorted({branch.id: branch for branch, _ in entries}.values(), key=lambda branch: branch.id)
                previous_count = len(existing.conflict_branches) if existing else 0
                previous_status = existing.status if existing else None
                conflict = existing or ConflictWatchConflict(
                    repository_id=repository.id,
                    conflict_key=conflict_key,
                    normalized_file_path=normalized_file_path,
                    status="warning",
                    memo="",
                    first_detected_at=now,
                    last_detected_at=now,
                    resolved_at=None,
                    reopened_at=None,
                    ignored_at=None,
                    resolved_reason=None,
                    resolved_context=None,
                    confidence="medium",
                    last_long_unresolved_bucket=0,
                    last_related_branches=[],
                    history=[],
                    created_at=now,
                    updated_at=now,
                )
                if existing is None:
                    db.add(conflict)
                    db.flush()
                    self._push_history(
                        conflict,
                        "warning",
                        f"新しい競合を検知 (related branches: {self._format_branch_names(entries)})",
                        now,
                    )
                    if not suppress_notifications:
                        self._append_notification(db, settings_row, conflict, "conflict_created", now)
                elif conflict.status == "resolved":
                    conflict.status = "warning"
                    conflict.reopened_at = now
                    self._push_history(
                        conflict,
                        "warning",
                        f"resolved 済み conflict が再発 (related branches: {self._format_branch_names(entries)})",
                        now,
                    )
                    if not suppress_notifications:
                        self._append_notification(db, settings_row, conflict, "conflict_reopened", now)

                conflict.last_detected_at = now
                conflict.updated_at = now
                conflict.resolved_at = None
                conflict.resolved_reason = None
                conflict.resolved_context = None
                conflict.confidence = self._compute_conflict_confidence(active_branches)
                if normalized_file_path in inconsistent_paths:
                    conflict.confidence = "low"
                conflict.last_related_branches = self._snapshot_conflict_branches(entries)
                self._update_conflict_links(db, conflict, branch_ids)

                if existing is not None:
                    if len(branch_ids) > previous_count and conflict.status != "conflict_ignored" and not suppress_notifications:
                        self._append_notification(db, settings_row, conflict, "conflict_scope_expanded", now)
                    if previous_status != conflict.status:
                        if not (settings_row.suppress_notice_notifications and conflict.status == "notice") and not suppress_notifications:
                            self._append_notification(db, settings_row, conflict, "conflict_status_changed", now)

                threshold = max(settings_row.long_unresolved_days, 1)
                age_days = (now - conflict.first_detected_at).days
                current_bucket = 0
                if conflict.status in {"warning", "notice"}:
                    current_bucket = age_days // threshold
                if current_bucket > (conflict.last_long_unresolved_bucket or 0) and current_bucket >= 1 and not suppress_notifications:
                    self._append_notification(db, settings_row, conflict, "long_unresolved", now)
                conflict.last_long_unresolved_bucket = max(conflict.last_long_unresolved_bucket or 0, current_bucket)

            for conflict in repository.conflicts:
                if conflict.conflict_key in processed_keys:
                    continue
                if conflict.normalized_file_path in inconsistent_paths:
                    if conflict.status == "warning":
                        conflict.status = "notice"
                        self._push_history(
                            conflict,
                            "notice",
                            "possibly_inconsistent branch を除外して判定保留",
                            now,
                        )
                    conflict.updated_at = now
                    conflict.confidence = "low"
                    continue
                if conflict.status in {"warning", "notice"}:
                    detail = self._coerce_resolution_context(resolution_reason, resolution_context)
                    conflict.status = "resolved"
                    conflict.resolved_at = now
                    conflict.resolved_reason = detail["reason"]
                    conflict.resolved_context = detail
                    conflict.updated_at = now
                    conflict.confidence = "low"
                    self._push_history(
                        conflict,
                        "resolved",
                        str(detail["summary"]),
                        now,
                    )
                self._update_conflict_links(db, conflict, [])

            if trace and (trace_repository_id is None or repository.id == trace_repository_id):
                trace_conflicts = self._trace_repository_conflicts(db, repository.id)
                self._trace_step(
                    trace,
                    "reconcile_repository_completed",
                    repositoryId=repository.id,
                    repositoryName=repository.repository_name,
                    branchCount=len(repository.branches),
                    activeGroupCount=len(active_groups),
                    activeGroupPaths=sorted(active_groups.keys()),
                    inconsistentPathCount=len(inconsistent_paths),
                    inconsistentPaths=sorted(inconsistent_paths),
                    conflictCount=len(trace_conflicts),
                    conflicts=trace_conflicts,
                )

        db.flush()

    def _serialize_repository(self, repository: ConflictWatchRepository) -> dict[str, Any]:
        return {
            "id": repository.id,
            "providerType": repository.provider_type,
            "externalRepoId": repository.external_repo_id,
            "repositoryName": repository.repository_name,
            "isActive": repository.is_active,
            "githubWebhookSecret": repository.github_webhook_secret or "",
            "backlogWebhookSecret": repository.backlog_webhook_secret or "",
            "createdAt": self._iso(repository.created_at),
            "updatedAt": self._iso(repository.updated_at),
        }

    def _serialize_branch(self, branch: ConflictWatchBranch) -> dict[str, Any]:
        return {
            "id": branch.id,
            "repositoryId": branch.repository_id,
            "branchName": branch.branch_name,
            "isMonitored": branch.is_monitored,
            "status": branch.status,
            "lastPushAt": self._iso(branch.last_push_at),
            "latestAfterSha": branch.latest_after_sha,
            "lastSeenAt": self._iso(branch.last_seen_at),
            "isDeleted": branch.is_deleted,
            "isBranchExcluded": branch.is_branch_excluded,
            "possiblyInconsistent": branch.possibly_inconsistent,
            "confidence": branch.confidence,
            "memo": branch.memo or "",
            "monitoringClosedReason": branch.monitoring_closed_reason,
            "monitoringClosedAt": self._iso(branch.monitoring_closed_at),
            "mergedDetectedBy": branch.merged_detected_by,
            "createdAt": self._iso(branch.created_at),
            "updatedAt": self._iso(branch.updated_at),
        }

    def _serialize_branch_file(self, branch_file: ConflictWatchBranchFile) -> dict[str, Any]:
        data = {
            "id": branch_file.id,
            "repositoryId": branch_file.repository_id,
            "branchId": branch_file.branch_id,
            "filePath": branch_file.file_path,
            "normalizedFilePath": branch_file.normalized_file_path,
            "changeType": branch_file.change_type,
            "firstSeenAt": self._iso(branch_file.first_seen_at),
            "lastSeenAt": self._iso(branch_file.last_seen_at),
            "updatedAt": self._iso(branch_file.updated_at),
        }
        if branch_file.previous_path:
            data["previousPath"] = branch_file.previous_path
        return data

    def _serialize_branch_commit(self, branch_commit: ConflictWatchBranchCommit) -> dict[str, Any]:
        return {
            "id": branch_commit.id,
            "repositoryId": branch_commit.repository_id,
            "branchId": branch_commit.branch_id,
            "commitSha": branch_commit.commit_sha,
            "sequenceNo": branch_commit.sequence_no,
            "observedViaEventId": branch_commit.observed_via_event_id,
            "observedAt": self._iso(branch_commit.observed_at),
            "isActive": branch_commit.is_active,
            "firstSeenAt": self._iso(branch_commit.first_seen_at),
            "lastSeenAt": self._iso(branch_commit.last_seen_at),
            "createdAt": self._iso(branch_commit.created_at),
            "updatedAt": self._iso(branch_commit.updated_at),
        }

    def _serialize_branch_commit_file(self, commit_file: ConflictWatchBranchCommitFile) -> dict[str, Any]:
        data = {
            "id": commit_file.id,
            "repositoryId": commit_file.repository_id,
            "branchId": commit_file.branch_id,
            "branchCommitId": commit_file.branch_commit_id,
            "commitSha": commit_file.commit_sha,
            "filePath": commit_file.file_path,
            "normalizedFilePath": commit_file.normalized_file_path,
            "changeType": commit_file.change_type,
            "observedAt": self._iso(commit_file.observed_at),
            "isActive": commit_file.is_active,
            "createdAt": self._iso(commit_file.created_at),
            "updatedAt": self._iso(commit_file.updated_at),
        }
        if commit_file.previous_path:
            data["previousPath"] = commit_file.previous_path
        return data

    def _serialize_branch_file_ignore(self, branch_file_ignore: ConflictWatchBranchFileIgnore) -> dict[str, Any]:
        return {
            "id": branch_file_ignore.id,
            "repositoryId": branch_file_ignore.repository_id,
            "branchId": branch_file_ignore.branch_id,
            "branchFileId": branch_file_ignore.branch_file_id,
            "normalizedFilePath": branch_file_ignore.normalized_file_path,
            "memo": branch_file_ignore.memo or "",
            "isActive": branch_file_ignore.is_active,
            "createdAt": self._iso(branch_file_ignore.created_at),
            "updatedAt": self._iso(branch_file_ignore.updated_at),
        }

    def _serialize_conflict(self, conflict: ConflictWatchConflict, branch_files_by_conflict: dict[tuple[int, str], list[ConflictWatchBranchFile]]) -> dict[str, Any]:
        branch_entries = []
        active_branch_ids = [link.branch_id for link in conflict.conflict_branches]
        for branch_id in active_branch_ids:
            match = next(
                (
                    branch_file
                    for branch_file in branch_files_by_conflict.get((conflict.repository_id, conflict.normalized_file_path), [])
                    if branch_file.branch_id == branch_id
                ),
                None,
            )
            branch_entries.append(
                {
                    "branchId": branch_id,
                    "changeType": match.change_type if match else None,
                    "previousPath": match.previous_path if match else None,
                    "lastSeenAt": self._iso(match.last_seen_at) if match else None,
                }
            )

        return {
            "id": conflict.id,
            "repositoryId": conflict.repository_id,
            "conflictKey": conflict.conflict_key,
            "normalizedFilePath": conflict.normalized_file_path,
            "status": conflict.status,
            "memo": conflict.memo or "",
            "firstDetectedAt": self._iso(conflict.first_detected_at),
            "lastDetectedAt": self._iso(conflict.last_detected_at),
            "resolvedAt": self._iso(conflict.resolved_at),
            "reopenedAt": self._iso(conflict.reopened_at),
            "ignoredAt": self._iso(conflict.ignored_at),
            "resolvedReason": conflict.resolved_reason,
            "resolvedContext": conflict.resolved_context,
            "confidence": conflict.confidence,
            "lastLongUnresolvedBucket": conflict.last_long_unresolved_bucket or 0,
            "lastRelatedBranches": conflict.last_related_branches or [],
            "createdAt": self._iso(conflict.created_at),
            "updatedAt": self._iso(conflict.updated_at),
            "history": conflict.history or [],
            "activeBranchIds": active_branch_ids,
            "branchEntries": branch_entries,
        }

    def _serialize_notification(
        self,
        notification: ConflictWatchNotification,
        conflict_key: str | None,
        conflict: ConflictWatchConflict | None,
    ) -> dict[str, Any]:
        return {
            "id": notification.id,
            "conflictId": notification.conflict_id,
            "repositoryId": conflict.repository_id if conflict else None,
            "conflictKey": conflict_key,
            "normalizedFilePath": conflict.normalized_file_path if conflict else None,
            "notificationType": notification.notification_type,
            "destinationType": notification.destination_type,
            "destinationValue": notification.destination_value,
            "sentAt": self._iso(notification.sent_at),
            "status": notification.status,
            "errorMessage": notification.error_message,
        }

    def _serialize_webhook_event(self, event: ConflictWatchWebhookEvent) -> dict[str, Any]:
        return {
            "id": event.id,
            "repositoryId": event.repository_id,
            "providerType": event.provider_type,
            "deliveryId": event.delivery_id,
            "eventType": event.event_type,
            "repositoryExternalId": event.repository_external_id,
            "repositoryName": event.repository_name,
            "branchName": event.branch_name,
            "beforeSha": event.before_sha,
            "afterSha": event.after_sha,
            "receivedAt": self._iso(event.received_at),
            "processedAt": self._iso(event.processed_at),
            "processStatus": event.process_status,
            "payloadHash": event.payload_hash,
            "rawPayloadRef": event.raw_payload_ref,
            "rawPayloadExpiredAt": self._iso(event.raw_payload_expired_at),
            "errorMessage": event.error_message,
            "pusher": event.pusher,
            "pushedAt": self._iso(event.pushed_at),
            "isDeleted": event.is_deleted,
            "isForced": event.is_forced,
            "filesAdded": list(event.files_added or []),
            "filesModified": list(event.files_modified or []),
            "filesRemoved": list(event.files_removed or []),
            "filesRenamed": list(event.files_renamed or []),
        }

    def _serialize_security_log(self, security_log: ConflictWatchSecurityLog) -> dict[str, Any]:
        return {
            "id": security_log.id,
            "providerType": security_log.provider_type,
            "deliveryId": security_log.delivery_id,
            "repositoryExternalId": security_log.repository_external_id,
            "branchName": security_log.branch_name,
            "receivedAt": self._iso(security_log.received_at),
            "statusCode": security_log.status_code,
            "reason": security_log.reason,
        }

    def _serialize_ignore_rule(self, rule: ConflictWatchIgnoreRule) -> dict[str, Any]:
        return {
            "id": rule.id,
            "repositoryId": rule.repository_id,
            "ruleType": rule.rule_type,
            "pattern": rule.pattern,
            "isActive": rule.is_active,
            "createdAt": self._iso(rule.created_at),
            "updatedAt": self._iso(rule.updated_at),
        }

    def _serialize_settings(self, settings_row: ConflictWatchSetting) -> dict[str, Any]:
        return {
            "staleDays": settings_row.stale_days,
            "longUnresolvedDays": settings_row.long_unresolved_days,
            "rawPayloadRetentionDays": settings_row.raw_payload_retention_days,
            "processingTraceEnabled": settings_row.processing_trace_enabled,
            "forcePushNoteEnabled": settings_row.force_push_note_enabled,
            "suppressNoticeNotifications": settings_row.suppress_notice_notifications,
            "notificationDestination": settings_row.notification_destination,
            "slackWebhookUrl": settings_row.slack_webhook_url or "",
            "githubWebhookEndpoint": settings_row.github_webhook_endpoint,
            "backlogWebhookEndpoint": settings_row.backlog_webhook_endpoint,
            "githubWebhookSecret": settings_row.github_webhook_secret,
            "backlogWebhookSecret": settings_row.backlog_webhook_secret,
        }

    def get_state(self, db: Session) -> dict[str, Any]:
        settings_row = self._get_or_create_settings(db)
        self._reconcile_all(db, suppress_notifications=True)
        db.commit()

        repositories = db.scalars(
            select(ConflictWatchRepository).options(
                selectinload(ConflictWatchRepository.ignore_rules),
            )
        ).all()
        branches = db.scalars(select(ConflictWatchBranch)).all()
        branch_commits = db.scalars(select(ConflictWatchBranchCommit)).all()
        branch_commit_files = db.scalars(select(ConflictWatchBranchCommitFile)).all()
        branch_files = db.scalars(select(ConflictWatchBranchFile)).all()
        branch_file_ignores = db.scalars(select(ConflictWatchBranchFileIgnore)).all()
        conflicts = db.scalars(
            select(ConflictWatchConflict).options(
                selectinload(ConflictWatchConflict.conflict_branches),
                selectinload(ConflictWatchConflict.notifications),
            )
        ).all()
        notifications = db.scalars(select(ConflictWatchNotification).order_by(ConflictWatchNotification.sent_at.desc())).all()
        webhook_events = db.scalars(
            select(ConflictWatchWebhookEvent).order_by(ConflictWatchWebhookEvent.received_at.desc())
        ).all()
        security_logs = db.scalars(
            select(ConflictWatchSecurityLog).order_by(ConflictWatchSecurityLog.received_at.desc())
        ).all()
        ignore_rules = db.scalars(select(ConflictWatchIgnoreRule)).all()

        branch_files_by_conflict: dict[tuple[int, str], list[ConflictWatchBranchFile]] = {}
        for branch_file in branch_files:
            branch_files_by_conflict.setdefault(
                (branch_file.repository_id, branch_file.normalized_file_path),
                [],
            ).append(branch_file)

        conflict_key_by_id = {conflict.id: conflict.conflict_key for conflict in conflicts}
        conflict_by_id = {conflict.id: conflict for conflict in conflicts}

        return {
            "repositories": [self._serialize_repository(repository) for repository in repositories],
            "branches": sorted(
                [self._serialize_branch(branch) for branch in branches if not branch.is_deleted],
                key=lambda branch: (-BRANCH_STATUS_ORDER.get(branch["status"], 0), branch["branchName"]),
            ),
            "branchCommits": [self._serialize_branch_commit(item) for item in branch_commits],
            "branchCommitFiles": [self._serialize_branch_commit_file(item) for item in branch_commit_files],
            "branchFiles": [self._serialize_branch_file(branch_file) for branch_file in branch_files],
            "branchFileIgnores": [self._serialize_branch_file_ignore(item) for item in branch_file_ignores],
            "conflicts": [self._serialize_conflict(conflict, branch_files_by_conflict) for conflict in conflicts],
            "notifications": [
                self._serialize_notification(
                    notification,
                    conflict_key_by_id.get(notification.conflict_id),
                    conflict_by_id.get(notification.conflict_id),
                )
                for notification in notifications
            ],
            "webhookEvents": [self._serialize_webhook_event(event) for event in webhook_events],
            "securityLogs": [self._serialize_security_log(log) for log in security_logs],
            "ignoreRules": [self._serialize_ignore_rule(rule) for rule in ignore_rules],
            "settings": self._serialize_settings(settings_row),
            "now": self._iso(self.now()),
        }

    def list_repositories(self, db: Session) -> list[dict[str, Any]]:
        return self.get_state(db)["repositories"]

    def list_branches(self, db: Session, repository_id: int | None = None) -> list[dict[str, Any]]:
        branches = self.get_state(db)["branches"]
        if repository_id is None:
            return branches
        return [branch for branch in branches if branch["repositoryId"] == repository_id]

    def get_branch_detail(self, db: Session, branch_id: int) -> dict[str, Any]:
        state = self.get_state(db)
        branch = next((item for item in state["branches"] if item["id"] == branch_id), None)
        if branch is None:
            raise HTTPException(status_code=404, detail="Branch not found")
        branch_files = [item for item in state["branchFiles"] if item["branchId"] == branch_id]
        related_events = [item for item in state["webhookEvents"] if item["branchName"] == branch["branchName"] and item["repositoryId"] == branch["repositoryId"]]
        return {
            "branch": branch,
            "branchFiles": branch_files,
            "webhookEvents": related_events,
        }

    def list_conflicts(self, db: Session, repository_id: int | None = None, resolved_only: bool = False) -> list[dict[str, Any]]:
        conflicts = self.get_state(db)["conflicts"]
        if repository_id is not None:
            conflicts = [conflict for conflict in conflicts if conflict["repositoryId"] == repository_id]
        if resolved_only:
            conflicts = [conflict for conflict in conflicts if conflict["status"] == "resolved"]
        return conflicts

    def get_conflict_detail(self, db: Session, conflict_id: int) -> dict[str, Any]:
        state = self.get_state(db)
        conflict = next((item for item in state["conflicts"] if item["id"] == conflict_id), None)
        if conflict is None:
            raise HTTPException(status_code=404, detail="Conflict not found")
        active_branch_ids = set(conflict.get("activeBranchIds", []))
        branches = [branch for branch in state["branches"] if branch["id"] in active_branch_ids]
        notifications = [item for item in state["notifications"] if item["conflictId"] == conflict_id]
        return {
            "conflict": conflict,
            "branches": branches,
            "notifications": notifications,
        }

    def add_repository(self, db: Session, provider_type: str, repository_name: str, external_repo_id: str) -> ServiceMessage:
        now = self.now()
        existing = db.scalar(
            select(ConflictWatchRepository).where(
                ConflictWatchRepository.provider_type == provider_type,
                ConflictWatchRepository.external_repo_id == external_repo_id,
            )
        )
        if existing:
            raise HTTPException(status_code=400, detail="同じ provider_type / external_repo_id の repository が既に存在します。")
        repository = ConflictWatchRepository(
            provider_type=provider_type,
            repository_name=repository_name,
            external_repo_id=external_repo_id,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        db.add(repository)
        db.flush()
        self._clone_default_ignore_rules(db, repository, now)
        db.commit()
        return ServiceMessage(f"repository を追加しました: {repository_name}")

    def toggle_repository_active(self, db: Session, repository_id: int) -> ServiceMessage:
        repository = self._get_repository(db, repository_id)
        repository.is_active = not repository.is_active
        repository.updated_at = self.now()
        db.commit()
        state = "有効" if repository.is_active else "無効"
        return ServiceMessage(f"{repository.repository_name} を {state} にしました。")

    def update_repository_webhook_secrets(
        self,
        db: Session,
        repository_id: int,
        payload: dict[str, Any],
    ) -> ServiceMessage:
        repository = self._get_repository(db, repository_id)
        repository.github_webhook_secret = (
            str(payload.get("githubWebhookSecret", repository.github_webhook_secret or "")).strip() or None
        )
        repository.backlog_webhook_secret = (
            str(payload.get("backlogWebhookSecret", repository.backlog_webhook_secret or "")).strip() or None
        )
        repository.updated_at = self.now()
        db.commit()
        return ServiceMessage(f"{repository.repository_name} の webhook secret を更新しました。")

    def update_settings(self, db: Session, payload: dict[str, Any]) -> ServiceMessage:
        settings_row = self._get_or_create_settings(db)
        settings_row.stale_days = int(payload.get("staleDays", settings_row.stale_days))
        settings_row.long_unresolved_days = int(payload.get("longUnresolvedDays", settings_row.long_unresolved_days))
        settings_row.raw_payload_retention_days = int(payload.get("rawPayloadRetentionDays", settings_row.raw_payload_retention_days))
        settings_row.processing_trace_enabled = bool(payload.get("processingTraceEnabled", settings_row.processing_trace_enabled))
        settings_row.force_push_note_enabled = bool(payload.get("forcePushNoteEnabled", settings_row.force_push_note_enabled))
        settings_row.suppress_notice_notifications = bool(payload.get("suppressNoticeNotifications", settings_row.suppress_notice_notifications))
        settings_row.notification_destination = str(payload.get("notificationDestination", settings_row.notification_destination)).strip() or settings_row.notification_destination
        settings_row.slack_webhook_url = str(payload.get("slackWebhookUrl", settings_row.slack_webhook_url or "")).strip() or None
        settings_row.github_webhook_endpoint = str(payload.get("githubWebhookEndpoint", settings_row.github_webhook_endpoint)).strip() or settings_row.github_webhook_endpoint
        settings_row.backlog_webhook_endpoint = str(payload.get("backlogWebhookEndpoint", settings_row.backlog_webhook_endpoint)).strip() or settings_row.backlog_webhook_endpoint
        settings_row.github_webhook_secret = str(payload.get("githubWebhookSecret", settings_row.github_webhook_secret)).strip() or settings_row.github_webhook_secret
        settings_row.backlog_webhook_secret = str(payload.get("backlogWebhookSecret", settings_row.backlog_webhook_secret)).strip() or settings_row.backlog_webhook_secret
        db.flush()
        self._reconcile_all(db)
        db.commit()
        return ServiceMessage("設定を更新しました。")

    def add_ignore_rule(self, db: Session, repository_id: int, pattern: str) -> ServiceMessage:
        repository = self._get_repository(db, repository_id)
        normalized_pattern = self.normalize_path(pattern)
        if not normalized_pattern:
            raise HTTPException(status_code=400, detail="ignore rule に追加する pattern を入力してください。")
        rule = ConflictWatchIgnoreRule(
            repository_id=repository.id,
            rule_type="path_pattern",
            pattern=normalized_pattern,
            is_active=True,
            created_at=self.now(),
            updated_at=self.now(),
        )
        db.add(rule)
        db.flush()
        self._reconcile_all(
            db,
            resolution_reason="ignore_rule_added",
            resolution_context=self._build_resolution_context("ignore_rule_added", pattern=normalized_pattern),
        )
        db.commit()
        return ServiceMessage(f"ignore rule を追加しました: {normalized_pattern}")

    def toggle_ignore_rule(self, db: Session, rule_id: int) -> ServiceMessage:
        rule = db.get(ConflictWatchIgnoreRule, rule_id)
        if not rule:
            raise HTTPException(status_code=404, detail="Ignore rule not found")
        rule.is_active = not rule.is_active
        rule.updated_at = self.now()
        reason = "ignore_rule_enabled" if rule.is_active else "ignore_rule_disabled"
        self._reconcile_all(
            db,
            resolution_reason=reason,
            resolution_context=self._build_resolution_context(reason, pattern=rule.pattern),
        )
        db.commit()
        return ServiceMessage(f"ignore rule を {'有効' if rule.is_active else '無効'} にしました。")

    def update_branch_memo(self, db: Session, branch_id: int, memo: str) -> ServiceMessage:
        branch = db.get(ConflictWatchBranch, branch_id)
        if not branch:
            raise HTTPException(status_code=404, detail="Branch not found")
        branch.memo = memo.strip()
        branch.updated_at = self.now()
        db.commit()
        return ServiceMessage(f"{branch.branch_name} の memo を更新しました。")

    def add_branch_file_ignore(
        self,
        db: Session,
        branch_id: int,
        normalized_file_path: str,
        memo: str,
    ) -> ServiceMessage:
        branch = db.get(ConflictWatchBranch, branch_id)
        if not branch:
            raise HTTPException(status_code=404, detail="Branch not found")
        normalized = self.normalize_path(normalized_file_path)
        if not normalized:
            raise HTTPException(status_code=400, detail="ignore 対象の file path を入力してください。")
        branch_file = db.scalar(
            select(ConflictWatchBranchFile).where(
                ConflictWatchBranchFile.branch_id == branch.id,
                ConflictWatchBranchFile.normalized_file_path == normalized,
            )
        )
        if not branch_file:
            raise HTTPException(status_code=404, detail="Branch file not found")
        ignore_entry = db.scalar(
            select(ConflictWatchBranchFileIgnore).where(
                ConflictWatchBranchFileIgnore.branch_id == branch.id,
                ConflictWatchBranchFileIgnore.normalized_file_path == normalized,
            )
        )
        now = self.now()
        if ignore_entry:
            ignore_entry.branch_file_id = branch_file.id
            ignore_entry.memo = memo.strip()
            ignore_entry.is_active = True
            ignore_entry.updated_at = now
        else:
            ignore_entry = ConflictWatchBranchFileIgnore(
                repository_id=branch.repository_id,
                branch_id=branch.id,
                branch_file_id=branch_file.id,
                normalized_file_path=normalized,
                memo=memo.strip(),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            db.add(ignore_entry)
        self._reconcile_all(
            db,
            resolution_reason="branch_file_ignored",
            resolution_context=self._build_resolution_context(
                "branch_file_ignored",
                branchName=branch.branch_name,
                normalizedFilePath=normalized,
                memo=memo.strip(),
            ),
        )
        db.commit()
        return ServiceMessage(f"{branch.branch_name} の {normalized} を ignore 登録しました。")

    def _get_branch_file_ignore(
        self,
        db: Session,
        branch_id: int,
        normalized_file_path: str,
    ) -> ConflictWatchBranchFileIgnore:
        normalized = self.normalize_path(normalized_file_path)
        ignore_entry = db.scalar(
            select(ConflictWatchBranchFileIgnore).where(
                ConflictWatchBranchFileIgnore.branch_id == branch_id,
                ConflictWatchBranchFileIgnore.normalized_file_path == normalized,
            )
        )
        if not ignore_entry:
            raise HTTPException(status_code=404, detail="Branch file ignore not found")
        return ignore_entry

    def toggle_branch_file_ignore(
        self,
        db: Session,
        ignore_id: int,
    ) -> ServiceMessage:
        ignore_entry = db.get(ConflictWatchBranchFileIgnore, ignore_id)
        if not ignore_entry:
            raise HTTPException(status_code=404, detail="Branch file ignore not found")
        branch = db.get(ConflictWatchBranch, ignore_entry.branch_id)
        ignore_entry.is_active = not ignore_entry.is_active
        ignore_entry.updated_at = self.now()
        reason = "branch_file_ignore_removed" if not ignore_entry.is_active else "branch_file_ignored"
        self._reconcile_all(
            db,
            resolution_reason=reason,
            resolution_context=self._build_resolution_context(
                reason,
                branchName=branch.branch_name if branch else None,
                normalizedFilePath=ignore_entry.normalized_file_path,
                memo=ignore_entry.memo or "",
            ),
        )
        db.commit()
        action = "取り消しました" if not ignore_entry.is_active else "再度有効化しました"
        return ServiceMessage(f"{ignore_entry.normalized_file_path} の ignore を {action}。")

    def remove_branch_file_ignore(
        self,
        db: Session,
        branch_id: int,
        normalized_file_path: str,
    ) -> ServiceMessage:
        ignore_entry = self._get_branch_file_ignore(db, branch_id, normalized_file_path)
        if not ignore_entry.is_active:
            return ServiceMessage(f"{ignore_entry.normalized_file_path} の ignore は既に解除されています。", tone="info")
        ignore_entry.is_active = False
        ignore_entry.updated_at = self.now()
        branch = db.get(ConflictWatchBranch, ignore_entry.branch_id)
        self._reconcile_all(
            db,
            resolution_reason="branch_file_ignore_removed",
            resolution_context=self._build_resolution_context(
                "branch_file_ignore_removed",
                branchName=branch.branch_name if branch else None,
                normalizedFilePath=ignore_entry.normalized_file_path,
                memo=ignore_entry.memo or "",
            ),
        )
        db.commit()
        return ServiceMessage(f"{ignore_entry.normalized_file_path} の ignore を取り消しました。")

    def update_branch_file_ignore_memo(
        self,
        db: Session,
        branch_id: int,
        normalized_file_path: str,
        memo: str,
    ) -> ServiceMessage:
        ignore_entry = self._get_branch_file_ignore(db, branch_id, normalized_file_path)
        ignore_entry.memo = memo.strip()
        ignore_entry.updated_at = self.now()
        db.commit()
        return ServiceMessage(f"{ignore_entry.normalized_file_path} の ignore メモを更新しました。")

    def apply_branch_action(self, db: Session, branch_id: int, action: str) -> ServiceMessage:
        branch = db.get(ConflictWatchBranch, branch_id)
        if not branch:
            raise HTTPException(status_code=404, detail="Branch not found")
        now = self.now()
        if action == "toggle-excluded":
            branch.is_branch_excluded = not branch.is_branch_excluded
            branch.updated_at = now
            reason = "branch_excluded" if branch.is_branch_excluded else "branch_included"
            self._reconcile_all(
                db,
                resolution_reason=reason,
                resolution_context=self._build_resolution_context(reason, branchName=branch.branch_name),
            )
            db.commit()
            return ServiceMessage(f"{branch.branch_name} を {'branch_excluded' if branch.is_branch_excluded else '監視対象'} にしました。")
        if action == "merge":
            changed = self._mark_branch_merged_to_main_or_master(
                db,
                branch,
                detected_at=now,
                detected_by="manual",
            )
            if not changed:
                db.commit()
                return ServiceMessage(f"{branch.branch_name} は既に main/master 取込済みです。", tone="info")
            resolution_context = self._build_merged_resolution_context(
                branch_names=[branch.branch_name],
                detected_by="manual",
                mainline_branch="main/master",
            )
            self._resolve_conflicts_for_merged_branches(
                db,
                [branch],
                resolution_context=resolution_context,
            )
            self._reconcile_all(
                db,
                resolution_reason="other_observed_resolution",
                resolution_context=self._build_resolution_context("other_observed_resolution"),
            )
            db.commit()
            return ServiceMessage(f"{branch.branch_name} を main/master マージ扱いでクローズしました。")
        if action == "delete":
            branch_name = branch.branch_name
            self._remove_branch(db, branch)
            self._reconcile_all(
                db,
                resolution_reason="branch_deleted",
                resolution_context=self._build_resolution_context("branch_deleted", branchName=branch_name),
            )
            db.commit()
            return ServiceMessage(f"{branch_name} を一覧から削除しました。")
        if action == "reset":
            branch.possibly_inconsistent = False
            branch.latest_after_sha = None
            branch.last_seen_at = None
            branch.updated_at = now
            self._clear_branch_cache(db, branch)
            db.query(ConflictWatchBranchCommitFile).filter(
                ConflictWatchBranchCommitFile.branch_id == branch.id,
            ).delete()
            db.query(ConflictWatchBranchCommit).filter(
                ConflictWatchBranchCommit.branch_id == branch.id,
            ).delete()
            self._reconcile_all(
                db,
                resolution_reason="manual_reset",
                resolution_context=self._build_resolution_context("manual_reset", branchName=branch.branch_name),
            )
            db.commit()
            return ServiceMessage(f"{branch.branch_name} の branch_files を手動リセットしました。")
        raise HTTPException(status_code=400, detail="Unknown branch action")

    def update_conflict_memo(self, db: Session, conflict_id: int, memo: str) -> ServiceMessage:
        conflict = db.get(ConflictWatchConflict, conflict_id)
        if not conflict:
            raise HTTPException(status_code=404, detail="Conflict not found")
        conflict.memo = memo.strip()
        conflict.updated_at = self.now()
        db.commit()
        return ServiceMessage("conflict memo を更新しました。")

    def update_conflict_status(self, db: Session, conflict_id: int, next_status: str) -> ServiceMessage:
        conflict = db.get(ConflictWatchConflict, conflict_id)
        if not conflict:
            raise HTTPException(status_code=404, detail="Conflict not found")
        if next_status == "resolved" and len(conflict.conflict_branches) >= 2:
            raise HTTPException(
                status_code=400,
                detail="resolved は監視対象 branch が 2 未満になったときだけ確定します。branch 側の削除や除外で監視対象を減らしてください。",
            )
        now = self.now()
        conflict.status = next_status
        conflict.updated_at = now
        if next_status == "conflict_ignored":
            conflict.ignored_at = now
        if next_status == "resolved":
            detail = self._build_resolution_context("manual_resolved")
            conflict.resolved_at = now
            conflict.resolved_reason = detail["reason"]
            conflict.resolved_context = detail
            history_note = str(detail["summary"])
        else:
            conflict.resolved_at = None
            conflict.resolved_reason = None
            conflict.resolved_context = None
            history_note = f"手動で {next_status} へ変更"
        self._push_history(conflict, next_status, history_note, now)
        self._reconcile_all(
            db,
            resolution_reason="manual_resolved" if next_status == "resolved" else None,
            resolution_context=self._build_resolution_context("manual_resolved") if next_status == "resolved" else None,
        )
        db.commit()
        return ServiceMessage(f"conflict status を {next_status} へ更新しました。")

    def delete_conflict(self, db: Session, conflict_id: int) -> ServiceMessage:
        conflict = db.get(ConflictWatchConflict, conflict_id)
        if not conflict:
            raise HTTPException(status_code=404, detail="Conflict not found")
        if conflict.status != "resolved":
            raise HTTPException(status_code=400, detail="削除できるのは resolved の conflict のみです。")
        normalized_file_path = conflict.normalized_file_path
        db.delete(conflict)
        db.commit()
        db.expunge_all()
        return ServiceMessage(f"{normalized_file_path} の resolved conflict を削除しました。")

    def _store_raw_payload(self, provider_type: str, delivery_id: str, payload_bytes: bytes) -> str:
        filename = f"{provider_type}-{delivery_id}.json"
        payload_path = self.settings.conflict_watch_payloads_dir / filename
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        payload_path.write_bytes(payload_bytes)
        return str(payload_path.relative_to(self.settings.base_dir))

    def _processing_trace_path(self, provider_type: str, delivery_id: str) -> Path:
        filename = f"{provider_type}-{delivery_id}.json"
        return self.settings.conflict_watch_processing_logs_dir / filename

    def _processing_trace_ref(self, provider_type: str, delivery_id: str) -> str:
        return str(self._processing_trace_path(provider_type, delivery_id).relative_to(self.settings.base_dir))

    def _read_raw_payload(self, raw_payload_ref: str | None) -> str | None:
        normalized_ref = str(raw_payload_ref or "").strip()
        if not normalized_ref:
            return None
        base_dir = self.settings.base_dir.resolve()
        payload_path = (base_dir / normalized_ref).resolve()
        try:
            payload_path.relative_to(base_dir)
        except ValueError:
            return None
        if not payload_path.exists() or not payload_path.is_file():
            return None
        return payload_path.read_text(encoding="utf-8", errors="replace")

    def _read_processing_trace(self, provider_type: str, delivery_id: str) -> str | None:
        trace_path = self._processing_trace_path(provider_type, delivery_id)
        if not trace_path.exists() or not trace_path.is_file():
            return None
        return trace_path.read_text(encoding="utf-8", errors="replace")

    def _store_processing_trace(self, provider_type: str, delivery_id: str, trace_payload: dict[str, Any]) -> str:
        trace_path = self._processing_trace_path(provider_type, delivery_id)
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        trace_path.write_text(
            json.dumps(trace_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return str(trace_path.relative_to(self.settings.base_dir))

    def _delete_processing_trace_for_event(self, event: ConflictWatchWebhookEvent) -> None:
        trace_path = self._processing_trace_path(event.provider_type, event.delivery_id)
        if trace_path.exists() and trace_path.is_file():
            trace_path.unlink()

    def _trace_safe_value(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return self._iso(value)
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(key): self._trace_safe_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._trace_safe_value(item) for item in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    def _start_processing_trace(
        self,
        *,
        enabled: bool,
        provider_type: str,
        delivery_id: str,
    ) -> dict[str, Any]:
        return {
            "enabled": enabled,
            "providerType": provider_type,
            "deliveryId": delivery_id,
            "startedAt": self._iso(self.now()),
            "_startedPerfNs": perf_counter_ns(),
            "entries": [],
        }

    def _trace_elapsed_ms(self, trace: dict[str, Any] | None) -> int:
        if not trace:
            return 0
        started_perf_ns = trace.get("_startedPerfNs")
        if not isinstance(started_perf_ns, int):
            return 0
        return max((perf_counter_ns() - started_perf_ns) // 1_000_000, 0)

    def _trace_step(self, trace: dict[str, Any] | None, label: str, **detail: Any) -> None:
        if not trace or not trace.get("enabled"):
            return
        entries = trace.setdefault("entries", [])
        elapsed_ms = self._trace_elapsed_ms(trace)
        entries.append({
            "at": self._iso(self.now()),
            "elapsedMs": elapsed_ms,
            "elapsedSeconds": round(elapsed_ms / 1000, 3),
            "label": label,
            "detail": self._trace_safe_value(detail),
        })

    def _trace_observed_commits(self, observed_commits: list[ObservedCommit]) -> list[dict[str, Any]]:
        return [
            {
                "commitSha": observed_commit.commit_sha,
                "observedAt": self._iso(observed_commit.observed_at),
                "message": observed_commit.message,
                "changes": [
                    {
                        "filePath": change.file_path,
                        "normalizedFilePath": change.normalized_file_path,
                        "changeType": change.change_type,
                        "previousPath": change.previous_path,
                    }
                    for change in observed_commit.changes
                ],
            }
            for observed_commit in observed_commits
        ]

    def _trace_branch_snapshot(self, db: Session, branch: ConflictWatchBranch) -> dict[str, Any]:
        branch_commits = db.scalars(
            select(ConflictWatchBranchCommit)
            .where(
                ConflictWatchBranchCommit.repository_id == branch.repository_id,
                ConflictWatchBranchCommit.branch_id == branch.id,
            )
            .order_by(ConflictWatchBranchCommit.sequence_no.asc())
        ).all()
        branch_files = db.scalars(
            select(ConflictWatchBranchFile)
            .where(
                ConflictWatchBranchFile.repository_id == branch.repository_id,
                ConflictWatchBranchFile.branch_id == branch.id,
            )
            .order_by(ConflictWatchBranchFile.normalized_file_path.asc())
        ).all()
        return {
            "branchId": branch.id,
            "branchName": branch.branch_name,
            "lastPushAt": self._iso(branch.last_push_at),
            "lastSeenAt": self._iso(branch.last_seen_at),
            "latestAfterSha": branch.latest_after_sha,
            "isDeleted": branch.is_deleted,
            "possiblyInconsistent": branch.possibly_inconsistent,
            "monitoringClosedReason": branch.monitoring_closed_reason,
            "monitoringClosedAt": self._iso(branch.monitoring_closed_at),
            "mergedDetectedBy": branch.merged_detected_by,
            "activeCommits": [
                {
                    "commitSha": item.commit_sha,
                    "sequenceNo": item.sequence_no,
                }
                for item in branch_commits
                if item.is_active
            ],
            "inactiveCommits": [
                {
                    "commitSha": item.commit_sha,
                    "sequenceNo": item.sequence_no,
                }
                for item in branch_commits
                if not item.is_active
            ],
            "touchedFiles": [
                {
                    "normalizedFilePath": item.normalized_file_path,
                    "changeType": item.change_type,
                    "previousPath": item.previous_path,
                }
                for item in branch_files
            ],
        }

    def _trace_repository_conflicts(self, db: Session, repository_id: int) -> list[dict[str, Any]]:
        conflicts = db.scalars(
            select(ConflictWatchConflict)
            .where(ConflictWatchConflict.repository_id == repository_id)
            .order_by(ConflictWatchConflict.normalized_file_path.asc())
        ).all()
        return [
            {
                "conflictKey": conflict.conflict_key,
                "normalizedFilePath": conflict.normalized_file_path,
                "status": conflict.status,
                "confidence": conflict.confidence,
                "activeBranchIds": [link.branch_id for link in conflict.conflict_branches],
            }
            for conflict in conflicts
        ]

    def _finalize_processing_trace(
        self,
        event: ConflictWatchWebhookEvent | None,
        trace: dict[str, Any] | None,
        *,
        outcome: str,
        error: str | None = None,
    ) -> None:
        if not trace or not trace.get("enabled") or event is None:
            return
        total_elapsed_ms = self._trace_elapsed_ms(trace)
        trace["finishedAt"] = self._iso(self.now())
        trace["outcome"] = outcome
        trace["totalElapsedMs"] = total_elapsed_ms
        trace["totalElapsedSeconds"] = round(total_elapsed_ms / 1000, 3)
        if error:
            trace["error"] = error
        trace.pop("_startedPerfNs", None)
        trace["processingTraceRef"] = self._store_processing_trace(
            event.provider_type,
            event.delivery_id,
            trace,
        )

    def get_webhook_event_raw_payload(self, db: Session, event_id: int) -> dict[str, Any]:
        event = db.get(ConflictWatchWebhookEvent, event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Webhook event not found")
        content = self._read_raw_payload(event.raw_payload_ref)
        return {
            "eventId": event.id,
            "providerType": event.provider_type,
            "deliveryId": event.delivery_id,
            "rawPayloadRef": event.raw_payload_ref or "",
            "rawPayloadExpiredAt": self._iso(event.raw_payload_expired_at),
            "isAvailable": content is not None,
            "content": content or "",
        }

    def get_webhook_event_processing_trace(self, db: Session, event_id: int) -> dict[str, Any]:
        event = db.get(ConflictWatchWebhookEvent, event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Webhook event not found")
        content = self._read_processing_trace(event.provider_type, event.delivery_id)
        return {
            "eventId": event.id,
            "providerType": event.provider_type,
            "deliveryId": event.delivery_id,
            "processingTraceRef": self._processing_trace_ref(event.provider_type, event.delivery_id),
            "processingTraceExpiredAt": self._iso(event.raw_payload_expired_at),
            "isAvailable": content is not None,
            "content": content or "",
        }

    def _record_security_log(
        self,
        db: Session,
        provider_type: str,
        delivery_id: str,
        repository_external_id: str,
        branch_name: str,
        status_code: int,
        reason: str,
    ) -> None:
        db.add(
            ConflictWatchSecurityLog(
                provider_type=provider_type,
                delivery_id=delivery_id,
                repository_external_id=repository_external_id,
                branch_name=branch_name,
                received_at=self.now(),
                status_code=status_code,
                reason=reason,
            )
        )
        db.commit()

    def _make_payload_hash(self, event: ConflictWatchWebhookEvent) -> str:
        seed = "|".join(
            [
                event.provider_type,
                event.repository_external_id,
                event.branch_name,
                event.after_sha or "",
                ",".join(event.files_added or []),
                ",".join(event.files_modified or []),
                ",".join(event.files_removed or []),
                ",".join(f"{item.get('oldPath')}->{item.get('newPath')}" for item in (event.files_renamed or [])),
            ]
        )
        return f"sha256:{hashlib.sha256(seed.encode('utf-8')).hexdigest()}"

    def _apply_event_to_branches(
        self,
        db: Session,
        event: ConflictWatchWebhookEvent,
        observed_commits: list[ObservedCommit] | None = None,
        trace: dict[str, Any] | None = None,
    ) -> bool:
        repository = db.scalar(select(ConflictWatchRepository).where(ConflictWatchRepository.id == event.repository_id))
        if not repository:
            return False
        if self._is_mainline_branch_name(event.branch_name):
            removed_existing_branch = self._clear_mainline_branch_tracking(
                db,
                repository,
                event.branch_name,
                trace=trace,
            )
            self._trace_step(
                trace,
                "mainline_branch_observed_without_tracking",
                repositoryId=repository.id,
                repositoryName=repository.repository_name,
                branchName=event.branch_name,
                removedExistingBranch=removed_existing_branch,
                isDeleted=event.is_deleted,
                isForced=event.is_forced,
            )
            return True
        now = self.now()
        branch = self._get_or_create_branch(db, repository, event.branch_name, now=now)
        observed_at = event.pushed_at or now
        previous_last_push_at = branch.last_push_at
        previous_monitoring_closed_reason = branch.monitoring_closed_reason
        previous_monitoring_closed_at = branch.monitoring_closed_at
        previous_merged_detected_by = branch.merged_detected_by
        self._trace_step(
            trace,
            "branch_loaded",
            repositoryId=repository.id,
            repositoryName=repository.repository_name,
            branchSnapshot=self._trace_branch_snapshot(db, branch),
        )
        if previous_last_push_at and event.pushed_at and event.pushed_at < previous_last_push_at:
            self._trace_step(
                trace,
                "out_of_order_event_observed",
                branchName=branch.branch_name,
                branchLastPushAt=self._iso(previous_last_push_at),
                eventPushedAt=self._iso(event.pushed_at),
            )
        if event.pushed_at is not None:
            if previous_last_push_at is None or event.pushed_at >= previous_last_push_at:
                branch.last_push_at = event.pushed_at
                branch.latest_after_sha = event.after_sha
        elif previous_last_push_at is None:
            branch.last_push_at = now
            branch.latest_after_sha = event.after_sha
        branch.last_seen_at = now
        branch.updated_at = now
        branch.is_monitored = True
        branch.monitoring_closed_reason = None
        branch.monitoring_closed_at = None
        branch.merged_detected_by = None

        if event.is_deleted is True:
            if previous_monitoring_closed_reason == "merged_to_main_or_master":
                branch.is_monitored = False
                branch.monitoring_closed_reason = previous_monitoring_closed_reason
                branch.monitoring_closed_at = previous_monitoring_closed_at
                branch.merged_detected_by = previous_merged_detected_by
                branch.is_deleted = False
                self._trace_step(
                    trace,
                    "branch_delete_observed_after_merge",
                    branchName=branch.branch_name,
                )
                return True
            branch.is_deleted = True
            branch.possibly_inconsistent = False
            self._trace_step(trace, "branch_delete_detected", branchName=branch.branch_name)
            self._mark_branch_history_inactive(
                db,
                branch,
                observed_at=observed_at,
                event_id=event.id,
            )
            self._clear_branch_cache(db, branch)
            self._trace_step(trace, "branch_deleted_applied", branchSnapshot=self._trace_branch_snapshot(db, branch))
            return True

        branch.is_deleted = False
        observed_commits = observed_commits if observed_commits is not None else self._event_to_single_observed_commit(event)
        self._trace_step(
            trace,
            "observed_commits_ready",
            branchName=branch.branch_name,
            observedCommits=self._trace_observed_commits(observed_commits),
        )

        if event.is_forced:
            self._trace_step(
                trace,
                "force_push_observed_only",
                branchName=branch.branch_name,
                beforeSha=event.before_sha,
                afterSha=event.after_sha,
                observedCommitCount=len(observed_commits),
                touchedFileCount=sum(len(item.changes) for item in observed_commits),
            )
            if observed_commits:
                self._append_observed_commits(
                    db,
                    branch,
                    event,
                    observed_commits,
                    is_active=True,
                )
            branch.possibly_inconsistent = False
            self._apply_observed_touched_files_to_branch_cache(
                db,
                branch,
                observed_commits,
                trace=trace,
            )
            self._trace_step(trace, "branch_update_completed", branchSnapshot=self._trace_branch_snapshot(db, branch))
            return True

        if observed_commits:
            self._append_observed_commits(
                db,
                branch,
                event,
                observed_commits,
                is_active=True,
            )
        branch.possibly_inconsistent = False
        self._apply_observed_touched_files_to_branch_cache(
            db,
            branch,
            observed_commits,
            trace=trace,
        )
        self._trace_step(trace, "normal_push_completed", branchSnapshot=self._trace_branch_snapshot(db, branch))
        return True

    def _create_webhook_event(
        self,
        db: Session,
        *,
        event_type: str,
        provider_type: str,
        repository: ConflictWatchRepository,
        delivery_id: str,
        branch_name: str,
        before_sha: str | None,
        after_sha: str | None,
        pusher: str | None,
        pushed_at: datetime | None,
        is_deleted: bool | None,
        is_forced: bool,
        files_added: list[str],
        files_modified: list[str],
        files_removed: list[str],
        files_renamed: list[dict[str, str]],
        raw_payload_ref: str | None,
    ) -> ConflictWatchWebhookEvent:
        event = ConflictWatchWebhookEvent(
            repository_id=repository.id,
            provider_type=provider_type,
            delivery_id=delivery_id,
            event_type=event_type,
            repository_external_id=repository.external_repo_id,
            repository_name=repository.repository_name,
            branch_name=branch_name,
            before_sha=before_sha,
            after_sha=after_sha,
            received_at=self.now(),
            processed_at=None,
            process_status="queued",
            payload_hash="",
            raw_payload_ref=raw_payload_ref,
            error_message=None,
            pusher=pusher,
            pushed_at=pushed_at,
            is_deleted=is_deleted,
            is_forced=is_forced,
            files_added=files_added,
            files_modified=files_modified,
            files_removed=files_removed,
            files_renamed=files_renamed,
        )
        event.payload_hash = self._make_payload_hash(event)
        db.add(event)
        db.flush()
        return event

    def _find_existing_delivery(
        self,
        db: Session,
        provider_type: str,
        delivery_id: str,
    ) -> ConflictWatchWebhookEvent | None:
        return db.scalar(
            select(ConflictWatchWebhookEvent).where(
                ConflictWatchWebhookEvent.provider_type == provider_type,
                ConflictWatchWebhookEvent.delivery_id == delivery_id,
            )
        )

    def apply_simulated_webhook(
        self,
        db: Session,
        repository_id: int,
        payload: dict[str, Any],
    ) -> ServiceMessage:
        settings_row = self._get_or_create_settings(db)
        repository = self._get_repository(db, repository_id)
        if not repository.is_active:
            raise HTTPException(status_code=400, detail="Webhook を適用する前に active な repository を選択してください。")
        branch_name = str(payload.get("branchName", "")).strip()
        if not branch_name:
            raise HTTPException(status_code=400, detail="Webhook を適用する branch 名を入力してください。")

        provider_type = str(payload.get("provider", repository.provider_type))
        delivery_id = str(payload.get("deliveryId", "")).strip() or f"{provider_type}-delivery-{int(self.now().timestamp() * 1000)}"
        trace = self._start_processing_trace(
            enabled=settings_row.processing_trace_enabled,
            provider_type=provider_type,
            delivery_id=delivery_id,
        )
        self._trace_step(
            trace,
            "simulate_webhook_received",
            repositoryId=repository.id,
            repositoryName=repository.repository_name,
            payload=payload,
        )
        signature_status = str(payload.get("signatureStatus", "valid"))
        if signature_status != "valid":
            self._record_security_log(
                db,
                provider_type,
                delivery_id,
                repository.external_repo_id,
                branch_name,
                401,
                "署名検証に失敗したため queue に積まず破棄",
            )
            raise HTTPException(
                status_code=401,
                detail="署名検証に失敗したため security log に記録し、branch 状態は更新していません。",
            )

        if self._find_existing_delivery(db, provider_type, delivery_id):
            return ServiceMessage(
                f"delivery_id {delivery_id} は既に観測済みです。冪等性により再処理をスキップしました。",
                "info",
            )
        raw_payload = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        raw_payload_ref = self._store_raw_payload(provider_type, delivery_id, raw_payload)

        def parse_list(value: str) -> list[str]:
            return [self.normalize_path(item) for item in str(value or "").splitlines() if self.normalize_path(item)]

        def parse_renamed(value: str) -> list[dict[str, str]]:
            pairs: list[dict[str, str]] = []
            for line in str(value or "").splitlines():
                line = line.strip()
                if not line or "->" not in line:
                    continue
                old_path, new_path = [part.strip() for part in line.split("->", 1)]
                if not old_path or not new_path:
                    continue
                pairs.append({"oldPath": self.normalize_path(old_path), "newPath": self.normalize_path(new_path)})
            return pairs

        deleted_state = str(payload.get("deletedState", "false"))
        is_deleted = None if deleted_state == "unknown" else deleted_state == "true"
        event = self._create_webhook_event(
            db,
            event_type="push",
            provider_type=provider_type,
            repository=repository,
            delivery_id=delivery_id,
            branch_name=branch_name,
            before_sha=f"before-{int(self.now().timestamp())}",
            after_sha=f"after-{int(self.now().timestamp())}",
            pusher=str(payload.get("pusher", "")).strip() or None,
            pushed_at=self.now(),
            is_deleted=is_deleted,
            is_forced=bool(payload.get("isForced", False)),
            files_added=parse_list(payload.get("added", "")),
            files_modified=parse_list(payload.get("modified", "")),
            files_removed=parse_list(payload.get("removed", "")),
            files_renamed=parse_renamed(payload.get("renamed", "")),
            raw_payload_ref=raw_payload_ref,
        )
        self._trace_step(
            trace,
            "webhook_event_created",
            eventId=event.id,
            rawPayloadRef=raw_payload_ref,
            filesAdded=event.files_added,
            filesModified=event.files_modified,
            filesRemoved=event.files_removed,
            filesRenamed=event.files_renamed,
        )

        if bool(payload.get("simulateFailure", False)):
            event.process_status = "processing_failed"
            event.processed_at = self.now()
            event.error_message = "worker が provider 共通形式への正規化中に失敗しました。"
            self._trace_step(trace, "simulated_processing_failure", errorMessage=event.error_message)
            self._finalize_processing_trace(event, trace, outcome="processing_failed")
            db.commit()
            return ServiceMessage(
                "Webhook は登録しましたが、非同期処理で failed にしました。イベント一覧から再処理できます。",
                "warning",
            )

        observed_commits = self._event_to_single_observed_commit(event)
        self._trace_step(trace, "observed_commits_extracted", observedCommits=self._trace_observed_commits(observed_commits))
        self._apply_event_to_branches(
            db,
            event,
            observed_commits,
            trace=trace,
        )
        event.process_status = "processed"
        event.processed_at = self.now()
        resolution_reason, resolution_context = self._webhook_resolution_context(event)
        self._reconcile_all(
            db,
            resolution_reason=resolution_reason,
            resolution_context=resolution_context,
            trace=trace,
            trace_repository_id=repository.id,
        )
        self._finalize_processing_trace(event, trace, outcome="processed")
        db.commit()
        return ServiceMessage(f"{event.branch_name} へ Webhook を適用しました。")

    def reprocess_webhook_event(self, db: Session, event_id: int) -> ServiceMessage:
        event = db.get(ConflictWatchWebhookEvent, event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Webhook event not found")
        if event.process_status != "processing_failed":
            raise HTTPException(status_code=400, detail="reprocess できるのは processing_failed の event のみです。")
        if not event.raw_payload_ref:
            raise HTTPException(status_code=400, detail="raw payload の保持期限が切れているため再処理できません。")
        settings_row = self._get_or_create_settings(db)
        trace = self._start_processing_trace(
            enabled=settings_row.processing_trace_enabled,
            provider_type=event.provider_type,
            delivery_id=event.delivery_id,
        )
        self._trace_step(trace, "reprocess_started", eventId=event.id, previousStatus=event.process_status)
        event.error_message = None
        event.process_status = "processed"
        event.processed_at = self.now()
        event.pushed_at = self.now()
        observed_commits = self._observed_commits_for_reprocess(event)
        self._trace_step(trace, "observed_commits_extracted", observedCommits=self._trace_observed_commits(observed_commits))
        self._apply_event_to_branches(
            db,
            event,
            observed_commits,
            trace=trace,
        )
        resolution_reason, resolution_context = self._webhook_resolution_context(event)
        self._reconcile_all(
            db,
            resolution_reason=resolution_reason,
            resolution_context=resolution_context,
            trace=trace,
            trace_repository_id=event.repository_id,
        )
        self._finalize_processing_trace(event, trace, outcome="reprocessed")
        db.commit()
        return ServiceMessage(f"{event.delivery_id} を raw payload から再処理しました。")

    def _validate_github_signature(self, secret: str, payload_bytes: bytes, provided_signature: str | None) -> bool:
        if not provided_signature or not provided_signature.startswith("sha256="):
            return False
        digest = hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()
        expected = f"sha256={digest}"
        return hmac.compare_digest(expected, provided_signature)

    def _validate_backlog_secret(self, secret: str, provided_secret: str | None) -> bool:
        if not secret:
            return True
        if not provided_secret:
            return False
        return hmac.compare_digest(secret, provided_secret)

    def _effective_repository_webhook_secret(
        self,
        db: Session,
        *,
        provider_type: str,
        repository_external_id: str,
        fallback_secret: str,
    ) -> tuple[str, str, int | None]:
        normalized_repo_id = str(repository_external_id or "").strip()
        if normalized_repo_id:
            repository = db.scalar(
                select(ConflictWatchRepository).where(
                    ConflictWatchRepository.provider_type == provider_type,
                    ConflictWatchRepository.external_repo_id == normalized_repo_id,
                )
            )
            if repository is not None:
                if provider_type == "github":
                    repository_secret = str(repository.github_webhook_secret or "").strip()
                else:
                    repository_secret = str(repository.backlog_webhook_secret or "").strip()
                if repository_secret:
                    return repository_secret, "repository", repository.id
        return str(fallback_secret or ""), "default", None

    def _collect_github_files(self, commits: list[dict[str, Any]], key: str) -> list[str]:
        files: list[str] = []
        for commit in commits:
            for file_path in commit.get(key, []) or []:
                normalized = self.normalize_path(file_path)
                if normalized and normalized not in files:
                    files.append(normalized)
        return files

    def _extract_backlog_renames(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        renamed_items: list[dict[str, str]] = []
        commits = payload.get("commits") or payload.get("changes") or []
        for commit in commits:
            for rename_item in commit.get("renamed", []) or []:
                if not isinstance(rename_item, dict):
                    continue
                old_path = self.normalize_path(rename_item.get("oldPath") or rename_item.get("from") or "")
                new_path = self.normalize_path(rename_item.get("newPath") or rename_item.get("to") or "")
                if not old_path or not new_path:
                    continue
                candidate = {"oldPath": old_path, "newPath": new_path}
                if candidate not in renamed_items:
                    renamed_items.append(candidate)
        return renamed_items

    def _extract_backlog_commit_observations(
        self,
        payload: dict[str, Any],
        *,
        delivery_id: str,
        pushed_at: datetime,
    ) -> list[ObservedCommit]:
        observed: list[ObservedCommit] = []
        commits = payload.get("commits") or payload.get("changes") or []
        for index, commit in enumerate(commits):
            if not isinstance(commit, dict):
                continue
            commit_sha = (
                str(commit.get("id") or commit.get("rev") or "").strip()
                or f"synthetic:backlog:{delivery_id}:{index}"
            )
            observed.append(
                ObservedCommit(
                    commit_sha=commit_sha,
                    observed_at=pushed_at,
                    changes=self._build_file_changes(
                        added=list(commit.get("added", []) or []),
                        modified=list(commit.get("modified", []) or []),
                        removed=list(commit.get("removed", []) or []),
                        renamed=list(commit.get("renamed", []) or []),
                    ),
                )
            )
        return observed

    def _observed_commits_for_reprocess(self, event: ConflictWatchWebhookEvent) -> list[ObservedCommit]:
        raw_payload = self._read_raw_payload(event.raw_payload_ref)
        if raw_payload and event.provider_type == "github":
            payload = json.loads(raw_payload)
            if isinstance(payload, dict) and "ref" in payload and isinstance(payload.get("repository"), dict):
                return self._extract_github_commit_observations(
                    payload,
                    delivery_id=event.delivery_id,
                    pushed_at=event.pushed_at or self.now(),
                )
        if raw_payload and event.provider_type == "backlog":
            raw = json.loads(raw_payload)
            payload = raw.get("payload") if isinstance(raw, dict) and isinstance(raw.get("payload"), dict) else raw
            if isinstance(payload, dict):
                observed = self._extract_backlog_commit_observations(
                    payload,
                    delivery_id=event.delivery_id,
                    pushed_at=event.pushed_at or self.now(),
                )
                if observed or any(key in payload for key in ("ref", "branch", "refName", "repository", "project")):
                    return observed
        return self._event_to_single_observed_commit(event)

    def _handle_github_pull_request_merged_webhook(
        self,
        db: Session,
        payload: dict[str, Any],
        *,
        delivery_id: str,
        raw_payload_ref: str,
        trace: dict[str, Any] | None,
    ) -> ServiceMessage:
        merge_payload = self._extract_github_pull_request_merge_payload(payload)
        if merge_payload is None:
            self._trace_step(trace, "github_pull_request_ignored", reason="not_merged_to_mainline")
            return ServiceMessage("Ignored non-merged GitHub pull_request event", "info")

        repository = self._ensure_repository(
            db,
            "github",
            str(merge_payload["repositoryExternalId"]),
            str(merge_payload["repositoryName"]),
        )
        self._trace_step(
            trace,
            "pull_request_merge_detected",
            repositoryId=repository.id,
            repositoryName=repository.repository_name,
            branchName=merge_payload["headRef"],
            mainlineBranch=merge_payload["baseRef"],
            headSha=merge_payload["headSha"],
            mergedAt=self._iso(merge_payload["mergedAt"]),
        )
        event: ConflictWatchWebhookEvent | None = None
        try:
            event = self._create_webhook_event(
                db,
                event_type="pull_request",
                provider_type="github",
                repository=repository,
                delivery_id=delivery_id,
                branch_name=str(merge_payload["headRef"]),
                before_sha=merge_payload["baseSha"],
                after_sha=merge_payload["headSha"],
                pusher=merge_payload["mergedBy"],
                pushed_at=merge_payload["mergedAt"],
                is_deleted=None,
                is_forced=False,
                files_added=[],
                files_modified=[],
                files_removed=[],
                files_renamed=[],
                raw_payload_ref=raw_payload_ref,
            )
            self._trace_step(
                trace,
                "webhook_event_created",
                eventId=event.id,
                pushedAt=self._iso(merge_payload["mergedAt"]),
                rawPayloadRef=raw_payload_ref,
                eventType="pull_request",
            )

            branch = self._get_branch_by_name(db, repository.id, str(merge_payload["headRef"]))
            merged_branch_names: list[str] = []
            resolved_conflict_count = 0
            if branch is None:
                self._trace_step(
                    trace,
                    "pull_request_merge_branch_not_found",
                    branchName=merge_payload["headRef"],
                )
            else:
                changed = self._mark_branch_merged_to_main_or_master(
                    db,
                    branch,
                    detected_at=merge_payload["mergedAt"],
                    detected_by="pull_request",
                    trace=trace,
                )
                if changed:
                    merged_branch_names = [branch.branch_name]
                    resolution_context = self._build_merged_resolution_context(
                        branch_names=merged_branch_names,
                        detected_by="pull_request",
                        mainline_branch=str(merge_payload["baseRef"]),
                        delivery_id=delivery_id,
                        after_sha=merge_payload["headSha"],
                    )
                    resolved_conflict_count = self._resolve_conflicts_for_merged_branches(
                        db,
                        [branch],
                        resolution_context=resolution_context,
                    )
            self._trace_step(
                trace,
                "pull_request_merge_completed",
                mergedBranchNames=merged_branch_names,
                resolvedConflictCount=resolved_conflict_count,
            )
            event.process_status = "processed"
            event.processed_at = self.now()
            self._reconcile_all(
                db,
                resolution_reason="other_observed_resolution",
                resolution_context=self._build_resolution_context("other_observed_resolution"),
                trace=trace,
                trace_repository_id=repository.id,
            )
            self._finalize_processing_trace(event, trace, outcome="processed")
            db.commit()
            return ServiceMessage("GitHub pull_request merged event を処理しました。", "success")
        except Exception as exc:
            if event is not None:
                self._trace_step(trace, "processing_exception", error=str(exc))
                self._finalize_processing_trace(event, trace, outcome="failed", error=str(exc))
            raise

    def _apply_mainline_push_merge_detection(
        self,
        db: Session,
        repository: ConflictWatchRepository,
        payload: dict[str, Any],
        *,
        branch_name: str,
        delivery_id: str,
        trace: dict[str, Any] | None,
    ) -> tuple[list[str], int]:
        if not self._is_mainline_branch_name(branch_name):
            return [], 0
        merged_branches = self._detect_branches_merged_by_mainline_push(
            db,
            repository,
            mainline_branch_name=branch_name,
            payload=payload,
            trace=trace,
        )
        merged_branch_names: list[str] = []
        if not merged_branches:
            return merged_branch_names, 0

        merged_at = self._extract_github_pushed_at(payload) or self.now()
        changed_branches: list[ConflictWatchBranch] = []
        for branch in merged_branches:
            changed = self._mark_branch_merged_to_main_or_master(
                db,
                branch,
                detected_at=merged_at,
                detected_by="push_contains_commit",
                trace=trace,
            )
            if changed:
                changed_branches.append(branch)
                merged_branch_names.append(branch.branch_name)
        if not changed_branches:
            return [], 0

        resolution_context = self._build_merged_resolution_context(
            branch_names=merged_branch_names,
            detected_by="push_contains_commit",
            mainline_branch=branch_name,
            delivery_id=delivery_id,
            after_sha=str(payload.get("after") or "").strip() or None,
        )
        resolved_conflict_count = self._resolve_conflicts_for_merged_branches(
            db,
            changed_branches,
            resolution_context=resolution_context,
        )
        self._trace_step(
            trace,
            "mainline_push_merge_applied",
            mainlineBranch=branch_name,
            mergedBranchNames=merged_branch_names,
            resolvedConflictCount=resolved_conflict_count,
        )
        return merged_branch_names, resolved_conflict_count

    def handle_github_webhook(
        self,
        db: Session,
        payload_bytes: bytes,
        *,
        delivery_id: str,
        signature_header: str | None,
        event_type: str,
    ) -> ServiceMessage:
        settings_row = self._get_or_create_settings(db)
        trace = self._start_processing_trace(
            enabled=settings_row.processing_trace_enabled,
            provider_type="github",
            delivery_id=delivery_id,
        )
        self._trace_step(
            trace,
            "webhook_received",
            providerType="github",
            eventType=event_type,
            payloadBytes=len(payload_bytes),
            signatureHeaderPresent=bool(signature_header),
        )
        payload = json.loads(payload_bytes.decode("utf-8"))
        merge_payload = self._extract_github_pull_request_merge_payload(payload) if event_type == "pull_request" else None
        if event_type == "push":
            repository_external_id = str(payload.get("repository", {}).get("full_name", "")).strip()
            repository_name = str(payload.get("repository", {}).get("name", "")).strip() or repository_external_id
            ref = str(payload.get("ref", ""))
            branch_name = ref.replace("refs/heads/", "", 1)
            before_sha = payload.get("before")
            after_sha = payload.get("after")
            forced = bool(payload.get("forced", False))
            deleted = payload.get("deleted")
            commit_count = len(payload.get("commits", []) or [])
            head_commit_id = (payload.get("head_commit") or {}).get("id")
        elif event_type == "pull_request":
            repository_external_id = str(
                (merge_payload or {}).get("repositoryExternalId")
                or (payload.get("repository") or {}).get("full_name")
                or ""
            ).strip()
            repository_name = str(
                (merge_payload or {}).get("repositoryName")
                or (payload.get("repository") or {}).get("name")
                or repository_external_id
            ).strip() or repository_external_id
            branch_name = str(
                (merge_payload or {}).get("headRef")
                or ((payload.get("pull_request") or {}).get("head") or {}).get("ref")
                or "unknown"
            ).strip() or "unknown"
            before_sha = (merge_payload or {}).get("baseSha")
            after_sha = (merge_payload or {}).get("headSha")
            forced = False
            deleted = False
            commit_count = len(payload.get("commits", []) or [])
            head_commit_id = after_sha
        else:
            self._trace_step(trace, "github_event_ignored", reason="unsupported_event_type")
            return ServiceMessage("Unsupported GitHub event", "info")

        self._trace_step(
            trace,
            "payload_parsed",
            branchName=branch_name,
            repositoryExternalId=repository_external_id,
            beforeSha=before_sha,
            afterSha=after_sha,
            forced=forced,
            deleted=deleted,
            commitCount=commit_count,
            headCommitId=head_commit_id,
        )

        effective_secret, secret_scope, secret_repository_id = self._effective_repository_webhook_secret(
            db,
            provider_type="github",
            repository_external_id=repository_external_id,
            fallback_secret=settings_row.github_webhook_secret,
        )
        self._trace_step(
            trace,
            "signature_secret_resolved",
            providerType="github",
            secretScope=secret_scope,
            repositoryId=secret_repository_id,
        )
        if not self._validate_github_signature(effective_secret, payload_bytes, signature_header):
            self._record_security_log(
                db,
                "github",
                delivery_id,
                repository_external_id,
                branch_name,
                401,
                "GitHub 署名検証に失敗したため queue に積まず破棄",
            )
            self._trace_step(trace, "signature_validation_failed", branchName=branch_name)
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

        if self._find_existing_delivery(db, "github", delivery_id):
            self._trace_step(trace, "duplicate_delivery_skipped", branchName=branch_name)
            return ServiceMessage(
                f"delivery_id {delivery_id} は既に処理済みです。冪等性により再処理をスキップしました。",
                "info",
            )
        if event_type == "pull_request":
            raw_payload_ref = self._store_raw_payload("github", delivery_id, payload_bytes)
            return self._handle_github_pull_request_merged_webhook(
                db,
                payload,
                delivery_id=delivery_id,
                raw_payload_ref=raw_payload_ref,
                trace=trace,
            )
        repository = self._ensure_repository(db, "github", repository_external_id, repository_name)
        self._trace_step(trace, "repository_ensured", repositoryId=repository.id, repositoryName=repository.repository_name)
        raw_payload_ref = self._store_raw_payload("github", delivery_id, payload_bytes)
        pushed_at = self._extract_github_pushed_at(payload) or self.now()
        files_added, files_modified, files_removed, files_renamed = self._extract_github_event_file_sets(payload)
        event: ConflictWatchWebhookEvent | None = None
        try:
            event = self._create_webhook_event(
                db,
                event_type="push",
                provider_type="github",
                repository=repository,
                delivery_id=delivery_id,
                branch_name=branch_name,
                before_sha=before_sha,
                after_sha=after_sha,
                pusher=(payload.get("pusher") or {}).get("name"),
                pushed_at=pushed_at,
                is_deleted=deleted,
                is_forced=forced,
                files_added=files_added,
                files_modified=files_modified,
                files_removed=files_removed,
                files_renamed=files_renamed,
                raw_payload_ref=raw_payload_ref,
            )
            self._trace_step(
                trace,
                "webhook_event_created",
                eventId=event.id,
                pushedAt=self._iso(pushed_at),
                rawPayloadRef=raw_payload_ref,
                filesAdded=files_added,
                filesModified=files_modified,
                filesRemoved=files_removed,
                filesRenamed=files_renamed,
            )
            observed_commits = self._extract_github_commit_observations(
                payload,
                delivery_id=delivery_id,
                pushed_at=pushed_at,
            )
            self._trace_step(trace, "observed_commits_extracted", observedCommits=self._trace_observed_commits(observed_commits))
            self._apply_event_to_branches(db, event, observed_commits, trace=trace)
            merged_branch_names, resolved_conflict_count = self._apply_mainline_push_merge_detection(
                db,
                repository,
                payload,
                branch_name=branch_name,
                delivery_id=delivery_id,
                trace=trace,
            )
            event.process_status = "processed"
            event.processed_at = self.now()
            resolution_reason, resolution_context = self._webhook_resolution_context(event)
            self._reconcile_all(
                db,
                resolution_reason=resolution_reason,
                resolution_context=resolution_context,
                trace=trace,
                trace_repository_id=repository.id,
            )
            self._trace_step(
                trace,
                "github_push_processing_completed",
                branchName=branch_name,
                mergedBranchNames=merged_branch_names,
                resolvedConflictCount=resolved_conflict_count,
            )
            self._finalize_processing_trace(event, trace, outcome="processed")
            db.commit()
            return ServiceMessage("GitHub Webhook を処理しました。", "success")
        except Exception as exc:
            if event is not None:
                self._trace_step(trace, "processing_exception", error=str(exc))
                self._finalize_processing_trace(event, trace, outcome="failed", error=str(exc))
            raise

    def _extract_backlog_files(self, payload: dict[str, Any], key: str) -> list[str]:
        files: list[str] = []
        commits = payload.get("commits") or payload.get("changes") or []
        for commit in commits:
            for file_path in commit.get(key, []) or []:
                normalized = self.normalize_path(file_path)
                if normalized and normalized not in files:
                    files.append(normalized)
        return files

    def handle_backlog_webhook(
        self,
        db: Session,
        payload_bytes: bytes,
        *,
        delivery_id: str,
        provided_secret: str | None,
    ) -> ServiceMessage:
        settings_row = self._get_or_create_settings(db)
        trace = self._start_processing_trace(
            enabled=settings_row.processing_trace_enabled,
            provider_type="backlog",
            delivery_id=delivery_id,
        )
        self._trace_step(
            trace,
            "webhook_received",
            providerType="backlog",
            payloadBytes=len(payload_bytes),
            secretProvided=bool(provided_secret),
        )
        raw = json.loads(payload_bytes.decode("utf-8"))
        payload = raw.get("payload") if isinstance(raw, dict) and isinstance(raw.get("payload"), dict) else raw
        branch_ref = str(payload.get("ref") or payload.get("branch") or payload.get("refName") or "").strip()
        branch_name = branch_ref.replace("refs/heads/", "", 1) if branch_ref else "unknown"

        repository_info = payload.get("repository") or {}
        project_info = payload.get("project") or {}
        repository_name = (
            str(repository_info.get("name") or repository_info.get("displayName") or payload.get("repositoryName") or "").strip()
            or "backlog-repository"
        )
        project_key = str(project_info.get("projectKey") or project_info.get("key") or "").strip()
        repository_external_id = f"{project_key}/{repository_name}" if project_key else repository_name
        self._trace_step(
            trace,
            "payload_parsed",
            branchName=branch_name,
            repositoryExternalId=repository_external_id,
            beforeSha=payload.get("before") or payload.get("old"),
            afterSha=payload.get("after") or payload.get("rev"),
            forced=bool(payload.get("forced", False)),
            deleted=payload.get("deleted"),
        )

        effective_secret, secret_scope, secret_repository_id = self._effective_repository_webhook_secret(
            db,
            provider_type="backlog",
            repository_external_id=repository_external_id,
            fallback_secret=settings_row.backlog_webhook_secret,
        )
        self._trace_step(
            trace,
            "secret_scope_resolved",
            providerType="backlog",
            secretScope=secret_scope,
            repositoryId=secret_repository_id,
        )
        if not self._validate_backlog_secret(effective_secret, provided_secret):
            self._record_security_log(
                db,
                "backlog",
                delivery_id,
                repository_external_id,
                branch_name,
                401,
                "Backlog 共有 secret 検証に失敗したため queue に積まず破棄",
            )
            self._trace_step(trace, "secret_validation_failed", branchName=branch_name)
            raise HTTPException(status_code=401, detail="Invalid backlog webhook secret")

        if self._find_existing_delivery(db, "backlog", delivery_id):
            self._trace_step(trace, "duplicate_delivery_skipped", branchName=branch_name)
            return ServiceMessage(
                f"delivery_id {delivery_id} は既に処理済みです。冪等性により再処理をスキップしました。",
                "info",
            )
        repository = self._ensure_repository(db, "backlog", repository_external_id, repository_name)
        self._trace_step(trace, "repository_ensured", repositoryId=repository.id, repositoryName=repository.repository_name)
        raw_payload_ref = self._store_raw_payload("backlog", delivery_id, payload_bytes)
        pushed_at = self.now()
        event: ConflictWatchWebhookEvent | None = None
        files_added = self._extract_backlog_files(payload, "added")
        files_modified = self._extract_backlog_files(payload, "modified")
        files_removed = self._extract_backlog_files(payload, "removed")
        files_renamed = self._extract_backlog_renames(payload)
        try:
            event = self._create_webhook_event(
                db,
                event_type="push",
                provider_type="backlog",
                repository=repository,
                delivery_id=delivery_id,
                branch_name=branch_name,
                before_sha=payload.get("before") or payload.get("old"),
                after_sha=payload.get("after") or payload.get("rev"),
                pusher=(payload.get("pusher") or {}).get("name") or (payload.get("user") or {}).get("name"),
                pushed_at=pushed_at,
                is_deleted=payload.get("deleted"),
                is_forced=bool(payload.get("forced", False)),
                files_added=files_added,
                files_modified=files_modified,
                files_removed=files_removed,
                files_renamed=files_renamed,
                raw_payload_ref=raw_payload_ref,
            )
            self._trace_step(
                trace,
                "webhook_event_created",
                eventId=event.id,
                pushedAt=self._iso(pushed_at),
                rawPayloadRef=raw_payload_ref,
                filesAdded=files_added,
                filesModified=files_modified,
                filesRemoved=files_removed,
                filesRenamed=files_renamed,
            )
            observed_commits = self._extract_backlog_commit_observations(
                payload,
                delivery_id=delivery_id,
                pushed_at=pushed_at,
            )
            self._trace_step(trace, "observed_commits_extracted", observedCommits=self._trace_observed_commits(observed_commits))
            self._apply_event_to_branches(db, event, observed_commits, trace=trace)
            event.process_status = "processed"
            event.processed_at = self.now()
            resolution_reason, resolution_context = self._webhook_resolution_context(event)
            self._reconcile_all(
                db,
                resolution_reason=resolution_reason,
                resolution_context=resolution_context,
                trace=trace,
                trace_repository_id=repository.id,
            )
            self._finalize_processing_trace(event, trace, outcome="processed")
            db.commit()
            return ServiceMessage("Backlog Webhook を処理しました。", "success")
        except Exception as exc:
            if event is not None:
                self._trace_step(trace, "processing_exception", error=str(exc))
                self._finalize_processing_trace(event, trace, outcome="failed", error=str(exc))
            raise
