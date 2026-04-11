from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.tools.conflict_watch_models import (
    ConflictWatchBranch,
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
    "branch_excluded": 1,
}


@dataclass(slots=True)
class ServiceMessage:
    message: str
    tone: str = "success"


class ConflictWatchService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.settings.conflict_watch_payloads_dir.mkdir(parents=True, exist_ok=True)

    def now(self) -> datetime:
        return datetime.now(UTC)

    def _iso(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

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

    def _compute_branch_status(self, branch: ConflictWatchBranch, stale_days: int, now: datetime) -> str:
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
        db.delete(branch)
        db.flush()

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
        normalized_file_path = context.get("normalizedFilePath")
        pattern = context.get("pattern")
        delivery_id = context.get("deliveryId")
        after_sha = context.get("afterSha")

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
            return f"main/master マージ扱いで解消 (branch: {branch_name})" if branch_name else "main/master マージ扱いで解消"
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
                ConflictWatchWebhookEvent.raw_payload_ref.is_not(None),
                ConflictWatchWebhookEvent.received_at < expire_before,
            )
        ).all()
        for event in expired_events:
            if event.raw_payload_ref:
                payload_path = self.settings.base_dir / event.raw_payload_ref
                if payload_path.exists():
                    payload_path.unlink()
            event.raw_payload_ref = None
            event.raw_payload_expired_at = now

    def _reconcile_all(
        self,
        db: Session,
        resolution_reason: str | None = None,
        resolution_context: dict[str, Any] | None = None,
        suppress_notifications: bool = False,
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
            for branch in repository.branches:
                if branch.is_deleted or not branch.is_monitored or branch.is_branch_excluded:
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

        db.flush()

    def _serialize_repository(self, repository: ConflictWatchRepository) -> dict[str, Any]:
        return {
            "id": repository.id,
            "providerType": repository.provider_type,
            "externalRepoId": repository.external_repo_id,
            "repositoryName": repository.repository_name,
            "isActive": repository.is_active,
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

    def update_settings(self, db: Session, payload: dict[str, Any]) -> ServiceMessage:
        settings_row = self._get_or_create_settings(db)
        settings_row.stale_days = int(payload.get("staleDays", settings_row.stale_days))
        settings_row.long_unresolved_days = int(payload.get("longUnresolvedDays", settings_row.long_unresolved_days))
        settings_row.raw_payload_retention_days = int(payload.get("rawPayloadRetentionDays", settings_row.raw_payload_retention_days))
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
            branch.is_monitored = False
            branch.monitoring_closed_reason = "merged_to_main_or_master"
            branch.monitoring_closed_at = now
            branch.updated_at = now
            db.query(ConflictWatchBranchFile).filter(ConflictWatchBranchFile.branch_id == branch.id).delete()
            self._reconcile_all(
                db,
                resolution_reason="merged_to_main_or_master",
                resolution_context=self._build_resolution_context("merged_to_main_or_master", branchName=branch.branch_name),
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
            branch.last_seen_at = None
            branch.updated_at = now
            db.query(ConflictWatchBranchFile).filter(ConflictWatchBranchFile.branch_id == branch.id).delete()
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
        return f"hash-{re.sub(r'[^a-z0-9]+', '-', seed.lower()).strip('-')}"

    def _apply_event_to_branches(self, db: Session, event: ConflictWatchWebhookEvent) -> bool:
        repository = db.scalar(select(ConflictWatchRepository).where(ConflictWatchRepository.id == event.repository_id))
        if not repository:
            return False
        branch = db.scalar(
            select(ConflictWatchBranch).where(
                ConflictWatchBranch.repository_id == repository.id,
                ConflictWatchBranch.branch_name == event.branch_name,
            )
        )
        now = self.now()
        if not branch:
            branch = ConflictWatchBranch(
                repository_id=repository.id,
                branch_name=event.branch_name,
                is_monitored=True,
                status="active",
                last_push_at=event.pushed_at or now,
                latest_after_sha=event.after_sha,
                last_seen_at=now,
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

        branch.last_push_at = event.pushed_at or now
        branch.last_seen_at = now
        branch.latest_after_sha = event.after_sha
        branch.updated_at = now
        branch.is_deleted = False
        branch.is_monitored = True
        branch.monitoring_closed_reason = None
        branch.monitoring_closed_at = None
        if event.is_forced:
            branch.possibly_inconsistent = True

        if event.is_deleted is True:
            self._remove_branch(db, branch)
            return True

        def upsert_file(path: str, change_type: str, previous_path: str | None = None) -> None:
            normalized_file_path = self.normalize_path(path)
            existing = db.scalar(
                select(ConflictWatchBranchFile).where(
                    ConflictWatchBranchFile.repository_id == repository.id,
                    ConflictWatchBranchFile.branch_id == branch.id,
                    ConflictWatchBranchFile.normalized_file_path == normalized_file_path,
                )
            )
            if existing:
                existing.file_path = normalized_file_path
                existing.change_type = change_type
                existing.previous_path = previous_path
                existing.last_seen_at = now
                existing.updated_at = now
                return
            db.add(
                ConflictWatchBranchFile(
                    repository_id=repository.id,
                    branch_id=branch.id,
                    file_path=normalized_file_path,
                    normalized_file_path=normalized_file_path,
                    change_type=change_type,
                    previous_path=previous_path,
                    first_seen_at=now,
                    last_seen_at=now,
                    updated_at=now,
                )
            )

        for file_path in event.files_added or []:
            upsert_file(file_path, "added")
        for file_path in event.files_modified or []:
            upsert_file(file_path, "modified")
        for file_path in event.files_removed or []:
            upsert_file(file_path, "removed")
        for item in event.files_renamed or []:
            upsert_file(item.get("newPath", ""), "renamed", item.get("oldPath"))
        return True

    def _create_webhook_event(
        self,
        db: Session,
        *,
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
            event_type="push",
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
        repository = self._get_repository(db, repository_id)
        if not repository.is_active:
            raise HTTPException(status_code=400, detail="Webhook を適用する前に active な repository を選択してください。")
        branch_name = str(payload.get("branchName", "")).strip()
        if not branch_name:
            raise HTTPException(status_code=400, detail="Webhook を適用する branch 名を入力してください。")

        provider_type = str(payload.get("provider", repository.provider_type))
        delivery_id = str(payload.get("deliveryId", "")).strip() or f"{provider_type}-delivery-{int(self.now().timestamp() * 1000)}"
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

        if bool(payload.get("simulateFailure", False)):
            event.process_status = "processing_failed"
            event.processed_at = self.now()
            event.error_message = "worker が provider 共通形式への正規化中に失敗しました。"
            db.commit()
            return ServiceMessage(
                "Webhook は登録しましたが、非同期処理で failed にしました。イベント一覧から再処理できます。",
                "warning",
            )

        self._apply_event_to_branches(db, event)
        event.process_status = "processed"
        event.processed_at = self.now()
        resolution_reason, resolution_context = self._webhook_resolution_context(event)
        self._reconcile_all(
            db,
            resolution_reason=resolution_reason,
            resolution_context=resolution_context,
        )
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
        event.error_message = None
        event.process_status = "processed"
        event.processed_at = self.now()
        event.pushed_at = self.now()
        self._apply_event_to_branches(db, event)
        resolution_reason, resolution_context = self._webhook_resolution_context(event)
        self._reconcile_all(
            db,
            resolution_reason=resolution_reason,
            resolution_context=resolution_context,
        )
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
        payload = json.loads(payload_bytes.decode("utf-8"))
        if event_type != "push":
            raise HTTPException(status_code=202, detail="Unsupported GitHub event")

        repository_external_id = str(payload.get("repository", {}).get("full_name", "")).strip()
        repository_name = str(payload.get("repository", {}).get("name", "")).strip() or repository_external_id
        ref = str(payload.get("ref", ""))
        branch_name = ref.replace("refs/heads/", "", 1)

        if not self._validate_github_signature(settings_row.github_webhook_secret, payload_bytes, signature_header):
            self._record_security_log(
                db,
                "github",
                delivery_id,
                repository_external_id,
                branch_name,
                401,
                "GitHub 署名検証に失敗したため queue に積まず破棄",
            )
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

        if self._find_existing_delivery(db, "github", delivery_id):
            return ServiceMessage(
                f"delivery_id {delivery_id} は既に処理済みです。冪等性により再処理をスキップしました。",
                "info",
            )
        repository = self._ensure_repository(db, "github", repository_external_id, repository_name)
        raw_payload_ref = self._store_raw_payload("github", delivery_id, payload_bytes)
        commits = payload.get("commits", []) or []
        event = self._create_webhook_event(
            db,
            provider_type="github",
            repository=repository,
            delivery_id=delivery_id,
            branch_name=branch_name,
            before_sha=payload.get("before"),
            after_sha=payload.get("after"),
            pusher=(payload.get("pusher") or {}).get("name"),
            pushed_at=self.now(),
            is_deleted=payload.get("deleted"),
            is_forced=bool(payload.get("forced", False)),
            files_added=self._collect_github_files(commits, "added"),
            files_modified=self._collect_github_files(commits, "modified"),
            files_removed=self._collect_github_files(commits, "removed"),
            files_renamed=[],
            raw_payload_ref=raw_payload_ref,
        )
        self._apply_event_to_branches(db, event)
        event.process_status = "processed"
        event.processed_at = self.now()
        resolution_reason, resolution_context = self._webhook_resolution_context(event)
        self._reconcile_all(
            db,
            resolution_reason=resolution_reason,
            resolution_context=resolution_context,
        )
        db.commit()
        return ServiceMessage("GitHub Webhook を処理しました。", "success")

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

        if not self._validate_backlog_secret(settings_row.backlog_webhook_secret, provided_secret):
            self._record_security_log(
                db,
                "backlog",
                delivery_id,
                repository_external_id,
                branch_name,
                401,
                "Backlog 共有 secret 検証に失敗したため queue に積まず破棄",
            )
            raise HTTPException(status_code=401, detail="Invalid backlog webhook secret")

        if self._find_existing_delivery(db, "backlog", delivery_id):
            return ServiceMessage(
                f"delivery_id {delivery_id} は既に処理済みです。冪等性により再処理をスキップしました。",
                "info",
            )
        repository = self._ensure_repository(db, "backlog", repository_external_id, repository_name)
        raw_payload_ref = self._store_raw_payload("backlog", delivery_id, payload_bytes)
        event = self._create_webhook_event(
            db,
            provider_type="backlog",
            repository=repository,
            delivery_id=delivery_id,
            branch_name=branch_name,
            before_sha=payload.get("before") or payload.get("old"),
            after_sha=payload.get("after") or payload.get("rev"),
            pusher=(payload.get("pusher") or {}).get("name") or (payload.get("user") or {}).get("name"),
            pushed_at=self.now(),
            is_deleted=payload.get("deleted"),
            is_forced=bool(payload.get("forced", False)),
            files_added=self._extract_backlog_files(payload, "added"),
            files_modified=self._extract_backlog_files(payload, "modified"),
            files_removed=self._extract_backlog_files(payload, "removed"),
            files_renamed=self._extract_backlog_renames(payload),
            raw_payload_ref=raw_payload_ref,
        )
        self._apply_event_to_branches(db, event)
        event.process_status = "processed"
        event.processed_at = self.now()
        resolution_reason, resolution_context = self._webhook_resolution_context(event)
        self._reconcile_all(
            db,
            resolution_reason=resolution_reason,
            resolution_context=resolution_context,
        )
        db.commit()
        return ServiceMessage("Backlog Webhook を処理しました。", "success")
