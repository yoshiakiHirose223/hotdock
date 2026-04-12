from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class ConflictWatchRepository(Base):
    __tablename__ = "cw_repositories"
    __table_args__ = (
        UniqueConstraint("provider_type", "external_repo_id", name="uq_cw_repositories_provider_external"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_type: Mapped[str] = mapped_column(String(50), index=True)
    external_repo_id: Mapped[str] = mapped_column(String(255), index=True)
    repository_name: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    branches: Mapped[list["ConflictWatchBranch"]] = relationship(back_populates="repository")
    conflicts: Mapped[list["ConflictWatchConflict"]] = relationship(back_populates="repository")
    ignore_rules: Mapped[list["ConflictWatchIgnoreRule"]] = relationship(back_populates="repository")
    webhook_events: Mapped[list["ConflictWatchWebhookEvent"]] = relationship(back_populates="repository")


class ConflictWatchBranch(Base):
    __tablename__ = "cw_branches"
    __table_args__ = (
        UniqueConstraint("repository_id", "branch_name", name="uq_cw_branches_repository_branch"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), index=True)
    branch_name: Mapped[str] = mapped_column(String(255), index=True)
    is_monitored: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(50), default="quiet", index=True)
    last_push_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    latest_after_sha: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_branch_excluded: Mapped[bool] = mapped_column(Boolean, default=False)
    possibly_inconsistent: Mapped[bool] = mapped_column(Boolean, default=False)
    confidence: Mapped[str] = mapped_column(String(20), default="medium")
    memo: Mapped[str] = mapped_column(Text, default="")
    monitoring_closed_reason: Mapped[str | None] = mapped_column(String(120), nullable=True)
    monitoring_closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    merged_detected_by: Mapped[str | None] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    repository: Mapped["ConflictWatchRepository"] = relationship(back_populates="branches")
    branch_files: Mapped[list["ConflictWatchBranchFile"]] = relationship(
        back_populates="branch",
        cascade="all, delete-orphan",
    )
    branch_commits: Mapped[list["ConflictWatchBranchCommit"]] = relationship(
        back_populates="branch",
        cascade="all, delete-orphan",
    )
    conflict_links: Mapped[list["ConflictWatchConflictBranch"]] = relationship(
        back_populates="branch",
        cascade="all, delete-orphan",
    )


class ConflictWatchBranchCommit(Base):
    __tablename__ = "cw_branch_commits"
    __table_args__ = (
        UniqueConstraint(
            "repository_id",
            "branch_id",
            "commit_sha",
            name="uq_cw_branch_commits_repo_branch_commit",
        ),
        Index(
            "ix_cw_branch_commits_repo_branch_sequence",
            "repository_id",
            "branch_id",
            "sequence_no",
        ),
        Index("ix_cw_branch_commits_commit_sha", "commit_sha"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), index=True)
    branch_id: Mapped[int] = mapped_column(ForeignKey("cw_branches.id", ondelete="CASCADE"), index=True)
    commit_sha: Mapped[str] = mapped_column(String(255))
    sequence_no: Mapped[int] = mapped_column(index=True)
    observed_via_event_id: Mapped[int | None] = mapped_column(
        ForeignKey("cw_webhook_events.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    branch: Mapped["ConflictWatchBranch"] = relationship(back_populates="branch_commits")
    commit_files: Mapped[list["ConflictWatchBranchCommitFile"]] = relationship(
        back_populates="branch_commit",
        cascade="all, delete-orphan",
    )


class ConflictWatchBranchCommitFile(Base):
    __tablename__ = "cw_branch_commit_files"
    __table_args__ = (
        UniqueConstraint(
            "repository_id",
            "branch_id",
            "commit_sha",
            "normalized_file_path",
            "change_type",
            name="uq_cw_branch_commit_files_repo_branch_commit_path_type",
        ),
        Index("ix_cw_branch_commit_files_normalized_path", "normalized_file_path"),
        Index("ix_cw_branch_commit_files_commit_sha", "commit_sha"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), index=True)
    branch_id: Mapped[int] = mapped_column(ForeignKey("cw_branches.id", ondelete="CASCADE"), index=True)
    branch_commit_id: Mapped[int] = mapped_column(ForeignKey("cw_branch_commits.id", ondelete="CASCADE"), index=True)
    commit_sha: Mapped[str] = mapped_column(String(255))
    file_path: Mapped[str] = mapped_column(String(500))
    normalized_file_path: Mapped[str] = mapped_column(String(500))
    change_type: Mapped[str] = mapped_column(String(30))
    previous_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    branch_commit: Mapped["ConflictWatchBranchCommit"] = relationship(back_populates="commit_files")


class ConflictWatchBranchFile(Base):
    __tablename__ = "cw_branch_files"
    __table_args__ = (
        UniqueConstraint(
            "repository_id",
            "branch_id",
            "normalized_file_path",
            name="uq_cw_branch_files_repo_branch_path",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), index=True)
    branch_id: Mapped[int] = mapped_column(ForeignKey("cw_branches.id", ondelete="CASCADE"), index=True)
    file_path: Mapped[str] = mapped_column(String(500))
    normalized_file_path: Mapped[str] = mapped_column(String(500), index=True)
    change_type: Mapped[str] = mapped_column(String(30))
    previous_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    branch: Mapped["ConflictWatchBranch"] = relationship(back_populates="branch_files")
    branch_file_ignores: Mapped[list["ConflictWatchBranchFileIgnore"]] = relationship(
        back_populates="branch_file",
        cascade="all, delete-orphan",
    )


class ConflictWatchBranchFileIgnore(Base):
    __tablename__ = "cw_branch_file_ignores"
    __table_args__ = (
        UniqueConstraint(
            "repository_id",
            "branch_id",
            "normalized_file_path",
            name="uq_cw_branch_file_ignores_repo_branch_path",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), index=True)
    branch_id: Mapped[int] = mapped_column(ForeignKey("cw_branches.id", ondelete="CASCADE"), index=True)
    branch_file_id: Mapped[int] = mapped_column(ForeignKey("cw_branch_files.id", ondelete="CASCADE"), index=True)
    normalized_file_path: Mapped[str] = mapped_column(String(500), index=True)
    memo: Mapped[str] = mapped_column(Text, default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    branch: Mapped["ConflictWatchBranch"] = relationship()
    branch_file: Mapped["ConflictWatchBranchFile"] = relationship(back_populates="branch_file_ignores")


class ConflictWatchConflict(Base):
    __tablename__ = "cw_conflicts"
    __table_args__ = (
        UniqueConstraint("conflict_key", name="uq_cw_conflicts_conflict_key"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), index=True)
    conflict_key: Mapped[str] = mapped_column(String(700), index=True)
    normalized_file_path: Mapped[str] = mapped_column(String(500), index=True)
    status: Mapped[str] = mapped_column(String(50), default="warning", index=True)
    memo: Mapped[str] = mapped_column(Text, default="")
    first_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reopened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ignored_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_reason: Mapped[str | None] = mapped_column(String(120), nullable=True)
    resolved_context: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    confidence: Mapped[str] = mapped_column(String(20), default="medium")
    last_long_unresolved_bucket: Mapped[int] = mapped_column(default=0)
    last_related_branches: Mapped[list[dict[str, object]]] = mapped_column(JSON, default=list)
    history: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    repository: Mapped["ConflictWatchRepository"] = relationship(back_populates="conflicts")
    conflict_branches: Mapped[list["ConflictWatchConflictBranch"]] = relationship(
        back_populates="conflict",
        cascade="all, delete-orphan",
    )
    notifications: Mapped[list["ConflictWatchNotification"]] = relationship(
        back_populates="conflict",
        cascade="all, delete-orphan",
    )


class ConflictWatchConflictBranch(Base):
    __tablename__ = "cw_conflict_branches"
    __table_args__ = (
        UniqueConstraint("conflict_id", "branch_id", name="uq_cw_conflict_branches_conflict_branch"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    conflict_id: Mapped[int] = mapped_column(ForeignKey("cw_conflicts.id", ondelete="CASCADE"), index=True)
    branch_id: Mapped[int] = mapped_column(ForeignKey("cw_branches.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    conflict: Mapped["ConflictWatchConflict"] = relationship(back_populates="conflict_branches")
    branch: Mapped["ConflictWatchBranch"] = relationship(back_populates="conflict_links")


class ConflictWatchNotification(Base):
    __tablename__ = "cw_notifications"

    id: Mapped[int] = mapped_column(primary_key=True)
    conflict_id: Mapped[int] = mapped_column(ForeignKey("cw_conflicts.id", ondelete="CASCADE"), index=True)
    notification_type: Mapped[str] = mapped_column(String(100))
    destination_type: Mapped[str] = mapped_column(String(50), default="slack")
    destination_value: Mapped[str] = mapped_column(String(255))
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(50), default="sent")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    conflict: Mapped["ConflictWatchConflict"] = relationship(back_populates="notifications")


class ConflictWatchWebhookEvent(Base):
    __tablename__ = "cw_webhook_events"
    __table_args__ = (
        UniqueConstraint("provider_type", "delivery_id", name="uq_cw_webhook_events_provider_delivery"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int | None] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), nullable=True, index=True)
    provider_type: Mapped[str] = mapped_column(String(50), index=True)
    delivery_id: Mapped[str] = mapped_column(String(255), index=True)
    event_type: Mapped[str] = mapped_column(String(50))
    repository_external_id: Mapped[str] = mapped_column(String(255), index=True)
    repository_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    branch_name: Mapped[str] = mapped_column(String(255), index=True)
    before_sha: Mapped[str | None] = mapped_column(String(255), nullable=True)
    after_sha: Mapped[str | None] = mapped_column(String(255), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    process_status: Mapped[str] = mapped_column(String(50), default="queued", index=True)
    payload_hash: Mapped[str] = mapped_column(String(255))
    raw_payload_ref: Mapped[str | None] = mapped_column(String(500), nullable=True)
    raw_payload_expired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    pusher: Mapped[str | None] = mapped_column(String(255), nullable=True)
    pushed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_deleted: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_forced: Mapped[bool] = mapped_column(Boolean, default=False)
    files_added: Mapped[list[str]] = mapped_column(JSON, default=list)
    files_modified: Mapped[list[str]] = mapped_column(JSON, default=list)
    files_removed: Mapped[list[str]] = mapped_column(JSON, default=list)
    files_renamed: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)

    repository: Mapped[ConflictWatchRepository | None] = relationship(back_populates="webhook_events")


class ConflictWatchIgnoreRule(Base):
    __tablename__ = "cw_ignore_rules"

    id: Mapped[int] = mapped_column(primary_key=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("cw_repositories.id", ondelete="CASCADE"), index=True)
    rule_type: Mapped[str] = mapped_column(String(50), default="path_pattern")
    pattern: Mapped[str] = mapped_column(String(500))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    repository: Mapped["ConflictWatchRepository"] = relationship(back_populates="ignore_rules")


class ConflictWatchSecurityLog(Base):
    __tablename__ = "cw_security_logs"

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_type: Mapped[str] = mapped_column(String(50), index=True)
    delivery_id: Mapped[str] = mapped_column(String(255), index=True)
    repository_external_id: Mapped[str] = mapped_column(String(255), index=True)
    branch_name: Mapped[str] = mapped_column(String(255))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status_code: Mapped[int] = mapped_column(default=401)
    reason: Mapped[str] = mapped_column(Text)


class ConflictWatchSetting(Base):
    __tablename__ = "cw_settings"

    id: Mapped[int] = mapped_column(primary_key=True)
    stale_days: Mapped[int] = mapped_column(default=15)
    long_unresolved_days: Mapped[int] = mapped_column(default=7)
    raw_payload_retention_days: Mapped[int] = mapped_column(default=14)
    processing_trace_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    force_push_note_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    suppress_notice_notifications: Mapped[bool] = mapped_column(Boolean, default=False)
    notification_destination: Mapped[str] = mapped_column(String(255), default="#conflict-watch")
    slack_webhook_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    github_webhook_endpoint: Mapped[str] = mapped_column(String(255), default="/tools/conflict-watch/webhooks/github")
    backlog_webhook_endpoint: Mapped[str] = mapped_column(String(255), default="/tools/conflict-watch/webhooks/backlog")
    github_webhook_secret: Mapped[str] = mapped_column(String(255), default="ghs_demo_hotdock")
    backlog_webhook_secret: Mapped[str] = mapped_column(String(255), default="backlog_demo_secret")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
