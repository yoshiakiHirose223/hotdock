from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Branch(Base):
    __tablename__ = "branches"
    __table_args__ = (
        UniqueConstraint("repository_id", "name", name="uq_repository_branch_name"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    workspace_id: Mapped[str] = mapped_column(String(36), ForeignKey("workspaces.id"), index=True)
    repository_id: Mapped[str] = mapped_column(String(36), ForeignKey("repositories.id"), index=True)
    name: Mapped[str] = mapped_column(String(255))
    current_head_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_before_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_after_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_push_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    last_commit_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    touched_files_count: Mapped[int] = mapped_column(Integer, default=0)
    conflict_files_count: Mapped[int] = mapped_column(Integer, default=0)
    branch_status: Mapped[str] = mapped_column(String(32), default="normal", index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    was_created_observed: Mapped[bool] = mapped_column(Boolean, default=False)
    was_force_pushed_observed: Mapped[bool] = mapped_column(Boolean, default=False)
    observed_via: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    touch_seed_source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    touch_seeded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    has_webhook_history: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    last_delivery_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_processed_compare_base: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_processed_compare_head: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
