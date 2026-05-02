from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class BranchFile(Base):
    __tablename__ = "branch_files"
    __table_args__ = (
        UniqueConstraint("branch_id", "path", name="uq_branch_file_path"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    workspace_id: Mapped[str] = mapped_column(String(36), ForeignKey("workspaces.id"), index=True)
    repository_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("repositories.id"), nullable=True, index=True)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id"), index=True)
    path: Mapped[str] = mapped_column(String(2048))
    normalized_path: Mapped[str | None] = mapped_column(String(2048), nullable=True, index=True)
    first_seen_change_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    change_type: Mapped[str] = mapped_column(String(32))
    last_change_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    previous_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    last_seen_commit_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    source_kind: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    is_conflict: Mapped[bool] = mapped_column(default=False, index=True)
    is_ignored_from_conflicts: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    ignored_from_conflicts_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ignored_from_conflicts_by_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
