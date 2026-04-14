from datetime import datetime
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
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
    last_push_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    last_commit_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    touched_files_count: Mapped[int] = mapped_column(Integer, default=0)
    conflict_files_count: Mapped[int] = mapped_column(Integer, default=0)
    branch_status: Mapped[str] = mapped_column(String(32), default="normal", index=True)
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
