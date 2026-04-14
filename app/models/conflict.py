from datetime import datetime
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Conflict(Base):
    __tablename__ = "conflicts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    workspace_id: Mapped[str] = mapped_column(String(36), ForeignKey("workspaces.id"), index=True)
    repository_id: Mapped[str] = mapped_column(String(36), ForeignKey("repositories.id"), index=True)
    primary_branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id"), index=True)
    secondary_branch_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("branches.id"), nullable=True)
    file_path: Mapped[str] = mapped_column(String(2048))
    conflict_status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    first_detected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_detected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
