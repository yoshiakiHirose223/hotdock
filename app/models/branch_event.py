from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class BranchEvent(Base):
    __tablename__ = "branch_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    repository_id: Mapped[str] = mapped_column(String(36), ForeignKey("repositories.id"), index=True)
    branch_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("branches.id"), nullable=True, index=True)
    webhook_delivery_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    before_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    after_sha: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created: Mapped[bool] = mapped_column(Boolean, default=False)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    forced: Mapped[bool] = mapped_column(Boolean, default=False)
    compare_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    compare_completed: Mapped[bool] = mapped_column(Boolean, default=False)
    compare_error: Mapped[bool] = mapped_column(Boolean, default=False)
    compare_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
