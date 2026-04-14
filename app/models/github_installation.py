from datetime import datetime
from uuid import uuid4

from sqlalchemy import BigInteger, DateTime, ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class GithubInstallation(Base):
    __tablename__ = "github_installations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    installation_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    github_account_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    github_account_login: Mapped[str | None] = mapped_column(String(255), nullable=True)
    github_account_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    target_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    permissions_snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
    events_snapshot: Mapped[list] = mapped_column(JSON, default=list)
    installation_status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    suspended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    uninstalled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    claimed_workspace_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("workspaces.id"), nullable=True, index=True)
    claimed_by_user_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("users.id"), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_webhook_event_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
