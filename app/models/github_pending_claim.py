from datetime import datetime
from uuid import uuid4

from sqlalchemy import BigInteger, DateTime, ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class GithubPendingClaim(Base):
    __tablename__ = "github_pending_claims"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    claim_token_hash: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    installation_id: Mapped[int] = mapped_column(BigInteger, index=True)
    setup_nonce: Mapped[str] = mapped_column(String(128), index=True)
    initiated_via: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    workspace_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("workspaces.id"), nullable=True, index=True)
    user_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    github_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    github_user_login: Mapped[str | None] = mapped_column(String(255), nullable=True)
    oauth_state_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_resume_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    state_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    callback_source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    setup_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
