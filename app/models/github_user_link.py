from datetime import datetime
from uuid import uuid4

from sqlalchemy import BigInteger, DateTime, ForeignKey, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class GithubUserLink(Base):
    __tablename__ = "github_user_links"
    __table_args__ = (
        UniqueConstraint("user_id", "github_user_id", name="uq_github_user_link_user_github"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), index=True)
    github_user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    github_login: Mapped[str] = mapped_column(String(255))
    access_token_encrypted: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    refresh_token_encrypted: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    token_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    scope_snapshot: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
