from datetime import datetime
from uuid import uuid4

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class GithubInstallationRepository(Base):
    __tablename__ = "github_installation_repositories"
    __table_args__ = (
        UniqueConstraint("installation_ref_id", "github_repository_id", name="uq_installation_repo"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    installation_ref_id: Mapped[str] = mapped_column(String(36), ForeignKey("github_installations.id"), index=True)
    workspace_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("workspaces.id"), nullable=True, index=True)
    github_repository_id: Mapped[int] = mapped_column(BigInteger, index=True)
    full_name: Mapped[str] = mapped_column(String(255), index=True)
    name: Mapped[str] = mapped_column(String(255))
    private: Mapped[bool] = mapped_column(Boolean, default=True)
    default_branch: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    removed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
