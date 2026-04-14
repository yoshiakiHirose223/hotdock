from datetime import datetime
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class BranchFile(Base):
    __tablename__ = "branch_files"
    __table_args__ = (
        UniqueConstraint("branch_id", "path", name="uq_branch_file_path"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    workspace_id: Mapped[str] = mapped_column(String(36), ForeignKey("workspaces.id"), index=True)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id"), index=True)
    path: Mapped[str] = mapped_column(String(2048))
    change_type: Mapped[str] = mapped_column(String(32))
    is_conflict: Mapped[bool] = mapped_column(default=False, index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
