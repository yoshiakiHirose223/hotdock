from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class FileCollisionBranch(Base):
    __tablename__ = "file_collision_branches"
    __table_args__ = (
        UniqueConstraint("collision_id", "branch_id", name="uq_file_collision_branch"),
    )

    collision_id: Mapped[str] = mapped_column(String(36), ForeignKey("file_collisions.id"), primary_key=True)
    branch_id: Mapped[str] = mapped_column(String(36), ForeignKey("branches.id"), primary_key=True)
    path: Mapped[str] = mapped_column(String(2048))
    last_change_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
