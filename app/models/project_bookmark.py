from datetime import datetime

from sqlalchemy import DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class ProjectBookmark(Base):
    __tablename__ = "project_bookmarks"
    __table_args__ = (
        UniqueConstraint("workspace_id", "project_id", name="uq_project_bookmarks_workspace_project"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = mapped_column(String(64), index=True, default="default-workspace")
    project_id: Mapped[int] = mapped_column(Integer, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
