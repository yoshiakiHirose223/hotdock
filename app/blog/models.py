from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, String, Table, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

blog_post_tags = Table(
    "blog_post_tags",
    Base.metadata,
    Column("post_id", ForeignKey("blog_posts.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_id", ForeignKey("blog_tags.id", ondelete="CASCADE"), primary_key=True),
)


class BlogPost(Base):
    __tablename__ = "blog_posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(255))
    summary: Mapped[str] = mapped_column(String(50), default="")
    source_filename: Mapped[str] = mapped_column(String(255), unique=True)
    is_published: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    published_at: Mapped[date] = mapped_column(Date, default=date.today)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    tags: Mapped[list["BlogTag"]] = relationship(
        secondary=blog_post_tags,
        back_populates="posts",
    )
    images: Mapped[list["BlogImage"]] = relationship(
        back_populates="post",
        cascade="all, delete-orphan",
    )


class BlogTag(Base):
    __tablename__ = "blog_tags"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    slug: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    posts: Mapped[list[BlogPost]] = relationship(
        secondary=blog_post_tags,
        back_populates="tags",
    )


class BlogImage(Base):
    __tablename__ = "blog_images"

    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int | None] = mapped_column(ForeignKey("blog_posts.id", ondelete="CASCADE"), nullable=True, index=True)
    draft_key: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    token: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    original_filename: Mapped[str] = mapped_column(String(255))
    stored_filename: Mapped[str] = mapped_column(String(255), unique=True)
    content_type: Mapped[str] = mapped_column(String(120), default="application/octet-stream")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    post: Mapped[BlogPost | None] = relationship(back_populates="images")
