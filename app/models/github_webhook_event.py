from datetime import datetime
from uuid import uuid4

from sqlalchemy import BigInteger, DateTime, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class GithubWebhookEvent(Base):
    __tablename__ = "github_webhook_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    delivery_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    event_name: Mapped[str] = mapped_column(String(64), index=True)
    action_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    installation_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    workspace_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    signature_valid: Mapped[bool] = mapped_column(default=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    payload_sha256: Mapped[str] = mapped_column(String(64), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    processing_status: Mapped[str] = mapped_column(String(32), default="received", index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
