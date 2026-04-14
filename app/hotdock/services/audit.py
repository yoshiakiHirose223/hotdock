from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from app.hotdock.services.security import utcnow
from app.models.audit_log import AuditLog


def record_audit_log(
    db: Session,
    request: Request | None,
    *,
    actor_type: str,
    actor_id: str | None,
    workspace_id: str | None,
    target_type: str,
    target_id: str | None,
    action: str,
    metadata: dict | None = None,
) -> AuditLog:
    log = AuditLog(
        actor_type=actor_type,
        actor_id=actor_id,
        workspace_id=workspace_id,
        target_type=target_type,
        target_id=target_id,
        action=action,
        event_metadata=metadata or {},
        ip=request.client.host if request and request.client else None,
        user_agent=request.headers.get("user-agent") if request else None,
        created_at=utcnow(),
    )
    db.add(log)
    db.flush()
    return log
