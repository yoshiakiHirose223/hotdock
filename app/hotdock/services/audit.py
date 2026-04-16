from __future__ import annotations

import json
from pathlib import Path

from fastapi import Request
from sqlalchemy.orm import Session

from app.hotdock.services.security import utcnow
from app.models.audit_log import AuditLog


AUDIT_STREAM_PATH = Path("storage/audit/security-events.jsonl")


def _append_audit_stream(payload: dict) -> None:
    try:
        AUDIT_STREAM_PATH.parent.mkdir(parents=True, exist_ok=True)
        with AUDIT_STREAM_PATH.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        # File audit is a best-effort secondary trail and must not break the app path.
        return


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
    created_at = utcnow()
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
        created_at=created_at,
    )
    db.add(log)
    db.flush()
    _append_audit_stream(
        {
            "id": log.id,
            "actor_type": actor_type,
            "actor_id": actor_id,
            "workspace_id": workspace_id,
            "target_type": target_type,
            "target_id": target_id,
            "action": action,
            "metadata": metadata or {},
            "ip": request.client.host if request and request.client else None,
            "user_agent": request.headers.get("user-agent") if request else None,
            "created_at": created_at.isoformat(),
        }
    )
    return log
