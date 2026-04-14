from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from fastapi import HTTPException, Request, Response
from sqlalchemy import Select, select
from sqlalchemy.orm import Session
from starlette import status

from app.core.config import get_settings
from app.hotdock.services.audit import record_audit_log
from app.hotdock.services.security import (
    future_session_expiry,
    generate_token,
    hash_token,
    utcnow,
    verify_password,
    verify_token_hash,
)
from app.models.auth_session import AuthSession
from app.models.user import User
from app.models.workspace import Workspace
from app.models.workspace_member import WorkspaceMember

settings = get_settings()
AUTH_ANON_CSRF_KEY = "anon_csrf_token"
FLASH_SESSION_KEY = "flash_message"
AFTER_LOGIN_KEY = "after_login_next"
GITHUB_CLAIM_KEY = "github_claim_context"

ROLE_ORDER = {
    "viewer": 1,
    "member": 2,
    "admin": 3,
    "owner": 4,
}


@dataclass
class AuthContext:
    user: User | None
    session: AuthSession | None
    csrf_token: str


def sanitize_next_path(next_path: str | None, default: str = "/dashboard") -> str:
    if not next_path:
        return default
    if not next_path.startswith("/") or next_path.startswith("//"):
        return default
    return next_path


def get_or_create_anon_csrf(request: Request) -> str:
    token = request.session.get(AUTH_ANON_CSRF_KEY)
    if not token:
        token = generate_token()
        request.session[AUTH_ANON_CSRF_KEY] = token
    return token


def get_flash(request: Request) -> dict[str, str] | None:
    flash = request.session.pop(FLASH_SESSION_KEY, None)
    if isinstance(flash, dict):
        return flash
    return None


def set_flash(request: Request, level: str, message: str) -> None:
    request.session[FLASH_SESSION_KEY] = {"level": level, "message": message}


def set_after_login(request: Request, next_path: str) -> None:
    request.session[AFTER_LOGIN_KEY] = sanitize_next_path(next_path)


def pop_after_login(request: Request) -> str | None:
    value = request.session.pop(AFTER_LOGIN_KEY, None)
    return sanitize_next_path(value, "/dashboard") if value else None


def store_github_claim_context(request: Request, *, claim_token: str, next_path: str) -> None:
    request.session[GITHUB_CLAIM_KEY] = {
        "claim_token": claim_token,
        "next": sanitize_next_path(next_path, f"/integrations/github/claim/{claim_token}"),
    }


def pop_github_claim_context(request: Request) -> dict[str, str] | None:
    value = request.session.pop(GITHUB_CLAIM_KEY, None)
    return value if isinstance(value, dict) else None


def build_login_redirect(next_path: str) -> str:
    return f"/login?{urlencode({'next': sanitize_next_path(next_path)})}"


def _session_query(session_token: str) -> Select[tuple[AuthSession]]:
    return select(AuthSession).where(AuthSession.session_token_hash == hash_token(session_token))


def load_auth_context(request: Request, db: Session) -> AuthContext:
    session_token = request.cookies.get(settings.auth_cookie_name)
    csrf_token = request.cookies.get(settings.csrf_cookie_name)
    if not session_token or not csrf_token:
        return AuthContext(user=None, session=None, csrf_token=get_or_create_anon_csrf(request))

    auth_session = db.scalar(_session_query(session_token))
    if auth_session is None or auth_session.revoked_at is not None or auth_session.expires_at <= utcnow():
        return AuthContext(user=None, session=None, csrf_token=get_or_create_anon_csrf(request))

    if not verify_token_hash(csrf_token, auth_session.csrf_token_hash):
        return AuthContext(user=None, session=None, csrf_token=get_or_create_anon_csrf(request))

    user = db.get(User, auth_session.user_id)
    if user is None or user.deleted_at is not None or user.status != "active":
        return AuthContext(user=None, session=None, csrf_token=get_or_create_anon_csrf(request))

    auth_session.last_seen_at = utcnow()
    db.commit()
    return AuthContext(user=user, session=auth_session, csrf_token=csrf_token)


def attach_auth_context(request: Request, db: Session) -> AuthContext:
    context = load_auth_context(request, db)
    request.state.auth = context
    request.state.current_user = context.user
    request.state.current_session = context.session
    return context


def require_user(request: Request, db: Session, next_path: str | None = None) -> AuthContext:
    context = getattr(request.state, "auth", None) or attach_auth_context(request, db)
    if context.user is None:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail=build_login_redirect(next_path or request.url.path),
        )
    return context


def verify_form_csrf(request: Request, csrf_token: str, db: Session) -> None:
    context = getattr(request.state, "auth", None) or attach_auth_context(request, db)
    expected = context.csrf_token
    if not expected or not verify_token_hash(csrf_token, hash_token(expected)):
        raise HTTPException(status_code=400, detail="Invalid CSRF token")


def _cookie_kwargs() -> dict[str, Any]:
    return {
        "httponly": True,
        "secure": settings.app_env == "production",
        "samesite": "lax",
        "path": "/",
    }


def _csrf_cookie_kwargs() -> dict[str, Any]:
    return {
        "httponly": False,
        "secure": settings.app_env == "production",
        "samesite": "lax",
        "path": "/",
    }


def create_login_session(
    db: Session,
    request: Request,
    response: Response,
    *,
    user: User,
    rotated_from: AuthSession | None = None,
) -> AuthSession:
    session_token = generate_token()
    csrf_token = generate_token()
    now = utcnow()
    auth_session = AuthSession(
        user_id=user.id,
        session_token_hash=hash_token(session_token),
        csrf_token_hash=hash_token(csrf_token),
        ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
        expires_at=future_session_expiry(),
        rotated_from_session_id=rotated_from.id if rotated_from else None,
        created_at=now,
        last_seen_at=now,
    )
    if rotated_from is not None:
        rotated_from.revoked_at = now
    user.last_login_at = now
    db.add(auth_session)
    db.commit()
    response.set_cookie(settings.auth_cookie_name, session_token, **_cookie_kwargs())
    response.set_cookie(settings.csrf_cookie_name, csrf_token, **_csrf_cookie_kwargs())
    request.session.pop(AUTH_ANON_CSRF_KEY, None)
    return auth_session


def revoke_session(db: Session, response: Response, auth_session: AuthSession | None) -> None:
    if auth_session is not None:
        auth_session.revoked_at = utcnow()
        db.commit()
    response.delete_cookie(settings.auth_cookie_name, path="/")
    response.delete_cookie(settings.csrf_cookie_name, path="/")


def authenticate_user(db: Session, email: str, password: str) -> User | None:
    user = db.scalar(select(User).where(User.email == email.lower().strip()))
    if user is None or user.deleted_at is not None or user.status != "active":
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


def default_workspace_for_user(db: Session, user_id: str) -> Workspace | None:
    member = db.scalar(
        select(WorkspaceMember)
        .where(
            WorkspaceMember.user_id == user_id,
            WorkspaceMember.status == "active",
            WorkspaceMember.revoked_at.is_(None),
        )
        .order_by(WorkspaceMember.created_at.asc())
    )
    if member is None:
        return None
    return db.get(Workspace, member.workspace_id)


def require_role(member: WorkspaceMember, required_role: str) -> None:
    if ROLE_ORDER.get(member.role, 0) < ROLE_ORDER.get(required_role, 0):
        raise HTTPException(status_code=403, detail="Forbidden")


def deny_workspace_access(db: Session, request: Request, workspace_slug: str, user_id: str | None) -> None:
    record_audit_log(
        db,
        request,
        actor_type="user" if user_id else "anonymous",
        actor_id=user_id,
        workspace_id=None,
        target_type="workspace",
        target_id=workspace_slug,
        action="unauthorized_workspace_access_denied",
        metadata={"workspace_slug": workspace_slug},
    )
    db.commit()
