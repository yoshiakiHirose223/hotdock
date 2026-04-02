from urllib.parse import quote

from fastapi import HTTPException, Request

from app.blog.common import BLOG_ADMIN_BASE_PATH, BLOG_ADMIN_LOGIN_PATH
from app.core.config import get_settings
from app.core.security import verify_password

BLOG_ADMIN_SESSION_KEY = "blog_admin_authenticated"
BLOG_ADMIN_SESSION_USER_KEY = "blog_admin_username"


def get_safe_admin_next_path(candidate: str | None) -> str:
    next_path = (candidate or "").strip()
    if not next_path:
        return BLOG_ADMIN_BASE_PATH
    if not next_path.startswith(BLOG_ADMIN_BASE_PATH):
        return BLOG_ADMIN_BASE_PATH
    return next_path


def build_blog_admin_login_redirect(candidate: str | None = None) -> str:
    next_path = get_safe_admin_next_path(candidate)
    return f"{BLOG_ADMIN_LOGIN_PATH}?next={quote(next_path, safe='/?=&')}"


def is_blog_admin_authenticated(request: Request) -> bool:
    return bool(request.session.get(BLOG_ADMIN_SESSION_KEY))


def authenticate_blog_admin(username: str, password: str) -> bool:
    settings = get_settings()
    if not settings.blog_admin_username or not settings.blog_admin_password_hash:
        return False
    if username.strip() != settings.blog_admin_username:
        return False
    return verify_password(password, settings.blog_admin_password_hash)


def login_blog_admin(request: Request, username: str) -> None:
    request.session[BLOG_ADMIN_SESSION_KEY] = True
    request.session[BLOG_ADMIN_SESSION_USER_KEY] = username.strip()


def logout_blog_admin(request: Request) -> None:
    request.session.pop(BLOG_ADMIN_SESSION_KEY, None)
    request.session.pop(BLOG_ADMIN_SESSION_USER_KEY, None)


async def require_blog_admin(request: Request) -> None:
    if is_blog_admin_authenticated(request):
        return

    if "/api/" in request.url.path:
        raise HTTPException(status_code=401, detail="Authentication required")

    target_path = request.url.path
    if request.url.query:
        target_path = f"{target_path}?{request.url.query}"

    raise HTTPException(
        status_code=303,
        detail="Authentication required",
        headers={"Location": build_blog_admin_login_redirect(target_path)},
    )
