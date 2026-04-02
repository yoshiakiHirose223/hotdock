from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from app.blog.auth import (
    authenticate_blog_admin,
    get_safe_admin_next_path,
    is_blog_admin_authenticated,
    login_blog_admin,
    logout_blog_admin,
)
from app.blog.common import BLOG_ADMIN_BASE_PATH, BLOG_ADMIN_ROUTE_PREFIX, render_blog_template

router = APIRouter(prefix=BLOG_ADMIN_ROUTE_PREFIX)


@router.get("/login")
async def blog_admin_login_page(request: Request, next: str = ""):
    next_path = get_safe_admin_next_path(next)
    if is_blog_admin_authenticated(request):
        return RedirectResponse(url=next_path, status_code=303)

    return render_blog_template(
        request,
        "blog/admin/login.html",
        page_title="Blog Admin Login",
        errors=[],
        login_username="",
        next_path=next_path,
    )


@router.post("/login")
async def blog_admin_login_submit(
    request: Request,
    username: str = Form(default=""),
    password: str = Form(default=""),
    next_path: str = Form(default=""),
):
    safe_next_path = get_safe_admin_next_path(next_path)
    if not authenticate_blog_admin(username, password):
        return render_blog_template(
            request,
            "blog/admin/login.html",
            page_title="Blog Admin Login",
            errors=["ID またはパスワードが正しくありません。"],
            login_username=username.strip(),
            next_path=safe_next_path,
        )

    login_blog_admin(request, username)
    return RedirectResponse(url=safe_next_path, status_code=303)


@router.post("/logout")
async def blog_admin_logout(request: Request):
    logout_blog_admin(request)
    return RedirectResponse(url=f"{BLOG_ADMIN_BASE_PATH}/login", status_code=303)
