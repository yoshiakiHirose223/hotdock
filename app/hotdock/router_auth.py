from typing import Any

from fastapi import APIRouter, Request

from app.hotdock.services.context import build_auth_context

router = APIRouter()


def render_auth(template_name: str, context: dict[str, Any]):
    request = context["request"]
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


@router.get("/login", name="hotdock-login")
async def login(request: Request):
    context = build_auth_context(
        request,
        page_title="ログイン | Hotdock",
        page_description="Hotdock のログインページ。メールアドレスとパスワードで共通ダッシュボードへ入る想定です。",
        page_heading="ログイン",
        active_nav="login",
        body_class="page-auth page-login",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Login", "href": "/login"}],
    )
    context.update(
        {
            "eyebrow": "共通ダッシュボードへアクセス",
            "support_copy": "GitHub App と SaaS のどちらから入っても、最終的な管理体験は共通です。",
        }
    )
    return render_auth("hotdock/auth/login.html", context)


@router.get("/signup", name="hotdock-signup")
async def signup(request: Request):
    context = build_auth_context(
        request,
        page_title="新規登録 | Hotdock",
        page_description="Hotdock の新規登録ページ。登録後に git 連携設定、通知設定、ダッシュボード利用開始へ進む想定です。",
        page_heading="新規登録",
        active_nav="signup",
        body_class="page-auth page-signup",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Signup", "href": "/signup"}],
    )
    context.update(
        {
            "eyebrow": "SaaS 版で始める",
            "support_copy": "登録後は git 連携設定、通知先設定、共通ダッシュボード利用開始の順で進む想定です。",
        }
    )
    return render_auth("hotdock/auth/signup.html", context)


@router.get("/install/github", name="hotdock-install-github")
async def install_github(request: Request):
    context = build_auth_context(
        request,
        page_title="GitHub App 公開予定 | Hotdock",
        page_description="Hotdock の GitHub App は未提供です。導入予定フローと問い合わせ導線を案内します。",
        page_heading="GitHub App 公開予定",
        active_nav="install-github",
        body_class="page-auth page-install-github",
        breadcrumbs=[
            {"label": "Home", "href": "/"},
            {"label": "Install GitHub", "href": "/install/github"},
        ],
    )
    context.update(
        {
            "eyebrow": "GitHub App 導線",
            "support_copy": "現時点では未提供です。公開後は GitHub App から始めても共通 /app ダッシュボードに接続する想定です。",
        }
    )
    return render_auth("hotdock/auth/install_github.html", context)
