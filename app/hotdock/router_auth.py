from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette import status

from app.core.config import get_settings
from app.core.database import get_db
from app.hotdock.services.audit import record_audit_log
from app.hotdock.services.auth import (
    attach_auth_context,
    authenticate_user,
    build_login_redirect,
    create_login_session,
    default_workspace_for_user,
    ensure_csrf_cookie,
    get_flash,
    get_or_create_anon_csrf,
    pop_after_login,
    pop_github_claim_context,
    revoke_session,
    sanitize_next_path,
    set_after_login,
    set_flash,
    store_github_claim_context,
)
from app.hotdock.services.context import build_auth_context
from app.hotdock.services.github import (
    GithubOAuthClient,
    complete_github_claim,
    consume_github_install_intent,
    create_callback_pending_claim,
    create_github_install_intent,
    find_resumable_pending_claims_for_github_user,
    finalize_github_claim,
    load_github_install_intent_by_state,
    load_pending_claim_by_token,
    mark_webhook_event_failed,
    pending_claim_has_verified_github_identity,
    record_push_event,
    record_webhook_event,
    reissue_pending_claim_token,
    resolve_callback_installation,
    select_claim_workspace,
    set_pending_oauth_state,
    sync_claimed_installation_repositories,
    sync_installation_event,
    sync_installation_repositories_event,
    unlink_github_installation,
    verify_github_webhook_signature,
    verify_pending_oauth_state,
)
from app.hotdock.services.security import generate_token, utcnow
from app.hotdock.services.workspaces import (
    accept_workspace_invitation,
    create_user,
    create_workspace,
    delete_workspace_and_related_data,
    invite_workspace_member,
    leave_workspace,
    list_user_workspaces,
    resolve_workspace_access,
    update_workspace_member_role,
)
from app.models.github_installation import GithubInstallation
from app.models.github_pending_claim import GithubPendingClaim
from app.models.github_webhook_event import GithubWebhookEvent
from app.models.workspace import Workspace
from app.models.workspace_invitation import WorkspaceInvitation
from app.models.workspace_member import WorkspaceMember

settings = get_settings()
router = APIRouter()


def _append_query_params(url: str, params: dict[str, str]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    for key, value in params.items():
        if value:
            query[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def render_auth(template_name: str, context: dict[str, Any]):
    request = context["request"]
    response = request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )
    ensure_csrf_cookie(response, context.get("csrf_token"))
    return response


def auth_page_context(
    request: Request,
    db: Session,
    *,
    page_title: str,
    page_description: str,
    page_heading: str,
    active_nav: str,
    body_class: str,
    breadcrumbs: list[dict[str, str]],
) -> dict[str, Any]:
    auth = attach_auth_context(request, db)
    context = build_auth_context(
        request,
        page_title=page_title,
        page_description=page_description,
        page_heading=page_heading,
        active_nav=active_nav,
        body_class=body_class,
        breadcrumbs=breadcrumbs,
    )
    context.update(
        {
            "flash": get_flash(request),
            "csrf_token": auth.csrf_token,
            "current_user": auth.user,
            "user_workspaces": list_user_workspaces(db, auth.user) if auth.user else [],
            "form_data": {},
        }
    )
    return context


def _verify_anonymous_csrf(request: Request, token: str) -> None:
    cookie_token = request.cookies.get(settings.csrf_cookie_name)
    if token and cookie_token and hmac.compare_digest(token, cookie_token):
        return
    expected = get_or_create_anon_csrf(request)
    if not token or not hmac.compare_digest(token, expected):
        raise ValueError("Invalid CSRF token")


def _require_login(request: Request, db: Session, next_path: str):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        set_after_login(request, next_path)
        return None, RedirectResponse(url=build_login_redirect(next_path), status_code=status.HTTP_303_SEE_OTHER)
    return auth, None


def _admin_workspaces(db: Session, user) -> list:
    return [item for item in list_user_workspaces(db, user) if item.membership.role in {"owner", "admin"}]


def _workspace_slug_from_install_intent(db: Session, request: Request, auth, install_intent: dict[str, Any] | None) -> str | None:
    if auth.user is None:
        return None
    if install_intent and install_intent.get("workspace_slug"):
        workspace_slug = str(install_intent["workspace_slug"])
        try:
            resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="admin")
        except HTTPException:
            return None
        return workspace_slug

    admin_workspaces = _admin_workspaces(db, auth.user)
    if len(admin_workspaces) == 1:
        return admin_workspaces[0].workspace.slug
    return None


def _github_authorize_url(state_token: str) -> str:
    if settings.github_mock_oauth_enabled:
        return f"/integrations/github/callback?{urlencode({'code': 'mock-code', 'state': state_token})}"
    if not settings.github_app_client_id:
        raise ValueError("GitHub OAuth is not configured")
    query = urlencode(
        {
            "client_id": settings.github_app_client_id,
            "state": state_token,
            "redirect_uri": f"{settings.site_url}/integrations/github/callback",
        }
    )
    return f"{settings.github_oauth_base_url}/login/oauth/authorize?{query}"


def _invitation_by_token(db: Session, invitation_token: str) -> WorkspaceInvitation | None:
    from app.hotdock.services.security import hash_token

    return db.scalar(
        select(WorkspaceInvitation).where(WorkspaceInvitation.invitation_token_hash == hash_token(invitation_token))
    )


@router.get("/register", name="hotdock-register")
@router.get("/signup", name="hotdock-signup")
async def register_page(request: Request, db: Session = Depends(get_db)):
    context = auth_page_context(
        request,
        db,
        page_title="新規登録 | Hotdock",
        page_description="Hotdock の新規登録ページ。",
        page_heading="新規登録",
        active_nav="signup",
        body_class="page-auth page-signup",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Register", "href": "/register"}],
    )
    context.update(
        {
            "eyebrow": "SaaS 版で始める",
            "support_copy": "登録後に workspace を作成し、GitHub App installation を claim できるようにします。",
            "next_path": sanitize_next_path(request.query_params.get("next"), "/dashboard"),
        }
    )
    return render_auth("hotdock/auth/signup.html", context)


@router.post("/register", name="hotdock-register-submit")
async def register_submit(
    request: Request,
    db: Session = Depends(get_db),
    display_name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    workspace_name: str = Form(""),
    workspace_scale: str = Form(""),
    next: str = Form("/dashboard"),
    csrf_token: str = Form(...),
):
    try:
        _verify_anonymous_csrf(request, csrf_token)
    except ValueError:
        set_flash(request, "error", "フォームの送信を確認できませんでした。")
        return RedirectResponse(url="/register", status_code=status.HTTP_303_SEE_OTHER)

    from app.models.user import User

    if db.scalar(select(User).where(User.email == email.lower().strip())) is not None:
        set_flash(request, "error", "このメールアドレスはすでに登録されています。")
        return RedirectResponse(url="/register", status_code=status.HTTP_303_SEE_OTHER)

    user = create_user(db, email=email, password=password, display_name=display_name)
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=None,
        target_type="user",
        target_id=user.id,
        action="user_register",
        metadata={"email": user.email, "workspace_scale": workspace_scale},
    )
    db.commit()

    workspace = None
    if workspace_name.strip():
        workspace = create_workspace(db, request, user=user, name=workspace_name.strip())

    redirect_to = sanitize_next_path(next, f"/workspaces/{workspace.slug}/dashboard" if workspace else "/dashboard")
    response = RedirectResponse(url=redirect_to, status_code=status.HTTP_303_SEE_OTHER)
    create_login_session(db, request, response, user=user)
    claim_context = pop_github_claim_context(request)
    if claim_context:
        response.headers["location"] = sanitize_next_path(claim_context["next"], redirect_to)
        set_flash(request, "success", "登録が完了しました。続けて GitHub App installation の claim を完了してください。")
    return response


@router.get("/login", name="hotdock-login")
async def login_page(request: Request, db: Session = Depends(get_db)):
    context = auth_page_context(
        request,
        db,
        page_title="ログイン | Hotdock",
        page_description="Hotdock のログインページ。",
        page_heading="ログイン",
        active_nav="login",
        body_class="page-auth page-login",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Login", "href": "/login"}],
    )
    context.update(
        {
            "eyebrow": "Hotdock へログイン",
            "support_copy": "GitHub App claim の再開、workspace 選択、ダッシュボード閲覧に使うセッションです。",
            "next_path": sanitize_next_path(request.query_params.get("next"), "/dashboard"),
            "signup_href": f"/register?{urlencode({'next': sanitize_next_path(request.query_params.get('next'), '/dashboard')})}",
        }
    )
    return render_auth("hotdock/auth/login.html", context)


@router.post("/login", name="hotdock-login-submit")
async def login_submit(
    request: Request,
    db: Session = Depends(get_db),
    email: str = Form(...),
    password: str = Form(...),
    next: str = Form("/dashboard"),
    csrf_token: str = Form(...),
):
    try:
        _verify_anonymous_csrf(request, csrf_token)
    except ValueError:
        set_flash(request, "error", "フォームの送信を確認できませんでした。")
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)

    user = authenticate_user(db, email, password)
    if user is None:
        record_audit_log(
            db,
            request,
            actor_type="anonymous",
            actor_id=None,
            workspace_id=None,
            target_type="user",
            target_id=email.lower().strip(),
            action="user_login_failure",
            metadata={"email": email.lower().strip()},
        )
        db.commit()
        set_flash(request, "error", "メールアドレスまたはパスワードが正しくありません。")
        return RedirectResponse(url=f"/login?{urlencode({'next': sanitize_next_path(next)})}", status_code=status.HTTP_303_SEE_OTHER)

    response = RedirectResponse(url=sanitize_next_path(next), status_code=status.HTTP_303_SEE_OTHER)
    previous_session = attach_auth_context(request, db).session
    create_login_session(db, request, response, user=user, rotated_from=previous_session)
    record_audit_log(
        db,
        request,
        actor_type="user",
        actor_id=user.id,
        workspace_id=None,
        target_type="user",
        target_id=user.id,
        action="user_login_success",
        metadata={"email": user.email},
    )
    db.commit()
    claim_context = pop_github_claim_context(request)
    if claim_context:
        response.headers["location"] = sanitize_next_path(claim_context["next"], "/dashboard")
    else:
        response.headers["location"] = pop_after_login(request) or sanitize_next_path(next)
    return response


@router.post("/logout", name="hotdock-logout")
async def logout(request: Request, db: Session = Depends(get_db), csrf_token: str = Form(...)):
    auth = attach_auth_context(request, db)
    if not auth.user or not hmac.compare_digest(csrf_token, auth.csrf_token):
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    response = RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    revoke_session(db, response, auth.session)
    return response


@router.get("/workspaces/new", name="hotdock-workspace-new")
async def workspace_new(request: Request, db: Session = Depends(get_db)):
    auth, redirect = _require_login(request, db, request.url.path if not request.url.query else f"{request.url.path}?{request.url.query}")
    if redirect:
        return redirect
    context = auth_page_context(
        request,
        db,
        page_title="Workspace 作成 | Hotdock",
        page_description="workspace を作成して Hotdock の管理単位を用意します。",
        page_heading="Workspace を作成",
        active_nav="workspace-new",
        body_class="page-auth page-workspace-new",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Workspace", "href": "/workspaces/new"}],
    )
    context.update(
        {
            "next_path": sanitize_next_path(request.query_params.get("next"), "/dashboard"),
            "form_data": {},
        }
    )
    return render_auth("hotdock/auth/workspace_new.html", context)


@router.post("/workspaces", name="hotdock-workspaces-create")
async def workspace_create(
    request: Request,
    db: Session = Depends(get_db),
    name: str = Form(...),
    slug: str = Form(""),
    next: str = Form("/dashboard"),
    csrf_token: str = Form(...),
):
    auth, redirect = _require_login(request, db, "/workspaces/new")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url="/workspaces/new", status_code=status.HTTP_303_SEE_OTHER)
    workspace = create_workspace(db, request, user=auth.user, name=name, slug=slug or None)
    return RedirectResponse(
        url=sanitize_next_path(next, f"/workspaces/{workspace.slug}/dashboard"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


def _resolve_workspace_target(request: Request, db: Session, *, required_role: str):
    auth, redirect = _require_login(request, db, str(request.url.path))
    if redirect:
        return None, None, redirect
    workspace_slug = request.query_params.get("workspace") or request.path_params.get("workspace_slug")
    if not workspace_slug:
        default_workspace = default_workspace_for_user(db, auth.user.id)
        if default_workspace is None:
            set_flash(request, "error", "先に workspace を作成してください。")
            return None, None, RedirectResponse(url="/workspaces/new", status_code=status.HTTP_303_SEE_OTHER)
        workspace_slug = default_workspace.slug
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role=required_role)
    return auth, access, None


@router.get("/settings/integrations/github", name="hotdock-github-settings")
async def github_settings(request: Request, db: Session = Depends(get_db)):
    auth, access, redirect = _resolve_workspace_target(request, db, required_role="admin")
    if redirect:
        return redirect
    context = auth_page_context(
        request,
        db,
        page_title="GitHub Integration | Hotdock",
        page_description="GitHub App installation と pending claim を管理します。",
        page_heading="GitHub Integration",
        active_nav="github-settings",
        body_class="page-auth page-github-settings",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "GitHub", "href": "/settings/integrations/github"}],
    )
    installations = db.scalars(
        select(GithubInstallation).where(GithubInstallation.claimed_workspace_id == access.workspace.id)
    ).all()
    pending_claims = db.scalars(
        select(GithubPendingClaim).where(
            GithubPendingClaim.workspace_id == access.workspace.id,
            GithubPendingClaim.status.in_(["pending", "workspace_selected", "awaiting_github_auth", "github_authorized", "expired"]),
        )
    ).all()
    context.update(
        {
            "workspace": access.workspace,
            "installations": installations,
            "pending_claims": pending_claims,
            "install_available": bool(settings.github_app_install_url or settings.github_app_slug),
            "can_unlink_installation": access.membership.role == "owner",
        }
    )
    return render_auth("hotdock/auth/github_settings.html", context)


@router.post("/integrations/github/installations/{installation_id}/unlink", name="hotdock-github-installation-unlink")
async def github_installation_unlink(
    installation_id: int,
    request: Request,
    db: Session = Depends(get_db),
    workspace_slug: str = Form(...),
    csrf_token: str = Form(...),
):
    auth, redirect = _require_login(request, db, f"/settings/integrations/github?workspace={workspace_slug}")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(
            url=f"/settings/integrations/github?{urlencode({'workspace': workspace_slug})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="owner")
    installation = db.scalar(
        select(GithubInstallation).where(
            GithubInstallation.installation_id == installation_id,
            GithubInstallation.claimed_workspace_id == access.workspace.id,
        )
    )
    if installation is None:
        set_flash(request, "error", "installation が見つかりません。")
        return RedirectResponse(
            url=f"/settings/integrations/github?{urlencode({'workspace': workspace_slug})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    unlink_github_installation(db, request, installation=installation, workspace=access.workspace, actor=auth.user)
    set_flash(request, "success", "installation の紐付けを解除しました。再利用する場合は GitHub App を再インストールして claim してください。")
    return RedirectResponse(
        url=f"/settings/integrations/github?{urlencode({'workspace': workspace_slug})}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/integrations/github/install/start", name="hotdock-github-install-start")
async def github_install_start(request: Request, db: Session = Depends(get_db)):
    install_url = settings.github_app_install_url or (
        f"https://github.com/apps/{settings.github_app_slug}/installations/new" if settings.github_app_slug else None
    )
    if not install_url:
        set_flash(request, "error", "GitHub App の install URL が設定されていません。")
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)

    auth = attach_auth_context(request, db)
    workspace_slug = request.query_params.get("workspace")
    if auth.user is not None and workspace_slug:
        resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="admin")
    elif auth.user is not None:
        workspace_slug = _workspace_slug_from_install_intent(db, request, auth, None)

    install_state = generate_token()
    create_github_install_intent(
        db,
        request,
        state_token=install_state,
        workspace_slug=workspace_slug,
        user_id=auth.user.id if auth.user else None,
        source="install_start",
    )
    request.session["github_install_intent"] = {
        "workspace_slug": workspace_slug,
        "user_id": auth.user.id if auth.user else None,
        "nonce": install_state,
        "issued_at": int(utcnow().timestamp()),
        "source": "install_start",
    }
    return RedirectResponse(url=_append_query_params(install_url, {"state": install_state}), status_code=status.HTTP_303_SEE_OTHER)


@router.get("/integrations/github/setup", name="hotdock-github-setup")
async def github_setup(
    request: Request,
    db: Session = Depends(get_db),
):
    set_flash(request, "error", "GitHub App の setup URL フローは廃止されました。現在の連携ボタンからやり直してください。")
    auth = attach_auth_context(request, db)
    if auth.user is not None:
        workspace_slug = _workspace_slug_from_install_intent(db, request, auth, None)
        if workspace_slug:
            return RedirectResponse(
                url=f"/settings/integrations/github?{urlencode({'workspace': workspace_slug})}",
                status_code=status.HTTP_303_SEE_OTHER,
            )
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/integrations/github/claim/{claim_token}", name="hotdock-github-claim")
async def github_claim_page(claim_token: str, request: Request, db: Session = Depends(get_db)):
    context = auth_page_context(
        request,
        db,
        page_title="GitHub Claim | Hotdock",
        page_description="GitHub App installation を workspace に紐付けます。",
        page_heading="GitHub App Claim",
        active_nav="github-claim",
        body_class="page-auth page-github-claim",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "GitHub Claim", "href": request.url.path}],
    )
    pending_claim = load_pending_claim_by_token(db, claim_token)
    installation = None
    workspace = None
    selected_workspace_id = None
    available_workspaces = []
    if pending_claim is not None:
        installation = db.scalar(select(GithubInstallation).where(GithubInstallation.installation_id == pending_claim.installation_id))
        if pending_claim.workspace_id:
            workspace = db.get(Workspace, pending_claim.workspace_id)
            selected_workspace_id = pending_claim.workspace_id
        if context["current_user"]:
            available_workspaces = [
                item for item in list_user_workspaces(db, context["current_user"]) if item.membership.role in {"owner", "admin"}
            ]
    context.update(
        {
            "support_copy": "GitHub callback で取得した installation 情報は一時保存され、workspace 選択後に claim が完了します。callback 単独では ownership を確定しません。",
            "claim_token": claim_token,
            "pending_claim": pending_claim,
            "installation": installation,
            "workspace": workspace,
            "selected_workspace_id": selected_workspace_id,
            "available_workspaces": available_workspaces,
            "github_authorized": pending_claim_has_verified_github_identity(pending_claim) if pending_claim else False,
        }
    )
    return render_auth("hotdock/auth/github_claim.html", context)


@router.post("/integrations/github/claim/{claim_token}/workspace", name="hotdock-github-claim-workspace")
async def github_claim_workspace(
    claim_token: str,
    request: Request,
    db: Session = Depends(get_db),
    workspace_id: str = Form(...),
    csrf_token: str = Form(...),
):
    auth, redirect = _require_login(request, db, f"/integrations/github/claim/{claim_token}")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/integrations/github/claim/{claim_token}", status_code=status.HTTP_303_SEE_OTHER)
    pending_claim = load_pending_claim_by_token(db, claim_token)
    if pending_claim is None:
        set_flash(request, "error", "claim が見つかりませんでした。")
        return RedirectResponse(url="/settings/integrations/github", status_code=status.HTTP_303_SEE_OTHER)
    workspace = db.get(Workspace, workspace_id)
    if workspace is None:
        set_flash(request, "error", "workspace が見つかりません。")
        return RedirectResponse(url=f"/integrations/github/claim/{claim_token}", status_code=status.HTTP_303_SEE_OTHER)
    resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace.slug, required_role="admin")
    pending_claim = select_claim_workspace(db, request, pending_claim=pending_claim, user=auth.user, workspace=workspace)
    if pending_claim_has_verified_github_identity(pending_claim):
        try:
            installation = finalize_github_claim(db, request, pending_claim=pending_claim, user=auth.user)
            sync_claimed_installation_repositories(db, installation)
        except Exception:
            set_flash(request, "error", "installation の claim 完了に失敗しました。")
            return RedirectResponse(url=f"/integrations/github/claim/{claim_token}", status_code=status.HTTP_303_SEE_OTHER)
        workspace = db.get(Workspace, installation.claimed_workspace_id)
        return RedirectResponse(url=f"/workspaces/{workspace.slug}/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(
        url=f"/integrations/github/authorize/start?{urlencode({'token': claim_token})}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/integrations/github/authorize/start", name="hotdock-github-authorize-start")
async def github_authorize_start(request: Request, db: Session = Depends(get_db), token: str = ""):
    auth, redirect = _require_login(request, db, request.url.path + (f"?token={token}" if token else ""))
    if redirect:
        return redirect
    pending_claim = load_pending_claim_by_token(db, token)
    if pending_claim is None or pending_claim.user_id != auth.user.id:
        set_flash(request, "error", "claim を再開できませんでした。")
        return RedirectResponse(url="/settings/integrations/github", status_code=status.HTTP_303_SEE_OTHER)
    if pending_claim_has_verified_github_identity(pending_claim):
        set_flash(request, "success", "GitHub 側の確認は完了しています。workspace を選択して claim を完了してください。")
        return RedirectResponse(url=f"/integrations/github/claim/{token}", status_code=status.HTTP_303_SEE_OTHER)
    state_token = generate_token()
    request.session["github_oauth_state"] = {"state": state_token, "claim_token": token}
    try:
        set_pending_oauth_state(db, pending_claim, state_token)
    except Exception:
        set_flash(request, "error", "claim 状態が無効です。最初からやり直してください。")
        return RedirectResponse(url=f"/integrations/github/claim/{token}", status_code=status.HTTP_303_SEE_OTHER)
    try:
        authorize_url = _github_authorize_url(state_token)
    except ValueError:
        set_flash(request, "error", "GitHub OAuth の設定が不足しています。")
        return RedirectResponse(url=f"/integrations/github/claim/{token}", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=authorize_url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/integrations/github/recover", name="hotdock-github-recover")
async def github_recover_page(request: Request, db: Session = Depends(get_db)):
    auth, redirect = _require_login(request, db, request.url.path)
    if redirect:
        return redirect
    context = auth_page_context(
        request,
        db,
        page_title="GitHub Claim Recovery | Hotdock",
        page_description="GitHub user authorization を使って保留中の installation claim を再開します。",
        page_heading="GitHub Claim Recovery",
        active_nav="github-recover",
        body_class="page-auth page-github-recover",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "GitHub Recovery", "href": "/integrations/github/recover"}],
    )
    pending_claims = db.scalars(
        select(GithubPendingClaim).where(
            GithubPendingClaim.user_id == auth.user.id,
            GithubPendingClaim.status.in_(["pending", "workspace_selected", "awaiting_github_auth", "github_authorized"]),
            GithubPendingClaim.expires_at > utcnow(),
        )
    ).all()
    context.update({"pending_claims": pending_claims})
    return render_auth("hotdock/auth/github_recover.html", context)


@router.get("/integrations/github/recover/start", name="hotdock-github-recover-start")
async def github_recover_start(request: Request, db: Session = Depends(get_db)):
    auth, redirect = _require_login(request, db, "/integrations/github/recover")
    if redirect:
        return redirect
    state_token = generate_token()
    request.session["github_recovery_state"] = {
        "state": state_token,
        "issued_at": int(utcnow().timestamp()),
        "user_id": auth.user.id,
    }
    try:
        authorize_url = _github_authorize_url(state_token)
    except ValueError:
        set_flash(request, "error", "GitHub OAuth の設定が不足しています。")
        return RedirectResponse(url="/integrations/github/recover", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=authorize_url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/integrations/github/callback", name="hotdock-github-callback")
@router.get("/integrations/github/authorize/callback", name="hotdock-github-authorize-callback")
async def github_callback(
    request: Request,
    db: Session = Depends(get_db),
    code: str = "",
    state: str = "",
    error: str = "",
    error_description: str = "",
    installation_id: int = 0,
    setup_action: str = "",
):
    auth = attach_auth_context(request, db)
    install_intent_session = request.session.get("github_install_intent")
    install_intent_workspace_slug = _workspace_slug_from_install_intent(db, request, auth, install_intent_session)
    state_verified = False
    install_intent = None

    if error:
        set_flash(
            request,
            "error",
            error_description or "GitHub 側で user authorization が完了しませんでした。もう一度やり直してください。",
        )
        redirect_url = "/install/github"
        if install_intent_workspace_slug:
            redirect_url = f"/settings/integrations/github?{urlencode({'workspace': install_intent_workspace_slug})}"
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    if not code:
        if installation_id > 0 or setup_action:
            set_flash(
                request,
                "error",
                "GitHub callback に authorization code が含まれていません。GitHub App 側で "
                "`Request user authorization (OAuth) during installation` を有効にし、"
                "Callback URL が正しく設定されているか確認してください。",
            )
        else:
            set_flash(request, "error", "GitHub callback を直接開くことはできません。Hotdock から連携を開始してください。")
        redirect_url = "/install/github"
        if install_intent_workspace_slug:
            redirect_url = f"/settings/integrations/github?{urlencode({'workspace': install_intent_workspace_slug})}"
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    oauth_client = GithubOAuthClient()

    legacy_oauth_state = request.session.get("github_oauth_state")
    if legacy_oauth_state and state and legacy_oauth_state.get("state") == state:
        auth, redirect = _require_login(request, db, "/dashboard")
        if redirect:
            return redirect
        request.session.pop("github_oauth_state", None)
        pending_claim = load_pending_claim_by_token(db, legacy_oauth_state["claim_token"])
        if pending_claim is None or not verify_pending_oauth_state(pending_claim, state):
            set_flash(request, "error", "claim 状態が一致しません。")
            return RedirectResponse(url="/settings/integrations/github", status_code=status.HTTP_303_SEE_OTHER)
        try:
            token_payload = await oauth_client.exchange_code(code)
            installation = await complete_github_claim(
                db,
                request,
                pending_claim=pending_claim,
                user=auth.user,
                access_token=token_payload["access_token"],
                token_payload=token_payload,
            )
            sync_claimed_installation_repositories(db, installation)
        except Exception:
            set_flash(request, "error", "GitHub authorization の完了に失敗しました。")
            return RedirectResponse(
                url=f"/integrations/github/claim/{legacy_oauth_state['claim_token']}",
                status_code=status.HTTP_303_SEE_OTHER,
            )

        workspace = db.get(Workspace, installation.claimed_workspace_id)
        return RedirectResponse(url=f"/workspaces/{workspace.slug}/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    if legacy_oauth_state:
        request.session.pop("github_oauth_state", None)
        record_audit_log(
            db,
            request,
            actor_type="user" if auth.user else "anonymous",
            actor_id=auth.user.id if auth.user else None,
            workspace_id=None,
            target_type="pending_claim",
            target_id=legacy_oauth_state.get("claim_token"),
            action="github_callback_failed",
            metadata={"reason": "legacy_oauth_state_mismatch"},
        )
        db.commit()
        set_flash(request, "error", "GitHub authorization state が一致しません。claim を最初からやり直してください。")
        return RedirectResponse(url="/integrations/github/recover", status_code=status.HTTP_303_SEE_OTHER)

    recovery_state = request.session.get("github_recovery_state")
    if recovery_state and state and recovery_state.get("state") == state:
        auth, redirect = _require_login(request, db, "/integrations/github/recover")
        if redirect:
            return redirect
        if recovery_state.get("user_id") != auth.user.id:
            request.session.pop("github_recovery_state", None)
            set_flash(request, "error", "GitHub recovery セッションが一致しません。")
            return RedirectResponse(url="/integrations/github/recover", status_code=status.HTTP_303_SEE_OTHER)
        request.session.pop("github_recovery_state", None)
        try:
            token_payload = await oauth_client.exchange_code(code)
            github_user = await oauth_client.fetch_user(token_payload["access_token"])
            claims = find_resumable_pending_claims_for_github_user(db, github_user_id=github_user["id"], user_id=auth.user.id)
        except Exception:
            set_flash(request, "error", "GitHub 側の確認に失敗しました。もう一度お試しください。")
            return RedirectResponse(url="/integrations/github/recover", status_code=status.HTTP_303_SEE_OTHER)
        if not claims:
            record_audit_log(
                db,
                request,
                actor_type="user",
                actor_id=auth.user.id,
                workspace_id=None,
                target_type="pending_claim",
                target_id=None,
                action="github_claim_recovery_failed",
                metadata={"reason": "no_resumable_claims", "github_user_id": github_user["id"]},
            )
            db.commit()
            set_flash(request, "error", "再開できる claim が見つかりませんでした。GitHub App のインストールからやり直してください。")
            return RedirectResponse(url="/integrations/github/recover", status_code=status.HTTP_303_SEE_OTHER)
        pending_claim = claims[0]
        set_flash(
            request,
            "success",
            "保留中の claim を見つけました。workspace を選んで再開してください。"
            if len(claims) == 1
            else "複数の claim 候補が見つかったため、最新のものを開きました。必要なら GitHub 設定画面も確認してください。",
        )
        claim_token = reissue_pending_claim_token(db, pending_claim)
        record_audit_log(
            db,
            request,
            actor_type="user",
            actor_id=auth.user.id,
            workspace_id=pending_claim.workspace_id,
            target_type="pending_claim",
            target_id=pending_claim.id,
            action="github_claim_recovery_succeeded",
            metadata={"github_user_id": github_user["id"], "installation_id": pending_claim.installation_id},
        )
        db.commit()
        claim_url = f"/integrations/github/claim/{claim_token}"
        store_github_claim_context(request, claim_token=claim_token, next_path=claim_url)
        return RedirectResponse(url=claim_url, status_code=status.HTTP_303_SEE_OTHER)
    if recovery_state:
        request.session.pop("github_recovery_state", None)
        record_audit_log(
            db,
            request,
            actor_type="user" if auth.user else "anonymous",
            actor_id=auth.user.id if auth.user else None,
            workspace_id=None,
            target_type="pending_claim",
            target_id=None,
            action="github_claim_recovery_failed",
            metadata={"reason": "recovery_state_mismatch"},
        )
        db.commit()
        set_flash(request, "error", "GitHub recovery state が一致しません。もう一度 recovery を実行してください。")
        return RedirectResponse(url="/integrations/github/recover", status_code=status.HTTP_303_SEE_OTHER)

    if state:
        install_intent = load_github_install_intent_by_state(db, state)
        if install_intent is None:
            record_audit_log(
                db,
                request,
                actor_type="user" if auth.user else "anonymous",
                actor_id=auth.user.id if auth.user else None,
                workspace_id=None,
                target_type="github_installation",
                target_id=str(installation_id) if installation_id else None,
                action="github_callback_failed",
                metadata={"reason": "missing_install_intent"},
            )
            db.commit()
        else:
            install_intent_workspace_slug = install_intent.workspace_slug
            request.session.pop("github_install_intent", None)
            if install_intent.expires_at <= utcnow():
                record_audit_log(
                    db,
                    request,
                    actor_type="user" if auth.user else "anonymous",
                    actor_id=auth.user.id if auth.user else None,
                    workspace_id=None,
                    target_type="github_installation",
                    target_id=str(installation_id) if installation_id else None,
                    action="github_callback_failed",
                    metadata={"reason": "install_intent_expired"},
                )
                db.commit()
                install_intent = None
            elif install_intent.consumed_at is not None:
                record_audit_log(
                    db,
                    request,
                    actor_type="user" if auth.user else "anonymous",
                    actor_id=auth.user.id if auth.user else None,
                    workspace_id=None,
                    target_type="github_installation",
                    target_id=str(installation_id) if installation_id else None,
                    action="github_callback_failed",
                    metadata={"reason": "install_intent_already_consumed"},
                )
                db.commit()
                install_intent = None
            else:
                state_verified = True
    else:
        record_audit_log(
            db,
            request,
            actor_type="user" if auth.user else "anonymous",
            actor_id=auth.user.id if auth.user else None,
            workspace_id=None,
            target_type="github_installation",
            target_id=str(installation_id) if installation_id else None,
            action="github_callback_missing_state",
            metadata={"reason": "state_not_returned"},
        )
        db.commit()

    if installation_id <= 0:
        set_flash(request, "error", "GitHub installation を特定できませんでした。Hotdock から連携をやり直してください。")
        return RedirectResponse(url="/install/github", status_code=status.HTTP_303_SEE_OTHER)

    try:
        preferred_workspace_id = None
        if state_verified and install_intent and install_intent.workspace_slug:
            preferred_workspace = db.scalar(select(Workspace).where(Workspace.slug == install_intent.workspace_slug))
            preferred_workspace_id = preferred_workspace.id if preferred_workspace is not None else None
        token_payload = await oauth_client.exchange_code(code)
        github_user, installation = await resolve_callback_installation(
            db,
            access_token=token_payload["access_token"],
            installation_id=installation_id or None,
            issued_at_ts=int(install_intent.created_at.timestamp()) if state_verified and install_intent else None,
            preferred_workspace_id=preferred_workspace_id,
        )
        result = create_callback_pending_claim(
            db,
            request,
            installation=installation,
            github_user=github_user,
            source="install_callback",
            callback_state=state,
            install_intent={
                "workspace_slug": install_intent.workspace_slug,
                "user_id": install_intent.user_id,
                "source": install_intent.source,
                "issued_at": int(install_intent.created_at.timestamp()),
                "state_verified": True,
            } if state_verified and install_intent else {
                "source": "callback_without_verified_state",
                "state_verified": False,
            },
        )
        if state_verified and install_intent:
            consume_github_install_intent(db, intent=install_intent)
    except Exception:
        record_audit_log(
            db,
            request,
            actor_type="user" if auth.user else "anonymous",
            actor_id=auth.user.id if auth.user else None,
            workspace_id=None,
            target_type="github_installation",
            target_id=None,
            action="github_callback_failed",
            metadata={"reason": "installation_resolution_failed"},
        )
        db.commit()
        set_flash(request, "error", "GitHub installation の確認に失敗しました。もう一度やり直してください。")
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)

    claim_url = f"/integrations/github/claim/{result.claim_token}"
    store_github_claim_context(request, claim_token=result.claim_token, next_path=claim_url)
    record_audit_log(
        db,
        request,
        actor_type="user" if auth.user else "anonymous",
        actor_id=auth.user.id if auth.user else None,
        workspace_id=None,
        target_type="pending_claim",
        target_id=result.pending_claim.id,
        action="github_callback_context_created",
        metadata={"installation_id": installation.installation_id, "github_user_id": github_user["id"]},
    )
    db.commit()

    if auth.user is None:
        set_flash(request, "success", "GitHub 側の確認が完了しました。Hotdock にログインまたは新規登録すると claim を再開できます。")
        return RedirectResponse(url=build_login_redirect(claim_url), status_code=status.HTTP_303_SEE_OTHER)

    workspace_slug = None
    if state_verified and install_intent:
        workspace_slug = _workspace_slug_from_install_intent(
            db,
            request,
            auth,
            {
                "workspace_slug": install_intent.workspace_slug,
                "user_id": install_intent.user_id,
            },
        )
    if workspace_slug:
        workspace = db.scalar(select(Workspace).where(Workspace.slug == workspace_slug))
        if workspace is not None:
            try:
                pending_claim = select_claim_workspace(
                    db,
                    request,
                    pending_claim=result.pending_claim,
                    user=auth.user,
                    workspace=workspace,
                )
                installation = finalize_github_claim(db, request, pending_claim=pending_claim, user=auth.user)
                sync_claimed_installation_repositories(db, installation)
                return RedirectResponse(url=f"/workspaces/{workspace.slug}/dashboard", status_code=status.HTTP_303_SEE_OTHER)
            except Exception:
                set_flash(request, "error", "workspace への自動紐付けに失敗しました。workspace を選択して再開してください。")

    if state_verified:
        set_flash(
            request,
            "success",
            "GitHub 側の確認は完了しました。workspace を選んで claim を完了してください。",
        )
    else:
        set_flash(
            request,
            "success",
            "GitHub 側の確認は完了しました。workspace を選んで claim を完了してください。GitHub callback に state が含まれなかったため、自動確定は行っていません。",
        )
    return RedirectResponse(url=claim_url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/invitations/{invitation_token}", name="hotdock-invitation-show")
async def invitation_show(invitation_token: str, request: Request, db: Session = Depends(get_db)):
    context = auth_page_context(
        request,
        db,
        page_title="招待を受ける | Hotdock",
        page_description="workspace 招待を受けます。",
        page_heading="招待を受ける",
        active_nav="invitation",
        body_class="page-auth page-invitation",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Invitation", "href": request.url.path}],
    )
    invitation = _invitation_by_token(db, invitation_token)
    if invitation is None:
        set_flash(request, "error", "招待が見つかりません。")
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    workspace = db.get(Workspace, invitation.workspace_id)
    context.update({"invitation": invitation, "workspace": workspace, "invitation_token": invitation_token})
    return render_auth("hotdock/auth/invitation_accept.html", context)


@router.post("/invitations/{invitation_token}/accept", name="hotdock-invitation-accept")
async def invitation_accept(invitation_token: str, request: Request, db: Session = Depends(get_db), csrf_token: str = Form(...)):
    auth, redirect = _require_login(request, db, f"/invitations/{invitation_token}")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/invitations/{invitation_token}", status_code=status.HTTP_303_SEE_OTHER)
    invitation = _invitation_by_token(db, invitation_token)
    if invitation is None:
        set_flash(request, "error", "招待が見つかりません。")
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    membership = accept_workspace_invitation(db, request, invitation=invitation, user=auth.user)
    workspace = db.get(Workspace, membership.workspace_id)
    return RedirectResponse(url=f"/workspaces/{workspace.slug}/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/workspaces/{workspace_slug}/members/invite", name="hotdock-workspace-member-invite")
async def workspace_member_invite(
    workspace_slug: str,
    request: Request,
    db: Session = Depends(get_db),
    email: str = Form(...),
    role: str = Form(...),
    csrf_token: str = Form(...),
):
    auth, redirect = _require_login(request, db, f"/workspaces/{workspace_slug}/members")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)
    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="owner")
    _, token = invite_workspace_member(
        db,
        request,
        workspace=access.workspace,
        inviter=auth.user,
        inviter_membership=access.membership,
        email=email,
        role=role,
    )
    set_flash(request, "success", f"招待リンクを発行しました: /invitations/{token}")
    return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/workspaces/{workspace_slug}/members/{member_id}/revoke", name="hotdock-workspace-member-revoke")
async def workspace_member_revoke(
    workspace_slug: str,
    member_id: str,
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(...),
):
    auth, redirect = _require_login(request, db, f"/workspaces/{workspace_slug}/members")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)
    from app.hotdock.services.workspaces import revoke_workspace_member

    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="owner")
    member = db.get(WorkspaceMember, member_id)
    if member is None or member.workspace_id != access.workspace.id:
        set_flash(request, "error", "member が見つかりません。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)
    try:
        revoke_workspace_member(
            db,
            request,
            workspace=access.workspace,
            actor=auth.user,
            actor_membership=access.membership,
            member=member,
        )
    except HTTPException as exc:
        set_flash(request, "error", str(exc.detail))
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)
    set_flash(request, "success", "member を revoke しました。")
    return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/workspaces/{workspace_slug}/members/{member_id}/role", name="hotdock-workspace-member-role-update")
async def workspace_member_role_update(
    workspace_slug: str,
    member_id: str,
    request: Request,
    db: Session = Depends(get_db),
    role: str = Form(...),
    csrf_token: str = Form(...),
):
    auth, redirect = _require_login(request, db, f"/workspaces/{workspace_slug}/members")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)

    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="owner")
    member = db.get(WorkspaceMember, member_id)
    if member is None or member.workspace_id != access.workspace.id:
        set_flash(request, "error", "member が見つかりません。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)

    try:
        update_workspace_member_role(
            db,
            request,
            workspace=access.workspace,
            actor=auth.user,
            actor_membership=access.membership,
            member=member,
            new_role=role,
        )
    except HTTPException as exc:
        set_flash(request, "error", str(exc.detail))
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)

    set_flash(request, "success", "member の role を更新しました。")
    return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/workspaces/{workspace_slug}/leave", name="hotdock-workspace-leave")
async def workspace_leave(
    workspace_slug: str,
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(...),
):
    auth, redirect = _require_login(request, db, f"/workspaces/{workspace_slug}/members")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)

    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="viewer")
    try:
        leave_workspace(
            db,
            request,
            workspace=access.workspace,
            member=access.membership,
            actor=auth.user,
        )
    except HTTPException as exc:
        set_flash(request, "error", str(exc.detail))
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)

    set_flash(request, "success", f"{access.workspace.name} から退会しました。")
    next_workspace = default_workspace_for_user(db, auth.user.id)
    if next_workspace is not None:
        return RedirectResponse(url=f"/workspaces/{next_workspace.slug}/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url="/workspaces/new", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/workspaces/{workspace_slug}/delete", name="hotdock-workspace-delete")
async def workspace_delete(
    workspace_slug: str,
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(...),
    confirm_slug: str = Form(""),
):
    auth, redirect = _require_login(request, db, f"/workspaces/{workspace_slug}/settings")
    if redirect:
        return redirect
    if not hmac.compare_digest(csrf_token, auth.csrf_token):
        set_flash(request, "error", "セッションが確認できませんでした。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/settings", status_code=status.HTTP_303_SEE_OTHER)
    if confirm_slug.strip() != workspace_slug:
        set_flash(request, "error", "退会確認のため、workspace slug を正しく入力してください。")
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/settings", status_code=status.HTTP_303_SEE_OTHER)

    access = resolve_workspace_access(db, request, user=auth.user, workspace_slug=workspace_slug, required_role="owner")
    workspace_name = access.workspace.name
    try:
        delete_workspace_and_related_data(
            db,
            request,
            workspace=access.workspace,
            actor=auth.user,
            actor_membership=access.membership,
        )
    except HTTPException as exc:
        set_flash(request, "error", str(exc.detail))
        return RedirectResponse(url=f"/workspaces/{workspace_slug}/settings", status_code=status.HTTP_303_SEE_OTHER)

    set_flash(request, "success", f"{workspace_name} を退会し、workspace データを削除しました。")
    next_workspace = default_workspace_for_user(db, auth.user.id)
    if next_workspace is not None:
        return RedirectResponse(url=f"/workspaces/{next_workspace.slug}/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url="/workspaces/new", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/webhooks/github", name="hotdock-github-webhook")
async def github_webhook(request: Request, db: Session = Depends(get_db)):
    body = await request.body()
    signature_valid = verify_github_webhook_signature(body, request.headers.get("x-hub-signature-256"))
    delivery_id = request.headers.get("x-github-delivery", "")
    event_name = request.headers.get("x-github-event", "")

    if not signature_valid:
        record_audit_log(
            db,
            request,
            actor_type="anonymous",
            actor_id=None,
            workspace_id=None,
            target_type="webhook_event",
            target_id=delivery_id or None,
            action="webhook_signature_failed",
            metadata={"event": event_name},
        )
        db.commit()
        return JSONResponse({"detail": "invalid signature"}, status_code=401)

    if not delivery_id or not event_name:
        return JSONResponse({"detail": "missing webhook headers"}, status_code=400)

    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        return JSONResponse({"detail": "invalid payload"}, status_code=400)

    action_name = payload.get("action") if isinstance(payload, dict) else None
    installation_id = ((payload.get("installation") or {}).get("id")) if isinstance(payload, dict) else None

    recorded = record_webhook_event(
        db,
        delivery_id=delivery_id,
        event_name=event_name,
        action_name=action_name,
        installation_id=installation_id,
        payload=payload,
        payload_sha256=hashlib.sha256(body).hexdigest(),
        signature_valid=signature_valid,
    )
    if recorded is None:
        record_audit_log(
            db,
            request,
            actor_type="github_app",
            actor_id=None,
            workspace_id=None,
            target_type="webhook_event",
            target_id=delivery_id,
            action="webhook_replay_detected",
            metadata={"event": event_name},
        )
        db.commit()
        return JSONResponse({"status": "replayed"})

    try:
        push_result = None
        if event_name == "installation":
            installation = sync_installation_event(db, payload)
            sync_claimed_installation_repositories(db, installation)
        elif event_name == "installation_repositories":
            sync_installation_repositories_event(db, payload)
        elif event_name == "push":
            push_result = await record_push_event(db, request, delivery_id=delivery_id, payload=payload)
    except Exception as exc:
        if recorded:
            mark_webhook_event_failed(db, recorded.id, str(exc))
        return JSONResponse({"status": "accepted_with_error", "detail": "processing failed"})

    if recorded:
        event_row = db.get(GithubWebhookEvent, recorded.id)
        if event_row:
            if push_result and push_result.get("status") == "accepted_with_error":
                event_row.processing_status = "failed"
                event_row.error_message = str(push_result.get("reason") or "compare failed")[:2000]
            else:
                event_row.processing_status = "processed"
                event_row.processed_at = event_row.processed_at or event_row.received_at
            db.commit()
    if push_result and push_result.get("status") == "accepted_with_error":
        return JSONResponse({"status": "accepted_with_error", "detail": push_result.get("reason")})
    return JSONResponse({"status": "ok"})


@router.get("/install/github", name="hotdock-install-github")
async def install_github(request: Request, db: Session = Depends(get_db)):
    context = auth_page_context(
        request,
        db,
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
            "support_copy": "GitHub callback で installation と GitHub user authorization を受け取り、Hotdock login 後に workspace claim を完了します。",
            "install_available": bool(settings.github_app_install_url or settings.github_app_slug),
            "install_href": "/integrations/github/install/start",
        }
    )
    return render_auth("hotdock/auth/install_github.html", context)
