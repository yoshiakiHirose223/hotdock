from __future__ import annotations

import hmac
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Request
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
    create_pending_github_claim,
    load_pending_claim_by_token,
    record_push_event,
    record_webhook_event,
    select_claim_workspace,
    set_pending_oauth_state,
    sync_claimed_installation_repositories,
    sync_installation_event,
    sync_installation_repositories_event,
    verify_github_webhook_signature,
    verify_pending_oauth_state,
)
from app.hotdock.services.security import generate_token
from app.hotdock.services.workspaces import (
    accept_workspace_invitation,
    create_user,
    create_workspace,
    invite_workspace_member,
    list_user_workspaces,
    resolve_workspace_access,
)
from app.models.github_installation import GithubInstallation
from app.models.github_pending_claim import GithubPendingClaim
from app.models.github_webhook_event import GithubWebhookEvent
from app.models.workspace import Workspace
from app.models.workspace_invitation import WorkspaceInvitation
from app.models.workspace_member import WorkspaceMember

settings = get_settings()
router = APIRouter()


def render_auth(template_name: str, context: dict[str, Any]):
    request = context["request"]
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


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
    expected = get_or_create_anon_csrf(request)
    if not token or not hmac.compare_digest(token, expected):
        raise ValueError("Invalid CSRF token")


def _require_login(request: Request, db: Session, next_path: str):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        set_after_login(request, next_path)
        return None, RedirectResponse(url=build_login_redirect(next_path), status_code=status.HTTP_303_SEE_OTHER)
    return auth, None


def _github_authorize_url(state_token: str) -> str:
    if settings.github_mock_oauth_enabled:
        return f"/integrations/github/authorize/callback?{urlencode({'code': 'mock-code', 'state': state_token})}"
    if not settings.github_app_client_id:
        raise ValueError("GitHub OAuth is not configured")
    query = urlencode(
        {
            "client_id": settings.github_app_client_id,
            "state": state_token,
            "redirect_uri": f"{settings.site_url}/integrations/github/authorize/callback",
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
            GithubPendingClaim.status.in_(["pending", "workspace_selected", "awaiting_github_auth"]),
        )
    ).all()
    context.update({"workspace": access.workspace, "installations": installations, "pending_claims": pending_claims})
    return render_auth("hotdock/auth/github_settings.html", context)


@router.get("/integrations/github/install/start", name="hotdock-github-install-start")
async def github_install_start(request: Request, db: Session = Depends(get_db)):
    auth, access, redirect = _resolve_workspace_target(request, db, required_role="admin")
    if redirect:
        return redirect
    install_url = settings.github_app_install_url or (
        f"https://github.com/apps/{settings.github_app_slug}/installations/new" if settings.github_app_slug else None
    )
    if not install_url:
        set_flash(request, "error", "GitHub App の install URL が設定されていません。")
        return RedirectResponse(
            url=f"/settings/integrations/github?{urlencode({'workspace': access.workspace.slug})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    request.session["github_install_intent"] = {
        "workspace_slug": access.workspace.slug,
        "user_id": auth.user.id,
        "nonce": generate_token(),
    }
    return RedirectResponse(url=install_url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/integrations/github/setup", name="hotdock-github-setup")
async def github_setup(request: Request, db: Session = Depends(get_db), installation_id: int = 0, setup_action: str = "install"):
    if installation_id <= 0:
        set_flash(request, "error", "installation_id が不正です。")
        return RedirectResponse(url="/install/github", status_code=status.HTTP_303_SEE_OTHER)
    result = create_pending_github_claim(
        db,
        request,
        installation_id=installation_id,
        initiated_via="setup_url",
        setup_payload={"setup_action": setup_action},
    )
    claim_url = f"/integrations/github/claim/{result.claim_token}"
    store_github_claim_context(request, claim_token=result.claim_token, next_path=claim_url)
    return RedirectResponse(url=claim_url, status_code=status.HTTP_303_SEE_OTHER)


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
            "support_copy": "setup URL の installation_id 単独では ownership を確定しません。workspace 選択と GitHub user authorization を通した後に claim が完了します。",
            "claim_token": claim_token,
            "pending_claim": pending_claim,
            "installation": installation,
            "workspace": workspace,
            "selected_workspace_id": selected_workspace_id,
            "available_workspaces": available_workspaces,
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
    select_claim_workspace(db, request, pending_claim=pending_claim, user=auth.user, workspace=workspace)
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
    state_token = generate_token()
    request.session["github_oauth_state"] = {"state": state_token, "claim_token": token}
    set_pending_oauth_state(db, pending_claim, state_token)
    try:
        authorize_url = _github_authorize_url(state_token)
    except ValueError:
        set_flash(request, "error", "GitHub OAuth の設定が不足しています。")
        return RedirectResponse(url=f"/integrations/github/claim/{token}", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=authorize_url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/integrations/github/authorize/callback", name="hotdock-github-authorize-callback")
async def github_authorize_callback(request: Request, db: Session = Depends(get_db), code: str = "", state: str = ""):
    auth, redirect = _require_login(request, db, "/dashboard")
    if redirect:
        return redirect
    oauth_state = request.session.pop("github_oauth_state", None)
    if not oauth_state or oauth_state.get("state") != state:
        set_flash(request, "error", "GitHub authorization state の検証に失敗しました。")
        return RedirectResponse(url="/settings/integrations/github", status_code=status.HTTP_303_SEE_OTHER)
    pending_claim = load_pending_claim_by_token(db, oauth_state["claim_token"])
    if pending_claim is None or not verify_pending_oauth_state(pending_claim, state):
        set_flash(request, "error", "claim 状態が一致しません。")
        return RedirectResponse(url="/settings/integrations/github", status_code=status.HTTP_303_SEE_OTHER)

    oauth_client = GithubOAuthClient()
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
            url=f"/integrations/github/claim/{oauth_state['claim_token']}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    workspace = db.get(Workspace, installation.claimed_workspace_id)
    return RedirectResponse(
        url=f"/workspaces/{workspace.slug}/dashboard",
        status_code=status.HTTP_303_SEE_OTHER,
    )


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
    revoke_workspace_member(
        db,
        request,
        workspace=access.workspace,
        actor=auth.user,
        actor_membership=access.membership,
        member=member,
    )
    set_flash(request, "success", "member を revoke しました。")
    return RedirectResponse(url=f"/workspaces/{workspace_slug}/members", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/webhooks/github", name="hotdock-github-webhook")
async def github_webhook(request: Request, db: Session = Depends(get_db)):
    body = await request.body()
    payload = await request.json()
    signature_valid = verify_github_webhook_signature(body, request.headers.get("x-hub-signature-256"))
    delivery_id = request.headers.get("x-github-delivery", "")
    event_name = request.headers.get("x-github-event", "")
    action_name = payload.get("action")
    installation_id = ((payload.get("installation") or {}).get("id")) if isinstance(payload, dict) else None

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

    recorded = record_webhook_event(
        db,
        delivery_id=delivery_id,
        event_name=event_name,
        action_name=action_name,
        installation_id=installation_id,
        payload=payload,
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

    if event_name == "installation":
        installation = sync_installation_event(db, payload)
        sync_claimed_installation_repositories(db, installation)
    elif event_name == "installation_repositories":
        sync_installation_repositories_event(db, payload)
    elif event_name == "push":
        record_push_event(db, payload)

    if recorded:
        event_row = db.get(GithubWebhookEvent, recorded.id)
        if event_row:
            event_row.processing_status = "processed"
            event_row.processed_at = event_row.processed_at or event_row.received_at
            db.commit()
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
            "support_copy": "公開後は setup URL から pending claim を作成し、Hotdock login と GitHub user authorization を通して workspace claim を完了します。",
        }
    )
    return render_auth("hotdock/auth/install_github.html", context)
