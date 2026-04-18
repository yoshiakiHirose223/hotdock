import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette import status

from app.core.config import get_settings
from app.core.database import get_db
from app.hotdock.data.compare import COMPARE_COLUMNS, COMPARE_ROWS
from app.hotdock.data.contact import CONTACT_INFO, CONTACT_SUBJECTS
from app.hotdock.data.content import GLOBAL_CTA, HOME_PAGE, HOW_IT_WORKS_CONTENT, SECURITY_CONTENT
from app.hotdock.data.docs import DOCS_SECTIONS
from app.hotdock.data.faq import FAQ_CATEGORIES
from app.hotdock.data.features import (
    FEATURES,
    HOME_FEATURE_HIGHLIGHTS,
    HOME_PROBLEMS,
    HOME_SOLUTIONS,
    START_PATHS,
)
from app.hotdock.data.integrations import GITHUB_APP_PAGE, INTEGRATIONS, INTEGRATION_STATUS_STYLES
from app.hotdock.data.pricing import PRICING_COMPARISON, PRICING_NOTES, PRICING_PLANS
from app.hotdock.services.auth import attach_auth_context, default_workspace_for_user, get_flash, set_flash, verify_form_csrf
from app.hotdock.services.context import build_public_context
from app.hotdock.services.github import delete_all_non_article_data
from app.models.audit_log import AuditLog
from app.models.branch_event import BranchEvent
from app.models.github_webhook_event import GithubWebhookEvent

router = APIRouter()
settings = get_settings()


def _json_log_default(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def format_log_details(details: dict[str, Any]) -> str:
    return json.dumps(details, ensure_ascii=False, indent=2, default=_json_log_default)


def render_public(template_name: str, context: dict[str, Any]):
    request = context["request"]
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


def public_page_context(request: Request, db: Session, **kwargs: Any) -> dict[str, Any]:
    auth = attach_auth_context(request, db)
    context = build_public_context(request, **kwargs)
    dashboard_href = "/dashboard"
    if auth.user is not None:
        default_workspace = default_workspace_for_user(db, auth.user.id)
        if default_workspace is not None:
            dashboard_href = f"/workspaces/{default_workspace.slug}/dashboard"
    context.update(
        {
            "current_user": auth.user,
            "dashboard_href": dashboard_href,
            "csrf_token": auth.csrf_token,
            "flash": get_flash(request),
            "show_github_reset": settings.app_env != "production",
        }
    )
    return context


@router.get("/", name="hotdock-home")
async def home(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="Hotdock | Git の衝突を、早く、わかりやすく。",
        page_description="GitHub App と SaaS の2つの入口を用意し、最終的には共通ダッシュボードへつながる Hotdock のトップページ。",
        page_heading="Hotdock",
        active_nav="home",
        body_class="page-home",
    )
    context.update(
        {
            "home": HOME_PAGE,
            "problems": HOME_PROBLEMS,
            "solutions": HOME_SOLUTIONS,
            "start_paths": START_PATHS,
            "feature_highlights": HOME_FEATURE_HIGHLIGHTS,
            "faq_categories": FAQ_CATEGORIES[:2],
            "pricing_plans": PRICING_PLANS[:3],
            "global_cta": GLOBAL_CTA,
        }
    )
    return render_public("hotdock/public/home.html", context)


@router.post("/debug/all-delete", name="hotdock-debug-all-delete")
async def debug_all_delete(
    request: Request,
    db: Session = Depends(get_db),
    csrf_token: str = Form(...),
):
    if settings.app_env == "production":
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    try:
        verify_form_csrf(request, csrf_token, db)
    except Exception:
        set_flash(request, "error", "リセット要求を確認できませんでした。")
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    auth = attach_auth_context(request, db)
    actor_type = "user" if auth.user else "anonymous"
    actor_id = auth.user.id if auth.user else None
    actor_label = auth.user.email if auth.user else "anonymous"

    try:
        result = delete_all_non_article_data(
            db,
            request,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_label=actor_label,
        )
    except Exception:
        set_flash(request, "error", "全データ削除に失敗しました。")
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    request.session.clear()
    set_flash(
        request,
        "success",
        "記事DBを除く全データを削除しました。"
        f" table={result['deleted_tables']} / row={result['deleted_rows']}",
    )
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    response.delete_cookie(settings.auth_cookie_name, path="/")
    response.delete_cookie(settings.csrf_cookie_name, path="/")
    response.delete_cookie("session", path="/")
    return response


@router.get("/features", name="hotdock-features")
async def features(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="機能一覧 | Hotdock",
        page_description="競合候補検知、状態変化通知、履歴整理、通知手段管理など Hotdock の主要機能一覧。",
        page_heading="機能一覧",
        active_nav="features",
        body_class="page-features",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Features", "href": "/features"}],
    )
    context.update({"features": FEATURES, "global_cta": GLOBAL_CTA})
    return render_public("hotdock/public/features.html", context)


@router.get("/integrations", name="hotdock-integrations")
async def integrations(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="連携一覧 | Hotdock",
        page_description="Git 連携、GitHub App、Slack、Chatwork、メール通知など Hotdock の連携一覧。",
        page_heading="連携一覧",
        active_nav="integrations",
        body_class="page-integrations",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Integrations", "href": "/integrations"}],
    )
    context.update(
        {
            "integrations": INTEGRATIONS,
            "status_styles": INTEGRATION_STATUS_STYLES,
            "global_cta": GLOBAL_CTA,
        }
    )
    return render_public("hotdock/public/integrations.html", context)


@router.get("/integrations/github-app", name="hotdock-github-app")
async def github_app(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="GitHub App 導入予定 | Hotdock",
        page_description="Hotdock の GitHub App はまだ未提供です。将来の導入フローと共通ダッシュボードへの接続方針を説明します。",
        page_heading="GitHub App 導入予定",
        active_nav="integrations",
        body_class="page-github-app",
        breadcrumbs=[
            {"label": "Home", "href": "/"},
            {"label": "Integrations", "href": "/integrations"},
            {"label": "GitHub App", "href": "/integrations/github-app"},
        ],
    )
    context.update({"github_app_page": GITHUB_APP_PAGE, "global_cta": GLOBAL_CTA})
    return render_public("hotdock/public/github_app.html", context)


@router.get("/how-it-works", name="hotdock-how-it-works")
async def how_it_works(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="仕組み | Hotdock",
        page_description="更新検知、競合候補抽出、状態変化通知、git 連携の意味を整理した Hotdock の仕組みページ。",
        page_heading="Hotdock の仕組み",
        active_nav="how-it-works",
        body_class="page-how-it-works",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "How it works", "href": "/how-it-works"}],
    )
    context.update({"how_it_works": HOW_IT_WORKS_CONTENT, "global_cta": GLOBAL_CTA})
    return render_public("hotdock/public/how_it_works.html", context)


@router.get("/pricing", name="hotdock-pricing")
async def pricing(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="料金 | Hotdock",
        page_description="GitHub App Lite、SaaS Starter、SaaS Team、SaaS Business の仮プラン一覧。",
        page_heading="料金",
        active_nav="pricing",
        body_class="page-pricing",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Pricing", "href": "/pricing"}],
    )
    context.update(
        {
            "pricing_plans": PRICING_PLANS,
            "pricing_comparison": PRICING_COMPARISON,
            "pricing_notes": PRICING_NOTES,
            "global_cta": GLOBAL_CTA,
        }
    )
    return render_public("hotdock/public/pricing.html", context)


@router.get("/security", name="hotdock-security")
async def security(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="セキュリティ | Hotdock",
        page_description="権限最小化、認証・認可、データ取り扱い、git 連携の基本方針を整理したページ。",
        page_heading="セキュリティ方針",
        active_nav="security",
        body_class="page-security",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Security", "href": "/security"}],
    )
    context.update({"security_sections": SECURITY_CONTENT, "contact_info": CONTACT_INFO, "global_cta": GLOBAL_CTA})
    return render_public("hotdock/public/security.html", context)


@router.get("/faq", name="hotdock-faq")
async def faq(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="FAQ | Hotdock",
        page_description="GitHub App と SaaS の違い、git 連携の意味、通知対応などの FAQ。",
        page_heading="FAQ",
        active_nav="faq",
        body_class="page-faq",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "FAQ", "href": "/faq"}],
    )
    context.update({"faq_categories": FAQ_CATEGORIES, "global_cta": GLOBAL_CTA})
    return render_public("hotdock/public/faq.html", context)


@router.get("/docs", name="hotdock-docs")
async def docs(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="Docs | Hotdock",
        page_description="Hotdock のドキュメント入口。はじめに、SaaS 初期設定、git 連携の考え方などを整理します。",
        page_heading="Docs",
        active_nav="docs",
        body_class="page-docs",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Docs", "href": "/docs"}],
    )
    context.update({"doc_sections": DOCS_SECTIONS, "global_cta": GLOBAL_CTA})
    return render_public("hotdock/public/docs.html", context)


@router.get("/contact", name="hotdock-contact")
async def contact(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="Contact | Hotdock",
        page_description="導入相談、技術的質問、料金相談、連携相談、不具合報告の問い合わせページ。",
        page_heading="問い合わせ",
        active_nav="contact",
        body_class="page-contact",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Contact", "href": "/contact"}],
    )
    context.update({"contact_info": CONTACT_INFO, "contact_subjects": CONTACT_SUBJECTS})
    return render_public("hotdock/public/contact.html", context)


@router.get("/compare", name="hotdock-compare")
async def compare(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="GitHub App と SaaS の比較 | Hotdock",
        page_description="GitHub App と SaaS は別サービスではなく、始め方の違いであることを整理した比較ページ。",
        page_heading="GitHub App と SaaS の比較",
        active_nav="pricing",
        body_class="page-compare",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Compare", "href": "/compare"}],
    )
    context.update({"compare_columns": COMPARE_COLUMNS, "compare_rows": COMPARE_ROWS, "start_paths": START_PATHS, "global_cta": GLOBAL_CTA})
    return render_public("hotdock/public/compare.html", context)


@router.get("/logs", name="hotdock-logs")
async def logs(request: Request, db: Session = Depends(get_db)):
    context = public_page_context(
        request,
        db,
        page_title="Logs | Hotdock",
        page_description="Hotdock の監査ログ、GitHub webhook、branch event をまとめて確認するページ。",
        page_heading="Logs",
        active_nav="logs",
        body_class="page-logs",
        breadcrumbs=[{"label": "Home", "href": "/"}, {"label": "Logs", "href": "/logs"}],
    )

    audit_logs = db.scalars(select(AuditLog).order_by(AuditLog.created_at.desc())).all()
    webhook_events = db.scalars(select(GithubWebhookEvent).order_by(GithubWebhookEvent.received_at.desc())).all()
    branch_events = db.scalars(select(BranchEvent).order_by(BranchEvent.occurred_at.desc())).all()

    timeline: list[dict[str, Any]] = []

    for item in audit_logs:
        timeline.append(
            {
                "at": item.created_at,
                "source": "audit",
                "title": item.action,
                "summary": f"{item.actor_type} / {item.target_type}",
                "details": format_log_details(
                    {
                    "actor_id": item.actor_id,
                    "workspace_id": item.workspace_id,
                    "target_id": item.target_id,
                    "metadata": item.event_metadata,
                    }
                ),
            }
        )

    for item in webhook_events:
        timeline.append(
            {
                "at": item.received_at,
                "source": "webhook",
                "title": f"{item.event_name}{' / ' + item.action_name if item.action_name else ''}",
                "summary": f"installation={item.installation_id or '-'} status={item.processing_status}",
                "details": format_log_details(
                    {
                    "delivery_id": item.delivery_id,
                    "workspace_id": item.workspace_id,
                    "signature_valid": item.signature_valid,
                    "error_message": item.error_message,
                    }
                ),
            }
        )

    for item in branch_events:
        timeline.append(
            {
                "at": item.occurred_at,
                "source": "branch",
                "title": item.event_type,
                "summary": f"repo={item.repository_id} branch={item.branch_id or '-'}",
                "details": format_log_details(
                    {
                    "delivery_id": item.webhook_delivery_id,
                    "before_sha": item.before_sha,
                    "after_sha": item.after_sha,
                    "created": item.created,
                    "deleted": item.deleted,
                    "forced": item.forced,
                    "compare_requested": item.compare_requested,
                    "compare_completed": item.compare_completed,
                    "compare_error": item.compare_error,
                    "compare_error_message": item.compare_error_message,
                    "reason": item.reason,
                    }
                ),
            }
        )

    timeline.sort(key=lambda row: row["at"], reverse=True)
    context.update({"timeline_logs": timeline})
    return render_public("hotdock/public/logs.html", context)
