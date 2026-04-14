from typing import Any

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

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
from app.hotdock.services.auth import attach_auth_context, default_workspace_for_user
from app.hotdock.services.context import build_public_context

router = APIRouter()


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
