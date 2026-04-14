from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from starlette import status

from app.core.database import get_db
from app.hotdock.data.dashboard_mock import (
    APP_OVERVIEW,
    BILLING_OVERVIEW,
    INTEGRATION_STATUS,
    NOTIFICATION_CHANNELS,
    RECENT_CONFLICTS,
    SETTINGS_SECTIONS,
    SUMMARY_CARDS,
)
from app.hotdock.services.auth import attach_auth_context, get_flash
from app.hotdock.services.context import build_app_context
from app.hotdock.services.projects import (
    add_project_bookmark,
    get_bookmark_redirect,
    get_branch_filters,
    get_branch_summaries,
    get_current_path,
    get_project_conflicts,
    get_project_or_404,
    get_project_settings,
    get_project_tabs,
    is_project_bookmarked,
    list_projects_view,
    list_sidebar_bookmarks,
    remove_project_bookmark,
)

router = APIRouter(prefix="/app")


def render_app(template_name: str, context: dict[str, Any]):
    request = context["request"]
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


def build_page_context(
    request: Request,
    db: Session,
    *,
    page_title: str,
    page_description: str,
    page_heading: str,
    active_nav: str,
    body_class: str,
    breadcrumbs: list[dict[str, str]],
    active_project_id: int | None = None,
    active_tab: str | None = None,
) -> dict[str, Any]:
    auth = attach_auth_context(request, db)
    context = build_app_context(
        request,
        page_title=page_title,
        page_description=page_description,
        page_heading=page_heading,
        active_nav=active_nav,
        body_class=body_class,
        breadcrumbs=breadcrumbs,
    )
    context["sidebar_bookmarks"] = list_sidebar_bookmarks(
        db,
        active_project_id=active_project_id,
        active_tab=active_tab,
    )
    context["current_user"] = auth.user
    context["flash"] = get_flash(request)
    return context


def build_project_detail_context(
    request: Request,
    db: Session,
    project_id: int,
    *,
    active_tab: str,
    body_class: str,
    page_title_suffix: str,
    page_description: str,
) -> dict[str, Any]:
    project = get_project_or_404(project_id)
    context = build_page_context(
        request,
        db,
        page_title=f"{project['name']} | {page_title_suffix} | Hotdock",
        page_description=page_description,
        page_heading=str(project["name"]),
        active_nav="projects",
        body_class=body_class,
        breadcrumbs=[
            {"label": "App", "href": "/app"},
            {"label": "Projects", "href": "/app/projects"},
            {"label": str(project["name"]), "href": str(project["urls"]["overview"])},
        ],
        active_project_id=project_id,
        active_tab=active_tab,
    )
    context.update(
        {
            "project": project,
            "active_tab": active_tab,
            "project_tabs": get_project_tabs(project, active_tab),
            "is_bookmarked": is_project_bookmarked(db, project_id),
            "current_path": get_current_path(request.url.path, request.url.query),
        }
    )
    return context


@router.get("", name="hotdock-app-index")
async def app_index(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="App Overview | Hotdock",
        page_description="Hotdock 管理画面の骨組みトップ。共通ダッシュボードへつながる前提を示します。",
        page_heading="Overview",
        active_nav="app-index",
        body_class="page-app page-app-index",
        breadcrumbs=[{"label": "App", "href": "/app"}],
    )
    context.update({"overview": APP_OVERVIEW, "summary_cards": SUMMARY_CARDS[:3]})
    return render_app("hotdock/app/index.html", context)


@router.get("/dashboard", name="hotdock-app-dashboard")
async def dashboard(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="Dashboard | Hotdock",
        page_description="競合候補、通知状況、連携状況を表示する Hotdock ダッシュボード骨組み。",
        page_heading="Dashboard",
        active_nav="dashboard",
        body_class="page-app page-app-dashboard",
        breadcrumbs=[{"label": "App", "href": "/app"}, {"label": "Dashboard", "href": "/app/dashboard"}],
    )
    context.update(
        {
            "summary_cards": SUMMARY_CARDS,
            "recent_conflicts": RECENT_CONFLICTS,
            "integration_status": INTEGRATION_STATUS,
            "notification_channels": NOTIFICATION_CHANNELS[:3],
        }
    )
    return render_app("hotdock/app/dashboard.html", context)


@router.get("/projects", name="hotdock-app-projects")
async def projects(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="Projects | Hotdock",
        page_description="監視対象プロジェクト一覧の骨組み。",
        page_heading="Projects",
        active_nav="projects",
        body_class="page-app page-app-projects",
        breadcrumbs=[{"label": "App", "href": "/app"}, {"label": "Projects", "href": "/app/projects"}],
    )
    context.update({"projects": list_projects_view()})
    return render_app("hotdock/app/projects/index.html", context)


@router.get("/projects/{project_id}", name="hotdock-app-project-overview")
async def project_overview(project_id: int, request: Request, db: Session = Depends(get_db)):
    project = get_project_or_404(project_id)
    return RedirectResponse(url=str(project["urls"]["branches"]), status_code=status.HTTP_303_SEE_OTHER)


@router.get("/projects/{project_id}/branches", name="hotdock-app-project-branches")
async def project_branches(project_id: int, request: Request, db: Session = Depends(get_db)):
    filters = get_branch_filters(request.query_params)
    context = build_project_detail_context(
        request,
        db,
        project_id,
        active_tab="branches",
        body_class="page-app page-app-project-branches",
        page_title_suffix="Branches",
        page_description="Project 詳細の Branches タブ。",
    )
    context.update(get_branch_summaries(project_id, filters))
    context["filters"] = filters
    return render_app("hotdock/app/projects/branches.html", context)


@router.get("/projects/{project_id}/conflicts", name="hotdock-app-project-conflicts")
async def project_conflicts(project_id: int, request: Request, db: Session = Depends(get_db)):
    context = build_project_detail_context(
        request,
        db,
        project_id,
        active_tab="conflicts",
        body_class="page-app page-app-project-conflicts",
        page_title_suffix="Conflicts",
        page_description="Project 詳細の Conflicts タブ。",
    )
    context["project_conflicts"] = get_project_conflicts(project_id)
    return render_app("hotdock/app/projects/conflicts.html", context)


@router.get("/projects/{project_id}/settings", name="hotdock-app-project-settings")
async def project_settings(project_id: int, request: Request, db: Session = Depends(get_db)):
    context = build_project_detail_context(
        request,
        db,
        project_id,
        active_tab="settings",
        body_class="page-app page-app-project-settings",
        page_title_suffix="Settings",
        page_description="Project 詳細の Settings タブ。",
    )
    context["project_settings"] = get_project_settings(project_id)
    return render_app("hotdock/app/projects/settings.html", context)


@router.post("/projects/{project_id}/bookmark", name="hotdock-app-project-bookmark")
async def bookmark_project(
    project_id: int,
    next: str = Form(""),
    db: Session = Depends(get_db),
):
    project = get_project_or_404(project_id)
    add_project_bookmark(db, project_id)
    redirect_to = get_bookmark_redirect(next, str(project["urls"]["branches"]))
    return RedirectResponse(url=redirect_to, status_code=status.HTTP_303_SEE_OTHER)


@router.post("/projects/{project_id}/unbookmark", name="hotdock-app-project-unbookmark")
async def unbookmark_project(
    project_id: int,
    next: str = Form(""),
    db: Session = Depends(get_db),
):
    project = get_project_or_404(project_id)
    remove_project_bookmark(db, project_id)
    redirect_to = get_bookmark_redirect(next, str(project["urls"]["branches"]))
    return RedirectResponse(url=redirect_to, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/conflicts", name="hotdock-app-conflicts")
async def conflicts(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="Conflicts | Hotdock",
        page_description="競合候補一覧の骨組み。",
        page_heading="Conflicts",
        active_nav="conflicts",
        body_class="page-app page-app-conflicts",
        breadcrumbs=[{"label": "App", "href": "/app"}, {"label": "Conflicts", "href": "/app/conflicts"}],
    )
    context.update({"recent_conflicts": RECENT_CONFLICTS})
    return render_app("hotdock/app/conflicts.html", context)


@router.get("/integrations", name="hotdock-app-integrations")
async def app_integrations(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="App Integrations | Hotdock",
        page_description="git 連携、通知連携、GitHub App 状況をまとめる管理画面骨組み。",
        page_heading="Integrations",
        active_nav="integrations",
        body_class="page-app page-app-integrations",
        breadcrumbs=[{"label": "App", "href": "/app"}, {"label": "Integrations", "href": "/app/integrations"}],
    )
    context.update({"integration_status": INTEGRATION_STATUS})
    return render_app("hotdock/app/integrations.html", context)


@router.get("/notifications", name="hotdock-app-notifications")
async def notifications(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="Notifications | Hotdock",
        page_description="メール、Slack、Chatwork などの通知設定画面骨組み。",
        page_heading="Notifications",
        active_nav="notifications",
        body_class="page-app page-app-notifications",
        breadcrumbs=[{"label": "App", "href": "/app"}, {"label": "Notifications", "href": "/app/notifications"}],
    )
    context.update({"notification_channels": NOTIFICATION_CHANNELS})
    return render_app("hotdock/app/notifications.html", context)


@router.get("/settings", name="hotdock-app-settings")
async def settings(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="Settings | Hotdock",
        page_description="組織設定とユーザー設定の骨組み。",
        page_heading="Settings",
        active_nav="settings",
        body_class="page-app page-app-settings",
        breadcrumbs=[{"label": "App", "href": "/app"}, {"label": "Settings", "href": "/app/settings"}],
    )
    context.update({"settings_sections": SETTINGS_SECTIONS})
    return render_app("hotdock/app/settings.html", context)


@router.get("/billing", name="hotdock-app-billing")
async def billing(request: Request, db: Session = Depends(get_db)):
    context = build_page_context(
        request,
        db,
        page_title="Billing | Hotdock",
        page_description="プラン名、利用状況、請求情報のプレースホルダを表示する骨組み。",
        page_heading="Billing",
        active_nav="billing",
        body_class="page-app page-app-billing",
        breadcrumbs=[{"label": "App", "href": "/app"}, {"label": "Billing", "href": "/app/billing"}],
    )
    context.update({"billing_overview": BILLING_OVERVIEW})
    return render_app("hotdock/app/billing.html", context)
