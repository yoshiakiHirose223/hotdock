from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from starlette import status

from app.core.database import get_db
from app.hotdock.services.auth import attach_auth_context, build_login_redirect, default_workspace_for_user

router = APIRouter(prefix="/app")


def _legacy_redirect(request: Request, db: Session, fallback_path: str) -> RedirectResponse:
    auth = attach_auth_context(request, db)
    if auth.user is None:
        next_path = request.url.path
        if request.url.query:
            next_path = f"{request.url.path}?{request.url.query}"
        return RedirectResponse(url=build_login_redirect(next_path), status_code=status.HTTP_303_SEE_OTHER)

    workspace = default_workspace_for_user(db, auth.user.id)
    if workspace is None:
        return RedirectResponse(url="/workspaces/new", status_code=status.HTTP_303_SEE_OTHER)

    if "{workspace}" in fallback_path:
        target = fallback_path.format(workspace=workspace.slug)
    else:
        target = fallback_path
    return RedirectResponse(url=target, status_code=status.HTTP_303_SEE_OTHER)


@router.get("", name="hotdock-app-index")
async def app_index(request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/dashboard")


@router.get("/dashboard", name="hotdock-app-dashboard")
async def dashboard(request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/dashboard")


@router.get("/projects", name="hotdock-app-projects")
async def projects(request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/branches")


@router.get("/projects/{project_id}", name="hotdock-app-project-overview")
async def project_overview(project_id: int, request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/branches")


@router.get("/projects/{project_id}/branches", name="hotdock-app-project-branches")
async def project_branches(project_id: int, request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/branches")


@router.get("/projects/{project_id}/conflicts", name="hotdock-app-project-conflicts")
async def project_conflicts(project_id: int, request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/conflicts")


@router.get("/projects/{project_id}/settings", name="hotdock-app-project-settings")
async def project_settings(project_id: int, request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/settings")


@router.post("/projects/{project_id}/bookmark", name="hotdock-app-project-bookmark")
async def bookmark_project(project_id: int, request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/branches")


@router.post("/projects/{project_id}/unbookmark", name="hotdock-app-project-unbookmark")
async def unbookmark_project(project_id: int, request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/branches")


@router.get("/conflicts", name="hotdock-app-conflicts")
async def conflicts(request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/conflicts")


@router.get("/integrations", name="hotdock-app-integrations")
async def app_integrations(request: Request, db: Session = Depends(get_db)):
    auth = attach_auth_context(request, db)
    if auth.user is None:
        return RedirectResponse(url=build_login_redirect(request.url.path), status_code=status.HTTP_303_SEE_OTHER)
    workspace = default_workspace_for_user(db, auth.user.id)
    if workspace is None:
        return RedirectResponse(url="/workspaces/new", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(
        url=f"/settings/integrations/github?{urlencode({'workspace': workspace.slug})}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/notifications", name="hotdock-app-notifications")
async def notifications(request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/settings")


@router.get("/settings", name="hotdock-app-settings")
async def settings(request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/settings")


@router.get("/billing", name="hotdock-app-billing")
async def billing(request: Request, db: Session = Depends(get_db)):
    return _legacy_redirect(request, db, "/workspaces/{workspace}/billing")
