from __future__ import annotations

from typing import Any

from fastapi import Request

from app.hotdock.data.navigation import (
    APP_NAVIGATION,
    AUTH_LINKS,
    FOOTER_LINK_GROUPS,
    FOOTER_META,
    PUBLIC_NAVIGATION,
)


def _base_context(
    request: Request,
    *,
    page_title: str,
    page_description: str,
    page_heading: str,
    active_nav: str,
    body_class: str,
    breadcrumbs: list[dict[str, str]] | None = None,
    meta_og_title: str | None = None,
    meta_og_description: str | None = None,
) -> dict[str, Any]:
    return {
        "request": request,
        "site_name": "Hotdock",
        "page_title": page_title,
        "page_description": page_description,
        "page_heading": page_heading,
        "active_nav": active_nav,
        "body_class": body_class,
        "breadcrumbs": breadcrumbs or [],
        "meta_og_title": meta_og_title or page_title,
        "meta_og_description": meta_og_description or page_description,
    }


def build_public_context(request: Request, **kwargs: Any) -> dict[str, Any]:
    context = _base_context(request, **kwargs)
    context.update(
        {
            "navigation": PUBLIC_NAVIGATION,
            "auth_links": AUTH_LINKS,
            "footer_groups": FOOTER_LINK_GROUPS,
            "footer_meta": FOOTER_META,
        }
    )
    return context


def build_auth_context(request: Request, **kwargs: Any) -> dict[str, Any]:
    context = _base_context(request, **kwargs)
    context.update(
        {
            "auth_links": AUTH_LINKS,
            "navigation": PUBLIC_NAVIGATION,
        }
    )
    return context


def build_app_context(request: Request, **kwargs: Any) -> dict[str, Any]:
    context = _base_context(request, **kwargs)
    context.update(
        {
            "app_navigation": APP_NAVIGATION,
            "app_status": "Preview",
            "app_workspace": "Hotdock Workspace",
        }
    )
    return context
