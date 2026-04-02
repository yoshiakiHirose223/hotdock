from typing import Any

from fastapi import Request

from app.core.config import get_settings
from app.core.constants import NAV_ITEMS


def build_template_context(request: Request, **extra: Any) -> dict[str, Any]:
    settings = get_settings()
    context = {
        "request": request,
        "app_name": settings.app_name,
        "nav_items": NAV_ITEMS,
    }
    context.update(extra)
    return context
