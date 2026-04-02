from fastapi import APIRouter, Request

from app.core.constants import NAV_ITEMS
from app.core.dependencies import build_template_context

router = APIRouter()


@router.get("/")
async def home(request: Request):
    context = build_template_context(
        request,
        page_title="Home",
        hero_sections=NAV_ITEMS,
    )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="site/home.html",
        context=context,
    )
