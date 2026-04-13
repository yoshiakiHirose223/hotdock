from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/")
async def home(request: Request):
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="site/home.html",
        context={
            "request": request,
            "app_name": "HotDock",
            "page_title": "Home",
        },
    )
