from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.blog.admin_public_router import router as blog_admin_public_router
from app.blog.admin_router import router as blog_admin_router
from app.blog.router import router as blog_router
from app.core.config import get_settings
from app.core.database import init_db
from app.core.templating import create_templates
from app.exam.router import router as exam_router
from app.site.router import router as site_router
from app.tools.router import router as tools_router

settings = get_settings()
templates = create_templates()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.init_db_on_startup:
        init_db()
    yield


app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.secret_key,
    same_site="lax",
    https_only=settings.app_env == "production",
)
app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")
app.state.templates = templates

app.include_router(site_router)
app.include_router(blog_admin_public_router, prefix="/blog", tags=["blog-admin"])
app.include_router(blog_admin_router, prefix="/blog", tags=["blog-admin"])
app.include_router(blog_router, prefix="/blog", tags=["blog"])
app.include_router(tools_router, prefix="/tools", tags=["tools"])
app.include_router(exam_router, prefix="/exam", tags=["exam"])
