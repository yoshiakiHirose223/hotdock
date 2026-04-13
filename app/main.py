from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from app.core.config import get_settings
from app.core.database import init_db
from app.core.templating import create_templates
from app.site.router import router as site_router

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
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=settings.proxy_trusted_hosts)
app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")
app.state.templates = templates

app.include_router(site_router)
