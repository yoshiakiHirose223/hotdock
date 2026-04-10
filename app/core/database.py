import time
from collections.abc import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.models.base import Base

settings = get_settings()

engine_kwargs: dict[str, object] = {"pool_pre_ping": True}
if settings.resolved_database_url.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(settings.resolved_database_url, future=True, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def wait_for_database() -> None:
    last_error: OperationalError | None = None
    for attempt in range(1, settings.database_connect_retries + 1):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except OperationalError as exc:
            last_error = exc
            if attempt == settings.database_connect_retries:
                break
            time.sleep(settings.database_connect_retry_interval)

    if last_error is not None:
        raise last_error


def init_db() -> None:
    from app.blog import models as blog_models  # noqa: F401
    from app.exam import models as exam_models  # noqa: F401
    from app.tools import conflict_watch_models as conflict_watch_models  # noqa: F401

    if not settings.resolved_database_url.startswith("sqlite"):
        wait_for_database()
    Base.metadata.create_all(bind=engine)
    ensure_legacy_blog_schema()


def ensure_legacy_blog_schema() -> None:
    inspector = inspect(engine)
    if "blog_posts" not in inspector.get_table_names():
        pass
    else:
        columns = {column["name"]: column for column in inspector.get_columns("blog_posts")}
        with engine.begin() as connection:
            if "summary" not in columns:
                connection.execute(text("ALTER TABLE blog_posts ADD COLUMN summary VARCHAR(50) NOT NULL DEFAULT ''"))
            else:
                connection.execute(text("UPDATE blog_posts SET summary = '' WHERE summary IS NULL"))

    if "cw_settings" in inspector.get_table_names():
        cw_columns = {column["name"]: column for column in inspector.get_columns("cw_settings")}
        with engine.begin() as connection:
            if "slack_webhook_url" not in cw_columns:
                connection.execute(text("ALTER TABLE cw_settings ADD COLUMN slack_webhook_url VARCHAR(500)"))
