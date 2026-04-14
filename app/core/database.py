import time
from collections.abc import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.models import Base

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
    if not settings.resolved_database_url.startswith("sqlite"):
        wait_for_database()
    Base.metadata.create_all(bind=engine)
