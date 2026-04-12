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

    if "cw_conflicts" in inspector.get_table_names():
        conflict_columns = {column["name"]: column for column in inspector.get_columns("cw_conflicts")}
        with engine.begin() as connection:
            if "resolved_context" not in conflict_columns:
                connection.execute(text("ALTER TABLE cw_conflicts ADD COLUMN resolved_context JSON"))
            if "last_related_branches" not in conflict_columns:
                connection.execute(text("ALTER TABLE cw_conflicts ADD COLUMN last_related_branches JSON NOT NULL DEFAULT '[]'"))

    ensure_conflict_watch_commit_history_schema()


def ensure_conflict_watch_commit_history_schema() -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS cw_branch_commits (
            id INTEGER PRIMARY KEY,
            repository_id INTEGER NOT NULL REFERENCES cw_repositories(id) ON DELETE CASCADE,
            branch_id INTEGER NOT NULL REFERENCES cw_branches(id) ON DELETE CASCADE,
            commit_sha VARCHAR(255) NOT NULL,
            sequence_no INTEGER NOT NULL,
            observed_via_event_id INTEGER REFERENCES cw_webhook_events(id) ON DELETE SET NULL,
            observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
            last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_cw_branch_commits_repo_branch_commit UNIQUE (repository_id, branch_id, commit_sha)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commits_repository_id ON cw_branch_commits (repository_id)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commits_branch_id ON cw_branch_commits (branch_id)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commits_sequence_no ON cw_branch_commits (sequence_no)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commits_observed_via_event_id ON cw_branch_commits (observed_via_event_id)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commits_is_active ON cw_branch_commits (is_active)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commits_commit_sha ON cw_branch_commits (commit_sha)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commits_repo_branch_sequence ON cw_branch_commits (repository_id, branch_id, sequence_no)",
        """
        CREATE TABLE IF NOT EXISTS cw_branch_commit_files (
            id INTEGER PRIMARY KEY,
            repository_id INTEGER NOT NULL REFERENCES cw_repositories(id) ON DELETE CASCADE,
            branch_id INTEGER NOT NULL REFERENCES cw_branches(id) ON DELETE CASCADE,
            branch_commit_id INTEGER NOT NULL REFERENCES cw_branch_commits(id) ON DELETE CASCADE,
            commit_sha VARCHAR(255) NOT NULL,
            file_path VARCHAR(500) NOT NULL,
            normalized_file_path VARCHAR(500) NOT NULL,
            change_type VARCHAR(30) NOT NULL,
            previous_path VARCHAR(500),
            observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_cw_branch_commit_files_repo_branch_commit_path_type UNIQUE (
                repository_id,
                branch_id,
                commit_sha,
                normalized_file_path,
                change_type
            )
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commit_files_repository_id ON cw_branch_commit_files (repository_id)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commit_files_branch_id ON cw_branch_commit_files (branch_id)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commit_files_branch_commit_id ON cw_branch_commit_files (branch_commit_id)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commit_files_is_active ON cw_branch_commit_files (is_active)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commit_files_commit_sha ON cw_branch_commit_files (commit_sha)",
        "CREATE INDEX IF NOT EXISTS ix_cw_branch_commit_files_normalized_path ON cw_branch_commit_files (normalized_file_path)",
    ]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
