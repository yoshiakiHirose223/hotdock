import time
from collections.abc import Generator

from sqlalchemy import create_engine, inspect, text
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
    apply_runtime_schema_upgrades()


def _ensure_column(table_name: str, column_name: str, ddl: str) -> None:
    with engine.begin() as connection:
        inspector = inspect(connection)
        existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
        if column_name in existing_columns:
            return
        connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {ddl}"))


def _execute_upgrade_sql(sql: str) -> None:
    with engine.begin() as connection:
        connection.execute(text(sql))


def apply_runtime_schema_upgrades() -> None:
    repository_columns = [
        ("default_branch", "default_branch VARCHAR(255)"),
        ("is_available", "is_available BOOLEAN NOT NULL DEFAULT TRUE"),
        ("is_active", "is_active BOOLEAN NOT NULL DEFAULT TRUE"),
        ("selection_status", "selection_status VARCHAR(32) DEFAULT 'unselected'"),
        ("activated_at", "activated_at TIMESTAMP"),
        ("deactivated_at", "deactivated_at TIMESTAMP"),
        ("inaccessible_reason", "inaccessible_reason VARCHAR(64)"),
        ("detail_sync_status", "detail_sync_status VARCHAR(32) DEFAULT 'not_started'"),
        ("detail_sync_error_message", "detail_sync_error_message VARCHAR(2048)"),
        ("last_detail_sync_started_at", "last_detail_sync_started_at TIMESTAMP"),
        ("last_detail_sync_completed_at", "last_detail_sync_completed_at TIMESTAMP"),
    ]
    branch_columns = [
        ("current_head_sha", "current_head_sha VARCHAR(64)"),
        ("last_before_sha", "last_before_sha VARCHAR(64)"),
        ("last_after_sha", "last_after_sha VARCHAR(64)"),
        ("is_deleted", "is_deleted BOOLEAN NOT NULL DEFAULT FALSE"),
        ("was_created_observed", "was_created_observed BOOLEAN NOT NULL DEFAULT FALSE"),
        ("was_force_pushed_observed", "was_force_pushed_observed BOOLEAN NOT NULL DEFAULT FALSE"),
        ("observed_via", "observed_via VARCHAR(32)"),
        ("touch_seed_source", "touch_seed_source VARCHAR(32)"),
        ("touch_seeded_at", "touch_seeded_at TIMESTAMP"),
        ("touch_seed_status", "touch_seed_status VARCHAR(32)"),
        ("touch_seed_warning", "touch_seed_warning VARCHAR(2048)"),
        ("has_authoritative_compare_history", "has_authoritative_compare_history BOOLEAN NOT NULL DEFAULT FALSE"),
        ("has_webhook_history", "has_webhook_history BOOLEAN NOT NULL DEFAULT FALSE"),
        ("last_delivery_id", "last_delivery_id VARCHAR(128)"),
        ("last_processed_compare_base", "last_processed_compare_base VARCHAR(64)"),
        ("last_processed_compare_head", "last_processed_compare_head VARCHAR(64)"),
    ]
    branch_event_columns = [
        ("reason", "reason VARCHAR(128)"),
    ]
    branch_file_columns = [
        ("repository_id", "repository_id VARCHAR(36)"),
        ("normalized_path", "normalized_path VARCHAR(2048)"),
        ("last_change_type", "last_change_type VARCHAR(32)"),
        ("previous_path", "previous_path VARCHAR(2048)"),
        ("last_seen_commit_sha", "last_seen_commit_sha VARCHAR(64)"),
        ("last_seen_at", "last_seen_at TIMESTAMP"),
        ("is_active", "is_active BOOLEAN NOT NULL DEFAULT TRUE"),
    ]
    pending_claim_columns = [
        ("last_resume_at", "last_resume_at TIMESTAMP"),
        ("state_verified_at", "state_verified_at TIMESTAMP"),
        ("callback_source", "callback_source VARCHAR(64)"),
    ]
    installation_columns = [
        ("unlink_requested_at", "unlink_requested_at TIMESTAMP"),
        ("unlinked_at", "unlinked_at TIMESTAMP"),
        ("unlinked_by_user_id", "unlinked_by_user_id VARCHAR(36)"),
    ]

    for column_name, ddl in repository_columns:
        _ensure_column("repositories", column_name, ddl)
    for column_name, ddl in branch_columns:
        _ensure_column("branches", column_name, ddl)
    for column_name, ddl in branch_event_columns:
        _ensure_column("branch_events", column_name, ddl)
    for column_name, ddl in branch_file_columns:
        _ensure_column("branch_files", column_name, ddl)
    for column_name, ddl in pending_claim_columns:
        _ensure_column("github_pending_claims", column_name, ddl)
    for column_name, ddl in installation_columns:
        _ensure_column("github_installations", column_name, ddl)

    _execute_upgrade_sql(
        "UPDATE repositories SET "
        "is_available = COALESCE(is_available, TRUE), "
        "is_active = COALESCE(is_active, TRUE), "
        "selection_status = COALESCE(selection_status, CASE WHEN COALESCE(is_active, TRUE) THEN 'active' ELSE 'unselected' END), "
        "detail_sync_status = COALESCE(detail_sync_status, CASE WHEN COALESCE(is_active, TRUE) AND last_synced_at IS NOT NULL THEN 'completed' ELSE 'not_started' END)"
    )
    _execute_upgrade_sql(
        "UPDATE repositories SET "
        "activated_at = COALESCE(activated_at, CASE WHEN selection_status = 'active' THEN last_synced_at ELSE NULL END), "
        "deactivated_at = COALESCE(deactivated_at, CASE WHEN selection_status IN ('inactive', 'inaccessible') THEN last_synced_at ELSE NULL END)"
    )
    _execute_upgrade_sql(
        "UPDATE repositories SET "
        "is_active = CASE WHEN selection_status = 'active' THEN TRUE ELSE FALSE END"
    )
    _execute_upgrade_sql(
        "UPDATE branches SET current_head_sha = COALESCE(current_head_sha, last_commit_sha), "
        "last_after_sha = COALESCE(last_after_sha, last_commit_sha), "
        "is_deleted = COALESCE(is_deleted, FALSE), "
        "was_created_observed = COALESCE(was_created_observed, FALSE), "
        "was_force_pushed_observed = COALESCE(was_force_pushed_observed, FALSE), "
        "has_authoritative_compare_history = COALESCE(has_authoritative_compare_history, FALSE), "
        "has_webhook_history = COALESCE(has_webhook_history, FALSE), "
        "observed_via = COALESCE(observed_via, CASE WHEN COALESCE(has_webhook_history, FALSE) THEN 'webhook' ELSE NULL END)"
    )
    _execute_upgrade_sql(
        "UPDATE branch_files SET normalized_path = COALESCE(normalized_path, path), "
        "last_change_type = COALESCE(last_change_type, change_type), "
        "last_seen_at = COALESCE(last_seen_at, observed_at), "
        "is_active = COALESCE(is_active, TRUE), "
        "repository_id = COALESCE(repository_id, (SELECT repository_id FROM branches WHERE branches.id = branch_files.branch_id))"
    )

    index_statements = [
        "CREATE INDEX IF NOT EXISTS ix_repositories_is_active ON repositories (is_active)",
        "CREATE INDEX IF NOT EXISTS ix_repositories_is_available ON repositories (is_available)",
        "CREATE INDEX IF NOT EXISTS ix_repositories_selection_status ON repositories (selection_status)",
        "CREATE INDEX IF NOT EXISTS ix_repositories_detail_sync_status ON repositories (detail_sync_status)",
        "CREATE INDEX IF NOT EXISTS ix_branches_is_deleted ON branches (is_deleted)",
        "CREATE INDEX IF NOT EXISTS ix_branches_observed_via ON branches (observed_via)",
        "CREATE INDEX IF NOT EXISTS ix_branches_touch_seed_status ON branches (touch_seed_status)",
        "CREATE INDEX IF NOT EXISTS ix_branches_has_authoritative_compare_history ON branches (has_authoritative_compare_history)",
        "CREATE INDEX IF NOT EXISTS ix_branches_has_webhook_history ON branches (has_webhook_history)",
        "CREATE INDEX IF NOT EXISTS ix_branch_events_reason ON branch_events (reason)",
        "CREATE INDEX IF NOT EXISTS ix_branch_files_repository_id ON branch_files (repository_id)",
        "CREATE INDEX IF NOT EXISTS ix_branch_files_normalized_path ON branch_files (normalized_path)",
        "CREATE INDEX IF NOT EXISTS ix_branch_files_last_seen_at ON branch_files (last_seen_at)",
        "CREATE INDEX IF NOT EXISTS ix_branch_files_is_active ON branch_files (is_active)",
        "CREATE INDEX IF NOT EXISTS ix_github_pending_claims_last_resume_at ON github_pending_claims (last_resume_at)",
        "CREATE INDEX IF NOT EXISTS ix_github_pending_claims_state_verified_at ON github_pending_claims (state_verified_at)",
        "CREATE INDEX IF NOT EXISTS ix_github_pending_claims_callback_source ON github_pending_claims (callback_source)",
        "CREATE INDEX IF NOT EXISTS ix_github_installations_unlinked_at ON github_installations (unlinked_at)",
    ]
    for statement in index_statements:
        _execute_upgrade_sql(statement)
