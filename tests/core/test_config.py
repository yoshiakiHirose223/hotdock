from app.core.config import Settings


def test_resolved_database_url_prefers_explicit_database_url():
    settings = Settings(
        database_url="postgresql+psycopg2://custom:secret@custom-db:5432/customdb",
        postgres_user="appuser",
        postgres_password="password",
        postgres_db="appdb",
        postgres_host="db",
    )

    assert settings.resolved_database_url == "postgresql+psycopg2://custom:secret@custom-db:5432/customdb"


def test_resolved_database_url_builds_postgres_url_from_compose_style_fields():
    settings = Settings(
        postgres_user="appuser",
        postgres_password="password",
        postgres_db="appdb",
        postgres_host="db",
        postgres_port=5432,
    )

    assert settings.resolved_database_url == "postgresql+psycopg2://appuser:password@db:5432/appdb"
