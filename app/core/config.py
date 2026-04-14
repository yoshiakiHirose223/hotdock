from functools import lru_cache
from base64 import b64decode
from pathlib import Path
from urllib.parse import quote_plus

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Hotdock"
    app_env: str = "development"
    debug: bool = True
    init_db_on_startup: bool = True
    secret_key: str = Field(
        default="replace-this-secret-key",
        description="Secret key used by future authentication features.",
    )
    proxy_trusted_hosts: str = "*"
    database_url: str | None = None
    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_db: str | None = None
    postgres_host: str | None = None
    postgres_port: int = 5432
    database_connect_retries: int = 30
    database_connect_retry_interval: float = 1.0
    site_url: str = "http://127.0.0.1:8000"
    auth_cookie_name: str = "hotdock_session"
    csrf_cookie_name: str = "hotdock_csrf"
    auth_session_ttl_seconds: int = 60 * 60 * 24 * 7
    invitation_ttl_hours: int = 72
    pending_claim_ttl_hours: int = 72
    github_app_slug: str | None = None
    github_app_id: str | None = None
    github_app_client_id: str | None = None
    github_app_client_secret: str | None = None
    github_app_private_key: str | None = None
    github_app_install_url: str | None = None
    github_app_setup_url: str | None = None
    github_app_webhook_secret: str | None = None
    github_api_base_url: str = "https://api.github.com"
    github_oauth_base_url: str = "https://github.com"
    github_mock_oauth_enabled: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def base_dir(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def storage_dir(self) -> Path:
        return self.base_dir / "storage"

    @property
    def shared_templates_dir(self) -> Path:
        return self.base_dir / "app" / "templates"

    @property
    def templates_dir(self) -> Path:
        return self.base_dir / "app" / "templates"

    @property
    def static_dir(self) -> Path:
        return self.base_dir / "app" / "static"

    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        if all(
            [
                self.postgres_user,
                self.postgres_password,
                self.postgres_db,
                self.postgres_host,
            ]
        ):
            quoted_password = quote_plus(self.postgres_password)
            return (
                "postgresql+psycopg2://"
                f"{self.postgres_user}:{quoted_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )
        return f"sqlite:///{self.storage_dir / 'app.db'}"

    @property
    def github_private_key_pem(self) -> str | None:
        if not self.github_app_private_key:
            return None
        raw = self.github_app_private_key.strip()
        if "BEGIN" in raw:
            return raw.replace("\\n", "\n")
        try:
            return b64decode(raw).decode("utf-8")
        except Exception:
            return raw.replace("\\n", "\n")


@lru_cache
def get_settings() -> Settings:
    return Settings()
