from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "HotDock"
    app_env: str = "development"
    debug: bool = True
    secret_key: str = Field(
        default="replace-this-secret-key",
        description="Secret key used by future authentication features.",
    )
    blog_admin_username: str = "yoshiaki0223"
    blog_admin_password_hash: str = (
        "9047061c525874f0e3158f1648cba7ed:"
        "439b1780e7ca54bdfc6a3deb487ea4690c955dd9b880e93b70d94381c13c7e62"
    )
    database_url: str | None = None
    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_db: str | None = None
    postgres_host: str | None = None
    postgres_port: int = 5432
    database_connect_retries: int = 30
    database_connect_retry_interval: float = 1.0
    site_url: str = "http://127.0.0.1:8000"

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
    def blog_posts_dir(self) -> Path:
        return self.storage_dir / "blog" / "posts"

    @property
    def blog_images_dir(self) -> Path:
        return self.storage_dir / "blog" / "images"

    @property
    def shared_templates_dir(self) -> Path:
        return self.base_dir / "app" / "templates"

    @property
    def site_templates_dir(self) -> Path:
        return self.base_dir / "app" / "site" / "templates"

    @property
    def blog_templates_dir(self) -> Path:
        return self.base_dir / "app" / "blog" / "templates"

    @property
    def tools_templates_dir(self) -> Path:
        return self.base_dir / "app" / "tools" / "templates"

    @property
    def exam_templates_dir(self) -> Path:
        return self.base_dir / "app" / "exam" / "templates"

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


@lru_cache
def get_settings() -> Settings:
    return Settings()
