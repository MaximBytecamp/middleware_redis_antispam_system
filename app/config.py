"""Runtime configuration loaded from environment variables."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration shared by the API routes and middleware.

    Field names intentionally mirror their environment variables.  Pydantic
    settings is case-insensitive by default, so ``redis_url`` is populated
    from ``REDIS_URL`` without maintaining a second set of aliases.
    """

    redis_url: str = "redis://localhost:6379/0"
    admin_key: str = "secret123"

    rate_limit: int = Field(default=10, gt=0)
    rate_window: int = Field(default=60, gt=0)
    duplicate_ttl: int = Field(default=120, gt=0)
    violation_limit: int = Field(default=3, gt=0)
    violation_window: int = Field(default=300, gt=0)
    block_ttl: int = Field(default=600, gt=0)
    stats_cache_ttl: int = Field(default=15, gt=0)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """Return one immutable-by-convention settings instance per process."""

    return Settings()
