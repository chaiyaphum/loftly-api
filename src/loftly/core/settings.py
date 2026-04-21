"""Application settings — pydantic-settings.

Env var catalog matches ../loftly/mvp/DEPLOYMENT.md §Environment variables.
Required vars fail fast at startup; optional vars warn but don't block dev.
"""

from __future__ import annotations

import logging
import warnings
from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

log = logging.getLogger(__name__)

Env = Literal["dev", "staging", "prod", "test"]


class Settings(BaseSettings):
    """Runtime configuration.

    Loaded from (in order): process env, `.env` file. Missing required vars
    raise at startup. Optional AI-provider keys (Anthropic, Typhoon) emit a
    warning in dev but let the app boot so scaffolding and tests can run
    without them.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Required ---
    database_url: str = Field(
        default="sqlite+aiosqlite:///./loftly_dev.db",
        description="SQLAlchemy async URL. Prod = asyncpg; tests/dev fallback = aiosqlite.",
    )
    jwt_signing_key: str = Field(
        default="dev-insecure-change-me",
        description="HMAC key for JWT issuance. Rotate quarterly in prod.",
    )

    # --- Environment ---
    loftly_env: Env = Field(default="dev")

    # --- JWT config ---
    jwt_access_ttl_sec: int = Field(default=900)
    jwt_refresh_ttl_sec: int = Field(default=2_592_000)
    jwt_algorithm: str = Field(default="HS256")

    # --- Redis / rate-limit backing store ---
    redis_url: str | None = Field(default=None)

    # --- AI providers (optional in dev) ---
    anthropic_api_key: str | None = Field(default=None)
    typhoon_api_key: str | None = Field(default=None)
    typhoon_api_base: str = Field(default="https://api.sambanova.ai/v1")

    # --- deal-harvester ---
    deal_harvester_base: str = Field(default="https://deals.biggo-analytics.dev/api/v1")
    deal_harvester_api_key: str | None = Field(default=None)

    # --- Observability ---
    sentry_dsn: str | None = Field(default=None)
    langfuse_secret_key: str | None = Field(default=None)
    langfuse_host: str | None = Field(default=None)
    posthog_project_api_key: str | None = Field(default=None)

    # --- Email ---
    resend_api_key: str | None = Field(default=None)

    # --- Prompt versioning ---
    loftly_prompt_version_override: str | None = Field(default=None)

    @field_validator("database_url")
    @classmethod
    def _require_async_driver(cls, v: str) -> str:
        # Permit common async drivers. Plain `postgresql://` would use a sync driver
        # and deadlock under async SQLAlchemy; reject early.
        allowed_prefixes = ("postgresql+asyncpg://", "sqlite+aiosqlite://")
        if not v.startswith(allowed_prefixes):
            raise ValueError(
                "DATABASE_URL must use an async driver "
                "(postgresql+asyncpg:// or sqlite+aiosqlite://). "
                f"Got: {v.split('://', 1)[0]}://..."
            )
        return v

    @property
    def is_prod(self) -> bool:
        return self.loftly_env == "prod"

    @property
    def is_test(self) -> bool:
        return self.loftly_env == "test"

    def warn_missing_optional(self) -> None:
        """Emit warnings for optional-but-strongly-recommended vars."""
        if not self.is_test:
            if not self.anthropic_api_key:
                warnings.warn(
                    "ANTHROPIC_API_KEY unset — LLM features (Selector, Valuation) "
                    "will degrade to rule-based fallback.",
                    stacklevel=2,
                )
            if not self.typhoon_api_key:
                warnings.warn(
                    "TYPHOON_API_KEY unset — Thai-optimized free-text paths disabled.",
                    stacklevel=2,
                )
            if self.is_prod and self.jwt_signing_key == "dev-insecure-change-me":
                raise RuntimeError(
                    "JWT_SIGNING_KEY is still the dev placeholder. Refusing to start in prod."
                )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide singleton Settings instance."""
    settings = Settings()
    settings.warn_missing_optional()
    return settings
