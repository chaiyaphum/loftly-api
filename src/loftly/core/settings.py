"""Application settings — pydantic-settings.

Env var catalog matches ../loftly/mvp/DEPLOYMENT.md §Environment variables.
Required vars fail fast at startup; optional vars warn but don't block dev.
"""

from __future__ import annotations

import json
import logging
import warnings
from functools import lru_cache
from typing import Any, Literal

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

    # --- LLM provider switch ---
    # `deterministic`  — rule-based ranker (no network). Default.
    # `anthropic`      — Claude Sonnet/Haiku (stub today; wired Week 7).
    loftly_llm_provider: Literal["deterministic", "anthropic"] = Field(
        default="deterministic",
        description="Which LLMProvider implementation the app should use.",
    )

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
    #: Public key for the Langfuse project — required alongside the secret key
    #: for the read-side metrics API used by the cost-anomaly check.
    langfuse_public_key: str | None = Field(default=None)
    langfuse_host: str | None = Field(default=None)
    posthog_project_api_key: str | None = Field(default=None)

    # --- Email ---
    resend_api_key: str | None = Field(default=None)
    resend_from_address: str = Field(default="Loftly <noreply@loftly.co.th>")
    #: Inbox for operator-addressed notifications (weekly content-stale digest,
    #: future ops pings). Distinct from `resend_from_address` which is the
    #: outbound FROM for user-facing mail.
    founder_notify_email: str = Field(default="founder@loftly.co.th")
    #: POST_V1 §2 kill-switch. `true` (default) → Haiku composes a personalized
    #: top-3 recap; `false` → magic-link flow falls back to the v1 static
    #: template. Required so ops can flip the path off without a redeploy when
    #: Anthropic is down or the prompt regresses in prod. Fail-open to
    #: personalized matches the product bet — we'd rather send a richer email
    #: most of the time than protect against the degenerate case.
    welcome_email_personalized: bool = Field(default=True)
    #: Public base URL used to build the tracking-pixel URL embedded in
    #: personalized welcome emails. Separate from ``resend_from_address`` so
    #: staging/prod can use their own domains without touching the FROM.
    api_public_base_url: str = Field(default="https://api.loftly.co.th")

    # --- OAuth providers (optional; stub mode when unset) ---
    loftly_oauth_google_client_id: str | None = Field(default=None)
    loftly_oauth_google_client_secret: str | None = Field(default=None)
    loftly_oauth_apple_client_id: str | None = Field(default=None)
    loftly_oauth_apple_team_id: str | None = Field(default=None)
    loftly_oauth_apple_key_id: str | None = Field(default=None)
    loftly_oauth_apple_private_key: str | None = Field(default=None)
    loftly_oauth_line_client_id: str | None = Field(default=None)
    loftly_oauth_line_client_secret: str | None = Field(default=None)

    # --- Affiliate partner HMAC secrets ---
    # JSON map partner_id -> shared_secret. Example:
    #   AFFILIATE_PARTNER_SECRETS='{"bigbank-affiliate":"xxx"}'
    # Parsed from env on startup; missing partner_id -> 401 on webhook.
    affiliate_partner_secrets: dict[str, str] = Field(default_factory=dict)

    # --- Prompt versioning ---
    loftly_prompt_version_override: str | None = Field(default=None)

    # --- Feature flags (env-driven kill switches) ---
    # POST_V1 §3 Tier A (2026-04-22) — Promo-Aware Card Selector. When ON, the
    # selector route builds a promo snapshot and injects it into Sonnet/Haiku's
    # cached prompt. Default OFF so existing callers keep the v1 behavior.
    # Will be flipped ON in staging once shadow-mode eval clears per
    # `.claude/plans/.../iverson.md §1.9 Rollout`.
    loftly_ff_selector_promo_context: bool = Field(default=False)

    # DEVLOG 2026-04-23 (evening) Known Issue §1 — Anthropic Sonnet+Haiku
    # both time out from DO Singapore, making `/v1/selector` ~15s on happy
    # path. When ON, `_run_with_fallback` skips Sonnet + Haiku entirely and
    # returns the deterministic result immediately — trading LLM quality for
    # sub-second UX while the upstream is unreachable. Result still stamps
    # `fallback_reason="both_failed"` so downstream metrics account for it;
    # additionally emits a `fallback_first_forced` warning so we can tell
    # "flag ON" from "both LLMs actually failed" in traces. Default OFF.
    loftly_ff_selector_fallback_first: bool = Field(default=False)

    # POST_V1 §9 Workstream B (ratified 2026-04-22 Q18) — Merchant Reverse
    # Lookup. Gates both the `/v1/merchants/*` read surface and the
    # `canonicalize_merchants` daily job. Flipped to default-ON 2026-04-22
    # (late) so the redesigned `/merchants/*` pages on staging resolve
    # instead of 405-ing. Env-var override (MERCHANTS_REVERSE_LOOKUP_ENABLED=false)
    # still honored if we need to kill-switch.
    merchants_reverse_lookup_enabled: bool = Field(default=True)

    @field_validator("affiliate_partner_secrets", mode="before")
    @classmethod
    def _parse_partner_secrets(cls, v: Any) -> Any:
        """Accept either a JSON string (from env) or a dict (from tests)."""
        if v is None or v == "":
            return {}
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    'AFFILIATE_PARTNER_SECRETS must be valid JSON (e.g. \'{"p":"s"}\').'
                ) from exc
            if not isinstance(parsed, dict):
                raise ValueError("AFFILIATE_PARTNER_SECRETS must decode to an object.")
            return parsed
        return v

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
