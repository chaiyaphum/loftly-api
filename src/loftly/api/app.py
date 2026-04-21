"""FastAPI application factory.

Wires settings, logging, middleware, routers, and exception handlers.
Matches the endpoint inventory in `../loftly/mvp/API_CONTRACT.md` and
Pydantic schemas generated from `mvp/artifacts/openapi.yaml`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from loftly import __version__
from loftly.ai import get_provider, set_provider
from loftly.api.errors import register_exception_handlers
from loftly.api.middleware.logging import (
    RequestLoggingMiddleware,
    TraceHeaderMiddleware,
)
from loftly.api.routes import (
    account,
    admin,
    admin_flags,
    affiliate,
    articles,
    auth,
    cards,
    consent,
    health,
    internal,
    promos,
    selector,
    valuations,
    webhooks,
)
from loftly.core.cache import get_cache, set_cache
from loftly.core.logging import configure_logging, get_logger
from loftly.core.settings import get_settings
from loftly.observability.langfuse import init_langfuse
from loftly.observability.sentry import init_sentry


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings)
    log = get_logger(__name__)
    # Observability first so any startup errors surface upstream.
    init_sentry(settings)
    init_langfuse(settings)
    # Prime the cache + provider singletons so request paths find them warm.
    cache = get_cache()
    provider = get_provider()
    # Optional keys: warn loudly in non-test envs so ops notices before traffic.
    if not settings.is_test:
        if not settings.anthropic_api_key:
            log.warning("anthropic_key_missing — selector will use deterministic fallback")
        if not settings.resend_api_key:
            log.warning("resend_key_missing — magic-link emails will log only")
    log.info(
        "loftly_api_startup",
        env=settings.loftly_env,
        version=__version__,
        llm_provider=provider.name,
        cache=type(cache).__name__,
        sentry=bool(settings.sentry_dsn),
        langfuse=bool(settings.langfuse_secret_key and settings.langfuse_host),
        resend=bool(settings.resend_api_key),
    )
    try:
        yield
    finally:
        # Clear singletons on shutdown so reload cycles get fresh instances.
        set_cache(None)
        set_provider(None)
        log.info("loftly_api_shutdown")


def create_app() -> FastAPI:
    """Return a fully-wired FastAPI application.

    Kept as a factory so tests can build isolated instances without leaking
    settings or DB state across cases.
    """
    settings = get_settings()

    app = FastAPI(
        title="Loftly API",
        description=(
            "Phase 1 MVP backend for Loftly — Thai AI-native credit-card rewards "
            "optimization. Source of truth for the contract is "
            "`../loftly/mvp/artifacts/openapi.yaml`."
        ),
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # CORS — see API_CONTRACT.md §Security baseline.
    allowed_origins = [
        "https://loftly.co.th",
        "https://www.loftly.co.th",
        "https://staging.loftly.co.th",
        "http://localhost:3000",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-API-Key", "X-Loftly-Signature"],
        expose_headers=["X-Trace-Id"],
    )

    # Observability: log every request + propagate trace_id. Order matters:
    # TraceHeader runs LAST (injects header on outbound) -> added FIRST.
    app.add_middleware(TraceHeaderMiddleware)
    app.add_middleware(RequestLoggingMiddleware)

    register_exception_handlers(app)

    # Health probes live at the root, not under /v1 — matches openapi.yaml.
    app.include_router(health.router)

    # Versioned API surface.
    app.include_router(auth.router)
    app.include_router(consent.router)
    app.include_router(account.router)
    app.include_router(cards.router)
    app.include_router(articles.router)
    app.include_router(selector.router)
    app.include_router(valuations.router)
    app.include_router(promos.router)
    app.include_router(affiliate.router)
    app.include_router(admin.router)
    app.include_router(admin_flags.router)
    app.include_router(internal.router)
    app.include_router(webhooks.router)

    # Bind settings onto state for debugging/introspection without re-reading env.
    app.state.settings = settings
    return app


app = create_app()
