"""FastAPI application factory.

Wires settings, logging, middleware, routers, and exception handlers.
Matches the endpoint inventory in `../loftly/mvp/API_CONTRACT.md` and
Pydantic schemas generated from `mvp/artifacts/openapi.yaml`.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

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
    admin_ingestion,
    admin_metrics,
    affiliate,
    articles,
    auth,
    authors,
    cards,
    consent,
    health,
    internal,
    me,
    merchants,
    metrics,
    promos,
    selector,
    selector_chat,
    tracking,
    valuations,
    waitlist,
    webhooks,
)
from loftly.api.routes.health import set_shutting_down
from loftly.core.cache import get_cache, set_cache
from loftly.core.logging import configure_logging, get_logger
from loftly.core.settings import get_settings
from loftly.db.engine import get_engine
from loftly.observability.langfuse import init_langfuse
from loftly.observability.prometheus import db_pool_gauge_snapshot
from loftly.observability.sentry import init_sentry

# Sample interval for the DB-pool gauge snapshot. Picked to match the Grafana
# api-latency dashboard refresh (30s); shorter is wasted work.
DB_POOL_SNAPSHOT_INTERVAL_SEC = 30.0


async def _db_pool_snapshot_loop(log: object) -> None:
    """Periodically update DB-pool gauges.

    Errors are logged and swallowed — the loop must never die, because a
    broken snapshot task would silently freeze the pool saturation panel on
    the dashboard. Cancellation is the only legitimate exit.
    """
    while True:
        try:
            db_pool_gauge_snapshot(get_engine())
        except asyncio.CancelledError:
            raise
        except Exception:
            # structlog's .warning takes exc_info=True like stdlib; typed as
            # `object` here to avoid a structlog import just for typing.
            with suppress(Exception):
                log.warning("db_pool_snapshot_failed", exc_info=True)  # type: ignore[attr-defined]
        try:
            await asyncio.sleep(DB_POOL_SNAPSHOT_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise


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
    # Background DB-pool gauge sampler — survives the life of the app.
    # Skipped in the test env because the aiosqlite StaticPool has nothing
    # useful to report and the loop clutters test logs.
    pool_task: asyncio.Task[None] | None = None
    if not settings.is_test:
        pool_task = asyncio.create_task(_db_pool_snapshot_loop(log), name="db_pool_snapshot_loop")
    try:
        yield
    finally:
        # ---- Graceful shutdown — see DRILL-003. ------------------------
        # Order matters:
        #   1. Flip `_shutting_down` so `/readyz` returns 503 *before* we
        #      start tearing anything down. DO App Platform's load
        #      balancer watches `/readyz` and pulls this pod out of
        #      rotation once it flips; that's what drains in-flight
        #      traffic before we close the DB pool.
        #   2. Cancel the background pool-snapshot task and await it so
        #      pytest doesn't emit "coroutine was never awaited" warnings
        #      and Sentry doesn't see a CancelledError at exit.
        #   3. Dispose the SQLAlchemy engine — returns pool connections
        #      to Postgres cleanly instead of letting TCP RST close them.
        #   4. Flush Sentry + Langfuse so any buffered events make it out
        #      before the process exits (DO's default grace window is
        #      30s, uvicorn gets 25, leaving ~5s for these flushes).
        #   5. Clear singletons last so reload cycles get fresh state.
        shutdown_start = time.perf_counter()
        set_shutting_down(True)
        log.info("shutdown_started")

        if pool_task is not None:
            pool_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await pool_task

        # Close DB engine so pooled connections are returned cleanly.
        with suppress(Exception):
            await get_engine().dispose()

        # Flush Sentry if it was initialised. `sentry_sdk.flush` is
        # sync+blocking but bounded — we cap it at 2s.
        with suppress(Exception):
            import sentry_sdk

            sentry_sdk.flush(timeout=2.0)

        # Flush Langfuse if wired. The client exposes `.flush()` in 2.x+
        # and `.shutdown()` for hard-stop; either is fine at exit.
        with suppress(Exception):
            from loftly.observability import langfuse as _lf

            lf_client = getattr(_lf, "_LANGFUSE_CLIENT", None)
            if lf_client is not None:
                flush_fn = getattr(lf_client, "flush", None)
                if callable(flush_fn):
                    flush_fn()
                shutdown_fn = getattr(lf_client, "shutdown", None)
                if callable(shutdown_fn):
                    shutdown_fn()

        # Clear singletons so reload cycles pick up fresh instances.
        set_cache(None)
        set_provider(None)

        duration_ms = round((time.perf_counter() - shutdown_start) * 1000.0, 2)
        log.info("shutdown_complete", duration_ms=duration_ms)
        # Reset the flag on clean exit so tests that reuse the module see
        # a fresh state; prod processes exit immediately after this.
        set_shutting_down(False)


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
    # Staging origins: `loftly.biggo-analytics.dev` is the live custom domain
    # (Cloudflare DNS → DO Apps); the `*.ondigitalocean.app` fallback covers
    # direct DO URL access. Without an explicit match the browser fails
    # preflight with 400 "Disallowed CORS origin" and /v1/selector POSTs
    # surface to users as "เครือข่ายขัดข้อง".
    allowed_origins = [
        "https://loftly.co.th",
        "https://www.loftly.co.th",
        "https://staging.loftly.co.th",
        "https://loftly.biggo-analytics.dev",
        "https://loftly-web-staging-xymb5.ondigitalocean.app",
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

    # Prometheus scrape surface — also at the root; contract in
    # `mvp/artifacts/grafana/README.md`.
    app.include_router(metrics.router)

    # Versioned API surface.
    app.include_router(auth.router)
    app.include_router(consent.router)
    app.include_router(account.router)
    app.include_router(me.router)
    app.include_router(cards.router)
    app.include_router(articles.router)
    app.include_router(authors.router)
    app.include_router(selector.router)
    app.include_router(selector_chat.router)
    app.include_router(valuations.router)
    app.include_router(promos.router)
    app.include_router(merchants.router)
    app.include_router(affiliate.router)
    app.include_router(admin.router)
    app.include_router(admin_flags.router)
    app.include_router(admin_ingestion.router)
    app.include_router(admin_metrics.router)
    app.include_router(internal.router)
    app.include_router(tracking.router)
    app.include_router(waitlist.router)
    app.include_router(webhooks.router)

    # Bind settings onto state for debugging/introspection without re-reading env.
    app.state.settings = settings
    return app


app = create_app()
