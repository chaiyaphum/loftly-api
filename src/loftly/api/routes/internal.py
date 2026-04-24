"""Internal service endpoints.

Protected via `X-API-Key` header (see `require_internal_api_key_or_test`). The
key is the `jwt_signing_key` reused to avoid proliferation — the fly secret
already carries it.

- `POST /v1/internal/sync/deal-harvester` — kick off a background sync.
- `GET  /v1/internal/sync/deal-harvester/last` — peek latest SyncRun.
- `POST /v1/internal/valuation/run` — scaffold stub kept for parity.
- `POST /v1/internal/audit-retention/run` — PDPA retention sweep (weekly cron).
- `POST /v1/internal/content-stale-digest` — weekly content re-verification
  digest (Mon 09:00 ICT cron).
- `GET  /v1/internal/cost-anomaly-check` — hourly LLM cost anomaly check;
  leading indicator for the Anthropic rate-limit storm pattern (issue #14,
  DRILL-002).
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, Depends, Header, Response, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_session
from loftly.db.models.audit import SyncRun
from loftly.jobs.canonicalize_merchants import run_canonicalization
from loftly.jobs.deal_harvester_sync import run_sync

router = APIRouter(prefix="/v1/internal", tags=["internal"])


async def require_internal_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    settings: Settings = Depends(get_settings),
) -> None:
    """Thin internal-service guard. Phase 1 reuses JWT_SIGNING_KEY as the API key.

    Ship a dedicated secret in prod once rotation is set up.
    """
    if not x_api_key or x_api_key != settings.jwt_signing_key:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="unauthorized",
            message_en="Missing or invalid X-API-Key.",
            message_th="ไม่พบคีย์ภายในระบบ",
        )


@router.post(
    "/sync/deal-harvester",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger deal-harvester sync",
)
async def trigger_sync(
    background_tasks: BackgroundTasks,
    _auth: None = Depends(require_internal_api_key),
) -> dict[str, str]:
    """Queue the sync. Returns a `JobHandle` immediately — poll `/last` later."""
    background_tasks.add_task(run_sync)
    return {"job_id": "deal-harvester", "status": "queued"}


@router.get("/sync/deal-harvester/last", summary="Last sync run status")
async def last_sync(
    _auth: None = Depends(require_internal_api_key),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                select(SyncRun)
                .where(SyncRun.source == "deal_harvester")
                .order_by(SyncRun.started_at.desc())
                .limit(1)
            )
        )
        .scalars()
        .one_or_none()
    )
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="no_sync_runs",
            message_en="No deal-harvester sync run recorded yet.",
            message_th="ยังไม่มีการซิงก์ข้อมูล",
        )
    return {
        "id": str(row.id),
        "source": row.source,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "status": row.status,
        "upstream_count": row.upstream_count,
        "inserted_count": row.inserted_count,
        "updated_count": row.updated_count,
        "deactivated_count": row.deactivated_count,
        "mapping_queue_added": row.mapping_queue_added,
        "error_message": row.error_message,
    }


@router.post(
    "/canonicalize-merchants",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger merchant canonicalization batch",
)
async def trigger_canonicalize_merchants(
    background_tasks: BackgroundTasks,
    _auth: None = Depends(require_internal_api_key),
) -> dict[str, str]:
    """Queue the canonicalization job. Poll `/sync/deal-harvester/last`-style
    status via `GET /v1/internal/sync/merchant-canonicalizer/last` (not yet
    wired — check `sync_runs` table directly for now) or via the admin
    metrics dashboard. Same shape as `/sync/deal-harvester` for consistency.
    """
    background_tasks.add_task(run_canonicalization)
    return {"job_id": "merchant-canonicalizer", "status": "queued"}


@router.post(
    "/valuation/run",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger weekly valuation job",
)
async def run_valuation(
    _auth: None = Depends(require_internal_api_key),
) -> dict[str, str]:
    """Scaffold — Phase 2 will hook into `jobs.valuation.run_all`."""
    return {"job_id": "valuation", "status": "queued"}


class CacheWarmRequest(BaseModel):
    """Optional request body for `POST /v1/internal/cache-warm`.

    - `scope=None` (default) — legacy behavior: ping Sonnet once to keep the
      server-side prompt-prefix cache hot.
    - `scope="selector_personas"` — iterate the 120-persona warm-up matrix
      (`jobs.selector_warm_personas.build_persona_payloads`), calling the
      internal `_compute_or_get_cached` directly so each persona's result
      lands in Redis under `selector:{profile_hash}` with the standard 24h
      TTL. Added 2026-04-24 per DEVLOG Known Issue §1 option (c) — warm the
      result cache so most first-time submissions hit <0.3s instead of the
      15s cold-path Anthropic round-trip.
    """

    scope: Literal["selector_personas"] | None = None


@router.post(
    "/cache-warm",
    summary="Keep the Anthropic prompt prefix and/or Selector result cache hot",
)
async def cache_warm(
    body: CacheWarmRequest | None = None,
    _auth: None = Depends(require_internal_api_key),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Cron-driven cache warmer per AI_PROMPTS.md §Cache warming.

    Two modes, selected via the request body:

    * **Legacy (default / no body / `{}`):** pings Sonnet with the cached
      card-catalog prefix so the server-side Anthropic prompt-prefix cache
      stays hot. Short-circuits to `warmed:false` when ANTHROPIC_API_KEY is
      unset.
    * **`{"scope": "selector_personas"}`:** runs `_compute_or_get_cached`
      against the 120-persona matrix so their `SelectorResult`s are written
      into Redis (`selector:{profile_hash}`, 24h TTL). Reuses the normal
      Selector fallback chain end-to-end — this is intentional so a warm
      entry renders identically to an on-demand entry; the cron is a
      latency preload, not a shortcut path.
    """
    import time

    start = time.perf_counter()

    # Route to the selector-persona warmer when requested. The persona path
    # uses the internal selector compute function directly (NOT a fresh
    # HTTP round-trip), so it writes the exact same cache entries a real
    # user request would — hits on the cached key return the cached envelope
    # verbatim.
    if body is not None and body.scope == "selector_personas":
        from loftly.api.routes.selector import _compute_or_get_cached
        from loftly.jobs.selector_warm_personas import build_persona_payloads

        payloads = build_persona_payloads()
        errors = 0
        warmed = 0
        for persona in payloads:
            try:
                await _compute_or_get_cached(persona, session)
                warmed += 1
            except BaseException:
                # Swallow per-persona so a single bad payload doesn't abort
                # the whole 120-iteration warm-up. Individual failures surface
                # in per-request logs already (selector_sonnet_fallback, etc.).
                errors += 1
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {
            "warmed": warmed,
            "errors": errors,
            "elapsed_ms": round(elapsed_ms, 2),
        }

    # Legacy prompt-prefix warm path.
    from loftly.ai.providers.anthropic import _should_use_real_anthropic
    from loftly.api.routes.selector import _load_context

    if not _should_use_real_anthropic():
        return {
            "warmed": False,
            "reason": "anthropic_key_not_configured",
            "latency_ms": 0.0,
        }

    # Load context so we touch the same cached block the real path uses.
    context = await _load_context(session)
    from anthropic import AsyncAnthropic

    from loftly.ai.providers.anthropic import (
        SONNET_MODEL,
        _serialize_context,
    )
    from loftly.core.settings import get_settings

    settings = get_settings()
    client = AsyncAnthropic(api_key=settings.anthropic_api_key)
    await client.messages.create(
        model=SONNET_MODEL,
        max_tokens=16,
        system=[
            {"type": "text", "text": "cache-warm"},
            {
                "type": "text",
                "text": f"### CARD CATALOG + VALUATIONS\n{_serialize_context(context)}",
                "cache_control": {"type": "ephemeral"},
            },
        ],
        messages=[{"role": "user", "content": "ok"}],
    )
    latency_ms = (time.perf_counter() - start) * 1000.0
    return {"warmed": True, "latency_ms": round(latency_ms, 2)}


@router.post(
    "/audit-retention/run",
    summary="Execute audit_log retention sweep (PDPA-aware, action_type classified)",
)
async def run_audit_retention(
    _auth: None = Depends(require_internal_api_key),
) -> dict[str, Any]:
    """Delete expired `audit_log` rows per the two-bucket retention policy.

    - `consent.*`, `account.delete.*`, `privacy.*`, `pdpa.*` → 7 years
    - all other `action` values → 18 months

    Driven weekly by the Cloudflare Worker cron (Mon 03:00 ICT / Sun 20:00 UTC).
    The import of `loftly.jobs.audit_log_retention` is deferred to keep app
    startup cheap and to avoid the import-cycle class of failures that caused
    a prior revert of this endpoint.
    """
    from loftly.jobs.audit_log_retention import run_retention

    result = await run_retention(dry_run=False)
    return result.to_log_dict()


@router.post(
    "/content-stale-digest",
    summary="Email the founder a weekly digest of 90-day-stale published articles",
    response_model=None,
)
async def run_content_stale_digest(
    response: Response,
    _auth: None = Depends(require_internal_api_key),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any] | Response:
    """Weekly content re-verification reminder.

    Driven by the Cloudflare Worker cron (Mon 09:00 ICT / Mon 02:00 UTC). See
    `loftly-scheduler/src/index.ts` for the caller wiring.

    Status codes:
    - 204 when no articles are stale (nothing to email, no body).
    - 200 when the digest was emailed successfully.
    - 202 when stale articles exist but Resend is not configured — we still
      write the audit row so the ops record is preserved, but the email did
      not leave. This matches the scheduler's expectation that 2xx = "handled,
      don't retry".
    """
    from loftly.jobs.content_stale_digest import run_digest

    result = await run_digest(session)

    if result.count == 0:
        # 204 No Content — no body.
        response.status_code = status.HTTP_204_NO_CONTENT
        return response

    payload = result.to_log_dict()
    if not result.email_sent:
        response.status_code = status.HTTP_202_ACCEPTED
    return payload


@router.get(
    "/cost-anomaly-check",
    summary="Hourly LLM cost anomaly check (DRILL-002 leading indicator)",
    response_model=None,
)
async def run_cost_anomaly_check(
    response: Response,
    _auth: None = Depends(require_internal_api_key),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Compute current-hour LLM cost vs. trailing 24h hourly mean.

    Driven by the loftly-scheduler hourly cron at ``:05``. Returns the raw
    numbers so the cron log is inspectable even when no anomaly fires.

    Status codes:
    - 200 — check ran, includes ``is_anomaly`` flag + numbers.
    - 503 — Langfuse not configured or unreachable; payload carries
      ``skip_reason`` so the caller can decide whether to retry or log-and-move.
    """
    from loftly.jobs.cost_anomaly import check_cost_anomaly

    result = await check_cost_anomaly(session)
    payload = result.to_log_dict()
    if result.degraded:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return payload
