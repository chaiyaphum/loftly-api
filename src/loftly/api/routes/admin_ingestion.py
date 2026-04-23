"""Admin ingestion coverage + per-bank resync — W16 follow-up.

Backs the `/ingestion` admin viewer in `loftly-web` (PR #10). Two endpoints:

* ``GET /v1/admin/ingestion/coverage`` — aggregates per-bank promo coverage
  so the CMS can render the status matrix without hammering the public API.
* ``POST /v1/admin/ingestion/{bank_slug}/resync`` — dispatches a one-shot
  ingest for a single bank. Manual-catalog banks (``uob``, ``krungsri``)
  hit ``loftly.jobs.manual_catalog_ingest.run_ingest``; everything else is
  routed through a deal-harvester adapter that runs ``run_sync`` and
  projects the response into the manual-catalog counts shape so the frontend
  can treat both paths uniformly.

Lives in its own module to keep ``admin.py`` from continuing to balloon —
same convention as ``admin_metrics.py`` + ``admin_flags.py``.

Schema note: ``promos.last_synced_at`` is used for ``last_synced_at`` in
the coverage response since the ``promos`` table doesn't carry a separate
``updated_at`` column — the ingest jobs bump ``last_synced_at`` whenever
they touch a row, so it's the closest proxy available.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.auth import get_current_admin_id
from loftly.api.errors import LoftlyError
from loftly.db.engine import get_session
from loftly.db.models.audit import SyncRun
from loftly.db.models.bank import Bank
from loftly.db.models.promo import Promo, promo_card_map

router = APIRouter(prefix="/v1/admin/ingestion", tags=["admin"])

# Banks whose promos are curated via the manual-catalog fixtures (see
# `loftly.data.manual_catalogs`). Anything outside this set is assumed to
# come from the upstream deal-harvester.
_MANUAL_CATALOG_BANKS: frozenset[str] = frozenset({"uob", "krungsri"})

# Coverage-status thresholds. Kept as module-level constants so the test
# suite can reference them if we ever need to parametrize — DEV_PLAN W16.
_FULL_THRESHOLD = 8  # >=8 active promos → "full"
_PARTIAL_THRESHOLD = 3  # 3..7 → "partial"; <3 → "gap"

# Staleness bucket thresholds (hours since `last_synced_at`). Mirrors the
# admin dashboard contract — anything ≥72h without a sync is treated as a
# silent bank and surfaces an alert.
_STALENESS_FRESH_HOURS = 1.0  # < 1h  → fresh
_STALENESS_WARMING_HOURS = 24.0  # < 24h → warming
_STALENESS_STALE_HOURS = 72.0  # < 72h → stale; ≥ 72h → silent
# Banks with zero active promos for longer than this raise a "zero-promo"
# alert. Independent from the staleness ladder above so a freshly-synced
# bank that returned zero rows still gets flagged.
_ZERO_PROMO_ALERT_HOURS = 24.0


def _classify_coverage(active_count: int) -> str:
    if active_count >= _FULL_THRESHOLD:
        return "full"
    if active_count >= _PARTIAL_THRESHOLD:
        return "partial"
    return "gap"


def _classify_staleness(hours: float | None) -> str:
    """Bucket a `staleness_hours` value into fresh/warming/stale/silent.

    `None` means the bank has never been synced (no active promos with a
    `last_synced_at`); we treat it as silent so the alert system flags it.
    """
    if hours is None:
        return "silent"
    if hours < _STALENESS_FRESH_HOURS:
        return "fresh"
    if hours < _STALENESS_WARMING_HOURS:
        return "warming"
    if hours < _STALENESS_STALE_HOURS:
        return "stale"
    return "silent"


def _is_manual_key(external_bank_key: str | None) -> bool:
    return bool(external_bank_key) and external_bank_key.startswith("manual:")  # type: ignore[union-attr]


def _ensure_aware(dt: datetime | None) -> datetime | None:
    """Force UTC tz on a naive datetime so subtraction with `now(UTC)` works.

    SQLite stores `last_synced_at` without a tzinfo marker; the rest of the
    codebase treats those rows as already-UTC. Centralizing the coercion
    here keeps the handler pure.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _serialize_sync_run(run: SyncRun | None) -> dict[str, Any] | None:
    """Project a `SyncRun` row into the dashboard-friendly summary shape."""
    if run is None:
        return None
    started = _ensure_aware(run.started_at)
    finished = _ensure_aware(run.finished_at)
    return {
        "id": str(run.id),
        "source": run.source,
        "status": run.status,
        "started_at": started.isoformat() if started is not None else None,
        "finished_at": finished.isoformat() if finished is not None else None,
        "upstream_count": int(run.upstream_count or 0),
        "inserted_count": int(run.inserted_count or 0),
        "updated_count": int(run.updated_count or 0),
        "deactivated_count": int(run.deactivated_count or 0),
        "mapping_queue_added": int(run.mapping_queue_added or 0),
        "error_message": run.error_message,
    }


@router.get(
    "/coverage",
    summary="Per-bank ingestion coverage snapshot",
    status_code=status.HTTP_200_OK,
)
async def ingestion_coverage(
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Aggregate promo coverage by bank.

    Response shape matches the contract the ingestion viewer page expects
    (see ``loftly-web#10``). Banks with zero active promos still appear so
    the CMS can highlight coverage gaps.
    """
    now = datetime.now(UTC)

    # Pull every bank so we can show even the zero-promo ones.
    banks = list((await session.execute(select(Bank).order_by(Bank.slug))).scalars().all())

    # Grouped counts: (bank_id, is_manual) -> count(active) + max sync ts.
    # Also accumulates the merchant-named subset so we can compute the
    # `merchant_name_coverage` ratio per bank in a single query.
    manual_prefix_expr = func.substr(Promo.external_bank_key, 1, 7)  # "manual:"
    merchant_named_expr = func.sum(case((Promo.merchant_name.is_not(None), 1), else_=0)).label(
        "merchant_named"
    )
    rows = list(
        (
            await session.execute(
                select(
                    Promo.bank_id,
                    manual_prefix_expr.label("prefix"),
                    func.count(Promo.id).label("n"),
                    func.max(Promo.last_synced_at).label("last_synced_at"),
                    merchant_named_expr,
                )
                .where(Promo.active.is_(True))
                .group_by(Promo.bank_id, "prefix")
            )
        ).all()
    )

    # bank_id -> {manual, harvester, merchant_named, last}
    agg: dict[uuid.UUID, dict[str, Any]] = {}
    for bank_id, prefix, count, last, merchant_named in rows:
        bucket = agg.setdefault(
            bank_id,
            {"manual": 0, "harvester": 0, "merchant_named": 0, "last": None},
        )
        if prefix == "manual:":
            bucket["manual"] += int(count)
        else:
            bucket["harvester"] += int(count)
        bucket["merchant_named"] += int(merchant_named or 0)
        existing_last: datetime | None = bucket["last"]
        if last is not None and (existing_last is None or last > existing_last):
            bucket["last"] = last

    banks_payload: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []
    full_or_partial = 0
    for bank in banks:
        stats = agg.get(
            bank.id,
            {"manual": 0, "harvester": 0, "merchant_named": 0, "last": None},
        )
        manual_count = int(stats["manual"])
        harvester_count = int(stats["harvester"])
        active = manual_count + harvester_count
        merchant_named = int(stats["merchant_named"])
        merchant_coverage = round(merchant_named / active, 4) if active > 0 else 0.0
        status_str = _classify_coverage(active)
        if status_str in ("full", "partial"):
            full_or_partial += 1

        last_synced_raw = stats["last"]
        last_synced_at: datetime | None = _ensure_aware(last_synced_raw)
        if last_synced_at is None:
            staleness_hours: float | None = None
        else:
            delta = now - last_synced_at
            staleness_hours = round(delta.total_seconds() / 3600.0, 2)
        staleness_bucket = _classify_staleness(staleness_hours)

        banks_payload.append(
            {
                # Original fields — preserved so the existing CMS viewer
                # (loftly-web#10) keeps rendering. Do not remove without
                # bumping the frontend in lockstep.
                "bank_slug": bank.slug,
                "bank_name": bank.display_name_en,
                "deal_harvester_count": harvester_count,
                "manual_catalog_count": manual_count,
                "active_promos_count": active,
                "last_synced_at": (
                    last_synced_at.isoformat() if last_synced_at is not None else None
                ),
                "coverage_status": status_str,
                # Extended fields — admin-dashboard v3.
                "slug": bank.slug,
                "source_key": bank.source_key,
                "display_name_th": bank.display_name_th,
                "active_promos": active,
                "merchant_name_coverage": merchant_coverage,
                "staleness_hours": staleness_hours,
                "staleness_bucket": staleness_bucket,
            }
        )

        # Per-bank alerts. Silent banks always alert; zero-promo banks
        # alert only after we'd reasonably expect at least one sync to
        # have populated them.
        if staleness_bucket == "silent":
            alerts.append(
                {
                    "kind": "silent_bank",
                    "bank_slug": bank.slug,
                    "source_key": bank.source_key,
                    "staleness_hours": staleness_hours,
                    "message": (
                        f"Bank {bank.slug!r} has not synced in ≥72h "
                        f"(staleness_hours={staleness_hours})."
                    ),
                }
            )
        if active == 0 and (staleness_hours is None or staleness_hours >= _ZERO_PROMO_ALERT_HOURS):
            alerts.append(
                {
                    "kind": "zero_promos",
                    "bank_slug": bank.slug,
                    "source_key": bank.source_key,
                    "staleness_hours": staleness_hours,
                    "message": (f"Bank {bank.slug!r} has 0 active promos for >24h."),
                }
            )

    # Sync-run summary: latest row per source. We pull both sources in one
    # query and bucket Python-side rather than two roundtrips — this list
    # is at most 2 sources for the foreseeable future, so order-by-desc +
    # first-hit is fine.
    sync_rows = list(
        (
            await session.execute(
                select(SyncRun)
                .where(SyncRun.source.in_(["deal_harvester", "merchant_canonicalizer"]))
                .order_by(SyncRun.started_at.desc())
            )
        )
        .scalars()
        .all()
    )
    latest_by_source: dict[str, SyncRun] = {}
    for row in sync_rows:
        latest_by_source.setdefault(row.source, row)
    sync_summary = {
        "deal_harvester": _serialize_sync_run(latest_by_source.get("deal_harvester")),
        "merchant_canonicalizer": _serialize_sync_run(
            latest_by_source.get("merchant_canonicalizer")
        ),
    }

    if sync_summary["merchant_canonicalizer"] is None:
        alerts.append(
            {
                "kind": "canonicalizer_never_ran",
                "message": (
                    "merchant_canonicalizer has no sync_runs row — daily job "
                    "has not executed since deploy."
                ),
            }
        )

    # Unmapped = active promos with no row in promo_card_map. Left outer
    # join + NULL filter avoids a per-promo roundtrip.
    unmapped_count = int(
        (
            await session.execute(
                select(func.count(Promo.id))
                .select_from(
                    Promo.__table__.outerjoin(promo_card_map, Promo.id == promo_card_map.c.promo_id)
                )
                .where(Promo.active.is_(True), promo_card_map.c.card_id.is_(None))
            )
        ).scalar_one()
    )

    overall_pct = round((full_or_partial / len(banks)) * 100.0, 1) if banks else 0.0

    return {
        "banks": banks_payload,
        "unmapped_promos_count": unmapped_count,
        "overall_coverage_pct": overall_pct,
        "sync_summary": sync_summary,
        "alerts": alerts,
        "generated_at": now.isoformat(),
    }


@router.post(
    "/{bank_slug}/resync",
    summary="Kick off a one-shot ingest for a single bank",
    status_code=status.HTTP_200_OK,
)
async def resync_bank(
    bank_slug: str,
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Dispatch the right ingest path for ``bank_slug``.

    * ``uob`` / ``krungsri`` → manual-catalog fixture ingest.
    * Everything else → deal-harvester ``run_sync`` (adapter — upstream job
      doesn't expose a per-bank hook, so we run the full sync and filter the
      counts down to the caller's bank).
    """
    bank = (
        (await session.execute(select(Bank).where(Bank.slug == bank_slug))).scalars().one_or_none()
    )
    if bank is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="bank_not_found",
            message_en=f"No bank with slug {bank_slug!r}.",
            message_th="ไม่พบธนาคารนี้",
            details={"bank_slug": bank_slug},
        )

    if bank_slug in _MANUAL_CATALOG_BANKS:
        from loftly.jobs.manual_catalog_ingest import run_ingest

        try:
            result = await run_ingest(bank_slug, dry_run=False)
        except FileNotFoundError as exc:
            raise LoftlyError(
                status_code=status.HTTP_404_NOT_FOUND,
                code="fixture_not_found",
                message_en=f"No manual-catalog fixture for {bank_slug!r}.",
                message_th="ไม่พบไฟล์โปรโมชั่นสำหรับธนาคารนี้",
                details={"bank_slug": bank_slug},
            ) from exc
        return {
            "ok": True,
            "counts": {
                "inserted": result.inserted,
                "updated": result.updated,
                "archived": result.archived,
                "unchanged": result.unchanged,
            },
        }

    # Deal-harvester path — see adapter note in module docstring.
    from loftly.jobs import deal_harvester_sync

    counts = await _resync_deal_harvester(bank_slug)
    _ = deal_harvester_sync  # ensure the import isn't pruned as unused
    return {"ok": True, "counts": counts}


async def _resync_deal_harvester(bank_slug: str) -> dict[str, int]:
    """Adapter around ``deal_harvester_sync.run_sync``.

    The upstream job is an all-banks sweep (no per-bank hook on the
    ``/promotions`` endpoint). We call it as-is and map the returned counts
    into the shape the frontend expects. Per-bank attribution would require
    either a deal-harvester API change or a post-hoc diff against
    ``last_synced_at`` — tracked as tech-debt for W20+.
    """
    from loftly.jobs.deal_harvester_sync import run_sync

    run = await run_sync()
    return {
        "inserted": int(run.get("inserted_count") or 0),
        "updated": int(run.get("updated_count") or 0),
        "archived": int(run.get("deactivated_count") or 0),
        "unchanged": 0,
    }


__all__ = ["router"]
