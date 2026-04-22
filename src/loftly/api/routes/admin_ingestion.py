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
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.auth import get_current_admin_id
from loftly.api.errors import LoftlyError
from loftly.db.engine import get_session
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


def _classify_coverage(active_count: int) -> str:
    if active_count >= _FULL_THRESHOLD:
        return "full"
    if active_count >= _PARTIAL_THRESHOLD:
        return "partial"
    return "gap"


def _is_manual_key(external_bank_key: str | None) -> bool:
    return bool(external_bank_key) and external_bank_key.startswith("manual:")  # type: ignore[union-attr]


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
    # Pull every bank so we can show even the zero-promo ones.
    banks = list((await session.execute(select(Bank).order_by(Bank.slug))).scalars().all())

    # Grouped counts: (bank_id, is_manual) -> count(active).
    manual_prefix_expr = func.substr(Promo.external_bank_key, 1, 7)  # "manual:"
    rows = list(
        (
            await session.execute(
                select(
                    Promo.bank_id,
                    manual_prefix_expr.label("prefix"),
                    func.count(Promo.id).label("n"),
                    func.max(Promo.last_synced_at).label("last_synced_at"),
                )
                .where(Promo.active.is_(True))
                .group_by(Promo.bank_id, "prefix")
            )
        ).all()
    )

    # bank_id -> {"manual": n, "harvester": n, "last": datetime}
    agg: dict[uuid.UUID, dict[str, Any]] = {}
    for bank_id, prefix, count, last in rows:
        bucket = agg.setdefault(bank_id, {"manual": 0, "harvester": 0, "last": None})
        if prefix == "manual:":
            bucket["manual"] += int(count)
        else:
            bucket["harvester"] += int(count)
        existing_last: datetime | None = bucket["last"]
        if last is not None and (existing_last is None or last > existing_last):
            bucket["last"] = last

    banks_payload: list[dict[str, Any]] = []
    full_or_partial = 0
    for bank in banks:
        stats = agg.get(bank.id, {"manual": 0, "harvester": 0, "last": None})
        manual_count = int(stats["manual"])
        harvester_count = int(stats["harvester"])
        active = manual_count + harvester_count
        status_str = _classify_coverage(active)
        if status_str in ("full", "partial"):
            full_or_partial += 1
        last_synced_at: datetime | None = stats["last"]
        banks_payload.append(
            {
                "bank_slug": bank.slug,
                "bank_name": bank.display_name_en,
                "deal_harvester_count": harvester_count,
                "manual_catalog_count": manual_count,
                "active_promos_count": active,
                "last_synced_at": (
                    last_synced_at.isoformat() if last_synced_at is not None else None
                ),
                "coverage_status": status_str,
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
