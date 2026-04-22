"""Promos public endpoints — `/v1/promos`.

Ships the list/filter surface defined in `mvp/API_CONTRACT.md §Promos`.

The promo rows themselves are kept fresh by `jobs/deal_harvester_sync.py`
(daily 04:00 ICT via `/v1/internal/sync/deal-harvester`). This route is
read-only; it surfaces the latest synced snapshot plus the freshness header
`X-Promo-Sync-Age-Hours` so clients (especially the Promo-Aware Selector's
context block per `SPEC.md §2 LOFTLY_FF_SELECTOR_PROMO_CONTEXT`) can fall
back to base-earn when the sync is stale (> 72h).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.api.rate_limit import FixedWindowLimiter
from loftly.db.engine import get_session
from loftly.db.models.audit import SyncRun
from loftly.db.models.bank import Bank
from loftly.db.models.merchant import MerchantCanonical, PromoMerchantCanonicalMap
from loftly.db.models.promo import Promo, promo_card_map
from loftly.schemas.promos import (
    BankRef,
    MerchantCanonicalRef,
    PromoListItem,
    PromoListMeta,
    PromoListResponse,
    PromoType,
)

router = APIRouter(prefix="/v1/promos", tags=["promos"])

# 120/min/IP matches API_CONTRACT.md.
LIST_LIMITER = FixedWindowLimiter(max_calls=120, window_sec=60)

_MAX_PAGE_SIZE = 50


def _ip_of(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _check_rate(request: Request) -> None:
    if not LIST_LIMITER.allow(_ip_of(request)):
        raise LoftlyError(
            status_code=429,
            code="rate_limited",
            message_en="Too many requests — slow down.",
            message_th="เรียกข้อมูลถี่เกินไป กรุณาลองใหม่ภายหลัง",
        )


async def _latest_sync_finished_at(session: AsyncSession) -> datetime | None:
    """Timestamp of the most recent successful deal-harvester sync.

    `None` means no successful sync has ever run (fresh install, or every
    attempt so far has failed). The public endpoint still returns data —
    callers use `meta.last_synced_at` / the `X-Promo-Sync-Age-Hours` header
    to decide whether to trust the snapshot.

    Always returns an aware UTC datetime when non-null (SQLite hands back
    naive timestamps; we normalize here so downstream serialization emits
    an ISO-8601 string with `+00:00`).
    """
    stmt = (
        select(SyncRun.finished_at)
        .where(
            SyncRun.source == "deal_harvester",
            SyncRun.status == "success",
            SyncRun.finished_at.is_not(None),
        )
        .order_by(SyncRun.finished_at.desc())
        .limit(1)
    )
    row = (await session.execute(stmt)).scalar_one_or_none()
    if row is None:
        return None
    return row if row.tzinfo else row.replace(tzinfo=UTC)


def _age_hours(finished: datetime | None) -> float | None:
    if finished is None:
        return None
    delta = datetime.now(UTC) - finished
    return round(delta.total_seconds() / 3600.0, 2)


@router.get(
    "",
    response_model=PromoListResponse,
    summary="Active promotions (filterable)",
)
async def list_promos(
    request: Request,
    response: Response,
    bank: str | None = Query(default=None, description="Bank slug (e.g. 'ktc')"),
    category: str | None = Query(default=None, description="Loftly category slug"),
    card_id: str | None = Query(default=None, description="Only promos mapped to this card"),
    merchant_name: str | None = Query(
        default=None, description="Partial match on raw merchant_name"
    ),
    active: bool = Query(default=True, description="Only active promos (default true)"),
    expiring_within_days: int | None = Query(
        default=None, ge=1, le=365, description="Only promos whose valid_until <= today + N"
    ),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=_MAX_PAGE_SIZE),
    session: AsyncSession = Depends(get_session),
) -> PromoListResponse:
    _check_rate(request)

    stmt = select(Promo).join(Bank, Promo.bank_id == Bank.id)
    count_stmt = select(Promo.id).join(Bank, Promo.bank_id == Bank.id)

    if active:
        stmt = stmt.where(Promo.active.is_(True))
        count_stmt = count_stmt.where(Promo.active.is_(True))
    if bank:
        stmt = stmt.where(Bank.slug == bank)
        count_stmt = count_stmt.where(Bank.slug == bank)
    if category:
        stmt = stmt.where(Promo.category == category)
        count_stmt = count_stmt.where(Promo.category == category)
    if merchant_name:
        pattern = f"%{merchant_name.lower()}%"
        stmt = stmt.where(Promo.merchant_name.ilike(pattern))
        count_stmt = count_stmt.where(Promo.merchant_name.ilike(pattern))
    if expiring_within_days is not None:
        from datetime import timedelta

        cutoff: date = datetime.now(UTC).date() + timedelta(days=expiring_within_days)
        stmt = stmt.where(
            Promo.valid_until.is_not(None),
            Promo.valid_until <= cutoff,
        )
        count_stmt = count_stmt.where(
            Promo.valid_until.is_not(None),
            Promo.valid_until <= cutoff,
        )
    if card_id:
        # Filter via the association table — only rows that have a mapping
        # to this card survive. Using EXISTS keeps the main query single-row
        # per promo (no duplication when a promo maps to >1 card).
        sub = (
            select(promo_card_map.c.promo_id)
            .where(promo_card_map.c.promo_id == Promo.id)
            .where(promo_card_map.c.card_id == card_id)
        )
        stmt = stmt.where(sub.exists())
        count_stmt = count_stmt.where(sub.exists())

    # Order: soonest to expire first, then most-recently synced. Drives a
    # sensible "what should I know about?" default for loftly-web.
    stmt = stmt.order_by(
        Promo.valid_until.is_(None),  # NULLs last (falsy=0, trues=1)
        Promo.valid_until.asc(),
        Promo.last_synced_at.desc(),
    )

    total = len((await session.execute(count_stmt)).scalars().all())

    # Aggregate coverage counts for `meta` — distinct banks and distinct
    # (non-null) merchant_names across the SAME filter set used for `items`.
    # Separate queries instead of piggy-backing on the item fetch because we
    # want the counts across ALL filtered rows, not just the current page.
    banks_count_stmt = (
        select(func.count(func.distinct(Promo.bank_id)))
        .select_from(Promo)
        .join(Bank, Promo.bank_id == Bank.id)
    )
    merchants_count_stmt = (
        select(func.count(func.distinct(Promo.merchant_name)))
        .select_from(Promo)
        .join(Bank, Promo.bank_id == Bank.id)
        .where(Promo.merchant_name.is_not(None))
    )

    if active:
        banks_count_stmt = banks_count_stmt.where(Promo.active.is_(True))
        merchants_count_stmt = merchants_count_stmt.where(Promo.active.is_(True))
    if bank:
        banks_count_stmt = banks_count_stmt.where(Bank.slug == bank)
        merchants_count_stmt = merchants_count_stmt.where(Bank.slug == bank)
    if category:
        banks_count_stmt = banks_count_stmt.where(Promo.category == category)
        merchants_count_stmt = merchants_count_stmt.where(Promo.category == category)
    if merchant_name:
        pattern = f"%{merchant_name.lower()}%"
        banks_count_stmt = banks_count_stmt.where(Promo.merchant_name.ilike(pattern))
        merchants_count_stmt = merchants_count_stmt.where(Promo.merchant_name.ilike(pattern))
    if expiring_within_days is not None:
        from datetime import timedelta

        cutoff2: date = datetime.now(UTC).date() + timedelta(days=expiring_within_days)
        banks_count_stmt = banks_count_stmt.where(
            Promo.valid_until.is_not(None), Promo.valid_until <= cutoff2
        )
        merchants_count_stmt = merchants_count_stmt.where(
            Promo.valid_until.is_not(None), Promo.valid_until <= cutoff2
        )
    if card_id:
        sub2 = (
            select(promo_card_map.c.promo_id)
            .where(promo_card_map.c.promo_id == Promo.id)
            .where(promo_card_map.c.card_id == card_id)
        )
        banks_count_stmt = banks_count_stmt.where(sub2.exists())
        merchants_count_stmt = merchants_count_stmt.where(sub2.exists())

    banks_count = (await session.execute(banks_count_stmt)).scalar_one() or 0
    merchants_count = (await session.execute(merchants_count_stmt)).scalar_one() or 0

    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    rows = list((await session.execute(stmt)).scalars().unique().all())

    # Bulk-load the join tables for just the promos on this page. Two extra
    # queries total, instead of N+1.
    bank_ids = {r.bank_id for r in rows}
    banks_stmt = select(Bank).where(Bank.id.in_(bank_ids))
    banks_by_id = {b.id: b for b in (await session.execute(banks_stmt)).scalars().all()}

    promo_ids = [r.id for r in rows]

    card_rows = (
        await session.execute(
            select(promo_card_map.c.promo_id, promo_card_map.c.card_id).where(
                promo_card_map.c.promo_id.in_(promo_ids)
            )
        )
        if promo_ids
        else None
    )
    cards_by_promo: dict[str, list[str]] = {}
    if card_rows is not None:
        for pid, cid in card_rows.all():
            cards_by_promo.setdefault(str(pid), []).append(str(cid))

    merchant_rows = (
        await session.execute(
            select(
                PromoMerchantCanonicalMap.promo_id,
                MerchantCanonical.slug,
                MerchantCanonical.display_name_th,
                MerchantCanonical.display_name_en,
            )
            .join(
                MerchantCanonical,
                PromoMerchantCanonicalMap.merchant_canonical_id == MerchantCanonical.id,
            )
            .where(PromoMerchantCanonicalMap.promo_id.in_(promo_ids))
        )
        if promo_ids
        else None
    )
    merchants_by_promo: dict[str, MerchantCanonicalRef] = {}
    if merchant_rows is not None:
        for pid, slug, name_th, name_en in merchant_rows.all():
            merchants_by_promo[str(pid)] = MerchantCanonicalRef(
                slug=slug, name_th=name_th, name_en=name_en
            )

    items: list[PromoListItem] = []
    for r in rows:
        b = banks_by_id[r.bank_id]
        items.append(
            PromoListItem(
                id=str(r.id),
                bank=BankRef(
                    id=str(b.id),
                    slug=b.slug,
                    name_th=b.display_name_th,
                    name_en=b.display_name_en,
                ),
                merchant_name=r.merchant_name,
                merchant_canonical=merchants_by_promo.get(str(r.id)),
                title_th=r.title_th,
                title_en=r.title_en,
                description_th=r.description_th,
                image_url=None,  # not stored in MVP — see SCHEMA.md
                category=r.category,
                promo_type=cast(PromoType | None, r.promo_type),
                discount_type=r.discount_type,
                discount_value=r.discount_value,
                discount_amount=r.discount_amount,
                discount_unit=r.discount_unit,
                minimum_spend=r.minimum_spend,
                valid_from=r.valid_from,
                valid_until=r.valid_until,
                source_url=r.source_url,
                card_ids=cards_by_promo.get(str(r.id), []),
            )
        )

    last_synced_at = await _latest_sync_finished_at(session)
    age = _age_hours(last_synced_at)
    response.headers["X-Promo-Sync-Age-Hours"] = str(age) if age is not None else "unknown"

    pages = (total + page_size - 1) // page_size if total else 0
    return PromoListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
        meta=PromoListMeta(
            total=total,
            banks=banks_count,
            merchants=merchants_count,
            last_synced_at=last_synced_at,
        ),
    )
