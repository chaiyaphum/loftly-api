"""Deal-harvester sync job.

Pulls `/api/v1/promotions?is_active=true&page_size=100` from upstream and
upserts `promos` keyed on `(external_bank_key, external_source_id)`. Skips
rows whose `external_checksum` matches cache (no useless updates). Soft-
deletes promos that disappeared from upstream. Auto-maps card_types → cards
for confident matches (exact + slug); unresolved go in the mapping queue.

Retries 3x with exponential backoff on transport failure; on terminal error
the `sync_runs` row lands as `status='failed'` with the exception message.
"""

from __future__ import annotations

import asyncio
import re
import unicodedata
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import SyncRun
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card as CardModel
from loftly.db.models.promo import Promo, promo_card_map

log = get_logger(__name__)

_SOURCE = "deal_harvester"
_PAGE_SIZE = 100
_MAX_ATTEMPTS = 3
_BACKOFF_BASE_SECONDS = 1.0
_DISCOUNT_VALUE_RE = re.compile(
    r"(?P<amount>\d+(?:,\d{3})*(?:\.\d+)?)\s*(?P<unit>%|baht|thb|points?|x|เท่า)",
    re.IGNORECASE,
)

# Normalize regex-captured unit strings to the enum values allowed by the
# `promos_discount_unit_check` CHECK constraint (migration 006): thb |
# percent | points | x_multiplier. Unknown units fall through as None so
# the row still inserts cleanly — amount alone is still useful downstream.
_DISCOUNT_UNIT_NORMALIZATION: dict[str, str] = {
    "%": "percent",
    "baht": "thb",
    "thb": "thb",
    "point": "points",
    "points": "points",
    "x": "x_multiplier",
    "เท่า": "x_multiplier",
}
_CATEGORY_SLUG_MAP = {
    "dining-restaurants": "dining",
    "shopping": "shopping",
    "travel": "travel",
    "grocery": "grocery",
    "petrol": "petrol",
    "online": "online",
}

# Upstream `discount_type` → Loftly `promos.promo_type` mapping per
# mvp/SCHEMA.md §9. Unknown / null upstream values stay NULL so the admin CMS
# can backfill — never default to a made-up enum value, because that would
# pollute the Selector's promo-filter logic.
_PROMO_TYPE_MAP: dict[str, str] = {
    "cashback": "cashback",
    "percentage": "category_bonus",
    "discount": "category_bonus",
    "points": "category_bonus",
}


def _map_promo_type(discount_type: str | None) -> str | None:
    if not discount_type:
        return None
    return _PROMO_TYPE_MAP.get(discount_type.lower())


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", ascii_only.lower()).strip("-")


def _parse_discount_value(raw: str | None) -> tuple[Decimal | None, str | None]:
    if not raw or not isinstance(raw, str):
        return None, None
    match = _DISCOUNT_VALUE_RE.search(raw)
    if match is None:
        return None, None
    amount = Decimal(match.group("amount").replace(",", ""))
    captured_unit = match.group("unit").lower()
    unit = _DISCOUNT_UNIT_NORMALIZATION.get(captured_unit)
    return amount, unit


def _parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            return None


async def _fetch_page(
    client: httpx.AsyncClient,
    *,
    base: str,
    api_key: str | None,
    page: int,
) -> dict[str, Any]:
    headers: dict[str, str] = {}
    if api_key:
        headers["X-API-Key"] = api_key
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            resp = await client.get(
                f"{base.rstrip('/')}/promotions",
                params={"is_active": "true", "page_size": _PAGE_SIZE, "page": page},
                headers=headers,
                timeout=30.0,
            )
            resp.raise_for_status()
            return dict(resp.json())
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:  # pragma: no cover
            last_exc = exc
            if attempt == _MAX_ATTEMPTS:
                break
            delay = _BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "deal_harvester_page_retry",
                attempt=attempt,
                page=page,
                error=str(exc),
                delay=delay,
            )
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc


async def _auto_map_cards(
    session: AsyncSession, promo: Promo, card_types: list[str], cards: list[CardModel]
) -> int:
    """Apply steps 1+2 from DATA_INGESTION.md — exact + slug match only."""
    name_index = {c.display_name.lower(): c.id for c in cards}
    slug_index = {_slugify(c.display_name): c.id for c in cards}
    matched: set[uuid.UUID] = set()
    for raw in card_types:
        if not isinstance(raw, str):
            continue
        hit = name_index.get(raw.lower().strip())
        if hit is None:
            hit = slug_index.get(_slugify(raw))
        if hit is not None:
            matched.add(hit)
    if not matched:
        return 0

    already = set(
        (
            await session.execute(
                select(promo_card_map.c.card_id).where(promo_card_map.c.promo_id == promo.id)
            )
        )
        .scalars()
        .all()
    )
    inserted = 0
    for cid in matched:
        if cid in already:
            continue
        await session.execute(promo_card_map.insert().values(promo_id=promo.id, card_id=cid))
        inserted += 1
    return inserted


async def _upsert_promo(
    session: AsyncSession,
    upstream: dict[str, Any],
    banks_by_source: dict[str, Bank],
    cards: list[CardModel],
) -> tuple[str, int]:
    """Upsert one upstream promo. Returns ('inserted'|'updated'|'skipped', mapped_count)."""
    external_source_id = str(upstream.get("id") or upstream.get("source_id") or "")
    bank_key = upstream.get("bank")
    if not external_source_id or not bank_key:
        return "skipped", 0
    bank = banks_by_source.get(str(bank_key))
    if bank is None:
        log.warning("deal_harvester_unknown_bank", bank=bank_key)
        return "skipped", 0

    checksum = upstream.get("checksum")
    existing = (
        (
            await session.execute(
                select(Promo).where(
                    Promo.external_bank_key == bank_key,
                    Promo.external_source_id == external_source_id,
                )
            )
        )
        .scalars()
        .unique()
        .one_or_none()
    )

    discount_amount, discount_unit = _parse_discount_value(upstream.get("discount_value"))
    category_raw = upstream.get("category")
    category = (
        _CATEGORY_SLUG_MAP.get(str(category_raw), str(category_raw)) if category_raw else None
    )

    payload: dict[str, Any] = {
        "bank_id": bank.id,
        "external_source_id": external_source_id,
        "external_bank_key": str(bank_key),
        "external_checksum": str(checksum) if checksum else None,
        "source_url": upstream.get("source_url") or upstream.get("url") or "",
        "promo_type": _map_promo_type(upstream.get("discount_type")),
        "title_th": upstream.get("title") or "",
        "description_th": upstream.get("description"),
        "merchant_name": upstream.get("merchant_name"),
        "category": category,
        "discount_type": upstream.get("discount_type"),
        "discount_value": upstream.get("discount_value"),
        "discount_amount": discount_amount,
        "discount_unit": discount_unit,
        "minimum_spend": (
            Decimal(str(upstream["minimum_spend"]))
            if upstream.get("minimum_spend") is not None
            else None
        ),
        "valid_from": _parse_date(upstream.get("start_date")),
        "valid_until": _parse_date(upstream.get("end_date")),
        "terms_and_conditions": upstream.get("terms_and_conditions"),
        "raw_data": upstream,
        "active": bool(upstream.get("is_active", True)),
        "last_synced_at": datetime.now(UTC),
    }

    card_types = list(upstream.get("card_types") or [])

    if existing is None:
        promo = Promo(**payload)
        session.add(promo)
        await session.flush()
        mapped = await _auto_map_cards(session, promo, card_types, cards)
        return "inserted", mapped

    # Skip content update if checksum matches — saves write volume.
    if checksum and existing.external_checksum == str(checksum):
        existing.last_synced_at = datetime.now(UTC)
        # Still attempt mapping in case a new card was seeded since last run.
        mapped = await _auto_map_cards(session, existing, card_types, cards)
        return "skipped", mapped

    for field, value in payload.items():
        setattr(existing, field, value)
    await session.flush()
    mapped = await _auto_map_cards(session, existing, card_types, cards)
    return "updated", mapped


async def run_sync(
    *,
    client_factory: Any = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Main entry point. Writes a `sync_runs` row and returns its payload.

    `client_factory`: optional callable returning an `httpx.AsyncClient`. Tests
    inject a mocked client; production leaves it `None`.
    """
    s = settings or get_settings()
    base = s.deal_harvester_base
    api_key = s.deal_harvester_api_key

    sessionmaker = get_sessionmaker()
    started = datetime.now(UTC)
    run_row_id: uuid.UUID | None = None
    async with sessionmaker() as session:
        run_row = SyncRun(source=_SOURCE, started_at=started, status="running")
        session.add(run_row)
        await session.flush()
        run_row_id = run_row.id
        await session.commit()

    upstream_count = 0
    inserted_count = 0
    updated_count = 0
    mapping_queue_added = 0
    deactivated_count = 0
    error_message: str | None = None
    status = "success"
    seen_ids: dict[str, set[str]] = {}  # bank_key -> {external_source_ids}

    client_ctx = client_factory() if client_factory else httpx.AsyncClient()
    try:
        async with client_ctx as client:
            page = 1
            async with sessionmaker() as session:
                banks = list((await session.execute(select(Bank))).scalars().all())
                banks_by_source = {b.source_key: b for b in banks if b.source_key}
                cards = list((await session.execute(select(CardModel))).scalars().unique().all())

                while True:
                    body = await _fetch_page(client, base=base, api_key=api_key, page=page)
                    items = list(body.get("items") or [])
                    if not items:
                        break
                    upstream_count += len(items)
                    for upstream in items:
                        bank_key = str(upstream.get("bank") or "")
                        ext_id = str(upstream.get("id") or upstream.get("source_id") or "")
                        if bank_key and ext_id:
                            seen_ids.setdefault(bank_key, set()).add(ext_id)
                        action, mapped = await _upsert_promo(
                            session, upstream, banks_by_source, cards
                        )
                        if action == "inserted":
                            inserted_count += 1
                        elif action == "updated":
                            updated_count += 1
                        if mapped:
                            mapping_queue_added += mapped
                    await session.commit()

                    pages = int(body.get("pages") or 1)
                    if page >= pages:
                        break
                    page += 1

                # Soft-delete missing upstream rows per bank_key.
                for bank_key, ids in seen_ids.items():
                    if not ids:
                        continue
                    result = await session.execute(
                        update(Promo)
                        .where(
                            Promo.external_bank_key == bank_key,
                            Promo.external_source_id.is_not(None),
                            Promo.external_source_id.not_in(ids),
                            Promo.active.is_(True),
                        )
                        .values(active=False)
                    )
                    deactivated_count += int(getattr(result, "rowcount", 0) or 0)
                await session.commit()
    except Exception as exc:
        log.exception("deal_harvester_sync_failed")
        error_message = f"{type(exc).__name__}: {exc}"[:500]
        status = "failed"

    async with sessionmaker() as session:
        run = (
            (await session.execute(select(SyncRun).where(SyncRun.id == run_row_id))).scalars().one()
        )
        run.status = status
        run.finished_at = datetime.now(UTC)
        run.upstream_count = upstream_count
        run.inserted_count = inserted_count
        run.updated_count = updated_count
        run.deactivated_count = deactivated_count
        run.mapping_queue_added = mapping_queue_added
        run.error_message = error_message
        await session.commit()
        await session.refresh(run)
        return {
            "id": str(run.id),
            "source": run.source,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "status": run.status,
            "upstream_count": run.upstream_count,
            "inserted_count": run.inserted_count,
            "updated_count": run.updated_count,
            "deactivated_count": run.deactivated_count,
            "mapping_queue_added": run.mapping_queue_added,
            "error_message": run.error_message,
        }


__all__ = ["run_sync"]
