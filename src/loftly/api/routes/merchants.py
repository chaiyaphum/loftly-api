"""Merchant Reverse Lookup routes — `/v1/merchants/*`.

Ships the read side of `mvp/POST_V1.md §9` (ratified 2026-04-22 Q18):

- `GET /v1/merchants/search?q=&locale=` — autocomplete, Redis-cached 600s,
  60/min/IP anon. Walks `merchants_canonical` with a best-effort exact /
  substring match; Postgres gains trigram + GIN on alt_names via migration.
- `GET /v1/merchants/{slug}` — full page data: merchant + ranked_cards
  (via `services/merchant_ranking.rank_cards_for_merchant`). Cache key
  includes user_scope; TTL 300s anon / 60s authed.
- `GET /v1/merchants` — browse hub with `category`, `letter` filters.
  SSG cached 15min.
- `POST /v1/admin/merchants/{id}/merge` — **501 Not Implemented** for v1.
- `POST /v1/admin/merchants/{id}/split` — **501 Not Implemented** for v1.
- `GET /v1/admin/merchants/mapping-queue` — **501 Not Implemented** for v1.

Admin routes are stubbed per the workstream spec; the schemas in
`schemas/merchants.py` (AdminMergeRequest / AdminSplitRequest) are ready
for the follow-up PR. See §9.1 in POST_V1.md for the queue design.

Gated behind the `merchants_reverse_lookup_enabled` settings flag — when
False the routes return 501 so staging can dark-launch without leaking a
half-built surface.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.api.rate_limit import FixedWindowLimiter
from loftly.core.cache import get_cache
from loftly.core.logging import get_logger
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_session
from loftly.db.models.merchant import (
    MerchantCanonical as MerchantCanonicalModel,
)
from loftly.db.models.merchant import (
    PromoMerchantCanonicalMap,
)
from loftly.db.models.promo import Promo
from loftly.schemas.merchants import (
    HreflangAlternate,
    MerchantCanonical,
    MerchantListItem,
    MerchantListResponse,
    MerchantPageData,
    MerchantSearchResult,
)
from loftly.services.merchant_ranking import rank_cards_for_merchant

router = APIRouter(prefix="/v1/merchants", tags=["merchants"])
log = get_logger(__name__)


# --- Rate limiters (IP-based, in-memory; mirrors /selector/recent pattern) ---
SEARCH_LIMITER = FixedWindowLimiter(max_calls=60, window_sec=60)
PAGE_LIMITER = FixedWindowLimiter(max_calls=120, window_sec=60)
HUB_LIMITER = FixedWindowLimiter(max_calls=120, window_sec=60)

# Cache TTLs — see module docstring.
_SEARCH_TTL_SEC = 600
_PAGE_ANON_TTL_SEC = 300
_HUB_TTL_SEC = 900


def _require_feature(settings: Settings) -> None:
    """501 when the workstream is gated off (default in dev)."""
    if not settings.merchants_reverse_lookup_enabled:
        raise LoftlyError(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            code="merchants_reverse_lookup_disabled",
            message_en="Merchant Reverse Lookup is not enabled in this environment.",
            message_th="ฟีเจอร์ค้นหาบัตรตามร้านค้ายังไม่เปิดใช้งาน",
        )


def _ip_of(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _check_rate(limiter: FixedWindowLimiter, ip: str) -> None:
    if not limiter.allow(ip):
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="rate_limited",
            message_en="Too many requests — slow down.",
            message_th="เรียกข้อมูลถี่เกินไป กรุณาลองใหม่ภายหลัง",
        )


def _build_hreflang(slug: str) -> list[HreflangAlternate]:
    """Static site URL mirrors the language-alternate contract in loftly-web."""
    # Keep in sync with `loftly-web/src/lib/seo/metadata.ts#languageAlternates`.
    base = "https://loftly.co.th"
    path = f"/merchants/{slug}"
    return [
        HreflangAlternate(locale="th-TH", href=f"{base}{path}"),
        HreflangAlternate(locale="en-US", href=f"{base}/en{path}"),
        HreflangAlternate(locale="x-default", href=f"{base}{path}"),
    ]


def _to_schema(m: MerchantCanonicalModel) -> MerchantCanonical:
    return MerchantCanonical(
        id=str(m.id),
        slug=m.slug,
        display_name_th=m.display_name_th,
        display_name_en=m.display_name_en,
        category_default=m.category_default,
        alt_names=list(m.alt_names or []),
        logo_url=m.logo_url,
        description_th=m.description_th,
        description_en=m.description_en,
        merchant_type=m.merchant_type,  # type: ignore[arg-type]
        status=m.status,  # type: ignore[arg-type]
    )


async def _count_active_promos(session: AsyncSession, merchant_id: Any) -> int:
    """Cheap helper — how many active promos are currently mapped here."""
    stmt = (
        select(Promo.id)
        .join(
            PromoMerchantCanonicalMap,
            PromoMerchantCanonicalMap.promo_id == Promo.id,
        )
        .where(
            PromoMerchantCanonicalMap.merchant_canonical_id == merchant_id,
            Promo.active.is_(True),
        )
    )
    rows = (await session.execute(stmt)).scalars().all()
    return len(list(rows))


# ---------------------------------------------------------------------------
# GET /v1/merchants/search
# ---------------------------------------------------------------------------


@router.get(
    "/search",
    response_model=list[MerchantSearchResult],
    summary="Autocomplete merchant name",
)
async def search_merchants(
    request: Request,
    q: str = Query(..., min_length=1, max_length=64),
    locale: str = Query(default="th"),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> list[MerchantSearchResult]:
    """Prefix / substring lookup. Top 10, Redis-cached 600s."""
    _require_feature(settings)
    _check_rate(SEARCH_LIMITER, _ip_of(request))
    _ = locale  # reserved for future per-locale ranking

    cache = get_cache()
    cache_key = f"merchants:search:{q.lower()}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return [MerchantSearchResult.model_validate(row) for row in cached]

    # Case-insensitive substring on display names + slug. Postgres gets the
    # proper GIN + trigram via the migration; this statement is portable.
    pattern = f"%{q.lower()}%"
    stmt = (
        select(MerchantCanonicalModel)
        .where(MerchantCanonicalModel.status == "active")
        .where(
            or_(
                MerchantCanonicalModel.slug.ilike(pattern),
                MerchantCanonicalModel.display_name_en.ilike(pattern),
                MerchantCanonicalModel.display_name_th.ilike(pattern),
            )
        )
        .limit(10)
    )
    rows = list((await session.execute(stmt)).scalars().all())

    results: list[MerchantSearchResult] = []
    for m in rows:
        count = await _count_active_promos(session, m.id)
        display = (
            m.display_name_th if locale.startswith("th") else m.display_name_en
        )
        results.append(
            MerchantSearchResult(
                slug=m.slug,
                display_name=display or m.display_name_en,
                logo_url=m.logo_url,
                active_promo_count=count,
                category_default=m.category_default,
            )
        )

    await cache.set(
        cache_key,
        [r.model_dump(mode="json") for r in results],
        _SEARCH_TTL_SEC,
    )
    return results


# ---------------------------------------------------------------------------
# GET /v1/merchants  — browse hub
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=MerchantListResponse,
    summary="Browse merchants by category/letter",
)
async def list_merchants(
    request: Request,
    category: str | None = Query(default=None),
    letter: str | None = Query(default=None, min_length=1, max_length=2),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> MerchantListResponse:
    """Browse hub: filter by category (promo category vocab) or leading letter."""
    _require_feature(settings)
    _check_rate(HUB_LIMITER, _ip_of(request))

    cache = get_cache()
    cache_key = f"merchants:list:cat={category or ''}:ltr={letter or ''}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return MerchantListResponse.model_validate(cached)

    stmt = select(MerchantCanonicalModel).where(
        MerchantCanonicalModel.status == "active"
    )
    if category:
        stmt = stmt.where(MerchantCanonicalModel.category_default == category)
    if letter:
        leading = letter.lower()[0]
        # Match either display_name_en OR display_name_th leading character
        stmt = stmt.where(
            or_(
                MerchantCanonicalModel.display_name_en.ilike(f"{leading}%"),
                MerchantCanonicalModel.display_name_th.ilike(f"{leading}%"),
            )
        )
    stmt = stmt.order_by(MerchantCanonicalModel.display_name_en.asc())
    rows = list((await session.execute(stmt)).scalars().all())

    items: list[MerchantListItem] = []
    for m in rows:
        count = await _count_active_promos(session, m.id)
        items.append(
            MerchantListItem(
                slug=m.slug,
                display_name_th=m.display_name_th,
                display_name_en=m.display_name_en,
                category_default=m.category_default,
                merchant_type=m.merchant_type,  # type: ignore[arg-type]
                active_promo_count=count,
            )
        )

    response = MerchantListResponse(
        data=items,
        total=len(items),
        category=category,
        letter=letter,
    )
    await cache.set(cache_key, response.model_dump(mode="json"), _HUB_TTL_SEC)
    return response


# ---------------------------------------------------------------------------
# GET /v1/merchants/{slug}
# ---------------------------------------------------------------------------


@router.get(
    "/{slug}",
    response_model=MerchantPageData,
    summary="Merchant page data: merchant + ranked cards",
)
async def get_merchant_page(
    slug: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> MerchantPageData:
    """Full page payload. Handles merged slugs by following `merged_into_id`."""
    _require_feature(settings)
    _check_rate(PAGE_LIMITER, _ip_of(request))

    cache = get_cache()
    # TODO: when auth context is plumbed through, include `user:{uid}:cardset_hash`
    # in the cache key and shorten TTL to 60s for authed callers.
    cache_key = f"merchants:page:anon:{slug}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return MerchantPageData.model_validate(cached)

    merchant = (
        await session.execute(
            select(MerchantCanonicalModel).where(
                MerchantCanonicalModel.slug == slug
            )
        )
    ).scalar_one_or_none()
    if merchant is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="merchant_not_found",
            message_en=f"No merchant for slug '{slug}'.",
            message_th="ไม่พบร้านค้าที่ระบุ",
        )

    # Follow merged alias — 301-like behaviour is the frontend's job; here
    # we just serve the canonical target so callers get correct data.
    if merchant.status == "merged" and merchant.merged_into_id is not None:
        target = (
            await session.execute(
                select(MerchantCanonicalModel).where(
                    MerchantCanonicalModel.id == merchant.merged_into_id
                )
            )
        ).scalar_one_or_none()
        if target is not None:
            merchant = target

    ranked = await rank_cards_for_merchant(session, merchant.id)

    payload = MerchantPageData(
        merchant=_to_schema(merchant),
        ranked_cards=ranked,
        generated_at=datetime.now(UTC),
        valuation_snapshot_id=None,
        canonical_url=f"https://loftly.co.th/merchants/{merchant.slug}",
        hreflang_alternates=_build_hreflang(merchant.slug),
    )
    await cache.set(cache_key, payload.model_dump(mode="json"), _PAGE_ANON_TTL_SEC)
    return payload


# ---------------------------------------------------------------------------
# Admin stubs — 501 until §9.1 follow-up PR. Schemas live in
# `schemas/merchants.py` so the admin UI can consume them early.
# ---------------------------------------------------------------------------


@router.post(
    "/admin/{merchant_id}/merge",
    summary="[stub] Merge a source canonical into this one",
)
async def admin_merge(merchant_id: str) -> dict[str, str]:
    """TODO(§9.1): implement admin merge with CDN bust + GSC reindex ping."""
    _ = merchant_id
    raise LoftlyError(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        code="not_implemented",
        message_en="Admin merchant merge is not implemented yet — see POST_V1.md §9.1.",
        message_th="ฟังก์ชันผู้ดูแล (merge) ยังไม่พร้อมใช้งาน",
    )


@router.post(
    "/admin/{merchant_id}/split",
    summary="[stub] Split a canonical into two",
)
async def admin_split(merchant_id: str) -> dict[str, str]:
    """TODO(§9.1): implement admin split with per-promo reassignment."""
    _ = merchant_id
    raise LoftlyError(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        code="not_implemented",
        message_en="Admin merchant split is not implemented yet — see POST_V1.md §9.1.",
        message_th="ฟังก์ชันผู้ดูแล (split) ยังไม่พร้อมใช้งาน",
    )


@router.get(
    "/admin/mapping-queue",
    summary="[stub] Review queue for low-confidence canonicalization",
)
async def admin_mapping_queue() -> dict[str, Any]:
    """TODO(§9.1): expose `confidence < 0.8 OR action='uncertain'` rows."""
    raise LoftlyError(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        code="not_implemented",
        message_en="Mapping review queue is not implemented yet — see POST_V1.md §9.1.",
        message_th="คิวตรวจสอบยังไม่พร้อมใช้งาน",
    )


__all__ = ["router"]
