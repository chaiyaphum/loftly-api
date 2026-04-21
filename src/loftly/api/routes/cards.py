"""Cards catalog — `GET /v1/cards`, `GET /v1/cards/{slug}`.

Phase 1 scaffold returns a 2-item baked-in fixture so tests pass without DB
seeding. Real DB-backed listing arrives in Week 3 once migrations 002-003 run
and card CMS seeding is live (SCHEMA.md §Seed data requirements).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from loftly.api.errors import LoftlyError
from loftly.schemas.cards import BankMini, Card, CardList, Currency
from loftly.schemas.common import Pagination

router = APIRouter(prefix="/v1/cards", tags=["cards"])


# --- Fixture ---------------------------------------------------------------

_K_POINT = Currency(
    code="K_POINT",
    display_name_en="K Point",
    display_name_th="เค พอยท์",
    currency_type="bank_proprietary",
    issuing_entity="Kasikornbank",
)

_KTC_FOREVER = Currency(
    code="KTC_FOREVER",
    display_name_en="KTC Forever",
    display_name_th="KTC Forever",
    currency_type="bank_proprietary",
    issuing_entity="KTC",
)

_FIXTURE: list[Card] = [
    Card(
        id="11111111-1111-4111-8111-111111111111",
        slug="kbank-wisdom",
        display_name="KBank WISDOM",
        bank=BankMini(
            slug="kbank",
            display_name_en="Kasikornbank",
            display_name_th="กสิกรไทย",
        ),
        tier="Signature",
        network="Visa",
        annual_fee_thb=5000.00,
        annual_fee_waiver="ฟรีปีแรก",
        min_income_thb=80000.00,
        min_age=20,
        earn_currency=_K_POINT,
        earn_rate_local={"dining": 2.0, "online": 1.5, "default": 1.0},
        earn_rate_foreign={"default": 2.5},
        benefits={"lounge": {"provider": "LoungeKey", "visits_per_year": 8}},
        signup_bonus=None,
        description_th="บัตรหลักสำหรับสะสม K Point — คุ้มกับการใช้จ่ายต่างประเทศ",
        description_en="Primary K Point earner — strong on foreign-currency spend",
        status="active",
    ),
    Card(
        id="22222222-2222-4222-8222-222222222222",
        slug="ktc-x-infinite",
        display_name="KTC X Infinite",
        bank=BankMini(
            slug="ktc",
            display_name_en="KTC",
            display_name_th="เคทีซี",
        ),
        tier="Infinite",
        network="Visa",
        annual_fee_thb=5350.00,
        annual_fee_waiver=None,
        min_income_thb=100000.00,
        min_age=20,
        earn_currency=_KTC_FOREVER,
        earn_rate_local={"travel": 3.0, "dining": 2.0, "default": 1.0},
        earn_rate_foreign={"default": 2.0},
        benefits={"lounge": {"provider": "Priority Pass", "visits_per_year": 12}},
        signup_bonus={
            "bonus_points": 15000,
            "spend_required": 100000.0,
            "timeframe_days": 60,
        },
        description_th="บัตรท่องเที่ยวและโรงแรม — คะแนนแลกไมล์ได้หลายสายการบิน",
        description_en="Travel-focused with flexible KTC Forever transfers",
        status="active",
    ),
]

_BY_SLUG = {c.slug: c for c in _FIXTURE}


# --- Routes ----------------------------------------------------------------


@router.get(
    "",
    response_model=CardList,
    summary="List cards (public catalog)",
)
async def list_cards(
    issuer: str | None = Query(default=None, description="Bank slug filter"),
    network: str | None = Query(default=None),
    tier: str | None = Query(default=None),
    max_annual_fee: float | None = Query(default=None, ge=0),
    earn_currency: str | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
) -> CardList:
    """Phase 1: returns the in-memory fixture. Filters are honored when present."""
    items = list(_FIXTURE)
    if issuer:
        items = [c for c in items if c.bank.slug == issuer]
    if network:
        items = [c for c in items if c.network == network]
    if tier:
        items = [c for c in items if c.tier == tier]
    if max_annual_fee is not None:
        items = [c for c in items if (c.annual_fee_thb or 0) <= max_annual_fee]
    if earn_currency:
        items = [c for c in items if c.earn_currency.code == earn_currency]

    items = items[:limit]
    return CardList(
        data=items,
        pagination=Pagination(
            cursor_next=None,
            has_more=False,
            total_estimate=len(items),
        ),
    )


@router.get(
    "/{slug}",
    response_model=Card,
    summary="Card detail by slug",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
    },
)
async def get_card(slug: str) -> Card:
    try:
        return _BY_SLUG[slug]
    except KeyError as exc:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="card_not_found",
            message_en=f"No card with slug {slug!r}.",
            message_th=f"ไม่พบบัตรรหัส {slug}",
            details={"slug": slug},
        ) from exc


# Admin / back-office pieces live in admin.py. This router intentionally exposes
# only the public read endpoints.
__all__ = ["router"]


def _not_implemented() -> HTTPException:
    return HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
