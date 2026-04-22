"""Idempotent catalog seed — banks, loyalty_currencies, sample cards.

Run after `alembic upgrade head`. Safe to invoke repeatedly; existing rows
(matched on unique columns) are left untouched.

Used by `scripts/seed_catalog.py` and by the `seeded_db` pytest fixture.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.models.author import Author
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card
from loftly.db.models.loyalty_currency import LoyaltyCurrency

# Stable UUID for the default organization byline. Pinned in migration 017
# and re-inserted here so the test harness (which uses `create_all` instead
# of Alembic) has the row available when exercising `/v1/authors/loftly`.
LOFTLY_ORG_AUTHOR_ID = uuid.UUID("10ff1170-0000-4000-8000-000000000001")

log = get_logger(__name__)


@dataclass(frozen=True)
class SeedStats:
    banks_inserted: int
    currencies_inserted: int
    cards_inserted: int
    authors_inserted: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "banks": self.banks_inserted,
            "loyalty_currencies": self.currencies_inserted,
            "cards": self.cards_inserted,
            "authors": self.authors_inserted,
        }


# ---------------------------------------------------------------------------
# Seed data — transcribed from mvp/artifacts/schema.sql seed comments.
# ---------------------------------------------------------------------------

BANKS: list[dict[str, Any]] = [
    {
        "slug": "kbank",
        "display_name_en": "Kasikornbank",
        "display_name_th": "กสิกรไทย",
        "source_key": "kasikorn",
    },
    {
        "slug": "scb",
        "display_name_en": "Siam Commercial Bank",
        "display_name_th": "ไทยพาณิชย์",
        "source_key": "cardx",
    },
    {
        "slug": "ktc",
        "display_name_en": "Krungthai Card",
        "display_name_th": "บัตรกรุงไทย",
        "source_key": "ktc",
    },
    {
        "slug": "krungsri",
        "display_name_en": "Bank of Ayudhya",
        "display_name_th": "กรุงศรีอยุธยา",
        "source_key": None,
    },
    {
        "slug": "uob",
        "display_name_en": "UOB Thailand",
        "display_name_th": "ยูโอบี",
        "source_key": None,
    },
    {
        "slug": "bbl",
        "display_name_en": "Bangkok Bank",
        "display_name_th": "กรุงเทพ",
        "source_key": None,
    },
    {
        "slug": "amex-th",
        "display_name_en": "American Express TH",
        "display_name_th": "อเมริกันเอ็กซ์เพรส",
        "source_key": None,
    },
    {
        "slug": "ttb",
        "display_name_en": "TMBThanachart",
        "display_name_th": "ทีเอ็มบีธนชาต",
        "source_key": None,
    },
]


CURRENCIES: list[dict[str, Any]] = [
    {
        "code": "ROP",
        "display_name_en": "Royal Orchid Plus",
        "display_name_th": "รอยัล ออร์คิด พลัส",
        "currency_type": "airline",
        "issuing_entity": "Thai Airways",
    },
    {
        "code": "KF",
        "display_name_en": "KrisFlyer",
        "display_name_th": "คริสฟลายเออร์",
        "currency_type": "airline",
        "issuing_entity": "Singapore Airlines",
    },
    {
        "code": "AM",
        "display_name_en": "Asia Miles",
        "display_name_th": "เอเชียไมล์ส",
        "currency_type": "airline",
        "issuing_entity": "Cathay Pacific",
    },
    {
        "code": "BONVOY",
        "display_name_en": "Marriott Bonvoy",
        "display_name_th": "มาริออท บอนวอย",
        "currency_type": "hotel",
        "issuing_entity": "Marriott",
    },
    {
        "code": "K_POINT",
        "display_name_en": "K Point",
        "display_name_th": "เค พอยท์",
        "currency_type": "bank_proprietary",
        "issuing_entity": "Kasikornbank",
    },
    {
        "code": "UOB_REWARDS",
        "display_name_en": "UOB Rewards",
        "display_name_th": "ยูโอบี รีวอร์ด",
        "currency_type": "bank_proprietary",
        "issuing_entity": "UOB",
    },
    {
        "code": "KTC_FOREVER",
        "display_name_en": "KTC Forever",
        "display_name_th": "เคทีซี ฟอร์เอเวอร์",
        "currency_type": "bank_proprietary",
        "issuing_entity": "KTC",
    },
    {
        "code": "SCB_REWARDS",
        "display_name_en": "SCB Rewards",
        "display_name_th": "เอสซีบี รีวอร์ด",
        "currency_type": "bank_proprietary",
        "issuing_entity": "SCB",
    },
    {
        "code": "MEMBERSHIP_REWARDS",
        "display_name_en": "Membership Rewards",
        "display_name_th": "เมมเบอร์ชิป รีวอร์ด",
        "currency_type": "bank_proprietary",
        "issuing_entity": "American Express",
    },
]


# Sample cards — placeholders for smoke-testing `/v1/cards`. Realistic issuer/
# network/tier values, but earn rates are illustrative, not authoritative.
SAMPLE_CARDS: list[dict[str, Any]] = [
    {
        "slug": "kbank-wisdom",
        "bank_slug": "kbank",
        "currency_code": "K_POINT",
        "display_name": "KBank WISDOM",
        "tier": "Signature",
        "network": "Visa",
        "annual_fee_thb": 5000.00,
        "annual_fee_waiver": "ฟรีปีแรก",
        "min_income_thb": 80000.00,
        "min_age": 20,
        "earn_rate_local": {"dining": 2.0, "online": 1.5, "default": 1.0},
        "earn_rate_foreign": {"default": 2.5},
        "benefits": {"lounge": {"provider": "LoungeKey", "visits_per_year": 8}},
        "signup_bonus": None,
        "description_th": "บัตรหลักสำหรับสะสม K Point — คุ้มกับการใช้จ่ายต่างประเทศ",
        "description_en": "Primary K Point earner — strong on foreign-currency spend",
    },
    {
        "slug": "uob-prvi-miles",
        "bank_slug": "uob",
        "currency_code": "UOB_REWARDS",
        "display_name": "UOB PRVI Miles",
        "tier": "Signature",
        "network": "Visa",
        "annual_fee_thb": 3210.00,
        "annual_fee_waiver": None,
        "min_income_thb": 70000.00,
        "min_age": 20,
        "earn_rate_local": {"default": 1.4},
        "earn_rate_foreign": {"default": 2.4},
        "benefits": {"lounge": {"provider": "Priority Pass", "visits_per_year": 6}},
        "signup_bonus": {
            "bonus_points": 20000,
            "spend_required": 80000.0,
            "timeframe_days": 60,
        },
        "description_th": "บัตรสะสมไมล์สำหรับนักเดินทาง — เรทต่างประเทศน่าสนใจ",
        "description_en": "Miles-focused card with strong foreign earn rate",
    },
    # Airline-currency card so the miles-goal path has a candidate under the
    # deterministic provider (it filters to currency_type == "airline"). Used
    # by the golden-set eval as well as real miles queries.
    {
        "slug": "scb-thai-airways",
        "bank_slug": "scb",
        "currency_code": "ROP",
        "display_name": "SCB Thai Airways Royal Orchid Plus",
        "tier": "Platinum",
        "network": "Visa",
        "annual_fee_thb": 3000.00,
        "annual_fee_waiver": "ฟรีปีแรก",
        "min_income_thb": 30000.00,
        "min_age": 20,
        "earn_rate_local": {"travel": 1.5, "dining": 1.2, "default": 1.0},
        "earn_rate_foreign": {"default": 1.5},
        "benefits": {"airline_partner": "Thai Airways", "tier_qualifying": True},
        "signup_bonus": {
            "bonus_points": 10000,
            "spend_required": 50000.0,
            "timeframe_days": 90,
        },
        "description_th": "บัตรสะสมไมล์ ROP สายการบินไทย ตรงโปรแกรม",
        "description_en": "Direct ROP earner for Thai Airways loyalists",
    },
]


# Batch-1 enrichment cards — the 3 additional Phase-1 priority cards that are
# NOT already in `SAMPLE_CARDS`. `seed_catalog.py` inserts these on top of the
# base set so the staging catalog has the 5 editorial starter cards from
# `/mvp/CARD_PRIORITY.md §Tier 1` (KBank WISDOM + UOB PRVI Miles are already in
# SAMPLE_CARDS; this block covers KTC Forever + SCB Prime + Amex Gold).
#
# Deliberately kept *out* of `seed_all` / the `seeded_db` test fixture so the
# merchant-ranking golden snapshot and other "== 3 sample cards" assertions
# keep working. Staging / production run `seed_batch1_cards` in addition via
# `scripts/seed_catalog.py`.
BATCH_1_CARDS: list[dict[str, Any]] = [
    {
        "slug": "ktc-forever",
        "bank_slug": "ktc",
        "currency_code": "KTC_FOREVER",
        "display_name": "KTC Forever",
        "tier": "Signature",
        "network": "Visa",
        "annual_fee_thb": 5000.00,
        "annual_fee_waiver": "ฟรีปีแรก",
        "min_income_thb": 50000.00,
        "min_age": 20,
        "earn_rate_local": {
            "dining": 3.0,
            "entertainment": 2.0,
            "online": 1.5,
            "default": 1.0,
        },
        "earn_rate_foreign": {"default": 1.0},
        "benefits": {
            "lounge": {"provider": "Priority Pass", "visits_per_year": 2},
            "insurance": {"travel_coverage_thb": 10_000_000},
            "dining_program": "KTC U SHOP (up to 30% off)",
            "concierge": "24/7 KTC World (Visa Infinite)",
            "no_expiry": True,
        },
        "signup_bonus": None,
        "description_th": "KTC Forever Points ไม่หมดอายุ — เหมาะกับสายสะสมระยะยาว",
        "description_en": "KTC Forever Points never expire — built for long-term accumulators",
    },
    {
        "slug": "scb-prime",
        "bank_slug": "scb",
        "currency_code": "SCB_REWARDS",
        "display_name": "SCB PRIME",
        "tier": "Signature",
        "network": "Visa",
        "annual_fee_thb": 3500.00,
        "annual_fee_waiver": "ฟรีปีแรก",
        "min_income_thb": 50000.00,
        "min_age": 20,
        "earn_rate_local": {
            "dining": 3.0,
            "supermarket": 2.0,
            "fuel": 2.0,
            "online": 1.5,
            "default": 1.0,
        },
        "earn_rate_foreign": {"default": 1.0},
        "benefits": {
            "lounge": {"provider": "Priority Pass", "visits_per_year": 2},
            "insurance": {"travel_coverage_thb": 3_000_000},
            "cashback_model": "direct_statement_credit",
            "integration": "SCB EASY app",
        },
        "signup_bonus": None,
        "description_th": "บัตรเรือธง SCB สำหรับไลฟ์สไตล์ในประเทศ — เครดิตเงินคืนผ่าน SCB EASY",
        "description_en": "SCB's flagship domestic lifestyle card — statement credit via SCB EASY",
    },
    {
        "slug": "amex-gold",
        "bank_slug": "amex-th",
        "currency_code": "MEMBERSHIP_REWARDS",
        "display_name": "Amex Gold",
        "tier": "Gold",
        "network": "Amex",
        "annual_fee_thb": 5250.00,
        "annual_fee_waiver": None,
        "min_income_thb": 50000.00,
        "min_age": 20,
        "earn_rate_local": {
            "dining": 2.0,
            "supermarket": 1.0,
            "amex_selects": 1.5,
            "default": 1.0,
        },
        "earn_rate_foreign": {"default": 1.0},
        "benefits": {
            "lounge": None,
            "insurance": {"travel_coverage_thb": 5_000_000},
            "dining_program": "Amex Selects TH (15-25% off partners)",
            "transfer_partners": ["KrisFlyer", "ROP", "Asia Miles", "Marriott Bonvoy"],
            "no_expiry": True,
        },
        "signup_bonus": None,
        "description_th": "Amex Gold — เครือร้านอาหารและ Membership Rewards โอนสายการบินหลายพันธมิตร",
        "description_en": "Amex Gold — dining-forward with multi-partner Membership Rewards transfers",
    },
]


# ---------------------------------------------------------------------------
# Seed logic — idempotent via unique-column lookup.
# ---------------------------------------------------------------------------


async def seed_all(session: AsyncSession) -> SeedStats:
    """Insert any missing banks/currencies/sample cards/authors. Commit on success."""
    banks_ins = await _seed_banks(session)
    curr_ins = await _seed_currencies(session)
    cards_ins = await _seed_sample_cards(session)
    authors_ins = await _seed_authors(session)

    await session.commit()
    stats = SeedStats(
        banks_inserted=banks_ins,
        currencies_inserted=curr_ins,
        cards_inserted=cards_ins,
        authors_inserted=authors_ins,
    )
    log.info("catalog_seed_complete", **stats.as_dict())
    return stats


async def _seed_authors(session: AsyncSession) -> int:
    """Insert the default Loftly organization author if missing.

    Migration 017 seeds the same row for production; this path covers the
    test harness (which uses `create_all` instead of running migrations) so
    `/v1/authors/loftly` works against the in-memory SQLite DB.
    """
    existing = (
        await session.execute(select(Author).where(Author.id == LOFTLY_ORG_AUTHOR_ID))
    ).scalar_one_or_none()
    if existing is not None:
        return 0
    session.add(
        Author(
            id=LOFTLY_ORG_AUTHOR_ID,
            slug="loftly",
            display_name="Loftly",
            display_name_en="Loftly",
            role="organization",
        )
    )
    return 1


async def _seed_banks(session: AsyncSession) -> int:
    existing = await session.scalars(select(Bank.slug))
    have = set(existing.all())
    inserted = 0
    for row in BANKS:
        if row["slug"] in have:
            continue
        session.add(Bank(**row))
        inserted += 1
    return inserted


async def _seed_currencies(session: AsyncSession) -> int:
    existing = await session.scalars(select(LoyaltyCurrency.code))
    have = set(existing.all())
    inserted = 0
    for row in CURRENCIES:
        if row["code"] in have:
            continue
        session.add(LoyaltyCurrency(**row))
        inserted += 1
    return inserted


async def _seed_cards_from(session: AsyncSession, cards: list[dict[str, Any]]) -> int:
    """Insert any `cards` rows missing from the DB (matched on `slug`).

    Shared helper so both the `seed_all` base path and the Batch-1 enrichment
    path use identical insert semantics.
    """
    # Need banks+currencies committed so we can look them up by slug/code.
    # They're added in the same session; flush to make them visible.
    await session.flush()

    existing_slugs = set(
        (await session.scalars(select(Card.slug))).all(),
    )
    bank_by_slug = {b.slug: b for b in (await session.scalars(select(Bank))).all()}
    cur_by_code = {c.code: c for c in (await session.scalars(select(LoyaltyCurrency))).all()}

    inserted = 0
    for row in cards:
        if row["slug"] in existing_slugs:
            continue
        bank = bank_by_slug.get(row["bank_slug"])
        currency = cur_by_code.get(row["currency_code"])
        if bank is None or currency is None:
            log.warning(
                "sample_card_skip_missing_refs",
                slug=row["slug"],
                bank_slug=row["bank_slug"],
                currency_code=row["currency_code"],
            )
            continue
        session.add(
            Card(
                slug=row["slug"],
                bank_id=bank.id,
                earn_currency_id=currency.id,
                display_name=row["display_name"],
                tier=row["tier"],
                network=row["network"],
                annual_fee_thb=row["annual_fee_thb"],
                annual_fee_waiver=row["annual_fee_waiver"],
                min_income_thb=row["min_income_thb"],
                min_age=row["min_age"],
                earn_rate_local=row["earn_rate_local"],
                earn_rate_foreign=row["earn_rate_foreign"],
                benefits=row["benefits"],
                signup_bonus=row["signup_bonus"],
                description_th=row["description_th"],
                description_en=row["description_en"],
                status="active",
            )
        )
        inserted += 1
    return inserted


async def _seed_sample_cards(session: AsyncSession) -> int:
    return await _seed_cards_from(session, SAMPLE_CARDS)


async def seed_batch1_cards(session: AsyncSession) -> int:
    """Insert the Batch-1 enrichment cards (KTC Forever, SCB Prime, Amex Gold).

    Idempotent: rows with slugs already present are skipped. Commits on
    success so it can be called standalone from `scripts/seed_catalog.py`
    after `seed_all` has returned. Intentionally NOT invoked from `seed_all`
    so the test fixture keeps its 3-sample-card baseline.
    """
    # Make sure banks + currencies needed by the new cards exist. `seed_all`
    # already runs these when called first, but this function can be invoked
    # independently (e.g. against a hand-prepared DB), so re-seed defensively.
    await _seed_banks(session)
    await _seed_currencies(session)

    inserted = await _seed_cards_from(session, BATCH_1_CARDS)
    await session.commit()
    log.info("batch1_cards_seed_complete", cards_inserted=inserted)
    return inserted
