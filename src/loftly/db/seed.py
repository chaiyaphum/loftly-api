"""Idempotent catalog seed — banks, loyalty_currencies, sample cards.

Run after `alembic upgrade head`. Safe to invoke repeatedly; existing rows
(matched on unique columns) are left untouched.

Used by `scripts/seed_catalog.py` and by the `seeded_db` pytest fixture.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.models.author import Author
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.promo import Promo

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
# Merchant logos — slug → public logo URL.
#
# Source: Google's public favicon service (`www.google.com/s2/favicons`) at
# size=128. Unauthenticated, free, and served from a stable Google CDN, which
# is acceptable for a seed image-hint (the frontend falls back to a letter
# monogram when `logo_url` is null or the fetch fails).
#
# Why not Clearbit: Clearbit retired their free logo API in Dec 2024; the
# `logo.clearbit.com` subdomain no longer resolves. Google's favicon service
# has wider coverage of Thai `.co.th` domains and is the lowest-friction
# substitute that doesn't require us to upload binary assets into the repo.
#
# Each entry pins a canonical domain. For Thai brands we prefer the `.co.th`
# site where Google indexes one; otherwise we fall back to the corporate
# parent (e.g. `onesiam.com` for Siam Paragon, `centralretail.com` for the
# Central group). Merchants with no recognizable favicon on any tried domain
# are omitted so `MERCHANT_LOGOS.get(slug)` returns `None` and the frontend
# renders its letter-monogram fallback.
#
# Verification: fetch `https://www.google.com/s2/favicons?sz=128&domain={d}`
# and compare the MD5 to Google's "no favicon" default
# (`b8a0bf372c762e966cc99ede8682bc71`, 726 bytes). Matches = placeholder globe.
MERCHANT_LOGOS: dict[str, str] = {
    "starbucks": "https://www.google.com/s2/favicons?sz=128&domain=starbucks.com",
    "grab-food": "https://www.google.com/s2/favicons?sz=128&domain=grab.com",
    "grab-rides": "https://www.google.com/s2/favicons?sz=128&domain=grab.com",
    "shopee": "https://www.google.com/s2/favicons?sz=128&domain=shopee.co.th",
    "lazada": "https://www.google.com/s2/favicons?sz=128&domain=lazada.com",
    "seven-eleven": "https://www.google.com/s2/favicons?sz=128&domain=cpall.co.th",
    "central-department-store": "https://www.google.com/s2/favicons?sz=128&domain=central.co.th",
    "central-restaurants-group": "https://www.google.com/s2/favicons?sz=128&domain=centralrestaurants.com",
    "siam-paragon": "https://www.google.com/s2/favicons?sz=128&domain=onesiam.com",
    # siam-discovery — parent group (OneSiam) since standalone siamdiscovery.com
    # has no indexed favicon.
    "siam-discovery": "https://www.google.com/s2/favicons?sz=128&domain=onesiam.com",
    "iconsiam": "https://www.google.com/s2/favicons?sz=128&domain=iconsiam.com",
    "foodpanda": "https://www.google.com/s2/favicons?sz=128&domain=foodpanda.com",
    "agoda": "https://www.google.com/s2/favicons?sz=128&domain=agoda.com",
    "booking-com": "https://www.google.com/s2/favicons?sz=128&domain=booking.com",
    "expedia": "https://www.google.com/s2/favicons?sz=128&domain=expedia.com",
    "bts": "https://www.google.com/s2/favicons?sz=128&domain=btsc.co.th",
    "mrt": "https://www.google.com/s2/favicons?sz=128&domain=bemplc.co.th",
    "tops-supermarket": "https://www.google.com/s2/favicons?sz=128&domain=tops.co.th",
    "makro": "https://www.google.com/s2/favicons?sz=128&domain=makro.pro",
    "big-c": "https://www.google.com/s2/favicons?sz=128&domain=bigc.com",
    "lotuss": "https://www.google.com/s2/favicons?sz=128&domain=lotuss.com",
    "cp-fresh-mart": "https://www.google.com/s2/favicons?sz=128&domain=cpfm.co.th",
    "villa-market": "https://www.google.com/s2/favicons?sz=128&domain=villamarket.com",
    "gourmet-market": "https://www.google.com/s2/favicons?sz=128&domain=gourmetthai.com",
    "terminal-21": "https://www.google.com/s2/favicons?sz=128&domain=terminal21.co.th",
    "mbk-center": "https://www.google.com/s2/favicons?sz=128&domain=mbk-center.com",
    "the-mall": "https://www.google.com/s2/favicons?sz=128&domain=themallgroup.com",
    "emporium": "https://www.google.com/s2/favicons?sz=128&domain=emquartier.co.th",
    # robinson — Central Retail parent (standalone robinson.co.th isn't indexed).
    "robinson": "https://www.google.com/s2/favicons?sz=128&domain=centralretail.com",
    "esso": "https://www.google.com/s2/favicons?sz=128&domain=esso.co.th",
    "ptt-station": "https://www.google.com/s2/favicons?sz=128&domain=ptt.com",
    "shell": "https://www.google.com/s2/favicons?sz=128&domain=shell.co.th",
    "bangchak": "https://www.google.com/s2/favicons?sz=128&domain=bangchak.co.th",
    "true-coffee": "https://www.google.com/s2/favicons?sz=128&domain=truecoffee.com",
    # amazon-cafe (PTT's Cafe Amazon) — intentionally unmapped; no tried domain
    # returned a non-default icon, so the monogram fallback applies.
    "au-bon-pain": "https://www.google.com/s2/favicons?sz=128&domain=aubonpain.com",
    "kfc": "https://www.google.com/s2/favicons?sz=128&domain=kfc.com",
    "mcdonalds": "https://www.google.com/s2/favicons?sz=128&domain=mcdonalds.com",
    "pizza-hut": "https://www.google.com/s2/favicons?sz=128&domain=pizzahut.com",
    "the-pizza-company": "https://www.google.com/s2/favicons?sz=128&domain=1112.com",
    "mk-suki": "https://www.google.com/s2/favicons?sz=128&domain=mk-restaurant.com",
    "fuji-restaurant": "https://www.google.com/s2/favicons?sz=128&domain=fuji.co.th",
    "coca-suki": "https://www.google.com/s2/favicons?sz=128&domain=coca.co.th",
    "bar-b-q-plaza": "https://www.google.com/s2/favicons?sz=128&domain=barbqplaza.com",
    "sukishi": "https://www.google.com/s2/favicons?sz=128&domain=sukishi.co.th",
    "oishi": "https://www.google.com/s2/favicons?sz=128&domain=oishi.com",
    "jim-thompson": "https://www.google.com/s2/favicons?sz=128&domain=jimthompson.com",
    "muji-thailand": "https://www.google.com/s2/favicons?sz=128&domain=muji.com",
    "uniqlo-thailand": "https://www.google.com/s2/favicons?sz=128&domain=uniqlo.com",
    "hm-thailand": "https://www.google.com/s2/favicons?sz=128&domain=hm.com",
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


# Batch-1 active promos — 8 SCB + 6 KTC cross-merchant promotions so the
# staging `/promos-today` feed shows as multi-bank rather than KBank-only.
# Date anchors assume catalog is freshly seeded around late April 2026;
# `valid_until` spread across short/medium/long urgencies so the feed has
# a realistic countdown mix. Copy mirrors the tone used on each issuer's
# promotions site — no "demo" markers, no example.com URLs.
#
# Kept outside `seed_all` / `seeded_db` fixture for the same reason as
# `BATCH_1_CARDS`: some test assertions count seeded rows, and the
# merchant-canonicalization pipeline owns any joins to MerchantCanonical.
BATCH_1_PROMOS: list[dict[str, Any]] = [
    # ---- SCB (8) --------------------------------------------------------
    {
        "external_source_id": "scb-starbucks-monday-cashback-2026-05",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/lifestyle/starbucks-monday-cashback.html",
        "promo_type": "cashback",
        "title_th": "รับเงินคืน 10% ที่ Starbucks ทุกวันจันทร์ กับบัตรเครดิต SCB FIRST",
        "title_en": "10% cashback at Starbucks every Monday with SCB FIRST",
        "description_th": "ใช้จ่ายที่ Starbucks ทุกสาขาในวันจันทร์ รับเครดิตเงินคืน 10% สูงสุด 300 บาท/เดือน ผ่านแอป SCB EASY",
        "merchant_name": "Starbucks",
        "category": "dining-cafe",
        "discount_type": "cashback",
        "discount_value": "10%",
        "discount_amount": Decimal("10.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("200"),
        "valid_from": date(2026, 3, 1),
        "valid_until": date(2026, 6, 30),
        "raw_data": {},
        "relevance_tags": ["category:dining", "category:cafe", "bank:scb", "merchant:starbucks"],
    },
    {
        "external_source_id": "scb-agoda-hotel-15pct-2026-summer",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/travel/agoda-summer-getaway.html",
        "promo_type": "discount",
        "title_th": "ส่วนลด 15% จองโรงแรมทั่วโลกที่ Agoda กับบัตรเครดิต SCB",
        "title_en": "15% off worldwide hotels on Agoda with SCB credit cards",
        "description_th": "จองที่พักผ่าน Agoda ด้วยบัตรเครดิต SCB รับส่วนลดสูงสุด 15% เมื่อจองขั้นต่ำ 3,000 บาท เหมาะกับทริปกลางปี",
        "merchant_name": "Agoda",
        "category": "travel",
        "discount_type": "percentage",
        "discount_value": "15%",
        "discount_amount": Decimal("15.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("3000"),
        "valid_from": date(2026, 4, 1),
        "valid_until": date(2026, 6, 15),
        "raw_data": {},
        "relevance_tags": ["category:travel", "bank:scb", "merchant:agoda"],
    },
    {
        "external_source_id": "scb-lazada-payday-500off-2026-04",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/online-shopping/lazada-payday.html",
        "promo_type": "discount",
        "title_th": "ลดทันที ฿500 ที่ Lazada Payday กับบัตรเครดิต SCB",
        "title_en": "฿500 off on Lazada Payday with SCB credit cards",
        "description_th": "กรอกโค้ดพิเศษที่ Lazada ช่วง Payday รับส่วนลด 500 บาท เมื่อช้อปครบ 3,500 บาท เฉพาะบัตรเครดิต SCB",
        "merchant_name": "Lazada",
        "category": "ecommerce",
        "discount_type": "baht_off",
        "discount_value": "฿500",
        "discount_amount": Decimal("500.00"),
        "discount_unit": "thb",
        "minimum_spend": Decimal("3500"),
        "valid_from": date(2026, 4, 20),
        "valid_until": date(2026, 4, 28),
        "raw_data": {},
        "relevance_tags": ["category:ecommerce", "category:online", "bank:scb", "merchant:lazada"],
    },
    {
        "external_source_id": "scb-shopee-3x-points-2026-05",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/online-shopping/shopee-rewards-x3.html",
        "promo_type": "points_bonus",
        "title_th": "รับคะแนน SCB Rewards x3 เมื่อช้อปที่ Shopee",
        "title_en": "Earn 3x SCB Rewards points on Shopee purchases",
        "description_th": "ช้อปที่ Shopee ด้วยบัตรเครดิต SCB รับคะแนนสะสม 3 เท่า สูงสุด 1,500 คะแนน/เดือน ตลอดเดือนพฤษภาคม",
        "merchant_name": "Shopee",
        "category": "ecommerce",
        "discount_type": "points_multiplier",
        "discount_value": "3x points",
        "discount_amount": Decimal("3.00"),
        "discount_unit": "x_multiplier",
        "minimum_spend": Decimal("500"),
        "valid_from": date(2026, 4, 15),
        "valid_until": date(2026, 5, 31),
        "raw_data": {},
        "relevance_tags": ["category:ecommerce", "category:online", "bank:scb", "merchant:shopee"],
    },
    {
        "external_source_id": "scb-themall-weekend-cashback-2026-05",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/shopping/themall-weekend-cashback.html",
        "promo_type": "cashback",
        "title_th": "รับเงินคืน 8% ที่ The Mall ทุกวันเสาร์-อาทิตย์",
        "title_en": "8% cashback at The Mall every weekend",
        "description_th": "ช้อปในศูนย์การค้า The Mall ทุกสาขาวันเสาร์-อาทิตย์ รับเครดิตเงินคืน 8% สูงสุด 800 บาท/เดือน เมื่อใช้จ่ายขั้นต่ำ 2,000 บาทต่อเซลส์สลิป",
        "merchant_name": "The Mall",
        "category": "retail",
        "discount_type": "cashback",
        "discount_value": "8%",
        "discount_amount": Decimal("8.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("2000"),
        "valid_from": date(2026, 4, 1),
        "valid_until": date(2026, 5, 10),
        "raw_data": {},
        "relevance_tags": ["category:retail", "bank:scb", "merchant:themall"],
    },
    {
        "external_source_id": "scb-central-dept-12pct-off-2026-05",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/shopping/central-department-store.html",
        "promo_type": "discount",
        "title_th": "ส่วนลด 12% ที่ห้างเซ็นทรัล เมื่อช้อปขั้นต่ำ 5,000 บาท",
        "title_en": "12% off at Central Department Store (min. ฿5,000)",
        "description_th": "รับส่วนลดทันที 12% ที่ Central Department Store ทุกสาขา ใช้ได้กับบัตรเครดิต SCB PRIME และ SCB FIRST",
        "merchant_name": "Central",
        "category": "retail",
        "discount_type": "percentage",
        "discount_value": "12%",
        "discount_amount": Decimal("12.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("5000"),
        "valid_from": date(2026, 4, 5),
        "valid_until": date(2026, 5, 18),
        "raw_data": {},
        "relevance_tags": ["category:retail", "bank:scb", "merchant:central"],
    },
    {
        "external_source_id": "scb-siam-paragon-dining-2026-04",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/dining/siam-paragon-restaurants.html",
        "promo_type": "cashback",
        "title_th": "รับเงินคืน 15% ร้านอาหารในสยามพารากอน",
        "title_en": "15% cashback at Siam Paragon restaurants",
        "description_th": "ทานอาหารร้านพันธมิตรในสยามพารากอน 40+ ร้าน รับเครดิตเงินคืน 15% สูงสุด 500 บาทต่อใบเสร็จ เฉพาะวันศุกร์-อาทิตย์",
        "merchant_name": "Siam Paragon",
        "category": "dining-restaurants",
        "discount_type": "cashback",
        "discount_value": "15%",
        "discount_amount": Decimal("15.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("1500"),
        "valid_from": date(2026, 4, 1),
        "valid_until": date(2026, 4, 26),
        "raw_data": {},
        "relevance_tags": ["category:dining", "bank:scb", "merchant:siam-paragon"],
    },
    {
        "external_source_id": "scb-emporium-luxury-2x-points-2026-06",
        "external_bank_key": "scb",
        "bank_slug": "scb",
        "source_url": "https://www.scb.co.th/th/personal-banking/promotions/lifestyle/emporium-luxury-rewards.html",
        "promo_type": "points_bonus",
        "title_th": "รับคะแนน SCB Rewards x2 ที่ Emporium และ EmQuartier",
        "title_en": "2x SCB Rewards points at Emporium & EmQuartier",
        "description_th": "ใช้จ่ายที่ร้านแบรนด์เนมในเอ็มโพเรียมและเอ็มควอเทียร์ รับคะแนนสะสมเพิ่ม 2 เท่า เมื่อใช้จ่ายตั้งแต่ 3,000 บาทขึ้นไป",
        "merchant_name": "Emporium",
        "category": "retail",
        "discount_type": "points_multiplier",
        "discount_value": "2x points",
        "discount_amount": Decimal("2.00"),
        "discount_unit": "x_multiplier",
        "minimum_spend": Decimal("3000"),
        "valid_from": date(2026, 4, 10),
        "valid_until": date(2026, 6, 20),
        "raw_data": {},
        "relevance_tags": ["category:retail", "bank:scb", "merchant:emporium"],
    },
    # ---- KTC (6) --------------------------------------------------------
    {
        "external_source_id": "ktc-7eleven-everyday-5pct-2026-q2",
        "external_bank_key": "ktc",
        "bank_slug": "ktc",
        "source_url": "https://www.ktc.co.th/promotion/shopping/7-eleven-everyday-cashback",
        "promo_type": "cashback",
        "title_th": "รับเงินคืน 5% ที่ 7-Eleven ทุกวัน สูงสุด 200 บาท/เดือน",
        "title_en": "5% daily cashback at 7-Eleven (up to ฿200/month)",
        "description_th": "ใช้จ่ายที่ 7-Eleven ทุกสาขาทั่วประเทศ รับเครดิตเงินคืน 5% ตลอดไตรมาส 2 เฉพาะบัตรเครดิต KTC",
        "merchant_name": "7-Eleven",
        "category": "grocery",
        "discount_type": "cashback",
        "discount_value": "5%",
        "discount_amount": Decimal("5.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("100"),
        "valid_from": date(2026, 4, 1),
        "valid_until": date(2026, 6, 30),
        "raw_data": {},
        "relevance_tags": ["category:grocery", "bank:ktc", "merchant:7-eleven"],
    },
    {
        "external_source_id": "ktc-bigc-weekend-300off-2026-05",
        "external_bank_key": "ktc",
        "bank_slug": "ktc",
        "source_url": "https://www.ktc.co.th/promotion/supermarket/bigc-weekend-300off",
        "promo_type": "discount",
        "title_th": "ลด ฿300 ที่ Big C ทุกสุดสัปดาห์ เมื่อช้อปครบ 3,000 บาท",
        "title_en": "฿300 off at Big C weekends (min. ฿3,000)",
        "description_th": "ช้อปที่ Big C Supercenter และ Big C Extra ทุกวันเสาร์-อาทิตย์ รับส่วนลดทันที 300 บาท เมื่อชำระด้วยบัตรเครดิต KTC",
        "merchant_name": "Big C",
        "category": "grocery",
        "discount_type": "baht_off",
        "discount_value": "฿300",
        "discount_amount": Decimal("300.00"),
        "discount_unit": "thb",
        "minimum_spend": Decimal("3000"),
        "valid_from": date(2026, 4, 1),
        "valid_until": date(2026, 5, 10),
        "raw_data": {},
        "relevance_tags": ["category:grocery", "bank:ktc", "merchant:bigc"],
    },
    {
        "external_source_id": "ktc-makro-bulk-2pct-2026-06",
        "external_bank_key": "ktc",
        "bank_slug": "ktc",
        "source_url": "https://www.ktc.co.th/promotion/wholesale/makro-cashback",
        "promo_type": "cashback",
        "title_th": "รับเงินคืน 2% ที่ Makro ทุกสาขา",
        "title_en": "2% cashback at Makro stores",
        "description_th": "ใช้บัตรเครดิต KTC ที่ Makro ทุกสาขา รับเครดิตเงินคืน 2% สำหรับการช้อปตั้งแต่ 5,000 บาทต่อใบเสร็จ สูงสุด 500 บาท/เดือน",
        "merchant_name": "Makro",
        "category": "grocery",
        "discount_type": "cashback",
        "discount_value": "2%",
        "discount_amount": Decimal("2.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("5000"),
        "valid_from": date(2026, 3, 15),
        "valid_until": date(2026, 6, 14),
        "raw_data": {},
        "relevance_tags": ["category:grocery", "bank:ktc", "merchant:makro"],
    },
    {
        "external_source_id": "ktc-foodpanda-delivery-100off-2026-04",
        "external_bank_key": "ktc",
        "bank_slug": "ktc",
        "source_url": "https://www.ktc.co.th/promotion/food-delivery/foodpanda-code-ktc100",
        "promo_type": "discount",
        "title_th": "ลด ฿100 ค่าอาหารที่ foodpanda โค้ด KTC100",
        "title_en": "฿100 off foodpanda orders with code KTC100",
        "description_th": "กรอกโค้ด KTC100 บนแอป foodpanda รับส่วนลดทันที 100 บาท เมื่อสั่งอาหารขั้นต่ำ 400 บาท ชำระด้วยบัตรเครดิต KTC",
        "merchant_name": "foodpanda",
        "category": "dining",
        "discount_type": "baht_off",
        "discount_value": "฿100",
        "discount_amount": Decimal("100.00"),
        "discount_unit": "thb",
        "minimum_spend": Decimal("400"),
        "valid_from": date(2026, 4, 18),
        "valid_until": date(2026, 4, 29),
        "raw_data": {},
        "relevance_tags": ["category:dining", "bank:ktc", "merchant:foodpanda"],
    },
    {
        "external_source_id": "ktc-grab-ride-15pct-2026-05",
        "external_bank_key": "ktc",
        "bank_slug": "ktc",
        "source_url": "https://www.ktc.co.th/promotion/transport/grab-ride-discount",
        "promo_type": "discount",
        "title_th": "ส่วนลด 15% ค่าบริการ Grab สูงสุด 60 บาทต่อครั้ง",
        "title_en": "15% off Grab rides (up to ฿60 per trip)",
        "description_th": "เรียก GrabCar หรือ GrabBike ชำระผ่านบัตรเครดิต KTC รับส่วนลด 15% ใช้ได้ 4 ครั้งต่อเดือน ตลอดเดือนพฤษภาคม 2026",
        "merchant_name": "Grab",
        "category": "online",
        "discount_type": "percentage",
        "discount_value": "15%",
        "discount_amount": Decimal("15.00"),
        "discount_unit": "percent",
        "minimum_spend": Decimal("150"),
        "valid_from": date(2026, 4, 25),
        "valid_until": date(2026, 5, 31),
        "raw_data": {},
        "relevance_tags": ["category:online", "bank:ktc", "merchant:grab"],
    },
    {
        "external_source_id": "ktc-mcdonalds-combo-2x-points-2026-05",
        "external_bank_key": "ktc",
        "bank_slug": "ktc",
        "source_url": "https://www.ktc.co.th/promotion/dining/mcdonalds-forever-points",
        "promo_type": "points_bonus",
        "title_th": "รับคะแนน KTC Forever Points x2 ที่ McDonald's",
        "title_en": "2x KTC Forever Points at McDonald's",
        "description_th": "ใช้จ่ายที่ McDonald's ทุกสาขารวมถึงแอป McDelivery รับคะแนนสะสม KTC Forever Points x2 ไม่จำกัดยอดสูงสุด",
        "merchant_name": "McDonald's",
        "category": "dining-restaurants",
        "discount_type": "points_multiplier",
        "discount_value": "2x points",
        "discount_amount": Decimal("2.00"),
        "discount_unit": "x_multiplier",
        "minimum_spend": Decimal("150"),
        "valid_from": date(2026, 4, 15),
        "valid_until": date(2026, 5, 14),
        "raw_data": {},
        "relevance_tags": ["category:dining", "bank:ktc", "merchant:mcdonalds"],
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


async def _seed_promos_from(session: AsyncSession, promos: list[dict[str, Any]]) -> int:
    """Insert any `promos` rows missing from the DB (matched on external_source_id).

    Mirrors `_seed_cards_from` semantics — looks up bank_id from bank_slug, skips
    rows whose external_source_id is already present, and fills every Promo column
    the dict supplies. `last_synced_at` defaults to "now" when not provided so
    the row looks fresh to the freshness-header logic.
    """
    # Banks may have been added in the same session; flush so lookups see them.
    await session.flush()

    existing_ids = set(
        (
            await session.scalars(
                select(Promo.external_source_id).where(Promo.external_source_id.is_not(None))
            )
        ).all(),
    )
    bank_by_slug = {b.slug: b for b in (await session.scalars(select(Bank))).all()}

    inserted = 0
    now = datetime.now(UTC)
    for row in promos:
        if row["external_source_id"] in existing_ids:
            continue
        bank = bank_by_slug.get(row["bank_slug"])
        if bank is None:
            log.warning(
                "batch1_promo_skip_missing_bank",
                external_source_id=row["external_source_id"],
                bank_slug=row["bank_slug"],
            )
            continue
        session.add(
            Promo(
                bank_id=bank.id,
                external_source_id=row["external_source_id"],
                external_bank_key=row.get("external_bank_key"),
                source_url=row["source_url"],
                promo_type=row.get("promo_type"),
                title_th=row["title_th"],
                title_en=row.get("title_en"),
                description_th=row.get("description_th"),
                description_en=row.get("description_en"),
                merchant_name=row.get("merchant_name"),
                category=row.get("category"),
                discount_type=row.get("discount_type"),
                discount_value=row.get("discount_value"),
                discount_amount=row.get("discount_amount"),
                discount_unit=row.get("discount_unit"),
                minimum_spend=row.get("minimum_spend"),
                valid_from=row.get("valid_from"),
                valid_until=row.get("valid_until"),
                raw_data=row.get("raw_data", {}),
                relevance_tags=row.get("relevance_tags", []),
                active=row.get("active", True),
                last_synced_at=row.get("last_synced_at", now),
            )
        )
        inserted += 1
    return inserted


async def seed_batch1_promos(session: AsyncSession) -> int:
    """Insert the Batch-1 active promos (8 SCB + 6 KTC) for staging coverage.

    Idempotent: rows with matching external_source_id are skipped. Commits on
    success so it can be called standalone from `scripts/seed_catalog.py`.
    Kept out of `seed_all` / the `seeded_db` test fixture for the same reason
    as `seed_batch1_cards` — promo-count assertions in the existing suite rely
    on the fixture keeping a clean (empty) promos table.
    """
    # Defensive: promos FK on banks, so re-seed banks when invoked standalone.
    await _seed_banks(session)

    inserted = await _seed_promos_from(session, BATCH_1_PROMOS)
    await session.commit()
    log.info("batch1_promos_seed_complete", promos_inserted=inserted)
    return inserted
