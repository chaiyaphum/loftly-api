"""Idempotent seeder for Batch-1 starter cards + sample point valuations.

Reads the 5 Batch-1 card-review markdown files from
`../loftly/content/card_reviews/` (the loftly docs repo checked out as a sibling
of this API repo) and upserts:

- Missing `banks` rows (matched on `slug`)
- Missing `loyalty_currencies` rows (matched on `code`) — covers currencies not
  already populated by `loftly.db.seed.CURRENCIES`, notably `MEMBERSHIP_REWARDS`
  for the Amex Gold card
- One `cards` row per file (matched on `slug`)
- One `articles` row per file (matched on `slug`; `state='published'`,
  `published_at=NOW()`, `policy_version='2026-04-01'`)
- One placeholder `affiliate_links` row per card (matched on
  `(card_id, partner_id='moneyguru')`)
- Three `point_valuations` fixture rows (ROP / K_POINT / UOB_REWARDS) so the
  `/v1/valuations` surface has data to render; matched on `currency_id` + a
  stable `methodology` so re-runs don't duplicate

The script is read-through idempotent: re-running against a populated DB is a
no-op (zero inserts, zero updates) except for refreshing `updated_at` on
existing articles — which we explicitly avoid.

## Usage

Local / staging Postgres:

```sh
uv run python scripts/seed_starter_cards.py --dry-run
uv run python scripts/seed_starter_cards.py --execute --db-url "$DATABASE_URL"
```

## One-off operational run against staging DO Postgres

After this PR is merged, the founder runs the seeder ONCE against staging:

```sh
# 1. Whitelist your IP on the managed cluster (DO firewall is closed by default)
doctl databases firewalls append <cluster_id> --rule ip_addr:$MY_IP4

# 2. Execute (STAGING_URL pulled from 1Password / DO connection string)
uv run python scripts/seed_starter_cards.py \
    --db-url "$STAGING_URL" \
    --execute

# 3. Close the firewall hole again
doctl databases firewalls remove <cluster_id> --uuid <rule-uuid>
```

Note: `LOFTLY_DR_ENCRYPTION_KEY` is unrelated and not required for this
script — the only env var consulted is the one passed via `--db-url`
(or `DATABASE_URL` for local dev).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loftly.db.models.affiliate import AffiliateLink
from loftly.db.models.article import Article
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation

# Stable actor for article authorship — matches migration 012's system user.
SYSTEM_USER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")

# Default source directory (can be overridden with --content-dir).
DEFAULT_CONTENT_DIR = (
    Path(__file__).resolve().parent.parent.parent / "loftly" / "content" / "card_reviews"
)

# Files to seed — Batch-1 starter set (matches /mvp/CARD_PRIORITY.md).
# The other drafts in the content directory (kbank-the-wisdom, kbank-precious-plus,
# kbank-jcb-platinum, kbank-wisdom's earlier draft) are uncommitted / superseded
# and intentionally skipped.
BATCH_1_FILES: tuple[str, ...] = (
    "kbank-wisdom.md",
    "uob-prvi-miles.md",
    "ktc-forever.md",
    "scb-prime.md",
    "amex-gold.md",
)

# Issuer string (as written in frontmatter) → canonical bank slug.
ISSUER_TO_BANK_SLUG: dict[str, str] = {
    "KBank": "kbank",
    "UOB TH": "uob",
    "KTC": "ktc",
    "SCB": "scb",
    "American Express (Thailand)": "amex-th",
}

# Loyalty currencies Batch-1 needs. Re-declared in full (not just the extras)
# so the script is safe to run standalone against a fresh DB that hasn't had
# `seed_catalog.py` applied yet. The first 4 entries mirror the defaults in
# `loftly.db.seed.CURRENCIES`; `MEMBERSHIP_REWARDS` is specific to Amex Gold.
REQUIRED_CURRENCIES: list[dict[str, Any]] = [
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
        "code": "ROP",
        "display_name_en": "Royal Orchid Plus",
        "display_name_th": "รอยัล ออร์คิด พลัส",
        "currency_type": "airline",
        "issuing_entity": "Thai Airways",
    },
    {
        "code": "MEMBERSHIP_REWARDS",
        "display_name_en": "Membership Rewards",
        "display_name_th": "เมมเบอร์ชิป รีวอร์ด",
        "currency_type": "bank_proprietary",
        "issuing_entity": "American Express",
    },
]

# Banks that may not yet exist in the DB (the default `loftly.db.seed.BANKS`
# covers all 5 issuers already, but we re-declare what we rely on here so this
# script is self-contained if run before `seed_catalog.py`).
REQUIRED_BANKS: list[dict[str, Any]] = [
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
        "slug": "uob",
        "display_name_en": "UOB Thailand",
        "display_name_th": "ยูโอบี",
        "source_key": None,
    },
    {
        "slug": "amex-th",
        "display_name_en": "American Express TH",
        "display_name_th": "อเมริกันเอ็กซ์เพรส",
        "source_key": None,
    },
]

# Sample point valuations. Values transcribed from /mvp/VALUATION_METHOD.md
# §Known values + article body copy. Matched on `(currency_code,
# methodology)` during upsert so reruns are idempotent.
SAMPLE_VALUATIONS: list[dict[str, Any]] = [
    {
        "currency_code": "ROP",
        "thb_per_point": Decimal("1.5200"),
        "methodology": "p80_award_chart_vs_cash",
        "percentile": 80,
        "sample_size": 27,
        "confidence": Decimal("0.70"),
        "top_redemption_example": "TG Business class BKK–NRT (1.52 THB / mile)",
    },
    {
        "currency_code": "K_POINT",
        "thb_per_point": Decimal("0.3800"),
        "methodology": "p80_transfer_to_best_airline",
        "percentile": 80,
        "sample_size": 18,
        "confidence": Decimal("0.60"),
        "top_redemption_example": "Transfer 4:1 → ROP → TG Business BKK–NRT",
    },
    {
        "currency_code": "UOB_REWARDS",
        "thb_per_point": Decimal("0.4400"),
        "methodology": "p80_transfer_to_best_airline",
        "percentile": 80,
        "sample_size": 8,
        "confidence": Decimal("0.60"),
        "top_redemption_example": "Transfer 3.2:1 → KrisFlyer → SQ Business BKK–SIN",
    },
]


# ---------------------------------------------------------------------------
# Frontmatter parser — deliberately tiny; no PyYAML dependency on the seeder.
# The 5 Batch-1 files all use flat key: value frontmatter with one list field
# (`best_for_tags`). If the schema grows (nested maps, multi-line strings),
# swap this for PyYAML — but keep this module ~self-contained for now.
# ---------------------------------------------------------------------------


_FRONTMATTER_RE = re.compile(
    r"\A(?:<!--.*?-->\s*)?---\s*\n(?P<fm>.*?)\n---\s*\n(?P<body>.*)\Z",
    re.DOTALL,
)


@dataclass(frozen=True)
class ParsedReview:
    path: Path
    frontmatter: dict[str, Any]
    body: str

    @property
    def card_slug(self) -> str:
        return str(self.frontmatter["card_slug"])

    @property
    def display_name(self) -> str:
        return str(self.frontmatter["display_name"])

    @property
    def issuer(self) -> str:
        return str(self.frontmatter["issuer"])

    @property
    def earn_currency(self) -> str:
        return str(self.frontmatter["earn_currency"])


def _parse_scalar(raw: str) -> Any:
    """Parse a simple YAML-ish scalar: string / int / null / quoted / list."""
    s = raw.strip()
    if s == "" or s.lower() in {"null", "none", "~"}:
        return None
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner:
            return []
        return [item.strip().strip("\"'") for item in inner.split(",") if item.strip()]
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    # Numeric?
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return s


def parse_review(path: Path) -> ParsedReview:
    text = path.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(text)
    if match is None:
        raise ValueError(f"no frontmatter block found in {path}")
    fm_block = match.group("fm")
    body = match.group("body").strip()

    frontmatter: dict[str, Any] = {}
    for line in fm_block.splitlines():
        line = line.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        # Only split on the FIRST colon to preserve Thai-language values with
        # internal colons.
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        frontmatter[key.strip()] = _parse_scalar(value)

    return ParsedReview(path=path, frontmatter=frontmatter, body=body)


# ---------------------------------------------------------------------------
# Summary extraction — first-paragraph heuristic. Keeps to the 120–180 char
# summary_th hard limit in /mvp/UI_CONTENT.md §CMS field spec.
# ---------------------------------------------------------------------------


def extract_summary_th(body: str, *, max_chars: int = 180) -> str:
    """Pull the first non-heading paragraph from the hero section."""
    in_hero = False
    paragraph_lines: list[str] = []
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("## 1. Hero") or stripped.startswith("# "):
            in_hero = stripped.startswith("## 1. Hero") or in_hero
            continue
        if in_hero:
            if stripped.startswith("## "):
                break
            if stripped and not stripped.startswith("<!--"):
                paragraph_lines.append(stripped)
            elif paragraph_lines:
                break
    summary = " ".join(paragraph_lines).strip()
    if len(summary) > max_chars:
        summary = summary[: max_chars - 1].rstrip() + "…"
    if not summary:
        # Fallback: first non-empty body line not a heading.
        for line in body.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith(("#", "<!--", "---")):
                summary = stripped[:max_chars]
                break
    return summary


# ---------------------------------------------------------------------------
# Per-card derived values that the frontmatter doesn't capture (network, earn
# rate JSONB, placeholder affiliate URL).
# ---------------------------------------------------------------------------


# Derived from card-review body tables (§4 Earn rate). Rounded monthly-cap-free
# rates in K_POINT / MR / UOB-Rewards equivalents — the values the UI renders.
CARD_ENRICHMENT: dict[str, dict[str, Any]] = {
    "kbank-wisdom": {
        "network": "Visa",
        "earn_rate_local": {"dining": 2.0, "online": 1.5, "grocery": 1.0, "default": 1.0},
        "earn_rate_foreign": {"default": 1.0},
        "benefits": {
            "lounge": {"provider": "Priority Pass", "unlimited_for_primary": True},
            "insurance": {"travel_coverage_thb": 30_000_000},
            "dining_program": "The Wisdom Dining (10-25% off partners)",
            "concierge": "24/7 The Wisdom desk",
        },
    },
    "uob-prvi-miles": {
        "network": "Visa",
        "earn_rate_local": {"default": 1.0},
        "earn_rate_foreign": {"default": 4.0, "online_travel": 4.0},
        "benefits": {
            "lounge": {"provider": "Priority Pass", "visits_per_year": 2},
            "insurance": {"travel_coverage_thb": 8_000_000},
            "direct_transfer_partners": ["KrisFlyer", "Asia Miles", "ROP"],
        },
    },
    "ktc-forever": {
        "network": "Visa",
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
    },
    "scb-prime": {
        "network": "Visa",
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
    },
    "amex-gold": {
        "network": "Amex",
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
    },
}


# ---------------------------------------------------------------------------
# Upsert logic
# ---------------------------------------------------------------------------


@dataclass
class SeedResult:
    banks_inserted: int = 0
    currencies_inserted: int = 0
    cards_inserted: int = 0
    articles_inserted: int = 0
    affiliate_links_inserted: int = 0
    valuations_inserted: int = 0

    def summary(self) -> str:
        return (
            f"banks={self.banks_inserted} "
            f"currencies={self.currencies_inserted} "
            f"cards={self.cards_inserted} "
            f"articles={self.articles_inserted} "
            f"affiliate_links={self.affiliate_links_inserted} "
            f"point_valuations={self.valuations_inserted}"
        )


async def _ensure_banks(session: AsyncSession) -> int:
    existing = set((await session.scalars(select(Bank.slug))).all())
    inserted = 0
    for row in REQUIRED_BANKS:
        if row["slug"] in existing:
            continue
        session.add(Bank(**row))
        inserted += 1
    return inserted


async def _ensure_currencies(session: AsyncSession) -> int:
    existing = set((await session.scalars(select(LoyaltyCurrency.code))).all())
    inserted = 0
    for row in REQUIRED_CURRENCIES:
        if row["code"] in existing:
            continue
        session.add(LoyaltyCurrency(**row))
        inserted += 1
    return inserted


async def _upsert_card(
    session: AsyncSession,
    review: ParsedReview,
    *,
    bank_by_slug: dict[str, Bank],
    currency_by_code: dict[str, LoyaltyCurrency],
) -> tuple[Card, bool]:
    """Return (card_row, was_inserted)."""
    existing = (
        await session.scalars(select(Card).where(Card.slug == review.card_slug))
    ).one_or_none()
    if existing is not None:
        return existing, False

    bank_slug = ISSUER_TO_BANK_SLUG[review.issuer]
    bank = bank_by_slug[bank_slug]
    currency = currency_by_code[review.earn_currency]
    enrichment = CARD_ENRICHMENT[review.card_slug]

    fm = review.frontmatter
    card = Card(
        slug=review.card_slug,
        bank_id=bank.id,
        earn_currency_id=currency.id,
        display_name=review.display_name,
        tier=fm.get("tier"),
        network=enrichment["network"],
        annual_fee_thb=(
            Decimal(str(fm["annual_fee_thb"])) if fm.get("annual_fee_thb") is not None else None
        ),
        annual_fee_waiver=fm.get("annual_fee_waiver"),
        min_income_thb=(
            Decimal(str(fm["min_income_thb"])) if fm.get("min_income_thb") is not None else None
        ),
        # `min_age` isn't present in the frontmatter — defaults to NULL, which
        # the cards table permits. See report §defaults.
        min_age=None,
        earn_rate_local=enrichment["earn_rate_local"],
        earn_rate_foreign=enrichment["earn_rate_foreign"],
        benefits=enrichment["benefits"],
        signup_bonus=None,
        description_th=extract_summary_th(review.body),
        description_en=None,
        status="active",
    )
    session.add(card)
    await session.flush()  # populate card.id for downstream FKs
    return card, True


async def _upsert_article(
    session: AsyncSession,
    review: ParsedReview,
    *,
    card: Card,
) -> bool:
    """Return True if a new article row was inserted."""
    existing = (
        await session.scalars(select(Article).where(Article.slug == review.card_slug))
    ).one_or_none()
    if existing is not None:
        return False

    from datetime import UTC, datetime

    title_match = re.search(r"^#\s+(.+)$", review.body, re.MULTILINE)
    title_th = title_match.group(1).strip() if title_match else review.display_name

    article = Article(
        slug=review.card_slug,
        card_id=card.id,
        article_type=str(review.frontmatter.get("article_type", "card_review")),
        title_th=title_th,
        title_en=None,
        summary_th=extract_summary_th(review.body),
        summary_en=None,
        body_th=review.body,
        body_en=None,
        best_for_tags=list(review.frontmatter.get("best_for_tags") or []),
        state="published",
        author_id=SYSTEM_USER_ID,
        policy_version=str(review.frontmatter.get("policy_version", "2026-04-01")),
        published_at=datetime.now(UTC),
        seo_meta={},
    )
    session.add(article)
    return True


async def _upsert_affiliate_link(session: AsyncSession, card: Card) -> bool:
    existing = (
        await session.scalars(
            select(AffiliateLink).where(
                AffiliateLink.card_id == card.id,
                AffiliateLink.partner_id == "moneyguru",
            )
        )
    ).one_or_none()
    if existing is not None:
        return False
    session.add(
        AffiliateLink(
            card_id=card.id,
            partner_id="moneyguru",
            # Placeholder. Real URL lands when the MoneyGuru contract signs;
            # see /mvp/PARTNERSHIP_OUTREACH.md.
            url_template=f"https://moneyguru.co.th/apply/{card.slug}?sub_id={{click_id}}",
            campaign_id=None,
            commission_model="cpa_approved",
            commission_amount_thb=None,
            active=True,
        )
    )
    return True


async def _upsert_sample_valuations(
    session: AsyncSession,
    *,
    currency_by_code: dict[str, LoyaltyCurrency],
) -> int:
    inserted = 0
    for row in SAMPLE_VALUATIONS:
        currency = currency_by_code.get(row["currency_code"])
        if currency is None:
            continue
        existing = (
            await session.scalars(
                select(PointValuation).where(
                    PointValuation.currency_id == currency.id,
                    PointValuation.methodology == row["methodology"],
                )
            )
        ).first()
        if existing is not None:
            continue
        session.add(
            PointValuation(
                currency_id=currency.id,
                thb_per_point=row["thb_per_point"],
                methodology=row["methodology"],
                percentile=row["percentile"],
                sample_size=row["sample_size"],
                confidence=row["confidence"],
                top_redemption_example=row["top_redemption_example"],
            )
        )
        inserted += 1
    return inserted


async def seed_starter_cards(
    session: AsyncSession,
    *,
    content_dir: Path,
    files: tuple[str, ...] = BATCH_1_FILES,
) -> SeedResult:
    result = SeedResult()

    # Parse all reviews up-front so we fail fast on missing files.
    reviews = [parse_review(content_dir / name) for name in files]

    result.banks_inserted = await _ensure_banks(session)
    result.currencies_inserted = await _ensure_currencies(session)
    await session.flush()

    bank_by_slug = {b.slug: b for b in (await session.scalars(select(Bank))).all()}
    currency_by_code = {
        c.code: c for c in (await session.scalars(select(LoyaltyCurrency))).all()
    }

    for review in reviews:
        card, inserted = await _upsert_card(
            session,
            review,
            bank_by_slug=bank_by_slug,
            currency_by_code=currency_by_code,
        )
        if inserted:
            result.cards_inserted += 1
        if await _upsert_article(session, review, card=card):
            result.articles_inserted += 1
        if await _upsert_affiliate_link(session, card):
            result.affiliate_links_inserted += 1

    result.valuations_inserted = await _upsert_sample_valuations(
        session, currency_by_code=currency_by_code
    )

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL"),
        help="Async SQLAlchemy URL. Defaults to $DATABASE_URL.",
    )
    parser.add_argument(
        "--content-dir",
        type=Path,
        default=DEFAULT_CONTENT_DIR,
        help="Directory containing card-review markdown files.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse + inspect, never commit. Default.",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Commit inserts to the database.",
    )
    return parser.parse_args(argv)


async def _async_main(args: argparse.Namespace) -> int:
    if args.db_url is None:
        print(
            "error: --db-url not provided and DATABASE_URL is unset",
            file=sys.stderr,
        )
        return 2

    dry_run = not args.execute

    engine = create_async_engine(args.db_url, pool_pre_ping=True)
    sessionmaker = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

    try:
        async with sessionmaker() as session:
            result = await seed_starter_cards(session, content_dir=args.content_dir)
            if dry_run:
                await session.rollback()
                print(f"seed_starter_cards: DRY RUN (no commit) — would insert {result.summary()}")
            else:
                await session.commit()
                print(f"seed_starter_cards: inserted {result.summary()}")
    finally:
        await engine.dispose()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
