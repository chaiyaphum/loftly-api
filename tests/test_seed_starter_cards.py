"""Tests for scripts/seed_starter_cards.py.

Verifies:
- Each of the 5 Batch-1 markdown files parses into (frontmatter + body)
- Seeder inserts cards + articles + affiliate links + valuations against a
  fresh in-memory DB
- Re-running the seeder is a no-op (idempotence)
- Article body_th contains Thai characters (UTF-8 round-trip safe)
- Each card is correctly linked to its declared issuer via banks.slug
"""

from __future__ import annotations

from pathlib import Path

import pytest
from scripts.seed_starter_cards import (
    BATCH_1_FILES,
    ISSUER_TO_BANK_SLUG,
    SYSTEM_USER_ID,
    parse_review,
    seed_starter_cards,
)
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import AffiliateLink
from loftly.db.models.article import Article
from loftly.db.models.bank import Bank
from loftly.db.models.card import Card
from loftly.db.models.loyalty_currency import LoyaltyCurrency
from loftly.db.models.point_valuation import PointValuation
from loftly.db.models.user import User

CONTENT_DIR = (
    Path(__file__).resolve().parent.parent.parent / "loftly" / "content" / "card_reviews"
)


@pytest.fixture
def content_dir() -> Path:
    if not CONTENT_DIR.exists():
        pytest.skip(f"content dir missing: {CONTENT_DIR}")
    return CONTENT_DIR


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", BATCH_1_FILES)
def test_parse_each_batch1_file(name: str, content_dir: Path) -> None:
    review = parse_review(content_dir / name)
    assert review.frontmatter["card_slug"]
    assert review.frontmatter["display_name"]
    assert review.frontmatter["issuer"] in ISSUER_TO_BANK_SLUG
    # Body must start with the H1 (`# KBank WISDOM ...`).
    assert review.body.lstrip().startswith("#"), f"{name} body missing H1"
    # Thai characters present — catches any accidental latin-only stub.
    assert any("฀" <= ch <= "๿" for ch in review.body), (
        f"{name} body has no Thai characters"
    )


def test_parse_slug_matches_filename(content_dir: Path) -> None:
    for name in BATCH_1_FILES:
        review = parse_review(content_dir / name)
        expected_slug = name.removesuffix(".md")
        assert review.card_slug == expected_slug


# ---------------------------------------------------------------------------
# End-to-end seed tests — reuse the `app` fixture from conftest.py, which
# creates all tables against the in-memory aiosqlite DB.
# ---------------------------------------------------------------------------


async def _seed_system_user_if_missing() -> None:
    """Articles FK to users.id — ensure the SYSTEM_USER_ID row exists (it is
    normally inserted by migration 012, but `Base.metadata.create_all` skips
    migrations)."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        existing = (
            await session.scalars(select(User).where(User.id == SYSTEM_USER_ID))
        ).one_or_none()
        if existing is None:
            session.add(
                User(
                    id=SYSTEM_USER_ID,
                    email="system@loftly.test",
                    oauth_provider="email_magic",
                    oauth_subject="__system__",
                    role="admin",
                )
            )
            await session.commit()


async def test_seed_inserts_five_cards_with_articles(app: object, content_dir: Path) -> None:
    _ = app  # ensure schema is created via the fixture
    await _seed_system_user_if_missing()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await seed_starter_cards(session, content_dir=content_dir)
        await session.commit()

    assert result.cards_inserted == 5
    assert result.articles_inserted == 5
    assert result.affiliate_links_inserted == 5
    assert result.valuations_inserted == 3

    async with sessionmaker() as session:
        cards = (await session.scalars(select(Card))).all()
        slugs = {c.slug for c in cards}
        expected = {name.removesuffix(".md") for name in BATCH_1_FILES}
        assert expected.issubset(slugs)

        # Every card resolves to the right bank slug.
        bank_by_id = {b.id: b for b in (await session.scalars(select(Bank))).all()}
        for card in cards:
            if card.slug not in expected:
                continue
            review = parse_review(content_dir / f"{card.slug}.md")
            bank_slug = ISSUER_TO_BANK_SLUG[review.issuer]
            assert bank_by_id[card.bank_id].slug == bank_slug, (
                f"{card.slug} → expected bank {bank_slug}, got {bank_by_id[card.bank_id].slug}"
            )

        # Articles are published + carry Thai text.
        articles = (await session.scalars(select(Article))).all()
        review_articles = [a for a in articles if a.slug in expected]
        assert len(review_articles) == 5
        for a in review_articles:
            assert a.state == "published"
            assert a.published_at is not None
            assert a.policy_version == "2026-04-01"
            assert any("฀" <= ch <= "๿" for ch in a.body_th), (
                f"article {a.slug} body_th has no Thai characters"
            )

        # Valuations — exactly the three configured currencies.
        valuations = (await session.scalars(select(PointValuation))).all()
        currency_by_id = {
            c.id: c for c in (await session.scalars(select(LoyaltyCurrency))).all()
        }
        codes = {currency_by_id[v.currency_id].code for v in valuations}
        assert {"ROP", "K_POINT", "UOB_REWARDS"}.issubset(codes)

        # Affiliate links — one placeholder per card, all MoneyGuru.
        links = (await session.scalars(select(AffiliateLink))).all()
        card_ids = {c.id for c in cards if c.slug in expected}
        link_card_ids = {link.card_id for link in links if link.partner_id == "moneyguru"}
        assert card_ids.issubset(link_card_ids)
        for link in links:
            assert "{click_id}" in link.url_template


async def test_seed_is_idempotent(app: object, content_dir: Path) -> None:
    _ = app
    await _seed_system_user_if_missing()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await seed_starter_cards(session, content_dir=content_dir)
        await session.commit()

    async with sessionmaker() as session:
        second = await seed_starter_cards(session, content_dir=content_dir)
        await session.commit()

    assert second.cards_inserted == 0
    assert second.articles_inserted == 0
    assert second.affiliate_links_inserted == 0
    assert second.valuations_inserted == 0
    # Banks + currencies may be 0 too — MEMBERSHIP_REWARDS was inserted on
    # the first pass.
    assert second.banks_inserted == 0
    assert second.currencies_inserted == 0


async def test_seed_creates_membership_rewards_currency(
    app: object, content_dir: Path
) -> None:
    _ = app
    await _seed_system_user_if_missing()

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await seed_starter_cards(session, content_dir=content_dir)
        await session.commit()

    async with sessionmaker() as session:
        mr = (
            await session.scalars(
                select(LoyaltyCurrency).where(LoyaltyCurrency.code == "MEMBERSHIP_REWARDS")
            )
        ).one_or_none()
        assert mr is not None
        assert mr.issuing_entity == "American Express"
