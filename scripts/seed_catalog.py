"""Idempotent catalog seed runner.

Usage:
    uv run python -m scripts.seed_catalog

Seeds the 8 banks + 9 loyalty currencies + the sample cards from
`mvp/artifacts/schema.sql` seed comments, then the 3 Batch-1 enrichment
cards (KTC Forever, SCB Prime, Amex Gold) from `/mvp/CARD_PRIORITY.md §Tier 1`.
Safe to re-run; existing rows are left alone (matched on unique columns:
banks.slug, loyalty_currencies.code, cards.slug).

Run *after* `alembic upgrade head`.
"""

from __future__ import annotations

import asyncio

from loftly.core.logging import configure_logging, get_logger
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.seed import seed_all, seed_batch1_cards, seed_batch1_promos


async def _run() -> None:
    configure_logging(get_settings())
    log = get_logger(__name__)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        stats = await seed_all(session)
    # Batch-1 cards live outside `seed_all` so the test fixture keeps its
    # 3-card baseline (merchant-ranking golden snapshot depends on it).
    # Open a fresh session — `seed_all` committed + implicit expire.
    async with sessionmaker() as session:
        batch1_inserted = await seed_batch1_cards(session)

    # Batch-1 promos — 8 SCB + 6 KTC. Kept out of `seed_all` for the same
    # reason as the Batch-1 cards: tests count rows against the fixture.
    async with sessionmaker() as session:
        batch1_promos_inserted = await seed_batch1_promos(session)

    total_cards = stats.cards_inserted + batch1_inserted
    log.info(
        "seed_summary",
        banks_inserted=stats.banks_inserted,
        currencies_inserted=stats.currencies_inserted,
        cards_inserted=total_cards,
        batch1_cards_inserted=batch1_inserted,
        batch1_promos_inserted=batch1_promos_inserted,
    )
    # Echo to stdout so shell callers can grep the tail regardless of log config.
    summary = (
        f"banks={stats.banks_inserted} "
        f"loyalty_currencies={stats.currencies_inserted} "
        f"cards={total_cards} "
        f"(batch1={batch1_inserted}) "
        f"promos={batch1_promos_inserted}"
    )
    print(f"seed_catalog: inserted {summary}")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
