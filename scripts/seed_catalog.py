"""Idempotent catalog seed runner.

Usage:
    uv run python -m scripts.seed_catalog

Seeds the 8 banks + 8 loyalty currencies + 2 sample cards from
`mvp/artifacts/schema.sql` seed comments. Safe to re-run; existing rows are
left alone (matched on unique columns: banks.slug, loyalty_currencies.code,
cards.slug).

Run *after* `alembic upgrade head`.
"""

from __future__ import annotations

import asyncio

from loftly.core.logging import configure_logging, get_logger
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.seed import seed_all


async def _run() -> None:
    configure_logging(get_settings())
    log = get_logger(__name__)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        stats = await seed_all(session)
    log.info(
        "seed_summary",
        banks_inserted=stats.banks_inserted,
        currencies_inserted=stats.currencies_inserted,
        cards_inserted=stats.cards_inserted,
    )
    # Echo to stdout so shell callers can grep the tail regardless of log config.
    summary = (
        f"banks={stats.banks_inserted} "
        f"loyalty_currencies={stats.currencies_inserted} "
        f"cards={stats.cards_inserted}"
    )
    print(f"seed_catalog: inserted {summary}")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
