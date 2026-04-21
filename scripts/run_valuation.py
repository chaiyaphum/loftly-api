"""Weekly valuation job runner.

Usage:
    uv run python -m scripts.run_valuation

Walks every `loyalty_currencies` row, reads the matching `data/award_charts/{code}.json`
+ `data/cash_fares/{code}.json` fixtures, runs the 80th-percentile math per
`mvp/VALUATION_METHOD.md`, and upserts a `point_valuations` row per currency.
Idempotent within a single ISO week — re-runs within the same week are no-ops
(the latest row is preserved).

Run *after* `uv run python -m scripts.seed_catalog`.
"""

from __future__ import annotations

import asyncio

from loftly.core.logging import configure_logging, get_logger
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.jobs.valuation import run_all


async def _run() -> None:
    configure_logging(get_settings())
    log = get_logger(__name__)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        results = await run_all(session)
    log.info("run_valuation_complete", rows=len(results))
    # Echo tail so shell callers can `| tail` without parsing JSON logs.
    for r in results:
        print(
            f"valuation {r.currency_code}: "
            f"thb_per_point={r.thb_per_point:.4f} "
            f"sample_size={r.sample_size} "
            f"confidence={r.confidence:.2f} "
            f"flags={r.sanity_flags}"
        )
    print(f"run_valuation: {len(results)} rows processed")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
