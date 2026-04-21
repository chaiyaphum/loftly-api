"""Seed-round anonymized metrics exporter CLI.

Writes a JSON artifact with ~6 aggregate sections (users / selector / affiliate
/ content / llm_costs / system) suitable for inclusion in the data room. Zero
PII leaves this process — see `src/loftly/jobs/metrics_export.py` for the
exact aggregations.

Usage:

    uv run python scripts/run_metrics_export.py \\
        --as-of 2026-10-01 \\
        --out data-room/metrics-2026-10.json

    # Default --as-of = now (UTC). Default --out = ./metrics-<date>.json.
    uv run python scripts/run_metrics_export.py

Review checklist for PII is in `docs/SEED_ROUND_DATA_ROOM.md`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Force test-like settings so running the script without a full env file
# (e.g. on the founder's laptop for a dry-run) doesn't crash at import time.
os.environ.setdefault("LOFTLY_ENV", "dev")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./loftly_dev.db")
os.environ.setdefault("JWT_SIGNING_KEY", "cli-metrics-export")


def _parse_as_of(raw: str | None) -> datetime:
    if raw is None:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        print(f"error: --as-of {raw!r} must be ISO8601 (YYYY-MM-DD).", file=sys.stderr)
        sys.exit(2)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _default_out_path(as_of: datetime) -> str:
    stamp = as_of.date().isoformat()
    return str(Path.cwd() / f"metrics-{stamp}.json")


async def _run(as_of: datetime, out_path: str) -> None:
    from loftly.core.logging import configure_logging
    from loftly.core.settings import get_settings
    from loftly.jobs.metrics_export import run_export

    configure_logging(get_settings())
    payload = await run_export(out_path, as_of)
    # Human-readable echo so shell callers can `| grep` without parsing the file.
    users = payload["users"]
    affiliate = payload["affiliate"]
    print(f"metrics_export: wrote {out_path}")
    print(f"  as_of={payload['as_of']}")
    print(f"  total_registered={users['total_registered']}")
    print(f"  wau={users['wau']} mau={users['mau']}")
    print(
        f"  affiliate_clicks={affiliate['total_clicks']} "
        f"conversions={affiliate['conversions']} "
        f"rate={affiliate['conversion_rate']}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Loftly anonymized metrics exporter.")
    parser.add_argument(
        "--as-of",
        default=None,
        help="ISO date/datetime snapshot anchor (default: now UTC).",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output JSON path (default: ./metrics-<as_of_date>.json).",
    )
    args = parser.parse_args()

    as_of = _parse_as_of(args.as_of)
    out_path = args.out or _default_out_path(as_of)
    asyncio.run(_run(as_of, out_path))


if __name__ == "__main__":
    main()
