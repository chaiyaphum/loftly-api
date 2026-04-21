"""CLI entry for the manual-catalog ingest.

Run dry-run first, inspect the delta, then `--execute` when the diff looks
right. Example:

    uv run python scripts/run_manual_ingest.py --bank uob --dry-run
    uv run python scripts/run_manual_ingest.py --bank uob --execute

Exits non-zero on validation/DB errors so CI or cron can pick up failures.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from loftly.core.logging import configure_logging, get_logger
from loftly.core.settings import get_settings
from loftly.jobs.manual_catalog_ingest import run_ingest


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_manual_ingest",
        description="Sync a manual-catalog bank fixture into the promos table.",
    )
    parser.add_argument(
        "--bank",
        required=True,
        help="Bank slug (e.g. 'uob', 'krungsri'). Must match a row in banks.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report diff counts without writing to the DB.",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Commit inserts / updates / archives to the promos table.",
    )
    return parser.parse_args(argv)


async def _run(bank: str, dry_run: bool) -> int:
    configure_logging(get_settings())
    log = get_logger(__name__)
    try:
        result = await run_ingest(bank, dry_run=dry_run)
    except FileNotFoundError as exc:
        log.error("manual_ingest_fixture_missing", bank=bank, error=str(exc))
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        log.error("manual_ingest_invalid", bank=bank, error=str(exc))
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result.model_dump(), indent=2, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    dry_run = bool(args.dry_run and not args.execute)
    return asyncio.run(_run(args.bank, dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
