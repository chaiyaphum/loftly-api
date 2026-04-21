"""End-to-end restore drill: fetch latest snapshot, restore into a scratch DB, verify, drop.

Usage::

    uv run python -m scripts.dr.drill \\
        --admin-database-url postgresql://postgres:pw@staging-cluster:5432/postgres \\
        --env staging

The admin URL must point at a user with ``CREATE DATABASE`` / ``DROP DATABASE``
privileges on the staging cluster. The scratch DB is named
``loftly_dr_drill_<YYYYMMDD_HHMMSS>`` and always dropped in the finally block,
even on failure, so we don't leak scratch DBs on repeat failures.

Exit 0 on PASS, 1 on FAIL. Timing is printed in the final line.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from scripts.dr._core import DEFAULT_BUCKET, DRError, build_r2_client

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:  # pragma: no cover
    S3Client = Any

log = logging.getLogger("loftly.dr.drill")


def _scratch_db_name(now: datetime | None = None) -> str:
    t = now or datetime.now(UTC)
    return f"loftly_dr_drill_{t.strftime('%Y%m%d_%H%M%S')}"


def _swap_db_in_url(admin_url: str, new_db: str) -> str:
    """Replace the database path in a libpq URL."""
    # Strip fragment/query first, then swap everything after the last '/'.
    base, _, tail = admin_url.partition("?")
    prefix, _, _old = base.rpartition("/")
    new_base = f"{prefix}/{new_db}"
    return f"{new_base}?{tail}" if tail else new_base


async def _create_db(admin_url: str, scratch_name: str) -> None:
    import asyncpg

    url = admin_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    conn = await asyncpg.connect(url)
    try:
        await conn.execute(f'CREATE DATABASE "{scratch_name}"')
    finally:
        await conn.close()


async def _drop_db(admin_url: str, scratch_name: str) -> None:
    import asyncpg

    url = admin_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    conn = await asyncpg.connect(url)
    try:
        # Force-disconnect any lingering sessions first; otherwise DROP fails
        # if pg_restore's temp connection is still winding down.
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            scratch_name,
        )
        await conn.execute(f'DROP DATABASE IF EXISTS "{scratch_name}"')
    finally:
        await conn.close()


async def run_drill(
    *,
    admin_database_url: str,
    env: str,
    bucket: str,
    s3: S3Client | None = None,
) -> bool:
    """Return True on PASS. Logs timing + table counts either way."""
    from scripts.dr.restore_db import run as run_restore

    started = time.monotonic()
    scratch = _scratch_db_name()
    target_url = _swap_db_in_url(admin_database_url, scratch)

    log.info("drill_begin", extra={"scratch_db": scratch, "env": env})

    s3 = s3 or build_r2_client()

    await _create_db(admin_database_url, scratch)
    passed = False
    try:
        # Resolve "today" shorthand; restore_db will walk into the latest.
        await run_restore(
            snapshot_ref=datetime.now(UTC).strftime("%Y-%m-%d"),
            target_database_url=target_url,
            really_prod=False,  # scratch DB, never prod
            skip_verify=False,
            bucket=bucket,
            env=env,
            s3=s3,
        )
        passed = True
    except DRError as exc:
        log.error("drill_restore_failed", extra={"error": str(exc)})
    finally:
        try:
            await _drop_db(admin_database_url, scratch)
        except Exception as exc:  # pragma: no cover - best-effort cleanup
            log.error("drill_drop_failed", extra={"error": str(exc)})

    elapsed = time.monotonic() - started
    status = "PASS" if passed else "FAIL"
    print(f"DR DRILL {status} scratch={scratch} env={env} elapsed={elapsed:.1f}s")
    return passed


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Loftly DR restore drill")
    parser.add_argument(
        "--admin-database-url",
        default=os.environ.get("DR_DRILL_ADMIN_DATABASE_URL"),
        help="Postgres URL with CREATE/DROP DATABASE on the staging cluster.",
    )
    parser.add_argument(
        "--env",
        default=os.environ.get("LOFTLY_ENV", "staging"),
        choices=("dev", "staging", "prod"),
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    if not args.admin_database_url:
        print(
            "error: --admin-database-url or $DR_DRILL_ADMIN_DATABASE_URL is required.",
            file=sys.stderr,
        )
        return 1
    if args.env == "prod":
        print("refusing to drill against prod env prefix.", file=sys.stderr)
        return 1

    passed = asyncio.run(
        run_drill(
            admin_database_url=args.admin_database_url,
            env=args.env,
            bucket=args.bucket,
        )
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
