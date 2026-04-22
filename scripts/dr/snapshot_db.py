"""Dump Postgres -> encrypt -> upload to Cloudflare R2 with Object Lock.

Usage::

    uv run python -m scripts.dr.snapshot_db \\
        --database-url postgresql://user:pw@host:5432/db \\
        --env staging

Environment vars consumed:

- ``LOFTLY_DR_ENCRYPTION_KEY`` — 32-byte hex AES-256 key (required).
- ``CF_ACCOUNT_ID``            — Cloudflare account ID for R2 endpoint.
- ``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` — R2 API token.
- ``LOFTLY_DR_BUCKET``         — override bucket name (default ``loftly-dr-snapshots``).

Exit codes:
    0 — uploaded snapshot + manifest.
    1 — any failure (pg_dump, encrypt, upload). Stderr has the reason.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripts.dr._core import (
    DEFAULT_BUCKET,
    OBJECT_LOCK_DAYS,
    DRError,
    build_manifest,
    build_object_key,
    build_r2_client,
    encrypt_bytes,
    load_encryption_key,
    manifest_key_for,
)

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:  # pragma: no cover
    S3Client = Any

log = logging.getLogger("loftly.dr.snapshot")


# ---------------------------------------------------------------------------
# pg_dump + row counting
# ---------------------------------------------------------------------------


def run_pg_dump(database_url: str, out_path: Path) -> None:
    """Invoke pg_dump in custom format. Raises ``DRError`` on non-zero exit."""
    cmd = [
        "pg_dump",
        "--format=custom",
        "--no-owner",
        "--no-acl",
        f"--file={out_path}",
        database_url,
    ]
    log.info("pg_dump_start", extra={"cmd": [*cmd[:-1], "<redacted>"]})
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise DRError("pg_dump binary not found on PATH") from exc
    except subprocess.CalledProcessError as exc:
        raise DRError(f"pg_dump failed (exit {exc.returncode}): {exc.stderr}") from exc


# SQL shape mirrors loftly.db.models — we intentionally keep this a plain
# query rather than reflecting via SQLAlchemy so this script can run with
# only psycopg / asyncpg in the box and no app imports. Filter to the
# public schema; pg_catalog / information_schema aren't in our manifest.
_ROW_COUNT_SQL = """
SELECT table_schema || '.' || table_name AS fqtn, table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name
"""


async def collect_row_counts(database_url: str) -> dict[str, int]:
    """Return ``{table_name: row_count}`` for every public table."""
    # Lazy import — asyncpg is already a top-level dep, but we don't want
    # to pay the import cost when this module is used in tests.
    import asyncpg

    # asyncpg doesn't understand SQLAlchemy's ``postgresql+asyncpg://``
    # prefix, so normalize if needed.
    raw_url = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)

    conn = await asyncpg.connect(raw_url)
    try:
        rows = await conn.fetch(_ROW_COUNT_SQL)
        counts: dict[str, int] = {}
        for row in rows:
            table = row["table_name"]
            # COUNT(*) with a bare quoted identifier. psycopg-style param
            # binding doesn't support schema objects, so inline but we
            # already filtered to table_schema = 'public' so this is safe.
            total = await conn.fetchval(f'SELECT COUNT(*) FROM public."{table}"')
            counts[table] = int(total)
        return counts
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------


def upload_with_object_lock(
    s3: S3Client,
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
    retention_days: int = OBJECT_LOCK_DAYS,
) -> None:
    """``put_object`` with Compliance-mode Object Lock for ``retention_days``."""
    from datetime import UTC, datetime, timedelta

    retain_until = datetime.now(UTC) + timedelta(days=retention_days)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=retain_until,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def run(
    *,
    database_url: str,
    env: str,
    bucket: str,
    s3: S3Client | None = None,
    pg_dump_runner: Any = run_pg_dump,
    row_counter: Any = collect_row_counts,
) -> tuple[str, str]:
    """End-to-end snapshot. Returns ``(object_key, manifest_key)``.

    Split out so tests can inject a fake ``s3``, skip pg_dump, and feed a
    known plaintext blob through the encrypt/upload path.
    """
    key = load_encryption_key()
    s3 = s3 or build_r2_client()
    object_key = build_object_key(env)
    manifest_key = manifest_key_for(object_key)

    with tempfile.TemporaryDirectory(prefix="loftly-dr-") as tmp:
        dump_path = Path(tmp) / "dump.pgcustom"
        pg_dump_runner(database_url, dump_path)
        plaintext = dump_path.read_bytes()

    if not plaintext:
        raise DRError("pg_dump produced an empty file")

    log.info("dump_complete", extra={"bytes": len(plaintext)})

    row_counts = await row_counter(database_url)
    log.info("row_counts_collected", extra={"tables": len(row_counts)})

    ciphertext = encrypt_bytes(plaintext, key)
    manifest = build_manifest(
        env=env,
        object_key=object_key,
        ciphertext=ciphertext,
        row_counts=row_counts,
    )

    upload_with_object_lock(
        s3,
        bucket=bucket,
        key=object_key,
        body=ciphertext,
        content_type="application/octet-stream",
    )
    upload_with_object_lock(
        s3,
        bucket=bucket,
        key=manifest_key,
        body=manifest.to_json().encode("utf-8"),
        content_type="application/json",
    )

    log.info(
        "snapshot_uploaded",
        extra={
            "bucket": bucket,
            "key": object_key,
            "manifest_key": manifest_key,
            "sha256": manifest.sha256,
            "size": manifest.size_bytes,
        },
    )
    return object_key, manifest_key


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Loftly DR snapshot uploader")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Source Postgres URL (default: $DATABASE_URL).",
    )
    parser.add_argument(
        "--env",
        default=os.environ.get("LOFTLY_ENV", "staging"),
        choices=("dev", "staging", "prod"),
        help="Environment tag baked into the object key prefix.",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="R2 bucket name (default: %(default)s).",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    _configure_logging(args.verbose)

    if not args.database_url:
        print("error: --database-url or $DATABASE_URL is required.", file=sys.stderr)
        return 1

    try:
        asyncio.run(run(database_url=args.database_url, env=args.env, bucket=args.bucket))
    except DRError as exc:
        log.error("snapshot_failed", extra={"error": str(exc)})
        print(f"snapshot failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
