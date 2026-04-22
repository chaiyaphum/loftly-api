"""Download + decrypt + pg_restore a DR snapshot.

Usage::

    uv run python -m scripts.dr.restore_db \\
        --snapshot s3://loftly-dr-snapshots/staging/2026-04-21/postgres-04-00-00.dump.enc \\
        --target-database-url postgresql://...

Flags:

- ``--snapshot``            — either a full ``s3://`` URI or ``YYYY-MM-DD``.
                              Date picks the *latest* object in that day's prefix.
- ``--target-database-url`` — where to restore (async prefix stripped automatically).
- ``--really-prod``         — required when the target host matches a prod pattern.
- ``--skip-verify``         — don't run post-restore row-count verification.

Exit codes match ``snapshot_db.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripts.dr._core import (
    DEFAULT_BUCKET,
    DRConfigError,
    DRError,
    SnapshotManifest,
    decrypt_bytes,
    load_encryption_key,
    manifest_key_for,
    parse_snapshot_uri,
)

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:  # pragma: no cover
    S3Client = Any

log = logging.getLogger("loftly.dr.restore")

#: Heuristic for "is this a prod DB URL?" — matches our fly.io prod cluster
#: hostnames + any explicit "prod" token. Kept intentionally fuzzy: the
#: founder would rather the guard fires on a false positive than silently
#: restores over production data.
_PROD_HOST_PATTERN = re.compile(
    r"(^|[@/.-])"  # boundary
    r"(loftly-(api-)?prod"  # loftly-prod, loftly-api-prod
    r"|loftly-postgres-prod"  # naming we've used in fly.toml
    r"|prod\.loftly"  # prod.loftly.co.th
    r"|[\w-]*-prod\.)"  # anything ending -prod.
)

#: Row-count drift tolerance for the post-restore verification. A little
#: slack lets us tolerate background writes during the snapshot itself
#: (e.g. audit_log events fired between row-count collection and pg_dump).
ROW_COUNT_TOLERANCE = 0.05  # 5%


def _target_looks_like_prod(url: str) -> bool:
    """True if the target URL likely points at production."""
    return bool(_PROD_HOST_PATTERN.search(url))


# ---------------------------------------------------------------------------
# Resolution of "YYYY-MM-DD" -> concrete object key
# ---------------------------------------------------------------------------


def resolve_latest_in_prefix(s3: S3Client, *, bucket: str, prefix: str) -> str:
    """List objects under ``prefix`` and return the newest ``.dump.enc`` key."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    dumps = [c for c in contents if c["Key"].endswith(".dump.enc")]
    if not dumps:
        raise DRError(f"no snapshots found under s3://{bucket}/{prefix}")
    # Keys are HH-MM-SS-suffixed so lex order == chronological order.
    newest = max(dumps, key=lambda c: c["Key"])
    return str(newest["Key"])


# ---------------------------------------------------------------------------
# Download + decrypt
# ---------------------------------------------------------------------------


def download_object(s3: S3Client, *, bucket: str, key: str) -> bytes:
    """GetObject -> raw bytes."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    return bytes(resp["Body"].read())


def load_manifest(s3: S3Client, *, bucket: str, dump_key: str) -> SnapshotManifest:
    """Fetch + parse the sidecar manifest matching ``dump_key``."""
    manifest_key = manifest_key_for(dump_key)
    raw = download_object(s3, bucket=bucket, key=manifest_key)
    return SnapshotManifest.from_json(raw)


# ---------------------------------------------------------------------------
# pg_restore
# ---------------------------------------------------------------------------


def run_pg_restore(database_url: str, dump_path: Path) -> None:
    """``pg_restore --clean --if-exists --no-owner --no-acl``."""
    # Normalize async driver prefix — pg_restore speaks libpq, not SQLA.
    url = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    cmd = [
        "pg_restore",
        "--clean",
        "--if-exists",
        "--no-owner",
        "--no-acl",
        f"--dbname={url}",
        str(dump_path),
    ]
    log.info("pg_restore_start", extra={"cmd": [*cmd[:-1], "<file>"]})
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise DRError("pg_restore binary not found on PATH") from exc
    except subprocess.CalledProcessError as exc:
        raise DRError(f"pg_restore failed (exit {exc.returncode}): {exc.stderr}") from exc


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


async def verify_row_counts(
    database_url: str,
    expected: dict[str, int],
    *,
    tolerance: float = ROW_COUNT_TOLERANCE,
) -> list[str]:
    """Return a list of drift warnings (empty list means everything matched)."""
    # Re-use the same query shape as snapshot_db to keep the comparison fair.
    from scripts.dr.snapshot_db import collect_row_counts

    actual = await collect_row_counts(database_url)
    warnings: list[str] = []
    for table, want in expected.items():
        got = actual.get(table)
        if got is None:
            warnings.append(f"table missing post-restore: {table}")
            continue
        # Absolute tolerance for tiny tables (±1 row), percentage for larger.
        allowed = max(1, int(want * tolerance))
        if abs(got - want) > allowed:
            warnings.append(
                f"row_count drift: {table} expected={want} actual={got} "
                f"delta={got - want} allowed=±{allowed}"
            )
    return warnings


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def run(
    *,
    snapshot_ref: str,
    target_database_url: str,
    really_prod: bool,
    skip_verify: bool,
    bucket: str,
    env: str,
    s3: S3Client | None = None,
    pg_restore_runner: Any = run_pg_restore,
) -> None:
    """Download -> decrypt -> restore -> verify."""
    if _target_looks_like_prod(target_database_url) and not really_prod:
        raise DRConfigError(
            "refusing to restore into a prod-looking target without --really-prod; "
            f"target: {target_database_url}"
        )

    # Lazy import to avoid requiring boto3 in test harnesses that inject ``s3``.
    if s3 is None:
        from scripts.dr._core import build_r2_client

        s3 = build_r2_client()

    bucket_from_ref, key_or_prefix = parse_snapshot_uri(snapshot_ref, bucket=bucket, env=env)
    effective_bucket = bucket_from_ref or bucket

    if key_or_prefix.endswith("/"):
        dump_key = resolve_latest_in_prefix(s3, bucket=effective_bucket, prefix=key_or_prefix)
    else:
        dump_key = key_or_prefix

    log.info("resolved_snapshot", extra={"bucket": effective_bucket, "key": dump_key})

    key = load_encryption_key()
    blob = download_object(s3, bucket=effective_bucket, key=dump_key)
    plaintext = decrypt_bytes(blob, key)

    manifest = load_manifest(s3, bucket=effective_bucket, dump_key=dump_key)

    with tempfile.TemporaryDirectory(prefix="loftly-dr-restore-") as tmp:
        dump_path = Path(tmp) / "restore.pgcustom"
        dump_path.write_bytes(plaintext)
        pg_restore_runner(target_database_url, dump_path)

    if skip_verify:
        log.info("verification_skipped")
        return

    warnings = await verify_row_counts(target_database_url, manifest.row_counts)
    if warnings:
        for w in warnings:
            log.warning("row_count_drift", extra={"detail": w})
        raise DRError(
            f"post-restore verification failed with {len(warnings)} drift(s); "
            "re-run with --skip-verify to bypass after investigating."
        )
    log.info("restore_verified", extra={"tables": len(manifest.row_counts)})


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Loftly DR snapshot restorer")
    parser.add_argument("--snapshot", required=True, help="s3:// URI or YYYY-MM-DD")
    parser.add_argument(
        "--target-database-url",
        required=True,
        help="Destination Postgres URL.",
    )
    parser.add_argument(
        "--really-prod",
        action="store_true",
        help="Acknowledge restoring into a prod-looking target.",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip post-restore row-count verification against the manifest.",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="R2 bucket (default: %(default)s).",
    )
    parser.add_argument(
        "--env",
        default=os.environ.get("LOFTLY_ENV", "staging"),
        choices=("dev", "staging", "prod"),
        help="Env prefix when resolving YYYY-MM-DD shorthand.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    _configure_logging(args.verbose)

    try:
        asyncio.run(
            run(
                snapshot_ref=args.snapshot,
                target_database_url=args.target_database_url,
                really_prod=args.really_prod,
                skip_verify=args.skip_verify,
                bucket=args.bucket,
                env=args.env,
            )
        )
    except DRError as exc:
        log.error("restore_failed", extra={"error": str(exc)})
        print(f"restore failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
