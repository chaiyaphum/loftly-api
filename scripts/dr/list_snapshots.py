"""List DR snapshots in R2, newest first.

Usage::

    uv run python -m scripts.dr.list_snapshots --env staging --limit 20

Output columns: ``date``, ``time``, ``size (MB)``, ``object-lock expiry``, ``key``.
Purely read-only — safe to run from any workstation with R2 creds.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from scripts.dr._core import DEFAULT_BUCKET, DRError, build_r2_client

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:  # pragma: no cover
    S3Client = Any

log = logging.getLogger("loftly.dr.list")


@dataclass(frozen=True)
class SnapshotRecord:
    """One row in the ``list_snapshots`` output."""

    key: str
    size_bytes: int
    last_modified: datetime
    retain_until: datetime | None


def list_snapshots(
    s3: S3Client,
    *,
    bucket: str,
    env: str,
    limit: int | None = None,
) -> list[SnapshotRecord]:
    """Paginate ``list_objects_v2`` and return newest-first records.

    We fetch Object Lock retention per object via a HEAD so the founder
    can see when a given snapshot becomes deletable. Capped by ``limit`` to
    avoid N HEAD requests on a year-old bucket.
    """
    prefix = f"{env}/"
    paginator = s3.get_paginator("list_objects_v2")
    records: list[SnapshotRecord] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".dump.enc"):
                continue
            records.append(
                SnapshotRecord(
                    key=key,
                    size_bytes=int(item["Size"]),
                    last_modified=item["LastModified"],
                    retain_until=None,
                )
            )

    # Sort newest first — keys contain ISO date/time, so lex descending works.
    records.sort(key=lambda r: r.key, reverse=True)
    if limit is not None:
        records = records[:limit]

    # Fill retention per object (best-effort; R2 may not return the header
    # for very old buckets configured without Object Lock).
    enriched: list[SnapshotRecord] = []
    for rec in records:
        retain_until: datetime | None = None
        try:
            head = s3.head_object(Bucket=bucket, Key=rec.key)
            raw = head.get("ObjectLockRetainUntilDate")
            if raw is not None:
                retain_until = raw  # boto returns datetime already
        except Exception:
            # Object Lock not enabled on bucket, or permission issue -
            # logged-not-fatal so list still prints something useful.
            log.debug("head_object_failed", extra={"key": rec.key})
        enriched.append(
            SnapshotRecord(
                key=rec.key,
                size_bytes=rec.size_bytes,
                last_modified=rec.last_modified,
                retain_until=retain_until,
            )
        )
    return enriched


def _format_rows(records: Iterable[SnapshotRecord]) -> str:
    lines = [f"{'date':<10}  {'time':<8}  {'MB':>8}  {'lock-until':<25}  key"]
    for r in records:
        mb = r.size_bytes / 1_048_576
        lock = r.retain_until.isoformat() if r.retain_until else "-"
        ts = r.last_modified.strftime("%Y-%m-%d  %H:%M:%S")
        lines.append(f"{ts}  {mb:>8.2f}  {lock:<25}  {r.key}")
    return "\n".join(lines)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List Loftly DR snapshots")
    parser.add_argument(
        "--env",
        default=os.environ.get("LOFTLY_ENV", "staging"),
        choices=("dev", "staging", "prod"),
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    try:
        s3 = build_r2_client()
        records = list_snapshots(s3, bucket=args.bucket, env=args.env, limit=args.limit)
    except DRError as exc:
        print(f"list failed: {exc}", file=sys.stderr)
        return 1

    if not records:
        print(f"(no snapshots under s3://{args.bucket}/{args.env}/)")
        return 0
    print(_format_rows(records))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
