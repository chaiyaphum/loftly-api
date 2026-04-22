"""audit_log retention cron — PDPA-aware, action-type classified.

Context
-------
`audit_log` is the accountability ledger for admin + system writes. PDPA
accountability requires retention, but retention isn't infinite: once an event
is old enough that it can't contribute to an incident investigation, the row
is legal exposure with no upside.

Neither `PDPA_COMPLIANCE.md` nor `mvp/SCHEMA.md §15` pin a retention period
for `audit_log` today. We default to the two-bucket policy captured below;
update those docs once Legal signs off.

Retention policy (defaults; override via env or CLI flags before prod run):
    - Consent bucket (`consent.*`, `account.delete.*`, `privacy.*`): 7 years.
      PDPA §19 requires the controller to be able to prove a user granted (or
      revoked) consent for a given purpose. Thai civil-law limitation periods
      are 5-10 years depending on claim — 7 years is the conservative middle.
    - Default bucket (everything else — `card.*`, `article.*`, `promo.*`,
      `webhook.*`, etc.): 18 months. Long enough to cover a normal audit cycle
      plus a grace window; short enough that non-sensitive operator activity
      doesn't accumulate forever.

Execution
---------
- Dry-run (default): counts rows per bucket that WOULD be deleted, no writes.
- Execute: batches of `BATCH_SIZE` rows until 0 rows affected per bucket.
- Emits structured log `audit_log_retention_run` with per-bucket counts +
  total duration. Prometheus counter stubs are recorded via the `_METRICS`
  in-process dict; when `/metrics` is wired in Phase 2 these flip to real
  `prometheus_client.Counter` increments without touching the call sites.

CLI
---
    uv run python -m loftly.jobs.audit_log_retention --dry-run
    uv run python -m loftly.jobs.audit_log_retention --execute

Invariants
----------
- Consent rows are never deleted before the 7-year cutoff, even if some other
  action_type classifier matches (the consent bucket is evaluated first).
- `--execute` without a TTY flag intentionally does NOT guard — the cron
  trigger needs to run unattended. Humans pressing go manually should use
  `--dry-run` first; the staging invariant is enforced in the loftly-scheduler
  repo (weekly cron only).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

#: `action` prefixes that touch consent proof. Kept the full 7 years.
CONSENT_ACTION_PREFIXES: tuple[str, ...] = (
    "consent.",
    "account.delete.",
    "privacy.",
    "pdpa.",
)

#: Days to keep consent-proof rows. PDPA-aligned.
CONSENT_RETENTION_DAYS: int = 7 * 365

#: Days to keep non-consent rows (card/article/promo/webhook/admin).
DEFAULT_RETENTION_DAYS: int = 18 * 30  # 540 days, ~18 months

#: DELETE batch size. Postgres will still hold row-locks per batch, so we
#: intentionally keep this small enough to not block concurrent writes.
BATCH_SIZE: int = 1000


@dataclass(frozen=True)
class BucketPolicy:
    """A (name, predicate, retention) row in the policy table."""

    name: str
    retention_days: int
    #: SQLAlchemy boolean expression selecting rows in this bucket.
    match: Any  # kept Any because the expression type is dialect-dependent.


def _consent_match() -> Any:
    """OR over `action LIKE '<prefix>%'` for every consent prefix."""
    clauses = [AuditLog.action.like(f"{p}%") for p in CONSENT_ACTION_PREFIXES]
    return or_(*clauses)


def _default_match() -> Any:
    """Inverse of `_consent_match` — everything not in the consent bucket."""
    if not CONSENT_ACTION_PREFIXES:
        return True
    # NOT (any consent prefix matches) — explicit `and_` of `NOT LIKE` so the
    # predicate composes cleanly in the LIMIT subquery below.
    return and_(*[AuditLog.action.notlike(f"{p}%") for p in CONSENT_ACTION_PREFIXES])


def _build_policies() -> list[BucketPolicy]:
    """Construct the policy list from env-overridable retention days."""
    consent_days = int(os.environ.get("AUDIT_RETENTION_CONSENT_DAYS", CONSENT_RETENTION_DAYS))
    default_days = int(os.environ.get("AUDIT_RETENTION_DEFAULT_DAYS", DEFAULT_RETENTION_DAYS))
    return [
        BucketPolicy(name="consent", retention_days=consent_days, match=_consent_match()),
        BucketPolicy(name="default", retention_days=default_days, match=_default_match()),
    ]


# ---------------------------------------------------------------------------
# Metrics stubs
# ---------------------------------------------------------------------------

# In-process counters. When /metrics lands (observability Phase 2) this module
# will register real prometheus_client.Counter instances named:
#   - audit_log_retention_run_total{mode="dry_run|execute"}
#   - audit_log_rows_deleted_total{bucket="consent|default"}
_METRICS: dict[str, int] = {
    "audit_log_retention_run_total_dry_run": 0,
    "audit_log_retention_run_total_execute": 0,
    "audit_log_rows_deleted_total_consent": 0,
    "audit_log_rows_deleted_total_default": 0,
}


def _inc(key: str, amount: int = 1) -> None:
    _METRICS[key] = _METRICS.get(key, 0) + amount


def get_metrics_snapshot() -> dict[str, int]:
    """Return a copy of in-process counter state (for tests + /metrics stub)."""
    return dict(_METRICS)


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BucketResult:
    name: str
    retention_days: int
    cutoff: datetime
    rows_deleted: int
    rows_matched: int


@dataclass(frozen=True)
class RetentionResult:
    dry_run: bool
    started_at: datetime
    duration_ms: float
    buckets: list[BucketResult]

    @property
    def total_rows_deleted(self) -> int:
        return sum(b.rows_deleted for b in self.buckets)

    @property
    def total_rows_matched(self) -> int:
        return sum(b.rows_matched for b in self.buckets)

    def to_log_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "started_at": self.started_at.isoformat(),
            "duration_ms": round(self.duration_ms, 2),
            "total_rows_deleted": self.total_rows_deleted,
            "total_rows_matched": self.total_rows_matched,
            "buckets": [
                {
                    "name": b.name,
                    "retention_days": b.retention_days,
                    "cutoff": b.cutoff.isoformat(),
                    "rows_deleted": b.rows_deleted,
                    "rows_matched": b.rows_matched,
                }
                for b in self.buckets
            ],
        }


async def _count_matching(session: AsyncSession, policy: BucketPolicy, cutoff: datetime) -> int:
    stmt = (
        select(func.count()).select_from(AuditLog).where(policy.match, AuditLog.created_at < cutoff)
    )
    return int((await session.execute(stmt)).scalar_one())


async def _delete_batch(session: AsyncSession, policy: BucketPolicy, cutoff: datetime) -> int:
    """Delete up to BATCH_SIZE matching rows. Returns rows actually deleted.

    Uses an `id IN (subquery LIMIT N)` pattern for portability — SQLite does
    not support `DELETE ... LIMIT` and Postgres requires a CTE / subquery for
    batched deletes anyway.
    """
    subq = (
        select(AuditLog.id)
        .where(policy.match, AuditLog.created_at < cutoff)
        .limit(BATCH_SIZE)
        .scalar_subquery()
    )
    result = await session.execute(delete(AuditLog).where(AuditLog.id.in_(subq)))
    await session.commit()
    # `rowcount` is a DBAPI attr on the CursorResult subclass returned by
    # UPDATE/DELETE; mypy's base Result type doesn't expose it.
    rowcount = getattr(result, "rowcount", 0) or 0
    return int(rowcount)


async def _run_bucket(
    session: AsyncSession,
    policy: BucketPolicy,
    *,
    now: datetime,
    dry_run: bool,
) -> BucketResult:
    cutoff = now - timedelta(days=policy.retention_days)
    matched = await _count_matching(session, policy, cutoff)
    if dry_run:
        return BucketResult(
            name=policy.name,
            retention_days=policy.retention_days,
            cutoff=cutoff,
            rows_deleted=0,
            rows_matched=matched,
        )

    deleted_total = 0
    # Loop until a batch deletes 0 rows. Hard cap by `matched` as a belt-and-
    # suspenders safeguard against an always-matching predicate (shouldn't
    # happen, but DELETE in a while-loop without a ceiling feels scary).
    max_iters = max(1, (matched // BATCH_SIZE) + 2)
    for _ in range(max_iters):
        deleted = await _delete_batch(session, policy, cutoff)
        deleted_total += deleted
        if deleted == 0:
            break
    _inc(f"audit_log_rows_deleted_total_{policy.name}", deleted_total)
    return BucketResult(
        name=policy.name,
        retention_days=policy.retention_days,
        cutoff=cutoff,
        rows_deleted=deleted_total,
        rows_matched=matched,
    )


async def run_retention(*, dry_run: bool = True, now: datetime | None = None) -> RetentionResult:
    """Top-level entry point. Opens its own session via the process sessionmaker.

    Args:
        dry_run: when True (default), only counts matching rows.
        now: override the reference time — used by tests to simulate clock skew.
    """
    started_at = datetime.now(UTC) if now is None else now
    t0 = time.perf_counter()
    sessionmaker = get_sessionmaker()
    buckets: list[BucketResult] = []
    async with sessionmaker() as session:
        for policy in _build_policies():
            bucket_result = await _run_bucket(session, policy, now=started_at, dry_run=dry_run)
            buckets.append(bucket_result)

    _inc(f"audit_log_retention_run_total_{'dry_run' if dry_run else 'execute'}")

    result = RetentionResult(
        dry_run=dry_run,
        started_at=started_at,
        duration_ms=(time.perf_counter() - t0) * 1000.0,
        buckets=buckets,
    )
    log.info("audit_log_retention_run", **result.to_log_dict())
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="loftly.jobs.audit_log_retention",
        description="Run or preview the audit_log retention sweep (PDPA-aware).",
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows that would be deleted — no writes.",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete matching rows in BATCH_SIZE batches.",
    )
    return p


def _cli(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(run_retention(dry_run=args.dry_run))
    # Also print the JSON-able dict so pipe-to-jq works from cron logs.
    import json

    print(json.dumps(result.to_log_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())


__all__ = [
    "BATCH_SIZE",
    "CONSENT_ACTION_PREFIXES",
    "CONSENT_RETENTION_DAYS",
    "DEFAULT_RETENTION_DAYS",
    "BucketResult",
    "RetentionResult",
    "get_metrics_snapshot",
    "run_retention",
]
