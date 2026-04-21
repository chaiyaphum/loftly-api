"""Seed-round data-room: anonymized metrics exporter.

Produces a JSON artifact containing only aggregates + rates — **zero raw PII**.
Intended for inclusion in the Loftly seed-round data room so investors can
review traction without touching personally-identifiable data (emails, user
UUIDs, IP hashes, click IDs, etc.).

PDPA compliance: the exporter never reads PII into the output payload. Every
query is either a `COUNT(*)`, `SUM(...)`, ratio, or small-cardinality group-by
over stable, non-PII columns (e.g. card slug). Raw identifiers (user_id,
click_id, email, oauth_subject) are never serialized.

Shape (top-level keys):

- ``generated_at``  — ISO8601 UTC timestamp when the export ran.
- ``as_of``         — caller-supplied snapshot anchor (ISO8601).
- ``window_days``   — 30 (for uptime / 5xx / latency baselines).
- ``users``         — totals, WAU/MAU, retention curve (12 weeks), consent %.
- ``selector``      — invocations, unique users, avg latency, top-1 conv rate.
- ``affiliate``     — clicks, conversions, commission buckets, top-5 cards.
- ``content``       — articles published + distinct cards covered.
- ``llm_costs``     — placeholder spend/cache/fallback metrics (Langfuse TBD).
- ``system``        — placeholder uptime + p95 latency (Grafana TBD).

The Langfuse + Grafana sections are **placeholders** until those integrations
are wired into prod (see DEPLOYMENT.md §Observability). The schema slots are
stable so adding the live values later is additive, not breaking.

Call sites:

- ``POST /v1/admin/metrics/export`` — returns the JSON body inline.
- ``scripts/run_metrics_export.py`` — CLI wrapper the founder runs to drop a
  file on disk (e.g. ``data-room/metrics-2026-10.json``).
"""

from __future__ import annotations

import json
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import AffiliateClick, AffiliateConversion
from loftly.db.models.article import Article
from loftly.db.models.card import Card as CardModel
from loftly.db.models.consent import UserConsent
from loftly.db.models.selector_session import SelectorSession
from loftly.db.models.user import User

log = get_logger(__name__)

EXPORT_SCHEMA_VERSION = "1.0"
SYSTEM_WINDOW_DAYS = 30
RETENTION_WEEKS = 12
AFFILIATE_MONTHS = 6
TOP_CARD_LIMIT = 5


# ---------------------------------------------------------------------------
# Time-bucket helpers
# ---------------------------------------------------------------------------


def _to_utc(dt: datetime) -> datetime:
    """Normalize naive / aware datetimes to tz-aware UTC.

    SQLite returns naive datetimes via SQLAlchemy's TIMESTAMP(timezone=True),
    while Postgres returns aware ones. Callers subtract `as_of` (aware) from
    these values, so normalize on read to avoid TypeError under aiosqlite.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _iso_week_start(dt: datetime) -> datetime:
    """Monday 00:00 UTC of the ISO week containing `dt`."""
    anchor = _to_utc(dt)
    monday = anchor - timedelta(days=anchor.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


def _month_start(dt: datetime) -> datetime:
    """First day of the calendar month containing `dt` (UTC)."""
    anchor = _to_utc(dt)
    return anchor.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _prev_month_start(m: datetime) -> datetime:
    """First day of the month *before* the month that `m` starts."""
    # `m` is already a month-start (day=1, midnight). Go back 1 day then snap.
    prev_last_day = m - timedelta(days=1)
    return prev_last_day.replace(day=1)


def _last_n_weeks(as_of: datetime, n: int) -> list[tuple[datetime, datetime]]:
    """Return [(week_start, week_end_exclusive)] for the last `n` ISO weeks ending on `as_of`.

    Ordered oldest → newest so retention curves read left-to-right in time.
    """
    current_week = _iso_week_start(as_of)
    weeks: list[tuple[datetime, datetime]] = []
    for i in range(n - 1, -1, -1):
        start = current_week - timedelta(weeks=i)
        end = start + timedelta(weeks=1)
        weeks.append((start, end))
    return weeks


def _last_n_months(as_of: datetime, n: int) -> list[tuple[datetime, datetime]]:
    """Return [(month_start, next_month_start)] for last `n` months ending on `as_of`."""
    current_month = _month_start(as_of)
    months: list[tuple[datetime, datetime]] = []
    cursor = current_month
    # Walk backwards to collect starts, then flip.
    starts: list[datetime] = []
    for _ in range(n):
        starts.append(cursor)
        cursor = _prev_month_start(cursor)
    for start in reversed(starts):
        next_month = _month_start(start + timedelta(days=32))
        months.append((start, next_month))
    return months


# ---------------------------------------------------------------------------
# Aggregation — users
# ---------------------------------------------------------------------------


async def _user_metrics(session: AsyncSession, as_of: datetime) -> dict[str, Any]:
    """Totals, WAU, MAU, retention curve, consent-granted rate per purpose."""
    # Total registered — excludes soft-deleted.
    total_registered = (
        await session.execute(select(func.count(User.id)).where(User.deleted_at.is_(None)))
    ).scalar_one()

    # WAU / MAU — proxy via SelectorSession activity (the only user-bound
    # interaction table in Phase 1; the app has no sessions/page-views yet).
    wau_start = as_of - timedelta(days=7)
    mau_start = as_of - timedelta(days=30)
    wau = (
        await session.execute(
            select(func.count(func.distinct(SelectorSession.user_id))).where(
                SelectorSession.user_id.is_not(None),
                SelectorSession.created_at >= wau_start,
                SelectorSession.created_at <= as_of,
            )
        )
    ).scalar_one()
    mau = (
        await session.execute(
            select(func.count(func.distinct(SelectorSession.user_id))).where(
                SelectorSession.user_id.is_not(None),
                SelectorSession.created_at >= mau_start,
                SelectorSession.created_at <= as_of,
            )
        )
    ).scalar_one()

    # Retention curve — per-week distinct active user counts for the last N weeks.
    retention: list[dict[str, Any]] = []
    weeks = _last_n_weeks(as_of, RETENTION_WEEKS)
    for start, end in weeks:
        active = (
            await session.execute(
                select(func.count(func.distinct(SelectorSession.user_id))).where(
                    SelectorSession.user_id.is_not(None),
                    SelectorSession.created_at >= start,
                    SelectorSession.created_at < end,
                )
            )
        ).scalar_one()
        retention.append(
            {
                "week_start": start.date().isoformat(),
                "active_users": int(active or 0),
            }
        )

    # Consent granted % per purpose — latest row per (user, purpose), then
    # ratio of `granted=true`. Done in Python (portable across PG + SQLite).
    consent_rows = list(
        (
            await session.execute(
                select(
                    UserConsent.user_id,
                    UserConsent.purpose,
                    UserConsent.granted,
                    UserConsent.granted_at,
                ).where(UserConsent.granted_at <= as_of)
            )
        ).all()
    )
    # Latest-per-(user, purpose). Normalize timestamps so naive SQLite +
    # aware Postgres rows sort consistently.
    latest: dict[tuple[uuid.UUID, str], tuple[datetime, bool]] = {}
    for user_id, purpose, granted, granted_at in consent_rows:
        key = (user_id, purpose)
        ts = _to_utc(granted_at)
        prev = latest.get(key)
        if prev is None or ts > prev[0]:
            latest[key] = (ts, bool(granted))

    # Aggregate by purpose.
    per_purpose_totals: Counter[str] = Counter()
    per_purpose_granted: Counter[str] = Counter()
    for (_user_id, purpose), (_ts, granted) in latest.items():
        per_purpose_totals[purpose] += 1
        if granted:
            per_purpose_granted[purpose] += 1
    consent_grant_rate: dict[str, dict[str, Any]] = {}
    for purpose, total in per_purpose_totals.items():
        granted_count = per_purpose_granted[purpose]
        consent_grant_rate[purpose] = {
            "users_prompted": int(total),
            "users_granted": int(granted_count),
            "grant_rate": float(granted_count) / float(total) if total else 0.0,
        }

    return {
        "total_registered": int(total_registered or 0),
        "wau": int(wau or 0),
        "mau": int(mau or 0),
        "retention_weekly": retention,
        "consent_grant_rate": consent_grant_rate,
    }


# ---------------------------------------------------------------------------
# Aggregation — selector
# ---------------------------------------------------------------------------


async def _selector_metrics(session: AsyncSession, as_of: datetime) -> dict[str, Any]:
    """Invocations, unique users, avg latency (ms), top-1 conversion rate."""
    window_start = as_of - timedelta(days=30)

    invocations = (
        await session.execute(
            select(func.count(SelectorSession.id)).where(
                SelectorSession.created_at >= window_start,
                SelectorSession.created_at <= as_of,
            )
        )
    ).scalar_one()
    unique_users = (
        await session.execute(
            select(func.count(func.distinct(SelectorSession.user_id))).where(
                SelectorSession.user_id.is_not(None),
                SelectorSession.created_at >= window_start,
                SelectorSession.created_at <= as_of,
            )
        )
    ).scalar_one()

    # Avg latency — extracted from output envelope if present. We read the
    # latency the provider already stamped into the `output` JSON rather than
    # re-timing here. SelectorOutput.latency_ms is optional — treat missing as 0.
    latency_rows = list(
        (
            await session.execute(
                select(SelectorSession.output).where(
                    SelectorSession.created_at >= window_start,
                    SelectorSession.created_at <= as_of,
                )
            )
        )
        .scalars()
        .all()
    )
    latencies: list[float] = []
    for out in latency_rows:
        if not isinstance(out, dict):
            continue
        val = out.get("latency_ms")
        if isinstance(val, int | float):
            latencies.append(float(val))
    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0.0

    # Top-1 conversion rate — fraction of selector sessions whose *primary*
    # recommendation resulted in an affiliate click within 7d. We identify
    # the primary slug from output.stack[0].slug and join back to clicks on
    # (user_id, card_slug) within the window.
    top1_invocations = 0
    top1_conversions = 0
    # Pre-load card slug → id so we can match output slug against click.card_id.
    card_rows = list(
        (
            await session.execute(
                select(CardModel.id, CardModel.slug)
            )
        ).all()
    )
    slug_to_card_id: dict[str, uuid.UUID] = {slug: cid for cid, slug in card_rows}

    sessions_with_user = list(
        (
            await session.execute(
                select(
                    SelectorSession.user_id,
                    SelectorSession.output,
                    SelectorSession.created_at,
                ).where(
                    SelectorSession.user_id.is_not(None),
                    SelectorSession.created_at >= window_start,
                    SelectorSession.created_at <= as_of,
                )
            )
        ).all()
    )
    for user_id, output, created_at in sessions_with_user:
        if not isinstance(output, dict):
            continue
        stack = output.get("stack") or []
        if not stack:
            continue
        primary = stack[0]
        if not isinstance(primary, dict):
            continue
        slug = primary.get("slug")
        card_id = slug_to_card_id.get(slug) if isinstance(slug, str) else None
        if card_id is None:
            continue
        top1_invocations += 1
        click_count = (
            await session.execute(
                select(func.count(AffiliateClick.click_id)).where(
                    AffiliateClick.user_id == user_id,
                    AffiliateClick.card_id == card_id,
                    AffiliateClick.created_at >= created_at,
                    AffiliateClick.created_at <= created_at + timedelta(days=7),
                )
            )
        ).scalar_one()
        if click_count and int(click_count) > 0:
            top1_conversions += 1

    top1_conv_rate = (
        float(top1_conversions) / float(top1_invocations) if top1_invocations else 0.0
    )

    # Eval recall — read from the latest scheduled run persisted as a SyncRun
    # with source='selector_eval'. Fallback: null when harness hasn't logged yet.
    from loftly.db.models.audit import SyncRun

    latest_eval = (
        await session.execute(
            select(SyncRun)
            .where(SyncRun.source == "selector_eval")
            .order_by(SyncRun.started_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    eval_recall: float | None = None
    if latest_eval is not None and latest_eval.upstream_count > 0:
        eval_recall = float(latest_eval.inserted_count) / float(latest_eval.upstream_count)

    return {
        "window_days": 30,
        "invocations": int(invocations or 0),
        "unique_users": int(unique_users or 0),
        "avg_latency_ms": round(avg_latency_ms, 2),
        "top1_conversion_rate": round(top1_conv_rate, 4),
        "top1_sample_size": int(top1_invocations),
        "eval_top1_recall": (round(eval_recall, 4) if eval_recall is not None else None),
    }


# ---------------------------------------------------------------------------
# Aggregation — affiliate
# ---------------------------------------------------------------------------


async def _affiliate_metrics(session: AsyncSession, as_of: datetime) -> dict[str, Any]:
    window_start = as_of - timedelta(days=30)

    total_clicks = (
        await session.execute(
            select(func.count(AffiliateClick.click_id)).where(
                AffiliateClick.created_at >= window_start,
                AffiliateClick.created_at <= as_of,
            )
        )
    ).scalar_one()
    unique_users_clicked = (
        await session.execute(
            select(func.count(func.distinct(AffiliateClick.user_id))).where(
                AffiliateClick.user_id.is_not(None),
                AffiliateClick.created_at >= window_start,
                AffiliateClick.created_at <= as_of,
            )
        )
    ).scalar_one()
    conversions = (
        await session.execute(
            select(func.count(AffiliateConversion.id)).where(
                AffiliateConversion.received_at >= window_start,
                AffiliateConversion.received_at <= as_of,
            )
        )
    ).scalar_one()
    conv_rate = (
        float(conversions) / float(total_clicks) if total_clicks else 0.0
    )

    # Commission THB by month for the last 6 months.
    monthly_commission: list[dict[str, Any]] = []
    for start, end in _last_n_months(as_of, AFFILIATE_MONTHS):
        amount = (
            await session.execute(
                select(func.coalesce(func.sum(AffiliateConversion.commission_thb), 0)).where(
                    AffiliateConversion.received_at >= start,
                    AffiliateConversion.received_at < end,
                )
            )
        ).scalar_one()
        monthly_commission.append(
            {
                "month_start": start.date().isoformat(),
                "commission_thb": float(amount or 0),
            }
        )

    # Top-5 cards by conversion count.
    top_cards_rows = (
        await session.execute(
            select(
                CardModel.slug,
                func.count(AffiliateConversion.id).label("conv_count"),
                func.coalesce(func.sum(AffiliateConversion.commission_thb), 0).label("commission"),
            )
            .join(AffiliateClick, AffiliateConversion.click_id == AffiliateClick.click_id)
            .join(CardModel, AffiliateClick.card_id == CardModel.id)
            .where(
                AffiliateConversion.received_at >= window_start,
                AffiliateConversion.received_at <= as_of,
            )
            .group_by(CardModel.slug)
            .order_by(func.count(AffiliateConversion.id).desc())
            .limit(TOP_CARD_LIMIT)
        )
    ).all()
    top_cards = [
        {
            "card_slug": slug,
            "conversions": int(cnt or 0),
            "commission_thb": float(comm or 0),
        }
        for slug, cnt, comm in top_cards_rows
    ]

    return {
        "window_days": 30,
        "total_clicks": int(total_clicks or 0),
        "unique_users_clicked": int(unique_users_clicked or 0),
        "conversions": int(conversions or 0),
        "conversion_rate": round(conv_rate, 4),
        "commission_thb_by_month": monthly_commission,
        "top_cards_by_conversions": top_cards,
    }


# ---------------------------------------------------------------------------
# Aggregation — content
# ---------------------------------------------------------------------------


async def _content_metrics(session: AsyncSession, as_of: datetime) -> dict[str, Any]:
    published_count = (
        await session.execute(
            select(func.count(Article.id)).where(
                Article.state == "published",
                Article.published_at.is_not(None),
                Article.published_at <= as_of,
            )
        )
    ).scalar_one()
    distinct_cards_covered = (
        await session.execute(
            select(func.count(func.distinct(Article.card_id))).where(
                Article.state == "published",
                Article.card_id.is_not(None),
                Article.published_at.is_not(None),
                Article.published_at <= as_of,
            )
        )
    ).scalar_one()

    # Average days since last update for published articles.
    rows = list(
        (
            await session.execute(
                select(Article.updated_at).where(
                    Article.state == "published",
                    Article.published_at.is_not(None),
                    Article.published_at <= as_of,
                )
            )
        )
        .scalars()
        .all()
    )
    ages_days: list[float] = []
    for updated_at in rows:
        if updated_at is None:
            continue
        delta = as_of - _to_utc(updated_at)
        ages_days.append(max(delta.total_seconds() / 86_400.0, 0.0))
    avg_age_days = sum(ages_days) / len(ages_days) if ages_days else 0.0

    # schema.org Review validation rate — proxy by checking whether published
    # card-review articles have an seo_meta["schema_review_valid"]=True flag.
    # Absent flag = not validated. This will become a live validator later.
    review_total = 0
    review_valid = 0
    reviews = list(
        (
            await session.execute(
                select(Article.seo_meta).where(
                    Article.state == "published",
                    Article.article_type == "card_review",
                    Article.published_at.is_not(None),
                    Article.published_at <= as_of,
                )
            )
        )
        .scalars()
        .all()
    )
    for meta in reviews:
        review_total += 1
        if isinstance(meta, dict) and meta.get("schema_review_valid") is True:
            review_valid += 1
    review_validation_rate = (
        float(review_valid) / float(review_total) if review_total else 0.0
    )

    return {
        "articles_published": int(published_count or 0),
        "distinct_cards_covered": int(distinct_cards_covered or 0),
        "avg_update_age_days": round(avg_age_days, 2),
        "schema_review_validation_rate": round(review_validation_rate, 4),
    }


# ---------------------------------------------------------------------------
# Aggregation — LLM + system (placeholder; Langfuse / Grafana TBD)
# ---------------------------------------------------------------------------


def _llm_cost_metrics(mau: int) -> dict[str, Any]:
    """Placeholder: Langfuse pricing ledger not yet wired in prod.

    Schema slot is stable so the real values drop in without a migration to
    downstream dashboards. Numbers default to 0.0 / null so investors can tell
    the difference between "zero spend" and "not yet measured".
    """
    total_thb: float = 0.0
    spend_per_mau = (total_thb / float(mau)) if mau else 0.0
    return {
        "window_days": 30,
        "total_spend_thb": total_thb,
        "spend_per_mau_thb": round(spend_per_mau, 4),
        "prompt_cache_hit_rate": None,
        "haiku_fallback_rate": None,
        "source": "placeholder — wire Langfuse pricing ledger",
    }


def _system_metrics() -> dict[str, Any]:
    """Placeholder: Grafana Cloud / Fly metrics not yet scraped into DB.

    Same pattern as LLM — stable slot, explicit null so readers see it's
    pending.
    """
    return {
        "window_days": SYSTEM_WINDOW_DAYS,
        "uptime_staging_pct": None,
        "uptime_prod_pct": None,
        "http_5xx_rate": None,
        "p95_request_latency_ms": None,
        "source": "placeholder — wire Grafana/Fly metrics scrape",
    }


# ---------------------------------------------------------------------------
# Build + entrypoint
# ---------------------------------------------------------------------------


async def build_export(as_of: datetime) -> dict[str, Any]:
    """Assemble the full anonymized metrics payload.

    Kept as a pure function (vs `run_export`) so the HTTP route can return the
    JSON inline without going through the filesystem.
    """
    as_of_utc = as_of.astimezone(UTC) if as_of.tzinfo else as_of.replace(tzinfo=UTC)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        users = await _user_metrics(session, as_of_utc)
        selector = await _selector_metrics(session, as_of_utc)
        affiliate = await _affiliate_metrics(session, as_of_utc)
        content = await _content_metrics(session, as_of_utc)

    llm_costs = _llm_cost_metrics(users["mau"])
    system = _system_metrics()

    return {
        "schema_version": EXPORT_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "as_of": as_of_utc.isoformat(),
        "window_days": SYSTEM_WINDOW_DAYS,
        "users": users,
        "selector": selector,
        "affiliate": affiliate,
        "content": content,
        "llm_costs": llm_costs,
        "system": system,
        "disclaimers": [
            "All numbers derived from aggregate staging + prod telemetry at as_of.",
            "No PII (emails, user UUIDs, IP/UA hashes, click IDs) is serialized.",
            "LLM + system slots marked 'placeholder' will fill in once Langfuse"
            " and Grafana scrapes land — schema is forward-compatible.",
        ],
    }


async def run_export(out_path: str, as_of: datetime) -> dict[str, Any]:
    """Build the payload and write it to `out_path` as pretty JSON.

    Returns the payload so callers can chain on it without re-reading the file.
    """
    payload = await build_export(as_of)
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(
        "metrics_export_written",
        path=str(path),
        as_of=payload["as_of"],
        bytes=path.stat().st_size,
    )
    return payload


__all__ = [
    "EXPORT_SCHEMA_VERSION",
    "build_export",
    "run_export",
]
