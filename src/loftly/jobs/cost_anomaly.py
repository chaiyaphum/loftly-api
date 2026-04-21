"""Langfuse cost-anomaly leading-indicator check.

Fires when the last completed hour of LLM spend exceeds 2× the trailing 24-hour
mean. Wired into the hourly scheduler cron (``:05`` past the hour) as a leading
indicator for the rate-limit storm pattern surfaced by DRILL-002 — Langfuse
cost typically spikes 1-2 min before Anthropic starts 429-ing, so alerting on
this ratio gives the founder roughly 60-90 seconds of warning before the
Sentry `APIRateLimitError` alert fires.

Contract
--------
- Langfuse creds unset → ``CostAnomalyResult`` with
  ``degraded=True, skip_reason="langfuse_not_configured"`` → route returns 503.
- Langfuse reachable, ratio ≤ 2.0 → no email, no audit row. Result carries the
  numbers so the cron log still shows the check ran.
- Langfuse reachable, ratio > 2.0 → audit row ``cost.anomaly_detected`` +
  (if Resend configured) a plain-text email to ``FOUNDER_NOTIFY_EMAIL``.
- Langfuse unreachable / errored → ``CostAnomalyResult`` with
  ``degraded=True, skip_reason="langfuse_unreachable"`` + audit row
  ``cost.anomaly_check_degraded`` so the degraded check leaves a trail.

Why THIS ratio
--------------
The `DRILL-002` playbook calls out "hourly spend up 8×" as the diagnostic
signal. 2× is the earliest point at which it is statistically unlikely to be
organic traffic variation (homepage traffic shows roughly ±30% hour-over-hour
noise in our PostHog data; 2× is ~4 sigma above that). Tighter thresholds
(1.5×) trip on normal weekend evening peaks; looser (3×) only fire once the
rate-limit storm is already in progress, which is not "leading".

Email-body language matches the content-stale digest convention — Thai-primary,
English-secondary, plain-text (no HTML template; if we grow past 5 ops emails
we'll switch to jinja per the notes in `notifications/email.py`).
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.audit import log_action

log = get_logger(__name__)

#: Ratio at which we flag an anomaly. See module docstring for the calibration
#: rationale — don't drop it without re-tuning against DRILL-002.
ANOMALY_RATIO_THRESHOLD: float = 2.0

#: Trailing window used to compute the baseline mean. 24h is long enough to
#: wash out hour-of-day seasonality (lunch-time + evening peaks) but short
#: enough that the signal reflects "current traffic regime" rather than last
#: month's average.
TRAILING_WINDOW_HOURS: int = 24

#: Stable system actor for the audit row. Mirrors the convention used by
#: `content_stale_digest.py` and `webhooks.py`.
SYSTEM_USER_ID: uuid.UUID = uuid.UUID("00000000-0000-0000-0000-000000000001")

#: Audit action strings. Two distinct values so the audit query can answer
#: "how many times did we detect?" separately from "how many times did the
#: check run at all?" (degraded checks still leave a trail).
AUDIT_ACTION_ANOMALY_DETECTED: str = "cost.anomaly_detected"
AUDIT_ACTION_CHECK_DEGRADED: str = "cost.anomaly_check_degraded"


@dataclass(frozen=True)
class CostAnomalyResult:
    """Return shape for `check_cost_anomaly`.

    Structured so the HTTP route can map cleanly to status codes without
    re-parsing strings:

    - ``degraded=True``    → 503
    - ``is_anomaly=True``  → 200 + email fired
    - else                 → 200, numbers only
    """

    current_hour_usd: float
    trailing_24h_mean: float
    ratio: float
    is_anomaly: bool
    degraded: bool
    email_sent: bool
    skip_reason: str | None
    duration_ms: float

    def to_log_dict(self) -> dict[str, Any]:
        return {
            "current_hour_usd": round(self.current_hour_usd, 6),
            "trailing_24h_mean": round(self.trailing_24h_mean, 6),
            "ratio": round(self.ratio, 4),
            "is_anomaly": self.is_anomaly,
            "degraded": self.degraded,
            "email_sent": self.email_sent,
            "skip_reason": self.skip_reason,
            "duration_ms": round(self.duration_ms, 2),
        }


@dataclass(frozen=True)
class HourlyCostSeries:
    """Projection of per-hour totals returned by the Langfuse metrics API.

    ``hours`` is ordered oldest → newest. ``current_hour_usd`` is the sum for
    the most recent completed hour (i.e. excludes the in-flight hour so the
    denominator and numerator are apples-to-apples).
    """

    current_hour_usd: float
    trailing_hours_usd: list[float]

    @property
    def trailing_mean(self) -> float:
        if not self.trailing_hours_usd:
            return 0.0
        return sum(self.trailing_hours_usd) / len(self.trailing_hours_usd)


async def _fetch_hourly_costs(
    *,
    window_hours: int,
    now: datetime,
) -> HourlyCostSeries:
    """Hit the Langfuse metrics API and bucket costs into hourly totals.

    Raises ``NotImplementedError`` when LANGFUSE_SECRET_KEY / LANGFUSE_HOST
    are unset — caller converts that into the "not configured" degraded path.
    Raises other exceptions on network / auth errors — caller converts into
    the "unreachable" degraded path.

    Wires live when LANGFUSE_SECRET_KEY + LANGFUSE_PUBLIC_KEY + LANGFUSE_HOST
    are set. The public_key is reused from the same Langfuse project — we
    accept it via an env var documented in DEPLOYMENT.md.
    """
    from loftly.core.settings import get_settings

    settings = get_settings()
    if not settings.langfuse_secret_key or not settings.langfuse_host:
        raise NotImplementedError("langfuse_not_configured")

    # `public_key` is required by the SDK for auth alongside the secret key.
    # We add a dedicated setting rather than scraping from langfuse_host.
    public_key = getattr(settings, "langfuse_public_key", None)
    if not public_key:
        raise NotImplementedError("langfuse_public_key_missing")

    # `now` is the reference moment. The "current" hour is the last completed
    # hour: [floor(now)-1h, floor(now)). The trailing window is the 24 hours
    # before that. We over-fetch by 1h to guarantee the boundary hour is
    # included after Langfuse bucket rounding.
    top_of_hour = now.replace(minute=0, second=0, microsecond=0)
    current_start = top_of_hour - timedelta(hours=1)
    window_start = current_start - timedelta(hours=window_hours)

    from langfuse import Langfuse

    client = Langfuse(
        secret_key=settings.langfuse_secret_key,
        public_key=public_key,
        host=settings.langfuse_host,
    )

    # Langfuse metrics query: totalCost per hour bucket within the window.
    # Format documented at https://langfuse.com/docs/analytics/metrics-api.
    import json as _json

    query = _json.dumps(
        {
            "view": "observations",
            "dimensions": [],
            "metrics": [{"measure": "totalCost", "aggregation": "sum"}],
            "timeDimension": {"granularity": "hour"},
            "fromTimestamp": window_start.isoformat().replace("+00:00", "Z"),
            "toTimestamp": top_of_hour.isoformat().replace("+00:00", "Z"),
        }
    )
    response = client.api.metrics.metrics(query=query)
    rows: list[dict[str, Any]] = list(response.data or [])

    # Normalize into {hour_start_iso: usd}. Langfuse uses `time_dimension` as
    # the key (the Python client returns dicts as-is from the API).
    by_hour: dict[str, float] = {}
    for row in rows:
        # Accept both snake_case and camelCase — the API has shipped both.
        ts = row.get("time_dimension") or row.get("timeDimension") or row.get("time")
        total = row.get("sum_totalCost") or row.get("totalCost") or row.get("sum_total_cost")
        if ts is None or total is None:
            continue
        by_hour[str(ts)] = float(total)

    # Pull the `current` hour specifically; everything else is trailing.
    current_key = current_start.isoformat().replace("+00:00", "Z")
    current_hour_usd = by_hour.pop(current_key, 0.0)
    # Sort trailing hours oldest → newest so the caller's denominator is
    # deterministic regardless of API response ordering.
    trailing = [by_hour[k] for k in sorted(by_hour.keys())]

    return HourlyCostSeries(
        current_hour_usd=current_hour_usd,
        trailing_hours_usd=trailing,
    )


def _render_email_body(
    *,
    current_hour_usd: float,
    trailing_mean: float,
    ratio: float,
    now: datetime,
) -> tuple[str, str]:
    """Return ``(subject, body)`` for the anomaly alert. Thai-primary."""
    subject = (
        f"[Loftly] LLM cost anomaly — hourly spend {ratio:.1f}× trailing 24h mean"
    )
    lines: list[str] = []
    # --- Thai block ---
    lines.append("สวัสดีค่ะ")
    lines.append("")
    lines.append(
        "ตรวจพบการใช้จ่าย LLM ชั่วโมงล่าสุดสูงกว่าค่าเฉลี่ย 24 ชั่วโมงที่ผ่านมาอย่างมีนัย — "
        "นี่เป็นสัญญาณเตือนล่วงหน้าของ rate-limit storm ตาม DRILL-002"
    )
    lines.append("")
    lines.append(f"ชั่วโมงปัจจุบัน: USD {current_hour_usd:.4f}")
    lines.append(f"ค่าเฉลี่ย 24 ชม.:  USD {trailing_mean:.4f}")
    lines.append(f"อัตราส่วน:        {ratio:.2f}×  (threshold 2.00×)")
    lines.append(f"เวลา (UTC):       {now.isoformat()}")
    lines.append("")
    lines.append(
        "เปิด Langfuse dashboard + Sentry `Anthropic.APIRateLimitError` filter "
        "เพื่อตัดสินใจว่าจะ flip kill-switch หรือปล่อยให้ fallback จัดการ"
    )
    lines.append("")
    # --- English block ---
    lines.append("---")
    lines.append("")
    lines.append(
        "Hourly LLM spend exceeded the 24h trailing mean — a leading indicator "
        "for the Anthropic rate-limit storm pattern (see DRILL-002)."
    )
    lines.append("")
    lines.append(f"  current hour:      USD {current_hour_usd:.4f}")
    lines.append(f"  trailing 24h mean: USD {trailing_mean:.4f}")
    lines.append(f"  ratio:             {ratio:.2f}x (threshold 2.00x)")
    lines.append(f"  reference (UTC):   {now.isoformat()}")
    lines.append("")
    lines.append(
        "Open the Langfuse cost dashboard and Sentry `Anthropic.APIRateLimitError` "
        "filter to decide between kill-switch vs. relying on the Haiku fallback."
    )
    lines.append("")
    lines.append("— Loftly ops")
    return subject, "\n".join(lines)


async def _send_alert_email(
    *,
    current_hour_usd: float,
    trailing_mean: float,
    ratio: float,
    now: datetime,
) -> tuple[bool, str | None]:
    """Send the alert. Returns ``(email_sent, skip_reason_or_none)``.

    Stub mode (no RESEND_API_KEY) returns ``(False, "resend_disabled")`` so
    the caller can still record the detection in the audit log.
    """
    from loftly.core.settings import get_settings
    from loftly.notifications.email import send_email

    settings = get_settings()
    subject, body = _render_email_body(
        current_hour_usd=current_hour_usd,
        trailing_mean=trailing_mean,
        ratio=ratio,
        now=now,
    )
    if not settings.resend_api_key:
        log.info(
            "cost_anomaly_email_stub",
            ratio=round(ratio, 4),
            current_hour_usd=round(current_hour_usd, 6),
        )
        return False, "resend_disabled"
    message_id = await send_email(
        to=settings.founder_notify_email,
        subject=subject,
        text=body,
    )
    if message_id is None:
        return False, "resend_returned_no_id"
    return True, None


async def check_cost_anomaly(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    window_hours: int = TRAILING_WINDOW_HOURS,
    threshold: float = ANOMALY_RATIO_THRESHOLD,
    fetch: Any = None,
) -> CostAnomalyResult:
    """Entry point for the internal route + tests.

    Parameters mirror `run_digest` — session in (so audit rows share the
    enclosing transaction), `now` overridable for deterministic tests,
    `fetch` overridable to stub out the Langfuse call in unit tests without
    monkey-patching import machinery.
    """
    t0 = time.perf_counter()
    reference = now if now is not None else datetime.now(UTC)
    series_fetcher = fetch if fetch is not None else _fetch_hourly_costs

    # Declared up-front so the two branches (degraded / normal) share a single
    # scope for mypy — assigning in-branch would narrow differently per path.
    skip_reason: str | None = None

    try:
        series = await series_fetcher(window_hours=window_hours, now=reference)
    except NotImplementedError as exc:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        skip_reason = str(exc) or "langfuse_not_configured"
        log.info("cost_anomaly_skipped", reason=skip_reason)
        # No audit row here — the check never really ran. The 503 itself is
        # the signal.
        return CostAnomalyResult(
            current_hour_usd=0.0,
            trailing_24h_mean=0.0,
            ratio=0.0,
            is_anomaly=False,
            degraded=True,
            email_sent=False,
            skip_reason=skip_reason,
            duration_ms=duration_ms,
        )
    except Exception as exc:
        duration_ms = (time.perf_counter() - t0) * 1000.0
        skip_reason = "langfuse_unreachable"
        log.warning(
            "cost_anomaly_degraded",
            reason=skip_reason,
            error=str(exc)[:200],
        )
        # Audit the degraded check so ops has evidence the cron fired and
        # couldn't reach Langfuse. This is a DIFFERENT action string from the
        # "detected" row so audit queries stay clean.
        await log_action(
            session,
            actor_id=SYSTEM_USER_ID,
            action=AUDIT_ACTION_CHECK_DEGRADED,
            subject_type="observability",
            subject_id=None,
            metadata={
                "skip_reason": skip_reason,
                "error": str(exc)[:200],
                "threshold": threshold,
                "window_hours": window_hours,
                "reference_at": reference.isoformat(),
            },
        )
        await session.commit()
        return CostAnomalyResult(
            current_hour_usd=0.0,
            trailing_24h_mean=0.0,
            ratio=0.0,
            is_anomaly=False,
            degraded=True,
            email_sent=False,
            skip_reason=skip_reason,
            duration_ms=duration_ms,
        )

    mean = series.trailing_mean
    # `ratio` is defined as current/mean. If mean is zero we treat the ratio
    # as 0 (not infinity) — a zero-spend baseline doesn't constitute an
    # anomaly; we just don't have enough data. The daily burn-rate panel on
    # the Grafana dashboard handles the "spend is zero forever" case.
    ratio = (series.current_hour_usd / mean) if mean > 0 else 0.0
    is_anomaly = ratio > threshold

    email_sent = False

    if is_anomaly:
        email_sent, skip_reason = await _send_alert_email(
            current_hour_usd=series.current_hour_usd,
            trailing_mean=mean,
            ratio=ratio,
            now=reference,
        )
        # Always audit the detection — email may have been skipped for
        # Resend-unavailability reasons but the observation itself is real.
        await log_action(
            session,
            actor_id=SYSTEM_USER_ID,
            action=AUDIT_ACTION_ANOMALY_DETECTED,
            subject_type="observability",
            subject_id=None,
            metadata={
                "current_hour_usd": round(series.current_hour_usd, 6),
                "trailing_24h_mean": round(mean, 6),
                "ratio": round(ratio, 4),
                "threshold": threshold,
                "window_hours": window_hours,
                "email_sent": email_sent,
                "skip_reason": skip_reason,
                "reference_at": reference.isoformat(),
            },
        )
        await session.commit()

    duration_ms = (time.perf_counter() - t0) * 1000.0
    result = CostAnomalyResult(
        current_hour_usd=series.current_hour_usd,
        trailing_24h_mean=mean,
        ratio=ratio,
        is_anomaly=is_anomaly,
        degraded=False,
        email_sent=email_sent,
        skip_reason=skip_reason,
        duration_ms=duration_ms,
    )
    log.info("cost_anomaly_check", **result.to_log_dict())
    return result


__all__ = [
    "ANOMALY_RATIO_THRESHOLD",
    "AUDIT_ACTION_ANOMALY_DETECTED",
    "AUDIT_ACTION_CHECK_DEGRADED",
    "SYSTEM_USER_ID",
    "TRAILING_WINDOW_HOURS",
    "CostAnomalyResult",
    "HourlyCostSeries",
    "check_cost_anomaly",
]
