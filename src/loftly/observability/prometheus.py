"""Prometheus metric registry + emitter helpers.

Contract-of-record: `../loftly/mvp/artifacts/grafana/README.md` §Metric-name
contract. Every metric exposed here is expected by one of the four Grafana
dashboards (api-latency, llm-cost, affiliate, pdpa). Rename or drop with care.

Design notes:
- Use a *dedicated* `CollectorRegistry` rather than the default global. The
  default registry is process-wide singleton state that gets polluted by test
  imports and by Sentry/Langfuse SDKs that piggy-back on it. Keeping our own
  registry means `generate_latest(registry)` produces only Loftly metrics.
- Helpers are thin wrappers around `.labels(...).inc()` / `.observe(...)` so
  callers don't have to know the metric object names — they just describe the
  event. This also makes it trivial to no-op the helper in tests if a module
  is imported without the app bootstrapped.
- Histogram buckets come from the dashboard spec; don't retune without
  updating the Grafana JSON.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from sqlalchemy.ext.asyncio import AsyncEngine

# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

# One registry per process. `get_registry()` is the hook tests use to reset
# state between runs.
_REGISTRY: CollectorRegistry = CollectorRegistry(auto_describe=True)


def get_registry() -> CollectorRegistry:
    return _REGISTRY


def reset_registry() -> None:
    """Rebuild the registry + every metric — test helper only.

    Production never calls this; between pytest cases we need a clean slate so
    counters / histograms don't accumulate across tests.
    """
    global _REGISTRY
    global HTTP_REQUESTS_TOTAL
    global HTTP_REQUEST_DURATION_SECONDS
    global DB_POOL_ACTIVE
    global DB_POOL_IDLE
    global DB_POOL_MAX
    global AFFILIATE_REVENUE_THB_TOTAL
    global USER_CONSENT_COUNT
    global CONSENT_GRANTED_TOTAL
    global CONSENT_WITHDRAWN_TOTAL
    global DSAR_REQUESTS_TOTAL
    global DSAR_REQUESTS_OPEN
    global DSAR_RESOLUTION_DAYS

    _REGISTRY = CollectorRegistry(auto_describe=True)
    _build_metrics()


# --------------------------------------------------------------------------- #
# Metric definitions
# --------------------------------------------------------------------------- #

# Histogram buckets — exactly as specced in
# `mvp/artifacts/grafana/README.md` for request latency and DSAR resolution.
HTTP_LATENCY_BUCKETS: tuple[float, ...] = (
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
)

# DSAR resolution: days-scale, so use a coarser set. Upper bound 30d matches
# the PDPA statutory response window.
DSAR_RESOLUTION_BUCKETS: tuple[float, ...] = (1, 3, 7, 14, 21, 30, 45, 60)

HTTP_REQUESTS_TOTAL: Counter
HTTP_REQUEST_DURATION_SECONDS: Histogram
DB_POOL_ACTIVE: Gauge
DB_POOL_IDLE: Gauge
DB_POOL_MAX: Gauge
AFFILIATE_REVENUE_THB_TOTAL: Counter
USER_CONSENT_COUNT: Gauge
CONSENT_GRANTED_TOTAL: Counter
CONSENT_WITHDRAWN_TOTAL: Counter
DSAR_REQUESTS_TOTAL: Counter
DSAR_REQUESTS_OPEN: Gauge
DSAR_RESOLUTION_DAYS: Histogram


def _build_metrics() -> None:
    """Instantiate every metric against the current `_REGISTRY`.

    Called once at import time + by `reset_registry()` in tests. Kept as a
    function so the global re-assignment is contained.
    """
    global HTTP_REQUESTS_TOTAL
    global HTTP_REQUEST_DURATION_SECONDS
    global DB_POOL_ACTIVE
    global DB_POOL_IDLE
    global DB_POOL_MAX
    global AFFILIATE_REVENUE_THB_TOTAL
    global USER_CONSENT_COUNT
    global CONSENT_GRANTED_TOTAL
    global CONSENT_WITHDRAWN_TOTAL
    global DSAR_REQUESTS_TOTAL
    global DSAR_REQUESTS_OPEN
    global DSAR_RESOLUTION_DAYS

    HTTP_REQUESTS_TOTAL = Counter(
        "loftly_api_http_requests_total",
        "Total HTTP requests served.",
        labelnames=("route", "method", "status_code"),
        registry=_REGISTRY,
    )
    HTTP_REQUEST_DURATION_SECONDS = Histogram(
        "loftly_api_http_request_duration_seconds",
        "HTTP request latency in seconds, observed at middleware boundary.",
        labelnames=("route", "method"),
        buckets=HTTP_LATENCY_BUCKETS,
        registry=_REGISTRY,
    )

    DB_POOL_ACTIVE = Gauge(
        "loftly_api_db_pool_connections_active",
        "Active (checked-out) connections in the SQLAlchemy async pool.",
        registry=_REGISTRY,
    )
    DB_POOL_IDLE = Gauge(
        "loftly_api_db_pool_connections_idle",
        "Idle (checked-in) connections in the SQLAlchemy async pool.",
        registry=_REGISTRY,
    )
    DB_POOL_MAX = Gauge(
        "loftly_api_db_pool_connections_max",
        "Configured max pool size (pool_size + max_overflow).",
        registry=_REGISTRY,
    )

    AFFILIATE_REVENUE_THB_TOTAL = Counter(
        "loftly_api_affiliate_revenue_thb_total",
        "Cumulative affiliate commission in THB, by partner.",
        labelnames=("partner_id",),
        registry=_REGISTRY,
    )

    USER_CONSENT_COUNT = Gauge(
        "loftly_api_user_consent_count",
        "Current count of users with consent granted per purpose.",
        labelnames=("purpose",),
        registry=_REGISTRY,
    )
    CONSENT_GRANTED_TOTAL = Counter(
        "loftly_api_consent_granted_total",
        "Count of consent grant events, by purpose.",
        labelnames=("purpose",),
        registry=_REGISTRY,
    )
    CONSENT_WITHDRAWN_TOTAL = Counter(
        "loftly_api_consent_withdrawn_total",
        "Count of consent withdrawal events, by purpose.",
        labelnames=("purpose",),
        registry=_REGISTRY,
    )

    DSAR_REQUESTS_TOTAL = Counter(
        "loftly_api_dsar_requests_total",
        "Data-subject access requests opened, by type (export/delete).",
        labelnames=("type",),
        registry=_REGISTRY,
    )
    DSAR_REQUESTS_OPEN = Gauge(
        "loftly_api_dsar_requests_open",
        "Currently-open DSAR request count (queued or running).",
        registry=_REGISTRY,
    )
    DSAR_RESOLUTION_DAYS = Histogram(
        "loftly_api_dsar_resolution_days",
        "Days elapsed from DSAR request to resolution.",
        labelnames=("type",),
        buckets=DSAR_RESOLUTION_BUCKETS,
        registry=_REGISTRY,
    )


_build_metrics()


# --------------------------------------------------------------------------- #
# Observer helpers
# --------------------------------------------------------------------------- #


def http_request_observer(
    route: str,
    method: str,
    status_code: int,
    duration_seconds: float,
) -> None:
    """Record one completed HTTP request.

    `route` should be the route *template* (`/v1/cards/{card_id}`), not the
    concrete URL — otherwise the cardinality explodes. The middleware handles
    that fallback; callers here just pass whatever they got.
    """
    HTTP_REQUESTS_TOTAL.labels(route=route, method=method, status_code=str(status_code)).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(route=route, method=method).observe(
        max(0.0, duration_seconds)
    )


def db_pool_gauge_snapshot(engine: AsyncEngine) -> None:
    """Sample the SQLAlchemy async pool and update the three DB-pool gauges.

    Defensive: SQLite/aiosqlite uses `StaticPool` in tests which doesn't
    expose `checkedout()` / `checkedin()`. We fall back to zeros rather than
    crashing the scrape, because metric collection must never take down the
    app.
    """
    pool = engine.pool
    try:
        active = pool.checkedout()  # type: ignore[attr-defined]
    except (AttributeError, NotImplementedError):
        active = 0
    try:
        idle = pool.checkedin()  # type: ignore[attr-defined]
    except (AttributeError, NotImplementedError):
        idle = 0
    try:
        # asyncpg QueuePool exposes size(); StaticPool does not.
        max_size = pool.size() + getattr(pool, "_max_overflow", 0)  # type: ignore[attr-defined]
    except (AttributeError, NotImplementedError, TypeError):
        max_size = active + idle

    DB_POOL_ACTIVE.set(active)
    DB_POOL_IDLE.set(idle)
    DB_POOL_MAX.set(max_size)


def affiliate_commission_observer(partner_id: str, thb: float | Decimal | int) -> None:
    """Bump the affiliate revenue counter for `partner_id` by `thb`.

    Called on every `affiliate_conversion` row insert (webhook path). Amounts
    below zero are dropped (silently) because Prometheus counters MUST be
    monotonic — refund events are rare enough that we prefer to leave the
    reconciliation to the analytics side rather than add a second gauge.
    """
    try:
        amount = float(thb)
    except (TypeError, ValueError):
        return
    if amount <= 0:
        return
    AFFILIATE_REVENUE_THB_TOTAL.labels(partner_id=partner_id).inc(amount)


ConsentAction = Literal["granted", "withdrawn"]


def consent_observer(purpose: str, action: ConsentAction) -> None:
    """Record a consent change event.

    Only the counters move here; the `user_consent_count` gauge is populated
    separately by the periodic snapshot task (see `db_pool_gauge_snapshot`'s
    scheduler — same job). Granted/withdrawn are event counters, not levels.
    """
    if action == "granted":
        CONSENT_GRANTED_TOTAL.labels(purpose=purpose).inc()
    elif action == "withdrawn":
        CONSENT_WITHDRAWN_TOTAL.labels(purpose=purpose).inc()


DsarType = Literal["export", "delete"]
DsarStatus = Literal["opened", "closed"]


def dsar_observer(
    dsar_type: DsarType,
    status: DsarStatus,
    resolution_days: float | None = None,
) -> None:
    """Record a DSAR lifecycle event.

    `status="opened"` → bump the total counter + the open gauge.
    `status="closed"` → decrement the open gauge + (if we know how long it
    took) record the histogram bucket.

    The `resolution_days` argument is ignored on `opened` events. We keep the
    signature union-y rather than splitting into two helpers because most
    callers want a single spot to bind both sides of the lifecycle.
    """
    if status == "opened":
        DSAR_REQUESTS_TOTAL.labels(type=dsar_type).inc()
        DSAR_REQUESTS_OPEN.inc()
    elif status == "closed":
        # Guard against negative drift; clamp at zero. Read the internal
        # `_value` slot — prometheus_client doesn't expose a public getter.
        current = DSAR_REQUESTS_OPEN._value.get()
        if current > 0:
            DSAR_REQUESTS_OPEN.dec()
        if resolution_days is not None and resolution_days >= 0:
            DSAR_RESOLUTION_DAYS.labels(type=dsar_type).observe(resolution_days)


def user_consent_count_set(purpose: str, value: int) -> None:
    """Set the current-consent-granted gauge for `purpose`. Called by snapshot."""
    USER_CONSENT_COUNT.labels(purpose=purpose).set(value)


__all__ = [
    "affiliate_commission_observer",
    "consent_observer",
    "db_pool_gauge_snapshot",
    "dsar_observer",
    "get_registry",
    "http_request_observer",
    "reset_registry",
    "user_consent_count_set",
]
