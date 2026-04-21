"""Prometheus `/metrics` exporter — contract + instrumentation hooks.

Covers the W13 metric-name contract in `mvp/artifacts/grafana/README.md`:
text-format response, scrape-token guard, request-flow instrumentation, DB
pool gauges, consent observer, DSAR observer.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import select

from loftly.api.app import create_app
from loftly.core.settings import get_settings
from loftly.db.engine import get_engine, get_sessionmaker
from loftly.db.models import Base
from loftly.db.models.affiliate import (
    AffiliateClick,
    AffiliateLink,
)
from loftly.db.models.card import Card as CardModel
from loftly.observability import prometheus as prom

_PARTNER = "test-partner"
_SECRET = "shhh-test-secret"


@pytest.fixture(autouse=True)
def _reset_registry_between_tests() -> None:
    """Each test sees a fresh Prometheus registry — counters don't leak."""
    prom.reset_registry()


@pytest.fixture(autouse=True)
def _clear_scrape_token() -> None:
    """Ensure a stale env var from a previous test doesn't leak in."""
    prior = os.environ.pop("LOFTLY_METRICS_SCRAPE_TOKEN", None)
    try:
        yield
    finally:
        if prior is not None:
            os.environ["LOFTLY_METRICS_SCRAPE_TOKEN"] = prior


def _metric_by_name(text: str, name: str) -> dict[str, float]:
    """Flatten scrape output → {label_signature: value} for quick lookups.

    `text_string_to_metric_families` strips the `_total` suffix from counter
    family names (per the OpenMetrics spec), so we walk samples and match on
    the sample name, which keeps the `_total` / `_bucket` / `_count` suffixes
    intact.
    """
    out: dict[str, float] = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            # Match on exact sample name, or on the name as a histogram prefix
            # (so callers can ask for `loftly_api_dsar_resolution_days` and
            # receive both `_bucket` and `_count` samples).
            if sample.name == name or sample.name.startswith(f"{name}_"):
                sig_parts = [sample.name]
                for key in sorted(sample.labels):
                    sig_parts.append(f"{key}={sample.labels[key]}")
                out["|".join(sig_parts)] = sample.value
    return out


# --------------------------------------------------------------------------- #
# 1) /metrics basic contract
# --------------------------------------------------------------------------- #


async def test_metrics_endpoint_returns_prometheus_text(
    client: AsyncClient,
) -> None:
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    ctype = resp.headers.get("content-type", "")
    # prometheus_client's CONTENT_TYPE_LATEST starts with "text/plain".
    assert ctype.startswith("text/plain")
    body = resp.text
    # At minimum, the request we just made should appear in the counter.
    assert "loftly_api_http_requests_total" in body
    assert "loftly_api_http_request_duration_seconds" in body
    assert "loftly_api_db_pool_connections_active" in body


# --------------------------------------------------------------------------- #
# 2) Scrape-token guard
# --------------------------------------------------------------------------- #


async def test_metrics_token_guard_rejects_wrong_token_when_configured(
    client: AsyncClient,
) -> None:
    os.environ["LOFTLY_METRICS_SCRAPE_TOKEN"] = "super-secret"
    try:
        resp = await client.get("/metrics?token=wrong")
        assert resp.status_code == 401
        body = resp.json()
        assert body["error"]["code"] == "scrape_token_invalid"
    finally:
        del os.environ["LOFTLY_METRICS_SCRAPE_TOKEN"]


async def test_metrics_token_guard_accepts_correct_token(client: AsyncClient) -> None:
    os.environ["LOFTLY_METRICS_SCRAPE_TOKEN"] = "super-secret"
    try:
        resp = await client.get("/metrics?token=super-secret")
        assert resp.status_code == 200
    finally:
        del os.environ["LOFTLY_METRICS_SCRAPE_TOKEN"]


async def test_metrics_open_in_dev_without_token(client: AsyncClient) -> None:
    # No env var set — dev/test env should allow the scrape.
    resp = await client.get("/metrics")
    assert resp.status_code == 200


async def test_metrics_requires_token_in_prod_even_if_env_unset() -> None:
    """Prod-env scrape without `LOFTLY_METRICS_SCRAPE_TOKEN` must 401.

    Building a dedicated app here instead of using the shared fixture because
    we want a `prod` settings snapshot; the conftest fixture forces `test`.
    """
    os.environ["LOFTLY_ENV"] = "prod"
    # Can't keep the dev-insecure JWT key in prod or settings validator trips.
    os.environ["JWT_SIGNING_KEY"] = "prod-test-key-for-metrics-guard"
    get_settings.cache_clear()
    get_engine.cache_clear()
    try:
        prom.reset_registry()
        app = create_app()
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        try:
            transport = ASGITransport(app=app)  # type: ignore[arg-type]
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                resp = await c.get("/metrics")
                assert resp.status_code == 401
                body = resp.json()
                assert body["error"]["code"] == "scrape_token_required"
        finally:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
            await engine.dispose()
    finally:
        os.environ["LOFTLY_ENV"] = "test"
        os.environ["JWT_SIGNING_KEY"] = "test-secret"
        get_settings.cache_clear()
        get_engine.cache_clear()


# --------------------------------------------------------------------------- #
# 3) Middleware increments request counters
# --------------------------------------------------------------------------- #


async def test_http_requests_total_increments_on_200(client: AsyncClient) -> None:
    # Drive one successful request through the middleware.
    r = await client.get("/healthz")
    assert r.status_code == 200

    scrape = await client.get("/metrics")
    parsed = _metric_by_name(scrape.text, "loftly_api_http_requests_total")

    # Look for the /healthz line specifically. Route label uses the template,
    # which for /healthz is identical to the path.
    keys = [
        k
        for k, v in parsed.items()
        if "route=/healthz" in k and "method=GET" in k and "status_code=200" in k and v > 0
    ]
    assert keys, f"no /healthz 200 counter found in: {list(parsed.keys())}"


async def test_http_request_duration_histogram_observed(client: AsyncClient) -> None:
    await client.get("/healthz")
    scrape = await client.get("/metrics")
    parsed = _metric_by_name(
        scrape.text, "loftly_api_http_request_duration_seconds"
    )
    # The `_count` sample is keyed like "<name>_count|method=GET|route=/healthz".
    count_keys = [
        k
        for k, v in parsed.items()
        if "_count|" in k and "route=/healthz" in k and v > 0
    ]
    assert count_keys, f"no histogram _count for /healthz, got: {list(parsed.keys())}"


async def test_metrics_scrape_not_self_instrumenting(client: AsyncClient) -> None:
    """`/metrics` scrapes must not increment `loftly_api_http_requests_total`."""
    # Seed a baseline by hitting /healthz once.
    await client.get("/healthz")
    before = await client.get("/metrics")
    for _ in range(3):
        await client.get("/metrics")
    after = await client.get("/metrics")

    before_parsed = _metric_by_name(before.text, "loftly_api_http_requests_total")
    after_parsed = _metric_by_name(after.text, "loftly_api_http_requests_total")

    # Look at anything with route=/metrics — should be absent in both.
    before_metrics_rows = [k for k in before_parsed if "route=/metrics" in k]
    after_metrics_rows = [k for k in after_parsed if "route=/metrics" in k]
    assert before_metrics_rows == []
    assert after_metrics_rows == []


# --------------------------------------------------------------------------- #
# 4) DB pool gauges plausible
# --------------------------------------------------------------------------- #


async def test_db_pool_gauges_populated_after_snapshot(client: AsyncClient) -> None:
    # Force an explicit snapshot — startup loop is skipped in the test env
    # (see app.py) so we drive it by hand.
    prom.db_pool_gauge_snapshot(get_engine())

    resp = await client.get("/metrics")
    parsed_active = _metric_by_name(resp.text, "loftly_api_db_pool_connections_active")
    parsed_idle = _metric_by_name(resp.text, "loftly_api_db_pool_connections_idle")
    parsed_max = _metric_by_name(resp.text, "loftly_api_db_pool_connections_max")

    assert parsed_active, "active gauge missing from /metrics output"
    assert parsed_idle, "idle gauge missing from /metrics output"
    assert parsed_max, "max gauge missing from /metrics output"

    # Gauges are all non-negative numbers — SQLite StaticPool in tests may
    # report zero for everything but MUST NOT crash or be None.
    for family in (parsed_active, parsed_idle, parsed_max):
        for v in family.values():
            assert v >= 0


# --------------------------------------------------------------------------- #
# 5) consent_observer increments correct labels
# --------------------------------------------------------------------------- #


async def test_consent_grant_increments_granted_counter(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.post(
        "/v1/consent",
        json={
            "purpose": "marketing",
            "granted": True,
            "policy_version": "2026-04-01",
            "source": "account_settings",
        },
    )
    assert resp.status_code == 200

    scrape = await seeded_client.get("/metrics")
    granted = _metric_by_name(scrape.text, "loftly_api_consent_granted_total")
    withdrawn = _metric_by_name(scrape.text, "loftly_api_consent_withdrawn_total")

    # Exactly one grant event for `marketing` should have landed.
    grant_hits = [v for k, v in granted.items() if "purpose=marketing" in k]
    assert grant_hits and grant_hits[0] >= 1
    # No withdrawal for `marketing`.
    withdraw_hits = [v for k, v in withdrawn.items() if "purpose=marketing" in k]
    assert not withdraw_hits or withdraw_hits[0] == 0


async def test_consent_withdraw_increments_withdrawn_counter(
    seeded_client: AsyncClient,
) -> None:
    # `optimization=False` is blocked by SPEC; use `analytics` for the
    # withdrawal path.
    resp = await seeded_client.post(
        "/v1/consent",
        json={
            "purpose": "analytics",
            "granted": False,
            "policy_version": "2026-04-01",
            "source": "account_settings",
        },
    )
    assert resp.status_code == 200

    scrape = await seeded_client.get("/metrics")
    withdrawn = _metric_by_name(scrape.text, "loftly_api_consent_withdrawn_total")
    hits = [v for k, v in withdrawn.items() if "purpose=analytics" in k]
    assert hits and hits[0] >= 1


# --------------------------------------------------------------------------- #
# 6) DSAR observer → histogram on close
# --------------------------------------------------------------------------- #


async def test_dsar_observer_records_histogram_on_close() -> None:
    prom.dsar_observer("delete", "opened")
    # Fake a closure after 3.5 days.
    prom.dsar_observer("delete", "closed", resolution_days=3.5)

    # Scrape via the registry directly; no need for the HTTP round-trip here.
    from prometheus_client import generate_latest

    text = generate_latest(prom.get_registry()).decode("utf-8")

    total = _metric_by_name(text, "loftly_api_dsar_requests_total")
    open_gauge = _metric_by_name(text, "loftly_api_dsar_requests_open")
    hist = _metric_by_name(text, "loftly_api_dsar_resolution_days")

    assert any("type=delete" in k and v >= 1 for k, v in total.items())
    # Net open count after one open + one close should be zero.
    assert any(v == 0 for v in open_gauge.values())
    # Histogram has at least one count for delete. Keys look like
    # "<metric>_count|type=delete" after flattening.
    assert any("_count|" in k and "type=delete" in k and v >= 1 for k, v in hist.items())
    # The 3.5-day value lands in the `le=7.0` bucket.
    assert any(
        "_bucket|" in k and "type=delete" in k and "le=7.0" in k and v >= 1
        for k, v in hist.items()
    )


# --------------------------------------------------------------------------- #
# 7) Affiliate commission observer — via webhook path
# --------------------------------------------------------------------------- #


def _sign(body: bytes) -> str:
    digest = hmac.new(_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


@pytest_asyncio.fixture
async def seeded_click(seeded_db: object) -> AsyncIterator[dict[str, Any]]:
    _ = seeded_db
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card_id = (
            await session.execute(select(CardModel.id).where(CardModel.slug == "kbank-wisdom"))
        ).scalar_one()
        link = AffiliateLink(
            card_id=card_id,
            partner_id=_PARTNER,
            url_template="https://partner.example.com/apply?cid={click_id}",
            commission_model="cpa_approved",
            active=True,
        )
        session.add(link)
        await session.flush()
        click_id = uuid.uuid4()
        session.add(
            AffiliateClick(
                click_id=click_id,
                affiliate_link_id=link.id,
                card_id=card_id,
                partner_id=_PARTNER,
                placement="review",
            )
        )
        await session.commit()
    yield {"click_id": click_id, "card_id": card_id}


async def test_affiliate_revenue_counter_increments_on_confirmed(
    seeded_client: AsyncClient, seeded_click: dict[str, Any]
) -> None:
    payload = {
        "click_id": str(seeded_click["click_id"]),
        "event": "application_approved",
        "event_at": "2026-04-21T10:00:00Z",
        "commission_thb": 750.0,
    }
    body = json.dumps(payload).encode("utf-8")
    resp = await seeded_client.post(
        f"/v1/webhooks/affiliate/{_PARTNER}",
        content=body,
        headers={"X-Loftly-Signature": _sign(body), "Content-Type": "application/json"},
    )
    assert resp.status_code == 204

    scrape = await seeded_client.get("/metrics")
    revenue = _metric_by_name(
        scrape.text, "loftly_api_affiliate_revenue_thb_total"
    )
    hits = [v for k, v in revenue.items() if f"partner_id={_PARTNER}" in k]
    assert hits and hits[0] == pytest.approx(750.0)


# --------------------------------------------------------------------------- #
# 8) DSAR opened when a data-export is requested
# --------------------------------------------------------------------------- #


async def test_data_export_request_increments_dsar_opened(
    seeded_client: AsyncClient,
) -> None:
    # The TEST_USER_ID is seeded in conftest — auth is dep-overridden.
    # Ensure no leftover rate-limiter budget from a previous test.
    from loftly.api.routes.account import DATA_EXPORT_LIMITER

    DATA_EXPORT_LIMITER.reset()

    resp = await seeded_client.post("/v1/account/data-export/request")
    assert resp.status_code == 202

    scrape = await seeded_client.get("/metrics")
    total = _metric_by_name(scrape.text, "loftly_api_dsar_requests_total")
    hits = [v for k, v in total.items() if "type=export" in k]
    assert hits and hits[0] >= 1
