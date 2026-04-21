"""Langfuse cost-anomaly check — `/v1/internal/cost-anomaly-check` + job logic.

Covers the four contract branches from issue #14:

1. Normal ratio (< 2.0) → 200, no email, no audit row.
2. Anomaly (ratio > 2.0) → 200 + email + audit row `cost.anomaly_detected`.
3. Langfuse not configured (NotImplementedError) → 503 + no audit row (no
   check actually happened).
4. Langfuse unreachable (generic Exception) → 503 + audit row
   `cost.anomaly_check_degraded`.

Plus a stub-mode email assertion that mirrors the style from
`test_content_stale_digest.py`: Resend stub returns `{"id": ...}` and we
validate the payload landed in the fake client.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog
from loftly.jobs.cost_anomaly import (
    ANOMALY_RATIO_THRESHOLD,
    AUDIT_ACTION_ANOMALY_DETECTED,
    AUDIT_ACTION_CHECK_DEGRADED,
    CostAnomalyResult,
    HourlyCostSeries,
    check_cost_anomaly,
)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _make_fetch(series: HourlyCostSeries):
    """Return a `fetch`-compatible coroutine that yields the given series."""

    async def _fake_fetch(*, window_hours: int, now: datetime) -> HourlyCostSeries:
        # The real fetcher requires Langfuse creds; tests inject via `fetch=`
        # so we never touch the SDK. Assertions on window_hours/now kept light
        # — the test cares about the ratio math, not the query shape.
        assert window_hours > 0
        assert now.tzinfo is not None
        return series

    return _fake_fetch


def _make_raising_fetch(exc: BaseException):
    async def _fake_fetch(*, window_hours: int, now: datetime) -> HourlyCostSeries:
        raise exc

    return _fake_fetch


async def _audit_rows_for(action: str) -> list[AuditLog]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(AuditLog).where(AuditLog.action == action)))
            .scalars()
            .all()
        )
    return rows


# --------------------------------------------------------------------------- #
# Auth
# --------------------------------------------------------------------------- #


async def test_requires_internal_api_key(seeded_client: AsyncClient) -> None:
    """Missing X-API-Key → 401. Mirrors the guard used by the stale-digest route."""
    resp = await seeded_client.get("/v1/internal/cost-anomaly-check")
    assert resp.status_code == 401


# --------------------------------------------------------------------------- #
# Ratio math
# --------------------------------------------------------------------------- #


async def test_normal_ratio_no_alert(seeded_db: object) -> None:
    """Ratio below threshold → no audit row, no email."""
    # 24 trailing hours at USD 1.0 each, current hour at USD 1.5 → ratio 1.5.
    series = HourlyCostSeries(
        current_hour_usd=1.5,
        trailing_hours_usd=[1.0] * 24,
    )
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await check_cost_anomaly(
            session,
            fetch=_make_fetch(series),
            now=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        )

    assert result.is_anomaly is False
    assert result.degraded is False
    assert result.email_sent is False
    assert result.ratio == pytest.approx(1.5)
    assert result.current_hour_usd == pytest.approx(1.5)
    assert result.trailing_24h_mean == pytest.approx(1.0)

    # Neither audit action should have landed.
    assert await _audit_rows_for(AUDIT_ACTION_ANOMALY_DETECTED) == []
    assert await _audit_rows_for(AUDIT_ACTION_CHECK_DEGRADED) == []


async def test_anomaly_fires_email_and_audit(
    seeded_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ratio above threshold + Resend configured → email + audit row."""
    monkeypatch.setenv("RESEND_API_KEY", "fake-key")
    get_settings.cache_clear()

    import resend  # type: ignore[import-untyped]

    sent: list[dict[str, object]] = []

    class _FakeEmails:
        @staticmethod
        def send(payload: dict[str, object]) -> dict[str, object]:
            sent.append(payload)
            return {"id": "email_cost_anomaly_123"}

    monkeypatch.setattr(resend, "Emails", _FakeEmails)

    # Current hour 8.0 USD, trailing 24h at 1.0 USD each → ratio 8.0.
    series = HourlyCostSeries(
        current_hour_usd=8.0,
        trailing_hours_usd=[1.0] * 24,
    )
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await check_cost_anomaly(
            session,
            fetch=_make_fetch(series),
            now=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        )

    assert result.is_anomaly is True
    assert result.degraded is False
    assert result.email_sent is True
    assert result.ratio == pytest.approx(8.0)
    assert result.ratio > ANOMALY_RATIO_THRESHOLD

    # Email payload sanity — Thai-first, mentions the ratio, goes to founder.
    assert len(sent) == 1
    payload = sent[0]
    assert payload["to"] == [get_settings().founder_notify_email]
    subject = str(payload["subject"])
    assert "anomaly" in subject.lower()
    assert "8.0" in subject
    text_body = str(payload["text"])
    # Thai block must appear before the English block.
    th_idx = text_body.find("สวัสดี")
    en_idx = text_body.find("Hourly LLM spend")
    assert th_idx != -1 and en_idx != -1
    assert th_idx < en_idx, "Thai block must precede English block"
    assert "2.00" in text_body  # threshold callout

    # Audit row landed with the computed metadata.
    rows = await _audit_rows_for(AUDIT_ACTION_ANOMALY_DETECTED)
    assert len(rows) == 1
    meta = rows[0].meta
    assert meta["ratio"] == pytest.approx(8.0)
    assert meta["email_sent"] is True
    assert meta["threshold"] == ANOMALY_RATIO_THRESHOLD
    assert meta["window_hours"] == 24


async def test_anomaly_route_returns_200_with_payload(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end HTTP: anomaly → 200 + JSON body with is_anomaly=true."""
    monkeypatch.setenv("RESEND_API_KEY", "fake-key")
    get_settings.cache_clear()

    import resend  # type: ignore[import-untyped]

    class _FakeEmails:
        @staticmethod
        def send(payload: dict[str, object]) -> dict[str, object]:
            return {"id": "email_route_id"}

    monkeypatch.setattr(resend, "Emails", _FakeEmails)

    # Patch the fetcher used by the route — same trick as `fetch=` but applied
    # at module scope because the route doesn't thread the kwarg through.
    from loftly.jobs import cost_anomaly as mod

    async def _fake_fetch(*, window_hours: int, now: datetime) -> HourlyCostSeries:
        return HourlyCostSeries(
            current_hour_usd=10.0,
            trailing_hours_usd=[2.0] * 24,
        )

    monkeypatch.setattr(mod, "_fetch_hourly_costs", _fake_fetch)

    api_key = os.environ["JWT_SIGNING_KEY"]
    resp = await seeded_client.get(
        "/v1/internal/cost-anomaly-check",
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["is_anomaly"] is True
    assert body["ratio"] == pytest.approx(5.0)
    assert body["email_sent"] is True
    assert body["degraded"] is False


async def test_normal_ratio_route_returns_200_no_email(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end HTTP: below threshold → 200, is_anomaly=false, no email.

    Also verifies the Resend stub path: when `RESEND_API_KEY` is unset the
    email sender short-circuits without hitting the network, returning the
    stub-mode 202-ish behavior. For cost-anomaly, normal ratio means the
    email path is never reached at all, so no call into send_email is made.
    """
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    get_settings.cache_clear()

    from loftly.jobs import cost_anomaly as mod

    async def _fake_fetch(*, window_hours: int, now: datetime) -> HourlyCostSeries:
        return HourlyCostSeries(
            current_hour_usd=1.2,
            trailing_hours_usd=[1.0] * 24,
        )

    monkeypatch.setattr(mod, "_fetch_hourly_costs", _fake_fetch)

    api_key = os.environ["JWT_SIGNING_KEY"]
    resp = await seeded_client.get(
        "/v1/internal/cost-anomaly-check",
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["is_anomaly"] is False
    assert body["ratio"] == pytest.approx(1.2)
    assert body["email_sent"] is False
    assert body["degraded"] is False


# --------------------------------------------------------------------------- #
# Degraded paths
# --------------------------------------------------------------------------- #


async def test_langfuse_not_configured_returns_503(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing LANGFUSE_SECRET_KEY → 503, no audit row (the check never ran)."""
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_HOST", raising=False)
    get_settings.cache_clear()

    api_key = os.environ["JWT_SIGNING_KEY"]
    resp = await seeded_client.get(
        "/v1/internal/cost-anomaly-check",
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["degraded"] is True
    assert body["skip_reason"] == "langfuse_not_configured"
    assert body["is_anomaly"] is False

    # No audit row — the "not configured" path is silent beyond the 503.
    assert await _audit_rows_for(AUDIT_ACTION_CHECK_DEGRADED) == []
    assert await _audit_rows_for(AUDIT_ACTION_ANOMALY_DETECTED) == []


async def test_langfuse_unreachable_returns_503_and_audits(
    seeded_db: object,
) -> None:
    """Generic exception from the fetcher → 503 + audit row."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result: CostAnomalyResult = await check_cost_anomaly(
            session,
            fetch=_make_raising_fetch(RuntimeError("connect timeout")),
            now=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        )

    assert result.degraded is True
    assert result.is_anomaly is False
    assert result.skip_reason == "langfuse_unreachable"

    rows = await _audit_rows_for(AUDIT_ACTION_CHECK_DEGRADED)
    assert len(rows) == 1
    meta = rows[0].meta
    assert meta["skip_reason"] == "langfuse_unreachable"
    assert "connect timeout" in meta["error"]


# --------------------------------------------------------------------------- #
# Email stub mode
# --------------------------------------------------------------------------- #


async def test_email_stub_mode_still_audits(
    seeded_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Anomaly + Resend unconfigured → audit row lands, email_sent=False.

    Mirrors the content-stale-digest test that asserts the audit row still
    exists in stub mode so ops has a record the check detected something.
    """
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    get_settings.cache_clear()

    series = HourlyCostSeries(
        current_hour_usd=6.0,
        trailing_hours_usd=[1.0] * 24,
    )
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await check_cost_anomaly(
            session,
            fetch=_make_fetch(series),
            now=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        )

    assert result.is_anomaly is True
    assert result.email_sent is False
    assert result.skip_reason == "resend_disabled"

    rows = await _audit_rows_for(AUDIT_ACTION_ANOMALY_DETECTED)
    assert len(rows) == 1
    meta = rows[0].meta
    assert meta["email_sent"] is False
    assert meta["skip_reason"] == "resend_disabled"
    assert meta["ratio"] == pytest.approx(6.0)


# --------------------------------------------------------------------------- #
# Edge case — zero baseline
# --------------------------------------------------------------------------- #


async def test_zero_baseline_does_not_fire(seeded_db: object) -> None:
    """Trailing mean == 0 → ratio forced to 0; no alert.

    A zero-spend 24h window happens in fresh staging envs + on clean-install
    test boxes. We shouldn't page the founder with `current/0 = inf`.
    """
    series = HourlyCostSeries(
        current_hour_usd=5.0,
        trailing_hours_usd=[0.0] * 24,
    )
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await check_cost_anomaly(
            session,
            fetch=_make_fetch(series),
            now=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
        )

    assert result.is_anomaly is False
    assert result.ratio == pytest.approx(0.0)
    assert await _audit_rows_for(AUDIT_ACTION_ANOMALY_DETECTED) == []
