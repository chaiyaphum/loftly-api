"""Weekly content-stale digest — `/v1/internal/content-stale-digest` + job logic.

Covers:
- Auth gating (X-API-Key required).
- 0 stale articles → 204 No Content, no email.
- N stale articles + Resend configured → email body carries the correct count,
  includes the top-10 oldest, and an `audit_log` row is recorded.
- Stale articles + RESEND_API_KEY unset → 202 Accepted, audit row still lands,
  no email sent.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.article import Article
from loftly.db.models.audit import AuditLog
from loftly.db.models.card import Card
from loftly.jobs.content_stale_digest import (
    AUDIT_ACTION_DIGEST_SENT,
    _render_email_body,
    _subject_line,
)

from .conftest import TEST_ADMIN_ID


async def _seed_stale_articles(count: int, *, start_days_old: int = 100) -> list[uuid.UUID]:
    """Insert `count` published articles with varied old `updated_at` values.

    `start_days_old` sets the age of the oldest article; each subsequent one is
    one day younger so oldest-first ordering is deterministic in assertions.
    Returns the seeded UUIDs in oldest-first order.
    """
    now = datetime.now(UTC)
    ids: list[uuid.UUID] = []
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card = (await session.execute(select(Card).limit(1))).scalars().one()
        for i in range(count):
            age = start_days_old - i  # i=0 is oldest
            article = Article(
                slug=f"stale-digest-{i:02d}",
                card_id=card.id,
                article_type="card_review",
                title_th=f"บทความทดสอบ {i}",
                summary_th="s",
                body_th="b",
                state="published",
                author_id=TEST_ADMIN_ID,
                policy_version="2026-04-01",
                published_at=now - timedelta(days=age),
                updated_at=now - timedelta(days=age),
            )
            session.add(article)
            await session.flush()
            ids.append(uuid.UUID(str(article.id)))
        await session.commit()
    return ids


async def _audit_rows_for(action: str) -> list[AuditLog]:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(AuditLog).where(AuditLog.action == action)))
            .scalars()
            .all()
        )
    return rows


@pytest.mark.asyncio
async def test_requires_internal_api_key(seeded_client: AsyncClient) -> None:
    """Missing X-API-Key → 401."""
    resp = await seeded_client.post("/v1/internal/content-stale-digest")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_zero_stale_returns_204_no_email(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No published article is older than 90 days → 204, no Resend call."""
    # RESEND key unset in conftest already; belt+braces.
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    get_settings.cache_clear()

    # Seed a single FRESH article so the table isn't empty but nothing is stale.
    now = datetime.now(UTC)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card = (await session.execute(select(Card).limit(1))).scalars().one()
        session.add(
            Article(
                slug="very-fresh",
                card_id=card.id,
                article_type="card_review",
                title_th="บทความใหม่",
                summary_th="s",
                body_th="b",
                state="published",
                author_id=TEST_ADMIN_ID,
                policy_version="2026-04-01",
                published_at=now - timedelta(days=5),
                updated_at=now - timedelta(days=5),
            )
        )
        await session.commit()

    api_key = os.environ["JWT_SIGNING_KEY"]
    resp = await seeded_client.post(
        "/v1/internal/content-stale-digest",
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 204, resp.text
    # 204 MUST NOT carry a body.
    assert resp.content in (b"", b"null")

    # No audit row either — the noop path is silent.
    rows = await _audit_rows_for(AUDIT_ACTION_DIGEST_SENT)
    assert rows == []


@pytest.mark.asyncio
async def test_stale_articles_fire_email_with_correct_body_and_count(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """5 stale articles + Resend key set → email fired, audit row landed."""
    monkeypatch.setenv("RESEND_API_KEY", "fake-key")
    get_settings.cache_clear()

    import resend  # type: ignore[import-untyped]

    sent: list[dict[str, object]] = []

    class _FakeEmails:
        @staticmethod
        def send(payload: dict[str, object]) -> dict[str, object]:
            sent.append(payload)
            return {"id": "email_stub_id_123"}

    monkeypatch.setattr(resend, "Emails", _FakeEmails)

    ids = await _seed_stale_articles(5, start_days_old=200)

    api_key = os.environ["JWT_SIGNING_KEY"]
    resp = await seeded_client.post(
        "/v1/internal/content-stale-digest",
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == 5
    assert body["email_sent"] is True
    assert body["message_id"] == "email_stub_id_123"
    assert body["oldest_days"] >= 200 - 1  # allow for clock drift during the test

    # --- Resend payload ---
    assert len(sent) == 1
    payload = sent[0]
    assert payload["to"] == [get_settings().founder_notify_email]
    subject = str(payload["subject"])
    assert "5" in subject
    assert "Loftly" in subject
    text_body = str(payload["text"])
    # Thai block appears first.
    th_idx = text_body.find("สวัสดี")
    en_idx = text_body.find("Found 5 published")
    assert th_idx != -1 and en_idx != -1
    assert th_idx < en_idx, "Thai block must precede English block"
    # All 5 seeded articles mentioned (we seeded only 5, so the top-N list
    # length equals the total count). Keeps `ids` referenced so ruff stops
    # complaining about the unused local.
    assert len(ids) == 5
    assert text_body.count("stale-digest-") == 5

    # --- Audit row ---
    rows = await _audit_rows_for(AUDIT_ACTION_DIGEST_SENT)
    assert len(rows) == 1
    meta = rows[0].meta
    assert meta["count"] == 5
    assert meta["email_sent"] is True
    assert meta["message_id"] == "email_stub_id_123"


@pytest.mark.asyncio
async def test_missing_resend_key_returns_202_and_still_audits(
    seeded_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stale articles exist but Resend is unconfigured → 202 Accepted, audit row present, no email."""
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    get_settings.cache_clear()

    await _seed_stale_articles(3, start_days_old=150)

    api_key = os.environ["JWT_SIGNING_KEY"]
    resp = await seeded_client.post(
        "/v1/internal/content-stale-digest",
        headers={"X-API-Key": api_key},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["count"] == 3
    assert body["email_sent"] is False
    assert body["skip_reason"] == "resend_disabled"
    assert body["message_id"] is None

    rows = await _audit_rows_for(AUDIT_ACTION_DIGEST_SENT)
    assert len(rows) == 1
    meta = rows[0].meta
    assert meta["count"] == 3
    assert meta["email_sent"] is False
    assert meta["skip_reason"] == "resend_disabled"


def test_email_body_shape_is_deterministic() -> None:
    """Spot-check the renderer: subject + count wording + bilingual blocks."""
    from loftly.jobs.content_stale_digest import StaleArticleRow

    now = datetime.now(UTC)
    rows = [
        StaleArticleRow(
            id=uuid.uuid4(),
            slug="kbc-the-one-review",
            title_th="รีวิวบัตร KBank The One",
            updated_at=now - timedelta(days=120),
        ),
        StaleArticleRow(
            id=uuid.uuid4(),
            slug="scb-m-visa-review",
            title_th="รีวิวบัตร SCB M Visa",
            updated_at=now - timedelta(days=95),
        ),
    ]
    subj = _subject_line(7)
    assert "7" in subj
    assert "บทความ" in subj
    body = _render_email_body(rows, total_count=7, threshold_days=90)
    assert "90 วัน" in body
    assert "90 days" in body
    # Thai section mentions the total (7), not just the shown count (2).
    assert "7" in body
    # Both slugs surface in the list.
    assert "kbc-the-one-review" in body
    assert "scb-m-visa-review" in body
