"""Admin stale-article re-verification — `/v1/admin/articles/stale` +
`/v1/admin/articles/{id}/mark-reviewed`.

Covers:
- auth gating (admin JWT required)
- the 90-day default threshold
- custom `days` threshold
- `state` filter (default `published`)
- `issuer` filter via bank slug
- oldest-first ordering + pagination
- `last_reviewed_by` surfacing from the most-recent `article.reviewed` audit row
- `mark-reviewed` bumps `updated_at` and writes one audit row per call
  (idempotent-on-effect: two calls in <24h still record both audit rows so we
  can reconstruct reviewer activity — per task spec).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.article import Article
from loftly.db.models.audit import AuditLog
from loftly.db.models.card import Card

from .conftest import TEST_ADMIN_ID


async def _seed_articles(
    *,
    now: datetime,
) -> dict[str, uuid.UUID | str]:
    """Insert a small spread of articles with varied updated_at values and
    return {name: article_id}. Uses the first two seeded cards as FK targets.
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        cards = list(
            (await session.execute(select(Card).order_by(Card.slug.asc()).limit(2))).scalars().all()
        )
        card_a = cards[0]
        card_b = cards[1] if len(cards) > 1 else cards[0]
        bank_a_slug = card_a.bank.slug
        bank_b_slug = card_b.bank.slug

        fresh = Article(
            slug="fresh-article",
            card_id=card_a.id,
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
        stale_100 = Article(
            slug="stale-100",
            card_id=card_a.id,
            article_type="card_review",
            title_th="บทความเก่า 100 วัน",
            summary_th="s",
            body_th="b",
            state="published",
            author_id=TEST_ADMIN_ID,
            policy_version="2026-04-01",
            published_at=now - timedelta(days=100),
            updated_at=now - timedelta(days=100),
        )
        stale_200 = Article(
            slug="stale-200",
            card_id=card_b.id,
            article_type="card_review",
            title_th="บทความเก่ามาก",
            summary_th="s",
            body_th="b",
            state="published",
            author_id=TEST_ADMIN_ID,
            policy_version="2026-04-01",
            published_at=now - timedelta(days=200),
            updated_at=now - timedelta(days=200),
        )
        stale_draft = Article(
            slug="stale-draft",
            card_id=card_a.id,
            article_type="card_review",
            title_th="ร่างเก่า",
            summary_th="s",
            body_th="b",
            state="draft",
            author_id=TEST_ADMIN_ID,
            policy_version="2026-04-01",
            published_at=None,
            updated_at=now - timedelta(days=150),
        )

        session.add_all([fresh, stale_100, stale_200, stale_draft])
        await session.commit()
        return {
            "fresh": fresh.id,
            "stale_100": stale_100.id,
            "stale_200": stale_200.id,
            "stale_draft": stale_draft.id,
            "card_a_bank_slug": bank_a_slug,
            "card_b_bank_slug": bank_b_slug,
        }


async def test_stale_requires_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/articles/stale")
    assert resp.status_code == 401


async def test_stale_default_90_day_threshold_returns_oldest_first(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    now = datetime.now(UTC)
    ids = await _seed_articles(now=now)

    resp = await seeded_client.get("/v1/admin/articles/stale", headers=admin_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    returned_ids = [a["id"] for a in body["data"]]
    # 200-day article first, then 100-day. Fresh (5 days) must be absent.
    assert returned_ids[0] == str(ids["stale_200"])
    assert returned_ids[1] == str(ids["stale_100"])
    assert str(ids["fresh"]) not in returned_ids
    # Draft (stale_draft) is filtered out because default state=published.
    assert str(ids["stale_draft"]) not in returned_ids

    assert body["pagination"]["total"] == 2
    assert body["pagination"]["page"] == 1
    assert body["threshold_days"] == 90


async def test_stale_custom_days_threshold(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    now = datetime.now(UTC)
    ids = await _seed_articles(now=now)

    # At 150 days only the 200-day article is stale.
    resp = await seeded_client.get(
        "/v1/admin/articles/stale?days=150",
        headers=admin_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    returned_ids = [a["id"] for a in body["data"]]
    assert returned_ids == [str(ids["stale_200"])]


async def test_stale_state_filter(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    now = datetime.now(UTC)
    ids = await _seed_articles(now=now)

    resp = await seeded_client.get(
        "/v1/admin/articles/stale?state=draft",
        headers=admin_headers,
    )
    assert resp.status_code == 200
    returned = [a["id"] for a in resp.json()["data"]]
    assert str(ids["stale_draft"]) in returned
    assert str(ids["stale_200"]) not in returned


async def test_stale_issuer_filter(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    now = datetime.now(UTC)
    ids = await _seed_articles(now=now)

    issuer_a = ids["card_a_bank_slug"]
    resp = await seeded_client.get(
        f"/v1/admin/articles/stale?issuer={issuer_a}",
        headers=admin_headers,
    )
    assert resp.status_code == 200
    returned = [a["id"] for a in resp.json()["data"]]
    assert str(ids["stale_100"]) in returned


async def test_stale_unknown_issuer_returns_empty(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    now = datetime.now(UTC)
    await _seed_articles(now=now)

    resp = await seeded_client.get(
        "/v1/admin/articles/stale?issuer=not-a-real-bank",
        headers=admin_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"] == []
    assert body["pagination"]["total"] == 0


async def test_stale_invalid_state_422(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.get(
        "/v1/admin/articles/stale?state=wonky",
        headers=admin_headers,
    )
    assert resp.status_code == 422


async def test_stale_pagination_page_size_20(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """Seed 25 stale articles and confirm page 1 returns 20, page 2 returns 5."""
    now = datetime.now(UTC)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        card = (await session.execute(select(Card).limit(1))).scalars().one()
        for i in range(25):
            session.add(
                Article(
                    slug=f"bulk-stale-{i:02d}",
                    card_id=card.id,
                    article_type="card_review",
                    title_th=f"t-{i}",
                    summary_th="s",
                    body_th="b",
                    state="published",
                    author_id=TEST_ADMIN_ID,
                    policy_version="2026-04-01",
                    published_at=now - timedelta(days=100 + i),
                    updated_at=now - timedelta(days=100 + i),
                )
            )
        await session.commit()

    page1 = await seeded_client.get(
        "/v1/admin/articles/stale",
        headers=admin_headers,
    )
    assert page1.status_code == 200
    b1 = page1.json()
    assert len(b1["data"]) == 20
    assert b1["pagination"]["has_more"] is True
    assert b1["pagination"]["total"] == 25

    page2 = await seeded_client.get(
        "/v1/admin/articles/stale?page=2",
        headers=admin_headers,
    )
    assert page2.status_code == 200
    b2 = page2.json()
    assert len(b2["data"]) == 5
    assert b2["pagination"]["has_more"] is False


async def test_mark_reviewed_bumps_updated_at_and_writes_audit(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    now = datetime.now(UTC)
    ids = await _seed_articles(now=now)
    article_id = ids["stale_200"]

    resp = await seeded_client.post(
        f"/v1/admin/articles/{article_id}/mark-reviewed",
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == str(article_id)

    # DB: updated_at now close to now.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        fresh = (
            (await session.execute(select(Article).where(Article.id == article_id))).scalars().one()
        )
        # SQLite sometimes strips tzinfo; normalise before diffing.
        ua = fresh.updated_at
        if ua.tzinfo is None:
            ua = ua.replace(tzinfo=UTC)
        assert (datetime.now(UTC) - ua) < timedelta(seconds=10)

        audit_rows = list(
            (
                await session.execute(
                    select(AuditLog).where(
                        AuditLog.action == "article.reviewed",
                        AuditLog.subject_id == article_id,
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(audit_rows) == 1
    assert audit_rows[0].meta.get("action_type") == "article_reviewed"


async def test_mark_reviewed_idempotent_but_logs_twice(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """Two calls within 24h both land audit rows — per task spec."""
    now = datetime.now(UTC)
    ids = await _seed_articles(now=now)
    article_id = ids["stale_200"]

    first = await seeded_client.post(
        f"/v1/admin/articles/{article_id}/mark-reviewed",
        headers=admin_headers,
    )
    assert first.status_code == 200
    second = await seeded_client.post(
        f"/v1/admin/articles/{article_id}/mark-reviewed",
        headers=admin_headers,
    )
    assert second.status_code == 200

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        audit_rows = list(
            (
                await session.execute(
                    select(AuditLog).where(
                        AuditLog.action == "article.reviewed",
                        AuditLog.subject_id == article_id,
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(audit_rows) == 2


async def test_mark_reviewed_missing_404(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/articles/00000000-0000-4000-8000-0000000000ff/mark-reviewed",
        headers=admin_headers,
    )
    assert resp.status_code == 404


async def test_stale_last_reviewed_by_surfaces_after_mark(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    """After mark-reviewed, the article drops off the stale list (updated_at
    just bumped). So we test last_reviewed_by by creating an older review
    record, then rolling updated_at back so the row is still stale, and confirm
    the payload surfaces the reviewer.
    """
    now = datetime.now(UTC)
    ids = await _seed_articles(now=now)
    article_id = ids["stale_200"]

    # Review it (audit row stamps now).
    await seeded_client.post(
        f"/v1/admin/articles/{article_id}/mark-reviewed",
        headers=admin_headers,
    )
    # Reset updated_at back to 200 days ago so the article re-enters the list.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        article = (
            (await session.execute(select(Article).where(Article.id == article_id))).scalars().one()
        )
        article.updated_at = now - timedelta(days=200)
        await session.commit()

    resp = await seeded_client.get("/v1/admin/articles/stale", headers=admin_headers)
    assert resp.status_code == 200
    payload = resp.json()
    target = next(a for a in payload["data"] if a["id"] == str(article_id))
    assert target["last_reviewed_by"] is not None
    assert target["last_reviewed_by"]["actor_email"] == "admin@loftly.test"
