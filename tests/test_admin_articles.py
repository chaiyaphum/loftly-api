"""Admin articles CRUD + state transitions + slug collision."""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.article import Article
from loftly.db.models.audit import AuditLog


async def test_admin_articles_requires_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/articles")
    assert resp.status_code == 401


async def test_admin_articles_create_and_list(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/articles",
        headers=admin_headers,
        json={
            "slug": "kbank-wisdom-review",
            "article_type": "card_review",
            "title_th": "รีวิวบัตร KBank WISDOM",
            "summary_th": "บัตรหลักสำหรับสายสะสม K Point",
            "body_th": "เนื้อหาแบบยาว...",
            "best_for_tags": ["dining", "lounge"],
            "state": "draft",
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["slug"] == "kbank-wisdom-review"
    assert body["state"] == "draft"
    assert body["policy_version"]

    listing = await seeded_client.get("/v1/admin/articles", headers=admin_headers)
    assert listing.status_code == 200
    slugs = {a["slug"] for a in listing.json()["data"]}
    assert "kbank-wisdom-review" in slugs

    # audit row
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(AuditLog).where(AuditLog.action == "article.created")))
            .scalars()
            .all()
        )
    assert len(rows) == 1


async def test_admin_articles_slug_collision(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    payload = {
        "slug": "collide-me",
        "article_type": "guide",
        "title_th": "t",
        "summary_th": "s",
        "body_th": "b",
    }
    first = await seeded_client.post("/v1/admin/articles", headers=admin_headers, json=payload)
    assert first.status_code == 201
    dup = await seeded_client.post("/v1/admin/articles", headers=admin_headers, json=payload)
    assert dup.status_code == 409
    assert dup.json()["error"]["code"] == "slug_conflict"


async def test_admin_articles_state_transition_to_published(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    created = await seeded_client.post(
        "/v1/admin/articles",
        headers=admin_headers,
        json={
            "slug": "about-to-publish",
            "article_type": "guide",
            "title_th": "t",
            "summary_th": "s",
            "body_th": "b",
            "state": "draft",
        },
    )
    article_id = created.json()["id"]

    resp = await seeded_client.patch(
        f"/v1/admin/articles/{article_id}",
        headers=admin_headers,
        json={"state": "published"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["state"] == "published"
    assert body["published_at"] is not None

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        transitions = list(
            (
                await session.execute(
                    select(AuditLog).where(AuditLog.action == "article.state.published")
                )
            )
            .scalars()
            .all()
        )
    assert len(transitions) == 1


async def test_admin_articles_patch_missing(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.patch(
        "/v1/admin/articles/00000000-0000-4000-8000-0000000000aa",
        headers=admin_headers,
        json={"title_th": "nope"},
    )
    assert resp.status_code == 404


async def test_admin_articles_missing_required_fields(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/articles",
        headers=admin_headers,
        json={"slug": "broken"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "missing_fields"


async def test_admin_articles_direct_publish_sets_author(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post(
        "/v1/admin/articles",
        headers=admin_headers,
        json={
            "slug": "fresh-publish",
            "article_type": "news",
            "title_th": "t",
            "summary_th": "s",
            "body_th": "b",
            "state": "published",
        },
    )
    assert resp.status_code == 201
    assert resp.json()["state"] == "published"
    assert resp.json()["published_at"] is not None

    # Verify author_id was stamped.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        article = (
            (await session.execute(select(Article).where(Article.slug == "fresh-publish")))
            .scalars()
            .one()
        )
    assert article.author_id is not None
