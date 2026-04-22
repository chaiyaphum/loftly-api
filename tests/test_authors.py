"""Authors endpoint — `GET /v1/authors/{slug}`.

Covers:
- The seeded Loftly organization row is present and retrievable.
- Unknown slugs return a 404 with the Loftly error envelope.
- Response shape matches `AuthorResponse` (including nullable fields).
- A second author slug resolves independently of the default one.
- `articles.authors_id` default is NULL (byline falls back to "Loftly").
"""

from __future__ import annotations

import uuid

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.article import Article
from loftly.db.models.author import Author
from loftly.db.seed import LOFTLY_ORG_AUTHOR_ID


async def test_get_author_loftly_default_returns_200(
    seeded_client: AsyncClient,
) -> None:
    """The seeded organization byline is addressable via slug='loftly'."""
    resp = await seeded_client.get("/v1/authors/loftly")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["slug"] == "loftly"
    assert body["display_name"] == "Loftly"
    assert body["display_name_en"] == "Loftly"
    assert body["role"] == "organization"
    # Pinned UUID — stable reference for frontend + downstream tests.
    assert body["id"] == str(LOFTLY_ORG_AUTHOR_ID)


async def test_get_author_unknown_slug_returns_404(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.get("/v1/authors/ghost-author")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["code"] == "not_found"
    assert body["error"]["details"] == {"slug": "ghost-author"}


async def test_get_author_response_shape_has_all_optional_fields(
    seeded_client: AsyncClient,
) -> None:
    """All documented fields must be present in the envelope, even when null."""
    resp = await seeded_client.get("/v1/authors/loftly")
    assert resp.status_code == 200
    body = resp.json()
    for key in (
        "id",
        "slug",
        "display_name",
        "display_name_en",
        "bio_th",
        "bio_en",
        "role",
        "image_url",
        "created_at",
    ):
        assert key in body, f"missing field: {key}"
    # The default org row doesn't have bio/photo populated — make sure they
    # come back as JSON null, not missing.
    assert body["bio_th"] is None
    assert body["bio_en"] is None
    assert body["image_url"] is None


async def test_get_author_second_row_resolves_independently(
    seeded_client: AsyncClient,
) -> None:
    """A freshly inserted contractor row is fetchable without affecting
    the default Loftly byline."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        session.add(
            Author(
                id=uuid.uuid4(),
                slug="chai",
                display_name="ชัยยภูมิ",
                display_name_en="Chai",
                bio_th="นักเขียนอิสระ",
                bio_en="Independent editor",
                role="contractor",
            )
        )
        await session.commit()

    resp = await seeded_client.get("/v1/authors/chai")
    assert resp.status_code == 200
    body = resp.json()
    assert body["slug"] == "chai"
    assert body["display_name"] == "ชัยยภูมิ"
    assert body["display_name_en"] == "Chai"
    assert body["role"] == "contractor"

    # Default row still reachable and untouched.
    default = await seeded_client.get("/v1/authors/loftly")
    assert default.status_code == 200
    assert default.json()["display_name"] == "Loftly"


async def test_articles_authors_id_column_defaults_null(
    seeded_client: AsyncClient,
) -> None:
    """Migration 017 adds `articles.authors_id` as nullable with no backfill
    — confirm inserting an article without setting the column leaves it NULL,
    so the frontend will default to the Loftly byline.
    """
    sessionmaker = get_sessionmaker()
    from tests.conftest import TEST_ADMIN_ID

    article_id = uuid.uuid4()
    async with sessionmaker() as session:
        session.add(
            Article(
                id=article_id,
                slug="default-byline-check",
                article_type="guide",
                title_th="ทดสอบ",
                summary_th="สรุป",
                body_th="เนื้อหา",
                state="draft",
                author_id=TEST_ADMIN_ID,
                policy_version="v1",
            )
        )
        await session.commit()

    async with sessionmaker() as session:
        row = (await session.execute(select(Article).where(Article.id == article_id))).scalar_one()
    # authors_id stays NULL → frontend renders the default "Loftly" byline.
    assert row.authors_id is None


async def test_get_author_slug_is_case_sensitive(
    seeded_client: AsyncClient,
) -> None:
    """Slugs are lowercase by convention; uppercase request must 404 rather
    than silently matching — avoids duplicate canonical URLs."""
    resp = await seeded_client.get("/v1/authors/Loftly")
    assert resp.status_code == 404
