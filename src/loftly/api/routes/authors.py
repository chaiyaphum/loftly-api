"""Public author endpoint — `GET /v1/authors/{slug}`.

Powers the byline on `/cards/[slug]` (loftly-web). Returns the editorial
metadata row from `authors`; 404 when the slug is unknown so the frontend
can fall back to the hardcoded "Loftly" byline without surfacing a crash.

Scope notes:
- Public, unauthenticated (shares the public-read rate bucket with
  `/v1/cards/{slug}`). No separate rate-limit middleware wired up here —
  the global middleware applies.
- No 304 / ETag yet; author rows change rarely and the payload is tiny, so
  add caching later only if the byline actually shows up in N+1 hot paths.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.db.engine import get_session
from loftly.db.models.author import Author
from loftly.schemas.author import AuthorResponse

router = APIRouter(prefix="/v1/authors", tags=["authors"])


@router.get(
    "/{slug}",
    summary="Get a public author profile by slug",
    response_model=AuthorResponse,
    responses={404: {"description": "Unknown author slug."}},
)
async def get_author(
    slug: str,
    session: AsyncSession = Depends(get_session),
) -> AuthorResponse:
    """Return the author row matching `slug`, else 404.

    Uses the unique `authors.slug` index (see migration 017) for the lookup.
    """
    row = (await session.execute(select(Author).where(Author.slug == slug))).scalar_one_or_none()
    if row is None:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="not_found",
            message_en=f"Unknown author slug: {slug}",
            message_th="ไม่พบผู้เขียนที่ระบุ",
            details={"slug": slug},
        )

    return AuthorResponse(
        id=str(row.id),
        slug=row.slug,
        display_name=row.display_name,
        display_name_en=row.display_name_en,
        bio_th=row.bio_th,
        bio_en=row.bio_en,
        role=row.role,
        image_url=row.image_url,
        created_at=row.created_at,
    )


__all__ = ["router"]
