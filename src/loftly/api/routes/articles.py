"""Public article/review endpoints — scaffold stub.

`GET /v1/articles/{slug}` serves review bodies to the web. See
API_CONTRACT.md §Cards catalog (articles share the public-read rate bucket).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/articles", tags=["articles"])


@router.get("/{slug}", summary="Article (review/guide) body")
async def get_article(slug: str) -> None:
    _ = slug
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
