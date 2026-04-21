"""Admin (CMS) endpoints — scaffold stubs. See API_CONTRACT.md §Admin."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/v1/admin", tags=["admin"])


@router.get("/cards", summary="List cards (all states)")
async def list_cards() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/cards", summary="Create card", status_code=status.HTTP_201_CREATED)
async def create_card() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.patch("/cards/{card_id}", summary="Update card")
async def update_card(card_id: str) -> None:
    _ = card_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/articles", summary="List articles (all states)")
async def list_articles() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/articles", summary="Create article", status_code=status.HTTP_201_CREATED)
async def create_article() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.patch("/articles/{article_id}", summary="Update article (state transitions)")
async def update_article(article_id: str) -> None:
    _ = article_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/promos", summary="List promos")
async def list_promos() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/promos", summary="Create manual promo", status_code=status.HTTP_201_CREATED)
async def create_promo() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/mapping-queue", summary="Unresolved promo → card mappings")
async def mapping_queue() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.post("/mapping-queue/{promo_id}/assign", summary="Bind promo to card(s)")
async def assign_mapping(promo_id: str) -> None:
    _ = promo_id
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/affiliate/stats", summary="30-day affiliate funnel")
async def affiliate_stats() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")


@router.get("/affiliate/export.csv", summary="CSV dump of last 30d clicks + conversions")
async def affiliate_export() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, detail="not implemented")
