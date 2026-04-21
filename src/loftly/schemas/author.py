"""Pydantic schemas for `GET /v1/authors/{slug}`.

The byline on `/cards/[slug]` (loftly-web) fetches this shape and renders
`display_name` (or `display_name_en` when the locale is en) — see
`loftly-web/src/components/articles/ArticleByline.tsx`.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class AuthorResponse(BaseModel):
    """Public projection of the `authors` table.

    All optional fields are passed through unchanged — the frontend decides
    which ones to render. `id` is exposed as a UUID string for easy React-key
    usage; the DB stores it as a native UUID (Postgres) / CHAR(36) (SQLite).
    """

    id: str
    slug: str
    display_name: str
    display_name_en: str | None = None
    bio_th: str | None = None
    bio_en: str | None = None
    role: str | None = None
    image_url: str | None = None
    created_at: datetime
