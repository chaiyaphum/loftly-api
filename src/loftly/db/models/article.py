"""Article model — editorial content. See SCHEMA.md §7."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, ForeignKey, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class Article(Base):
    """Card reviews, guides, news, comparisons.

    `best_for_tags` is a portable JSON list at the ORM level; migrations
    create a Postgres `text[]` with a GIN index for production.
    """

    __tablename__ = "articles"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    card_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("cards.id"), nullable=True)
    article_type: Mapped[str] = mapped_column(Text, nullable=False)
    title_th: Mapped[str] = mapped_column(Text, nullable=False)
    title_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_th: Mapped[str] = mapped_column(Text, nullable=False)
    summary_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_th: Mapped[str] = mapped_column(Text, nullable=False)
    body_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    best_for_tags: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, server_default=text("'[]'")
    )
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'draft'"))
    author_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    policy_version: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    seo_meta: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, server_default=text("'{}'")
    )
