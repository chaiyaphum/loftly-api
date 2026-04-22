"""Merchant models — canonical merchants + promo↔merchant mapping.

See `mvp/SCHEMA.md §15 + §16`. Canonical merchant rows power the
`/merchants/[slug]` reverse-lookup surface; the map table joins them to
the ingested promos so rankings can surface "what's active at Starbucks
right now for each card".

Portable across Postgres (prod) + aiosqlite (tests):
- `alt_names` stored as JSON at the ORM layer; migration promotes to `text[]`
  on Postgres so the GIN index works. Reads are list[str] either way.
- `seo_meta` JSONB on PG, JSON elsewhere.
- UUIDs via the shared `GUID` TypeDecorator.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import JSON, ForeignKey, Numeric, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from loftly.db.models import Base

if TYPE_CHECKING:
    from loftly.db.models.promo import Promo


class MerchantCanonical(Base):
    """One canonical merchant entity. See SCHEMA.md §15."""

    __tablename__ = "merchants_canonical"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    display_name_th: Mapped[str] = mapped_column(Text, nullable=False)
    display_name_en: Mapped[str] = mapped_column(Text, nullable=False)
    category_default: Mapped[str | None] = mapped_column(Text, nullable=True)
    # JSON at the ORM layer; migration casts to Postgres text[] for GIN.
    alt_names: Mapped[list[str]] = mapped_column(JSON, nullable=False, server_default=text("'[]'"))
    logo_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_th: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    merchant_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    merged_into_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("merchants_canonical.id"),
        nullable=True,
    )
    seo_meta: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, server_default=text("'{}'")
    )
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    # Self-referential FK for merge rollback.
    merged_into: Mapped[MerchantCanonical | None] = relationship(
        "MerchantCanonical",
        remote_side="MerchantCanonical.id",
        foreign_keys=[merged_into_id],
    )


class PromoMerchantCanonicalMap(Base):
    """Promo ↔ canonical merchant mapping with confidence. See SCHEMA.md §16."""

    __tablename__ = "promos_merchant_canonical_map"

    promo_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("promos.id", ondelete="CASCADE"), primary_key=True
    )
    merchant_canonical_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("merchants_canonical.id"), nullable=False
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)
    method: Mapped[str] = mapped_column(Text, nullable=False)
    mapped_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    reviewed_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(nullable=True)

    merchant: Mapped[MerchantCanonical] = relationship(
        "MerchantCanonical",
        foreign_keys=[merchant_canonical_id],
    )
    promo: Mapped[Promo] = relationship("Promo")


__all__ = ["MerchantCanonical", "PromoMerchantCanonicalMap"]
