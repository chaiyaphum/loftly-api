"""Promo + promo_card_map models — deal-harvester synced promotions. See SCHEMA.md §8."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    ForeignKey,
    Numeric,
    Table,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from loftly.db.models import GUID, Base

if TYPE_CHECKING:
    from loftly.db.models.bank import Bank
    from loftly.db.models.card import Card


# Association table between promos and cards (composite PK).
# Declared here so Base.metadata.create_all builds it for tests.
promo_card_map = Table(
    "promo_card_map",
    Base.metadata,
    Column(
        "promo_id",
        GUID(),
        ForeignKey("promos.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "card_id",
        GUID(),
        ForeignKey("cards.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class Promo(Base):
    __tablename__ = "promos"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    bank_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("banks.id"), nullable=False)
    external_source_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    external_bank_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    external_checksum: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    promo_type: Mapped[str] = mapped_column(Text, nullable=False)
    title_th: Mapped[str] = mapped_column(Text, nullable=False)
    title_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_th: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    merchant_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str | None] = mapped_column(Text, nullable=True)
    discount_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    discount_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    discount_amount: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    discount_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    minimum_spend: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    valid_from: Mapped[date | None] = mapped_column(nullable=True)
    valid_until: Mapped[date | None] = mapped_column(nullable=True)
    terms_and_conditions: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_data: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, server_default=text("'{}'")
    )
    relevance_tags: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, server_default=text("'[]'")
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("1"))
    last_synced_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    bank: Mapped[Bank] = relationship(back_populates="promos")
    cards: Mapped[list[Card]] = relationship(secondary=promo_card_map)
