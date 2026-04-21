"""LoyaltyCurrency model — point/mile currencies. See SCHEMA.md §4."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from loftly.db.models import Base

if TYPE_CHECKING:
    from loftly.db.models.card import Card


class LoyaltyCurrency(Base):
    __tablename__ = "loyalty_currencies"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    code: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    display_name_en: Mapped[str] = mapped_column(Text, nullable=False)
    display_name_th: Mapped[str] = mapped_column(Text, nullable=False)
    currency_type: Mapped[str] = mapped_column(Text, nullable=False)
    issuing_entity: Mapped[str | None] = mapped_column(Text, nullable=True)

    cards: Mapped[list[Card]] = relationship(back_populates="earn_currency")
