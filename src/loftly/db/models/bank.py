"""Bank model — issuer catalog. See SCHEMA.md §4."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from loftly.db.models import Base

if TYPE_CHECKING:
    from loftly.db.models.card import Card
    from loftly.db.models.promo import Promo


class Bank(Base):
    __tablename__ = "banks"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    display_name_en: Mapped[str] = mapped_column(Text, nullable=False)
    display_name_th: Mapped[str] = mapped_column(Text, nullable=False)
    source_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("1"))

    cards: Mapped[list[Card]] = relationship(back_populates="bank")
    promos: Mapped[list[Promo]] = relationship(back_populates="bank")
