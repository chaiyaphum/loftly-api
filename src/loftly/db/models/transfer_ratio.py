"""TransferRatio model — point→mile conversion rules. See SCHEMA.md §4."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import ForeignKey, Integer, Numeric, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class TransferRatio(Base):
    __tablename__ = "transfer_ratios"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_currency_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("loyalty_currencies.id"), nullable=False
    )
    destination_currency_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("loyalty_currencies.id"), nullable=False
    )
    ratio_source: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    ratio_destination: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    min_transfer: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bonus_percentage: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False, server_default=text("0")
    )
    effective_from: Mapped[date] = mapped_column(nullable=False)
    effective_until: Mapped[date | None] = mapped_column(nullable=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    verified_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
