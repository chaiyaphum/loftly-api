"""PointValuation model — THB-per-point valuations. See SCHEMA.md §6."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import ForeignKey, Integer, Numeric, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class PointValuation(Base):
    __tablename__ = "point_valuations"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    currency_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("loyalty_currencies.id"), nullable=False
    )
    thb_per_point: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    methodology: Mapped[str] = mapped_column(Text, nullable=False)
    percentile: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("80"))
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)
    top_redemption_example: Mapped[str | None] = mapped_column(Text, nullable=True)
    override_thb_per_point: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    override_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    computed_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
