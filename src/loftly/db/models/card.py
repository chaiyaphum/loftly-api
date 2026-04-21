"""Card model — stub for Phase 1 migration 003. See SCHEMA.md §5.

Only enough columns to be useful for the `/v1/cards` route when DB integration
lands in Week 3. Phase 1 route returns a baked-in fixture (no DB read).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, Numeric, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class Card(Base):
    __tablename__ = "cards"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    bank_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("banks.id"), nullable=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    tier: Mapped[str | None] = mapped_column(Text, nullable=True)
    network: Mapped[str] = mapped_column(Text, nullable=False)
    annual_fee_thb: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    annual_fee_waiver: Mapped[str | None] = mapped_column(Text, nullable=True)
    min_income_thb: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    description_th: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
