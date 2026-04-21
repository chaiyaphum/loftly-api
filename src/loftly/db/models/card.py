"""Card model — product catalog. See SCHEMA.md §5."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import JSON, ForeignKey, Integer, Numeric, Text, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from loftly.db.models import Base

if TYPE_CHECKING:
    from loftly.db.models.bank import Bank
    from loftly.db.models.loyalty_currency import LoyaltyCurrency


class Card(Base):
    """Credit card product row.

    JSON columns (`earn_rate_local`, `earn_rate_foreign`, `benefits`, `signup_bonus`)
    use SQLAlchemy's portable JSON type — JSONB on Postgres via migration, JSON
    elsewhere. Model-level is always portable `sa.JSON`.
    """

    __tablename__ = "cards"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    bank_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("banks.id"), nullable=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    tier: Mapped[str | None] = mapped_column(Text, nullable=True)
    network: Mapped[str] = mapped_column(Text, nullable=False)
    annual_fee_thb: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    annual_fee_waiver: Mapped[str | None] = mapped_column(Text, nullable=True)
    min_income_thb: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    min_age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    earn_currency_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("loyalty_currencies.id"), nullable=False
    )
    earn_rate_local: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    earn_rate_foreign: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    earn_cap_monthly_thb: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    benefits: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, server_default=text("'{}'")
    )
    signup_bonus: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    description_th: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    bank: Mapped[Bank] = relationship(back_populates="cards", lazy="joined")
    earn_currency: Mapped[LoyaltyCurrency] = relationship(back_populates="cards", lazy="joined")
