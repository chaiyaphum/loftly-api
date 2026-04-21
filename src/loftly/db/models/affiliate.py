"""Affiliate models — links, clicks, conversions. See SCHEMA.md §9."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    ForeignKey,
    LargeBinary,
    Numeric,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class AffiliateLink(Base):
    __tablename__ = "affiliate_links"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    card_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("cards.id"), nullable=False)
    partner_id: Mapped[str] = mapped_column(Text, nullable=False)
    url_template: Mapped[str] = mapped_column(Text, nullable=False)
    campaign_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    commission_model: Mapped[str] = mapped_column(Text, nullable=False)
    commission_amount_thb: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("1"))


class AffiliateClick(Base):
    __tablename__ = "affiliate_clicks"

    click_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    affiliate_link_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("affiliate_links.id"), nullable=False
    )
    card_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("cards.id"), nullable=False)
    partner_id: Mapped[str] = mapped_column(Text, nullable=False)
    placement: Mapped[str] = mapped_column(Text, nullable=False)
    utm_campaign: Mapped[str | None] = mapped_column(Text, nullable=True)
    referrer: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_hash: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )


class AffiliateConversion(Base):
    __tablename__ = "affiliate_conversions"
    __table_args__ = (
        UniqueConstraint(
            "click_id", "partner_id", "conversion_type", name="affiliate_conversions_idem"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    click_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("affiliate_clicks.click_id"), nullable=False
    )
    partner_id: Mapped[str] = mapped_column(Text, nullable=False)
    conversion_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    commission_thb: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    raw_payload: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, server_default=text("'{}'")
    )
