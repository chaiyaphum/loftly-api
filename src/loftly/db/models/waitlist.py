"""Waitlist model — pricing-page email capture (W24). See migration 015."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, Integer, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class Waitlist(Base):
    __tablename__ = "waitlist"
    __table_args__ = (
        UniqueConstraint("email", "source", name="uq_waitlist_email_source"),
    )

    # BigInteger in Postgres, Integer in SQLite (SQLite AUTOINCREMENT only
    # triggers for `INTEGER PRIMARY KEY`, not `BIGINT PRIMARY KEY`). Using
    # `with_variant` keeps the column type correct per-dialect; the migration
    # handles the dialect split separately for DDL.
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    email: Mapped[str] = mapped_column(Text, nullable=False)
    variant: Mapped[str | None] = mapped_column(Text, nullable=True)
    tier: Mapped[str | None] = mapped_column(Text, nullable=True)
    monthly_price_thb: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'pricing'")
    )
    meta: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, server_default=text("'{}'")
    )
    # Stored as hex text (not LargeBinary) so CSV exports stay readable and
    # the dedupe / abuse-scan paths don't need a binary cast.
    ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
