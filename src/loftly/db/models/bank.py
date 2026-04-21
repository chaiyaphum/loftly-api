"""Bank model — issuer catalog. Stub for Phase 1 migration 002. See SCHEMA.md §4."""

from __future__ import annotations

import uuid

from sqlalchemy import Boolean, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class Bank(Base):
    __tablename__ = "banks"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    display_name_en: Mapped[str] = mapped_column(Text, nullable=False)
    display_name_th: Mapped[str] = mapped_column(Text, nullable=False)
    source_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("1"))
