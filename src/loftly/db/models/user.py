"""User model. See SCHEMA.md §1."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Index, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class User(Base):
    """Core identity row. One per account. Soft-delete via `deleted_at`."""

    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("oauth_provider", "oauth_subject", name="users_oauth_unique"),
        Index("idx_users_deleted_at", "deleted_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    email: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    phone: Mapped[str | None] = mapped_column(Text, nullable=True)
    oauth_provider: Mapped[str] = mapped_column(Text, nullable=False)
    oauth_subject: Mapped[str] = mapped_column(Text, nullable=False)
    preferred_locale: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'th'"))
    role: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'user'"))
    created_at: Mapped[datetime] = mapped_column(
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(nullable=True)
