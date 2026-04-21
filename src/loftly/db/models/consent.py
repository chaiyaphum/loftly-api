"""UserConsent model — append-only log. See SCHEMA.md §2."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, ForeignKey, Index, LargeBinary, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class UserConsent(Base):
    """PDPA consent record. Never UPDATE — INSERT a new row to change state.

    NB: no ON DELETE CASCADE on `user_id` — the consent log must outlive account
    deletion for 7 years (PDPA accountability). See SCHEMA.md §Things to remember.
    """

    __tablename__ = "user_consents"
    __table_args__ = (
        Index(
            "idx_user_consents_user_purpose_latest",
            "user_id",
            "purpose",
            "granted_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    purpose: Mapped[str] = mapped_column(Text, nullable=False)
    granted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    policy_version: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    ip_hash: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    user_agent_hash: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    granted_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
