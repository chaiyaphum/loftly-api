"""SelectorSession model — one row per `/v1/selector` call.

Stores the full input + output envelope for:
- retrieving historical results (`GET /v1/selector/{session_id}`)
- binding anon sessions to a user after magic-link email capture (see
  `POST /v1/auth/magic-link/consume`)
- future: Selector follow-up prompts (POST_V1 §1) replay the input

Append-only in practice; we do update `user_id` + `bound_at` once at
bind time and never mutate the envelope itself.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, ForeignKey, Index, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class SelectorSession(Base):
    __tablename__ = "selector_sessions"
    __table_args__ = (
        Index("idx_selector_sessions_user_created", "user_id", "created_at"),
        Index("idx_selector_sessions_profile_hash", "profile_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    # SHA-256 hex digest of the normalized SelectorInput JSON — 64 chars.
    profile_hash: Mapped[str] = mapped_column(Text, nullable=False)
    input: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    output: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    bound_at: Mapped[datetime | None] = mapped_column(nullable=True)


__all__ = ["SelectorSession"]
