"""Job model — async per-user work (data export, scheduled account delete).

Matches migration 011. Two `job_type` values in Phase 1:

- `data_export` — produces a JSON bundle under `result_url` valid until
  `expires_at` (48h from completion).
- `account_delete_scheduled` — 14-day grace period; `expires_at` = the
  moment the purge executor is free to finalize deletion.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, ForeignKey, Index, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("idx_jobs_user_created", "user_id", "created_at"),
        Index("idx_jobs_status_created", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'queued'"))
    result_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    # `metadata` collides with DeclarativeBase.metadata — attribute is `meta`,
    # column stays `metadata` so SCHEMA.md matches.
    meta: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON, nullable=False, server_default=text("'{}'")
    )


__all__ = ["Job"]
