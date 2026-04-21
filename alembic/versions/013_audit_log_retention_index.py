"""013 — add (action, created_at) index on audit_log for the retention cron.

The retention job filters by `action` (classified as consent / non-sensitive)
and `created_at < cutoff`. Existing 008 indexes cover `(actor_id, created_at)`
and `(subject_type, subject_id)` — neither is a good match for the retention
scan. A covering `(action, created_at)` index keeps the weekly sweep fast once
the table grows past ~100k rows.

Revision ID: 013_audit_log_retention_index
Revises: 012_system_user
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres

revision: str = "013_audit_log_retention_index"
down_revision: str | None = "012_system_user"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    if is_postgres():
        op.create_index(
            "idx_audit_log_action_time",
            "audit_log",
            ["action", sa.text("created_at DESC")],
        )
    else:
        op.create_index(
            "idx_audit_log_action_time",
            "audit_log",
            ["action", "created_at"],
        )


def downgrade() -> None:
    op.drop_index("idx_audit_log_action_time", table_name="audit_log")
