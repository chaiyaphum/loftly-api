"""014 — add (state, updated_at) index on articles for the re-verification scan.

The admin stale-article list (`/v1/admin/articles/stale`) filters by
`state = 'published'` AND `updated_at < cutoff`, sorted by `updated_at ASC`.
The existing `idx_articles_state_published` covers `(state, published_at)` —
which is what public reads (sorted by `published_at DESC`) need — but it
does not help this re-verification scan.

A composite `(state, updated_at)` index keeps the weekly content audit fast
once the table crosses a few hundred rows and `updated_at` is no longer
roughly-correlated with `published_at`.

Revision ID: 014_articles_stale_index
Revises: 013_audit_log_retention_index
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres

revision: str = "014_articles_stale_index"
down_revision: str | None = "013_audit_log_retention_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    if is_postgres():
        op.create_index(
            "idx_articles_state_updated",
            "articles",
            ["state", sa.text("updated_at ASC")],
        )
    else:
        op.create_index(
            "idx_articles_state_updated",
            "articles",
            ["state", "updated_at"],
        )


def downgrade() -> None:
    op.drop_index("idx_articles_state_updated", table_name="articles")
