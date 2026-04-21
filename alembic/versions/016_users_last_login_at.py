"""016 — users.last_login_at nullable timestamptz.

Backs the `last_login_at` field on `GET /v1/me` (loftly-web account settings
page). Nullable because users created before this migration have never had
their login stamped, and the column is also legitimately null on first-ever
issuance (account exists via upsert but token issuance completes in the same
request — we set it there).

Revision ID: 016_users_last_login_at
Revises: 015_waitlist
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "016_users_last_login_at"
down_revision: str | None = "015_waitlist"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("last_login_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "last_login_at")
