"""015 — waitlist table for pricing-page email capture (W24).

The loftly-web pricing-tier stub POSTs to `/v1/waitlist` so the founder can
build a list of premium-tier interest ahead of Phase 2. Rows carry just
enough signal (variant, tier, price shown) to run a simple cohort analysis
later without shipping a full experiments service.

PDPA posture:
- `email` is the only raw PII column; we keep it because the whole point is
  to be able to email these people when the feature ships.
- `ip_hash` / `user_agent_hash` are SHA-256 bytes, stored as text hex so the
  dedupe / abuse-scan paths don't need a LargeBinary cast. They are NOT
  reversible and never leave this table.
- Unique on `(email, source)` so a user joining the pricing waitlist twice
  (back-button, double-submit) is idempotent — the API returns 204 the
  second time without raising.

Revision ID: 015_waitlist
Revises: 014_articles_stale_index
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, json_type, now_default

revision: str = "015_waitlist"
down_revision: str | None = "014_articles_stale_index"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # BigInteger PK is appropriate — this is an append-only capture table
    # and an integer id keeps CSV exports human-readable for the founder.
    id_column = (
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
        )
        if is_postgres()
        else sa.Column(
            "id",
            sa.Integer(),  # SQLite autoincrement requires INTEGER, not BIGINT
            primary_key=True,
            autoincrement=True,
        )
    )

    op.create_table(
        "waitlist",
        id_column,
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("variant", sa.Text(), nullable=True),
        sa.Column("tier", sa.Text(), nullable=True),
        sa.Column("monthly_price_thb", sa.Integer(), nullable=True),
        sa.Column(
            "source",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pricing'"),
        ),
        sa.Column(
            "meta",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("ip_hash", sa.Text(), nullable=True),
        sa.Column("user_agent_hash", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        # Inline UNIQUE so SQLite migration runs — SQLite doesn't support
        # post-facto ADD CONSTRAINT without batch mode, and we'd rather keep
        # the table creation atomic than introduce a batch_alter_table block.
        sa.UniqueConstraint("email", "source", name="uq_waitlist_email_source"),
    )
    # created_at index supports the admin list's `ORDER BY created_at DESC`.
    if is_postgres():
        op.create_index(
            "idx_waitlist_created_at",
            "waitlist",
            [sa.text("created_at DESC")],
        )
    else:
        op.create_index(
            "idx_waitlist_created_at",
            "waitlist",
            ["created_at"],
        )


def downgrade() -> None:
    op.drop_index("idx_waitlist_created_at", table_name="waitlist")
    # UNIQUE was declared inline at create_table time, so dropping the table
    # removes it too — no separate drop_constraint needed.
    op.drop_table("waitlist")
