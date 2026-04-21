"""009 — selector_sessions.

Persists every `/v1/selector` call (anon or authed). Binds anon sessions to
a user_id at magic-link consume time.

Revision ID: 009_selector_sessions
Revises: 008_audit
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, json_type, now_default, uuid_type

# revision identifiers, used by Alembic.
revision: str = "009_selector_sessions"
down_revision: str | None = "008_audit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _uuid_pk() -> sa.Column[object]:
    if is_postgres():
        return sa.Column(
            "id",
            uuid_type(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        )
    return sa.Column("id", uuid_type(), primary_key=True)


def upgrade() -> None:
    op.create_table(
        "selector_sessions",
        _uuid_pk(),
        sa.Column(
            "user_id",
            uuid_type(),
            sa.ForeignKey("users.id"),
            nullable=True,
        ),
        # SHA-256 hex = 64 chars. VARCHAR(64) fits cleanly.
        sa.Column("profile_hash", sa.String(length=64), nullable=False),
        sa.Column("input", json_type(), nullable=False),
        sa.Column("output", json_type(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column("bound_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )

    if is_postgres():
        op.create_index(
            "idx_selector_sessions_user_created",
            "selector_sessions",
            ["user_id", sa.text("created_at DESC")],
        )
    else:
        op.create_index(
            "idx_selector_sessions_user_created",
            "selector_sessions",
            ["user_id", "created_at"],
        )
    op.create_index(
        "idx_selector_sessions_profile_hash",
        "selector_sessions",
        ["profile_hash"],
    )


def downgrade() -> None:
    op.drop_index("idx_selector_sessions_profile_hash", table_name="selector_sessions")
    op.drop_index("idx_selector_sessions_user_created", table_name="selector_sessions")
    op.drop_table("selector_sessions")
