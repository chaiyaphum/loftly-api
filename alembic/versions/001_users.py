"""001 — users, user_consents, set_updated_at trigger function.

Mirrors ../loftly/mvp/artifacts/schema.sql §001.

Revision ID: 001_users
Revises:
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, now_default, uuid_type

# revision identifiers, used by Alembic.
revision: str = "001_users"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


SET_UPDATED_AT_FN = """
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

DROP_SET_UPDATED_AT_FN = "DROP FUNCTION IF EXISTS set_updated_at();"


def _uuid_pk() -> sa.Column[object]:
    """Primary key column — server-side gen on Postgres; Python-side on SQLite."""
    if is_postgres():
        return sa.Column(
            "id",
            uuid_type(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        )
    return sa.Column("id", uuid_type(), primary_key=True)


def upgrade() -> None:
    if is_postgres():
        # pgcrypto for gen_random_uuid(); safe to run repeatedly.
        op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

        # Shared updated_at touch function — used by later migrations' triggers.
        op.execute(SET_UPDATED_AT_FN)

    op.create_table(
        "users",
        _uuid_pk(),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("phone", sa.Text(), nullable=True),
        sa.Column("oauth_provider", sa.Text(), nullable=False),
        sa.Column("oauth_subject", sa.Text(), nullable=False),
        sa.Column(
            "preferred_locale",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'th'"),
        ),
        sa.Column(
            "role",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'user'"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "oauth_provider IN ('google','apple','line')",
            name="users_oauth_provider_check",
        ),
        sa.CheckConstraint(
            "preferred_locale IN ('th','en')",
            name="users_preferred_locale_check",
        ),
        sa.CheckConstraint(
            "role IN ('user','admin')",
            name="users_role_check",
        ),
        sa.UniqueConstraint("email", name="users_email_key"),
        sa.UniqueConstraint(
            "oauth_provider",
            "oauth_subject",
            name="users_oauth_unique",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_users_deleted_at",
            "users",
            ["deleted_at"],
            postgresql_where=sa.text("deleted_at IS NOT NULL"),
        )
    else:
        op.create_index("idx_users_deleted_at", "users", ["deleted_at"])

    # user_consents — append-only. NO ON DELETE CASCADE on user_id per PDPA (SCHEMA.md §2).
    op.create_table(
        "user_consents",
        _uuid_pk(),
        sa.Column(
            "user_id",
            uuid_type(),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column("purpose", sa.Text(), nullable=False),
        sa.Column("granted", sa.Boolean(), nullable=False),
        sa.Column("policy_version", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("ip_hash", sa.LargeBinary(), nullable=True),
        sa.Column("user_agent_hash", sa.LargeBinary(), nullable=True),
        sa.Column(
            "granted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.CheckConstraint(
            "purpose IN ('optimization','marketing','analytics','sharing')",
            name="user_consents_purpose_check",
        ),
        sa.CheckConstraint(
            "source IN ('onboarding','account_settings','selector','admin')",
            name="user_consents_source_check",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_user_consents_user_purpose_latest",
            "user_consents",
            ["user_id", "purpose", sa.text("granted_at DESC")],
        )
    else:
        op.create_index(
            "idx_user_consents_user_purpose_latest",
            "user_consents",
            ["user_id", "purpose", "granted_at"],
        )


def downgrade() -> None:
    op.drop_index("idx_user_consents_user_purpose_latest", table_name="user_consents")
    op.drop_table("user_consents")

    op.drop_index("idx_users_deleted_at", table_name="users")
    op.drop_table("users")

    if is_postgres():
        op.execute(DROP_SET_UPDATED_AT_FN)
