"""010 — extend users.oauth_provider CHECK to include 'email_magic'.

Magic-link flow (added in migration 009 era, enabled in routes/auth.py)
inserts users with oauth_provider='email_magic'. The CHECK constraint
from 001 only allowed ('google','apple','line'), which SQLite ignores
but Postgres enforces. This migration relaxes the enum on Postgres.
No-op on SQLite.

Revision ID: 010_magic_link_provider
Revises: 009_selector_sessions
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from loftly.db.migration_helpers import is_postgres

revision: str = "010_magic_link_provider"
down_revision: str | None = "009_selector_sessions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    if not is_postgres():
        return
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_oauth_provider_check;")
    op.execute(
        "ALTER TABLE users ADD CONSTRAINT users_oauth_provider_check "
        "CHECK (oauth_provider IN ('google','apple','line','email_magic'));"
    )


def downgrade() -> None:
    if not is_postgres():
        return
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_oauth_provider_check;")
    op.execute(
        "ALTER TABLE users ADD CONSTRAINT users_oauth_provider_check "
        "CHECK (oauth_provider IN ('google','apple','line'));"
    )
