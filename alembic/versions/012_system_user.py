"""012 — seed a deterministic 'system' user for actor-less audit log writes.

The webhook rejection path (and any other non-admin system event) needs an
`audit_log.actor_id` that satisfies the users FK. We insert a single system
row keyed to `00000000-0000-0000-0000-000000000001` with a synthetic
`email_magic` oauth_provider (enabled in migration 010) so it passes the
CHECK constraint on Postgres.

Portable: runs on both Postgres and SQLite. Idempotent — uses `INSERT ... ON
CONFLICT DO NOTHING` on Postgres and a pre-check on SQLite.

Revision ID: 012_system_user
Revises: 011_jobs
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres

revision: str = "012_system_user"
down_revision: str | None = "011_jobs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


SYSTEM_USER_ID = "00000000-0000-0000-0000-000000000001"


def upgrade() -> None:
    bind = op.get_bind()
    if is_postgres():
        bind.execute(
            sa.text(
                """
                INSERT INTO users (id, email, oauth_provider, oauth_subject, role)
                VALUES (:id, :email, :provider, :subject, :role)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {
                "id": SYSTEM_USER_ID,
                "email": "system@loftly.co.th",
                "provider": "email_magic",
                "subject": "__system__",
                "role": "admin",
            },
        )
    else:
        # SQLite path — no ON CONFLICT here; insert only if missing.
        exists = bind.execute(
            sa.text("SELECT 1 FROM users WHERE id = :id"),
            {"id": SYSTEM_USER_ID},
        ).scalar()
        if not exists:
            bind.execute(
                sa.text(
                    """
                    INSERT INTO users (id, email, oauth_provider, oauth_subject, role)
                    VALUES (:id, :email, :provider, :subject, :role)
                    """
                ),
                {
                    "id": SYSTEM_USER_ID,
                    "email": "system@loftly.co.th",
                    "provider": "email_magic",
                    "subject": "__system__",
                    "role": "admin",
                },
            )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        sa.text("DELETE FROM users WHERE id = :id"),
        {"id": SYSTEM_USER_ID},
    )
