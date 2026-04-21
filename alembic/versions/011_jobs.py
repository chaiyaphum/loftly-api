"""011 — jobs table for async account-level work (data export, scheduled delete).

Tracks the state of long-running per-user operations. `data_export` rows get a
signed `result_url` valid 48h; `account_delete_scheduled` rows carry the 14-day
grace-period expiry and are polled by the purge executor.

Revision ID: 011_jobs
Revises: 010_magic_link_provider
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, json_type, now_default, uuid_type

revision: str = "011_jobs"
down_revision: str | None = "010_magic_link_provider"
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
        "jobs",
        _uuid_pk(),
        sa.Column(
            "user_id",
            uuid_type(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("result_url", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "started_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column(
            "metadata",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.CheckConstraint(
            "job_type IN ('data_export','account_delete_scheduled')",
            name="jobs_type_check",
        ),
        sa.CheckConstraint(
            "status IN ('queued','running','done','failed','cancelled')",
            name="jobs_status_check",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_jobs_user_created",
            "jobs",
            ["user_id", sa.text("created_at DESC")],
        )
    else:
        op.create_index("idx_jobs_user_created", "jobs", ["user_id", "created_at"])
    op.create_index("idx_jobs_status_created", "jobs", ["status", "created_at"])


def downgrade() -> None:
    op.drop_index("idx_jobs_status_created", table_name="jobs")
    op.drop_index("idx_jobs_user_created", table_name="jobs")
    op.drop_table("jobs")
