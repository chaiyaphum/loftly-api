"""008 — audit_log, sync_runs.

Mirrors ../loftly/mvp/artifacts/schema.sql §008.

Revision ID: 008_audit
Revises: 007_affiliate
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, json_type, now_default, uuid_type

# revision identifiers, used by Alembic.
revision: str = "008_audit"
down_revision: str | None = "007_affiliate"
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
        "audit_log",
        _uuid_pk(),
        sa.Column(
            "actor_id",
            uuid_type(),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("subject_type", sa.Text(), nullable=False),
        sa.Column("subject_id", uuid_type(), nullable=True),
        sa.Column(
            "metadata",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("ip_hash", sa.LargeBinary(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_audit_log_actor_time",
            "audit_log",
            ["actor_id", sa.text("created_at DESC")],
        )
    else:
        op.create_index("idx_audit_log_actor_time", "audit_log", ["actor_id", "created_at"])
    op.create_index("idx_audit_log_subject", "audit_log", ["subject_type", "subject_id"])

    op.create_table(
        "sync_runs",
        _uuid_pk(),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column(
            "started_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("upstream_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("inserted_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("updated_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "deactivated_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "mapping_queue_added",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('running','success','partial','failed')",
            name="sync_runs_status_check",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_sync_runs_source_time",
            "sync_runs",
            ["source", sa.text("started_at DESC")],
        )
    else:
        op.create_index("idx_sync_runs_source_time", "sync_runs", ["source", "started_at"])


def downgrade() -> None:
    op.drop_index("idx_sync_runs_source_time", table_name="sync_runs")
    op.drop_table("sync_runs")

    op.drop_index("idx_audit_log_subject", table_name="audit_log")
    op.drop_index("idx_audit_log_actor_time", table_name="audit_log")
    op.drop_table("audit_log")
