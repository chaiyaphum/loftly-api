"""005 — point_valuations.

Mirrors ../loftly/mvp/artifacts/schema.sql §005.

Revision ID: 005_valuations
Revises: 004_content
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, now_default, uuid_type

# revision identifiers, used by Alembic.
revision: str = "005_valuations"
down_revision: str | None = "004_content"
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
        "point_valuations",
        _uuid_pk(),
        sa.Column(
            "currency_id",
            uuid_type(),
            sa.ForeignKey("loyalty_currencies.id"),
            nullable=False,
        ),
        sa.Column("thb_per_point", sa.Numeric(8, 4), nullable=False),
        sa.Column("methodology", sa.Text(), nullable=False),
        sa.Column(
            "percentile",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("80"),
        ),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
        sa.Column("top_redemption_example", sa.Text(), nullable=True),
        sa.Column("override_thb_per_point", sa.Numeric(8, 4), nullable=True),
        sa.Column("override_reason", sa.Text(), nullable=True),
        sa.Column(
            "computed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.CheckConstraint(
            "confidence BETWEEN 0 AND 1",
            name="point_valuations_confidence_check",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_point_valuations_currency_time",
            "point_valuations",
            ["currency_id", sa.text("computed_at DESC")],
        )
    else:
        op.create_index(
            "idx_point_valuations_currency_time",
            "point_valuations",
            ["currency_id", "computed_at"],
        )


def downgrade() -> None:
    op.drop_index("idx_point_valuations_currency_time", table_name="point_valuations")
    op.drop_table("point_valuations")
