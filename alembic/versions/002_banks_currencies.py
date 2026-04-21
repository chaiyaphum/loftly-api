"""002 — banks, loyalty_currencies, transfer_ratios.

Mirrors ../loftly/mvp/artifacts/schema.sql §002.

Revision ID: 002_banks_currencies
Revises: 001_users
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, now_default, uuid_type

# revision identifiers, used by Alembic.
revision: str = "002_banks_currencies"
down_revision: str | None = "001_users"
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
        "banks",
        _uuid_pk(),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("display_name_en", sa.Text(), nullable=False),
        sa.Column("display_name_th", sa.Text(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=True),
        sa.Column(
            "active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true") if is_postgres() else sa.text("1"),
        ),
        sa.UniqueConstraint("slug", name="banks_slug_key"),
    )

    op.create_table(
        "loyalty_currencies",
        _uuid_pk(),
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("display_name_en", sa.Text(), nullable=False),
        sa.Column("display_name_th", sa.Text(), nullable=False),
        sa.Column("currency_type", sa.Text(), nullable=False),
        sa.Column("issuing_entity", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "currency_type IN ('bank_proprietary','airline','hotel')",
            name="loyalty_currencies_currency_type_check",
        ),
        sa.UniqueConstraint("code", name="loyalty_currencies_code_key"),
    )

    op.create_table(
        "transfer_ratios",
        _uuid_pk(),
        sa.Column(
            "source_currency_id",
            uuid_type(),
            sa.ForeignKey("loyalty_currencies.id"),
            nullable=False,
        ),
        sa.Column(
            "destination_currency_id",
            uuid_type(),
            sa.ForeignKey("loyalty_currencies.id"),
            nullable=False,
        ),
        sa.Column("ratio_source", sa.Numeric(), nullable=False),
        sa.Column("ratio_destination", sa.Numeric(), nullable=False),
        sa.Column("min_transfer", sa.Integer(), nullable=True),
        sa.Column(
            "bonus_percentage",
            sa.Numeric(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_until", sa.Date(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column(
            "verified_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.CheckConstraint("ratio_source > 0", name="transfer_ratios_ratio_source_check"),
        sa.CheckConstraint("ratio_destination > 0", name="transfer_ratios_ratio_destination_check"),
        sa.CheckConstraint("bonus_percentage >= 0", name="transfer_ratios_bonus_percentage_check"),
    )

    # Ordering index (granted_at DESC) — SQLite doesn't grok DESC in index cols
    # pre-3.3, but modern SQLite accepts it. Keep it ASC for portability.
    if is_postgres():
        op.create_index(
            "idx_transfer_ratios_lookup",
            "transfer_ratios",
            ["source_currency_id", "destination_currency_id", sa.text("effective_from DESC")],
        )
    else:
        op.create_index(
            "idx_transfer_ratios_lookup",
            "transfer_ratios",
            ["source_currency_id", "destination_currency_id", "effective_from"],
        )


def downgrade() -> None:
    op.drop_index("idx_transfer_ratios_lookup", table_name="transfer_ratios")
    op.drop_table("transfer_ratios")
    op.drop_table("loyalty_currencies")
    op.drop_table("banks")
