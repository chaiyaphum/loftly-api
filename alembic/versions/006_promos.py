"""006 — promos, promo_card_map.

Mirrors ../loftly/mvp/artifacts/schema.sql §006.

Revision ID: 006_promos
Revises: 005_valuations
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import (
    is_postgres,
    json_type,
    now_default,
    string_array_type,
    uuid_type,
)

# revision identifiers, used by Alembic.
revision: str = "006_promos"
down_revision: str | None = "005_valuations"
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
    empty_array_default = sa.text("'{}'::text[]") if is_postgres() else sa.text("'[]'")

    op.create_table(
        "promos",
        _uuid_pk(),
        sa.Column("bank_id", uuid_type(), sa.ForeignKey("banks.id"), nullable=False),
        sa.Column("external_source_id", sa.Text(), nullable=True),
        sa.Column("external_bank_key", sa.Text(), nullable=True),
        sa.Column("external_checksum", sa.Text(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("promo_type", sa.Text(), nullable=False),
        sa.Column("title_th", sa.Text(), nullable=False),
        sa.Column("title_en", sa.Text(), nullable=True),
        sa.Column("description_th", sa.Text(), nullable=True),
        sa.Column("description_en", sa.Text(), nullable=True),
        sa.Column("merchant_name", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), nullable=True),
        sa.Column("discount_type", sa.Text(), nullable=True),
        sa.Column("discount_value", sa.Text(), nullable=True),
        sa.Column("discount_amount", sa.Numeric(10, 2), nullable=True),
        sa.Column("discount_unit", sa.Text(), nullable=True),
        sa.Column("minimum_spend", sa.Numeric(10, 2), nullable=True),
        sa.Column("valid_from", sa.Date(), nullable=True),
        sa.Column("valid_until", sa.Date(), nullable=True),
        sa.Column("terms_and_conditions", sa.Text(), nullable=True),
        sa.Column(
            "raw_data",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column(
            "relevance_tags",
            string_array_type(),
            nullable=False,
            server_default=empty_array_default,
        ),
        sa.Column(
            "active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true") if is_postgres() else sa.text("1"),
        ),
        sa.Column(
            "last_synced_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.CheckConstraint(
            "promo_type IN ('category_bonus','cashback','transfer_bonus','signup',"
            "'statement_credit','dining_program')",
            name="promos_promo_type_check",
        ),
        sa.CheckConstraint(
            "discount_type IS NULL OR discount_type IN "
            "('percentage','cashback','discount','points')",
            name="promos_discount_type_check",
        ),
        sa.CheckConstraint(
            "discount_unit IS NULL OR discount_unit IN ('thb','percent','points','x_multiplier')",
            name="promos_discount_unit_check",
        ),
    )

    # Partial unique index — Postgres only. SQLite doesn't honor partial UNIQUE
    # via alembic; enforce via app-layer check for now.
    if is_postgres():
        op.create_index(
            "idx_promos_external_unique",
            "promos",
            ["external_bank_key", "external_source_id"],
            unique=True,
            postgresql_where=sa.text("external_source_id IS NOT NULL"),
        )

    op.create_index("idx_promos_bank_active", "promos", ["bank_id", "active"])
    op.create_index("idx_promos_valid_until", "promos", ["valid_until"])

    op.create_table(
        "promo_card_map",
        sa.Column(
            "promo_id",
            uuid_type(),
            sa.ForeignKey("promos.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "card_id",
            uuid_type(),
            sa.ForeignKey("cards.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("promo_id", "card_id", name="promo_card_map_pkey"),
    )


def downgrade() -> None:
    op.drop_table("promo_card_map")
    op.drop_index("idx_promos_valid_until", table_name="promos")
    op.drop_index("idx_promos_bank_active", table_name="promos")
    if is_postgres():
        op.drop_index("idx_promos_external_unique", table_name="promos")
    op.drop_table("promos")
