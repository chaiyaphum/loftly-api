"""003 — cards, user_cards.

Mirrors ../loftly/mvp/artifacts/schema.sql §003.

Revision ID: 003_cards
Revises: 002_banks_currencies
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, json_type, now_default, uuid_type

# revision identifiers, used by Alembic.
revision: str = "003_cards"
down_revision: str | None = "002_banks_currencies"
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
        "cards",
        _uuid_pk(),
        sa.Column(
            "bank_id",
            uuid_type(),
            sa.ForeignKey("banks.id"),
            nullable=False,
        ),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("tier", sa.Text(), nullable=True),
        sa.Column("network", sa.Text(), nullable=False),
        sa.Column("annual_fee_thb", sa.Numeric(10, 2), nullable=True),
        sa.Column("annual_fee_waiver", sa.Text(), nullable=True),
        sa.Column("min_income_thb", sa.Numeric(10, 2), nullable=True),
        sa.Column("min_age", sa.Integer(), nullable=True),
        sa.Column(
            "earn_currency_id",
            uuid_type(),
            sa.ForeignKey("loyalty_currencies.id"),
            nullable=False,
        ),
        sa.Column("earn_rate_local", json_type(), nullable=False),
        sa.Column("earn_rate_foreign", json_type(), nullable=True),
        sa.Column("earn_cap_monthly_thb", sa.Numeric(10, 2), nullable=True),
        sa.Column(
            "benefits",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("signup_bonus", json_type(), nullable=True),
        sa.Column("description_th", sa.Text(), nullable=True),
        sa.Column("description_en", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.CheckConstraint(
            "network IN ('Visa','Mastercard','Amex','JCB','UnionPay')",
            name="cards_network_check",
        ),
        sa.CheckConstraint(
            "status IN ('active','inactive','archived')",
            name="cards_status_check",
        ),
        sa.UniqueConstraint("slug", name="cards_slug_key"),
    )

    op.create_index("idx_cards_bank", "cards", ["bank_id"])
    op.create_index("idx_cards_status", "cards", ["status"])
    op.create_index("idx_cards_earn_currency", "cards", ["earn_currency_id"])

    if is_postgres():
        op.execute(
            "CREATE TRIGGER trg_cards_updated_at BEFORE UPDATE ON cards "
            "FOR EACH ROW EXECUTE FUNCTION set_updated_at();"
        )

    op.create_table(
        "user_cards",
        _uuid_pk(),
        sa.Column(
            "user_id",
            uuid_type(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "card_id",
            uuid_type(),
            sa.ForeignKey("cards.id"),
            nullable=False,
        ),
        sa.Column(
            "declared_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column("declared_balance_points", sa.Integer(), nullable=True),
        sa.UniqueConstraint("user_id", "card_id", name="user_cards_unique"),
    )

    op.create_index("idx_user_cards_user", "user_cards", ["user_id"])


def downgrade() -> None:
    op.drop_index("idx_user_cards_user", table_name="user_cards")
    op.drop_table("user_cards")

    if is_postgres():
        op.execute("DROP TRIGGER IF EXISTS trg_cards_updated_at ON cards;")

    op.drop_index("idx_cards_earn_currency", table_name="cards")
    op.drop_index("idx_cards_status", table_name="cards")
    op.drop_index("idx_cards_bank", table_name="cards")
    op.drop_table("cards")
