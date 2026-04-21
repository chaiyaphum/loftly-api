"""007 — affiliate_links, affiliate_clicks, affiliate_conversions.

Mirrors ../loftly/mvp/artifacts/schema.sql §007.

Revision ID: 007_affiliate
Revises: 006_promos
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
    uuid_type,
)

# revision identifiers, used by Alembic.
revision: str = "007_affiliate"
down_revision: str | None = "006_promos"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _uuid_pk(name: str = "id") -> sa.Column[object]:
    if is_postgres():
        return sa.Column(
            name,
            uuid_type(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        )
    return sa.Column(name, uuid_type(), primary_key=True)


def upgrade() -> None:
    op.create_table(
        "affiliate_links",
        _uuid_pk(),
        sa.Column(
            "card_id",
            uuid_type(),
            sa.ForeignKey("cards.id"),
            nullable=False,
        ),
        sa.Column("partner_id", sa.Text(), nullable=False),
        sa.Column("url_template", sa.Text(), nullable=False),
        sa.Column("campaign_id", sa.Text(), nullable=True),
        sa.Column("commission_model", sa.Text(), nullable=False),
        sa.Column("commission_amount_thb", sa.Numeric(10, 2), nullable=True),
        sa.Column(
            "active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true") if is_postgres() else sa.text("1"),
        ),
        sa.CheckConstraint(
            "commission_model IN ('cpa_approved','cpa_applied')",
            name="affiliate_links_commission_model_check",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_affiliate_links_card",
            "affiliate_links",
            ["card_id"],
            postgresql_where=sa.text("active = true"),
        )
    else:
        op.create_index("idx_affiliate_links_card", "affiliate_links", ["card_id"])

    op.create_table(
        "affiliate_clicks",
        _uuid_pk("click_id"),
        sa.Column(
            "user_id",
            uuid_type(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "affiliate_link_id",
            uuid_type(),
            sa.ForeignKey("affiliate_links.id"),
            nullable=False,
        ),
        sa.Column(
            "card_id",
            uuid_type(),
            sa.ForeignKey("cards.id"),
            nullable=False,
        ),
        sa.Column("partner_id", sa.Text(), nullable=False),
        sa.Column("placement", sa.Text(), nullable=False),
        sa.Column("utm_campaign", sa.Text(), nullable=True),
        sa.Column("referrer", sa.Text(), nullable=True),
        sa.Column("ip_hash", sa.LargeBinary(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.CheckConstraint(
            "placement IN ('review','selector_result','cards_index','promo')",
            name="affiliate_clicks_placement_check",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_affiliate_clicks_link_time",
            "affiliate_clicks",
            ["affiliate_link_id", sa.text("created_at DESC")],
        )
        op.create_index(
            "idx_affiliate_clicks_card_time",
            "affiliate_clicks",
            ["card_id", sa.text("created_at DESC")],
        )
        op.create_index(
            "idx_affiliate_clicks_user",
            "affiliate_clicks",
            ["user_id"],
            postgresql_where=sa.text("user_id IS NOT NULL"),
        )
    else:
        op.create_index(
            "idx_affiliate_clicks_link_time",
            "affiliate_clicks",
            ["affiliate_link_id", "created_at"],
        )
        op.create_index(
            "idx_affiliate_clicks_card_time",
            "affiliate_clicks",
            ["card_id", "created_at"],
        )
        op.create_index("idx_affiliate_clicks_user", "affiliate_clicks", ["user_id"])

    op.create_table(
        "affiliate_conversions",
        _uuid_pk(),
        sa.Column(
            "click_id",
            uuid_type(),
            sa.ForeignKey("affiliate_clicks.click_id"),
            nullable=False,
        ),
        sa.Column("partner_id", sa.Text(), nullable=False),
        sa.Column("conversion_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("commission_thb", sa.Numeric(10, 2), nullable=True),
        sa.Column(
            "received_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column(
            "raw_payload",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.CheckConstraint(
            "conversion_type IN ('application_submitted','application_approved',"
            "'application_rejected')",
            name="affiliate_conversions_conversion_type_check",
        ),
        sa.CheckConstraint(
            "status IN ('pending','confirmed','rejected','paid')",
            name="affiliate_conversions_status_check",
        ),
        sa.UniqueConstraint(
            "click_id",
            "partner_id",
            "conversion_type",
            name="affiliate_conversions_idem",
        ),
    )

    if is_postgres():
        op.create_index(
            "idx_affiliate_conversions_status",
            "affiliate_conversions",
            ["status", sa.text("received_at DESC")],
        )
    else:
        op.create_index(
            "idx_affiliate_conversions_status",
            "affiliate_conversions",
            ["status", "received_at"],
        )


def downgrade() -> None:
    op.drop_index("idx_affiliate_conversions_status", table_name="affiliate_conversions")
    op.drop_table("affiliate_conversions")

    op.drop_index("idx_affiliate_clicks_user", table_name="affiliate_clicks")
    op.drop_index("idx_affiliate_clicks_card_time", table_name="affiliate_clicks")
    op.drop_index("idx_affiliate_clicks_link_time", table_name="affiliate_clicks")
    op.drop_table("affiliate_clicks")

    op.drop_index("idx_affiliate_links_card", table_name="affiliate_links")
    op.drop_table("affiliate_links")
