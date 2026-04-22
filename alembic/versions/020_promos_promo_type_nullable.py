"""020 — relax promos.promo_type to nullable.

The sync job (`src/loftly/jobs/deal_harvester_sync.py`) now maps upstream
`discount_type` → `promo_type` per the table in `mvp/SCHEMA.md §9`:

    cashback   → cashback
    percentage → category_bonus
    discount   → category_bonus
    points     → category_bonus
    null/other → null   (admin backfills via CMS)

The pre-fix code silently defaulted unknown upstream values to
`category_bonus`, which polluted Selector promo-filter logic — a Starbucks
promo with `discount_type=null` would masquerade as a category bonus even
though we have no evidence it applies to any spend category. Null is the
honest answer; the Selector excludes null-typed promos from the promo-chip
and the admin CMS surfaces them in the mapping queue for classification.

Also relaxes the CHECK constraint to allow NULL so the DB enforces the
same contract as the mapping function.

Zero-downtime: `NOT NULL → NULL` on an existing column is a metadata-only
change in both Postgres and SQLite. No data rewrite, no lock upgrade
beyond `ACCESS EXCLUSIVE` for the DDL transaction itself (<10ms on a table
this size).

Sprint migration budget: 3 of 4 (018 idx, 019 merchants_canonical, 020 this).
Still within the ≤4/month cap from STRATEGY.md 2026-04-22.

Revision ID: 020_promos_promo_type_nullable
Revises: 019_merchants_canonical
Create Date: 2026-04-22
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from loftly.db.migration_helpers import is_postgres

revision: str = "020_promos_promo_type_nullable"
down_revision: str | None = "019_merchants_canonical"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Drop old CHECK constraint before altering column — on both backends,
    # a CHECK constraint referencing the column would block the NOT NULL
    # change on some backend configurations.
    if is_postgres():
        op.execute("ALTER TABLE promos DROP CONSTRAINT IF EXISTS promos_promo_type_check")
        op.execute("ALTER TABLE promos ALTER COLUMN promo_type DROP NOT NULL")
        op.execute(
            "ALTER TABLE promos ADD CONSTRAINT promos_promo_type_check "
            "CHECK (promo_type IS NULL OR promo_type IN ("
            "'category_bonus','cashback','transfer_bonus','signup',"
            "'statement_credit','dining_program'))"
        )
    else:
        # SQLite: use batch mode to recreate the table with relaxed column +
        # CHECK. Alembic's batch_alter_table handles the copy-rename dance.
        with op.batch_alter_table("promos") as batch_op:
            batch_op.alter_column("promo_type", nullable=True)
            batch_op.drop_constraint("promos_promo_type_check", type_="check")
            batch_op.create_check_constraint(
                "promos_promo_type_check",
                "promo_type IS NULL OR promo_type IN ("
                "'category_bonus','cashback','transfer_bonus','signup',"
                "'statement_credit','dining_program')",
            )


def downgrade() -> None:
    # Downgrade first sets any NULLs back to 'category_bonus' so the NOT NULL
    # upgrade doesn't fail. This is lossy — admin-classified nulls get folded
    # back into category_bonus — so avoid downgrading in prod.
    op.execute("UPDATE promos SET promo_type='category_bonus' WHERE promo_type IS NULL")

    if is_postgres():
        op.execute("ALTER TABLE promos DROP CONSTRAINT IF EXISTS promos_promo_type_check")
        op.execute("ALTER TABLE promos ALTER COLUMN promo_type SET NOT NULL")
        op.execute(
            "ALTER TABLE promos ADD CONSTRAINT promos_promo_type_check "
            "CHECK (promo_type IN ("
            "'category_bonus','cashback','transfer_bonus','signup',"
            "'statement_credit','dining_program'))"
        )
    else:
        with op.batch_alter_table("promos") as batch_op:
            batch_op.alter_column("promo_type", nullable=False)
            batch_op.drop_constraint("promos_promo_type_check", type_="check")
            batch_op.create_check_constraint(
                "promos_promo_type_check",
                "promo_type IN ("
                "'category_bonus','cashback','transfer_bonus','signup',"
                "'statement_credit','dining_program')",
            )
