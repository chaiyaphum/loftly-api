"""018 — index on (active, valid_until) for active-promo snapshot queries.

Index-only migration supporting POST_V1 Tier A fast-follow "Promo-Aware Card
Selector" (ratified 2026-04-22). The new `promo_snapshot` service (see
`src/loftly/selector/promo_snapshot.py`) filters promos by `active=true AND
(valid_until IS NULL OR valid_until >= today)` on every Selector request;
that query hits ~160 rows today but will grow as deal-harvester's coverage
extends to more banks (SCB/Krungsri/UOB/Amex TH per STRATEGY.md Q1).

Partial index on `active=true` keeps the index small (inactive promos never
match the filter) and speeds up Selector cache warmups where the snapshot
digest is re-computed. Also adds a covering index on `promo_card_map.card_id`
so the inverse join (rank cards by applicable promos) is efficient.

No table changes; zero-downtime. Respects the ≤4 migrations/month cap
(STRATEGY.md 2026-04-22 readyz decision) — this is migration 1 of 2 this
sprint (019 adds merchants_canonical for §9).

Revision ID: 018_promos_active_idx
Revises: 017_authors
Create Date: 2026-04-22
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from loftly.db.migration_helpers import is_postgres

revision: str = "018_promos_active_idx"
down_revision: str | None = "017_authors"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    if is_postgres():
        # CONCURRENTLY can't run inside a transaction — use raw SQL + autocommit
        # block. Alembic's default transactional_ddl would break this, so we use
        # op.execute which defers transaction boundary handling to Alembic config.
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_promos_active_valid_until "
            "ON promos (active, valid_until) WHERE active = true"
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_promo_card_map_card_id "
            "ON promo_card_map (card_id)"
        )
    else:
        # SQLite (tests) — partial indexes supported, but no CONCURRENTLY.
        op.create_index(
            "idx_promos_active_valid_until",
            "promos",
            ["active", "valid_until"],
            sqlite_where=None,
        )
        op.create_index(
            "idx_promo_card_map_card_id",
            "promo_card_map",
            ["card_id"],
        )


def downgrade() -> None:
    op.drop_index("idx_promo_card_map_card_id", table_name="promo_card_map")
    op.drop_index("idx_promos_active_valid_until", table_name="promos")
