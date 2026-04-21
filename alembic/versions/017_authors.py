"""017 — authors table + articles.authors_id nullable FK.

Adds a first-class `authors` table so `/cards/[slug]` (loftly-web) can surface
a real byline instead of hardcoding "Loftly". The existing
`articles.author_id` FK → `users.id` records the *authoring user* (the editor
who drafted the piece) and is kept as-is; this migration adds a NEW nullable
column `articles.authors_id` → `authors.id` for the *display byline*.

Why a second column instead of repurposing `author_id`:
- `articles.author_id` is NOT NULL and points at `users.id` today. Renaming
  it would force a backfill to populate `authors` rows for every historical
  article, which the spec explicitly wants to defer.
- A nullable `authors_id` lets the frontend fall back to "Loftly" (the
  default org byline seeded below) whenever the column is NULL.

Seed:
- One stable organization author — id `10ff1170-0000-4000-8000-000000000001`
  (hand-chosen so references from tests / frontend stay stable across
  deploys; NOT randomly generated at seed time). slug='loftly',
  display_name='Loftly', role='organization'.

Revision ID: 017_authors
Revises: 016_users_last_login_at
Create Date: 2026-04-21
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.migration_helpers import is_postgres, now_default, uuid_type

revision: str = "017_authors"
down_revision: str | None = "016_users_last_login_at"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


LOFTLY_ORG_AUTHOR_ID = "10ff1170-0000-4000-8000-000000000001"


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
        "authors",
        _uuid_pk(),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("display_name_en", sa.Text(), nullable=True),
        sa.Column("bio_th", sa.Text(), nullable=True),
        sa.Column("bio_en", sa.Text(), nullable=True),
        sa.Column("role", sa.Text(), nullable=True),
        sa.Column("image_url", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.CheckConstraint(
            "role IS NULL OR role IN ('founder','contractor','organization')",
            name="authors_role_check",
        ),
        sa.UniqueConstraint("slug", name="authors_slug_key"),
    )
    op.create_index("idx_authors_slug", "authors", ["slug"])

    # Seed the default "Loftly" organization author. Stable UUID so that
    # any frontend / test that hard-codes the reference stays consistent.
    bind = op.get_bind()
    if is_postgres():
        bind.execute(
            sa.text(
                """
                INSERT INTO authors
                  (id, slug, display_name, display_name_en, role)
                VALUES
                  (:id, :slug, :name, :name_en, :role)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {
                "id": LOFTLY_ORG_AUTHOR_ID,
                "slug": "loftly",
                "name": "Loftly",
                "name_en": "Loftly",
                "role": "organization",
            },
        )
    else:
        exists = bind.execute(
            sa.text("SELECT 1 FROM authors WHERE id = :id"),
            {"id": LOFTLY_ORG_AUTHOR_ID},
        ).scalar()
        if not exists:
            bind.execute(
                sa.text(
                    """
                    INSERT INTO authors
                      (id, slug, display_name, display_name_en, role)
                    VALUES
                      (:id, :slug, :name, :name_en, :role)
                    """
                ),
                {
                    "id": LOFTLY_ORG_AUTHOR_ID,
                    "slug": "loftly",
                    "name": "Loftly",
                    "name_en": "Loftly",
                    "role": "organization",
                },
            )

    # articles.authors_id — nullable FK to authors. Left NULL for all existing
    # rows (no backfill) so the migration stays O(1) DDL. The frontend
    # interprets NULL as "use the default Loftly org byline", which matches
    # what loftly-web#21 already renders.
    #
    # SQLite can't ALTER TABLE to add a FK inline — use batch mode (copy-and-
    # move) so both aiosqlite (tests) and Postgres (prod) migrate cleanly.
    if is_postgres():
        op.add_column(
            "articles",
            sa.Column(
                "authors_id",
                uuid_type(),
                sa.ForeignKey("authors.id", name="fk_articles_authors_id"),
                nullable=True,
            ),
        )
    else:
        with op.batch_alter_table("articles") as batch:
            batch.add_column(
                sa.Column(
                    "authors_id",
                    uuid_type(),
                    sa.ForeignKey("authors.id", name="fk_articles_authors_id"),
                    nullable=True,
                )
            )
    op.create_index("idx_articles_authors_id", "articles", ["authors_id"])


def downgrade() -> None:
    op.drop_index("idx_articles_authors_id", table_name="articles")
    if is_postgres():
        op.drop_column("articles", "authors_id")
    else:
        with op.batch_alter_table("articles") as batch:
            batch.drop_column("authors_id")
    op.drop_index("idx_authors_slug", table_name="authors")
    op.drop_table("authors")
