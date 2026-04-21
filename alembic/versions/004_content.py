"""004 — articles.

Mirrors ../loftly/mvp/artifacts/schema.sql §004.

Revision ID: 004_content
Revises: 003_cards
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
revision: str = "004_content"
down_revision: str | None = "003_cards"
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
        "articles",
        _uuid_pk(),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("card_id", uuid_type(), sa.ForeignKey("cards.id"), nullable=True),
        sa.Column("article_type", sa.Text(), nullable=False),
        sa.Column("title_th", sa.Text(), nullable=False),
        sa.Column("title_en", sa.Text(), nullable=True),
        sa.Column("summary_th", sa.Text(), nullable=False),
        sa.Column("summary_en", sa.Text(), nullable=True),
        sa.Column("body_th", sa.Text(), nullable=False),
        sa.Column("body_en", sa.Text(), nullable=True),
        sa.Column(
            "best_for_tags",
            string_array_type(),
            nullable=False,
            server_default=empty_array_default,
        ),
        sa.Column(
            "state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'draft'"),
        ),
        sa.Column(
            "author_id",
            uuid_type(),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column("policy_version", sa.Text(), nullable=False),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=now_default(),
        ),
        sa.Column(
            "seo_meta",
            json_type(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.CheckConstraint(
            "article_type IN ('card_review','guide','news','comparison')",
            name="articles_article_type_check",
        ),
        sa.CheckConstraint(
            "state IN ('draft','review','published','archived')",
            name="articles_state_check",
        ),
        sa.UniqueConstraint("slug", name="articles_slug_key"),
    )

    if is_postgres():
        op.create_index(
            "idx_articles_state_published",
            "articles",
            ["state", sa.text("published_at DESC")],
        )
    else:
        op.create_index("idx_articles_state_published", "articles", ["state", "published_at"])

    op.create_index("idx_articles_card", "articles", ["card_id"])

    if is_postgres():
        op.execute("CREATE INDEX idx_articles_best_for ON articles USING GIN (best_for_tags);")
        op.execute(
            "CREATE TRIGGER trg_articles_updated_at BEFORE UPDATE ON articles "
            "FOR EACH ROW EXECUTE FUNCTION set_updated_at();"
        )


def downgrade() -> None:
    if is_postgres():
        op.execute("DROP TRIGGER IF EXISTS trg_articles_updated_at ON articles;")
        op.execute("DROP INDEX IF EXISTS idx_articles_best_for;")

    op.drop_index("idx_articles_card", table_name="articles")
    op.drop_index("idx_articles_state_published", table_name="articles")
    op.drop_table("articles")
