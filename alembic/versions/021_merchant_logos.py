"""021 — backfill merchants_canonical.logo_url for the 50 seeded brands.

Migration 019 seeded the 50 canonical merchants with `logo_url=NULL`, which
forced the `/merchants/[slug]` page to render a letter-monogram fallback. This
migration backfills a logo URL per slug from `loftly.db.seed.MERCHANT_LOGOS`,
pointing at Google's public favicon service (the low-friction substitute for
Clearbit's retired free logo API — see seed.py's note for rationale).

Idempotent: each UPDATE is keyed on `slug` and only sets `logo_url` when it
is currently NULL, so re-running on a partially-manually-set DB won't clobber
an admin-curated override. Merchants absent from the map are left alone.

Revision ID: 021_merchant_logos
Revises: 021_merchant_descriptions
Create Date: 2026-04-22
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from loftly.db.seed import MERCHANT_LOGOS

revision: str = "021_merchant_logos"
down_revision: str | None = "021_merchant_descriptions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    stmt = sa.text(
        "UPDATE merchants_canonical "
        "SET logo_url = :logo_url "
        "WHERE slug = :slug AND logo_url IS NULL"
    )
    for slug, logo_url in MERCHANT_LOGOS.items():
        bind.execute(stmt, {"slug": slug, "logo_url": logo_url})


def downgrade() -> None:
    # Clear only the URLs this migration could have set (i.e. rows whose
    # current `logo_url` exactly matches the seeded value). Admin-curated
    # overrides entered since the upgrade are preserved.
    bind = op.get_bind()
    stmt = sa.text(
        "UPDATE merchants_canonical SET logo_url = NULL WHERE slug = :slug AND logo_url = :logo_url"
    )
    for slug, logo_url in MERCHANT_LOGOS.items():
        bind.execute(stmt, {"slug": slug, "logo_url": logo_url})
