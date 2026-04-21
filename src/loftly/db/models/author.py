"""Author model — display byline for articles. See migration 017.

The `authors` table is intentionally separate from `users`:
- `users` rows are authentication identities (email, oauth provider, role).
- `authors` rows are editorial bylines — public-facing name, bio, photo —
  that may or may not correspond to a human with a user account. The default
  row is the Loftly organization itself (seeded in migration 017).

`articles.authors_id` is a nullable FK; NULL means "use the default Loftly
organization byline" on the rendering side. The existing `articles.author_id`
FK → `users.id` is the authoring user (editorial ownership) and is unrelated
to what gets rendered in the byline.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Index, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from loftly.db.models import Base


class Author(Base):
    """Editorial byline row."""

    __tablename__ = "authors"
    __table_args__ = (
        UniqueConstraint("slug", name="authors_slug_key"),
        Index("idx_authors_slug", "slug"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    display_name_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    bio_th: Mapped[str | None] = mapped_column(Text, nullable=True)
    bio_en: Mapped[str | None] = mapped_column(Text, nullable=True)
    # One of 'founder' | 'contractor' | 'organization'; nullable because
    # early seed data may not distinguish.
    role: Mapped[str | None] = mapped_column(Text, nullable=True)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
