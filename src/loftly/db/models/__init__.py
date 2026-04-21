"""SQLAlchemy declarative models.

Keep model definitions portable — tests use aiosqlite (SCHEMA.md constraint). Any
Postgres-specific DDL (pgcrypto, triggers, GIN, partial indexes) lives in
Alembic migrations only, not on the model.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, ClassVar

from sqlalchemy import CHAR, TIMESTAMP, TypeDecorator
from sqlalchemy.orm import DeclarativeBase


class GUID(TypeDecorator[uuid.UUID]):
    """Portable UUID: uses Postgres UUID when available, CHAR(36) otherwise.

    Stored as native uuid in Postgres, string in SQLite.
    """

    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        if dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import UUID

            return dialect.type_descriptor(UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(
        self, value: uuid.UUID | str | None, dialect: Any
    ) -> str | uuid.UUID | None:
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        return str(value)

    def process_result_value(self, value: Any, dialect: Any) -> uuid.UUID | None:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""

    type_annotation_map: ClassVar[dict[Any, Any]] = {
        uuid.UUID: GUID,
        datetime: TIMESTAMP(timezone=True),
    }


# Import models so they register with Base.metadata.
# (These imports must follow `Base` to avoid circular refs; the `noqa: E402`
# suppresses the import-at-top rule for that specific reason.)
from loftly.db.models.bank import Bank  # noqa: E402
from loftly.db.models.card import Card  # noqa: E402
from loftly.db.models.consent import UserConsent  # noqa: E402
from loftly.db.models.user import User  # noqa: E402

__all__ = [
    "Bank",
    "Base",
    "Card",
    "User",
    "UserConsent",
]
