"""Shared helpers for Alembic migrations.

Keeps migration DDL portable: Postgres gets rich types (UUID, JSONB, ARRAY,
GIN, partial unique), SQLite gets equivalent CHAR(36)/JSON/text fallbacks so
the full chain runs against aiosqlite scratch DBs without error.

Migrations import from here — `src` is on sys.path via alembic.ini's
`prepend_sys_path`.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


def is_postgres() -> bool:
    """True when the bound Alembic connection targets PostgreSQL."""
    return op.get_bind().dialect.name == "postgresql"


def uuid_type() -> sa.types.TypeEngine[Any]:
    """Portable UUID column type — native on Postgres, CHAR(36) elsewhere."""
    if is_postgres():
        return postgresql.UUID(as_uuid=True)
    return sa.CHAR(36)


def json_type() -> sa.types.TypeEngine[Any]:
    """Portable JSON column type — JSONB on Postgres, JSON elsewhere."""
    if is_postgres():
        return postgresql.JSONB()
    return sa.JSON()


def string_array_type() -> sa.types.TypeEngine[Any]:
    """Portable text[] column type — ARRAY on Postgres, JSON elsewhere.

    SQLite stores lists as JSON; Postgres uses ARRAY so GIN indexes work.
    """
    if is_postgres():
        return postgresql.ARRAY(sa.Text())
    return sa.JSON()


def now_default() -> sa.sql.elements.TextClause:
    """Portable `now()` server default for timestamptz columns."""
    return sa.text("now()") if is_postgres() else sa.text("CURRENT_TIMESTAMP")
