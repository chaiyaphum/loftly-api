"""Audit-log helpers.

Thin convenience around `AuditLog` so route handlers don't have to remember
the exact column names. Does NOT commit — caller controls the transaction so
an audit row and the business write land atomically.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from loftly.db.models.audit import AuditLog


async def log_action(
    session: AsyncSession,
    *,
    actor_id: uuid.UUID,
    action: str,
    subject_type: str,
    subject_id: uuid.UUID | None = None,
    metadata: dict[str, Any] | None = None,
    ip_hash: bytes | None = None,
) -> AuditLog:
    """Append an `audit_log` row. Caller commits the enclosing transaction."""
    row = AuditLog(
        actor_id=actor_id,
        action=action,
        subject_type=subject_type,
        subject_id=subject_id,
        meta=metadata or {},
        ip_hash=ip_hash,
    )
    session.add(row)
    return row


__all__ = ["log_action"]
