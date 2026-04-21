"""Admin seed-round metrics exporter — `POST /v1/admin/metrics/export`.

Thin HTTP wrapper over ``loftly.jobs.metrics_export.build_export``. Returns the
JSON body inline (not a file download) because the data room is admin-only and
admins paste it into Google Drive / Notion manually for now.

Lives in its own module to keep admin.py (cards + articles + promos CRUD) from
growing; also means we can add future admin endpoints (cohort dashboards, etc.)
adjacent without diffing a 1.2k-line file.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, status

from loftly.api.auth import get_current_admin_id
from loftly.api.errors import LoftlyError
from loftly.jobs.metrics_export import build_export

router = APIRouter(prefix="/v1/admin", tags=["admin"])


@router.post(
    "/metrics/export",
    summary="Anonymized seed-round metrics export",
    status_code=status.HTTP_200_OK,
)
async def metrics_export(
    payload: dict[str, Any] = Body(default_factory=dict),
    _admin_id: uuid.UUID = Depends(get_current_admin_id),
) -> dict[str, Any]:
    """Return the anonymized metrics JSON for inclusion in the data room.

    Request body:

    ```
    {"as_of": "2026-10-01"}   // or full ISO datetime
    ```

    Omitting `as_of` defaults to "now". Response is the full export payload
    (see `docs/SEED_ROUND_DATA_ROOM.md` for the schema).
    """
    raw = payload.get("as_of")
    if raw is None:
        as_of = datetime.now(UTC)
    else:
        if not isinstance(raw, str):
            raise LoftlyError(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                code="invalid_as_of",
                message_en="`as_of` must be an ISO8601 date or datetime string.",
                message_th="รูปแบบวันที่ไม่ถูกต้อง",
            )
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise LoftlyError(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                code="invalid_as_of",
                message_en=f"Could not parse `as_of` {raw!r} as ISO8601.",
                message_th="ไม่สามารถแปลงค่า as_of ได้",
            ) from exc
        as_of = parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    return await build_export(as_of)


__all__ = ["router"]
