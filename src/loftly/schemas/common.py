"""Shared schema primitives — Health, Pagination, JobHandle."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

HealthStatus = Literal["ok", "degraded"]


class Health(BaseModel):
    """Liveness/readiness payload — `openapi.yaml#Health`."""

    status: HealthStatus
    checks: dict[str, str] = Field(default_factory=dict)


class Pagination(BaseModel):
    """Cursor-based pagination envelope — `openapi.yaml#Pagination`."""

    cursor_next: str | None = None
    has_more: bool = False
    total_estimate: int | None = None


class JobHandle(BaseModel):
    """Async job handle — `openapi.yaml#JobHandle`."""

    job_id: str
    status: Literal["queued", "running", "done", "failed"]
