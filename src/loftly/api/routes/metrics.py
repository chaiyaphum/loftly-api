"""Prometheus scrape endpoint — `GET /metrics`.

Exposes the OpenMetrics text format that Grafana Cloud / Prometheus scrapes
per `../loftly/mvp/artifacts/grafana/README.md`.

Auth model:
- **Not admin-protected.** Prometheus scrapers don't carry JWTs, and the
  metric surface contains no PII (route templates + aggregates). The primary
  control is the network ACL (Fly / DO private network; external scrapes
  come via an allow-list proxy).
- **Optional scrape-token guard.** When `LOFTLY_METRICS_SCRAPE_TOKEN` is set
  we require `?token=...` to match. That gives us a second line of defense
  in prod where the private network doesn't extend to Grafana Cloud.
- **Dev/staging** leave the env unset so local Prometheus setups and quick
  curl-debug work without ceremony.
"""

from __future__ import annotations

import os

from fastapi import APIRouter, Query, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from loftly.api.errors import LoftlyError
from loftly.core.settings import get_settings
from loftly.observability.prometheus import get_registry

router = APIRouter(tags=["observability"])


# Env var name kept in sync with `DEPLOYMENT.md` env catalog.
SCRAPE_TOKEN_ENV = "LOFTLY_METRICS_SCRAPE_TOKEN"


def _expected_token() -> str | None:
    """Return the configured scrape token, or None if unset."""
    return os.environ.get(SCRAPE_TOKEN_ENV) or None


@router.get(
    "/metrics",
    summary="Prometheus scrape endpoint (OpenMetrics text format)",
    responses={
        200: {"description": "Metric snapshot", "content": {"text/plain": {}}},
        401: {"description": "Scrape token required/invalid"},
    },
)
async def metrics(token: str | None = Query(default=None)) -> Response:
    """Return the current metric snapshot as `text/plain; version=0.0.4`.

    If `LOFTLY_METRICS_SCRAPE_TOKEN` is set **and** the environment is prod,
    require `?token=...` to match.  Staging/dev only enforce when explicitly
    configured, so local `curl :8080/metrics` keeps working.
    """
    settings = get_settings()
    expected = _expected_token()
    if expected is not None:
        if token is None or token != expected:
            raise LoftlyError(
                status_code=status.HTTP_401_UNAUTHORIZED,
                code="scrape_token_invalid",
                message_en="Invalid or missing metrics scrape token.",
                message_th="โทเคนสำหรับเก็บ metrics ไม่ถูกต้อง",
            )
    elif settings.is_prod:
        # Prod without a token configured is a misconfiguration — refuse
        # the scrape rather than leak metrics to the internet.
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="scrape_token_required",
            message_en="Metrics scrape token is required in production.",
            message_th="ต้องตั้งโทเคนสำหรับเก็บ metrics ในโปรดักชัน",
        )

    payload = generate_latest(get_registry())
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


__all__ = ["router"]
