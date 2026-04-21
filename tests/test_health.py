"""Health-probe tests — `/healthz` always up, `/readyz` green when DB reachable."""

from __future__ import annotations

from httpx import AsyncClient


async def test_healthz_is_always_ok(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "checks" in body


async def test_readyz_is_ready_when_db_up(client: AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["checks"].get("database") == "ok"
