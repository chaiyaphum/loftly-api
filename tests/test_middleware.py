"""Middleware tests — trace_id propagation + response header injection."""

from __future__ import annotations

from httpx import AsyncClient


async def test_trace_id_header_set_on_every_response(client: AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert "x-trace-id" in resp.headers
    assert len(resp.headers["x-trace-id"]) >= 8


async def test_trace_id_echoes_incoming_header(client: AsyncClient) -> None:
    resp = await client.get("/healthz", headers={"X-Trace-Id": "my-trace-abc123"})
    assert resp.status_code == 200
    assert resp.headers["x-trace-id"] == "my-trace-abc123"


async def test_trace_id_distinct_per_request(client: AsyncClient) -> None:
    r1 = await client.get("/healthz")
    r2 = await client.get("/healthz")
    assert r1.headers["x-trace-id"] != r2.headers["x-trace-id"]
