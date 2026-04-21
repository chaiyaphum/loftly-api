"""SSE streaming tests for POST /v1/selector."""

from __future__ import annotations

from httpx import AsyncClient


def _payload() -> dict[str, object]:
    return {
        "monthly_spend_thb": 80_000,
        "spend_categories": {
            "dining": 20_000,
            "online": 20_000,
            "travel": 20_000,
            "grocery": 10_000,
            "other": 10_000,
        },
        "current_cards": [],
        "goal": {"type": "miles", "currency_preference": "UOB_REWARDS"},
        "locale": "th",
    }


async def test_sse_stream_emits_envelope_and_done(seeded_client: AsyncClient) -> None:
    headers = {"Accept": "text/event-stream"}
    async with seeded_client.stream(
        "POST", "/v1/selector", json=_payload(), headers=headers
    ) as resp:
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers["content-type"]
        body = "".join([chunk async for chunk in resp.aiter_text()])

    # Verify we saw at least envelope + done. Rationale chunks are optional
    # depending on rationale length.
    assert "event: envelope" in body
    assert "event: done" in body
    # Envelope payload should carry the stack JSON (even if empty).
    assert "session_id" in body


async def test_json_mode_still_works_without_accept(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post("/v1/selector", json=_payload())
    assert resp.status_code == 200
    body = resp.json()
    assert "session_id" in body
    assert "stack" in body
