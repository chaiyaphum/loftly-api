"""Data-export flow — request, poll, HMAC-signed download."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient

from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.job import Job
from loftly.jobs.data_export import sign_download_token
from tests.conftest import TEST_USER_ID


async def _wait_for_done(job_id: str, client: AsyncClient, headers: dict[str, str]) -> dict:
    for _ in range(50):
        resp = await client.get(
            f"/v1/account/data-export/{job_id}",
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        if body["status"] == "done":
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id} never completed: {body}")


async def test_data_export_request_returns_queued_handle(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    """Smoke-check the endpoint without polling: 202 + queued status body."""
    resp = await seeded_client.post("/v1/account/data-export/request", headers=user_headers)
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert body["job_id"]


async def test_data_export_happy_path_download(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post("/v1/account/data-export/request", headers=user_headers)
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    job_id = body["job_id"]

    done = await _wait_for_done(job_id, seeded_client, user_headers)
    assert done["download_url"] is not None
    assert done["expires_at"] is not None

    download_resp = await seeded_client.get(done["download_url"])
    assert download_resp.status_code == 200
    payload = json.loads(download_resp.content.decode("utf-8"))
    assert payload["export_version"] == "1.0"
    assert payload["user"]["id"] == str(TEST_USER_ID)
    assert "consents" in payload and isinstance(payload["consents"], list)
    assert "user_cards" in payload
    assert "affiliate_clicks" in payload


async def test_data_export_rate_limit(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    for _ in range(2):
        ok = await seeded_client.post("/v1/account/data-export/request", headers=user_headers)
        assert ok.status_code == 202
    blocked = await seeded_client.post("/v1/account/data-export/request", headers=user_headers)
    assert blocked.status_code == 429
    assert blocked.json()["error"]["code"] == "rate_limited"


async def test_data_export_status_hidden_across_users(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    # Create a job owned by another user.
    sessionmaker = get_sessionmaker()
    foreign_user_id = uuid.UUID("00000000-0000-4000-8000-00000000abcd")
    from loftly.db.models.user import User

    async with sessionmaker() as session:
        session.add(
            User(
                id=foreign_user_id,
                email="other@loftly.test",
                oauth_provider="google",
                oauth_subject="other",
            )
        )
        foreign_job = Job(
            user_id=foreign_user_id,
            job_type="data_export",
            status="done",
        )
        session.add(foreign_job)
        await session.commit()
        foreign_job_id = foreign_job.id

    resp = await seeded_client.get(
        f"/v1/account/data-export/{foreign_job_id}",
        headers=user_headers,
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "job_not_found"


async def test_data_export_download_rejects_bad_token(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post("/v1/account/data-export/request", headers=user_headers)
    job_id = resp.json()["job_id"]
    await _wait_for_done(job_id, seeded_client, user_headers)

    bad = await seeded_client.get(
        f"/v1/account/data-export/{job_id}/download?token=garbage",
    )
    assert bad.status_code == 401
    assert bad.json()["error"]["code"] == "invalid_token"


async def test_data_export_download_token_expiry() -> None:
    """Signed token with past `exp` is rejected even with correct HMAC."""
    from loftly.jobs.data_export import verify_download_token

    settings = get_settings()
    job_id = uuid.uuid4()
    past_exp = datetime.now(UTC) - timedelta(minutes=5)
    token = sign_download_token(job_id, expires_at=past_exp, secret=settings.jwt_signing_key)
    assert verify_download_token(job_id, token, secret=settings.jwt_signing_key) is False


async def test_data_export_token_cannot_be_reused_on_different_job() -> None:
    from loftly.jobs.data_export import verify_download_token

    settings = get_settings()
    a = uuid.uuid4()
    b = uuid.uuid4()
    expires_at = datetime.now(UTC) + timedelta(minutes=30)
    tok = sign_download_token(a, expires_at=expires_at, secret=settings.jwt_signing_key)
    assert verify_download_token(a, tok, secret=settings.jwt_signing_key) is True
    assert verify_download_token(b, tok, secret=settings.jwt_signing_key) is False
