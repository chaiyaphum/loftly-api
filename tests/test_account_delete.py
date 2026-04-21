"""Account delete — request, cancel, grace-period expiry, purge correctness."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import AffiliateClick, AffiliateLink
from loftly.db.models.card import Card as CardModel
from loftly.db.models.consent import UserConsent
from loftly.db.models.job import Job
from loftly.db.models.selector_session import SelectorSession
from loftly.db.models.user import User
from loftly.db.models.user_card import UserCard
from loftly.jobs.account_delete import run_due_purges
from tests.conftest import TEST_USER_ID


async def test_delete_request_creates_job(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    resp = await seeded_client.post("/v1/account/delete/request", headers=user_headers)
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "pending"
    assert body["grace_ends_at"] is not None

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        user = (await session.execute(select(User).where(User.id == TEST_USER_ID))).scalars().one()
        assert user.deleted_at is not None


async def test_delete_request_conflicts_on_duplicate(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    await seeded_client.post("/v1/account/delete/request", headers=user_headers)
    dup = await seeded_client.post("/v1/account/delete/request", headers=user_headers)
    assert dup.status_code == 409
    assert dup.json()["error"]["code"] == "delete_already_pending"


async def test_delete_cancel_happy_path(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    await seeded_client.post("/v1/account/delete/request", headers=user_headers)
    resp = await seeded_client.post("/v1/account/delete/cancel", headers=user_headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        user = (await session.execute(select(User).where(User.id == TEST_USER_ID))).scalars().one()
        assert user.deleted_at is None


async def test_delete_cancel_after_grace_expires(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    await seeded_client.post("/v1/account/delete/request", headers=user_headers)

    # Force-expire the grace period by editing `expires_at` in DB.
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        job = (
            (
                await session.execute(
                    select(Job).where(
                        Job.user_id == TEST_USER_ID,
                        Job.job_type == "account_delete_scheduled",
                    )
                )
            )
            .scalars()
            .one()
        )
        job.expires_at = datetime.now(UTC) - timedelta(hours=1)
        await session.commit()

    resp = await seeded_client.post("/v1/account/delete/cancel", headers=user_headers)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "grace_period_expired"


async def test_delete_status_reflects_transitions(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    nothing = await seeded_client.get("/v1/account/delete/status", headers=user_headers)
    assert nothing.json()["status"] == "not_requested"

    await seeded_client.post("/v1/account/delete/request", headers=user_headers)
    pending = await seeded_client.get("/v1/account/delete/status", headers=user_headers)
    assert pending.json()["status"] == "pending"

    await seeded_client.post("/v1/account/delete/cancel", headers=user_headers)
    cancelled = await seeded_client.get("/v1/account/delete/status", headers=user_headers)
    assert cancelled.json()["status"] == "cancelled"


async def test_purge_scrubs_pii_and_keeps_consents(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    sessionmaker = get_sessionmaker()
    # Seed user data: a UserCard, a consent row, a selector session, a click.
    async with sessionmaker() as session:
        card = (await session.execute(select(CardModel))).scalars().first()
        assert card is not None
        session.add(UserCard(user_id=TEST_USER_ID, card_id=card.id))
        session.add(
            UserConsent(
                user_id=TEST_USER_ID,
                purpose="optimization",
                granted=True,
                policy_version="2026-04-01",
                source="onboarding",
            )
        )
        session.add(
            SelectorSession(
                user_id=TEST_USER_ID,
                profile_hash="x" * 64,
                input={"monthly_spend_thb": 30000},
                output={"stack": []},
                provider="deterministic",
            )
        )
        link = AffiliateLink(
            card_id=card.id,
            partner_id="test-partner",
            url_template="https://example/{click_id}",
            commission_model="CPA",
        )
        session.add(link)
        await session.flush()
        session.add(
            AffiliateClick(
                click_id=uuid.uuid4(),
                user_id=TEST_USER_ID,
                affiliate_link_id=link.id,
                card_id=card.id,
                partner_id="test-partner",
                placement="card-detail",
            )
        )
        await session.commit()

    await seeded_client.post("/v1/account/delete/request", headers=user_headers)

    # Force expiry and run the purge executor.
    async with sessionmaker() as session:
        job = (
            (
                await session.execute(
                    select(Job).where(
                        Job.user_id == TEST_USER_ID,
                        Job.job_type == "account_delete_scheduled",
                    )
                )
            )
            .scalars()
            .one()
        )
        job.expires_at = datetime.now(UTC) - timedelta(hours=1)
        await session.commit()

    results = await run_due_purges()
    assert results, "expected at least one purge result"
    first = results[0]
    assert first["user_cards_deleted"] == 1
    assert first["selector_sessions_scrubbed"] == 1
    assert first["affiliate_clicks_unlinked"] == 1

    # Consent row must survive.
    async with sessionmaker() as session:
        consents = list(
            (await session.execute(select(UserConsent).where(UserConsent.user_id == TEST_USER_ID)))
            .scalars()
            .all()
        )
        assert len(consents) == 1

        # UserCard gone.
        cards_left = list(
            (await session.execute(select(UserCard).where(UserCard.user_id == TEST_USER_ID)))
            .scalars()
            .all()
        )
        assert cards_left == []

        # Selector session scrubbed.
        sess = (
            (
                await session.execute(
                    select(SelectorSession).where(SelectorSession.user_id == TEST_USER_ID)
                )
            )
            .scalars()
            .first()
        )
        assert sess is not None
        assert sess.input == {"purged": True}
        assert sess.output == {"purged": True}

        # User row scrubbed.
        user = (await session.execute(select(User).where(User.id == TEST_USER_ID))).scalars().one()
        assert user.email.startswith("purged+")
        assert user.oauth_subject.startswith("purged:")

        # Affiliate click unlinked but row still there.
        clicks = list((await session.execute(select(AffiliateClick))).scalars().all())
        assert len(clicks) == 1
        assert clicks[0].user_id is None
