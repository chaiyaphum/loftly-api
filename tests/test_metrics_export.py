"""Tests for the seed-round anonymized metrics exporter.

Covers:
- Admin-only auth gate on `/v1/admin/metrics/export` (401 without token).
- Aggregation math: seeded users / consents / selector sessions /
  affiliate clicks + conversions / articles produce the expected counts,
  rates, and time-bucketed series.
- PII scan: the serialized payload contains no emails, no UUIDs, no
  digit-only strings longer than 9 chars (which would indicate phone
  numbers or raw IDs slipped through).
- `run_export` writes a parseable JSON file to disk and returns the same
  payload the HTTP route returns.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import (
    AffiliateClick,
    AffiliateConversion,
    AffiliateLink,
)
from loftly.db.models.article import Article
from loftly.db.models.audit import SyncRun
from loftly.db.models.card import Card as CardModel
from loftly.db.models.consent import UserConsent
from loftly.db.models.selector_session import SelectorSession
from loftly.db.models.user import User
from loftly.jobs.metrics_export import build_export, run_export

# `as_of` fixed so retention/bucket math is deterministic across runs.
AS_OF = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


async def _seed_metrics_fixtures() -> dict[str, object]:
    """Insert a small but non-trivial dataset that exercises every aggregation.

    Returns a dict of handles (user_ids, card_id, click_ids) so individual
    tests can cross-check per-entity counts without duplicating the setup.
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # 5 regular users spread across the 12-week retention window.
        user_ids: list[uuid.UUID] = []
        for i in range(5):
            uid = uuid.uuid4()
            user_ids.append(uid)
            session.add(
                User(
                    id=uid,
                    email=f"seed-user-{i}@loftly.test",
                    oauth_provider="google",
                    oauth_subject=f"seed-subject-{i}",
                )
            )
        await session.flush()

        # Consent rows — 4/5 granted `analytics_cookies`, 3/5 granted `email_marketing`.
        for idx, uid in enumerate(user_ids):
            session.add(
                UserConsent(
                    user_id=uid,
                    purpose="analytics_cookies",
                    granted=idx != 0,  # 4 granted
                    policy_version="2026-04-01",
                    source="test",
                    granted_at=AS_OF - timedelta(days=20),
                )
            )
            session.add(
                UserConsent(
                    user_id=uid,
                    purpose="email_marketing",
                    granted=idx < 3,  # 3 granted
                    policy_version="2026-04-01",
                    source="test",
                    granted_at=AS_OF - timedelta(days=20),
                )
            )
        await session.flush()

        # Selector sessions:
        # - 3 sessions in the last 7d (WAU=3 unique users)
        # - 1 additional session 20d old (same user → MAU=4 unique)
        # - 1 more session 60d old (outside MAU window)
        card_id = (
            await session.execute(select(CardModel.id).where(CardModel.slug == "kbank-wisdom"))
        ).scalar_one()

        def _output(slug: str, latency_ms: float = 123.4) -> dict[str, object]:
            return {
                "stack": [{"slug": slug, "rank": 1}],
                "latency_ms": latency_ms,
            }

        # User 0 — 2 recent sessions (dedup WAU to 1)
        session.add(
            SelectorSession(
                user_id=user_ids[0],
                profile_hash="a" * 64,
                input={"goal": "miles"},
                output=_output("kbank-wisdom", 100.0),
                provider="deterministic",
                created_at=AS_OF - timedelta(days=1),
            )
        )
        session.add(
            SelectorSession(
                user_id=user_ids[0],
                profile_hash="b" * 64,
                input={"goal": "miles"},
                output=_output("kbank-wisdom", 140.0),
                provider="deterministic",
                created_at=AS_OF - timedelta(days=3),
            )
        )
        # User 1 — 1 recent session
        session.add(
            SelectorSession(
                user_id=user_ids[1],
                profile_hash="c" * 64,
                input={"goal": "miles"},
                output=_output("kbank-wisdom", 180.0),
                provider="deterministic",
                created_at=AS_OF - timedelta(days=5),
            )
        )
        # User 2 — 1 session 20d old (in MAU but not WAU)
        session.add(
            SelectorSession(
                user_id=user_ids[2],
                profile_hash="d" * 64,
                input={"goal": "miles"},
                output=_output("kbank-wisdom", 220.0),
                provider="deterministic",
                created_at=AS_OF - timedelta(days=20),
            )
        )
        # User 3 — 1 session 60d old (outside MAU)
        session.add(
            SelectorSession(
                user_id=user_ids[3],
                profile_hash="e" * 64,
                input={"goal": "miles"},
                output=_output("kbank-wisdom", 260.0),
                provider="deterministic",
                created_at=AS_OF - timedelta(days=60),
            )
        )
        await session.flush()

        # Affiliate link + clicks + conversions.
        link = AffiliateLink(
            card_id=card_id,
            partner_id="test-partner",
            url_template="https://p.example.com/?cid={click_id}",
            commission_model="cpa_approved",
            active=True,
        )
        session.add(link)
        await session.flush()

        # User 0 clicks kbank-wisdom 1d after the selector rec → counts as top-1 conv.
        click0 = uuid.uuid4()
        session.add(
            AffiliateClick(
                click_id=click0,
                user_id=user_ids[0],
                affiliate_link_id=link.id,
                card_id=card_id,
                partner_id="test-partner",
                placement="selector_result",
                created_at=AS_OF - timedelta(hours=12),
            )
        )
        # User 1 clicks but not within 7d of their session — doesn't count
        # toward top1 conv (session was 5d ago, this click is 6d ago so it's
        # +(-1d) relative... keep simple: click BEFORE session → excluded).
        click1 = uuid.uuid4()
        session.add(
            AffiliateClick(
                click_id=click1,
                user_id=user_ids[1],
                affiliate_link_id=link.id,
                card_id=card_id,
                partner_id="test-partner",
                placement="cards_index",
                created_at=AS_OF - timedelta(days=10),  # before session @ -5d
            )
        )
        # Extra conversion on click0 for commission math.
        session.add(
            AffiliateConversion(
                click_id=click0,
                partner_id="test-partner",
                conversion_type="application_approved",
                status="confirmed",
                commission_thb=Decimal("500.00"),
                received_at=AS_OF - timedelta(days=2),
                raw_payload={},
            )
        )
        await session.flush()

        # Articles — 2 published (one is a card_review with valid schema flag).
        session.add(
            Article(
                slug="metrics-review-1",
                card_id=card_id,
                article_type="card_review",
                title_th="รีวิว 1",
                summary_th="สรุป",
                body_th="เนื้อหา",
                best_for_tags=[],
                state="published",
                author_id=user_ids[0],
                policy_version="2026-04-01",
                published_at=AS_OF - timedelta(days=10),
                updated_at=AS_OF - timedelta(days=5),
                seo_meta={"schema_review_valid": True},
            )
        )
        session.add(
            Article(
                slug="metrics-guide-1",
                card_id=None,
                article_type="guide",
                title_th="ไกด์ 1",
                summary_th="สรุป",
                body_th="เนื้อหา",
                best_for_tags=[],
                state="published",
                author_id=user_ids[0],
                policy_version="2026-04-01",
                published_at=AS_OF - timedelta(days=3),
                updated_at=AS_OF - timedelta(days=3),
                seo_meta={},
            )
        )
        # Draft — should NOT count.
        session.add(
            Article(
                slug="metrics-draft-1",
                card_id=card_id,
                article_type="card_review",
                title_th="ดราฟ",
                summary_th="สรุป",
                body_th="เนื้อหา",
                best_for_tags=[],
                state="draft",
                author_id=user_ids[0],
                policy_version="2026-04-01",
                published_at=None,
                updated_at=AS_OF - timedelta(days=1),
                seo_meta={},
            )
        )

        # Selector eval run — upstream=20, inserted=17 → recall=0.85
        session.add(
            SyncRun(
                source="selector_eval",
                status="ok",
                upstream_count=20,
                inserted_count=17,
                updated_count=0,
                deactivated_count=0,
                mapping_queue_added=0,
                started_at=AS_OF - timedelta(days=1),
                finished_at=AS_OF - timedelta(days=1, minutes=-1),
            )
        )
        await session.commit()

    return {
        "user_ids": user_ids,
        "card_id": card_id,
        "click0": click0,
        "click1": click1,
    }


@pytest_asyncio.fixture
async def metrics_seeded(seeded_db: object) -> dict[str, object]:
    _ = seeded_db
    return await _seed_metrics_fixtures()


# ---------------------------------------------------------------------------
# Unit-level aggregation tests
# ---------------------------------------------------------------------------


async def test_build_export_top_level_schema(metrics_seeded: dict[str, object]) -> None:
    _ = metrics_seeded
    payload = await build_export(AS_OF)
    assert payload["schema_version"] == "1.0"
    for key in (
        "generated_at",
        "as_of",
        "window_days",
        "users",
        "selector",
        "affiliate",
        "content",
        "llm_costs",
        "system",
        "disclaimers",
    ):
        assert key in payload, f"missing {key}"


async def test_user_counts_and_consent_rate(metrics_seeded: dict[str, object]) -> None:
    _ = metrics_seeded
    payload = await build_export(AS_OF)
    users = payload["users"]
    # 5 seeded + TEST_USER + TEST_ADMIN + SYSTEM_USER = 8.
    assert users["total_registered"] == 8
    # WAU: users 0 + 1 active in last 7d (user 0 has 2 sessions; dedup = 1).
    assert users["wau"] == 2
    # MAU: users 0, 1, 2 active in last 30d.
    assert users["mau"] == 3

    # Retention curve has exactly 12 weekly buckets.
    assert len(users["retention_weekly"]) == 12
    # Most-recent bucket should have 2 active users (matching WAU within
    # the current ISO week).
    assert users["retention_weekly"][-1]["active_users"] >= 1

    # Consent — analytics_cookies: 4/5 granted, email_marketing: 3/5 granted.
    ac = users["consent_grant_rate"]["analytics_cookies"]
    em = users["consent_grant_rate"]["email_marketing"]
    assert ac["users_prompted"] == 5
    assert ac["users_granted"] == 4
    assert ac["grant_rate"] == 0.8
    assert em["users_granted"] == 3
    assert em["grant_rate"] == 0.6


async def test_selector_metrics(metrics_seeded: dict[str, object]) -> None:
    _ = metrics_seeded
    payload = await build_export(AS_OF)
    sel = payload["selector"]
    # 4 sessions in last 30d (excluding the 60-day old one).
    assert sel["invocations"] == 4
    assert sel["unique_users"] == 3
    # Avg latency over 4 sessions: (100+140+180+220)/4 = 160.0
    assert sel["avg_latency_ms"] == 160.0
    # Top-1 conversion: user 0's click @ -12h is within 7d of both sessions.
    # Two sessions for user 0, both have stack[0]=kbank-wisdom and each
    # finds a click within window → 2 conversions out of 4 eligible.
    # User 2 session @ -20d: click0 @ -12h is within 7d? No, diff is +19.5d.
    # So sample is 4 (all 4 sessions with user_id), top1_conversions = 2.
    assert sel["top1_sample_size"] == 4
    assert sel["top1_conversion_rate"] == 0.5
    # Eval recall from the SyncRun we seeded.
    assert sel["eval_top1_recall"] == 0.85


async def test_affiliate_metrics(metrics_seeded: dict[str, object]) -> None:
    _ = metrics_seeded
    payload = await build_export(AS_OF)
    aff = payload["affiliate"]
    assert aff["total_clicks"] == 2
    assert aff["unique_users_clicked"] == 2
    assert aff["conversions"] == 1
    assert aff["conversion_rate"] == 0.5
    # 6 monthly buckets, Oct 2026 is the last.
    assert len(aff["commission_thb_by_month"]) == 6
    assert aff["commission_thb_by_month"][-1]["month_start"] == "2026-10-01"
    # Conversion received at AS_OF - 2d = 2026-09-29 → September bucket,
    # which is the second-to-last entry.
    sep_bucket = aff["commission_thb_by_month"][-2]
    assert sep_bucket["month_start"] == "2026-09-01"
    assert sep_bucket["commission_thb"] == 500.0
    # Top card is kbank-wisdom.
    assert aff["top_cards_by_conversions"]
    assert aff["top_cards_by_conversions"][0]["card_slug"] == "kbank-wisdom"
    assert aff["top_cards_by_conversions"][0]["conversions"] == 1


async def test_content_metrics(metrics_seeded: dict[str, object]) -> None:
    _ = metrics_seeded
    payload = await build_export(AS_OF)
    content = payload["content"]
    assert content["articles_published"] == 2
    assert content["distinct_cards_covered"] == 1
    # 1 published card_review with schema_review_valid → 1/1 = 1.0
    assert content["schema_review_validation_rate"] == 1.0
    # Ages positive.
    assert content["avg_update_age_days"] > 0


async def test_llm_and_system_are_placeholder_slots(metrics_seeded: dict[str, object]) -> None:
    _ = metrics_seeded
    payload = await build_export(AS_OF)
    assert "placeholder" in payload["llm_costs"]["source"]
    assert payload["llm_costs"]["prompt_cache_hit_rate"] is None
    assert "placeholder" in payload["system"]["source"]
    assert payload["system"]["uptime_prod_pct"] is None


# ---------------------------------------------------------------------------
# PII scan — the core guarantee
# ---------------------------------------------------------------------------


# Allow-list fields whose values are by-design non-PII strings that might
# otherwise trip a digit-only or hyphen-y regex (e.g. ISO dates, schema
# version). We only apply the allow-list to *leaf string* matches.
_ALLOWED_SHAPES = re.compile(
    r"^(\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})?)?|\d+\.\d+)$"
)
# UUID v4-ish.
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
# Digit-only strings longer than 9 chars (looks like a phone or raw ID).
_DIGITS_LONG_RE = re.compile(r"\b\d{10,}\b")


def _walk_strings(node: object) -> list[str]:
    out: list[str] = []
    if isinstance(node, str):
        out.append(node)
    elif isinstance(node, dict):
        for v in node.values():
            out.extend(_walk_strings(v))
    elif isinstance(node, list | tuple):
        for item in node:
            out.extend(_walk_strings(item))
    return out


async def test_export_contains_no_pii(metrics_seeded: dict[str, object]) -> None:
    _ = metrics_seeded
    payload = await build_export(AS_OF)
    serialized = json.dumps(payload, ensure_ascii=False)

    # Hard guards against known PII sources.
    assert "@" not in serialized, "@ char detected — email may have leaked"
    assert "seed-user" not in serialized
    assert "oauth_provider" not in serialized
    assert "oauth_subject" not in serialized

    # No UUIDs in the blob at all.
    uuids_found = _UUID_RE.findall(serialized)
    assert uuids_found == [], f"raw UUIDs leaked: {uuids_found[:3]}"

    # No digit-only 10+ chars except ISO date fragments (covered by regex since
    # dates include dashes).
    for candidate in _DIGITS_LONG_RE.findall(serialized):
        assert False, f"long digit run detected (possible ID/phone): {candidate!r}"

    # Every leaf string is either short, ISO-date-shaped, or a human-readable
    # label we explicitly emit. This is a stricter defence-in-depth check.
    for leaf in _walk_strings(payload):
        if len(leaf) <= 3:
            continue
        if _ALLOWED_SHAPES.match(leaf):
            continue
        # Known-safe substrings the exporter emits intentionally.
        allow_substrings = (
            "kbank",
            "scb",
            "ktc",
            "uob",
            "bbl",
            "citi",
            "placeholder",
            "wire Langfuse",
            "wire Grafana",
            "window_days",
            "staging",
            "prod",
            "data room",
            "analytics_cookies",
            "email_marketing",
            "card_review",
            "PII",
            "snapshot",
            "aggregate",
        )
        if any(tok in leaf for tok in allow_substrings):
            continue
        # It's fine for the export to include words like "Loftly" etc.
        if leaf.replace(" ", "").replace(".", "").isalpha():
            continue
        # Nothing else should be there.
        assert False, f"Unexpected leaf string in export (possible PII): {leaf!r}"


# ---------------------------------------------------------------------------
# HTTP route + file-writing wrapper
# ---------------------------------------------------------------------------


async def test_http_route_requires_admin(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.post("/v1/admin/metrics/export", json={})
    assert resp.status_code == 401


async def test_http_route_returns_export(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
    metrics_seeded: dict[str, object],
) -> None:
    _ = metrics_seeded
    resp = await seeded_client.post(
        "/v1/admin/metrics/export",
        headers=admin_headers,
        json={"as_of": AS_OF.date().isoformat()},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["schema_version"] == "1.0"
    assert body["users"]["total_registered"] >= 5


async def test_http_route_rejects_garbage_as_of(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
    metrics_seeded: dict[str, object],
) -> None:
    _ = metrics_seeded
    resp = await seeded_client.post(
        "/v1/admin/metrics/export",
        headers=admin_headers,
        json={"as_of": "not a date"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "invalid_as_of"


async def test_run_export_writes_file(
    metrics_seeded: dict[str, object], tmp_path: Path
) -> None:
    _ = metrics_seeded
    out_path = tmp_path / "nested" / "metrics-2026-10.json"
    payload = await run_export(str(out_path), AS_OF)
    assert out_path.exists()
    on_disk = json.loads(out_path.read_text(encoding="utf-8"))
    assert on_disk["schema_version"] == payload["schema_version"]
    assert on_disk["as_of"] == payload["as_of"]
