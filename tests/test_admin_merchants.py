"""Tests for `/v1/admin/merchants/*` — merge, split, mapping review.

Covers §9.1 of POST_V1.md (Q18, ratified 2026-04-22). The admin routes are
not feature-flag-gated (unlike the public read surface); we only exercise
the happy paths + the rejection branches explicitly called out in spec.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from loftly.core.cache import get_cache
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import AuditLog
from loftly.db.models.bank import Bank
from loftly.db.models.merchant import (
    MerchantCanonical as MerchantCanonicalModel,
)
from loftly.db.models.merchant import (
    PromoMerchantCanonicalMap,
)
from loftly.db.models.promo import Promo

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_merchant(
    *,
    slug: str,
    display_th: str | None = None,
    display_en: str | None = None,
    status_: str = "active",
    merchant_type: str = "retail",
) -> uuid.UUID:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        row = MerchantCanonicalModel(
            slug=slug,
            display_name_th=display_th or slug,
            display_name_en=display_en or slug,
            merchant_type=merchant_type,
            status=status_,
        )
        session.add(row)
        await session.commit()
        return row.id


async def _insert_promo(raw_merchant: str = "Starbucks TH") -> uuid.UUID:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        bank = (await session.execute(select(Bank).where(Bank.slug == "kbank"))).scalars().one()
        promo = Promo(
            bank_id=bank.id,
            source_url="https://example.test/promo",
            title_th="Test promo",
            merchant_name=raw_merchant,
        )
        session.add(promo)
        await session.commit()
        return promo.id


async def _insert_mapping(
    *,
    promo_id: uuid.UUID,
    merchant_id: uuid.UUID,
    confidence: float = 0.95,
    method: str = "exact",
    mapped_at: datetime | None = None,
    reviewed: bool = False,
) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        row = PromoMerchantCanonicalMap(
            promo_id=promo_id,
            merchant_canonical_id=merchant_id,
            confidence=Decimal(str(confidence)),
            method=method,
            mapped_at=mapped_at or datetime.now(UTC),
            reviewed_at=datetime.now(UTC) if reviewed else None,
        )
        session.add(row)
        await session.commit()


async def _mapping_row(promo_id: uuid.UUID) -> PromoMerchantCanonicalMap | None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return (
            await session.execute(
                select(PromoMerchantCanonicalMap).where(
                    PromoMerchantCanonicalMap.promo_id == promo_id
                )
            )
        ).scalar_one_or_none()


async def _fetch_merchant(merchant_id: uuid.UUID) -> MerchantCanonicalModel | None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        return (
            await session.execute(
                select(MerchantCanonicalModel).where(MerchantCanonicalModel.id == merchant_id)
            )
        ).scalar_one_or_none()


async def _count_audit(action: str) -> int:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        rows = list(
            (await session.execute(select(AuditLog).where(AuditLog.action == action)))
            .scalars()
            .all()
        )
        return len(rows)


# ---------------------------------------------------------------------------
# Auth gating
# ---------------------------------------------------------------------------


async def test_merge_requires_admin(
    seeded_client: AsyncClient, user_headers: dict[str, str]
) -> None:
    target_id = await _insert_merchant(slug="starbucks-th")
    source_id = await _insert_merchant(slug="starbucks-thailand")
    resp = await seeded_client.post(
        f"/v1/admin/merchants/{target_id}/merge",
        json={"source_id": str(source_id)},
        headers=user_headers,
    )
    assert resp.status_code == 403


async def test_mapping_queue_requires_auth(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/merchants/mapping-queue")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


async def test_merge_happy_path_rewrites_mappings(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    target_id = await _insert_merchant(slug="sbucks")
    source_id = await _insert_merchant(slug="sbux")

    promo_ids = [await _insert_promo(f"Starbucks {i}") for i in range(3)]
    for pid in promo_ids:
        await _insert_mapping(promo_id=pid, merchant_id=source_id)

    # Pre-populate the page cache so we can assert invalidation.
    cache = get_cache()
    await cache.set("merchants:page:anon:sbux", {"stale": True}, 600)
    await cache.set("merchants:page:anon:sbucks", {"stale": True}, 600)

    resp = await seeded_client.post(
        f"/v1/admin/merchants/{target_id}/merge",
        json={"source_id": str(source_id), "reason": "duplicate"},
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {"ok": True, "promo_count": 3}

    # Mappings moved.
    for pid in promo_ids:
        row = await _mapping_row(pid)
        assert row is not None
        assert row.merchant_canonical_id == target_id

    # Source flipped.
    source = await _fetch_merchant(source_id)
    assert source is not None
    assert source.status == "merged"
    assert source.merged_into_id == target_id

    # Audit row written.
    assert await _count_audit("merchant.merge") == 1

    # Cache invalidated for both slugs.
    assert await cache.get("merchants:page:anon:sbux") is None
    assert await cache.get("merchants:page:anon:sbucks") is None


async def test_merge_rejects_source_equals_target(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    m_id = await _insert_merchant(slug="solo")
    resp = await seeded_client.post(
        f"/v1/admin/merchants/{m_id}/merge",
        json={"source_id": str(m_id)},
        headers=admin_headers,
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "source_equals_target"


async def test_merge_idempotent_when_already_merged(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    target_id = await _insert_merchant(slug="t")
    source_id = await _insert_merchant(slug="s")

    # First merge.
    first = await seeded_client.post(
        f"/v1/admin/merchants/{target_id}/merge",
        json={"source_id": str(source_id)},
        headers=admin_headers,
    )
    assert first.status_code == 200

    # Second merge into same target -> 200 with already_merged.
    second = await seeded_client.post(
        f"/v1/admin/merchants/{target_id}/merge",
        json={"source_id": str(source_id)},
        headers=admin_headers,
    )
    assert second.status_code == 200
    body = second.json()
    assert body["ok"] is True
    assert body["already_merged"] is True
    assert body["promo_count"] == 0


async def test_merge_rejects_already_merged_to_different_target(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    first_target = await _insert_merchant(slug="t1")
    second_target = await _insert_merchant(slug="t2")
    source_id = await _insert_merchant(slug="s")

    ok = await seeded_client.post(
        f"/v1/admin/merchants/{first_target}/merge",
        json={"source_id": str(source_id)},
        headers=admin_headers,
    )
    assert ok.status_code == 200

    # Source is no longer active -> conflict.
    conflict = await seeded_client.post(
        f"/v1/admin/merchants/{second_target}/merge",
        json={"source_id": str(source_id)},
        headers=admin_headers,
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "source_not_active"


# ---------------------------------------------------------------------------
# Split
# ---------------------------------------------------------------------------


async def test_split_with_new_merchant_creates_row(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    source_id = await _insert_merchant(slug="foodland")
    promo_id_keep = await _insert_promo("Foodland old")
    promo_id_move = await _insert_promo("Foodland Gourmet")
    await _insert_mapping(promo_id=promo_id_keep, merchant_id=source_id)
    await _insert_mapping(promo_id=promo_id_move, merchant_id=source_id)

    resp = await seeded_client.post(
        f"/v1/admin/merchants/{source_id}/split",
        json={
            "new_merchant": {
                "id": str(uuid.uuid4()),  # ignored by server
                "slug": "foodland-gourmet",
                "display_name_th": "ฟู้ดแลนด์ กูร์เมต์",
                "display_name_en": "Foodland Gourmet",
                "merchant_type": "retail",
                "alt_names": [],
            },
            "reassignments": [{"promo_id": str(promo_id_move)}],
            "reason": "separate brand",
        },
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["reassigned_count"] == 1
    new_id = uuid.UUID(body["new_merchant_id"])

    # Moved promo now points at the new canonical.
    moved = await _mapping_row(promo_id_move)
    assert moved is not None
    assert moved.merchant_canonical_id == new_id

    # Untouched promo stays put.
    kept = await _mapping_row(promo_id_keep)
    assert kept is not None
    assert kept.merchant_canonical_id == source_id

    # Source stays active (split != merge).
    src = await _fetch_merchant(source_id)
    assert src is not None
    assert src.status == "active"

    assert await _count_audit("merchant.split") == 1


async def test_split_with_existing_target(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    source_id = await _insert_merchant(slug="lotuses")
    target_id = await _insert_merchant(slug="lotus-premium")
    promo_id = await _insert_promo("Lotus's Premium")
    await _insert_mapping(promo_id=promo_id, merchant_id=source_id)

    resp = await seeded_client.post(
        f"/v1/admin/merchants/{source_id}/split",
        json={
            "new_merchant_id": str(target_id),
            "reassignments": [{"promo_id": str(promo_id)}],
        },
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["new_merchant_id"] == str(target_id)
    assert body["reassigned_count"] == 1

    moved = await _mapping_row(promo_id)
    assert moved is not None
    assert moved.merchant_canonical_id == target_id


async def test_split_requires_exactly_one_target(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    source_id = await _insert_merchant(slug="somewhere")
    resp = await seeded_client.post(
        f"/v1/admin/merchants/{source_id}/split",
        json={"reassignments": [{"promo_id": str(uuid.uuid4())}]},
        headers=admin_headers,
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "missing_target"


# ---------------------------------------------------------------------------
# Mapping queue
# ---------------------------------------------------------------------------


async def test_mapping_queue_lists_only_unreviewed(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    m_id = await _insert_merchant(slug="starbucks")
    pid_open = await _insert_promo("Starbucks A")
    pid_done = await _insert_promo("Starbucks B")
    await _insert_mapping(promo_id=pid_open, merchant_id=m_id, confidence=0.9)
    await _insert_mapping(promo_id=pid_done, merchant_id=m_id, confidence=0.9, reviewed=True)

    resp = await seeded_client.get("/v1/admin/merchants/mapping-queue", headers=admin_headers)
    assert resp.status_code == 200
    body = resp.json()
    ids = {item["promo_id"] for item in body["data"]}
    assert str(pid_open) in ids
    assert str(pid_done) not in ids


async def test_mapping_queue_sorts_by_confidence_asc(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    m_id = await _insert_merchant(slug="sort-test")
    now = datetime.now(UTC)
    confidences = [0.55, 0.92, 0.71]
    promo_ids: list[uuid.UUID] = []
    for i, conf in enumerate(confidences):
        pid = await _insert_promo(f"raw-{i}")
        await _insert_mapping(
            promo_id=pid,
            merchant_id=m_id,
            confidence=conf,
            method="llm",
            mapped_at=now - timedelta(minutes=i),
        )
        promo_ids.append(pid)

    resp = await seeded_client.get("/v1/admin/merchants/mapping-queue", headers=admin_headers)
    body = resp.json()
    returned = [item for item in body["data"] if item["promo_id"] in {str(p) for p in promo_ids}]
    returned_conf = [row["confidence"] for row in returned]
    assert returned_conf == sorted(returned_conf)
    assert returned_conf[0] == pytest.approx(0.55)


async def test_mapping_queue_filter_by_method(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    m_id = await _insert_merchant(slug="filter-test")
    p_llm = await _insert_promo("llm-x")
    p_exact = await _insert_promo("exact-x")
    await _insert_mapping(promo_id=p_llm, merchant_id=m_id, confidence=0.7, method="llm")
    await _insert_mapping(promo_id=p_exact, merchant_id=m_id, confidence=0.99, method="exact")

    resp = await seeded_client.get(
        "/v1/admin/merchants/mapping-queue?method=llm", headers=admin_headers
    )
    body = resp.json()
    ids = {item["promo_id"] for item in body["data"]}
    assert str(p_llm) in ids
    assert str(p_exact) not in ids


async def test_mapping_queue_review_approve_stamps_reviewed_at(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    m_id = await _insert_merchant(slug="approve-test")
    pid = await _insert_promo("needs review")
    await _insert_mapping(promo_id=pid, merchant_id=m_id, confidence=0.7, method="llm")

    resp = await seeded_client.post(
        f"/v1/admin/merchants/mapping-queue/{pid}/review",
        json={"approved": True, "reason": "LGTM"},
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["stamped"] is True

    row = await _mapping_row(pid)
    assert row is not None
    assert row.reviewed_at is not None
    assert row.reviewed_by is not None
    assert await _count_audit("merchant.mapping_reviewed") == 1


async def test_mapping_queue_review_reject_rewrites_mapping(
    seeded_client: AsyncClient, admin_headers: dict[str, str]
) -> None:
    src_merchant = await _insert_merchant(slug="wrong")
    correct_merchant = await _insert_merchant(slug="correct")
    pid = await _insert_promo("ambiguous")
    await _insert_mapping(promo_id=pid, merchant_id=src_merchant, confidence=0.65, method="fuzzy")

    resp = await seeded_client.post(
        f"/v1/admin/merchants/mapping-queue/{pid}/review",
        json={
            "approved": False,
            "new_merchant_canonical_id": str(correct_merchant),
            "reason": "wrong match",
        },
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["rewrote_to"] == str(correct_merchant)

    row = await _mapping_row(pid)
    assert row is not None
    assert row.merchant_canonical_id == correct_merchant
    assert row.method == "manual"
    assert row.reviewed_at is not None
