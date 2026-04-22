"""Admin affiliate CSV export — W17 per mvp/DEV_PLAN.md.

Contract under test:
- `GET /v1/admin/affiliate/stats.csv` streams a UTF-8 CSV with the canonical
  columns declared in `routes/admin.py::_AFFILIATE_CSV_COLUMNS`.
- Filters on `?partner_id=` and bounds on `?from=&to=` (ISO-8601 dates).
- Requires an admin JWT — either via `Authorization: Bearer` OR `?token=` so a
  plain `<a download>` anchor in the admin UI can trigger the download.
- Response headers include a disposition with `affiliate-stats-{from}-{to}.csv`.

We seed two clicks + one conversion on `kbank-wisdom` for `test-partner` and a
second partner's click, then assert aggregation + filtering.
"""

from __future__ import annotations

import csv
import io
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select

from loftly.db.engine import get_sessionmaker
from loftly.db.models.affiliate import (
    AffiliateClick,
    AffiliateConversion,
    AffiliateLink,
)
from loftly.db.models.card import Card as CardModel

_PARTNER = "test-partner"
_OTHER_PARTNER = "other-partner"

_EXPECTED_COLUMNS = [
    "date",
    "partner_id",
    "card_id",
    "card_name",
    "clicks",
    "unique_visitors",
    "conversions",
    "conversion_rate_pct",
    "commission_thb",
    "avg_time_to_convert_hours",
]


async def _seed_csv_fixtures() -> dict[str, object]:
    """Two clicks (same ip_hash) on kbank-wisdom for _PARTNER + one conversion;
    plus a single click for _OTHER_PARTNER on a different card, so filtering is
    visible in the output.
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        kbank_card_id = (
            await session.execute(select(CardModel.id).where(CardModel.slug == "kbank-wisdom"))
        ).scalar_one()

        # Reuse any second seeded card if available; otherwise fall back to the
        # same id so the test stays stable regardless of seed fixture growth.
        other_card_id = (
            await session.execute(
                select(CardModel.id).where(CardModel.slug != "kbank-wisdom").limit(1)
            )
        ).scalar_one_or_none() or kbank_card_id

        link = AffiliateLink(
            card_id=kbank_card_id,
            partner_id=_PARTNER,
            url_template="https://p.example.com/?cid={click_id}",
            commission_model="cpa_approved",
            active=True,
        )
        other_link = AffiliateLink(
            card_id=other_card_id,
            partner_id=_OTHER_PARTNER,
            url_template="https://o.example.com/?cid={click_id}",
            commission_model="cpa_approved",
            active=True,
        )
        session.add(link)
        session.add(other_link)
        await session.flush()

        click_ids = [uuid.uuid4() for _ in range(2)]
        now = datetime.now(UTC)
        for cid in click_ids:
            session.add(
                AffiliateClick(
                    click_id=cid,
                    affiliate_link_id=link.id,
                    card_id=kbank_card_id,
                    partner_id=_PARTNER,
                    placement="cards_index",
                    ip_hash=b"shared-ip-hash-32bytes-abcdef0123",
                    created_at=now - timedelta(hours=5),
                )
            )
        # One conversion, ~3h after click.
        session.add(
            AffiliateConversion(
                click_id=click_ids[0],
                partner_id=_PARTNER,
                conversion_type="application_approved",
                status="confirmed",
                commission_thb=Decimal("500.00"),
                received_at=now - timedelta(hours=2),
                raw_payload={},
            )
        )

        # Second partner click, same day.
        other_click_id = uuid.uuid4()
        session.add(
            AffiliateClick(
                click_id=other_click_id,
                affiliate_link_id=other_link.id,
                card_id=other_card_id,
                partner_id=_OTHER_PARTNER,
                placement="cards_index",
                ip_hash=b"another-ip-hash-32bytes-abcdef01",
                created_at=now - timedelta(hours=3),
            )
        )
        await session.commit()

        return {
            "kbank_card_id": str(kbank_card_id),
            "other_card_id": str(other_card_id),
        }


@pytest_asyncio.fixture
async def csv_seeded(seeded_db: object) -> dict[str, object]:
    _ = seeded_db
    return await _seed_csv_fixtures()


def _parse_csv(body: str) -> tuple[list[str], list[dict[str, str]]]:
    reader = csv.reader(io.StringIO(body))
    rows = list(reader)
    header = rows[0]
    data = [dict(zip(header, r, strict=False)) for r in rows[1:]]
    return header, data


async def test_csv_export_happy_path(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
    csv_seeded: dict[str, object],
) -> None:
    today = datetime.now(UTC).date()
    from_ = (today - timedelta(days=1)).isoformat()
    to_ = today.isoformat()
    resp = await seeded_client.get(
        "/v1/admin/affiliate/stats.csv",
        params={"from": from_, "to": to_},
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/csv")
    assert (
        resp.headers["content-disposition"]
        == f'attachment; filename="affiliate-stats-{from_}-{to_}.csv"'
    )

    header, rows = _parse_csv(resp.text)
    assert header == _EXPECTED_COLUMNS

    # Expect exactly 2 rows: one per partner.
    assert len(rows) == 2, rows

    partners = {r["partner_id"]: r for r in rows}
    assert _PARTNER in partners and _OTHER_PARTNER in partners

    me = partners[_PARTNER]
    assert me["card_id"] == csv_seeded["kbank_card_id"]
    assert int(me["clicks"]) == 2
    assert int(me["unique_visitors"]) == 1  # shared ip_hash -> dedupe to 1
    assert int(me["conversions"]) == 1
    assert float(me["conversion_rate_pct"]) == 50.00
    assert float(me["commission_thb"]) == 500.0
    avg = float(me["avg_time_to_convert_hours"])
    assert 2.5 <= avg <= 3.5

    other = partners[_OTHER_PARTNER]
    assert int(other["clicks"]) == 1
    assert int(other["conversions"]) == 0
    assert other["avg_time_to_convert_hours"] == ""  # no conversion -> blank


async def test_csv_export_filters_by_partner_id(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
    csv_seeded: dict[str, object],
) -> None:
    _ = csv_seeded
    resp = await seeded_client.get(
        "/v1/admin/affiliate/stats.csv",
        params={"partner_id": _PARTNER},
        headers=admin_headers,
    )
    assert resp.status_code == 200, resp.text
    _header, rows = _parse_csv(resp.text)
    assert len(rows) == 1
    assert rows[0]["partner_id"] == _PARTNER
    # Aggregate totals still add up.
    assert sum(int(r["clicks"]) for r in rows) == 2
    assert sum(float(r["commission_thb"]) for r in rows) == 500.0


async def test_csv_export_requires_admin(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/v1/admin/affiliate/stats.csv")
    assert resp.status_code == 401


async def test_csv_export_rejects_non_admin(
    seeded_client: AsyncClient,
    user_headers: dict[str, str],
) -> None:
    resp = await seeded_client.get("/v1/admin/affiliate/stats.csv", headers=user_headers)
    assert resp.status_code == 403


async def test_csv_export_accepts_token_query_param(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
    csv_seeded: dict[str, object],
) -> None:
    """Plain `<a download>` can only send query strings — so admin JWT via
    `?token=` must work identically to the Authorization header.
    """
    _ = csv_seeded
    bearer = admin_headers["Authorization"].split(" ", 1)[1]
    resp = await seeded_client.get("/v1/admin/affiliate/stats.csv", params={"token": bearer})
    assert resp.status_code == 200, resp.text
    header, rows = _parse_csv(resp.text)
    assert header == _EXPECTED_COLUMNS
    assert len(rows) >= 1


async def test_csv_export_invalid_date_returns_422(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    resp = await seeded_client.get(
        "/v1/admin/affiliate/stats.csv",
        params={"from": "not-a-date"},
        headers=admin_headers,
    )
    assert resp.status_code == 422


async def test_csv_export_empty_when_no_data(
    seeded_client: AsyncClient,
    admin_headers: dict[str, str],
) -> None:
    resp = await seeded_client.get("/v1/admin/affiliate/stats.csv", headers=admin_headers)
    assert resp.status_code == 200
    header, rows = _parse_csv(resp.text)
    assert header == _EXPECTED_COLUMNS
    assert rows == []
