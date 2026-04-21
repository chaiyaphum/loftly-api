"""Manual-catalog ingest job.

Reads a JSON fixture under `loftly/data/manual_catalogs/{bank_slug}.json`,
validates against `ManualPromo`, and diffs against the `promos` table keyed
on the natural tuple `(bank_id, title_th, valid_from)`.

Semantics per W18 DEV_PLAN:
- **Insert** new rows (natural key not present).
- **Update** existing rows whose curated payload drifted.
- **Archive** rows present before but now absent from the fixture by flipping
  `active = False` — we never hard-delete. Only rows originally sourced from
  the manual catalog (`external_source_id = 'manual:{bank}:…'`) are eligible
  for archival; deal-harvester rows for the same bank are untouched.
- **Unchanged** rows get their `last_synced_at` bumped only (no content write).

Idempotent: re-running with the same fixture must produce
`inserted=0, updated=0, archived=0, unchanged=N`.

Dry-run mode reports the same counts without writing. Used by the admin API
and the CLI (`scripts.run_manual_ingest`) to preview changes before commit.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import resources
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.engine import get_sessionmaker
from loftly.db.models.bank import Bank
from loftly.db.models.promo import Promo
from loftly.schemas.manual_catalog import IngestResult, ManualCatalogFile, ManualPromo

log = get_logger(__name__)

_SOURCE_PREFIX = "manual"
_PROMO_TYPE_DEFAULT = "category_bonus"
_FIXTURES_PACKAGE = "loftly.data.manual_catalogs"


# ---------------------------------------------------------------------------
# Fixture loader — uses `importlib.resources` so the files ship inside the
# wheel rather than relying on a runtime CWD.
# ---------------------------------------------------------------------------


def _fixture_path(bank_slug: str) -> Path:
    safe = bank_slug.strip().lower()
    if not safe or "/" in safe or "\\" in safe or ".." in safe:
        raise ValueError(f"invalid bank_slug: {bank_slug!r}")
    ref = resources.files(_FIXTURES_PACKAGE).joinpath(f"{safe}.json")
    # `Traversable` does not expose a real filesystem path in all cases, but
    # for package-bundled JSON it does. Fall back to string coercion.
    return Path(str(ref))


def load_fixture(bank_slug: str) -> ManualCatalogFile:
    """Load + validate the fixture for `bank_slug`. Raises on missing or invalid."""
    path = _fixture_path(bank_slug)
    if not path.exists():
        raise FileNotFoundError(f"no manual-catalog fixture for bank {bank_slug!r}: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    return ManualCatalogFile.model_validate(raw)


# ---------------------------------------------------------------------------
# Core diff + apply
# ---------------------------------------------------------------------------


def _external_source_id(bank_slug: str, promo: ManualPromo) -> str:
    """Stable idempotency key for manual-catalog rows.

    Chosen so re-reading the same fixture produces an identical key — the
    natural tuple (bank, title, start_date) is good enough. If a curator
    renames a promo or shifts its start date, the old row archives and a new
    one inserts, which is the behavior we want.
    """
    return f"{_SOURCE_PREFIX}:{bank_slug}:{promo.start_date.isoformat()}:{promo.title}"


def _external_bank_key(bank_slug: str) -> str:
    """Distinguish manual-catalog rows from deal-harvester rows.

    Deal-harvester writes `external_bank_key = upstream 'bank' field` (e.g.
    `"uob"`). We prefix `manual:` so we can safely filter archival to our own
    rows without stomping on harvester-synced ones.
    """
    return f"{_SOURCE_PREFIX}:{bank_slug}"


def _discount_unit(promo: ManualPromo) -> str | None:
    if promo.discount_pct is not None:
        return "percent"
    if promo.cashback_thb is not None:
        return "thb"
    return None


def _discount_type(promo: ManualPromo) -> str | None:
    if promo.cashback_thb is not None:
        return "cashback"
    if promo.discount_pct is not None:
        return "percentage"
    return None


def _discount_value_str(promo: ManualPromo) -> str | None:
    if promo.discount_pct is not None:
        # Strip trailing zeros so `10.00` ingested twice doesn't flap.
        return f"{promo.discount_pct.normalize():f}%"
    if promo.cashback_thb is not None:
        return f"{promo.cashback_thb.normalize():f} THB"
    return None


def _promo_payload(bank_id: Any, bank_slug: str, promo: ManualPromo) -> dict[str, Any]:
    discount_amount = promo.discount_pct if promo.discount_pct is not None else promo.cashback_thb
    return {
        "bank_id": bank_id,
        "external_source_id": _external_source_id(bank_slug, promo),
        "external_bank_key": _external_bank_key(bank_slug),
        "external_checksum": None,
        "source_url": promo.source_url,
        "promo_type": _PROMO_TYPE_DEFAULT,
        "title_th": promo.title,
        "description_th": promo.notes,
        "merchant_name": None,
        "category": promo.category,
        "discount_type": _discount_type(promo),
        "discount_value": _discount_value_str(promo),
        "discount_amount": discount_amount,
        "discount_unit": _discount_unit(promo),
        "minimum_spend": promo.min_spend_thb,
        "valid_from": promo.start_date,
        "valid_until": promo.end_date,
        "terms_and_conditions": None,
        "raw_data": {
            **promo.model_dump(mode="json"),
            # Persist cap separately since the schema doesn't have a column
            # for it — surfaces under `raw_data.cap_thb` for UI use.
            "cap_thb": (float(promo.cap_thb) if promo.cap_thb is not None else None),
            "ingest_source": "manual_catalog",
        },
        "active": True,
        "last_synced_at": datetime.now(UTC),
    }


def _row_matches_payload(row: Promo, payload: dict[str, Any]) -> bool:
    """Compare the fields we manage against the in-DB row.

    Ignores `last_synced_at` (always changes) and `raw_data` inner timestamps.
    Any drift on a managed field → update.
    """
    ignore = {"last_synced_at", "raw_data", "external_checksum"}
    for k, v in payload.items():
        if k in ignore:
            continue
        current = getattr(row, k)
        if isinstance(v, Decimal) or isinstance(current, Decimal):
            if (v is None) != (current is None):
                return False
            if v is not None and current is not None and Decimal(str(v)) != Decimal(str(current)):
                return False
            continue
        if isinstance(v, date) or isinstance(current, date):
            if v != current:
                return False
            continue
        if current != v:
            return False
    return True


async def _load_bank(session: AsyncSession, bank_slug: str) -> Bank:
    bank = (
        (await session.execute(select(Bank).where(Bank.slug == bank_slug)))
        .scalars()
        .one_or_none()
    )
    if bank is None:
        raise ValueError(f"bank_slug {bank_slug!r} not found in banks table")
    return bank


async def _existing_manual_rows(
    session: AsyncSession, bank_id: Any, bank_slug: str
) -> dict[str, Promo]:
    """Return {external_source_id: Promo} for manual-catalog rows of this bank."""
    rows = list(
        (
            await session.execute(
                select(Promo).where(
                    Promo.bank_id == bank_id,
                    Promo.external_bank_key == _external_bank_key(bank_slug),
                )
            )
        )
        .scalars()
        .unique()
        .all()
    )
    return {r.external_source_id: r for r in rows if r.external_source_id}


async def run_ingest(
    bank_slug: str,
    dry_run: bool,
    *,
    promos: list[ManualPromo] | None = None,
) -> IngestResult:
    """Ingest the fixture for `bank_slug` into `promos`.

    If `promos` is passed explicitly (e.g. from a CSV upload), the on-disk
    fixture is bypassed. Otherwise `load_fixture(bank_slug)` is called.
    """
    if promos is None:
        fixture = load_fixture(bank_slug)
        if fixture.bank != bank_slug:
            log.warning(
                "manual_catalog_bank_mismatch",
                fixture_bank=fixture.bank,
                requested=bank_slug,
            )
        promos = fixture.promos

    sessionmaker = get_sessionmaker()
    inserted = updated = archived = unchanged = 0
    errors: list[str] = []

    async with sessionmaker() as session:
        bank = await _load_bank(session, bank_slug)
        existing = await _existing_manual_rows(session, bank.id, bank_slug)
        seen_keys: set[str] = set()

        for promo in promos:
            payload = _promo_payload(bank.id, bank_slug, promo)
            ext_id = payload["external_source_id"]
            seen_keys.add(ext_id)
            row = existing.get(ext_id)
            if row is None:
                if not dry_run:
                    session.add(Promo(**payload))
                inserted += 1
                continue
            if _row_matches_payload(row, payload):
                if not dry_run:
                    row.last_synced_at = datetime.now(UTC)
                unchanged += 1
                continue
            if not dry_run:
                for field, value in payload.items():
                    setattr(row, field, value)
            updated += 1

        # Archive: rows we had before that aren't in the fixture anymore.
        # Count BEFORE the UPDATE — the session would otherwise refresh the
        # in-memory `active` attribute to False and we'd undercount.
        stale_ids = [eid for eid in existing if eid not in seen_keys]
        archived = sum(1 for eid in stale_ids if existing[eid].active)
        if stale_ids and not dry_run:
            await session.execute(
                update(Promo)
                .where(
                    Promo.bank_id == bank.id,
                    Promo.external_bank_key == _external_bank_key(bank_slug),
                    Promo.external_source_id.in_(stale_ids),
                    Promo.active.is_(True),
                )
                .values(active=False, last_synced_at=datetime.now(UTC))
            )

        if dry_run:
            await session.rollback()
        else:
            await session.commit()

    log.info(
        "manual_catalog_ingest_complete",
        bank=bank_slug,
        dry_run=dry_run,
        inserted=inserted,
        updated=updated,
        archived=archived,
        unchanged=unchanged,
    )
    return IngestResult(
        bank_slug=bank_slug,
        dry_run=dry_run,
        upstream_count=len(promos),
        inserted=inserted,
        updated=updated,
        archived=archived,
        unchanged=unchanged,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# CSV parsing — the admin upload endpoint uses this to convert the uploaded
# file into a list of `ManualPromo` before calling `run_ingest`.
# ---------------------------------------------------------------------------


REQUIRED_CSV_COLUMNS: tuple[str, ...] = (
    "title",
    "bank",
    "card_types",
    "category",
    "start_date",
    "end_date",
    "discount_pct",
    "min_spend_thb",
    "cashback_thb",
    "cap_thb",
    "source_url",
    "notes",
)

MAX_CSV_ROWS = 500


def parse_csv(content: str) -> list[ManualPromo]:
    """Validate + parse CSV content into `ManualPromo` rows.

    Raises `ValueError` on column-shape problems and on row-count overflow.
    Row-level validation errors from Pydantic propagate as `ValueError` too.
    """
    import csv
    import io

    reader = csv.DictReader(io.StringIO(content))
    if not reader.fieldnames:
        raise ValueError("CSV has no header row")
    missing = [c for c in REQUIRED_CSV_COLUMNS if c not in reader.fieldnames]
    if missing:
        raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

    rows: list[ManualPromo] = []
    for i, raw in enumerate(reader, start=2):  # header = row 1
        if len(rows) >= MAX_CSV_ROWS:
            raise ValueError(f"CSV exceeds max rows ({MAX_CSV_ROWS})")
        try:
            cleaned = {
                "title": raw.get("title", "").strip(),
                "bank": raw.get("bank", "").strip(),
                "card_types": [
                    t.strip() for t in (raw.get("card_types") or "").split("|") if t.strip()
                ],
                "category": (raw.get("category") or "").strip() or None,
                "start_date": (raw.get("start_date") or "").strip(),
                "end_date": (raw.get("end_date") or "").strip(),
                "discount_pct": _opt_decimal(raw.get("discount_pct")),
                "min_spend_thb": _opt_decimal(raw.get("min_spend_thb")),
                "cashback_thb": _opt_decimal(raw.get("cashback_thb")),
                "cap_thb": _opt_decimal(raw.get("cap_thb")),
                "source_url": (raw.get("source_url") or "").strip(),
                "notes": (raw.get("notes") or "").strip() or None,
            }
            rows.append(ManualPromo.model_validate(cleaned))
        except Exception as exc:
            raise ValueError(f"row {i}: {exc}") from exc
    return rows


def _opt_decimal(v: str | None) -> Decimal | None:
    if v is None:
        return None
    s = v.strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except Exception as exc:  # pragma: no cover — decimal errors vary by locale
        raise ValueError(f"invalid decimal: {v!r}") from exc


__all__ = [
    "MAX_CSV_ROWS",
    "REQUIRED_CSV_COLUMNS",
    "load_fixture",
    "parse_csv",
    "run_ingest",
]
