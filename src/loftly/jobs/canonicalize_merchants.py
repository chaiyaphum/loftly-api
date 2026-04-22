"""Daily canonicalization pipeline: promos.merchant_name → canonical merchants.

Runs after `sync_deal_harvester` per `mvp/DATA_INGESTION.md §Merchant
canonicalization pipeline`. Finds promos that don't yet have a row in
`promos_merchant_canonical_map`, then walks the five-step ladder:

    1. Normalize (lowercase, strip whitespace, drop suffixes TH / Thailand /
       บริษัท / จำกัด).
    2. Exact match against `merchants_canonical.slug` + `alt_names` (→
       method='exact', confidence=1.0).
    3. Fuzzy match — Postgres pg_trgm `similarity()` ≥ 0.85, SQLite falls
       back to Levenshtein if the `python-Levenshtein` package is installed;
       if not, fuzzy step is a no-op on SQLite (tests). Method='fuzzy',
       confidence = similarity score.
    4. Remaining unmatched → Haiku batch (≤20 candidates/call) via Prompt 8
       (method='llm', action-dependent).
    5. Anything with confidence < 0.8 OR `action='uncertain'` is left with
       `reviewed_at IS NULL` for the admin review queue.

Observability mirrors `deal_harvester_sync` — a `sync_runs` row bookends
the run, structlog events tag each step, failures do NOT abort the batch.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.core.logging import get_logger
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import SyncRun
from loftly.db.models.merchant import (
    MerchantCanonical,
    PromoMerchantCanonicalMap,
)
from loftly.db.models.promo import Promo

log = get_logger(__name__)

_SOURCE = "canonicalize_merchants"
_BATCH_SIZE = 20
_FUZZY_THRESHOLD = 0.85
_REVIEW_CONFIDENCE_THRESHOLD = 0.80

# Suffix patterns stripped during normalization. Case-insensitive.
_SUFFIX_TOKENS = (
    "thailand",
    "th",
    "(thailand)",
    "co., ltd.",
    "co.,ltd.",
    "co ltd",
    "ltd.",
    "ltd",
    "บริษัท",
    "จำกัด",
    "(มหาชน)",
)


def _normalize(name: str) -> str:
    """Lowercase, strip punctuation/whitespace, drop common corporate suffixes.

    Keeps Thai characters as-is; only ASCII punctuation + suffix tokens
    get removed. Non-destructive for Thai brand names like "สตาร์บัคส์".
    """
    if not name:
        return ""
    text = unicodedata.normalize("NFKC", name).strip().lower()
    # Drop known corporate suffix tokens regardless of position.
    for suffix in _SUFFIX_TOKENS:
        text = text.replace(suffix, " ")
    # Collapse runs of whitespace + strip stray punctuation that would
    # otherwise break a slug-ish exact match.
    text = re.sub(r"[,.;:!?'\"()\[\]\-_/\\]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _exact_match(
    normalized: str, canonicals: list[MerchantCanonical]
) -> MerchantCanonical | None:
    """Step 2 — exact hit against slug or any `alt_names` entry (normalized)."""
    if not normalized:
        return None
    for m in canonicals:
        if m.status != "active":
            continue
        if _normalize(m.slug) == normalized:
            return m
        if _normalize(m.display_name_en) == normalized:
            return m
        if _normalize(m.display_name_th) == normalized:
            return m
        for alt in m.alt_names or []:
            if _normalize(alt) == normalized:
                return m
    return None


def _fuzzy_score(a: str, b: str) -> float:
    """Best-effort string similarity in [0.0, 1.0].

    Prefers `python-Levenshtein` if present (it's a common dep but not
    guaranteed in the test sandbox). Fallback: SequenceMatcher from stdlib
    `difflib` — slower but always available.
    """
    if not a or not b:
        return 0.0
    try:
        import Levenshtein  # type: ignore[import-not-found]

        return float(Levenshtein.ratio(a, b))
    except ImportError:
        from difflib import SequenceMatcher

        return SequenceMatcher(None, a, b).ratio()


def _fuzzy_match(
    normalized: str, canonicals: list[MerchantCanonical]
) -> tuple[MerchantCanonical, float] | None:
    """Step 3 — return best fuzzy hit above `_FUZZY_THRESHOLD`, else None."""
    if not normalized:
        return None
    best: tuple[MerchantCanonical, float] | None = None
    for m in canonicals:
        if m.status != "active":
            continue
        candidates = [
            _normalize(m.slug),
            _normalize(m.display_name_en),
            _normalize(m.display_name_th),
            *(_normalize(alt) for alt in (m.alt_names or [])),
        ]
        for c in candidates:
            score = _fuzzy_score(normalized, c)
            if score >= _FUZZY_THRESHOLD and (best is None or score > best[1]):
                best = (m, score)
    return best


async def _unmapped_promos(session: AsyncSession) -> list[Promo]:
    """Promos missing a row in `promos_merchant_canonical_map`."""
    mapped_stmt = select(PromoMerchantCanonicalMap.promo_id)
    mapped_ids = set((await session.execute(mapped_stmt)).scalars().all())

    stmt = (
        select(Promo)
        .where(Promo.active.is_(True))
        .where(Promo.merchant_name.is_not(None))
    )
    rows = list((await session.execute(stmt)).scalars().unique().all())
    return [p for p in rows if p.id not in mapped_ids]


async def _write_map_row(
    session: AsyncSession,
    *,
    promo_id: uuid.UUID,
    merchant_id: uuid.UUID,
    confidence: float,
    method: str,
) -> None:
    """Insert a `promos_merchant_canonical_map` row; idempotent on promo_id."""
    existing = await session.get(PromoMerchantCanonicalMap, promo_id)
    if existing is not None:
        return
    row = PromoMerchantCanonicalMap(
        promo_id=promo_id,
        merchant_canonical_id=merchant_id,
        confidence=Decimal(str(round(confidence, 2))),
        method=method,
    )
    session.add(row)


async def _run_llm_batch(
    session: AsyncSession,
    candidates: list[Promo],
    canonicals: list[MerchantCanonical],
) -> dict[uuid.UUID, tuple[uuid.UUID | None, float, str]]:
    """Invoke Prompt 8 on a batch of ≤ _BATCH_SIZE candidates.

    Returns a map: promo_id → (merchant_id | None, confidence, method).
    In dev/test we skip LLM calls (Anthropic provider lazy-loaded) and
    return empty — the records fall into the admin-review queue.

    TODO: wire AnthropicHaikuProvider.canonicalize_merchants once the
    provider gains a typed batch method (tracked in follow-up PR).
    """
    _ = session, candidates, canonicals  # placeholders until provider wired
    log.info(
        "canonicalize_llm_stub",
        batch=len(candidates),
        reason="haiku_merchant_canonicalizer_not_yet_wired",
    )
    return {}


async def run_canonicalization(
    *,
    sessionmaker: Any = None,
) -> dict[str, Any]:
    """Entry point. Mirrors the contract of `deal_harvester_sync.run_sync()`.

    Returns the SyncRun payload so the cron wrapper can log it verbatim.
    """
    sm = sessionmaker or get_sessionmaker()
    started = datetime.now(UTC)
    run_row_id: uuid.UUID | None = None
    async with sm() as session:
        run = SyncRun(source=_SOURCE, started_at=started, status="running")
        session.add(run)
        await session.flush()
        run_row_id = run.id
        await session.commit()

    upstream_count = 0
    exact_count = 0
    fuzzy_count = 0
    llm_count = 0
    review_queue_count = 0
    error_message: str | None = None
    status = "success"

    try:
        async with sm() as session:
            canonicals = list(
                (await session.execute(select(MerchantCanonical))).scalars().all()
            )
            unmapped = await _unmapped_promos(session)
            upstream_count = len(unmapped)

            llm_queue: list[Promo] = []
            for promo in unmapped:
                normalized = _normalize(promo.merchant_name or "")
                if not normalized:
                    continue

                # Step 2: exact match.
                hit = _exact_match(normalized, canonicals)
                if hit is not None:
                    await _write_map_row(
                        session,
                        promo_id=promo.id,
                        merchant_id=hit.id,
                        confidence=1.0,
                        method="exact",
                    )
                    exact_count += 1
                    continue

                # Step 3: fuzzy match.
                fuzzy = _fuzzy_match(normalized, canonicals)
                if fuzzy is not None:
                    merchant, score = fuzzy
                    await _write_map_row(
                        session,
                        promo_id=promo.id,
                        merchant_id=merchant.id,
                        confidence=score,
                        method="fuzzy",
                    )
                    fuzzy_count += 1
                    if score < _REVIEW_CONFIDENCE_THRESHOLD:
                        review_queue_count += 1
                    continue

                # Step 4: queue for LLM.
                llm_queue.append(promo)

            # Step 5: batch the LLM call(s) — ≤ 20 per batch.
            for i in range(0, len(llm_queue), _BATCH_SIZE):
                batch = llm_queue[i : i + _BATCH_SIZE]
                results = await _run_llm_batch(session, batch, canonicals)
                for promo_id, (merchant_id, confidence, _method) in results.items():
                    if merchant_id is None:
                        review_queue_count += 1
                        continue
                    await _write_map_row(
                        session,
                        promo_id=promo_id,
                        merchant_id=merchant_id,
                        confidence=confidence,
                        method="llm",
                    )
                    llm_count += 1
                    if confidence < _REVIEW_CONFIDENCE_THRESHOLD:
                        review_queue_count += 1

            await session.commit()
    except Exception as exc:
        log.exception("canonicalize_merchants_failed")
        error_message = f"{type(exc).__name__}: {exc}"[:500]
        status = "failed"

    async with sm() as session:
        run = (
            (await session.execute(select(SyncRun).where(SyncRun.id == run_row_id)))
            .scalars()
            .one()
        )
        run.status = status
        run.finished_at = datetime.now(UTC)
        run.upstream_count = upstream_count
        run.inserted_count = exact_count + fuzzy_count + llm_count
        run.updated_count = 0
        run.mapping_queue_added = review_queue_count
        run.error_message = error_message
        await session.commit()
        log.info(
            "canonicalize_merchants_done",
            upstream=upstream_count,
            exact=exact_count,
            fuzzy=fuzzy_count,
            llm=llm_count,
            review_queue=review_queue_count,
            status=status,
        )
        return {
            "id": str(run.id),
            "source": run.source,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "status": run.status,
            "upstream_count": run.upstream_count,
            "inserted_count": run.inserted_count,
            "mapping_queue_added": run.mapping_queue_added,
            "error_message": run.error_message,
        }


__all__ = ["run_canonicalization"]
