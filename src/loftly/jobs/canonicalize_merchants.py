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

LLM step degrades gracefully: when `ANTHROPIC_API_KEY` is unset (or is the
"stub"/"test" sentinel), step 4 is skipped with a warning and unmatched
candidates flow into the admin review queue as orphan rows. This mirrors
`AnthropicProvider._should_use_real_anthropic()` in `ai/providers/anthropic.py`.
"""

from __future__ import annotations

import json
import re
import unicodedata
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.ai.providers.anthropic import _should_use_real_anthropic
from loftly.ai.providers.anthropic_haiku import HAIKU_MODEL
from loftly.core.logging import get_logger
from loftly.core.settings import get_settings
from loftly.db.engine import get_sessionmaker
from loftly.db.models.audit import SyncRun
from loftly.db.models.merchant import (
    MerchantCanonical,
    PromoMerchantCanonicalMap,
)
from loftly.db.models.promo import Promo
from loftly.prompts.merchant_canonicalizer import load as load_prompt
from loftly.schemas.merchants import (
    CandidatePromo,
    CanonicalizerResult,
    MerchantCanonicalizerInput,
    MerchantCanonicalizerOutput,
)

log = get_logger(__name__)

# Per the acceptance contract: the source tag on sync_runs rows is
# `merchant_canonicalizer` to mirror Prompt 8's slug. Tests query on this
# string so do not rename without updating the route + tests in lockstep.
_SOURCE = "merchant_canonicalizer"
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


def _exact_match(normalized: str, canonicals: list[MerchantCanonical]) -> MerchantCanonical | None:
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

    stmt = select(Promo).where(Promo.active.is_(True)).where(Promo.merchant_name.is_not(None))
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
    """Insert a `promos_merchant_canonical_map` row; idempotent on promo_id.

    `reviewed_at` is intentionally left NULL — the admin review queue
    (`GET /v1/admin/merchants/mapping-queue`) filters on
    `reviewed_at IS NULL` to surface items that still need human QA. An
    admin action flips it to `now()` when the mapping is approved.
    """
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


def _serialize_canonicals(canonicals: list[MerchantCanonical]) -> str:
    """Pack canonical merchants into the stable JSON block Prompt 8 expects.

    Sorted by id so cache prefixes stay byte-stable across calls — see
    `anthropic.py::_serialize_context` for the same discipline.
    """
    payload = [
        {
            "id": str(m.id),
            "slug": m.slug,
            "display_name_th": m.display_name_th,
            "display_name_en": m.display_name_en,
            "alt_names": list(m.alt_names or []),
            "merchant_type": m.merchant_type,
        }
        for m in sorted(canonicals, key=lambda m: str(m.id))
        if m.status == "active"
    ]
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _serialize_candidates(candidates: list[CandidatePromo]) -> str:
    return json.dumps(
        [c.model_dump(mode="json") for c in candidates],
        separators=(",", ":"),
        ensure_ascii=False,
    )


_VALID_MERCHANT_TYPES = {"retail", "fnb", "ecommerce", "travel", "service"}
# Map Haiku's drift outputs (often KTC category slugs or English descriptors)
# to the closest valid MerchantType. Anything unmapped → "retail" (the
# safest catch-all; admin can override via /admin/merchants/{id}).
_MERCHANT_TYPE_FALLBACK_MAP = {
    "dining": "fnb",
    "dining-restaurants": "fnb",
    "dining-cafe": "fnb",
    "restaurant": "fnb",
    "restaurants": "fnb",
    "cafe": "fnb",
    "food": "fnb",
    "shopping": "retail",
    "department-store": "retail",
    "supermarket": "retail",
    "grocery": "retail",
    "convenience-store": "retail",
    "online": "ecommerce",
    "online-shopping-services": "ecommerce",
    "e-commerce": "ecommerce",
    "marketplace": "ecommerce",
    "travel": "travel",
    "hotels": "travel",
    "air-ticket-hotels-travel": "travel",
    "airline": "travel",
    "transport": "travel",
    "petrol": "service",
    "auto-gas-ev": "service",
    "education": "service",
    "health-beauty": "service",
    "insurance-investment": "service",
    "telecom": "service",
    "entertainment": "service",
    "entertainment-hobby": "service",
    "pet": "service",
    "sports-fitness": "service",
    "home-furniture": "retail",
    "electronics-mobile": "retail",
    "gold-diamond-jewelry": "retail",
    "recommended": "retail",
}


def _coerce_merchant_types(payload: dict[str, Any]) -> dict[str, Any]:
    """Map Haiku's free-text merchant_type into the strict 5-value enum.

    Haiku occasionally returns KTC category slugs (`entertainment-hobby`,
    `pet`, `dining-restaurants`) instead of one of the 5 enum values. Pydantic
    rejects those with a literal_error and the whole batch was failing. Coerce
    via a lookup map; unmapped values default to "retail" so downstream lookup
    still succeeds. Admin can re-classify via `/v1/admin/merchants/{id}`.
    """
    for result in payload.get("results", []):
        proposed = result.get("proposed")
        if proposed is None:
            continue
        mt = proposed.get("merchant_type")
        if mt and mt not in _VALID_MERCHANT_TYPES:
            normalized = mt.lower().strip()
            proposed["merchant_type"] = _MERCHANT_TYPE_FALLBACK_MAP.get(
                normalized, "retail"
            )
    return payload


def _parse_llm_response(raw: str) -> MerchantCanonicalizerOutput:
    """Parse Haiku's JSON response. Tolerates stray whitespace + trailing prose."""
    text = raw.strip()
    # Claude occasionally wraps in ```json … ```. Strip before parsing.
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    payload = _coerce_merchant_types(json.loads(text))
    return MerchantCanonicalizerOutput.model_validate(payload)


async def _call_haiku(
    candidates: list[CandidatePromo],
    canonicals: list[MerchantCanonical],
) -> MerchantCanonicalizerOutput:
    """Invoke Prompt 8 on a batch of ≤ _BATCH_SIZE candidates.

    Imports the Anthropic SDK lazily — matches `ai/providers/anthropic.py`
    so stub/test deploys don't pay the SDK import cost.
    """
    from anthropic import AsyncAnthropic

    settings = get_settings()
    prompt = load_prompt()
    template = prompt.text
    # The prompt lives as a single Markdown doc with {candidates} +
    # {canonical_merchants} placeholders. Because the doc also contains
    # literal `{...}` JSON-schema examples, we can't use `str.format_map`
    # (it'd try to interpret every brace). Use plain token replacement.
    candidates_json = _serialize_candidates(candidates)
    canonicals_json = _serialize_canonicals(canonicals)
    filled = template.replace("{candidates}", candidates_json).replace(
        "{canonical_merchants}", canonicals_json
    )
    # Split: the "## System" section becomes the system prompt (cached
    # prefix), the "## User" section becomes the per-call message.
    system_block, _, user_block = filled.partition("## User")
    system_text = system_block.strip()
    user_text = user_block.strip() or filled.strip()

    client = AsyncAnthropic(api_key=settings.anthropic_api_key, max_retries=0)
    # Bump from 2k → 6k because batches of 20 candidates produce structured
    # JSON output with proposed.alt_names + reasoning_th per result. Live
    # staging hit the 2k cap mid-string, returned truncated JSON, every
    # batch failed with `Unterminated string at line N`. 6k gives ~150 chars
    # × 20 results × ~10 reasoning lines + alt_names headroom.
    response: Any = await cast(Any, client.messages.create)(
        model=HAIKU_MODEL,
        max_tokens=6_000,
        system=[
            {
                "type": "text",
                "text": system_text,
                "cache_control": {"type": "ephemeral"},
            },
        ],
        messages=[{"role": "user", "content": user_text}],
    )

    usage = response.usage
    input_tokens = getattr(usage, "input_tokens", 0) or 0
    output_tokens = getattr(usage, "output_tokens", 0) or 0
    cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
    log.info(
        "merchant_canonicalizer_haiku_call",
        model=HAIKU_MODEL,
        prompt_version=prompt.version,
        prompt_slug=prompt.slug,
        batch=len(candidates),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_tokens=cache_read,
    )

    # Haiku returns a text block (no tool-use in this prompt — strict JSON).
    for block in response.content:
        btype = getattr(block, "type", None)
        if btype == "text":
            return _parse_llm_response(getattr(block, "text", "") or "")
    raise ValueError("Haiku response did not contain a text block.")


async def _run_llm_batch(
    session: AsyncSession,
    candidates: list[Promo],
    canonicals: list[MerchantCanonical],
) -> MerchantCanonicalizerOutput | None:
    """Invoke Prompt 8 on a batch of ≤ _BATCH_SIZE candidates.

    Returns the parsed `MerchantCanonicalizerOutput`, or None when the
    Anthropic key is not configured (dev/test stub mode). The caller is
    responsible for applying the results to the DB.
    """
    _ = session  # the LLM call itself is read-only; writes happen upstream
    if not _should_use_real_anthropic():
        log.warning(
            "merchant_canonicalizer_llm_skipped",
            batch=len(candidates),
            reason="anthropic_api_key_not_configured",
        )
        return None

    batch_input = MerchantCanonicalizerInput(
        candidates=[
            CandidatePromo(
                promo_id=str(p.id),
                raw_merchant_name=p.merchant_name or "",
                promo_category=p.category,
                promo_title_th=p.title_th or "",
            )
            for p in candidates
        ]
    )
    return await _call_haiku(batch_input.candidates, canonicals)


def _canonical_by_id(
    canonicals: list[MerchantCanonical], merchant_id: str
) -> MerchantCanonical | None:
    try:
        target_uuid = uuid.UUID(merchant_id)
    except (TypeError, ValueError):
        return None
    for m in canonicals:
        if m.id == target_uuid:
            return m
    return None


async def _apply_llm_result(
    session: AsyncSession,
    *,
    promo: Promo,
    result: CanonicalizerResult,
    canonicals: list[MerchantCanonical],
    counters: dict[str, int],
) -> None:
    """Translate a single `CanonicalizerResult` into DB writes.

    `counters` is mutated in-place — the caller owns the aggregate counts
    so the structlog summary event can surface them all together.
    """
    action = result.action
    confidence = float(result.confidence)

    if action == "match":
        if not result.merchant_id:
            log.warning(
                "merchant_canonicalizer_malformed_match",
                promo_id=str(promo.id),
                reason="missing_merchant_id",
            )
            counters["failed"] += 1
            return
        merchant = _canonical_by_id(canonicals, result.merchant_id)
        if merchant is None:
            log.warning(
                "merchant_canonicalizer_unknown_merchant_id",
                promo_id=str(promo.id),
                merchant_id=result.merchant_id,
            )
            counters["failed"] += 1
            return
        await _write_map_row(
            session,
            promo_id=promo.id,
            merchant_id=merchant.id,
            confidence=confidence,
            method="llm",
        )
        counters["llm_matched"] += 1
        if confidence < _REVIEW_CONFIDENCE_THRESHOLD:
            counters["review_queue"] += 1
        return

    if action == "new":
        proposed = result.proposed
        if proposed is None:
            log.warning(
                "merchant_canonicalizer_malformed_new",
                promo_id=str(promo.id),
                reason="missing_proposed",
            )
            counters["failed"] += 1
            return
        # Reject duplicate slug to preserve the "new-merchant slug
        # uniqueness 100%" acceptance bar from Prompt 8 §Evaluation.
        if any(m.slug == proposed.slug for m in canonicals):
            log.warning(
                "merchant_canonicalizer_new_slug_collision",
                promo_id=str(promo.id),
                slug=proposed.slug,
            )
            counters["failed"] += 1
            return
        merchant = MerchantCanonical(
            slug=proposed.slug,
            display_name_th=proposed.display_name_th,
            display_name_en=proposed.display_name_en,
            merchant_type=proposed.merchant_type,
            alt_names=list(proposed.alt_names),
            status="active",
        )
        session.add(merchant)
        await session.flush()
        canonicals.append(merchant)  # make visible to the rest of this batch
        await _write_map_row(
            session,
            promo_id=promo.id,
            merchant_id=merchant.id,
            confidence=confidence,
            method="llm",
        )
        counters["llm_new"] += 1
        if confidence < _REVIEW_CONFIDENCE_THRESHOLD:
            counters["review_queue"] += 1
        return

    # action == "uncertain"
    top = result.top_candidates or []
    if not top:
        counters["llm_uncertain"] += 1
        counters["review_queue"] += 1
        return
    # Drop null-merchant_id candidates (Haiku occasionally emits these to
    # express "I don't know which seeded merchant" — same effect as no
    # candidate at all). If they're all null, fall through to the empty path.
    top_with_id = [c for c in top if c.merchant_id is not None]
    if not top_with_id:
        counters["llm_uncertain"] += 1
        counters["review_queue"] += 1
        return
    best = max(top_with_id, key=lambda c: c.confidence)
    assert best.merchant_id is not None  # narrow for mypy
    merchant = _canonical_by_id(canonicals, best.merchant_id)
    if merchant is None:
        log.warning(
            "merchant_canonicalizer_uncertain_unknown_candidate",
            promo_id=str(promo.id),
            candidate_merchant_id=best.merchant_id,
        )
        counters["llm_uncertain"] += 1
        counters["review_queue"] += 1
        return
    # Write the top-candidate pointer with the low LLM confidence so the
    # admin queue (reviewed_at IS NULL) can surface + approve/reject it.
    await _write_map_row(
        session,
        promo_id=promo.id,
        merchant_id=merchant.id,
        confidence=float(best.confidence),
        method="llm",
    )
    counters["llm_uncertain"] += 1
    counters["review_queue"] += 1


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

    counters: dict[str, int] = {
        "candidates": 0,
        "exact_matched": 0,
        "fuzzy_matched": 0,
        "llm_matched": 0,
        "llm_new": 0,
        "llm_uncertain": 0,
        "failed": 0,
        "review_queue": 0,
    }
    error_message: str | None = None
    status = "success"

    try:
        async with sm() as session:
            canonicals = list((await session.execute(select(MerchantCanonical))).scalars().all())
            unmapped = await _unmapped_promos(session)
            counters["candidates"] = len(unmapped)

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
                    counters["exact_matched"] += 1
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
                    counters["fuzzy_matched"] += 1
                    if score < _REVIEW_CONFIDENCE_THRESHOLD:
                        counters["review_queue"] += 1
                    continue

                # Step 4: queue for LLM.
                llm_queue.append(promo)

            # Step 5: batch the LLM call(s) — ≤ 20 per batch.
            promo_by_id = {str(p.id): p for p in llm_queue}
            for i in range(0, len(llm_queue), _BATCH_SIZE):
                batch = llm_queue[i : i + _BATCH_SIZE]
                try:
                    output = await _run_llm_batch(session, batch, canonicals)
                except Exception as exc:  # per-batch isolation by design
                    log.exception(
                        "merchant_canonicalizer_batch_failed",
                        batch_index=i // _BATCH_SIZE,
                        batch_size=len(batch),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    counters["failed"] += len(batch)
                    counters["review_queue"] += len(batch)
                    continue
                if output is None:
                    # Stub path: no key configured. Candidates remain
                    # unmapped — admin queue picks them up on the next
                    # pass as unreviewed orphans.
                    counters["review_queue"] += len(batch)
                    continue
                for result in output.results:
                    target = promo_by_id.get(result.promo_id)
                    if target is None:
                        log.warning(
                            "merchant_canonicalizer_unknown_promo_in_response",
                            promo_id=result.promo_id,
                        )
                        counters["failed"] += 1
                        continue
                    await _apply_llm_result(
                        session,
                        promo=target,
                        result=result,
                        canonicals=canonicals,
                        counters=counters,
                    )

            await session.commit()
    except Exception as exc:
        log.exception("merchant_canonicalizer_failed")
        error_message = f"{type(exc).__name__}: {exc}"[:500]
        status = "failed"

    async with sm() as session:
        run = (
            (await session.execute(select(SyncRun).where(SyncRun.id == run_row_id))).scalars().one()
        )
        run.status = status
        run.finished_at = datetime.now(UTC)
        # Aggregate counts — the SyncRun schema is shared with
        # deal_harvester_sync so we reuse existing columns:
        #   upstream_count  = candidates considered
        #   inserted_count  = new map rows written (exact + fuzzy + llm_*)
        #   updated_count   = new canonical merchants created (action='new')
        #   deactivated_count = hard failures (malformed / unknown id)
        #   mapping_queue_added = rows left for admin review (reviewed_at IS NULL)
        run.upstream_count = counters["candidates"]
        run.inserted_count = (
            counters["exact_matched"]
            + counters["fuzzy_matched"]
            + counters["llm_matched"]
            + counters["llm_new"]
            + counters["llm_uncertain"]
        )
        run.updated_count = counters["llm_new"]
        run.deactivated_count = counters["failed"]
        run.mapping_queue_added = counters["review_queue"]
        run.error_message = error_message
        await session.commit()
        log.info(
            "merchant_canonicalizer_done",
            candidates=counters["candidates"],
            exact_matched=counters["exact_matched"],
            fuzzy_matched=counters["fuzzy_matched"],
            llm_matched=counters["llm_matched"],
            llm_new=counters["llm_new"],
            llm_uncertain=counters["llm_uncertain"],
            failed=counters["failed"],
            review_queue=counters["review_queue"],
            status=status,
        )
        return {
            "id": str(run.id),
            "source": run.source,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "status": run.status,
            "candidates": counters["candidates"],
            "exact_matched": counters["exact_matched"],
            "fuzzy_matched": counters["fuzzy_matched"],
            "llm_matched": counters["llm_matched"],
            "llm_new": counters["llm_new"],
            "llm_uncertain": counters["llm_uncertain"],
            "failed": counters["failed"],
            "review_queue": counters["review_queue"],
            "error_message": run.error_message,
        }


__all__ = ["run_canonicalization"]
