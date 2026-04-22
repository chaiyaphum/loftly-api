"""Active-promo snapshot for Card Selector cached context.

POST_V1 Tier A fast-follow (ratified 2026-04-22). Fills the 10k
`active_promos` slot that `mvp/AI_PROMPTS.md §Prompt 1` reserved but left
empty in v1. The block is injected into the Sonnet cached portion per
`src/loftly/ai/providers/anthropic.py::_serialize_context`; the snapshot's
`digest` is included in the cache key so a new deal-harvester sync
invalidates the cached block cleanly (no stale-data risk).

Stacking rules sent to the LLM (mirrored in the system prompt):
1. Stack promo on top of base earn, show THB math in reason_th.
2. Cite promo by title; don't paraphrase merchant.
3. If valid_until <= 21 days from snapshot date, prefix "หมดเขตเร็ว — ".
4. If user spend < min_spend: don't stack; add `promo_min_spend_unmet` to warnings.
5. Skip promos with empty card mapping silently.
6. Never invent promos.

Failure modes:
- Sync stale > 72h -> caller injects sentinel `PROMO_CONTEXT_UNAVAILABLE`
  and sets `promo_context_status='degraded'`. Selector still runs.
- Snapshot query >500ms -> same degraded path.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from loftly.core.logging import get_logger
from loftly.db.models.promo import Promo

log = get_logger(__name__)

# Token-budget cap for the cached slot (see AI_PROMPTS.md §Prompt 1 cached context).
# ~40 promos * ~250 tokens each ~= 10k tokens. Beyond this we truncate by rank.
MAX_PROMOS_IN_SNAPSHOT = 40

# Per-promo serialized token estimate (used for truncation + Langfuse metrics).
APPROX_TOKENS_PER_PROMO = 250


@dataclass(frozen=True)
class PromoSnapshotEntry:
    """One promo in the Sonnet cached context. Fields match the format in
    AI_PROMPTS.md §Prompt 1 active_promos block.
    """

    promo_id: str
    title_th: str
    merchant: str | None
    category: str | None
    discount_type: str | None
    discount_value: str | None
    minimum_spend: float | None
    valid_until: str | None  # ISO date
    applicable_card_ids: list[str]


@dataclass(frozen=True)
class PromoSnapshot:
    """Output of `build_promo_snapshot`. Passed into SelectorContext."""

    as_of: date
    entries: list[PromoSnapshotEntry]
    digest: str  # sha256 of ids+checksums; participates in prompt cache key
    total_count_before_cap: int
    approx_tokens: int
    status: str  # 'ok' | 'degraded' | 'stale'


async def build_promo_snapshot(
    session: AsyncSession,
    *,
    as_of: date | None = None,
    max_entries: int = MAX_PROMOS_IN_SNAPSHOT,
) -> PromoSnapshot:
    """Query active promos and serialize into a snapshot.

    Filtering:
    - `active = true`
    - `valid_until IS NULL OR valid_until >= as_of`
    - `valid_from IS NULL OR valid_from <= as_of`

    Ranking (within cap):
    - resolved_cards non-empty first (unmapped promos are LLM-confusing)
    - highest discount_amount desc
    - nearest valid_until asc

    Returns empty entries + status='ok' if DB returns zero rows (valid:
    legitimately no active promos). Caller decides degrade behavior.
    """
    if as_of is None:
        as_of = datetime.now(tz=UTC).date()

    stmt = (
        select(Promo)
        .where(
            and_(
                Promo.active.is_(True),
                or_(
                    Promo.valid_until.is_(None),
                    Promo.valid_until >= as_of,
                ),
                or_(
                    Promo.valid_from.is_(None),
                    Promo.valid_from <= as_of,
                ),
            )
        )
        .options(selectinload(Promo.cards))
    )
    result = await session.execute(stmt)
    all_promos = list(result.scalars().all())
    total_count_before_cap = len(all_promos)

    # Rank + truncate
    def _rank_key(p: Promo) -> tuple[int, float, float]:
        has_cards = 1 if p.cards else 0
        discount = -float(p.discount_amount or 0)
        days_to_expire = (
            (p.valid_until - as_of).days if p.valid_until else 10_000
        )
        return (-has_cards, discount, float(days_to_expire))

    ranked = sorted(all_promos, key=_rank_key)[:max_entries]

    entries = [
        PromoSnapshotEntry(
            promo_id=str(p.id),
            title_th=p.title_th,
            merchant=p.merchant_name,
            category=p.category,
            discount_type=p.discount_type,
            discount_value=p.discount_value,
            minimum_spend=float(p.minimum_spend) if p.minimum_spend is not None else None,
            valid_until=p.valid_until.isoformat() if p.valid_until else None,
            applicable_card_ids=[str(c.id) for c in p.cards],
        )
        for p in ranked
    ]

    # Digest = sha256 over sorted (id, external_checksum) pairs. A promo being
    # updated by deal-harvester changes external_checksum -> digest changes
    # -> Selector cache key changes -> stale context evicted naturally.
    digest_material = sorted(
        (str(p.id), p.external_checksum or "") for p in ranked
    )
    digest = hashlib.sha256(
        json.dumps(digest_material, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]

    approx_tokens = len(entries) * APPROX_TOKENS_PER_PROMO

    log.info(
        "promo_snapshot_built",
        as_of=as_of.isoformat(),
        total_count_before_cap=total_count_before_cap,
        entries=len(entries),
        digest=digest,
        approx_tokens=approx_tokens,
    )

    return PromoSnapshot(
        as_of=as_of,
        entries=entries,
        digest=digest,
        total_count_before_cap=total_count_before_cap,
        approx_tokens=approx_tokens,
        status="ok",
    )


def serialize_snapshot_for_prompt(snapshot: PromoSnapshot) -> str:
    """Format the snapshot as the `<active_promos>` block for the Sonnet cached context.

    Format per entry matches AI_PROMPTS.md §Prompt 1 spec:
      [promo_id] title_th | merchant | category | type=<t> value=<v>
      min_spend=<THB> valid_until=<YYYY-MM-DD> cards=[card_id,...]
    """
    if snapshot.status != "ok":
        return (
            f"<active_promos status=\"{snapshot.status}\">\n"
            "PROMO_CONTEXT_UNAVAILABLE: selector should not cite promos.\n"
            "</active_promos>"
        )
    if not snapshot.entries:
        return (
            f'<active_promos as_of="{snapshot.as_of.isoformat()}" count="0">\n'
            "No active promos in any category. Rank on base earn only; do not invent.\n"
            "</active_promos>"
        )

    lines = [
        f'<active_promos as_of="{snapshot.as_of.isoformat()}" count="{len(snapshot.entries)}">',
        "Each entry is a currently-active Thai-bank promo. Consider ONLY when the user's",
        "category allocation overlaps the promo's category AND their spend meets min_spend",
        "AND at least one applicable_card_id is in the stack (or in current_cards).",
        "",
        "Stacking rules:",
        "1. Stack promo on base earn; show THB math in reason_th ",
        '   ("ฐาน 2% + โปร 15% Starbucks = ~340 THB/เดือน").',
        "2. Cite promo by title; do NOT paraphrase merchant.",
        '3. If valid_until <= 21d from as_of, prefix "หมดเขตเร็ว — ".',
        '4. If user spend < min_spend: do NOT stack; add "promo_min_spend_unmet" to warnings.',
        "5. Skip promos with cards=[] silently.",
        "6. Never invent promos. If none fit, omit mention and rank on base earn only.",
        "7. Populate cited_promo_ids with the ids you used in the stack.",
        "",
    ]
    for e in snapshot.entries:
        min_spend_s = f"{e.minimum_spend:.0f}" if e.minimum_spend is not None else "n/a"
        cards_s = ",".join(e.applicable_card_ids) if e.applicable_card_ids else ""
        lines.append(
            f"[{e.promo_id}] {e.title_th}"
            f" | {e.merchant or 'n/a'}"
            f" | {e.category or 'n/a'}"
            f" | type={e.discount_type or 'n/a'} value={e.discount_value or 'n/a'}"
            f" min_spend={min_spend_s}"
            f" valid_until={e.valid_until or 'n/a'}"
            f" cards=[{cards_s}]"
        )
    lines.append("</active_promos>")
    return "\n".join(lines)


def degraded_snapshot(as_of: date | None = None, reason: str = "stale") -> PromoSnapshot:
    """Sentinel snapshot for failure modes (sync > 72h, query timeout, etc).

    `reason` is preserved for Langfuse trace correlation but isn't surfaced to
    the LLM — the prompt just sees the sentinel block.
    """
    return PromoSnapshot(
        as_of=as_of or datetime.now(tz=UTC).date(),
        entries=[],
        digest="degraded",
        total_count_before_cap=0,
        approx_tokens=0,
        status="degraded" if reason == "stale" else reason,
    )


def snapshot_to_dict(snapshot: PromoSnapshot) -> dict[str, Any]:
    """Serialize snapshot for API responses / Langfuse metadata."""
    return {
        "as_of": snapshot.as_of.isoformat(),
        "digest": snapshot.digest,
        "count": len(snapshot.entries),
        "total_before_cap": snapshot.total_count_before_cap,
        "approx_tokens": snapshot.approx_tokens,
        "status": snapshot.status,
    }


__all__ = [
    "MAX_PROMOS_IN_SNAPSHOT",
    "PromoSnapshot",
    "PromoSnapshotEntry",
    "build_promo_snapshot",
    "degraded_snapshot",
    "serialize_snapshot_for_prompt",
    "snapshot_to_dict",
]
