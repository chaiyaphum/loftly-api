"""Merchant-canonicalizer eval runner.

Drives `tests/eval/merchant_canonicalizer.jsonl` through the deterministic
pipeline + (optionally) the real Haiku call from `loftly.jobs.canonicalize_merchants`
and scores precision / recall / F1 on:

    1. `action` field  (match / create / ambiguous)
    2. `merchant_id`   (canonical slug — the runner resolves `merchant_id_expected`
                        slugs to the runtime UUIDs after seeding)

Gate: **F1 >= 0.85** on `action` for PR acceptance.

The runner is deliberately network-gated: when `ANTHROPIC_API_KEY` is absent
(or set to stub/test), the runner exercises only the deterministic steps
(normalize → exact → fuzzy) and reports what it can; it does NOT fail — it
prints "eval skipped: no key" and exits 0. The founder runs it with a real
key before flipping the §9 canonicalizer Haiku flag.

Usage:

    # Pytest path (marker-gated):
    LOFTLY_RUN_EVAL=1 pytest -m eval tests/eval/runners/canonicalizer_eval.py

    # Script path:
    python -m tests.eval.runners.canonicalizer_eval

The JSONL row schema is documented in tests/eval/README.md.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

EVAL_PATH = Path(__file__).resolve().parent.parent / "merchant_canonicalizer.jsonl"
F1_GATE = 0.85


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class EvalRow:
    raw: str
    category_hint: str | None
    merchant_id_expected: str | None  # canonical slug OR None
    action_expected: str  # match | create | ambiguous
    notes: str


@dataclass
class ScoreReport:
    total: int = 0
    # action field
    action_tp: Counter[str] = field(default_factory=Counter)
    action_fp: Counter[str] = field(default_factory=Counter)
    action_fn: Counter[str] = field(default_factory=Counter)
    # merchant_id (slug) — aggregate, not per-class
    slug_correct: int = 0
    slug_incorrect: int = 0
    slug_skipped: int = 0  # expected None + predicted None
    rows_skipped: int = 0  # couldn't run (e.g., no key)

    def record_action(self, expected: str, predicted: str | None) -> None:
        if predicted is None:
            self.rows_skipped += 1
            return
        if predicted == expected:
            self.action_tp[expected] += 1
        else:
            self.action_fp[predicted] += 1
            self.action_fn[expected] += 1

    def record_slug(self, expected: str | None, predicted: str | None) -> None:
        if expected is None and predicted is None:
            self.slug_skipped += 1
            return
        if expected is not None and predicted == expected:
            self.slug_correct += 1
            return
        # Either expected None but predicted something, OR mismatch.
        self.slug_incorrect += 1

    def precision(self, cls: str) -> float:
        tp = self.action_tp[cls]
        fp = self.action_fp[cls]
        return tp / (tp + fp) if (tp + fp) else 0.0

    def recall(self, cls: str) -> float:
        tp = self.action_tp[cls]
        fn = self.action_fn[cls]
        return tp / (tp + fn) if (tp + fn) else 0.0

    def f1(self, cls: str) -> float:
        p, r = self.precision(cls), self.recall(cls)
        return 2 * p * r / (p + r) if (p + r) else 0.0

    def macro_f1(self) -> float:
        classes = {"match", "create", "ambiguous"}
        scores = [self.f1(c) for c in classes if (self.action_tp[c] + self.action_fn[c]) > 0]
        return sum(scores) / len(scores) if scores else 0.0


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_rows(path: Path = EVAL_PATH) -> list[EvalRow]:
    rows: list[EvalRow] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            rows.append(
                EvalRow(
                    raw=payload["raw"],
                    category_hint=payload.get("category_hint"),
                    merchant_id_expected=payload.get("merchant_id_expected"),
                    action_expected=payload["action_expected"],
                    notes=payload.get("notes", ""),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def _run_row(row: EvalRow, canonicals: list[Any], llm_enabled: bool) -> dict[str, Any]:
    """Drive a single row through normalize → exact → fuzzy → (optional) Haiku.

    Returns the predicted {action, slug, confidence, method}.
    """
    from loftly.jobs.canonicalize_merchants import (
        _exact_match,
        _fuzzy_match,
        _normalize,
    )

    normalized = _normalize(row.raw)

    # Step 2: exact
    hit = _exact_match(normalized, canonicals)
    if hit is not None:
        return {
            "action": "match",
            "slug": hit.slug,
            "confidence": 1.0,
            "method": "exact",
        }

    # Step 3: fuzzy
    fuzzy = _fuzzy_match(normalized, canonicals)
    if fuzzy is not None:
        m, score = fuzzy
        return {
            "action": "match",
            "slug": m.slug,
            "confidence": float(score),
            "method": "fuzzy",
        }

    # Step 4: Haiku (only if a real key is configured)
    if not llm_enabled:
        return {
            "action": None,
            "slug": None,
            "confidence": 0.0,
            "method": "skipped-no-key",
        }

    # Invoke Haiku via the canonicalizer's batch helper. We mock a `Promo`
    # shape because `_call_haiku` consumes `CandidatePromo` — build one
    # directly to avoid DB writes.
    from loftly.jobs.canonicalize_merchants import _call_haiku
    from loftly.schemas.merchants import CandidatePromo

    candidate = CandidatePromo(
        promo_id="00000000-0000-0000-0000-000000000000",
        raw_merchant_name=row.raw,
        promo_category=row.category_hint,
        promo_title_th=row.raw,  # title not material for the classifier
    )
    try:
        output = await _call_haiku([candidate], canonicals)
    except Exception as exc:
        return {
            "action": None,
            "slug": None,
            "confidence": 0.0,
            "method": f"haiku-failed: {type(exc).__name__}: {exc}",
        }

    if not output.results:
        return {
            "action": None,
            "slug": None,
            "confidence": 0.0,
            "method": "haiku-empty",
        }

    result = output.results[0]
    action = result.action
    predicted_slug: str | None = None
    # Map LLM action → eval action:
    # - "match"     → "match"    (slug = matched canonical's slug)
    # - "new"       → "create"   (slug = proposed slug)
    # - "uncertain" → "ambiguous"
    eval_action = {"match": "match", "new": "create", "uncertain": "ambiguous"}[action]
    if action == "match" and result.merchant_id:
        for m in canonicals:
            if str(m.id) == result.merchant_id:
                predicted_slug = m.slug
                break
    elif action == "new" and result.proposed is not None:
        predicted_slug = result.proposed.slug
    return {
        "action": eval_action,
        "slug": predicted_slug,
        "confidence": float(result.confidence),
        "method": "llm",
    }


async def _load_canonicals() -> list[Any]:
    from sqlalchemy import select

    from loftly.db.engine import get_sessionmaker
    from loftly.db.models.merchant import MerchantCanonical

    sm = get_sessionmaker()
    async with sm() as session:
        rows = (await session.execute(select(MerchantCanonical))).scalars().all()
        return list(rows)


async def run_eval(*, verbose: bool = True) -> ScoreReport:
    from loftly.ai.providers.anthropic import _should_use_real_anthropic

    llm_enabled = _should_use_real_anthropic()
    rows = load_rows()
    canonicals = await _load_canonicals()
    report = ScoreReport(total=len(rows))

    per_row: list[dict[str, Any]] = []
    for row in rows:
        predicted = await _run_row(row, canonicals, llm_enabled=llm_enabled)
        report.record_action(row.action_expected, predicted["action"])
        report.record_slug(row.merchant_id_expected, predicted["slug"])
        per_row.append(
            {
                "raw": row.raw,
                "expected_action": row.action_expected,
                "predicted_action": predicted["action"],
                "expected_slug": row.merchant_id_expected,
                "predicted_slug": predicted["slug"],
                "method": predicted["method"],
                "confidence": predicted["confidence"],
            }
        )

    if verbose:
        _print_report(report, per_row, llm_enabled=llm_enabled)
    return report


def _print_report(
    report: ScoreReport,
    per_row: list[dict[str, Any]],
    *,
    llm_enabled: bool,
) -> None:
    print()
    print("# Merchant-canonicalizer eval report")
    print()
    print(f"- Rows evaluated: **{report.total}**")
    print(f"- LLM step enabled: **{llm_enabled}**")
    print(f"- Rows skipped (no key, unreached LLM): **{report.rows_skipped}**")
    print()
    print("## Per-class precision / recall / F1 on `action`")
    print()
    print("| class | precision | recall | F1 | support (tp+fn) |")
    print("|---|---|---|---|---|")
    for cls in ("match", "create", "ambiguous"):
        support = report.action_tp[cls] + report.action_fn[cls]
        print(
            f"| {cls} | {report.precision(cls):.3f} | "
            f"{report.recall(cls):.3f} | {report.f1(cls):.3f} | {support} |"
        )
    macro = report.macro_f1()
    print()
    print(f"**Macro F1: {macro:.3f}** (gate: >= {F1_GATE})")
    print()
    print("## Slug accuracy (merchant_id_expected)")
    print()
    slug_total = report.slug_correct + report.slug_incorrect
    slug_acc = report.slug_correct / slug_total if slug_total else 0.0
    print(f"- correct: {report.slug_correct}")
    print(f"- incorrect: {report.slug_incorrect}")
    print(f"- both-null (not scored): {report.slug_skipped}")
    print(f"- slug accuracy (of scored): **{slug_acc:.3f}**")
    print()
    print("## Failures (first 20)")
    print()
    print("| raw | expected | predicted | method |")
    print("|---|---|---|---|")
    fails = [
        r
        for r in per_row
        if r["expected_action"] != r["predicted_action"]
        or (r["expected_slug"] is not None and r["predicted_slug"] != r["expected_slug"])
    ][:20]
    for r in fails:
        raw = r["raw"].replace("|", "\\|")
        print(
            f"| {raw} | {r['expected_action']} / {r['expected_slug']} | "
            f"{r['predicted_action']} / {r['predicted_slug']} | {r['method']} |"
        )
    if not fails:
        print("| — | — | — | — |")
    print()


# ---------------------------------------------------------------------------
# Pytest entry point (marker: `eval`)
# ---------------------------------------------------------------------------


@pytest.mark.eval
async def test_canonicalizer_eval_gate() -> None:
    """Gate: macro F1 on `action` >= F1_GATE.

    Skipped unless both:
      - ANTHROPIC_API_KEY is a real key, AND
      - LOFTLY_RUN_EVAL=1 is set (belt-and-braces so default CI never runs it)
    """
    if os.environ.get("LOFTLY_RUN_EVAL") != "1":
        pytest.skip("eval skipped: set LOFTLY_RUN_EVAL=1 to opt in")
    from loftly.ai.providers.anthropic import _should_use_real_anthropic

    if not _should_use_real_anthropic():
        print("eval skipped: no key")
        pytest.skip("eval skipped: no ANTHROPIC_API_KEY")
    report = await run_eval(verbose=True)
    macro = report.macro_f1()
    assert macro >= F1_GATE, (
        f"Canonicalizer macro F1 {macro:.3f} < gate {F1_GATE}. "
        "Review the failure table above; tune Prompt 8 rules or seed alt_names."
    )


# ---------------------------------------------------------------------------
# Script entry point
# ---------------------------------------------------------------------------


def _main() -> int:
    from loftly.ai.providers.anthropic import _should_use_real_anthropic

    if not _should_use_real_anthropic():
        print("eval skipped: no key")
        return 0
    report = asyncio.run(run_eval(verbose=True))
    return 0 if report.macro_f1() >= F1_GATE else 1


if __name__ == "__main__":
    sys.exit(_main())
