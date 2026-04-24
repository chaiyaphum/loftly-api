"""Selector golden-set eval runner.

Merges:
  - `src/loftly/data/selector_eval/profiles.json` (v1 — 25 profiles)
  - `tests/eval/selector_golden_v2.jsonl`       (v2 — +10 edge cases)

Scores top-1 recall (pick-in-expected-top-3). Gate: **>= 0.75** vs v1's
baseline — failure of the v2 set should not silently regress the v1 set,
so the runner reports both separately + the merged number.

Runs the deterministic provider by default. When `ANTHROPIC_API_KEY` is
a real key AND `LOFTLY_EVAL_PROVIDER=anthropic` is set, it drives the
real Sonnet provider instead.

Usage:

    LOFTLY_RUN_EVAL=1 pytest -m eval tests/eval/runners/selector_eval.py
    python -m tests.eval.runners.selector_eval
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

V1_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "src"
    / "loftly"
    / "data"
    / "selector_eval"
    / "profiles.json"
)
V2_PATH = Path(__file__).resolve().parent.parent / "selector_golden_v2.jsonl"
RECALL_GATE = 0.75


@dataclass
class ProfileResult:
    id: str
    source: str  # "v1" or "v2"
    top1: str | None
    expected: list[str]
    passed: bool


@dataclass
class EvalReport:
    results: list[ProfileResult] = field(default_factory=list)

    def recall(self, *, source: str | None = None) -> float:
        rows = (
            self.results
            if source is None
            else [r for r in self.results if r.source == source]
        )
        if not rows:
            return 0.0
        hits = sum(1 for r in rows if r.passed)
        return hits / len(rows)


def _load_v1() -> list[dict[str, Any]]:
    if not V1_PATH.exists():
        return []
    with V1_PATH.open("r", encoding="utf-8") as fh:
        return list(json.load(fh).get("profiles", []))


def _load_v2() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with V2_PATH.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


async def _run_one(profile: dict[str, Any], provider: Any, context: Any) -> str | None:
    from loftly.schemas.selector import SelectorInput

    payload = SelectorInput.model_validate(
        {
            "monthly_spend_thb": profile["monthly_spend_thb"],
            "spend_categories": profile["spend_categories"],
            "current_cards": profile.get("current_cards", []),
            "goal": profile["goal"],
            "locale": profile.get("locale", "th"),
        }
    )
    try:
        result = await provider.card_selector(payload, context)
    except Exception as exc:
        print(f"  [{profile['id']}] provider error: {type(exc).__name__}: {exc}")
        return None
    return result.stack[0].slug if result.stack else None


async def run_eval(*, verbose: bool = True) -> EvalReport:
    from loftly.ai import get_provider
    from loftly.api.routes.selector import _load_context
    from loftly.db.engine import get_sessionmaker

    sm = get_sessionmaker()
    async with sm() as session:
        context = await _load_context(session)

    provider = get_provider()
    report = EvalReport()

    for source, rows in (("v1", _load_v1()), ("v2", _load_v2())):
        for profile in rows:
            expected = profile.get("expected_top3_card_slugs", []) or []
            top1 = await _run_one(profile, provider, context)
            passed = top1 in expected if top1 else False
            report.results.append(
                ProfileResult(
                    id=profile["id"],
                    source=source,
                    top1=top1,
                    expected=expected,
                    passed=passed,
                )
            )

    if verbose:
        _print_report(report)
    return report


def _print_report(report: EvalReport) -> None:
    print()
    print("# Selector golden-set eval report")
    print()
    v1 = report.recall(source="v1")
    v2 = report.recall(source="v2")
    overall = report.recall()
    v1_n = sum(1 for r in report.results if r.source == "v1")
    v2_n = sum(1 for r in report.results if r.source == "v2")
    print(f"- v1 profiles: **{v1_n}** — top-1 recall **{v1:.3f}**")
    print(f"- v2 profiles: **{v2_n}** — top-1 recall **{v2:.3f}**")
    print(f"- overall:     **{len(report.results)}** — top-1 recall **{overall:.3f}**")
    print(f"- gate: overall recall >= **{RECALL_GATE}**")
    print()
    print("## Failures")
    print()
    print("| source | id | top1 | expected |")
    print("|---|---|---|---|")
    fails = [r for r in report.results if not r.passed]
    for r in fails:
        exp = ", ".join(r.expected[:3])
        print(f"| {r.source} | {r.id} | {r.top1 or '—'} | {exp} |")
    if not fails:
        print("| — | — | — | — |")
    print()


@pytest.mark.eval
async def test_selector_eval_gate(seeded_db: object) -> None:
    """Gate: overall top-1 recall >= RECALL_GATE across v1 + v2 profiles."""
    _ = seeded_db
    if os.environ.get("LOFTLY_RUN_EVAL") != "1":
        pytest.skip("eval skipped: set LOFTLY_RUN_EVAL=1 to opt in")
    # Selector eval works with the deterministic provider too — no key
    # required unless the reviewer wants to exercise real Sonnet.
    report = await run_eval(verbose=True)
    recall = report.recall()
    assert recall >= RECALL_GATE, (
        f"Selector top-1 recall {recall:.3f} < gate {RECALL_GATE}. "
        "Review the failure table; either fix the ranking regression or "
        "update the golden set with a Decision log entry."
    )


def _main() -> int:
    # Honour the same skip rule as the pytest path when invoked via the
    # real Anthropic provider — deterministic provider needs no key.
    wants_anthropic = os.environ.get("LOFTLY_EVAL_PROVIDER") == "anthropic"
    if wants_anthropic:
        from loftly.ai.providers.anthropic import _should_use_real_anthropic

        if not _should_use_real_anthropic():
            print("eval skipped: no key (LOFTLY_EVAL_PROVIDER=anthropic requires ANTHROPIC_API_KEY)")
            return 0
    report = asyncio.run(run_eval(verbose=True))
    return 0 if report.recall() >= RECALL_GATE else 1


if __name__ == "__main__":
    sys.exit(_main())
