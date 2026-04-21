"""Golden-set evaluation harness for the Card Selector.

Loads `src/loftly/data/selector_eval/profiles.json`, runs each profile through
the configured provider, and scores two recalls:

- `recall_top1`: does the provider's rank-1 pick appear in the fixture's
  `expected_top3_card_slugs`?
- `recall_top3`: does *any* slug in the provider's top-3 stack appear in the
  fixture's expected list?

Acceptance threshold from AI_PROMPTS.md §Evaluation: **recall_top1 ≥ 0.75**.
The threshold is overridable via env `LOFTLY_EVAL_MIN_RECALL` so CI can tighten
it without a code change. Below threshold, the script exits non-zero so CI
blocks the merge.

Usage:
    uv run python -m scripts.run_selector_eval
    uv run python -m scripts.run_selector_eval --provider deterministic
    uv run python -m scripts.run_selector_eval --provider anthropic --verbose
    uv run python -m scripts.run_selector_eval --json > eval-result.json

The script uses an in-memory sqlite DB seeded with the default catalog so it
works in CI without real infra. For production-like runs, override
`DATABASE_URL` before invoking.

CI failure playbook: download the `selector-eval-report` artifact, open
`eval-result.json`, inspect `failed_profiles` — for each, decide whether to
tighten the Selector prompt/provider or adjust the profile's expected slugs
(the fixture is a living spec, but changes need review).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Force test-like settings so an unset DATABASE_URL doesn't crash the import.
os.environ.setdefault("LOFTLY_ENV", "test")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("JWT_SIGNING_KEY", "eval-secret")

FIXTURES_PATH = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "loftly"
    / "data"
    / "selector_eval"
    / "profiles.json"
)
DEFAULT_RECALL_THRESHOLD = 0.75


def _resolve_threshold() -> float:
    """Read the recall floor from env (CI-overridable) with a sane default."""
    raw = os.environ.get("LOFTLY_EVAL_MIN_RECALL")
    if raw is None or raw == "":
        return DEFAULT_RECALL_THRESHOLD
    try:
        return float(raw)
    except ValueError:
        print(
            f"[WARN] LOFTLY_EVAL_MIN_RECALL={raw!r} is not a float; "
            f"falling back to {DEFAULT_RECALL_THRESHOLD}",
            file=sys.stderr,
        )
        return DEFAULT_RECALL_THRESHOLD


@dataclass
class EvalResult:
    profile_id: str
    persona: str
    top1_slug: str | None
    top3_slugs: list[str]
    expected: list[str]
    passed_top1: bool
    passed_top3: bool


async def _prepare_db() -> None:
    """Create schema + seed catalog for the eval run."""
    from loftly.db.engine import get_engine, get_sessionmaker
    from loftly.db.models import Base
    from loftly.db.seed import seed_all

    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await seed_all(session)


async def _run_one(profile: dict[str, Any]) -> EvalResult:
    from loftly.api.routes.selector import _load_context
    from loftly.db.engine import get_sessionmaker
    from loftly.schemas.selector import SelectorInput

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        context = await _load_context(session)

    payload = SelectorInput.model_validate(
        {
            "monthly_spend_thb": profile["monthly_spend_thb"],
            "spend_categories": profile["spend_categories"],
            "current_cards": profile.get("current_cards", []),
            "goal": profile["goal"],
            "locale": "th",
        }
    )
    # Import selector provider lazily so tests can swap it.
    from loftly.ai import get_provider

    provider = get_provider()
    result = await provider.card_selector(payload, context)
    top3 = [pick.slug for pick in result.stack[:3]]
    top1 = top3[0] if top3 else None
    expected: list[str] = profile["expected_top3_card_slugs"]
    expected_set = set(expected)
    return EvalResult(
        profile_id=profile["id"],
        persona=profile["persona"],
        top1_slug=top1,
        top3_slugs=top3,
        expected=expected,
        passed_top1=top1 in expected_set if top1 else False,
        passed_top3=any(slug in expected_set for slug in top3),
    )


async def _main(
    provider_override: str | None,
    verbose: bool,
    limit: int | None,
    emit_json: bool,
) -> int:
    if provider_override:
        os.environ["LOFTLY_LLM_PROVIDER"] = provider_override
    # Re-resolve settings after env override.
    from loftly.core.settings import get_settings

    get_settings.cache_clear()
    await _prepare_db()

    with FIXTURES_PATH.open() as f:
        fixtures = json.load(f)
    profiles = fixtures["profiles"]
    if limit:
        profiles = profiles[:limit]

    results: list[EvalResult] = []
    for profile in profiles:
        try:
            res = await _run_one(profile)
        except Exception as exc:
            print(f"[ERROR] {profile['id']}: {exc}", file=sys.stderr)
            results.append(
                EvalResult(
                    profile_id=profile["id"],
                    persona=profile["persona"],
                    top1_slug=None,
                    top3_slugs=[],
                    expected=profile["expected_top3_card_slugs"],
                    passed_top1=False,
                    passed_top3=False,
                )
            )
            continue
        results.append(res)
        if verbose and not emit_json:
            marker = "PASS" if res.passed_top1 else "FAIL"
            print(f"[{marker}] {res.profile_id} top1={res.top1_slug} expected={res.expected}")

    total = len(results)
    passed_top1 = sum(1 for r in results if r.passed_top1)
    passed_top3 = sum(1 for r in results if r.passed_top3)
    recall_top1 = passed_top1 / total if total else 0.0
    recall_top3 = passed_top3 / total if total else 0.0
    threshold = _resolve_threshold()

    failed_profiles = [
        {
            "profile_id": r.profile_id,
            "persona": r.persona,
            "top1_slug": r.top1_slug,
            "top3_slugs": r.top3_slugs,
            "expected": r.expected,
        }
        for r in results
        if not r.passed_top1
    ]

    report: dict[str, Any] = {
        "provider": provider_override or "default",
        "profile_count": total,
        "passed_top1": passed_top1,
        "passed_top3": passed_top3,
        "recall_top1": round(recall_top1, 4),
        "recall_top3": round(recall_top3, 4),
        "threshold": threshold,
        "failed_profiles": failed_profiles,
    }

    if emit_json:
        # Emit a single JSON document on stdout for CI artifact capture.
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print()
        print(f"Selector eval — provider={provider_override or 'default'}")
        print(f"  total:       {total}")
        print(f"  passed_top1: {passed_top1}")
        print(f"  failed_top1: {total - passed_top1}")
        print(f"  recall_top1: {recall_top1:.3f}")
        print(f"  recall_top3: {recall_top3:.3f}")
        print(f"  threshold:   {threshold}")

    if recall_top1 < threshold:
        print(
            f"FAIL — recall_top1 {recall_top1:.3f} below threshold {threshold}",
            file=sys.stderr,
        )
        return 1
    if not emit_json:
        print("PASS")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Selector golden-set eval.")
    parser.add_argument(
        "--provider",
        choices=["deterministic", "anthropic"],
        default=None,
        help="Override the configured LLM provider for this run.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print per-profile pass/fail.")
    parser.add_argument("--limit", type=int, default=None, help="Run only N profiles (smoke).")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON report on stdout (for CI artifacts).",
    )
    args = parser.parse_args()

    exit_code = asyncio.run(_main(args.provider, args.verbose, args.limit, args.json))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
