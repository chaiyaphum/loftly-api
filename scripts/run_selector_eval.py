"""Golden-set evaluation harness for the Card Selector.

Loads `src/loftly/data/selector_eval/profiles.json`, runs each profile through
the configured provider, and scores top-1 recall: does the provider's primary
(rank 1) pick appear in the fixture's `expected_top3_card_slugs`?

Acceptance threshold from AI_PROMPTS.md §Evaluation: **recall ≥ 0.75**.
Below that, the script exits non-zero so CI blocks the merge.

Usage:
    uv run python -m scripts.run_selector_eval
    uv run python -m scripts.run_selector_eval --provider deterministic
    uv run python -m scripts.run_selector_eval --provider anthropic --verbose

The script uses an in-memory sqlite DB seeded with the default catalog so it
works in CI without real infra. For production-like runs, override
`DATABASE_URL` before invoking.
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
RECALL_THRESHOLD = 0.75


@dataclass
class EvalResult:
    profile_id: str
    persona: str
    top1_slug: str | None
    expected: list[str]
    passed: bool


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
    top1 = result.stack[0].slug if result.stack else None
    expected: list[str] = profile["expected_top3_card_slugs"]
    return EvalResult(
        profile_id=profile["id"],
        persona=profile["persona"],
        top1_slug=top1,
        expected=expected,
        passed=top1 in expected if top1 else False,
    )


async def _main(provider_override: str | None, verbose: bool, limit: int | None) -> int:
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
                    expected=profile["expected_top3_card_slugs"],
                    passed=False,
                )
            )
            continue
        results.append(res)
        if verbose:
            marker = "PASS" if res.passed else "FAIL"
            print(f"[{marker}] {res.profile_id} top1={res.top1_slug} expected={res.expected}")

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    recall = passed / total if total else 0.0

    print()
    print(f"Selector eval — provider={provider_override or 'default'}")
    print(f"  total:  {total}")
    print(f"  passed: {passed}")
    print(f"  failed: {total - passed}")
    print(f"  recall: {recall:.3f}")
    print(f"  threshold: {RECALL_THRESHOLD}")

    if recall < RECALL_THRESHOLD:
        print(f"FAIL — recall {recall:.3f} below threshold {RECALL_THRESHOLD}", file=sys.stderr)
        return 1
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
    args = parser.parse_args()

    exit_code = asyncio.run(_main(args.provider, args.verbose, args.limit))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
