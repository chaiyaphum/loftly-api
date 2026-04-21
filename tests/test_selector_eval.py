"""Smoke test for the selector golden-set eval harness."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loftly.api.routes.selector import _load_context
from loftly.db.engine import get_sessionmaker
from loftly.schemas.selector import SelectorInput

FIXTURES_PATH = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "loftly"
    / "data"
    / "selector_eval"
    / "profiles.json"
)


async def test_fixtures_file_exists_and_has_25_profiles() -> None:
    with FIXTURES_PATH.open() as f:
        data = json.load(f)
    assert len(data["profiles"]) == 25
    for profile in data["profiles"]:
        assert profile["id"]
        assert profile["persona"] in {
            "miles-optimizer",
            "cashback-maximizer",
            "benefit-collector",
        }
        assert profile["expected_top3_card_slugs"]


async def test_harness_runs_3_profiles_end_to_end(seeded_db: object) -> None:
    """Exercise the scoring logic against the real deterministic provider."""
    _ = seeded_db
    from loftly.ai import get_provider

    with FIXTURES_PATH.open() as f:
        data = json.load(f)
    # Skip to benefit-collector profiles — the seeded catalog only has
    # bank_proprietary currencies so "miles" (airline-filtered) picks are
    # empty by design. benefits keeps all cards in play and exercises the
    # same ranking path without making seed the harness's problem.
    profiles = [p for p in data["profiles"] if p["persona"] == "benefit-collector"][:3]

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        context = await _load_context(session)

    provider = get_provider()
    results = []
    for profile in profiles:
        payload = SelectorInput.model_validate(
            {
                "monthly_spend_thb": profile["monthly_spend_thb"],
                "spend_categories": profile["spend_categories"],
                "current_cards": profile.get("current_cards", []),
                "goal": profile["goal"],
                "locale": "th",
            }
        )
        result = await provider.card_selector(payload, context)
        top1 = result.stack[0].slug if result.stack else None
        results.append(
            {
                "id": profile["id"],
                "top1": top1,
                "expected": profile["expected_top3_card_slugs"],
                "passed": top1 in profile["expected_top3_card_slugs"] if top1 else False,
            }
        )

    # Smoke assertion: at least some profiles should produce a stack.
    # (Recall threshold is enforced by the CLI harness, not here.)
    assert any(r["top1"] is not None for r in results), (
        f"No profiles produced a top1 pick — harness is broken: {results}"
    )


@pytest.mark.parametrize(
    "persona",
    ["miles-optimizer", "cashback-maximizer", "benefit-collector"],
)
async def test_all_personas_represented(persona: str) -> None:
    with FIXTURES_PATH.open() as f:
        data = json.load(f)
    matches = [p for p in data["profiles"] if p["persona"] == persona]
    assert len(matches) >= 3, f"Persona {persona} underrepresented in eval set."
