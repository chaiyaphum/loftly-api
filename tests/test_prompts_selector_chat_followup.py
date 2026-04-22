"""Tests for `selector_chat_followup` prompt loader (POST_V1 §1, PR-8).

Scope is the prompt artifact only — route + provider wiring lands in PR-9
(`POST /v1/selector/{session_id}/chat`) and has its own test module.

Guarantees asserted here:

1. `load()` returns a non-empty `ChatFollowupPrompt`.
2. `version` matches the `PROMPT_VERSION` constant — drift means someone edited
   one side without the other.
3. All four placeholders from `mvp/AI_PROMPTS.md §Prompt 5` are present in the
   markdown source so the route can substitute them.
4. Exactly two few-shot examples labelled `explain` + `what-if` exist, so
   Haiku sees both classifier branches in-context.
5. No banned phrase slipped into the prompt text itself (catches accidental
   examples that would bypass the route linter).
"""

from __future__ import annotations

import re

from loftly.prompts.selector_chat_followup import (
    BANNED_PHRASES,
    CLASSIFIER_CATEGORIES,
    COST_CAP_THB,
    HAIKU_TIMEOUT_S,
    MAX_QUESTION_CHARS,
    PROMPT_NAME,
    PROMPT_VERSION,
    RATE_LIMIT_PER_SESSION,
    REQUIRED_PLACEHOLDERS,
    ChatFollowupPrompt,
    load,
    prompt_slug,
)


def test_load_returns_non_empty_chat_followup_prompt() -> None:
    """`load()` returns a populated dataclass with content."""
    prompt = load()
    assert isinstance(prompt, ChatFollowupPrompt)
    assert len(prompt.text) > 500, "prompt text suspiciously short"
    assert prompt.banned_phrases == BANNED_PHRASES
    assert prompt.placeholders == REQUIRED_PLACEHOLDERS


def test_version_matches_constant() -> None:
    """Loader `version` field tracks `PROMPT_VERSION` exactly — no drift."""
    prompt = load()
    assert prompt.version == PROMPT_VERSION
    assert PROMPT_VERSION.startswith("v")


def test_all_required_placeholders_present_in_prompt_text() -> None:
    """Every placeholder the route substitutes must appear in the md source."""
    prompt = load()
    for placeholder in REQUIRED_PLACEHOLDERS:
        assert placeholder in prompt.text, (
            f"required placeholder {placeholder!r} missing from prompt text"
        )


def test_four_required_placeholders_exact() -> None:
    """Exactly the four POST_V1 §1 placeholders — no extras, no omissions."""
    assert set(REQUIRED_PLACEHOLDERS) == {
        "{question_th}",
        "{classifier_category}",
        "{selector_context}",
        "{cached_result}",
    }


def test_two_few_shot_examples_one_explain_one_what_if() -> None:
    """Prompt contains one `explain` example + one `what-if` example.

    Grep against the labelled example headers — Haiku sees both classifier
    branches in-context, which matters for grounding + format adherence.
    """
    prompt = load()
    text = prompt.text.lower()
    # Header labels from the md source.
    assert "example 1 — explain case" in text, "explain example header missing"
    assert "example 2 — what-if case" in text, "what-if example header missing"
    # And the classifier lines inside each example block.
    classifier_lines = re.findall(r"classifier:\s*(explain|what-if|other)", text)
    assert classifier_lines.count("explain") >= 1, "no `explain` classifier in examples"
    assert classifier_lines.count("what-if") >= 1, "no `what-if` classifier in examples"


def test_no_banned_phrases_in_prompt_text() -> None:
    """The prompt itself must not contain any banned phrase.

    The "Banned phrases" block at the bottom of the md enumerates them
    descriptively — we exclude that section before scanning so its listing
    does not trigger the test.
    """
    prompt = load()
    # Drop everything from the banned-phrase section heading onward; that
    # section legitimately quotes the banned words as a list.
    scan_region = prompt.text.split("## Banned phrases", 1)[0]
    for phrase in BANNED_PHRASES:
        assert phrase.lower() not in scan_region.lower(), (
            f"banned phrase {phrase!r} leaked into prompt body (before the Banned-phrases section)"
        )


def test_classifier_categories_enum() -> None:
    """Route + prompt must agree on the 3-value classifier enum."""
    assert CLASSIFIER_CATEGORIES == ("explain", "what-if", "other")


def test_budget_constants_match_spec() -> None:
    """Latency + cost budgets track `AI_PROMPTS.md §Prompt 5 — Latency + cost budget`.

    These values end up in the route's pre-flight guard (PR-9). If the spec
    changes, this test should fail first so the constants are updated.
    """
    assert COST_CAP_THB == 0.10
    assert HAIKU_TIMEOUT_S == 5.0
    assert RATE_LIMIT_PER_SESSION == 10
    assert MAX_QUESTION_CHARS == 500


def test_prompt_slug_format() -> None:
    """Slug is `{name}@{version}` — stamped into PostHog + Langfuse."""
    assert prompt_slug() == f"{PROMPT_NAME}@{PROMPT_VERSION}"
    assert prompt_slug() == "selector_chat_followup@v1"


def test_load_is_cached() -> None:
    """Repeated `load()` calls return the same object — `lru_cache` works."""
    assert load() is load()
