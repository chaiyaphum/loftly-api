"""Tests for Prompt 6 — `personalized_welcome_email` loader + text contract.

Locks the text file to the shape required by `mvp/AI_PROMPTS.md §Prompt 6`
and `mvp/POST_V1.md §2`:

- Loader returns non-empty text + the pinned version
- Required runtime placeholders are present
- Both `## System` and `## User` sections exist
- Banned phrases (BRAND.md §4) are absent
- Thai and English few-shot examples are both present
"""

from __future__ import annotations

import pytest

from loftly.prompts.personalized_welcome_email import (
    WelcomeEmailPrompt,
    load,
)

PINNED_VERSION = "2026-10-15"

REQUIRED_PLACEHOLDERS = (
    "{selector_stack}",
    "{magic_link_url}",
    "{locale}",
    "{user_display_name}",
)

BANNED_PHRASES = (
    "ปฏิวัติ",
    "ล้ำยุค",
    "synergy",
    "cutting-edge",
    "revolutionary",
    "disrupt",
    "next-gen",
)


def test_load_returns_welcome_email_prompt_dataclass() -> None:
    prompt = load()
    assert isinstance(prompt, WelcomeEmailPrompt)


def test_load_returns_non_empty_text() -> None:
    prompt = load()
    assert prompt.text.strip(), "prompt text must not be empty"
    # Some meaningful body, not just a header.
    assert len(prompt.text) > 500, "prompt text looks suspiciously short"


def test_load_returns_pinned_version() -> None:
    prompt = load()
    assert prompt.version == PINNED_VERSION


@pytest.mark.parametrize("placeholder", REQUIRED_PLACEHOLDERS)
def test_text_contains_required_placeholder(placeholder: str) -> None:
    prompt = load()
    assert placeholder in prompt.text, (
        f"prompt must include runtime placeholder {placeholder!r}"
    )


def test_text_has_system_section() -> None:
    prompt = load()
    assert "## System" in prompt.text


def test_text_has_user_section() -> None:
    prompt = load()
    assert "## User" in prompt.text


def _scrubbed_text(raw: str) -> str:
    """Strip the explicit `<!-- BANNED_PHRASES_BEGIN -->` … `END` block from
    the prompt before scanning. That block intentionally names the banlist so
    the model can honor it; it should not count as a violation.
    """
    begin = "<!-- BANNED_PHRASES_BEGIN -->"
    end = "<!-- BANNED_PHRASES_END -->"
    if begin in raw and end in raw:
        before, rest = raw.split(begin, 1)
        _, after = rest.split(end, 1)
        return (before + after).lower()
    return raw.lower()


@pytest.mark.parametrize("phrase", BANNED_PHRASES)
def test_text_has_no_banned_phrase(phrase: str) -> None:
    prompt = load()
    scrubbed = _scrubbed_text(prompt.text)
    assert phrase.lower() not in scrubbed, (
        f"prompt body must not contain banned phrase {phrase!r}"
    )


def test_banned_phrases_block_is_present() -> None:
    """The scrubber requires the explicit marker block to exist so that
    BRAND.md §4 banlist names are isolated from the rest of the prompt."""
    prompt = load()
    assert "<!-- BANNED_PHRASES_BEGIN -->" in prompt.text
    assert "<!-- BANNED_PHRASES_END -->" in prompt.text


def test_text_documents_both_locales() -> None:
    """Thai-primary + English variant are both required per POST_V1 §2."""
    prompt = load()
    assert 'locale == "th"' in prompt.text
    assert 'locale == "en"' in prompt.text


def test_load_is_idempotent() -> None:
    a = load()
    b = load()
    assert a == b
