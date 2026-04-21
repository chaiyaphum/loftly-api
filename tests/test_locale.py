"""Tests for `loftly.core.locale.detect_locale`.

Covers POST_V1 §2 AC-4: Thai is the default on ambiguous / missing / mixed
Accept-Language input; English only wins on a clear, Thai-absent signal.
"""

from __future__ import annotations

import pytest

from loftly.core.locale import detect_locale


def test_empty_accept_language_falls_back_to_thai() -> None:
    assert detect_locale(None) == "th"
    assert detect_locale("") == "th"


def test_plain_thai_header_returns_thai() -> None:
    assert detect_locale("th") == "th"


def test_english_only_header_returns_english() -> None:
    assert detect_locale("en-US") == "en"
    assert detect_locale("en-GB,en;q=0.8") == "en"


def test_mixed_thai_and_english_prefers_thai() -> None:
    # AC-4 — Thai wins even when both are present.
    assert detect_locale("th-TH,en-US;q=0.9") == "th"
    assert detect_locale("en-US,th-TH;q=0.7") == "th"


def test_unknown_languages_fall_back_to_thai() -> None:
    assert detect_locale("en-GB,fr-FR") == "en"  # English still wins — Thai absent
    assert detect_locale("fr-FR,de-DE") == "th"  # No Thai, no English → Thai default
    assert detect_locale("ja-JP;q=0.9") == "th"


def test_explicit_override_beats_header() -> None:
    assert detect_locale("en-US", override="th") == "th"
    assert detect_locale("th-TH", override="en") == "en"


def test_override_ignores_unknown_values() -> None:
    # Guards against callers passing a DB value like "auto" — we fall back
    # to header-based detection rather than coercing unknown strings.
    assert detect_locale("th-TH", override="auto") == "th"
    assert detect_locale("en-US", override="") == "en"


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("th-TH", "th"),
        ("TH", "th"),
        ("th-TH;q=1.0, en;q=0.5", "th"),
        ("en;q=0.5, th;q=0.1", "th"),
        ("en-US;q=0.9", "en"),
        (" en-US , fr-FR ", "en"),
    ],
)
def test_detect_locale_table(header: str, expected: str) -> None:
    assert detect_locale(header) == expected
