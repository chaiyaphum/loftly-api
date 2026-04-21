"""Loader for Prompt 6 — personalized welcome email.

Live text lives in personalized_welcome_email.md (adjacent). This module
exposes a typed accessor so the email composer can swap prompt versions
without touching call sites.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

_PROMPT_PATH = Path(__file__).with_suffix(".md")
_PROMPT_VERSION = "2026-10-15"  # bump when text changes; PostHog events include this


@dataclass(frozen=True)
class WelcomeEmailPrompt:
    text: str
    version: str


def load() -> WelcomeEmailPrompt:
    return WelcomeEmailPrompt(
        text=_PROMPT_PATH.read_text(encoding="utf-8"),
        version=_PROMPT_VERSION,
    )


__all__ = ["WelcomeEmailPrompt", "load"]
