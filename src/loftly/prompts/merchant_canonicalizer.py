"""Loader for Prompt 8 — `merchant_canonicalizer`.

The live prompt text lives in the adjacent .md file so copy edits don't
require a Python review. This module exposes:

- `load()` → returns the prompt template text + a stable version string
  (bump when the .md changes; Langfuse traces include this version).
- `prompt_slug()` → short identifier for observability events.

The call site in `jobs/canonicalize_merchants.py` substitutes
`{candidates}` and `{canonical_merchants}` via `str.format_map` at
runtime. See `mvp/AI_PROMPTS.md §Prompt 8` for the contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from loftly.schemas.merchants import (
    CandidatePromo,
    MerchantCanonicalizerInput,
    MerchantCanonicalizerOutput,
)

_PROMPT_PATH = Path(__file__).with_suffix(".md")
# Bump when the .md file changes materially. PostHog / Langfuse trace include
# this so offline evals can stratify by version.
_PROMPT_VERSION = "2026-04-22"
_PROMPT_SLUG = "merchant_canonicalizer.v1"


@dataclass(frozen=True)
class MerchantCanonicalizerPrompt:
    text: str
    version: str
    slug: str


def load() -> MerchantCanonicalizerPrompt:
    """Return the prompt template + metadata."""
    return MerchantCanonicalizerPrompt(
        text=_PROMPT_PATH.read_text(encoding="utf-8"),
        version=_PROMPT_VERSION,
        slug=_PROMPT_SLUG,
    )


def prompt_slug() -> str:
    """Short identifier for observability events."""
    return _PROMPT_SLUG


__all__ = [
    "CandidatePromo",
    "MerchantCanonicalizerInput",
    "MerchantCanonicalizerOutput",
    "MerchantCanonicalizerPrompt",
    "load",
    "prompt_slug",
]
