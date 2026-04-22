"""Versioned prompt loader for `selector_chat_followup` (POST_V1 §1).

Per `mvp/AI_PROMPTS.md §Prompt 5` — handle follow-up questions on
`/selector/results/[session_id]` (explain ranking, what-if scenarios) via
Haiku 4.5, reusing the 50k Selector cache + cached SelectorResult.

Loading contract (mirrors the pattern adopted for Prompt 6
`personalized_welcome_email` in the adjacent PR-3 — once that lands the two
loaders share the same shape):

- `load() -> ChatFollowupPrompt` returns a frozen dataclass holding the prompt
  text, version, banned-phrase list, and required placeholder names.
- `render(variables) -> RenderedPrompt` is what the route calls at request
  time. It splits the markdown into the static `system` half and the
  per-request `user` half, substitutes `{placeholder}` tokens in the user
  block, and returns a plain dataclass with `.system` / `.user` attrs. The
  system block is intentionally *not* templated so it stays eligible for
  Anthropic prompt caching (same bytes across all requests).
- `PROMPT_NAME` + `PROMPT_VERSION` constants are stamped into every PostHog
  event + Langfuse trace via `prompt_slug()`.
- The markdown source lives in `selector_chat_followup.md`; we load it once
  at import time so tests can assert content without repeatedly hitting disk.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

PROMPT_NAME = "selector_chat_followup"
PROMPT_VERSION = "v1"

# Placeholders the route MUST substitute before the prompt hits Haiku. CI
# linter (see `AI_PROMPTS.md §Prompt versioning`) will eventually fail builds
# when a placeholder is added to the markdown but not registered here.
REQUIRED_PLACEHOLDERS: tuple[str, ...] = (
    "{question_th}",
    "{classifier_category}",
    "{selector_context}",
    "{cached_result}",
)

# Mirrors the "Banned phrases" block at the bottom of the markdown. The route
# output linter scans `answer_th` + `answer_en` against this list and retries
# once on hit; persistent hit → static fallback.
BANNED_PHRASES: tuple[str, ...] = (
    "revolutionary",
    "cutting-edge",
    "synergy",
    "game-changer",
    "ปฏิวัติวงการ",
    "ล้ำสมัย",
    "เปลี่ยนโลก",
)

# Classifier hint values the route may pass in. Values outside this set → the
# route coerces to "other" before the prompt is built. Kept here so the route
# and prompt agree on the enum.
CLASSIFIER_CATEGORIES: tuple[str, ...] = ("explain", "what-if", "other")

# Per `mvp/AI_PROMPTS.md §Prompt 5 — Latency + cost budget` — enforced
# pre-flight by the route (PR-9).
MAX_QUESTION_CHARS: int = 500
COST_CAP_THB: float = 0.10
HAIKU_TIMEOUT_S: float = 5.0
P95_BUDGET_S: float = 3.0
RATE_LIMIT_PER_SESSION: int = 10

_PROMPT_MD_PATH = Path(__file__).parent / "selector_chat_followup.md"


@dataclass(frozen=True)
class ChatFollowupPrompt:
    """Loaded prompt artifact. Route treats this as opaque + read-only."""

    text: str
    version: str
    banned_phrases: tuple[str, ...]
    placeholders: tuple[str, ...]


@lru_cache(maxsize=1)
def load() -> ChatFollowupPrompt:
    """Read the markdown source and return a cached `ChatFollowupPrompt`.

    Importers should call `load()` at request time; the `lru_cache` makes it
    O(1) after first call. Tests that want a fresh read (e.g., after editing
    the md fixture) can `load.cache_clear()`.
    """
    text = _PROMPT_MD_PATH.read_text(encoding="utf-8")
    if not text.strip():
        raise RuntimeError(f"selector_chat_followup.md is empty at {_PROMPT_MD_PATH}")
    return ChatFollowupPrompt(
        text=text,
        version=PROMPT_VERSION,
        banned_phrases=BANNED_PHRASES,
        placeholders=REQUIRED_PLACEHOLDERS,
    )


def prompt_slug() -> str:
    """PostHog/Langfuse identifier: `selector_chat_followup@v1`."""
    return f"{PROMPT_NAME}@{PROMPT_VERSION}"


# ---------------------------------------------------------------------------
# Rendering — splits the markdown source into `system` / `user` halves and
# substitutes `{placeholder}` tokens in the user half.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RenderedPrompt:
    """A rendered prompt ready to hand to the Anthropic SDK.

    `system` is the cached context block (identical across all requests → the
    provider can attach `cache_control: ephemeral` to it). `user` is the
    per-request content with all template variables substituted in.
    """

    system: str
    user: str


# Section headers used to split the markdown source. The `## User template`
# header marks the boundary between the static system block and the
# per-request user block. `## Output JSON schema` marks the end of the user
# block (everything after is few-shot examples + versioning metadata that
# belong in the system-side cache, not the per-request user turn).
_SYSTEM_HEADER = "## System"
_USER_HEADER = "## User template"
_USER_END_HEADER = "## Output JSON schema"

# Matches `{identifier}` but not `{{escaped}}` or JSON-style `{ ` with spaces.
# Identifiers are the Python-ish `[A-Za-z_][A-Za-z0-9_]*` shape — matches the
# placeholders documented in REQUIRED_PLACEHOLDERS.
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _extract_section(text: str, start_header: str, end_header: str | None) -> str:
    """Return the substring between `start_header` and `end_header` (exclusive).

    The md file uses triple-backtick fences inside each section; we strip
    those so the returned text is directly usable as a prompt half.
    """
    start = text.find(start_header)
    if start == -1:
        raise RuntimeError(
            f"selector_chat_followup.md missing expected header {start_header!r}"
        )
    # Skip past the header line itself.
    after_header = text.find("\n", start)
    if after_header == -1:
        raise RuntimeError(f"malformed markdown: header {start_header!r} has no body")
    if end_header is None:
        body = text[after_header + 1 :]
    else:
        end = text.find(end_header, after_header)
        if end == -1:
            raise RuntimeError(
                f"selector_chat_followup.md missing expected header {end_header!r}"
            )
        body = text[after_header + 1 : end]
    # Strip surrounding triple-backtick fences if present. The md wraps each
    # section body in a ``` fence; for the prompt we want the raw contents.
    body = body.strip()
    if body.startswith("```"):
        # Drop the opening fence line (may include a language hint).
        first_newline = body.find("\n")
        if first_newline != -1:
            body = body[first_newline + 1 :]
    if body.endswith("```"):
        body = body[: body.rfind("```")]
    return body.strip()


def render(variables: Mapping[str, object]) -> RenderedPrompt:
    """Render the prompt into a `RenderedPrompt` with `system` + `user` fields.

    Loads the cached `ChatFollowupPrompt` via `load()`, splits the markdown on
    the `## System` / `## User template` / `## Output JSON schema` headers,
    and regex-substitutes `variables` into the user half. The system half is
    returned verbatim — it contains no templates and must stay byte-identical
    across requests so Anthropic prompt caching hits.

    Raises `ValueError` if any placeholder listed in `REQUIRED_PLACEHOLDERS`
    is missing from `variables`, or if the rendered output still contains an
    un-substituted `{placeholder}` token.
    """
    prompt = load()
    text = prompt.text

    system_block = _extract_section(text, _SYSTEM_HEADER, _USER_HEADER)
    user_template = _extract_section(text, _USER_HEADER, _USER_END_HEADER)

    # Required-placeholder preflight: every name in REQUIRED_PLACEHOLDERS must
    # be supplied. `{locale}` is documented in the md but not required — the
    # route always passes it.
    required_names = {p.strip("{}") for p in REQUIRED_PLACEHOLDERS}
    missing = required_names - set(variables.keys())
    if missing:
        raise ValueError(
            f"selector_chat_followup.render: missing required placeholders: "
            f"{sorted(missing)}"
        )

    # Substitute. Use a dict with str-coerced values so callers can pass ints
    # or other stringifiable objects without surprising the regex sub.
    str_vars: dict[str, str] = {k: str(v) for k, v in variables.items()}

    # Manual regex sub so we can collect any unfilled names into a single
    # descriptive error (str.format_map would raise KeyError on the first).
    unfilled: list[str] = []

    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in str_vars:
            return str_vars[name]
        unfilled.append(name)
        return match.group(0)  # leave untouched so the error message is clear

    rendered_user = _PLACEHOLDER_RE.sub(_sub, user_template)
    if unfilled:
        # De-dup while preserving order for a stable error message.
        seen: set[str] = set()
        ordered: list[str] = []
        for name in unfilled:
            if name not in seen:
                seen.add(name)
                ordered.append(name)
        raise ValueError(
            f"selector_chat_followup.render: unfilled placeholders in user "
            f"template: {ordered}"
        )

    return RenderedPrompt(system=system_block, user=rendered_user)


__all__ = [
    "BANNED_PHRASES",
    "CLASSIFIER_CATEGORIES",
    "COST_CAP_THB",
    "HAIKU_TIMEOUT_S",
    "MAX_QUESTION_CHARS",
    "P95_BUDGET_S",
    "PROMPT_NAME",
    "PROMPT_VERSION",
    "RATE_LIMIT_PER_SESSION",
    "REQUIRED_PLACEHOLDERS",
    "ChatFollowupPrompt",
    "RenderedPrompt",
    "load",
    "prompt_slug",
    "render",
]
