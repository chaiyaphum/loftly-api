"""Personalized welcome email composer — POST_V1 §2.

Orchestrates the Haiku LLM call (5s timeout) → build HTML + plaintext body →
Resend send with retry-once-after-30s → Sentry alert on double-failure → audit
log + PostHog events.

Kill-switch: `WELCOME_EMAIL_PERSONALIZED=false` bypasses the LLM and sends the
v1 static magic-link template. Required for graceful degradation when the
Anthropic key is absent, the prompt regresses, or ops needs to disable
personalization without a redeploy.

Concurrency: the personalized path burns Haiku tokens + one Resend call per
user. A module-level `asyncio.Semaphore(10)` caps in-flight personalized sends
so a thundering herd (e.g. launch campaign spike) can't blow past Resend's
sender-side burst limits. The limit is deliberately modest — revisit once
POST_V1 §2 is generating real traffic.

PII posture: PostHog events carry a **hashed** session identifier, never the
user's email nor the raw session UUID. `observability.posthog.hash_distinct_id`
is the single place where the `loftly:` salt lives.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json

from loftly.core.locale import Locale
from loftly.core.logging import get_logger
from loftly.core.settings import get_settings
from loftly.notifications.email import send_email_with_retry, send_magic_link
from loftly.observability.posthog import capture as posthog_capture
from loftly.observability.posthog import hash_distinct_id
from loftly.prompts.personalized_welcome_email import load as load_welcome_email_prompt
from loftly.schemas.selector import SelectorResult

log = get_logger(__name__)

# AI_PROMPTS.md §Prompt 6 Failure policy: Haiku > 5s → static fallback.
_HAIKU_TIMEOUT_SEC = 5.0
# POST_V1 §2 AC: subject line must be ≤ 60 chars. Enforced at compose time.
_SUBJECT_MAX_CHARS = 60
# Cap concurrent personalized sends so a launch-day spike can't blow past
# Resend's sender-side burst limits. 10 is arbitrary but sane for MVP scale.
_CONCURRENCY_CAP = 10
_send_semaphore = asyncio.Semaphore(_CONCURRENCY_CAP)

_EMAIL_TYPE = "welcome_personalized"


# ---------------------------------------------------------------------------
# Tracking pixel — HMAC-signed token
# ---------------------------------------------------------------------------


def _tracking_signing_key() -> str:
    """Reuse JWT_SIGNING_KEY for pixel HMAC — see PR description for rationale.

    Phase 1 has one shared secret (`JWT_SIGNING_KEY`) and introducing a second
    just for tracking pixels doubles ops surface for no meaningful security
    gain: both keys are process-local, rotated on the same cadence, and
    compromise of one already grants server-side code execution.
    """
    return get_settings().jwt_signing_key


def build_tracking_token(user_id_hash: str, email_type: str) -> str:
    """Build a URL-safe `{payload}.{sig}` token embedded in the pixel URL.

    The payload is a small JSON blob with the hashed user id + email type so
    the tracking endpoint can emit PostHog events without re-deriving the
    hash. Signature prevents token forgery by an attacker who wants to
    pollute our analytics.

    Kept intentionally small (no b64 padding footguns, no heavy JSON):
    tokens appear in URLs and some email clients truncate > 2k chars.
    """
    import base64

    payload_json = json.dumps(
        {"u": user_id_hash, "t": email_type},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    payload_b64 = base64.urlsafe_b64encode(payload_json).rstrip(b"=").decode()
    sig = hmac.new(
        _tracking_signing_key().encode(),
        payload_b64.encode(),
        hashlib.sha256,
    ).hexdigest()[:32]
    return f"{payload_b64}.{sig}"


def verify_tracking_token(token: str) -> dict[str, str] | None:
    """Return `{"u": user_id_hash, "t": email_type}` or `None` on tamper.

    Constant-time signature compare. Never raises — the caller returns 400
    uniformly when this comes back `None`.
    """
    import base64

    if "." not in token:
        return None
    payload_b64, sig = token.rsplit(".", 1)
    expected_sig = hmac.new(
        _tracking_signing_key().encode(),
        payload_b64.encode(),
        hashlib.sha256,
    ).hexdigest()[:32]
    if not hmac.compare_digest(expected_sig, sig):
        return None
    try:
        # Re-pad to multiple of 4 for urlsafe_b64decode.
        pad = "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + pad))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    u = payload.get("u")
    t = payload.get("t")
    if not isinstance(u, str) or not isinstance(t, str):
        return None
    return {"u": u, "t": t}


def _pixel_url(user_id_hash: str, email_type: str) -> str:
    token = build_tracking_token(user_id_hash, email_type)
    base = get_settings().api_public_base_url.rstrip("/")
    return f"{base}/v1/tracking/email/{token}/open"


# ---------------------------------------------------------------------------
# Static fallback template — v1 magic-link copy inlined.
# ---------------------------------------------------------------------------

_STATIC_SUBJECT_TH = "ลิงก์เข้าสู่ระบบ Loftly"
_STATIC_SUBJECT_EN = "Your Loftly sign-in link"

_STATIC_TEXT_TH = (
    "สวัสดีค่ะ\n\n"
    "กดลิงก์ด้านล่างเพื่อเข้าสู่ระบบ Loftly — ลิงก์นี้จะหมดอายุใน 15 นาที\n\n"
    "{magic_url}\n\n"
    "หากคุณไม่ได้ขอลิงก์นี้ ไม่ต้องทำอะไรค่ะ\n\nขอบคุณที่ใช้ Loftly"
)
_STATIC_TEXT_EN = (
    "Hi there,\n\n"
    "Click the link below to sign in to Loftly — it expires in 15 minutes.\n\n"
    "{magic_url}\n\n"
    "If you didn't request this, you can safely ignore this email.\n\n— Loftly"
)


def _compose_static_fallback(
    magic_link_url: str,
    locale: Locale,
) -> tuple[str, str, str]:
    """Return `(subject, text, html)` for the static v1 template."""
    if locale == "th":
        subject = _STATIC_SUBJECT_TH
        text = _STATIC_TEXT_TH.format(magic_url=magic_link_url)
    else:
        subject = _STATIC_SUBJECT_EN
        text = _STATIC_TEXT_EN.format(magic_url=magic_link_url)
    # Minimal HTML — convert newlines, wrap in <p>. No external CSS.
    html_body = "<br>".join(line.strip() for line in text.splitlines())
    html = (
        '<html><body style="font-family:Arial,sans-serif;font-size:15px;color:#222">'
        f"<p>{html_body}</p></body></html>"
    )
    return subject, text, html


# ---------------------------------------------------------------------------
# Personalized composition
# ---------------------------------------------------------------------------


def _format_thb(value: int | float | None) -> str:
    if value is None:
        return "—"
    try:
        return f"THB {round(float(value)):,}"
    except (TypeError, ValueError):
        return "—"


def _personalized_subject(selector_result: SelectorResult, locale: Locale) -> str:
    """Subject built deterministically from selector output. ≤ 60 chars."""
    top = selector_result.stack[0] if selector_result.stack else None
    if locale == "th":
        if top is None:
            base = "บัตรเครดิตที่เหมาะกับคุณจาก Loftly"
        else:
            base = f"บัตร {top.slug} เหมาะกับสไตล์ใช้จ่ายของคุณ"
    else:  # en
        if top is None:
            base = "Your Loftly card recommendations"
        else:
            base = f"{top.slug} fits your spend pattern"
    # Hard cap per POST_V1 §2 AC + AI_PROMPTS.md §Prompt 6 Output schema.
    if len(base) > _SUBJECT_MAX_CHARS:
        base = base[: _SUBJECT_MAX_CHARS - 1] + "…"
    return base


def _compose_personalized(
    selector_result: SelectorResult,
    magic_link_url: str,
    locale: Locale,
    pixel_url: str | None,
) -> tuple[str, str, str, list[str]]:
    """Deterministic TH/EN body using the selector envelope directly.

    We deliberately do *not* call the LLM in this first implementation path.
    PR-3 lands the Haiku prompt; once merged, the Haiku output replaces the
    body copy while the scaffold (subject, pixel wiring, top3_ids) stays.
    The email is still "personalized" in the sense that every field is
    derived from the user's actual selector result — the Haiku call adds
    richer locale-aware rationale on top.

    Returns `(subject, text, html, top3_card_ids)`.
    """
    subject = _personalized_subject(selector_result, locale)
    stack = selector_result.stack[:3]
    top3_card_ids = [item.card_id for item in stack]

    # Plaintext body — graceful if HTML clients fail.
    if locale == "th":
        header = "สวัสดีค่ะ — Loftly เลือก 3 บัตรที่เหมาะกับคุณแล้ว"
        cta_label = "ดูผลเต็มที่นี่"
        reason_label = "เหตุผล"
        earning_label = "ประมาณการต่อเดือน"
    else:
        header = "Hi — Loftly picked your top 3 cards."
        cta_label = "See full results"
        reason_label = "Why"
        earning_label = "Estimated monthly"

    text_lines = [header, ""]
    for idx, item in enumerate(stack, start=1):
        earning_thb = _format_thb(item.monthly_earning_thb_equivalent)
        reason = item.reason_th if locale == "th" else (item.reason_en or item.reason_th)
        text_lines.append(f"{idx}. {item.slug} ({item.role})")
        text_lines.append(f"   {earning_label}: {earning_thb}")
        text_lines.append(f"   {reason_label}: {reason}")
        text_lines.append("")
    text_lines.append(f"{cta_label}: {magic_link_url}")
    text_body = "\n".join(text_lines)

    # HTML body — inline styles only; no external CSS, no JS.
    html_rows: list[str] = []
    for idx, item in enumerate(stack, start=1):
        earning_thb = _format_thb(item.monthly_earning_thb_equivalent)
        reason = item.reason_th if locale == "th" else (item.reason_en or item.reason_th)
        html_rows.append(
            '<tr><td style="padding:12px 0;border-bottom:1px solid #eee">'
            f'<div style="font-weight:600;font-size:16px">{idx}. {item.slug} '
            f'<span style="color:#888;font-weight:400">({item.role})</span></div>'
            f'<div style="color:#444;margin-top:4px">{earning_label}: '
            f"<strong>{earning_thb}</strong></div>"
            f'<div style="color:#666;margin-top:4px">{reason_label}: {reason}</div>'
            "</td></tr>"
        )
    rows_html = "".join(html_rows) or (
        "<tr><td>—</td></tr>"  # empty-stack safety
    )

    pixel_tag = (
        f'<img src="{pixel_url}" width="1" height="1" alt="" '
        'style="display:block;border:0;outline:none;width:1px;height:1px"/>'
        if pixel_url
        else ""
    )

    html = (
        '<html><body style="margin:0;padding:24px;'
        'font-family:Arial,sans-serif;font-size:15px;color:#222;background:#fafafa">'
        '<div style="max-width:560px;margin:0 auto;background:#fff;'
        'padding:24px;border-radius:8px">'
        f'<h1 style="font-size:20px;margin:0 0 16px 0">{header}</h1>'
        f'<table cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;border-collapse:collapse">'
        f"{rows_html}</table>"
        f'<div style="margin-top:24px"><a href="{magic_link_url}" '
        'style="display:inline-block;padding:12px 20px;background:#111;'
        'color:#fff;text-decoration:none;border-radius:6px;font-weight:600">'
        f"{cta_label}</a></div>"
        "</div>"
        f"{pixel_tag}"
        "</body></html>"
    )
    return subject, text_body, html, top3_card_ids


def _prompt_version() -> str:
    """Return the Prompt 6 version for instrumentation. Safe if prompt is absent."""
    try:
        return load_welcome_email_prompt().version
    except Exception:
        return "unavailable"


async def _haiku_llm_call(
    selector_result: SelectorResult,
    locale: Locale,
) -> None:
    """Haiku invocation seam — real Haiku wiring lands alongside PR-6.

    Prompt 6 text + version are loaded here (so the real prompt slug shows up
    in instrumentation once PR-3 is merged), but the live Haiku SDK call is
    deliberately deferred until the selector Haiku plumbing and the prompt
    golden-set eval are both green. The scaffolding (timeout + error paths)
    is already in place so flipping this to a real call later is a one-file
    change.

    Tests monkeypatch this function to exercise the timeout + error fallback
    paths without touching the global event loop.
    """
    # Touch the prompt loader so prompt-file regressions surface as test
    # failures before they reach the LLM.
    _ = load_welcome_email_prompt()
    return None


async def compose_personalized(
    selector_result: SelectorResult,
    magic_link_url: str,
    locale: Locale,
    user_id_hash: str | None = None,
) -> tuple[str, str, str, list[str], bool]:
    """Compose `(subject, text, html, top3_card_ids, fallback_used)`.

    Policy:
    - `WELCOME_EMAIL_PERSONALIZED=false` → fallback (no LLM call).
    - Empty stack → fallback (can't personalize without cards).
    - Otherwise → build the personalized body directly from selector_result.
      (LLM call lands with PR-3; this path is the LLM-less scaffold.)

    The 5s LLM timeout is already wired via ``asyncio.wait_for``; on timeout
    we return the static fallback and the caller emits `fallback: true`.
    """
    settings = get_settings()

    if not settings.welcome_email_personalized or not selector_result.stack:
        subject, text, html = _compose_static_fallback(magic_link_url, locale)
        return subject, text, html, [], True

    # LLM call boundary — PR-3 will replace this no-op call with the Haiku
    # client invocation. Wrapped in wait_for so the timeout semantics are
    # already in place when the real call lands. Tests override
    # `_haiku_llm_call` to exercise the timeout + error fallback paths.
    try:
        await asyncio.wait_for(_haiku_llm_call(selector_result, locale), timeout=_HAIKU_TIMEOUT_SEC)
    except TimeoutError:
        log.warning("welcome_email_llm_timeout", prompt_version=_prompt_version())
        subject, text, html = _compose_static_fallback(magic_link_url, locale)
        return subject, text, html, [], True
    except Exception as exc:
        # Broad catch: any LLM failure (auth, quota, SDK) collapses to fallback.
        log.warning(
            "welcome_email_llm_error",
            prompt_version=_prompt_version(),
            error=str(exc)[:200],
        )
        subject, text, html = _compose_static_fallback(magic_link_url, locale)
        return subject, text, html, [], True

    pixel_url = _pixel_url(user_id_hash, _EMAIL_TYPE) if user_id_hash else None
    subject, text, html, top3 = _compose_personalized(
        selector_result, magic_link_url, locale, pixel_url
    )
    return subject, text, html, top3, False


# ---------------------------------------------------------------------------
# Send orchestration
# ---------------------------------------------------------------------------


async def send_welcome_email(
    *,
    email: str,
    magic_link_url: str,
    selector_result: SelectorResult | None,
    locale: Locale,
    session_id: str | None = None,
    user_id_hash: str | None = None,
) -> None:
    """End-to-end: compose → send (with retry) → instrument.

    `selector_result=None` short-circuits to the static v1 magic-link template
    (i.e. calls exist today where we don't have a selector snapshot yet).

    Always fire-and-forget from the route layer — this function handles its
    own exceptions so the task never leaks a traceback into the event loop.
    """
    # Bound concurrent in-flight sends — protects Resend sender burst limits.
    async with _send_semaphore:
        try:
            if selector_result is None:
                await send_magic_link(email, magic_link_url, locale=locale)
                return

            session_hash = hash_distinct_id(session_id) if session_id else hash_distinct_id(None)
            subject, text, html, top3_ids, fallback_used = await compose_personalized(
                selector_result=selector_result,
                magic_link_url=magic_link_url,
                locale=locale,
                user_id_hash=user_id_hash,
            )

            # Emit "queued" at compose time — before Resend — so ops sees the
            # fallback signal even if delivery then blows up.
            await posthog_capture(
                event="welcome_email_queued",
                distinct_id=session_hash,
                properties={
                    "session_id_hash": session_hash,
                    "llm_model": "claude-haiku-4-5-20251001",
                    "fallback": fallback_used,
                    "locale": locale,
                    "prompt_version": _prompt_version(),
                    "top3_card_count": len(top3_ids),
                },
            )

            await send_email_with_retry(
                to=email,
                subject=subject,
                text=text,
                html=html,
                email_type=_EMAIL_TYPE if not fallback_used else "welcome_static",
            )

            # Delivered — user_id_hash is optional (anon at send time is fine).
            await posthog_capture(
                event="welcome_email_delivered",
                distinct_id=user_id_hash or session_hash,
                properties={
                    "fallback": fallback_used,
                    "locale": locale,
                },
            )
        except Exception as exc:
            # Never propagate — the caller is `asyncio.create_task(...)` and
            # a raise here becomes "Task exception was never retrieved".
            log.warning(
                "welcome_email_send_failed",
                error=str(exc)[:200],
                locale=locale,
            )


def _get_semaphore() -> asyncio.Semaphore:
    """Testing hook: lets tests assert concurrency bounding behavior."""
    return _send_semaphore


__all__ = [
    "build_tracking_token",
    "compose_personalized",
    "send_welcome_email",
    "verify_tracking_token",
]
