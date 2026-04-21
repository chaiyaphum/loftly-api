"""Auth & session endpoints.

Phase 1 (Week 5-6) implements:
- Magic-link request (POST /v1/auth/magic-link/request)
- Magic-link consume (POST /v1/auth/magic-link/consume)
- `_test/issue` helper (test envs only)

Magic-link emails are **not actually sent** yet; delivery via Resend lands
with the email-template work (Week 7+). We `structlog.info("magic_link_issued", ...)`
with the full URL so dev + ops can copy-paste it into a browser.

OAuth + refresh + logout remain 501 stubs pending provider app credentials
(see `docs/MANUAL_ACTIONS.md`).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

from fastapi import APIRouter, Depends, Request, status
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loftly.api.errors import LoftlyError
from loftly.api.jwt_util import Locale, Role, TokenPair, issue_token_pair
from loftly.api.rate_limit import FixedWindowLimiter
from loftly.auth.oauth import OAuthExchangeFailed, OAuthNotConfigured, Provider
from loftly.core.logging import get_logger
from loftly.core.settings import Settings, get_settings
from loftly.db.engine import get_session
from loftly.db.models.selector_session import SelectorSession
from loftly.db.models.user import User
from loftly.notifications.email import send_magic_link

router = APIRouter(prefix="/v1/auth", tags=["auth"])
log = get_logger(__name__)

# 15-minute magic-link window per SPEC.md §1 + §2 email-gate story.
_MAGIC_LINK_TTL_SECONDS = 900
_MAGIC_LINK_PURPOSE = "magic_link"
# 5/min per IP — tight because magic-link is a send-side mutation with email cost.
MAGIC_LINK_LIMITER = FixedWindowLimiter(max_calls=5, window_sec=60)
# Web-facing consume URL; the web app reads `?token=...` and POSTs to /consume.
_MAGIC_LINK_BASE_URL = "https://loftly.co.th/auth/magic-link/consume"


# ---------------------------------------------------------------------------
# Request/response schemas (openapi.yaml#MagicLinkRequest / #MagicLinkConsume)
# ---------------------------------------------------------------------------


class MagicLinkRequestBody(BaseModel):
    email: EmailStr
    session_id: uuid.UUID | None = Field(default=None)


class MagicLinkRequestResponse(BaseModel):
    message_th: str
    message_en: str


class MagicLinkConsumeBody(BaseModel):
    token: str


class TokenPairUser(BaseModel):
    id: uuid.UUID
    email: str
    locale: Locale
    role: Role


class TokenPairResponse(BaseModel):
    """openapi.yaml#TokenPair — access + refresh + optional user block."""

    access_token: str
    refresh_token: str
    expires_in: int
    user: TokenPairUser | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _not_implemented() -> LoftlyError:
    return LoftlyError(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        code="not_implemented",
        message_en="Endpoint not yet implemented.",
        message_th="ยังไม่เปิดใช้งานจุดให้บริการนี้",
    )


def _issue_magic_link_token(
    email: str,
    session_id: uuid.UUID | None,
    settings: Settings,
) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": email,
        "session_id": str(session_id) if session_id else None,
        "purpose": _MAGIC_LINK_PURPOSE,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=_MAGIC_LINK_TTL_SECONDS)).timestamp()),
    }
    return jwt.encode(
        payload,
        settings.jwt_signing_key,
        algorithm=settings.jwt_algorithm,
    )


def _verify_magic_link_token(token: str, settings: Settings) -> dict[str, object]:
    try:
        claims = jwt.decode(
            token,
            settings.jwt_signing_key,
            algorithms=[settings.jwt_algorithm],
        )
    except JWTError as exc:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_magic_link",
            message_en="Magic link is invalid or expired.",
            message_th="ลิงก์หมดอายุหรือไม่ถูกต้อง",
        ) from exc
    if claims.get("purpose") != _MAGIC_LINK_PURPOSE:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_magic_link",
            message_en="Token purpose mismatch.",
            message_th="โทเคนไม่ถูกต้อง",
        )
    return dict(claims)


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


# ---------------------------------------------------------------------------
# Routes — magic link
# ---------------------------------------------------------------------------


@router.post(
    "/magic-link/request",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=MagicLinkRequestResponse,
    summary="Send magic link for email-only signup",
)
async def magic_link_request(
    payload: MagicLinkRequestBody,
    request: Request,
    settings: Settings = Depends(get_settings),
) -> MagicLinkRequestResponse:
    """Issue a 15-min signed token, "send" via structlog (Resend wiring TBD).

    Rate-limited to 5/min per client IP. On limit, return 429 per
    API_CONTRACT.md §Rate limits.
    """
    ip = _client_ip(request)
    if not MAGIC_LINK_LIMITER.allow(ip):
        raise LoftlyError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="rate_limited",
            message_en="Too many magic-link requests — slow down.",
            message_th="ขอลิงก์ถี่เกินไป กรุณาลองใหม่ภายหลัง",
        )

    token = _issue_magic_link_token(payload.email, payload.session_id, settings)
    magic_link_url = f"{_MAGIC_LINK_BASE_URL}?token={token}"

    # Always log — Resend path just adds a real send on top.
    log.info(
        "magic_link_issued",
        email=payload.email,
        session_id=str(payload.session_id) if payload.session_id else None,
        magic_link=magic_link_url,
        ttl_sec=_MAGIC_LINK_TTL_SECONDS,
    )
    # Fire-and-forget email; failures are logged inside send_magic_link but
    # we don't surface them (202 ACCEPTED semantics).
    try:
        await send_magic_link(payload.email, magic_link_url, locale="th")
    except Exception as exc:
        log.warning("magic_link_email_failed", error=str(exc)[:200])
    return MagicLinkRequestResponse(
        message_th="ส่งลิงก์ไปที่อีเมลแล้ว",
        message_en="Magic link sent to your email.",
    )


@router.post(
    "/magic-link/consume",
    response_model=TokenPairResponse,
    summary="Redeem magic link token",
)
async def magic_link_consume(
    payload: MagicLinkConsumeBody,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> TokenPairResponse:
    """Verify magic link, upsert user, bind any session_id, issue access+refresh."""
    claims = _verify_magic_link_token(payload.token, settings)
    email = str(claims["sub"])

    # Upsert user on email. New users get oauth_provider="email_magic" per
    # DEV_PLAN W5-6 decision (we treat magic link as its own provider so the
    # users_oauth_unique constraint stays clean).
    existing = (
        (await session.execute(select(User).where(User.email == email))).scalars().one_or_none()
    )
    if existing is None:
        user = User(
            email=email,
            oauth_provider="email_magic",
            oauth_subject=email,
            preferred_locale="th",
        )
        session.add(user)
        await session.flush()
    else:
        user = existing

    # Stamp last_login_at — consumed in `GET /v1/me` for the settings page.
    user.last_login_at = datetime.now(UTC)

    # Bind selector_sessions row if the token carries a session_id.
    session_id_claim = claims.get("session_id")
    if session_id_claim:
        try:
            target = uuid.UUID(str(session_id_claim))
        except ValueError:
            target = None
        if target is not None:
            selector_row = (
                (await session.execute(select(SelectorSession).where(SelectorSession.id == target)))
                .scalars()
                .one_or_none()
            )
            if selector_row is not None and selector_row.user_id is None:
                selector_row.user_id = user.id
                selector_row.bound_at = datetime.now(UTC)

    await session.commit()

    pair: TokenPair = issue_token_pair(
        user_id=user.id,
        role="user",
        locale="th",
        settings=settings,
    )
    return TokenPairResponse(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.expires_in,
        user=TokenPairUser(
            id=user.id,
            email=user.email,
            locale="th",
            role="user",
        ),
    )


# ---------------------------------------------------------------------------
# Remaining stubs
# ---------------------------------------------------------------------------


class OAuthCallbackBody(BaseModel):
    """OAuth callback payload — see openapi.yaml#OAuthCallback."""

    provider: Provider
    code: str
    redirect_uri: str
    session_id: uuid.UUID | None = Field(default=None)


@router.post(
    "/oauth/callback",
    response_model=TokenPairResponse,
    summary="Complete OAuth and mint JWT pair",
)
async def oauth_callback(
    payload: OAuthCallbackBody,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> TokenPairResponse:
    """Exchange code via provider, upsert user, bind session, mint tokens."""
    # Dispatch lazily — keeps test paths from importing google-auth etc.
    if payload.provider == "google":
        from loftly.auth.oauth.google import exchange_code
    elif payload.provider == "apple":
        from loftly.auth.oauth.apple import exchange_code
    elif payload.provider == "line":
        from loftly.auth.oauth.line import exchange_code
    else:  # pragma: no cover — pydantic already narrowed the literal
        raise _not_implemented()

    try:
        info = await exchange_code(payload.code, payload.redirect_uri)
    except OAuthNotConfigured as exc:
        raise LoftlyError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="oauth_provider_unavailable",
            message_en=f"OAuth provider {exc.provider} not configured.",
            message_th="ผู้ให้บริการ OAuth ยังไม่เปิดใช้งาน",
        ) from exc
    except OAuthExchangeFailed as exc:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="oauth_exchange_failed",
            message_en=str(exc),
            message_th="ไม่สามารถยืนยันตัวตนกับผู้ให้บริการได้",
        ) from exc

    # Upsert on (oauth_provider, oauth_subject) — the unique constraint key.
    existing = (
        (
            await session.execute(
                select(User).where(
                    User.oauth_provider == info.provider,
                    User.oauth_subject == info.subject,
                )
            )
        )
        .scalars()
        .one_or_none()
    )
    if existing is None:
        user = User(
            email=info.email or f"{info.provider}-{info.subject}@loftly.local",
            oauth_provider=info.provider,
            oauth_subject=info.subject,
            preferred_locale="th",
        )
        session.add(user)
        await session.flush()
    else:
        user = existing

    # Stamp last_login_at — consumed in `GET /v1/me` for the settings page.
    user.last_login_at = datetime.now(UTC)

    # Bind any dangling selector_session.
    if payload.session_id is not None:
        selector_row = (
            (
                await session.execute(
                    select(SelectorSession).where(SelectorSession.id == payload.session_id)
                )
            )
            .scalars()
            .one_or_none()
        )
        if selector_row is not None and selector_row.user_id is None:
            selector_row.user_id = user.id
            selector_row.bound_at = datetime.now(UTC)

    await session.commit()

    pair = issue_token_pair(
        user_id=user.id,
        role="user",
        locale="th",
        settings=settings,
    )
    return TokenPairResponse(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.expires_in,
        user=TokenPairUser(
            id=user.id,
            email=user.email,
            locale="th",
            role="user",
        ),
    )


class _RefreshBody(BaseModel):
    refresh_token: str


@router.post("/refresh", response_model=TokenPairResponse, summary="Rotate access token")
async def refresh(
    payload: _RefreshBody,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> TokenPairResponse:
    """Verify refresh JWT, issue a fresh access+refresh pair.

    Phase 1 refresh tokens are stateless — we rely on expiry alone. Adding a
    revocation list is tracked as a MANUAL_ACTION.
    """
    try:
        claims = jwt.decode(
            payload.refresh_token,
            settings.jwt_signing_key,
            algorithms=[settings.jwt_algorithm],
        )
    except JWTError as exc:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_refresh",
            message_en="Refresh token is invalid or expired.",
            message_th="Refresh token ไม่ถูกต้องหรือหมดอายุ",
        ) from exc

    if claims.get("type") != "refresh":
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_refresh",
            message_en="Token is not a refresh token.",
            message_th="Token ไม่ถูกต้อง",
        )
    try:
        user_id = uuid.UUID(str(claims["sub"]))
    except (KeyError, ValueError) as exc:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_refresh",
            message_en="Token subject malformed.",
            message_th="Token ไม่ถูกต้อง",
        ) from exc

    user = (await session.execute(select(User).where(User.id == user_id))).scalars().one_or_none()
    if user is None or user.deleted_at is not None:
        raise LoftlyError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="invalid_refresh",
            message_en="User not found or deleted.",
            message_th="ไม่พบบัญชี",
        )

    # Refresh is a successful login surface too — keep last_login_at current
    # so long-lived sessions still show a plausible value in /v1/me.
    user.last_login_at = datetime.now(UTC)
    await session.commit()

    pair = issue_token_pair(
        user_id=user.id,
        role=cast(Role, user.role),
        locale=cast(Locale, user.preferred_locale),
        settings=settings,
    )
    return TokenPairResponse(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.expires_in,
        user=TokenPairUser(
            id=user.id,
            email=user.email,
            locale=cast(Locale, user.preferred_locale),
            role=cast(Role, user.role),
        ),
    )


class _LogoutResponse(BaseModel):
    ok: bool = True


@router.post("/logout", response_model=_LogoutResponse, summary="Invalidate refresh token")
async def logout() -> _LogoutResponse:
    """Stateless logout — client drops the tokens. Server-side revocation TBD."""
    log.info("logout")
    return _LogoutResponse()


# ---------------------------------------------------------------------------
# Test-only token issuer (unchanged from Week 3).
# ---------------------------------------------------------------------------


class _TestIssueRequest(BaseModel):
    user_id: uuid.UUID
    role: Role = "user"
    locale: Locale = "th"


class _TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int


@router.post(
    "/_test/issue",
    summary="(test-only) Mint a JWT pair for an arbitrary user_id + role",
    response_model=_TokenPairResponse,
)
async def _test_issue(
    payload: _TestIssueRequest,
    settings: Settings = Depends(get_settings),
) -> _TokenPairResponse:
    """Only enabled in `LOFTLY_ENV=test`. Lets integration tests mint real JWTs."""
    if not settings.is_test:
        raise LoftlyError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="not_found",
            message_en="Route not found.",
            message_th="ไม่พบเส้นทาง",
        )
    pair = issue_token_pair(
        user_id=payload.user_id,
        role=payload.role,
        locale=payload.locale,
        settings=settings,
    )
    return _TokenPairResponse(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.expires_in,
    )
