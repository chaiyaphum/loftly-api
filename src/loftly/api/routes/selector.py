"""Card Selector — `POST /v1/selector`, `GET /v1/selector/{session_id}`.

Phase 1 scaffold: returns a stubbed `SelectorResult` (no LLM call). Flagged
with `fallback=true` so the client treats it as the rule-based path. Real
LLM orchestration lands Week 4 per DEV_PLAN.md and AI_PROMPTS.md.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status

from loftly.schemas.selector import SelectorInput, SelectorResult, SelectorStackItem

router = APIRouter(prefix="/v1/selector", tags=["selector"])


def _stub_result(session_id: str) -> SelectorResult:
    return SelectorResult(
        session_id=session_id,
        stack=[
            SelectorStackItem(
                card_id="11111111-1111-4111-8111-111111111111",
                slug="kbank-wisdom",
                role="primary",
                monthly_earning_points=5400,
                monthly_earning_thb_equivalent=8200,
                annual_fee_thb=5000.00,
                reason_th="ตัวเลือกเริ่มต้น (stub). เปลี่ยนเป็นผลจริงเมื่อ Selector พร้อม.",
                reason_en="Scaffold stub. Replaced when the Selector LLM path lands in Week 4.",
            )
        ],
        total_monthly_earning_points=5400,
        total_monthly_earning_thb_equivalent=8200,
        months_to_goal=None,
        with_signup_bonus_months=None,
        valuation_confidence=0.0,
        rationale_th="ผลนี้เป็นข้อมูลจำลองสำหรับการทดสอบ scaffold เท่านั้น",
        rationale_en="Stub selector response. Not a real recommendation.",
        warnings=["selector_stub"],
        llm_model="stub",
        fallback=True,
        partial_unlock=False,
    )


@router.post(
    "",
    response_model=SelectorResult,
    summary="Submit spend profile; receive ranked card stack (Phase 1 stub)",
)
async def submit(_payload: SelectorInput) -> SelectorResult:
    """Return a deterministic stub envelope.

    The real path streams SSE rationale chunks after the envelope; the stub
    returns only the JSON envelope (acceptable per openapi.yaml — SSE is an
    additional content-type on the same status).
    """
    session_id = str(uuid.uuid4())
    return _stub_result(session_id)


@router.get(
    "/{session_id}",
    response_model=SelectorResult,
    summary="Retrieve previously computed result",
)
async def get_session(session_id: str, token: str | None = None) -> SelectorResult:
    """Phase 1: no persistence yet. Raise 501 until session storage is wired."""
    _ = (session_id, token)
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Selector session retrieval not yet implemented",
    )
