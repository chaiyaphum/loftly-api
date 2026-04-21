"""Exception handlers that produce `openapi.yaml#Error`-shaped bodies."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from loftly.core.logging import get_logger
from loftly.schemas.errors import Error, ErrorBody

log = get_logger(__name__)


class LoftlyError(Exception):
    """Domain-level error — renders as `openapi.yaml#Error`."""

    def __init__(
        self,
        *,
        code: str,
        message_en: str,
        message_th: str | None = None,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message_en)
        self.code = code
        self.message_en = message_en
        self.message_th = message_th
        self.status_code = status_code
        self.details = details


def _as_error_response(
    *,
    status_code: int,
    code: str,
    message_en: str,
    message_th: str | None = None,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    payload = Error(
        error=ErrorBody(
            code=code,
            message_en=message_en,
            message_th=message_th,
            details=details,
        )
    )
    return JSONResponse(status_code=status_code, content=jsonable_encoder(payload))


async def _loftly_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, LoftlyError)
    log.info(
        "loftly_error",
        code=exc.code,
        status=exc.status_code,
        details=exc.details,
    )
    return _as_error_response(
        status_code=exc.status_code,
        code=exc.code,
        message_en=exc.message_en,
        message_th=exc.message_th,
        details=exc.details,
    )


async def _http_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, HTTPException)
    code = _status_code_to_error_code(exc.status_code)
    message_en = str(exc.detail) if exc.detail else "HTTP error"
    return _as_error_response(
        status_code=exc.status_code,
        code=code,
        message_en=message_en,
    )


async def _validation_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    return _as_error_response(
        status_code=422,
        code="validation_error",
        message_en="Request body failed validation.",
        message_th="ข้อมูลไม่ถูกต้อง",
        details={"errors": exc.errors()},
    )


async def _unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    log.exception("unhandled_exception", exc_type=type(exc).__name__)
    return _as_error_response(
        status_code=500,
        code="internal_error",
        message_en="An internal error occurred.",
        message_th="เกิดข้อผิดพลาดภายในระบบ",
    )


def _status_code_to_error_code(status_code: int) -> str:
    mapping = {
        400: "bad_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        422: "unprocessable_entity",
        429: "rate_limited",
        500: "internal_error",
        501: "not_implemented",
        503: "service_unavailable",
    }
    return mapping.get(status_code, "error")


def register_exception_handlers(app: FastAPI) -> None:
    """Wire the handlers above onto a FastAPI app."""
    app.add_exception_handler(LoftlyError, _loftly_error_handler)
    app.add_exception_handler(HTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    app.add_exception_handler(Exception, _unhandled_exception_handler)
