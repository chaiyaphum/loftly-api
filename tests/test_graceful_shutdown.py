"""Graceful shutdown — DRILL-003.

Covers the lifespan teardown wiring added for DO App Platform rolling deploys:

- Teardown runs cleanly (no unhandled exceptions, no warnings)
- `_shutting_down` flips True during teardown, resets False after
- `/readyz` returns 503 `{"database":"shutting_down"}` while the flag is set
- The `db_pool_snapshot_loop` background task is cancelled and awaited
- Structured `shutdown_complete` log event is emitted with `duration_ms`
"""

from __future__ import annotations

import asyncio
import warnings
from contextlib import suppress

import pytest
from httpx import ASGITransport, AsyncClient

from loftly.api.app import _db_pool_snapshot_loop, create_app, lifespan
from loftly.api.routes.health import is_shutting_down, set_shutting_down


@pytest.fixture(autouse=True)
def _reset_shutdown_flag() -> None:
    """Every test starts with the flag cleared."""
    set_shutting_down(False)
    yield
    set_shutting_down(False)


async def test_readyz_returns_503_when_shutting_down() -> None:
    """Flag flipped → /readyz immediately reports shutting_down."""
    app = create_app()
    set_shutting_down(True)
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["checks"]["database"] == "shutting_down"


async def test_readyz_ok_when_not_shutting_down() -> None:
    """Sanity check — flag off, the normal DB probe still runs."""
    app = create_app()
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    # Bring up schema so the DB probe succeeds.
    from loftly.db.engine import get_engine
    from loftly.db.models import Base

    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/readyz")
        assert resp.status_code == 200
        assert resp.json()["checks"]["database"] == "ok"
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()


async def test_lifespan_teardown_runs_cleanly() -> None:
    """Enter + exit the real lifespan — no exceptions, flag flips both ways."""
    app = create_app()
    async with lifespan(app):
        # Startup done; flag should still be False.
        assert is_shutting_down() is False
    # Teardown done; flag should have been reset to False after
    # `shutdown_complete` was logged.
    assert is_shutting_down() is False


async def test_lifespan_flips_flag_during_teardown(monkeypatch: pytest.MonkeyPatch) -> None:
    """While teardown is running, `/readyz` must see the flag as True.

    We inject a spy into `set_shutting_down` so we can assert the True→False
    sequence observed by the health module.
    """
    from loftly.api import app as app_module

    seen: list[bool] = []

    real_setter = app_module.set_shutting_down

    def _spy(value: bool) -> None:
        seen.append(value)
        real_setter(value)

    monkeypatch.setattr(app_module, "set_shutting_down", _spy)

    app = create_app()
    async with lifespan(app):
        pass

    # Teardown should set True first, then back to False on clean exit.
    assert seen[0] is True, f"expected first set to be True, got {seen!r}"
    assert seen[-1] is False, f"expected final set to be False, got {seen!r}"


async def test_background_task_cancelled_without_warnings(
    recwarn: pytest.WarningsRecorder,
) -> None:
    """Cancelling + awaiting the pool-snapshot loop must not emit warnings.

    Python emits a ResourceWarning / RuntimeWarning if a coroutine is cancelled
    but never awaited. We do both; recwarn verifies nothing leaks.
    """
    from loftly.core.logging import get_logger

    log = get_logger(__name__)

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # turn any warning into a test failure
        task = asyncio.create_task(_db_pool_snapshot_loop(log), name="test_pool_loop")
        # Let the loop enter its first sleep.
        await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    # No warnings captured.
    assert [w for w in recwarn.list if w.category is RuntimeWarning] == []


async def test_lifespan_emits_shutdown_complete_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`shutdown_complete` + `duration_ms` are logged at teardown.

    `configure_logging` uses `cache_logger_on_first_use=True` + a
    `PrintLoggerFactory(file=sys.stdout)` which makes it awkward to capture
    structlog events from test harness. We patch `configure_logging` inside
    the lifespan to a no-op, install a capture processor, and assert on the
    event dicts directly.
    """
    import logging as _logging

    import structlog

    from loftly.api import app as app_module

    captured: list[dict[str, object]] = []

    def _capture(_logger: object, _method: str, event_dict: dict[str, object]) -> dict[str, object]:
        captured.append(dict(event_dict))
        return event_dict

    monkeypatch.setattr(app_module, "configure_logging", lambda _settings: None)

    old_config = structlog.get_config()
    structlog.reset_defaults()
    structlog.configure(
        processors=[_capture],
        wrapper_class=structlog.make_filtering_bound_logger(_logging.DEBUG),
        context_class=dict,
        logger_factory=structlog.ReturnLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    try:
        app = create_app()
        async with lifespan(app):
            pass
    finally:
        structlog.reset_defaults()
        structlog.configure(**old_config)

    names = [e.get("event") for e in captured]
    assert "shutdown_started" in names, f"expected shutdown_started in {names!r}"
    assert "shutdown_complete" in names, f"expected shutdown_complete in {names!r}"
    row = next(e for e in captured if e.get("event") == "shutdown_complete")
    assert isinstance(row.get("duration_ms"), (int, float))
    assert row["duration_ms"] >= 0.0
