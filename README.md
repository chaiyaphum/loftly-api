# loftly-api

FastAPI backend for Loftly — the Thai AI-native credit-card rewards optimization platform.

This is the Phase 1 MVP backend. The full specification lives in the sibling
`loftly/` repo under `mvp/`:

- `../loftly/mvp/README.md` — index
- `../loftly/mvp/SPEC.md` — 8 MVP features
- `../loftly/mvp/API_CONTRACT.md` — endpoint guide
- `../loftly/mvp/SCHEMA.md` — 14 tables
- `../loftly/mvp/artifacts/openapi.yaml` — OpenAPI 3.1 contract (source of truth)
- `../loftly/mvp/artifacts/schema.sql` — executable DDL
- `../loftly/mvp/DEPLOYMENT.md` — Fly.io target + env vars

## Stack

- Python 3.12 (via `uv`)
- FastAPI 0.115+ async
- Pydantic v2 + pydantic-settings
- SQLAlchemy 2.0 async + asyncpg
- Alembic for migrations
- structlog for logging
- httpx for outbound
- pytest + pytest-asyncio (aiosqlite for tests Phase 1)
- ruff + mypy strict

## Quickstart

```bash
uv python install 3.12
uv sync
cp .env.example .env   # edit DATABASE_URL + JWT_SIGNING_KEY

uv run ruff check .
uv run ruff format --check .
uv run mypy src/
uv run pytest -q

uv run uvicorn loftly.api.app:app --reload --port 8000
```

Open http://localhost:8000/docs for the auto-generated Swagger UI.

## Layout

```
src/loftly/
  api/        FastAPI app, routers, middleware, auth deps
  core/       settings, logging
  db/         async engine + SQLAlchemy models
  schemas/    Pydantic request/response models (mirror openapi.yaml)
  prompts/    AI prompt registry (see AI_PROMPTS.md)
alembic/      migrations — 001..009 per SCHEMA.md §Migration order
docs/         openapi.yaml (copy of source of truth)
tests/        pytest — aiosqlite for Phase 1
```

## Migrations

```bash
uv run alembic upgrade head
uv run alembic revision -m "add something" --autogenerate
```

## Deploy

Fly.io target — see `fly.toml` and `../loftly/mvp/DEPLOYMENT.md`.

```bash
fly deploy --remote-only
```
