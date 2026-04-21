# CLAUDE.md — loftly-api

This repo is `loftly-api`: the FastAPI backend for Loftly's Phase 1 MVP.

**Source of truth for the contract and data model lives in `../loftly/mvp/`** —
always read those files before making changes:

- `../loftly/mvp/API_CONTRACT.md` — endpoint inventory
- `../loftly/mvp/artifacts/openapi.yaml` — OpenAPI 3.1 (binding)
- `../loftly/mvp/SCHEMA.md` + `artifacts/schema.sql` — 14 tables
- `../loftly/mvp/DEPLOYMENT.md` — env vars + Fly.io target

Stack: Python 3.12 (uv), FastAPI, Pydantic v2, SQLAlchemy 2.0 async + asyncpg,
Alembic, structlog. Tests use aiosqlite so models must stay portable. Lint:
ruff + mypy strict. Do not diverge field names or types from `openapi.yaml`.
No Celery Phase 1 — BackgroundTasks only.
