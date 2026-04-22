# syntax=docker/dockerfile:1.6

# ----- Stage 1: builder -----
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.10.8 /uv /usr/local/bin/uv

WORKDIR /app

# Resolve deps first (cache-friendly)
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Copy project and install. pyproject.toml declares `readme = "README.md"`, so
# hatchling insists on it during the editable-install step even though we ship
# no docs in the image.
COPY src/ ./src/
COPY alembic.ini ./alembic.ini
COPY alembic/ ./alembic/
COPY scripts/ ./scripts/
COPY README.md ./README.md
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# ----- Stage 2: runtime -----
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --system --gid 1000 loftly \
 && useradd --system --uid 1000 --gid loftly --create-home --home /home/loftly loftly

WORKDIR /app

COPY --from=builder --chown=loftly:loftly /app/.venv /app/.venv
COPY --from=builder --chown=loftly:loftly /app/src /app/src
COPY --from=builder --chown=loftly:loftly /app/alembic /app/alembic
COPY --from=builder --chown=loftly:loftly /app/alembic.ini /app/alembic.ini
COPY --from=builder --chown=loftly:loftly /app/scripts /app/scripts

USER loftly

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz').status == 200 else 1)"

# --timeout-graceful-shutdown=25 — on SIGTERM uvicorn stops accepting new
# conns, drains in-flight requests for up to 25s, then force-closes. DO App
# Platform's default grace window is 30s; the 5s buffer leaves room for the
# lifespan teardown (DB dispose, Sentry/Langfuse flush — see DRILL-003).
CMD ["uvicorn", "loftly.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "25"]
