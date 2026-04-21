"""One-shot script: finalize every due account-delete job.

Phase 1 invocation model: founder runs this manually after checking the
Fly.io logs. Phase 2 will wire it to a daily cron inside Fly.

    uv run python -m scripts.run_purges
"""

from __future__ import annotations

import asyncio
import json

from loftly.core.logging import configure_logging, get_logger
from loftly.core.settings import get_settings
from loftly.jobs.account_delete import run_due_purges


async def main() -> None:
    configure_logging(get_settings())
    log = get_logger(__name__)
    results = await run_due_purges()
    log.info("account_purge_run_summary", count=len(results))
    # Human-readable echo — useful when running interactively.
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
