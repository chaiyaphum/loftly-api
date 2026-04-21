"""Disaster-recovery tooling for Loftly Postgres snapshots.

Contents:

- ``snapshot_db``   - pg_dump -> AES-256-GCM -> Cloudflare R2 (with Object Lock).
- ``restore_db``    - download + decrypt + pg_restore, with prod-guard.
- ``list_snapshots``- enumerate recent snapshots from R2.
- ``drill``         - create scratch DB, restore latest staging snapshot, verify, drop.

Run any script with ``uv run python -m scripts.dr.<name> --help``.

The heavy lifting (encryption, R2 client, manifest shape) lives in ``_core``
so the CLI entrypoints stay thin and the tests can exercise the pure
functions without spawning subprocesses.
"""
