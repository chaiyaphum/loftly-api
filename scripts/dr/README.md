# DR snapshot tooling

Postgres disaster-recovery scripts for Loftly. Encrypts with AES-256-GCM and
uploads to Cloudflare R2 with a 30-day Object Lock retention; decryption +
restore is symmetrical and guarded against accidental prod targets.

## Layout

- `snapshot_db.py` — `pg_dump` → encrypt → upload. Writes a `.manifest.json` sidecar.
- `restore_db.py`  — download → decrypt → `pg_restore`. Verifies row counts.
- `list_snapshots.py` — lists recent snapshots with Object-Lock expiry.
- `drill.py` — creates a scratch DB, restores the latest staging snapshot, verifies, drops.
- `_core.py` — shared encryption + manifest + R2 client helpers.

All scripts are standalone CLIs: `uv run python -m scripts.dr.<name> --help`.

## Prerequisites

Install the optional `dr` dependency group:

```sh
uv sync --extra dr
```

Environment variables (all required unless noted):

| Var                          | Purpose                                              |
| ---------------------------- | ---------------------------------------------------- |
| `LOFTLY_DR_ENCRYPTION_KEY`   | 32-byte AES-256 key, hex-encoded.                    |
| `CF_ACCOUNT_ID`              | Cloudflare account ID (for the R2 endpoint).         |
| `R2_ACCESS_KEY_ID`           | R2 API token access key.                             |
| `R2_SECRET_ACCESS_KEY`       | R2 API token secret.                                 |
| `LOFTLY_DR_BUCKET` (optional)| Override bucket (default `loftly-dr-snapshots`).     |
| `LOFTLY_ENV`                 | `dev` / `staging` / `prod` — prefix for object keys. |

Generate a fresh encryption key on any machine with Python:

```sh
python -c "import secrets; print(secrets.token_hex(32))"
```

Store the key as a DO App Platform secret env var on `loftly-api-staging`
(`doctl apps update <app_id> --spec ...` with `LOFTLY_DR_ENCRYPTION_KEY`)
**and** in the founder's 1Password vault. Losing it = losing the snapshots.

Create the R2 bucket with Object Lock enabled via the Cloudflare dashboard or
`wrangler r2 bucket create loftly-dr-snapshots --with-object-lock`. Object
Lock must be enabled at bucket creation — it cannot be toggled on later.

## Cadence

- **Prod snapshot** — daily at 04:00 UTC (DO App Platform scheduled job / Cloudflare Worker cron).
- **Staging snapshot** — daily at 04:15 UTC.
- **Drill** — weekly on Monday 05:00 UTC; restores the latest staging snapshot
  into a scratch DB on the staging cluster, verifies row counts, drops.
- **Manual verification** — founder restores prod into a throwaway DB once a
  quarter and spot-checks selected rows.

## Runbook — "The prod DB is gone"

1. Recreate the DO Managed Postgres cluster + DB + app user, then note the
   connection string (full procedure: `../../../loftly/mvp/artifacts/do/README.md`):
   ```sh
   doctl databases create loftly-staging --engine pg --version 16 --region sgp1 --size db-s-1vcpu-1gb --num-nodes 1
   DB_ID=$(doctl databases list --format ID,Name --no-header | awk '$2=="loftly-staging"{print $1}')
   doctl databases db create   "$DB_ID" loftly_prod
   doctl databases user create "$DB_ID" loftly_prod_app
   # then run the schema GRANT block from loftly/mvp/artifacts/do/README.md as doadmin
   ```
2. From a workstation with R2 creds + `LOFTLY_DR_ENCRYPTION_KEY`:
   ```sh
   uv run python -m scripts.dr.list_snapshots --env prod --limit 5
   ```
   Pick the newest `.dump.enc` key.
3. Restore (explicit `--really-prod`, because the target URL now contains
   "prod"):
   ```sh
   uv run python -m scripts.dr.restore_db \
       --snapshot s3://loftly-dr-snapshots/prod/2026-04-21/postgres-04-00-00.dump.enc \
       --target-database-url postgresql://... \
       --really-prod
   ```
4. Point the API at the new cluster: update the `DATABASE_URL` secret on the
   `loftly-api-staging` app spec and apply (`doctl apps update <app_id> --spec /tmp/...`),
   and re-append the app as a DB trusted source
   (`doctl databases firewalls append "$DB_ID" --rule app:<app_id>`).
5. DO App Platform redeploys on the spec update (PRE_DEPLOY runs `alembic upgrade head`).
   Smoke-test `/healthz` and `/readyz`.
6. Document the incident in `docs/INCIDENTS.md`.

## Runbook — "The drill is failing"

- If `list_snapshots` shows nothing under `staging/<today>/`: the snapshot
  cron failed overnight. Check Fly machine logs for `scripts.dr.snapshot_db`.
- If row-count verification fails with a small delta (<5%): re-run the drill
  — likely a snapshot-time write race. If it reproduces, investigate the
  source of the drift before shipping more snapshots.
- If `pg_restore` itself errors: the ciphertext may be corrupted. Download
  the previous day's snapshot and retry; open an incident to root-cause.

## Testing

Unit tests live in `tests/test_dr_snapshot.py` and use `moto[s3]` to mock R2.
They exercise:

- Encrypt/decrypt round-trip.
- Tamper detection (GCM tag failure).
- Prod-URL guard (`--really-prod` refusal).
- Manifest JSON round-trip.
- Snapshot listing / date ordering.

Run:

```sh
uv run pytest tests/test_dr_snapshot.py -v
```
