# Seed round data room — anonymized metrics export

This doc describes the JSON artifact produced by `scripts/run_metrics_export.py`
(and the identical payload returned from `POST /v1/admin/metrics/export`). The
artifact is designed for inclusion in the seed-round data room so investors can
review Loftly's traction without ever touching PII.

*Last updated: October 2026*

## Schema

Top-level keys. Stable across minor versions (additive changes only;
backwards-incompatible changes bump `schema_version`).

| Key | Type | Description |
| --- | --- | --- |
| `schema_version` | string | Semver-ish. Bump on breaking structural change. |
| `generated_at` | ISO datetime | Moment the export ran (UTC). |
| `as_of` | ISO datetime | Caller-supplied snapshot anchor. All windows end here. |
| `window_days` | int | Default rolling window for system-level metrics (30). |
| `users` | object | Totals, WAU/MAU, retention curve, consent %. |
| `selector` | object | Selector invocations, unique users, avg latency, top-1 conv. |
| `affiliate` | object | Click / conversion funnel + commission buckets + top-5 cards. |
| `content` | object | Published articles, distinct cards covered, schema validation. |
| `llm_costs` | object | Anthropic spend + cache hit rate + Haiku fallback rate. |
| `system` | object | Uptime + p95 latency + 5xx rate. |
| `disclaimers` | array<string> | Plain-English provenance notes for the reader. |

### `users`

```
total_registered         int     // soft-deleted users excluded
wau                      int     // distinct users active in last 7d
mau                      int     // distinct users active in last 30d
retention_weekly         array   // 12 entries, oldest → newest
  [].week_start          ISO date
  [].active_users        int
consent_grant_rate       object  // keyed by PDPA purpose
  [purpose].users_prompted  int
  [purpose].users_granted   int
  [purpose].grant_rate      float (0–1)
```

"Active" is proxied by any Selector invocation bound to a user. When we add
a client-side heartbeat we'll switch to that definition and bump the schema.

### `selector`

```
window_days              int
invocations              int
unique_users             int
avg_latency_ms           float   // from output.latency_ms, provider-stamped
top1_conversion_rate     float   // (clicks on rec card within 7d) / eligible sessions
top1_sample_size         int
eval_top1_recall         float | null  // latest selector_eval SyncRun, if any
```

### `affiliate`

```
window_days              int
total_clicks             int
unique_users_clicked     int
conversions              int
conversion_rate          float
commission_thb_by_month  array   // 6 entries, oldest → newest
  [].month_start         ISO date
  [].commission_thb      float
top_cards_by_conversions array   // at most 5 entries
  [].card_slug           string  // bank-neutral slug, NOT user-identifying
  [].conversions         int
  [].commission_thb      float
```

### `content`

```
articles_published              int
distinct_cards_covered          int
avg_update_age_days             float
schema_review_validation_rate   float
```

### `llm_costs` (placeholder until Langfuse pricing ledger is wired)

```
window_days              int
total_spend_thb          float
spend_per_mau_thb        float
prompt_cache_hit_rate    float | null
haiku_fallback_rate      float | null
source                   string  // "placeholder — wire Langfuse pricing ledger"
```

### `system` (placeholder until Grafana scrape lands)

```
window_days              int
uptime_staging_pct       float | null
uptime_prod_pct          float | null
http_5xx_rate            float | null
p95_request_latency_ms   float | null
source                   string  // "placeholder — wire Grafana/Fly metrics scrape"
```

## How to run the export

```sh
# Default — drops ./metrics-<today>.json in cwd
uv run python scripts/run_metrics_export.py

# Explicit snapshot + output path for the data room folder
uv run python scripts/run_metrics_export.py \
    --as-of 2026-10-01 \
    --out data-room/metrics-2026-10.json
```

The HTTP route mirrors the same output (admin JWT required):

```sh
curl -X POST https://api.loftly.co.th/v1/admin/metrics/export \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d '{"as_of": "2026-10-01"}'
```

## PII leakage review checklist

Before dropping a new export into the data room, grep for patterns that would
indicate a raw identifier slipped through:

```sh
# Fail-loud checks — any hit = block
grep -F '@' data-room/metrics-2026-10.json         # email char
grep -E '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' \
     data-room/metrics-2026-10.json                # UUID shape
grep -E '\b[0-9]{10,}\b' data-room/metrics-2026-10.json  # phone / raw ID

# These should all return *no matches*.
```

The `tests/test_metrics_export.py::test_export_contains_no_pii` test asserts
this at CI time; the manual grep above is a paranoia double-check before the
file leaves the VPN.

## Disclaimers

- All numbers derived from staging + prod **aggregates** at `as_of`. No
  per-user rows, no IP hashes, no user-agent hashes, no click IDs.
- Data-room snapshots are **point-in-time**. Investors should expect the
  numbers to drift between exports; that's the feature, not a bug.
- `llm_costs` and `system` slots are forward-compatible placeholders. Real
  values land when Langfuse + Grafana are wired into the ops stack (tracked
  in `mvp/DEPLOYMENT.md §Observability`).
- Consent rates are computed on **latest per (user, purpose)** — re-prompts
  where a user toggles off are reflected the next time the export runs.

## Change log

- 2026-10 — initial schema v1.0 (seed-round data-room requirement, W25).
