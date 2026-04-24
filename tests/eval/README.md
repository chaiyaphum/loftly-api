# tests/eval — offline golden-set evaluations

Slow, network-gated runners for the two AI surfaces whose quality we commit to:

| Surface | Data file | Runner | Gate |
|---|---|---|---|
| Merchant canonicalizer (Prompt 8, §9) | `merchant_canonicalizer.jsonl` (50 rows) | `runners/canonicalizer_eval.py` | macro F1 on `action` >= **0.85** |
| Card selector (Prompt 1) | `src/loftly/data/selector_eval/profiles.json` (v1, 25 rows) + `selector_golden_v2.jsonl` (v2, 10 rows) | `runners/selector_eval.py` | top-1 recall >= **0.75** |

Both gate unblock specific ship-decisions:
- Canonicalizer gate → `§9` Haiku flag flip (Prompt 8 wiring) — `mvp/MANUAL_ITEMS.md` row 26b
- Selector gate → Idea 1 Promo-Aware Selector flag flip (`LOFTLY_FF_SELECTOR_PROMO_CONTEXT`) — `mvp/MANUAL_ITEMS.md` row 26c

## Why they're not in default CI

These runners drive **real Haiku / Sonnet calls** at ~$0.10–$0.30 per pass and take tens of seconds. CI should stay free + fast. Both runners are registered under the `eval` pytest marker and additionally require `LOFTLY_RUN_EVAL=1` (belt + braces: marker-only opt-in is too easy to trigger by accident).

## How to run

### Canonicalizer

```bash
cd loftly-api
export ANTHROPIC_API_KEY=sk-ant-...
export LOFTLY_RUN_EVAL=1
# As pytest (asserts the gate):
uv run pytest -m eval tests/eval/runners/canonicalizer_eval.py -s
# As a plain script (prints the report, exit 0/1 on gate):
uv run python -m tests.eval.runners.canonicalizer_eval
```

Behaviour matrix:

| State | Outcome |
|---|---|
| `LOFTLY_RUN_EVAL` unset | `pytest.skip` |
| `ANTHROPIC_API_KEY` unset / stub / test | prints `eval skipped: no key`, returns 0 |
| real key + `LOFTLY_RUN_EVAL=1` | exercises steps 1–4 (normalize → exact → fuzzy → Haiku); scores & asserts macro F1 >= 0.85 |

### Selector

```bash
cd loftly-api
export LOFTLY_RUN_EVAL=1
# Deterministic provider (default) — no key needed:
uv run pytest -m eval tests/eval/runners/selector_eval.py -s
# Real Sonnet:
export LOFTLY_EVAL_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...
uv run python -m tests.eval.runners.selector_eval
```

The runner merges v1 (25 profiles) + v2 (10 edge-case profiles) and reports top-1 recall per source plus the combined number. The gate asserts on the combined recall so v2 additions can't silently regress the v1 baseline.

## Data shape

### `merchant_canonicalizer.jsonl`

One JSON object per line:

```json
{
  "raw": "สตาร์บัคส์",
  "category_hint": "dining-restaurants",
  "merchant_id_expected": "starbucks",
  "action_expected": "match",
  "notes": "Thai script — in alt_names; exact match"
}
```

- `raw` — string to classify (raw upstream `promos.merchant_name`)
- `category_hint` — nullable, mirrors `promos.category`; canonicalizer uses it as tiebreaker for rule-1 "Central" disambiguation
- `merchant_id_expected` — canonical **slug** (the runner resolves slugs to runtime UUIDs). `null` means "no match expected" (create / ambiguous)
- `action_expected` — one of `match` / `create` / `ambiguous` (the runner maps LLM's `new` → `create` and `uncertain` → `ambiguous` before scoring)
- `notes` — human-readable rationale, not scored

Row counts (locked by `mvp/MANUAL_ITEMS.md` row 26b):

| Section | Rows |
|---|---|
| Transliterations (Starbucks / Grab / 7-Eleven / Shopee / Lazada / KFC / McDonald's variants) | 20 |
| Fuzzy-match edges (format suffixes, Thai prefixes, Tesco-Lotus rebrand, etc.) | 15 |
| Genuinely new merchants (Yayoi, After You, Nitori, Don Don Donki, AIS Shop, Boots, Watsons, Decathlon, etc.) | 10 |
| Ambiguous (Central, Shell, Big C / Tops co-brand, generic Thai noun, PTT) | 5 |

### `selector_golden_v2.jsonl`

One JSON object per line — same shape as `src/loftly/data/selector_eval/profiles.json`'s `profiles[]` entries, with extra `promo_context_hint` fields documenting the edge case:

```json
{
  "id": "promo-expiring-7d",
  "persona": "cashback-maximizer",
  "monthly_spend_thb": 50000,
  "spend_categories": {"dining": 15000, "...": "..."},
  "goal": {"type": "cashback", "horizon_months": 6},
  "current_cards": [],
  "notes": "edge: active promo expires in 7 days...",
  "promo_context_hint": {"active_promos_expiring_7d": ["scb-starbucks-10pct"]},
  "expected_top3_card_slugs": ["scb-thai-airways", "kbank-wisdom", "uob-prvi-miles"]
}
```

The v2 set covers 10 edges:

| id | What it tests |
|---|---|
| `high-dining-spender` | 66% dining — promo-aware pick should surface dining-promo card |
| `low-dining-no-fnb-card` | Minimal dining — dining promos must not over-weight |
| `promo-expiring-7d` | Prompt 1 rule 3 — "หมดเขตเร็ว" prefix on valid_until <= 21d |
| `no-promo-category` | Petrol-heavy + no petrol promos — base-earn fallback |
| `unresolved-card-types-upstream` | Empty `card_types` from deal-harvester — silent skip per rule 5 |
| `expat-en-locale` | `locale=en` — rationale_en populated |
| `all-promos-expired` | Empty snapshot — `cited_promo_ids=[]`, status=`ok` not `degraded` |
| `stale-sync-24h` | 28h sync — below 72h threshold, no `PROMO_CONTEXT_UNAVAILABLE` sentinel |
| `duplicate-merchant-names` | Unresolved-canonical dupes — no double-counting in THB projection |
| `split-spend-5-category` | Even 20/20/20/20/20 split — benefits-depth ranking |

## Threshold interpretation

- **F1 >= 0.85** (canonicalizer) — honours `AI_PROMPTS.md §Prompt 8 Evaluation`'s
  "precision >= 0.9 on action=match" gate. We use macro F1 across all three
  actions rather than per-class precision so `create` + `ambiguous` regressions
  can't hide behind a high-support `match` class.
- **top-1 recall >= 0.75** (selector) — matches `AI_PROMPTS.md §Prompt 1 Evaluation`
  verbatim. The AI's top pick must land inside the expert-annotated top-3 for
  75%+ of profiles.

A sub-gate reading means the PR should not merge. Typical remediations:

1. Canonicalizer regression → extend `alt_names` in the seed migration (021 already layered onto 019), or tighten Prompt 8 rules. Don't raise the fuzzy threshold (0.85) without a Decision log entry.
2. Selector regression on v1 → the ranking code changed; diff `services/selector_rank.py` and `ai/providers/deterministic.py`.
3. Selector regression on v2 only → the edge case is legitimately unsupported; expand the seed catalog (miles card outside ROP, dining-tier card, etc.) OR update the expected slugs with a Decision log entry explaining why.

## Files

```
tests/eval/
├── README.md                        (this file)
├── merchant_canonicalizer.jsonl     (50 rows, row 26b)
├── selector_golden_v2.jsonl         (10 rows, row 26c)
└── runners/
    ├── __init__.py
    ├── canonicalizer_eval.py
    └── selector_eval.py
```

Last updated: April 2026
