"""Manual-curation promo fixtures for banks without deal-harvester coverage.

Each `{slug}.json` is a hand-curated list of promos for that issuer. The
ingest job in `loftly.jobs.manual_catalog_ingest` reads these files and syncs
them into the `promos` table. Founder fact-checks entries against the live
bank promo page before we turn on the scheduled ingest.
"""
