"""Pure-ish application services.

Reserved for deterministic compute that doesn't belong to a route handler
(e.g. merchant ranking, valuation math). No I/O side effects beyond the
passed-in `AsyncSession` — callers own caching + persistence.
"""
