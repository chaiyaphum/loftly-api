"""Slow offline eval runners for the canonicalizer + selector golden sets.

These runners are excluded from default `pytest` CI. Run manually with:

    ANTHROPIC_API_KEY=sk-ant-... pytest -m eval tests/eval/runners/

or as standalone scripts:

    python -m tests.eval.runners.canonicalizer_eval
    python -m tests.eval.runners.selector_eval

See tests/eval/README.md for thresholds + interpretation.
"""
