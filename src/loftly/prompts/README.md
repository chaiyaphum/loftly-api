# Prompts

Phase 1 prompt-text registry.

For the shapes, inputs, outputs, and eval harness, see
`../../../../loftly/mvp/AI_PROMPTS.md`.

Structure (prompts land here as Week 4 lights up):

- `card_selector/` — spend-profile → ranked card stack
- `valuation/` — weekly THB-per-point computation
- `card_review/` — editorial draft generation

Each prompt is versioned: the directory name is the slug, the file inside is
`v<N>.md` with a YAML frontmatter (model, temperature, max_tokens, cache
boundaries). Loaded via `loftly.core.prompts.load(name, version)` — wiring
arrives with the Selector implementation.

Nothing here is live in the Phase 1 scaffold. Do not write prompt text in
this scaffold PR.
