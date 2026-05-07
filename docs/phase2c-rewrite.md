# Phase 2C — Russian Rewrite / Editor

Phase 2C consumes normalized Phase 2B output and prepares a
publishable-only rewrite pack for the agent.

## Scope

Phase 2C does not:

- decide publishability
- reclassify boroughs
- normalize canonical entities

Phase 2C does:

- rewrite English/source prose into Russian digest bullets
- use `fact_type`, `borough`, `needs_second_source`, and `primary_entity`
- keep cautious wording for items flagged `needs_second_source=true`

## Current style baseline

The currently approved rewrite baseline is:

- `BBC Manchester`
- `GMP`
- `MEN`

For these sources, the target style is:

- concise
- self-contained
- practical for the reader
- no invented detail

## The Mill rule

`The Mill` long-form items are allowed into Phase 2C, but they need a
stricter review rule.

If the summary is teaser-like or too thin to support a confident,
self-contained Russian bullet, set:

- `needs_manual_review=true`
- a short `review_note`

Do not force a polished-sounding rewrite when the source itself is too
underspecified.

## Commands

```bash
# Print the Phase 2C contract
python3 scripts/run_local_digest.py phase2c-contract

# Build a publishable-only rewrite pack from Phase 2B output
python3 scripts/run_local_digest.py phase2c-build-pack \
  --input-path data/state/phase2b_second_rss_normalized.json \
  --output-prefix phase2c_second_rss

# Activate an agent-produced rewrites JSON for optional writer use
python3 scripts/run_local_digest.py phase2c-activate-rewrites \
  --rewrite-path data/experiments/phase2c_second_rss_rewrites.json
```

## Generated artifacts

- `phase2c_*_inputs.json`
- `phase2c_*_prompt.txt`

These files are the handoff for the agent-driven rewrite step. The
project itself still does not call an LLM API.

## Optional writer integration

Phase 2C is integrated conservatively.

- only agent-produced rewrites that pass local validation are activated
- only `BBC Manchester`, `GMP`, and `MEN` are trusted rewrite sources
- `The Mill` remains outside automatic activation when a rewrite is
  marked `needs_manual_review=true`
- if no active rewrite exists for a candidate, `writer.py` falls back to
  the existing deterministic rendering path
