# Phase 2B — Normalization Contract

Phase 2B sits on top of Phase 2A `fact_candidate` output. It does not
rewrite text. It standardizes a small set of fields that we now trust
enough to use downstream.

## Trusted extraction fields

These fields are currently treated as trusted extraction output:

- `fact_type`
- `borough`
- `publishable`
- `needs_second_source`

## Downstream use

- `fact_type`
  - section routing
  - editorial grouping
  - later rewrite tone selection
- `borough`
  - borough-specific relevance
  - district / outer-borough grouping
  - location-aware filtering
- `publishable`
  - pre-writer keep/drop decision
  - shortlist creation for later rewrite
- `needs_second_source`
  - editorial caution flag
  - verification queue
  - review priority

## Scope of normalization

Phase 2B currently adds:

- borough normalization
- canonical entity normalization
- a `primary_entity` hint
- explicit `trusted_extraction_fields` metadata

It does **not** yet:

- rewrite English into Russian
- decide final digest phrasing
- guarantee full entity recall

## Initial dictionaries

The first dictionary set is intentionally small:

- clubs
- councils
- major venues
- common place aliases

This should stay conservative until we see repeated evidence that the
layer is stable.

## Commands

```bash
# Print the contract
python3 scripts/run_local_digest.py phase2b-contract

# Normalize a Phase 2A output file
python3 scripts/run_local_digest.py phase2b-normalize \
  --input-path data/state/phase2a_runtime_rss_llm_output.json \
  --output-path data/state/phase2b_runtime_rss_normalized.json
```
