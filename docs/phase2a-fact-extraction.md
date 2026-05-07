# Phase 2A — Fact Extraction

Phase 2A is the first LLM-facing layer for the digest. It does not
write Russian digest bullets. It converts already-collected source
items into structured `fact_candidate` JSON.

## Scope

This stage is intentionally narrow:

1. Define the `fact_candidate` schema.
2. Build a prompt pack for clean RSS/feed-style items.
3. Compare LLM output to the current deterministic candidate view.

Do **not** connect this to the production `writer` yet.

## Commands

```bash
# Print the schema
python3 scripts/run_local_digest.py phase2-fact-schema

# Build inputs + prompt + deterministic baseline from current state
python3 scripts/run_local_digest.py phase2-rss-pack

# Build the same pack from runtime state instead of project-local state
python3 scripts/run_local_digest.py phase2-rss-pack \
  --state-root "$HOME/.mnewsdigest/data/state" \
  --output-root data/state \
  --output-prefix phase2a_runtime_rss

# Compare an LLM response file to the deterministic baseline
python3 scripts/run_local_digest.py phase2-compare-facts \
  --llm-output data/state/phase2a_rss_llm_output.json
```

## Generated artifacts

`phase2-rss-pack` writes three files under the chosen state directory:

- `phase2a_rss_inputs.json`
- `phase2a_rss_prompt.txt`
- `phase2a_rss_deterministic_baseline.json`
- `phase2a_rss_llm_output.json` (written by the agent after running the prompt)

These are the handoff files for the first extraction experiments on:

- BBC Manchester
- GMP
- MEN
- The Mill

## Notes

- The baseline is **not** the ground truth for borough/entity
  normalization. It is only the current deterministic view of the same
  candidates.
- The project itself does **not** call an LLM API. The agent uses
  `phase2a_rss_prompt.txt` externally, then writes the returned JSON to
  `phase2a_rss_llm_output.json` for comparison.
- The schema includes `publishable` and `drop_reason` on purpose so the
  LLM can explicitly reject affiliate, evergreen, or out-of-scope items.
- `reader_relevance` in Phase 2A is kept in English and short. Russian
  rewrite belongs to the later writer/editor stage.
