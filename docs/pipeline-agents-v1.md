# Pipeline Agents v1

> ARCHIVED: historical planning note from the pre-Phase-1 period.
> Current pipeline is rule-based and authoritative behavior now lives in:
> `AGENTS.md`, `scripts/run_local_digest.py`, and `src/news_digest/pipeline/*`.
> Do not use this file as the source of truth for current architecture.

Прозрачная схема стадий, моделей и ограничений для `Greater Manchester Brief`.

## Model strategy

| Stage | Agent | Model | Reasoning |
|---|---|---|---|
| collect | collector | `gpt-5.4-mini` | `low` |
| dedupe | dedupe-classifier | `gpt-5.4-mini` | `medium` |
| validate | candidate-validator | `gpt-5.4-mini` | `medium` |
| write | writer | `gpt-5.4` | `medium` |
| edit | editor-balancer | `gpt-5.4` | `medium` |
| release | red-team-gate | `GPT-5.5` | `high` |

`GPT-5.5` намеренно ограничена только финальным gate.

## File contracts

| File | Role | Input | Output | Hard limits |
|---|---|---|---|---|
| `src/news_digest/pipeline/collector.py` | broad scan collector | source scan notes | `data/state/collector_report.json` | no prose, must mark each category checked true/false |
| `src/news_digest/pipeline/dedupe.py` | repeat handling | `candidates.json`, `published_facts.json` | updated `candidates.json`, `dedupe_memory.json` | every decision needs reason; carry-over only with label |
| `src/news_digest/pipeline/candidate_validator.py` | source gate | `candidates.json` | updated `candidates.json`, `candidate_validation_report.json` | no rewriting, only validation |
| `src/news_digest/pipeline/writer.py` | draft writer | include=true validated candidates | `draft_digest.html`, `writer_report.json` | cannot invent facts or source refs |
| `src/news_digest/pipeline/editor.py` | balancer and self-repair | `draft_digest.html`, candidates context | updated `draft_digest.html`, `editor_report.json` | cannot add new facts; must fail on weak city layer |
| `src/news_digest/pipeline/release.py` | final pass/fail | collector report, candidates, draft | `release_report.json`, promote to `current_digest.html` | fail closed, no city layer -> no send |

## State files

- `data/state/collector_report.json`
- `data/state/candidates.json`
- `data/state/candidate_validation_report.json`
- `data/state/draft_digest.html`
- `data/state/writer_report.json`
- `data/state/editor_report.json`
- `data/state/release_report.json`
- `data/state/last_sent_digest.html`
- `data/state/published_facts.json`
- `data/state/dedupe_memory.json`

## CLI commands

- `python3 scripts/run_local_digest.py init-build-state --overwrite`
- `python3 scripts/run_local_digest.py pipeline-config`
- `python3 scripts/run_local_digest.py collect-digest`
- `python3 scripts/run_local_digest.py dedupe-digest`
- `python3 scripts/run_local_digest.py validate-candidates`
- `python3 scripts/run_local_digest.py write-digest`
- `python3 scripts/run_local_digest.py edit-digest`
- `python3 scripts/run_local_digest.py build-digest`

## Fail policy

1. `build-digest` never sends directly.
2. Daily runner calls `build-digest` first.
3. If release gate fails, the main send is blocked.
4. Technical repair belongs in `editor`.
5. A digest with no real city/public-affairs layer must not ship.
