# Build Pipeline v1

Практический контракт для fail-closed сборки `Greater Manchester Brief`.

## Что появилось

- `python3 scripts/run_local_digest.py init-build-state`
- `python3 scripts/run_local_digest.py pipeline-config`
- `python3 scripts/run_local_digest.py collect-digest`
- `python3 scripts/run_local_digest.py dedupe-digest`
- `python3 scripts/run_local_digest.py validate-candidates`
- `python3 scripts/run_local_digest.py write-digest`
- `python3 scripts/run_local_digest.py edit-digest`
- `python3 scripts/run_local_digest.py build-digest`
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

## Как теперь должен работать daily pipeline

1. `collector` заполняет `collector_report.json`
2. `dedupe/classifier` пишет dedupe-решения в `candidates.json` и `dedupe_memory.json`
3. `candidate-validator` помечает publishable candidates
4. `writer` пишет `draft_digest.html`
5. `editor` делает self-repair и balance pass
6. `build-digest` прогоняет release gate
7. только при `release_decision = pass` обновляется `data/outgoing/current_digest.html`
8. только после этого `run_daily_digest.sh` имеет право отправлять выпуск

## collector_report.json

Обязательные категории:

- `media_layer`
- `transport`
- `gmp`
- `public_services`
- `culture_weekly`
- `venues_tickets`
- `food_openings`
- `football`

Минимальный формат:

```json
{
  "run_date_london": "2026-04-25",
  "categories": {
    "media_layer": {
      "checked": true,
      "sources": ["MEN", "BBC Manchester"],
      "candidate_count": 4,
      "notes": ""
    },
    "public_services": {
      "checked": true,
      "sources": ["GMMH", "ITV Granada"],
      "candidate_count": 1,
      "notes": "",
      "active_disruption_today": true
    }
  }
}
```

## candidates.json

Минимальный формат:

```json
{
  "run_date_london": "2026-04-25",
  "candidates": [
    {
      "title": "GMP raided Norton Street in Old Trafford",
      "category": "gmp",
      "summary": "Arrest, drugs seized, closure order work with Trafford Council.",
      "source_url": "https://...",
      "source_label": "GMP",
      "include": true,
      "dedupe_decision": "new"
    }
  ]
}
```

`dedupe_decision` допускает только:

- `drop`
- `carry_over_with_label`
- `new_phase`
- `new`

Если используется `carry_over_with_label`, обязателен `carry_over_label`.

## release gate

`build-digest` заваливает выпуск, если:

- не пройден broad scan
- нет валидных включённых candidates
- нет `draft_digest.html`
- драфт не на сегодняшнюю дату
- нет обязательных блоков
- в погоде нет цифр
- есть `/amp/`, placeholders или голос сборщика
- есть доступные city/public-affairs candidates, но выпуск перекошен в soft layer
- есть active public-services disruption today, но в тексте он не виден
- нет включённых city/public-affairs candidates вообще

Итог всегда пишется в `data/state/release_report.json`.
