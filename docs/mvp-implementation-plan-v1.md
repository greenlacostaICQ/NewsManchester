# MVP Implementation Plan v1

Практический план сборки production-пайплайна для `Greater Manchester AM Brief`.

## 1. Что строим в MVP

Цель MVP:

- автоматически собирать источники каждый день
- выпускать один утренний дайджест к `08:00 Europe/London`
- доставлять его в `Telegram`
- сохранять историю запусков и уже опубликованных фактов
- не зависеть от включенного локального компьютера

Что сознательно НЕ включаем в MVP:

- красивую web-панель
- сложную ML-классификацию
- многоформатную доставку в 5 каналов
- full real-time monitoring в течение дня
- сложные social integrations

Фокус MVP:

- надёжность
- верификация
- дедупликация
- понятная эксплуатация

## 2. Рекомендуемый стек

Для `бесплатного первого этапа` я рекомендую такой стек:

- `Python 3.11+`
- `JSON / SQLite state`
- `Playwright` для сложных страниц
- `httpx` + `BeautifulSoup` / `selectolax` для обычного HTML
- `GitHub Actions schedule`
- `Telegram Bot API` для доставки
- `GitHub repository secrets`

Почему так:

- Python удобен для scraping / parsing / ranking / text assembly
- JSON или SQLite достаточно для MVP без отдельной инфраструктуры
- Playwright нужен для ticketing и тяжёлых фронтов
- GitHub Actions даёт бесплатный scheduler на старте
- Telegram проще всего для утренней доставки

## 3. Где запускать

Стартовый бесплатный вариант:

- `private GitHub repository`
- `GitHub Actions` по расписанию
- `Telegram Bot` для доставки

То есть на первом этапе у нас `нет отдельного сервера`.

Как это работает:

- GitHub Actions запускает пайплайн по расписанию
- код и состояние хранятся в приватном репозитории
- итоговый дайджест уходит в Telegram
- локальный компьютер может быть выключен

Как храним состояние без отдельной базы:

- `data/published_items.json`
- `data/ticket_radar_state.json`
- `data/source_health.json`
- `data/archive/YYYY-MM-DD.md`

После успешного запуска workflow обновляет эти файлы и коммитит их обратно в репозиторий.

Почему это хороший старт:

- бесплатно
- достаточно надёжно для MVP
- просто развернуть
- легко перейти на VPS позже

Ограничения этого варианта:

- scheduled jobs в GitHub Actions могут задерживаться
- тяжёлые динамические страницы будут более хрупкими
- состояние в JSON-файлах менее удобно, чем PostgreSQL

Следующий этап после MVP:

- маленький `VPS` в Европе
- `PostgreSQL`
- более устойчивый scheduler

## 4. Канал доставки

Рекомендация для MVP:

- `Telegram` как основной канал

Почему:

- утренний формат лучше читается в мессенджере
- push в `08:00` работает лучше email
- проще технически
- проще тестировать private chat / private channel

Email на первом этапе не делаем.

## 5. Архитектура MVP

Минимальные компоненты:

1. `scheduler`
2. `fetchers`
3. `raw storage`
4. `extractors`
5. `normalizer`
6. `verifier`
7. `deduper`
8. `ranker`
9. `digest assembler`
10. `telegram sender`
11. `run logger`

## 6. Минимальная схема данных

На бесплатном этапе это логическая схема. Физически она может храниться в `JSON` или `SQLite`.

### Таблица `sources`

- `id`
- `name`
- `category`
- `tier`
- `base_url`
- `entry_url`
- `poll_frequency`
- `parser_type`
- `active`
- `notes`

### Таблица `raw_documents`

- `id`
- `source_id`
- `url`
- `canonical_url`
- `fetched_at`
- `published_at`
- `content_hash`
- `raw_text`
- `raw_html_path`
- `status`

### Таблица `fact_candidates`

- `id`
- `raw_document_id`
- `fact_type`
- `title`
- `summary`
- `borough`
- `district`
- `venue`
- `event_date`
- `event_time`
- `price_text`
- `source_confidence`
- `needs_second_source`
- `fact_fingerprint`
- `normalized_payload`

### Таблица `published_items`

- `id`
- `fact_fingerprint`
- `section_name`
- `published_on`
- `title_snapshot`
- `status`

### Таблица `pipeline_runs`

- `id`
- `run_date`
- `started_at`
- `finished_at`
- `status`
- `stats_json`
- `error_summary`

## 7. Source Registry v1

MVP должен стартовать не со всех возможных источников, а с `ядра`, иначе мы утонем в парсерах.

### Wave 1: обязательно в MVP

- TfGM
- National Rail
- GMP newsroom
- Manchester Airport Media Centre
- Manchester Council
- Salford Council
- Trafford Council
- Stockport Council
- Oldham Council
- Rochdale Council
- Bolton Council
- Tameside Council
- Bury Council
- Wigan Council
- Manchester United
- Manchester City
- Salford City
- Visit Manchester
- Factory International
- AO Arena
- Co-op Live
- Ticketmaster Manchester
- DICE
- See Tickets
- Manchester Digital
- Prolific North
- University of Manchester
- Manchester Metropolitan
- University of Salford
- BBC Manchester
- Manchester Evening News
- ManchesterWorld
- The Mill
- Manchester's Finest
- Confidentials

### Wave 2: добавить после стабилизации

- Altrincham Today
- hyperlocal district sources
- additional theatres and museums
- food halls direct sources
- Reddit / X signal watchers

## 8. Parser strategy

Для каждого источника задаём parser class:

- `rss_parser`
- `article_list_parser`
- `event_calendar_parser`
- `fixture_parser`
- `ticket_parser`
- `planning_parser`

Принцип:

- один parser class на тип страницы
- source-specific overrides только если без них никак

Это сильно упростит поддержку.

## 9. Ежедневный pipeline run

### 06:00–07:20

- incremental fetch по high-priority источникам
- refresh источников с hourly / 4h polling

### 07:20–07:35

- extraction
- normalization
- verification
- dedupe

### 07:35–07:45

- ranking
- section assignment
- first draft digest assembly

### 07:45–07:52

- link validation
- style-guide validation
- empty-state handling

### 07:52–07:58

- Telegram render
- send dry output to internal log
- final send

### 08:00+

- archive issue
- mark published fingerprints
- store final digest

## 10. Validation before send

Перед отправкой выпуск должен пройти автоматические проверки:

- не меньше `8` сигналов суммарно
- нет broken links
- нет unverified exact details
- нет item без source reference
- нет повторов из последних `7` дней без update
- у каждого TL;DR есть раскрытие ниже
- ticket items имеют корректный lifecycle status
- обязательные блоки не пустые без empty-state строки

Если любая проверка падает:

- выпуск не отправляется в production-чат
- создаётся alert
- сохраняется failed draft

## 11. Telegram delivery design

Рекомендованный формат MVP:

- одно сообщение, если влезает
- иначе:
  - message 1: шапка + TL;DR + focus + transport
  - message 2: 24h + 7d + Ticket Radar + финальный блок

Нужно учитывать:

- Telegram limits по длине
- аккуратную Markdown-разметку
- короткие ссылки

Я бы в MVP избегал:

- слишком большого количества эмодзи
- тяжёлой декоративности
- вложенных списков

## 12. Архив

На первом этапе архив храним в репозитории:

- markdown-версия финального выпуска
- лог запуска
- список опубликованных fingerprint-ов

Email-архив можно добавить позже, но в MVP он не нужен.

## 13. Monitoring

Нужны минимум 5 типов алертов:

- source fetch failure
- parser failure
- suspiciously empty section
- broken link in final digest
- delivery failure

MVP-вариант мониторинга:

- error log file
- Telegram admin alert
- daily run summary

## 14. Security и секреты

Нужно вынести в secrets:

- Telegram bot token
- Telegram chat id
- GitHub token для записи состояния, если понадобится отдельный

Не хранить secrets:

- в git
- в markdown docs
- в коде

## 15. Что вы ещё могли не учесть

Вот практические вещи, о которых обычно вспоминают слишком поздно:

- нужна `ручная пауза` на дни, когда выпуск временно не нужен
- нужен `test chat`, чтобы не слать сырой выпуск в production
- нужен `archive mode`, чтобы пересобрать выпуск за конкретную дату
- нужен `source health dashboard`, хотя бы текстовый
- нужно хранить `почему item был исключён`
- нужно уметь быстро выключать проблемный parser
- нужно отдельно логировать `high-value misses`, например пустой Ticket Radar в крупный концертный сезон

## 16. Порядок разработки

### Phase 0: foundation

- создать репозиторную структуру
- описать source registry
- настроить GitHub Actions
- подготовить state storage

### Phase 1: ingestion MVP

- fetcher framework
- raw storage
- source registry loading
- 5–8 ключевых источников

### Phase 2: extraction + normalization

- candidate facts
- canonical entities
- borough mapping
- exact detail flags

### Phase 3: verification + dedupe

- primary/secondary logic
- link checking
- 7-day dedupe memory
- ticket lifecycle

### Phase 4: assembly + Telegram

- section builder
- style guide validator
- Telegram sender
- internal test output

### Phase 5: production hardening

- retries
- alerts
- health checks
- archive mode
- backup schedule

## 17. Рекомендуемая структура проекта

```text
news-brief/
  docs/
  src/
    app/
    config/
    fetchers/
    parsers/
    extractors/
    normalize/
    verify/
    dedupe/
    ranking/
    assembly/
    delivery/
    models/
    jobs/
  scripts/
  tests/
  data/
  docker/
```

## 18. MVP definition of done

MVP считается готовым, если:

- 14 дней подряд daily run проходит по расписанию
- Telegram digest приходит к `08:00 +/- 15 минут`
- broken links нет
- Ticket Radar не пропускает критичные sale windows
- transport section стабильно собирается
- повторы между днями редки и контролируемы
- выпуск соответствует `Editorial Style Guide v1.1`

## 19. Моя рекомендация по старту

Я бы запускал проект так:

1. `Telegram-only MVP`
2. `Private GitHub repo + GitHub Actions`
3. `JSON / SQLite state, без отдельной БД`
4. `Wave 1 sources only`
5. `No fancy UI`
6. `Strong logging and archive first`

Это даст fastest path к полезному продукту без лишней сложности.
