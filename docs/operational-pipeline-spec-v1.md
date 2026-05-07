# Operational Pipeline Spec v1

Техническая спецификация для автоматизации `Greater Manchester AM Brief` без участия пользователя.

## 1. Цель системы

Система должна ежедневно:

- собирать сигналы из утверждённого реестра источников
- извлекать из них кандидатные факты
- нормализовать, дедуплицировать и ранжировать эти факты
- проверять ссылки и подтверждения
- собирать финальный выпуск строго по `Editorial Style Guide v1.2`
- доставлять выпуск пользователю без ручного участия

## 2. Ключевое ограничение

Если система должна работать `ежедневно без вас` и `при выключенном компьютере`, её нельзя оставлять только в локальной desktop-автоматизации.

Для бесплатного этапа внешний runtime будет таким:

- `private GitHub repository`
- `GitHub Actions` по расписанию
- `Telegram Bot API`
- `state files` внутри репозитория

Локальная автоматизация в Codex остаётся только как инструмент разработки и ручных проверок.

## 3. Рекомендованная архитектура

Минимально надёжная схема бесплатного этапа:

1. `Scheduler`
   - запускает пайплайн ежедневно в `07:30 Europe/London`
2. `Source fetcher`
   - опрашивает источники по их частоте
3. `Raw store`
   - сохраняет сырые документы и метаданные
4. `Extractor`
   - превращает документы в кандидатные факты
5. `Normalizer`
   - приводит даты, имена, borough tags, venue names, event types к единому виду
6. `Verifier`
   - проверяет ссылки, наличие подтверждения и полноту точных деталей
7. `Deduper`
   - убирает повторы внутри выпуска и между выпусками
8. `Ranker`
   - считает приоритет и определяет блок выпуска
9. `Assembler`
   - собирает выпуск строго по style guide
10. `Delivery`
   - отправляет итог в Telegram
11. `Run log`
   - пишет статус запуска, ошибки и набор включённых / исключённых фактов

## 4. Хранилища данных

Нужно минимум 4 логических слоя данных:

- `sources`
  - реестр URL, tier, category, polling frequency, parser type
- `raw_documents`
  - сырой HTML / RSS / JSON / текст
  - source_id
  - fetched_at
  - canonical_url
  - hash
- `fact_candidates`
  - факт после извлечения
  - title
  - summary
  - event_type
  - place
  - borough
  - start_time
  - end_time
  - importance score
  - source references
  - confidence
- `published_items`
  - что уже выходило в прошлых выпусках
  - fact_fingerprint
  - first_published_at
  - last_published_at
  - item_status
  - primary_block
  - carry_over_allowed_until
  - last_known_phase
  - source_urls

Минимальная practical реализация бесплатного этапа:

- `JSON` или `SQLite` для состояния
- файлы в репозитории для archive/state
- raw snapshots в `data/raw/`

Следующий этап:

- `PostgreSQL`
- object storage

## 5. Реестр источников

Каждый источник в registry должен иметь:

- `source_id`
- `name`
- `category`
- `tier`
- `type`
- `base_url`
- `entry_urls`
- `batch_window`
- `fetch_method`
- `parser_type`
- `geo_scope`
- `content_types`
- `anti_bot_risk`
- `primary_alternative`
- `active`

Пример категорий:

- transport
- council
- safety
- airport
- football
- events
- tickets
- openings
- planning
- business
- tech
- university
- media
- hyperlocal

Пример fetch methods:

- rss
- sitemap
- static_html
- dynamic_html
- json_api
- manual_fallback

Правило выбора метода:

- сначала `api`
- потом `rss`
- потом `html`
- `signal` никогда не считается подтверждением сам по себе

## 6. Batch-окна опроса

Для single-shot morning brief не нужен постоянный hourly polling в течение суток.

Нужны три batch-волны перед сборкой:

- `Wave A — 06:00`
  - councils
  - media
  - universities
  - venues
  - openings
  - planning / licensing
  - tech / business
- `Wave B — 07:00`
  - TfGM
  - National Rail
  - Met Office
  - GMP
  - official football pages
- `Wave C — 07:15`
  - только в дни presale / general sale
  - ticket refresh
  - повторный transport check, если утром есть ограничения

Это даёт свежесть без лишней нагрузки на pipeline и без сотен запросов в день.

## 7. Fetch layer

Каждый fetch-run делает:

- загрузку страницы или feed
- определение canonical URL
- вычисление content hash
- сохранение raw snapshot
- сохранение fetch metadata

Антидубли на уровне fetch:

- если hash не изменился, документ можно не переизвлекать
- если URL новый, извлечение обязательно

Ошибки fetch делятся на:

- temporary
- permanent
- blocked / anti-bot
- content changed / parser mismatch

## 8. Extraction layer

Цель extraction — не написать выпуск, а получить `fact candidates`.

Для каждого документа надо извлекать:

- headline
- short summary
- source publication timestamp
- source type
- content category
- entities
  - venue
  - borough
  - district
  - club
  - company
  - university
- exact details
  - dates
  - times
  - prices
  - routes
  - scores
  - sale windows
- raw citations

Extractor обязан помечать:

- `confirmed_exact_details`
- `missing_exact_details`
- `needs_second_source`
- `news_layer_source`
- `official_source`
- `media_source`

## 9. Normalization layer

Нормализация приводит разные форматы к единому стандарту:

- `Manchester city centre`, `City Centre`, `central Manchester` -> `Manchester / City Centre`
- `Sale`, `Altrincham`, `Chorlton` связываются с borough
- даты и время приводятся к `Europe/London`
- цены приводятся к формату `£45–85`
- имена клубов и venues приводятся к утверждённому canonical form
- все нарицательные формулировки переводятся в нормальный русский
- служебный Spanglish и внутренний редакторский жаргон удаляются ещё до сборки выпуска
- для типовых терминов используется словарь замен:
  - `borough` -> `район` / `часть Greater Manchester`
  - `planned engineering works` -> `плановые ремонтные работы`
  - `civic event` -> `городское мероприятие`
  - `art event` -> `вечер рисования` / `выставка` / `творческая встреча` по факту
- допустимые рабочие слова:
  - `HR`
  - `digital-команды`
  - `founders`
  - `preview`
  - `report`
  - `alerts`

Нормализатор также ставит теги:

- `horizon_24h`
- `horizon_7d`
- `future_announcement`
- `ticket_radar`
- `transport`
- `football`
- `opening`
- `planning`
- `tech_business`
- `district_radar`

## 10. Verification layer

Это критический шаг. Он защищает от красивого, но ненадёжного выпуска.

Для каждого candidate:

1. Проверить, что ссылка реальна и открывается
2. Проверить наличие первичного источника
3. Проверить, какие exact details подтверждены
4. Определить, нужна ли вторая верификация

Правила верификации:

- `Primary source present` -> можно публиковать при достаточной полноте
- `Only secondary source` -> публиковать только если это не high-risk detail
- `Signal-only only` -> не публиковать
- `Search / amp / reseller URL only` -> не публиковать, пока не найден canonical source

Обязательно двойное подтверждение, если речь о:

- transport disruption
- sale timing
- price
- safety / protest / incident
- NHS disruption
- match score / next fixture
- opening date

Если второго подтверждения нет, но primary есть, допускается публикация с мягкой формулировкой.

## 11. Deduplication layer

Дедуп делается не по URL, а по смыслу факта.

Нужен `fact_fingerprint`, который строится из:

- normalized title
- event type
- entity set
- venue
- date
- borough

Типы дубликатов:

- exact duplicate
- source duplicate
- phase update
- follow-up
- stale repeat

Правила:

- exact duplicate -> удалить
- source duplicate -> слить ссылки
- phase update -> обновить существующий факт
- follow-up -> оставить как новый item, но связать с предыдущим
- stale repeat -> не брать в выпуск

Cross-issue dedupe:

- факт, уже опубликованный в выпуске, считается закрытым на `7 дней`
- published item обязан записываться в state сразу после успешной отправки выпуска
- минимум state:
  - `fact_fingerprint`
  - `canonical_headline`
  - `primary_entity`
  - `event_type`
  - `location`
  - `event_date`
  - `primary_block`
  - `published_at`
  - `phase`
- исключения:
  - новая цена
  - новый статус
  - новая дата
  - cancellation
  - extra venue
  - workaround update
  - новый этап продаж
- правило соседних выпусков:
  - `next_day_repeat = false` по умолчанию
  - `next_day_repeat = true` только если candidate имеет один из флагов:
    - `new_phase`
    - `new_deadline`
    - `today_effect`
    - `sale_state_changed`
    - `entered_imminent_window`
    - `new_service_impact`
- если candidate совпал с fingerprint вчерашнего выпуска и не получил один из этих флагов, он удаляется до ranking

## 12. Ranking layer

Каждому candidate присваивается score по нескольким осям:

- `impact`
  - city-wide
  - borough-wide
  - district-only
- `urgency`
  - today
  - this week
  - future only
- `actionability`
  - купить билет
  - изменить маршрут
  - учесть ограничения
  - пойти на событие
- `reliability`
  - primary
  - secondary
  - mixed
- `novelty`
  - новый факт
  - follow-up
  - stale
- `personal relevance`
  - football scope
  - openings
  - tech cluster
  - airport
  - neighbourhood life

Простая practical модель score:

- impact: 0–5
- urgency: 0–5
- actionability: 0–5
- reliability: 0–5
- novelty: 0–3
- personal relevance: 0–4

Итог:

- `>= 18` -> strong candidate
- `12–17` -> secondary candidate
- `< 12` -> deep read or discard

Дополнительные boost-правила:

- подтверждённая история `NHS / public services` получает приоритетный boost и не должна теряться ниже soft lifestyle items
- новый `future announcement` уровня большого концерта, мегасобытия, фестиваля или city-wide culture/sports event получает отдельный boost, даже если событие далеко по дате

Penalty-правила:

- candidate с флагом `stale_repeat` получает сильный штраф
- candidate, который уже был в предыдущем выпуске и не имеет `new_phase`, должен по умолчанию выбывать
- candidate, который уже раскрыт в одном блоке текущего выпуска, не может повторно попасть в `district_radar`
- candidate с плохой explainability:
  - непонятно что это
  - непонятно где это
  - непонятно на что влияет
  должен терять баллы до удаления

Дополнительное правило для блока `last_24h`:

- ranker не имеет права обнулить городской news layer только потому, что новости не дотягивают до top-story уровня
- для `last_24h` действует отдельный floor:
  - если media-layer + official news layer дали `10+` candidates за окно
  - в блок должно попасть минимум `3` пункта
- если в блок прошло меньше `3` пунктов при `10+` candidates:
  - это считается `ranking failure`
  - выпуск не должен считаться полностью валидным без ручной проверки или debug-note

## 13. Section assignment

После ranking каждый факт попадает в один основной блок:

- short_actions_candidate
- today_focus
- transport
- last_24h
- next_7_days
- future_announcements
- ticket_radar
- openings
- tech_business
- football
- district_radar
- deep_read

Правила:

- один факт имеет один primary block
- в `Коротко` он может появиться только как действие, а не как повтор полной новости
- основной текст живёт только в одном месте
- `district_radar` получает только facts с новым geographic angle
- если система не может доказать новый geographic angle, `district_radar` выключается полностью
- блок `NHS / public services` не создаётся, если его единственный смысловой факт уже полностью раскрыт в `today_focus`
- блок `last_24h` не может строиться только из official sources, если media-layer успешно отработал
- для `last_24h` assembler обязан рассматривать минимум такие источники:
  - Manchester Evening News
  - BBC Manchester
  - ManchesterWorld
  - The Mill
  - Prolific North
  - ITV News Granada
  - The Manc
- если найден candidate с тегом `future_announcement`, он не должен растворяться в `next_7_days` или `ticket_radar` по умолчанию
- для него сначала проверяется отдельный блок `future_announcements`

## 14. Ticket Radar lifecycle

Для ticket-led items хранить:

- announcement_date
- presale_start
- presale_end
- general_sale_start
- sale_status

Переходы:

- `future_announcement` -> `T3`
- `14 days before sale` -> `T2`
- `72 hours before sale` -> `T1`
- `presale live` -> `T1 active`
- `general sale first 24h` -> `T1 active`
- `sold out / stable availability` -> `sale update`
- потом archive

## 15. Assembly layer

Assembler получает ranked facts и строит выпуск строго по guide:

1. Шапка
2. Коротко
3. Что важно сегодня
4. Транспорт и сбои
5. Что произошло за 24 часа
6. Что важно в ближайшие 7 дней
7. Дальние анонсы
8. Ticket Radar
9. Открытия и еда
10. IT и бизнес
11. Футбол
12. Радар по районам

Assembler обязан:

- соблюдать лимиты блоков
- вставлять empty states
- использовать короткие ссылки
- не использовать длинные URL в теле
- не допускать фактов без раскрытия ниже блока `Коротко`
- не ставить один и тот же item в два содержательных блока
- при первом упоминании каждой сущности добавлять краткий контекст:
  - что это за событие
  - что это за человек / артист / организация
  - что это за место
  - если это спектакль, выставка, компания, юридический термин или полицейское полномочие, добавлять не общее, а прикладное пояснение
  - примеры:
    - `Private Lives — комедия Ноэла Кауарда`
    - `Canopius — страховая компания`
    - `Section 34 — право полиции разгонять скопления и требовать покинуть зону`
- не писать внутреннюю методологию вместо факта
- не выпускать англо-русскую мешанину
- не использовать поисковые страницы, `/amp/` и reseller-ссылки как primary
- в погоде всегда указывать цифры температуры
- все обязательные блоки из style guide должны физически присутствовать в финальном выпуске
- не выпускать `last_24h` в форме `без значимых изменений`, если во входных данных были городские news candidates
- не прятать `future_announcement` в weekly block, если это новый большой анонс с горизонтом дальше `7 дней`
- если в weekly weather есть скачок температуры `>= 5°C`, добавить короткий trend note
- сделать отдельный `carry-over pass` перед сборкой:
  - удалить всё, что уже было вчера без новой фазы
  - оставить только новые факты, новые фазы и пункты, вошедшие в более срочное окно
- каждый оставленный carry-over обязан получить label:
  - `Актуально сегодня`
  - `Продолжается`
  - `Сегодня вступает в силу`
- carry-over без label запрещён
- сделать отдельный `section collision pass`:
  - если один смысловой факт попал в несколько блоков, оставить его только в primary block
  - если collision затрагивает `district_radar`, удалить районный дубль первым
- сделать отдельный `self-contained wording pass`:
  - первая фраза каждого item должна быть понятна без знания вчерашнего выпуска
  - item не может начинаться с нерасшифрованного `после инцидента`, `после нападения`, `по этому делу`, `по этой истории`
  - если такой паттерн найден, item возвращается на переписывание
- low-signal sections не печатать пустыми:
  - `openings`
  - `tech_business`
  - `district_radar`
  - если в них нет нового подтверждённого факта, блок скрывается

## 16. Validation layer перед отправкой

Перед Delivery нужен отдельный post-assembly validator.

Он обязан проверить:

- нет bot prefixes:
  - `> MNewsDigest:`
  - подобных служебных префиксов
- нет плейсхолдеров:
  - `[link]`
  - `[todo]`
  - `[source]`
- нет двойной вставки одного и того же блока или всего выпуска
- нет англо-русской мешанины и необъяснённых английских слов из style guide
- нет фраз от первого лица и голоса сборщика:
  - `я не вижу`
  - `я не нашёл`
  - `у меня нет подтверждения`
  - `главный реальный шанс`
  - `для вашего слоя`
- нет секций, которые повторяют один и тот же факт без новой информации
- блок `Коротко` не дублирует дословно развёрнутый блок ниже
- `За 24 часа` не содержит пунктов, которые предполагают знание вчерашней версии:
  - если reader без контекста не понимает, что случилось, validator должен завалить выпуск
- блок `last_24h` содержит:
  - минимум `3` пункта
  - либо явную техническую пометку, что сбор media-layer провалился
- первое упоминание каждой сущности имеет контекст
- первое упоминание нетривиальной сущности имеет прикладное пояснение:
  - спектакль -> жанр
  - выставка -> тема
  - компания -> сектор
  - юридический термин -> человеческая расшифровка
- transport items написаны человеческим языком:
  - `без значимых сбоев`
  - `частичные ограничения`
  - `замены поездов автобусами`
  - а не инженерным жаргоном
- sources labels человекочитаемы:
  - `mancity.com`
  - `Salford Council`
  - `National Rail`
  - а не `MCFC MUFC GMP` подряд
- weather line содержит цифры
- links canonical:
  - без `/amp/`
  - без search pages
  - без reseller URLs как primary
- если у события есть official event page, validator запрещает подставлять:
  - homepage площадки
  - secondary listing
  - aggregator page
- присутствуют все обязательные блоки:
  - шапка
  - Коротко
  - транспорт
  - что важно сегодня
  - 24 часа
  - 7 дней
  - Ticket Radar
- не печатать пустые low-signal blocks:
  - `Открытия и еда`
  - `IT и бизнес`
  - `Радар по районам`
  - если в них нет нового факта
- если есть candidates с тегом `future_announcement`, присутствует блок `Дальние анонсы`
- если есть candidates с тегом `public_services`, validator проверяет, что они не были вытеснены soft items без причины
- если есть active public-services disruption c датой, покрывающей сегодняшний день, validator требует:
  - либо включить его в выпуск
  - либо явно зафиксировать причину исключения в debug output
- нет next-day repeats без `new_phase`
- нет explainability failures:
  - читателю понятно, что это
  - где это
  - когда это
  - на что это влияет

## 17. Broad Scan Matrix

Перед финальной сборкой система обязана пройти не только по transport + football + tickets, но и по минимальной матрице категорий.

Ежедневный обязательный scan:

- `media / city news`
  - MEN
  - BBC Manchester
  - ManchesterWorld
  - ITV Granada
  - The Mill
  - The Manc
- `transport`
  - TfGM
  - National Rail
  - relevant council roads pages
- `police / incidents / courts`
  - GMP newsroom
- `public services`
  - NHS / trust / ITV / council if active disruption exists
- `culture weekly`
  - theatre
  - exhibitions / museums
  - major what's-on
- `venues / tickets`
  - Co-op Live
  - AO Arena
  - Factory International
  - club / stadium / other official venue pages when relevant
- `food / openings`
  - at least one strong openings source
- `football`
  - MUFC
  - MCFC
  - Salford City

Если хоть одна из этих категорий не была проверена, выпуск не должен считаться fully complete.

Если хотя бы один из этих тестов провален:

- выпуск не отправляется в основной канал
- выпуск уходит только в debug / test output
- run log получает причину блокировки

## 18. Delivery layer

### Вариант A: Telegram

Плюсы:

- лучший канал для утреннего краткого чтения
- push-уведомление
- высокая вероятность, что вы реально это увидите в 08:00
- удобно для коротких ссылок и компактного digest

Минусы:

- длинные выпуски читать тяжелее
- formatting limits
- сложнее архив и поиск по старым выпускам, чем в email

### Рекомендация

Для первого этапа выбираем только:

- `Telegram` как основной и единственный канал

Email пока не делаем, чтобы не распылять сложность.

## 19. Delivery format recommendation

Я бы предложил одну модель для MVP:

- `Telegram AM Brief`
  - компактный, но полный выпуск
  - Коротко
  - что важно сегодня
  - transport
  - 24h
  - 7d
  - Ticket Radar
  - openings
  - football

Архив храним не в email, а в markdown-файлах внутри репозитория.

## 20. Scheduler и надёжность запуска

Для daily production на GitHub Actions нужны:

- timezone-aware gating logic
- retry policy
- duplicate-send protection
- missed-run recovery

Правила:

- GitHub официально предупреждает, что scheduled workflows могут задерживаться, а при высокой нагрузке некоторые queued jobs могут быть dropped, особенно около начала часа
- поэтому workflow нельзя ставить ровно на `08:00`
- workflow запускается в нескольких точках по `UTC`
- внутри job проверяется текущее локальное время в `Europe/London`
- если локальное время не попадает в окно отправки, job завершается без отправки
- если выпуск за текущую дату уже был отправлен, job завершается без повторной отправки
- рекомендуемое окно доставки: `08:00 +/- 15 минут Europe/London`
- если выпуск не собрался к `08:00`, система шлёт failure alert в служебный канал
- если запуск пропущен, можно сделать delayed issue с явной пометкой

## 21. Monitoring и алерты

Нужно мониторить:

- успешность fetch
- parser failures
- empty high-priority sources
- broken links
- unusually low item count
- duplicate send risk

Минимальные alert conditions:

- не загрузился primary source
- Ticket Radar пуст уже 7+ дней подряд
- transport section empty из-за parser failure
- итоговый выпуск < 8 сигналов
- выпуск не доставлен

## 22. Что ещё нужно учесть

Обычно на этом этапе забывают 10 вещей:

- daylight saving / переходы British Summer Time
- broken selectors при редизайне сайтов
- paywalls
- anti-bot на ticketing pages
- дубль одной новости в 5 медиа
- stale items из вчерашнего выпуска
- weekend / bank holiday patterns
- Thursday/Friday special weekend emphasis
- manual override на важный день
- архив выпусков и возможность пересобрать выпуск за дату
- очистку Telegram-артефактов и дублированных сообщений перед отправкой

## 23. Рекомендуемый delivery roadmap

### Этап 1

- собрать registry
- поднять ingestion
- запустить daily dry-run
- сохранять raw data и candidates
- выпуск пока писать в файл и Telegram test chat

### Этап 2

- включить dedupe
- включить ranking
- собрать production-format digest
- доставлять в основной Telegram

### Этап 3

- добавить email archive
- добавить Thursday/Friday enhanced weekend mode
- добавить run dashboard

## 24. Технический вывод

Да, это можно реализовать так, чтобы оно работало ежедневно без вас и при выключенном компьютере.

Для бесплатного этапа внешний automation stack будет таким:

- GitHub Actions scheduler
- state files в private repo
- parsers
- verification
- Telegram bot

Позже это можно перенести на VPS без смены логики пайплайна.
