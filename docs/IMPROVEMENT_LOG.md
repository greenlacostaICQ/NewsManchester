# Журнал доработок (Improvement Log)

Назначение: чтобы через месяц можно было ответить на вопрос **«почему доработка X не сработала и где была ошибка»**, не перечитывая весь код и логи.

Формат — на основе **ADR** (Architecture Decision Records, Michael Nygard, 2011: контекст → решение → последствия) с добавленным полем **ПРОВЕРКА** (чего в классическом ADR нет, а нам критично, потому что одни и те же доработки переделывались по 5-8 раз).

## Правила ведения
1. Одна запись = одна доработка. Нумерация сквозная (0001, 0002, …), не переиспользуется.
2. Запись **неизменяема** после статуса `ПРОВЕРЕНО`. Если переделываем — заводим новую запись со ссылкой `заменяет #NNNN` / `заменено #NNNN`.
3. Поле **ПРОВЕРКА** заполняется ТОЛЬКО по факту реального прогона, с числами из отчётов (`data/state/*.json`). Без прогона статус не может быть `ПРОВЕРЕНО-работает`.
4. Если доработка не сработала — обязательно поле **Где была ошибка** с привязкой к коду (`файл:строка`). Это и есть главная ценность журнала.
5. Альтернативы, которые отвергли, фиксируем — чтобы не предлагать их повторно.

## Статусы
`предложено` → `принято` → `внедрено` → `ПРОВЕРЕНО-работает` | `ПРОВЕРЕНО-НЕ-работает` → (если не работает) `заменено #NNNN`

## Шаблон записи
```
### NNNN — <короткое имя> — <YYYY-MM-DD>
- Статус: <см. выше>
- Проблема: <что болит, с числами/доказательством>
- Причина (корень): <почему именно, по коду/логам — файл:строка>
- Решение: <что и как меняем>
- Почему так (отвергнутые альтернативы): <...>
- Ожидаемый эффект и метрика проверки: <конкретное число в конкретном отчёте>
- Файлы/места: <path:line>
- ПРОВЕРКА (после прогона): <дата, числа, сработало ли>
- Где была ошибка (если не сработало): <файл:строка + объяснение>
```

---

# Записи

### 0001 — Бэкфилл новостных блоков не видит выброшенные новости (два разных пула резерва) — 2026-06-27
- Статус: предложено
- Проблема: «Свежие новости» вышли 3 при минимуме 6; «Городской радар» 3 при 5. В резерве лежали сильные новости (Burnham — оценка 185, взрыв в Cheetham Hill — 140, Galloway в мэры — 157), но в выпуск не попали. Бэкфилл переделывался 5-8 раз и не помогал.
- Причина (корень): **два непересекающихся пула резерва.** `_apply_post_board_translation_cut` (`llm_rewrite.py:1262-1264`) помечает всё, что не влезло в доску-42, как `public_reserve=False, backup_pool_only=True`. А `_same_section_reserve_line` (`editor.py:666`) добирает ТОЛЬКО из `public_reserve==True and backup_pool_only==False`. Пересечение пустое → бэкфилл физически не видит выброшенные новости. Все прошлые «починки бэкфилла» правили механику добора, но ни одна не трогала рассинхрон флагов пула.
- Решение: единый guaranteed-coverage добор (паттерн «fallback-пул с полным покрытием» из recsys): после рендера, если блок < минимума — детерминированно взять сильнейшие из ЕДИНОГО пула резерва этого блока (включая `backup_pool_only`), обогатить, перевести, вставить. Один источник правды по пулу.
- Почему так (отвергли): «просто поднять floor доски» — не помогает, т.к. теряются ниже по конвейеру (см. #0002); «чинить LLM-editor backfill» — отвергнуто, он LLM-gated и падает по лимиту (см. #0003).
- Ожидаемый эффект и метрика: `section_underflow` пустой; Свежие ≥6, Радар ≥5 в `pre_send_quality_report.product_completeness.section_counts`.
- Файлы/места: `llm_rewrite.py:1262`, `editor.py:646-705`, `editor.py:1033-1055`.
- ПРОВЕРКА: —
- Где была ошибка: —

### 0002 — Отобранные новости теряются перед рендером (drop вместо обогащения) — 2026-06-27
- Статус: предложено
- Проблема: для «Свежих» доска отобрала 7 (floor=7 работает), но отрисовано 3. По всему выпуску `missing_draft_line: 92`, `held_for_editorial_quality: 89` — карточки отобраны, но без готовой строки тихо выпадают (напр. футбол «EFL Trophy»: selected+show, draft_line пустой → дроп).
- Причина (корень): нет ветки «обогатить-и-переписать» для selected-карточки без строки/со слабой строкой — она просто считается потерянной (`writer.py:1466` возвращает `dropped_missing_draft_line`). Инфраструктура обогащения существует (`_candidate_full_evidence_text`, `_final_replacement_line`), но на этот путь не заведена.
- Решение: selected-карточка без валидной строки → принудительно через обогащение (рефетч полного текста статьи → перепись), и только при полном провале — детерминированная строка из заголовка+сути. Обогащение в приоритете над «собрать из говна».
- Почему так (отвергли): «детерминированно собрать строку» как основной путь — отвергнуто (owner: «почему собирать из говна?»). Только как последний резерв.
- Ожидаемый эффект и метрика: `missing_draft_line` → ~0 в `rewrite_inventory.totals`.
- Файлы/места: `writer.py:1466,5880`, `editor.py:681-705`.
- ПРОВЕРКА: —
- Где была ошибка: —

### 0003 — Финальный редактор падает целиком по лимиту OpenAI (нет backoff) — 2026-06-27
- Статус: предложено
- Проблема: второй заход редактора вернул 0 из 61 строки; 68 строк выпуска без вычитки.
- Причина (корень): лимит gpt-4o 30 000 токенов/мин (tier-1). Пачки летят параллельно (`editor.py:614`), второй заход запросил ~74k за минуту → 429 на всех трёх пачках. Нет повтора при 429 (`editor.py:619-624` берёт `future.result()` без retry), хотя API отдаёт «retry in 49s».
- Решение: (а) включить кэш промптов (кэш-токены меньше считаются против лимита); (б) backoff-повтор на 429; (в) слать пачки последовательно/с паузой под лимит.
- Ожидаемый эффект и метрика: `final_editor_report…rounds[*].failed_batches == 0`, `coverage_complete == true`.
- Файлы/места: `editor.py:603-643`.
- ПРОВЕРКА: —
- Где была ошибка: —

### 0004 — Кэш промптов не используется вообще — 2026-06-27
- Статус: предложено
- Проблема: `cache_hit_ratio: 0.0`; стоимость выпуска $0.346 (2.5× от среднего); переполнение лимита редактора.
- Причина (корень): запросы не структурированы под кэш (статичная часть не вынесена в начало, нет стабильного cache-key).
- Решение: статичные рубрики/инструкции — в начало промпта; стабильный `prompt_cache_key`. OpenAI и DeepSeek кэшируют автоматически при общем префиксе.
- Ожидаемый эффект и метрика: `cache_hit_ratio > 0` в `release_report.cost_summary`; стоимость ↓.
- Файлы/места: промпты в `editor.py:51`, `curator.py`, `llm_rewrite.py`.
- ПРОВЕРКА: —
- Где была ошибка: —

### 0005 — «Выходные в GM»: источник афиш отдаёт страницы-списки, не события — 2026-06-27
- Статус: предложено
- Проблема: из 134 кандидатов на блок прошло 10, выкинуто 110. Реальные события (Ai Weiwei, Rochdale/Bolton festivals) собраны, но выпали на гейте качества событий.
- Причина (корень): Visit Manchester скрейпится на уровне индексных страниц («What's on at Co-op Live», «What's On This Week») — это меню, не события, без даты/цены/брони. trafilatura (добавлена `extract.py:698`) вытаскивает текст СТАТЕЙ, но не структуру события из листинга — поэтому не помогла (другой слой).
- Решение: для афиш-агрегаторов ходить со страницы-списка внутрь на карточку события; опереться на структурные источники (Skiddle/DMN/JSON-LD дали те 10, что прошли); добавить недостающие источники (фестивали/выставки/городские).
- Ожидаемый эффект и метрика: «Выходные» ≥6 событий с датой/местом; Ai Weiwei/фестивали проходят (проверять поimenно).
- Файлы/места: `collector/extract.py`, источники афиш в конфиге коллектора.
- ПРОВЕРКА: —
- Где была ошибка: —

### 0006 — Lead («Главная история дня») исчезает из видимого HTML — 2026-06-27 (нашёл Codex, я пропустил)
- Статус: предложено
- Проблема: кандидат «Greater Manchester's mayoral election explained» = `is_lead=True, publish_plan_status=must_show, draft_line` непустой — но в `current_digest.html` **нет ни блока «Главная история дня», ни этой строки** (`grep "Главная история дня" = 0`). Release при этом `lost_leads=[]`.
- Причина (корень): **нет договора «выбрали → видно в HTML».** Lead помечен `is_lead`, но `primary_block=city_watch`, и в видимом Радаре его нет (там стадион/Metrolink/Lib Dems — его съел кросс-секционный dedup мэрских сюжетов). `must_show`/lead проверяются по счётчикам writer'а и rendered-fingerprints, а не по финальному HTML. Никто не сверяет видимый выпуск с publish_plan.
- Решение: после build парсить финальный HTML и сверять с publish_plan: lead присутствует как отдельный блок, must_show отрисованы, поблочные минимумы, caps. Нарушение → recovery loop, не warning.
- Ожидаемый эффект и метрика: в HTML ровно один lead-блок; `must_show_missing=0`, считается по HTML.
- Файлы/места: `release.py`, `editor.py:1137,1286`, build HTML.
- ПРОВЕРКА: —
- Где была ошибка: —

### 0007 — LLM CV-match не запускается ни на одном показанном проф-событии — 2026-06-27 (Codex прав, я был неправ)
- Статус: предложено
- Проблема: я утверждал «CV работает 34/42». На деле у всех 6 показанных проф-событий `professional_llm_match=NO, match_status=needs_llm_cv_match`. Их отобрал ДЕТЕРМИНИРОВАННЫЙ keyword-скорер (`professional_event_match_v1`: MD Future fit=100, остальные 28-52). Реальная модель CV (gpt-4o-mini) применилась к 0.
- Причина (корень): `_professional_event_has_minimum_facts` (`professional_events.py:267`) требует `event.date` + venue/online + url; у проф-событий дата не извлекается на этапе enrichment → eligible=1 из 42 → LLM CV-матч не на чем запускать. Цепочка fact-extraction → eligibility → CV-match разорвана на первом звене.
- Решение: извлекать факты события (дата/место/free/booking) ДО матча; затем LLM CV-матч управляет публикацией (go/consider/skip); skip не виден.
- Ожидаемый эффект и метрика: `eligible` покрывает все fact-complete проф-события (не 1); `applied>0`; в отчёте видно «проверено N → подошло M моделью gpt-4o-mini».
- Файлы/места: `professional_events.py:267,333`.
- ПРОВЕРКА: —
- Где была ошибка: —

### 0008 — «84 источника дали 0» — НЕ мёртвые, items отфильтрованы (поправка моего неверного вывода) — 2026-06-27
- Статус: справочная (поправка)
- Проблема: я написал «84 мёртвых/бесполезных источника, впустую тратим время». Owner верно возразил.
- Факт: `source_health_counts: fetch_failed=0, all_rejected=84, idle_no_items=10`. Источники **скачались нормально**, но все их items отсеяны фильтрами/гейтами. Не мёртвые. Это может быть и легитимный off-topic, и пере-фильтрация (как Visit Manchester: страница ok, события не извлеклись).
- Вывод: чинить не «источник», а loss-class: для каждого zero-yield источника писать, на каком шаге и почему отсеяно (parser/filter/dedupe/selection/writer/editor/cap), и только тогда решать — это off-topic или пере-фильтрация.
- Файлы/места: `source_status` в release_report; per-source loss trace.

### 0009 — «Выходные» ломают ТРИ разных механизма, не один — 2026-06-27 (уточнение #0005)
- Статус: предложено (расширяет #0005)
- Проблема: owner назвал пропавшие Armed Forces Day, Didsbury Arts, Creative Bazaar.
- Факт по каждому: (а) **Armed Forces Day, Creative Bazaar, Street Culture Market — НЕ собраны вообще** (0 в candidates) — дыра в источниках, не экстракции. (б) **Didsbury Arts — собран, но убит подавлением повторов** («Повтор темы без новой фазы: уже был 2026-06-26») — многодневный фестиваль 27.06–5.07 рубится как дубль. (в) ещё одна копия Didsbury убита **неверной датой** (извлекло `2026-05-16`, в прошлом). (г) Visit Manchester — листинги вместо событий (#0005).
- Решение: (а) добавить источники городских событий (council/what's-on c Armed Forces Day, Didsbury Arts, Creative Bazaar); (б) multi-day событие = одно длящееся, не повтор; (в) починить извлечение дат; (г) #0005.
- Файлы/места: источники коллектора; repeat-logic `editorial_contracts.py:1525`; date-extraction в event_extraction.
- ПРОВЕРКА: —

### 0010 — География билетов: London-площадки попадают в GM-радар — 2026-06-27 (Codex)
- Статус: предложено
- Проблема: Kasabian/Biffy Clyro (Finsbury Park, London) отрисованы в «Билеты / Ticket Radar» (GM-блок).
- Причина (корень): `_looks_like_local_gm_venue` (`candidate_validator.py:174,185`) ищет GM-токены во всём haystack (включая source/text), а не строит строгий scope площадки → лондонское шоу с упоминанием GM-токена перерутится в GM.
- Решение: venue-scope `GM/nearby/outside/unknown` из площадки+города+региона источника; GM-копирайт только при scope=GM; A-tier outside-GM остаётся виден, но с городом.
- Файлы/места: `candidate_validator.py:174,185`.
- ПРОВЕРКА: —

### 0011 — KEYSTONE: гейты измеряют writer_report, а не финальный HTML (RC1) — 2026-06-27 (Codex, проверено мной)
- Статус: предложено — корень всего класса «выбрали, но не видно»
- Проблема: writer сообщил {lead:1, transport:9, today:2, outside-GM:25}; в `current_digest.html` реально {0, 7, 1, 17}. Все пороги (floors, ticket-dominance, underflow) считаются на до-редакторских числах → тюнинг порогов структурно не меняет HTML.
- Причина (корень, проверено по коду): `pre_send_quality_judge.py:190` — `section_counts = dict(writer_report.get("section_counts"))`, HTML (`digest_lines`) только fallback `if not section_counts`. Между writer и HTML стоит editor (block-actions `trim`, cross-section dedup) — судья этих мутаций не видит. Writer-counts строятся `writer.py:6201` ДО editor.
- Решение: F1 (см. backlog) — reconciler в `release.py` ДО промоута draft→outgoing: парсить draft-HTML, сверять с publish_plan, при нарушении → bounded recovery actuator (не warning) → ре-валидировать → промоут. Never-block = ремонт до отправки, при неустранимом — отправка с явной human-readable причиной, инварианты сохранены.
- 5 поправок owner (приняты): (1) пул резерва = recoverable (прошёл dedupe/validator/curator), НЕ quarantine/manual-hold/rejected/stale/non-GM/duplicate/low-trust; (2) «A-tier не исчезает» = из inventory/tracking, не из утреннего digest — в digest A-tier кэпируется, излишек в отдельный ticket-report; (3) reconciler до промоута в outgoing, не чинить уже промоутнутое; (4) hard product invariant + bounded recovery + честная причина, не молчаливый блок; (5) prompt-cache: stable prefix + smaller batches + backoff + lower concurrency; конкретный cache-key проверить по API, не принимать как факт.
- Метрика проверки: на HTML — lead-блок=1; `writer_report.section_counts == пер-секционный счёт bullets в HTML` (контрольный ассерт); must_show_missing(HTML)=0.
- Файлы/места: `pre_send_quality_judge.py:190`, `release.py`, `editor.py:1017,1133`.
- ПРОВЕРКА: —

### 0012 — ВОЛНА 1 (S1–S5) внедрена — 2026-06-27
- Статус: внедрено (ветка `wave1-html-contract`), ПРОВЕРКА на следующем прогоне в проде
- Что сделано (один эпик, 5 коммитов, 664 теста зелёные):
  - **S1** единый recoverable-пул: `common.recoverable_reserve_eligible/is_recoverable_reserve`; board-overflow тегируется `recoverable_reserve` в 3 точках `llm_rewrite`; `editor:666` читает единый пул. Закрывает #0001.
  - **S2** редактор выживает под лимитом: `_editor_create_with_backoff` (429 «try again in Ns», capped), последовательная отправка (workers 3→1), батчи 90k→60k, честный `coverage_complete`. Закрывает #0003.
  - **S3** детерминированный чекер: меж-словный микс (обе стороны) + «английский артикль + кириллица» («On The линия»). Попутно вскрыт источник: глоссарий `line→линия`. Закрывает «On The линия».
  - **S4** visible-HTML контракт: `release_reconcile.reconcile_visible_html` — control-assertion (writer vs HTML), детект lead, bounded recovery (cap 8) из единого пула с обогащением, до промоута, guarded (never-block); судья считает по HTML (`:190`). Закрывает #0006, #0011, #0002 (частично — обогащение selected-без-строки идёт через `_same_section_reserve_line`).
  - **S5** лид: глобальный арбитраж по reader_value (`_arbitrate_global_lead`) вместо «первый в списке»; лид-блок обрабатывается первым в кросс-секционном dedup и не удаляется. Закрывает batch-local lead + dedup-смерть лида.
- ПРОВЕРКА (заполнить после первого прод-прогона ветки, всё мерить на `current_digest.html`): lead-блок ≥1 и не дублируется в Радаре; `must_show_missing(HTML)=0`; Fresh≥6/Today≥3/City≥5/Football≥2 или честная причина; `final_editor_report.rounds[*].failed_batches=0`, `coverage_complete=true`; `cache_hit_ratio>0`; нет лат+кир гибридов; `visible_contract_report.control_assertion.ok=true` (writer==HTML).
- Где была ошибка: — (пока не проверено на проде)

### 0013 — ВОЛНА 2 / W1: канонический модуль фактов события (RC3) — 2026-06-27
- Статус: внедрено (main), ПРОВЕРКА на следующем прод-прогоне
- Проблема: четыре блока (Weekend/Professional/Next-7/Repeat) ломались об один корень — пустая/неверная `event.date`. Профессиональный CV `eligible=1/42` (#0007); Didsbury Arts убит как «уже прошёл» (видна только дата старта) и как далёкий-recurring; «21 мая 2027» Jazz Festival в «Выходных»; venue-ложняк `venue="Practice"` из «…in Practice».
- Причина (корень): `event_extraction` отдавал `date` без диапазона/уверенности; `_professional_event_has_minimum_facts` требовал распарсенный venue (GM Chamber/Manchester Digital листинги дают дату+URL, но без строки venue → 6 дат отсеяны на venue-гейте, см. реальные данные); repeat-логика и weekend-гейт смотрели только на старт; bare «Month Day» с откатом в след. год давал фантомные far-future даты.
- Решение (один модуль, потребители его читают):
  - `event_extraction`: `_parse_date_details` → (start, **end**, text, **confidence**); cross-month диапазоны EN/RU («27 June – 5 July»); `free` bool; `date_confidence` high/medium/**low** (low = bare month/day с откатом в след. год → стрелочный ложняк); venue-эвристика требует мультислова/venue-ключевого слова (убит «in Practice»). Хелперы для потребителей: `event_is_far_future`, `event_is_multi_day`, `event_active_on`, `event_end_date`, `event_date_is_trustworthy`.
  - Professional (`professional_events.py:267`): venue больше не обязателен — место даёт borough / GM-токен / GM-источник (`_has_place_or_online`); дата обязана быть trustworthy (отсекает 2027-ложняк). На реальных данных `min_facts` 1→6.
  - Weekend (`candidate_validator.py:_demote_distant_weekend_event`): trustworthy far-future (>30 дн) уходит из блока ДО recurrence-короткого замыкания; low-confidence откат (recurring-рынок «5 April»→2027) не трогаем.
  - Repeat (`editorial_contracts.py:calendar_repeat_review`): «уже прошёл» считается по `date_end`, не по старту → многодневный фестиваль в разгаре = d0, не stale-repeat.
- Почему так (отвергли): deep-fetch карточки события из листинга — отдельная коллекторная способность, не детерминируемо-тестируемая офлайн; оставлено как consumer-follow-up (Ai Weiwei = ongoing-выставка без даты в листинге, всё ещё `is_event=False`).
- Ожидаемый эффект и метрика: `professional_llm_match.eligible > 1`; Didsbury получает диапазон и не падает в repeat/weekend; в Weekend нет дат > +30 дн.
- Файлы/места: `event_extraction.py`, `professional_events.py:267`, `candidate_validator.py:1694`, `editorial_contracts.py:1486`.
- ПРОВЕРКА: офлайн на `data/state/candidates.json` — professional `min_facts` 1→6, Manifesto(2027) conf=low отсеян, Didsbury 27 Jun–5 Jul (range, high). Прод — на следующем прогоне.
- Где была ошибка: —

### 0014 — ВОЛНА 2 / W3: авторитетный venue-scope резолвер (гео) — 2026-06-27
- Статус: внедрено (main), ПРОВЕРКА на следующем прод-прогоне
- Проблема: London-площадки в GM-радаре с копирайтом «в GM» (Kasabian/Biffy Clyro, Finsbury Park — оба реально в `ticket_radar`, #0010).
- Причина (корень): `_looks_like_local_gm_venue` (`candidate_validator.py:174`) искал GM-токен во ВСЁМ блобе (title+summary+lead+evidence+source_url) → лондонское шоу с упоминанием манчестерской даты тура перерутилось в GM. Плюс список outside-городов не знал именованных площадок («Finsbury Park» — не город).
- Решение: `resolve_venue_scope(candidate) → (scope, city)`, scope ∈ {GM, nearby, outside, unknown}, считается из venue+title+borough (НЕ из тела/URL). Приоритет: именованная non-GM площадка → nearby-город → outside-город → GM-площадка → unknown. `_reclassify_outside_gm_when_local_venue` промоутит в GM только при scope=GM; `_reclassify_gm_when_outside_venue` срабатывает на outside/nearby (теперь ловит Finsbury Park). Writer: «в GM» только при scope=GM, outside/nearby — никогда (`writer.py:2297`); unknown падает на block-routing (без churn). `venue_scope`/`venue_city` штампуются в validate (`_assign_venue_scope`) и читаются W2-кэпом.
- Почему так (отвергли): расширять блоб-скан — он и есть корень ложняка; большой реестр площадок — взяли маленький whitelist + консервативный unknown (Heaton Park = unknown, не врём).
- Ожидаемый эффект и метрика: 0 outside-scope площадок в `ticket_radar`; Kasabian/Biffy в `outside_gm_tickets` с city=London.
- Файлы/места: `candidate_validator.py:174,279,~217` (резолвер), `writer.py:2297`.
- ПРОВЕРКА: офлайн — Finsbury Park → ('outside','London'), reclassify → outside_gm_tickets; Co-op Live → GM (остаётся). Прод — на следующем прогоне.
- Где была ошибка: —

### 0015 — ВОЛНА 2 / W4: транспорт = контракт пассажирского эффекта — 2026-06-27
- Статус: внедрено (main), ПРОВЕРКА на следующем прод-прогоне
- Проблема: в блоке транспорта — Bury Interchange (стройка/£25m), Rawtenstall (ЧП у автостанции), и generic «проверьте TfGM» вместо/рядом с конкретными сбоями (P0 в PROJECT_STATE).
- Причина (корень): (а) правило негативное — `_should_route_to_transport` пускает TfGM-источники безусловно, нет позитивного требования пассажирского эффекта; (б) `editor.py:1142,1218` подменял неремонтируемую КОНКРЕТНУЮ строку generic-статусом «сбоев нет» (`_transport_status_fallback_line`) — прямое нарушение «generic запрещён при наличии конкретного».
- Решение: (а) `_reroute_non_impact_transport` — решает по ЗАГОЛОВКУ: title-impact (closure/cancelled/no trains/buses replace/...) → транспорт; иначе infra/funding (£Nm/revamp/boost/...) → City Radar; иначе incident-у-узла (police/air ambulance/vandalism/...) → из транспорта. (б) editor: неремонтируемую строку только STRIP, никогда не подменять generic; generic-статус ставится ТОЛЬКО на пустой блок (честное «сбоев нет»), в т.ч. добавлен empty-check после 2-го раунда.
- Почему так (отвергли): blob-скан на impact — ложно-срабатывал (Bury имел «works» в теле); решаем по заголовку — он и есть суть карточки.
- Ожидаемый эффект и метрика: Bury→Городской радар; Rawtenstall→вне транспорта; Northern/bus-stop с конкретикой остаются; в HTML нет generic-строки рядом с конкретной.
- Файлы/места: `candidate_validator.py:1131` (`_reroute_non_impact_transport`), `editor.py:1141,1216` (strip+empty-check).
- ПРОВЕРКА: офлайн на 12 транспорт-кандидатах — Bury/Rawtenstall/heritage-vandalism уходят, 9 конкретных остаются. Прод — на следующем прогоне.
- Где была ошибка: —

### 0016 — ВОЛНА 2 / W5: классификатор позитив-evidence для рус-блока — 2026-06-27
- Статус: внедрено (main), ПРОВЕРКА на следующем прод-прогоне
- Проблема: в рус-блок попадают элементы «по идентичности источника» — англоязычный спектакль (Young Vic / Alexander Zeldin) как `russian_events` только потому, что листинг с Afisha London; Goran Bregovic от Kontramarka без языковой/аудиторной привязки.
- Причина (корень): только НЕГАТИВНЫЙ фильтр (`candidate_validator.py:525` — отсекал классику без сигнала), не было ПОЗИТИВНОГО требования доказательства; source label трактовался как доказательство.
- Решение: `classify_russian_evidence` — strong-сигналы (доказательство): кириллица в тексте СОБЫТИЯ (бренд источника вырезается, чтобы повтор «Афиша Лондон» не считался), либо явная фраза «in Russian / на русском / українською». weak: промоутер/источник, ru/ua-URL — сами по себе НЕ доказательство. `_require_russian_positive_evidence` дропает рус-кандидата без strong-сигнала и штампует `russian_evidence` на всех (видимые показывают evidence).
- Почему так (отвергли): доверять source label — это и есть корень; кириллица только в бренде — вырезаем бренд перед проверкой.
- Ожидаемый эффект и метрика: 0 рус-items без `russian_evidence.has_evidence`; англо-Afisha и Goran/Kontramarka уходят.
- Файлы/места: `candidate_validator.py:602` (классификатор+gate), wired после `russian_event_classifier`.
- ПРОВЕРКА: офлайн на 20 рус-кандидатах — 15 keep (все со strong-evidence), 5 drop (Goran + англо-Afisha/индекс-страницы). Прод — на следующем прогоне.
- Где была ошибка: —

### 0017 — ВОЛНА 2 / W2: pre-writer balance + payload cap (RC4) — 2026-06-27
- Статус: внедрено (main), ПРОВЕРКА на следующем прод-прогоне
- Проблема: outside-GM (565→143, ВСЁ A-tier) давит core; editor ловит 429 (RC4). Кэп холостой — доказано: blanket A-tier exemption + outside-GM весь A-tier.
- Причина (корень): `_is_a_tier_ticket` делал A-tier exempt от ВСЕХ кэпов в обоих блоках (`writer.py:1631,1669`), а outside-GM весь A-tier → ни секционный, ни глобальный кэп не считали его → секция росла без границ.
- Решение: `_is_budget_exempt_a_tier` — A-tier exempt от кэпа ТОЛЬКО при `venue_scope ∈ {GM, nearby}` (использует резолвер W3). Outside/unknown A-tier теперь СЧИТАЕТСЯ → кэпируется tier-blind (SECTION_MAX_ITEMS + CORE_UNDERFLOW_TICKET_CAPS снова кусают). «Worth-showing» hold (`:1430`) на `_is_a_tier_ticket` оставлен — outside A-tier по-прежнему показывается, но топ-N. Излишек outside A-tier → `ticket_inventory` репорт + флаг `ticket_inventory_held` (трекается, не дроп) — #0011 п.2.
- Почему так (отвергли): отдельный кэп в llm_rewrite-board до writer — не нужен: после починки exemption рендер-кэп реально кусает, в editor уходит меньше строк → меньше токенов (вместе с S2 backoff даёт editor ≤ TPM). `missing_draft_line` в editor НЕ идёт — дропается/обогащается на writer (S4 #0002), в секции редактора не попадает.
- Ожидаемый эффект и метрика: outside-GM кэпируется (на данных: 503/565 теперь не-exempt — outside 170 + unknown 333; exempt только nearby 39 + GM 23); `release_report…ticket_inventory.outside_gm_a_tier_held_count` ≥0; tickets(HTML) ≤ max(6,core).
- Файлы/места: `writer.py:1594` (`_is_budget_exempt_a_tier`), `:1631,:1672` (budget-сайты), `:6353` (inventory-трекинг), report `ticket_inventory`.
- ПРОВЕРКА: офлайн — outside/unknown A-tier не exempt, GM/nearby exempt; unit: 8 outside A-tier при cap 6 → 6 (было 8). Прод — на следующем прогоне.
- Где была ошибка: —

### 0018 — ВОЛНА 3 / W9-fix: on-sale окно не читает дату СОБЫТИЯ — 2026-06-27
- Статус: внедрено (main c7c15e3), runtime synced; ПРОВЕРКА офлайн (unit), прод на следующем прогоне
- Проблема: `onsale_datetime_from_blob` на тексте «on sale soon. Event date 20 August 2026» возвращал onsale=2026-08-20 (дату СОБЫТИЯ) → в выпуск могло уйти «в продаже с 20 августа» = выдуманный факт (reader-facing).
- Причина (корень): окно `[start-8 : end+56]` без границы предложения перепрыгивало «soon.» и хватало следующую дату. Конкурентная правка добавила sentence-bound, но её marker-reject был МЁРТВЫМ кодом: `window.find(parsed.start[:10])` искал ISO-строку «2026-08-20» в человеческом тексте «20 August 2026» → всегда -1 → guard не срабатывал; вариант с запятой/тире (`,`/`—` не входят в `[.!?;\n]`) протекал.
- Решение: окно режется по границе клаузы (`[.!?;\n]`) с обеих сторон + tail обрезается по event-date маркеру (`event date|date of|концерт|show|festival|событи|event`) на СЫРОМ тексте до парсинга — смещение даты не нужно, guard срабатывает независимо от ISO-формата. Нет даты в окне → None → newly_listed (факт не фабрикуется).
- Почему так (отвергли): parse-then-find-ISO (конкурентная версия) структурно не чинится — парсер не отдаёт позицию даты; cut-on-marker до парсинга надёжнее.
- Ожидаемый эффект и метрика: 0 onsale из event-date; «on sale 4 July» сохраняется (presale_soon).
- Файлы/места: `editorial_contracts.py:192`; тест `test_ticket_consolidation.py::TicketOnsaleFromBlobTest::test_event_date_after_onsale_phrase_is_not_read_as_sale_date` (период+запятая).
- ПРОВЕРКА: офлайн — owner-синтетика период→None, запятая→None; позитив «on sale 4 July»→presale_soon; полный `unittest discover` 714 OK. Прод — на следующем прогоне (в выпуске нет «в продаже с <event date>»).
- Где была ошибка: —

### 0019 — ВОЛНА 3 / W6-fix: не-отсуженные eligible держатся (held), не дропаются — 2026-06-27
- Статус: внедрено (main c7c15e3), runtime synced; ПРОВЕРКА офлайн (unit), прод на следующем прогоне
- Проблема: при cap / model-unavailable / model-failed professional-кандидаты С фактами выходили `include=False` БЕЗ `held_for_enrichment`, и `held N` в отчёте = 0 → выглядело как drop, восстановимость на следующий день терялась.
- Причина (корень): post-model sweep (`professional_events.py:386-400`) пропускает уже-`include=False`; pending-путь `_drop_pending_llm_candidates` (`:354`) выходил раньше и не ставил статус и не считался в held.
- Решение: `_drop_pending_llm_candidates` ставит `editorial_status="held_for_enrichment"`; в `apply_professional_event_llm_matches` pending-счёт (`dropped_not_sent_pending`+`dropped_pending`) влит в `held`. Genuine model-skip (путь rows, `:525+`) НЕ тронут — остаётся drop.
- Почему так (отвергли): считать held по статусу на всех кандидатах — риск пересчёта (candidate_validator тоже ставит held); локальная сумма по report-ключам точна и изолирована.
- Ожидаемый эффект и метрика: cap/no-key eligible → `include=False` + `held_for_enrichment`; `held N` ≠ 0 при pending; genuine skip не растит held.
- Файлы/места: `professional_events.py:354` (статус), `:400` (сумма); тест `test_professional_events.py::ProfessionalEventsTest::test_eligible_event_unevaluated_by_model_is_held_not_dropped`.
- ПРОВЕРКА: офлайн — eligible + no-route → `held_for_enrichment`, summary «held 1»; sibling no-facts тест зелёный; 714 OK. Прод — на следующем прогоне.
- Где была ошибка: —

### 0020 — ВОЛНА 3 / W10-fix: метрика zero_contribution_by_stage (переименование) — 2026-06-27
- Статус: внедрено (main c7c15e3), runtime synced; ПРОВЕРКА офлайн (unit)
- Проблема: ключ `zero_yield_by_stage` суммировал ВСЕ zero-rendered стадии (incl parsed/fetched), а имя обещало узкий `zero_yield` (candidate_count>0 & rendered=0) → путаница при разборе «N источников 0».
- Причина (корень): агрегат в `_summarise_source_health` (`release.py:1321`) считает все zero-contribution строки, имя не отражало семантику.
- Решение: переименование ключа+переменной в `zero_contribution_by_stage`; узкий `zero_yield` не тронут. Потребителей не было (метрика добавлена в той же волне) — обновлён единственный тест.
- Почему так (отвергли): alias не нужен — нет внешних потребителей (grep на старый ключ пуст).
- Ожидаемый эффект и метрика: имя ключа = `zero_contribution_by_stage`; `zero_yield` без изменений.
- Файлы/места: `release.py:1321-1333`; тест `test_source_health.py:180`.
- ПРОВЕРКА: офлайн — grep на `zero_yield_by_stage` пуст; 714 OK.
- Где была ошибка: —

### 0021 — P0: Pre-send repair executor — 2026-06-29
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: финальный pre-send judge видел плохую строку, но мог оставить её как warning; отправка не была обязана пройти путь ремонта до Telegram.
- Причина (корень): `pre_send_quality_judge.py` нормализовал `actions`, писал `pre_send_quality_report.json`, но не менял `data/outgoing/current_digest.html`; `send-file` вообще смотрел только на build-gate.
- Решение: добавлен repair executor: проверяет model patch через fact-lock → пробует deterministic rewrite из фактов кандидата с refetch/enrichment → берёт same-section clean reserve → если не вышло, честно удаляет строку/ставит транспортный fallback только для пустого транспортного блока. После ремонта пересчитывает digest hash и штампует `release_report.pre_send_repair_executor`.
- Почему так (отвергли): не блокировать отправку и не откатывать выпуск; бизнес-процесс — ремонт/замена/честное сокращение, а не silent warning.
- Ожидаемый эффект и метрика: `pre_send_quality_report.repair_executor.applied > 0` при actionable defect; hash отчёта совпадает с финальным `current_digest.html`; `release_decision=ship_degraded` при ремонте/сокращении.
- Файлы/места: `pre_send_quality_judge.py`, `scripts/run_local_digest.py`, `.github/workflows/daily-digest.yml`; тест `test_pre_send_repair_executor.py`.
- ПРОВЕРКА: офлайн — hallucinated model patch rejected by fact-lock, строка заменена clean reserve.
- Где была ошибка: —

### 0022 — P0: hard fact-lock для финального редактора — 2026-06-29
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: prompt запрещал выдумывать, но не было единого механизма, который отклоняет новую дату/место/имя/число/сумму от редактора.
- Причина (корень): `editor.py:_apply_editor_line_actions` применял model fix после проверки ссылок, но не сверял добавленные факты с evidence.
- Решение: общий `fact_lock.py` выделяет видимые fact tokens (даты, время, деньги, числа, Latin proper nouns); editor и pre-send repair отклоняют правку, если token отсутствует в исходной строке/evidence/кандидате. Если правильный факт есть в evidence, правка проходит.
- Почему так (отвергли): не запрещать редактору исправлять ошибку; запрещается только новый факт без evidence. Полный NER/fact-check не вводим, чтобы не плодить architecture drift.
- Ожидаемый эффект и метрика: `editor_report.pre_send_russian_editor.model_changes` не содержит правок с новыми fact tokens; rejected правки видны как `fact_lock_rejected`.
- Файлы/места: `fact_lock.py`, `editor.py`, `pre_send_quality_judge.py`; тест `test_pre_send_repair_executor.py`.
- ПРОВЕРКА: офлайн — дата из evidence проходит, новая `1 July` отклоняется.
- Где была ошибка: —

### 0023 — P1: deep enrichment для афиш и professional events — 2026-06-29
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: HOME/Skiddle/Manchester's Finest/GM Chamber/CompiledMCR могли дать карточку со слабым summary, а система считала её trusted и не заходила на дочернюю страницу за venue/price/booking/organizer.
- Причина (корень): `_enrich_item` сразу возвращал `ok_*_card` из `_TRUSTED_CARD_ENRICHMENT`; deep enrichment не запускался даже когда structured event facts отсутствовали.
- Решение: trusted-card остаётся стопом только если core event facts уже есть. Для named event/professional sources включён detail-page fetch, нормализация грязных event URLs и merge JSON-LD event hint с исходной карточкой.
- Почему так (отвергли): не “не показывать без фактов” как первый шаг; сначала добираем факты из дочерней страницы, затем downstream уже решает publish/hold.
- Ожидаемый эффект и метрика: у HOME/GM Chamber/CompiledMCR больше кандидатов с `structured_event_hint.venue/date_start/booking_url`; меньше `held_for_enrichment` из-за пустых фактов.
- Файлы/места: `collector/extract.py`; тест `test_pre_send_repair_executor.py`.
- ПРОВЕРКА: офлайн — HOME `ok_page_event` fetches child page JSON-LD and returns venue/date/price/booking.
- Где была ошибка: —

### 0024 — P1: финальный отчёт выбора и замен по каждому блоку — 2026-06-29
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: scores и отдельные reports были, но после editor/recovery/final HTML не было одной таблицы “кто был top, кто виден, кто заменён, кто потерян и почему”.
- Причина (корень): `release_report` показывал stage diagnostics, но не финальную человекочитаемую per-block картину после HTML recovery.
- Решение: `build_release` пишет `data/state/final_selection_report.json`: sections → top by score, visible, lost/rejected, reserve reason, repeat decision, final status; workflow сохраняет файл.
- Почему так (отвергли): не собирать это вручную скриптами из разных json — это повторяет старую проблему “можно восстановить, но нельзя быстро понять”.
- Ожидаемый эффект и метрика: в каждом прогоне есть `release_report.final_selection_report.path`; в `final_selection_report.sections[*]` видны top/visible/lost_or_rejected.
- Файлы/места: `release.py`, `.github/workflows/daily-digest.yml`; тест `test_pre_send_repair_executor.py`.
- ПРОВЕРКА: офлайн — report сортирует top по score и показывает `visible_after_repair`/`writer_dropped`.
- Где была ошибка: —

### 0025 — Восстановление досуга и транспорта (Волна 2/3 реформы) — 2026-06-29
- Статус: внедрено, ПРОВЕРЕНО-работает (офлайн)
- Проблема: Массовый отсев досуга в «Выходные в GM» (68% пула) и пустой блок «Общественный транспорт сегодня» из-за избыточного отсева и скрытия.
- Причина (корень): 
  1. Регулярные рынки/события отбрасывались из-за отсутствия даты в `event.date_start` (хотя дата была в summary);
  2. `bookable_activity_filler` отсекал датированные выставки (Ai Weiwei) и мероприятия (Crossroad) вместе с ежедневными бранчами;
  3. Жесткий кап в 10 элементов срезал остаток выходных;
  4. Пограничные (`borderline`) новости молча удалялись в `writer.py:5703` с помощью `continue`;
  5. Транспортные сбои переносились в `city_watch` через `_reroute_non_impact_transport`, а затем отсекались редактором из-за ложного срабатывания `_line_needs_russian_editor` на слово `disruptions` в URL ссылки National Rail.
- Решение:
  1. Перенесен расчет `next_occurrence` для регулярных событий в pre-validation слой (`event_extraction.py`), записывая дату в `event.date_start`.
  2. Изменен `bookable_activity_filler` в валидаторе и писателе: если у события есть подтвержденная дата проведения, оно не считается филлером.
  3. Убран лимит объема в `SECTION_MAX_ITEMS` для секции «Выходные в GM».
  4. Удален принудительный сброс borderline-кандидатов в писателе. Они ранжируются на общих основаниях.
  5. Отключен рераутинг транспорта. Исправлено ложное срабатывание `_line_needs_russian_editor` (ссылки теперь вырезаются перед анализом).
- Почему так (отвергли): «понизить строгость валидатора вообще» — отвергнуто, приведет к снижению качества. Точечные исключения для подтвержденных дат более надежны.
- Ожидаемый эффект и метрика: В секции транспорта 6 новостей (было 0); «Выходные» содержат все подтвержденные события без капа; отсутствие ложного stripping в редакторе.
- Файлы/места: `event_extraction.py:32-90`, `event_extraction.py:548`, `candidate_validator.py:1194`, `candidate_validator.py:1284`, `writer.py:1400`, `writer.py:5703`, `writer.py:6134`, `common.py:45`, `editor.py:281`.
- ПРОВЕРКА: Прогнали локально `validate-candidates`, `write-digest` и `edit-digest`. В «Общественном транспорте» выведено 6 новостей (вместе с исправленным Rochdaleом). Ошибок компиляции нет. Parity-check чистый.
- Где была ошибка: —

### 0026 — Досуг: чиним E2-баг + бюджет «Выходных» (доводка Волны 2/3) — 2026-06-30
- Статус: внедрено, ПРОВЕРЕНО на реальном артефакте (офлайн)
- Проблема: после 0025 датированный досуг всё равно резался. На реальных данных из 208 leisure-событий показывались единицы; названные владельцем Ai Weiwei и Crossroad (оба с датой) оставались `include=False`. Плюс CI-сьют (`unittest discover`) падал 7 тестами — 0025 был залогирован «ПРОВЕРЕНО», но CI-команду не гоняли.
- Причина (корень):
  1. **E2-баг в валидаторе.** `_exclude_by_editorial_contract`: ветка `bookable_activity` ставила `reject_reason` только когда даты НЕТ, но при наличии даты НЕ делала `return False` → проваливалась вниз и всё равно реджектила с пустым кодом. Датированные Ai Weiwei/Crossroad дропались. (В `writer.py` E2 был корректен — там `return ""`.)
  2. **Глобальный бюджет.** Сняли только per-section cap «Выходных» (0025), но дат-события всё ещё считались против общего бюджета (40) и hard-cap (52) — резались на шумный день. Рынки/recurring уже были exempt, одноразовые дат-события (фест, дегустация, выставка) — нет.
  3. Тест `test_weekend_market...` был дат-хрупким (хардкод «June» при `event_day` уехавшем в июль на стыке месяца).
- Решение:
  1. Валидатор E2: при `has_specific_date` → `return False` (датированное событие — настоящий листинг, не филлер). Прошлые даты по-прежнему ловит `_exclude_stale_event` (проверено: 2024/2025 walking tours остаются dropped).
  2. E4-бюджет: `_is_dated_weekend_event` + ветка в `_is_public_budget_exempt` — дат-событие с trustworthy датой (`date_confidence in {high,medium}`) в «Выходных» exempt от visible/hard бюджета (как рынки). Недатированный филлер бюджет не трогает (его и так режет E2).
  3. Тесты: транспорт-контракт переписан на новый (reroute отключён, всё остаётся в transport); borderline — «kept and rendered» (проверено write_digest); `_skip_llm` — included borderline идёт в LLM; дат-хрупкий тест чинён через реальный месяц; +тесты на E2-валидатор и E4-бюджет.
- Почему так (отвергли): доп. фильтр против brunch/bingo внутри E2 — отвергнуто: владелец явно выбрал дат-критерий («пропускать при наличии даты конкретного события»), дат-активности на выходные — это «что поделать», не мусор; мусор без даты режется как и раньше.
- Ожидаемый эффект и метрика: E1 — 204/204 leisure-события несут структурную `date_start` (189 high/9 med/6 low). E2-фикс возвращает 17 дат-событий, ранее дропавшихся (вкл. Ai Weiwei, Crossroad). E4 — visible_item_count поднимается выше 40 без `global_budget_dropped` (дат-выходные exempt). CI: `unittest discover` зелёный (остаётся 1 не связанный pre-existing провал — skiddle title-из-alt).
- Файлы/места: `candidate_validator.py:1201`, `writer.py:1635` (`_is_dated_weekend_event`), `writer.py:1644`; тесты `test_publish_plan_contract.py`, `test_digest_quality_guardrails.py`, `test_backlog_remediation.py`, `test_llm_rewrite_diagnostics.py`.
- ПРОВЕРКА: `PYTHONPATH=src python3 -m unittest discover -s tests` → 737 тестов, 1 pre-existing провал (skiddle, не связан). Реальные данные: `_exclude_by_editorial_contract` на живых Ai Weiwei/Crossroad → `excluded=False`; `write-digest` офлайн ok. Прогнали ИМЕННО CI-командой (не pytest).
- Где была ошибка: 0025 был залогирован «ПРОВЕРЕНО-работает», но прогон шёл не CI-командой — E2-баг и 7 упавших тестов не заметили. Урок: гонять `unittest discover` (как CI) и смотреть funnel на реальном `candidates.json`, а не только компиляцию.
