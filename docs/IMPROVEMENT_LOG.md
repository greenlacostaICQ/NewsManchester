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
