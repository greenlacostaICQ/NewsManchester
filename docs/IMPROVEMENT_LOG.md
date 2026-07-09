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

### 0027 — Операционная страховка: workflow timeout 35 минут — 2026-06-30
- Статус: внедрено
- Проблема: GitHub Actions run `28426401071` был отменен по job timeout через 25 минут: `edit-digest` завершился, но `build-digest`, pre-send judge и Telegram send не стартовали. `delivery_state.json` остался за 2026-06-29, а `release_report.json` тоже остался старым, что создало смешанное состояние отчетов.
- Причина (корень): в daily workflow стоял общий `timeout-minutes: 25`; 2026-06-30 критический путь вырос из-за трех одновременных факторов: долгий Visit Manchester extraction/enrichment, увеличенный LLM rewrite board и второй круг final editor.
- Решение: увеличить общий job timeout до 35 минут как временный предохранитель, чтобы текущий выпуск успевал дойти до `build-digest`/send, пока внедряются настоящие stage budgets и перенос broad event/ticket inventory из утреннего critical path.
- Почему так (отвергнутые альтернативы): не считаем это корневым решением. Просто поднять timeout выше 35 минут отвергнуто: это скрывает деградацию и может сдвигать утреннюю отправку слишком поздно. Оставить 25 минут отвергнуто: при уже известном 24m+ pre-build path следующий похожий день снова отменит выпуск до отправки.
- Ожидаемый эффект и метрика проверки: следующий workflow при похожем профиле должен завершить `build-digest` и Telegram send вместо cancel; в GitHub Actions duration может быть >25m, но conclusion должен быть `success`, а `delivery_state.last_delivery_day_london` должен обновиться на день прогона.
- Файлы/места: `.github/workflows/daily-digest.yml:13`.
- ПРОВЕРКА (после прогона): не проверено реальным прогоном; это config-only страховка до следующего daily/workflow_dispatch.
- Где была ошибка (если не сработало): —

### 0028 — Cancel-proof observability до build — 2026-06-30
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон/cancel-drill
- Проблема: при отмене до `build-digest` не оставалось актуальных `speed_report.json` и `final_selection_report.json`; разбор 30 июня приходилось собирать по mtimes и смешанным state-файлам.
- Причина (корень): `speed_report.json` и финальная таблица выбора писались только внутри `build_release`; pipeline-стадии запускаются отдельными командами, поэтому kill до build не даёт release-артефактов.
- Решение: добавлен `flush_stage_observability`: после стадий пишет provisional `speed_report.json`, append-only `stage_timings.jsonl`, `source_run_log.jsonl` после collect и `selection_snapshot.json` после стадий, где уже есть/меняется candidate state (`collect`, `dedupe`, `validate`, `curator`, `transport_fill`, `llm_rewrite`, `write`, `edit`). `final_selection_report.json` остаётся финальным post-render отчётом.
- Почему так (отвергнутые альтернативы): не переносить финальный отчёт до render — до HTML это только provisional snapshot; не делать flush fail-closed — observability не должна блокировать выпуск.
- Ожидаемый эффект и метрика проверки: kill до build оставляет stage timings до последней завершённой стадии, source rows после collect и selection snapshot начиная с collect и после каждого следующего изменения candidate state.
- Файлы/места: `release.py:_stage_seconds`, `release.py:flush_stage_observability`, `scripts/run_local_digest.py:_flush_obs`; тест `CancelProofObservabilityTest`.
- ПРОВЕРКА (после прогона): офлайн — `PYTHONPATH=src python3 -m unittest tests.test_backlog_remediation.CancelProofObservabilityTest.test_flush_writes_speed_source_log_timings_and_selection_snapshot` пишет `speed_report`, `source_run_log`, `stage_timings`, `selection_snapshot`; prod/cancel-drill не выполнялся.
- Где была ошибка (если не сработало): —

### 0029 — Targeted editor round 2 без перечитывания всего выпуска — 2026-06-30
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: второй круг final editor 30 июня отправлял почти весь выпуск заново (примерно 43 строки после 45 в первом круге), добавляя около 100 секунд к critical path.
- Причина (корень): `editor.py` при `needs_second_round` строил `second_items` как полный `_visible_line_items`, а `_apply_editor_line_actions` одновременно требовал полный список items, потому что при model fixes пересобирает `polished`.
- Решение: round 2 отправляет в модель только строки `action != ok`, uncovered/newly inserted и sensitive; применение всё равно идёт по полному `second_items_all`, чтобы не потерять untouched lines при rebuild. Удалены незавершённые debug prints.
- Почему так (отвергнутые альтернативы): не передавать filtered items в `_apply_editor_line_actions` — это роняет untouched lines; не включать detector-hit поверх round1 `ok` — это снова расширяет второй круг до почти полного выпуска и нарушает утверждённый контракт.
- Ожидаемый эффект и метрика проверки: `editor_report.pre_send_russian_editor.rounds[1].selection_policy == "targeted_second_round"`; `targeted_items < visible_items` при recovery round; `coverage_complete` остаётся true за счёт union(round1 ok, round2 targeted).
- Файлы/места: `editor.py:_editor_line_identity`, `editor.py:_line_is_sensitive`, `editor.py:_pre_send_polish_sections`; тест `TargetedEditorSecondRoundTest`.
- ПРОВЕРКА (после прогона): офлайн — `PYTHONPATH=src python3 -m unittest tests.test_backlog_remediation.TargetedEditorSecondRoundTest.test_round2_is_targeted_not_whole_digest` проверяет targeted subset, сохранение всех строк и `coverage_complete`; prod run не выполнялся.
- Где была ошибка (если не сработало): —

### 0030 — `show = renderable` и строгий Fresh incident dedupe — 2026-06-30
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: `publish_plan_status=show/must_show` мог ставиться выбранному кандидату без публичной строки; writer затем рендерил по остаточному `include=True`. Параллельно writer-level Fresh dedupe мог склеить разные crime/incident строки по общим словам.
- Причина (корень): `_publish_plan_status` смотрел только на `digest_selection_verdict=selected`; `_fresh_rows_are_same_story` разрешал token-overlap/Jaccard для incident/court так же, как для council/planning.
- Решение: selected без `draft_line` или явных deterministic-ready полей получает `needs_enrichment`; writer не рендерит non-`show/must_show` даже при `include=True`. Для crime/incident/court token-overlap больше не склеивает строки сам по себе: нужен общий конкретный anchor или общая location + incident marker, совместимые date/type; обычный non-incident overlap сохранён.
- Почему так (отвергнутые альтернативы): не удалять Jaccard/overlap глобально — иначе ломаются нормальные council/planning/service дубли; не считать любой `venues_tickets` deterministic-ready — ticket должен иметь хотя бы title/date/venue.
- Ожидаемый эффект и метрика проверки: `show_missing` должен стремиться к 0 структурно; murder-trial / Fallowfield-car не склеиваются; Moston firearms/guns склеиваются с `dedupe_merge_evidence`.
- Файлы/места: `llm_rewrite.py:_uses_deterministic_writer`, `llm_rewrite.py:_publish_plan_status`, `writer.py:fresh_dedupe_evidence`, `writer.py:_fresh_rows_are_same_story`, `writer.py:write_digest`; тест `tests/test_dedupe_and_show.py`.
- ПРОВЕРКА (после прогона): офлайн — `PYTHONPATH=src python3 -m unittest tests.test_dedupe_and_show` проверяет incident dedupe, non-incident overlap, deterministic-ready и writer enforcement; prod run не выполнялся.
- Где была ошибка (если не сработало): —

### 0031 — No weak fallback + prevalidated reserve — 2026-06-30
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: после repair попыток hard news/events/tickets всё ещё могли деградировать в строку из одного title/summary, а editor reserve снова проходил мини-квест refetch/rewrite/quality во время replacement.
- Причина (корень): в конце writer render-loop оставался общий headline fallback для категорий вне строгого draft_line gate; `_same_section_reserve_line` смешивал две роли — подбор готового резерва и попытку его дообогатить на лету.
- Решение: для hard news/events/tickets title-only fallback запрещён: item либо чинится recovery/deterministic rewrite, либо честно drops/held с `recoverable_reserve=False`. В editor добавлен `_PrevalidatedReservePool`: render-ready reserve строки собираются один раз по секциям, проходят section/date/URL/story/lint checks и сортируются по score; stop-loss replacement делает pop из готовой очереди.
- Почему так (отвергнутые альтернативы): не блокировать send из-за одной строки — bad row strip/remove уже есть; не тянуть textless reserve в replacement — это возвращает ту же проблему weak fallback, только позже.
- Ожидаемый эффект и метрика проверки: в send нет headline-only строк для hard news/events/tickets; non-renderable drops не становятся recoverable reserve; при непустом prevalidated reserve replacement не падает на `reserve_failed_quality_or_caps`.
- Файлы/места: `writer.py:_headline_fallback_forbidden`, `writer.py:write_digest`, `editor.py:_PrevalidatedReservePool`, `editor.py:_same_section_reserve_line`, `editor.py:_pre_send_polish_sections`; тесты `tests/test_dedupe_and_show.py`, `tests/test_pre_send_repair_executor.py`, `tests/test_recoverable_reserve.py`.
- ПРОВЕРКА (после прогона): офлайн — `PYTHONPATH=src python3 -m unittest tests.test_dedupe_and_show tests.test_recoverable_reserve tests.test_pre_send_repair_executor tests.test_publish_plan_contract` + focused editor/writer regressions; prod run не выполнялся.
- Где была ошибка (если не сработало): —

### 0032 — A-tier microfix: future_announcements распознаётся как A-tier — 2026-07-01
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: будущий A-tier анонс (The Weeknd, The Fratellis в блоке `future_announcements`) не проходил A-tier проверку и молча уходил в manual-review вместо показа/удержания в инвентаре.
- Причина (корень): `_is_a_tier_ticket` гейтил блок только по `{ticket_radar, outside_gm_tickets}`, поэтому `future_announcements` с `ticket_notability.tier=A` (notability уже считается для `category=venues_tickets`) не признавался A-tier.
- Решение: добавлен `future_announcements` в допустимый набор блоков `_is_a_tier_ticket`. Downstream уже корректен: `_hold_global_capped_a_tier` теперь держит выпавший будущий A-tier в ticket inventory, а не теряет молча; budget-exempt остаётся scope-gated (gm/nearby), т.е. без блоат-риска для outside/unknown.
- Почему так (отвергнутые альтернативы): не расширять notability-enrichment на `future_announcements` по блоку (лишние сетевые lookups) — venues_tickets category уже даёт notability; большой horizon-gating (milestone-only) вынесен в отдельный пункт бэклога, здесь только распознавание.
- Ожидаемый эффект и метрика проверки: `_is_a_tier_ticket(future_announcements A-tier)` == True; выпавший по бюджету будущий A-tier виден в ticket_inventory_held, а не в тихом manual-review.
- Файлы/места: `writer.py:_is_a_tier_ticket`; тест `tests/test_ticket_consolidation.py::ATierBudgetExemptionTest.test_future_announcement_a_tier_is_recognised`.
- ПРОВЕРКА (после прогона): офлайн — `PYTHONPATH=src python3 -m unittest tests.test_ticket_consolidation` (10 ok) + A-tier/budget regressions `tests.test_editorial_regression tests.test_digest_quality_guardrails tests.test_release_20260604_fixes tests.test_public_output_contracts tests.test_product_backlog` (270 ok); prod run не выполнялся.
- Где была ошибка (если не сработало): —

### 0033 — Пустой «Что важно сегодня» не должен блокировать весь выпуск, если writer доказал честный shortfall — 2026-07-01
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit + локальный build), прод — следующий прогон
- Проблема: run 2026-07-01 собрал и написал выпуск, но `build-digest` заблокировал отправку: `Draft digest is missing required block: Что важно сегодня.` При этом `writer_report.today_focus_board` показывал `eligible_candidates=0`, `rendered_candidates=0`, `underflow_reason=not_enough_eligible_practical_items`; editor уже снял блок как warning, но release снова сделал hard fail.
- Причина (корень): `_validate_draft` в `release.py` проверял только наличие included-кандидатов с `primary_block=today_focus`. Он не учитывал post-writer правду: были ли у writer-а реальные eligible/rendered строки для practical-блока. Поэтому слабые или не render-ready кандидаты превращали честный underflow в полный missed digest.
- Решение: для «Что важно сегодня» release теперь читает `writer_report.today_focus_board`. Если board говорит `eligible_candidates=0` и `rendered_candidates=0`, отсутствие/пустота секции становится warning с причиной, а не `errors[]`. Остальные required blocks не ослаблены; если eligible rows есть, пустой блок по-прежнему fail-closed.
- Почему так (отвергнутые альтернативы): не убираем «Что важно сегодня» из `REQUIRED_BLOCKS` целиком — это скроет настоящие потери; не вытаскиваем автоматически `backup_pool_only` новости из Fresh в Today Focus в этом же патче — 2026-07-01 показал M60-кандидата в backup pool, но изменение public backup policy шире и может ухудшить выпуск без отдельного board-reservation правила.
- Ожидаемый эффект и метрика проверки: при честном `today_focus_board.eligible_candidates=0` release пишет `ship_degraded`/warning и отправка доходит до Telegram; при `eligible_candidates>0` и пустом HTML release остаётся `fail`.
- Файлы/места: `release.py:_validate_draft`; тесты `tests/test_backlog_remediation.py::PublishedReviewTest.test_empty_today_focus_with_no_eligible_writer_rows_warns_not_blocks`, `tests/test_backlog_remediation.py::PublishedReviewTest.test_empty_today_focus_with_eligible_writer_rows_still_blocks`.
- ПРОВЕРКА (после прогона): офлайн — targeted unit tests прошли (2 ok); локально на state 2026-07-01 после пересборки `write-digest` → `build-digest` release прошёл с `errors=[]`, `release_decision=ship_degraded`, предупреждениями по `Что важно сегодня 0/3`. Прод run не выполнялся.
- Где была ошибка (если не сработало): —

### 0034 — «Что важно сегодня» добирает практический reserve до/после writing board — 2026-07-01
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit), прод — следующий прогон
- Проблема: emergency-fix #0033 не улучшал сам блок: 2026-07-01 общий выпуск имел 54 видимых пункта, но «Что важно сегодня» остался 0/3. В общем пуле был практический M60/Prestwich traffic candidate, но он остался `backup_pool_only` без `draft_line` и не мог быть восстановлен в блок.
- Причина (корень): `llm_rewrite` помечал часть capacity-cut новостей как `recoverable_reserve=True`, но `writer._apply_section_min_floor_pull_back` не считал `backup_pool_only` таким резервом для публикации. Кроме того, Today Focus recovery смотрел только `primary_block=today_focus`, хотя реальные практические новости часто приходят как `last_24h` или `city_watch`.
- Решение: добавлена защищённая квота до 3 practical Today Focus reserve candidates перед русским writing board; writer теперь использует общий `is_recoverable_reserve`, Today Focus recovery добирает eligible кандидатов из `{today_focus,last_24h,city_watch}`, строит deterministic hard-news строку для road/incident reserve и пишет `writer_report.today_focus_loss_trace`.
- Почему так (отвергнутые альтернативы): не ослаблять eligibility для всего `backup_pool_only` — только clean `recoverable_reserve`; не делать новый LLM stage — используем существующий writing board и deterministic recovery; не переносить все Fresh incidents в Today Focus — нужен активный reader action/road/service/deadline signal.
- Ожидаемый эффект и метрика проверки: в следующем run при наличии M60/road/service/deadline reserve `writer_report.section_counts["Что важно сегодня"] >= 1` и `writer_report.today_focus_loss_trace.items[]` показывает, что candidate либо `rendered_today_focus`, либо конкретно почему не восстановлен. Цель блока остаётся 3+.
- Файлы/места: `llm_rewrite.py:_mark_today_practical_translation_reserve`, `writer.py:_apply_section_min_floor_pull_back`, `writer.py:_today_focus_loss_trace`, `writer.py:_hard_news_recovery_line`; тесты `tests/test_digest_quality_guardrails.py`, `tests/test_prompt_versioning.py`.
- ПРОВЕРКА (после прогона): офлайн — targeted unit tests прошли (8 ok), соседние writer/prompt/reserve regressions прошли (16 ok), `git diff --check` clean. Локальный `write-digest` на state 2026-07-01 дал `section_counts["Что важно сегодня"]=3`, `today_focus_board.rendered_after_recovery=3`, `recovery_inserted=1`, `underflow_reason=""`; видимые строки: M60 / Oldham pub / Bury school. Prod run не выполнялся.
- Где была ошибка (если не сработало): —

### 0035 — Backlog 8 (сокращённый): category health verdict, cross-stage no-loss check, evidence-cache hardening — 2026-07-01
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit + real artifact), прод — следующий прогон
- Проблема: пункт 8 бэклога («данные/inventory/ночной сбор») в исходном виде — 6 расписаний, ~25 источников, новая схема данных — это архитектурная стройка на недели, а не микрозадача. При разведке выяснилось, что часть intent'а (8.6 show=renderable, 8.8 prevalidated reserve) уже закрыта в 0030/0031, а часть (8.10 «причина по каждому непоказанному item») уже даёт `final_selection_report.json` через `_disposition_for_candidate`/`final_status`/`_candidate_selection_reason`. Дублировать это новой таксономией значило бы плодить вторую систему дисположиций поверх рабочей.
- Причина (корень): исходный пункт 8 писал план заново с нуля, не проверив, что уже реализовано текущим (единственным дневным) collect-прогоном; collector уже собирает 11 категорий (media_layer/transport/gmp/public_services/culture_weekly/venues_tickets/food_openings/football/tech_business/professional_events/diaspora_events) за один проход — ночной сплит на 6 волн не нужен, чтобы получить honest per-category health и no-loss guarantee сегодня.
- Решение: новый модуль `inventory.py` — (1) `aggregate_category_health`/`classify_category_health`: ok/partial/failed/empty_legit/empty_suspicious по существующему `source_run_log.jsonl` (item 1), различает «источник упал» от «просто тихий день»; (2) `verify_conservation`: сравнивает `collected_found` (сумма `source_run_log` `found`) с `candidates.json` count — флагает только реальную потерю (delta<0), терпим к маленькому положительному сдвигу от synthetic weather/transport карточек; (3) evidence-cache hardening в `llm_rewrite._candidate_content_hash` — добавлены `prompt_version` (PROMPT_REGISTRY_VERSION, lazy import из-за цикла prompts_meta↔llm_rewrite), `schema_version`, structured story facts (what_happened/who_affected/why_now/event_type) — меняющийся факт инвалидирует кэш даже за пределами обрезки evidence_text (2200/3200 симв.). Оба отчёта (`selection_snapshot.json`, `final_selection_report.json`) теперь несут `category_health` + `conservation`. Добавлен `write_json_atomic` (temp+rename) в common.py для будущего inventory state layer.
- Почему так (отвергнутые альтернативы): не строить `data/state/inventory/*.jsonl` с night-job расписанием (8.1/8.2/8.4/8.5) — нет реального продюсера без верификации/постройки ~25 скраперов и без production-инфры (launchd/cron) на машине owner'а, которую нельзя трогать молча; не дублировать disposition/reserve-логику (8.6/8.7/8.8/8.11 в исходной форме) — она уже есть и работает (0030/0031, `_disposition_for_candidate`).
- Ожидаемый эффект и метрика проверки: per-category verdict виден в обоих отчётах без парсинга сырых чисел; net-loss между collect и candidates.json ловится структурно (`conservation.conserved=False`), а не молча; hard-news reuse-кэш не отдаёт устаревшую строку при смене факта.
- Файлы/места: новый `inventory.py` (`aggregate_category_health`, `classify_category_health`, `verify_conservation`, `evidence_cache_extra_fields`); `common.py:write_json_atomic`; `llm_rewrite.py:_candidate_content_hash`; `release.py:_read_jsonl_rows`, `_write_final_selection_report`, `_write_selection_snapshot`; тест `tests/test_inventory.py`.
- ПРОВЕРКА (после прогона): офлайн — `PYTHONPATH=src python3 -m unittest tests.test_inventory` (9 ok); проверено на РЕАЛЬНОМ `collector_report.json`/`candidates.json` с диска (не выдуманная фикстура): category_health корректно дал `ok`×9 и `partial`×2 (culture_weekly/football, по 1 ошибке при полном enrichment), `conservation` дал `collected_found=1447, candidates_json_count=1448, delta=+1, conserved=True` — совпадает с ручной сверкой. Полный `unittest discover -s tests`: 765 тестов, 1 pre-existing fail (`test_skiddle_cards_extract_event_link_and_date`, воспроизведён на чистом main до моих правок — не регрессия, не мой файл, не трогал).
- Где была ошибка (если не сработало): —
- Отложено, требует явного OK owner'а: 8.1 (data/state/inventory/*.jsonl с night-job producer'ом), 8.4 (расписание 00:30/02:00/03:30/06:15/06:30/07:45 — новая production-инфраструктура: launchd на машине owner'а или GH Actions cron, ни то ни другое не трогалось); 8.5 per-domain scraper card rules — уже покрыто существующим `event_extraction.py`/`ticket_notability.py` (event_name/venue/date_start/tier/venue_scope уже извлекаются), отдельной работы не потребовалось.

### 0036 — Backlog 8 (полный): inventory-слой + night waves (8.1–8.11) — 2026-07-01
- Статус: внедрено локально; ПРОВЕРКА офлайн (unit + real artifact + shell syntax), прод — установка launchd owner'ом. Снимает отложенный статус 0035 (8.1/8.4) — owner дал явную авторизацию «делай все как описано».
- Проблема: 0035 закрыл только семантику (health/conservation/cache) и отложил 8.1/8.4. Нужно построить весь слой: inventory как проверяемый слой + ночной сбор по волнам.
- Причина (корень): пункт 8 требовал (а) inventory-слой (record schema, readiness, disposition, morning-contract, re-entry), (б) ночной сбор по волнам. Риск для 08:00-релиза снят архитектурно: night waves пишут ТОЛЬКО в inventory (upsert), никогда в candidates.json — хот-пат неприкосновенен.
- Решение: `inventory.py` — 8.1 `write_json_atomic`+`InventoryLock`(stale-break 900с)+`write/read/merge_inventory` (schema_version на каждом record); 8.2 `build_inventory_record` (canonical schema, английский raw хранится, рабочая единица — fact_card); 8.4 `collect-inventory --wave` через `_collect_single_source` + launchd `com.mnewsdigest.inventory.plist` (00:30/02:00/03:30/06:15/07:45) + `run_night_inventory.sh` (волна по времени); 8.5 `evaluate_card` (render_ready ⇔ обязательные поля + draft_line); 8.6 `passes_morning_contract` (stale/last-known-good не рендерится как свежий); 8.7 `classify_disposition` (10 значений) + `verify_dispositions` (sum==captured, silent_loss) + `reentry_candidates` (дедуп против `published_facts`, не второй дедуп); 8.9 `categories_needing_live_fallback`; 8.10 оба отчёта несут category_health/live_fallback/collect_conservation/disposition_conservation; 8.11 `annotate_morning_relevance`. 8.6/8.8 (show=renderable, prevalidated reserve) уже есть (0030/0031) — inventory их питает.
- Почему так (отвергнутые альтернативы): night waves НЕ пишут candidates.json и НЕ меняют collect_digest (правило never-block-release, first-pass quality); одна plist с выбором волны по времени вместо 6; inventory НЕ kickstart'ится при установке (фон по расписанию); морнинг-билд пока НЕ кормится из inventory — gradual-шаг из пункта 15, чтобы не флипать хот-пат непроверенным.
- Ожидаемый эффект и метрика проверки: `build-inventory` пишет schema-versioned карточки; night wave обновляет inventory не трогая хот-пат; disposition conservation ловит silent loss; health verdict → live fallback для мёртвой категории.
- Файлы/места: `inventory.py`, `common.py:write_json_atomic`, `release.py` (отчёты), `scripts/run_local_digest.py:cmd_build_inventory/cmd_collect_inventory`, `scripts/run_night_inventory.sh`, `ops/launchd/com.mnewsdigest.inventory.plist`, `scripts/install_launchd_job.sh`, `scripts/sync_runtime_bundle.sh`; тест `tests/test_inventory.py`.
- ПРОВЕРКА (после прогона): офлайн — `tests.test_inventory` 20 ok (вкл. night-wave safety: волна пишет inventory, candidates.json НЕ создаётся); `build-inventory` на РЕАЛЬНОМ candidates.json — 1379 records/12 категорий, canonical schema; `verify_dispositions` на реальных 1392 кандидатах — conserved=True, captured==accounted; `bash -n` OK, wave-mapping 00:30→events…07:45→breaking OK; полный `unittest discover` 776 тестов, 1 pre-existing fail (Skiddle, воспроизведён на чистом main, не мой файл). Prod run не выполнялся.
- Где была ошибка (если не сработало): —
- Активация owner'ом (единственный machine-step, не «вопрос»): `bash scripts/install_launchd_job.sh` установит night-inventory launchd job. До установки слой inert (inventory-файлов нет). Морнинг-билд 08:00 остаётся как есть.

### 0037 — Backlog 8.4 substrate fix: ночной inventory → GitHub Actions, launchd убран — 2026-07-01
- Статус: внедрено локально; ПРОВЕРКА офлайн (YAML parse, notify-python прогон, tests), прод — по расписанию GitHub.
- Проблема: 0036 повесил ночные волны на launchd (`~/.mnewsdigest`). Owner указал: прод-утро — это GitHub Actions (`daily-digest.yml`, `workflow_dispatch` от cron-job.org в 08:00), а не launchd. launchd-`daily` вообще не установлен; на Mac только `bot-updates`. GitHub-runner в 08:00 никогда не увидит inventory-файлы с Mac — substrate неверный.
- Причина (корень): я доверился устаревшей memory «прод бежит из launchd» вместо проверки `.github/workflows/`. Реальность (сверено с кодом): `daily-digest.yml` бежит на ubuntu-latest из git checkout и персистит state **коммитом в repo** (шаг «Commit updated state files» → `chore: digest state` → pull --rebase --autostash → push). Значит inventory тоже должен жить в repo через GitHub.
- Решение: `.github/workflows/night-inventory.yml` — `schedule:` (UTC, волна привязана к `github.event.schedule`, DST-безопасно; breaking в 06:30 UTC = 07:30 BST/06:30 GMT, всегда до 08:00-утра) + `workflow_dispatch` с input `wave`. Гоняет `collect-inventory --wave X`, коммитит `data/state/inventory/*.jsonl` + `inventory_run_log.jsonl`, шлёт краткое Telegram-уведомление (успех/провал). `concurrency: {group: digest-state, cancel-in-progress: false}` на ОБОИХ workflow (night + daily) сериализует 07:30↔08:00 — морнинг queue'ится за волной, не отменяется. Убрана launchd-обвязка inventory: удалены `ops/launchd/com.mnewsdigest.inventory.plist` и `scripts/run_night_inventory.sh`, откачены добавления в `install_launchd_job.sh`/`sync_runtime_bundle.sh`. Командный слой (`collect-inventory`, `inventory.py`) остался — он substrate-agnostic, workflow зовёт ровно его.
- Почему так (отвергнутые альтернативы): не GitHub `schedule:` с наивным UTC для breaking — уехал бы на час при BST/GMT и мог бы залезть за 08:00; вместо этого breaking-время выбрано безопасным в обоих сезонах + concurrency-страховка. Не cron-job.org для ночи (owner спросил «мне создавать крон?») — сделано полностью в repo, ноль ручной настройки. Breaking оставлен отдельной волной 07:30 (выбор owner'а), не вложен в утро.
- Ожидаемый эффект и метрика проверки: ночные волны коммитят inventory в repo → 08:00-runner его видит; breaking всегда до утра; Telegram шлёт «волна X · N источников · M карточек».
- Файлы/места: `.github/workflows/night-inventory.yml` (новый), `.github/workflows/daily-digest.yml` (+concurrency), `scripts/install_launchd_job.sh` + `scripts/sync_runtime_bundle.sh` (откат inventory-обвязки), удалены `ops/launchd/com.mnewsdigest.inventory.plist` + `scripts/run_night_inventory.sh`. Memory `project_deploy_runtime_sync` переписана (была причиной ошибки).
- ПРОВЕРКА (после прогона): офлайн — оба YAML парсятся (`yaml.safe_load`), у обоих `concurrency=digest-state`; notify-python прогнан на фейковом summary → корректное сообщение; `tests.test_inventory` 20 ok; `bash -n` откаченных скриптов OK; нет висячих ссылок на удалённые файлы. GitHub-прогон workflow — по расписанию/`workflow_dispatch` (owner может проверить вручную через Actions UI). Прод-ночь ещё не бежала.
- Где была ошибка (если не сработало): —
- Следующий шаг (после нескольких чистых ночей, gradual, пункт 15): научить `daily-digest.yml` читать inventory-first с live-fallback. НЕ в этом заходе.

### 0038 — Общая repeat-policy защита: food/openings больше не наследуют ticket/weekend carry-over — 2026-07-01
- Статус: внедрено локально; ПРОВЕРКА офлайн (focused unit + compile), прод — следующий прогон
- Проблема: ресторанный opening мог вернуться в выпуск как будто это билет/ивент day-of reminder. Видимый симптом: один и тот же ресторан повторялся, хотя у него нет права на ежедневный повтор.
- Причина (корень): `openings`/`food_openings` были включены в calendar carry-over вместе с ticket/weekend blocks, а validator разрешал некоторые повторы по одному anchor (`dated_event`/`ticket_opportunity`) без проверки бизнес-типа блока. Release после writer не имел финальной видимой проверки exact fingerprint против `published_facts`.
- Решение: добавлен `repeat_policy.py` как единый диспетчер поверх существующих `calendar_repeat_review`/`lifecycle_repeat_review`: operational, ticket, event и обычные news/food/opening разделены явно. `openings`/`food_openings` исключены из calendar carry-over; validator same-fingerprint allowlist стал typed, а не anchor-only. В release добавлен post-writer visible repeat review: bad exact-fingerprint line удаляется из draft, выпуск продолжает идти как `ship_degraded`, а `repeat_policy_report.json` считает exact, lifecycle, people и vector/semantic matches отдельно.
- Почему так (отвергнутые альтернативы): не вводить глобальный запрет всех повторов — это сломало бы билеты день-в-день, два ticket-блока и weekend planning; не удалять строки по vector/semantic match на финальном шаге — вектора считаются и попадают в отчет, но финальный quarantine основан только на exact visible fingerprint, чтобы не выкинуть похожие, но разные новости.
- Ожидаемый эффект и метрика проверки: ORME-класс (`food_openings/openings` + сегодняшняя дата + прежний fingerprint) не проходит ни calendar carry-over, ни visible repeat policy; day-of ticket exact repeat всё еще разрешен; undated ticket не получает bypass; release-аудит видит canonical URL в HTML и считает `vector_or_semantic_matches`.
- Файлы/места: новый `repeat_policy.py`; `dedupe.py:_calendar_item_should_carry_over`; `candidate_validator.py:_exclude_cross_day_rehash`; `release.py:_classify_visible_repeat_policy`, `_quarantine_repeat_rendered_html_items`, `_write_repeat_policy_report`; тест `tests/test_digest_quality_guardrails.py`.
- ПРОВЕРКА (после прогона): офлайн — 6 focused tests OK: food/opening repeat denied, day-of ticket allowed, undated ticket denied, canonical visible URL matched, exact visible repeat quarantined, vector/semantic match counted; `py_compile` по измененным runtime-файлам OK; `git diff --check` clean. Prod run не выполнялся.
- Где была ошибка (если не сработало): —

---

# План реформы качества (диагностика прогона 2026-07-01, run 20260701T101913)

Записи 0039–0052 заведены как единый план по итогам глубокого разбора выпуска 2026-07-01. Все со статусом `предложено`; ПРОВЕРКА заполняется по факту прогона. Корневой вывод разбора: систему добивают длину строк, а не ценность; финальный судья на этом прогоне не отработал (429), а проверки заточены на «не выдумай факт», а не на «не потеряй факт».

### 0039 — Судья: Map/Reduce + общий limiter + урезанный payload — 2026-07-02
- Статус: внедрено локально; прод-проверка на следующем реальном прогоне
- Проблема: `pre_send_quality_report.json` за 2026-07-01: `decision=block`, `Error 429 ... Limit 30000, Requested 43078`. Финальная проверка качества не выполнилась вообще; выпуск ушёл без неё (правило never-block).
- Причина (корень): один монолитный вызов — весь выпуск + до 250 карточек, у каждой `evidence_text[:700]`+`draft_line[:700]`+`summary[:360]`+`lead[:360]` (`pre_send_quality_judge.py:913-925`, payload из `_rendered_candidates:204-221`). На 57 карточек ≈ 43k токенов > 30k TPM gpt-4o. Всегда 429.
- Решение: Map/Reduce вместо монолита. Map режет финальный HTML по границам секций; крупная секция дополнительно режется по 12 видимых строк. Payload строки = visible text + matched candidate `{title, source_url, source_label, primary_block, category, compact_facts, compact event}`, без `summary`, `lead`, полного `evidence_text` и `draft_line`. Reduce получает только контур всего выпуска, `product_completeness`, lead/protected markers и summaries chunk-судей. Все вызовы идут через общий token-bucket limiter по паттерну `llm_rewrite` (`PRE_SEND_JUDGE_MAX_TPM=27000`, старт `PRE_SEND_JUDGE_MAX_WORKERS=3`). Упавший chunk/reduce не маскируется: общий `status=partial`, а чистый `pass` повышается минимум до `warn`.
- Почему так (отвергли): «взять модель с большим TPM» — не устраняет монолит и стоит дороже; «просто поднять max_tokens» — проблема во входе, не выходе; «параллелить без общего limiter» — снова даст 429 на минутном лимите.
- Ожидаемый эффект и метрика: каждый map-chunk ≤~8k estimated tokens; `pre_send_quality_report.status=ok|partial` с реальным `decision`; при штатном прогоне 0 ошибок 429; в `raw.map_reduce` видны chunk statuses, failed count, reduce status.
- Файлы/места: `src/news_digest/pipeline/pre_send_quality_judge.py` (`_compact_candidate_for_judge`, `_chunk_digest_slots`, `_call_judge_payload`, `_run_map_reduce_judge`, `_combine_map_reduce_results`); `src/news_digest/pipeline/prompts_meta.py`.
- ПРОВЕРКА (после прогона): offline — `py_compile` OK; `tests.test_pre_send_quality_judge`, `tests.test_editor_pacing`, `tests.test_prompt_versioning`, `tests.test_pre_send_repair_executor` OK. На текущем artefact: 49 visible lines, 57 rendered candidates, 14 map chunks, max estimated map call 4509 tokens. Прод-проверка 429/`status=ok|partial` — после следующего реального прогона.
- Где была ошибка (если не сработало): —

### 0040 — Судья: детерминированная сеть при падении модели — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн (unittest судьи + smoke детектора), прод — следующий прогон. Реализовано поверх закоммиченного Map/Reduce (0039, коммит cd22986 параллельной сессии): новый `_deterministic_html_scan` вызывается в except-пути, parsed-None пути и при `judge_status=="failed"` (главный путь при 429 всех чанков) — отчёт больше не пустой, findings идут в warnings + `deterministic_post_check.model_unavailable_fallback`. Report-only, HTML не мутируется.
- Проблема: при падении судьи (429/timeout) финальная проверка = ноль; штампы и битые ссылки уходят в эфир.
- Причина (корень): ветка except просто пишет `status=failed` и по never-block выпуск идёт как есть (`pre_send_quality_judge.py` except → `_write_report`).
- Решение: при недоступности модели прогнать локальный чек без LLM: список пустых призывов (следите/проверьте/сверяйте/уточните без факта после), href не-URL/с `<`/с `&lt;`, латиница+кириллица в одном слове, обрезанные предложения. Помечать/чинить/резать конкретные строки, не блокируя выпуск.
- Ожидаемый эффект и метрика: даже при `status=failed` в отчёте есть `deterministic_post_check` с числом пойманных дефектов.
- Файлы/места: `pre_send_quality_judge.py` (except-ветка + новый `_deterministic_post_check` расширить, он уже объявлен на :301).
- ПРОВЕРКА (после прогона): —

### 0041 — Убрать добивку шаблоном, обогащать каждую короткую карточку — 2026-07-02
- Статус: внедрено локально; прод-проверка на следующем реальном прогоне
- Проблема: лид, футбол, M60 и др. заканчиваются штампом «следите за обновлениями / проверьте карту дорог». Это добивка длины, не факт.
- Причина (корень): контракт `LONG_FORMAT_MIN_CHARS=150` + `LONG_FORMAT_MIN_SENTENCES=2` (`writer.py:78,86`); при недоборе вызывается `_soft_recovery_action_sentence` (`writer.py:3247-3264`), пришивающий фразу из зашитого списка. Настоящий обогатитель `_model_recover_section_line` (`writer.py:4073`) включается ТОЛЬКО когда секция ниже порога по количеству, кап `_RECOVERY_MODEL_MAX_PER_SECTION=2`. На отдельную короткую карточку не вызывается.
- Решение: заменить путь. `_soft_recovery_action_sentence` удалён. Короткая карточка идёт через controlled recovery: структурированное событие/билет с датой+местом+названием публикуется как `short_but_complete`; thin evidence не добивается штампом, а выходит честно коротко после снятия generic-хвоста или удерживается, если строки почти нет; важная новость с достаточным evidence уходит в `_model_recover_section_line` под run-cap и token limiter. После восстановления строка снова проходит fact/quality checks.
- Почему так (отвергли): «просто снизить 150 до 100» — теряем длинные качественные там, где evidence богатый; правильный порядок — сначала обогатить, потом разрешить короче.
- Ожидаемый эффект и метрика: в `writer_report.controlled_enrichment` видны `model_enriched`, `short_but_complete`, `held_thin_evidence`; HTML без добивочных «следите/проверьте детали».
- Файлы/места: `src/news_digest/pipeline/writer.py` (`_recover_soft_draft_line`, `_model_recover_section_line`, `controlled_enrichment` report).
- ПРОВЕРКА (после прогона): offline — `py_compile` OK; focused tests OK: controlled structured event stays short without filler; thin evidence strips «Проверьте детали» instead of padding; existing borderline render contract preserved. Прод-проверка — следующий прогон.

### 0042 — Кэш перевода не отдаёт короткое как есть — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн (smoke + unittest llm_rewrite), прод — следующий прогон. Гейт `_cached_line_meets_contract` перед reuse в `_apply_translation_memory`. Где была ошибка первой версии: использовал полный `_draft_line_quality_errors`, который включает числовую проверку против сегодняшнего evidence — валидная кэш-проза ложно отклонялась при тонком re-fetch (риск обнуления кэша). Сужено до чисто структурных проверок: block-aware длина (short-блоки пропускаются), ≥2 предложений для прозы, mixed-script, штамп-хвост (scrub_vague_ending). Проверено: валидная проза reused, короткие билеты reused, stub/штамп → cache miss.
- Проблема: футбольные карточки пришли из кэша одним предложением и были добиты штампом.
- Причина (корень): `_apply_translation_memory` (`llm_rewrite.py:2910-2954`) присваивает `draft_line` из кэша без проверки длины/контракта блока (строка 2936).
- Решение: при попадании в кэш проверять, проходит ли строка контракт блока; если нет — не переиспользовать, гнать через обогащение (0041).
- Ожидаемый эффект и метрика: `rewrite_inventory` — `translation_memory` карточки не ниже контракта; кэш-мисс осознанный.
- Файлы/места: `llm_rewrite.py:2910-2954`.
- ПРОВЕРКА (после прогона): —

### 0043 — Проверка полноты перевода (пропажа факта, не выдумка) — 2026-07-02
- Статус: внедрено в `main` и runtime-synced (детерминированный слой); прод-проверка на следующем реальном прогоне
- Проблема: история Tinder потеряла severity-деталь «rape fantasy», хотя она была в evidence. Ни один гейт не поймал.
- Причина (корень): все проверки односторонние. Промпт rewrite просит «сохрани всё» + самоаттестация (`llm_rewrite.py:268-274`); `fact_lock.unsupported_fact_tokens` (`fact_lock.py:105`) проверяет русский→evidence (выдумка), не evidence→русский (пропажа); редактор sensitive-проход (`editor.py:171`) ловит добавленную ложь, не потерю.
- Реализация (детерминированное сначала — LLM-судья остаётся семантическим fallback'ом, нового вызова не добавлял):
  - Новый `fact_completeness.py`: обратный fact-lock. Двуязычный лексикон grave-severity концептов (sexual_offence/homicide/death/weapon_violence/acquittal): en-паттерн ⇒ «факт есть в источнике», ru-паттерн ⇒ «сохранён ли в строке». en есть, ru нет ⇒ critical omission (MQM omission). `critical_fact_obligations`, `translation_completeness_review`, `line_satisfies_concept`.
  - `fact_lock.scalar_fact_tokens` — только числа/деньги/даты без Latin-имён (для non-critical диффа; digest обязан сжимать ⇒ warning-only).
  - `pre_send_quality_judge`: `_deterministic_completeness_scan` по shipped-строкам с match по URL; critical omission ⇒ critical_error (`risk=translation`, `suggested_action=repair`) уходит в существующий repair-executor (rewrite из кандидата → иначе строка снимается). После repair `_recount_completeness_recovery` считает `recovered` / `pulled_for_rework` / `still_missing`: замена на reserve или снятие строки не выдаётся за восстановление, но и не остаётся как shipped omission.
- Ожидаемый эффект и метрика: отчёт с `critical_omission_count`/`recovered`/`pulled_for_rework`/`still_missing`; на Tinder-классе «rape fantasy» присутствует (изнасил…) или строка на доработке.
- Файлы/места: `fact_completeness.py` (нов.); `fact_lock.py:97` (`scalar_fact_tokens`); `pre_send_quality_judge.py` (скан+рекаунт+поле); `tests/test_fact_completeness.py`.
- ПРОВЕРКА: `PYTHONPATH=src python3 -m unittest tests.test_fact_completeness tests.test_pre_send_quality_judge` — 13 зелёных; `python3 -m py_compile src/news_digest/pipeline/fact_completeness.py src/news_digest/pipeline/fact_lock.py src/news_digest/pipeline/pre_send_quality_judge.py` OK; Tinder-нейтрализованная строка ⇒ missing_critical=['sexual_offence'], верная строка ⇒ []; benign event не срабатывает; post-repair recount различает recovered и pulled_for_rework.

### 0044 — Редактор: правило пустых концовок + deterministic post-check — 2026-07-02
- Статус: внедрено локально; прод-проверка на следующем реальном прогоне
- Проблема: редактор оставляет штамп, если он «по теме» (футбол/лид), и не может дотянуть фактом.
- Причина (корень): уточнение после проверки кода — evidence уже прикладывается по строкам: routine до 4000 chars, sensitive до 18000 chars. Слабое место было другое: prompt снимал штамп только когда он «не по теме» (`PRE_SEND_RUSSIAN_EDITOR_PROMPT`), а после модельного редактора не было deterministic post-check для финального хвоста.
- Решение: (1) в prompt заменить узкое правило на «любой общий призыв без нового конкретного факта удалить или заменить фактом из evidence», без оговорки «не по теме»; (2) сохранить per-line tiered evidence, не расширять payload до full-всем; (3) добавить final post-check после model rounds и stop-loss: `_strip_empty_editor_ending` снимает только известные generic endings, сохраняет source link и не трогает конкретные действия вроде «проверьте страницу статуса TfGM»; результат пишется в `editor_report.pre_send_russian_editor.empty_ending_post_check`.
- Ожидаемый эффект и метрика: после редактора строки не заканчиваются «следите за обновлениями / проверьте детали / уточните доступность» без факта; в отчёте видны `removed`, `remaining`, examples; полезные конкретные CTA не удаляются.
- Файлы/места: `src/news_digest/pipeline/editor.py` (`PRE_SEND_RUSSIAN_EDITOR_PROMPT`, `_strip_empty_editor_ending`, `_apply_empty_ending_post_check`, `_pre_send_polish_sections`); `tests/test_editor_pacing.py`.
- ПРОВЕРКА (после прогона): offline — `py_compile` OK; `tests.test_editor_pacing` OK. Регрессии: generic ending strips + preserves link; concrete TfGM status action stays unchanged; report counts removed rows. Прод-проверка по `editor_report.pre_send_russian_editor.empty_ending_post_check` — после следующего реального прогона.

### 0045 — Лифты/эскалаторы не публиковать (только влияние на движение) — 2026-07-02
- Статус: внедрено локально; прод-проверка на следующем реальном прогоне
- Проблема: owner хочет только новости, влияющие на движение; лифты не нужны. В выпуске Firswood и Queens Road (лифты) опубликованы.
- Причина (корень): гард `writer.py:1678` держит лифт только если `_LIFT_ESCALATOR_RE` И НЕ `_TRANSPORT_MOVEMENT_RE`. Но `_TRANSPORT_MOVEMENT_RE` (`writer.py:1798`) включает слова-локации `tram|rail|station|stop|platform|трамва|остановк` — любой алерт про лифт на остановке их содержит → «нет движения»=ложь → гард не срабатывает.
- Решение: введён отдельный признак `transport_movement_impact` = только слова сбоя (cancel/delay/no service/replacement/closure/diversion/strike + RU). Транспортная карточка с lift/escalator/step-free/accessibility и БЕЗ признака сбоя движения дропается в validator с `transport_accessibility_only_no_movement`, до writer.
- Почему так (отвергли): оставить hold в writer — поздно и не отсекает; чинить только регэксп — полумера, owner хочет исключение класса.
- Ожидаемый эффект и метрика: в выпуске 0 карточек про лифт/эскалатор без сбоя движения.
- Файлы/места: `src/news_digest/pipeline/candidate_validator.py` (`transport_movement_impact`, `_exclude_transport_accessibility_only`).
- ПРОВЕРКА (после прогона): offline — Firswood/Queens Road-style lift/escalator → drop; no trains/replacement bus and station closure → keep; focused tests OK. Прод-проверка — следующий прогон.

### 0046 — Профсобытия: матч смыслом, не словами — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн — `PYTHONPATH=src python3 -m unittest tests.test_professional_events` (16 tests OK), прод — следующий прогон.
- Проблема: подходящая по смыслу конференция может не иметь слов из резюме и не дойти до модели. 25 из 42 не дошли до CV-модели вообще.
- Причина (корень): keyword-скорер гейтит доступ к модели; кап `LLM_MATCH_MAX_CANDIDATES=16` (`professional_events.py:21`); held → `needs_llm_cv_match` (`professional_events.py:249-258`).
- Решение: Stage 1 deterministic eligibility (реальное событие, trustworthy date, place/online/source-locality, URL, не programme/listing page); Stage 2 отправляет всех eligible в CV-модель батчами. Keyword оставлен как ordering/diagnostic signal, но не финальный score и не visible-гейт.
- Ожидаемый эффект и метрика: `needs_llm_cv_match` (не дошедшие) → ~0; события без keyword-overlap, но подходящие, проходят.
- Файлы/места: `professional_events.py` (`LLM_MATCH_BATCH_SIZE`, `_professional_event_has_minimum_facts`, `_run_professional_cv_match`).
- ПРОВЕРКА (после прогона): offline OK; prod pending.

### 0047 — Профсобытия: показывать реальное обоснование модели — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн (unittest профсобытий), прод — следующий прогон. `professional_events.py:566` теперь `reason → why → шаблон`. Диагностическое поле; видимую карточку не меняет (её пишет rewrite-промпт) — это отмечено честно.
- Проблема: `why_this_fits_aleksei` идентичен у всех — шаблон. Реальное per-событийное рассуждение модели (`professional_llm_match.reason`) выбрасывается.
- Причина (корень): `professional_events.py:566` берёт `llm_match["why"] or <шаблон>`; модельный `why` пустой → шаблон.
- Решение: брать `reason`/`why` модели; шаблон только если модель реально пуста.
- Файлы/места: `professional_events.py:553,566`.
- ПРОВЕРКА (после прогона): —

### 0048 — Профсобытия: убрать требование бесплатности / честная метка доступа — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн — `PYTHONPATH=src python3 -m unittest tests.test_professional_events` (16 tests OK), прод — следующий прогон.
- Проблема: 2 из 3 показанных — `free_access_status=unknown` в секции «Бесплатные»; требование free — затык.
- Причина (корень): секция обещает бесплатность, а гейт/данные это не держат (`professional_events.py` free_access_*).
- Решение: секция переименована в нейтральную `Business/tech события для тебя`; карточка получает `access_label` (`free`/`paid`/`unknown`/`booking_required`); paid/unknown публикуются только при CV `go` или сильном `consider` + полной дате/месте; квота не расширена.
- Файлы/места: `professional_events.py`, `common.py`, `writer.py`.
- ПРОВЕРКА (после прогона): offline OK; prod pending.

### 0049 — Профсобытия: согласовать гейты отбора — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн — `PYTHONPATH=src python3 -m unittest tests.test_professional_events` (16 tests OK), прод — следующий прогон.
- Проблема: go/100 (Networking North) выпал из общей доски-90 и куратора, а consider/51 (event_level=reject, Pro-Manchester) прошёл.
- Причина (корень): 3 несогласованных гейта поверх CV-вердикта: `Outside DeepSeek ranking board (soft max 90)` (`llm_rewrite.py:1302`), `Curator drop: чистый PR` (`curator.py`), секционный кап=3.
- Решение: `skip` never visible; `go` сортируется выше `consider`; professional cap=3 режет только model-approved внутри своего блока; уже выбранные CV-approved профсобытия защищены от общей hard-news board и финальной Russian-board отсечки. Capacity/date/duplicate/access-facts причины пишутся в candidate reason / held examples.
- Файлы/места: `professional_events.py`, `llm_rewrite.py`, `writer.py`.
- ПРОВЕРКА (после прогона): offline OK; prod pending.

### 0050 — Валидация URL при извлечении (сырой HTML в href) — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн (golden unittest на реальном CONEXEN-кейсе + smoke), прод — следующий прогон. `valid_http_url`/`first_valid_http_url` в common.py; фолбэк в `_jsonld_event_node_to_item` (extract.py:351) + санитайзинг source_url при создании кандидата. Нормальные URL (query `&`, фрагмент, percent-encoding) сохраняются; `clean_url` не тронут (он ключ dedup). Тест `tests/test_market_event_sources.py::JsonLdUrlSanitizationTest`.
- Проблема: у CONEXEN href = `https://events.compiledmcr.com/<p><strong>Registration needed...`.
- Причина (корень): `extract.py:351` `event_url = _jsonld_text(node.get("url")) or ...`; JSON-LD `url` содержал HTML; `urljoin` (`:352`) приклеил, `clean_url` (`:3180`) не отбраковал.
- Решение: в `clean_url` — проверка формы: обязателен `^https?://`, запрет пробелов, `<`, `>`, `&lt;`/сущностей; при провале откат booking_url→source.url, иначе карточка без ссылки не публикуется.
- Ожидаемый эффект и метрика: 0 href с `<`/сущностями/пробелами в HTML.
- Файлы/места: `collector/extract.py:351-352,3180`; `clean_url`.
- ПРОВЕРКА (после прогона): —

### 0051 — Единая шкала ранжирования (доска смешивает 2 шкалы) — 2026-07-02
- Статус: внедрено локально; ПРОВЕРКА офлайн — `PYTHONPATH=src python3 -m unittest tests.test_professional_events` (16 tests OK), прод — следующий прогон.
- Проблема: модель оценила 66 из 375; у 36 вердикт `none` — профсобытия с keyword-100 забивают верх доски выше реальных новостей (Бёрнэм 90), но режутся секционным капом.
- Причина (корень): `_rewrite_shortlist_priority` (`llm_rewrite.py:1051`) использует `english_editorial_score`, куда профсобытиям пишется keyword `fit_score` без модельного вердикта (`professional_events.py:564,572`).
- Решение: введены `score_value`, `score_source`, `score_scope`, `score_verdict`; deterministic keyword пишет `score_source=keyword`, CV-модель пишет `score_source=model`; professional ranking читает свою шкалу и не использует keyword/CV score как `english_editorial_score` news-board score. Held reports показывают `not model scored`/provenance.
- Файлы/места: `professional_events.py`, `llm_rewrite.py`, `writer.py`.
- ПРОВЕРКА (после прогона): offline OK; prod pending.

## Корректировка плана 0039–0052 после архитектурной рецензии — 2026-07-02

Рецензия owner'а (внешний разбор) проверена по коду; принятые поправки к записям выше (сами записи не переписываются — уточнения действуют поверх):

- **0039 (судья):** формулировка меняется на **Map/Reduce**: map = чанки (секция целиком, крупные секции резать по ~10-12 видимых строк; payload = строка + compact facts, БЕЗ summary/lead/700+700), reduce = отдельный маленький вызов на весь выпуск (баланс секций, дубли между чанками, must_show/lead). Общий token-bucket limiter (переиспользовать паттерн `llm_rewrite`), старт 2-3 workers. Упавший чанк ⇒ `status=partial`, не «ok».
- **0040 (сеть):** подтверждено по коду — `_deterministic_action_post_check` вызывается только в success-path (`pre_send_quality_judge.py:996`); в 429-пути отчёт содержит `deterministic_post_check: null`. Фикс: вызывать сканер и в except-пути. Авточинить только механику (href, хвосты, mixed-script); смысловое — report-only.
- **0041 (обогащение):** не «каждую короткую в модель без границ», а controlled recovery: классы (важная новость → enrichment; событие/билет со структурированными фактами → short-but-complete; тонкий evidence → честно коротко), общий limiter + cap на прогон, после обогащения — fact-lock и запрет generic-концовки. В отчёт: `model_enriched` / `short_but_complete` / `held_thin_evidence`.
- **0042 (кэш):** контракт длины проверять **по блоку**, не общий 150 (иначе сломаются короткие tickets/weekend/russian). Кэш v2: хранить prompt_version/contract_version/block_key; несоответствие ⇒ cache miss.
- **0043 (полнота):** staged: сначала sensitive/hard-news (крим/суд/транспорт/civic), обязательства = 1-3 reader-critical факта; сначала детерминированное покрытие entity/number/date, LLM-семантика только где детерминированное не решает. Дайджест обязан сжимать — проверяются только critical obligations (MQM: omission — отдельный класс ошибки; QAGS-паттерн).
- **0044 (редактор):** мой диагноз был частично устаревшим — routine-строки УЖЕ получают evidence до 4000 chars (`editor.py:42`), sensitive до 18000 (`:41`). Реальный фикс: (1) промпт-правило «любой общий призыв без конкретного факта — удалить или заменить фактом», без оговорки «не по теме»; (2) compact facts каждой строке вместо full-всем; (3) детерминированный empty-ending post-check после редактора.
- **0046 (проф. смыслом):** двухступенчато: Stage 1 детерминированная eligibility (реальное событие, дата, место, ссылка, не programme page) → Stage 2 LLM CV-fit на ВСЕХ eligible батчами. Keyword — только candidate generation, не финальный балл. Убрать кап без eligibility нельзя — модель будет жевать мусор.
- **0047 (обоснование):** приоритет наружу: `professional_llm_match.reason` → `why` → шаблон. Подтверждено данными: `reason` различен и полезен, `why` = шаблон.
- **0048 (бесплатность):** не «снять фильтр в лоб», а: переименовать секцию (убрать обещание «Бесплатные»), честный access-label (free/paid/unknown/booking required), paid/unknown допускать только при сильном fit + полной дате/месте, квоту блока не расширять.
- **0049 (гейты):** контракт решения: skip никогда не visible; go > consider; keyword не перебивает вердикт модели; cap режет внутри model-approved; drop go — только с причиной capacity/date/duplicate. Отдельная маленькая квота (2-3) вне hard-news доски.
- **0051 (шкала):** сначала score provenance (`score_value`+`score_source`+`score_scope`+`score_verdict`), потом единая отчётность. Не гнать всех через одну модельную шкалу.
- **Порядок реализации (пакеты):** П1 быстрые безопасные: 0050, 0047, 0042, 0040 → П2 контроль: 0039, 0044 → П3 видимый мусор: 0045 + runtime-энфорсер 0052, 0041 → П4: 0043 → П5 связанный ranking-пакет: 0046, 0049, 0048, 0051. F1-контракт (0052-док) уже записан в PRODUCT_CONTRACTS.md.

### 0052 — Контракт маршрутизации для ВСЕХ блоков — 2026-07-02
- Статус: внедрено локально для leisure→next_7_days enforcement; прод-проверка на следующем реальном прогоне
- Проблема: car boot и обеденный концерт попали в «важное за 7 дней»; принципы «что куда» разбросаны и заданы точечно.
- Причина (корень): «Выходные в GM» скрыты Пн-Ср (`writer.py:6786` `show_weekend=weekday>=3`), досуг перетекает в next_7_days; правила размазаны по `PRIMARY_BLOCKS` (`common.py`), block_contract (`writer.py:1665`), reroute (`candidate_validator.py:1732`).
- Решение: контракт уже записан в `docs/PRODUCT_CONTRACTS.md`; runtime-энфорсер добавлен в validator: leisure/event items не могут оставаться в `next_7_days`. Market/car boot/fair сохраняются в `weekend_activities`; ticket-like events reroute в `ticket_radar`; прочий leisure/culture в `next_7_days` получает `leisure_not_next_7_days`.
- Файлы/места: `src/news_digest/pipeline/candidate_validator.py` (`_enforce_leisure_routing_contract`, `_reroute_market_planning_to_weekend` integration).
- ПРОВЕРКА (после прогона): offline — car boot не остаётся в `next_7_days`; lunchtime concert drops from `next_7_days`; ticket event moves to `ticket_radar`; focused tests OK. Прод-проверка — следующий прогон.

### 0053 — Weekend Inventory защищён от ranking/caps/repeat — 2026-07-02
- Статус: внедрено; ПРОВЕРКА офлайн (focused unittest), прод — следующий прогон.
- Проблема: «Выходные в GM» в выпуске 2026-07-02 показал 6 строк из 128 weekend-кандидатов; Rum Festival и часть рынков/ярмарок потерялись на ranking/caps/repeat, Campfield `Every Saturday` не получил конкретную дату.
- Причина (корень): weekend item мог быть `selected_uncapped`, но позже всё равно выпадал на глобальном DeepSeek board cap / final Russian board cap (`llm_rewrite.py`); writer per-section cap считал eligible Weekend items и мог их резать; repeat-policy сравнивал recurring events не как occurrence; event extraction не вычислял простое `every Saturday/Sunday`.
- Решение: отдельный helper `weekend_inventory.py` с узким scope (markets/fairs/festivals/car boots/special public weekend activity, не обычная афиша); weekly recurrence → concrete `event.date_start` + `is_recurring`; eligible Weekend Inventory never-drop на rewrite caps; writer caps не режут eligible inventory; repeat-policy разрешает current-weekend occurrence; writer report получает `weekend_inventory_loss_trace`; release source-status получает `weekend_source_coverage` для parser-empty/no-date/zero-render weekend источников.
- Почему так (отвергли): не расширяем Weekend до всех концертов/театров/ночной афиши; не отключаем dedupe глобально; не строим отдельный календарный storage layer в этом патче.
- Ожидаемый эффект и метрика: `writer_report.weekend_inventory_loss_trace.counts.missing == 0` для eligible collected inventory; Campfield-like weekly pages имеют `event.date_start` ближайшей субботы/воскресенья; `Outside DeepSeek ranking board` не появляется для eligible Weekend Inventory.
- Файлы/места: `src/news_digest/pipeline/weekend_inventory.py`; `event_extraction.py`; `llm_rewrite.py`; `editorial_contracts.py`; `writer.py`; `collector/core.py`; `release.py`; `tests/test_weekend_inventory_contract.py`; `tests/test_source_health.py`.
- ПРОВЕРКА (после прогона): offline unittest only until next prod run.
- Где была ошибка (если не сработало): —

### 0054 — Night Inventory: cron-job.org стал единственным будильником — 2026-07-02
- Статус: внедрено; ПРОВЕРКА — cron-job.org test run `events` вернул `204 No Content`; GitHub `schedule` отключён, `workflow_dispatch` оставлен.
- Проблема: GitHub `schedule` для night inventory сработал с большим лагом: `live_news` и `breaking` пришли уже после утреннего выпуска, поэтому ночной слой не мог помочь 08:00-пайплайну. Плюс GitHub cron живёт в UTC и уже исторически был слабым местом проекта.
- Причина (корень): `.github/workflows/night-inventory.yml` совмещал executor и scheduler; `schedule:` запускал волны сам, а cron-job.org дублировал бы их после настройки внешних jobs.
- Решение: `night-inventory.yml` теперь только `workflow_dispatch` executor. Production schedule вынесен в cron-job.org с timezone `Europe/London`: `00:31 events`, `02:07 tickets`, `03:37 pro_food_russian`, `06:17 live_news`, `07:31 breaking`. Утренний `Manchester Digest` cron-job не трогается.
- Ожидаемый эффект и метрика: ночью ровно 5 `GM Night Inventory` runs с `event=workflow_dispatch`, без `event=schedule`; все завершаются до 08:00 London и коммитят `data/state/inventory/*.jsonl`.
- Файлы/места: `.github/workflows/night-inventory.yml`.
- ПРОВЕРКА (после прогона): после первой полной ночи сверить `gh run list --workflow night-inventory.yml --limit 10`, `data/state/inventory_run_log.jsonl`, и отсутствие scheduled runs.

### 0055 — Фикс 8 регрессий пакета 0039–0053 (полный CI-набор был красный) — 2026-07-02
- Статус: внедрено; ПРОВЕРКА — полный `PYTHONPATH=src python3 -m unittest discover -s tests` (как в `.github/workflows/tests.yml`): 815 тестов, было 15 failures → стало 7. Все 8 регрессий пакета устранены; остаток 7 — старый music-notability/ticket-headliner кластер (`test_public_output_contracts.*`, `test_skiddle...`), красный ещё на baseline `c52cc8d` до пакета, к 0039–0053 отношения не имеет.
- Проблема: у каждого пункта пакета «focused tests OK», но интегральный CI-набор никто не гонял — он падал 8 новыми регрессиями (правило 7: «зелёно у меня ≠ зелёно в CI»; правило 8: guard без учёта существующих кейсов).
- Корень и фиксы:
  - **0052** (5 регрессий): гейт `leisure_routing_contract` стоял раньше event/date-гейтов и рубил всё is_event/culture_weekly в `next_7_days`. Фикс: (а) перенёс гейт ПОСЛЕ `event_schema_completeness` — undated событие теперь дропается с `no_date` (hard), а не мис-лейблится `leisure_not_next_7_days`; (б) `_enforce_leisure_routing_contract` не роняет leisure с конкретной будущей датой (`_has_future_or_concrete_date`) — годовой фестиваль/датированный воркшоп остаются в next_7, дропается только недатированный routine-leisure (обеденный концерт). `candidate_validator.py`.
  - **0041** (1 регрессия): core-новость с достаточным evidence шла в model-enrichment, а при недоступной модели (тест/прод-даунтайм) — дроп. Фикс: детерминированный fallback `_keep_core_card_short` перед дропом (чинит glossary, снимает generic-хвост, оставляет строку честно короткой, если остаются только soft-ошибки длины). Штамп не возвращается. `writer.py`. Тест `test_compact_core_news_card_is_recovered_instead_of_dropped` обновлён: ассерт устаревшего филлера «проверьте сроки» заменён на «строка рендерится коротко БЕЗ филлера» (0041 явно убирает штамп).
  - **0053** (2 регрессии): (а) `calendar_repeat_review` — ветка `current_weekend_inventory_occurrence` затеняла более специфичную `planning_item_reached_weekend` для перехода next_7→weekend; добавлен guard. `editorial_contracts.py`. (б) `_is_actionable_weekend_candidate` был сведён к узкому `is_weekend_inventory_candidate` (требует блок weekend_activities), из-за чего датированное weekend-событие не распознавалось; восстановлена широкая проверка (дата на ближайшие сб/вс ИЛИ weekly-рекурренс), inventory-кейс оставлен. `llm_rewrite.py`.
- Файлы/места: `candidate_validator.py`, `writer.py`, `editorial_contracts.py`, `llm_rewrite.py`, `tests/test_backlog_remediation.py`.
- ПРОВЕРКА: полный `unittest discover` — 7 failures (все pre-existing, вне пакета); 8 бывших регрессий + `tests.test_weekend_inventory_contract` зелёные.

### 0056 — Night Inventory: commit-шаг устойчив к гонке за state-файлы — 2026-07-02
- Статус: внедрено; ПРОВЕРКА — воспроизвёл конфликт в одноразовом git-репо: без `.gitattributes` `git rebase` падает `CONFLICT (content): Merge conflict in data/state/inventory_run_log.jsonl`; с `merge=union` в базовом коммите rebase проходит `exit 0`, обе волны сохранены, маркеров нет.
- Проблема: ран `28585118004` (ручной TEST RUN, волна `breaking`) упал на шаге `Commit inventory to repo` — `git pull --rebase --autostash` не смог слить две append-строки в `inventory_run_log.jsonl`, когда параллельная волна успела запушить main между checkout и push → exit 1 → Telegram «волна упала».
- Причина (корень): `inventory_run_log.jsonl` — чистый append (`open("a")`); при расхождении соседние дописанные строки git видит как конфликт. `git pull --rebase` поверх append-only JSONL хрупок к любому продвижению main (параллельная волна или утренний build в той же `concurrency: digest-state`).
- Решение: (1) `.gitattributes` — `merge=union` для `data/state/inventory_run_log.jsonl` и `data/state/inventory/*.jsonl`: git сохраняет строки обеих сторон вместо конфликта; дубли в `inventory/*.jsonl` самозалечиваются на следующем `merge_inventory` (upsert по fingerprint). (2) commit-шаг обёрнут в bounded retry (5 попыток `pull --rebase && push` с backoff) — гасит гонку push, не роняет волну на транзиенте.
- Требование к деплою: `.gitattributes` должен попасть в main ДО ближайшей ночи (union-драйвер читается из дерева на момент rebase; если атрибут придёт позже расхождения — не применится).
- Файлы/места: `.gitattributes` (новый), `.github/workflows/night-inventory.yml` (шаг `Commit inventory to repo`).
- ПРОВЕРКА (после ночи): в логах волн нет `CONFLICT`/`could not apply`; при наложении — строка `Pushed inventory on attempt N`.

### 0057 — Остаток из 7 pre-existing падений (не из пакета) — весь набор зелёный — 2026-07-02
- Статус: внедрено; ПРОВЕРКА — полный `PYTHONPATH=src python3 -m unittest discover -s tests`: 815 тестов, 0 failures.
- Проблема: после 0055 оставалось 7 красных тестов старого music-notability/ticket-headliner кластера + skiddle-заголовок; красные ещё до пакета 0039–0053.
- Корень и фиксы:
  - **6 notability-тестов** (`test_public_output_contracts`): time-bomb в фикстуре, не баг продукта. `_ticket_notability_cache` хардкодил `checked_at="2026-06-02"`; окно recheck в `_artist_notability` = 30 дней (`ticket_notability.py:560`). Как только реальные часы прошли 30 дней (сегодня 2026-07-02), кэш-записи протухали → `tier="unknown"`. Фикс: `checked_at = (now_london() - 1d)` — фикстура всегда внутри окна. `tests/test_public_output_contracts.py`.
  - **skiddle-заголовок** (`test_market_event_sources`): `_enrich_item` при успешном deep-fetch перезаписывал чистый заголовок карточки (из `<img alt>` = «Event at Venue») заголовком дочерней страницы (без venue) — `preserve_listing_title` не включал Skiddle (`extract.py:917`). Фикс: добавил `Skiddle Manchester`/`Skiddle Manchester Bank Holiday` в allow-list — тот же паттерн, что RNCM/HOME. Заголовок карточки сохраняется, deep-enrichment всё равно тянет факты. Сеть-независимо.
- Файлы/места: `tests/test_public_output_contracts.py`, `src/news_digest/pipeline/collector/extract.py`.
- ПРОВЕРКА: полный `unittest discover` — 815 pass, 0 fail.

### 0058 — CI time-bomb: датированный weekend-тест хардкодил прошедшую субботу — 2026-07-07
- Статус: внедрено; ПРОВЕРКА — `unittest discover`: 815 pass, 0 fail (был 1 fail с 2026-07-04).
- Проблема: `test_dated_weekend_event_is_budget_exempt` падал начиная с 2026-07-04. На 02.07 (0057) был зелёный.
- Причина: фикстура хардкодила `event.date_start="2026-07-04"`; `_is_public_budget_exempt`→`is_weekend_inventory_candidate`→`has_current_weekend_occurrence(now_london())` — после прохода даты событие перестаёт быть текущим weekend. Тот же анти-паттерн, что 0026/0057, но новый (пришёл с weekend-тестами 0053).
- Решение: вычислять ближайшую субботу от `now_london()` вместо хардкода. `tests/test_publish_plan_contract.py`.
- ПРОВЕРКА: тест зелёный в любой день недели; полный набор 815/0.

### 0059 — Потерянный лид восстанавливается в лид-блок, а не демотируется в «Свежие» — 2026-07-07
- Статус: внедрено; ПРОВЕРКА на реальном артефакте (выпуск 2026-07-02) + 815/0.
- Проблема: в отправленном 02.07 `lead_visible=false` — блок «Главная история дня» пуст, а curator-лид (Бернхэм, BBC) стоял обычной строкой в «Свежие» рядом со своим твином (About Manchester) = видимый дубль.
- Причина (корень): writer не отрисовал лид (кандидат пришёл `status=drop` из-за дубля fingerprint), а must_show-recovery (`release_reconcile.py:147-170`) вставляла лид как `•`-bullet в его `primary_block` (last_24h→«Свежие»), игнорируя `is_lead`. Заголовок лид-блока в HTML отсутствовал, а `insert_bullets_after_section` его не создаёт.
- Решение: `_candidate_lead_line` (жирная первая фраза + источник, без bullet) + `_insert_or_create_lead` (вставляет в существующий лид-блок или создаёт его сразу после титула). В must_show-цикле `is_lead`-кандидат идёт по лид-пути, а не bullet'ом в секцию.
- ПРОВЕРКА: реконструирован pre-recovery draft из реального `current_digest.html` (убрана recovery-строка) + реальный кандидат Бернхэма → `lead_visible=True`, лид-блок вверху жирным с источником, 0 дублей в «Свежие». Общий набор 815/0.
- Осталось: семантический твин (About Manchester vs лид) — writer-level story-clustering, требует прод-прогона; не в этом заходе.

### 0060 — Гео: билет вне GM (Ньюкасл) не попадает в GM-секцию «Дальние анонсы» — 2026-07-07
- Статус: внедрено; ПРОВЕРКА на реальных кандидатах + 815/0.
- Проблема: в «Дальних анонсах» 02.07 висел фестиваль в Ньюкасле (Lost Minds, Exhibition Park) и события «O2 City Hall Newcastle» с `venue_scope=unknown`.
- Причина (корень): (1) `resolve_venue_scope` применял `_OUTSIDE_GM_PLACE_TOKENS` (где есть newcastle) к `explicit_location`/title, но НЕ к `venue_text` — а у ticket-фидов city/borough часто пусты, город сидит в venue; (2) `future_announcements` назначался билету без учёта гео.
- Решение: (1) добавлена проверка place-token по `venue_text` в `resolve_venue_scope`; (2) при назначении `future_announcements` scope=`outside` → `outside_gm_tickets` («Крупные концерты вне GM», где показывается только A-tier). `candidate_validator.py`.
- ПРОВЕРКА: `resolve_venue_scope` на реальных «O2 City Hall Newcastle» → `('outside','Newcastle')`; GM-venue (Bridgewater) остаётся `GM` (без регрессии); 16 outside-scope из 86 future_announcements реклассифицируются. Набор 815/0.
- Осталось: потеря writer 11→HTML 1 на рендере «Дальних» — оказалось by-design (`_future_announcement_decision`: показывается только окно 8–45 дней, дальше hold), не баг.

### 0061 — Не отправлять голый заголовок секции без содержимого — 2026-07-07
- Статус: внедрено; ПРОВЕРКА на реальном артефакте (выпуск 2026-07-07) + 815/0.
- Проблема: в отправленном 07.07 читатель видел ТРИ пустых заголовка — «Главная история дня», «Еда, открытия и рынки», «Дальние анонсы» — без единой строки под ними.
- Причина (корень): (а) pre-send repair executor снял лид-строку как `stripped_honest_shortfall` (судья: «утверждение о приговоре не подтверждено источником», замену модели fact-lock отклонил), но оставил заголовок лида; (б) Еда/Дальние собрали 0 render-ready карточек. Shortfall честно писался в `still_under_minimum`, но презентационно в HTML оставался голый `<b>Section</b>`.
- Решение: `_strip_empty_section_headings` в финализации `pre_send_quality_judge` (после repair, всегда, не dry-run): убирает заголовок секции без буллета/жирного лида до следующего заголовка. Титул и секции с контентом не трогаются; shortfall остаётся в release-отчёте.
- ПРОВЕРКА: на реальном `current_digest.html` 07.07 — 3 голых заголовка убраны, 12 контентных секций и 42 буллета целы; полный набор 815/0.
- Осталось (контент, не презентация): «Еда»/«Выходные» пусты из-за дедупа/повторов — калибровка отдельно; лид-«корова» (транспорт в лиды) — качество отбора лида, отдельно.

### 0062 — Post-repair добор секций и строгий транспортный контракт — 2026-07-07
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после следующего реального прогона.
- Проблема: 0061 убирал пустой заголовок, но не возвращал контент после того, как pre-send judge снимал строку; «Свежие» могли снова упасть ниже floor уже ПОСЛЕ release-reconcile. Параллельно транспортный контракт 0015/0045 был узким: лифты снялись, но funding/app/police-near-station и completed works оставались в Transport без passenger movement impact.
- Причина (корень): `pre_send_quality_judge` после `_apply_repair_executor` делал только lead/heading hygiene, без повторного same-section recovery. В `candidate_validator._reroute_non_impact_transport` контракт был явно отключён (`return False`), поэтому все не-impact TfGM/инфра/incident карточки продолжали жить в transport.
- Решение: (1) после финального judge repair добавлен `_recover_section_minimums_after_repair`: добирает только уже видимые секции из существующего `_same_section_reserve_line`, значит сохраняет event-window, дедуп, русский lint и source-link; Weekend не добирается Пн-Ср, если скрыт по продукту. (2) `release_reconcile` больше не считает скрытый Пн-Ср Weekend underflow. (3) `_reroute_non_impact_transport` снова стал позитивным контрактом: реальный disruption/closure/delay/replacement stays in Transport, funding/policy/app/incident-near-node без влияния на поездку уходит в `city_watch`. (4) lead arbitration/final hygiene не дают transport/weather/listing стать «Главной историей дня», если есть нормальная городская новость; Today Focus режет старые ретроспективы/приговоры без текущего действия. (5) rewrite translation board поднят 42→50, чтобы глобальный cap не душил section floors раньше, чем local floors успеют сработать.
- Почему так (отвергнутые альтернативы): не создаём отсутствующие optional-блоки ради floor; не отключаем release-gate и не блокируем отправку из-за пустого low-signal блока; не дропаем полезную городскую транспортную политику, а переносим её из Transport в City Radar.
- Ожидаемый эффект и метрика проверки: после pre-send repair `pre_send_quality_report.repair_executor.post_repair_section_recovery.inserted_total` > 0 при recoverable shortfall; `visible_contract_report.still_under_minimum` не содержит Weekend Пн-Ср; Transport не содержит funding/app/police-near-station без cancellation/closure/delay/diversion/replacement/roadworks.
- Файлы/места: `candidate_validator.py:_reroute_non_impact_transport`; `pre_send_quality_judge.py:_recover_section_minimums_after_repair`, `_ensure_lead_present`; `release_reconcile.py:_section_minimum_active`; `curator.py:_is_weak_lead`; `writer.py:_today_focus_candidate_is_eligible`; `llm_rewrite.py:REWRITE_TRANSLATION_BOARD_MAX`; тесты `tests/test_backlog_remediation.py`, `tests/test_pre_send_repair_executor.py`, `tests/test_lead_arbitration.py`, `tests/test_digest_quality_guardrails.py`.
- ПРОВЕРКА: offline — `py_compile` OK; `tests.test_lead_arbitration`, `tests.test_digest_quality_guardrails.DigestQualityGuardrailsTest.test_today_focus_requires_reader_action_not_just_serious_topic`, `tests.test_backlog_remediation.TransportPassengerImpactContractTest`, `tests.test_backlog_remediation.PublishedReviewTest`, `tests.test_pre_send_repair_executor`, `tests.test_publish_plan_contract`, `tests.test_weekend_inventory_contract` OK. Прод-прогон не выполнялся.

### 0063 — P1: не путать скрытый Weekend с missing и сделать 0043 проверяемым — 2026-07-07
- Статус: внедрено; ПРОВЕРКА offline на реальном артефакте 2026-07-07, prod-proof после следующего прогона.
- Проблема: P1-аудит показывал ложные/непроверяемые сигналы: (1) `pre_send_quality_report.translation_completeness.checked_lines=0`, хотя HTML-строки матчились к кандидатам; (2) во вторник `pre_send_quality_report.product_completeness.alerts` писал `Выходные в GM: 0 item(s), emergency floor 3`, а `writer_report.weekend_inventory_loss_trace.counts.missing=6`, хотя Weekend по продукту скрыт Пн-Ср; (3) если writer уже хотел показать optional-секцию, а финальный judge/editor снял её из HTML, post-repair recovery не мог восстановить отсутствующий заголовок.
- Причина (корень): `checked_lines` в 0043 считался только для строк, где сработал grave-concept rule (`review.applies`), а не для всех строк, сматченных к rendered candidate. Weekend floor/trace не учитывали `show_weekend`. `_recover_section_minimums_after_repair` добирал только секции, уже присутствующие в HTML, и не различал "optional hidden" vs "writer intended but later lost".
- Решение: (1) 0043 теперь пишет `checked_lines=matched_lines`, отдельно `applicable_lines` и `unmatched_lines`; (2) pre-send product completeness исключает Weekend из core floors Пн-Ср; (3) `weekend_inventory_loss_trace` получает `show_weekend` и в скрытые дни пишет `hidden_by_schedule`, `missing=0`; (4) post-repair recovery может пересоздать writer-intended секцию с clean same-section reserve, не создавая Weekend Пн-Ср и не фабрикуя строк без резерва.
- Почему так (отвергнутые альтернативы): не объявляем Weekend сломанным в скрытые дни; не подменяем 0043 числом "нашли grave lines" без denominators; не создаём optional-блоки только ради красивого floor, если writer не планировал их показывать.
- Ожидаемый эффект и метрика проверки: на скрытых Weekend-днях нет Weekend emergency alert и `weekend_inventory_loss_trace.counts.missing=0`; `translation_completeness.checked_lines` > 0 при сматченных HTML-строках; `post_repair_section_recovery.created_section=true` только для секций с `writer_report.section_counts[section] > 0` и clean reserve.
- Файлы/места: `pre_send_quality_judge.py:_deterministic_completeness_scan`, `_product_completeness_context`, `_recover_section_minimums_after_repair`; `writer.py:_weekend_inventory_loss_trace`; тесты `tests/test_fact_completeness.py`, `tests/test_pre_send_repair_executor.py`, `tests/test_pre_send_quality_judge.py`, `tests/test_weekend_inventory_contract.py`.
- ПРОВЕРКА: offline — `py_compile` OK; `tests.test_fact_completeness`, `tests.test_pre_send_repair_executor`, `tests.test_pre_send_quality_judge`, `tests.test_weekend_inventory_contract`, `tests.test_professional_events` OK. На реальном `current_digest.html` 07.07: `checked_lines=38`, `matched_lines=38`, `applicable_lines=0`, `unmatched_lines=4`; Weekend alerts `[]`; hidden trace `eligible=6, hidden_by_schedule=6, missing=0`.

### 0064 — P2: cache/inventory observability показывает реальный эффект, а не нули — 2026-07-07
- Статус: внедрено; ПРОВЕРКА offline на реальных state-файлах 2026-07-07, prod-proof после следующего build.
- Проблема: P2-аудит говорил `Prompt cache: cache_hit_ratio 0`, хотя stage-отчёты уже показывали cache hits (`llm_rewrite_report.cost_summary.cache_hit_ratio=0.1473`, `editor_report=0.1181`). Верхний `release_report.cost_summary` обнулял cache tokens и вводил в заблуждение. Night inventory тоже выглядел как "инфра есть, effect слабый", но release report не отвечал, использовал ли 08:00 build inventory вообще.
- Причина (корень): `_aggregate_cost` восстанавливал `CallRecord` из `cost_*.json`, но не прокидывал `cache_hit_tokens/cache_miss_tokens`; `cost_history.json` тоже не сохранял cache totals. Для night inventory в release не было compact status-блока: workflow пишет `data/state/inventory/*.jsonl`, но morning digest still live-collect, а это нигде явно не видно.
- Решение: (1) `_aggregate_cost` сохраняет cache hit/miss tokens из stage cost files; (2) `_append_cost_history` пишет `total_cache_hit_tokens`, `total_cache_miss_tokens`, `cache_hit_ratio`; (3) `release_report.inventory_morning_effect` показывает `morning_consumed=false`, количество inventory-файлов/записей/render_ready, последнюю волну и per-category counts.
- Почему так (отвергнутые альтернативы): не включаем inventory в morning selection в P2-пакете — это уже product-path изменение и риск для выпуска; не объявляем prompt cache сломанным по `release_report`, если stage files доказывают обратное; сначала чиним верхнюю observability.
- Ожидаемый эффект и метрика проверки: `release_report.cost_summary.cache_hit_ratio` совпадает с агрегированными `cost_*.json`; `cost_history.json[-1].cache_hit_ratio` не теряется; `release_report.inventory_morning_effect.morning_consumed=false` до отдельного inventory-first product change, при этом видны `total_records/render_ready_records`.
- Файлы/места: `release.py:_aggregate_cost`, `_append_cost_history`, `_summarise_inventory_morning_effect`; тесты `tests/test_prompt_and_fetch_cache.py`, `tests/test_inventory.py`.
- ПРОВЕРКА: offline — `py_compile` OK; `tests.test_prompt_and_fetch_cache`, `tests.test_inventory` OK. На реальных `data/state/cost_*.json` 07.07: `total_cache_hit_tokens=25216`, `total_cache_miss_tokens=130876`, `cache_hit_ratio=0.1615` вместо `0.0`. На реальном inventory: `inventory_files=11`, `total_records=2366`, `render_ready_records=0`, `last_wave=breaking`, `morning_consumed=false`.

### 0065 — Night inventory fact-ready gate без schema v2 — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после следующей ночи/утра.
- Проблема: night inventory уже различал `quality_status=needs_text`, но morning contract требовал `render_ready=true`, то есть готовую `draft_line`. Из-за этого карточка с полными фактами, но без русского текста, считалась непригодной для утра.
- Причина (корень): `passes_morning_contract` смотрел только `record.render_ready`, хотя `evaluate_card` уже умеет состояние `needs_text` = факты есть, нужен только текст.
- Решение: добавлен `inventory_fact_ready`: `render_ready` или `quality_status=needs_text` с единственным missing `draft_line`. `passes_morning_contract` теперь пропускает такие записи как `morning_relevant_needs_text`, а не требует schema v2. В inventory-record добавлены недостающие для восстановления candidate поля: `title`, `summary`, `lead`, `published_at`, `freshness_status`, `practical_angle`, `draft_line`.
- Почему так (отвергнутые альтернативы): не создаём новую схему v2, потому что базовый split `missing_facts/needs_text/ready` уже есть; не пускаем `missing_facts` в утренний поток.
- Ожидаемый эффект и метрика проверки: после новой ночной волны `release_report.inventory_morning_effect.fact_ready_records` должен быть выше `render_ready_records`, а `report_only_intake.reasons.morning_relevant_needs_text` покажет кандидатов, которые можно утром писать, не собирая заново.
- Файлы/места: `inventory.py:inventory_fact_ready`, `passes_morning_contract`, `build_inventory_record`; тесты `tests/test_inventory.py`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; на реальном inventory 2026-07-09 старые файлы теперь дают `fact_ready_records=41`, `render_ready_records=0`, но все 41 уже `ttl_expired` к моменту проверки.
- Где была ошибка (если не сработало): —

### 0066 — Night inventory per-card enrichment (0066a), без corpus-дедупа ночью — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после следующей ночной волны.
- Проблема: `collect-inventory` писал записи прямо из `_collect_single_source`, до entity/event enrichment. Поэтому event/ticket/weekend карточки часто имели пустые `event_name`, `venue`, `date_start` и не могли стать fact-ready.
- Причина (корень): утренний `collect_digest` после сбора делает per-card enrichment (`enrich_candidates_entities`, `enrich_candidates_events`) и отдельные corpus-level шаги. Ночная волна не делала даже безопасную per-card часть.
- Решение: `cmd_collect_inventory` теперь перед `build_inventory_record` запускает только `enrich_candidates_entities` и `enrich_candidates_events` для кандидатов текущего source. Corpus-level дедуп/кластеры/общая story intelligence оставлены в утреннем пути.
- Почему так (отвергнутые альтернативы): не переносим `apply_cheap_dedup_before_enrich` и `attach_story_clusters` в одну ночную волну, потому что они требуют всего корпуса, а не одного source/wave.
- Ожидаемый эффект и метрика проверки: в `inventory_run_log.jsonl` у новых волн появится `fact_ready`; в `data/state/inventory/*.jsonl` для events/tickets/weekend должны расти `quality_status=needs_text` вместо `missing_facts` по venue/date/event_name.
- Файлы/места: `scripts/run_local_digest.py:cmd_collect_inventory`; тест `NightWaveTest.test_wave_writes_inventory_only_never_candidates`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; тест доказывает, что ночная волна не создаёт `candidates.json`, но пишет в fact_card результат per-card enrichment.
- Где была ошибка (если не сработало): —

### 0067 — Report-only morning inventory intake — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline на реальном inventory 2026-07-09, prod-proof после следующего build.
- Проблема: 0064 показывал, что morning inventory не потребляется, но не отвечал, сколько ночных карточек уже можно было бы безопасно взять в утренний поток и почему остальные не проходят.
- Причина (корень): `_summarise_inventory_morning_effect` считал только files/records/render_ready и последнюю волну. Не было product-фаннела: fact-ready → eligible → converted-candidate → rejected reasons.
- Решение: добавлен `summarise_morning_intake` и вложенный `release_report.inventory_morning_effect.report_only_intake`. Он ничего не меняет в выпуске (`morning_consumed=false`), но считает `records`, `fact_ready`, `needs_text`, `eligible`, `converted_candidates`, причины отказа и примеры eligible.
- Почему так (отвергнутые альтернативы): не включаем inventory-first сразу; сначала измеряем на реальных ночных данных, что бы попало в утро.
- Ожидаемый эффект и метрика проверки: после следующего build в `release_report.inventory_morning_effect.report_only_intake` должны быть ненулевые counts и причины, достаточные для решения, какие блоки включать первыми.
- Файлы/места: `inventory.py:summarise_morning_intake`; `release.py:_summarise_inventory_morning_effect`; тесты `tests/test_inventory.py`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; на реальном inventory 2026-07-09: `records=2726`, `fact_ready=41`, `eligible=0`, reasons: `missing_facts=2685`, `ttl_expired=41`.
- Где была ошибка (если не сработало): —

### 0068 — Inventory record → normal candidate converter — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, пока не подключено к изменению выпуска.
- Проблема: даже если ночная запись fact-ready, утро не имело общей формы, как вернуть её в обычный кандидат, не обходя dedupe/validator/writer.
- Причина (корень): inventory-record хранил fact_card и raw_evidence, но не было функции, которая восстанавливает стандартные поля candidate и помечает provenance (`night_inventory`).
- Решение: добавлен `inventory_record_to_candidate`: восстанавливает `title/summary/lead/source_url/source_label/category/primary_block/evidence_text/event/venue_scope/ticket_type/what_happened/why_now`, ставит `inventory_source=night_inventory`, `inventory_needs_text`, `inventory_requires_refetch` для transport и `include=true`. В текущем пакете используется только в report-only примерах.
- Почему так (отвергнутые альтернативы): не пишем ночные записи сразу в HTML и не создаём второй writer; будущий intake должен идти через обычный candidate vocabulary.
- Ожидаемый эффект и метрика проверки: report-only intake сможет показать examples как нормальные candidates; следующий пакет сможет подключить converter перед dedupe/validate без нового формата.
- Файлы/места: `inventory.py:inventory_record_to_candidate`; тесты `tests/test_inventory.py`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; тест восстанавливает weekend inventory record в candidate с `inventory_source=night_inventory`, `event.venue`, `inventory_needs_text=true`.
- Где была ошибка (если не сработало): —

### 0069 — Block-specific TTL для night inventory — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после следующей ночи/утра.
- Проблема: ночные карточки нельзя оценивать одним сроком свежести. Transport и fresh быстро тухнут, а weekend/tickets/food/pro/russian могут жить дольше, если дата не прошла.
- Причина (корень): `passes_morning_contract` проверял dead/expired/ticket reason, но не ограничивал `last_seen_at` по типу блока.
- Решение: добавлены TTL по `primary_block`: `transport=1h`, `last_24h/today_focus=6h`, `city_watch/tech_business=24h`, `weekend/next7=96h`, `openings/pro/russian/ticket_radar=168h`, `future/outside_gm=336h`. Transport без `render_ready` отдельно возвращает `needs_live_refetch`, а не проходит как обычная утренняя карточка.
- Почему так (отвергнутые альтернативы): не доверяем ночному transport как утреннему факту; не заставляем long-horizon blocks жить по hard-news TTL.
- Ожидаемый эффект и метрика проверки: `report_only_intake.reasons.ttl_expired` должен объяснять старые ночные карточки, а свежие stable-block записи должны проходить как eligible/needs_text.
- Файлы/места: `inventory.py:passes_ttl_contract`, `inventory_ttl_hours`, `passes_morning_contract`; тесты `tests/test_inventory.py`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; на реальном inventory 2026-07-09 старые fact-ready transport-like записи дали `ttl_expired=41`, что подтверждает, что просроченный ночной запас не считается пригодным.
- Где была ошибка (если не сработало): —

### 0070 — Inventory assist/first для стабильных блоков — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после следующей ночи/утра.
- Проблема: ночной inventory оставался отчётом, а стабильные блоки (`Выходные`, `Ticket Radar`, `Еда`) утром всё равно начинали только с live-crawl.
- Причина (корень): `collect_digest` не читал `data/state/inventory/*.jsonl` как candidate source.
- Решение: добавлен morning inventory intake в `collect_digest`. Default `MORNING_INVENTORY_MODE=assist`: eligible stable records (`weekend_activities`, `ticket_radar`, `openings`) добавляются как обычные candidates после live-сбора и до dedupe/validate. `mode=on` готовит inventory-first режим для source skipping.
- Почему так: default не пропускает live scan, чтобы первый prod-день не потерял блоки из-за неполного inventory.
- Ожидаемый эффект и метрика проверки: `collector_report.morning_inventory.actual_intake.inserted_candidates > 0` после свежей ночи; candidates получают `inventory_source=night_inventory`.
- Файлы/места: `collector/core.py:collect_digest`; `inventory.py:build_morning_inventory_intake`; тесты `tests/test_inventory.py`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; на старом inventory 2026-07-09 inserted=0 из-за `missing_facts/ttl_expired`.

### 0071 — Ticket Radar inventory shortlist + cap — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после свежей night tickets wave.
- Проблема: ticket inventory может давать сотни записей; если пустить всё утром, мы вернём 900-кандидатную лавину.
- Причина (корень): inventory intake не имел отдельного cap/shortlist для `ticket_radar`.
- Решение: stable intake сортирует ticket candidates по tier/type и режет `ticket_radar` до 20 записей. Остальные считаются `inventory_block_cap`, не исчезают из inventory.
- Почему так: morning writer/validator видит короткий shortlist + reserve, а не весь билетный каталог.
- Ожидаемый эффект и метрика проверки: `morning_inventory_intake_report.actual_intake.held_by_cap` показывает срез; `ticket_radar.candidate_count <= 20` из inventory.
- Файлы/места: `inventory.py:INVENTORY_INTAKE_CAPS`, `_inventory_candidate_priority`, `build_morning_inventory_intake`; тест `test_ticket_inventory_intake_is_capped`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; synthetic 25 A-tier tickets → 20 inserted, 5 held_by_cap.

### 0072 — Weekend completeness из inventory — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после свежей events wave.
- Проблема: `Выходные` нельзя считать здоровыми только по наличию строк; нужен floor и источник полноты для ночного слоя.
- Причина (корень): release/collector не имели stable-block completeness по inventory candidates.
- Решение: `inventory_stable_block_completeness` считает floor для `weekend_activities` (6), source_count, with_prewrite и complete/incomplete. Collector кладёт это в `morning_inventory_intake_report`.
- Почему так: пока это floor/completeness checkpoint, без попытки заменить весь weekend source coverage.
- Ожидаемый эффект и метрика проверки: после свежей ночи `completeness.blocks.weekend_activities.complete=true/false` объясняет, можно ли доверять inventory-first.
- Файлы/места: `inventory.py:inventory_stable_block_completeness`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; 6 weekend candidates → block complete.

### 0073 — Food/openings inventory резерв и completeness — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после fresh `pro_food_russian` wave.
- Проблема: `Еда, открытия и рынки` часто пустеет, но не было видно, может ли ночной слой закрыть floor.
- Причина (корень): food/openings inventory не входил в morning candidate pool и не имел отдельного floor.
- Решение: `openings` добавлен в stable intake с floor=3 и cap=10. Если ночная карточка fact-ready, она станет обычным candidate; если нет — отчёт покажет `missing_facts`.
- Почему так: не фабрикуем food-текст без фактов; prewrite для food сработает только через существующий event fallback, если структура достаточная.
- Ожидаемый эффект и метрика проверки: `completeness.blocks.openings.candidate_count` и `complete` показывают, спасает ли inventory блок еды.
- Файлы/места: `inventory.py:INVENTORY_ASSIST_BLOCKS`, `INVENTORY_COMPLETENESS_FLOORS`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; старый inventory 2026-07-09 пока даёт `openings.complete=false`.

### 0074 — Transport остаётся live-refetch/hybrid, не inventory-publish — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline.
- Проблема: транспорт нельзя публиковать из ночного склада как текущий факт.
- Причина (корень): после fact-ready gate транспорт мог бы выглядеть как обычный candidate без отдельной защиты.
- Решение: `passes_morning_contract` возвращает `needs_live_refetch` для transport без `render_ready`; `build_morning_inventory_intake` не вставляет transport в stable candidates, а считает его в `hybrid_signals`.
- Почему так: transport должен подтверждаться утром по passenger impact.
- Ожидаемый эффект и метрика проверки: `actual_intake.hybrid_signals.needs_live_refetch`/TTL reasons есть, но transport не попадает в inserted candidates.
- Файлы/места: `inventory.py:passes_morning_contract`, `build_morning_inventory_intake`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; fact-ready transport returns `needs_live_refetch`.

### 0075 — Fresh/lead остаются hybrid/report, не inventory-first — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline.
- Проблема: fresh/lead нельзя выбирать только ночью, иначе лид может быть устаревшим.
- Причина (корень): night inventory содержит `last_24h/today_focus` records, но они должны конкурировать с утренним breaking/live layer.
- Решение: `last_24h`, `today_focus`, `lead_story` добавлены в hybrid-only set: intake считает причины/сигналы, но не вставляет их как stable candidates.
- Почему так: стабильные блоки включаются первыми, fresh/lead после отдельного live-delta дизайна.
- Ожидаемый эффект и метрика проверки: fresh/lead records видны в `hybrid_signals`, но `inserted_candidates` содержит только stable blocks.
- Файлы/места: `inventory.py:INVENTORY_HYBRID_BLOCKS`, `build_morning_inventory_intake`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; `last_24h` synthetic record counted in hybrid, not inserted.

### 0077 — Skip-wide для здоровых night sources в режиме `on` — 2026-07-09
- Статус: внедрено как capability; default не активирует skip (`assist`).
- Проблема: без пропуска широкого утреннего скана night inventory улучшает полноту, но не снимает collect-время.
- Причина (корень): daily collect всегда обходил все sources, независимо от качества night inventory.
- Решение: `MORNING_INVENTORY_MODE=on` позволяет collector skip-wide для `venues_tickets` и `food_openings`, если соответствующий stable block уже complete по inventory. Skipped sources получают source_health `skipped_by_inventory=true`, а не выглядят как падение.
- Почему так: `culture_weekly` пока не skip'аем, потому что он кормит weekend/next7/future сразу; риск выше.
- Ожидаемый эффект и метрика проверки: в режиме `on` `collector_report.categories.*.source_health[].skipped_by_inventory=true`, collect seconds ниже; в default `assist` skip=0.
- Файлы/места: `collector/core.py:_morning_inventory_mode`, `_inventory_skip_source_health`, `collect_digest`.
- ПРОВЕРКА: offline — `py_compile`/`tests.test_inventory` OK; prod включение требует `MORNING_INVENTORY_MODE=on`.

### 0078 — Night prewrite для стабильных structured карточек — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, prod-proof после следующей night wave.
- Проблема: inventory-first без текста всё равно переносит работу в утренний rewrite/writer.
- Причина (корень): `collect-inventory` не пытался использовать уже существующие deterministic writer templates ночью.
- Решение: после night per-card enrichment `cmd_collect_inventory` вызывает `prewrite_stable_inventory_candidate`. Она использует существующие writer fallback templates для tickets/events/pro, пишет `draft_line_provider=night_inventory_prewrite`, а run_log пишет `prewritten`.
- Почему так: не создаём новый слабый phrasebook; если writer fallback не может безопасно написать строку, prewrite не происходит.
- Ожидаемый эффект и метрика проверки: новые night wave logs показывают `prewritten > 0`, а соответствующие records становятся `render_ready=true`.
- Файлы/места: `inventory.py:prewrite_stable_inventory_candidate`; `scripts/run_local_digest.py:cmd_collect_inventory`.
- ПРОВЕРКА: offline — `tests.test_inventory` OK; structured A-tier ticket получает deterministic `draft_line`.

### 0079 — Inventory effect report после collect/release — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline.
- Проблема: после включения assist/on нужно видеть не только общий склад, а фактическое влияние на утренний collect.
- Причина (корень): 0067 был report-only по raw inventory; не было `actual_intake` после dedupe против live fingerprints.
- Решение: collector пишет `data/state/morning_inventory_intake_report.json` и `collector_report.morning_inventory`; release вкладывает его в `inventory_morning_effect.collect_intake`.
- Почему так: forensic truth теперь виден и в collect-stage, и в release-stage.
- Ожидаемый эффект и метрика проверки: `release_report.inventory_morning_effect.collect_intake.actual_intake.inserted_candidates` показывает реальный вклад.
- Файлы/места: `collector/core.py`, `release.py:_summarise_inventory_morning_effect`.
- ПРОВЕРКА: offline — py_compile OK; текущий old inventory даёт inserted=0 и причины `missing_facts/ttl_expired`.

### 0080 — Inventory performance budget — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline, реальные числа после следующего build.
- Проблема: success night inventory нельзя мерить только наличием кандидатов; нужно видеть, падают ли collect/rewrite/total.
- Причина (корень): `speed_report` показывал bottlenecks, но не имел inventory activation targets.
- Решение: `speed_report.inventory_performance_budget` задаёт targets: collect ≤180s, llm_rewrite ≤360s, total_known_stage_seconds ≤1320s, и пишет breaches/status.
- Почему так: цели warning/report-only, не блокируют выпуск.
- Ожидаемый эффект и метрика проверки: после prod build видно, какие стадии ещё превышают цель после inventory activation.
- Файлы/места: `release.py:_build_speed_report`.
- ПРОВЕРКА: offline — py_compile OK; prod-proof после следующего daily build.

### 0081 — Порядок гейта: перемерять visible-contract ПОСЛЕ pre-send repair (FIX-1) — 2026-07-09
- Статус: внедрено; ПРОВЕРКА на реальном артефакте 2026-07-09 + 837/0.
- Проблема: на отправленном выпуске 09.07 `visible_contract_report.json` рапортовал `lead_visible=false` и `Свежие новости=5`, тогда как реально отправлено `lead_visible=true, Свежие=2`. RC1-гейт (0011) описывал не тот HTML, что ушёл читателю → «measure on shipped HTML» нарушено, `product_completeness` завышал (5 vs 2).
- Причина (корень): порядок стадий в `scripts/run_daily_digest.sh` — `build-digest` меряет контракт и промоутит draft→`current_digest.html` (`release.py:4657` `reconcile_visible_html`), ПОТОМ `pre-send-quality-judge` (`pre_send_quality_judge.py`) правит уже промоутнутый файл (strip/patch/добор), но контракт никто не перемеряет. Отчёт остаётся до-repair снимком.
- Решение: в конце `evaluate_pre_send_quality` (после финального write) перегоняем `reconcile_visible_html` на ФИНАЛЬНОМ `current_digest.html` и перезаписываем `visible_contract_report.json`; заодно последний reserve-добор идёт по реальному HTML. Guarded — ошибка рефреша не блокирует отправку.
- Почему так (отвергнутые альтернативы): не переносим весь judge до build (ломает промоушен-гейт и требует LLM внутри build); не дублируем измерение в отдельную стадию — переиспользуем существующий reconcile как источник правды.
- Ожидаемый эффект и метрика проверки: `visible_contract_report.lead_visible`/`html_section_counts` совпадают с реально отправленным `current_digest.html` (перепрогон `_html_section_counts` на файле == отчёт).
- Файлы/места: `pre_send_quality_judge.py:evaluate_pre_send_quality` (после hygiene-блока); `release_reconcile.py:reconcile_visible_html`.
- ПРОВЕРКА: на реальном `current_digest.html` 09.07 — устаревший отчёт `lead_visible=false, Свежие=5`; рефреш на том же файле даёт `lead_visible=true, Свежие=2` (честно ниже floor). Полный набор 837/0.

### 0082 — Repair не стрипает секцию ниже floor в пустоту (FIX-2) — 2026-07-09
- Статус: внедрено; ПРОВЕРКА на реальных данных 09.07 + новый тест.
- Проблема: repair-executor 09.07 снял 3 строки «Свежих» (`stripped: 3`) → секция 5→2 при floor 6, три пустые строки в HTML. Две сняты как «дубль M62/M6», хотя судья в тех же warnings написал «разные дороги, не критично».
- Причина (корень): в `_apply_repair_executor` (`pre_send_quality_judge.py`) при отсутствии model/deterministic/reserve-замены строка безусловно обнулялась (`html_lines[raw_index] = ""` → `stripped_honest_shortfall`), без учёта floor секции.
- Решение: floor-guard перед strip — если секция уже на минимуме/ниже (`_html_lines_for_section` vs `SECTION_MIN_ITEMS`) и замены нет, оригинальная (уже прошедшая writer/editor/release) строка сохраняется (`kept_below_floor_no_reserve`). Исключение — fact-integrity риск (unsupported/fabricat/hallucin/не подтвержд/выдум/не соответствует источнику): целостность важнее floor, такую строку всё равно снимаем.
- Почему так (отвергнутые альтернативы): не сужаем сам семантический дедуп (dedupe_dropped=422 — невидимый pool-churn, риск широкой регрессии); чиним именно видимый вред — schlop секции в пустые строки; не оставляем галлюцинацию ради floor.
- Ожидаемый эффект и метрика проверки: `repair_executor.kept_below_floor` > 0 вместо `stripped`, когда strip уронил бы секцию ниже минимума без резерва; «Свежие» не схлопывается 5→2 пустыми строками.
- Файлы/места: `pre_send_quality_judge.py:_apply_repair_executor` (floor-guard), init `kept_below_floor`; тест `tests/test_pre_send_repair_executor.py:test_strip_below_floor_keeps_original_but_still_drops_unsupported_fact`.
- ПРОВЕРКА: новый тест зелёный — дубль-strip ниже floor сохраняет строку (`kept_below_floor=1`), непроверенный факт всё равно снят (`stripped=1`). Полный набор 837/0.

### 0083 — Шаблонные концовки чистятся на финальном HTML, регекс расширен (FIX-3) — 2026-07-09
- Статус: внедрено; ПРОВЕРКА на реальном выпуске 09.07 + новый тест.
- Проблема: в отправленном 09.07 живьём остались «сверьте детали и условия перед поездкой» (Выходные), «Сверьте часы и условия перед поездкой», «Проверьте часы работы…», «Сроки и объёмы работ уточняйте на странице перевозчика» (Transport) — 0041/0044 не сработали в проде.
- Причина (корень): (а) `_EMPTY_ENDING_RE` (`editor.py`) не ловил «часы/время» и «уточняйте … на странице/перевозчика»; (б) даже пойманные строки уходили в HTML, т.к. `_apply_empty_ending_post_check` видит только `polished`, а weekend/transport/reserve-backfill строки приходят другими путями и не проходят через post-check; (в) `<55c` guard оставлял короткие transport-заглушки только в отчёте.
- Решение: (1) расширил регекс (свер/проверьте + часы|время; отдельные альтернативы для «сроки и объёмы работ уточняйте…» и «уточн(ите|яйте) … на странице|перевозчика»); (2) `_strip_empty_editor_ending(strip_short=True)` — на этапе отправки короткие строки чистятся (пол 20c), а не только репортятся; (3) новый `_strip_empty_endings_in_html` в pre-send hygiene проходит по каждому буллету ФИНАЛЬНОГО HTML, независимо от стадии-источника.
- Почему так (отвергнутые альтернативы): не трогаем `<55c` guard на editor-стадии (там enrichment ещё возможен) — strip_short только для ship-time last-resort; не переписываем регекс целиком, а дописываю альтернативы; конкретные действия («проверьте страницу статуса TfGM», «проверьте карту дорог») остаются нетронутыми.
- Ожидаемый эффект и метрика проверки: в `current_digest.html` нет «сверьте/проверьте … условия перед поездкой» и «уточняйте на странице перевозчика»; `repair_executor.empty_endings_stripped_at_ship` считает снятые.
- Файлы/места: `editor.py:_EMPTY_ENDING_RE`, `_strip_empty_editor_ending` (strip_short); `pre_send_quality_judge.py:_strip_empty_endings_in_html`, вызов в hygiene-блоке; тест `tests/test_editor_pacing.py:test_ship_time_pass_strips_broadened_and_short_boilerplate_endings`.
- ПРОВЕРКА: на реальном `current_digest.html` 09.07 — `_strip_empty_endings_in_html` снял 5 концовок (Выходные×3, Transport×2), ссылки целы, контент сохранён. Новый тест зелёный; полный набор 837/0.

### 0084 — July Weekend direct-source recovery — 2026-07-09
- Статус: внедрено; ПРОВЕРКА offline.
- Проблема: `Выходные в GM` не мог дать полный список 11–12 июля: часть user-visible событий не была в registry, Pedddle challenge pages превращались в no-date кандидатов, а editor мог снять weekend строку без замены.
- Причина (корень): broad guides (`Visit Manchester`, Secret/Finest) не заменяют прямые pages для рынков/фестивалей; `html_page_event` принимал bot-challenge HTML как страницу события; `_apply_editor_line_actions` сохранял безрезервные removal-запросы только для транспорта, но не для Weekend Inventory.
- Решение: добавлены прямые weekend sources `Manchester Brick Festival`, `Foodies Festival Tatton Park`, `Festwich`, `Prestwich Makers Market`; `html_page_event` теперь возвращает 0 items для bot-challenge shells; final editor держит weekend row, если requested removal не имеет same-section replacement.
- Почему так: reader получает больше реальных weekend-вариантов из прямых страниц, а сломанная страница становится видимой source-health проблемой вместо фальшивого no-date события.
- Ожидаемый эффект и метрика проверки: новые direct sources дают candidates для weekend funnel; Pedddle challenge не попадает в candidates; `writer_report.weekend_inventory_loss_trace.counts.missing` не растёт из-за editor strip без резерва.
- Файлы/места: `data/sources.toml`; `collector/extract.py:_extract_html_page_event`; `editor.py:_apply_editor_line_actions`; тесты `tests/test_source_parser_resilience.py`, `tests/test_editor_pacing.py`, `tests/test_digest_quality_guardrails.py`.
- ПРОВЕРКА: offline — targeted tests/source probes; prod-proof после следующего weekend collect.

### 0085 — Weekend funnel replay closure: recurrence dates, false repeats, visible duplicates — 2026-07-09
- Статус: внедрено; ПРОВЕРКА replay 2026-07-07..09 + targeted tests.
- Проблема: после восстановления источников `Выходные в GM` всё ещё терял реальные варианты 11–12 июля: recurring car boot pages с прошлым schema.org date падали как stale/outside-window, Bowlee мог отбиться как повтор чужой planning story про housing, а один и тот же weekend event мог выйти двумя строками из разных источников.
- Причина (корень): recurrence-сигналы источников были шире, чем parser/policy (`SaleDates:`, `Dates: From Saturday`, `Dates: Every Sunday`, `Sundays and Bank Holiday Mondays`); writer имел собственную копию weekend-window logic вместо общего protected inventory helper; topic lifecycle сравнивал current weekend inventory с published planning/news story по ошибочному topic key; visible weekend rows не схлопывали same date + same venue + same event-family дубли.
- Решение: `weekend_inventory.weekend_occurrence_date` стал общей датой планирования для writer/repeat policy; validator/editorial contract распознают реальные schedule-форматы Manchester Rocks/market pages; dedupe не считает current weekend inventory compatible с planning/news published fact; writer схлопывает видимые weekend-дубли по date+venue+event tokens. Удалён локальный weekend-window механизм writer (`_current_weekend_start`, `_current_weekend_end`, `_is_late_may_bank_holiday`) — теперь используется общий `current_weekend_window`.
- Почему так: это чинит весь класс recurring weekend inventory с устаревшим structured date, не только Bowlee/Barton; при этом не отключает dedupe для настоящих повторов событий и не публикует старые одноразовые майские/июньские события.
- Ожидаемый эффект и метрика проверки: recoverable recurring losses в weekend replay становятся 0 после фикса; challenge shells остаются 0 candidates; visible duplicate rows не проходят в финальный weekend block.
- Файлы/места: `weekend_inventory.py`, `candidate_validator.py`, `editorial_contracts.py`, `repeat_policy.py`, `dedupe.py`, `writer.py`, `scripts/replay_weekend_defects.py`; тесты `tests/test_weekend_inventory_contract.py`, `tests/test_digest_quality_guardrails.py`.
- ПРОВЕРКА: `PYTHONPATH=src python3 scripts/replay_weekend_defects.py` на 2026-07-07..09: date-loss `6->0`, false-topic `1->0`, duplicate rows `11->0`, challenge candidates `7->0`; `tests.test_weekend_inventory_contract` OK; targeted `DigestQualityGuardrailsTest` recurrence/topic tests OK; `tests.test_source_parser_resilience` OK; py_compile OK.
