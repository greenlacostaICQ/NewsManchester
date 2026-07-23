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

### 0097 — удалён Ticket balance guard — 2026-07-20
- Статус: развёрнуто в `main`; следующий утренний production-proof ожидается.
- Проблема: билеты душились до 4 при «тонких» core-разделах (19.07: 8→4, из них 3 — не повторы, а жертвы гарда; A-tier не задет). Правило-рудимент эпохи ремонтов: резервировало место под добор, который теперь гарантирует планёрка.
- Решение: гард удалён целиком из plan_digest (+ неиспользуемая константа CORE_UNDERFLOW_TICKET_CAPS из writer). По решению owner 2026-07-20 («показываем всё»).
- ПРОВЕРКА: replay 18/19/20 без гарда: Ticket Radar 7/4/1 (по реальному пулу дня, без искусственного потолка), verify clean все три дня, lead 3/3, дырок 0. Тесты 842: 4 падения — все воспроизводятся на чистом origin/main (823fed1).
- Файлы/места: plan_digest.py, writer.py; артефакты передачи — docs/stage3-handover/.



### 0096 — hotfix: контентные prose-маркеры не блокируют выпуск — 2026-07-18
- Статус: внедрено
- Проблема: выпуск 2026-07-18 НЕ отправлен: release gate упал с единственной ошибкой «bad editorial prose marker: это событие подчеркивает» (Actions run 29634998854). Одна штампованная фраза в одной строке лишила читателя всего выпуска.
- Причина (корень): BAD_EDITORIAL_PROSE в `_validate_draft` пишет в errors (fail-closed) — вопреки правилу owner'а «контент никогда не блокирует отправку» (2026-06-16); маркеры добавлены в a0ae428.
- Решение: маркерный цикл переведён errors → warnings. Технический гейт не тронут.
- Ожидаемый эффект: повторный запуск workflow отправляет выпуск; фраза видна в warnings.
- Файлы/места: `release.py:_validate_draft` (BAD_EDITORIAL_PROSE loop).
- ПРОВЕРКА: перезапуск daily-digest 2026-07-18 после фикса — выпуск отправлен (см. run после этого коммита).


### 0095 — Этап 3: состав выпуска решается один раз (слот-план) — 2026-07-17
- Статус: развёрнуто в `main`; следующий утренний production-proof ожидается.
- Проблема: состав решали ~9 точек конвейера, поверх — 10 ремонтных механизмов, переделывавших друг друга. Живой пример 2026-07-15: release вырезал 5 повторов + 3 брака, reconcile вставил 1 строку, судья применил 4 правки — и «Городской радар» всё равно ушёл 3/5 при 1101 кандидате в backup_pool. Воронка 319 (publish_plan) → 44 (writer) → 37 (HTML). Golden-дефекты: lead missing (07-02), lead empty + пустые строки (07-07), пустые строки + штампы (07-09).
- Причина (корень): не существовало единого утверждённого плана выпуска; каждый этап пере-решал состав, «запасные» жили в 4 несовместимых карманах (public_reserve/backup_pool_only/recoverable_reserve/backup_candidate), ремонты (reconcile/quarantine/floors/backfill/editor reserve) конфликтовали.
- Решение: граница rank-digest → plan-digest → llm-rewrite. Планёрка (plan_digest.py, детерминированная, без сети/LLM) один раз фиксирует неизменяемый слот-план: слоты+цепочки запасных (3 стержневым, 2 остальным), lead + 2 дублёра из-под границы отбора, лимиты/бюджет/повторы/межсекционный дедуп историй, недоборы с причиной; при пуле<min планёрка сама повышает пригодных запасных. Писатель рендерит строго план (лестница: обогатить→пересобрать→заменить запасным слота→снять по коду причины; общий бюджет ремонтов в plan_execution). Редактор и судья — только слова; безнадёжная строка → замена из цепочки слота; снятие только fact-integrity с записью. Release — verify-only + сводка исполнения. Новый финальный этап verify-digest-plan после судьи: контентные расхождения = warnings/ship_degraded, технически негодный артефакт (нет плана/чужой run_id/стухшая дата/битый HTML) блокирует отправку. publish_plan.json остаётся read-only зеркалом плана для ночного контура.
- Удалено: release_reconcile.py целиком; в release — оба карантина HTML, force-remove, зачистка пустых заголовков, backup_pool; в editor — _PrevalidatedReservePool/_same_section_reserve_line/_reserve_insert_allowed/_transport_replacement_for_line/block-actions/трим-секции; в writer — floor pull-back, allocate_fresh_and_today_focus, final_section_role_routing, budget-slicing, emergency floors, PROTECTED_RECOVERY_SECTIONS, loss-traces, старые publish_plan-хелперы; в llm_rewrite — post-board translation cut, RESERVE_PREWRITE, генератор publish_plan; в judge — reserve_replacement/recover_section_minimums/ensure_lead/insert_bullets; в common — is_recoverable_reserve/recoverable_reserve_eligible. Тесты удалённых механизмов удалены/переписаны; добавлен tests/test_plan_contract.py (7 контрактных случаев).
- Почему так (отвергнуто): упорядочивание ремонтов по приоритету (остаётся N источников правды); LLM-планировщик (недетерминизм, ломает replay); план после llm-rewrite (translation cut оставался бы вторым решателем — рецензия п.1).
- Ожидаемый эффект и метрика: на golden-днях 0 пустых строк, lead виден на всех дефектных днях, каждый недобор с причиной в plan_execution/verify-отчётах; live-паттерн «вырезали 8 → вставили 1 → допатчили 4» невозможен (код удалён).
- Файлы/места: plan_digest.py, plan_execution.py, verify_digest_plan.py (новые); llm_rewrite.py (run_rank_digest + пишущая половина по плану), writer.py (write_digest по слотам, produce_replacement_for_slot, lead soft-accept + date-token strip), editor.py, release.py, pre_send_quality_judge.py, common.py, run_local_digest.py, daily-digest.yml, replay_day.py, AGENTS.md.
- ПРОВЕРКА (offline, до боевого прогона): полный конвейер офлайн на состоянии 2026-07-16: план 59 слотов → writer 48 строк (1 замена, 12 снятий с кодами) → editor → release promoted → verify: 47 shown / 1 replaced / 12 removed, 0 расхождений, 0 строк вне плана, 0 пустых буллетов, lead visible. «Городской радар» в плане 5/5 (в проде утром было 3/5). Тесты: 828 прошли; 4 падения унаследованы с main (ночной контур inventory + tram-card, воспроизводятся на main без этой ветки). Replay --golden (12 дней, offline, socket-blocked): lead_status ok 12/12 (в sent: missing 07-02 и 07-03, empty 07-07); max_blank_run=1 на всех днях (в sent до 3); blank_runs_2plus=0 на всех (в sent 4 дня с прогонами пустых); каждое снятие слота — с кодом причины. Baseline старого кода на тех же снапшотах терял lead в 3 репле-днях (07-02/07-09/07-12). Один харнесс-фикс по ходу: stages_ok ожидал 3 стадии, теперь 4 (plan-digest). ПОСЛЕ интеграции origin/main (merge 9e29393, конфликты writer/release/common решены в пользу плановой архитектуры, football-роутинг за main, реконсилер остаётся удалённым) и фиксов рецензии (fail-closed сверка: отсутствие execution report / несовпадение run-id и даты / неполнота слотов / pending-статусы / битые теги Telegram-HTML — блокируют отправку; 5 новых отчётов сохраняются workflow; verify-digest-plan в replay-цепочке, stages_ok=5): повторный replay --golden 12/12 дней OK, lead ok 12/12 (в sent: missing 07-02 и 07-03, empty 07-07), blank_runs_2plus=0 на всех, verify в цепочке каждого дня (10 clean, 2 honest ship_degraded по 2 контентных расхождения). Тесты: 842, падают 2 — воспроизводятся на чистом origin/main (наследство ночного контура: road_only_transport, tram_card). Контрактных кейсов теперь 9 (добавлены: fail-closed сверка ×3, promotion-below-min, A-tier во всех трёх venue_scope).
- Где была ошибка (если не сработает): —

### 0094 — A-tier билеты не режутся лимитами выпуска — 2026-07-11
- Статус: внедрено
- Проблема: A-tier концерты outside-GM/unknown после шестого в своей секции и после лимита Ticket Radar=15 не попадали в видимый выпуск; они уходили только в служебный ticket inventory. Это расходилось с правилом: все A-tier билеты должны быть показаны.
- Причина (корень): `_is_budget_exempt_a_tier` (`writer.py`) освобождал от лимитов только GM/nearby; per-section/global/hard caps резали outside/unknown A-tier, а `public_html_contract_errors` (`release.py`) блокировал выпуск при 16-й строке Ticket Radar.
- Решение: любой распознанный A-tier билет освобождён от лимита секции и обоих лимитов выпуска независимо от venue_scope; release больше не считает 16+ строк Ticket Radar ошибкой. Обычные не-A-tier билеты остаются под прежними лимитами.
- Почему так (отвергнуто): оставлять excess в инвентаре означает всё ещё скрывать интересующий читателя билет; отдельный новый билетный выпуск не нужен, потому что существующий рендер уже умеет показать все строки.
- Ожидаемый эффект и метрика проверки: при 8 outside-GM A-tier и cap=6 отрисовываются все 8; HTML с 16 билетными строками проходит public-output contract; `ticket_inventory.outside_gm_a_tier_held_count=0`.
- Файлы/места: `writer.py:_is_budget_exempt_a_tier`, `writer.py:_slice_counting_only_non_exempt`, `release.py:public_html_contract_errors`; тесты `test_ticket_consolidation.py`, `test_publish_plan_contract.py`, `test_public_output_contracts.py`.
- ПРОВЕРКА (после прогона): replay 2026-07-09 до/после: A-tier trace `rendered` 3→16, `not_rendered` 29→16; «Крупные концерты вне GM» в draft 0→12 строк; `ticket_inventory.outside_gm_a_tier_held_count` 13→0. Точечные тесты: 6 OK (`ATierBudgetExemptionTest`, outside-GM global-budget regression, HTML с 16 билетными строками).
- Где была ошибка: старое ограничение scope и release-gate противоречили правилу видимости A-tier.

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
- Удалено: ничего — build-reconcile остаётся release-гейтом и recovery ДО промоушена, pre-send re-run лишь перемеряет финал; поведения не дублируются полностью (re-creation секций живёт в 0062-пути).

### 0082 — Repair не стрипает секцию ниже floor в пустоту (FIX-2) — 2026-07-09
- Статус: внедрено; ПРОВЕРКА на реальных данных 09.07 + новый тест.
- Проблема: repair-executor 09.07 снял 3 строки «Свежих» (`stripped: 3`) → секция 5→2 при floor 6, три пустые строки в HTML. Две сняты как «дубль M62/M6», хотя судья в тех же warnings написал «разные дороги, не критично».
- Причина (корень): в `_apply_repair_executor` (`pre_send_quality_judge.py`) при отсутствии model/deterministic/reserve-замены строка безусловно обнулялась (`html_lines[raw_index] = ""` → `stripped_honest_shortfall`), без учёта floor секции.
- Решение: floor-guard перед strip — если секция уже на минимуме/ниже (`_html_lines_for_section` vs `SECTION_MIN_ITEMS`) и замены нет, оригинальная (уже прошедшая writer/editor/release) строка сохраняется (`kept_below_floor_no_reserve`). Исключение — fact-integrity риск (unsupported/fabricat/hallucin/не подтвержд/выдум/не соответствует источнику): целостность важнее floor, такую строку всё равно снимаем.
- Почему так (отвергнутые альтернативы): не сужаем сам семантический дедуп (dedupe_dropped=422 — невидимый pool-churn, риск широкой регрессии); чиним именно видимый вред — schlop секции в пустые строки; не оставляем галлюцинацию ради floor.
- Ожидаемый эффект и метрика проверки: `repair_executor.kept_below_floor` > 0 вместо `stripped`, когда strip уронил бы секцию ниже минимума без резерва; «Свежие» не схлопывается 5→2 пустыми строками.
- Файлы/места: `pre_send_quality_judge.py:_apply_repair_executor` (floor-guard), init `kept_below_floor`; тест `tests/test_pre_send_repair_executor.py:test_strip_below_floor_keeps_original_but_still_drops_unsupported_fact`.
- ПРОВЕРКА: новый тест зелёный — дубль-strip ниже floor сохраняет строку (`kept_below_floor=1`), непроверенный факт всё равно снят (`stripped=1`). Полный набор 837/0.
- Удалено: ничего — strip-путь остаётся нужным для fact-integrity случаев, guard только сужает его применение.

### 0083 — Шаблонные концовки чистятся на финальном HTML, регекс расширен (FIX-3) — 2026-07-09
- Статус: внедрено; ПРОВЕРКА на реальном выпуске 09.07 + новый тест.
- Проблема: в отправленном 09.07 живьём остались «сверьте детали и условия перед поездкой» (Выходные), «Сверьте часы и условия перед поездкой», «Проверьте часы работы…», «Сроки и объёмы работ уточняйте на странице перевозчика» (Transport) — 0041/0044 не сработали в проде.
- Причина (корень): (а) `_EMPTY_ENDING_RE` (`editor.py`) не ловил «часы/время» и «уточняйте … на странице/перевозчика»; (б) даже пойманные строки уходили в HTML, т.к. `_apply_empty_ending_post_check` видит только `polished`, а weekend/transport/reserve-backfill строки приходят другими путями и не проходят через post-check; (в) `<55c` guard оставлял короткие transport-заглушки только в отчёте.
- Решение: (1) расширил регекс (свер/проверьте + часы|время; отдельные альтернативы для «сроки и объёмы работ уточняйте…» и «уточн(ите|яйте) … на странице|перевозчика»); (2) `_strip_empty_editor_ending(strip_short=True)` — на этапе отправки короткие строки чистятся (пол 20c), а не только репортятся; (3) новый `_strip_empty_endings_in_html` в pre-send hygiene проходит по каждому буллету ФИНАЛЬНОГО HTML, независимо от стадии-источника.
- Почему так (отвергнутые альтернативы): не трогаем `<55c` guard на editor-стадии (там enrichment ещё возможен) — strip_short только для ship-time last-resort; не переписываем регекс целиком, а дописываю альтернативы; конкретные действия («проверьте страницу статуса TfGM», «проверьте карту дорог») остаются нетронутыми.
- Ожидаемый эффект и метрика проверки: в `current_digest.html` нет «сверьте/проверьте … условия перед поездкой» и «уточняйте на странице перевозчика»; `repair_executor.empty_endings_stripped_at_ship` считает снятые.
- Файлы/места: `editor.py:_EMPTY_ENDING_RE`, `_strip_empty_editor_ending` (strip_short); `pre_send_quality_judge.py:_strip_empty_endings_in_html`, вызов в hygiene-блоке; тест `tests/test_editor_pacing.py:test_ship_time_pass_strips_broadened_and_short_boilerplate_endings`.
- ПРОВЕРКА: на реальном `current_digest.html` 09.07 — `_strip_empty_endings_in_html` снял 5 концовок (Выходные×3, Transport×2), ссылки целы, контент сохранён. Новый тест зелёный; полный набор 837/0.
- Удалено: ничего — editor-стадийный post-check чистит строки ДО судьи (меньше ложных repair-действий), ship-time проход — last resort для строк, минующих editor.

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

### 0086 — Replay-харнесс: офлайн-повтор любого прошлого дня (write→edit→build) — 2026-07-09
- Статус: внедрено; ПРОВЕРКА по факту прогона golden-набора (см. ниже).
- Проблема: единственный способ проверить правку writer/editor/release — завтрашний живой выпуск. Цена проверки = 1 день + риск испорченного выпуска читателям. В git лежат ~40 дней state-снимков ("chore: digest state YYYY-MM-DD"), которые никак не использовались.
- Причина (корень): не было инструмента, который поднимает снапшот дня в песочницу и прогоняет детерминированные стадии офлайн.
- Решение: `scripts/replay_day.py <дата>` — достаёт `data/state/` и отправленный `current_digest.html` из коммита дня, прогоняет write→edit→build в песочнице (project_root=sandbox), сравнивает метрики и diff с реально отправленным. Время заморожено через `NEWS_DIGEST_FAKE_NOW` (новый env-override в `common.now_london()`, плюс роутинг `event_extraction._today_london()` и fallback в `city_trends.py` через него); сеть жёстко заблокирована на уровне socket; LLM выключен (ключи вычищены — draft_line уже в кандидатах); Telegram off. Детекторы дефектов: прогоны пустых строк, missing/empty лид, штампы-концовки (прод-регекс `_EMPTY_ENDING_RE` из editor.py). `--golden` = 4 дефектных дня (2026-06-27, 07-02, 07-07, 07-09) + 6 обычных, с assert'ами дефектов на отправленном артефакте.
- Почему так (отвергли): байтовое сравнение replay-vs-sent как gate — отвергнуто: снапшот коммитится ПОСЛЕ прогона, пост-send мутации (`is_lead` переставлен на другую историю, ±2 quarantine) делают точное совпадение невозможным. Регрессионный сигнал = replay-vs-replay до/после правки кода (replay детерминирован при фиксированных code+snapshot); diff с sent — контекст. Брать memory-файлы из коммита предыдущего дня — отложено: расхождение живёт в самих candidates-флагах, не в dedupe-памяти.
- Ожидаемый эффект и метрика проверки: replay любого дня < 5 мин без сети; дефекты golden-дней детектируются на sent-артефактах (07-09: blank_run=3, штампы=5; 07-07: пустой лид + blank_run=2; 07-02: лид отсутствует; 06-27: штампы); правка стадий проверяется до/после на этих днях (правило в AGENTS.md «Replay harness — MANDATORY check»).
- Файлы/места: `scripts/replay_day.py` (новый); `common.py:now_london` (FAKE_NOW override); `event_extraction.py:_today_london`; `city_trends.py:390`; `AGENTS.md` (обязательное правило + workflow).
- ПРОВЕРКА (после прогона): 2026-07-09, `--golden` (4 golden + 6 обычных). Все 10 дней: стадии ok, максимум 136.5s/день (критерий <5 мин), сеть заблокирована socket-guard'ом. Детерминизм: два независимых прогона 2026-07-09 → байт-в-байт идентичный HTML. Golden-дефекты подтверждены на sent-артефактах всех 4 дней (07-09: blank_run=3, штампы=5; 07-07: lead=empty, blank_run=2; 07-02: lead=missing, штампы=6; 06-27: штампы). Побочные находки: replay показывает эффект фикса 0083 на истории — штампы 07-01: sent 7 → replay 0, 06-29: 3 → 0; у 07-03 в sent лид отсутствовал (ранее незадокументированный дефектный день); 2026-06-30 прод сам не выпустился (в коммите лежит stale HTML от 06-29) — из ordinary-набора исключён, замена 2026-07-06 (чистый прогон 121.9s). Один баг харнесса пойман первым же прогоном и исправлен: заморозка времени по шапке sent-HTML брала чужую дату у stale-файла — теперь fallback на время коммита. CI-suite (`unittest discover`): 844 теста, 4 pre-existing падения — без изменений до/после правок.

### 0087 — Food/Ticket/Inventory/Transport прод-пруф после 10.07 — 2026-07-10
- Статус: внедрено; ПРОВЕРКА на реальных артефактах 2026-07-10.
- Проблема: после включения morning inventory `Еда, открытия и рынки` осталась 0; `Asian Food Night Market` был в candidates с датой 10 июля, но сброшен как `cross_day_rehash`; Co-op Live ticket inventory был `fact_ready`, но без `draft_line`; recurring weekend inventory пропускал карточки с `fact_card.date_start=2026-07-05`, хотя русская строка писала `12 июля`; transport reserve мог дать обрывок `TfGM: ремонтные работы — Brooklands`; `inventory_morning_effect.reason` говорил, что morning re-entry не включён, хотя `collect_intake.actual_intake=3`; workflow не коммитил `morning_inventory_intake_report.json`.
- Причина (корень): cross-day validator применял same-fingerprint TTL к событию в день проведения; writer floor pull-back не мапил `food_openings` на уже существующий event fallback; `is_recoverable_reserve` не включал безопасный `backup_candidate` overflow, хотя комментарий обещал объединение pool'ов; ticket fallback требовал notability даже для official venue `major_upcoming`; inventory morning contract не сверял противоречие structured date vs готовая русская строка; transport fallback строил строку без явного passenger-effect; daily workflow не добавлял новый intake report в git.
- Решение: событие в `openings/weekend/next_7_days` с `event.date_start == today` не режется cross-day rehash; `food_openings/openings` используют существующий event fallback в writer/replacement; `backup_candidate` допускается в recoverable pool только через старый `recoverable_reserve_eligible`; official venue `major_upcoming` с датой может получить ticket fallback без notability; inventory rejects `draft_line_date_conflicts_with_fact`; transport fallback требует passenger tokens и для Brooklands пишет автобусный объезд; release summary берёт статус из `collect_intake`; workflow коммитит `data/state/morning_inventory_intake_report.json`.
- Почему так: не добавлялись новые стадии, recovery-механизмы или fallback-слои; исправлены существующие predicates/mappings, которые уже должны были выполнять этот контракт.
- Ожидаемый эффект и метрика проверки: Asian Food Night Market не падает как rehash в день события; Food floor может добираться тем же same-block recovery; Co-op venue tickets получают draft_line; conflicting recurring inventory не попадает в morning intake; transport fallback публикует только пассажирский эффект; release report не врёт о consumed mode; отдельный intake report сохраняется.
- Файлы/места: `candidate_validator.py:_exclude_cross_day_rehash`; `common.py:is_recoverable_reserve`; `writer.py:_FALLBACK_BUILDER_BY_CATEGORY`, `_final_replacement_line`, `_ticket_watch_decision`, `_build_transport_fallback_line`; `inventory.py:passes_morning_contract`; `release.py:_summarise_inventory_morning_effect`; `.github/workflows/daily-digest.yml`.
- ПРОВЕРКА: реальные данные 2026-07-10 — до: Food section count `0`, Asian `reject_reasons=['cross_day_rehash']`, Brooklands HTML contained `TfGM: ремонтные работы — Brooklands`, intake report file absent, summary `morning_consumed=false` при `actual_intake=3`. После на тех же candidate/inventory records: Asian `_exclude_cross_day_rehash=False` и fallback line строится; five stale Food reserves remain blocked (`stale_opening`); Co-op Live ticket records now return ticket lines; Burnage/The BIG Stockport car boot return `draft_line_date_conflicts_with_fact`; Brooklands fallback becomes `автобусы идут в объезд`; `py_compile` OK; targeted editor/dedupe regression tests OK.

### 0088 — Weekend market rescue reaches final visible HTML — 2026-07-10
- Статус: внедрено; ПРОВЕРКА replay 2026-07-10.
- Проблема: `The Asian Food Night Market Stockport` был собран из `SK Lowdown Markets`, но не появился в финальном блоке `Выходные в GM`. Предыдущий фикс доказывал только кандидат/fallback, но не весь путь до `current_digest.html`.
- Причина (корень): источник `SK Lowdown Markets` был зарегистрирован как `food_openings/openings`, поэтому событие выходного дня шло через opening/repeat-политику; writer мог восстановить строку, но не сохранял изменённый `include=true/category= culture_weekly/primary_block=weekend_activities` обратно в `candidates.json`; release затем видел старый `include=false/drop` и quarantine удалял уже видимую строку как `rejected_candidate_visible`.
- Решение: источник `SK Lowdown Markets` перенесён в weekend inventory (`culture_weekly`, `weekend_activities`); writer добавил rescue для текущих weekend-market/fair/festival событий, ошибочно сброшенных как old opening/cross-day rehash; после rescue writer сохраняет обновлённый candidate-state, чтобы release не считал строку rejected.
- Почему так: это не отключает release-quarantine и не публикует все старые openings; спасаются только датированные события текущего weekend-window с market/fair/festival сигналом или rubric `weekend_market`.
- Ожидаемый эффект и метрика проверки: current-weekend markets из misrouted food/openings больше не теряются между writer и release; `writer_report.misrouted_weekend_market_rescue` показывает спасённые события; replay final HTML содержит строку события.
- Файлы/места: `data/sources.toml`; `writer.py:_rescue_misrouted_weekend_markets`; `writer.py:write_digest`; тесты `tests/test_weekend_inventory_contract.py`, `tests/test_digest_quality_guardrails.py`.
- ПРОВЕРКА: replay 2026-07-10 до фикса: `Asian Food Night Market=false` в sent и baseline replay; после фикса: `python3 scripts/replay_day.py 2026-07-10 --sandbox /private/tmp/nm_replay_after3_20260710` ok, `bullet_total 62 sent → 29 replayed`, `max_blank_run 2→1`, `lead_status ok→ok`; final `current_digest.html` содержит `Stockport's Asian Food Night Market is back in July` и URL `sklowdown.co.uk/whats-on-stockport/asian-food-night-market-july`; `writer_report.misrouted_weekend_market_rescue.count=1`; `release_report.rendered_html_quarantine.bad_fingerprints` больше не содержит этот fingerprint. Targeted tests: `py_compile writer.py` OK; `WeekendInventoryContractTests.test_misrouted_food_opening_market_is_rescued_to_weekend`, weekend duplicate test, `DigestQualityGuardrailsTest.test_sk_lowdown_markets_source_routes_to_weekend_inventory` OK. Дополнительно `python3 scripts/replay_day.py --golden --sandbox /private/tmp/nm_replay_golden_after_0088`: 7/10 дней stage-ok; 3/10 упали на уже существующем release-gate долге `ticket_radar_over_cap (18 > 15)` на 2026-07-09/07-03/07-06, не связанном с weekend-market rescue.

### 0089 — Панель качества: 5 показателей на отправленный выпуск вместо мёртвых отчётов — 2026-07-10
- Статус: внедрено; ПРОВЕРКА на реальных выпусках 2026-06-25…2026-07-10.
- Проблема: ~50 отчёт-файлов в `data/state` + 53 ключа в `release_report.json` в день — качество выпуска оценивалось на глаз. При этом реальные дефекты не ловились: 2026-06-25 одна и та же новость (провал дороги в Ли, один URL MEN) вышла ДВАЖДЫ; 2026-06-28 в выпуск ушло буквальное «следите за обновлениями»; 2026-07-07 «Главная история дня» вышла ПУСТОЙ. Ни один из 53 ключей этого не показал. Параллельно ежедневный шум: warnings «Event miss review: 75…/Final loss check: 201/204/217/231 possible real misses» (реальные числа 07-07…07-10), пустой quality_scorecard (`suspicious_published=0` все 14 дней истории), model_bakeoff (статичный «ready» каждый день).
- Причина (корень): отчёты писались для конвейера, а не для редактора — считали воронку кандидатов (`release.py:_quality_scorecard`), а не то, что увидел читатель в отправленном HTML.
- Решение: новый `pipeline/quality_panel.py` — 5 детерминированных показателей по ОТПРАВЛЕННОМУ HTML (все секции заполнены + ноль пустых строк; лид на месте; ноль внутривыпускных повторов по URL/тексту; ноль фраз-заглушек по `VAGUE_ENDING_MARKERS`; баланс новости/билеты). Считается в `cmd_send_file` после успешной отправки, одна строка в день в `quality_panel_history.json` (коммитится workflow). Раз в неделю (вс) — `send-weekly-quality` в Telegram. Удалены: `_quality_scorecard` + история, `_event_miss_review` + оба warnings «possible real misses» + ключи `event_miss_review`/`final_loss_check` из release_report, `model_bakeoff.py` + `_model_bakeoff_readiness` + CLI `model-bakeoff` + `MODEL_BAKEOFF_SPEC`, секции admin-отчёта («Возможные пропуски событий», «ВАЖНЫЕ СОБЫТИЯ, КОТОРЫЕ НЕ ДОШЛИ», «КОНТРОЛЬ КАЧЕСТВА», билетные типы из scorecard). `_final_loss_check` оставлен НЕ как отчёт, а как внутренний вход backup_pool (recovery активно используется: 1155 кандидатов, «recovered 4 lines» 2026-07-10).
- Почему так (отвергнутое): LLM-judge для панели — уже есть post_publish_judge, панель обязана быть детерминированной и бесплатной; блокирующий gate — запрещено (never block the release).
- Ожидаемый эффект и метрика проверки: одна строка в день отвечает «растёт ли качество»; за 06-25…07-10 (16 реальных выпусков): 5 OK-дней из 16, пустые секции 6 дней, пустой лид 07-07, повторы 06-25/06-28, заглушки 5→0 к июлю.
- Файлы/места: `src/news_digest/pipeline/quality_panel.py` (новый); `scripts/run_local_digest.py:cmd_send_file`, `cmd_send_weekly_quality`; `release.py` (удаления); `.github/workflows/daily-digest.yml`; `tests/test_quality_panel.py`.
- ПРОВЕРКА: A/B на копии реального state 2026-07-10 — старый код: 50 ключей report, warnings «75/204 possible real misses», scorecard пишется; новый код: 46 ключей, шумовых warnings НОЛЬ, scorecard не пишется, backup_pool=1155 не сломан. Панель по git-истории реальных отправленных выпусков поймала все три задокументированных дефекта (повтор 06-25, заглушка 06-28, пустой лид 07-07). Admin-текст рендерится (83 строки) без удалённых секций. `unittest discover`: 846 тестов, 3 падения — те же, что на чистом дереве (чужой WIP weekend-rescue), новых нет.

### 0090 — Удалён мёртвый демо-путь (render-demo/send-demo) — 2026-07-10
- Статус: внедрено
- Проблема: демо-контур не запускался с 2026-04-21 (mtime `run_state.json`, единственный писатель — `mark_demo_run`; единственный артефакт — `data/archive/2026-04-21-demo.md`). Ни один workflow/скрипт его не звал.
- Причина (корень): демо-выпуск был нужен до запуска прод-пайплайна; после перехода на GH Actions (0053+) путь остался без вызывающих.
- Решение: удалены пакеты `assembly/`, `jobs/`, `models/` (models импортировался только демо), команды `render-demo`/`send-demo` и их cmd_-функции, `StateStore.mark_demo_run` + `run_state_path` + `archive_dir` (в т.ч. из Settings), локальные `run_state.json`, `data/archive/`.
- Почему так (отвергли): оставить как ручной инструмент — отвергнуто, демо рендерит из mock-моделей и давно не отражает реальный формат выпуска.
- Ожидаемый эффект и метрика проверки: unittest discover зелёный (846 тестов, 3 падения — pre-existing в свежих test_inventory/test_public_output_contracts, воспроизводятся на чистом HEAD до этой правки); `run_local_digest.py --help` работает.
- Файлы/места: scripts/run_local_digest.py, src/news_digest/state/store.py:23, src/news_digest/config/settings.py:26, .gitignore
- ПРОВЕРКА (после прогона): 2026-07-10, CI-командой `PYTHONPATH=src python3 -m unittest discover -s tests`: 846 ran, те же 3 pre-existing failures до и после (diff пустой).

### 0091 — Удалены обработчики source_type, которых никогда не было в конфиге — 2026-07-10
- Статус: внедрено
- Проблема: в диспетчере `_extract_source_candidates` жили ветки под source_type, которые не встречаются в `data/sources.toml` за всю git-историю (`git log -S` пусто): `json_wp_rest`, `markdown_links`; плюс ветка `json_national_rail` — тип убран из toml 2026-06-08 (e9567b0, замена на NRE Incidents).
- Причина (корень): обработчики писались "на вырост" под источники, которые так и не завели; national_rail-тип пережил замену источника.
- Решение: удалены `_extract_wp_rest_items`, `_extract_markdown_link_items` (по 20 строк) и три мёртвые ветки диспетчера. `_extract_national_rail` СОХРАНЁН — он живой fallback внутри NRE-экстрактора (extract.py, `return items or _extract_national_rail(...)`).
- Почему так (отвергли): удалить также `html_eventbrite` + `_extract_eventbrite_markets` — отвергнуто: единственный источник этого типа (Eventbrite Manchester Markets) выключен из-за WAF 405 на GH-раннере с документированной заметкой, это чинимый источник (curl_cffi-каскад), а не мёртвый код.
- Ожидаемый эффект и метрика проверки: unittest discover без новых падений; поведение пайплайна не меняется (ветки недостижимы по конфигу).
- Файлы/места: src/news_digest/pipeline/collector/extract.py
- ПРОВЕРКА (после прогона): 2026-07-10, `PYTHONPATH=src python3 -m unittest discover -s tests`: 846 ran, те же 3 pre-existing failures (test_inventory / test_public_output_contracts), новых нет.

### 0092 — Удалён launchd-контур и мёртвый приём Telegram-реакций — 2026-07-10
- Статус: внедрено
- Проблема: прод — GitHub Actions (см. 0053+, daily-digest.yml via cron-job.org); launchd-контур мёртв с 2026-05-16 (mtime launchd.std*.log). Вместе с ним де-факто умер приём реакций читателей: `process-updates` звался ТОЛЬКО из launchd-plist → входящих реакций нет с 16 мая (bot_state.json без изменений с 27 апреля; personalization_feedback.json меняется только decay-логикой release.py). Owner решил: признать бот-приём мёртвым, удалить (вариант «а» из разбора 2026-07-10).
- Причина (корень): миграция прод-пути на GH Actions не сопровождалась переносом/похоронами bot-updates контура.
- Решение: удалены ops/launchd/ (2 plist), scripts/{install,uninstall}_launchd_job.sh, run_daily_digest.sh, run_pipeline_collect.sh (не ссылался никто), run_pipeline_publish.sh, sync_runtime_bundle.sh, process_bot_updates.sh, docs/local-scheduler.md, пакет `news_digest.bot`, команды `get-updates`/`process-updates`/`poll-updates`, `TelegramClient.get_updates`, методы StateStore: get/set_last_update_id, add/remove_subscriber, record_item_feedback, _save_bot_state. СОХРАНЕНО: `bot-info` (проверка токена), `list_subscribers` + bot_state.json (список получателей читается при доставке), decay-логика personalization_feedback в release.py. Локально удалены launchd.std*.log, auto_editor_report.json (писатель удалён из кода ранее, файл от 15 мая).
- Почему так (отвергли): перенос process-updates в GH Actions cron — отвергнут owner'ом («1 удаляй»); подписка/отписка теперь только правкой bot_state.json руками.
- Ожидаемый эффект и метрика проверки: unittest discover без новых падений; `--help` и `digest-status` работают; завтрашний daily-digest прогон зелёный (send-file берёт получателей из bot_state.json как раньше).
- Файлы/места: scripts/run_local_digest.py, src/news_digest/state/store.py, src/news_digest/delivery/telegram.py:225, AGENTS.md (регенерирован)
- ПРОВЕРКА (после прогона): 2026-07-10 локально: 846 ran, те же 3 pre-existing failures. Прод-подтверждение — после прогона daily-digest 2026-07-11.

### 0093 — Sitemap-экстрактор читает lastmod; Albert Hall переведён с архивного sitemap на /whats-on/ — 2026-07-10
- Статус: внедрено
- Проблема: Albert Hall Manchester за 36 прогонов (03.06–09.07): raw=180, curated=0. В кандидатах — события 2018 года (TOKIO MYERS, published 2018-04-16).
- Причина (корень): `_extract_sitemap_items` (extract.py) брал `<loc>` в документном порядке и игнорировал `<lastmod>`; event-sitemap.xml — замороженный Yoast-архив на 1000 записей (свежайший lastmod 2022), первые записи — 2018. Коллектор вечно брал топ-5 из 2018-го.
- Решение: (1) экстрактор парсит по `<url>`-блокам, прикрепляет lastmod как published_at и сортирует свежие первыми — проверено на живых sitemap: MEN map_news.xml отдаёт сегодняшние статьи первыми с датами (2026-07-10T13:17), архивы честно датируются и отсеются как stale; (2) Albert Hall: url → /whats-on/, source_type → default html — на живой странице даёт 3 актуальных события с датами (GK Barry 2026-06-02, Danny Beard, Adam F).
- Почему так (отвергли): чинить только URL без экстрактора — отвергнуто: у MEN дата из sitemap тоже полезна, а любой будущий архивный sitemap воспроизвёл бы ту же болезнь.
- Ожидаемый эффект и метрика проверки: в source_health_history Albert Hall curated>0 в течение недели; кандидаты Albert Hall в candidates.json датированы 2026.
- Файлы/места: src/news_digest/pipeline/collector/extract.py (_extract_sitemap_items), data/sources.toml:554, tests/test_source_parser_resilience.py (golden-тест на реальной форме Yoast-sitemap)
- ПРОВЕРКА (после прогона): 2026-07-10 локально на живых артефактах (см. Решение); прод-числа — после прогонов 11–14.07.

### 0094 — Telegram-поток: только выпуск ежедневно; алерт лишь когда выпуск НЕ вышел; одна недельная сводка — 2026-07-12
- Статус: внедрено; ПРОВЕРКА на реальных state 2026-07-11 и 2026-07-12.
- Проблема: owner 2026-07-12 «почему мне сегодня все пришло опять». В воскресенье 07-12 пришло 4 отчёт-сообщения поверх выпуска: ежедневный send-warnings (~80 строк, срабатывает почти всегда — сегодня 20 warnings), weekly-quality, weekly-cost, weekly-city-rollup. Хуже: логика была перевёрнута — 2026-07-11 release gate ЗАБЛОКИРОВАЛ выпуск (ticket_radar_over_cap 17>15), выпуск не дошёл, а delivery-guard в send-warnings при этом молча пропустил алерт. Отчёт при успехе, тишина при провале.
- Причина (корень): `cmd_send_warnings` слал полный отчёт при любом has_signal (а signal есть всегда), guard «не доставлен → skip» глушил единственный реально нужный алерт; три отдельных воскресных письма никто не просил.
- Решение: контракт инвертирован — выпуск доставлен → в Telegram ТИШИНА, полный отчёт печатается в stdout (лежит в Actions-логе); выпуск НЕ доставлен (gate fail / send упал) → короткий ⛔ алерт (3 строки, первая реальная ошибка гейта). Воскресенье: одно сообщение — сводка панели качества + строка расходов за 7 дней (`_weekly_cost_line`). Удалены: команды `send-weekly-cost`, `weekly-city-rollup`, `send-weekly-city-rollup`, их шаги в workflow, `build_weekly_city_rollup`/`weekly_city_rollup_text` из city_trends, `weekly_city_rollup.json`; мёртвая legacy-ветка send-warnings (~650 строк) и осиротевшие display-хелперы (`_translate_health_signal`, `_borderline_verdict`, `_humanize_source_reason`, `_source_counts_phrase`, `_explain_source_failure`, `_humanize_borough_flag`, `_ticket_type_human`, `_diaspora_verdict_human`, `_group_suspicious_rejects`, `_classify_reject_reason`, `_REJECT_GROUPS`, `_ALREADY_FIXED_CAUSES`, `_section_drops`, `_source_streak_tag`, `_humanize_writer_reason`) + их копи-тесты. run_local_digest.py: 2100 → 1720 строк.
- Почему так (отвергнутое): «WARNINGS_TO_TELEGRAM=0» — глушит и алерт о недоставке, недопустимо; оставить weekly-cost отдельным письмом — owner просил одну сводку, бюджет-контроль сохранён строкой в ней.
- Ожидаемый эффект и метрика проверки: будни — 1 сообщение (выпуск), воскресенье — 2 (выпуск + сводка), провал — выпускa нет, но есть ⛔ алерт. Было в вс: 5.
- Файлы/места: `scripts/run_local_digest.py:cmd_send_warnings`; `quality_panel.py:_weekly_cost_line`; `.github/workflows/daily-digest.yml`; `city_trends.py`; тесты `test_send_warnings_delivery_guard.py` (контракт в обе стороны).
- ПРОВЕРКА: реальный state 07-12 (доставлен, ship_degraded, 20 warnings) → 0 отправок в Telegram, отчёт 4803 символа в stdout; реальный state 07-11 (fail, не доставлен) → ровно 1 алерт: «⛔ Выпуск 2026-07-11 НЕ дошёл… Причина: ticket_radar_over_cap (17 > 15)» — старый код в этой ситуации молчал. Недельная сводка на реальной истории: 7 дней + «💰 Расходы за 7 дн.: $2.21 (≈$0.32/день)». `unittest discover`: 845 тестов, 4 падения — те же на чистом HEAD (weekend/inventory WIP параллельной сессии), новых нет.

### 0095 — Зелёный CI: fix seller-page регрессии + 5 time-fragile тестов; подключён critical_fact_obligations — 2026-07-10
- Статус: внедрено
- Проблема: `unittest discover` падал (6 тестов на разных прогонах). Плюс helper `critical_fact_obligations` (fact_completeness.py) не имел вызовов.
- Причина (корень):
  - **Реальная регрессия (test 2, seller-page):** коммит 2651af8 ослабил guard weekend-карточки с `if not detail_text` на `if not detail_text and not day_month` (owner: публиковать по дате+месту+типу). Это обнажило дыру в `_is_weekend_seller_admin_page`: слово "stall" внутри *продавецкой* фразы "apply for a stall" триггерило *визитёрский* `_WEEKEND_VISITOR_RE`, из-за чего seller-страница не отсекалась и, получив вычисленную дату, протекала строкой в выпуск.
  - **Time-fragile (5 тестов):** `passes_ttl_contract`/`current_weekend_window` меряют возраст/окно по настенным `now_london()`, а тесты передавали фиксированный `today=`/`last_seen_at`. Писались 2026-06-13…07-09, проходили в тот день, ломались при дрейфе даты (сегодня записи «старше» TTL 96ч, событие-дата вне weekend-окна).
  - **critical_fact_obligations:** guard `translation_completeness_review` подключён и работает (pre_send_quality_judge.py:601), но список obligations он собирал ИНЛАЙНОМ, дублируя helper — helper остался без вызовов.
- Решение:
  - writer.py `_is_weekend_seller_admin_page`: вырезать matched seller-фразы из blob перед проверкой visitor-сигнала (детектор — правильное место гейта, не откат owner-правки 2651af8).
  - Тесты: замокать `now_london` в нужном модуле (weekend_inventory для occurrence-даты, inventory для TTL) — конвенция уже была в test_public_output. test_misrouted: пин на субботу + мок окна.
  - fact_completeness.py: `translation_completeness_review` теперь строит obligations через `critical_fact_obligations(source)` (DRY, поведение идентично — та же итерация/порядок).
- Почему так (отвергли): откат guard'а 2651af8 — нет, это owner-правка; треды `now`/`today` в прод-сигнатуры — нет, ломает тонкие TTL (transport 1ч, last_24h 6ч) и рискует конфликтом с параллельной сессией в inventory.
- Ожидаемый эффект и метрика проверки: `PYTHONPATH=src python3 -m unittest discover -s tests` — 846 ran, OK.
- Файлы/места: src/news_digest/pipeline/writer.py:3547, src/news_digest/pipeline/fact_completeness.py:95, tests/{test_public_output_contracts,test_inventory,test_weekend_inventory_contract,test_fact_completeness}.py
- ПРОВЕРКА (после прогона): 2026-07-10, `unittest discover`: **846 ran, OK** (было 6 разных падений).

### 0095 — Рус-блок: повторы Онегина/Скамейки — история видит отправленный HTML, must_show уважает repeat-policy — 2026-07-13
- Статус: внедрено; ПРОВЕРКА на реальных данных 07-12 + 852/0 + replay 07-12 (diff=0).
- Проблема: «Скамейка» (29 ноября) и «Евгений Онегин» (Хэмпшир, «до 12 июля») выходили 3+ выпуска подряд (07-02…07-12). Судья 07-12 даже патчил строку «Хэмпшир не является частью GM» — но строка возвращалась каждый день.
- Причина (корень), три слоя: (1) `_rendered_candidates_for_delivery` (run_local_digest.py) брала только `writer_report.rendered_candidate_fingerprints` — строки, вставленные ПОСЛЕ writer'а (must_show-recovery в release_reconcile / pre-send top-up), никогда не попадали в `published_facts.json` → у Скамейки `prev=None` → repeat-политика видела «no_previous_match» вечно; (2) `_publish_plan_must_show` (llm_rewrite.py:3338) давал must_show ЛЮБОМУ selected `russian_events` — обходя все caps и кварантин; (3) у Онегина усечение date_text «26 June – 12 July»→«26 June» (сайт убрал прошедший диапазон; date_start откатился на 2027) считалось `event_date_text_changed` = материальное изменение → повтор разрешён.
- Решение: (A1) delivery-история дополнительно матчит kandidat'ов по canonical URL против href'ов РЕАЛЬНО отправленного HTML; (A2) russian_events must_show только при `visible_repeat_verdict(...).allow` (published_facts прокинуты в `_build_publish_plan`); (A3) subset/усечение date_text не считается материальным изменением.
- Почему так (отвергнуто): чинить только кварантин на release — не помогает: reconcile/pre-send re-insert возвращали must_show строку после кварантина (пинг-понг); глушить рус-блок целиком — ломает продукт (свежие события, как Тимошенко «сегодня», обязаны показываться — проверено: у него must_show остаётся, блокируется только после прохождения даты как `event_already_passed`).
- Ожидаемый эффект и метрика проверки: `visible_repeat_review.bad_visible_repeats` начинает ловить диаспорные повторы (раньше 10/10 allowed); Скамейка получает запись в published_facts с первого же выпуска; далёкое событие без нового reader-момента не re-must_show'ится.
- Файлы/места: `scripts/run_local_digest.py:_rendered_candidates_for_delivery`; `llm_rewrite.py:_publish_plan_must_show/_publish_plan_status/_build_publish_plan`; `editorial_contracts.py:_event_material_change`; тесты `tests/test_send_warnings_delivery_guard.py`, `tests/test_dedupe_and_show.py`, `tests/test_digest_quality_guardrails.py`.
- Удалено (2026-07-13, вслед): ветка writer-fingerprints в `_rendered_candidates_for_delivery` — после URL-матчинга по sent HTML она не просто лишняя, а вредная: записывала «опубликованными» строки, снятые ПОСЛЕ writer'а и не дошедшие до читателя (на реальном 07-13 — 4 фантома: Manchester Open, Beers In The Garden, Nia Archives, Naming rights), уча repeat-policy блокировать их будущий первый показ. История = ровно отправленный HTML. На реальном 07-13: старая логика 42 записи (Скамейки нет) → новая 68 (Скамейка есть, 4 фантомов нет).
- ПРОВЕРКА: на реальных candidates/published_facts 07-12 — Онегин: verdict `allow=False / same_calendar_item_without_new_reader_moment` (был `allow=True / event_date_text_changed`), must_show False; Тимошенко (прошёл вчера): `event_already_passed`, не форсится; Скамейка: пока True (prev появится после первой A1-доставки). Полный набор 852/0; replay 07-12 до/после — байт-в-байт.

### 0096 — Еда: прошедшие датированные события не публикуются, билдер не отдаёт site-chrome и адресные хвосты — 2026-07-13
- Статус: внедрено; ПРОВЕРКА на реальных кандидатах 07-12 + 852/0.
- Проблема: выпуск 07-12 в «Еда, открытия и рынки» отдал 3 строки: рынки 10–11 июля (посетить нельзя — прошли), сырые английские заголовки с хвостом источника («…is back in July — The SK Lowdown»), адресный блоб («Churchgate Stockport, England, SK1 1YG United Kingdom») и шаблон «Новое место или запуск стоит проверить перед визитом».
- Причина (корень): (1) `_exclude_bad_food_opening_timing` давал 3-дневный grace ВСЕМ food/openings — правильно для новостей об открытии, неправильно для датированных СОБЫТИЙ; при этом дата жила только в голове draft_line («10 июля…»), structured event был пуст — blob-даты её не видели; (2) `_build_event_fallback_line` строил карточку из title=«The SK Lowdown» (имя сайта) — ноль фактов события; (3) `_clean_event_venue_name` не резал «, England, SK1 1YG United Kingdom».
- Решение: (B1) датированное market/fair/festival/car boot/night market событие с прошедшей датой (structured event ∪ blob ∪ RU-дата в голове draft_line ≤45 дней назад) → `stale_opening` без grace; opening-новости grace сохраняют (Joe & The Juice проверено — не режется); (B2) title, чьи токены ⊆ токенов source_label → билдер возвращает "" (0030 show=renderable), хвост «— {источник}» срезается, venue теряет адресный хвост.
- Почему так (отвергнуто): переводить/обогащать такие карточки на лету — LLM на pre-send пути; RU-дата старше 45 дней игнорируется (могла означать следующий год — «5 января» в июле).
- Ожидаемый эффект и метрика проверки: в «Еде» нет прошедших дат и сырых «— The SK Lowdown»; reject_reasons получают `stale_opening` для прошедших рынков.
- Файлы/места: `candidate_validator.py:_exclude_bad_food_opening_timing`, `_ru_date_from_draft_line_head`; `writer.py:_build_event_fallback_line`, `_clean_event_venue_name`; тесты `tests/test_backlog_remediation.py`, `tests/test_public_output_contracts.py`.
- ПРОВЕРКА: на реальном кандидате Asian Food Night Market (07-12): excluded=True/stale_opening (13.07); билдер по source-ish title → ''; на реальном названии с хвостом — «SK Lowdown» и «United Kingdom» исчезают, «Asian Food Night Market» и «Churchgate Stockport» остаются. 852/0.
- Удалено: ничего — 3-дневный grace остаётся правильным для новостей об открытии (Joe & The Juice), новая ветка отрезает только датированные события.

### 0097 — Штампы: «Новое место…»/«Проверьте маршрут…» в регексе, болтающийся лейбл перед ссылкой не прячет концовку — 2026-07-13
- Статус: внедрено; ПРОВЕРКА на реальном HTML 07-12 + 852/0.
- Проблема: 07-12 отдал «Новое место или запуск стоит проверить перед визитом» (Еда ×2) и «TfGM: ремонтные работы — значительные. Проверьте маршрут и время отправления перед выходом. TfGM <a>…</a>» (Радар) — 0083-регекс их не покрывал, а у TfGM-строки утёкший лейбл «TfGM» между концовкой и ссылкой ломал якорь `$`.
- Решение: два новых паттерна в `_EMPTY_ENDING_RE`; link-захват в `_strip_empty_editor_ending` опционально поглощает короткий латинский лейбл (≤40c) перед `<a>` (не ре-аппендится — дублирует текст якоря).
- Ожидаемый эффект и метрика проверки: `_strip_empty_endings_in_html` на HTML 07-12 снимает 4 концовки (было 0 покрытия этих паттернов), 67/67 href целы.
- Файлы/места: `editor.py:_EMPTY_ENDING_RE`, `_strip_empty_editor_ending`; тест `tests/test_editor_pacing.py`.
- ПРОВЕРКА: реальный HTML 07-12 — 4 снято, оба паттерна исчезают, ссылки целы. 852/0.
- Удалено: ничего — регекс дополнен альтернативами, существующие паттерны по-прежнему ловят свои случаи.

### 0098 — Reserve prewrite: тонкие секции получают написанный резерв — 2026-07-13
- Статус: внедрено; ПРОВЕРКА offline (852/0); prod-числа после следующего build.
- Проблема: «Что важно сегодня» 2/3 и «Городской радар» 3/5 на 07-12 с `no_recoverable_reserve_with_facts` — при этом у city_watch было 14 recoverable-кандидатов, но 0 с draft_line: `to_rewrite` фильтрует по `include`, резерв не переводится вообще.
- Причина (корень): 0001-класс — резерв существует, но никогда не проходит rewrite → recovery нечего вставлять.
- Решение: bounded-квота `RESERVE_PREWRITE_PER_SECTION=2` для `RESERVE_PREWRITE_BLOCKS={city_watch,last_24h,today_focus,openings}`: топ-резерв по section_board_score добавляется в rewrite-борд (verdict/include не трогаются — строки рендерятся только если recovery их вытащит). ~8 строк/день доп. стоимости.
- Почему так (отвергнуто): full enrich-and-rewrite на pre-send — LLM в критическом пути отправки; расширять include — риск сломать баланс доски.
- Ожидаемый эффект и метрика проверки: `section_ranking_report.reserve_prewrite` > 0; `still_under_minimum` для Радара/Что важно уходит или reason меняется с `no_recoverable_reserve_with_facts` на реальную вставку.
- Файлы/места: `llm_rewrite.py:RESERVE_PREWRITE_BLOCKS`, `run_llm_rewrite` (quota block), `rewrite_shortlist.reserve_prewrite`.
- ПРОВЕРКА: offline — py_compile, 852/0; прод-эффект проверить на следующем утреннем прогоне (reserve_prewrite и floor'ы секций).
- Удалено: ничего — enrich-rewrite fallback в `_same_section_reserve_line` покрывает секции вне квоты и промахи prewrite.

### 0099 — Безопасная замена утренних источников ночным инвентарём — 2026-07-13
- Статус: внедрено; режим production остаётся `assist` до проверки новых ночных волн.
- Проблема: `MORNING_INVENTORY_MODE=on` мог отключить всю категорию `venues_tickets`, если только Ticket Radar набрал floor. Эти же 15 источников кормят `next_7_days`, `future_announcements` и `outside_gm_tickets`, но intake их не восстанавливал.
- Причина (корень): старый `_INVENTORY_SKIP_CATEGORIES` связывал категорию с одним блоком и принимал решение по предварительной полноте до live-дедупа.
- Решение: категория описывает все публичные блоки, которые она кормит. Broad scan можно заменить только для single-output категории или когда inventory восстанавливает весь набор блоков, последняя night-wave здорова и проверенная полнота выполнена. Ticket/Culture пока всегда остаются live; Food может быть заменён после доказательства.
- Почему решает класс: новый ticket/venue source не сможет потерять соседний блок из-за floor другого блока.
- ПРОВЕРКА: реальные состояния 10–13.07 — `venues_tickets safe_to_skip=false` во все четыре дня; на 13.07 старый preview считал Ticket complete (6), после live-дедупа было 1, новый контракт допускает 0 и не отключает источники.
- Удалено: `_INVENTORY_SKIP_CATEGORIES` — старое одно-блочное решение стало опасным и больше не используется.

### 0100 — Требования ночной карточки зависят от публичного блока — 2026-07-13
- Статус: внедрено; ПРОВЕРКА на реальных inventory snapshots 10–13.07.
- Проблема: одинаковое правило `event/date/venue` объявляло готовыми календарные страницы, `Next page`, события 2027 года, Food без конкретного места, Ticket без тира и прошедшие однодневные рынки.
- Решение: Weekend/7 days требуют конкретное событие, место, актуальную дату и action URL; Ticket — событие, дата, venue, ticket trigger и tier; Food — конкретный объект и venue плюс opening phase или дата; Pro — deterministic/CV match; Russian — positive language evidence. Хранятся `date_end`, recurrence, next occurrence и event status. Понедельник–среда Weekend не вставляется в утро; в остальные дни дата должна пересекать текущее weekend-окно.
- Почему решает класс: readiness теперь означает «эту карточку можно безопасно показать в её блоке», а не просто «у неё заполнены три поля».
- ПРОВЕРКА: replay intake 13.07 — 44 Weekend-карточки стали `weekend_hidden_by_schedule`, 32 прошедшие карточки `event_expired`, Ticket 6→0 без тира; Asian Food Night Market 10.07 и Makers Market 11.07 не проходят 13.07.
- Удалено: ничего — общий `evaluate_card` сохранён как одна точка решения, заменены только его слишком слабые требования.

### 0101 — Ночные факты сохраняются, резерв становится доступен только после validation — 2026-07-13
- Статус: внедрено; ПРОВЕРКА на реальном Asian Food record и replay 13.07.
- Проблема: утренний event enrichment мог заменить полный ночной `event` пустым результатом; inventory intake заранее ставил `recoverable_reserve/public_reserve`, а `is_recoverable_reserve` доверял этим флагам без validation.
- Решение: для `inventory_source=night_inventory` утреннее enrichment дополняет существующую карточку и не стирает date/venue/range; generic title не заменяет более конкретное night event name. Intake больше не ставит reserve-флаги. Любой explicit reserve проходит существующий `recoverable_reserve_eligible`.
- Почему решает класс: потеря поля на повторном extraction не разрушает карточку, а rejected/stale/non-GM запись нельзя вернуть одним старым флагом.
- ПРОВЕРКА: реальный Asian Food record — standalone morning extraction давал `The SK Lowdown`, пустые venue/date; после merge сохранены `Stockport's Asian Food Night Market`, Churchgate и 2026-07-10. Replay 13.07: три rejected/stale Food reserve строки исчезли; 24→23 видимых пункта без пустого обязательного блока.
- Удалено: прежняя безусловная выдача reserve по `recoverable_reserve/public_reserve`; отдельного старого пула не осталось.

### 0102 — Полнота считается после карточного контракта; иначе остаётся live-сбор — 2026-07-13
- Статус: внедрено; default `assist`, production skip ещё не включён.
- Проблема: `complete=true` считался по числу предварительных кандидатов до проверки и cap; `by_block.inserted` считался до cap. Утро могло пропустить источники по карточкам, которые затем исчезали.
- Решение: полнота требует post-contract candidates, готовый текст не ниже floor и минимум два источника. Если любое условие или health последней волны не выполнено, существующий обычный live-сбор категории остаётся включённым; нового fallback/recovery пути нет.
- Почему решает класс: тихая потеря превращается в обычный live collect, а не в пустой блок после ошибочного skip.
- ПРОВЕРКА: реальные snapshots 10/11/12/13.07 — новый intake W/T/F: `3/0/1`, `5/0/2`, `5/0/1`, `0/0/1`; ни в один день категория не признана безопасной для skip. Старые вставки: 16/23/25 на 11–13.07; новые: 7/6/1.
- Удалено: отдельный targeted fallback не добавлялся; используется существующий live collect, когда replacement не доказан.

### 0103 — Честная night→morning→HTML воронка — 2026-07-13
- Статус: внедрено; ПРОВЕРКА на state 13.07 и 860/0.
- Проблема: green night workflow скрывал source errors; Telegram показывал общий размер склада вместо найденного за волну; `morning_consumed=true` мог означать только наличие report; inserted по блокам не учитывал cap; не было связи с final HTML.
- Решение: night row получает `run_id`, expected/checked/errors/found/fact-ready/render-ready; source error делает wave `degraded` и command non-zero, но partial inventory всё равно коммитится. Morning report показывает records→card-ready→eligible→after live dedupe→after cap, причины live collect/skip. Release связывает inventory fingerprints с validation, writer и URL финального HTML.
- Почему решает класс: по одному отчёту видно не размер склада, а где именно потерялась карточка и была ли строка видна читателю.
- ПРОВЕРКА: старый state 13.07 теперь читается как 25 inserted → 25 present → 25 validated → 2 writer rendered → 2 visible HTML; причины остальных разложены по блокам. Полный набор: 860 tests, OK.
- Удалено: ложное правило `morning_consumed = mode + report exists`; теперь consumed только при `inserted_candidates>0`.

### 0104 — Ночной текст пишется обычным writer и принимается только после его quality contract — 2026-07-13
- Статус: закрыто решением 0111: model text prewrite полностью удалён; ночью остаётся только существующий deterministic writer.
- Проблема: deterministic prewrite объявлял render-ready английские заголовки с шаблонными концовками; на складе 13.07 было 149 render-ready, но повторная проверка старых строк оставила только 1 Weekend и 2 Pro безопасных deterministic строки.
- Решение: ночью используется только существующий deterministic writer для поддержанных стабильных блоков; Food и любые карточки без безопасной deterministic-строки пишет утром обычный rewrite. Model text prewrite, его prompt и ночной provider route удалены. Professional LLM сохранён только как CV-решение, не как автор публичного текста.
- Почему решает класс: ночь не создаёт второй слабый writer; утро получает строку того же качества и переписывает только выбранные `needs_text`.
- ПРОВЕРКА: реальные inventory records 13.07 — старые `old_render_ready`: Weekend 75, Ticket 29, Food 7; после текущего deterministic quality contract: 1/0/0. Wave `20260713T124932+0100`: 15 источников, 75 найдено, 0 errors, model 8 requested / 3 written / 5 rejected; ручная проверка нашла future-tense Food и рекламный diaspora-текст. После prompt-правки wave `20260713T125804+0100`: те же 75/0, model 8/5/3; diaspora стал нейтральнее, но Food снова написал прошедшее открытие 10.07 как будущее 13.07. После удаления Food prewrite wave `20260713T130239+0100`: 75/0, 13 fact-ready, 10 morning-eligible, model 7/3/4; Joe & The Juice теперь `needs_text`, `render_ready=false`, `draft_line=''`. Но одна из трёх принятых diaspora-строк закончилась рекламным «Не упустите возможность» вопреки prompt и прошла существующий quality contract. По правилу задачи новый смысловой gate не добавлен; требуется owner-решение: удалить весь model night prewrite или разрешить новый контроль.
- Удалено: deterministic prewrite для Food/diaspora и затем весь model prewrite для Food; отдельный новый text fallback или date-gate не добавлялся.

#### План включения после 0112
- После push: production-wave `pro_food_russian`; проверить source health, CV status и provenance, model text count обязан быть 0.
- 14–16.07: три утра в `assist`; сравнивать `scan_complete`, `block_sufficient`, inserted/validated/rendered/HTML по всем 17 идентификаторам.
- Не раньше 17.07: включать `on` только для Food, если три волны подряд дали floor 3 и минимум два источника; Russian/Pro остаются assist до отдельного разрешения в реестре.
- После трёх успешных Food-canary утр сравнить потерянные live-кандидаты и время. Culture/Ticket не переключать, пока весь их output-набор не восстановлен.

### 0105 — Один реестр ночной политики для всех 17 блоков — 2026-07-13
- Статус: внедрено; production остаётся `assist`.
- Проблема: assist, hybrid, completeness, category outputs, intake caps и prewrite жили в пяти списках и одном hardcoded union; Next7/Pro/Russian не доходили до morning intake.
- Решение: `INVENTORY_BLOCK_REGISTRY` содержит все 17 active/legacy ID и их source/candidate categories, полный output-набор, mode, serving TTL, retention, text policy, floor, cap и отдельное разрешение replacement. Все прежние списки удалены; output-набор выводится из реестра.
- ПРОВЕРКА: реальный inventory 13.07 строит отчёт по 17 ID; `venues_tickets` выводится как Ticket+Future+Outside, `culture_weekly` как Weekend+Future; registry==PRIMARY_BLOCKS. Offline: целевые тесты OK.
- Удалено: `INVENTORY_ASSIST_BLOCKS`, `INVENTORY_HYBRID_BLOCKS`, `INVENTORY_COMPLETENESS_*`, `INVENTORY_CATEGORY_OUTPUT_BLOCKS`, `INVENTORY_INTAKE_CAPS`, `NIGHT_PREWRITE_CAPS` и hardcoded prewrite-union.

### 0106 — Evidence hash инвалидирует текст при изменении любого текстового факта — 2026-07-13
- Статус: внедрено; prod-proof после следующего morning intake.
- Проблема: смена конца события, recurrence/status, тира, площадки, CV/access или booking URL не меняла hash; старый русский текст мог пережить отмену или новую фазу.
- Решение: hash включает canonical action URL, дату/диапазон/occurrence/status, venue scope/city, ticket trigger+tier, Food phase, Pro CV/access, Russian evidence/geography и hard-news facts. Изменившаяся fact-complete карточка возвращается в `needs_text`.
- ПРОВЕРКА: реальный state 3392 records — 276 прежних строк инвалидированы; tracking query не меняет hash, `scheduled→cancelled` меняет. Offline: regression OK.
- Удалено: прежняя узкая identity `name+date_start+venue`.

### 0107 — Полная provenance и жизненный цикл карточки — 2026-07-13
- Статус: внедрено и подтверждено production-wave; режим остаётся `assist`.
- Проблема: source report category терялась после routing; provider/model/time не сохранялись; нельзя было отличить первое появление от обновления фактов.
- Решение: collector штампует `source_report_category`; record хранит candidate category, run/wave/source, provider/model/written_at, first_seen/last_seen/last_changed. Deterministic night text помечается `night_inventory_deterministic`, а не двусмысленным prewrite-label. Merge сохраняет first_seen и меняет last_changed только при новом evidence hash.
- ПРОВЕРКА: GitHub production run `29266787848`, run_id `20260713T173233+0100`: 75 found, 68 unique observed; 68/68 имели полный provenance, observation и retention. Deterministic provider 5/5=`night_inventory_deterministic`, старый label=0. Offline merge regression подтверждает unchanged/changed timestamps.
- Удалено: утреннее связывание inventory с report по routed candidate category.

### 0108 — Единственный operational health только по текущей ночной волне — 2026-07-13
- Статус: внедрено и подтверждено production-wave; replacement не включён.
- Проблема: старый зелёный прогон мог разрешить source skip через несколько дней; причины source errors терялись.
- Решение: `operational_night_category_health` выбирает последний run_id категории, требует текущую London-date, checked==expected и 0 errors, хранит source+reason. Исторический rollup остаётся только отчётной статистикой и не участвует в replacement.
- ПРОВЕРКА: production run `20260713T173233+0100`: Food 3/3, Pro 6/6, Diaspora 6/6, errors=0; health каждого блока ссылается только на этот latest category run. Старый healthy run в regression получает `stale`.
- Удалено: `latest_night_category_health`, допускавший зелёный run без проверки даты.

### 0109 — Observation, action liveness, serving TTL и retention разделены — 2026-07-13
- Статус: внедрено; action URL остаётся честно `unknown`, пока сам URL не проверен.
- Проблема: успешный list fetch выдавался за liveness карточки; `expires_at` смешивал утреннюю свежесть и удаление будущих событий.
- Решение: отдельные `observed_in_wave/observed_run_id`, `action_url_liveness/action_url_checked_at`, `serving_ttl_hours/serving_expires_at`, `retention_until`. Future/Ticket retention строится от даты события и не сокращается serving TTL.
- ПРОВЕРКА: production run `20260713T173233+0100` — 68/68 observed, 68/68 retention, liveness 68 unknown (список не выдаётся за проверку action URL). Offline record schema regression OK.
- Удалено: запись нового общего `expires_at/liveness_status`; legacy read остаётся только для старых records.

### 0110 — Scan completeness и достаточность блока больше не смешиваются — 2026-07-13
- Статус: внедрено; source replacement не включён.
- Проблема: count+text floor одновременно изображал здоровье источников и полноту блока; Weekend/A-tier терялись по intake cap; optional zero считался поломкой; слабые карточки принимали leisure Next7 и deterministic Pro.
- Решение: `scan_complete` считается по current run/source errors, `block_sufficient` — по post-card facts/floor/source diversity до visibility schedule/cap. Weekend и A-tier cap-exempt; Future/Outside honest-zero. Карты требуют Weekend activity+GM, Next7 non-leisure, Ticket scope+why-now, Outside A, Food meaning, Pro governing LLM CV+access, Russian geography. Если extracted и LLM access спорят между `free` и `paid`, итог хранится единообразно как conditional/booking-required, без взаимоисключающих полей.
- ПРОВЕРКА: реальный current pool: 7/7 visible-candidate Next7 leisure rerouted, после правила 0 остаётся в Next7; production run `20260713T173233+0100`: Food scan complete=true/block sufficient=false (1<3), Russian true/true (2), Pro true/true (2). До правки Bolton networking хранил free+paid одновременно; после — `booking_required/conditional`. Offline 25 A-tier → 25 intake, held=0.
- Удалено: text как обязательная часть Food completeness и cap для protected Weekend/A-tier.

### 0111 — Удалён ночной model text prewrite, Pro CV сохранён — 2026-07-13
- Статус: внедрено и подтверждено GitHub production CV.
- Проблема: model prewrite пропустил рекламную diaspora-концовку; новый смысловой gate был бы вторым слабым редактором.
- Решение: удалены model text function/prompt/route/tests. Ночью остаётся deterministic writer; Pro запускает существующий `apply_professional_event_llm_matches` после per-card enrichment, но модель решает только fit/access.
- ПРОВЕРКА: production run `29266787848`: model-text providers=0, deterministic providers=5; Pro CV `status=ok`, 8 eligible/8 sent, 4 applied/4 skipped, provider OpenAI, model `gpt-4o-mini`.
- Удалено: `prewrite_inventory_candidates`, `NIGHT_INVENTORY_PREWRITE_RULES`, model prewrite pool/caps/report/tests.

### 0112 — Командный файл, продуктовые правила и расписание приведены к production truth — 2026-07-13
- Статус: внедрено; workflow production proof получен.
- Проблема: AGENTS требовал удалённый runtime sync и утверждал, что Python не вызывает модели; Ticket/Outside docs спорили с A-tier; District выглядел активным; 0104-plan ссылался на удалённый model prewrite.
- Решение: AGENTS описывает GitHub deployment и Python model runtime; Product Contracts фиксирует A-tier, night inventory и retired District; workflow документирует cron-job.org Europe/London 00:31/02:07/03:37/06:17/07:31; план 0104 заменён планом после 0112.
- ПРОВЕРКА: docs/code anchors сверены; `rg` не находит удалённые model-prewrite symbols в active code/tests. Replay 13.07 old/new: оба 12 sections, 23 bullets, lead ok, max blank 1; rebuilt HTML SHA-1 идентичен. Workflow run `29266787848` завершён за 2m31s и отправил production state/уведомление; schedule не менялся, только зафиксирован без UTC/BST двусмысленности.
- Удалено: устаревшие команды `sync_runtime_bundle.sh`/`run_daily_digest.sh` из AGENTS; District из active purpose classes, writer order, editor trim, pre-send judge и low-signal sections.

### 0113 — Ночная и live-карточка сохраняются как одна редакционная линия — 2026-07-15
- Статус: реализовано локально; production proof ожидается после push.
- Проблема: 15.07 из 30 morning-eligible ночных карточек 29 были помечены `duplicate_live_or_inventory` и исчезли из дальнейшей воронки; ночные event/CV/ticket-факты не дополняли утреннюю карточку.
- Решение: fingerprint остаётся главным identity; canonical URL используется вторично только для отдельной статьи/события. Свежая live-карточка остаётся основной, ночь дополняет только пустые структурированные поля и записывает lineage/provenance.
- Почему решает класс: повторный утренний fetch больше не уничтожает накопленные ночью факты, но не может заменить свежий live-факт устаревшим значением или склеить записи через index/homepage.
- ПРОВЕРКА: реальные state 15.07, 3735 inventory records + 1372 final candidates: до — 30 eligible, 29 dropped-as-live-duplicate и 1 inserted; после на том же наборе — 30/30 lineages merged into live, 0 потерянных совпадений. Targeted inventory tests OK; production proof pending.
- Удалено: отдельный параметр/путь `existing_fingerprints`, который умел только выбрасывать совпадение без доступа к live-карточке.
- Production-proof 17.07: из 31 активной ночной линии 30 объединены с live-карточками и 1 добавлена после live-дедупа; все 31 найдены в конечном pipeline, `missing_after_pipeline=0`. Режим source replacement не включался.

### 0114 — Зарезервировано: решение финального судьи — отдельный пакет — 2026-07-15
- Статус: не реализовывалось в этом пакете по решению owner.
- Граница: pre-send/final judge, его решения и repair loop не менялись; ID сохранён, чтобы следующий пакет не смешивался с night inventory.
- ПРОВЕРКА: `git diff` текущего пакета не затрагивает `pre_send_quality_judge.py` и judge-функции release.
- Удалено: ничего — это запись о границе работ, не доработка.

### 0115 — Удалено заполнение блоков контентом другого назначения — 2026-07-15
- Статус: внедрено; повторно исправлено по production-proof 16.07, новый morning proof ожидается.
- Проблема: validator после всех правил переносил кандидатов между Weekend/Next7/Today, поэтому 15.07 отчёт показал 4 переноса в Next7 и 3 в Today независимо от конечного назначения. В writer параллельно лежал никогда не вызываемый `_backfill_today_focus`; отчёт всегда писал `backfilled_today_focus=0`.
- Решение: удалён весь `practical_backfill` и мёртвый writer-backfill. Существующая `_allocate_fresh_and_today_focus` сохранена: она выбирает только Today-eligible practical items и честно логирует исходный блок.
- Почему решает класс: раздел нельзя наполнить строкой только ради количества; событие за пределами текущего weekend остаётся Future, а Today получает только элементы, прошедшие его собственный смысловой контракт.
- ПРОВЕРКА: реальные state 15.07 до — `practical_backfill={next_7_days:4,today_focus:3}`, мёртвый writer count=0. После: 877/880 tests OK; три оставшихся падения — существующие calendar-sensitive Ticket fixtures с `last_seen_at=2026-07-09`, которые 16.07 дают `ttl_expired`; этот пакет TTL не меняет. Все 12 replay-дней прошли. 12.07 baseline/replay после одинаковы: 13 sections/44 bullets, lead missing, max blank 1; 15.07 — 13/20, lead ok, max blank 1. Active Today board не изменён.
- Удалено: `practical_backfill.py`, его validator timing/report/tests; `_backfill_today_focus`, четыре dead constants и всегда-нулевое report field.
- Повторная production-проверка 16.07: первоначальное решение оказалось неполным. Writer снова переносил три `culture_weekly` карточки из `future_announcements` в `next_7_days`, validator переносил два transport-no-impact кандидата (`Major tram works`, `Prestwich Tram Stop works`) в City Radar, а Today выбрал две жилищные истории без действия сегодня.
- Дополнительное решение 16.07: non-impact transport теперь удерживается своим существующим passenger-impact контрактом и не переезжает в City; leisure из Today/Future возвращается только в Weekend/Tickets/Russian/Future своего purpose-класса; любое Today-событие обязано иметь явное текущее действие, предупреждение, закрытие или срок.
- ПРОВЕРКА 16.07 на реальном state: writer replay `next_7_days 3→0`, `future_announcements 3→6`; финальный Today вместо жилья/старого суда оставил два действующих предупреждения. Реальные карточки `Major tram works` и `Prestwich Tram Stop works` на повторном validator-probe получили `transport_no_passenger_movement_impact`, `include=false`, не меняя block на City. Production morning proof нового validator ожидается 17.07.
- Дополнительно удалено: `_reroute_non_impact_transport`, его infra/incident routing regex и мёртвый `_complete_next_7_rescue_candidate`; отдельного recovery/fallback не добавлено.
- Дополнение 17.07: release больше не дописывает обычные строки после writer/editor ради minimum или `must_show`; он только сообщает недобор. Сохранён единственный временный curator-lead guard до отдельной 0114, потому что A/B replay выявил потерю уже выбранного лида в 5 из 12 исторических дней. Удалены Football→City soft-route и связанные predicates: несодержательный football теперь отклоняет validator, а спортивная карточка остаётся только в Football. Today проверяет исходные факты, поэтому сгенерированная practical-фраза не может превратить ретроспективу в действие на сегодня; leisure ближайших 7 дней больше не остаётся в Future.
- ПРОВЕРКА 17.07: в replay teacher/coroner retrospective исчез из Today, карточка Manchester City вернулась из City в Football, near-term HOME film исчез из Future; обычных поздних вставок release `0`. На реальной Russian-карточке `Скамейка` частично распроданные ценовые категории больше не считаются полным sold-out, строка стала видимой. После возврата только lead guard replay 29.06 сохранил 43 bullets/13 sections/lead ok, 02.07 — 60/13/lead ok против baseline 62/13/ok, 09.07 — 70/15/lead ok против 74/14/ok. Остальные широкие replay остановлены как избыточные; 155 узких тестов проходят.
- Дополнительно удалено 17.07: late ordinary section recovery, non-lead `must_show` insertion, Football→City routing и его regex/helpers. Editor same-block reserve и final-judge repair не удалялись: первый остаётся владельцем восстановления внутри того же блока, второй относится к исключённой из пакета 0114.

### 0116 — Одна конечная воронка для каждой ночной карточки — 2026-07-15
- Статус: внедрено; исправлено по production-proof 16.07, новый morning proof ожидается.
- Проблема: отчёт 15.07 показывал только 1 inserted candidate и терял 29 совпавших с live; верхний `morning_consumed=false` спорил с фактическим intake, а synthetic `Night Inventory` имел `fetched=false` и выглядел как failed source.
- Решение: actual intake хранит lineage всех records и отдельно считает rejected/hybrid/merged/inserted/held-cap. Release связывает каждую активную lineage с validation, selection, writer fingerprint и URL final HTML. Единственный top-level operational truth считает consumed при merge или insert; успешно прочитанный inventory отмечается loaded/fetched.
- Почему решает класс: count склада, готовность source replacement и судьба публичной строки больше не смешиваются; совпадение с live остаётся измеримым вкладом ночи.
- ПРОВЕРКА: реальный baseline 15.07 — 3735→138→30→1→0 visible и 29 lineages вне финала; после локального intake те же 30 eligible дают 30 merged lineages. Synthetic merged-live→writer→HTML regression даёт 1→1 visible. Production proof pending.
- Удалено: вложенное всегда-ложное `report_only_intake.morning_consumed` и логика финальной воронки только по `inventory_source=night_inventory`.
- Повторная production-проверка 16.07: 4 активные lineage были ложно помечены `missing_after_pipeline`, включая видимый в HTML `Chorlton Makers' Market`. После merge evidence-suffix оставался в `inventory_lineages[].fingerprint`, а финальный candidate использовал нормальный fingerprint без suffix; release искал только прямое равенство.
- Дополнительное решение 16.07: release строит обратный индекс по сохранённым `inventory_lineages`, после разрешения кандидата использует его настоящий fingerprint для validation/writer и сохраняет `final_reason`.
- ПРОВЕРКА 16.07 на реальном state/HTML: активные `missing_after_pipeline 4→0`; `present_after_pipeline 28→32`; Chorlton стал `visible_html`, Monatik — `rejected_by_validation` с сохранённой причиной старой даты; итоговая воронка 3871 lineage → 32 present → 32 validated → 29 accepted → 7 writer-rendered → 5 visible.
- Дополнительно удалено: ничего — прямой lookup по candidate fingerprint нужен для inserted и неизменённых карточек; добавлен только отсутствовавший lineage identity.
- Дополнение 17.07: итоговый отчёт отделяет исторические записи без доказанного текущего происхождения от operational lineages и отдельно показывает active-current судьбы по блокам. Карточка, снятая repeat/rendered quarantine, получает конечную причину и не считается потерянным `must_show`. Термин `silent_loss` заменён на точный `selected_not_rendered`.
- ПРОВЕРКА 17.07: реальная цепочка `3917 stored → 140 fact-ready → 31 morning-eligible → 30 merged + 1 inserted → 31 present → 28 validation-accepted → 8 writer-rendered → 6 visible`; активных `missing_after_pipeline=0`. Replay harness восстановлен на pre-send `published_facts`, иначе post-send snapshot дедупил выпуск сам с собой; sent HTML теперь контекст, а продуктовый regression — replay-before/replay-after согласно AGENTS.
- Дополнительно удалено 17.07: блокирующее сравнение current replay с post-send sent HTML и ложное смешение legacy inventory с текущей operational-воронкой.

### 0117 — Action URL и retention реально управляют складом — 2026-07-15
- Статус: реализовано и подтверждено production-wave.
- Проблема: на 15.07 все 1369 observed current-run records имели `action_url_liveness=unknown`, ни одна ссылка не была проверена, 9 записей уже вышли за сохранённый retention, физической очистки не было.
- Решение: ночью HEAD-проверяются уникальные action URL только fact-ready карточек. 2xx/3xx=alive; 404/410 требуют двух разных run_id; 403/405/429/timeout/network=unknown. Replacement требует достаточного alive-набора. Retention каждый раз пересчитывается из реестра; dead/expired/retired физически удаляются.
- Почему решает класс: временная блокировка сайта не уничтожает карточку, будущий билет не удаляется по serving TTL, а склад перестаёт расти бесконечно за счёт реально устаревших строк.
- ПРОВЕРКА: real-state dry run 15.07 на копии склада: 3735→3716, удалено 19 expired (Culture 15, Diaspora 2, Ticket 2), Food 20/20 сохранены, dead 0 до первой URL-волны. Production run `29418506950`, run_id `20260715T141718+0100`: URL checked 5, alive 5, not_found/unknown 0; cleanup 3741→3722, удалено 19 expired (Culture 15, Diaspora 2, Ticket 2), Food сохранён 20/20. Offline contract: 200/302 alive, 404/410 two-run dead, 403/429 unknown; future Ticket 01.12 retained to 31.12, Transport last seen 01.06 removed on 15.07. 877 tests OK.
- Удалено: доверие к сохранённому старому `retention_until` при очистке; отдельного fallback/refetch не добавлено.
- Дополнение 17.07: recurring-события получают вычисленный `next_occurrence`, а retention считается от следующей реальной даты, не от устаревшей structured date. При этом past one-off с явной датой нельзя оживить случайным словом из описания; Friday добавлен в поддерживаемую recurring-сетку Weekend.
- ПРОВЕРКА 17.07 на реальной карточке `The Asian Food Night Market Stockport`: до — `date_start=2026-07-10`, `next_occurrence` пуст, retention `2026-08-09`; после перестроения той же карточки — `next_occurrence=2026-08-14`, fact/render ready, retention `2026-09-13`. В текущем operational inventory: 1849 записей, URL `alive=76`, `unknown=1773`; unknown не считается dead и не разрешает replacement. Expired retention rows текущего run: `0`.
- Production-wave 17.07 после доставки (`29579584379`, `run_id=20260717T131549+0100`): 59/59 источников проверены, 57 fetched, 2 source errors, поэтому статус честно `degraded`; найдено 196 карточек, fact-ready 70, render-ready 43, morning-eligible 2; action URL 70 checked / 69 alive / 1 unknown. Cleanup `3938→3927`, удалено 11 expired Culture, Food сохранён 20/20. Source replacement не разрешён.
- Найденный production-дефект и исправление 17.07: recurring fact уже хранил `next_occurrence=2026-08-14`, но deterministic draft продолжал писать `10 июля` и `back in July`. Вычисление следующей даты перенесено перед prewrite, writer использует `next_occurrence`, а recurring-строка состоит из двух фактических предложений без пустой концовки. На той же сохранённой карточке до/после: `10 июля … back in July` → `14 августа … Asian Food Night Market. На площадке: рынок, еда.` Targeted test OK; replay 17.07 прошёл за 63.4s: 14 sections, 47 bullets, lead ok, release passed.
- Performance-ограничение: production events-wave заняла 880.96s. URL-проверка уже выполняется параллельно (20 workers), поэтому вслепую добавлять ещё одну параллелизацию нельзя; до controlled `on` нужно отдельно разложить время source collection/enrichment по существующим стадиям.
- Дополнительно удалено: дублированное вычисление recurrence внутри record builder; новых gate/fallback/recovery нет, deterministic writer сохранён по решению 0111.

### 0118 — Pro CV сохраняет решение для каждой отправленной карточки — 2026-07-15
- Статус: реализовано и подтверждено production-wave.
- Проблема: production night 15.07 отправил 8 Pro-карточек, модель вернула 7, но report остался `status=ok`; одна карточка не имела финального CV-исхода.
- Решение: каждому sent event назначается ровно один outcome: `go`, `consider`, `skip` или `held_error`. Пропущенный/дублированный ID остаётся held и делает batch `partial_failed`; новых model calls/retries нет.
- Почему решает класс: partial JSON больше нельзя принять за полный CV-отбор, и ни одна карточка не исчезает между `sent` и итогом.
- ПРОВЕРКА: реальный baseline 15.07 — 8 sent / 7 returned / status ok; regression на частичном ответе — `{go:1, held_error:1}`, `outcomes_conserved=true`, `status=partial_failed`. Production run `29418506950`: eligible/sent 7/7, outcomes `go=3`, `consider=2`, `skip=2`, accounted 7/7, `outcomes_conserved=true`, status ok. 18 Pro tests OK.
- Удалено: неявное допущение, что отсутствие ID в parseable model response является успешным результатом.

### 0119 — Ночная волна различает success, degraded и failed — 2026-07-15
- Статус: внедрено и подтверждено повторной production-wave 16.07.
- Проблема: 15.07 events/live_news/breaking собрали 59/37/21 источников, но из-за 3/2/1 source errors workflow стал красным, хотя inventory был сохранён. Частичная проблема сайта выглядела как падение всей ночи.
- Решение: `success` требует всех expected sources без ошибок; `degraded` означает представленный полный список источников с частичными errors/unchecked и сохраняет данные; `failed` — нет checked результата либо команда не представила все expected sources. Только failed возвращает non-zero. Replacement по-прежнему разрешён лишь при operational health `ok`.
- Почему решает класс: данные не теряются из-за одного 403/timeout, но частичная волна не получает права отключить live collection.
- ПРОВЕРКА: реальные run_id 15.07 классифицируются: events 59 checked/3 errors=degraded, tickets 15/0=success, pro-food-russian 15/0=success, live-news 37/2=degraded, breaking 21/1=degraded. Production run `29418506950`: все 15 источников представлены и checked/fetched, 0 source errors, `health=success`, exit 0, state commit `6e8e9a6`; helper regression отдельно подтверждает degraded/failed границы.
- Удалено: правило `any source error => command exit 1`; сохранение partial inventory существовало и оставлено.
- Повторная production-проверка 16.07: `wave_status=degraded` записывался в каждую строку и затем использовался как operational verdict категории. Поэтому здоровые `transport 2/2/0 errors`, `gmp 1/1/0`, `public_services 1/1/0`, `tech_business 6/6/0` ошибочно считались degraded из-за ошибок Media/Football той же волны.
- Дополнительное решение 16.07: night run сохраняет отдельный `category_status`; `wave_status` остаётся только сводкой. Source replacement принимает решение только по expected/checked/errors собственной категории и по-прежнему требует status=ok.
- ПРОВЕРКА 16.07 на реальном `inventory_run_log.jsonl`: `transport/gmp/public_services/tech_business degraded→ok`; `media_layer` и `football` остались degraded; все перечисленные категории по-прежнему имеют `safe_to_skip=false`, потому что source replacement для них не разрешён.
- Production-proof после доставки: GitHub run `29493494104`, run_id `20260716T121147+0100`, 37/37 источников представлены и checked, одна ошибка Football. Общая волна корректно осталась `degraded`; категории получили независимые статусы: `football=degraded`, `media_layer/transport/tech_business/public_services/gmp=success`. State commit `8700e3a`; operational health после чтения сохранённых строк: Football `degraded`, остальные пять категорий `ok`.
- Дополнительно удалено: использование общего `wave_status` как блокирующего условия категории; само поле оставлено для сводного состояния workflow.

### 0120 — Replay-песочницы удаляются по умолчанию — 2026-07-17
- Статус: внедрено и подтверждено реальным replay.
- Проблема: 48 обычных запусков `scripts/replay_day.py DATE` оставили 48 временных каталогов общим логическим размером 24,11 GiB; отдельные sandboxes занимали до 685,9 MiB.
- Причина (корень): `replay_one` создавал каталог через `tempfile.mkdtemp()`, а `main` удалял его только при `--golden`; одиночный запуск не имел пути очистки, а `--keep` управлял только golden-режимом.
- Решение: жизненный цикл вынесен в `replay_sandbox`: обычные запуски используют `TemporaryDirectory` и очищаются при успехе и исключении; `--keep` явно сохраняет временный каталог; `--sandbox` сохраняет пользовательский каталог. Для аварийного завершения временный каталог получает project-owned marker, а следующий запуск удаляет только помеченные каталоги этого проекта старше 24 часов. Каталоги `--keep`, `--sandbox` и чужие каталоги не удаляются.
- Почему так (отвергнутые альтернативы): безусловная очистка по одному glob опасна для чужих каталогов и ломает диагностический `--keep`; постоянное копирование всего sandbox сохраняет исходную утечку. Marker + возраст ограничивают аварийную уборку каталогами, которыми владеет этот harness.
- Ожидаемый эффект и метрика проверки: после обычного успешного или упавшего replay число оставшихся default-sandboxes не растёт; сохранение происходит только по явному запросу.
- Файлы/места: `scripts/replay_day.py:replay_sandbox`, `scripts/replay_day.py:cleanup_stale_replay_sandboxes`, `tests/test_replay_day.py:ReplaySandboxLifecycleTest`.
- ПРОВЕРКА (после прогона): до изменения — 48 каталогов / 24,11 GiB; после ручной очистки — 0. Реальный `replay 2026-07-16`: write/edit/build прошли за 62,3 с, временный каталог `replay_2026-07-16_5iq699t1` удалён при выходе, остаток — 0 каталогов. Точечные lifecycle-тесты: 5 OK, включая success, exception, `--keep`, `--sandbox` и безопасную аварийную очистку.
- Где была ошибка: временное размещение ошибочно трактовалось как постоянное диагностическое хранение; opt-in `--keep` не был применён к одиночному режиму.

### 0121 — Review-копии и тестовые каталоги получают явный жизненный цикл — 2026-07-17
- Статус: внедрено и проверено локально.
- Проблема: после исправления replay lifecycle в `/private/tmp` оставалось 10,60 GiB ручных baseline/review/stage3-копий, а `.claude/worktrees` внутри проекта занимал ещё 1,29 GiB. Отдельный helper теста ticket notability создавал каталоги через `mkdtemp()` без cleanup; накопилось 560 Python-temp-каталогов, 34 из них содержали cache-файл.
- Причина (корень): improvement 0120 намеренно очищал только marker-owned replay sandboxes. Независимые checkout/review-копии создавались вне этого контракта, Git worktree не удалялись через Git, а `_cache_path()` терял объект, отвечающий за lifecycle временного каталога.
- Решение: чистые завершённые worktree удалены через `git worktree remove` + `prune`, остальные подтверждённые review/audit-копии удалены по точным путям. В `AGENTS.md` добавлен обязательный lifecycle для временных workspace: context manager или EXIT trap, Git-aware removal, сохранение только по явному запросу и финальная проверка project-owned temp. Тестовый cache helper переведён на `TemporaryDirectory` с `addCleanup`.
- Почему так (отвергнутые альтернативы): расширять replay glob на `stage3*`/`news*` опасно — эти имена могут принадлежать текущей работе и не имеют marker. Безусловно удалять все worktree тоже нельзя из-за незакоммиченных изменений. Сначала проверяются status/ownership, затем удаляется точный путь правильным владельцем lifecycle.
- Ожидаемый эффект и метрика проверки: завершённая агентская проверка не оставляет baseline/review/worktree; `PrefetchTest` не увеличивает число `ticket_notability_cache.json` во временной папке.
- Файлы/места: `AGENTS.md:Temporary workspace lifecycle`, `tests/test_ticket_notability_enrich.py:PrefetchTest._cache_path`.
- ПРОВЕРКА (после прогона): суммарно `/private/tmp` + рабочее дерево проекта уменьшились на 11,10 GiB; `/private/tmp` 10,60→0,80 GiB, проект 2,85→1,56 GiB, `.claude/worktrees` 1,29 GiB→0. Единственный новый `news-stage3` (около 798 MiB) создан другой текущей работой после очистки и сохранён. `PrefetchTest`: 3 теста OK, число cache-файлов до/после 0→0.

### 0122 — Единая A-tier политика до timing/watch/cap/repeat — 2026-07-20
- Статус: развёрнуто в `main`; offline replay на реальных state подтверждён, следующий утренний production-proof ожидается.
- Проблема: аудит 20.07 показывал 28 распознанных A-tier артистов, 4 видимых и 24 потерянных по разным поздним правилам: timing, outside routing, watch, cap и repeat. Даже обязательные Bruno Mars/The Weeknd затем снимались writer из-за ложного конфликта длинной серии дат; Bruno дополнительно нарушал glossary внутренним сокращением `GM`.
- Причина (корень): A-tier определялся после части решений и не имел одной identity/консервации; общее правило same-artist-per-block схлопывало обычные разные концерты; `_line_has_conflicting_event_date` умел только короткую серию из двух дат.
- Решение: одна публичная карточка на распознанного A-tier артиста, приоритет Greater Manchester → nearby → ближайшая outside-GM дата. Выжившая карточка получает `must_show` до timing/watch/cap/repeat. Точные/служебные дубли, отменённые события и остальные строки того же A-tier артиста остаются вне выпуска с причиной. Финальная сверка считает `eligible/visible/missing/conserved` по HTML.
- Почему так (отвергнутые альтернативы): каждая feed-строка не является отдельным продуктом — 97 строк 20.07 представляли 31 артиста; новый cap снова нарушил бы правило A-tier. Общий same-artist guard для всех tier удалён, потому что разные обычные концерты — разные события.
- Ожидаемый эффект и метрика проверки: `a_tier_conservation.missing=[]`, `conserved=true`; обычные билеты разных venue/date не схлопываются.
- Файлы/места: `ticket_notability.py:a_tier_ticket_policy`, `plan_digest.py:_collapse_a_tier_event_runs`, `dedupe.py:_apply_ticket_tour_dedup`, `writer.py:_ticket_watch_decision`, `writer.py:_line_has_conflicting_event_date`, `verify_digest_plan.py:run_verify_digest_plan`.
- ПРОВЕРКА (offline replay реальных state): 18/19/20.07 — `eligible=29`, `visible=29`, `missing=0`, `conserved=true` во все три дня; Bruno Mars и The Weeknd больше не снимаются. Golden replay 12/12 прошёл: lead ok и blank-runs 0. Цена правила видимости зафиксирована честно: итоговый объём на golden-днях 60–101 bullet, потому что A-tier идёт сверх общего бюджета.
- Удалено: общее `Same artist — one ticket card per block`, которое резало все tier; A-tier identity заменяет его только для распознанных артистов.

### 0123 — Recurring Weekend использует следующую дату, а не прошлую — 2026-07-20
- Статус: развёрнуто в `main`; проверено на реальном inventory 19.07, следующий утренний production-proof ожидается.
- Проблема: 310 Weekend-записей → 29 fact-ready → 0 morning-eligible. Burnage/Bowlee имели `date_start=12.07`, `next_occurrence=19.07`, но intake отклонял их как `event_expired`.
- Причина (корень): TTL, morning contract, supply contract и writer по-разному читали `date_start/date_end`; только часть кода знала о `next_occurrence`.
- Решение: `effective_occurrence_window` сдвигает старый recurring-интервал на `next_occurrence`; одна и та же эффективная дата применяется в intake, supply, draft-date conflict и writer.
- Почему так (отвергнутые альтернативы): перезаписывать исходный `date_start` нельзя — он нужен для provenance; специальный hardcode Burnage/Bowlee не исправил бы остальные recurring-события.
- Ожидаемый эффект и метрика проверки: recurring-карточка с текущим `next_occurrence` проходит intake и пишет текущую дату; one-off со старой датой остаётся expired.
- Файлы/места: `weekend_inventory.py:effective_occurrence_window`, `inventory.py:passes_morning_contract`, `writer.py:_is_expired_event_candidate`.
- ПРОВЕРКА (реальный inventory 19.07): Weekend bucket `310 records → 29 card-ready → 3 eligible/inserted` вместо 0; проходят The BIG Stockport Car Boot, Burnage RFC и Bowlee с effective date 19.07 при сохранённом `date_start=12.07`. На 20.07 те же события корректно скрыты расписанием понедельника, а не помечены expired.
- Удалено: прямое принятие Weekend-решений по старому `date_start` в затронутых местах; provenance-поле не удалялось.

### 0124 — Food восстанавливает именованный объект и входит в утро с валидным dedupe — 2026-07-20
- Статус: развёрнуто в `main`; проверено на реальном inventory 20.07, следующий утренний production-proof ожидается.
- Проблема: 22 записи → 4 fact-ready → 2 eligible → 0 visible; только один источник. Rudy's терялся как `Invalid dedupe decision`, а старый Joe & The Juice нельзя было возвращать ради floor.
- Причина (корень): существующие title/evidence уже содержали название ресторана/магазина, но fact-card оставлял `venue` пустым; inventory-кандидат повторно входил в pipeline с невалидным старым dedupe-состоянием.
- Решение: детерминированно восстановить именованный Food-объект только из уже собранных title/evidence; при morning intake начинать обычный dedupe как `new / pending dedupe`. Food доверяет `next_occurrence` только при явном weekly/monthly тексте; одноразовое `opens on Friday` больше не классифицируется как recurrence.
- Почему так (отвергнутые альтернативы): новые источники и модельные догадки не нужны; ослабление freshness вернуло бы Joe & The Juice и другие старые открытия.
- Ожидаемый эффект и метрика проверки: минимум 3 morning candidates из минимум 2 sources; Rudy's проходит, Joe остаётся expired.
- Файлы/места: `inventory.py:_repair_food_opening_card`, `inventory.py:inventory_record_to_candidate`, `inventory.py:passes_morning_contract`.
- ПРОВЕРКА (реальный inventory 20.07): `22 records → 8 card-ready → 5 eligible/inserted`, два источника (`About Manchester Food & Drink`, `Manchester's Finest`); среди пяти Rudy's, OSMA, Fosforo Lounge, Sainsbury's и Gaucho. Rudy's = `dedupe_decision=new`; Joe не возвращён и остаётся `event_expired`, несмотря на ошибочно сохранённый старый `next_occurrence`. Отдельный контракт подтверждает: настоящий monthly Asian Food Night Market продолжает использовать следующую дату после свежего наблюдения.
- Удалено: ничего — нормальная morning dedupe-стадия нужна; заменено только невалидное унаследованное стартовое решение inventory-кандидата.

### 0125 — Финальная видимость считается только по отправляемому HTML — 2026-07-20
- Статус: развёрнуто в `main`; проверено replay 19/20.07, следующий утренний production-proof ожидается.
- Проблема: writer сообщал Weekend=6 и Transport=3, тогда как sent HTML 19.07 содержал 5 и 1. Visible-contract замечал часть расхождений, но итоговый selection report объединял writer fingerprints с HTML и завышал видимость.
- Причина (корень): `_write_final_selection_report` считал строку visible, если она была либо в HTML, либо когда-то rendered writer; финальная проверка не пересчитывала минимумы по последнему HTML после pre-send judge.
- Решение: при наличии финального HTML видимость определяется только его source links; verify после judge пересчитывает section counts, required-slot loss, A-tier conservation и заново пишет final selection report.
- Почему так (отвергнутые альтернативы): writer-report полезен как стадийная воронка, но не может быть доказательством того, что получил читатель.
- Ожидаемый эффект и метрика проверки: отчёт и HTML имеют одинаковые visible counts; underflow/потеря required slot всегда получают divergence.
- Файлы/места: `release.py:_write_final_selection_report`, `verify_digest_plan.py:run_verify_digest_plan`.
- ПРОВЕРКА (real-state replay): 19.07 финальный HTML теперь измерен как Weekend=8 и Transport=3 после Stage 3 (sent было 5 и 1); 20.07 A-tier `29/29`, а реальные underflow Food/7-days/Today остаются явными divergences, не маскируются writer count.
- Удалено: объединение `HTML-visible ∪ writer-rendered`, потому что оно создавало ложную видимость.

### 0126 — Events sources собираются с bounded parallelism и per-source timing — 2026-07-20
- Статус: развёрнуто в `main` и подтверждено реальной GitHub Events-wave на production-substrate.
- Проблема: реальные events-wave занимали 15:52 и 16:12; 59 источников обходились последовательно, параллельной была только последующая проверка URL.
- Причина (корень): `cmd_collect_inventory` вызывал `_collect_single_source` внутри обычного `for source in sources`.
- Решение: fetch/extract источников выполняются пулом максимум 8 workers с сохранением registry-order результатов. В run log добавлены `collect/fetch/extract/enrich/duration_seconds`; enrichment, merge и URL probe остаются существующими стадиями.
- Почему так (отвергнутые альтернативы): новая очередь/модель/стадия не нужна; 59 одновременных запросов опасны для сайтов, поэтому concurrency ограничена 8.
- Ожидаемый эффект и метрика проверки: 59 source rows сохранены в прежнем порядке, peak concurrency ≤8, реальная events-wave заметно короче 15:52 при той же полноте.
- Файлы/места: `scripts/run_local_digest.py:_collect_inventory_sources`, `scripts/run_local_digest.py:cmd_collect_inventory`.
- ПРОВЕРКА: 4 искусственно медленных источника при max_workers=2 дали peak=2 и сохранили исходный порядок. Реальный GitHub run `29740487193`, run_id `20260720T125954+0100`: 59/59 источников проверены, 203 записи найдены, 70 fact-ready, 41 render-ready, 2 source errors, итог честно `degraded`; `max_workers=8`, `ordered_results=true`, `per_source_timing=true`. Время сбора `216.42s` против прежних `952–972s`: сокращение на 77–78%, примерно в 4,4 раза. Per-source timing локализовал оставшийся хвост в четырёх Visit Manchester extractors: `125.7–195.2s` каждый.
- Удалено: последовательный source-fetch loop; параллельный action-URL probe оставлен, потому что решает другую I/O-стадию.

### 0127 — Daily state commit переиспользует ночной retry-loop — 2026-07-20
- Статус: развёрнуто в `main`; production race proof ожидается при следующей реальной гонке refs.
- Проблема: отправленный выпуск 18.07 потерял production-state после единственного `git push`, отклонённого GitHub как `commit_refs failure`.
- Причина (корень): night workflow уже имел bounded pull/rebase/push retry, daily workflow выполнял только одну попытку.
- Решение: daily state commit делает до 5 попыток `pull --rebase --autostash + push`, между попытками отменяет незавершённый rebase и ждёт 5/10/15/20/25 секунд.
- Почему так (отвергнутые альтернативы): новый state backend не требуется для обычной гонки refs; бесконечный retry мог бы удерживать workflow и скрывать постоянную ошибку.
- Ожидаемый эффект и метрика проверки: transient ref race сохраняет state на одной из 5 попыток; после пятой workflow честно падает.
- Файлы/места: `.github/workflows/daily-digest.yml:Commit updated state files`.
- ПРОВЕРКА (offline contract): workflow содержит тот же bounded 5-attempt pattern и явный fail after exhaustion; production race пока не воспроизводилась.
- Удалено: одноразовый daily `pull + push`; ночной loop не дублируется новой архитектурой, а переиспользуется тем же shell-паттерном.

### 0128 — Night state commit сохраняет dispatched branch — 2026-07-20
- Статус: развёрнуто в `main` и подтверждено повторным production-substrate run.
- Проблема: реальная acceptance Events-wave `29739894935` собрала все 59 source rows за 237,39 с, но state commit пять раз пытался rebase feature-ветку на `origin/main`, получил массовые add/add conflicts и завершил workflow красным.
- Причина (корень): `.github/workflows/night-inventory.yml` жёстко использовал `git pull ... origin main`, хотя `workflow_dispatch --ref` корректно checkout-ил `stage3-plan-lock`.
- Решение: pull/push retry работает с `${GITHUB_REF_NAME:-main}` и явно пушит `HEAD:<dispatched branch>`. Для production dispatch это без изменения остаётся `main`; acceptance state не загрязняет main и не теряется.
- Почему так (отвергнутые альтернативы): отключить state commit на branch-run лишило бы проверку доказательства сохранения; force-push или rebase feature-ветки на main недопустимы.
- Ожидаемый эффект и метрика проверки: acceptance wave сохраняет inventory commit в свою ветку с первой попытки; production wave продолжает сохранять в main.
- Файлы/места: `.github/workflows/night-inventory.yml:Commit inventory to repo`, `tests/test_inventory.py:NightWaveTest`.
- ПРОВЕРКА: первый реальный run `29739894935` — collection `59 sources / 203 found / 70 fact-ready / 2 source errors / degraded`, `duration_seconds=237.39`; state push не прошёл и стал основанием для этой доработки. Повторный run `29740487193` завершился `success`: state commit `c0e7cac` создан и запушен в `stage3-plan-lock` с первой попытки (`Pushed inventory on attempt 1`), Telegram-уведомление вернуло HTTP 200.
- Удалено: жёстко прошитый `origin main` в night retry; сам bounded retry остаётся.

### 0129 — Внутренний fallback запускает пропущенную Events-волну без дубля — 2026-07-20
- Статус: ПРОВЕРЕНО-работает в `main`.
- Проблема: 20.07 внешний cron-job.org не вызвал Events; GitHub workflow был только исполнителем и не имел собственного расписания. Пропуск обнаружился лишь утром.
- Причина (корень): `.github/workflows/night-inventory.yml` имел только `workflow_dispatch`; внутри GitHub не существовало второго независимого триггера и проверки уже завершённой волны за лондонскую дату.
- Решение: GitHub вызывает только Events в 00:41 London двумя DST-safe UTC schedules. Перед сбором команда `inventory-wave-complete` проверяет по `run_id`, дате London и всем `expected_sources`, завершалась ли волна. Уже выполненная внешним cron волна даёт `skip`; отсутствие полной волны запускает обычный сбор. `degraded` считается завершённым trigger: fallback исправляет пропущенный запуск, а не бесконечно повторяет ошибки отдельных сайтов.
- Почему так (отвергнутые альтернативы): переносить все пять расписаний в GitHub сейчас не требуется; запускать Events без idempotency создаст дубль; считать `degraded` отсутствующей волной превратит fallback в неограниченный retry источников.
- Ожидаемый эффект и метрика проверки: при существующей волне job заканчивается без `collect-inventory` и state commit; при её отсутствии создаётся ровно одна Events-волна за текущую London date.
- Файлы/места: `.github/workflows/night-inventory.yml:Resolve wave from dispatch input`, `scripts/run_local_digest.py:_complete_inventory_wave_for_day`, `tests/test_inventory.py:NightWaveTest`.
- ПРОВЕРКА: отдельный GitHub run `29747545437` в `mode=fallback` на реальном state 20.07 распознал run `20260720T125954+0100` как complete: `represented=59`, `checked=59`, `expected=59`, `errors=2`; шаги collect, state commit и оба Telegram notification были пропущены. Дневной workflow не запускался.
- Удалено: ничего — внешний cron остаётся первичным scheduler для всех волн; новый GitHub schedule является независимой страховкой только Events.

### 0130 — Events: исправлены мёртвые источники, catalog-шум и дочерний хвост Visit Manchester — 2026-07-20
- Статус: ПРОВЕРЕНО-работает в `main`.
- Проблема: production Events-wave `29740487193` имела 2 source errors и 127 записей с missing facts. Manchester Armed Forces Day возвращал 404, New Smithfield Sunday Market — 403. Четыре Visit Manchester source extractors занимали `125.7–195.2s`; в inventory попадали `${Tripbuilder.Path}` и страницы-каталоги `What's on at…`/`Events at…`, не являющиеся конкретными событиями.
- Причина (корень): завершившийся 27.06 one-off остался enabled; старый New Smithfield URL закрыт для runner; Visit Manchester дочерние страницы обогащались последовательно внутри уже параллельного source loop, а slug-extractor принимал CMS-template и venue/category indexes за события.
- Решение: истёкший Armed Forces source disabled до появления новой официальной датированной страницы; после того как в первой production-проверке второй council URL тоже вернул 403, New Smithfield переведён на датированную Manchester Rocks listing, которая называет оператором Manchester City Council и ведёт на официальный сайт; template/index URLs отсекаются до child fetch; оставшиеся child pages обогащаются максимум четырьмя workers на source и общим semaphore=8 с сохранением порядка и прежнего `max_candidates`. Дополнительный missing-facts аудит доказал, что Manchester Theatres `this-weekend/next-weekend` возвращают общий far-future Highlights catalogue — оба источника disabled. Для реальных Manchester's Finest карточек venue берётся из доказательного префикса `date - venue - exact title`; `comic con` признан конкретным protected activity type.
- Почему так (отвергнутые альтернативы): подменять истёкшее событие старой news-страницей нельзя; искусственно заполнять факты 127 строк нельзя, потому что значительная часть — каталоги, а не события; новый queue/stage не нужен после локализации времени существующим per-source timing.
- Ожидаемый эффект и метрика проверки: 0 ошибок этих двух источников; catalog/template строки не входят в inventory; Visit Manchester extract time заметно ниже 125 секунд при неизменном bound; missing-facts уменьшается только за счёт удаления ложных карточек, а не придуманных фактов.
- Файлы/места: `data/sources.toml`, `collector/filters.py:_looks_like_candidate_title`, `collector/extract.py:_extract_visit_manchester_events`, `collector/extract.py:_enrich_visit_manchester_items`.
- ПРОВЕРКА: первая production-wave `29747700346` после child-fix: Events `216.42→54.3s`; Visit Manchester `125.7–195.2→30.4–31.0s`; старые `${Tripbuilder.Path}` и venue indexes исчезли, missing-facts `127→121`. Она же доказала, что второй council URL New Smithfield всё ещё даёт 403. После замены URL и полного seasonal-index фильтра run `29748068447`: `health=success`, 58/58 sources checked, `source_errors=0`, Visit missing-facts=0, общий missing-facts `121→115`. Финальный production run `29749312146` после аудита всех 115: `health=success`, 56/56 sources checked, `source_errors=0`, 135 found, 59 fact-ready, 45 render-ready, `duration=53.84s`; оба ложных Manchester Theatres каталога отсутствуют, New Smithfield дал 1 карточку, Manchester's Finest fact-ready `1/12→4/12` (Bury Food Festival, Creatures Comedy Festival, Green Island Festival, Comic Con), общий active missing-facts `115→84` (`127→84` от исходного). Оставшиеся 8/12 Finest честно не проходят узкий protected Weekend activity type, а не теряют venue/date. Локальный child bound: `1 < peak <= 4`, input order сохранён.
- Удалено: оба проверенных New Smithfield council URL, возвращавших 403 на runner; последовательный Visit Manchester child-fetch path; две активные Manchester Theatres weekend поверхности, которые фактически отдавали общий каталог. Armed Forces и Theatres записи сохранены disabled в registry с точной причиной и условиями возврата.

### 0131 — CI снова проверяет main и не зависит от сети/дня недели — 2026-07-20
- Статус: ПРОВЕРЕНО-работает в `main`.
- Проблема: все последние Stage 3 branch pushes были красными: run `29741377844` падал на четырёх тестах, а production `main` вообще исключался из `tests.yml`, поэтому тот же SHA не получал CI verdict.
- Причина (корень): tram test ходил на живую главную TfGM и подмешивал текущий Eccles alert; два Weekend tests строили `now+5 days`, что по понедельникам попадало в текущий weekend и противоречило собственному ожиданию; road-only validator считал слова `road closure` доказательством воздействия на пассажиров; workflow имел `branches-ignore: main`.
- Решение: enrichment разрешён только для конкретного TfGM alert URL; Weekend tests заморожены на четверг; road alert остаётся только при явном bus/tram/train signal; `main` включён в Tests, но state-only commits не запускают 859 тестов.
- Почему так (отвергнутые альтернативы): ретраи CI не исправляют недетерминизм; ослаблять assertions нельзя, потому что road-only и Weekend routing — продуктовые правила; запускать полный suite после каждого ночного state commit бессмысленно.
- Ожидаемый эффект и метрика проверки: один и тот же commit зелёный локально и в GitHub; code/workflow push в main создаёт Tests run, state-only push — нет.
- Файлы/места: `.github/workflows/tests.yml`, `transport_card.py:_maybe_fetch_alert_text`, `candidate_validator.py:_exclude_road_only_transport`, соответствующие regression tests.
- ПРОВЕРКА: локально полный suite после Weekend-аудита — `863 tests`, `OK`, включая прежние 4 падения. Main CI `29747539017` прошёл после первого code push; финальный main CI `29749303386` — `863 tests in 24.593s`, `OK`. State-only Events commits новых Tests runs не создают.
- Удалено: `branches-ignore: main` и обогащение transport-карточки с generic TfGM homepage; отдельного старого механизма больше нет.

### 0132 — Stage 3: один исполнитель состава и честная финальная сверка — 2026-07-21
- Статус: внедрено; offline replay и real-state proof пройдены, следующий утренний production-proof ожидается.
- Проблема: 21.07 `plan_execution_report.json` уже имел 77 слотов (`70 shown + 7 removed`), но старый `final_selection_report.json` занимал около 500 KB и заново считал весь `candidates.json` по URL. Verify проверял URL «где-то в выпуске», editor сам вызывал позднюю замену, writer/editor/judge могли вставить transport-строку после плана, а repair executor называл 6 неисправленных строк `kept_below_floor`/`kept_style_issue_plan_locked` и показывал `unresolved=0`. Поэтому две Steel Panther и неверные The Stranglers/Ladytron могли выглядеть исправленными без проверки результата.
- Причина (корень): финальная отчётность была candidate-centric, а не slot-centric; блок строки не входил в идентичность исполнения; repair actions исполнялись независимо и не имели обязательного defect-specific post-check; любое применённое действие переводило judge в `warn/can_send=true`, включая оставшуюся известную фактическую ошибку.
- Решение: `plan_execution` хранит исходный/итоговый fingerprint, плановый block, status/reason/stage и после judge сопоставляет каждый слот ровно с одной HTML-строкой в том же разделе. `final_selection_report.json` теперь пишется только финальным verify после всех ремонтов и содержит `final_rows`, `removed_slots`, все `slot_outcomes`, URL/HTML line и отдельные `planned_shortfall`/`execution_loss`. Editor больше не заменяет/удаляет строки; pre-send executor один выполняет цепочку enrich/refetch → fact-locked patch → deterministic rewrite → backup того же слота → remove → post-check. Связанные замечания объединяются в operation; одна неудачная часть делает всю operation `unresolved`; unresolved known fact даёт `block`, а не `ship_degraded`. Общий prose-policy повторно применяется к финальным байтам после последней hygiene-правки. Neutral transport fallback создаётся только планёркой, только при свежем полном TfGM scan и только если ни одного конкретного transport slot нет.
- Почему так (отвергнутые альтернативы): считать URL-set достаточно нельзя — одинаковый URL и перенос между блоками теряют кратность/размещение; сохранять плохую строку ради floor скрывает execution loss; поздняя transport-вставка снова создаёт карточку вне плана; исправлять Steel Panther/The Stranglers/Ladytron точечными исключениями не закрывает класс группового и fact-specific ремонта.
- Ожидаемый эффект и метрика проверки: `final_html_rows == final_report_rows`; `slots == shown + replaced + removed`; у каждой shown/replaced строки совпадает planned/final section, removed URL отсутствует, foreign/duplicate HTML rows блокируют send; Today loss попадает только в `execution_loss`, а пустой план «7 дней» — только в `planned_shortfall`; unresolved wrong-artist блокирует send.
- Файлы/места: `plan_execution.py:build_final_execution_report/record_repair`, `verify_digest_plan.py:run_verify_digest_plan`, `pre_send_quality_judge.py:_apply_repair_executor/_finalize_repair_report`, `plan_digest.py:_planned_transport_status_candidate`, `editor.py:_apply_editor_line_actions/_pre_send_polish_sections`, `writer.py:write_digest`, `release.py:build_release`; `tests/test_plan_contract.py`, `tests/test_pre_send_repair_executor.py`, `tests/test_editor_pacing.py`.
- ПРОВЕРКА (offline, реальный state 21.07): replay до/после сохранил `70` карточек (`69` bullets + lead), `7` снятых слотов, lead `ok`, blank runs `0`, boilerplate endings `0`; после — verify `0.3s` вместо `10.0s`. Новый финальный отчёт: `77 slots = 70 shown + 0 replaced + 7 removed`, `70 HTML rows = 70 final_rows`, `lines_outside_plan=0`; Fresh `9/9`, Today `2/4` и `execution_loss=2`, Food `3/3`, «7 дней» `0/3` и `planned_shortfall=3`. На тех же HTML/actions 21.07 две Steel Panther дали одну operation `unresolved` при `visible_count=2`; две wrong-artist строки дали одну `unresolved`, `blocking_unresolved=1`, ожидаемый Gary Numan всё ещё отсутствовал. Точечные тесты: `43`, `OK`.
- Удалено: candidate-based `_write_final_selection_report`; финальный report до judge; editor `_plan_substitute_for_line`; поздние transport insertions writer/editor/judge; статусы `kept_below_floor` и `kept_style_issue_plan_locked`; автоматическое понижение unresolved factual error до `ship_degraded`.

### 0133 — План 2: правильные факты, night→live и продуктовый состав — 2026-07-21
- Статус: внедрено в `main`; offline tests/replay и ticket night production-run пройдены. Календарная проверка Weekend Thursday→Sunday остаётся до ближайшего четверга, потому что внедрение выполнено во вторник.
- Проблема: текущий night ticket inventory содержал 40 публичных строк «Фестивальный состав, не один артист»; Grace Jones и Sex Pistols/Gary Numan могли получать владельцем более рейтингового участника/support. Night-карточка могла самостоятельно войти в morning pipeline и Food разрешал пропуск live scan. Today занимал строки из Fresh/City; Food имел общий cap=6 вместо трёх; transport сохранял ограничение только после include; recurring/date/prose/protected решения расходились по локальным правилам.
- Причина (корень): artist notability одновременно выбирал event owner; A-tier имел отдельные repeat-обходы; inventory intake смешивал факты, текст и самостоятельную публикацию; writer держал поздний Weekend rescue; даты и prose-маркеры были продублированы между validator/writer/editor/release; protected назначался до проверок валидности.
- Решение: event owner отделён от lineup ranking; фестиваль сохраняет имя события и перечисляет A-tier acts внутри одной карточки, support не заменяет headliner. `Open Air` в имени площадки больше не превращает обычный концерт в фестивальную карточку: lineup определяется по идентичности события или явному structured lineup. `visible_repeat_verdict` стал единым repeat-решением с единственным `a_tier_must_show_override` после duplicate/status/expiry checks. Night дополняет только пустые structured facts совпавшей live-карточки и никогда не публикуется без morning confirmation; Food не может отключить live scan. Один `effective_occurrence_window` используется intake/Today/Weekend/writer/repeat/protected. Today больше не берёт доноров; market маршрутизируется до планёрки; late writer rescue удалён. Authoritative bounded TfGM restriction сохраняется независимо от news include и ежедневно создаёт планируемый reminder до expiry. Protected назначается после include/dedupe/date/block/prose validity. Один prose classifier используется writer/editor/repair/release/verify. Food cap=3 и в тройку попадают минимум два live-источника, если они есть; IT определяется содержанием, cap=5 остаётся максимумом без fill quota. Events-часть Плана 2 уже выполнена и production-проверена в #0130, повторно не переделывалась.
- Почему так (отвергнутые альтернативы): не добавлялись новые LLM, stages или repair queues; отдельные точечные исключения для Grace Jones/Bowlee/Asian Market отвергнуты в пользу общих owner/occurrence/routing contracts; night не повышен до второго издателя, потому что только morning live подтверждает актуальность.
- Ожидаемый эффект и метрика проверки: service ticket phrase 40→0, wrong owner=0, date/venue/link conservation 40/40; night funnel отдельно показывает fact-ready→live confirmed→enriched→planned→visible; Today не получает foreign donors; Food ровно 3 и ≥2 sources при eligible diversity; reminders исчезают после end date; prose service/empty markers отсутствуют в final HTML.
- Файлы/места: `ticket_notability.py`, `repeat_policy.py`, `dedupe.py`, `inventory.py`, `weekend_inventory.py`, `candidate_validator.py`, `transport_fill.py`, `story_intelligence.py`, `editorial_contracts.py`, `plan_digest.py`, `writer.py`, `editor.py`, `pre_send_quality_judge.py`, `release.py`, `verify_digest_plan.py`, `common.py`, `scripts/run_local_digest.py`, `scripts/replay_day.py`, `docs/PRODUCT_CONTRACTS.md` и точечные regression tests.
- ПРОВЕРКА (offline): текущий `venues_tickets.jsonl` перерендерен без записи state: служебная фраза `40→0`, неверные owners в именованных дефектах `0`, исходные date/venue/link сохранены `40/40`. Полный suite: `875 tests`, `OK`; отдельный production-дефект `SEX PISTOLS FT. FRANK CARTER` + площадка `Scarborough Open Air Theatre` закреплён одним targeted test, `OK`. Replay 19.07 до Плана 2: `81 slots / 79 lines / 3 removed`; после основного изменения: `74 slots / 71 shown / 4 removed`; после owner-нормализации: `73 slots / 70 shown / 4 removed` — две даты одного headliner больше не расходятся как разные владельцы; lead `ok`, max blank run `1`, blank runs 2+ `0`, boilerplate `0`. Golden replay 12/12 дней: все stages OK, lead `ok` 12/12, max blank run `1`, blank runs 2+ `0`, boilerplate endings `0`; known sent defects на golden-днях подтверждены harness.
- ПРОВЕРКА (production): GitHub Tests run `29824467868` прошёл на owner-fix commit `887c813`. Night tickets run `29824484181`: `15` источников, `815` найденных карточек, `86` morning-eligible, `0` source errors, `40.66s`, state push с первой попытки, Telegram status `200`. В сохранённом run `20260721T120216+0100` после merge: `814` карточек, служебных ticket-фраз `0`, именованных wrong-owner случаев `0`, owner `814/814`, date `803/814`, venue `802/814`, action link `814/814`.
- Где была ошибка (если не сработает): —

### 0134 — План 1: закрытие обходов финального состава после независимого аудита — 2026-07-21
- Статус: внедрено; offline tests и replay ключевых дней пройдены, следующий обычный утренний production-proof ожидается.
- Проблема: после #0132 оставалось пять обходов доказательного исполнения плана. Editor удалял байт-в-байт одинаковые строки, хотя они могли принадлежать разным слотам; финальный repair post-check мог пропустить служебный шаблон из общего prose-classifier; verify принимал любой непустой текст как код снятия; ручной `send-file` доверял ранее записанному verify-report; replay заканчивался без детерминированной pre-send финализации.
- Причина (корень): старый line-level dedup жил ниже immutable plan; repair использовал более узкий legacy-предикат; reason проверялся только на непустоту; send gate проверял release, но не пересчитывал соответствие точных отправляемых байтов плану; harness отражал старую границу `write→edit→build`.
- Решение: editor сохраняет все запланированные строки и только предупреждает о межсекционных коллизиях; repair и финальный prose-report используют `classify_prose_defects` с кодом и severity; removed-slot валиден только с причиной из `REMOVAL_REASONS`; `send-file` повторно запускает verifier на выбранном HTML; replay выполняет последнюю очистку пустых окончаний/заголовков и общий prose post-check до verify.
- Почему так (отвергнутые альтернативы): не добавлялись line hash, новый stage или второй отчёт состава; точный HTML уже можно проверить существующим slot verifier. Молчаливое удаление одинаковой строки отвергнуто, потому что это execution loss, который должен быть выражен через slot controller.
- Ожидаемый эффект и метрика проверки: ни один slot не исчезает в editor; произвольная причина снятия и stale/manual HTML блокируют send; служебный шаблон остаётся `unresolved`, пока реально присутствует; replay проверяет тот же детерминированный последний текстовый слой, что production.
- Файлы/места: `editor.py:edit_digest/_line_needs_russian_editor`, `pre_send_quality_judge.py:_finalize_repair_report`, `verify_digest_plan.py:run_verify_digest_plan`, `scripts/run_local_digest.py:_release_gate_error_for_file`, `scripts/replay_day.py:run_stages`; точечные regression tests.
- ПРОВЕРКА (offline): targeted contract tests `32`, affected-stage tests `335`, полный suite `884 tests`, все `OK`. Replay 09/19/20/21.07: все шесть стадий каждого дня `OK`, новая `pre-send-finalize` на каждом дне дала `0` пустых окончаний и `0` prose findings; verify сохранил slot outcomes и завершился `ship_degraded`, а не технической ошибкой. Golden replay `12/12`: все стадии `OK`, lead `ok` 12/12, max blank run `1`, blank runs 2+ `0`, boilerplate endings `0`; 27.06 честно показал `2` unresolved non-factual prose findings, остальные 11 дней — `0`. Ручной send без plan/execution и removed-slot с произвольным reason блокируются новыми regression tests.
- Где была ошибка (если не сработает): —

### 0135 — План 2: canonical A-tier, единый repeat/occurrence и гарантированный Food после независимого аудита — 2026-07-21
- Статус: внедрено; offline tests и replay ключевых дней пройдены, следующий обычный утренний production-proof ожидается.
- Проблема: после #0133 planner всё ещё схлопывал разные даты и площадки до одной карточки на артиста; validator/dedupe сохраняли локальные repeat-обходы; поверх существующего `effective_occurrence_window` оставался отдельный candidate-wrapper; protected вычислялся до финального `validated`; общий issue budget мог сократить Food с контрактных трёх строк до двух.
- Причина (корень): A-tier identity была artist-centric вместо physical-event-centric; repeat verdict применялся рядом с прежними календарными/lifecycle исключениями; occurrence API имел два входа; final story intelligence видел старое значение validity; Food отсутствовал в reserved minimum глобального writer budget.
- Решение: технически дублируется только совпадение `event owner + venue + occurrence date`, а другая дата/площадка остаётся отдельным canonical A-tier; все внешние repeat-решения проходят через `visible_repeat_verdict`, который внутри координирует same-day, lifecycle, calendar, ordinary и единственный A-tier override; существующий `effective_occurrence_window` принимает и fact card, и candidate, отдельный wrapper удалён; protected пересчитывается после финальной валидации; Food получил reserved minimum=3. Контракт physical-event identity закреплён в `PRODUCT_CONTRACTS.md`.
- Почему так (отвергнутые альтернативы): новый A-tier cap запрещён продуктовым контрактом; отдельный occurrence helper и новый repeat engine не создавались; расширение общего issue maximum отвергнуто — Food защищён точечной резервацией, а A-tier остаётся явным must-show исключением.
- Ожидаемый эффект и метрика проверки: разные реальные A-tier события одного артиста видимы, два источника одного события дают одну карточку; invalid homepage ticket не получает protected; recurring date одинакова для intake/routing/repeat/writer/protected; Food планирует ровно 3 даже при переполненных ранних разделах.
- Файлы/места: `plan_digest.py:_collapse_a_tier_event_runs`, `ticket_notability.py:a_tier_ticket_policy`, `repeat_policy.py:visible_repeat_verdict`, `dedupe.py:dedupe_candidates`, `candidate_validator.py:_exclude_cross_day_rehash/validate_candidates`, `weekend_inventory.py:effective_occurrence_window`, `story_intelligence.py:protected_lane`, `writer.py:PUBLIC_SECTION_RESERVED_MIN`, `docs/PRODUCT_CONTRACTS.md`; точечные regression tests.
- ПРОВЕРКА (offline): отдельные tests подтверждают `3` разных physical events одного артиста и collapse `2→1` только при одинаковых owner/venue/date; invalid homepage A-tier имеет `validated=false` и `protected=false`; recurring Saturday даёт одну дату `25.07` и одинаковое окно для candidate/fact; noisy issue сохраняет Food `3/3`. На replay 09.07 final rows выросли `82→117`, 19.07 `70→96`, 20.07 `52→79`, 21.07 `70→96`: возвращены distinct A-tier dates/venues, при этом lead `ok`, max blank run `1`, blank runs 2+ `0`, boilerplate endings `0`.
- Удалено: candidate-only occurrence wrapper; локальные calendar regex/constants и самостоятельные lifecycle/calendar решения в dedupe/validator; artist-level A-tier collapse.
- Где была ошибка (если не сработает): —

### 0136 — Quality repair больше не отменяет весь выпуск — 2026-07-22
- Статус: ПРОВЕРЕНО-работает.
- Проблема: daily run `29898660301` собрал выпуск 22.07 (`86` видимых карточек), но не отправил его: финальная сверка превратила `4` unresolved quality operations в техническую ошибку. Среди четырёх сигналов два были заведомо ложными: строка уже содержала «найден мертвым», а карточке парковочного сервиса ошибочно приписали `grooming` из навигации исходной страницы; третья претензия требовала заменить уже правильную дату `23 июля` на ту же дату.
- Причина (корень): #0132 ошибочно разрешил `verify_digest_plan` блокировать доставку из-за результата контентного ремонта; reverse completeness не распознавал русское `мёртв/мертв` и читал навигационный `evidence_text` вместо основного утверждения статьи; repair принимал модельную правку без defect-specific post-check, не перебирал запасных и мог оставить известную ошибку как unresolved.
- Решение: качество снова только управляет одной строкой: enrich/refetch → fact-locked model patch → deterministic rewrite → до четырёх запасных того же plan-slot с собственной проверкой фактов → кодированное снятие `fact_lock_failed`. Каждая правка повторно проверяется именно на исходный дефект; уже присутствующий обязательный факт, правильная structured date или предложенная судьёй правка, буквально совпадающая с уже видимыми фактами, дают `verified_existing_fact`. Неисправленная quality-проблема становится `ship_degraded`, а не техническим запретом выпуска; блокировать доставку по-прежнему могут только отсутствие/чужой run, битый HTML или нарушение slot-plan. Эта запись заменяет blocking-часть решения #0132, но сохраняет его строгую slot-conformance проверку.
- Почему так (отвергнутые альтернативы): нельзя сохранять ложный global fail-closed — он нарушает правило «выпуск выходит всегда»; нельзя молча принимать любую модельную правку; нельзя брать произвольную карточку вне цепочки слота. Снятие одной строки после исчерпания ремонта и запасных сохраняет и фактическую безопасность, и выпуск.
- Ожидаемый эффект и метрика проверки: quality verdict/model outage всегда оставляет `can_send=true`; `blocking_unresolved` отражается только как divergence; после неудачной правки конкретный слот имеет `replaced` или `removed` с кодом, `verify_digest_plan.ok=true`; workflow доходит до Telegram send.
- Файлы/места: `fact_completeness.py:_SEVERITY_CONCEPTS`, `pre_send_quality_judge.py:_completeness_source_blob/_repair_request_already_satisfied/_apply_repair_executor/quality_gate_error_for_digest`, `verify_digest_plan.py:run_verify_digest_plan`; regression tests в `test_fact_completeness.py`, `test_pre_send_repair_executor.py`, `test_pre_send_quality_judge.py`, `test_plan_contract.py`.
- ПРОВЕРКА (offline): targeted `51/51`, полный suite `891/891`, `git diff --check` — OK. На финальном HTML 22.07 reverse scan: `85` matched, ложные death/grooming omissions исчезли, осталась `1` реальная death omission у lead. На изолированной копии state 22.07: `2` operations, enrich `2`; дефектная model patch отвергнута post-check, lead без запасного снят как `fact_lock_failed`, правильная дата сохранена как `verified_existing_fact`; `unresolved=0`, `blocking_unresolved=0`. Replay 22.07: stages OK, `86 shown / 7 removed`, verify `ship_degraded`, lead `ok`, blank runs 2+ `0`, boilerplate `0`. Точный production no-op с Терренсом Кингом после дополнительного исправления: строка сохранена как `verified_existing_fact`, `unresolved=0`, `blocking_unresolved=0`.
- ПРОВЕРКА (production): rerun `29904111767` на `dd5e2ff` завершил все стадии за `21m58s`; pre-send `can_send=true`, verify `ok_technical=true` и `technical_errors=[]`, Telegram send успешен. `delivery_state`: `status=delivered`, `last_delivery_day_london=2026-07-22`, `09:49:51 Europe/London`, `9` частей, message ids `826–834`. Финальный выпуск: `86` карточек; исполнение плана `95 slots = 87 shown + 0 replaced + 8 removed`, `87 final HTML rows = 87 report rows`, `lines_outside_plan=0`. Repair реально обработал `16` запросов: enrich `16`, model patch `8`, снято `5`, уже правильных фактов распознано `3`; все пять снятий получили `fact_lock_failed`. Одна самопротиворечивая no-op правка даты была честно выявлена при пост-аудите и закрыта вышеописанным exact-state regression.
- Где была ошибка (если не сработает): —

### 0137 — Food использует подтверждённый ночной факт при утреннем 304 — 2026-07-22
- Статус: внедрено; проверено offline на реальном state 22.07, production-proof ожидается на следующем штатном night/morning run.
- Проблема: в утреннем run `20260722T092902+0100` источник About Manchester Food & Drink был здоров и ответил `304 Not Modified`, но все его пригодные ночные карточки получили `morning_live_candidate_not_found`. В результате inventory отчитался о достаточном Food-запасе `6 карточек / 2 источника`, реальный intake вставил `0`, план выбрал вместо открытия криминальную историю The Thirsty Korean, а финальный Food был `0/3`.
- Причина (корень): `build_morning_inventory_intake` признавал актуальность только при совпадении отдельной live-карточки по fingerprint/URL и не учитывал уже существующий здоровый ответ источника `304`; полнота считалась до этого подтверждения. `_exclude_wrong_food_opening_category` принимал любой текст со словом `restaurant`, поэтому кража проходила как Food. После восстановления Gaucho прежний timing-фильтр дополнительно считал 15-дневную статью stale, хотя карточка была заново наблюдена ночью, источник подтверждён утром и впереди оставалась августовская фаза reopening.
- Решение: единый реестр разрешает `304`-подтверждение только для Food; только карточка с current-day `last_seen_at`, operational provenance и точным совпадением `source_report_category + source_name` возвращается в обычный intake. Дальше работают прежние dedupe, validator и planner. Отчёт разделяет весь ночной запас и запас после утреннего подтверждения. Food-валидатор отклоняет происшествия без текущего открытия/reopening/рынка/menu/concept change; текущая ночная карточка подтверждённого `304`-источника не снимается только из-за возраста статьи. Source replacement не включён.
- Почему так (отвергнутые альтернативы): новый Food fallback, cross-block backfill и прямой рендер из inventory не создавались. Безусловно доверять всем старым карточкам источника нельзя: разрешены только наблюдённые сегодня карточки Food, а повторы и просрочка по-прежнему снимаются действующими механизмами.
- Ожидаемый эффект и метрика проверки: при здоровом утреннем `304` current-wave Food-карточки имеют lineage `inserted_into_pipeline/morning_source_not_modified`; `actual_intake.completeness` считается после подтверждения и не выдаёт ложный `block_sufficient`; published repeats не проходят dedupe; криминальные сюжеты не занимают Food; `release_plan.sections[Food]` показывает реальное число и честный shortfall.
- Файлы/места: `inventory.py:INVENTORY_BLOCK_REGISTRY/food_opening_has_product_meaning/build_morning_inventory_intake/inventory_block_completeness`, `collector/core.py:collect_digest`, `candidate_validator.py:_exclude_wrong_food_opening_category/_exclude_bad_food_opening_timing`, `docs/PRODUCT_CONTRACTS.md`; точечные regression tests.
- ПРОВЕРКА (real-state offline): полный state 22.07 `4204 records`: до правки `28 merged + 0 inserted + 6 held`; после `28 merged + 4 inserted + 2 held`. Ночной Food supply остаётся `6 / 2, sufficient=true`, подтверждённый утренний слой честно стал `4 / 1, sufficient=false`. На четырёх реальных About Manchester карточках существующий dedupe снял Rudy's, Fosforo Lounge и Lucky Gyros как `food_repeat_without_comparable_new_fact`; Gaucho дал `new`, прошёл validator и стал единственным Food-слотом плана `1/3` с `pool_exhausted_after_upstream_gates`. Реальная The Thirsty Korean burglary: до правки Food predicate `False` (не исключена), после `True / wrong_openings_category`. Targeted tests: inventory `63/63`, Food contracts `8/8`. Replay 22.07 до/после идентичен: `94 slots`, `92 shown`, `3 removed`, `section_count=12`, `lead=ok`, blank runs 2+ `0`, boilerplate `0`.
- Удалено: ничего; безусловное удержание standalone night-карточек остаётся необходимым для всех неподтверждённых источников и остальных блоков.
- Где была ошибка (если не сработает): проверить exact source-name/category match и current-day `last_seen_at` в `build_morning_inventory_intake`, затем судьбу Food lineage в dedupe/validator/plan.

### 0138 — Полная проверка фактов после ремонта и ранняя очистка неверных секций — 2026-07-22
- Статус: внедрено; targeted tests, exact-state probes и replay 22.07 пройдены, production-proof ожидается на следующем штатном выпуске.
- Проблема: production 22.07 доставил выпуск, но post-audit нашёл `1` реальную критическую потерю факта: модель исправила место процедуры в строке BBC и одновременно убрала факт смерти Элис Уэбб. Кроме того, до планёрки дошли два полностью non-GM transport alert (Mansfield–Worksop и Thirsk), пожар в Irlam как IT/business, три дальних Ticketmaster-события Crawley/Whitby/York как GM future announcements и кража в ресторане как Food. Поздний судья снял эти строки, но запасные цепочки были пусты.
- Причина (корень): post-check model/deterministic repair перепроверял только дефект, который попросили исправить; полная `candidate-own completeness` применялась только к запасному. Transport доверял категории TfGM без географии фактов. `resolve_venue_scope` не читал структурированный город из Ticketmaster summary. Слово `platform` в описании пожарной автолестницы давало ложный IT-сигнал. Food incident видел историческое `opened last year` в теле и ошибочно считал его текущим открытием — это уточняет неполный incident-предикат #0137.
- Решение: после model patch, deterministic rewrite и slot replacement выполняется полная reverse fact-completeness исходной карточки; на точных финальных байтах отчёт пересчитывается ещё раз, а найденная критическая потеря честно маркируется degraded, не блокируя выпуск. До планёрки transport без GM-якоря в публичных фактах снимается; incident-only строки выводятся из IT/business; Ticketmaster city из формата `venue | city | genre` становится авторитетной географией; Food incident проходит только при текущем opening/reopening/change в самом заголовке.
- Почему так (отвергнутые альтернативы): новый глобальный release block запрещён правилом «выпуск выходит всегда»; списки из трёх городов и двух маршрутов не использованы — правила опираются на структурированный city и наличие GM-якоря. Позднее снятие судьёй оставлено последней страховкой, но не основной маршрутизацией. Историческое слово `opened` в теле больше не считается текущей фазой продукта.
- Ожидаемый эффект и метрика проверки: `repair_executor.final_fact_completeness.critical_omission_count=0` после ремонта; BBC patch, теряющий death, отвергается до записи; указанные transport/section/Food карточки имеют reject/reroute до `plan-digest`; Ticketmaster Crawley/Whitby/York получают `venue_scope=outside`; delivery остаётся `can_send=true` при любой строковой неисправности.
- Файлы/места: `pre_send_quality_judge.py:_apply_repair_executor/_finalize_repair_report`, `candidate_validator.py:resolve_venue_scope/_apply_section_routing_quality/_exclude_non_gm_transport/validate_candidates`, `inventory.py:food_opening_has_product_meaning`; regression tests `test_pre_send_repair_executor.py`, `test_quality_layers.py`.
- ПРОВЕРКА (offline): targeted `38/38` OK. Exact-state на production candidates 22.07: Mansfield/Thirsk → `transport_non_gm`; Irlam → `city_watch` с `incident_is_not_it_content`; The Thirsty Korean → `wrong_openings_category`; Crawley/Whitby/York → `outside`; BBC replacement без смерти → `replacement drops death`. Replay 22.07: все 6 стадий OK; `94 slots`, `92 shown`, `3 removed`, lead `ok`, max blank run `1`, blank runs 2+ `0`, boilerplate endings `0`, verify `ship_degraded` без технической ошибки. Повторная Telegram-отправка не выполнялась, чтобы не дублировать уже доставленный выпуск.
- Где была ошибка (если не сработает): сначала проверить `final_fact_completeness` и method конкретной repair operation; затем до планёрки — `reject_reasons`, `section_routing_quality` и `venue_scope` исходной карточки.

### 0139 — Hybrid-карточки сохраняют уже рассчитанные факты — 2026-07-22
- Статус: внедрено; offline проверено на всех текущих карточках пяти ночных hybrid-блоков, production-proof ожидается на следующей штатной ночной волне.
- Проблема: ночь собирала Fresh, City, Transport, IT/business и Football, но каждая карточка оставалась `missing_facts`: утро видело ссылку и заголовок, однако не получало структурированные `what_happened` и `why_now`.
- Причина (корень): утренний validator применял существующий `editorial_contract`, а `collect-inventory` ограничивался entity/event/change enrichment и никогда не переносил story frame в ночную карточку. Реестр уже требовал эти поля, поэтому собственный контракт ночного слоя отклонял весь hybrid-запас.
- Решение: ночное per-card enrichment для блоков с режимом `hybrid` использует тот же существующий `attach_editorial_contract`, что и утро, и сохраняет из него `what_happened`, `why_now` и `story_type` до построения inventory record. Новых моделей, фильтров, fallback, recovery и этапов нет.
- Почему так: повторно извлекать факты отдельными регулярками означало бы создать вторую редакционную логику. Единый действующий story frame сохраняет одинаковое понимание новости ночью и утром; live-подтверждение hybrid-блоков и запрет source replacement не менялись.
- Ожидаемый эффект и метрика проверки: текущая ночная карточка каждого hybrid-блока имеет полные факты и статус `needs_text`, а не `missing_facts`; утром она может участвовать как фактический сигнал, но не публикуется без действующего live-пути.
- Файлы/места: `inventory.py:enrich_hybrid_inventory_facts`, `run_local_digest.py:cmd_collect_inventory`, `tests/test_inventory.py:BuildRecordTest`.
- ПРОВЕРКА (real-state offline): run-specific inventory 22.07 до — `246/246 missing_facts`: Fresh `115`, City `68`, Football `32`, IT/business `21`, Transport `10`. После повторного построения тех же карточек новой функцией — `246/246 needs_text`, остаточных `missing_facts=0`. `tests.test_inventory`: `64/64` OK; `py_compile` и `git diff --check` OK. Production-proof — следующая штатная night wave; workflow вручную не запускался.
- Next 7 Days: отдельная трассировка опровергла первоначальный диагноз потери. В current runs блок получил `0` карточек, потому что non-leisure источники не дали ни одного подходящего события 23–29.07. Все `47` retained next7-записей — старый leisure из `culture_weekly/venues_tickets`, последний раз виденный 02–13.07; использовать его означало бы нарушить продуктовый контракт. Код и floor блока не менялись: дальнейшее решение — покрытие источниками, а не fallback чужим содержанием.
- Удалено: ничего; дублирующего ночного story-frame механизма не было, а утренний `editorial_contract` остаётся общей обязательной логикой.
- Где была ошибка (если не сработает): проверить вызов `enrich_hybrid_inventory_facts` до `build_inventory_record`, затем `fact_card.what_happened/why_now` конкретного current run_id.

### 0140 — Контентный дефект не может остановить выпуск ни на одном pre-send этапе — 2026-07-23
- Статус: внедрено; реальный state 23.07 и replay до/после пройдены, production recovery run ожидается после push.
- Проблема: daily run `29986898122` не отправил выпуск. Writer уже собрал `55` строк и кодифицированно снял `50` неисполнимых слотов, но `edit-digest` вернул exit `1` из-за отсутствующего блока «Свежие новости». Workflow с `set -e` пропустил build, pre-send judge, verify и Telegram.
- Причина (корень): #0136 убрал контентную блокировку из позднего judge/verify, но ревизия не прошла вверх по всей цепочке. В editor сохранился старый `errors.append` для обязательных разделов и city-пула; release также продолжал считать пустые блоки, тонкий выпуск, редакционную прозу, повторы, погоду и невидимый public-service техническими ошибками. Продуктовый контракт уже запрещал такое поведение.
- Решение: editor всегда завершает технически корректную обработку с `stage_status=complete`; отсутствующие разделы и нехватка city-контента остаются явными warnings. Release переводит все дефекты видимого содержания в warnings и `ship_degraded`; `fail` остаётся только для отсутствующего/чужого state, неверной даты, отсутствующего draft и последующей технической slot-plan/HTML сверки.
- Почему так: пустой Fresh нельзя превращать в хороший выпуск, но отмена всего выпуска делает результат для читателя хуже. Правильная реакция — честно отправить доступные `55` строк, зафиксировать недобор и чинить источник/отбор отдельно. Новые fallback, recovery, модели и этапы не добавлялись.
- Ожидаемый эффект и метрика проверки: любой editorial/content warning на editor/release оставляет команды с exit `0`; release_decision становится `ship_degraded`; workflow всегда достигает judge, verify и Telegram, пока run/date/state/HTML/slot-plan технически согласованы.
- Файлы/места: `editor.py:edit_digest`, `release.py:_validate_draft/build_release`, `.github/workflows/daily-digest.yml` (поведение `set -e` не менялось), `AGENTS.md`, `PRODUCT_CONTRACTS.md`; regression tests `WriterRenderedFingerprintTest` и `PublishedReviewTest`.
- ПРОВЕРКА (real-state replay 23.07): до — `plan 104`, writer `55 lines / 50 removed`, `edit-digest FAIL`, дальнейшие стадии не запускались. После — `edit ok`, `build ok`, `verify ok`, `55 shown / 0 replaced / 50 removed`, `14 divergences`, итог `ship_degraded`; missing Fresh больше не останавливает выпуск. Targeted affected tests `32/32` OK, `py_compile` и `git diff --check` OK.
- Удалено: старый editor content-blocking path (`stage_status=failed`, `StageResult.ok=false`, blocking message) и release-классификация редакционных дефектов как `errors`.
- Где была ошибка (если не сработает): сначала проверить exit code `edit-digest`, затем `release_report.errors` против `warnings`, после этого `verify_digest_plan_report.technical_errors`; контентная причина не должна появляться ни в одном blocking-поле.
