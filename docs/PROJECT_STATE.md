# NewsManchester Project State

Last reviewed: 2026-06-27.

This document describes how the system actually works now, not how it was
originally intended to work. Use it before changing pipeline code, prompts,
sources, or release rules.

Evidence used for this snapshot:

- `AGENTS.md`
- `docs/pre-send-checklist-v1.md`
- `docs/editorial-style-guide-v1.1.md`
- `docs/product-change-log-2026-05.md`
- current code under `src/news_digest/pipeline/`, `scripts/run_local_digest.py`,
  and `.github/workflows/daily-digest.yml`
- latest local reports in `data/state/`
- `data/outgoing/current_digest.html`
- GitHub Actions run `28281912111` from 2026-06-27
- git history from 2026-06-13 through 2026-06-27

## Current Pipeline

The user-facing conceptual pipeline is:

`collect -> normalize -> filter -> dedupe -> validate -> curate -> enrich -> writer -> editor -> recovery -> build HTML -> final validation -> send`

The actual pipeline currently run by GitHub Actions is:

1. `collect-digest`
   - Fetches source URLs from `data/sources.toml`.
   - Parses RSS, HTML, JSON APIs, sitemaps, event pages and synthetic sources.
   - Normalizes titles, summaries, dates, URLs, categories and primary blocks.
   - Applies source-specific filters, GM relevance, freshness and routing.
   - Writes `data/state/candidates.json` and `collector_report.json`.

2. `dedupe-digest`
   - Applies exact URL/fingerprint dedupe, topic dedupe, cross-day history and
     semantic dedupe.
   - Writes `dedupe_memory.json`.

3. `validate-candidates`
   - Applies deterministic quality checks, section routing checks, practical
     angle backfill, city intelligence and professional-event profile checks.
   - Professional CV-match exists, but the latest 2026-06-27 report says only
     `eligible=1`, `sent=1`, `applied=0`, `skipped=1`, so it is not yet acting
     as a decisive publish rule for the whole professional block.

4. `curator-pass`
   - LLM editorial pass for broad candidate review.
   - Drops PR/evergreen/non-GM items and marks the lead.
   - On 2026-06-27, curator succeeded but one batch timed out; partial results
     were applied and `lead_set=true`.

5. `transport-fill`
   - Adds deterministic transport status/reminders when needed.
   - On 2026-06-27 it completed but injected nothing.

6. `llm-rewrite`
   - Performs prewrite enrichment for thin selected cards.
   - Builds a source-language board, currently with DeepSeek primary board
     ranking plus OpenAI reserve/fallback routes.
   - Cuts to a final Russian writing board.
   - Writes Russian `draft_line` values through OpenAI direct Russian writer.
   - Runs hard-defect repair only; soft issues are left to writer/editor
     recovery.
   - Writes `llm_rewrite_report.json`, `publish_plan.json`,
     `section_selection_report.json`, `rewrite_inventory.json`.

7. `write-digest`
   - Applies the publish plan to candidates.
   - Assembles sections and writes `data/state/draft_digest.html`.
   - Enforces many line-level and block-level quality rules.
   - Applies fallback/recovery lines for transport, weekend, professional,
     events and hard news.
   - Important current gap: the writer can report missing `must_show` items
     without a same-block replacement and still complete. Completing is by design
     (delivery is never blocked); the gap is the missing replacement, not the
     completion.

8. `edit-digest`
   - Runs line dedupe, Russian cleanup and whole-digest editor/recovery logic.
   - Writes `editor_report.json` and `final_editor_report.json`.
   - Important current behavior: `partial_failed` and `failed` final editor
     states are warning-only by design (never-block rule) and do not stop
     `build-digest` or `send-file`. The quality gap is when no recovery/rebalance
     runs before that warning ships.

9. `build-digest`
   - Runs release validation.
   - If `release_report.release_decision == "pass"`, promotes
     `draft_digest.html` to `data/outgoing/current_digest.html`.
   - The release gate blocks on `errors`, not on many product warnings.

10. `pre-send-quality-judge`
    - Runs after build.
    - Writes `pre_send_quality_report.json` and stamps the release report.
    - It is intentionally non-blocking in the workflow and in current send
      policy. It may return `decision=warn`, `can_send=true`, and delivery still
      proceeds.

11. `send-file`
    - Sends `data/outgoing/current_digest.html` to Telegram.
    - Blocks only on technical consistency: current file, fresh matching
      release report, release gate pass, gate version, current date/header.
    - Does not block on pre-send content warnings.

12. `post-publish-judge`, warnings, cost and state commit
    - Post-publish quality judge scores the already-sent issue.
    - Internal warnings are sent.
    - State files are committed back to GitHub.

Run `28281912111` on 2026-06-27 confirms the actual path: workflow success,
send success, and state commit success, while product-health warnings remained.

Important 2026-06-27 facts:

- release decision: `pass`
- release health: `unhealthy`, risk score `5`
- visible HTML: 61 bullets in `current_digest.html`
- pre-send judge: `decision=warn`, `can_send=true`
- pre-send warnings: ticket dominance `35` ticket/concert items vs `21` core
  items; low writer yield `73/329`
- section underflow: `Что важно сегодня` 2 vs min 3, `Городской радар` 3 vs
  min 5, `Футбол` 1 vs min 2
- writer contract: 17 `must_show`, 14 rendered, 3 missing
- editor warnings included `partial_failed` and `failed`
- source health: 119 checked, 109 ok, 3 empty parsers, 0 failed fetches, 84
  zero-yield in release source-status

## Blocks

### Weather

Should work:

- Reflect actual reader impact today: heat, cold, rain, wind, warning, comfort
  and hourly relevance.
- A neutral summary is not enough if the day has obvious heat/comfort risk.

Works now:

- Generated as a synthetic weather candidate.
- Required by `common.REQUIRED_BLOCKS`.
- Release has synthetic freshness handling and weather placeholder downgrade
  when live weather sources fail.

Main sources:

- Met Office HTML parser.
- Open-Meteo fallback / synthetic weather logic.

Weak spots and known failure modes:

- Can read as a neutral forecast even when heat/comfort risk should be called
  out.
- Numeric weather presence can pass while the practical impact is weak.
- If weather sources fail, the placeholder can still ship.

### Fresh News

Should work:

- Prioritize important local news: public safety, council, crime, housing,
  transport, health, education, major civic and service changes.
- Strong rejected stories need a human-readable reason.
- Selected stories must either appear in final HTML or be replaced with reason.

Works now:

- Maps mainly to `last_24h` -> `Свежие новости`.
- Required by `common.REQUIRED_BLOCKS`.
- Selected by collector/dedupe/validator/curator/LLM board and capped by writer.
- Underflow emits warnings, not a hard release failure.

Main sources:

- BBC Manchester, BBC Manchester Web, MEN, MEN Latest, MEN News Sitemap,
  ITV Granada, Place North West, About Manchester, The Mill, Altrincham Today,
  GMCA and council feeds/pages.

Weak spots and known failure modes:

- Strong stories can be selected by the board and still not reach HTML.
- On 2026-06-27, only 3 Fresh items rendered while the release minimum is 6.
- Low writer yield can hide upstream collection success.
- Final loss reports can contain hundreds of possible misses, but they are not
  currently a blocking source of truth.

### Today

Should work:

- `Что важно сегодня` means practical impact today: closures, service changes,
  events affecting movement, deadlines, weather impacts, or urgent public
  service/safety information.
- It is not a place for random civic items.

Works now:

- Maps to `today_focus`.
- Required by `common.REQUIRED_BLOCKS`.
- Writer uses deterministic board logic and practical-action checks.
- Underflow warns but can ship.

Main sources:

- Media layer, public services, transport, weather, council, event and
  deadline-driven candidates.

Weak spots and known failure modes:

- The block can underflow despite a successful run.
- A practical but less glamorous public-service item can lose to soft/ticket
  volume before final HTML.
- On 2026-06-27 the writer counted 2 items, while the common HTML extractor saw
  only 1 bullet after final output structure.

### Transport

Should work:

- Include route/line/stop/section, time window, passenger impact and
  action/advice.
- No passenger impact means not Transport.
- Long-term infrastructure without today/tomorrow travel impact belongs in City
  Radar.
- Generic TfGM fallback is forbidden when a concrete disruption exists.

Works now:

- Maps to `transport` -> `Общественный транспорт сегодня`.
- Transport source collection uses TfGM and National Rail.
- `transport-fill` can inject deterministic reminders/status.
- Writer has transport fallback and transport language repair.

Main sources:

- TfGM travel alerts.
- National Rail Enquiries incidents API.
- Media-layer transport stories can be rerouted to transport.

Weak spots and known failure modes:

- Non-movement infrastructure can still appear in the transport block.
- Generic TfGM fallback can be inserted as degradation.
- Media incidents near bus stations can be mistaken for transport.
- On 2026-06-27, the final HTML included Bury Interchange infrastructure,
  generic TfGM fallback, bus-stop closures, and an emergency-services story near
  a bus station in Transport.

### City Radar

Should work:

- Carry important civic, planning, borough, local service and city-change items
  that are useful but not urgent today.
- Should not steal urgent transport, safety or Fresh items.

Works now:

- Maps to `city_watch` -> `Городской радар`.
- Low-signal block in `common.LOW_SIGNAL_BLOCKS`, but has a soft minimum in
  `SECTION_MIN_ITEMS`.

Main sources:

- Councils, media layer, Place North West, BusinessLive/MIDAS where civic or
  built-environment impact is clear.

Weak spots and known failure modes:

- Can underflow and still ship.
- Can contain future/analysis items that should not be presented as urgent.
- Can inherit classification noise from business/transport topics.

### Weekend

Should work:

- Actual upcoming weekend activity: markets, fairs, food, community, family,
  festivals, free/low-cost Greater Manchester events.
- Single concerts go to Tickets.
- 2027 events do not go to Weekend.
- Weak "check details" items should be rejected or enriched.

Works now:

- Maps to `weekend_activities` -> `Выходные в GM`.
- Writer only shows the block from Thursday onward.
- Writer has weekend occurrence/date logic and fallback lines.
- Markets and car boot sources were expanded in June.

Main sources:

- Visit Manchester this-week/weekend pages, Manchester's Finest events, makers
  markets, car boot pages, theatre/weekend guides, Skiddle and venue/event
  sources.

Weak spots and known failure modes:

- Selection remains unstable.
- Weak or far-future events can enter the block.
- On 2026-06-27, a 21 May 2027 Manchester Jazz Festival item appeared in
  Weekend, which violates the product contract.
- Single concerts and low-detail listings can crowd out community/weekend
  planning value.

### Next 7 Days

Should work:

- Weekly look-ahead: shows, exhibitions, events, confirmed restrictions,
  business/tech events and practical planning items in the next week.
- Each item needs date, place and why it matters.

Works now:

- Maps to `next_7_days` -> `Что важно в ближайшие 7 дней`.
- Uses structured event extraction where available.
- Writer has event completeness checks and repair.

Main sources:

- HOME, Whitworth, Factory International, The Lowry, Palace Theatre, Contact,
  People's History Museum, John Rylands, Visit Manchester and event guides.

Weak spots and known failure modes:

- Event pages can produce date/venue gaps.
- Generic venue copy can enter when parser extraction is weak.
- Repeats can pass if the "new phase" label is trusted without concrete change.

### Food/Openings

Should work:

- Exact place, area/station if possible, opening status/date, and why it
  matters.
- Repeats require a new fact.

Works now:

- Maps to `openings` -> `Еда, открытия и рынки`.
- Low-signal conditional block; can be hidden or small.
- Writer has openings/event fallback and repeat handling.

Main sources:

- Manchester's Finest, About Manchester Food & Drink, The Manc Eats,
  SK Lowdown Markets, plus selected culture/weekend market sources.

Weak spots and known failure modes:

- Can ship with missing location or vague "check details" endings.
- Food/market items can drift between Weekend, Next 7 Days and Openings.
- Repeats require stronger new-fact enforcement.

### Business/IT

Should work:

- Explain who, what changed, where, why it matters, and money/jobs/product or
  service impact where available.
- Generic "check details" copy is not acceptable.

Works now:

- Maps to `tech_business` -> `IT и бизнес`.
- Uses business/tech sources and reader-value scoring.

Main sources:

- Manchester Digital, Prolific North Manchester, BusinessCloud, Bdaily
  Manchester, MIDAS Manchester, BusinessLive Greater Manchester.

Weak spots and known failure modes:

- Personnel PR, campaigns, anniversaries and generic business support pages can
  leak through if the concrete action rule is too weak.
- Business items can be misclassified as professional events or city radar.

### Professional/Conferences

Should work:

- Include date, place/online, free/paid/booking, relevance to the user's profile
  and CV-match verdict: `go`, `consider`, or `skip`.
- `skip` cannot be `must_show` or visible.
- CV-match must happen after fact extraction and before publish selection.

Works now:

- Maps to `professional_events` -> `Бесплатные business/tech события для тебя`.
- Deterministic scoring exists in `professional_events.py`.
- LLM CV-match exists, but latest report shows it did not apply a decisive
  result on 2026-06-27.
- Writer can generate fallback professional cards.

Main sources:

- Manchester Digital Events, GM Business Growth Hub, University of Manchester,
  pro-manchester, GM Chamber, CompiledMCR Tech Events.

Weak spots and known failure modes:

- Professional pages without concrete event facts can become `must_show`.
- On 2026-06-27, three professional `must_show` items were missing because of
  untranslated English.
- CV-match is not yet the final decisive filter for all candidates.

### Ticket Radar

Should work:

- Include venue scope, event date, sale/ticket status if available, tier with
  evidence, why now, and horizon category.
- Outside venue cannot use GM wording.
- A-tier must be evidence-based.
- Important concerts should be discovered early, not one day before.

Works now:

- Maps to `ticket_radar` -> `Билеты / Ticket Radar`.
- Outside concerts map to `outside_gm_tickets` -> `Крупные концерты вне GM`.
- Ticket types are classified in `editorial_contracts.py`.
- Ticket notability and structured ticket lines exist.

Main sources:

- Co-op Live, Ticketmaster Manchester/Liverpool/London/UK on-sale and upcoming
  APIs, major venue/event pages, Bridgewater Hall, Albert Hall, Band on the
  Wall, RNCM and related sources.

Weak spots and known failure modes:

- Geography and horizon remain unstable.
- Outside-GM and ticket volume can dominate core news.
- On 2026-06-27, pre-send judge warned about 35 ticket/concert items vs 21 core
  items; send still proceeded.
- Outside venues can still receive Greater Manchester wording.
- Ticket parser/selection can produce too many future and outside-GM items.

### Outside-GM

Should work:

- Include only genuinely important outside-GM concerts/events with explicit
  outside venue wording.
- Must be capped before editor so it cannot dominate core news.

Works now:

- Maps to `outside_gm_tickets` -> `Крупные концерты вне GM`.
- Low-signal block but latest selection report selected 143 outside-GM items
  before writer caps and public selection.

Main sources:

- Ticketmaster UK/London/Liverpool major on-sale/upcoming feeds and major venue
  pages.

Weak spots and known failure modes:

- Can dominate public output.
- Can create false "nearby" or GM wording for outside venues.
- Needs stronger cap and geography resolution before writing text.

### Russian Events

Should work:

- Require positive evidence: Russian/Ukrainian language, diaspora promoter,
  Russian-language page, performer/audience evidence or explicit
  cultural/community relevance.
- Afisha London as a source is not enough by itself.

Works now:

- Maps to `russian_events` -> `Русскоязычные концерты и стендап UK`.
- Protected in publish plan (`must_show` can apply).
- Diaspora diagnostics exist in release.

Main sources:

- EventFirst Diaspora, UK Stand-Up Club, Eventbrite, Kontramarka UK, Afisha
  London, MTicket Russian Concerts.

Weak spots and known failure modes:

- Source label can be mistaken for positive Russian evidence.
- London-heavy sources can weaken UK/GM relevance.
- Needs classifier evidence stored before publish selection.

### Football

Should work:

- Prioritize official club sources and reliable BBC/Guardian/Sky reporting.
- MEN transfer/opinion only if fact is confirmed.
- Avoid opinion/rumour as the main football item.

Works now:

- Maps to `football` -> `Футбол`.
- Uses official club feeds plus BBC/MEN/Guardian football sources.
- Underflow warns but can ship.

Main sources:

- Manchester United, Manchester City, Manchester City Men, BBC Sport team
  feeds/pages, MEN United/City, Guardian United/City.

Weak spots and known failure modes:

- Official pages can be PR/fluff.
- MEN live transfer/opinion can enter if confirmation checks are weak.
- On 2026-06-27, Football rendered 1 item vs minimum 2.

### Other Blocks

Current code also uses:

- `Главная история дня` from `lead_story`
- `Дальние анонсы` from `future_announcements`
- `Радар по районам` from `district_radar`

Known state:

- `Главная история дня` is selected by curator or deterministic fallback, but
  lead visibility must be checked in final HTML, not only in counters.
- `Дальние анонсы` can absorb far-future event noise.
- `Радар по районам` is low-signal and should not repeat facts already shown
  above.

## Known Weak Points

- Selected / `must_show` items can disappear before final HTML.
- Internal counters are not enough; writer counts, release counts and final HTML
  extraction can disagree.
- Final HTML is not always treated as the source of truth.
- Editor `failed` / `partial_failed` can be recorded as warnings while release
  and send proceed.
- Recovery can insert weak reserve/fallback lines without a full
  enrich -> rewrite -> editor -> final HTML validation loop.
- Outside-GM and tickets can dominate core-news.
- Weekend selection is unstable.
- Ticket geography and horizon are unstable.
- Professional CV-match may be applied too late or to too few candidates.
- Russian classifier must require positive evidence.
- Repeat policy may trust false "new phase" without a concrete new fact.
- Source health does not always explain exactly where candidate loss happened:
  parser, candidate creation, filter, selection, writer, editor or HTML.
- Source fixed yesterday can still be empty today.

## Current Open Risks

These are product-quality risks. The remedy is recover/rebalance-before-send and
RCA, not blocking delivery — the issue still ships (never-block rule).

### P0

- A selected lead or `must_show` item can be absent from final HTML while the
  run still succeeds.
- Editor `failed` / `partial_failed` can ship as a warning.
- Critical balance warnings can ship without rebalance, especially ticket
  dominance over core news.
- Generic transport fallback can replace concrete disruption and still look like
  a valid transport block.
- Final HTML can violate block contract even when reports say the pipeline
  passed.

### P1

- Core sections can underflow and ship.
- Weekend can publish wrong-horizon or low-value events.
- Ticket Radar can publish outside-GM / future / low-tier items with misleading
  wording.
- Professional block can publish or protect items without decisive CV verdict.
- Russian Events can overtrust source identity instead of positive evidence.
- Repeat policy can allow stale repeats labelled as new phase.
- Source diagnostics can show "ok" while all candidates from that source are
  lost downstream.

## What Must Not Be Considered Pass

"Pass" here means a clean PRODUCT/QUALITY pass, not a delivery decision.
Delivery always proceeds once the issue is built and technically consistent
(never-block rule); the items below must instead trigger
recover/rebalance-before-send and an RCA for the next run. None of them may hold
or block the send.

- Editor `failed` or `partial_failed` is not a clean quality pass.
- Selected lead missing from final HTML is not a clean quality pass.
- `must_show` missing from final HTML is not a clean quality pass.
- Block underflow without an explicit human-readable reason is not a clean
  quality pass.
- Ticket dominance warning without rebalance is not a clean quality pass.
- Generic fallback replacing a concrete disruption is not a clean quality pass.
- Source parser "empty" without trace is not enough.
- Final HTML that contradicts writer/release counters is not a clean quality
  pass until the discrepancy is explained.
- A warning-only critical product issue is not a clean quality pass, even though
  delivery succeeded.
