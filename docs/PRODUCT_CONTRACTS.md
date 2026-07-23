# NewsManchester Product Contracts

Last reviewed: 2026-07-23.

These are product rules, not implementation preferences. If code, prompts,
reports, or old docs conflict with this file, treat the conflict as a bug or an
RCA item.

## Global Release Contract

- Delivery is never blocked. Once the issue is built and technically consistent,
  it always ships. Every quality contract below is a *product* bar that triggers
  recover/rebalance-before-send and RCA — never a held or blocked send (see
  "Never block the release" in DECISIONS_AND_LESSONS.md). "Pass" anywhere in
  these docs means a clean *quality* pass, not a delivery decision.
- Final HTML is the source of truth. A candidate only counts as shipped if it is
  visible in `data/outgoing/current_digest.html`.
- `publish_plan` selected, `must_show`, lead and protected items must be visible
  in final HTML or explicitly replaced with a human-readable reason.
- A failed or partially failed editor must trigger recovery/rebalance before
  send and is not a clean quality pass. It must never block or hold delivery —
  the issue still ships.
- No pre-send pipeline command, from collection through final verification, may
  return non-zero solely because a source/category is incomplete, a candidate
  is malformed, a model produced no prose, or a content block is empty, thin,
  imbalanced or contains an unrepaired editorial defect. The affected row is
  held or removed with a reason and the issue is reported as `ship_degraded`;
  only technical state/date/HTML/slot-plan inconsistency can stop delivery.
- A pre-send warning for critical balance issues must trigger action before
  send: rebalance, recovery or replacement. It must not only report — and it
  must not hold delivery either.
- Every visible item must have source, date relevance, section fit and a reason
  to read.
- Internal counters are advisory. Final validation must compare counters with
  rendered HTML.
- Recovery must preserve useful facts first: enrich, rewrite, replace inside the
  same block, then omit only when facts cannot be recovered.

## Section Routing Contract

Every block belongs to exactly one **purpose class**. A block only accepts items
of its own class; content never leaks across classes, even when a block is
hidden or thin. This is the single source of truth for "what belongs where" —
per-block contracts below refine it, they do not override it.

Purpose classes and their blocks:

- **News** — what happened, matters now: `lead_story` (Главная история дня),
  `last_24h` (Свежие новости), `today_focus` (Что важно сегодня),
  `city_watch` (Городской радар).
- **Service** — what is disrupted / act today: `transport`
  (Общественный транспорт сегодня), `weather` (Погода).
- **Leisure / what's-on** — plan your free time: `weekend_activities`
  (Выходные в GM), `future_announcements` (Дальние анонсы).
- **Tickets** — buy / plan a ticketed show: `ticket_radar` (Билеты),
  `outside_gm_tickets` (Крупные концерты вне GM).
- **Planning (dated, non-leisure)** — `next_7_days` (Что важно в ближайшие
  7 дней): confirmed restrictions, deadlines, last-chance civic/service items
  in the coming week. NOT a what's-on calendar.
- **Culture-diaspora** — `russian_events` (Русскоязычные концерты и стендап UK).
- **Local commerce** — `openings` (Еда, открытия и рынки).
- **Business / career (personal)** — `tech_business` (IT и бизнес),
  `professional_events` (business/tech события для тебя).
- **Sport** — `football` (Футбол).

Rules:

- A News-class block never receives a leisure/what's-on item, and a
  Leisure/Tickets block never receives a hard-news item. A car boot, market or
  concert is Leisure/Tickets — it must never appear in `next_7_days`,
  `today_focus`, `last_24h` or `city_watch`.
- `next_7_days` is Planning, not Leisure. A dated market/fair/festival/concert
  is Leisure/Tickets and does not qualify for `next_7_days` on the ground of
  "it has a date this week".
- Each item has one primary purpose class; if two could apply, News > Service >
  Planning > Tickets > Leisure decides.
- `district_radar` is retired: it remains a legacy identifier for old state but
  has no source, selection target or public section.

Hidden-block spillover map (a conditionally-shown block must declare where its
items go when it is not rendered — content is held or re-homed **within its own
purpose class**, never promoted into a News/Planning block):

- `weekend_activities` is shown Thu–Sun only (`writer.py` `show_weekend =
  weekday >= 3`). When hidden (Mon–Wed): a weekend leisure item is **held in
  reserve** for the weekend; if it is a single ticketed show it re-homes to
  `ticket_radar`; it must NOT flow into `next_7_days`.
- `outside_gm_tickets` non-A-tier overflow is held to backup reserve, not
  promoted into GM ticket or event blocks.
- Any block below its floor recovers from its own class first (see Recovery
  Contract); it never borrows a leisure item to fill a news block.

Failure examples:

- Bolton car boot / Manchester Open shown in "Что важно в ближайшие 7 дней"
  (Leisure leaking into Planning because Weekend was hidden midweek).
- A tribute concert with no ticket data shown in `next_7_days` instead of
  Tickets.
- A council roundup pulled into Weekend to fill an empty leisure block.

## Lead Contract

- The public issue has exactly one lead story.
- Supporting sentences may elaborate that same story; a second unrelated story
  must remain in its own News block.
- The lead must contain a current verified development and explain why it leads
  the Greater Manchester issue today.
- A lead removed by factual or repeat review is reported as missing; release
  does not resurrect its old text after quarantine.

Failure examples:

- Two unrelated bold stories under the lead heading.
- A story visible in City or Fresh is inserted again as a second lead.

## City Contract

- City contains consequential GM civic, public-service, planning, safety and
  local-economy developments that do not belong in Fresh or Today.
- Football, leisure listings, ticket listings and transport service cards do
  not move into City merely because their own block is thin or capped.
- A story already used as lead cannot appear again in City from another source.

Failure examples:

- A Manchester City pre-season article used to fill City.
- The same escaped-prisoner story shown in Lead and City under different links.

## Future Announcements Contract

- Future contains a genuine advance announcement with a dated occurrence more
  than seven days away and a current why-now trigger such as a new announcement
  or newly opened sale.
- A show, film, exhibition or market happening today or this week belongs in
  Weekend/Tickets when eligible; otherwise it is held, not used as Future fill.
- Routine old on-sale listings without a new phase are not Future announcements.
- A workshop needs a real future occurrence. A recurring series contributes one
  nearest concrete occurrence, and a current/near occurrence cannot be moved to
  Future merely because the source also lists later dates.

Failure examples:

- A HOME film starting today shown under `Дальние анонсы`.
- A year-old on-sale ticket shown only because its event date is months away.

## Weather Contract

Weather must reflect actual reader impact:

- heat, cold, rain, wind and warnings;
- hourly or day-part relevance where available;
- not just a neutral summary;
- if the morning is already hot and the max is high, text must say heat,
  comfort, hydration or travel risk.

Required fields:

- temperature range;
- rain/wind/warning signal or explicit calm state;
- one concrete reader action where conditions matter.

Failure examples:

- "20-28C, calm" when the practical issue is heat stress.
- Weather placeholder that hides source failure without saying so in reports.

## Fresh News Contract

Fresh News must prioritize important local news:

- high-impact public safety;
- council and civic decisions;
- crime/courts;
- housing and planning;
- transport;
- health and public services;
- education;
- major local economic or community impact.

Contract:

- visible minimum must be recovered/rebalanced before send (never enforced by
  holding delivery);
- rejected strong stories require reason;
- if selected but not visible, recovery must replace from the same block or
  explain why no replacement exists;
- no soft/lifestyle/ticket item should displace a strong hard-news item.

Failure examples:

- Fresh ships below minimum while tickets/outside-GM dominate.
- A selected public-safety story is present in reports but absent from HTML.

## Today Contract

Today means practical impact today:

- closures;
- service changes;
- events affecting movement;
- deadlines;
- weather impacts;
- active safety/public-service issues.

Not Today:

- random civic explainer;
- weak political analysis;
- future item with no action today;
- soft event that belongs in Weekend or Next 7 Days.

Contract:

- every item must answer: what is happening today, where, who is affected, and
  what should the reader do or remember.
- Today accepts only candidates routed upstream for an action, event,
  restriction or deadline that applies today. Fresh and City never donate rows
  to fill Today; a short native pool is reported as an honest underflow.

## Transport Contract

Transport item must include:

- line, route, stop or section;
- date/time window;
- passenger impact;
- action/advice.

Rules:

- No passenger impact = not Transport.
- Long-term infrastructure without today/tomorrow travel impact goes to City
  Radar.
- Generic TfGM fallback is forbidden if concrete disruption exists.
- A nearby incident is not transport unless it changes travel.
- A stop/road/work item must explain what passenger or driver should do.
- A bounded authoritative TfGM tram restriction is persisted independently of
  whether the article is selected as new news. It needs real movement impact
  and a concrete end date; one ordinary plan candidate is regenerated each day
  until expiry, before Stage 3 composition is locked.

Failure examples:

- "Bury Interchange received funding" in Transport.
- "services were at a bus station" treated as transport disruption.
- "check TfGM" replacing known bus or rail disruption.

## Weekend Contract

Weekend item must be actual upcoming weekend activity:

- markets;
- fairs;
- food;
- community;
- family;
- festivals;
- free/low-cost;
- Greater Manchester relevance.

Rules:

- Single concerts go to Tickets.
- 2027 events do not go to Weekend.
- Weak "check details" items should be rejected or enriched.
- The item must have date, place and activity type.
- A market/fair/community event should beat a generic concert if Weekend is
  thin.

Failure examples:

- Far-future festival in Weekend.
- Single arena concert in Weekend.
- Vague guide page with no exact date or place.

## Weekend Inventory Protection Contract

`weekend_activities` is a protected inventory block for the current weekend,
not a ranked sample of leisure content. Any change that affects the whole
issue - public item budgets, LLM ranking, dedupe, repeat policy, source
selection, enrichment, editor repair, QA or release reconciliation - must
verify this block separately.

Inventory scope:

- all Greater Manchester public visitor weekend events from trusted weekend
  sources when they happen in the current weekend window: markets, makers /
  artisan markets, fairs, car boots, flea/vintage sales, food and drink
  festivals, beer festivals, Pride/community festivals, heritage / medieval /
  re-enactment festivals, family/community days and distinctive public weekend
  activities;
- distinctive one-off weekend activities can qualify when the source gives a
  concrete date, place and visitor value, for example a beauty brunch, a museum
  after-hours/protest-music event, a public workshop or a themed public trail;
- current weekend means the rendered issue's Friday-Sunday window, plus bank
  holiday Monday where the weekend window explicitly includes it;
- ordinary standalone theatre shows, arena/gig/concert listings, comedy club
  runs, generic nightlife and Ticketmaster-style ticket inventory do not qualify
  by default. They stay in Tickets / future announcement blocks unless the item
  is part of a qualifying festival/community/public-weekend activity above.

Selection rule:

- Eligibility is date + place + activity type + GM fit.
- The public target is at least six unique concrete activities. Landing pages,
  expired first occurrences and duplicate rows from the same recurring series
  do not count.
- Market/fair routing happens before planning; writer is not allowed to rescue
  a Food/Planning row into Weekend after the slot plan exists.
- Ranking may order eligible Weekend items, but must not exclude an eligible
  current-weekend inventory item.
- Global public-budget caps, DeepSeek board caps, ticket balancing and soft-item
  throttles must not remove eligible current-weekend inventory. If the full
  digest needs compression, Weekend inventory is compacted inside the section,
  not silently dropped.
- If inventory is large, render grouped compact subsections rather than dropping
  eligible items, for example markets/car boots, festivals/community/family,
  food/drink/beer, Pride/heritage/special activity and museum/workshop/trails.

Recurring and repeat rule:

- Recurring source text such as "every Saturday", "every Sunday", "first
  Saturday of the month" or source-declared next-market dates must be promoted
  into a concrete `event.date_start` before validator, publish plan, dedupe and
  writer decisions.
- Repeats are evaluated by occurrence, not only by source URL/title. A recurring
  market already shown before is allowed again when the new occurrence date is
  inside the current weekend, and the visible line must name that occurrence.
- A repeated recurring event is held only when the occurrence is not current, the
  date cannot be recovered, or the item is not actually useful to visitors
  (for example a seller/admin page rather than a public visitor event).
- Intake, happening-today, Weekend eligibility, writer expiry, repeat and
  protected eligibility all read the same `effective_occurrence_window()`.

Recovery rule:

- Before holding a trusted Weekend source for missing facts, run the available
  page/detail enrichment and recurrence extraction. Hold only after enrichment
  still cannot recover date, place or public activity type.
- A trusted market/fair/festival source that fetched successfully but parsed
  zero candidates is a coverage incident, not a clean empty source, unless the
  source explicitly has no current public event.

Reporting rule:

- Weekend reports must show collected current-weekend eligible inventory,
  rendered eligible inventory and every missing eligible item with a plain
  reason: parser empty, date not recovered, not current weekend, duplicate
  same occurrence, not public visitor event, or source facts too thin.
- Candidate-level loss is reported in `writer_report.weekend_inventory_loss_trace`.
  Source-level coverage incidents are reported in
  `release_report.source_status.weekend_source_coverage`.
- The final HTML is the truth: an eligible Weekend item counts only if it is
  visible in `data/outgoing/current_digest.html` or replaced by another eligible
  item with a recorded reason.

## Next 7 Days Contract

Next 7 Days is Planning (see Section Routing Contract): important dated,
**non-leisure** items in the coming week that the reader must act on or know:

- confirmed restrictions / roadworks / closures starting within the week;
- civic deadlines, consultations closing, last chance / final week;
- practical service changes with a concrete date.

Rules:

- Must have date and place/online.
- Leisure / what's-on (markets, fairs, festivals, shows, exhibitions, concerts)
  does NOT belong here — it is Weekend, Tickets or Дальние анонсы. A date this
  week is not enough to admit a leisure item.
- Must not duplicate a full item from Today, Fresh, Weekend or Tickets.
- Repeat from yesterday requires a stronger window: starts today, tomorrow,
  final week, sale starts, sold out, extra date or changed venue.

Failure examples:

- Bolton car boot or a gallery exhibition shown here instead of Weekend/Tickets.
- A leisure item admitted only because Weekend was hidden midweek.

## Ticket Radar Contract

Ticket item must include:

- `venue_scope`: GM / nearby / outside;
- event date;
- sale/ticket status if available;
- tier with evidence;
- why now;
- horizon category: tomorrow / this week / next month / future major.

Rules:

- Outside venue cannot use GM wording.
- A-tier must be evidence-based.
- The event owner is resolved before artist notability. A normal show keeps its
  headliner; support never replaces it. A festival keeps the real event name,
  lists genuine A-tier acts inside one card and is not multiplied per artist.
- A canonical physical event is identified by event owner, venue and occurrence
  date. Different dates or venues must remain represented; a same-venue
  multi-night run may share one card only when that card lists every date.
- Important concerts should be discovered early, not one day before.
- Ordinary ticket volume must be capped before editor, not only reported after
  render. Every canonical active A-tier ticket bypasses timing, watch, repeat,
  section and issue caps. Technical duplicates, wrong-owner fragments,
  cancelled/postponed and expired rows are rejected before this protection.
- Public ticket text contains event facts and reader timing only. Ranking
  reasons, internal tiers and service phrases never appear in the card.
- Ticket Radar is not a general event calendar.

Failure examples:

- London venue described as Greater Manchester.
- Future major concert shown without why-now.
- Ticket dominance warning with no rebalance action.

## Outside-GM Contract

Outside-GM exists only for genuinely important UK events outside Greater
Manchester.

Rules:

- Always say the city/venue plainly.
- Never use "in Greater Manchester" wording for outside venue.
- Non-A-tier inventory must be capped before writer/editor; every recognised
  outside-GM A-tier item remains visible with explicit city/venue wording.
- Must not reduce Fresh, Today, Transport, Weekend or public-service coverage.

Failure examples:

- Outside-GM selected pool larger than the whole core-news pool.
- Outside-GM concerts crowd out hard local news.

## Professional Contract

Professional item must include:

- date;
- place or online;
- free/paid/booking;
- relevance to user profile;
- CV-match verdict: `go`, `consider`, or `skip`.

Rules:

- `skip` cannot be `must_show` or visible.
- CV-match must happen after fact extraction and before publish selection.
- Pages without a concrete event date/place must not become protected publish
  items.
- A professional item without booking/access clarity is held unless another
  source enriches it.

Failure examples:

- "Business engagement services" or generic programme page treated as
  must-show.
- CV-match report says skipped/applied 0 but the block still publishes based on
  generic business terms.

## Russian Events Contract

Russian Events require positive evidence:

- Russian/Ukrainian language;
- diaspora promoter;
- Russian-language page;
- performer/audience evidence;
- explicit cultural/community relevance.

Rules:

- Afisha London as a source is not enough.
- UK-wide/London-heavy items must say geography clearly.
- Generic comedy/music listing cannot be Russian Events without evidence.
- Positive evidence must be stored on candidate/report before publish selection.
- City or other UK geography must be stored as a fact, not inferred only by the
  final Russian sentence.

Failure examples:

- Source label alone makes a candidate `russian_events`.
- London event appears without UK/outside-GM context.

## Food/Openings Contract

Food/opening item must include:

- exact place;
- area/station if possible;
- opening status/date;
- why it matters.

Rules:

- Repeats require a new fact: opening started, date changed, resident changed,
  menu/concept changed, venue reopened, official confirmation.
- Markets belong to Weekend when they are weekend activities.
- Vague "check details" copy is not enough.
- The public section contains exactly three real cards. When two independent
  sources are present in the eligible live pool, at least two sources must be
  represented in those three cards.
- Night readiness is based on concrete object, place, opening/market phase,
  current date and local reader meaning. A night Russian line is not required;
  the normal morning writer creates public text from these facts.

Failure examples:

- "new pie shop at a station" without exact station.
- Repeating yesterday's opening with no new status.

## Night Inventory Contract

- `INVENTORY_BLOCK_REGISTRY` defines all 17 active/legacy block identifiers and
  is the only source for collection category, candidate category, output blocks,
  intake mode, freshness, text policy, completeness and replacement permission.
- Night collection stores facts and provenance. Any cached deterministic line
  is inventory diagnostics only and never replaces morning public prose.
  Professional CV matching remains a separate factual selection step.
- Every record stores source report category and routed candidate category,
  run/wave/source, first/last/change times, canonical evidence hash, observation
  in the current wave, action-URL liveness, serving TTL and retention horizon.
- When a night card and a live card identify the same standalone article/event,
  the live card remains primary and receives only facts missing from the live
  extraction: event owner, occurrence, venue, lineup, booking URL and factual
  detail fields. Night never overwrites summary/lead/practical angle/draft line.
  A standalone night card is held until a morning-live candidate confirms it.
  For Food only, a card observed in the current night wave is also confirmed
  when that same source is reached successfully in the morning and answers
  `304 Not Modified`; it then follows normal dedupe, validation and planning.
  The night lineage remains visible through validation, selection, writer and
  final HTML reporting instead of being counted as a dropped duplicate.
- Action URLs are checked only for fact-ready cards. HTTP 2xx/3xx is alive;
  404/410 becomes dead only after two different night runs; 403/405/429 and
  network/timeout failures remain unknown. Unknown is usable in assist but can
  never authorize source replacement. Confirmed-dead and out-of-retention rows
  are physically removed; event/ticket retention is event end plus 30 days,
  Food is 90 days from its dated phase/last observation, and transport is 14
  days from its last observation.
- A night wave is `success` only when all expected sources complete without
  source errors. `degraded` preserves partial inventory and prevents source
  replacement. `failed` means the command did not produce a complete usable
  wave. Professional CV reports conserve every sent event as go, consider,
  skip or held-error; an omitted model row makes the response partial, not OK.
- `scan_complete` means today's expected sources were checked without source
  errors. `block_sufficient` means the post-card inventory meets the block floor
  and source diversity. Neither value implies the other.
- Optional Future and Outside-GM blocks may be honestly empty after a complete
  scan. Weekend never loses eligible current-weekend inventory to an intake cap;
  A-tier never loses visibility to an intake cap.
- A broad morning source is skipped only when every block it can feed is restored,
  today's scan is complete, required blocks are sufficient and source replacement
  is explicitly enabled for every output block, and enough action URLs are alive.
  Otherwise the ordinary live scan remains on.
- Night inventory never authorises skipping the morning live scan. The final
  funnel reports `night fact-ready -> live confirmed -> enriched -> planned ->
  visible` separately from inventory collection totals.

## Business/IT Contract

Business item must explain:

- who;
- what changed;
- where;
- why it matters;
- money/jobs/product/service impact if available.

Rules:

- IT membership is decided from the story's technology/product/software/data
  content, not from the publisher category. Politics/Burnham coverage without
  actual IT content stays in City or News.
- IT is capped at five and has no fill quota; three real IT developments are
  preferable to five mixed business/political rows.
- Generic "check details" copy is not acceptable.
- Personnel PR, anniversary, award and campaign posts need concrete action or
  local impact.
- Business event listings with date/place/free access belong in Professional,
  not Business/IT.

Failure examples:

- Staff appointment published with no reader value.
- Business support homepage treated as a new development.

## Football Contract

Source priority:

- official club;
- BBC / Guardian / Sky reliable reporting;
- MEN transfer/opinion only if the fact is confirmed.

Rules:

- Avoid opinion/rumour as main football item.
- Use match/result/fixture/injury/manager/official confirmation as anchors.
- If football is quiet, do not force weak filler above stronger city news.
- Underflow needs a reason: no relevant match/update, not writer loss.

Failure examples:

- Transfer liveblog/speculation as primary item.
- Club PR quiz/interview as the only football card.

## Protected Eligibility Contract

Protection is evaluated only after current occurrence, block, event owner,
date, duplicate status and prose validity are known. It exempts a valid row
from a cap; it never turns an invalid, expired, duplicated or malformed row into
a valid candidate.

## Prose Policy Contract

Writer, editor, deterministic repair, release reporting and final plan verify
use the same classifier from `editorial_contracts`. It returns defect code and
severity, removes empty/service wording such as `проверьте детали`, `сверяйте
обновления` and `сроки уточняйте на странице перевозчика`, while preserving a
concrete useful action such as `проверьте страницу статуса TfGM`. Prose defects
trigger row repair or degraded reporting; they do not introduce a new
whole-issue blocking gate.

## Recovery Contract

Recovery must not only insert a reserve line.

For each underflow:

1. Find same-block reserve or adjacent allowed source.
2. Enrich facts from candidate/source fields or refetch if allowed.
3. Rewrite the line.
4. Run editor/check on the candidate line.
5. Insert into draft.
6. Validate final HTML.

If no replacement exists, report a human-readable reason:

- no candidate with date/place;
- no passenger impact;
- no positive Russian evidence;
- duplicate already covered;
- outside horizon;
- source too thin to write without inventing facts.

Failure examples:

- Generic TfGM fallback replacing a concrete rail/bus issue.
- Must-show missing because line had English, without same-block replacement.

## Repeat Contract

A repeated story is allowed only with a concrete new reader-useful fact:

- new date;
- booking opened;
- sale started;
- sold out;
- extra date;
- venue changed;
- opening started;
- official confirmation;
- new disruption window;
- new court/safety stage;
- materially more urgent window.

Rules:

- "Announced", "updated", "new phase" or "still important" is not enough by
  itself.
- The concrete changed fact must be named in candidate/report.
- If the changed fact cannot be shown to the reader, the repeat is held.
- Every stage uses `visible_repeat_verdict`. A canonical active A-tier event is
  the one explicit exception: `allow=true`,
  `reason=a_tier_must_show_override`. The exception does not apply to technical
  duplicates, wrong-owner cards, cancellations or expired events.

Failure examples:

- Same venue/event repeated because it is closer, without a new action.
- Same story from a new source treated as a new phase.
