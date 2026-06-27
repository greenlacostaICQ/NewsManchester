# NewsManchester Decisions And Lessons

Last reviewed: 2026-06-27.

This document records decisions that should not be re-litigated without a new
RCA. It is intentionally product-first: what the reader saw, what the system
did, and what the decision prevents.

## Decisions

### Standing rule - Never block the release (delivery always ships)

Context:
A pre-send content check once "silently killed the send for three days" when a
factual nit failed a gate. Delivery is the product's whole point; a quiet
non-send is worse than a flawed-but-delivered issue.

Decision:
Delivery is never blocked or held by a content/quality finding. The send gate
blocks ONLY on technical consistency: built and promoted digest, passing release
decision, current gate version, and matching date/header. Every quality finding
is warning-only and must drive recover/rebalance-before-send plus RCA, never a
held send. "Not pass" in these docs means *not a clean quality pass*, never a
delivery decision.

Why:
The reader getting a slightly-off issue beats the reader getting nothing. The
fix for a quality defect is the next run, not a blocked delivery today.

What this prevents:
Re-introducing a content-based send blocker — the exact failure that suppressed
delivery for three days.

Related files/modules:
`scripts/run_local_digest.py:138` (send gate is technical-only),
`.github/workflows/daily-digest.yml:90` (content judge must never block),
`src/news_digest/pipeline/release.py:2668` (named "never-block rule"),
`AGENTS.md`.

### 2026-06-27 - Final HTML is source of truth

Context:
Reports can say selected, rendered, recovered or pass while the final Telegram
HTML tells a different story.

Decision:
A candidate counts as published only if it is visible in
`data/outgoing/current_digest.html`.

Why:
The reader sees HTML, not `writer_report.json`, `publish_plan.json` or internal
counters.

What this prevents:
Claims that a lead, `must_show` item or replacement was delivered when it only
exists in state files.

Related files/modules:
`src/news_digest/pipeline/common.py`, `writer.py`, `editor.py`, `release.py`,
`data/outgoing/current_digest.html`.

### 2026-06-27 - Internal counters are not enough

Context:
On 2026-06-27, writer/release/pre-send counts differed from final HTML
extraction, and product-health warnings still shipped.

Decision:
Counters are diagnostics. The pass condition must reconcile selected,
protected, rendered and final HTML-visible items.

Why:
A successful stage can still lose items later.

What this prevents:
False confidence from `stage_status=complete`, section counts or
`release_decision=pass`.

Related files/modules:
`writer_report.json`, `release_report.json`, `pre_send_quality_report.json`,
`src/news_digest/pipeline/common.py:extract_sections`.

### 2026-06-27 - Editor failed/partial_failed is not a clean quality pass

Context:
The 2026-06-27 editor report contained `Pre-send Russian editor
skipped/failed: partial_failed` and `failed`, but release and send still
proceeded.

Decision:
Editor `failed` or `partial_failed` must trigger recovery/rebalance before send
and is not a clean quality pass. It must never block or hold delivery — the
issue still ships (never-block rule).

Why:
The final editor is the last chance to catch visible Russian, structure and
balance defects before the reader sees them.

What this prevents:
Shipping known-bad or unchecked text while calling the run successful.

Related files/modules:
`src/news_digest/pipeline/editor.py`, `final_editor_report.json`,
`scripts/run_local_digest.py`.

### 2026-06-27 - must_show must be visible or explicitly replaced

Context:
The 2026-06-27 writer contract reported 17 `must_show` items, 14 rendered and 3
missing professional items.

Decision:
`must_show` means visible in final HTML or explicitly replaced with a
human-readable reason.

Why:
Protected selection has no product meaning if it can vanish later.

What this prevents:
Lead/protected/professional/russian/transport items disappearing silently behind
quality drops or caps.

Related files/modules:
`publish_plan.json`, `writer.py`, `release.py`, `writer_report.json`.

### 2026-06-27 - Transport requires passenger impact

Context:
Earlier fixes blocked some lift/escalator and business-event leaks, but
non-movement infrastructure and nearby incidents can still enter Transport.

Decision:
Transport requires passenger/driver impact today or tomorrow.

Why:
The transport block is a travel-decision tool, not a general transport-themed
news bucket.

What this prevents:
Metrolink funding, station redevelopment, generic infrastructure or nearby
emergency-service stories replacing real disruption.

Related files/modules:
`collector/routing.py`, `candidate_validator.py`, `transport_language.py`,
`writer.py`, `release.py`.

### 2026-06-27 - Weekend requires a product contract, not generic events

Context:
Weekend sources and writer fallbacks improved, but wrong-horizon and low-value
events can still appear.

Decision:
Weekend means upcoming weekend activity in or clearly relevant to Greater
Manchester: markets, fairs, food, family, community, festivals and low-cost
things to do.

Why:
The reader uses Weekend for plans this weekend, not for generic event discovery.

What this prevents:
Single concerts, far-future events and weak listings crowding out actual weekend
plans.

Related files/modules:
`writer.py`, `collector/routing.py`, `event_extraction.py`,
`data/sources.toml`.

### 2026-06-27 - Ticket geography must be resolved before writing text

Context:
Ticket Radar and Outside-GM can create misleading wording if the venue scope is
decided late or only in the writer.

Decision:
Every ticket candidate needs `venue_scope` before visible text is written:
GM, nearby or outside.

Why:
The same artist can have Manchester, Liverpool and London dates. The reader
must not see outside venues described as GM.

What this prevents:
Wrong geography, wrong horizon and overpromotion of outside-GM concerts.

Related files/modules:
`editorial_contracts.py`, `ticket_notability.py`, `collector/routing.py`,
`writer.py`.

### 2026-06-27 - Afisha London is not positive Russian evidence by itself

Context:
Russian Events are source-driven and UK/London-heavy. A source label alone does
not prove Russian-speaking relevance for every item.

Decision:
Russian Events require positive evidence in the candidate: language, diaspora
promoter, Russian-language page, performer/audience evidence or explicit
community relevance.

Why:
The block is for Russian-speaking/diaspora relevance, not every listing from a
known source.

What this prevents:
False Russian Events caused only by source identity.

Related files/modules:
`data/sources.toml`, `editorial_contracts.py`, `writer.py`, `release.py`.

### 2026-06-27 - CV-match skip cannot be published

Context:
Professional matching exists, but the 2026-06-27 run applied no LLM CV-match
decision while professional `must_show` items still existed.

Decision:
`skip` from CV-match cannot be `must_show` or visible. Missing CV-match for a
borderline professional event must hold or require enrichment.

Why:
The block is personalized; generic business keywords are not enough.

What this prevents:
Publishing irrelevant professional pages just because they match business words.

Related files/modules:
`professional_events.py`, `candidate_validation_report.json`, `writer.py`.

### 2026-06-27 - Recovery must run full enrich/rewrite/check loop

Context:
Recovery currently can insert fallback/reserve lines and report degradation
without proving that the replacement went through full fact enrichment,
rewriting, editor check and final HTML validation.

Decision:
For underflow or missing protected items, recovery is:
reserve -> enrich facts -> rewrite -> editor/check -> insert -> final HTML
validation.

Why:
A weak reserve line can make the section count look better while making the
digest worse.

What this prevents:
Replacing a concrete disruption or strong story with vague generic filler.

Related files/modules:
`llm_rewrite.py`, `writer.py`, `editor.py`, `release.py`,
`pre_send_quality_judge.py`.

### 2026-06-27 - Outside-GM must be capped before editor

Context:
On 2026-06-27, pre-send judge warned about 35 ticket/concert items versus 21
core items, and the outside-GM selected pool was extremely large.

Decision:
Outside-GM and ticket dominance must be capped/rebalanced before editor, not
only reported after render.

Why:
Once the final editor sees an already ticket-heavy digest, it is too late to
recover core-news balance reliably.

What this prevents:
Successful sends that read like ticket catalogues instead of a Manchester daily
brief.

Related files/modules:
`section_selection_report.json`, `publish_plan.json`, `writer.py`,
`pre_send_quality_judge.py`.

### 2026-06-27 - Repeat new phase requires concrete new fact

Context:
The pipeline has repeat and calendar review logic, but "new phase" labels can
still be overtrusted.

Decision:
A repeat is allowed only when the concrete new fact is named: date, sale,
sold-out, extra date, venue change, opening started, official confirmation,
new disruption window or new court/safety stage.

Why:
Readers should not see the same story again because another source republished
it or a vague "updated" label appeared.

What this prevents:
False novelty and recurring stale items.

Related files/modules:
`dedupe.py`, `editorial_contracts.py`, `history.py`, `published_facts.json`.

## Lessons

### Lesson - Do not fix visible symptom first

Example:
Transport v1 blocked lift/escalator items but still allowed non-movement
infrastructure.

Learning:
Define the passenger-impact contract, not only negative examples.

### Lesson - Reports are not gates

Example:
Pre-send judge detected ticket dominance and low writer yield, but returned
`decision=warn`, `can_send=true`, and the workflow sent the digest.

Learning:
Critical quality findings must trigger rebalance, recovery or replacement before
send, not only reporting — but never a held or blocked send (never-block rule).

### Lesson - Fixing writer does not fix selection

Example:
Ticket text improved, but wrong venues, timing and outside-GM volume still
entered the block.

Learning:
Selection, geography and horizon must be fixed before writer.

### Lesson - Source fixed yesterday can still fail today

Example:
Recent commits fixed dead parsers and source health reports, but 2026-06-27
still had empty parsers and many zero-yield sources.

Learning:
Source health must show where loss happened: parser, candidates, filter,
selection, writer, editor or HTML.

### Lesson - A successful run is not the same as a healthy issue

Example:
Run `28281912111` succeeded and sent, while release health was `unhealthy` and
warnings showed underflow, ticket dominance, low writer yield and editor
failure states.

Learning:
Separate transport success from product success in every RCA.

### Lesson - Lead selection is not lead delivery

Example:
Curator can set `lead_set=true`, but only final HTML proves that the lead was
visible as the lead.

Learning:
Lead checks must compare candidate identity to final HTML, not stop at curator
or writer counters.

### Lesson - Personalization must be decisive or absent

Example:
Professional event CV-match can run but apply no verdict, while generic
professional pages still reach protected selection.

Learning:
If a personalized block exists, its personalization verdict must decide
visibility before publish selection.

## Open Questions

- Where exactly do selected / `must_show` items disappear?
- Which block minimums are real gates, and which are only warnings?
- Which candidate losses are correct versus wrong?
- Which sources are parser-empty versus downstream-lost?
- What is the correct ticket horizon logic?
- What is the right Weekend source set?
- How should a balance-critical pre-send `warn` drive rebalance/recovery before
  send? (Delivery still always proceeds — never-block rule.)
- How should source health join parser, candidate, selected, rendered and HTML
  visibility in one trace?
- What exact fields should define positive Russian-event evidence?
- What is the final cap policy for Outside-GM and future major tickets?
