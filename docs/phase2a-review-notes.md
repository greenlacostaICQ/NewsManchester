# Phase 2A Review Notes

Last reviewed: 2026-04-28

This note captures the current human review of the first Phase 2A
agent-extraction pass. It is intentionally short and non-normative: the
rules here are guidance for the next extraction comparison, not a
runtime gate.

## Current verdict

On the first 12-item runtime pack (`BBC Manchester`, `GMP`, `MEN`,
`The Mill`), agent extraction was materially better than the current
deterministic baseline for:

- `fact_type`
- `borough`
- `publishable`
- `needs_second_source`

The baseline remains useful only as a conservative comparator. It is not
the source of truth for civic classification.

## `publishable` guidance

Set `publishable=true` when all of the following hold:

- the item has a clear Greater Manchester angle
- it describes a concrete new development, not a feature/profile/promo
- a reader can understand why it matters today
- it is not merely a wrapper around another publication's work

Set `publishable=false` for:

- neighbourhood or lifestyle features without a concrete trigger
- generic UK explainers/quizzes with no Greater Manchester relevance
- promotional or subscription-wrapper items
- background institutional explainers without a new development
- cultural teases with no date/window/concrete trigger

## `needs_second_source` guidance

Set `needs_second_source=true` when:

- the item looks important but the operational detail is thin
- the exact where/when is incomplete
- the summary is teaser-like rather than self-contained
- the item is policy/analysis-heavy and the direct public impact is not yet concrete

Set `needs_second_source=false` when:

- the item is already a clear official fact
- the named event/date/development is explicit enough to publish as-is
- the missing detail is non-critical rather than release-blocking

## Field confidence after pass 1

- `fact_type`: promising
- `borough`: promising, but still needs another pass on long-form and soft-feature items
- `publishable`: promising if the guidance above holds on another pack
- `needs_second_source`: promising as an editorial flag
- `entities`: useful, but not ready for trust without a normalization layer

## Phase 2B readiness bar

We can treat these fields as ready for Phase 2B once a second fresh pack
shows a similar pattern:

- `fact_type`
- `borough`
- `publishable`
- `needs_second_source`
