from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from news_digest.pipeline.common import now_london
from news_digest.pipeline.block_policy import BLOCK_POLICY_REGISTRY
from news_digest.pipeline.editorial_contracts import (
    build_editorial_contract,
    calendar_repeat_review,
    lifecycle_repeat_review,
)
from news_digest.pipeline.weekend_inventory import weekend_occurrence_date


OPERATIONAL_REPEAT_BLOCKS = frozenset(
    block
    for block, policy in BLOCK_POLICY_REGISTRY.items()
    if policy.get("repeat_policy") == "operational"
)
TICKET_REPEAT_BLOCKS = frozenset(
    block
    for block, policy in BLOCK_POLICY_REGISTRY.items()
    if policy.get("repeat_policy") == "ticket_calendar_milestones"
)
EVENT_REPEAT_BLOCKS = frozenset(
    block
    for block, policy in BLOCK_POLICY_REGISTRY.items()
    if policy.get("repeat_policy") == "calendar_milestones"
)
EVENT_REPEAT_CATEGORIES = frozenset({
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "diaspora_events",
    "professional_events",
})


# 0165: for an event or ticket card a new phase is a changed FACT — date,
# place, status, lineup, availability or sale. A fresh model retelling of the
# very same facts is not one, so it must not buy a daily repeat slot.
_CONCRETE_PHASE_FACTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("date", ("date_start", "date_end", "next_occurrence")),
    ("place", ("venue", "borough")),
    ("status", ("event_status",)),
    ("lineup", ("lineup", "attractions")),
    ("availability", ("price", "free")),
    ("sale", ("ticket_type",)),
)


def _phase_fact_value(entry: dict[str, Any], key: str) -> str:
    event = entry.get("event") if isinstance(entry.get("event"), dict) else {}
    if key == "attractions":
        rows = event.get("attractions") if isinstance(event.get("attractions"), list) else []
        names = [str((row or {}).get("name") or "") for row in rows if isinstance(row, dict)]
        return "|".join(sorted(name for name in names if name))
    if key == "lineup":
        rows = event.get("lineup") if isinstance(event.get("lineup"), list) else []
        return "|".join(sorted(str(row) for row in rows if str(row or "").strip()))
    if key == "ticket_type":
        return str(entry.get("ticket_type") or event.get("ticket_type") or "").strip().lower()
    if key == "date_start":
        return str(event.get("date_start") or event.get("date") or "").strip()[:10]
    return str(event.get(key) if event.get(key) is not None else "").strip().lower()


def concrete_phase_changes(candidate: dict[str, Any], previous: dict[str, Any] | None) -> list[str]:
    """Which of the six concrete facts moved between the published card and today's.

    Only compares facts both sides actually state: an older published entry that
    never stored a venue must not read as "the venue changed".
    """
    if not previous:
        return []
    changed: list[str] = []
    for label, keys in _CONCRETE_PHASE_FACTS:
        for key in keys:
            now_value = _phase_fact_value(candidate, key)
            was_value = _phase_fact_value(previous, key)
            if now_value and was_value and now_value != was_value:
                changed.append(label)
                break
    return changed


def needs_concrete_phase_fact(candidate: dict[str, Any]) -> bool:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    return block in TICKET_REPEAT_BLOCKS or block in EVENT_REPEAT_BLOCKS or category in EVENT_REPEAT_CATEGORIES


@dataclass(frozen=True, slots=True)
class RepeatVerdict:
    allow: bool
    repeat_class: str
    reason: str
    matched_by: str = "none"
    previous_fingerprint: str = ""
    previous_title: str = ""
    previous_published_day: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "allow": self.allow,
            "repeat_class": self.repeat_class,
            "reason": self.reason,
            "matched_by": self.matched_by,
            "previous_fingerprint": self.previous_fingerprint,
            "previous_title": self.previous_title,
            "previous_published_day": self.previous_published_day,
        }


def _contract(candidate: dict[str, Any]) -> dict[str, Any]:
    raw = candidate.get("editorial_contract")
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    # Routing and event enrichment happen after an earlier contract may have
    # been attached. Calendar decisions must read today's structured date, not
    # a stale ``event_shape=none`` snapshot saved before enrichment.
    if (
        block in TICKET_REPEAT_BLOCKS
        or block in EVENT_REPEAT_BLOCKS
        or category in EVENT_REPEAT_CATEGORIES
        or category == "venues_tickets"
    ):
        return build_editorial_contract(candidate)
    return raw if isinstance(raw, dict) else build_editorial_contract(candidate)


def _event_day(candidate: dict[str, Any]) -> date | None:
    from news_digest.pipeline.weekend_inventory import effective_occurrence_window  # noqa: PLC0415

    start, _ = effective_occurrence_window(candidate)
    if start is not None:
        return start
    if str(candidate.get("primary_block") or "") == "weekend_activities":
        return weekend_occurrence_date(candidate)
    return None


def _previous_day(previous: dict[str, Any]) -> str:
    return str(
        previous.get("last_published_day_london")
        or previous.get("first_published_day_london")
        or previous.get("published_day_london")
        or previous.get("ts")
        or ""
    )[:10]


def is_calendar_carry_candidate(candidate: dict[str, Any]) -> bool:
    """True only for event/ticket classes that may legitimately reappear.

    Food/opening articles enter only with a concrete future opening occurrence.
    Undated announcement copy remains one-shot; dated launches may return at
    the block's D7/D1/D0 reader milestones.
    """
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block == "openings" or category == "food_openings":
        from news_digest.pipeline.event_extraction import event_start_date  # noqa: PLC0415

        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        event_day = event_start_date(candidate)
        return bool(
            event_day
            and event_day >= now_london().date()
            and str(event.get("event_name") or candidate.get("title") or "").strip()
            and str(event.get("venue") or "").strip()
            and str(event.get("date_confidence") or "high") != "low"
            and event.get("is_event") is not False
        )
    if str(_contract(candidate).get("anchor_type") or "") == "bookable_listing":
        return True
    if block in TICKET_REPEAT_BLOCKS or block in EVENT_REPEAT_BLOCKS:
        return True
    return category in EVENT_REPEAT_CATEGORIES


def calendar_carry_verdict(candidate: dict[str, Any], previous: dict[str, Any]) -> RepeatVerdict:
    if not is_calendar_carry_candidate(candidate):
        return RepeatVerdict(False, "not_calendar_carry", "block_or_category_not_calendar_repeatable")

    review = calendar_repeat_review(candidate, previous)
    if review.get("applies"):
        return RepeatVerdict(
            bool(review.get("allow")),
            "calendar",
            str(review.get("reason") or "calendar_repeat_review"),
            matched_by="calendar_review",
            previous_fingerprint=str(previous.get("fingerprint") or ""),
            previous_title=str(previous.get("title") or ""),
            previous_published_day=_previous_day(previous),
        )

    text_date = _event_day(candidate)
    anchor = str(_contract(candidate).get("anchor_type") or "")
    if (
        anchor == "bookable_listing"
        and text_date is not None
        and now_london().date() <= text_date <= now_london().date() + timedelta(days=14)
    ):
        return RepeatVerdict(
            True,
            "calendar",
            "upcoming_event_occurrence_window",
            matched_by="effective_occurrence_window",
            previous_fingerprint=str(previous.get("fingerprint") or ""),
            previous_title=str(previous.get("title") or ""),
            previous_published_day=_previous_day(previous),
        )

    return RepeatVerdict(False, "not_calendar_carry", "calendar_review_not_applicable")


def validator_same_fingerprint_allow(candidate: dict[str, Any]) -> RepeatVerdict:
    """Typed same-fingerprint exceptions for validator cross-day rehash.

    This replaces anchor-only allowlisting. Anchors such as ``dated_event`` are
    only repeatable inside known event/ticket blocks, not in food/opening/news.
    """
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    contract = _contract(candidate)
    anchor = str(contract.get("anchor_type") or "")
    story_type = str(contract.get("story_type") or "")
    event_shape = str(contract.get("event_shape") or "")

    if block in OPERATIONAL_REPEAT_BLOCKS or anchor in {"service_status", "today_weather", "ongoing_disruption"}:
        return RepeatVerdict(True, "operational", f"operational_anchor:{anchor or block}")

    if (
        block in TICKET_REPEAT_BLOCKS
        or category == "venues_tickets"
        or story_type == "ticket"
        or event_shape == "ticket"
        or anchor == "ticket_opportunity"
    ):
        return RepeatVerdict(True, "ticket", "ticket_repeat_managed_by_calendar_policy")

    if is_calendar_carry_candidate(candidate) and (
        block == "openings"
        or category == "food_openings"
        or anchor in {"dated_event", "recurring_occurrence", "bookable_listing"}
        or event_shape in {"recurring", "festival", "one_off", "event_like", "bookable_activity"}
    ):
        event_day = _event_day(candidate)
        if event_day is None or event_day >= now_london().date():
            return RepeatVerdict(True, "event", f"event_anchor:{anchor or event_shape}")

    return RepeatVerdict(False, "same_fingerprint", f"anchor_not_repeatable:{anchor or 'none'}")


def visible_repeat_verdict(candidate: dict[str, Any], previous: dict[str, Any] | None) -> RepeatVerdict:
    from news_digest.pipeline.ticket_notability import (  # noqa: PLC0415
        a_tier_ticket_policy,
        is_a_tier_ticket,
    )

    if is_a_tier_ticket(candidate):
        eligible, reason = a_tier_ticket_policy(candidate)
        if not eligible:
            return RepeatVerdict(
                False,
                "a_tier_ineligible",
                f"a_tier_ineligible:{reason}",
                previous_fingerprint=str((previous or {}).get("fingerprint") or ""),
                previous_title=str((previous or {}).get("title") or ""),
                previous_published_day=_previous_day(previous or {}),
            )
    if not previous:
        return RepeatVerdict(True, "new", "no_previous_match")

    previous_fp = str(previous.get("fingerprint") or "")
    previous_title = str(previous.get("title") or "")
    previous_day = _previous_day(previous)
    matched_by = "fingerprint" if previous_fp and previous_fp == str(candidate.get("fingerprint") or "") else "history"

    if previous_day == now_london().date().isoformat() and matched_by == "fingerprint":
        return RepeatVerdict(
            True,
            "same_day",
            "same_day_correction",
            matched_by=matched_by,
            previous_fingerprint=previous_fp,
            previous_title=previous_title,
            previous_published_day=previous_day,
        )

    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if (
        matched_by == "fingerprint"
        and (block == "openings" or category == "food_openings")
        and not is_calendar_carry_candidate(candidate)
    ):
        comparable_previous_facts = any(
            previous.get(key)
            for key in ("summary", "lead", "event", "change_phase", "editorial_contract")
        )
        candidate_day = str(candidate.get("published_at") or "")[:10]
        if not comparable_previous_facts or (
            candidate_day and previous_day and candidate_day <= previous_day
        ):
            return RepeatVerdict(
                False,
                "same_fingerprint",
                "food_repeat_without_comparable_new_fact",
                matched_by=matched_by,
                previous_fingerprint=previous_fp,
                previous_title=previous_title,
                previous_published_day=previous_day,
            )

    if str(candidate.get("change_type") or "") in {"same_story_new_facts", "follow_up"}:
        # 0165: for an event/ticket card the claim "same story, new facts" has to
        # name the fact. Without a moved date, place, status, lineup, availability
        # or sale it is a fresh retelling of yesterday's card, and the ordinary
        # repeat rules below decide it.
        changed = concrete_phase_changes(candidate, previous) if needs_concrete_phase_fact(candidate) else ["declared"]
        if changed:
            return RepeatVerdict(
                True,
                "lifecycle",
                f"concrete_story_change:{','.join(changed)}",
                matched_by=matched_by,
                previous_fingerprint=previous_fp,
                previous_title=previous_title,
                previous_published_day=previous_day,
            )

    validator_verdict = validator_same_fingerprint_allow(candidate)
    if validator_verdict.allow:
        if validator_verdict.repeat_class in {"ticket", "event"} or is_calendar_carry_candidate(candidate):
            calendar_verdict = calendar_carry_verdict(candidate, previous)
            if calendar_verdict.repeat_class == "calendar":
                return RepeatVerdict(
                    calendar_verdict.allow,
                    calendar_verdict.repeat_class,
                    calendar_verdict.reason,
                    matched_by=matched_by,
                    previous_fingerprint=previous_fp,
                    previous_title=previous_title,
                    previous_published_day=previous_day,
                )
            return RepeatVerdict(
                False,
                "calendar",
                calendar_verdict.reason,
                matched_by=matched_by,
                previous_fingerprint=previous_fp,
                previous_title=previous_title,
                previous_published_day=previous_day,
            )
        return RepeatVerdict(
            True,
            validator_verdict.repeat_class,
            validator_verdict.reason,
            matched_by=matched_by,
            previous_fingerprint=previous_fp,
            previous_title=previous_title,
            previous_published_day=previous_day,
        )

    lifecycle = lifecycle_repeat_review(candidate, previous)
    if lifecycle.get("repeat"):
        return RepeatVerdict(
            False,
            "lifecycle",
            str(lifecycle.get("reason") or "lifecycle_repeat"),
            matched_by=matched_by,
            previous_fingerprint=previous_fp,
            previous_title=previous_title,
            previous_published_day=previous_day,
        )
    reason = str(lifecycle.get("reason") or "")
    if reason.startswith("publishable_anchor:") or lifecycle.get("changed_fact"):
        return RepeatVerdict(
            True,
            "lifecycle",
            reason or "real_lifecycle_change",
            matched_by=matched_by,
            previous_fingerprint=previous_fp,
            previous_title=previous_title,
            previous_published_day=previous_day,
        )

    return RepeatVerdict(
        False,
        "same_fingerprint",
        "exact_fingerprint_already_published",
        matched_by=matched_by,
        previous_fingerprint=previous_fp,
        previous_title=previous_title,
        previous_published_day=previous_day,
    )
