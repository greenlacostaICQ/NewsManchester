from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from news_digest.pipeline.common import now_london
from news_digest.pipeline.editorial_contracts import (
    build_editorial_contract,
    calendar_repeat_review,
    lifecycle_repeat_review,
)
from news_digest.pipeline.weekend_inventory import weekend_occurrence_date


OPERATIONAL_REPEAT_BLOCKS = frozenset({"weather", "transport"})
TICKET_REPEAT_BLOCKS = frozenset({"ticket_radar", "outside_gm_tickets"})
EVENT_REPEAT_BLOCKS = frozenset({
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "russian_events",
    "professional_events",
})
EVENT_REPEAT_CATEGORIES = frozenset({
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "diaspora_events",
    "professional_events",
})


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

    Food/opening articles are intentionally excluded. A restaurant launch is a
    one-shot story; if it is actually a dated market/fair, routing should move it
    into an event block before repeat policy sees it.
    """
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block == "openings" or category == "food_openings":
        return False
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
        anchor in {"dated_event", "recurring_occurrence", "bookable_listing"}
        or event_shape in {"recurring", "festival", "one_off", "event_like", "bookable_activity"}
    ):
        event_day = _event_day(candidate)
        if event_day is None or event_day >= now_london().date():
            return RepeatVerdict(True, "event", f"event_anchor:{anchor or event_shape}")

    return RepeatVerdict(False, "same_fingerprint", f"anchor_not_repeatable:{anchor or 'none'}")


def visible_repeat_verdict(candidate: dict[str, Any], previous: dict[str, Any] | None) -> RepeatVerdict:
    if not previous:
        return RepeatVerdict(True, "new", "no_previous_match")

    previous_fp = str(previous.get("fingerprint") or "")
    previous_title = str(previous.get("title") or "")
    previous_day = _previous_day(previous)
    matched_by = "fingerprint" if previous_fp and previous_fp == str(candidate.get("fingerprint") or "") else "history"

    # The only global repeat exception: a canonical, still-valid A-tier event.
    # Invalid owners, duplicates, cancelled/expired rows fail the A-tier policy
    # before reaching this override and continue through ordinary repeat rules.
    from news_digest.pipeline.ticket_notability import a_tier_ticket_policy  # noqa: PLC0415

    a_tier_allow, _ = a_tier_ticket_policy(candidate)
    if a_tier_allow:
        return RepeatVerdict(
            True,
            "a_tier",
            "a_tier_must_show_override",
            matched_by=matched_by,
            previous_fingerprint=previous_fp,
            previous_title=previous_title,
            previous_published_day=previous_day,
        )

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
    if matched_by == "fingerprint" and (block == "openings" or category == "food_openings"):
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
        return RepeatVerdict(
            True,
            "lifecycle",
            "concrete_story_change",
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
