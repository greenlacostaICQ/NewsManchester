from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
    if str(candidate.get("primary_block") or "") == "weekend_activities":
        occurrence = weekend_occurrence_date(candidate)
        if occurrence is not None:
            return occurrence
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("date_start") or event.get("date") or "").strip()[:10]
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _previous_day(previous: dict[str, Any]) -> str:
    return str(
        previous.get("last_published_day_london")
        or previous.get("first_published_day_london")
        or previous.get("published_day_london")
        or ""
    )


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
    if text_date is not None and now_london().date() <= text_date <= now_london().date() + timedelta(days=14):
        return RepeatVerdict(False, "calendar", "calendar_review_not_applicable")

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
