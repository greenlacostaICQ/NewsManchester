"""Protected Weekend Inventory rules.

This module answers one product question shared by rewrite, repeat policy and
writer caps: is this a real "what can I do this weekend" inventory item?
It is deliberately narrower than generic what's-on / ticket listings.
"""
from __future__ import annotations

import calendar
from datetime import date, timedelta
import re

from news_digest.pipeline.common import now_london
from news_digest.pipeline.event_extraction import event_end_date, event_start_date


_ORDINALS = {
    "first": 1,
    "1st": 1,
    "second": 2,
    "2nd": 2,
    "third": 3,
    "3rd": 3,
    "fourth": 4,
    "4th": 4,
    "last": -1,
}
_WEEKDAYS = {
    "friday": 4,
    "fridays": 4,
    "saturday": 5,
    "saturdays": 5,
    "sunday": 6,
    "sundays": 6,
}

_INVENTORY_TYPE_RE = re.compile(
    r"\b(?:"
    r"car\s*boot|boot\s*sale|makers?\s+market|artisan\s+market|farmers?\s+market|"
    r"food\s+market|flea\s+market|vintage\s+(?:sale|market)|market|fair|fayre|"
    r"festival|food\s+festival|drink\s+festival|beer\s+festival|rum\s+festival|"
    r"comic\s+con(?:vention)?|"
    r"pride|community\s+(?:festival|day)|family\s+day|heritage|medieval|"
    r"re-?enact(?:ment)?|beauty\s+brunch|protest(?:\s+music)?|"
    r"workshops?|public\s+trail|themed\s+trail|museum\s+after[-\s]?hours"
    r")\b",
    re.IGNORECASE,
)

_ORDINARY_AFISHA_RE = re.compile(
    r"\b(?:ticketmaster|arena\s+show|standalone\s+(?:concert|gig)|"
    r"comedy\s+club|nightclub|club\s+night|dj\s+set)\b",
    re.IGNORECASE,
)

_SELLER_ADMIN_RE = re.compile(
    r"\b(?:"
    r"you\s+can\s+sell|casual\s+trading|become\s+a\s+(?:regular\s+)?trader|"
    r"apply\s+for\s+(?:a\s+)?stall|book\s+(?:a\s+)?pitch|seller\s+information|"
    r"trader\s+(?:information|application|licen[cs]e)|stallholder\s+(?:information|application)"
    r")\b",
    re.IGNORECASE,
)

_WEEKLY_RE = re.compile(
    r"(?:"
    r"dates?:\s*(?:from\s+|every\s+)?(fridays?|saturdays?|sundays?)(?:\s+and\s+bank\s+holiday\s+mondays?)?|"
    r"(?:every|each|all|most|weekly)\s+(fridays?|saturdays?|sundays?)|"
    r"(fridays?|saturdays?|sundays?)\s+(?:weekly|every\s+week)|"
    r"runs?\s+(?:on\s+)?(fridays?|saturdays?|sundays?)|"
    r"(?:open\s+(?:hours?\s+)?(?:on\s+)?|opening\s+hours?\s+(?:on\s+)?)(fridays|saturdays|sundays)"
    r")",
    re.IGNORECASE,
)
_MONTHLY_RE = re.compile(
    r"\b(first|1st|second|2nd|third|3rd|fourth|4th|last)\s+"
    r"(fridays?|saturdays?|sundays?)\s+(?:of\s+)?(?:each|every|the)?\s*month\b",
    re.IGNORECASE,
)
_THIS_WEEKEND_RE = re.compile(r"\b(?:this\s+weekend|bank\s+holiday\s+weekend)\b", re.IGNORECASE)


def _blob(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    return " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            candidate.get("source_url"),
            event.get("event_name"),
            event.get("venue"),
            event.get("date_text"),
        )
    )


def _activity_blob(candidate: dict) -> str:
    """Short identity text only; long evidence contains incidental type words."""
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    return " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            str(candidate.get("summary") or "")[:500],
            str(candidate.get("lead") or "")[:300],
            event.get("event_name"),
            event.get("date_text"),
        )
    )


def _is_late_may_bank_holiday(day: date) -> bool:
    return day.month == 5 and day.weekday() == 0 and day + timedelta(days=7) > date(day.year, 5, 31)


def current_weekend_window(*, today: date | None = None) -> tuple[date, date]:
    today = today or now_london().date()
    friday = today + timedelta(days=(4 - today.weekday()) % 7)
    start = today if today.weekday() in {5, 6} or _is_late_may_bank_holiday(today) else friday
    sunday = today + timedelta(days=(6 - today.weekday()) % 7)
    bank_monday = sunday + timedelta(days=1)
    end = bank_monday if _is_late_may_bank_holiday(bank_monday) else sunday
    return start, end


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date | None:
    if ordinal == -1:
        day = date(year, month, calendar.monthrange(year, month)[1])
        while day.weekday() != weekday:
            day -= timedelta(days=1)
        return day
    count = 0
    day = date(year, month, 1)
    while day.month == month:
        if day.weekday() == weekday:
            count += 1
            if count == ordinal:
                return day
        day += timedelta(days=1)
    return None


def recurring_occurrence_date(text: str, *, today: date | None = None) -> date | None:
    today = today or now_london().date()
    weekly = _WEEKLY_RE.search(text)
    if weekly:
        weekday_text = next((group for group in weekly.groups() if group), "")
        weekday = _WEEKDAYS.get(weekday_text.lower())
        if weekday is not None:
            return today + timedelta(days=(weekday - today.weekday()) % 7)

    monthly = _MONTHLY_RE.search(text)
    if monthly:
        ordinal = _ORDINALS[monthly.group(1).lower()]
        weekday = _WEEKDAYS[monthly.group(2).lower()]
        for delta_months in (0, 1):
            month = today.month + delta_months
            year = today.year + (month - 1) // 12
            month = ((month - 1) % 12) + 1
            occurrence = _nth_weekday(year, month, weekday, ordinal)
            if occurrence and occurrence >= today:
                return occurrence
    return None


def _iso_day(value: object) -> date | None:
    raw = str(value or "").strip()[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def effective_occurrence_window(
    value: dict | None,
    *,
    today: date | None = None,
) -> tuple[date | None, date | None]:
    """Return the one effective date window for a fact card or candidate.

    Candidate callers are normalised here so intake, routing, repeat, writer
    and protection cannot disagree about a recurring occurrence. Inventory
    callers may continue to pass the structured fact card directly.
    """
    value = value if isinstance(value, dict) else {}
    is_candidate = isinstance(value.get("event"), dict) or any(
        key in value for key in ("primary_block", "category", "source_url", "draft_line")
    )
    if is_candidate:
        candidate = value
        fact = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        occurrence = candidate_recurring_occurrence_date(candidate, today=today)
        if occurrence is not None:
            fact["is_recurring"] = True
            fact["next_occurrence"] = occurrence.isoformat()
            candidate["event"] = fact
    else:
        fact = value
    original_start = _iso_day(fact.get("date_start") or fact.get("date"))
    original_end = _iso_day(fact.get("date_end"))
    next_occurrence = _iso_day(fact.get("next_occurrence"))
    if next_occurrence:
        duration = timedelta(0)
        if original_start and original_end:
            candidate_duration = original_end - original_start
            if timedelta(0) <= candidate_duration <= timedelta(days=7):
                duration = candidate_duration
        return next_occurrence, next_occurrence + duration
    return original_start, original_end or original_start


def candidate_recurring_occurrence_date(candidate: dict, *, today: date | None = None) -> date | None:
    return recurring_occurrence_date(_blob(candidate), today=today)


def _contract_occurrence_date(candidate: dict) -> date | None:
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    occurrence = contract.get("occurrence") if isinstance(contract.get("occurrence"), dict) else {}
    raw = str(occurrence.get("date") or "").strip()[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def weekend_occurrence_date(candidate: dict, *, today: date | None = None) -> date | None:
    """Return the effective date for current-weekend inventory.

    Recurring market/car-boot pages often expose a stale schema.org startDate
    while the visible copy says "Sundays" or "every Saturday". The computed
    occurrence is the public planning truth for this weekend; the stale
    structured date remains evidence only.
    """
    today = today or now_london().date()
    start, end = current_weekend_window(today=today)
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    effective_start, effective_end = effective_occurrence_window(candidate, today=today)
    if effective_start and effective_end and effective_start <= end and effective_end >= start:
        return max(effective_start, start)
    rec_date = candidate_recurring_occurrence_date(candidate, today=today)
    if rec_date and start <= rec_date <= end:
        return rec_date
    event_start = event_start_date(candidate)
    if event_start:
        event_end = event_end_date(candidate) or event_start
        if event_start <= end and event_end >= start:
            return max(event_start, start)
    confidence = str(event.get("date_confidence") or "")
    contract_day = _contract_occurrence_date(candidate)
    if (not event_start or confidence in {"", "none", "low"}) and contract_day and start <= contract_day <= end:
        return contract_day
    return None


def has_current_weekend_occurrence(candidate: dict, *, today: date | None = None) -> bool:
    today = today or now_london().date()
    if weekend_occurrence_date(candidate, today=today):
        return True
    if event_start_date(candidate):
        return False
    return bool(_THIS_WEEKEND_RE.search(_blob(candidate)))


def weekend_activity_type(candidate: dict | None) -> str:
    """Return the concrete protected-inventory activity family, if any."""
    if not isinstance(candidate, dict):
        return ""
    text = _activity_blob(candidate)
    match = _INVENTORY_TYPE_RE.search(text)
    if not match or _SELLER_ADMIN_RE.search(text):
        return ""
    if _ORDINARY_AFISHA_RE.search(text) and not re.search(r"\bfestival|fair|market|pride|heritage\b", text, re.IGNORECASE):
        return ""
    return re.sub(r"\s+", "_", match.group(0).strip().lower())


def is_weekend_inventory_candidate(candidate: dict | None, *, today: date | None = None) -> bool:
    if not isinstance(candidate, dict):
        return False
    if str(candidate.get("primary_block") or "") != "weekend_activities":
        return False
    if not weekend_activity_type(candidate):
        return False
    return has_current_weekend_occurrence(candidate, today=today)
