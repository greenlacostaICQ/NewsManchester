"""I3 — Structured event extraction.

For event-related candidates (culture_weekly, venues_tickets,
russian_speaking_events, plus food_openings markets) we pull a
small structured ``event`` dict out of the free-text fields the
collector produced:

    {
        "schema_version": 1,
        "event_name": str,    # cleaned title without source/venue prefix
        "venue": str,         # entities.venues[0] or regex-matched
        "date": str,          # ISO YYYY-MM-DD if we could pin a date
        "date_text": str,     # human-readable as found ("16-17 мая", "Fri 1 May")
        "borough": str,       # entities.boroughs[0]
        "price": str,         # "£15", "£15-£75", "from £49.75", "free", or ""
        "booking_url": str,   # source_url for ticket sources, ticket-host URL otherwise
        "is_event": bool,     # True only when we have name + date + venue/url
    }

Pure deterministic — regex + entities lookup, no LLM. Runs AFTER
entity_extraction so it can reuse boroughs/districts/venues. Stored
on the candidate as ``candidate["event"]`` and forwarded to
``llm_rewrite`` payload and ``published_facts.json``.

Why this stage exists (Q5/I3 in the backlog):
    Without structured fields the only thing the rest of the pipeline
    sees is free prose in ``summary``/``evidence_text``. That makes it
    impossible to filter "events without a date" cheaply, to dedup
    "same event covered by two listings" by ``(venue, date)``, or to
    enforce price/booking-URL completeness in Q5 Event Quality Gate.
"""
from __future__ import annotations

import re
import calendar
from datetime import date as date_cls, datetime, timedelta
from typing import NamedTuple
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Recurring-event date extraction helpers (E1)
# ---------------------------------------------------------------------------
_ORDINAL_MAP = {"first": 1, "1st": 1, "second": 2, "2nd": 2,
                "third": 3, "3rd": 3, "fourth": 4, "4th": 4, "last": -1}
_WEEKDAY_MAP = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                "friday": 4, "saturday": 5, "sunday": 6}
_RUSSIAN_WEEKDAY_ACCUS = ["понедельник", "вторник", "среду", "четверг",
                          "пятницу", "субботу", "воскресенье"]
_RUSSIAN_MONTHS = ["января", "февраля", "марта", "апреля", "мая", "июня",
                   "июля", "августа", "сентября", "октября", "ноября", "декабря"]

_RECURRING_PATTERN = re.compile(
    r'\b(first|1st|second|2nd|third|3rd|fourth|4th|last)\s+'
    r'(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+'
    r'(?:of\s+)?(?:each|every)\s+month\b',
    re.IGNORECASE,
)
_WEEKLY_RECURRING_PATTERN = re.compile(
    r"\b(?:"
    r"(?:every|each|all|most|weekly)\s+"
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?|"
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?\s+"
    r"(?:weekly|every\s+week)|"
    r"runs?\s+(?:on\s+)?"
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?|"
    r"open(?:ing)?\s+(?:hours?\s+)?(?:on\s+)?"
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?"
    r")\b",
    re.IGNORECASE,
)

def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date_cls | None:
    if n == -1:
        last_day = calendar.monthrange(year, month)[1]
        d = date_cls(year, month, last_day)
        while d.weekday() != weekday:
            d -= timedelta(days=1)
        return d
    count = 0
    d = date_cls(year, month, 1)
    while d.month == month:
        if d.weekday() == weekday:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)
    return None

def _calculate_recurring_date(text: str, from_date: date_cls) -> tuple[date_cls, str] | None:
    m = _RECURRING_PATTERN.search(text)
    if m:
        ordinal = _ORDINAL_MAP[m.group(1).lower()]
        weekday = _WEEKDAY_MAP[m.group(2).lower()]

        for delta_months in (0, 1):
            month = from_date.month + delta_months
            year = from_date.year + (month - 1) // 12
            month = ((month - 1) % 12) + 1
            occurrence = _nth_weekday_of_month(year, month, weekday, ordinal)
            if occurrence and occurrence >= from_date:
                day_name = _RUSSIAN_WEEKDAY_ACCUS[occurrence.weekday()]
                month_name = _RUSSIAN_MONTHS[occurrence.month - 1]
                date_text = f"{day_name}, {occurrence.day} {month_name} {occurrence.year}"
                return occurrence, date_text

    weekly = _WEEKLY_RECURRING_PATTERN.search(text)
    if weekly:
        weekday_text = next((str(group or "").lower() for group in weekly.groups() if group), "")
        weekday = _WEEKDAY_MAP.get(weekday_text)
        if weekday is not None:
            occurrence = from_date + timedelta(days=(weekday - from_date.weekday()) % 7)
            day_name = _RUSSIAN_WEEKDAY_ACCUS[occurrence.weekday()]
            month_name = _RUSSIAN_MONTHS[occurrence.month - 1]
            date_text = f"{day_name}, {occurrence.day} {month_name} {occurrence.year}"
            return occurrence, date_text
    return None

EVENT_SCHEMA_VERSION = 1

# How far ahead a "this weekend / next 7 days" block may reasonably reach.
# A date beyond this is far-future and must not be presented as imminent
# (e.g. a "21 May 2027" festival must never land in the Weekend block).
NEAR_HORIZON_DAYS = 120

_LONDON_TZ = ZoneInfo("Europe/London")


# ── Which candidates to extract for ───────────────────────────────────────

_EVENT_CATEGORIES: frozenset[str] = frozenset({
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "diaspora_events",
    "professional_events",
})

# Categories whose main job isn't events but whose weekend / openings
# blocks carry venue+date items that benefit from structuring.
_OPTIONAL_EVENT_CATEGORIES: frozenset[str] = frozenset({
    "food_openings",
})

# When category is "optional", only treat the candidate as an event if
# its primary_block is one of these.
_OPTIONAL_EVENT_BLOCKS: frozenset[str] = frozenset({
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "russian_events",
    "openings",
    "professional_events",
})


def is_event_candidate(candidate: dict) -> bool:
    """Should we try to pull a structured event out of this candidate?"""
    category = str(candidate.get("category") or "")
    if category in _EVENT_CATEGORIES:
        return True
    block = str(candidate.get("primary_block") or "")
    if block in _OPTIONAL_EVENT_BLOCKS:
        return True
    if category in _OPTIONAL_EVENT_CATEGORIES:
        return block in _OPTIONAL_EVENT_BLOCKS
    return False


# ── Source text blob ──────────────────────────────────────────────────────

_TEXT_FIELDS = ("title", "summary", "lead", "practical_angle", "evidence_text")


def _candidate_blob(candidate: dict) -> str:
    parts = [str(candidate.get(field) or "") for field in _TEXT_FIELDS]
    return " \n ".join(p for p in parts if p)


# ── Date parsing ──────────────────────────────────────────────────────────

# Russian month forms (nominative/genitive — events most often use genitive
# "16 мая", but RSS feeds occasionally produce "май 2026").
_RU_MONTHS: dict[str, int] = {
    "января": 1, "январь": 1, "янв": 1,
    "февраля": 2, "февраль": 2, "фев": 2,
    "марта": 3, "март": 3, "мар": 3,
    "апреля": 4, "апрель": 4, "апр": 4,
    "мая": 5, "май": 5,
    "июня": 6, "июнь": 6, "июн": 6,
    "июля": 7, "июль": 7, "июл": 7,
    "августа": 8, "август": 8, "авг": 8,
    "сентября": 9, "сентябрь": 9, "сен": 9, "сент": 9,
    "октября": 10, "октябрь": 10, "окт": 10,
    "ноября": 11, "ноябрь": 11, "ноя": 11,
    "декабря": 12, "декабрь": 12, "дек": 12,
}

_EN_MONTHS: dict[str, int] = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# ISO date: 2026-05-16
_ISO_DATE_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")
_UK_SLASH_DATE_RE = re.compile(r"\b(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>20\d{2})\b")

# English: "16 May 2026", "May 16, 2026", "16 May", "May 16",
# and listing format "Sun 28th Jun, 2026" (ordinal suffix + comma before year,
# weekday prefix ignored). Manchester's Finest / Skiddle put the date there.
_EN_DAY_MONTH_YEAR_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month>"
    + "|".join(_EN_MONTHS.keys())
    + r")(?:,?\s+(?P<year>20\d{2}))?\b",
    re.IGNORECASE,
)
_EN_MONTH_DAY_YEAR_RE = re.compile(
    r"\b(?P<month>"
    + "|".join(_EN_MONTHS.keys())
    + r")\s+(?P<day>\d{1,2})(?:[,\s]+(?P<year>20\d{2}))?\b",
    re.IGNORECASE,
)

# Russian: "16 мая 2026", "16 мая"
_RU_DAY_MONTH_YEAR_RE = re.compile(
    r"\b(?P<day>\d{1,2})\s+(?P<month>"
    + "|".join(_RU_MONTHS.keys())
    + r")(?:\s+(?P<year>20\d{2}))?\b",
    re.IGNORECASE,
)

# Day range: "16-17 May", "16–17 мая", "Fri 1 May - Tue 19 May"
_DAY_RANGE_EN_RE = re.compile(
    r"\b(?P<start>\d{1,2})\s*[-–—]\s*(?P<end>\d{1,2})\s+(?P<month>"
    + "|".join(_EN_MONTHS.keys())
    + r")(?:\s+(?P<year>20\d{2}))?\b",
    re.IGNORECASE,
)
_DAY_RANGE_RU_RE = re.compile(
    r"\b(?P<start>\d{1,2})\s*[-–—]\s*(?P<end>\d{1,2})\s+(?P<month>"
    + "|".join(_RU_MONTHS.keys())
    + r")(?:\s+(?P<year>20\d{2}))?\b",
    re.IGNORECASE,
)

# Cross-month day range: "27 June – 5 July", "30 December – 2 January" (year
# wrap). The same-month range above only covers "16–17 May"; multi-day
# festivals that span a month boundary (Didsbury Arts 27 Jun – 5 Jul) need this.
_CROSS_MONTH_RANGE_EN_RE = re.compile(
    r"\b(?P<sday>\d{1,2})(?:st|nd|rd|th)?\s+(?P<smonth>" + "|".join(_EN_MONTHS.keys()) + r")"
    r"(?:,?\s+(?P<syear>20\d{2}))?\s*[-–—]\s*(?:(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\s+)?"
    r"(?P<eday>\d{1,2})(?:st|nd|rd|th)?\s+(?P<emonth>" + "|".join(_EN_MONTHS.keys()) + r")"
    r"(?:,?\s+(?P<eyear>20\d{2}))?\b",
    re.IGNORECASE,
)
_CROSS_MONTH_RANGE_RU_RE = re.compile(
    r"\b(?P<sday>\d{1,2})\s+(?P<smonth>" + "|".join(_RU_MONTHS.keys()) + r")"
    r"(?:\s+(?P<syear>20\d{2}))?\s*[-–—]\s*"
    r"(?P<eday>\d{1,2})\s+(?P<emonth>" + "|".join(_RU_MONTHS.keys()) + r")"
    r"(?:\s+(?P<eyear>20\d{2}))?\b",
    re.IGNORECASE,
)


def _today_london() -> date_cls:
    return datetime.now(tz=_LONDON_TZ).date()


def _resolve_year(month: int, year_hint: str | None, today: date_cls | None = None) -> int:
    """If year is missing, pick "this year unless that month has already
    passed and would imply the event is in the past — then next year".

    Avoids the obvious bug of treating "16 May" in October as a stale
    May event from this year when it almost certainly means May next year.
    """
    if year_hint:
        try:
            return int(year_hint)
        except ValueError:
            pass
    today = today or _today_london()
    if month >= today.month:
        return today.year
    return today.year + 1


def _safe_date(year: int, month: int, day: int) -> str:
    try:
        return date_cls(year, month, day).isoformat()
    except (ValueError, TypeError):
        return ""


class DateParse(NamedTuple):
    start: str        # ISO start date, "" if none
    end: str          # ISO end date for ranges, "" otherwise
    text: str         # human-readable as found
    confidence: str   # "high" | "medium" | "low" | "none"


def _bare_month_confidence(month: int, year_hint: str | None, today: date_cls) -> str:
    """Confidence for a single day+month with no inline year.

    high   — the year was written out ("16 May 2026").
    medium — no year, but the month is still ahead this year ("3 July" in June):
             almost certainly the real upcoming event.
    low    — no year and the month already passed, so we rolled it to *next*
             year ("May 2" seen in late June → 2027). That rollover is the
             classic false positive: a stray "May 2" in body copy becomes a
             phantom far-future event. Mark it low so gates can hold-for-enrich
             instead of publishing a wrong date.
    """
    if year_hint:
        return "high"
    return "medium" if month >= today.month else "low"


def _parse_date_details(blob: str, *, today: date_cls | None = None) -> DateParse:
    """Structured date parse: start, end (ranges), human text, confidence.

    Tries in order:
      1. Day-range English ("16-17 May")            — most informative
      2. Day-range Russian ("16-17 мая")
      3. ISO ("2026-05-16")
      4. UK slash date ("12/06/2026")
      5. English "16 May [2026]"
      6. English "May 16[, 2026]"
      7. Russian "16 мая [2026]"

    For ranges, ``start`` is the first day (a single sortable value for
    dedupe / "is in the future?" gates) and ``end`` is the last day, so a
    multi-day festival reads as one continuous occurrence rather than a
    repeat. Ranges and fully-written ISO/year dates are high confidence.
    """
    today = today or _today_london()

    for cross_re, months in ((_CROSS_MONTH_RANGE_EN_RE, _EN_MONTHS), (_CROSS_MONTH_RANGE_RU_RE, _RU_MONTHS)):
        if m := cross_re.search(blob):
            smonth = months[m.group("smonth").lower()]
            emonth = months[m.group("emonth").lower()]
            syear = _resolve_year(smonth, m.group("syear"), today)
            # End month before start month means the range wraps the new year.
            eyear = int(m.group("eyear")) if m.group("eyear") else (syear + 1 if emonth < smonth else syear)
            start_iso = _safe_date(syear, smonth, int(m.group("sday")))
            end_iso = _safe_date(eyear, emonth, int(m.group("eday")))
            if start_iso and end_iso:
                year_suffix = f" {m.group('eyear')}" if m.group("eyear") else ""
                text = f"{m.group('sday')} {m.group('smonth')} – {m.group('eday')} {m.group('emonth')}{year_suffix}".strip()
                return DateParse(start_iso, end_iso, text, "high")

    if m := _DAY_RANGE_EN_RE.search(blob):
        start = int(m.group("start"))
        end = int(m.group("end"))
        month = _EN_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, start)
        if iso:
            text = f"{start}–{end} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()
            return DateParse(iso, _safe_date(year, month, end), text, "high")

    if m := _DAY_RANGE_RU_RE.search(blob):
        start = int(m.group("start"))
        end = int(m.group("end"))
        month = _RU_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, start)
        if iso:
            text = f"{start}–{end} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()
            return DateParse(iso, _safe_date(year, month, end), text, "high")

    if m := _ISO_DATE_RE.search(blob):
        iso = _safe_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if iso:
            return DateParse(iso, "", iso, "high")

    if m := _UK_SLASH_DATE_RE.search(blob):
        iso = _safe_date(int(m.group("year")), int(m.group("month")), int(m.group("day")))
        if iso:
            return DateParse(iso, "", iso, "high")

    if m := _EN_DAY_MONTH_YEAR_RE.search(blob):
        day = int(m.group("day"))
        month = _EN_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, day)
        if iso:
            text = f"{day} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()
            return DateParse(iso, "", text, _bare_month_confidence(month, m.group("year"), today))

    if m := _EN_MONTH_DAY_YEAR_RE.search(blob):
        day = int(m.group("day"))
        month = _EN_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, day)
        if iso:
            text = f"{m.group('month')} {day}{(', ' + m.group('year')) if m.group('year') else ''}".strip()
            return DateParse(iso, "", text, _bare_month_confidence(month, m.group("year"), today))

    if m := _RU_DAY_MONTH_YEAR_RE.search(blob):
        day = int(m.group("day"))
        month = _RU_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, day)
        if iso:
            text = f"{day} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()
            return DateParse(iso, "", text, _bare_month_confidence(month, m.group("year"), today))

    return DateParse("", "", "", "none")


def _parse_date_from_blob(blob: str, *, today: date_cls | None = None) -> tuple[str, str]:
    """Back-compat 2-tuple wrapper: (iso_start, date_text)."""
    parsed = _parse_date_details(blob, today=today)
    return parsed.start, parsed.text


# ── Price extraction ──────────────────────────────────────────────────────

_PRICE_RANGE_RE = re.compile(
    r"£\s*(?P<low>\d{1,4}(?:\.\d{1,2})?)\s*[-–—]\s*£?\s*(?P<high>\d{1,4}(?:\.\d{1,2})?)\b"
)
_PRICE_FROM_RE = re.compile(
    r"\b(?:from|от)\s+£\s*(?P<amount>\d{1,4}(?:\.\d{1,2})?)\b",
    re.IGNORECASE,
)
_PRICE_SINGLE_RE = re.compile(r"£\s*(?P<amount>\d{1,4}(?:\.\d{1,2})?)\b")
_FREE_RE = re.compile(
    r"\b(?:free entry|free admission|free of charge|free|"
    r"бесплатн[ыа]й вход|бесплатно|вход свободный|вход бесплатный)\b",
    re.IGNORECASE,
)


def _format_price_number(raw: str) -> str:
    """£15.00 → £15; £15.50 → £15.50."""
    try:
        value = float(raw)
    except ValueError:
        return f"£{raw}"
    if value == int(value):
        return f"£{int(value)}"
    return f"£{value:.2f}"


def _extract_price(blob: str) -> str:
    if m := _PRICE_RANGE_RE.search(blob):
        return f"{_format_price_number(m.group('low'))}–{_format_price_number(m.group('high'))[1:]}"
    if m := _PRICE_FROM_RE.search(blob):
        prefix = "от " if "от" in m.group(0).lower() else "from "
        return f"{prefix}{_format_price_number(m.group('amount'))}"
    if m := _PRICE_SINGLE_RE.search(blob):
        context = str(blob or "")[max(0, m.start() - 40): m.end() + 40].lower()
        if re.search(r"\b(?:booking|transaction|admin|handling|service)\s+fee\b", context):
            return ""
        return _format_price_number(m.group("amount"))
    if _FREE_RE.search(blob):
        return "free"
    return ""


# ── Booking URL extraction ────────────────────────────────────────────────

_BOOKING_HOSTS: tuple[str, ...] = (
    "eventbrite.co.uk", "eventbrite.com",
    "ticketmaster.co.uk", "ticketmaster.com",
    "dice.fm",
    "seetickets.com",
    "skiddle.com",
    "designmynight.com",
    "tickets.foodfestivaltickets.com",
    "ticketline.co.uk",
    "kontramarka.uk",
    "eventfirst.co.uk",
    "songkick.com",
    "bandsintown.com",
)

# Categories whose source IS the ticket page — pass source_url through
# rather than searching evidence_text.
_TICKET_NATIVE_CATEGORIES: frozenset[str] = frozenset({
    "venues_tickets",
    "russian_speaking_events",
    "diaspora_events",
})

_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


def _url_host(url: str) -> str:
    match = re.match(r"https?://([^/]+)", url, flags=re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).lower().lstrip("www.")


def _is_booking_url(url: str) -> bool:
    host = _url_host(url)
    return any(host == h or host.endswith("." + h) for h in _BOOKING_HOSTS)


def _extract_booking_url(candidate: dict) -> str:
    category = str(candidate.get("category") or "")
    source_url = str(candidate.get("source_url") or "")
    if category in _TICKET_NATIVE_CATEGORIES and source_url:
        return source_url
    if _is_booking_url(source_url):
        return source_url
    blob = " ".join(str(candidate.get(f) or "") for f in ("evidence_text", "summary", "lead"))
    for match in _URL_RE.finditer(blob):
        url = match.group(0).rstrip(".,);")
        if _is_booking_url(url):
            return url
    return ""


# ── Venue resolution ──────────────────────────────────────────────────────

# Fallback regex for "at <Venue>" / "in <Venue>" / "в <Площадка>" /
# "на <Площадка>" — only used when entity_extraction didn't find one,
# and only as a heuristic hint.
_AT_VENUE_RE = re.compile(
    r"\b(?:at|in)\s+(?P<name>[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){0,3})\b"
)
# A single capitalised word after "at/in" is only a venue if it reads like a
# place. Without this, "...Technology in Practice" yields venue="Practice".
_VENUE_KEYWORD_RE = re.compile(
    r"\b(?:hall|centre|center|theatre|theater|arena|museum|gallery|library|"
    r"stadium|club|bar|venue|hotel|park|church|cathedral|warehouse|studios?|"
    r"rooms?|institute|academy|university|college|cinema|chapel|exchange|"
    r"factory|works|mill|square|gardens?|house|live)\b",
    re.IGNORECASE,
)
_LABELLED_LOCATION_RE = re.compile(
    r"\b(?:Location|Venue)\s*:\s*(?P<name>[^.;\n\r]+)",
    re.IGNORECASE,
)


def _extract_venue(candidate: dict, entities: dict) -> str:
    venues = entities.get("venues") if isinstance(entities, dict) else None
    if venues and isinstance(venues, list) and venues:
        return str(venues[0])
    # Heuristic fallback — looks for "at <Name>" or "in <Name>" with
    # capital-cased multiword name. Keeps Title-Case-only matches to
    # avoid false-positives on common nouns like "at home".
    blob = " ".join(str(candidate.get(f) or "") for f in ("title", "summary", "lead"))
    if m := _LABELLED_LOCATION_RE.search(blob):
        name = m.group("name").strip()
        parts = [part.strip() for part in name.split(",") if part.strip()]
        if parts:
            return ", ".join(parts[:2])
    if m := _AT_VENUE_RE.search(blob):
        name = m.group("name").strip()
        if name.lower() in {"home", "manchester", "london", "the uk", "the future"}:
            return ""
        # Accept only multi-word names or names that read like a venue, so a
        # stray "in Practice" / "at Scale" doesn't become a phantom venue.
        if " " in name or _VENUE_KEYWORD_RE.search(name):
            return name
    return ""


# ── Event-name cleanup ────────────────────────────────────────────────────

# Trailing " - Source Label", " | Source Label", " — Source Label"
_TRAILING_SOURCE_RE = re.compile(r"\s*[\-–—|]\s*[A-Z][\w'\s&.]+$")
# Trailing parenthesised certificate / age rating: "(15)", "(PG)", "(18)"
_AGE_CERT_RE = re.compile(r"\s*\((?:U|PG|12A?|15|18|TBC)\)\s*$", re.IGNORECASE)
# Leading "Source: " or "Source - "
_LEADING_SOURCE_RE = re.compile(r"^(?:[A-Z][\w']+\s*[:\-—–]\s*)")


def _extract_event_name(candidate: dict, entities: dict, venue: str) -> str:
    title = str(candidate.get("title") or "").strip()
    if not title:
        return ""
    # Strip trailing "- The Lowry" / "| BBC Manchester" style suffix only
    # if the suffix looks like a source label rather than part of the name.
    source_label = str(candidate.get("source_label") or "").strip()
    if source_label and title != source_label and title.endswith(source_label):
        title = title[: -len(source_label)].rstrip(" -–—|:")
    # Strip "Source Label - " prefix similarly.
    if source_label and title.startswith(source_label + ":"):
        title = title[len(source_label) + 1 :].strip()
    # Strip trailing venue if it duplicates information already in `venue`.
    if venue and title.lower().endswith(venue.lower()):
        # Only strip when there's a separator before the venue suffix,
        # so we don't damage names like "HOME" (whose own name == venue).
        for sep in (" - ", " — ", " – ", " | ", " at "):
            tail = sep + venue
            if title.lower().endswith(tail.lower()):
                title = title[: -len(tail)].rstrip()
                break
    return title.strip()


# ── Public API ────────────────────────────────────────────────────────────


def extract_event(candidate: dict, entities: dict | None = None) -> dict:
    """Return the structured event payload for one candidate.

    Empty dict if the candidate is not in an event category — callers
    should check ``"event" in candidate`` rather than relying on truthy
    sub-fields.
    """
    if not isinstance(candidate, dict):
        return {}
    if not is_event_candidate(candidate):
        return {}

    if entities is None:
        entities = candidate.get("entities") or {}
    if not isinstance(entities, dict):
        entities = {}

    hint = candidate.get("structured_event_hint") if isinstance(candidate.get("structured_event_hint"), dict) else {}
    blob = _candidate_blob(candidate)
    venue = str(hint.get("venue") or "").strip() or _extract_venue(candidate, entities)
    parsed = _parse_date_details(blob)
    iso_date, iso_end, date_text, date_confidence = parsed.start, parsed.end, parsed.text, parsed.confidence
    recurring_date_used = False
    if not iso_date:
        today = _today_london()
        rec_res = _calculate_recurring_date(blob, today)
        if rec_res:
            rec_date, rec_text = rec_res
            iso_date = rec_date.isoformat()
            date_text = rec_text
            date_confidence = "high"
            recurring_date_used = True
    hint_date = str(hint.get("date_start") or hint.get("date") or "").strip()
    if hint_date:
        hint_parsed = _parse_date_details(hint_date)
        parsed_hint = hint_parsed.start or hint_date[:10]
        if parsed_hint:
            # A structured source (JSON-LD / API) is authoritative: trust it.
            iso_date = parsed_hint
            iso_end = str(hint.get("date_end") or "").strip()[:10] or hint_parsed.end
            date_text = str(hint.get("date_text") or "").strip() or date_text or parsed_hint
            date_confidence = "high"
    boroughs = entities.get("boroughs") if isinstance(entities.get("boroughs"), list) else []
    borough = str(boroughs[0]) if boroughs else ""
    price = str(hint.get("price") or "").strip() or _extract_price(blob)
    is_free = price.lower() == "free" or bool(_FREE_RE.search(blob))
    booking_url = str(hint.get("booking_url") or "").strip() or _extract_booking_url(candidate)
    event_name = str(hint.get("event_name") or "").strip() or _extract_event_name(candidate, entities, venue)

    # An item only counts as a structured event when we know what it IS,
    # when it happens, and where/how to identify the occurrence. Without that the
    # downstream Q5 gate would just see noise; better to leave the field
    # empty and let the existing prose-based fallback handle it.
    has_event = bool(event_name and iso_date and (venue or booking_url or candidate.get("source_url")))

    return {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_name": event_name,
        "venue": venue,
        "date": iso_date,
        "date_start": iso_date,
        "date_end": iso_end,
        "date_text": date_text,
        "date_confidence": date_confidence if iso_date else "none",
        "borough": borough,
        "price": price,
        "free": is_free,
        "booking_url": booking_url,
        "schema_source": str(hint.get("schema_source") or ""),
        "event_status": str(hint.get("event_status") or ""),
        "genre": str(hint.get("genre") or "").strip(),
        "subGenre": str(hint.get("subGenre") or "").strip(),
        "classifications": hint.get("classifications") if isinstance(hint.get("classifications"), dict) else {},
        "attractions": hint.get("attractions") if isinstance(hint.get("attractions"), list) else [],
        "ticketmaster_attraction_id": str(hint.get("ticketmaster_attraction_id") or "").strip(),
        "event_instance_id": str(hint.get("event_instance_id") or candidate.get("event_instance_id") or "").strip(),
        "promoter": str(hint.get("promoter") or "").strip(),
        "ticket_type": str(hint.get("ticket_type") or "").strip(),
        "is_recurring": bool(hint.get("is_recurring") or recurring_date_used),
        "is_event": has_event,
    }


# ── Canonical consumers' helpers ──────────────────────────────────────────
# The event blocks (Weekend, Next 7 Days, Tickets) and the repeat policy read
# these instead of re-parsing dates themselves — one source of truth.


def _event_dict(candidate_or_event: dict) -> dict:
    """Accept either a candidate (read its ``event`` sub-dict) or an event dict
    directly (use it as-is)."""
    if not isinstance(candidate_or_event, dict):
        return {}
    inner = candidate_or_event.get("event")
    if isinstance(inner, dict):
        return inner
    return candidate_or_event


def event_start_date(candidate_or_event: dict) -> date_cls | None:
    event = _event_dict(candidate_or_event)
    raw = str(event.get("date_start") or event.get("date") or "").strip()[:10]
    try:
        return date_cls.fromisoformat(raw) if raw else None
    except ValueError:
        return None


def event_end_date(candidate_or_event: dict) -> date_cls | None:
    event = _event_dict(candidate_or_event)
    raw = str(event.get("date_end") or "").strip()[:10]
    try:
        return date_cls.fromisoformat(raw) if raw else None
    except ValueError:
        return None


def event_is_multi_day(candidate_or_event: dict) -> bool:
    start = event_start_date(candidate_or_event)
    end = event_end_date(candidate_or_event)
    return bool(start and end and end > start)


def event_active_on(candidate_or_event: dict, day: date_cls) -> bool:
    """True when ``day`` falls inside the event's run (start..end inclusive).

    A multi-day festival that started yesterday and ends next week is still
    *active today* — so the repeat policy must not kill it as a stale repeat.
    """
    start = event_start_date(candidate_or_event)
    if start is None:
        return False
    end = event_end_date(candidate_or_event) or start
    return start <= day <= end


def event_is_far_future(
    candidate_or_event: dict,
    *,
    today: date_cls | None = None,
    horizon_days: int = NEAR_HORIZON_DAYS,
) -> bool:
    """True when the event starts beyond the near horizon — so a 2027 item
    can never be presented as 'this weekend' / imminent."""
    start = event_start_date(candidate_or_event)
    if start is None:
        return False
    today = today or _today_london()
    return (start - today).days > horizon_days


def event_date_is_trustworthy(candidate_or_event: dict) -> bool:
    """A date we can publish as-is. Low-confidence dates (a bare month/day that
    had to roll into next year — the classic stray-mention false positive) are
    held for enrichment rather than shown with a possibly wrong year."""
    event = _event_dict(candidate_or_event)
    if not str(event.get("date") or "").strip():
        return False
    return str(event.get("date_confidence") or "").strip() != "low"


def enrich_candidate_event(candidate: dict) -> dict:
    """Idempotent — overwrites any prior ``event`` block with a fresh
    extraction. Safe to call from the collector AND from dedupe when
    a pipeline resumes with an existing candidates.json."""
    if isinstance(candidate, dict):
        candidate["event"] = extract_event(candidate, candidate.get("entities") or {})
    return candidate


def enrich_candidates_events(candidates: list[dict]) -> list[dict]:
    for candidate in candidates:
        if isinstance(candidate, dict):
            enrich_candidate_event(candidate)
    return candidates
