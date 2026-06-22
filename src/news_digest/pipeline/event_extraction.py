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
from datetime import date as date_cls
from datetime import datetime
from zoneinfo import ZoneInfo

EVENT_SCHEMA_VERSION = 1

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

# English: "16 May 2026", "May 16, 2026", "16 May", "May 16"
_EN_DAY_MONTH_YEAR_RE = re.compile(
    r"\b(?P<day>\d{1,2})\s+(?P<month>"
    + "|".join(_EN_MONTHS.keys())
    + r")(?:\s+(?P<year>20\d{2}))?\b",
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


def _parse_date_from_blob(blob: str, *, today: date_cls | None = None) -> tuple[str, str]:
    """Return (iso_date, date_text) — both empty strings if no date found.

    Tries in order:
      1. Day-range English ("16-17 May")            — most informative
      2. Day-range Russian ("16-17 мая")
      3. ISO ("2026-05-16")
      4. UK slash date ("12/06/2026")
      5. English "16 May [2026]"
      6. English "May 16[, 2026]"
      7. Russian "16 мая [2026]"

    For ranges, returns the START date as ``iso_date`` so other code
    (deduplication, "is in the future?" gates) has a single sortable
    value, and the human range as ``date_text``.
    """
    today = today or _today_london()

    if m := _DAY_RANGE_EN_RE.search(blob):
        start = int(m.group("start"))
        end = int(m.group("end"))
        month = _EN_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, start)
        if iso:
            return iso, f"{start}–{end} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()

    if m := _DAY_RANGE_RU_RE.search(blob):
        start = int(m.group("start"))
        end = int(m.group("end"))
        month = _RU_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, start)
        if iso:
            return iso, f"{start}–{end} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()

    if m := _ISO_DATE_RE.search(blob):
        iso = _safe_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if iso:
            return iso, iso

    if m := _UK_SLASH_DATE_RE.search(blob):
        iso = _safe_date(int(m.group("year")), int(m.group("month")), int(m.group("day")))
        if iso:
            return iso, iso

    if m := _EN_DAY_MONTH_YEAR_RE.search(blob):
        day = int(m.group("day"))
        month = _EN_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, day)
        if iso:
            return iso, f"{day} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()

    if m := _EN_MONTH_DAY_YEAR_RE.search(blob):
        day = int(m.group("day"))
        month = _EN_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, day)
        if iso:
            return iso, f"{m.group('month')} {day}{(', ' + m.group('year')) if m.group('year') else ''}".strip()

    if m := _RU_DAY_MONTH_YEAR_RE.search(blob):
        day = int(m.group("day"))
        month = _RU_MONTHS[m.group("month").lower()]
        year = _resolve_year(month, m.group("year"), today)
        iso = _safe_date(year, month, day)
        if iso:
            return iso, f"{day} {m.group('month')}{(' ' + m.group('year')) if m.group('year') else ''}".strip()

    return "", ""


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
        if name.lower() not in {"home", "manchester", "london", "the uk", "the future"}:
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
    if source_label and title.endswith(source_label):
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
    iso_date, date_text = _parse_date_from_blob(blob)
    hint_date = str(hint.get("date_start") or hint.get("date") or "").strip()
    if hint_date:
        parsed_hint = _parse_date_from_blob(hint_date)[0] or hint_date[:10]
        if parsed_hint:
            iso_date = parsed_hint
            date_text = str(hint.get("date_text") or "").strip() or date_text or parsed_hint
    boroughs = entities.get("boroughs") if isinstance(entities.get("boroughs"), list) else []
    borough = str(boroughs[0]) if boroughs else ""
    price = str(hint.get("price") or "").strip() or _extract_price(blob)
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
        "date_end": "",
        "date_text": date_text,
        "borough": borough,
        "price": price,
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
        "is_event": has_event,
    }


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
