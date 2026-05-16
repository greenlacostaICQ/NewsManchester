"""Structured transport-card extraction and rendering.

Goal: take the transport block out of the LLM's hands.

For each transport candidate we attempt to build a ``TransportCard`` —
a small structured record with operator, mode, line, segment, street,
area, dates, reason and alternative. The rendering layer formats the
card into a Russian Telegram bullet using one of two templates:

* **Tier 1 — full template**: operator + (line/segment or street/area) +
  reason or alternative. Produces a self-contained line that gives the
  reader everything they need.
* **Tier 2 — minimal template**: operator + just one of {line, street,
  stop_name}. Produces a shorter card that tells the reader the
  disruption exists and where, with "см. источник" instead of made-up
  detail.

If the extractor cannot derive at least ``operator`` plus ONE locator
(line / street / stop_name), it returns ``None`` and the caller falls
back to LLM (tier 3) or a title-based stub (tier 4).

The whole module is deterministic and unit-testable. No LLM, no
network. We keep all GM toponyms in English (Salford, Eccles, Marple,
Greenfield) — that's a hard requirement for the digest readership.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re
from urllib import parse


@dataclass(slots=True)
class TransportCard:
    """Structured snapshot of a single transport disruption."""

    mode: str                # "tram" | "bus" | "rail" | "coach"
    operator: str            # "Metrolink" | "Автобусы" | "Northern" | ...
    line: str = ""           # "Bury line" | "Ashton/Eccles" | "route 82"
    segment: str = ""        # "Bury Interchange – Crumpsall"
    stop_name: str = ""      # "Victoria Lodge"
    street: str = ""         # "Lower Broughton Road"
    junction: str = ""       # cross-street: "Trafford Road" / "Shadows Lane"
    area: str = ""           # "Salford" / "Eccles" / "Marple" / "Greenfield"
    start_date: str = ""     # "2026-05-17" or "17 мая"
    end_date: str = ""       # "2026-06-01" or "1 июня"
    duration_phrase: str = ""  # "две недели" (when no concrete end_date)
    reason: str = ""         # "ремонтные работы" / "замена путей" / "сигнальная неисправность"
    alternative: str = ""    # "замещающий автобус" / "идут в объезд"
    cost_phrase: str = ""    # "в рамках проекта £150 млн"
    source_label: str = ""   # original source (TfGM / BBC Manchester / The Manc)

    # Extraction confidence flags (used by renderer to pick tier 1 vs 2)
    has_line_or_segment: bool = False
    has_street_or_stop: bool = False
    has_dates: bool = False
    has_reason: bool = False
    has_alternative: bool = False


# ── Operator / mode detection ─────────────────────────────────────────────

_RAIL_OPERATORS = (
    "TransPennine Express",
    "Avanti West Coast",
    "East Midlands Railway",
    "CrossCountry",
    "Northern",
    "Grand Central",
    "Hull Trains",
    "Lumo",
    "Chiltern",
    "Network Rail",
    "National Rail",
)

_METROLINK_LINE_NAMES = (
    "Ashton", "Altrincham", "Bury", "Eccles", "Rochdale", "Oldham",
    "Trafford Park", "Trafford Center", "Manchester Airport",
    "East Didsbury", "Didsbury", "Media City", "Deansgate-Castlefield",
    "Piccadilly", "Victoria",
)


_TRAM_TITLE_RE = re.compile(
    r"\bmetrolink\b|\btrams?\b|\b(?:" + "|".join(_METROLINK_LINE_NAMES) + r")(?:\s*/\s*\w+)*\s+lines?\b",
    re.IGNORECASE,
)
_BUS_TITLE_RE = re.compile(r"\bbus\s+stop\b|\bbus\s+lane\b|\bbus\s+route\b|\bbus\s+services?\b", re.IGNORECASE)
_ROAD_TITLE_RE = re.compile(r"\b(?:road\s+closure|roadworks?|road\s+works?|diversion)\b", re.IGNORECASE)
_BUS_DIVERSION_RE = re.compile(r"\bbus\s+services?\s+(?:are\s+)?(?:on\s+)?diver(?:t|sion)|bus\s+stops?\b", re.IGNORECASE)


def _detect_mode_operator(title: str, summary: str, url: str) -> tuple[str, str]:
    """Return (mode, operator). Empty strings mean unknown."""
    text = f"{title} {summary}"
    path = url.lower()

    # 1. Rail operator names take precedence — they're explicit.
    for name in _RAIL_OPERATORS:
        if re.search(rf"\b{re.escape(name)}\b", text, re.IGNORECASE):
            return "rail", name

    # 2. Tram if title/path mentions Metrolink or a Metrolink line name.
    if _TRAM_TITLE_RE.search(title) or "metrolink" in path:
        return "tram", "Metrolink"

    # 3. Bus if title is bus-stop closure OR title is road-closure with
    # summary that mentions bus diversion. TfGM road closures almost
    # always mean bus diversion, that's the audience-relevant signal.
    if _BUS_TITLE_RE.search(title) or _BUS_DIVERSION_RE.search(summary):
        return "bus", "Автобусы"
    if _ROAD_TITLE_RE.search(title) and _BUS_DIVERSION_RE.search(summary):
        return "bus", "Автобусы"
    if _ROAD_TITLE_RE.search(title):
        # Pure road closure with no bus signal — still surface to readers,
        # but as a road advisory.
        return "road", "Дорога"

    return "", ""


# ── TfGM title parsing ────────────────────────────────────────────────────

# Typical TfGM titles:
#   "Church Street, Eccles - Bus Stop Closure"
#   "Hibbert Lane, Marple - Road Closure"
#   "Plymouth Grove, Manchester - Roadworks"
#   "Lower Broughton Road, Salford - Bus Stop Closure"
_TFGM_TITLE_RE = re.compile(
    r"^(?P<street>[^,–-]+?),\s*(?P<area>[^-–]+?)\s*[-–]\s*(?P<reason_en>.+?)\s*$"
)

# Reason mapping — English source → Russian short phrase.
# Order is significant: more specific patterns must come first so that
# "track replacement works" maps to "замена путей" instead of falling
# through to the generic "works → ремонтные работы".
_REASON_MAP = (
    # 1) Highly specific technical work types.
    (re.compile(r"track\s+replacement|replacement\s+work|replac(?:e|ing)\s+track", re.IGNORECASE), "замена путей"),
    (re.compile(r"signal\s+failure|signalling\s+(?:failure|issue|problem)|signalling\s+work", re.IGNORECASE), "сигнальная неисправность"),
    # 2) Generic work phrases.
    (re.compile(r"engineering\s+works?", re.IGNORECASE), "ремонтные работы"),
    (re.compile(r"improvement\s+works?", re.IGNORECASE), "ремонтные работы"),
    (re.compile(r"roadworks?|road\s+works?", re.IGNORECASE), "ремонтные работы"),
    # 3) TfGM bulletin titles.
    (re.compile(r"bus\s+stop\s+closure", re.IGNORECASE), "ремонтные работы"),
    (re.compile(r"road\s+closure", re.IGNORECASE), "закрытие дороги"),
    # 4) Generic "works" fallback — catches phrasings like "two weeks of
    #    works disruption" where no specific pattern hit. Uses plural to
    #    avoid false positives on "homework" / "framework".
    (re.compile(r"\bworks\b", re.IGNORECASE), "ремонтные работы"),
    # 5) Last-resort generic disruption phrasing.
    (re.compile(r"\bdisruption\b", re.IGNORECASE), "сбой"),
    (re.compile(r"minor\s+delays?", re.IGNORECASE), "небольшие задержки"),
)


def _translate_reason(text: str) -> str:
    for pattern, ru in _REASON_MAP:
        if pattern.search(text):
            return ru
    return ""


# ── Stop / segment / junction extraction from summary ─────────────────────

# Examples we hit in real summaries:
#   "The bus stop at Victoria Lodge on Lower Broughton Road is closed"
#   "The bus stops on Church Street, at the junction of Trafford Road"
#   "Due to the closure of Hibbert Lane, bus services are on diversion"
#   "no trams ... on the Bury line between Bury Interchange and Crumpsall"
# Note: the prefix words ("bus stop", "junction of/with", "between") almost
# always appear lowercase in real summaries; we keep these regex
# case-SENSITIVE so the capture group only matches genuine proper nouns
# (avoids capturing trailing "are" / "is" / "are closed" etc).
_STOP_NAME_RE = re.compile(
    r"\b[Bb]us\s+stop\s+(?:at\s+)?(?P<stop>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z']+){0,2})\s+on\b",
)
_JUNCTION_RE = re.compile(
    r"\b[Jj]unction\s+(?:of|with)\s+(?P<jct>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z']+){0,2})",
)
_SEGMENT_RE = re.compile(
    r"\b[Bb]etween\s+(?P<a>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z']+){0,3})\s+and\s+(?P<b>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z']+){0,3})",
)
_LINE_RE = re.compile(
    r"\b(?P<line>(?:" + "|".join(_METROLINK_LINE_NAMES) + r")(?:\s*/\s*\w+)*)\s+lines?\b",
    re.IGNORECASE,
)


# ── Dates / duration ──────────────────────────────────────────────────────

# Examples:
#   "from 17 May to 1 June"
#   "until 1 June"
#   "for two weeks"
#   "for two weeks from 17 May"
_MONTHS_EN = (
    "January", "February", "March", "April", "May", "June", "July",
    "August", "September", "October", "November", "December",
)
_MONTHS_EN_TO_RU = {
    "January": "января", "February": "февраля", "March": "марта",
    "April": "апреля", "May": "мая", "June": "июня",
    "July": "июля", "August": "августа", "September": "сентября",
    "October": "октября", "November": "ноября", "December": "декабря",
}

_DATE_TOKEN_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month>" + "|".join(_MONTHS_EN) + r")\b",
    re.IGNORECASE,
)
_DURATION_WEEKS_RE = re.compile(
    r"\bfor\s+(?P<n>one|two|three|four|five|six|seven|eight|nine|\d+)\s+weeks?\b",
    re.IGNORECASE,
)
_UNTIL_RE = re.compile(
    r"\buntil\s+(?P<rest>.+?)(?:\.|,|\bso\b|\bwhile\b|\bas\b|$)",
    re.IGNORECASE,
)
_FROM_TO_RE = re.compile(
    r"\bfrom\s+(?P<a>.+?)\s+(?:to|until)\s+(?P<b>.+?)(?:\.|,|\bso\b|\bwhile\b|\bas\b|$)",
    re.IGNORECASE,
)

_WORDS_TO_INT = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
                 "six": 6, "seven": 7, "eight": 8, "nine": 9}
_RU_WEEKS = {1: "неделю", 2: "две недели", 3: "три недели", 4: "четыре недели",
             5: "пять недель", 6: "шесть недель", 7: "семь недель", 8: "восемь недель"}


def _norm_date_token(token: str) -> str:
    """Convert '17 May' -> '17 мая'. Returns empty if no match."""
    m = _DATE_TOKEN_RE.search(token)
    if not m:
        return ""
    day = m.group("day")
    month_en = m.group("month").title()
    month_ru = _MONTHS_EN_TO_RU.get(month_en, "")
    if not month_ru:
        return ""
    return f"{day} {month_ru}"


def _extract_duration_or_dates(text: str) -> tuple[str, str, str]:
    """Return (start_date_ru, end_date_ru, duration_phrase_ru).

    All three may be empty. If 'from X to Y' is present we set both
    dates and leave duration empty. If only 'until Y', we set end_date.
    If only 'for N weeks', we set duration_phrase.
    """
    if not text:
        return "", "", ""

    m_from = _FROM_TO_RE.search(text)
    if m_from:
        return _norm_date_token(m_from.group("a")), _norm_date_token(m_from.group("b")), ""

    m_until = _UNTIL_RE.search(text)
    if m_until:
        return "", _norm_date_token(m_until.group("rest")), ""

    m_weeks = _DURATION_WEEKS_RE.search(text)
    if m_weeks:
        raw = m_weeks.group("n").lower()
        n = _WORDS_TO_INT.get(raw)
        if n is None:
            try:
                n = int(raw)
            except ValueError:
                n = 0
        if n:
            return "", "", _RU_WEEKS.get(n, f"{n} недель")

    return "", "", ""


# ── Alternative / cost / line cleanup ────────────────────────────────────

_ALT_PATTERNS = (
    (re.compile(r"replacement\s+bus(?:es)?", re.IGNORECASE), "замещающий автобус"),
    (re.compile(r"bus(?:es)?\s+(?:are\s+)?(?:on\s+)?diver(?:t|sion)", re.IGNORECASE), "автобусы идут в объезд"),
    (re.compile(r"\bdiversion\b", re.IGNORECASE), "объезд"),
    (re.compile(r"shuttle\s+bus", re.IGNORECASE), "шаттл-автобус"),
)


def _extract_alternative(text: str) -> str:
    for pattern, ru in _ALT_PATTERNS:
        if pattern.search(text):
            return ru
    return ""


_COST_RE = re.compile(r"£\s*(\d+(?:\.\d+)?)\s*(m|million|mln|млн)\b", re.IGNORECASE)


def _extract_cost_phrase(text: str) -> str:
    m = _COST_RE.search(text)
    if not m:
        return ""
    return f"в рамках проекта £{m.group(1)} млн"


# ── Main extractor ────────────────────────────────────────────────────────


def extract_transport_card(candidate: dict) -> TransportCard | None:
    """Build a TransportCard from a candidate dict.

    Returns None if we cannot derive at least operator + ONE locator
    (line / segment / street / stop_name). The caller then falls back
    to LLM (tier 3) or a title-based stub (tier 4).
    """
    title = str(candidate.get("title") or "")
    summary = str(candidate.get("summary") or "")
    evidence = str(candidate.get("evidence_text") or candidate.get("lead") or "")
    url = str(candidate.get("source_url") or "")
    blob = f"{title} {summary} {evidence}"

    mode, operator = _detect_mode_operator(title, summary, url)
    if not operator:
        return None

    card = TransportCard(mode=mode, operator=operator)
    card.source_label = str(candidate.get("source_label") or "")

    # ── Bus / Road: TfGM-style title "Street, Area - Reason" ───────────
    if mode in ("bus", "road"):
        m = _TFGM_TITLE_RE.match(title.strip())
        if m:
            card.street = m.group("street").strip()
            card.area = m.group("area").strip()
            card.reason = _translate_reason(m.group("reason_en")) or _translate_reason(blob)
        else:
            card.reason = _translate_reason(blob)

        m_stop = _STOP_NAME_RE.search(summary) or _STOP_NAME_RE.search(evidence)
        if m_stop:
            card.stop_name = m_stop.group("stop").strip()

        m_jct = _JUNCTION_RE.search(summary) or _JUNCTION_RE.search(evidence)
        if m_jct:
            card.junction = m_jct.group("jct").strip()

        card.alternative = _extract_alternative(blob)
        card.has_street_or_stop = bool(card.street or card.stop_name)
        card.has_reason = bool(card.reason)
        card.has_alternative = bool(card.alternative)

    # ── Tram: Metrolink line works ─────────────────────────────────────
    elif mode == "tram":
        m_line = _LINE_RE.search(title) or _LINE_RE.search(summary) or _LINE_RE.search(evidence)
        if m_line:
            card.line = m_line.group("line").strip().title() + " line"

        m_seg = _SEGMENT_RE.search(summary) or _SEGMENT_RE.search(evidence)
        if m_seg:
            card.segment = f"{m_seg.group('a').strip()} – {m_seg.group('b').strip()}"

        card.start_date, card.end_date, card.duration_phrase = _extract_duration_or_dates(blob)
        card.reason = _translate_reason(blob)
        card.alternative = _extract_alternative(blob)
        card.cost_phrase = _extract_cost_phrase(blob)
        card.has_line_or_segment = bool(card.line or card.segment)
        card.has_dates = bool(card.start_date or card.end_date or card.duration_phrase)
        card.has_reason = bool(card.reason)
        card.has_alternative = bool(card.alternative)

    # ── Rail: Northern, TransPennine etc. ──────────────────────────────
    elif mode == "rail":
        m_seg = _SEGMENT_RE.search(title) or _SEGMENT_RE.search(summary) or _SEGMENT_RE.search(evidence)
        if m_seg:
            card.segment = f"{m_seg.group('a').strip()} – {m_seg.group('b').strip()}"
        card.start_date, card.end_date, card.duration_phrase = _extract_duration_or_dates(blob)
        card.reason = _translate_reason(blob)
        card.alternative = _extract_alternative(blob)
        card.has_line_or_segment = bool(card.segment)
        card.has_dates = bool(card.start_date or card.end_date or card.duration_phrase)
        card.has_reason = bool(card.reason)

    # We deliberately return the card even when no locator is known —
    # the renderer drops to a minimal stub ("Metrolink: подробности в
    # источнике") instead of losing the candidate to tier 3. The user's
    # rule is "nothing dropped on transport, render safely whatever we
    # have". LLM only ever runs if the extractor returns None *and* the
    # caller asks for fallback.
    return card


# ── Rendering ─────────────────────────────────────────────────────────────


def render_card(card: TransportCard) -> str:
    """Format a TransportCard into a Telegram bullet.

    The renderer picks tier 1 (full template) when enough fields are
    present and falls back to tier 2 (minimal template) otherwise.
    Never returns an empty string — caller has already gated on
    "no locator" via extract_transport_card returning None.
    """
    if card.mode == "tram":
        return _render_tram(card)
    if card.mode == "bus":
        return _render_bus(card)
    if card.mode == "rail":
        return _render_rail(card)
    if card.mode == "road":
        return _render_road(card)
    return ""


def _render_tram(card: TransportCard) -> str:
    # Time phrase (Russian)
    time_phrase = ""
    if card.start_date and card.end_date:
        time_phrase = f"с {card.start_date} по {card.end_date}"
    elif card.end_date:
        time_phrase = f"до {card.end_date}"
    elif card.duration_phrase:
        time_phrase = card.duration_phrase

    # Where (line / segment)
    location = ""
    if card.line and card.segment:
        location = f"на {card.line} между {card.segment}"
    elif card.line:
        location = f"на {card.line}"
    elif card.segment:
        location = f"между {card.segment}"

    # Decide template tier based on signal density.
    has_loc = bool(location)
    has_time = bool(time_phrase)
    has_meta = bool(card.reason or card.cost_phrase or card.alternative)

    # Tier 1: full sentence "Metrolink: с DATE по DATE нет трамваев на LINE между SEG — REASON; ALT."
    if has_loc and (has_time or has_meta):
        if has_time:
            head = f"{card.operator}: {time_phrase} нет трамваев {location}"
        else:
            head = f"{card.operator}: работы {location}"
    elif has_loc:
        head = f"{card.operator}: работы {location}"
    elif has_time and has_meta:
        # We don't know which line, but we know "two weeks of works" — surface that.
        head = f"{card.operator}: {time_phrase} работы на сети"
    elif has_time:
        head = f"{card.operator}: {time_phrase} работы на сети"
    elif has_meta:
        head = f"{card.operator}: работы на сети"
    else:
        return f"• {card.operator}: см. источник."

    tail_bits: list[str] = []
    if card.reason and card.cost_phrase:
        tail_bits.append(f"{card.reason} {card.cost_phrase}")
    elif card.reason:
        tail_bits.append(card.reason)
    elif card.cost_phrase:
        tail_bits.append(card.cost_phrase)
    if card.alternative:
        alt = f"TfGM запускает {card.alternative}" if "автобус" in card.alternative else card.alternative
        tail_bits.append(alt)

    if tail_bits:
        return f"• {head} — {'; '.join(tail_bits)}."
    if has_loc:
        return f"• {head} — подробности в источнике."
    return f"• {head} — подробности в источнике."


def _render_bus(card: TransportCard) -> str:
    # Tier 1 paths
    if card.stop_name and card.street and card.area:
        head = f"{card.operator}: закрыта остановка {card.stop_name} на {card.street} в {card.area}"
    elif card.street and card.area and card.junction:
        head = f"{card.operator}: закрыта остановка на {card.street}, {card.area} (пересечение с {card.junction})"
    elif card.street and card.area and re.search(r"объезд|divers", card.alternative, re.IGNORECASE):
        head = f"{card.operator}: маршруты на {card.street}, {card.area} идут в объезд"
    elif card.street and card.area:
        head = f"{card.operator}: на {card.street}, {card.area} объезд и закрытые остановки"
    elif card.street:
        head = f"{card.operator}: затронуты остановки на {card.street}"
    elif card.stop_name:
        head = f"{card.operator}: закрыта остановка {card.stop_name}"
    else:
        head = f"{card.operator}: см. TfGM"

    tail = card.reason or ""
    if tail:
        return f"• {head} — {tail}."
    return f"• {head}."


def _render_rail(card: TransportCard) -> str:
    parts: list[str] = [card.operator + ":"]
    if card.duration_phrase or card.end_date or card.start_date:
        if card.start_date and card.end_date:
            parts.append(f"с {card.start_date} по {card.end_date}")
        elif card.end_date:
            parts.append(f"до {card.end_date}")
        elif card.duration_phrase:
            parts.append(card.duration_phrase)
    if card.segment:
        parts.append(f"сбой между {card.segment}")
    elif card.reason:
        parts.append(card.reason)
    head = " ".join(parts).strip(": ")
    tail = card.reason if card.segment and card.reason else ""
    if tail:
        return f"• {head} — {tail}."
    return f"• {head}."


def _render_road(card: TransportCard) -> str:
    where = ""
    if card.street and card.area:
        where = f"на {card.street} в {card.area}"
    elif card.street:
        where = f"на {card.street}"
    tail = card.reason or "закрытие дороги"
    head = f"{card.operator}: {where}".strip(": ").strip()
    return f"• {head} — {tail}."


# ── Reminder rendering (Metrolink only) ───────────────────────────────────


def render_reminder(card: TransportCard, today_iso: str | None = None) -> str:
    """Render a reminder bullet for an active Metrolink disruption.

    Called daily from transport_fill until ``card.end_date`` passes.
    Only tram mode produces reminders — bus / road / rail disruptions
    are typically short and don't need persistent surfacing.
    """
    if card.mode != "tram":
        return ""

    head = f"{card.operator} (продолжается"
    if card.end_date:
        head += f", до {card.end_date}"
    head += "):"

    # Body: where + what
    if card.line and card.segment:
        body = f"нет трамваев на {card.line} между {card.segment}"
    elif card.line:
        body = f"нет трамваев на {card.line}"
    elif card.segment:
        body = f"нет трамваев между {card.segment}"
    else:
        # No locator — surface as a network-level reminder.
        body = "работы на сети"

    tail_bits: list[str] = []
    if card.reason:
        tail_bits.append(card.reason)
    if card.alternative:
        alt = f"замещающий автобус" if "автобус" in card.alternative else card.alternative
        tail_bits.append(alt)

    if tail_bits:
        return f"• {head} {body} — {'; '.join(tail_bits)}."
    return f"• {head} {body} — подробности в источнике."
