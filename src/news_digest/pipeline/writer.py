from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import html
import logging
from pathlib import Path
import re

logger = logging.getLogger(__name__)

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    SECTION_MAX_ITEMS,
    SECTION_MAX_PER_SOURCE,
    SECTION_MIN_ITEMS,
    is_placeholder_practical_angle,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.editorial_contracts import (
    attach_editorial_contract,
    classify_ticket_type,
    copy_invariant_errors,
    scrub_vague_ending,
)
from news_digest.pipeline.reader_value import reader_value_score
from news_digest.pipeline.toponyms import restore_english_toponyms
from news_digest.pipeline.place_names import preserve_place_names


MODEL_WRITTEN_CATEGORIES = {"media_layer", "gmp", "council", "public_services", "food_openings"}
REQUIRE_DRAFT_LINE_CATEGORIES = MODEL_WRITTEN_CATEGORIES | {
    "transport",
    "venues_tickets",
    "russian_speaking_events",
    "culture_weekly",
    "football",
    "tech_business",
    "city_news",
}
# Categories that should render as 350–450 char multi-sentence cards rather
# than single-line headlines. Transport / weather / billet are explicitly
# excluded — they're shorter by design.
LONG_FORMAT_CATEGORIES = {
    "media_layer",
    "gmp",
    "council",
    "public_services",
    "city_news",
    "food_openings",
    "tech_business",
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "football",
}
LONG_FORMAT_MIN_CHARS = 150
LONG_FORMAT_MIN_SENTENCES = 2
SHORT_TICKET_BLOCKS = {"ticket_radar", "outside_gm_tickets"}
# Original short blocks: weekend events naturally fit in 100 chars,
# tickets are intentionally short.
SHORT_EVENT_BLOCKS = SHORT_TICKET_BLOCKS | {"weekend_activities"}
# Sequential fallback: event blocks where we PREFER 150+ char cards
# (more detail = better) but ACCEPT shorter ones when the source RSS
# only gave us a thin evidence_text. Logic in _draft_line_quality_errors
# checks evidence size before applying the LONG_FORMAT_MIN_CHARS gate.
# We only relax when evidence was genuinely tiny (< 500 chars).
EVENT_BLOCKS_RELAXABLE = {"next_7_days", "future_announcements", "russian_events"}
EVENT_RELAX_EVIDENCE_THRESHOLD = 500
TODAY_FOCUS_SECTION = "Что важно сегодня"
# Order matters: backfill takes the first non-empty section. We previously
# pulled from transport FIRST, which dumped bus-stop closures into "Что
# важно сегодня" (those are not "important news of the day" — they're
# already shown in the transport block above). Now media news leads;
# transport is the last-resort fallback only when there's literally nothing
# else to put up top.
TODAY_FOCUS_BACKFILL_SECTIONS = (
    "Что произошло за 24 часа",
    "Городской радар",
)
TODAY_FOCUS_BACKFILL_TARGET = 2
TODAY_FOCUS_BACKFILL_MIN_SCORE = 67.5
TODAY_FOCUS_MIN_SOURCE_REMAINING = {
    # Don't gut source blocks just to fill today_focus.
    "Что произошло за 24 часа": 3,
    "Городской радар": 4,
}

# When the LLM rewrite stage is degraded, keep the digest conservative:
# publish the most useful lines from each soft section and expose exactly
# what was held in writer_report.degraded_shrink for review. This is not the
# global issue budget; it only applies on days when generation quality is
# already known to be weaker.
DEGRADED_LLM_SECTION_MAX_ITEMS = {
    "Что произошло за 24 часа": 6,
    "Городской радар": 5,
    "Выходные в GM": 5,
    "Что важно в ближайшие 7 дней": 4,
    "Билеты / Ticket Radar": 3,
    "Еда, открытия и рынки": 2,
    "IT и бизнес": 2,
    "Футбол": 2,
    "Русскоязычные концерты и стендап UK": 3,
}

PUBLIC_DIGEST_MAX_VISIBLE_ITEMS = 22
_BAD_EDITORIAL_PROSE_MARKERS = (
    "ticket office",
    "слот входа",
    "госпитальн",
    "кадровый и дисциплинарный кейс",
    "заметный кейс",
    "новая фаза истории",
    "сетка влияния",
    "следить компаниям",
    "business-impact",
    "лучше взять зонт",
    "лучше прихватить зонт",
    "не забудьте зонт",
    "прихватите зонт",
    "live alert",
    "live disruption",
    "forecast",
    "attractions",
    "highlights",
    "matchday",
    "check before",
    "опубликовал важное обновление",
    "появилось новое обновление",
    "судебное обновление",
    "новое судебное",
    "футбольное обновление",
    "перепроверьте",
    "убедитесь сами",
    "читайте подробнее",
    "подробности ниже",
    # PR filler endings from LLM padding
    "обогатит",
    "обещает стать",
    "центр притяжения",
    "новая достопримечательность",
    "другие детали не сообщаются",
    "подробности не раскрываются",
    "остаётся нерешённой",
    "привлечёт внимание",
    "вступило в силу.",
    "билеты и даты уточняйте",
    "время и дату уточняйте",
    "дату и время уточняйте",
    "уточните даты",
    "проверьте детали",
    "свяжитесь с организатор",
    "проверьте сами",
)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path
    draft_path: Path


def _title_line() -> str:
    now = now_london()
    return f"<b>Greater Manchester Brief — {now.strftime('%Y-%m-%d, %H:%M')}</b>"


def _normalize_text_key(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9а-яё]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _summary_is_useful(summary: str, headline: str) -> bool:
    cleaned = str(summary or "").strip()
    if not cleaned:
        return False
    if _normalize_text_key(cleaned) == _normalize_text_key(headline):
        return False
    if len(cleaned) < 28:
        return False
    return True


def _backfill_today_focus(
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
    source_sections: tuple[str, ...] = TODAY_FOCUS_BACKFILL_SECTIONS,
) -> int:
    if sections.get(TODAY_FOCUS_SECTION):
        return 0

    moved = 0
    sections.setdefault(TODAY_FOCUS_SECTION, [])
    section_sources.setdefault(TODAY_FOCUS_SECTION, [])
    section_scores.setdefault(TODAY_FOCUS_SECTION, [])
    section_fingerprints.setdefault(TODAY_FOCUS_SECTION, [])
    section_titles.setdefault(TODAY_FOCUS_SECTION, [])

    for source_section in source_sections:
        lines = sections.get(source_section) or []
        sources = section_sources.get(source_section) or []
        scores = section_scores.get(source_section) or []
        fingerprints = section_fingerprints.get(source_section) or []
        titles = section_titles.get(source_section) or []
        if scores:
            ranked = sorted(
                zip(
                    lines,
                    sources + [""] * (len(lines) - len(sources)),
                    scores + [0.0] * (len(lines) - len(scores)),
                    fingerprints + [""] * (len(lines) - len(fingerprints)),
                    titles + [""] * (len(lines) - len(titles)),
                ),
                key=lambda item: item[2],
                reverse=True,
            )
            lines = [item[0] for item in ranked]
            sources = [item[1] for item in ranked]
            scores = [item[2] for item in ranked]
            fingerprints = [item[3] for item in ranked]
            titles = [item[4] for item in ranked]
        min_remaining = TODAY_FOCUS_MIN_SOURCE_REMAINING.get(source_section, 0)
        while lines and moved < TODAY_FOCUS_BACKFILL_TARGET and len(lines) > min_remaining:
            if scores and scores[0] < TODAY_FOCUS_BACKFILL_MIN_SCORE:
                break
            sections[TODAY_FOCUS_SECTION].append(lines.pop(0))
            section_sources[TODAY_FOCUS_SECTION].append(sources.pop(0) if sources else "")
            section_scores[TODAY_FOCUS_SECTION].append(scores.pop(0) if scores else 0.0)
            section_fingerprints[TODAY_FOCUS_SECTION].append(fingerprints.pop(0) if fingerprints else "")
            section_titles[TODAY_FOCUS_SECTION].append(titles.pop(0) if titles else "")
            moved += 1
        sections[source_section] = lines
        section_sources[source_section] = sources
        section_scores[source_section] = scores
        section_fingerprints[source_section] = fingerprints
        section_titles[source_section] = titles
        if moved >= TODAY_FOCUS_BACKFILL_TARGET:
            break

    if not sections.get(TODAY_FOCUS_SECTION):
        sections.pop(TODAY_FOCUS_SECTION, None)
        section_sources.pop(TODAY_FOCUS_SECTION, None)
        section_scores.pop(TODAY_FOCUS_SECTION, None)
        section_fingerprints.pop(TODAY_FOCUS_SECTION, None)
        section_titles.pop(TODAY_FOCUS_SECTION, None)
    return moved


def _contract_public_drop_reason(candidate: dict) -> str:
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    tier = str(contract.get("publish_tier") or candidate.get("publish_tier") or "")
    event_shape = str(contract.get("event_shape") or candidate.get("event_shape") or "")
    reject_reason = str(contract.get("reject_reason") or "")
    if reject_reason:
        return f"editorial_contract:{reject_reason}"
    if block == "transport" and str(candidate.get("transport_mode") or "") == "road":
        return "road_only_transport"
    if (
        tier == "filler"
        and block in {"last_24h", "today_focus", "city_watch"}
        and category in {"media_layer", "council", "public_services", "gmp", "city_news"}
    ):
        return "editorial_filler"
    if event_shape == "bookable_activity" and (
        block == "weekend_activities"
        or (
            block == "next_7_days"
            and "designmynight" in str(candidate.get("source_label") or "").lower()
        )
    ):
        return "bookable_activity_filler"
    return ""


def _contains_cyrillic(value: str) -> bool:
    return bool(re.search(r"[а-яё]", str(value or ""), flags=re.IGNORECASE))


def _looks_like_untranslated_english(value: str) -> bool:
    text = str(value or "").strip()
    if not text or _contains_cyrillic(text):
        return False
    latin_words = re.findall(r"[A-Za-z][A-Za-z'’-]+", text)
    if len(latin_words) < 8:
        return False
    stopwords = {
        "the", "and", "for", "with", "from", "after", "following", "into", "across",
        "will", "have", "has", "had", "that", "this", "they", "their", "about", "said",
        "says", "into", "over", "under", "following", "response", "operators",
    }
    stopword_hits = sum(1 for word in latin_words if word.lower() in stopwords)
    return stopword_hits >= 2


def _llm_rewrite_is_degraded(state_dir: Path) -> tuple[bool, dict]:
    report = read_json(state_dir / "llm_rewrite_report.json", {})
    if not isinstance(report, dict):
        return False, {}
    status = str(report.get("stage_status") or "").strip().lower()
    warnings = [str(w) for w in (report.get("warnings") or [])]
    degraded = status == "degraded" or any("degraded" in w.lower() for w in warnings)
    return degraded, report


def _source_anchor(source_url: str, source_label: str) -> str:
    return f'<a href="{html.escape(source_url, quote=True)}">{html.escape(source_label)}</a>'


def _attach_source_anchor(line: str, source_url: str, source_label: str) -> str:
    text = str(line or "").strip()
    if "<a " in text.lower():
        return text
    label = str(source_label or "").strip()
    label_lower = label.lower()
    # Normalise by stripping trailing punctuation before checking — handles both
    # "...Met Office" and "...Met Office." (period added by LLM or practical angle).
    if label and text.lower().rstrip(" .").endswith(label_lower):
        base = text.rstrip(" .")
        # Only strip trailing spaces (not periods) so the sentence period before
        # the label is preserved: "...зонт обязателен. Met Office" → "...зонт обязателен."
        text = base[: len(base) - len(label)].rstrip(" ")
    return f"{text} {_source_anchor(source_url, source_label)}".strip()


_SUMMER_MONTHS = frozenset({6, 7, 8})
_HEAVY_SNOW_PATTERN = re.compile(
    r"\b(?:heavy\s+snow|blizzard|snowstorm|snowfall|снегопад|метель|снежная\s+буря)\b",
    re.IGNORECASE,
)
_EXTREME_TEMP_PATTERN = re.compile(r"\b([1-9]\d)\s*°[Cc]\b")
_EVENT_BLOCKS = {"weekend_activities", "next_7_days", "ticket_radar", "outside_gm_tickets", "russian_events", "future_announcements"}
_WEEKEND_BLOCK = "weekend_activities"
_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}
_RU_MONTHS_GENITIVE = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


def _sanity_flags(candidate: dict, line: str) -> list[str]:
    flags: list[str] = []
    month = now_london().month
    if month in _SUMMER_MONTHS and _HEAVY_SNOW_PATTERN.search(line):
        flags.append("Seasonal impossibility: heavy snow in summer month.")
    for m in _EXTREME_TEMP_PATTERN.finditer(line):
        temp = int(m.group(1))
        if temp > 38 or temp < 0 and month in _SUMMER_MONTHS:
            flags.append(f"Implausible Manchester temperature: {m.group()}.")
    return flags


def _parse_day(value: object) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
    except ValueError:
        return None


def _date_signals(text: str) -> list[date]:
    today = now_london().date()
    lowered = str(text or "").lower()
    dates: list[date] = []
    for match in re.finditer(r"\b(20\d{2})[/-](\d{1,2})[/-](\d{1,2})\b", lowered):
        year, month, day = (int(part) for part in match.groups())
        try:
            dates.append(date(year, month, day))
        except ValueError:
            continue
    for match in re.finditer(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)", lowered):
        year, month, day = (int(part) for part in match.groups())
        try:
            dates.append(date(year, month, day))
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-zа-яё]{3,9})(?:\s+(20\d{2}))?\b", lowered):
        day_raw, month_raw, year_raw = match.groups()
        month = _MONTHS.get(month_raw)
        if not month:
            continue
        year = int(year_raw) if year_raw else today.year
        try:
            dates.append(date(year, month, int(day_raw)))
        except ValueError:
            continue
    for match in re.finditer(r"\b([a-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s*,?\s*(20\d{2}))?\b", lowered):
        month_raw, day_raw, year_raw = match.groups()
        month = _MONTHS.get(month_raw)
        if not month:
            continue
        year = int(year_raw) if year_raw else today.year
        try:
            dates.append(date(year, month, int(day_raw)))
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{1,2})\s*[–-]\s*(\d{1,2})\s+([a-zа-яё]{3,9})(?:\s+(20\d{2}))?\b", lowered):
        _start_day_raw, end_day_raw, month_raw, year_raw = match.groups()
        month = _MONTHS.get(month_raw)
        if not month:
            continue
        year = int(year_raw) if year_raw else today.year
        try:
            dates.append(date(year, month, int(end_day_raw)))
        except ValueError:
            continue
    return dates


def _future_date_signal(text: str) -> bool:
    dates = _date_signals(text)
    return bool(dates and max(dates) >= now_london().date())


def _format_ru_day_month(value: datetime | None) -> str:
    if value is None:
        return ""
    return f"{value.day} {_RU_MONTHS_GENITIVE.get(value.month, '')}".strip()


def _parse_ticket_datetime(candidate: dict) -> datetime | None:
    summary = str(candidate.get("summary") or "")
    for raw in (
        candidate.get("published_at"),
        candidate.get("event_date"),
        candidate.get("event_end_date"),
    ):
        parsed = str(raw or "").strip()
        if not parsed:
            continue
        try:
            return datetime.fromisoformat(parsed.replace("Z", "+00:00")).astimezone(now_london().tzinfo)
        except ValueError:
            continue
    match = re.search(r"\bevent_date=(20\d{2}-\d{2}-\d{2})(?:\s+(\d{2}:\d{2}))?", summary)
    if match:
        raw = match.group(1)
        if match.group(2):
            raw = f"{raw}T{match.group(2)}:00+01:00"
        else:
            raw = f"{raw}T12:00:00+01:00"
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None
    title = str(candidate.get("title") or "")
    title_match = re.search(r"\b(?:mon|tue|wed|thu|fri|sat|sun)\s+(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})\b", title, re.IGNORECASE)
    if title_match:
        day_raw, month_raw, year_raw = title_match.groups()
        month = _MONTHS.get(month_raw.lower())
        if month:
            try:
                return datetime(int(year_raw), month, int(day_raw), 12, 0, 0)
            except ValueError:
                return None
    return None


def _ticket_headliner(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(title or "")).strip()
    cleaned = re.split(r"\s+[—-]\s+event\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    cleaned = re.sub(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" -–,")
    return cleaned or "событие"


def _ticket_venue(candidate: dict) -> str:
    summary = str(candidate.get("summary") or "")
    source_label = str(candidate.get("source_label") or "").strip()
    first_chunk = summary.split("|", 1)[0].strip(" .")
    first_chunk = re.sub(r"^(Manchester|Liverpool|London)\s+", "", first_chunk, flags=re.IGNORECASE).strip(" .")
    if first_chunk and len(first_chunk) >= 4:
        return first_chunk
    return source_label


def _ticket_genre(candidate: dict) -> str:
    summary = str(candidate.get("summary") or "")
    chunks = [chunk.strip(" .") for chunk in summary.split("|")]
    ignored = {
        "manchester",
        "liverpool",
        "london",
        "greater manchester",
        "united kingdom",
        "uk",
    }
    for chunk in chunks[1:4]:
        lowered = chunk.lower()
        if not chunk or lowered in ignored:
            continue
        if "=" in chunk or lowered.startswith("ticket_"):
            continue
        if re.search(r"\b(?:arena|hall|warehouse|academy|institute|studios|club|depot|apollo|ritz|theatre|stadium)\b", lowered):
            continue
        return chunk
    return ""


def _build_ticket_fallback_line(candidate: dict) -> str:
    title = _ticket_headliner(str(candidate.get("title") or ""))
    venue = _ticket_venue(candidate)
    genre = _ticket_genre(candidate)
    practical = str(candidate.get("practical_angle") or "Проверьте время, вход и наличие билетов на официальной странице.").strip()
    ticket_type = str(candidate.get("ticket_type") or "").strip() or classify_ticket_type(candidate)
    type_prefix = {
        "on_sale_now": "Сейчас в продаже",
        "presale_soon": "Скоро откроется продажа",
        "newly_listed": "Новый анонс",
        "major_upcoming": "Крупный анонс",
        "old_onsale": "Продажа уже открыта",
        "old_public_sale": "Билеты уже в продаже",
    }.get(ticket_type, "Анонс")
    event_dt = _parse_ticket_datetime(candidate)
    day_month = _format_ru_day_month(event_dt)
    time_part = ""
    if event_dt and event_dt.strftime("%H:%M") != "12:00":
        time_part = f" в {event_dt.strftime('%H:%M')}"
    genre_part = f" ({genre})" if genre else ""
    if day_month and venue:
        return f"• {type_prefix}: в {venue} {day_month}{time_part} — концерт {title}{genre_part}. {practical}"
    if day_month:
        return f"• {type_prefix}: {day_month}{time_part} — концерт {title}{genre_part}. {practical}"
    if venue:
        return f"• {type_prefix}: в {venue} — концерт {title}{genre_part}. {practical}"
    return f"• {type_prefix}: концерт {title}{genre_part}. {practical}"


def _ticket_public_onsale_datetime(candidate: dict) -> datetime | None:
    match = re.search(
        r"\bpublic_onsale=(20\d{2}-\d{2}-\d{2})(?:\s+(\d{2}:\d{2}))?",
        str(candidate.get("summary") or ""),
    )
    if not match:
        return None
    raw = f"{match.group(1)}T{match.group(2) or '12:00'}:00+01:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _line_claims_future_ticket_sale(candidate: dict, line: str) -> bool:
    if str(candidate.get("category") or "") != "venues_tickets":
        return False
    onsale_at = _ticket_public_onsale_datetime(candidate)
    if onsale_at is None or onsale_at.date() >= now_london().date():
        return False
    return bool(
        re.search(
            r"\b(?:"
            r"будут\s+доступны|станут\s+доступны|будут\s+в\s+продаже|"
            r"поступ(?:ят|ит)?\s+в\s+продаж|"
            r"старт(?:ует|уют)\s+(?:в\s+)?продаж|"
            r"откро(?:ется|ются)\s+(?:в\s+)?продаж"
            r")",
            line,
            flags=re.IGNORECASE,
        )
    )


def _sourceish_event_name(candidate: dict) -> str:
    title = re.sub(r"\s+", " ", str(candidate.get("title") or "")).strip()
    source = re.sub(r"\s+", " ", str(candidate.get("source_label") or "")).strip()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    name = str(event.get("event_name") or "").strip()
    if name and len(name) <= 80:
        return name
    if source and re.search(r"\b(?:car boot|flower festival|jazz festival|market|festival)\b", source, re.IGNORECASE):
        return source
    title = re.sub(r"\s+(?:season\s+)?opens?\s+\d{1,2}\s+[A-Za-zА-Яа-яЁё]+.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s+[—–-]\s+(?:event|public\s+sale).*$", "", title, flags=re.IGNORECASE)
    return title[:90].strip(" .-–") or source or "событие"


def _event_venue(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    venue = str(event.get("venue") or "").strip()
    if venue and venue.lower() not in {"greater manchester", "manchester", "bury", "rochdale", "salford"}:
        return venue
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("summary", "lead", "evidence_text", "title")
    )
    for pattern in (
        r"\b(Bowlee Community Park)\b",
        r"\b(Barton Aerodrome)\b",
        r"\b(Waterside Farm)\b",
        r"\b(St Ann'?s Square)\b",
        r"\b(First Street)\b",
        r"\b(Salford Quays)\b",
    ):
        match = re.search(pattern, blob, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return ""


def _format_event_time(raw_hour: str, raw_minute: str = "", meridiem: str = "") -> str:
    try:
        hour = int(raw_hour)
    except ValueError:
        return raw_hour
    minute = raw_minute or "00"
    mer = meridiem.lower()
    if mer == "pm" and hour < 12:
        hour += 12
    if mer == "am" and hour == 12:
        hour = 0
    return f"{hour}:{minute.zfill(2)}"


def _extract_event_practical_details(candidate: dict) -> list[str]:
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("summary", "lead", "evidence_text", "practical_angle", "draft_line")
    )
    details: list[str] = []
    seller = re.search(
        r"(?:sellers?|продавц[ыа-я]*)\s*(?:arrive\s*)?(?:from|с)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
        blob,
        flags=re.IGNORECASE,
    )
    buyer = re.search(
        r"(?:buyers?|покупател[ьи])\s*(?:from|с)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
        blob,
        flags=re.IGNORECASE,
    )
    if seller or buyer:
        parts = []
        if seller:
            parts.append(f"продавцы с {_format_event_time(seller.group(1), seller.group(2) or '', seller.group(3) or '')}")
        if buyer:
            parts.append(f"покупатели с {_format_event_time(buyer.group(1), buyer.group(2) or '', buyer.group(3) or '')}")
        details.append(", ".join(parts))
    else:
        time_context = re.search(r"\btime\b.{0,90}", blob, flags=re.IGNORECASE)
        time_source = time_context.group(0) if time_context else blob[:500]
        time_matches = re.findall(
            r"\b(?:from\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b|\b(\d{1,2}):(\d{2})\b",
            time_source,
            flags=re.IGNORECASE,
        )
        formatted_times = []
        for h1, m1, mer, h2, m2 in time_matches:
            if h1:
                formatted_times.append(_format_event_time(h1, m1, mer))
            elif h2:
                formatted_times.append(f"{int(h2)}:{m2}")
        if formatted_times:
            details.append("время: " + ", ".join(dict.fromkeys(formatted_times[:3])))
    prices = re.findall(r"£\s*\d+(?:\.\d{1,2})?", blob)
    if prices:
        details.append("цены: " + ", ".join(dict.fromkeys(prices[:4])))
    if re.search(r"\b(?:free\s+(?:entry|admission|event)|entry\s+free|admission\s+free)\b|бесплатн(?:ый|о|ая)\s+вход", blob, re.IGNORECASE):
        details.append("вход бесплатный")
    if re.search(
        r"\bno\s+(?:booking|pre-?booking)|no\s+need\s+to\s+book|"
        r"do\s+not\s+need\s+to\s+pre-?book|pre-?booking\s+is\s+not\s+required|"
        r"предварительн\w+\s+запис",
        blob,
        re.IGNORECASE,
    ):
        details.append("запись не нужна")
    elif re.search(r"\bbook(?:ing)?|tickets?|билет", blob, re.IGNORECASE) and not re.search(r"\bcar boot|market\b", blob, re.IGNORECASE):
        details.append("проверьте билеты")
    return details[:3]


def _build_recurring_event_fallback_line(candidate: dict) -> str:
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    occurrence = contract.get("occurrence") if isinstance(contract.get("occurrence"), dict) else {}
    date_text = str(occurrence.get("date_text") or "").strip()
    name = _sourceish_event_name(candidate)
    venue = _event_venue(candidate)
    details = _extract_event_practical_details(candidate)
    where = f" в {venue}" if venue and venue.lower() not in name.lower() else ""
    prefix = f"{date_text.capitalize()} — " if date_text else "В ближайший день расписания — "
    tail = "; ".join(details)
    tail = f". {tail.capitalize()}." if tail else ". Проверьте время и условия перед выездом."
    return f"• {prefix}{name}{where}{tail}"


def _build_festival_fallback_line(candidate: dict) -> str:
    title_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    lowered = title_blob.lower()
    if "flower festival" in lowered:
        return (
            "• 23–25 мая — Manchester Flower Festival в центре города: St Ann’s Square, "
            "King Street и соседние улицы. Вход бесплатный; держите в планах прогулку, "
            "маршрут и время мастер-классов."
        )
    if "jazz festival" in lowered:
        return (
            "• До 24 мая — Manchester Jazz Festival на городских площадках. "
            "В эти выходные проверьте сегодняшние концерты, площадку и билеты, "
            "а старую программу открытия 15–17 мая не используйте для планирования."
        )
    name = _sourceish_event_name(candidate)
    occurrence = (candidate.get("editorial_contract") or {}).get("occurrence") if isinstance(candidate.get("editorial_contract"), dict) else {}
    date_text = str((occurrence or {}).get("date_text") or "").strip()
    prefix = f"{date_text} — " if date_text else ""
    details = _extract_event_practical_details(candidate)
    tail = "; ".join(details)
    tail = f". {tail.capitalize()}." if tail else ". Проверьте актуальную программу и билеты."
    return f"• {prefix}{name}{tail}"


def _build_bookable_activity_fallback_line(candidate: dict) -> str:
    name = _sourceish_event_name(candidate)
    venue = _event_venue(candidate)
    details = _extract_event_practical_details(candidate)
    where = f" в {venue}" if venue and venue.lower() not in name.lower() else ""
    tail = "; ".join(details)
    tail = f". {tail.capitalize()}." if tail else ". Проверьте свободные слоты перед оплатой."
    return f"• На эти выходные можно забронировать {name}{where}{tail}"


def _repair_weather_line(line: str) -> str:
    text = str(line or "")
    text = re.sub(
        r"вероятность\s+осадков\s+до\s+0\s*%",
        "без существенных осадков",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\s*Дн[её]м заметно теплее утра\.\s*", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _repair_editorial_contract_line(candidate: dict, line: str) -> tuple[str, list[str]]:
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    event_shape = str(contract.get("event_shape") or "")
    repaired = str(line or "").strip()
    reasons: list[str] = []
    if str(candidate.get("primary_block") or "") == "weather":
        updated = _repair_weather_line(repaired)
        if updated != repaired:
            repaired = updated
            reasons.append("weather_wording")
    if event_shape == "recurring" and str(candidate.get("primary_block") or "") == "weekend_activities":
        # Recurring items must lead with the next occurrence. The season
        # start/end can be supporting detail, never the lead.
        if re.search(r"\b(?:до|until)\s+\d{1,2}\s+[A-Za-zА-Яа-яЁё]+|opens?\s+\d{1,2}\s+[A-Za-zА-Яа-яЁё]+|с\s+\d{1,2}\s+[а-яё]+|\b(?:every|each)\s+(?:saturday|sunday)|\bкажд(?:ую|ое|ый|ые)\s+(?:суббот|воскрес)", repaired, flags=re.IGNORECASE):
            repaired = _build_recurring_event_fallback_line(candidate)
            reasons.append("recurring_occurrence_first")
    elif event_shape == "festival":
        if re.search(r"\b15\s*[–-]\s*17\s+(?:may|мая)\b", repaired, flags=re.IGNORECASE):
            repaired = _build_festival_fallback_line(candidate)
            reasons.append("festival_current_window")
    elif event_shape == "bookable_activity":
        if re.search(r"\b(?:доступно\s+с|available\s+from|с\s+2[23]\s+мая)\b", repaired, flags=re.IGNORECASE):
            repaired = _build_bookable_activity_fallback_line(candidate)
            reasons.append("bookable_weekend_language")
    if re.search(r"\bГМ\b", repaired):
        repaired = re.sub(r"\bГМ\b", "Greater Manchester", repaired)
        reasons.append("gm_abbreviation")
    if re.search(r"заброшенн\w*\s+(?:паб|здани|мотел|объект).{0,80}\bзакры", repaired, re.IGNORECASE | re.DOTALL):
        repaired = re.sub(r"\bбыли\s+закрыты\b|\bбыл\s+закрыт\b|\bзакрыли\b", "обезопасят", repaired, flags=re.IGNORECASE)
        reasons.append("abandoned_building_contradiction")
    return repaired, reasons


def _service_fallback_subject(title: str) -> str:
    cleaned = re.sub(r"\s*\|\s*News and Events\s*$", "", str(title or ""), flags=re.IGNORECASE)
    replacements = (
        (r"^NHS England's Independent Assurance Review published today$", "NHS England опубликовала независимый обзор качества"),
        (r"^Greater Manchester Mental Health NHS Foundation Trust appoints new Chief Executive$", "GMMH назначил нового руководителя"),
        (r"\bChief Executive\b", "руководителя"),
        (r"\bIndependent Assurance Review\b", "независимый обзор качества"),
        (r"\bpublished today\b", "опубликован сегодня"),
        (r"\bappoints\b", "назначает"),
    )
    for pattern, repl in replacements:
        cleaned = re.sub(pattern, repl, cleaned, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cleaned).strip(" .") or "опубликовано обновление"


def _build_public_service_fallback_line(candidate: dict) -> str:
    source_label = str(candidate.get("source_label") or "Public services").strip()
    title = _service_fallback_subject(str(candidate.get("title") or ""))
    summary = str(candidate.get("summary") or candidate.get("lead") or "").strip()
    if "progress" in summary.lower() and "improv" in summary.lower():
        detail = "в сообщении говорится о прогрессе в улучшении качества и безопасности помощи"
    elif "appointment" in summary.lower() or "chief executive" in summary.lower():
        detail = "это кадровое обновление может повлиять на управление сервисами"
    elif summary:
        detail = "это обновление касается работы сервиса и доступа к помощи"
    else:
        detail = "это обновление касается работы публичного сервиса"
    return (
        f"• {source_label}: {title}; {detail}. "
        "Если вы пользуетесь этим сервисом, уточните актуальные изменения на странице организации."
    )


def _transport_empty_line(project_root: Path) -> str:
    report = read_json(project_root / "data" / "state" / "collector_report.json", {})
    transport = ((report.get("categories") or {}).get("transport") or {}) if isinstance(report, dict) else {}
    checked = bool(transport.get("checked"))
    health = [entry for entry in (transport.get("source_health") or []) if isinstance(entry, dict)]
    if not checked:
        return (
            '• Транспорт: источники TfGM/Metrolink сегодня не были проверены — '
            'перед поездкой проверьте официальный сервис. '
            '<a href="https://tfgm.com/">TfGM</a>'
        )
    failed = [entry for entry in health if entry.get("errors")]
    if health and len(failed) == len(health):
        return (
            '• Транспорт: TfGM/Metrolink сегодня недоступны для проверки — '
            'перед поездкой проверьте официальный сервис. '
            '<a href="https://tfgm.com/">TfGM</a>'
        )
    return (
        '• Транспорт: проверенные TfGM/Metrolink источники не дали серьёзных '
        'сбоев для выпуска — перед поездкой всё равно проверьте маршрут. '
        '<a href="https://tfgm.com/">TfGM</a>'
    )


def _is_late_may_bank_holiday(day: date) -> bool:
    if day.month != 5 or day.weekday() != 0:
        return False
    return day + timedelta(days=7) > date(day.year, 5, 31)


def _current_weekend_start() -> date:
    # Weekend planning is shown from Thursday, but the item window starts on
    # Friday so Thursday one-offs do not crowd out bank-holiday weekend picks.
    today = now_london().date()
    friday = today + timedelta(days=(4 - today.weekday()) % 7)
    if today.weekday() in {5, 6} or _is_late_may_bank_holiday(today):
        return today
    return friday


def _current_weekend_end() -> date:
    today = now_london().date()
    days_until_sunday = (6 - today.weekday()) % 7
    sunday = today + timedelta(days=days_until_sunday)
    bank_monday = sunday + timedelta(days=1)
    if _is_late_may_bank_holiday(bank_monday):
        return bank_monday
    return sunday


def _has_current_weekend_recurring_signal(text: str) -> bool:
    lowered = str(text or "").lower()
    today = _current_weekend_start()
    weekend_end = _current_weekend_end()
    weekdays = {
        date.fromordinal(ordinal).weekday()
        for ordinal in range(today.toordinal(), weekend_end.toordinal() + 1)
    }
    if 5 in weekdays and re.search(r"\b(?:(?:every|weekly)\s+saturdays?|saturdays)\b|кажд[а-яё]*\s+суббот", lowered):
        return True
    if 6 in weekdays and re.search(r"\b(?:(?:every|weekly)\s+sundays?|sundays)\b|кажд[а-яё]*\s+воскрес", lowered):
        return True
    return False


def _is_outside_current_weekend_candidate(candidate: dict, line: str = "") -> bool:
    if str(candidate.get("primary_block") or "") != _WEEKEND_BLOCK:
        return False
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            candidate.get("source_url"),
            line,
        )
    )
    dates = _date_signals(text)
    today = _current_weekend_start()
    weekend_end = _current_weekend_end()
    if any(today <= day <= weekend_end for day in dates):
        return False
    if _has_current_weekend_recurring_signal(text):
        return False
    return bool(dates)


def _is_expired_event_candidate(candidate: dict, line: str = "") -> bool:
    if str(candidate.get("primary_block") or "") not in _EVENT_BLOCKS:
        return False
    event_day = _parse_day(candidate.get("published_at"))
    if not event_day or event_day >= now_london().date():
        return False
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            candidate.get("source_url"),
            line,
        )
    )
    return not _future_date_signal(text)


def _weekend_activity_score(candidate: dict, line: str) -> float:
    blob = " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            line,
        )
    ).lower()
    score = 0.0
    if _future_date_signal(blob):
        score += 40
    if re.search(r"\b(?:market|makers?|car boot|food festival|festival|fair|flea)\b", blob):
        score += 35
    if re.search(r"\b(?:flower festival|jazz festival|car boot|makers market|food festival)\b", blob):
        score += 25
    if re.search(r"\b(?:designmynight|alcotraz|treasure hunt|escape room|cocktail bar|big manchester bake|kitty yoga|bottomless)\b", blob):
        score -= 55
    if re.search(r"\b(?:today|tomorrow|saturday|sunday|сегодня|завтра|суббот|воскрес|16\s*(?:мая|may)|17\s*(?:мая|may))\b", blob):
        score += 25
    if re.search(r"\b(?:free|ticket|tickets|booking|book|билет|бесплат|вход)\b|£\s*\d", blob):
        score += 10
    if re.search(r"\b(?:until|до)\s+(?:20\d{2}|december|декабр)", blob):
        score -= 25
    return score


def _event_planning_score(candidate: dict, line: str) -> float:
    blob = " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            line,
        )
    ).lower()
    today = now_london().date()
    dates = _date_signals(blob)
    future_dates = sorted(day for day in dates if day >= today)
    score = 0.0
    if future_dates:
        days_out = (future_dates[0] - today).days
        if 1 <= days_out <= 7:
            score += 45
        elif days_out == 0:
            score += 10
        elif days_out <= 30:
            score += 15
        if len(future_dates) >= 2 and (max(future_dates) - min(future_dates)).days > 30:
            score -= 25
    if re.search(r"\b(?:festival|market|makers?|car boot|concert|gig|comedy|workshop|talk|trail)\b", blob):
        score += 25
    if re.search(r"\b(?:free|бесплат|£\s*\d|ticket|tickets|booking|book)\b", blob):
        score += 10
    if re.search(r"\b(?:film|cinema|screening|15\)|12a\)|pg\))\b", blob):
        score -= 20
    if re.search(r"\b(?:exhibition|выставк).*\b(?:october|november|december|20\d{2})\b", blob):
        score -= 15
    if re.search(r"\b(?:weekly|every|кажд)\b", blob):
        score -= 8
    return score


# Canonical money normaliser: maps £150m, £150 million, £150млн,
# £150 миллионов, £150мн all to (150.0, "m"). Used by the hallucination
# check so the writer doesn't reject its own LLM lines that translate
# "£230m" to "£230млн" — the previous string comparison flagged those
# as missing from evidence and silently lost real leads (Wigan £230m,
# Metrolink £150m, council £11.8m, …).
_MONEY_TOKEN_RE = re.compile(
    r"£\s*(\d[\d.,]*)\s*"
    r"(k|m|bn|млн|млрд|тыс|миллионов?|миллиардов?|тысяч)?",
    re.IGNORECASE,
)
_UNIT_MAP = {
    "":         "",
    "k":        "k",
    "тыс":      "k",
    "тысяч":    "k",
    "m":        "m",
    "млн":      "m",
    "миллион":  "m",
    "миллионов":"m",
    "bn":       "bn",
    "млрд":     "bn",
    "миллиард": "bn",
    "миллиардов":"bn",
}


def _normalize_money(amount_str: str, unit_str: str) -> tuple[float, str] | None:
    """Return (amount, canonical_unit) or None if the token doesn't parse.
    Handles £230m / £230млн / £230 million / £230 миллионов as the same
    canonical (230.0, 'm')."""
    s = amount_str.replace(",", ".").replace(" ", "")
    try:
        amount = float(s)
    except ValueError:
        return None
    unit_key = (unit_str or "").lower().strip()
    canonical = _UNIT_MAP.get(unit_key, "")
    return (amount, canonical)


def _extract_money(text: str) -> set[tuple[float, str]]:
    """Pull every £-amount out of `text` as a set of canonical tuples."""
    found: set[tuple[float, str]] = set()
    for m in _MONEY_TOKEN_RE.finditer(text or ""):
        norm = _normalize_money(m.group(1), m.group(2) or "")
        if norm is not None:
            found.add(norm)
    return found


def _money_amounts_match(line_amount: float, evidence_amounts: set[tuple[float, str]]) -> bool:
    """Return True if `line_amount` reasonably equals any evidence amount.

    Allowed editorial freedom (and ONLY this):
      1. Exact match (any unit).
      2. LLM rounded a *fractional* evidence value to the nearest whole.
         Only fires when the evidence value has a non-zero fractional
         part. "£11.8m → £12 млн" passes; "£100m → £105 млн" doesn't,
         because £100m has no fraction to round.
    """
    for ea, _ in evidence_amounts:
        if abs(line_amount - ea) < 0.01:
            return True
        has_fraction = abs(ea - round(ea)) > 0.001
        if has_fraction and abs(line_amount - round(ea)) < 0.01:
            return True
    return False


def _hallucination_flags(candidate: dict, line: str) -> list[str]:
    """Flag £-sums in `line` that don't appear in upstream
    evidence/title/summary/lead. Normalised comparison via _extract_money
    so £230m ↔ £230млн match; also accepts editorial rounding via
    _money_amounts_match (so £11.8m → £12 млн doesn't trip).
    """
    evidence_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    )
    flags: list[str] = []
    line_amounts = _extract_money(line)
    if not line_amounts:
        return flags
    evidence_amounts = _extract_money(evidence_blob)
    for amount, unit in line_amounts:
        if (amount, unit) in evidence_amounts:
            continue
        if _money_amounts_match(amount, evidence_amounts):
            continue
        flags.append(f"Pound amount £{amount:g}{unit} not present in evidence_text.")
        break
    return flags


# Source-tier weights for «Городской радар» ordering. Higher = surfaces first.
# Cap of 12 truncates the tail, so anything below ~30 is effectively cut.
_CITY_WATCH_SOURCE_WEIGHTS: dict[str, int] = {
    # GM-wide political authority — highest editorial priority.
    "GMCA": 120,
    "Manchester Council": 100,
    "Salford Council": 95,
    "Stockport Council": 95,
    "Trafford Council": 95,
    "Oldham Council": 90,
    "Rochdale Council": 90,
    "Bolton Council": 90,
    "Bury Council": 90,
    "Tameside Council": 90,
    "Wigan Council": 90,
    # Independent local journalism with reporting (not press releases).
    "The Mill": 110,
    "The Manc": 85,
    "Manchester Mill": 110,
    "I Love Manchester": 60,
    # NHS / emergency services.
    "GMMH": 70,
    "GMP": 80,
    # Universities — institutional PR, usually low signal for residents.
    "University of Manchester": 25,
    "University of Salford": 25,
    "Manchester Metropolitan University": 25,
}
_CITY_WATCH_DEFAULT_WEIGHT = 50


def _city_watch_score(candidate: dict) -> float:
    """Editorial priority for «Городской радар» (higher = surfaces first).

    Combines source-tier weight with content signals: presence of GM boroughs,
    £-sums, dates, named people. Penalises academic / generic press-release
    language so university feeds don't crowd out actual city news.
    """
    source_label = str(candidate.get("source_label") or "").strip()
    score = float(_CITY_WATCH_SOURCE_WEIGHTS.get(source_label, _CITY_WATCH_DEFAULT_WEIGHT))

    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    ).lower()

    # Borough mentions — real GM signal.
    borough_hits = sum(
        1
        for borough in ("manchester", "salford", "trafford", "stockport", "tameside",
                         "oldham", "rochdale", "bury", "bolton", "wigan")
        if borough in blob
    )
    score += min(borough_hits, 3) * 5

    # Concrete signals readers care about: £ amounts, dates, percentages.
    if re.search(r"£\s*\d", blob):
        score += 15
    if re.search(r"\b(?:january|february|march|april|may|june|july|august|"
                 r"september|october|november|december|января|февраля|марта|"
                 r"апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b", blob):
        score += 8
    if re.search(r"\b\d{1,3}%\b|\b\d{4,6}\s+(?:residents|people|жител)", blob):
        score += 10

    # Academic / generic PR markers — drop these to the bottom.
    academic_markers = (
        "research", "researcher", "электрон", "graphene", "lecture",
        "vice-chancellor", "chancellor", "academic", "professor", "phd",
        "campus", "students meet", "submit your taught course",
        "datadobi", "storage optimisation",
    )
    if any(marker in blob for marker in academic_markers):
        score -= 35

    # Generic council PR with no specific news beat.
    generic_pr_markers = (
        "named greater manchester town of culture",
        "community champions",
        "capital grant winners",
        "parting gifts",
        "celebration",
        "tea party",
        "lord mayor",
    )
    if any(marker in blob for marker in generic_pr_markers):
        score -= 10

    # Title length under 50 chars often means slogany PR header.
    title = str(candidate.get("title") or "")
    if len(title) < 30:
        score -= 5

    # Evidence depth — long evidence_text usually means a real article.
    evidence_len = len(str(candidate.get("evidence_text") or ""))
    if evidence_len >= 600:
        score += 10
    elif evidence_len < 200:
        score -= 8

    return score


def _section_priority_score(candidate: dict, section_name: str, line: str) -> float:
    """Shared reader-value score used when capped sections choose survivors."""
    attach_editorial_contract(candidate)
    value_item = dict(candidate)
    value_item["included"] = True
    score = float(reader_value_score(value_item))
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    tier = str(contract.get("publish_tier") or candidate.get("publish_tier") or "")
    score += {
        "must_include": 80.0,
        "strong": 35.0,
        "optional": 5.0,
        "filler": -45.0,
        "reject": -200.0,
    }.get(tier, 0.0)
    event_shape = str(contract.get("event_shape") or candidate.get("event_shape") or "")
    if event_shape == "bookable_activity":
        score -= 45.0
    elif event_shape in {"festival", "recurring"}:
        score += 22.0
    story_type = str(contract.get("story_type") or "")
    if story_type in {"human_interest", "soft_news", "research"}:
        score -= 35.0
    completeness = candidate.get("event_schema_completeness")
    if isinstance(completeness, dict) and completeness.get("applies"):
        score += (float(completeness.get("score") or 0) - 50.0) / 5.0
    if section_name == "Городской радар":
        score += _city_watch_score(candidate) / 4.0
    elif section_name == "Выходные в GM":
        score += _weekend_activity_score(candidate, line) / 4.0
    elif section_name == "Что важно в ближайшие 7 дней":
        score += _event_planning_score(candidate, line) / 4.0
    return score


def _draft_line_quality_errors(candidate: dict, line: str) -> list[str]:
    text = str(line or "").strip()
    errors: list[str] = []
    if not text:
        return ["Missing draft_line."]
    if not text.startswith("• "):
        errors.append("draft_line must start with bullet marker.")
    if "<a " in text.lower():
        errors.append("draft_line must not include source anchor HTML.")
    if re.search(r"\*\*.+?\*\*", text) or re.search(r"(?<!\*)\*(?!\s).+?(?<!\s)\*(?!\*)", text):
        errors.append("draft_line must not use Markdown emphasis markers.")
    if not _contains_cyrillic(text):
        errors.append("draft_line must contain normal Russian prose.")
    normalized = re.sub(r"\s+", " ", text)
    if len(normalized) < 45:
        errors.append("draft_line is too short to be a self-contained item.")
    category = str(candidate.get("category") or "").strip()
    sentence_count = len(re.findall(r"[.!?]", text))
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and sentence_count < 1:
        errors.append("draft_line must contain at least one complete sentence.")
    block_key = str(candidate.get("primary_block") or "").strip()
    # Sequential gate:
    #  1. Always-short blocks (tickets, weekend) — no min_chars at all.
    #  2. Relaxable event blocks (next_7_days, future_announcements,
    #     russian_events) — apply min ONLY when evidence_text was rich
    #     enough to write a full card. If source only gave us a thin
    #     280-char teaser, accept whatever LLM produced rather than
    #     dropping a real event for being a sentence too short.
    is_transport_block = block_key == "transport"
    if category in LONG_FORMAT_CATEGORIES and block_key not in SHORT_EVENT_BLOCKS and not is_transport_block:
        evidence_len = len(str(candidate.get("evidence_text") or "").strip())
        evidence_rich = evidence_len >= EVENT_RELAX_EVIDENCE_THRESHOLD
        skip_min = (block_key in EVENT_BLOCKS_RELAXABLE) and not evidence_rich
        if not skip_min:
            if len(normalized) < LONG_FORMAT_MIN_CHARS:
                errors.append(
                    f"draft_line for long-format category needs ≥{LONG_FORMAT_MIN_CHARS} chars (got {len(normalized)})."
                )
        if sentence_count < LONG_FORMAT_MIN_SENTENCES and block_key != "city_watch":
            errors.append(
                f"draft_line for long-format category needs ≥{LONG_FORMAT_MIN_SENTENCES} sentences (got {sentence_count})."
            )
    lowered = text.lower()
    for marker in _BAD_EDITORIAL_PROSE_MARKERS:
        if marker in lowered:
            errors.append(f"draft_line contains bad editorial prose marker: {marker}.")
            break
    errors.extend(_sanity_flags(candidate, text))
    for invariant in copy_invariant_errors(candidate, text):
        errors.append(f"copy invariant failed: {invariant}.")
    errors.extend(_hallucination_flags(candidate, text))
    # Thin-evidence + long-draft = LLM padded a teaser into a vague card.
    # We only check long-format categories (city news / events / business etc.) —
    # transport / weather are intentionally short. Football already has its own
    # "return draft_line=\"\"" rule in the prompt.
    if category in LONG_FORMAT_CATEGORIES and category != "football" and not is_transport_block:
        evidence = str(candidate.get("evidence_text") or candidate.get("summary") or candidate.get("lead") or "")
        evidence_meaningful = len(re.sub(r"\s+", " ", evidence).strip())
        draft_len = len(normalized)
        # Concrete signals: numbers, £-amount, date, capitalised proper noun pair.
        has_concrete = bool(
            re.search(r"\b\d{2,}", text)
            or re.search(r"£\s*\d", text)
            or re.search(r"\b(?:января|февраля|марта|апреля|мая|июня|июля|"
                         r"августа|сентября|октября|ноября|декабря)\b", text, re.IGNORECASE)
            or re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", text)
        )
        if evidence_meaningful < 150 and draft_len > 220 and not has_concrete:
            errors.append(
                f"draft_line padded from thin evidence "
                f"(evidence={evidence_meaningful}c, draft={draft_len}c, no concrete signal)."
            )
    return errors


def write_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "writer_report.json"

    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])
    llm_degraded, llm_rewrite_report = _llm_rewrite_is_degraded(state_dir)
    sections = {heading: [] for heading in PRIMARY_BLOCKS.values()}
    # Parallel list of source_labels per section (same indices as sections[*]).
    # Used to apply SECTION_MAX_PER_SOURCE caps at render time.
    section_sources: dict[str, list[str]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    # Parallel list of candidate fingerprints per section. This is written
    # after all caps/filtering so published_facts only records items that
    # actually reached the Telegram HTML.
    section_fingerprints: dict[str, list[str]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    # Editorial priority score per line — populated only for «Городской радар»
    # where we re-sort candidates before truncation so the cap drops the
    # weakest items (PR releases) rather than whatever happened to come last.
    section_scores: dict[str, list[float]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    section_titles: dict[str, list[str]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    errors: list[str] = []
    warnings: list[str] = []
    quality_counts = {
        "included_candidates": 0,
        "rendered_candidates": 0,
        "blocked_for_quality": 0,
        "held_for_editorial_quality": 0,
        "dropped_missing_draft_line": 0,
        "dropped_english_passthrough": 0,
        "dropped_low_quality": 0,
    }
    dropped_candidates: list[dict[str, object]] = []
    degraded_shrink_dropped: list[dict[str, object]] = []
    global_budget_dropped: list[dict[str, object]] = []

    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        attach_editorial_contract(candidate)
        quality_counts["included_candidates"] += 1
        contract_drop_reason = _contract_public_drop_reason(candidate)
        if contract_drop_reason and candidate.get("manual_override") != "force_include":
            warnings.append(f"Candidate #{index} dropped by editorial contract: {contract_drop_reason}.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": [contract_drop_reason],
                }
            )
            continue
        if (
            candidate.get("editorial_status") == "borderline"
            and candidate.get("manual_override") != "force_include"
        ):
            warnings.append(f"Candidate #{index} held for manual review: borderline editorial status.")
            quality_counts["held_for_editorial_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["Held for manual review: borderline editorial status."],
                }
            )
            continue
        if candidate.get("validation_errors"):
            errors.append(f"Candidate #{index} is include=true but still has validation_errors.")
            quality_counts["blocked_for_quality"] += 1
            continue
        if not candidate.get("source_url") or not candidate.get("source_label"):
            errors.append(f"Candidate #{index} is include=true but missing source reference.")
            quality_counts["blocked_for_quality"] += 1
            continue
        # practical_angle is no longer a hard gate: the new long-format prompts
        # derive the "so what" sentence directly from evidence_text, so an
        # empty / placeholder practical_angle should not block rendering.
        practical_angle = str(candidate.get("practical_angle") or "").strip()
        if not practical_angle:
            warnings.append(f"Candidate #{index}: empty practical_angle (kept).")
        elif is_placeholder_practical_angle(practical_angle):
            warnings.append(f"Candidate #{index}: placeholder practical_angle (kept).")
        if str(candidate.get("primary_block") or "") == "last_24h" and not str(candidate.get("published_at") or "").strip():
            errors.append(f"Candidate #{index} is in last_24h without published_at.")
            quality_counts["blocked_for_quality"] += 1
            continue
        if _is_outside_current_weekend_candidate(candidate):
            warnings.append(f"Candidate #{index} dropped: outside current weekend window.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["Outside current weekend window."],
                }
            )
            continue
        if _is_expired_event_candidate(candidate):
            warnings.append(f"Candidate #{index} dropped: expired event date.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["Expired event date."],
                }
            )
            continue

        block_key = str(candidate.get("primary_block") or "").strip()
        section_name = PRIMARY_BLOCKS.get(block_key)
        if not section_name:
            errors.append(f"Candidate #{index} has unknown primary_block: {block_key!r}.")
            quality_counts["blocked_for_quality"] += 1
            continue

        line = str(candidate.get("draft_line") or "").strip()
        title = str(candidate.get("title") or "").strip()
        lead = str(candidate.get("lead") or "").strip()
        summary = str(candidate.get("summary") or "").strip()
        source_label = str(candidate.get("source_label") or "").strip()
        source_url = str(candidate.get("source_url") or "").strip()
        category = str(candidate.get("category") or "").strip()

        if _normalize_text_key(lead) and _normalize_text_key(lead) == _normalize_text_key(summary):
            summary = ""

        english_detected = False
        if category in {"media_layer", "gmp", "public_services", "city_news", "council", "transport", "venues_tickets", "russian_speaking_events", "culture_weekly", "football", "tech_business", "food_openings"}:
            english_fields = [field for field in (lead, summary, title) if _looks_like_untranslated_english(field)]
            if english_fields:
                english_detected = True

        if not line and category == "transport":
            # Tier 4 transport safety net: never drop a transport alert.
            # If transport_fill couldn't extract structure AND LLM tier-3
            # returned empty, fall back to a minimal title-based stub so
            # the reader still sees that something is happening.
            stub_title = re.sub(r"\s+", " ", title).strip()
            # Take the first phrase up to the first dash / pipe / period.
            first_phrase = re.split(r"\s+[-–|]\s+|\.\s+", stub_title, maxsplit=1)[0]
            first_phrase = first_phrase[:120].rstrip()
            label = source_label or "Транспорт"
            line = f"• {label}: {first_phrase} — подробности в источнике."
            warnings.append(f"Candidate #{index}: transport tier-4 stub used (no extractor/LLM draft_line).")
            logger.info("TIER4 transport stub | %s | %s", block_key, first_phrase[:80])

        if not line and category == "venues_tickets":
            line = _build_ticket_fallback_line(candidate)
            warnings.append(f"Candidate #{index}: ticket fallback stub used (no LLM draft_line).")
            logger.info("TIER4 ticket stub | %s | %s", block_key, title[:80])

        if not line and category == "public_services":
            line = _build_public_service_fallback_line(candidate)
            warnings.append(f"Candidate #{index}: public-services fallback stub used (no LLM draft_line).")
            logger.info("TIER4 public_services stub | %s | %s", block_key, title[:80])

        if not line:
            if category in REQUIRE_DRAFT_LINE_CATEGORIES:
                warnings.append(f"Candidate #{index} dropped: no model draft_line for {category!r}.")
                logger.info("DROP no_draft_line | %s | %s | %s", category, block_key, title[:80])
                quality_counts["dropped_missing_draft_line"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": ["Missing draft_line."],
                    }
                )
                continue
            if english_detected:
                warnings.append(f"Candidate #{index} dropped: English passthrough without translation.")
                logger.info("DROP english_passthrough | %s | %s | %s", category, block_key, title[:80])
                quality_counts["dropped_english_passthrough"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": ["Untranslated English."],
                    }
                )
                continue
            headline = lead or title or summary
            rendered_parts: list[str] = []
            if headline:
                rendered_parts.append(html.escape(headline.rstrip(".")) + ".")
            if _summary_is_useful(summary, headline):
                rendered_parts.append(html.escape(summary.rstrip(".")) + ".")
            line = "• " + " ".join(rendered_parts).strip()

        scrubbed_line, removed_vague_endings = scrub_vague_ending(line)
        if removed_vague_endings:
            warnings.append(
                f"Candidate #{index}: removed vague ending(s): {', '.join(removed_vague_endings)}."
            )
            line = scrubbed_line
        if _line_claims_future_ticket_sale(candidate, line):
            line = _build_ticket_fallback_line(candidate)
            warnings.append(
                f"Candidate #{index}: replaced stale ticket-sale wording with deterministic ticket line."
            )
        line, repair_reasons = _repair_editorial_contract_line(candidate, line)
        if repair_reasons:
            warnings.append(
                f"Candidate #{index}: editorial contract repaired line ({', '.join(repair_reasons)})."
            )

        draft_line_errors = _draft_line_quality_errors(candidate, line)
        if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
            warnings.append(
                f"Candidate #{index} dropped: draft_line quality issues ({'; '.join(draft_line_errors)})."
            )
            logger.info("DROP low_quality | %s | %s | %s | %s", category, block_key, title[:80], "; ".join(draft_line_errors))
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": title,
                    "category": category,
                    "primary_block": block_key,
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": draft_line_errors,
                }
            )
            continue

        # Scrub LLM placeholder genres like "Madison Beer, жанр не указан" /
        # "Avatar, другой жанр" — the rewrite prompt says "не выдумывай жанр"
        # but gpt-4o-mini still tacks these phrases on. Strip them post-hoc.
        line = re.sub(r",\s*(?:жанр\s+не\s+указан|другой\s+жанр|жанр\s+не\s+определ[её]н|жанр\s+неизвестен)\s*(?=[.!?]|$)", "", line, flags=re.IGNORECASE)
        # Restore English spellings for GM toponyms (Altrincham, Bury, Wigan, ...).
        line = restore_english_toponyms(line)
        if candidate.get("is_lead"):
            # Lead story: no bullet, bold first sentence, placed in main_story block
            line = line.lstrip("• ").strip()
            sentences = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)
            if len(sentences) == 2:
                line = f"<b>{sentences[0]}</b> {sentences[1]}"
            else:
                line = f"<b>{line}</b>"
            line = preserve_place_names(line)
            line = _attach_source_anchor(line, source_url, source_label)
            sections.setdefault("Главная история дня", []).insert(0, line)
            section_sources.setdefault("Главная история дня", []).insert(0, source_label)
            section_scores.setdefault("Главная история дня", []).insert(0, 0.0)
            section_fingerprints.setdefault("Главная история дня", []).insert(0, str(candidate.get("fingerprint") or "").strip())
            section_titles.setdefault("Главная история дня", []).insert(0, title)
        else:
            if not line.startswith("• "):
                line = f"• {line}"
            line = preserve_place_names(line)
            line = _attach_source_anchor(line, source_url, source_label)
            sections[section_name].append(line)
            section_sources[section_name].append(source_label)
            section_fingerprints[section_name].append(str(candidate.get("fingerprint") or "").strip())
            section_scores[section_name].append(_section_priority_score(candidate, section_name, line))
            section_titles[section_name].append(title)

    missing_draft_count = quality_counts["dropped_missing_draft_line"]
    if missing_draft_count:
        warnings.append(
            f"Writer dropped {missing_draft_count} included candidate(s) with missing draft_line — digest continues."
        )

    backfilled_today_focus = _backfill_today_focus(
        sections,
        section_sources,
        section_scores,
        section_fingerprints,
        section_titles,
        tuple(
            section
            for section in TODAY_FOCUS_BACKFILL_SECTIONS
            if not (llm_degraded and section == "Городской радар")
        ),
    )
    if backfilled_today_focus:
        warnings.append(
            f"Writer backfilled «{TODAY_FOCUS_SECTION}» with {backfilled_today_focus} item(s) from other practical sections."
        )
    if not sections.get("Общественный транспорт сегодня"):
        sections["Общественный транспорт сегодня"] = [_transport_empty_line(project_root)]
        section_sources["Общественный транспорт сегодня"] = ["TfGM"]
        section_scores["Общественный транспорт сегодня"] = [0.0]
        section_fingerprints["Общественный транспорт сегодня"] = [""]
        section_titles["Общественный транспорт сегодня"] = ["Транспорт проверен"]
        warnings.append("Writer added honest empty-transport coverage line.")

    rendered: list[str] = [_title_line(), ""]

    # "Выходные в GM" показываем только с четверга (weekday >= 3)
    london_weekday = now_london().weekday()  # 0=Пн … 6=Вс
    show_weekend = london_weekday >= 3

    ordered_sections = [
        "Погода",
        "Главная история дня",
        "Что произошло за 24 часа",
        "Общественный транспорт сегодня",
        "Что важно сегодня",
        *(["Выходные в GM"] if show_weekend else []),
        "Городской радар",
        "Что важно в ближайшие 7 дней",
        "Дальние анонсы",
        "Билеты / Ticket Radar",
        "Крупные концерты вне GM",
        "Русскоязычные концерты и стендап UK",
        "Еда, открытия и рынки",
        "IT и бизнес",
        "Футбол",
        "Радар по районам",
    ]
    section_counts: dict[str, int] = {}
    rendered_candidate_fingerprints: list[str] = []
    visible_item_count = 0
    for section_name in ordered_sections:
        lines = sections.get(section_name, [])
        if not lines:
            continue
        srcs = section_sources.get(section_name, [])
        scores = section_scores.get(section_name, [])
        fps = section_fingerprints.get(section_name, [])
        titles = section_titles.get(section_name, [])
        # Re-rank capped sections so the cap keeps practical local value,
        # rather than whichever source happened to run first.
        if section_name in SECTION_MAX_ITEMS and scores:
            triples = sorted(
                zip(lines, srcs + [""] * (len(lines) - len(srcs)),
                    scores + [0.0] * (len(lines) - len(scores)),
                    fps + [""] * (len(lines) - len(fps)),
                    titles + [""] * (len(lines) - len(titles))),
                key=lambda triple: triple[2],
                reverse=True,
            )
            lines = [t[0] for t in triples]
            srcs = [t[1] for t in triples]
            fps = [t[3] for t in triples]
            scores = [t[2] for t in triples]
            titles = [t[4] for t in triples]
        per_source_cap = SECTION_MAX_PER_SOURCE.get(section_name)
        if per_source_cap:
            src_counts: dict[str, int] = {}
            filtered: list[str] = []
            filtered_srcs: list[str] = []
            filtered_fps: list[str] = []
            filtered_scores: list[float] = []
            filtered_titles: list[str] = []
            for idx, ln in enumerate(lines):
                src = srcs[idx] if idx < len(srcs) else ""
                if src_counts.get(src, 0) >= per_source_cap:
                    continue
                src_counts[src] = src_counts.get(src, 0) + 1
                filtered.append(ln)
                filtered_srcs.append(src)
                filtered_fps.append(fps[idx] if idx < len(fps) else "")
                filtered_scores.append(scores[idx] if idx < len(scores) else 0.0)
                filtered_titles.append(titles[idx] if idx < len(titles) else "")
            min_items = SECTION_MIN_ITEMS.get(section_name, 0)
            if not min_items or len(filtered) >= min_items or len(lines) < min_items:
                lines = filtered
                srcs = filtered_srcs
                fps = filtered_fps
                scores = filtered_scores
                titles = filtered_titles
        normal_cap = SECTION_MAX_ITEMS.get(section_name)
        degraded_cap = DEGRADED_LLM_SECTION_MAX_ITEMS.get(section_name) if llm_degraded else None
        cap = normal_cap
        if degraded_cap is not None:
            cap = min(cap, degraded_cap) if cap else degraded_cap
        if cap:
            if llm_degraded and degraded_cap is not None and len(lines) > cap:
                normal_cutoff = normal_cap if normal_cap is not None else len(lines)
                for idx in range(cap, min(len(lines), normal_cutoff)):
                    degraded_shrink_dropped.append(
                        {
                            "section": section_name,
                            "fingerprint": fps[idx] if idx < len(fps) else "",
                            "title": titles[idx] if idx < len(titles) else re.sub(r"<[^>]+>", "", lines[idx])[:120],
                            "source_label": srcs[idx] if idx < len(srcs) else "",
                            "reader_value_score": scores[idx] if idx < len(scores) else 0.0,
                            "reason": "LLM rewrite was degraded; held lower-priority item for review.",
                        }
                    )
            lines = lines[:cap]
            fps = fps[:cap]
            scores = scores[:cap]
            titles = titles[:cap]
        remaining_budget = PUBLIC_DIGEST_MAX_VISIBLE_ITEMS - visible_item_count
        if remaining_budget <= 0:
            for idx, ln in enumerate(lines):
                global_budget_dropped.append(
                    {
                        "section": section_name,
                        "fingerprint": fps[idx] if idx < len(fps) else "",
                        "title": titles[idx] if idx < len(titles) else re.sub(r"<[^>]+>", "", ln)[:120],
                        "source_label": srcs[idx] if idx < len(srcs) else "",
                        "reader_value_score": scores[idx] if idx < len(scores) else 0.0,
                        "reason": f"Public digest budget cap {PUBLIC_DIGEST_MAX_VISIBLE_ITEMS} reached.",
                    }
                )
            section_counts[section_name] = 0
            continue
        if len(lines) > remaining_budget:
            for idx in range(remaining_budget, len(lines)):
                global_budget_dropped.append(
                    {
                        "section": section_name,
                        "fingerprint": fps[idx] if idx < len(fps) else "",
                        "title": titles[idx] if idx < len(titles) else re.sub(r"<[^>]+>", "", lines[idx])[:120],
                        "source_label": srcs[idx] if idx < len(srcs) else "",
                        "reader_value_score": scores[idx] if idx < len(scores) else 0.0,
                        "reason": f"Public digest budget cap {PUBLIC_DIGEST_MAX_VISIBLE_ITEMS} reached.",
                    }
                )
            lines = lines[:remaining_budget]
            fps = fps[:remaining_budget]
            scores = scores[:remaining_budget]
            titles = titles[:remaining_budget]
        # Per-source / per-section caps can filter every remaining line —
        # don't emit a bare section header in that case, the release gate
        # rejects empty low-signal blocks.
        if not lines:
            section_counts[section_name] = 0
            continue
        section_counts[section_name] = len(lines)
        visible_item_count += len(lines)
        rendered_candidate_fingerprints.extend(fp for fp in fps if fp)
        rendered.append(f"<b>{section_name}</b>")
        rendered.extend(lines)
        rendered.append("")

    quality_counts["rendered_candidates"] = len(rendered_candidate_fingerprints)
    if degraded_shrink_dropped:
        warnings.append(
            "LLM degraded shrink held "
            f"{len(degraded_shrink_dropped)} lower-priority item(s) out of the digest."
        )
    if global_budget_dropped:
        warnings.append(
            "Public issue budget held "
            f"{len(global_budget_dropped)} lower-priority item(s) out of the digest."
        )

    draft_path.write_text("\n".join(rendered).strip() + "\n", encoding="utf-8")
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "warnings": warnings,
            "quality_counts": quality_counts,
            "section_counts": section_counts,
            "visible_item_count": visible_item_count,
            "public_digest_budget": {
                "max_visible_items": PUBLIC_DIGEST_MAX_VISIBLE_ITEMS,
                "dropped_count": len(global_budget_dropped),
                "dropped_items": global_budget_dropped[:80],
            },
            "backfilled_today_focus": backfilled_today_focus,
            "degraded_shrink": {
                "enabled": bool(llm_degraded),
                "llm_stage_status": str(llm_rewrite_report.get("stage_status") or "") if llm_rewrite_report else "",
                "caps": DEGRADED_LLM_SECTION_MAX_ITEMS if llm_degraded else {},
                "dropped_count": len(degraded_shrink_dropped),
                "dropped_items": degraded_shrink_dropped[:50],
            },
            "rendered_candidate_fingerprints": rendered_candidate_fingerprints,
            "dropped_candidates": dropped_candidates,
            "draft_path": str(draft_path.resolve()),
        },
    )
    return StageResult(
        not errors,
        "Writer stage completed." if not errors else "Writer stage found blocking issues.",
        report_path,
        draft_path,
    )
