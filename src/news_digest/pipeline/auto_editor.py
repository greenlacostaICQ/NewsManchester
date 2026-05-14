from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import re

from news_digest.pipeline.collector.extract import _extract_meta_description, _extract_paragraph_evidence
from news_digest.pipeline.collector.fetch import _fetch_text
from news_digest.pipeline.collector.summary import _clean_snippet
from news_digest.pipeline.common import (
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


_EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
}
_STALE_EVENT_CATEGORIES = {
    "public_services",
    "tech_business",
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
}
_EVENT_SECTION_NAMES = {
    "Выходные в GM",
    "Что важно в ближайшие 7 дней",
    "Дальние анонсы",
    "Билеты / Ticket Radar",
    "Крупные концерты вне GM",
    "Русскоязычные концерты и стендап UK",
}
_DRAFT_REQUIRED_CATEGORIES = {
    "media_layer",
    "gmp",
    "council",
    "public_services",
    "food_openings",
    "transport",
    "venues_tickets",
    "russian_speaking_events",
    "culture_weekly",
    "football",
    "tech_business",
    "city_news",
}
_REPORTING_PAST_EVENT_MARKERS = (
    "report",
    "reported",
    "review",
    "statement",
    "announced",
    "confirmed",
    "investigation",
    "after",
    "following",
    "отчет",
    "сообщ",
    "объяв",
    "подтверд",
    "расслед",
)
_VAGUE_TRANSPORT_MARKERS = (
    "одной из основных линий",
    "одна из главных линий",
    "major line",
    "main line",
)
_TRANSPORT_MODE_PATTERN = re.compile(
    r"\b(?:автобус|трамва|поезд|железнодорож|метро|tram|bus|rail|train|metrolink|transpennine)\b",
    re.IGNORECASE,
)
_CULTURE_FACT_PATTERN = re.compile(
    r"\b(?:\d{1,2}:\d{2}|£\s*\d|\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b|"
    r"\b\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b|"
    r"street|road|avenue|hall|theatre|gallery|museum|venue|tickets?|билет|адрес|зал|театр|галерея|музей)\b",
    re.IGNORECASE,
)
_CULTURE_MEDIUM_PATTERN = re.compile(
    r"\b(?:film|movie|screening|cinema|play|theatre|show|concert|gig|festival|exhibition|gallery|workshop|comedy|stand-?up|"
    r"tour|orchestra|symphony|opera|live|final|league|sport|netball|"
    r"фильм|кино|показ|спектакль|театр|шоу|концерт|фестиваль|выставка|галерея|мастер-класс|стендап|тур|опера|оркестр|финал|спорт)\b",
    re.IGNORECASE,
)
_UKRAINIAN_MARKERS = re.compile(
    r"[іїєґІЇЄҐ]|\b(?:квитки|вистава|гурт|після|сьогодні|місто|україн)\b",
    re.IGNORECASE,
)
_ENGLISH_PROSE_MARKERS = re.compile(
    r"\b(?:the|and|for|with|from|after|following|today|launched|confirmed|announced|tickets?|event|show)\b",
    re.IGNORECASE,
)
_LINE_NAMES = (
    "Bury line",
    "Altrincham line",
    "Eccles line",
    "Ashton line",
    "Rochdale line",
    "Oldham line",
    "East Manchester line",
    "Airport line",
    "Trafford Park line",
    "East Didsbury line",
)
_PARTY_REPLACEMENTS = (
    (r"\bLabour\s+(councillor|MP)\b", "представитель Лейбористской партии"),
    (r"\bGreen\s+(councillor|MP)\b", "представитель Зеленой партии"),
    (r"\bLib\s*Dem\s+(councillor|MP)\b", "представитель Либеральных демократов"),
    (r"\bLiberal Democrat\s+(councillor|MP)\b", "представитель Либеральных демократов"),
    (r"\bConservative\s+(councillor|MP)\b", "представитель Консервативной партии"),
    (r"\bLabour\b", "Лейбористская партия"),
    (r"\bGreen Party\b", "Зеленая партия"),
    (r"\bLib\s*Dem\b", "Либеральные демократы"),
    (r"\bLiberal Democrats?\b", "Либеральные демократы"),
    (r"\bConservatives?\b", "Консервативная партия"),
)
_MONTHS_EN = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_MONTHS_RU = {
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
_MONTHS_RU_GENITIVE = {
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


def _candidate_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text", "source_url", "draft_line")
    )


def _visible_candidate_blob(candidate: dict) -> str:
    return " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text"))


def _append_reason(candidate: dict, note: str) -> None:
    existing = str(candidate.get("reason") or "").strip()
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note


def _enrich_candidate_from_source(candidate: dict) -> bool:
    url = str(candidate.get("source_url") or "").strip()
    if not url.startswith(("http://", "https://")):
        return False
    # Avoid repeated network work across reruns and within the same stage.
    if str(candidate.get("auto_editor_enrichment_status") or "") in {"ok", "failed"}:
        return False
    try:
        html_text = _fetch_text(url)
    except Exception as exc:  # noqa: BLE001
        candidate["auto_editor_enrichment_status"] = "failed"
        candidate["auto_editor_enrichment_error"] = str(exc)[:240]
        return False
    evidence = _extract_paragraph_evidence(html_text, str(candidate.get("title") or ""))
    if not evidence:
        evidence = _extract_meta_description(html_text)
    evidence = _clean_snippet(evidence)
    if len(evidence) < 60:
        candidate["auto_editor_enrichment_status"] = "failed"
        candidate["auto_editor_enrichment_error"] = "no usable source evidence"
        return False
    existing_evidence = str(candidate.get("evidence_text") or "")
    if evidence not in existing_evidence:
        candidate["evidence_text"] = (existing_evidence + " " + evidence).strip()[:2500]
    existing_summary = str(candidate.get("summary") or "")
    if len(existing_summary) < 120:
        candidate["summary"] = evidence[:700].rstrip()
    candidate["auto_editor_enrichment_status"] = "ok"
    return True


def _repair_translation_terms(text: str) -> tuple[str, bool]:
    original = str(text or "")
    repaired = original
    replacements = (
        (r"\bOnlyFans creator\b", "автор OnlyFans"),
        (r"\bOnlyFans model\b", "модель OnlyFans"),
        (r"\btakeaway\b", "еда навынос"),
        (r"\btake-away\b", "еда навынос"),
        (r"\bcivic reception\b", "официальный прием в городе"),
        (r"\baccess requests?\b", ""),
        (r"\bbooking fee\b", ""),
        (r"\bcouncillors\b", "депутаты совета"),
        (r"\bgreengrocer\b", "овощная лавка"),
        (r"\bopening\b", "открытие"),
        (r"\bbaby-and-carer\b", "для родителей с младенцами"),
        (r"\bcask ale\b", "живое пиво"),
        (r"\bdining room\b", "обеденный зал"),
        (r"\bpub[- ]menu\b", "меню паба"),
        (r"\bslot\b", "временной слот"),
    )
    for pattern, replacement in replacements:
        repaired = re.sub(pattern, replacement, repaired, flags=re.IGNORECASE)
    for pattern, replacement in _PARTY_REPLACEMENTS:
        repaired = re.sub(pattern, replacement, repaired, flags=re.IGNORECASE)
    repaired = re.sub(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b", "", repaired)
    repaired = re.sub(r"(?:для\s+)?access[- ]запросов?:?\s*(?:или\s*)?", "", repaired, flags=re.IGNORECASE)
    repaired = re.sub(r"Для бронирования\s*[:—-]?\s*$", "", repaired, flags=re.IGNORECASE)
    repaired = re.sub(r"\s+([,.!?;:])", r"\1", repaired)
    repaired = re.sub(r"\s{2,}", " ", repaired).strip()
    repaired = re.sub(r"\.\s*\.", ".", repaired)
    if "Bee Network" in repaired and not re.search(r"автобус|трамва|поезд|транспорт", repaired, re.IGNORECASE):
        repaired = repaired.replace("Bee Network", "транспорт Bee Network")
    return repaired, repaired != original


def repair_rendered_line(section_name: str, line: str) -> tuple[str | None, list[str]]:
    """Final HTML-level repair for known classes that can still leak post-write."""
    notes: list[str] = []
    anchor_match = re.search(r"\s*<a\s", line, flags=re.IGNORECASE)
    if anchor_match:
        visible_part = line[: anchor_match.start()]
        anchor_part = line[anchor_match.start() :]
    else:
        visible_part = line
        anchor_part = ""
    visible_part, changed = _repair_translation_terms(visible_part)
    if changed:
        notes.append("translated deterministic English terms")
    text = visible_part + anchor_part
    text = re.sub(r"\s+(?:Met Office|Open-Meteo)(?=\s*<a\s)", "", text)
    text = re.sub(r"^(•\s*Погода:\s*)Погода:\s*", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\.\s*\.\s*(?=<a\s)", ". ", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    lowered = re.sub(r"<[^>]+>", " ", text).lower()
    if any(marker in lowered for marker in _VAGUE_TRANSPORT_MARKERS):
        notes.append("dropped vague transport line")
        return None, notes
    if section_name in _EVENT_SECTION_NAMES and _has_only_past_dates(lowered, now_london().date()):
        notes.append("dropped stale event line")
        return None, notes
    return text, notes


def _split_mixed_summary(candidate: dict) -> bool:
    changed = False
    for field in ("summary", "evidence_text"):
        value = str(candidate.get(field) or "").strip()
        if not value:
            continue
        if len(re.findall(r"<h[12]\b|(?:^|\s)#{1,2}\s+|(?:^|\s)###\s+", value, flags=re.IGNORECASE)) > 1:
            value = re.split(r"<h[12]\b|(?:^|\s)#{1,3}\s+", value, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if len(value) >= 45:
                candidate[field] = value.rstrip(" .") + "."
                changed = True
                continue
        parts = re.split(
            r"\b(?:also announced|meanwhile|separately|in a statement|the statement added|live well|leadership programme|canal street)\b",
            value,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        if len(parts) == 2 and len(parts[0].strip()) >= 45:
            candidate[field] = parts[0].strip().rstrip(" .") + "."
            changed = True
    return changed


def _format_ru_date(day: date) -> str:
    return f"{day.day} {_MONTHS_RU_GENITIVE[day.month]}"


def _date_from_parts(day_raw: str, month_raw: str, year_raw: str | None = None) -> date | None:
    month = _MONTHS_EN.get(month_raw.lower()) or _MONTHS_RU.get(month_raw.lower())
    if not month:
        return None
    year = int(year_raw) if year_raw else now_london().year
    try:
        return date(year, month, int(day_raw))
    except ValueError:
        return None


def _extract_dates(text: str) -> list[date]:
    lowered = str(text or "").lower()
    dates: list[date] = []
    for match in re.finditer(r"\b(20\d{2})[/-](\d{1,2})[/-](\d{1,2})\b", lowered):
        year, month, day = (int(part) for part in match.groups())
        try:
            dates.append(date(year, month, day))
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]{3,9})(?:\s+(20\d{2}))?\b", lowered):
        parsed = _date_from_parts(*match.groups())
        if parsed:
            dates.append(parsed)
    for match in re.finditer(r"\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)(?:\s+(20\d{2}))?\b", lowered):
        parsed = _date_from_parts(*match.groups())
        if parsed:
            dates.append(parsed)
    return dates


def _summary_field_date(candidate: dict, field: str) -> date | None:
    summary = str(candidate.get("summary") or "")
    match = re.search(rf"\b{re.escape(field)}=(20\d{{2}}-\d{{2}}-\d{{2}})(?:[T\s]\d{{2}}:\d{{2}})?", summary)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _has_only_past_dates(text: str, today: date) -> bool:
    dates = _extract_dates(text)
    return bool(dates and max(dates) < today)


def _is_stale_event(candidate: dict) -> bool:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block not in _EVENT_BLOCKS and category not in _STALE_EVENT_CATEGORIES:
        return False
    today = now_london().date()
    onsale = _summary_field_date(candidate, "public_onsale")
    if block == "ticket_radar" and onsale and onsale < today:
        return True
    event_date = _summary_field_date(candidate, "event_date")
    if event_date:
        return event_date < today
    if block not in _EVENT_BLOCKS:
        visible_blob = _visible_candidate_blob(candidate).lower()
        if not re.search(r"\b(event|conference|summit|expo|election|poll|vote|onsale|on sale|выбор|конференц|саммит|билет)\b", visible_blob):
            return False
        return _has_only_past_dates(visible_blob, today)
    return _has_only_past_dates(_candidate_blob(candidate), today)


def _repair_stale_event_routing(candidate: dict) -> str:
    if not _is_stale_event(candidate):
        return ""
    blob = _visible_candidate_blob(candidate).lower()
    if any(marker in blob for marker in _REPORTING_PAST_EVENT_MARKERS):
        candidate["primary_block"] = "last_24h"
        if str(candidate.get("category") or "") in {"culture_weekly", "venues_tickets", "russian_speaking_events"}:
            candidate["category"] = "city_news"
        return "move_last_24h"
    return "drop"


def _repair_weather(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "weather" and str(candidate.get("category") or "") != "weather":
        return False
    blob = _candidate_blob(candidate)
    temp_match = re.search(r"(-?\d{1,2})\s*[–-]\s*(-?\d{1,2})\s*°", blob)
    rain_match = re.search(r"(?:до\s*)?(\d{1,3})\s*%", blob)
    if temp_match:
        min_temp, max_temp = temp_match.groups()
    else:
        min_temp, max_temp = "?", "?"
    rain_probability = int(rain_match.group(1)) if rain_match else None
    details: list[str] = [f"{min_temp}-{max_temp}°C"]
    if re.search(r"heavy rain|сильн\w*\s+дожд|ливн", blob, re.IGNORECASE):
        details.append("сильный дождь")
    if rain_probability is not None:
        details.append(f"вероятность осадков до {rain_probability}%")
    practical = "Зонт нужен." if rain_probability is None or rain_probability >= 45 else "Сверьте прогноз перед выходом."
    candidate["draft_line"] = "• Погода: " + ", ".join(details) + f". {practical}"
    candidate["draft_line"] = re.sub(r"^(•\s*Погода:\s*)Погода:\s*", r"\1", candidate["draft_line"], flags=re.IGNORECASE)
    candidate["summary"] = candidate["draft_line"].removeprefix("• ")
    candidate["practical_angle"] = practical
    return True


def _extract_transport_fields(candidate: dict) -> dict[str, str]:
    blob = _candidate_blob(candidate)
    lowered = blob.lower()
    fields: dict[str, str] = {}
    if "metrolink" in lowered or "tram" in lowered or "трамва" in lowered:
        fields["transport_mode"] = "tram"
        fields["operator"] = "Metrolink"
    elif "transpennine" in lowered or "train" in lowered or "rail" in lowered:
        fields["transport_mode"] = "rail"
        fields["operator"] = "TransPennine" if "transpennine" in lowered else "rail"
    elif "bus" in lowered or "go north west" in lowered or "tfgm" in lowered or "автобус" in lowered:
        fields["transport_mode"] = "bus"
        fields["operator"] = "Go North West" if "go north west" in lowered else "TfGM"

    for line_name in _LINE_NAMES:
        if line_name.lower() in lowered:
            fields["line"] = line_name
            break
    if "bury" in lowered and "crumpsall" in lowered:
        fields.setdefault("line", "Bury line")
        fields["segment"] = "Bury Interchange – Crumpsall"

    stop_match = re.search(r"\b(Victoria Lodge)\b", blob, re.IGNORECASE)
    if stop_match:
        fields["segment"] = "Victoria Lodge"
    road_match = re.search(r"\b(Lower Broughton Road)\b", blob, re.IGNORECASE)
    if road_match:
        fields["road"] = road_match.group(1)
    if "salford" in lowered:
        fields["place"] = "Salford"
    if re.search(r"replacement\s+bus|bus\s+replacement|замещающ", lowered):
        fields["alternative"] = "replacement bus"

    date_match = re.search(
        r"\b(?:from\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\s+(?:to|until|[-–])\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\b",
        blob,
        re.IGNORECASE,
    )
    if date_match:
        start = _date_from_parts(date_match.group(1), date_match.group(2))
        end = _date_from_parts(date_match.group(3), date_match.group(4))
        if start:
            fields["start_date"] = start.isoformat()
        if end:
            fields["end_date"] = end.isoformat()
    return fields


def _route_candidate(candidate: dict) -> bool:
    blob = _candidate_blob(candidate).lower()
    changed = False
    if re.search(r"\b(metrolink|tram|trams|no trams|bury line|replacement bus)\b", blob) and not re.search(r"\bfootball|restaurant|concert|theatre\b", blob):
        candidate["primary_block"] = "transport"
        candidate["category"] = "transport"
        changed = True
    if "go north west" in blob and ("strike ballot" in blob or "strike vote" in blob or "забастов" in blob):
        candidate["primary_block"] = "transport"
        candidate["category"] = "transport"
        changed = True
    if "transpennine" in blob and ("night train" in blob or "overnight train" in blob or "ночн" in blob):
        candidate["primary_block"] = "transport"
        candidate["category"] = "transport"
        changed = True
    if re.search(r"\b(junction|cycling|cycle safety|road safety)\b", blob) and not re.search(r"\b(bus|tram|rail|train|metrolink)\b", blob):
        candidate["primary_block"] = "city_watch"
        changed = True
    if "angel's bone" in blob or "angels bone" in blob:
        candidate["primary_block"] = "next_7_days"
        candidate["category"] = "culture_weekly"
        changed = True
    return changed


def _draft_from_candidate(candidate: dict) -> str:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    title = _clean_snippet(str(candidate.get("title") or ""))
    summary = _clean_snippet(str(candidate.get("summary") or candidate.get("lead") or candidate.get("evidence_text") or ""))
    if not title and not summary:
        return ""
    if block == "transport" or category == "transport":
        repaired, unfixable = _repair_transport(candidate)
        if repaired and not unfixable:
            return str(candidate.get("draft_line") or "")
        return ""
    if block in _EVENT_BLOCKS:
        dates = _extract_dates(f"{title} {summary}")
        if not dates:
            return ""
        when = _format_ru_date(min(dates))
        subject = title or summary
        return f"• Событие: {subject}. Дата: {when}; уточните время и билеты перед поездкой."
    if category == "food_openings" or block == "openings":
        if not re.search(r"\b(open|opening|launch|new|откры)\b", f"{title} {summary}", re.IGNORECASE):
            return ""
        return f"• Еда: {title}. {summary.rstrip('.')}. Уточните адрес и часы перед визитом."
    if category == "tech_business" and re.search(r"games?\s+network", f"{title} {summary}", re.IGNORECASE):
        return (
            "• IT и бизнес: в Манчестере запущена Manchester Games Network для местных игровых студий и основателей. "
            "Следите за программой, если работаете в игровой индустрии."
        )
    if category in {"media_layer", "gmp", "council", "public_services", "city_news", "tech_business"}:
        if re.search(r"\b(the|and|for|with|from|after|following|today|launched|confirmed)\b", f"{title} {summary}", re.IGNORECASE):
            return ""
        if len(summary) < 45:
            return ""
        return f"• {title}. {summary.rstrip('.')}. Следите за обновлениями."
    return ""


def _line_language_problem(line: str) -> bool:
    text = str(line or "").strip()
    if not text:
        return False
    cyrillic = len(re.findall(r"[а-яёА-ЯЁ]", text))
    ukrainian = bool(_UKRAINIAN_MARKERS.search(text))
    if ukrainian:
        return True
    latin_words = re.findall(r"[A-Za-z][A-Za-z'’-]+", text)
    english_hits = len([word for word in latin_words if _ENGLISH_PROSE_MARKERS.fullmatch(word)])
    return bool(english_hits >= 3 and cyrillic < 40)


def _repair_language(candidate: dict) -> bool:
    line = str(candidate.get("draft_line") or "")
    if not _line_language_problem(line):
        return False
    generated = _draft_from_candidate(candidate)
    if generated and not _line_language_problem(generated):
        candidate["draft_line"] = generated
        candidate["draft_line_provider"] = "auto-editor"
        return True
    return False


def _named_line_missing(candidate: dict) -> str:
    evidence = _visible_candidate_blob(candidate)
    line = str(candidate.get("draft_line") or "")
    for line_name in _LINE_NAMES:
        if re.search(rf"\b{re.escape(line_name)}\b", evidence, flags=re.IGNORECASE) and not re.search(
            rf"\b{re.escape(line_name)}\b", line, flags=re.IGNORECASE
        ):
            return line_name
    return ""


def _repair_named_line(candidate: dict) -> bool:
    line_name = _named_line_missing(candidate)
    if not line_name:
        return False
    line = str(candidate.get("draft_line") or "").strip()
    if not line:
        return False
    if re.search(r"Metrolink|трамва", line, re.IGNORECASE):
        candidate["draft_line"] = re.sub(r"^(•\s*Metrolink:\s*)", rf"\1{line_name}: ", line, count=1)
        if candidate["draft_line"] == line:
            candidate["draft_line"] = f"• Metrolink: {line_name}. " + line.removeprefix("• ").strip()
        return True
    return False


def _culture_fields_missing(candidate: dict) -> str:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    if category not in {"culture_weekly", "venues_tickets", "russian_speaking_events"} and block not in _EVENT_BLOCKS:
        return ""
    blob = _candidate_blob(candidate)
    if not _CULTURE_FACT_PATTERN.search(blob):
        return "missing concrete date/time/price/address"
    if not _CULTURE_MEDIUM_PATTERN.search(blob):
        return "missing event medium"
    return ""


def _transport_mode_missing(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "transport":
        return False
    return not _TRANSPORT_MODE_PATTERN.search(str(candidate.get("draft_line") or ""))


def _repair_transport(candidate: dict) -> tuple[bool, bool]:
    if str(candidate.get("primary_block") or "") != "transport" and str(candidate.get("category") or "") != "transport":
        return False, False
    fields = _extract_transport_fields(candidate)
    candidate.update(fields)
    blob = _candidate_blob(candidate)
    lowered = blob.lower()
    current_line = str(candidate.get("draft_line") or "")
    needs_vague_repair = any(marker in current_line.lower() for marker in _VAGUE_TRANSPORT_MARKERS)

    if fields.get("line") == "Bury line" and "crumpsall" in lowered:
        start = date.fromisoformat(fields["start_date"]) if fields.get("start_date") else None
        end = date.fromisoformat(fields["end_date"]) if fields.get("end_date") else None
        when = f"с {_format_ru_date(start)} по {_format_ru_date(end)} " if start and end else ""
        second = "TfGM запускает замещающие автобусы" if fields.get("alternative") == "replacement bus" else "сверьте альтернативный маршрут"
        works = "Идёт замена путей; " if re.search(r"track|rail replacement|замен", lowered) else ""
        candidate["draft_line"] = (
            f"• Metrolink: {when}не ходят трамваи на Bury line между Bury Interchange и Crumpsall. "
            f"{works}{second}, закладывайте дополнительное время."
        )
        return True, False

    if fields.get("line") and needs_vague_repair:
        candidate["draft_line"] = (
            f"• Metrolink: проверьте движение на {fields['line']}. "
            "Сверьте маршрут перед поездкой."
        )
        return True, False

    if "victoria lodge" in lowered and "lower broughton road" in lowered:
        routes = _route_numbers(blob)
        route_phrase = f" Маршруты: {routes}." if routes else ""
        candidate["draft_line"] = (
            "• Автобусы: закрыта автобусная остановка Victoria Lodge на Lower Broughton Road в Salford из-за дорожных работ."
            f"{route_phrase} Сверьте остановку перед поездкой."
        )
        return True, False

    if "go north west" in lowered and ("strike ballot" in lowered or "strike vote" in lowered or "забастов" in lowered):
        candidate["draft_line"] = (
            "• Автобусы: Go North West проводит голосование о забастовке. "
            "Следите за обновлениями TfGM перед поездкой."
        )
        return True, False

    if fields.get("transport_mode") and _transport_mode_missing(candidate):
        mode_label = {"tram": "Трамваи", "bus": "Автобусы", "rail": "Поезда"}.get(fields["transport_mode"], "Транспорт")
        line = current_line.removeprefix("• ").strip()
        candidate["draft_line"] = f"• {mode_label}: {line}"
        return True, False

    if needs_vague_repair and not fields.get("line") and not fields.get("segment"):
        return False, True
    return False, False


def _route_numbers(text: str) -> str:
    match = re.search(r"\b(?:route|routes|service|services)\s+([0-9A-Za-z, /&-]{1,40})", text, re.IGNORECASE)
    if not match:
        return ""
    value = re.sub(r"\s+", " ", match.group(1)).strip(" .;:-")
    value = re.sub(r"\b(?:will|are|is|from|to|between|affected|closed|diverted).*$", "", value, flags=re.IGNORECASE).strip(" .;:-")
    return value


def _is_unfixable_promo(candidate: dict) -> bool:
    blob = _candidate_blob(candidate).lower()
    if "cocktail" not in blob:
        return False
    if re.search(r"\b(promo|promotion|discount|deal|offer|code)\b", blob) and not re.search(r"\b(open|opening|launch|new venue|new bar)\b", blob):
        return True
    return False


def _line_is_usable(candidate: dict) -> bool:
    category = str(candidate.get("category") or "")
    line = str(candidate.get("draft_line") or "").strip()
    if category not in _DRAFT_REQUIRED_CATEGORIES:
        return True
    if category == "transport":
        _repair_transport(candidate)
        line = str(candidate.get("draft_line") or "").strip()
    if not line:
        generated = _draft_from_candidate(candidate)
        if generated:
            candidate["draft_line"] = generated
            candidate["draft_line_provider"] = "auto-editor"
            line = generated
    if _line_language_problem(line):
        return False
    if _culture_fields_missing(candidate):
        return False
    if _transport_mode_missing(candidate):
        return False
    if _named_line_missing(candidate):
        return False
    return bool(line.startswith("• ") and re.search(r"[а-яё]", line, re.IGNORECASE))


def _replacement_blocks(block: str) -> list[str]:
    if block == "transport":
        return ["transport", "today_focus", "city_watch"]
    if block in _EVENT_BLOCKS:
        return [block, "next_7_days", "future_announcements", "city_watch", "openings", "tech_business"]
    if block == "tech_business":
        return ["tech_business", "city_watch", "openings"]
    return [block, "city_watch", "openings", "tech_business"]


def _promote_replacement(candidates: list[dict], dropped: dict, actions: list[dict]) -> bool:
    old_block = str(dropped.get("primary_block") or "")
    dropped_fingerprint = str(dropped.get("fingerprint") or "").strip()
    blocked_fingerprints = {
        str(item.get("fingerprint") or "")
        for item in candidates
        if isinstance(item, dict) and item.get("include")
    }
    for block in _replacement_blocks(old_block):
        for candidate in candidates:
            if not isinstance(candidate, dict) or candidate.get("include"):
                continue
            candidate_fingerprint = str(candidate.get("fingerprint") or "").strip()
            if candidate_fingerprint == dropped_fingerprint:
                continue
            if candidate_fingerprint in blocked_fingerprints:
                continue
            if str(candidate.get("primary_block") or "") != block:
                continue
            if candidate.get("validation_errors") or not candidate.get("source_url") or not candidate.get("source_label"):
                continue
            if _is_stale_event(candidate) or _is_unfixable_promo(candidate):
                continue
            if str(candidate.get("dedupe_decision") or "") == "drop":
                continue
            if not _line_is_usable(candidate):
                continue
            candidate["include"] = True
            _append_reason(candidate, f"Auto-editor replacement for dropped candidate {dropped.get('fingerprint') or dropped.get('title')}.")
            actions.append(
                {
                    "action": "promote_replacement",
                    "replacement_fingerprint": candidate.get("fingerprint"),
                    "replacement_title": candidate.get("title"),
                    "old_block": old_block,
                    "new_block": block,
                }
            )
            return True
    return False


def auto_repair_weather(candidate: dict) -> bool:
    return _repair_weather(candidate)


def auto_repair_transport(candidate: dict) -> tuple[bool, bool]:
    return _repair_transport(candidate)


def auto_repair_event_dates(candidate: dict) -> bool:
    return _is_stale_event(candidate)


def auto_repair_translation_terms(text: str) -> tuple[str, bool]:
    return _repair_translation_terms(text)


def auto_repair_section_routing(candidate: dict) -> bool:
    return _route_candidate(candidate)


def auto_replace_unfixable_candidate(candidates: list[dict], dropped: dict, actions: list[dict]) -> bool:
    return _promote_replacement(candidates, dropped, actions)


def auto_edit_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    report_path = state_dir / "auto_editor_report.json"
    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])

    actions: list[dict] = []
    dropped: list[dict] = []
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        if _split_mixed_summary(candidate):
            actions.append({"action": "split_mixed_summary", "fingerprint": candidate.get("fingerprint"), "title": candidate.get("title")})
        if auto_repair_section_routing(candidate):
            actions.append({"action": "repair_section_routing", "fingerprint": candidate.get("fingerprint"), "primary_block": candidate.get("primary_block")})
        line = str(candidate.get("draft_line") or "")
        repaired_line, translated = auto_repair_translation_terms(line)
        if translated:
            candidate["draft_line"] = repaired_line
            actions.append({"action": "repair_translation_terms", "fingerprint": candidate.get("fingerprint")})
        if _repair_language(candidate):
            actions.append({"action": "repair_language", "fingerprint": candidate.get("fingerprint")})
        if auto_repair_weather(candidate):
            actions.append({"action": "repair_weather", "fingerprint": candidate.get("fingerprint")})
        repaired_transport, unfixable_transport = auto_repair_transport(candidate)
        if unfixable_transport and _enrich_candidate_from_source(candidate):
            actions.append({"action": "enrich_from_source", "fingerprint": candidate.get("fingerprint")})
            repaired_transport, unfixable_transport = auto_repair_transport(candidate)
        if repaired_transport:
            actions.append({"action": "repair_transport", "fingerprint": candidate.get("fingerprint")})
        if _repair_named_line(candidate):
            actions.append({"action": "repair_named_transport_line", "fingerprint": candidate.get("fingerprint")})

        drop_reason = ""
        stale_action = _repair_stale_event_routing(candidate)
        if stale_action == "move_last_24h":
            actions.append({"action": "repair_event_dates_move_to_last_24h", "fingerprint": candidate.get("fingerprint")})
        elif stale_action == "drop":
            drop_reason = "Auto-editor: stale event/ticket date."
        elif _line_language_problem(str(candidate.get("draft_line") or "")):
            drop_reason = "Auto-editor: draft_line is not Russian prose."
        elif _culture_fields_missing(candidate):
            drop_reason = f"Auto-editor: culture/event candidate {_culture_fields_missing(candidate)}."
        elif _transport_mode_missing(candidate):
            drop_reason = "Auto-editor: transport line missing explicit mode."
        elif _named_line_missing(candidate):
            drop_reason = f"Auto-editor: transport line missing named line {_named_line_missing(candidate)}."
        elif _is_unfixable_promo(candidate):
            drop_reason = "Auto-editor: promo/deal item without city value."
        elif unfixable_transport:
            drop_reason = "Auto-editor: vague transport line without concrete line/segment evidence."

        if drop_reason:
            candidate["include"] = False
            _append_reason(candidate, drop_reason)
            dropped_item = {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "primary_block": candidate.get("primary_block"),
                "reason": drop_reason,
            }
            dropped.append(dropped_item)
            actions.append({"action": "drop_unfixable_candidate", **dropped_item})

    replacements = 0
    for item in dropped:
        if auto_replace_unfixable_candidate(candidates, item, actions):
            replacements += 1

    payload["run_date_london"] = today_london()
    write_json(candidates_path, payload)
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete",
            "errors": [],
            "warnings": [],
            "action_count": len(actions),
            "dropped_count": len(dropped),
            "replacement_count": replacements,
            "actions": actions[:120],
        },
    )
    return StageResult(True, "Auto-editor stage completed.", report_path)
