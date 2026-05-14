from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import re
from urllib import parse
from zoneinfo import ZoneInfo


LONDON_TZ = ZoneInfo("Europe/London")

REQUIRED_SCAN_CATEGORIES = {
    "media_layer": "media-layer",
    "transport": "transport",
    "gmp": "GMP",
    "public_services": "public services",
    "culture_weekly": "culture weekly",
    "venues_tickets": "venues / tickets",
    "food_openings": "food / openings",
    "football": "football",
    "tech_business": "tech / business",
    "diaspora_events": "Russian-speaking / diaspora events",
}

REQUIRED_BLOCKS = [
    "Погода",
    "Что важно сегодня",
    "Что произошло за 24 часа",
]

LOW_SIGNAL_BLOCKS = [
    "Городской радар",
    "Дальние анонсы",
    "Билеты / Ticket Radar",
    "Крупные концерты вне GM",
    "Русскоязычные концерты и стендап UK",
    "Еда, открытия и рынки",
    "IT и бизнес",
    "Радар по районам",
]

SECTION_MAX_ITEMS = {
    # Length 350–450 char cards are 3× longer than old headlines, so caps
    # are pulled down to keep the digest readable in Telegram.
    "Что произошло за 24 часа": 9,
    "Городской радар": 12,
    "Футбол": 3,
    "IT и бизнес": 5,
    "Выходные в GM": 5,
    "Еда, открытия и рынки": 6,
    "Билеты / Ticket Radar": 6,
    "Русскоязычные концерты и стендап UK": 6,
    "Что важно сегодня": 5,
}

# Soft minimums: release gate emits a warning (does not block) when a
# section ends up below this count after caps and quality drops. Used to
# catch days when curator only nominated 2 items for "Что важно сегодня"
# or similar — so the underflow is visible in release_report instead of
# silently shipping a thin section.
SECTION_MIN_ITEMS = {
    "Что произошло за 24 часа": 5,
    "Что важно сегодня": 3,
    "Городской радар": 5,
    "Что важно в ближайшие 7 дней": 3,
    "Еда, открытия и рынки": 3,
    "Футбол": 2,
}

# Max items per single source per section. Universities pump out 5+ press
# releases a day each and dominated city_watch on 2026-05-12 — keep them
# capped so they don't crowd out actual city news.
SECTION_MAX_PER_SOURCE = {
    "Городской радар": 2,
    "Что произошло за 24 часа": 3,
    # Whitworth/venue sources publish many recurring events — cap at 2 per
    # venue so a single gallery doesn't dominate the 7-day calendar.
    "Что важно в ближайшие 7 дней": 2,
}

VAGUE_PRACTICAL_ANGLES = {
    "Оценить городскую значимость перед выпуском.",
    "Проверить матчевый контекст перед включением в футбольный блок.",
}

PRIMARY_BLOCKS = {
    "weather": "Погода",
    "transport": "Общественный транспорт сегодня",
    "today_focus": "Что важно сегодня",
    "last_24h": "Что произошло за 24 часа",
    "lead_story": "Главная история дня",
    "city_watch": "Городской радар",
    "weekend_activities": "Выходные в GM",
    "next_7_days": "Что важно в ближайшие 7 дней",
    "future_announcements": "Дальние анонсы",
    "ticket_radar": "Билеты / Ticket Radar",
    "outside_gm_tickets": "Крупные концерты вне GM",
    "russian_events": "Русскоязычные концерты и стендап UK",
    "openings": "Еда, открытия и рынки",
    "tech_business": "IT и бизнес",
    "football": "Футбол",
    "district_radar": "Радар по районам",
}


def now_london() -> datetime:
    return datetime.now(LONDON_TZ)


def today_london() -> str:
    return now_london().strftime("%Y-%m-%d")


def new_pipeline_run_id() -> str:
    return now_london().strftime("%Y%m%dT%H%M%S%z")


def pipeline_run_id_from(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("pipeline_run_id") or payload.get("run_id") or "").strip()


def read_json(path: Path, default: dict | None = None) -> dict:
    if not path.exists():
        return {} if default is None else default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def clean_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = parse.urlsplit(raw)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = re.sub(r"/+$", "", parsed.path or "") or "/"
    return parse.urlunsplit((scheme, netloc, path, "", ""))


def canonical_url_identity(url: str) -> str:
    cleaned = clean_url(url)
    if not cleaned:
        return ""
    parsed = parse.urlsplit(cleaned)
    return f"{parsed.netloc}{parsed.path}"


def normalize_title(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9а-яё]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def fingerprint_for_candidate(candidate: dict) -> str:
    source_url = canonical_url_identity(str(candidate.get("source_url") or ""))
    source_label = str(candidate.get("source_label") or "").strip().lower()
    title = str(candidate.get("title") or "").strip().lower()
    category = str(candidate.get("category") or "").strip().lower()
    base = f"{category}-{source_label}-{source_url}" if source_url else f"{category}-{source_label}-{title}"
    normalized = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    return normalized[:180]


def is_placeholder_practical_angle(value: str) -> bool:
    text = str(value or "").strip()
    return text in VAGUE_PRACTICAL_ANGLES or text.startswith("Включать только")


def extract_sections(html_text: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current_section: str | None = None

    for raw_line in html_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = re.fullmatch(r"<b>([^<]+)</b>", line)
        if match:
            heading = match.group(1).strip()
            if heading.startswith("Greater Manchester Brief"):
                current_section = None
                continue
            current_section = heading
            sections.setdefault(current_section, [])
            continue
        if current_section is None:
            continue
        if line.startswith("• "):
            sections[current_section].append(line)

    return sections
