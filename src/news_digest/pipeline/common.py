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
    "Открытия и еда",
    "IT и бизнес",
    "Радар по районам",
]

SECTION_MAX_ITEMS = {
    "Футбол": 3,
    "IT и бизнес": 5,
    "Выходные в GM": 6,
}

VAGUE_PRACTICAL_ANGLES = {
    "Оценить городскую значимость перед выпуском.",
    "Проверить матчевый контекст перед включением в футбольный блок.",
}

PRIMARY_BLOCKS = {
    "weather": "Погода",
    "transport": "Транспорт и сбои",
    "today_focus": "Что важно сегодня",
    "last_24h": "Что произошло за 24 часа",
    "lead_story": "Главная история дня",
    "city_watch": "Городской радар",
    "weekend_activities": "Выходные в GM",
    "next_7_days": "Что важно в ближайшие 7 дней",
    "future_announcements": "Дальние анонсы",
    "ticket_radar": "Билеты / Ticket Radar",
    "openings": "Открытия и еда",
    "tech_business": "IT и бизнес",
    "football": "Футбол",
    "district_radar": "Радар по районам",
}


def now_london() -> datetime:
    return datetime.now(LONDON_TZ)


def today_london() -> str:
    return now_london().strftime("%Y-%m-%d")


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
