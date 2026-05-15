from __future__ import annotations

import re


EVENT_CATEGORIES = {"culture_weekly", "venues_tickets", "russian_speaking_events"}
EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
}

_DATE_RE = re.compile(
    r"\b(?:event_date|public_onsale)=20\d{2}-\d{2}-\d{2}\b|"
    r"\b20\d{2}[/-]\d{1,2}[/-]\d{1,2}\b|"
    r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b|"
    r"\b\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b|"
    r"\b\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b|"
    r"\b(?:today|tonight|tomorrow|сегодня|завтра)\b",
    re.IGNORECASE,
)
_PLACE_RE = re.compile(
    r"\b(?:arena|hall|theatre|theater|gallery|museum|venue|academy|depot|apollo|ritz|"
    r"club|bar|pub|library|park|stadium|centre|center|square|street|road|avenue|lane|"
    r"зал|театр|галерея|музей|арена|площадк|клуб|бар|паб|библиотек|парк|стадион|центр|улиц)\b",
    re.IGNORECASE,
)
_DISTRICT_RE = re.compile(
    r"\b(?:greater manchester|manchester|salford|trafford|stockport|tameside|oldham|rochdale|bury|"
    r"bolton|wigan|altrincham|stretford|ashton|eccles|city centre|deansgate|piccadilly|ancoats|"
    r"northern quarter|oxford road|london|birmingham|leeds|liverpool|sheffield|glasgow|cardiff|"
    r"манчестер|солфорд|траффорд|стокпорт|лондон|бирмингем|лидс|ливерпуль)\b",
    re.IGNORECASE,
)
_PRICE_OR_FREE_RE = re.compile(
    r"(?:£\s*\d|\bfree\b|\bgratis\b|\bfrom\s+£|\b\d+\s*gbp\b|"
    r"\bбесплатн\w*|\bвход\s+свободн\w*|\bот\s+£)",
    re.IGNORECASE,
)
_BOOKING_RE = re.compile(
    r"\b(?:ticket|tickets|booking|book now|book\b|register|registration|on sale|onsale|"
    r"presale|public sale|sale starts|билет|билеты|бронь|регистрац|в продаже|продаж)\b",
    re.IGNORECASE,
)


def is_event_candidate(candidate: dict) -> bool:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    return category in EVENT_CATEGORIES or block in EVENT_BLOCKS


def _blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in (
            "title",
            "summary",
            "lead",
            "practical_angle",
            "evidence_text",
            "draft_line",
            "source_label",
        )
    )


def event_quality_report(candidate: dict) -> dict[str, object]:
    if not is_event_candidate(candidate):
        return {"is_event": False, "ok": True, "checks": {}, "missing": []}

    blob = _blob(candidate)
    has_price_or_free = bool(_PRICE_OR_FREE_RE.search(blob))
    has_booking_signal = bool(_BOOKING_RE.search(blob))
    checks = {
        "date": bool(_DATE_RE.search(blob)),
        "place": bool(_PLACE_RE.search(blob)),
        "district": bool(_DISTRICT_RE.search(blob)),
        "price_or_free": has_price_or_free,
        "booking": has_booking_signal,
        "source": bool(str(candidate.get("source_url") or "").strip() and str(candidate.get("source_label") or "").strip()),
    }
    checks["access"] = has_price_or_free or has_booking_signal

    missing: list[str] = []
    if not checks["date"]:
        missing.append("date")
    if not checks["place"]:
        missing.append("place")
    if not checks["district"]:
        missing.append("district")
    if not checks["access"]:
        missing.append("price_or_free_or_booking")
    if not checks["source"]:
        missing.append("source")

    return {"is_event": True, "ok": not missing, "checks": checks, "missing": missing}


def event_quality_errors(candidate: dict) -> list[str]:
    report = event_quality_report(candidate)
    if not report.get("is_event") or report.get("ok"):
        return []
    labels = {
        "date": "no usable event date",
        "place": "missing venue/place",
        "district": "missing district/location",
        "price_or_free_or_booking": "missing price/free/booking signal",
        "source": "missing booking/source reference",
    }
    missing = [labels.get(str(item), str(item)) for item in report.get("missing", [])]
    return [f"under-specified event: {item}." for item in missing]


def event_quality_reject_reasons(candidate: dict) -> list[str]:
    missing = event_quality_report(candidate).get("missing", [])
    reasons: list[str] = []
    if "date" in missing:
        reasons.append("no_date")
    if any(item in missing for item in ("place", "district", "price_or_free_or_booking", "source")):
        reasons.append("source_thin")
    return reasons or ["weak_value"]
