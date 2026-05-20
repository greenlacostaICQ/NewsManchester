from __future__ import annotations

from datetime import datetime, timedelta
import re

from news_digest.pipeline.common import now_london


MAJOR_TICKET_VENUES: tuple[str, ...] = (
    "ao arena",
    "co-op live",
    "coop live",
    "manchester apollo",
    "o2 apollo",
    "o2 apollo manchester",
    "manchester academy",
    "manchester academy 1",
    "manchester academy 2",
    "manchester academy 3",
    "albert hall",
    "albert hall manchester",
    "new century hall",
    "manchester new century hall",
    "bridgewater hall",
    "the bridgewater hall",
    "aviva studios",
    "factory international",
    "etihad campus",
    "old trafford",
    "castlefield bowl",
    "home",
    "rncm",
    "royal northern college of music",
    "the lowry",
    "stoller hall",
    "the stoller hall",
    "the o2",
    "ovo arena",
    "wembley arena",
    "wembley stadium",
    "royal albert hall",
    "alexandra palace",
    "eventim apollo",
    "london stadium",
    "tottenham hotspur stadium",
)

KNOWN_PLACE_NAMES: tuple[str, ...] = MAJOR_TICKET_VENUES + (
    "arndale",
    "manchester arndale",
    "trafford centre",
    "middleton shopping centre",
    "printworks",
    "circle square",
    "john dalton street",
    "northern quarter",
    "ancoats",
    "spinningfields",
    "media city",
    "media city uk",
)

VAGUE_ENDING_MARKERS: tuple[str, ...] = (
    "это важный сигнал",
    "это заметный кейс",
    "это событие подчеркивает",
    "следите за развитием",
    "следите за обновлениями",
    "проверьте детали",
    "подробности уточняйте",
    "подробности ниже",
    "читайте подробнее",
    "это станет новым акцентом",
    "обещает новое дыхание",
    "обещает стать",
    "может привлечь внимание",
    "стоит знать",
)

_SUMMARY_DATETIME_PATTERN = re.compile(
    r"\b(?P<field>event_date|public_onsale)="
    r"(?P<value>\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?)"
)


def _blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text", "source_label")
    )


def is_major_ticket_venue(venue: str) -> bool:
    lowered = re.sub(r"\s+", " ", str(venue or "").lower()).strip()
    if not lowered:
        return False
    return any(token in lowered for token in MAJOR_TICKET_VENUES)


def has_known_place_name(text: str) -> bool:
    lowered = re.sub(r"\s+", " ", str(text or "").lower())
    return any(token in lowered for token in KNOWN_PLACE_NAMES)


def summary_field_datetime(summary: str, field: str) -> datetime | None:
    for match in _SUMMARY_DATETIME_PATTERN.finditer(str(summary or "")):
        if match.group("field") != field:
            continue
        raw = match.group("value").replace("T", " ")
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
            except ValueError:
                continue
            return parsed.replace(tzinfo=now_london().tzinfo)
    return None


def ticket_venue(candidate: dict) -> str:
    summary = str(candidate.get("summary") or "")
    chunks = [part.strip(" .") for part in summary.split("|")]
    for chunk in chunks:
        if not chunk:
            continue
        if chunk.lower().startswith(("event_date=", "public_onsale=", "ticket_signal=", "ticket_type=", "major_venue=")):
            continue
        if chunk.lower() in {"manchester", "liverpool", "london", "rock", "pop", "music"}:
            continue
        return chunk
    return str(candidate.get("source_label") or "").strip()


def classify_ticket_type(candidate: dict) -> str:
    summary = str(candidate.get("summary") or "")
    lowered = summary.lower()
    explicit = re.search(r"\bticket_type=([a-z_]+)\b", lowered)
    if explicit:
        return explicit.group(1)
    onsale_at = summary_field_datetime(summary, "public_onsale")
    now = now_london()
    if "ticket_signal=onsale" in lowered:
        if onsale_at is None:
            return "newly_listed"
        if onsale_at <= now:
            return "on_sale_now" if (now - onsale_at) <= timedelta(days=3) else "old_onsale"
        return "presale_soon"
    if is_major_ticket_venue(ticket_venue(candidate)) or "major_venue=true" in lowered:
        return "major_upcoming"
    return "regular_upcoming"


def scrub_vague_ending(line: str) -> tuple[str, list[str]]:
    text = str(line or "").strip()
    if not text:
        return text, []
    removed: list[str] = []
    bullet = "• " if text.startswith("• ") else ""
    body = text[2:].strip() if bullet else text
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", body) if part.strip()]
    while parts:
        last = parts[-1].lower()
        hit = next((marker for marker in VAGUE_ENDING_MARKERS if marker in last), "")
        if not hit:
            break
        remainder = re.sub(re.escape(hit), "", parts[-1], flags=re.IGNORECASE)
        remainder = re.sub(r"\s*(?:[,;:—-]\s*){1,2}", " ", remainder)
        remainder = re.sub(r"\s+", " ", remainder).strip(" .")
        # If the last sentence contained a vague clause plus a real action
        # ("следите за обновлениями и проверьте маршрут"), keep the action.
        if len(remainder) >= 25 and re.search(r"\b(?:проверьте|сверьте|уточните|закладывайте|держите)\b", remainder, re.IGNORECASE):
            parts[-1] = remainder + "."
            removed.append(hit)
            break
        removed.append(hit)
        parts.pop()
    if not removed:
        return text, []
    cleaned = " ".join(parts).strip()
    if cleaned and not re.search(r"[.!?]$", cleaned):
        cleaned += "."
    return (bullet + cleaned).strip(), removed


_CRIME_MARKERS = re.compile(
    r"\b(?:police|gmp|murder|stab(?:bing|bed)?|knife|killed|death|dead|"
    r"sex(?:ual)? offence|rape|assault|arrest(?:ed)?|charged|court|jailed|"
    r"sentence(?:d)?|appeal|investigation|witness(?:es)?|cctv)\b",
    re.IGNORECASE,
)
_CRIME_ACTION = re.compile(
    r"\b(?:charged|arrested|jailed|sentenced|convicted|appeal(?:ed|s)?|"
    r"investigat(?:e|es|ing|ion)|named|released|hunt(?:ing)?|seek(?:ing)?|"
    r"found guilty|pleaded guilty)\b",
    re.IGNORECASE,
)
_CRIME_EVENT = re.compile(
    r"\b(?:murder|stab(?:bing|bed)?|knife|assault|attack|crash|collision|"
    r"death|died|killed|sex(?:ual)? offence|rape|drug|cocaine|burglary|robbery)\b",
    re.IGNORECASE,
)
_DATE_OR_STAGE = re.compile(
    r"\b(?:today|yesterday|this morning|this week|last night|on \d{1,2} |"
    r"\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)|"
    r"20\d{2}|sentenced|jailed|charged|appeal|court)\b",
    re.IGNORECASE,
)
_LOCATION_SIGNAL = re.compile(
    r"\b(?:manchester|salford|stockport|oldham|rochdale|bury|bolton|wigan|"
    r"trafford|tameside|moss side|old trafford|timperley|altrincham|"
    r"road|street|avenue|lane|park|centre|farm|hospital)\b",
    re.IGNORECASE,
)
_PERSON_SIGNAL = re.compile(r"\b(?:\d{1,3}-year-old|man|woman|boy|girl|child|teenager|victim|suspect|officer)\b", re.IGNORECASE)


def crime_specificity_review(candidate: dict) -> dict[str, object]:
    blob = _blob(candidate)
    if not _CRIME_MARKERS.search(blob):
        return {"applies": False, "missing": [], "severity": "none"}
    missing: list[str] = []
    if not _CRIME_EVENT.search(blob):
        missing.append("what_happened")
    if not (_PERSON_SIGNAL.search(blob) or "victim" in blob.lower()):
        missing.append("who_affected")
    if not _LOCATION_SIGNAL.search(blob):
        missing.append("where")
    if not (_DATE_OR_STAGE.search(blob) or _CRIME_ACTION.search(blob)):
        missing.append("why_now")
    evidence_len = len(re.sub(r"\s+", " ", str(candidate.get("evidence_text") or "")).strip())
    if len(missing) >= 3 and evidence_len < 120:
        severity = "hard"
    elif missing:
        severity = "borderline"
    else:
        severity = "ok"
    return {
        "applies": True,
        "missing": missing,
        "severity": severity,
        "enrichment_attempted": bool(candidate.get("enrichment_status")),
    }


_PROPERTY_MARKERS = re.compile(
    r"\b(?:property|properties|planning|developer|development|office building|"
    r"flats|apartments|homes|housing|converted|conversion|shopping centre|"
    r"retail park|for sale|sold|landlord|rightmove)\b",
    re.IGNORECASE,
)
_STREET_OR_ADDRESS = re.compile(
    r"\b(?:[A-Z][A-Za-z'’-]+\s+){0,3}(?:Road|Street|Avenue|Lane|Drive|Way|Square|"
    r"Place|Quay|Gate|Grove|Close|Crescent|Dalton Street|Oxford Road)\b"
)
_NAMED_BUILDING = re.compile(
    r"\b(?:[A-Z][A-Za-z'’-]+\s+){1,4}(?:House|Hall|Tower|Centre|Center|Building|"
    r"Works|Mill|Exchange|Arcade|Court|Kitchens|Market|Studios)\b"
)
_GENERIC_PROPERTY = re.compile(r"\b(?:office building|shopping centre|retail park|building|site)\b", re.IGNORECASE)


def property_specificity_review(candidate: dict) -> dict[str, object]:
    blob = _blob(candidate)
    if not _PROPERTY_MARKERS.search(blob):
        return {"applies": False, "missing": [], "severity": "none"}
    has_location = bool(
        has_known_place_name(blob)
        or _STREET_OR_ADDRESS.search(blob)
        or _NAMED_BUILDING.search(blob)
        or re.search(r"\b(?:in|at|on)\s+(?:Stockport|Bury|Bolton|Wigan|Trafford|Timperley|Altrincham|Middleton|Chorlton|Ancoats|Northern Quarter)\b", blob)
    )
    missing: list[str] = []
    if _GENERIC_PROPERTY.search(blob) and not has_location:
        missing.append("specific_location")
    if not re.search(r"\b(?:approved|rejected|submitted|filed|sale|sold|for sale|could be|plans?|application|developer)\b", blob, re.IGNORECASE):
        missing.append("decision_or_action")
    evidence_len = len(re.sub(r"\s+", " ", str(candidate.get("evidence_text") or "")).strip())
    if len(missing) >= 2 and evidence_len < 120:
        severity = "hard"
    elif missing:
        severity = "borderline"
    else:
        severity = "ok"
    return {
        "applies": True,
        "missing": missing,
        "severity": severity,
        "enrichment_attempted": bool(candidate.get("enrichment_status")),
        "known_place": has_known_place_name(blob),
    }


_EVENT_COMPLETENESS_FIELDS = ("date_start", "venue", "price", "booking_url", "borough")


def event_schema_completeness(candidate: dict) -> dict[str, object]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if not event or not event.get("is_event"):
        return {"applies": False, "score": 0, "missing": list(_EVENT_COMPLETENESS_FIELDS)}
    present = []
    for field in _EVENT_COMPLETENESS_FIELDS:
        value = event.get("date") if field == "date_start" and not event.get("date_start") else event.get(field)
        if str(value or "").strip():
            present.append(field)
    missing = [field for field in _EVENT_COMPLETENESS_FIELDS if field not in present]
    score = int(round(len(present) / len(_EVENT_COMPLETENESS_FIELDS) * 100))
    return {
        "applies": True,
        "score": score,
        "present": present,
        "missing": missing,
    }


def attach_scoring_trace(candidate: dict) -> dict:
    """Persist enough scoring context for future personalization training."""
    if not isinstance(candidate, dict):
        return candidate
    trace = dict(candidate.get("scoring_trace") or {})
    trace.update(
        {
            "primary_block": str(candidate.get("primary_block") or ""),
            "category": str(candidate.get("category") or ""),
            "source_label": str(candidate.get("source_label") or ""),
            "reader_value_score": candidate.get("reader_value_score"),
            "reader_value_label": candidate.get("reader_value_label"),
            "why_now": candidate.get("why_now") or "",
            "editorial_status": candidate.get("editorial_status") or "",
            "quality_warnings": candidate.get("quality_warnings") or [],
            "reject_reasons": candidate.get("reject_reasons") or [],
            "event_schema_completeness": candidate.get("event_schema_completeness") or {},
        }
    )
    candidate["scoring_trace"] = trace
    return candidate


def infer_why_now(candidate: dict) -> str:
    """Q1: explicit reason a candidate deserves today's morning issue."""
    block = str(candidate.get("primary_block") or "")
    change_type = str(candidate.get("change_type") or "")
    category = str(candidate.get("category") or "")
    blob = _blob(candidate).lower()
    if block == "weather":
        return "today_weather"
    if block == "transport":
        if "no_change" in change_type or "same_story_rehash" in change_type:
            return "stale"
        return "ongoing_disruption"
    if change_type in {"new_story", "new_phase", "same_story_new_facts", "follow_up"}:
        return "new_today" if change_type == "new_story" else "update_today"
    if change_type == "reminder":
        return "happening_today"
    if change_type in {"no_change", "same_story_rehash"}:
        return "stale"
    if re.search(r"\b(today|this morning|this afternoon|tonight|tomorrow|сегодня|завтра)\b", blob, re.IGNORECASE):
        return "happening_today"
    if re.search(r"\b(deadline|closes?|last chance|final day|until \d{1,2}|до \d{1,2})\b", blob, re.IGNORECASE):
        return "deadline_soon"
    if category == "venues_tickets" and classify_ticket_type(candidate) in {
        "on_sale_now", "presale_soon", "newly_listed", "major_upcoming",
    }:
        return "ticket_opportunity"
    if candidate.get("published_at"):
        try:
            pub_day = datetime.fromisoformat(str(candidate["published_at"]).replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
            delta = (now_london().date() - pub_day).days
            if delta <= 1:
                return "new_today"
            if delta <= 7 and re.search(r"\b(updated|confirmed|announced|approved|rejected|sentenced|charged|arrested|launched|opened)\b", blob):
                return "update_today"
        except ValueError:
            pass
    return "unclear"


def why_now_is_publishable(why_now: str) -> bool:
    return why_now in {
        "new_today",
        "update_today",
        "happening_today",
        "deadline_soon",
        "ongoing_disruption",
        "today_weather",
        "ticket_opportunity",
    }
