from __future__ import annotations

from datetime import date, datetime, timedelta
import re

from news_digest.pipeline.common import normalize_title, now_london


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
            "editorial_contract": candidate.get("editorial_contract") or {},
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


EDITORIAL_CONTRACT_VERSION = "2026-05-23.1"

_EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
}
_OPERATIONAL_BLOCKS = {"weather", "transport"}
_SPECIFIC_TOPIC_PREFIXES = (
    "politics:",
    "memorial:",
    "incident:",
    "opening:",
    "event:",
    "research:",
    "planning:",
    "property:",
    "ticket:",
)

_HUMAN_INTEREST_RE = re.compile(
    r"\b(?:"
    r"first\s+job|too\s+stupid|question(?:ed)?\s+if\s+i\s+was|"
    r"failed\s+(?:his|her|their|my)?\s*a-?levels?|"
    r"dyslexia|dream(?:ed|t)?s?\s+of\s+(?:making|becoming|a\s+career)|"
    r"next\s+big\s+name|proper\s+career|"
    r"classic\s+sports\s+story|getting\s+an\s+injury|"
    r"pandemic\s+(?:inspired|sparked)|now\s+(?:helps|inspires|teaches|runs)|"
    r"turning\s+point|not\s+knowing\s+what\s+i\s+wanted|"
    r"overcame|inspir(?:e|es|ing)|turned\s+(?:his|her|their)\s+life|"
    r"struggled\s+at\s+school|told\s+(?:he|she|they|i)\s+would\s+never|"
    r"вдохновля(?:ет|ют)|преодоле(?:л|ла|ли)|провалил(?:а)?\s+экзамен"
    r")\b",
    re.IGNORECASE,
)
_LOCAL_ACTION_RE = re.compile(
    r"\b(?:"
    r"opens?|opening|opened|launch(?:es|ed|ing)?|starts?|started|"
    r"announc(?:es|ed|ing)|confirm(?:s|ed|ing)|approv(?:es|ed)|reject(?:s|ed)|"
    r"consultation|deadline|trial|pilot|funding|seed|investment|jobs?|"
    r"charged|arrested|sentenced|jailed|convicted|verdict|inquest|appeal|"
    r"closed|reopened|fire|crash|collision|killed|died|death|strike|"
    r"открыв|запуска|объяв|подтверд|одобр|отклони|консультац|срок|"
    r"арест|обвин|приговор|суд|пожар|авар|погиб|забастов"
    r")\b",
    re.IGNORECASE,
)
_BOOKABLE_ACTIVITY_RE = re.compile(
    r"\b(?:"
    r"designmynight|alcotraz|treasure\s+hunt|escape\s+room|cocktail\s+bar|"
    r"big\s+manchester\s+bake|available\s+from|bookable|things?\s+to\s+do|"
    r"experience|immersive|bottomless|brunch|yoga|paint\s+and\s+sip|"
    r"можно\s+забронировать|квест|иммерсив|коктейльн"
    r")\b",
    re.IGNORECASE,
)
_RECURRING_EVENT_RE = re.compile(
    r"\b(?:every|weekly|each)\s+(?:saturdays?|sundays?|mondays?|tuesdays?|wednesdays?|thursdays?|fridays?)\b|"
    r"\b(?:runs?|regular|returns?)\s+(?:on\s+)?(?:saturdays?|sundays?|bank\s+holiday\s+mondays?)\b|"
    r"\bnext\s+dates?\b.{0,80}\b(?:saturday|sunday|monday)\b|"
    r"\b(?:saturdays?|sundays?)\s+(?:until|through|throughout)\b|"
    r"\b(?:кажд(?:ую|ое|ый|ые)|по)\s+(?:суббот|воскрес|понедельник|вторник|сред|четверг|пятниц)",
    re.IGNORECASE,
)
_FESTIVAL_RE = re.compile(r"\b(?:festival|фестивал|jazz\s+festival|flower\s+festival)\b", re.IGNORECASE)
_RESEARCH_RE = re.compile(
    r"\b(?:research|study|researchers?|professor|university|academy\s+of\s+medical\s+sciences|"
    r"исследован|профессор|университет|академи[яи]\s+медицинских\s+наук)\b",
    re.IGNORECASE,
)
_MEMORIAL_RE = re.compile(
    r"\b(?:manchester\s+arena|arena\s+(?:attack|bombing)|ariana\s+grande|"
    r"22\s+(?:people|lives)|terror\s+attack|теракт|годовщин)\b",
    re.IGNORECASE,
)
_PUBLIC_SERVICE_STALE_RE = re.compile(
    r"\b(?:resident\s+doctors?|junior\s+doctors?|strike|забастовк)\b.*\b(?:0?7\s+april|0?13\s+april|апрел)",
    re.IGNORECASE | re.DOTALL,
)
_OLD_EXISTING_FOOD_RE = re.compile(
    r"\b(?:since|from|started|began|launched)\s+(?:in\s+)?20(?:1\d|2[0-3])\b|"
    r"\b(?:back\s+in|starting\s+off\s+life\s+as|started\s+life\s+as).{0,80}\b20(?:1\d|2[0-3])\b|"
    r"\bработа(?:ет|ла)\s+с\s+20(?:1\d|2[0-3])\b",
    re.IGNORECASE,
)
_REAL_OPENING_ACTION_RE = re.compile(
    r"\b(?:opens?|opening|launch(?:es|ed)?|new\s+(?:site|venue|branch|home)|"
    r"second\s+(?:site|branch)|reopen(?:s|ed|ing)|from\s+\d{1,2}\s+[a-z]+|"
    r"in\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+20\d{2}|"
    r"с\s+\d{1,2}\s+[а-яё]+)\b",
    re.IGNORECASE,
)
_NEW_PHASE_RE = re.compile(
    r"\b(?:"
    r"charged|arrested|sentenced|jailed|convicted|verdict|trial|hearing|inquest|appeal|cps|"
    r"approved|rejected|submitted|consultation|deadline|opens?|opened|reopens?|launched|"
    r"confirmed|announced|updated|new\s+date|sale\s+(?:starts|opens)|on\s+sale|"
    r"обвин|арест|приговор|вердикт|слушан|расследован|одобр|отклон|подан|консультац|"
    r"откры|запуск|подтверд|обнов|новая\s+дат|продаж"
    r")\b",
    re.IGNORECASE,
)

_TOPIC_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("politics:makerfield_by_election_2026", re.compile(r"\b(?:makerfield|josh\s+simons|andy\s+burnham.*makerfield|by-?election.*makerfield)\b", re.IGNORECASE)),
    ("memorial:manchester_arena_anniversary", _MEMORIAL_RE),
    ("incident:bolton_erika_de_souza_correia", re.compile(r"\b(?:erika\s+de\s+souza|de\s+souza\s+correia|walker\s+fold|police\s+pursuit)\b", re.IGNORECASE)),
    ("opening:grub_stretford", re.compile(r"\b(?:grub\b.*stretford|stretford.*\bgrub\b|sir\s+tony\s+lloyd\s+square)\b", re.IGNORECASE)),
    ("opening:old_abbey_taphouse_hulme", re.compile(r"\b(?:old\s+abbey\s+taphouse|the\s+abbey.*hulme|guildhall\s+road)\b", re.IGNORECASE)),
    ("property:pelican_inn_altrincham_north_lodge", re.compile(r"\b(?:pelican\s+inn|altrincham\s+north\s+lodge|greene\s+king.*timperley)\b", re.IGNORECASE)),
    ("event:manchester_flower_festival_2026", re.compile(r"\b(?:manchester\s+flower\s+festival|floral\s+trail)\b", re.IGNORECASE)),
    ("event:manchester_jazz_festival_2026", re.compile(r"\b(?:manchester\s+jazz\s+festival|yellowjackets|china\s+moses|andy\s+sheppard)\b", re.IGNORECASE)),
    ("event:bowlee_car_boot_sale", re.compile(r"\b(?:bowlee|bowlee\s+community\s+park)\b", re.IGNORECASE)),
    ("event:barton_aerodrome_car_boot", re.compile(r"\b(?:barton\s+aerodrome)\b", re.IGNORECASE)),
    ("event:big_stockport_car_boot", re.compile(r"\b(?:big\s+stockport\s+car\s+boot|waterside\s+farm|otterspool\s+road|romiley)\b", re.IGNORECASE)),
    ("event:rent_hope_mill_theatre", re.compile(r"\b(?:\brent\b.*(?:ancoats|hope\s+mill|stranger\s+things)|hope\s+mill.*\brent\b)\b", re.IGNORECASE)),
    ("research:alcohol_addiction_recovery_brain", re.compile(r"\b(?:alcohol(?:ic)?\s+addiction|alcohol\s+dependence|recovery.*brain|brain.*recovery.*alcohol)\b", re.IGNORECASE)),
)

_WEEKDAY_NAMES: dict[int, str] = {
    0: "понедельник",
    1: "вторник",
    2: "среду",
    3: "четверг",
    4: "пятницу",
    5: "субботу",
    6: "воскресенье",
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


def _contract_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in (
            "title",
            "summary",
            "lead",
            "practical_angle",
            "evidence_text",
            "source_label",
            "source_url",
        )
    )


def _topic_key(candidate: dict, story_type: str = "", event_shape: str = "") -> str:
    blob = _contract_blob(candidate)
    short_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "source_label")
    )
    for key, pattern in _TOPIC_PATTERNS:
        if pattern.search(short_blob):
            return key
    for key, pattern in _TOPIC_PATTERNS:
        if pattern.search(blob):
            return key
    if str(candidate.get("category") or "") == "venues_tickets":
        title = re.sub(r"\s+[—–-]\s+(?:event|public\s+sale).*$", "", str(candidate.get("title") or ""), flags=re.IGNORECASE)
        venue = ticket_venue(candidate)
        return "ticket:" + normalize_title(f"{title} {venue}")[:120]
    if event_shape and event_shape != "none":
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        title = str(event.get("event_name") or candidate.get("title") or "")
        venue = str(event.get("venue") or ticket_venue(candidate) or candidate.get("source_label") or "")
        return "event:" + normalize_title(f"{title} {venue}")[:120]
    if story_type in {"research", "human_interest", "soft_news"}:
        return f"{story_type}:" + normalize_title(str(candidate.get("title") or ""))[:120]
    source = normalize_title(str(candidate.get("source_label") or ""))
    title = normalize_title(str(candidate.get("title") or ""))
    return "generic:" + normalize_title(f"{source} {title}")[:140]


def _event_shape(candidate: dict) -> str:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if category == "venues_tickets" or block in {"ticket_radar", "outside_gm_tickets"}:
        return "ticket"
    if block not in _EVENT_BLOCKS and category not in {"culture_weekly", "russian_speaking_events"}:
        return "none"
    blob = _contract_blob(candidate)
    bookable_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text", "source_label")
    )
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if event.get("is_recurring") or _RECURRING_EVENT_RE.search(blob):
        return "recurring"
    if _BOOKABLE_ACTIVITY_RE.search(bookable_blob) and not re.search(r"\b(?:car boot|market|festival|fair)\b", bookable_blob, re.IGNORECASE):
        return "bookable_activity"
    if _FESTIVAL_RE.search(blob):
        return "festival"
    if event.get("date_start") or event.get("date") or summary_field_datetime(str(candidate.get("summary") or ""), "event_date"):
        return "one_off"
    return "event_like"


def _story_type(candidate: dict, event_shape: str) -> str:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    source = str(candidate.get("source_label") or "").lower()
    blob = _contract_blob(candidate)
    lowered = blob.lower()
    if block in _OPERATIONAL_BLOCKS:
        return block
    if event_shape == "ticket":
        return "ticket"
    if event_shape != "none":
        return "event"
    if _MEMORIAL_RE.search(blob):
        return "memorial"
    if _HUMAN_INTEREST_RE.search(blob):
        return "human_interest"
    if _RESEARCH_RE.search(blob) or "university" in source:
        return "research"
    if category == "food_openings" or block == "openings":
        if _OLD_EXISTING_FOOD_RE.search(blob) and not _REAL_OPENING_ACTION_RE.search(blob):
            return "old_existing_food"
        return "opening"
    if re.search(r"\b(?:planning|development|application|developer|housing|pub|building|site|junction|road\s+scheme)\b", lowered):
        return "planning"
    if re.search(r"\b(?:police|gmp|court|charged|arrested|sentenced|crash|collision|fire|death|killed|died)\b", lowered):
        return "incident"
    if re.search(r"\b(?:council|mayor|election|by-?election|candidate|consultation)\b", lowered):
        return "civic"
    if re.search(r"\b(?:award|celebration|anniversary|selected|fellow|lord\s+mayor|community\s+champion)\b", lowered):
        return "soft_news"
    return "news"


def _anchor_type(candidate: dict, story_type: str, event_shape: str) -> str:
    block = str(candidate.get("primary_block") or "")
    if block == "weather":
        return "today_weather"
    if block == "transport":
        return "service_status"
    if story_type == "ticket":
        return "ticket_opportunity"
    if event_shape in {"one_off", "festival"}:
        return "dated_event"
    if event_shape == "recurring":
        return "recurring_occurrence"
    if event_shape == "bookable_activity":
        return "bookable_listing"
    blob = _contract_blob(candidate)
    if _MEMORIAL_RE.search(blob):
        if re.search(r"\b(?:today|22\s+may|anniversary|годовщин|сегодня)\b", blob, re.IGNORECASE):
            return "anniversary_today"
        return "memorial_plan"
    if _PUBLIC_SERVICE_STALE_RE.search(blob):
        return "stale_public_service"
    if _NEW_PHASE_RE.search(blob):
        return "new_phase"
    if _LOCAL_ACTION_RE.search(blob):
        return "local_action"
    if story_type == "research":
        return "research_publication"
    if story_type == "human_interest":
        return "biographical_profile"
    return "none"


def _publish_tier(candidate: dict, story_type: str, event_shape: str, anchor_type: str) -> str:
    block = str(candidate.get("primary_block") or "")
    if block in {"weather", "transport"}:
        return "must_include"
    if anchor_type == "stale_public_service":
        return "reject"
    if story_type == "old_existing_food":
        return "reject"
    if story_type == "human_interest" and anchor_type in {"biographical_profile", "none"}:
        return "reject"
    if story_type == "soft_news" and anchor_type in {"none", "local_action"}:
        return "filler"
    if story_type == "research" and anchor_type == "research_publication":
        return "filler"
    if event_shape == "bookable_activity":
        return "filler"
    if event_shape in {"festival", "recurring"}:
        return "strong"
    if story_type in {"incident", "planning", "civic", "opening", "memorial"} and anchor_type != "none":
        return "strong"
    if story_type == "ticket":
        ticket_type = classify_ticket_type(candidate)
        if ticket_type in {"on_sale_now", "presale_soon", "newly_listed", "major_upcoming"}:
            return "strong"
        return "optional"
    if anchor_type in {"new_phase", "local_action", "dated_event", "recurring_occurrence"}:
        return "optional"
    return "filler"


def _weekday_from_text(text: str) -> int | None:
    lowered = text.lower()
    if re.search(r"\b(?:saturdays?|суббот)", lowered):
        return 5
    if re.search(r"\b(?:sundays?|воскрес)", lowered):
        return 6
    if re.search(r"\b(?:mondays?|понедельник)", lowered):
        return 0
    if re.search(r"\b(?:tuesdays?|вторник)", lowered):
        return 1
    if re.search(r"\b(?:wednesdays?|сред)", lowered):
        return 2
    if re.search(r"\b(?:thursdays?|четверг)", lowered):
        return 3
    if re.search(r"\b(?:fridays?|пятниц)", lowered):
        return 4
    return None


def _next_weekday(day: int, *, today: date | None = None) -> date:
    base = today or now_london().date()
    delta = (day - base.weekday()) % 7
    return base + timedelta(days=delta)


def event_occurrence(candidate: dict) -> dict[str, object]:
    """Return the actionable occurrence the reader needs, not stale start dates."""
    shape = _event_shape(candidate)
    blob = _contract_blob(candidate)
    today = now_london().date()
    if shape == "recurring":
        weekday = _weekday_from_text(blob)
        if weekday is None:
            return {"shape": shape, "date": "", "date_text": "повторяющееся событие"}
        occurrence = _next_weekday(weekday, today=today)
        return {
            "shape": shape,
            "weekday": weekday,
            "date": occurrence.isoformat(),
            "date_text": f"в {_WEEKDAY_NAMES[weekday]} {occurrence.day} {_RU_MONTHS_GENITIVE[occurrence.month]}",
        }
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("date_start") or event.get("date") or "").strip()
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
            return {
                "shape": shape,
                "date": parsed.isoformat(),
                "date_text": f"{parsed.day} {_RU_MONTHS_GENITIVE[parsed.month]}",
            }
        except ValueError:
            pass
    event_dt = summary_field_datetime(str(candidate.get("summary") or ""), "event_date")
    if event_dt:
        parsed = event_dt.date()
        return {
            "shape": shape,
            "date": parsed.isoformat(),
            "date_text": f"{parsed.day} {_RU_MONTHS_GENITIVE[parsed.month]}",
        }
    return {"shape": shape, "date": "", "date_text": ""}


def build_editorial_contract(candidate: dict) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {}
    event_shape = _event_shape(candidate)
    story_type = _story_type(candidate, event_shape)
    anchor_type = _anchor_type(candidate, story_type, event_shape)
    tier = _publish_tier(candidate, story_type, event_shape, anchor_type)
    occurrence = event_occurrence(candidate) if event_shape != "none" else {}
    topic_key = _topic_key(candidate, story_type, event_shape)
    reject_reason = ""
    if tier == "reject":
        if story_type == "human_interest":
            reject_reason = "no_news_anchor"
        elif story_type == "old_existing_food":
            reject_reason = "old_existing_food"
        elif anchor_type == "stale_public_service":
            reject_reason = "stale_public_service"
        else:
            reject_reason = "editorial_contract_reject"
    return {
        "version": EDITORIAL_CONTRACT_VERSION,
        "story_type": story_type,
        "topic_key": topic_key,
        "anchor_type": anchor_type,
        "event_shape": event_shape,
        "occurrence": occurrence,
        "publish_tier": tier,
        "reject_reason": reject_reason,
        "section_policy": {
            "allow_public": tier != "reject",
            "global_budget_class": "public_utility" if tier in {"must_include", "strong"} else tier,
            "repeat_ttl_days": 3 if story_type in {"incident", "memorial", "opening", "research"} else 1,
        },
    }


def attach_editorial_contract(candidate: dict) -> dict:
    if not isinstance(candidate, dict):
        return candidate
    contract = build_editorial_contract(candidate)
    candidate["editorial_contract"] = contract
    candidate["topic_key"] = contract.get("topic_key", "")
    candidate["publish_tier"] = contract.get("publish_tier", "")
    if contract.get("event_shape") and contract.get("event_shape") != "none":
        candidate["event_shape"] = contract.get("event_shape")
    if contract.get("occurrence"):
        candidate["event_occurrence"] = contract.get("occurrence")
    return candidate


def topic_key_for_candidate(candidate: dict) -> str:
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    if not contract:
        contract = build_editorial_contract(candidate)
    return str(contract.get("topic_key") or "")


def is_specific_topic_key(topic_key: str) -> bool:
    key = str(topic_key or "")
    return any(key.startswith(prefix) for prefix in _SPECIFIC_TOPIC_PREFIXES)


def lifecycle_repeat_review(candidate: dict, previous: dict) -> dict[str, object]:
    """Cross-day repeat policy for named topics.

    This is intentionally narrow: only specific topic keys are eligible.
    It avoids the dangerous "similar word" behaviour that could suppress
    unrelated stories across boroughs.
    """
    current_contract = build_editorial_contract(candidate)
    previous_contract = (
        previous.get("editorial_contract")
        if isinstance(previous.get("editorial_contract"), dict)
        else build_editorial_contract(previous)
    )
    topic = str(current_contract.get("topic_key") or "")
    if not topic or topic != str(previous_contract.get("topic_key") or ""):
        return {"repeat": False, "reason": "different_topic"}
    if not is_specific_topic_key(topic):
        return {"repeat": False, "reason": "generic_topic"}
    if str(candidate.get("primary_block") or "") in _OPERATIONAL_BLOCKS:
        return {"repeat": False, "reason": "operational_repeat_allowed"}

    anchor = str(current_contract.get("anchor_type") or "")
    story_type = str(current_contract.get("story_type") or "")
    event_shape = str(current_contract.get("event_shape") or "")
    if event_shape in {"ticket", "recurring", "festival", "one_off"}:
        current_occurrence = ((current_contract.get("occurrence") or {}) if isinstance(current_contract.get("occurrence"), dict) else {})
        previous_occurrence = ((previous_contract.get("occurrence") or {}) if isinstance(previous_contract.get("occurrence"), dict) else {})
        if current_occurrence.get("date") and current_occurrence.get("date") != previous_occurrence.get("date"):
            return {"repeat": False, "reason": "new_event_occurrence"}

    if anchor in {"new_phase", "dated_event", "ticket_opportunity", "service_status", "today_weather"}:
        return {"repeat": False, "reason": f"publishable_anchor:{anchor}"}
    if story_type in {"incident", "memorial", "opening", "research", "human_interest", "soft_news"}:
        return {
            "repeat": True,
            "topic_key": topic,
            "reason": f"topic_lifecycle_rehash:{story_type}:{anchor}",
        }
    return {"repeat": False, "reason": "not_lifecycle_suppressed"}


def copy_invariant_errors(candidate: dict, line: str) -> list[str]:
    text = str(line or "")
    errors: list[str] = []
    lowered = text.lower()
    if re.search(r"(?:вероятность\s+осадков\s+)?до\s+0\s*%", lowered):
        errors.append("weather_zero_percent_wording")
    if "днём заметно теплее утра" in lowered or "днем заметно теплее утра" in lowered:
        errors.append("weather_empty_temperature_comparison")
    if re.search(r"\bГМ\b", text):
        errors.append("unexplained_gm_abbreviation")
    if re.search(r"заброшенн\w*\s+(?:паб|здани|мотел|объект).{0,80}\bзакры", lowered, re.DOTALL):
        errors.append("abandoned_building_closed_contradiction")
    if str(candidate.get("category") or "") == "venues_tickets":
        onsale_at = summary_field_datetime(str(candidate.get("summary") or ""), "public_onsale")
        if onsale_at and onsale_at.date() < now_london().date():
            if re.search(
                r"\b(?:будут\s+доступны|станут\s+доступны|будут\s+в\s+продаже|"
                r"поступ(?:ят|ит)?\s+в\s+продаж|старт(?:ует|уют)\s+продаж|"
                r"откро(?:ется|ются)\s+продаж)",
                lowered,
            ):
                errors.append("past_ticket_sale_written_as_future")
    return errors
