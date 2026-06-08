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
    "держать своих подписчиков в курсе",
    "держать подписчиков в курсе",
    "будет держать в курсе",
    "держит в курсе событий",
    "в курсе событий",
    "будет сообщать новости",
    "поделится подробностями позже",
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
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    event_venue = str(event.get("venue") or "").strip()
    if event_venue:
        return event_venue
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


_TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\s+(20\d{2})\b",
    re.IGNORECASE,
)


def _ticket_date_token(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("date_start") or event.get("date") or "").strip()
    if raw:
        return raw[:10]
    event_dt = summary_field_datetime(str(candidate.get("summary") or ""), "event_date")
    if event_dt:
        return event_dt.date().isoformat()
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead"))
    iso = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", blob)
    if iso:
        return iso.group(1)
    match = _TEXT_DATE_RE.search(blob)
    if not match:
        return ""
    day, month, year = match.groups()
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(f"{int(day)} {month} {year}", fmt).date().isoformat()
        except ValueError:
            continue
    return ""


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
# Appeals / missing-person / witness calls. The affected person is by
# definition not yet identified ("police appeal to find missing girl",
# "have you seen this man", "witnesses urged to come forward"), so a missing
# `who_affected` is expected, not a quality gap. On 2026-05-29 this held
# "Police make appeal to help find missing teenage girl" — a clearly
# publishable public-interest story — in the borderline queue.
_APPEAL_MARKERS = re.compile(
    r"\b(?:appeal|missing|have you seen|can you help|help (?:find|trace|identify)|"
    r"witness(?:es)?(?: are| have| should)?(?: urged| asked| sought)?|"
    r"come forward|urged to (?:come|contact|get in touch)|wanted in connection)\b",
    re.IGNORECASE,
)


def crime_specificity_review(candidate: dict) -> dict[str, object]:
    blob = _blob(candidate)
    # Byline noise: MEN/BBC blobs carry the reporter's job title ("Andrew
    # Bardsley, Court reporter", "Crime reporter"). On 2026-06-01 this made
    # the crime gate fire on "Andy Burnham and Nigel Farage in social media
    # clash" — a political story with zero crime content — purely because
    # "Court reporter" matched _CRIME_MARKERS on "court". Strip those byline
    # phrases before detection so a journalist's beat can't crime-flag a story.
    blob = re.sub(r"\b(?:court|crime|courts)\s+(?:reporter|correspondent|editor)\b", " ", blob, flags=re.IGNORECASE)
    if not _CRIME_MARKERS.search(blob):
        return {"applies": False, "missing": [], "severity": "none"}
    is_appeal = bool(_APPEAL_MARKERS.search(blob))
    missing: list[str] = []
    # For an appeal / missing-person / search story the "what happened" IS the
    # appeal itself ("police appeal to find missing girl", "national search
    # launched"). _CRIME_EVENT only knows violent verbs (murder/stab/assault),
    # so it always reported what_happened missing and pushed every appeal to
    # borderline — where the writer then silently held it without a draft_line.
    # The appeal marker already proves there is a concrete event to report.
    if not is_appeal and not _CRIME_EVENT.search(blob):
        missing.append("what_happened")
    if not is_appeal and not (_PERSON_SIGNAL.search(blob) or "victim" in blob.lower()):
        missing.append("who_affected")
    if not _LOCATION_SIGNAL.search(blob):
        missing.append("where")
    if not (_DATE_OR_STAGE.search(blob) or _CRIME_ACTION.search(blob)):
        missing.append("why_now")
    evidence_len = len(re.sub(r"\s+", " ", str(candidate.get("evidence_text") or "")).strip())
    # #2 Rich-article guard. The field-detectors above are narrow (no abuse
    # imagery, cyberflashing, drugs-raid arrestees, knife-safety schemes), so on
    # a full article they report false "missing" and quarantine REAL crime news:
    # on 2026-06-05 a 1584-char drugs-raid story, a 1418-char child-abuse
    # sentencing, an 881-char cyberflashing case and a 1025-char knife scheme were
    # all held as borderline. A 600+ char article from a news source HAS the
    # facts — trust it and let it publish; never hold it on missing-field guesses.
    if evidence_len >= 600:
        return {
            "applies": True,
            "missing": [],
            "severity": "ok",
            "is_appeal": is_appeal,
            "enrichment_attempted": bool(candidate.get("enrichment_status")),
        }
    # Appeals are exempt from BOTH who_affected and what_happened (the appeal
    # itself is the event, the subject is implied). That leaves only `where`
    # and `why_now` checkable, so the hard floor drops to 1: a contentless
    # "Police appeal for help" stub (no location, <120 chars evidence) is still
    # hard-rejected, while a real appeal ("find missing teenage girl in
    # Bolton") carries a location → missing=[] → stays publishable.
    hard_floor = 1 if is_appeal else 3
    if len(missing) >= hard_floor and evidence_len < 120:
        severity = "hard"
    elif missing:
        severity = "borderline"
    else:
        severity = "ok"
    return {
        "applies": True,
        "missing": missing,
        "severity": severity,
        "is_appeal": is_appeal,
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
    # Cross-domain bug seen on 2026-05-27: "Man arrested over Manchester
    # synagogue attack" came back with property_borderline:
    # decision_or_action because _PROPERTY_MARKERS matched 'attack'. If
    # the story is clearly a crime / incident / court matter, property
    # review must back off — it has nothing useful to say about an arrest
    # write-up.
    if _CRIME_MARKERS.search(blob) or _CRIME_EVENT.search(blob) or _CRIME_ACTION.search(blob):
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
# Only these block an event from publishing. Price/booking_url/borough are
# enrichment that the source often does not expose on the page — requiring
# them held ~2/3 of the borderline queue (50 items on 2026-05-29) for nothing
# the reader actually needed. A reader can act on "date + venue"; price can be
# checked at the door. So those stay in `missing` for reporting but never gate.
_EVENT_REQUIRED_FIELDS = ("date_start", "venue")


def event_schema_completeness(candidate: dict) -> dict[str, object]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if not event or not event.get("is_event"):
        return {
            "applies": False,
            "score": 0,
            "missing": list(_EVENT_COMPLETENESS_FIELDS),
            "required_missing": list(_EVENT_REQUIRED_FIELDS),
        }
    present = []
    for field in _EVENT_COMPLETENESS_FIELDS:
        value = event.get("date") if field == "date_start" and not event.get("date_start") else event.get(field)
        if str(value or "").strip():
            present.append(field)
    missing = [field for field in _EVENT_COMPLETENESS_FIELDS if field not in present]
    required_missing = [field for field in _EVENT_REQUIRED_FIELDS if field not in present]
    score = int(round(len(present) / len(_EVENT_COMPLETENESS_FIELDS) * 100))
    return {
        "applies": True,
        "score": score,
        "present": present,
        "missing": missing,
        "required_missing": required_missing,
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
    if re.search(
        r"\b(?:watch|monitor|follow|check)\s+(?:for\s+)?(?:local\s+)?updates?\b|"
        r"\bbefore\s+travelling\b|\bпроверьте\s+маршрут\b",
        blob,
        re.IGNORECASE,
    ) and re.search(r"\b(?:police|gmp|fire|crash|collision|stabbing|murder|incident)\b", blob, re.IGNORECASE):
        return "update_today"
    if category == "venues_tickets" and (
        str(candidate.get("ticket_type") or "") == "event_this_week"
        or classify_ticket_type(candidate) in {
            "on_sale_now", "presale_soon", "newly_listed", "major_upcoming", "event_this_week",
        }
    ):
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
    "civic:",
    "local_cost:",
    "property:",
    "ticket:",
)

_HUMAN_INTEREST_RE = re.compile(
    r"\b(?:"
    r"started\s+in\s+a\s+garage|hobby\s+is\s+worth\s+millions|garage\s+in\s+altrincham|"
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
_PRIVATE_PROPERTY_LISTING_RE = re.compile(
    r"\b(?:"
    r"what\s+£?\d[\d,]*(?:k|,\d{3})?\s+buys\s+you|"
    r"for\s+sale|on\s+the\s+market|asking\s+price|guide\s+price|"
    r"semi[-\s]?detached|detached\s+house|terraced\s+house|"
    r"bed(?:room)?\s+(?:home|house)|\d+[-\s]bed(?:room)?"
    r")\b",
    re.IGNORECASE,
)
_DAY_OUT_GUIDE_RE = re.compile(
    r"\b(?:"
    r"things?\s+to\s+do|day\s+out|perfect\s+for\s+(?:a\s+)?sunny\s+day|"
    r"free\s+(?:play\s+park|park)|water\s+park|beach|villages?\s+near|"
    r"watch\s+planes?\s+(?:take\s+off|coming\s+into\s+land)|"
    r"beer\s+garden\s+where\s+you\s+can\s+watch|"
    r"all\s+the\s+places\s+you\s+can(?:not|'t)?|where\s+you\s+can(?:not|'t)|"
    r"family\s+editor|kids\s+can\s+watch|near\s+manchester"
    r")\b",
    re.IGNORECASE,
)
_SOFT_NEWS_RE = re.compile(
    r"\b(?:"
    r"quiz|general\s+knowledge|lazy\s+sunday|goes\s+viral|gone\s+viral|"
    r"viral\s+(?:video|clip|post)|"
    r"(?:dad|mum|couple|family).{0,60}\b(?:pool|garden|diy|hack)|"
    r"12ft\s+pool|garden\s+pool|"
    r"bargain\s+(?:hunter|home|house|garden)|"
    r"readers?\s+react|people\s+are\s+saying"
    r")\b",
    re.IGNORECASE,
)
_LOCAL_COST_RE = re.compile(
    r"\b(?:parking|car\s+park|bus\s+fare|fares?|council\s+tax|rent|rents|"
    r"price(?:s)?|cost(?:s)?|expensive|charges?|tariff)\b",
    re.IGNORECASE,
)
_PUBLIC_REALM_RE = re.compile(
    r"\b(?:bridge|lights?|lanterns?|restoration|restored|reopened|public\s+realm|"
    r"junction|crossing|streets?\s+for\s+all|road\s+scheme|cycle\s+lane|"
    r"мост|фонар|реставрац|перекр[её]ст|переход|улиц)\b",
    re.IGNORECASE,
)
_LOCAL_ACTION_RE = re.compile(
    r"\b(?:"
    r"opens?|opening|opened|launch(?:es|ed|ing)?|starts?|started|plans?|planned|"
    r"announc(?:es|ed|ing)|confirm(?:s|ed|ing)|approv(?:es|ed)|reject(?:s|ed)|"
    r"clos(?:e|es|ed|ing)|take(?:s|n)?\s+over|set\s+to\s+take\s+over|"
    r"replac(?:e|es|ed|ing)|switch(?:es|ed|ing)|"
    r"consultation|deadline|trial|pilot|funding|seed|investment|jobs?|"
    r"restor(?:e|es|ed|ation)|reopen(?:s|ed|ing)?|"
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


def _human_interest_has_public_anchor(candidate: dict) -> bool:
    blob = _contract_blob(candidate).lower()
    return bool(
        re.search(
            r"\b(?:"
            r"opens?|opened|opening|launch(?:es|ed|ing)?\s+(?:a\s+)?(?:office|site|venue|charity|workshop|programme)|"
            r"new\s+(?:office|site|venue|charity|workshop|programme)|"
            r"jobs?|funding|seed|investment|grant|"
            r"event\s+on|workshop\s+on"
            r")\b",
            blob,
            re.IGNORECASE,
        )
    )
_NEW_PHASE_RE = re.compile(
    r"\b(?:"
    r"charged|arrested|sentenced|jailed|convicted|verdict|trial|hearing|inquest|appeal|cps|"
    r"approved|rejected|submitted|consultation|deadline|opens?|opened|reopens?|launched|"
    r"clos(?:e|es|ed|ing)|take(?:s|n)?\s+over|set\s+to\s+take\s+over|replac(?:e|es|ed|ing)|"
    r"confirmed|announced|updated|plans?|planned|new\s+date|sale\s+(?:starts|opens)|on\s+sale|"
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
    # Do not scan evidence_text/source_url for named topics: event pages often
    # contain unrelated page chrome and recommendation links, which previously
    # made unrelated stories inherit a specific event topic.
    if str(candidate.get("category") or "") == "venues_tickets":
        title = re.sub(r"\s+[—–-]\s+(?:event|public\s+sale).*$", "", str(candidate.get("title") or ""), flags=re.IGNORECASE)
        # Strip Ticketmaster premium/resale prefixes so the same concert
        # served as "Calum Scott", "Venue Premium Tickets - Calum Scott"
        # and "VIP Package - Calum Scott" collapses to one cluster
        # instead of three rejected duplicates.
        title = re.sub(
            r"^(?:venue\s+premium\s+tickets|premium\s+tickets|vip\s+package|"
            r"resale\s+tickets|official\s+platinum|platinum\s+tickets|"
            r"hospitality\s+packages?)\s*[-–—:]\s*",
            "",
            title,
            flags=re.IGNORECASE,
        ).strip()
        try:
            from news_digest.pipeline.ticket_notability import ticket_artist_name  # noqa: PLC0415
            title = ticket_artist_name(candidate) or title
        except Exception:  # noqa: BLE001
            pass
        venue = ticket_venue(candidate)
        # Include the event date so Calum Scott on 2026-05-27 and Calum
        # Scott on 2026-05-28 do NOT collapse into one cluster — that
        # cost us a second day-of concert in the 2026-05-27 report
        # under verdict=dedupe_lost_event.
        date_token = _ticket_date_token(candidate)
        suffix = f"|{date_token}" if date_token else ""
        return "ticket:" + normalize_title(f"{title} {venue}")[:120] + suffix
    if event_shape and event_shape != "none":
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        title = str(event.get("event_name") or candidate.get("title") or "")
        venue = str(event.get("venue") or ticket_venue(candidate) or candidate.get("source_label") or "")
        if event_shape == "recurring" and re.search(r"\b(?:market|car boot|makers market|artisan market|flea market)\b", _contract_blob(candidate), re.IGNORECASE):
            venue = str(event.get("venue") or candidate.get("source_label") or ticket_venue(candidate) or "")
        return "event:" + normalize_title(f"{title} {venue}")[:120]
    if story_type in {"planning", "civic", "incident", "opening", "memorial", "local_cost", "local_service_change"}:
        title = normalize_title(str(candidate.get("title") or ""))
        entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
        entity_bits: list[str] = []
        for key in ("venues", "councils", "boroughs", "districts", "stations"):
            values = entities.get(key)
            if isinstance(values, list):
                entity_bits.extend(str(value) for value in values[:2] if str(value).strip())
        entity_text = normalize_title(" ".join(entity_bits))
        joined = normalize_title(f"{entity_text} {title}") if entity_text else title
        return f"{story_type}:{joined[:140]}"
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
    if _PRIVATE_PROPERTY_LISTING_RE.search(blob) and not re.search(
        r"\b(?:planning|development|developer|approved|submitted|council|hotel|tower|skyscraper|office|scheme|regeneration)\b",
        lowered,
    ):
        return "property_listing"
    if _DAY_OUT_GUIDE_RE.search(blob) and not re.search(
        r"\b(?:opens?|opened|launch(?:es|ed)?|closed|closure|planning|approved|submitted|fire|court|police|charged|arrested)\b",
        lowered,
    ):
        return "day_out_guide"
    if _SOFT_NEWS_RE.search(blob):
        return "soft_news"
    if _HUMAN_INTEREST_RE.search(blob):
        return "human_interest"
    if re.search(r"\b(?:started\s+in\s+a\s+garage|hobby\s+is\s+worth\s+millions|turned\s+(?:a\s+)?hobby|from\s+garage)\b", lowered):
        return "human_interest"
    if _RESEARCH_RE.search(blob) or "university" in source:
        return "research"
    if category == "food_openings" or block == "openings":
        if _OLD_EXISTING_FOOD_RE.search(blob) and not _REAL_OPENING_ACTION_RE.search(blob):
            return "old_existing_food"
        return "opening"
    if _PUBLIC_REALM_RE.search(blob) and re.search(r"\b(?:council|gmca|rochdale|bury|stockport|trafford|oldham|wigan|bolton|tameside|manchester)\b", lowered):
        return "planning"
    if re.search(r"\b(?:planning|development|application|developer|housing|hotel|tower|skyscraper|pub|building|site|junction|road\s+scheme)\b", lowered):
        return "planning"
    if _LOCAL_COST_RE.search(blob) and _LOCATION_SIGNAL.search(blob):
        return "local_cost"
    if re.search(
        r"\b(?:supermarket|store|shop|retail|asda|waitrose|tesco|sainsbury|aldi|lidl|morrisons|co-op)\b",
        lowered,
    ) and re.search(r"\b(?:clos(?:e|es|ed|ing)|take(?:s|n)?\s+over|set\s+to\s+take\s+over|replac(?:e|es|ed|ing))\b", lowered):
        return "local_service_change"
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
    if story_type == "local_cost" and why_now_is_publishable(infer_why_now(candidate)):
        return "new_local_cost"
    return "none"


def _publish_tier(candidate: dict, story_type: str, event_shape: str, anchor_type: str) -> str:
    block = str(candidate.get("primary_block") or "")
    if block in {"weather", "transport"}:
        return "must_include"
    if anchor_type == "stale_public_service":
        return "reject"
    if story_type in {"old_existing_food", "property_listing", "day_out_guide"}:
        return "reject"
    if story_type == "human_interest" and not _human_interest_has_public_anchor(candidate):
        return "reject"
    if story_type == "soft_news" and anchor_type in {"none", "local_action"}:
        return "filler"
    if story_type == "research" and anchor_type == "research_publication":
        return "filler"
    if event_shape == "bookable_activity":
        return "filler"
    if event_shape in {"festival", "recurring"}:
        return "strong"
    if story_type in {"incident", "planning", "civic", "opening", "memorial", "local_cost", "local_service_change"} and anchor_type != "none":
        return "strong"
    if story_type == "ticket":
        ticket_type = str(candidate.get("ticket_type") or "") or classify_ticket_type(candidate)
        if ticket_type in {"on_sale_now", "presale_soon", "newly_listed", "major_upcoming", "event_this_week"}:
            return "strong"
        return "optional"
    if anchor_type in {"new_phase", "local_action", "dated_event", "recurring_occurrence"}:
        return "optional"
    return "filler"


def history_window_days_for_contract(story_type: str, event_shape: str, anchor_type: str = "") -> int:
    """How far back cross-day repeat checks should look for this rubric.

    A single global window is unsafe: court/planning stories need a longer
    memory, while recurring markets must remain eligible for their next
    occurrence instead of being collapsed forever.
    """
    story_type = str(story_type or "")
    event_shape = str(event_shape or "")
    anchor_type = str(anchor_type or "")
    if event_shape == "recurring":
        return 2
    if event_shape in {"ticket", "one_off", "festival"} or story_type == "ticket":
        return 30
    if story_type in {"incident", "planning", "civic", "memorial", "local_cost"}:
        return 14
    if story_type in {"opening", "research"}:
        return 7
    if story_type in {"human_interest", "soft_news", "property_listing", "day_out_guide", "old_existing_food"}:
        return 2
    if anchor_type in {"new_phase", "local_action"}:
        return 7
    return 7


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
            event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
            raw = str(event.get("date_start") or event.get("date") or event.get("date_end") or "").strip()
            if raw:
                try:
                    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
                    if parsed >= today:
                        return {
                            "shape": shape,
                            "date": parsed.isoformat(),
                            "date_text": f"{parsed.day} {_RU_MONTHS_GENITIVE[parsed.month]}",
                        }
                except ValueError:
                    pass
            return {"shape": shape, "date": "", "date_text": ""}
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


def _first_location(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    for value in (
        event.get("venue"),
        candidate.get("borough"),
        candidate.get("district"),
        candidate.get("area"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    for key in ("venues", "boroughs", "districts", "stations", "places"):
        values = entities.get(key)
        if isinstance(values, list):
            for value in values:
                text = str(value or "").strip()
                if text:
                    return text
    blob = _contract_blob(candidate)
    match = re.search(
        r"\b(?:in|at|on)\s+([A-Z][A-Za-z'’-]+(?:\s+[A-Z][A-Za-z'’-]+){0,3})\b",
        blob,
    )
    return match.group(1).strip() if match else ""


def _first_when(candidate: dict, occurrence: dict[str, object]) -> str:
    text = str((occurrence or {}).get("date_text") or "").strip()
    if text:
        return text
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("date_start") or event.get("date") or candidate.get("published_at") or "").strip()
    if raw:
        return raw[:16].replace("T", " ")
    return ""


def story_frame_for_candidate(
    candidate: dict,
    *,
    story_type: str = "",
    event_shape: str = "",
    anchor_type: str = "",
    occurrence: dict[str, object] | None = None,
) -> dict[str, object]:
    """Single public-output frame: what/where/when/who/why now/missing facts."""
    title = re.sub(r"\s+", " ", str(candidate.get("title") or "")).strip()
    summary = re.sub(r"\s+", " ", str(candidate.get("summary") or candidate.get("lead") or "")).strip()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    event_type = story_type or str(candidate.get("category") or "")
    what = str(event.get("event_name") or "").strip() if event_shape and event_shape != "none" else ""
    if not what:
        what = title or summary[:160]
    where = _first_location(candidate)
    when = _first_when(candidate, occurrence or {})
    who = ""
    blob = _contract_blob(candidate)
    person = re.search(r"\b(?:\d{1,3}-year-old\s+)?(?:man|woman|boy|girl|child|teenager|victim|suspect|driver|teacher|council|police|residents?)\b", blob, re.IGNORECASE)
    if person:
        who = person.group(0)
    elif event_shape and event_shape != "none":
        who = "посетители события"
    why_now = infer_why_now(candidate)
    missing: list[str] = []
    if not what:
        missing.append("what_happened")
    if not where and event_type not in {"weather"}:
        missing.append("where_exact")
    if not when and event_type not in {"transport", "weather"}:
        missing.append("when")
    if event_type in {"incident", "civic", "planning", "local_service_change"} and not who:
        missing.append("who_affected")
    if not why_now_is_publishable(why_now):
        missing.append("why_now")
    if event_shape in {"ticket", "one_off", "festival", "recurring"}:
        if not where:
            missing.append("venue")
        if not when:
            missing.append("event_date")
    return {
        "version": "2026-06-02.1",
        "event_type": event_type,
        "what_happened": what,
        "where_exact": where,
        "when": when,
        "who_affected": who,
        "why_now": why_now,
        "reader_value": candidate.get("reader_value_score") or candidate.get("reader_value_label") or "",
        "missing_facts": list(dict.fromkeys(missing)),
        "repair_policy": "repair_first_then_hold",
    }


def build_editorial_contract(candidate: dict) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {}
    event_shape = _event_shape(candidate)
    story_type = _story_type(candidate, event_shape)
    anchor_type = _anchor_type(candidate, story_type, event_shape)
    tier = _publish_tier(candidate, story_type, event_shape, anchor_type)
    occurrence = event_occurrence(candidate) if event_shape != "none" else {}
    topic_key = _topic_key(candidate, story_type, event_shape)
    story_frame = story_frame_for_candidate(
        candidate,
        story_type=story_type,
        event_shape=event_shape,
        anchor_type=anchor_type,
        occurrence=occurrence,
    )
    reject_reason = ""
    if tier == "reject":
        if story_type == "human_interest":
            reject_reason = "no_news_anchor"
        elif story_type == "old_existing_food":
            reject_reason = "old_existing_food"
        elif story_type == "property_listing":
            reject_reason = "property_listing"
        elif story_type == "day_out_guide":
            reject_reason = "day_out_guide"
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
        "story_frame": story_frame,
        "publish_tier": tier,
        "reject_reason": reject_reason,
        "section_policy": {
            "allow_public": tier != "reject",
            "global_budget_class": "public_utility" if tier in {"must_include", "strong"} else tier,
            "repeat_ttl_days": 3 if story_type in {"incident", "memorial", "opening", "research"} else 1,
            "history_window_days": history_window_days_for_contract(story_type, event_shape, anchor_type),
        },
    }


def attach_editorial_contract(candidate: dict) -> dict:
    if not isinstance(candidate, dict):
        return candidate
    contract = build_editorial_contract(candidate)
    candidate["editorial_contract"] = contract
    candidate["topic_key"] = contract.get("topic_key", "")
    candidate["publish_tier"] = contract.get("publish_tier", "")
    candidate["story_frame"] = contract.get("story_frame") or {}
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


_CALENDAR_REPEAT_MILESTONE_DAYS = frozenset({0, 1, 7, 14, 30})


def _occurrence_date_from_contract(contract: dict) -> date | None:
    occurrence = contract.get("occurrence") if isinstance(contract.get("occurrence"), dict) else {}
    raw = str(occurrence.get("date") or "").strip()
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except ValueError:
            pass
    return None


def _ticket_sale_date(candidate: dict) -> date | None:
    onsale_at = summary_field_datetime(str(candidate.get("summary") or ""), "public_onsale")
    if onsale_at:
        return onsale_at.date()
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead"))
    match = re.search(r"\bpublic\s+sale\s+(\d{4}-\d{2}-\d{2})\b", blob, re.IGNORECASE)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def calendar_repeat_review(candidate: dict, previous: dict) -> dict[str, object]:
    """Decide whether a previously published event/ticket deserves a repeat.

    This is the product rule behind "do not show Jason Isbell / diaspora /
    market cards every morning": first discovery is publishable; later repeats
    need a real reader moment (today/tomorrow, a milestone, or a fresh sale).
    """
    current_contract = build_editorial_contract(candidate)
    previous_contract = (
        previous.get("editorial_contract")
        if isinstance(previous.get("editorial_contract"), dict)
        else build_editorial_contract(previous)
    )
    event_shape = str(current_contract.get("event_shape") or "")
    story_type = str(current_contract.get("story_type") or "")
    if event_shape not in {"ticket", "recurring", "festival", "one_off", "event_like"} and story_type != "ticket":
        return {"applies": False, "allow": True, "reason": "not_calendar_item"}

    current_date = _occurrence_date_from_contract(current_contract)
    previous_date = _occurrence_date_from_contract(previous_contract)
    if current_date and previous_date and current_date != previous_date:
        return {"applies": True, "allow": True, "reason": "new_event_occurrence"}

    last_published = str(previous.get("last_published_day_london") or "").strip()
    today = now_london().date()
    if last_published == today.isoformat():
        return {"applies": True, "allow": False, "reason": "already_shown_today"}

    if current_date:
        days_until = (current_date - today).days
        if days_until < 0:
            return {"applies": True, "allow": False, "reason": "event_already_passed"}
        if days_until in _CALENDAR_REPEAT_MILESTONE_DAYS:
            return {
                "applies": True,
                "allow": True,
                "reason": f"event_milestone_d{days_until}",
                "days_until_event": days_until,
            }

    sale_date = _ticket_sale_date(candidate)
    if sale_date:
        sale_age = (today - sale_date).days
        if -3 <= sale_age <= 7:
            return {
                "applies": True,
                "allow": True,
                "reason": "fresh_ticket_sale",
                "sale_age_days": sale_age,
            }

    return {
        "applies": True,
        "allow": False,
        "reason": "same_calendar_item_without_new_reader_moment",
    }


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
    if event_shape in {"ticket", "recurring", "festival", "one_off", "event_like"}:
        calendar_review = calendar_repeat_review(candidate, previous)
        if calendar_review.get("allow"):
            return {"repeat": False, "reason": str(calendar_review.get("reason") or "calendar_repeat_allowed")}
        return {
            "repeat": True,
            "topic_key": topic,
            "reason": str(calendar_review.get("reason") or "same_calendar_item_without_new_reader_moment"),
            "calendar_repeat_review": calendar_review,
        }

    if anchor in {"new_phase", "service_status", "today_weather"}:
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
