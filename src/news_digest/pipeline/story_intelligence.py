from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
import re

from news_digest.pipeline.common import (
    PRIMARY_BLOCKS,
    canonical_url_identity,
    fingerprint_for_candidate,
    normalize_title,
    now_london,
)
from news_digest.pipeline.editorial_contracts import attach_editorial_contract, is_specific_topic_key
from news_digest.pipeline.reader_value import reader_value_score
from news_digest.pipeline.source_selection import pick_winner, source_score


EVIDENCE_PACKET_VERSION = 1
STORY_CLUSTER_VERSION = 1
STORY_IDENTITY_VERSION = 1
ENGLISH_JUDGE_SCHEMA_VERSION = 1
BACKUP_POOL_SCHEMA_VERSION = 1
AUDIT_TRAIL_SCHEMA_VERSION = 1

COST_LATENCY_BUDGETS: dict[str, object] = {
    "schema_version": 1,
    "warning_only": True,
    "max_total_calls": 70,
    "max_total_estimated_tokens": 220_000,
    "max_total_cost_usd": 0.20,
    "target_wall_time_seconds": 480,
    "hard_wall_time_warning_seconds": 600,
}

REASON_CODE_ENUM: tuple[str, ...] = (
    "duplicate_exact",
    "same_story_rehash",
    "new_facts",
    "no_news_anchor",
    "non_gm",
    "expired_event",
    "missing_event_date",
    "missing_venue",
    "property_listing",
    "old_existing_food",
    "human_interest_no_public_anchor",
    "bookable_activity",
    "public_safety",
    "transport_disruption",
    "planning_or_civic",
    "ticket_opportunity",
    "market_or_fair",
    "russian_event",
    "source_authority",
    "enrichment_warning",
    "other",
)

_STAGE_MARKERS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("charged", re.compile(r"\bcharged|обвин", re.IGNORECASE)),
    ("sentenced", re.compile(r"\bsentenced|jailed|приговор|осужд", re.IGNORECASE)),
    ("verdict", re.compile(r"\bverdict|convicted|guilty|вердикт|винов", re.IGNORECASE)),
    ("appeal", re.compile(r"\bappeal|апелляц", re.IGNORECASE)),
    ("approved", re.compile(r"\bapproved|одобр", re.IGNORECASE)),
    ("rejected", re.compile(r"\brejected|отклони", re.IGNORECASE)),
    ("submitted", re.compile(r"\bsubmitted|application|подан", re.IGNORECASE)),
    ("opened", re.compile(r"\bopened|reopened|launch(?:ed|es)?|открыл", re.IGNORECASE)),
    ("closed", re.compile(r"\bclosed|closure|закры", re.IGNORECASE)),
    ("named", re.compile(r"\bnamed|identified|назван", re.IGNORECASE)),
)
_IMPACT_VERB_RE = re.compile(
    r"\b(?:announc(?:e|es|ed|ing)|confirm(?:s|ed|ing)|approve(?:s|d)|reject(?:s|ed)|"
    r"submit(?:s|ted)|vote(?:s|d)|charge(?:s|d)|sentence(?:s|d)|jail(?:s|ed)|"
    r"convict(?:s|ed)|name(?:s|d)|close(?:s|d)|open(?:s|ed|ing)|reopen(?:s|ed|ing)|"
    r"launch(?:es|ed)|cancel(?:s|led)|delay(?:s|ed)|disrupt(?:s|ed)|strike(?:s)?|"
    # 2026-05-27 audit gap: regulatory / judicial / enforcement verbs were
    # missing, so "Inappropriate Instagram advert featuring Haaland banned"
    # came back with has_news_anchor=False. Added: banned, fined, evicted,
    # deported, ruled, struck, suspended, dismissed, acquitted, recalled,
    # halted, seized, raided, arrested, found guilty, pleaded.
    r"ban(?:s|ned|ning)|fin(?:e|es|ed)|evict(?:s|ed)|deport(?:s|ed)|"
    r"rule(?:s|d)|struck|suspend(?:s|ed)|dismiss(?:es|ed)|acquit(?:s|ted)|"
    r"recall(?:s|ed)|halt(?:s|ed)|seiz(?:e|es|ed)|raid(?:s|ed)|arrest(?:s|ed)|"
    r"plead(?:s|ed)|"
    r"fire|crash|collision|death|died|killed|"
    r"объяв|подтверд|одобр|отклони|голос|обвин|приговор|осужд|назван|"
    r"закры|откры|запуск|отмен|задерж|сбой|забастов|пожар|авар|погиб|"
    r"запрещ|оштрафова|выселе|депортирова|постанови|приостанов)\b",
    re.IGNORECASE,
)
_DATE_RE = re.compile(
    r"\b(?:20\d{2}-\d{2}-\d{2}|\d{1,2}(?:st|nd|rd|th)?\s+"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*|"
    r"\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|"
    r"сентября|октября|ноября|декабря)|today|tomorrow|yesterday|сегодня|завтра)\b",
    re.IGNORECASE,
)
_AMOUNT_RE = re.compile(r"\b(?:£\s*\d[\d,.]*(?:m|bn|k)?|\d[\d,.]*\s*(?:million|billion|млн|млрд|%))\b", re.IGNORECASE)
_MARKET_RE = re.compile(r"\b(?:market|makers?\s+market|car\s+boot|fair|flea|ярмарк|рынок)\b", re.IGNORECASE)
_PUBLIC_SAFETY_RE = re.compile(r"\b(?:death|died|killed|fire|crash|collision|court|charged|sentenced|stab|murder|пожар|авар|суд|обвин|погиб)\b", re.IGNORECASE)
_TRANSPORT_RE = re.compile(r"\b(?:tfgm|metrolink|rail|train|bus|tram|station|line|route|national\s+rail|трамва|автобус|поезд)\b", re.IGNORECASE)
_PLANNING_CIVIC_RE = re.compile(r"\b(?:council|planning|application|development|vote|consultation|mayor|gmca|совет|планиров|заявк|консультац)\b", re.IGNORECASE)
_TICKET_RE = re.compile(r"\b(?:ticket|tickets|on\s+sale|presale|venue|co-op live|ao arena|ticketmaster|билет)\b", re.IGNORECASE)
_ENRICHMENT_FAILURE_RE = re.compile(r"\b(?:failed|timeout|timed\s*out|403|405|429|503|cloudflare|waf|forbidden)\b", re.IGNORECASE)


def _blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )


def _unique(values: list[object], *, limit: int = 12) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _compact_text(value: object, *, limit: int = 1200) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def _parse_date_value(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
    except ValueError:
        pass
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _entity_values(candidate: dict) -> list[str]:
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    out: list[str] = []
    for key in ("people", "venues", "councils", "companies", "stations", "districts", "boroughs"):
        raw = entities.get(key)
        if isinstance(raw, list):
            out.extend(str(value) for value in raw if str(value).strip())
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    for key in ("event_name", "venue", "borough"):
        if str(event.get(key) or "").strip():
            out.append(str(event.get(key)))
    return _unique(out, limit=30)


def _stage_set(text: str) -> set[str]:
    return {name for name, pattern in _STAGE_MARKERS if pattern.search(text)}


def _story_date_key(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    for value in (
        event.get("date_start"),
        event.get("date"),
        event.get("date_end"),
        candidate.get("published_date_london"),
        str(candidate.get("published_at") or "")[:10],
    ):
        text = str(value or "").strip()
        if text:
            return normalize_title(text[:32])
    return ""


def _first_entity_key(candidate: dict, groups: tuple[str, ...]) -> str:
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    for group in groups:
        raw = entities.get(group)
        if isinstance(raw, list):
            values = [normalize_title(str(value or "")) for value in raw if normalize_title(str(value or ""))]
            if values:
                return "|".join(values[:3])
    return ""


def event_identity_key(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if not event.get("is_event"):
        return ""
    name = normalize_title(str(event.get("event_name") or candidate.get("title") or ""))
    venue = normalize_title(str(event.get("venue") or ""))
    date_key = normalize_title(str(event.get("date_start") or event.get("date") or event.get("date_text") or ""))
    if name and (venue or date_key):
        return f"event:{name[:90]}|{date_key[:32]}|{venue[:70]}"
    return ""


def story_identity_key(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    event_key = event_identity_key(candidate)
    if event_key:
        return event_key
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    topic_key = str(contract.get("topic_key") or candidate.get("topic_key") or "")
    if topic_key and is_specific_topic_key(topic_key):
        return f"topic:{topic_key}"
    subject = _first_entity_key(candidate, ("people", "companies", "venues", "councils", "stations", "clubs"))
    place = _first_entity_key(candidate, ("boroughs", "districts"))
    text = _blob(candidate)
    stages = sorted(_stage_set(text))
    story_type = normalize_title(str(contract.get("story_type") or candidate.get("category") or ""))
    stage_or_type = "|".join(stages[:2]) or story_type
    if subject and stage_or_type:
        return f"story:{subject[:100]}|{stage_or_type[:60]}|{place[:60]}"
    title = normalize_title(str(candidate.get("title") or ""))
    if title and place and story_type:
        # Last-resort identity for non-event local stories with no extracted
        # named subject. Keep it shorter than the old full-title key and include
        # place/type so two unrelated MEN articles do not merge on boilerplate.
        return f"story:{story_type[:50]}|{place[:60]}|{title[:90]}"
    return ""


def story_phase_key(candidate: dict) -> str:
    base = story_identity_key(candidate)
    if not base:
        return ""
    event_key = event_identity_key(candidate)
    if event_key:
        return event_key
    phase = str(candidate.get("change_phase") or candidate.get("change_type") or "").strip()
    text = _blob(candidate)
    stages = sorted(_stage_set(text))
    phase_key = normalize_title(phase or "|".join(stages[:2]) or "new")
    date_key = _story_date_key(candidate)
    bits = [base, phase_key]
    if date_key:
        bits.append(date_key)
    return "|".join(bits)


def attach_story_identity(candidate: dict) -> dict:
    if not isinstance(candidate, dict):
        return candidate
    event_key = event_identity_key(candidate)
    identity = story_identity_key(candidate)
    phase = story_phase_key(candidate)
    candidate["story_identity_version"] = STORY_IDENTITY_VERSION
    if event_key:
        candidate["event_identity_key"] = event_key
    if identity:
        candidate["story_identity_key"] = identity
    if phase:
        candidate["story_phase_key"] = phase
    change_type = str(candidate.get("change_type") or candidate.get("dedupe_decision") or "")
    candidate["has_new_story_phase"] = change_type in {"new_phase", "same_story_new_facts", "follow_up"}
    return candidate


def _fact_signature(candidate: dict) -> dict[str, set[str]]:
    text = _blob(candidate)
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    dates = set(_DATE_RE.findall(text))
    for key in ("date_start", "date", "date_end", "date_text"):
        value = str(event.get(key) or "").strip()
        if value:
            dates.add(value.lower())
    return {
        "entities": {normalize_title(value) for value in _entity_values(candidate) if normalize_title(value)},
        "dates": {normalize_title(value) for value in dates if normalize_title(value)},
        "amounts": {normalize_title(value) for value in _AMOUNT_RE.findall(text) if normalize_title(value)},
        "stages": _stage_set(text),
    }


def new_facts_diff(candidate: dict, previous: dict | None) -> dict[str, object]:
    if not isinstance(candidate, dict) or not isinstance(previous, dict):
        return {"has_new_facts": False, "new_fact_types": [], "new_values": {}}
    current = _fact_signature(candidate)
    old = _fact_signature(previous)
    new_values: dict[str, list[str]] = {}
    for key in ("entities", "dates", "amounts", "stages"):
        diff = sorted(current.get(key, set()) - old.get(key, set()))
        if diff:
            new_values[key] = diff[:8]
    # Entity-only diffs are too noisy across transliteration / inflection
    # ("Erica de Souza Correa" vs "Эрики де Соуза Корреа"). Treat a new
    # entity as a publishable new fact only when the text also carries a
    # new stage/date/number, e.g. named suspect + charged/court date.
    substantive = {key: value for key, value in new_values.items() if key != "entities"}
    has_new_facts = bool(substantive)
    return {
        "has_new_facts": has_new_facts,
        "new_fact_types": sorted(new_values.keys()),
        "new_values": new_values,
    }


def formal_news_anchor(candidate: dict) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {"has_news_anchor": False, "components": []}
    text = _blob(candidate)
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    entities = _entity_values(candidate)
    has_date_or_stage = bool(
        _DATE_RE.search(text)
        or str(candidate.get("published_at") or "").strip()
        or str(event.get("date_start") or event.get("date") or "").strip()
        or _stage_set(text)
    )
    has_named_entity = bool(entities)
    has_impact_verb = bool(_IMPACT_VERB_RE.search(text))
    components = []
    if has_date_or_stage:
        components.append("datable_event_or_stage")
    if has_named_entity:
        components.append("named_entity_or_place")
    if has_impact_verb:
        components.append("impact_verb")
    return {
        "has_news_anchor": has_date_or_stage and has_named_entity and has_impact_verb,
        "components": components,
        "missing": [
            name for name, present in (
                ("datable_event_or_stage", has_date_or_stage),
                ("named_entity_or_place", has_named_entity),
                ("impact_verb", has_impact_verb),
            )
            if not present
        ],
    }


def rubric_contract(candidate: dict) -> dict[str, object]:
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    story_type = str(contract.get("story_type") or "")
    event_shape = str(contract.get("event_shape") or "")
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    rubric = story_type or category or block or "generic"
    required = ["news_anchor"] if block in {"last_24h", "today_focus", "city_watch"} else []
    if block == "transport" or story_type == "transport":
        required = ["mode_or_operator", "route_line_station_or_stop", "effect"]
    elif event_shape == "recurring" or _MARKET_RE.search(_blob(candidate)):
        rubric = "weekend_market"
        required = ["next_occurrence", "venue_or_place", "time_or_price_if_available"]
    elif event_shape in {"one_off", "festival"}:
        rubric = "event"
        required = ["event_date", "venue_or_place", "event_name"]
    elif story_type == "ticket" or category == "venues_tickets":
        rubric = "ticket"
        required = ["artist_or_event", "venue", "event_date", "ticket_state"]
    elif block == "russian_events" or category in {"russian_speaking_events", "diaspora_events"}:
        rubric = "russian_event"
        required = ["artist_or_show", "venue_or_city", "date", "booking_source"]
    elif story_type in {"planning", "civic", "local_cost"}:
        required = ["decision_or_action", "place_or_authority", "why_now"]
    elif story_type == "incident":
        required = ["what_happened", "who_affected_or_stage", "where", "why_now"]
    elif story_type == "opening":
        required = ["opening_or_change", "place", "why_new"]
    return {
        "schema_version": 1,
        "rubric": rubric,
        "required_fields": required,
        "story_type": story_type,
        "event_shape": event_shape,
    }


def protected_lane(candidate: dict) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {"protected": False, "lanes": [], "reason_codes": []}
    text = _blob(candidate)
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    contract = attach_editorial_contract(candidate).get("editorial_contract") or {}
    story_type = str(contract.get("story_type") or "")
    event_shape = str(contract.get("event_shape") or "")
    lanes: list[str] = []
    codes: list[str] = []
    if block == "transport" or _TRANSPORT_RE.search(text):
        lanes.append("transport")
        codes.append("transport_disruption")
    if story_type == "incident" or _PUBLIC_SAFETY_RE.search(text):
        lanes.append("public_safety")
        codes.append("public_safety")
    if story_type in {"planning", "civic", "local_cost"} or _PLANNING_CIVIC_RE.search(text):
        lanes.append("planning_civic")
        codes.append("planning_or_civic")
    if event_shape == "recurring" or _MARKET_RE.search(text):
        lanes.append("weekend_market")
        codes.append("market_or_fair")
    if block == "russian_events" or category in {"russian_speaking_events", "diaspora_events"}:
        lanes.append("russian_event")
        codes.append("russian_event")
    if story_type == "ticket" or category == "venues_tickets" or _TICKET_RE.search(text):
        lanes.append("ticket")
        codes.append("ticket_opportunity")
    return {
        "protected": bool(lanes),
        "lanes": _unique(lanes, limit=8),
        "reason_codes": _unique(codes, limit=8),
    }


def english_judge_stub(candidate: dict) -> dict[str, object]:
    """Deterministic JSON-shaped contract for the future English judge.

    This is deliberately not an LLM call; it gives downstream stages a stable
    schema and lets us audit rejects before we turn on a model bake-off.
    """
    anchor = formal_news_anchor(candidate)
    lane = protected_lane(candidate)
    contract = attach_editorial_contract(candidate).get("editorial_contract") or {}
    tier = str(contract.get("publish_tier") or "")
    reject_reason = str(contract.get("reject_reason") or "")
    value = reader_value_score({**candidate, "included": bool(candidate.get("include", True))})
    reason_codes: list[str] = []
    if reject_reason:
        reason_codes.append(reject_reason if reject_reason in REASON_CODE_ENUM else "other")
    reason_codes.extend(lane.get("reason_codes") or [])
    if not anchor.get("has_news_anchor") and str(candidate.get("primary_block") or "") in {"last_24h", "today_focus", "city_watch"}:
        reason_codes.append("no_news_anchor")
    if tier == "reject" and not lane.get("protected"):
        decision = "reject"
    elif lane.get("protected") or value >= 60 or anchor.get("has_news_anchor"):
        decision = "publish_candidate"
    else:
        decision = "backup_candidate"
    false_negative_risk = "high" if lane.get("protected") else ("medium" if anchor.get("has_news_anchor") else "low")
    return {
        "schema_version": ENGLISH_JUDGE_SCHEMA_VERSION,
        "decision": decision,
        "section_fit": _section_fit(candidate),
        "editorial_score": section_board_score(candidate),
        "false_negative_risk": false_negative_risk,
        "reason_codes": _unique(reason_codes, limit=12),
    }


def enrichment_health(candidate: dict) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {"schema_version": 1, "status": "", "warning": False}
    status = str(candidate.get("enrichment_status") or "").strip()
    evidence = str(candidate.get("evidence_text") or "")
    summary = str(candidate.get("summary") or "")
    lead = str(candidate.get("lead") or "")
    text_chars = len(evidence.strip())
    fallback_chars = len((summary + " " + lead).strip())
    failed = bool(status and _ENRICHMENT_FAILURE_RE.search(status))
    thin = text_chars < 220 and fallback_chars < 220
    lane = protected_lane(candidate)
    anchor = formal_news_anchor(candidate)
    warning = bool((failed or thin) and (lane.get("protected") or anchor.get("has_news_anchor")))
    policy = "backup_not_reject" if warning else "normal"
    return {
        "schema_version": 1,
        "status": status,
        "evidence_chars": text_chars,
        "fallback_chars": fallback_chars,
        "failed": failed,
        "thin": thin,
        "warning": warning,
        "policy": policy,
    }


def backup_ttl_policy(candidate: dict, *, today: date | None = None) -> dict[str, object]:
    today = today or now_london().date()
    contract = attach_editorial_contract(candidate).get("editorial_contract") or {}
    rubric = str((candidate.get("rubric_contract") or {}).get("rubric") or contract.get("story_type") or candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    event_day = _parse_date_value(event.get("date_start") or event.get("date") or event.get("date_end"))

    if block in {"weather", "transport"} or rubric == "transport":
        ttl_days = 1
        reason = "transport_weather_short_ttl"
    elif rubric in {"weekend_market"} or _MARKET_RE.search(_blob(candidate)):
        ttl_days = max(1, min(4, ((event_day - today).days + 1) if event_day else 3))
        reason = "recurring_market_short_window"
    elif rubric in {"event", "ticket", "russian_event"} or block in {"weekend_activities", "next_7_days", "ticket_radar", "russian_events"}:
        ttl_days = max(1, min(45, ((event_day - today).days + 1) if event_day else 10))
        reason = "event_until_occurrence"
    elif rubric in {"incident", "planning", "civic", "local_cost"}:
        ttl_days = 14
        reason = "hard_news_followup_window"
    elif rubric in {"opening"} or block == "openings":
        ttl_days = 7
        reason = "opening_short_memory"
    else:
        ttl_days = 2
        reason = "generic_short_ttl"
    expires = today + timedelta(days=ttl_days)
    return {
        "schema_version": 1,
        "ttl_days": ttl_days,
        "expires_on_london": expires.isoformat(),
        "reason": reason,
    }


def backup_pool_record(
    candidate: dict,
    *,
    reason: str = "",
    current_day_london: str | None = None,
) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {}
    run_day = current_day_london or today_london()
    try:
        today = datetime.strptime(run_day, "%Y-%m-%d").date()
    except ValueError:
        today = now_london().date()
    fp = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
    ttl = backup_ttl_policy(candidate, today=today)
    judge = candidate.get("english_judge") if isinstance(candidate.get("english_judge"), dict) else english_judge_stub(candidate)
    lane = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else protected_lane(candidate)
    return {
        "schema_version": BACKUP_POOL_SCHEMA_VERSION,
        "created_on_london": run_day,
        "expires_on_london": ttl["expires_on_london"],
        "ttl_days": ttl["ttl_days"],
        "ttl_reason": ttl["reason"],
        "fingerprint": fp,
        "title": candidate.get("title") or "",
        "source_label": candidate.get("source_label") or "",
        "source_url": candidate.get("source_url") or "",
        "category": candidate.get("category") or "",
        "primary_block": candidate.get("primary_block") or "",
        "rubric": (candidate.get("rubric_contract") or {}).get("rubric") if isinstance(candidate.get("rubric_contract"), dict) else "",
        "protected_lanes": lane.get("lanes") or [],
        "english_judge": judge,
        "section_board_score": candidate.get("section_board_score"),
        "enrichment_health": candidate.get("enrichment_health") or enrichment_health(candidate),
        "reason": reason or str(candidate.get("reason") or ""),
    }


def _section_fit(candidate: dict) -> list[str]:
    block = str(candidate.get("primary_block") or "")
    label = PRIMARY_BLOCKS.get(block, block)
    fits = [label] if label else []
    lane = protected_lane(candidate)
    if "ticket" in lane.get("lanes", []):
        fits.append(PRIMARY_BLOCKS["ticket_radar"])
    if "transport" in lane.get("lanes", []):
        fits.append(PRIMARY_BLOCKS["transport"])
    if "weekend_market" in lane.get("lanes", []):
        fits.append(PRIMARY_BLOCKS["weekend_activities"])
    if "russian_event" in lane.get("lanes", []):
        fits.append(PRIMARY_BLOCKS["russian_events"])
    return _unique(fits, limit=6)


def section_board_score(candidate: dict, section_name: str = "") -> float:
    if not isinstance(candidate, dict):
        return 0.0
    contract = attach_editorial_contract(candidate).get("editorial_contract") or {}
    value = float(reader_value_score({**candidate, "included": True}))
    tier = str(contract.get("publish_tier") or "")
    tier_bonus = {
        "must_include": 80.0,
        "strong": 35.0,
        "optional": 5.0,
        "filler": -45.0,
        "reject": -200.0,
    }.get(tier, 0.0)
    source_bonus = source_score(str(candidate.get("source_label") or ""), str(candidate.get("category") or "")) / 4.0
    anchor_bonus = 18.0 if formal_news_anchor(candidate).get("has_news_anchor") else -8.0
    protected_bonus = 24.0 if protected_lane(candidate).get("protected") else 0.0
    evidence_len = len(str(candidate.get("evidence_text") or ""))
    evidence_bonus = 8.0 if evidence_len >= 700 else (-6.0 if evidence_len < 160 else 0.0)
    recency_bonus = 0.0
    raw_date = str(candidate.get("published_at") or "")
    if raw_date:
        try:
            pub_day = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
            age = (now_london().date() - pub_day).days
            recency_bonus = max(-12.0, 14.0 - age * 4.0)
        except ValueError:
            pass
    return value + tier_bonus + source_bonus + anchor_bonus + protected_bonus + evidence_bonus + recency_bonus


def apply_story_intelligence(candidate: dict) -> dict:
    if not isinstance(candidate, dict):
        return candidate
    attach_editorial_contract(candidate)
    attach_story_identity(candidate)
    candidate["rubric_contract"] = rubric_contract(candidate)
    candidate["news_anchor"] = formal_news_anchor(candidate)
    candidate["protected_lane"] = protected_lane(candidate)
    candidate["enrichment_health"] = enrichment_health(candidate)
    if (candidate.get("enrichment_health") or {}).get("warning"):
        # Flag split (D13): a protected/anchored or already-included item with
        # thin enrichment is a PUBLIC reserve — it may be pulled back into a thin
        # section. Only a non-anchored, non-included item is archive-only.
        lane = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
        anchor = candidate.get("news_anchor") if isinstance(candidate.get("news_anchor"), dict) else {}
        pullable = bool(candidate.get("include") or lane.get("protected") or anchor.get("has_news_anchor"))
        candidate["backup_candidate"] = True
        candidate["public_reserve"] = pullable
        candidate["backup_pool_only"] = not pullable
        candidate["second_opinion_required"] = True
        candidate["enrichment_warning"] = {
            "policy": "backup_not_reject",
            "status": candidate["enrichment_health"].get("status") or "",
            "evidence_chars": candidate["enrichment_health"].get("evidence_chars") or 0,
            "reason": "protected_or_anchored_item_has_failed_or_thin_enrichment",
        }
    candidate["english_judge"] = english_judge_stub(candidate)
    candidate["section_board_score"] = section_board_score(candidate)
    attach_evidence_packet(candidate)
    return candidate


def mark_reject_second_opinion(candidate: dict, code: str) -> None:
    if not isinstance(candidate, dict):
        return
    apply_story_intelligence(candidate)
    lane = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    anchor = candidate.get("news_anchor") if isinstance(candidate.get("news_anchor"), dict) else {}
    if not (lane.get("protected") or anchor.get("has_news_anchor")):
        return
    candidate["backup_candidate"] = True
    candidate["backup_pool_only"] = True
    candidate["public_reserve"] = False
    candidate["second_opinion_required"] = True
    candidate["second_opinion_reason"] = {
        "reject_code": code,
        "protected_lanes": lane.get("lanes") or [],
        "news_anchor": anchor,
    }


def build_evidence_packet(
    candidate: dict,
    *,
    history_matches: list[dict] | None = None,
    story_cluster: dict | None = None,
) -> dict[str, object]:
    """Build the English-first evidence object used by judge/shortlist layers.

    It intentionally stores factual inputs, not Russian prose. Downstream
    rewrite may read this packet, but should not add facts that are absent here.
    """
    if not isinstance(candidate, dict):
        return {}
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    contract = attach_editorial_contract(candidate).get("editorial_contract") or {}
    fp = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
    packet: dict[str, object] = {
        "schema_version": EVIDENCE_PACKET_VERSION,
        "fingerprint": fp,
        "title": str(candidate.get("title") or ""),
        "source_label": str(candidate.get("source_label") or ""),
        "source_url": str(candidate.get("source_url") or ""),
        "published_at": str(candidate.get("published_at") or ""),
        "category": str(candidate.get("category") or ""),
        "primary_block": str(candidate.get("primary_block") or ""),
        "lead": _compact_text(candidate.get("lead"), limit=600),
        "summary": _compact_text(candidate.get("summary"), limit=900),
        "evidence_text": _compact_text(candidate.get("evidence_text"), limit=1800),
        "entities": entities,
        "event": event,
        "editorial_contract": {
            "story_type": contract.get("story_type") or "",
            "event_shape": contract.get("event_shape") or "",
            "anchor_type": contract.get("anchor_type") or "",
            "topic_key": contract.get("topic_key") or "",
            "publish_tier": contract.get("publish_tier") or "",
            "section_policy": contract.get("section_policy") or {},
        },
        "story_identity": {
            "schema_version": STORY_IDENTITY_VERSION,
            "event_identity_key": candidate.get("event_identity_key") or "",
            "story_identity_key": candidate.get("story_identity_key") or "",
            "story_phase_key": candidate.get("story_phase_key") or "",
            "has_new_story_phase": bool(candidate.get("has_new_story_phase")),
            "change_type": str(candidate.get("change_type") or ""),
        },
        "history_matches": history_matches or candidate.get("history_matches") or [],
    }
    cluster = story_cluster if isinstance(story_cluster, dict) else candidate.get("story_cluster")
    if isinstance(cluster, dict) and cluster:
        packet["story_cluster"] = {
            "cluster_key": cluster.get("cluster_key") or "",
            "canonical_fingerprint": cluster.get("canonical_fingerprint") or "",
            "canonical_source_label": cluster.get("canonical_source_label") or "",
            "source_count": cluster.get("source_count") or 0,
            "sources": cluster.get("sources") or [],
            "union_facts": cluster.get("union_facts") or {},
        }
    return packet


def attach_evidence_packet(
    candidate: dict,
    *,
    history_matches: list[dict] | None = None,
    story_cluster: dict | None = None,
) -> dict:
    if not isinstance(candidate, dict):
        return candidate
    candidate["fingerprint"] = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
    if history_matches is not None:
        candidate["history_matches"] = history_matches
    if story_cluster is not None:
        candidate["story_cluster"] = story_cluster
    candidate["evidence_packet"] = build_evidence_packet(
        candidate,
        history_matches=history_matches,
        story_cluster=story_cluster,
    )
    return candidate


def attach_evidence_packets(candidates: list[dict]) -> None:
    for candidate in candidates:
        if isinstance(candidate, dict):
            attach_evidence_packet(candidate)


def _cheap_identity_key(candidate: dict) -> tuple[str, str] | None:
    block = str(candidate.get("primary_block") or "")
    if block in {"weather", "transport"}:
        return None
    url_key = canonical_url_identity(str(candidate.get("source_url") or ""))
    if url_key:
        return ("url", url_key)
    title = normalize_title(str(candidate.get("title") or ""))
    source = normalize_title(str(candidate.get("source_label") or ""))
    category = normalize_title(str(candidate.get("category") or ""))
    if len(title) >= 28 and source:
        return ("source_title", f"{category}|{source}|{title}")
    return None


def apply_cheap_dedup_before_enrich(candidates: list[dict]) -> dict[str, object]:
    """Cheap exact dedup before entity/event enrichment.

    This only catches deterministic duplicates (same canonical URL, or same
    source+title). Cross-source same-story logic stays in story clustering so
    we don't lose new facts from a different outlet.
    """
    groups: dict[tuple[str, str], list[dict]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        key = _cheap_identity_key(candidate)
        if key:
            groups.setdefault(key, []).append(candidate)

    drops: list[dict[str, object]] = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        winner = pick_winner(group) or group[0]
        winner_fp = str(winner.get("fingerprint") or "").strip() or fingerprint_for_candidate(winner)
        winner["fingerprint"] = winner_fp
        for candidate in group:
            if candidate is winner:
                continue
            candidate["fingerprint"] = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
            candidate["include"] = False
            candidate["dedupe_decision"] = "drop"
            candidate["change_type"] = "same_story_rehash"
            candidate["cheap_dedup_drop"] = True
            candidate["reason"] = (
                "Cheap pre-enrich duplicate — same URL/title kept from stronger source."
            )
            drops.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": candidate.get("title") or "",
                    "source_label": candidate.get("source_label") or "",
                    "kept_fingerprint": winner_fp,
                    "kept_title": winner.get("title") or "",
                    "kept_source_label": winner.get("source_label") or "",
                    "key_type": key[0],
                }
            )

    return {
        "version": 1,
        "groups_seen": sum(1 for group in groups.values() if len(group) > 1),
        "drops": drops,
        "dropped_count": len(drops),
    }


def story_cluster_key(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    attach_story_identity(candidate)
    phase_key = str(candidate.get("story_phase_key") or "").strip()
    if phase_key:
        return phase_key
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    topic_key = str(contract.get("topic_key") or "")
    if topic_key and is_specific_topic_key(topic_key):
        return topic_key

    category = str(candidate.get("category") or "")
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if category in {"culture_weekly", "venues_tickets", "russian_speaking_events", "diaspora_events"} and event.get("is_event"):
        name = normalize_title(str(event.get("event_name") or candidate.get("title") or ""))
        venue = normalize_title(str(event.get("venue") or ""))
        date = str(event.get("date_start") or event.get("date") or "")
        if name and (venue or date):
            return f"event:{name[:80]}|{venue[:60]}|{date}"

    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    entity_bits: list[str] = []
    for key in ("people", "venues", "councils", "companies", "boroughs", "districts", "stations"):
        values = entities.get(key)
        if isinstance(values, list):
            entity_bits.extend(str(value) for value in values[:2] if str(value).strip())
    title = normalize_title(str(candidate.get("title") or ""))
    if entity_bits and title:
        return "story:" + normalize_title(" ".join(entity_bits) + " " + title)[:160]
    return ""


def _merge_entities(cluster: list[dict]) -> dict[str, list[str]]:
    keys = ("boroughs", "districts", "stations", "councils", "venues", "clubs", "companies", "people")
    merged: dict[str, list[str]] = {}
    for key in keys:
        values: list[object] = []
        for candidate in cluster:
            entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
            raw = entities.get(key)
            if isinstance(raw, list):
                values.extend(raw)
        merged[key] = _unique(values, limit=10)
    return merged


def _best_event(cluster: list[dict]) -> dict:
    events = [
        candidate.get("event") for candidate in cluster
        if isinstance(candidate.get("event"), dict) and candidate.get("event", {}).get("is_event")
    ]
    if not events:
        return {}

    def score(event: dict) -> int:
        return sum(1 for key in ("event_name", "venue", "date_start", "date", "date_text", "borough", "price", "booking_url") if str(event.get(key) or "").strip())

    best = dict(sorted(events, key=score, reverse=True)[0])
    for event in events:
        for key, value in event.items():
            if not str(best.get(key) or "").strip() and str(value or "").strip():
                best[key] = value
    return best


def _cluster_union_facts(cluster: list[dict]) -> dict[str, object]:
    return {
        "titles": _unique([c.get("title") for c in cluster], limit=8),
        "leads": _unique([c.get("lead") for c in cluster], limit=5),
        "summaries": _unique([c.get("summary") for c in cluster], limit=5),
        "evidence_texts": _unique([_compact_text(c.get("evidence_text"), limit=900) for c in cluster], limit=4),
        "entities": _merge_entities(cluster),
        "event": _best_event(cluster),
    }


def attach_story_clusters(candidates: list[dict]) -> dict[str, object]:
    groups: dict[str, list[dict]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate["fingerprint"] = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
        key = story_cluster_key(candidate)
        if key:
            groups.setdefault(key, []).append(candidate)

    clusters: list[dict[str, object]] = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        winner = pick_winner(group) or group[0]
        canonical_fp = str(winner.get("fingerprint") or "").strip() or fingerprint_for_candidate(winner)
        sources = []
        for candidate in group:
            sources.append(
                {
                    "fingerprint": candidate.get("fingerprint") or "",
                    "source_label": candidate.get("source_label") or "",
                    "source_url": candidate.get("source_url") or "",
                    "title": candidate.get("title") or "",
                }
            )
        cluster_payload = {
            "schema_version": STORY_CLUSTER_VERSION,
            "cluster_key": key,
            "canonical_fingerprint": canonical_fp,
            "canonical_source_label": winner.get("source_label") or "",
            "canonical_source_url": winner.get("source_url") or "",
            "source_count": len(_unique([c.get("source_label") for c in group], limit=50)),
            "sources": sources,
            "union_facts": _cluster_union_facts(group),
        }
        for candidate in group:
            candidate["story_cluster_key"] = key
            candidate["story_cluster"] = cluster_payload
            attach_evidence_packet(candidate, story_cluster=cluster_payload)
        clusters.append(
            {
                "cluster_key": key,
                "canonical_fingerprint": canonical_fp,
                "canonical_source_label": winner.get("source_label") or "",
                "member_count": len(group),
                "source_count": cluster_payload["source_count"],
            }
        )

    for candidate in candidates:
        if isinstance(candidate, dict) and not candidate.get("evidence_packet"):
            attach_evidence_packet(candidate)

    counts = Counter(int(item.get("member_count") or 0) for item in clusters)
    return {
        "version": STORY_CLUSTER_VERSION,
        "cluster_count": len(clusters),
        "cluster_size_counts": dict(counts),
        "clusters": clusters[:100],
    }


def history_match_records(matches: list[dict]) -> list[dict]:
    out: list[dict] = []
    for match in matches[:5]:
        if not isinstance(match, dict):
            continue
        out.append(
            {
                "fingerprint": match.get("fingerprint") or "",
                "title": match.get("title") or "",
                "match_type": match.get("match_type") or "",
                "overlap": match.get("overlap"),
                "published_day": (
                    match.get("last_published_day_london")
                    or match.get("first_published_day_london")
                    or match.get("published_day_london")
                    or ""
                ),
            }
        )
    return out


def attach_story_intelligence(candidates: list[dict]) -> None:
    for candidate in candidates:
        if isinstance(candidate, dict):
            apply_story_intelligence(candidate)
