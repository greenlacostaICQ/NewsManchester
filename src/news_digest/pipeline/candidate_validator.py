from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
import time
from urllib import parse

from news_digest.pipeline.change_classifier import attach_change_phase, classify_change_phase
from news_digest.pipeline.city_intelligence import annotate_city_intelligence
from news_digest.pipeline.common import clean_url, now_london, pipeline_run_id_from, read_json, today_london, write_json
from news_digest.pipeline.editorial_contracts import (
    attach_editorial_contract,
    attach_scoring_trace,
    classify_ticket_type,
    crime_specificity_review,
    event_schema_completeness,
    infer_why_now,
    property_specificity_review,
    why_now_is_publishable,
)
from news_digest.pipeline.entity_extraction import enrich_candidate_entities
from news_digest.pipeline.event_extraction import enrich_candidate_event
from news_digest.pipeline.event_quality import event_quality_reject_reasons, event_quality_report
from news_digest.pipeline.practical_backfill import apply_practical_backfill
from news_digest.pipeline.professional_events import apply_professional_event_match
from news_digest.pipeline.reader_actions import attach_reader_action
from news_digest.pipeline.reader_value import attach_reader_value
from news_digest.pipeline.story_intelligence import apply_story_intelligence, mark_reject_second_opinion
from news_digest.pipeline.transport_classifier import classify_transport_candidate


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


def _is_search_url(url: str) -> bool:
    parsed = parse.urlsplit(url)
    path_segments = [segment.lower() for segment in parsed.path.split("/") if segment]
    if any(segment in {"search", "search-results", "results"} for segment in path_segments):
        return True
    query_keys = {key.lower() for key in parse.parse_qs(parsed.query).keys()}
    if any(key in {"search", "keyword"} for key in query_keys):
        return True
    return False


def _is_topic_or_index_url(url: str) -> bool:
    path_segments = [segment.lower() for segment in parse.urlsplit(url).path.split("/") if segment]
    return any(segment in {"all-about", "topic", "topics", "tag", "tags", "author"} for segment in path_segments)


_SUMMARY_DATETIME_PATTERN = re.compile(
    r"\b(?P<field>event_date|public_onsale)="
    r"(?P<value>\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?)"
)


def _summary_field_datetime(summary: str, field: str) -> datetime | None:
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


def _exclude_stale_ticket_onsale(candidate: dict) -> bool:
    if str(candidate.get("category") or "") != "venues_tickets":
        return False
    summary = str(candidate.get("summary") or "")
    lowered = summary.lower()
    if "ticket_signal=onsale" not in lowered and "public_onsale=" not in lowered:
        return False
    onsale_at = _summary_field_datetime(summary, "public_onsale")
    if onsale_at is None or onsale_at >= now_london():
        return False
    # Day-of / week-of event override: if the concert itself is happening in
    # the next 7 days, the public_onsale date is irrelevant — this is a hot
    # ticket, not stale coverage. Without this, today's Manchester concerts
    # were being demoted out of ticket_radar because tickets went on sale a
    # year ago (e.g. Calum Scott, Ray LaMontagne, My Leonard Cohen on
    # 2026-05-27).
    event_at = _summary_field_datetime(summary, "event_date")
    if event_at is not None:
        days_to_event = (event_at.date() - now_london().date()).days
        if 0 <= days_to_event <= 7:
            candidate["ticket_type"] = "event_this_week"
            return False
    age_days = (now_london() - onsale_at).days
    if age_days <= 3:
        candidate["ticket_type"] = "on_sale_now"
        return False
    if "ticket_signal=upcoming_event" in lowered:
        candidate["ticket_type"] = "old_public_sale"
        existing = str(candidate.get("reason") or "").strip()
        note = (
            f"Validator: public_onsale opened {age_days} day(s) ago; "
            "kept in ticket_radar as already-on-sale coverage."
        )
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return False
    candidate["primary_block"] = "future_announcements"
    candidate["ticket_type"] = "old_onsale"
    if age_days > 14:
        candidate["editorial_status"] = "borderline"
        candidate["quality_warnings"] = sorted(set(
            [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
            + [f"ticket_old_onsale:{age_days}d"]
        ))
    existing = str(candidate.get("reason") or "").strip()
    note = f"Validator: public_onsale opened {age_days} day(s) ago; moved out of ticket_radar."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return False


def _ensure_default_ticket_type(candidate: dict) -> None:
    """Make sure every venues_tickets candidate carries a ticket_type so
    the release_report ticket funnel does not drop 110 items into
    bucket `unknown`. Closes C1 from the 2026-05-27 audit
    ('Тип билета не распознан: 43, опубликовано 4').

    Items whose event_date is within 14 days and that already passed
    the GM-venue filter are promoted to `event_this_week` so they sit
    in the same protected bucket as today's day-of concerts."""
    if str(candidate.get("category") or "") != "venues_tickets":
        return
    if str(candidate.get("ticket_type") or "").strip():
        return
    summary = str(candidate.get("summary") or "")
    event_at = _summary_field_datetime(summary, "event_date")
    if event_at is not None:
        days_to_event = (event_at.date() - now_london().date()).days
        if 0 <= days_to_event <= 14:
            candidate["ticket_type"] = "event_this_week"
            return
    candidate["ticket_type"] = classify_ticket_type(candidate)


_EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
    "professional_events",
}
# Tokens that prove a Ticketmaster row is actually a Manchester / Greater
# Manchester concert. Used to recover items that the collector defaulted to
# outside_gm_tickets purely because the source label contains "UK Major".
_LOCAL_GM_VENUE_TOKENS = (
    "manchester", "salford", "bury", "rochdale", "oldham",
    "stockport", "tameside", "trafford", "wigan",
    "co-op live", "co op live", "ao arena", "manchester academy",
    "manchester arena", "etihad", "old trafford", "the lowry",
    " home mcr", "royal northern college", "rncm", "albert hall",
    "victoria warehouse", "manchester apollo", "o2 apollo manchester",
    "bridgewater hall", "band on the wall", "deaf institute",
    "gorilla manchester", "yes manchester", "new century manchester",
)


def _looks_like_local_gm_venue(candidate: dict) -> bool:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    venue = str(event.get("venue") or "").lower()
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_url")
    ).lower()
    haystack = f"{venue} {blob}"
    return any(token in haystack for token in _LOCAL_GM_VENUE_TOKENS)


def _reclassify_outside_gm_when_local_venue(candidate: dict) -> bool:
    """Recover Manchester concerts that the collector dumped into
    outside_gm_tickets because the Ticketmaster source label contained
    'UK Major'. Without this, today's Calum Scott / Ray LaMontagne were
    leaving the digest as 'outside GM'."""
    if str(candidate.get("primary_block") or "") != "outside_gm_tickets":
        return False
    if str(candidate.get("category") or "") != "venues_tickets":
        return False
    if not _looks_like_local_gm_venue(candidate):
        return False
    candidate["primary_block"] = "ticket_radar"
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: reclassified outside_gm_tickets → ticket_radar (local GM venue detected)."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


# Clearly non-GM UK places. Used to push a ticket that landed in the GM radar
# back to outside_gm_tickets so it never carries an "в GM" label (owner
# 2026-06-16: Suzanne Vega at Salisbury City Hall shown as a GM concert).
_OUTSIDE_GM_PLACE_TOKENS = (
    "salisbury", "edinburgh", "glasgow", "newcastle", "london", "cardiff",
    "newport", "birmingham", "leeds", "liverpool", "sheffield", "bristol",
    "brighton", "thetford", "isle of wight", "kentish town", "anfield",
    "tottenham", "scarborough", "delamere", "halifax", "glasgow green",
    "cardiff castle", "edinburgh playhouse",
)

_TRANSPORT_SECTION_RE = re.compile(
    r"\b(?:metrolink|tram|bus(?:es)?|bee\s+network|national\s+rail|"
    r"northern\s+(?:rail|trains?)|transpennine|transport\s+for\s+wales|"
    r"tfgm|rail\s+replacement|train(?:s)?|railway|platform)\b|"
    r"\b(?:bus|tram|metrolink)\s+stop\b|\bstop\s+closure\b",
    re.IGNORECASE,
)
_TRANSPORT_IMPACT_RE = re.compile(
    r"\b(?:disruption|delay|cancelled|diverted|closure|closed|works?|"
    r"lift\s+out\s+of\s+service|not\s+running|suspended|replacement\s+bus|"
    r"сбой|задерж|отмен|объезд|закрыт|работы|лифт)\b",
    re.IGNORECASE,
)
_TRANSPORT_SOURCE_LABELS = {
    "tfgm",
    "metrolink",
    "national rail enquiries",
    "national rail",
}
_TRANSPORT_CATEGORIES = {"transport"}
_NEVER_AUTO_TRANSPORT_CATEGORIES = {
    "professional_events",
    "tech_business",
    "food_openings",
    "venues_tickets",
    "culture_weekly",
    "russian_speaking_events",
    "diaspora_events",
}
_PROPERTY_HOUSING_RE = re.compile(
    r"\b(?:homes?|housing|flats?|apartments?|student\s+accommodation|pbsa|"
    r"planning|developer|development|warehouse|office\s+to\s+residential|"
    r"build-to-rent|affordable\s+homes|жиль|квартир|домов|застрой|планирован)\b",
    re.IGNORECASE,
)
_TECH_BUSINESS_RE = re.compile(
    r"\b(?:ai|api|saas|fintech|startup|software|cyber|cloud|data\s+centre|"
    r"digital|platform|app|semiconductor|robotics|open\s+banking)\b",
    re.IGNORECASE,
)
_SENSITIVE_EVIDENCE_RE = re.compile(
    r"\b(?:court|trial|charged|sentence(?:d)?|jailed|convicted|guilty|"
    r"arrest(?:ed)?|murder|killed|death|died|stab(?:bed|bing)?|knife|"
    r"rape|sexual|abuse|assault|inquest|coroner|child|school|fire|"
    r"crash|collision|missing|explosive|bomb|terror|domestic\s+abuse|"
    r"суд|обвин|приговор|осужд|арест|задерж|убий|погиб|нож|изнасил|"
    r"насили|инквест|коронер|реб[её]нок|школ|пожар|авар|пропал|взрыв)\b",
    re.IGNORECASE,
)
_SENSITIVE_INCIDENT_DETAIL_RE = re.compile(
    r"\b(?:stab(?:bed|bing)?|knife|assault|crash|collision|fire|arrest(?:ed)?|charged|jailed|"
    r"murder|death|died|missing)\b",
    re.IGNORECASE,
)
_SENSITIVE_FOLLOWUP_ACTION_RE = re.compile(
    r"\b(?:appeal(?:ing)?|witness(?:es)?|footage|information|charged|arrest(?:ed)?|jailed|sentenced|"
    r"court|trial)\b",
    re.IGNORECASE,
)


def _reclassify_gm_when_outside_venue(candidate: dict) -> bool:
    """Inverse of _reclassify_outside_gm_when_local_venue: a ticket routed to
    the GM radar whose venue is clearly a non-GM city (Salisbury City Hall)
    must move to outside_gm_tickets. Match on venue + title only — body text
    can mention other cities — and leave unknown venues untouched."""
    if str(candidate.get("primary_block") or "") != "ticket_radar":
        return False
    if str(candidate.get("category") or "") != "venues_tickets":
        return False
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    haystack = f"{str(event.get('venue') or '')} {str(candidate.get('title') or '')}".lower()
    # Word-boundary match: "bury" (GM borough) must NOT match inside
    # "salisbury", and an outside-GM city must be a whole word.
    if not any(re.search(rf"\b{re.escape(token)}\b", haystack) for token in _OUTSIDE_GM_PLACE_TOKENS):
        return False
    if any(re.search(rf"\b{re.escape(token)}\b", haystack) for token in _LOCAL_GM_VENUE_TOKENS):
        return False
    candidate["primary_block"] = "outside_gm_tickets"
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: reclassified ticket_radar → outside_gm_tickets (non-GM venue detected)."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


# A national / UK-wide story with no Greater Manchester angle must not sit in
# the top GM news blocks (owner 2026-06-16: national vape/tobacco law shown as
# local news).
_NATIONAL_NO_GM_RE = re.compile(
    r"\b(?:prime minister|sir keir|keir starmer|starmer|downing street|nationwide|"
    r"across the uk|uk[- ]wide|nationally|parliament|westminster|general election|"
    r"whitehall|becomes? law|royal assent)\b",
    re.IGNORECASE,
)
_GM_ANCHOR_RE = re.compile(
    r"\b(?:greater manchester|\bgm\b|manchester|salford|stockport|tameside|trafford|"
    r"wigan|bolton|bury|oldham|rochdale|wythenshawe|prestwich|altrincham|metrolink|"
    r"tfgm|gmp|gmca)\b",
    re.IGNORECASE,
)


def _apply_section_routing_quality(candidate: dict) -> list[str]:
    """Fix obvious English-data routing mistakes before translation.

    This is intentionally narrow: it handles cases where the collected facts
    already prove the section, so the Russian repair layer does not have to
    explain why a Metrolink item appeared in City Radar or housing in IT.
    """
    if not candidate.get("include"):
        return []
    reasons: list[str] = []
    blob = _candidate_blob(candidate)
    source_label = str(candidate.get("source_label") or "")
    block = str(candidate.get("primary_block") or "")
    if block != "transport" and _should_route_to_transport(candidate, blob, source_label):
        candidate["primary_block"] = "transport"
        reasons.append("section_routing:transport")
    if (
        str(candidate.get("category") or "") == "tech_business"
        and _PROPERTY_HOUSING_RE.search(blob)
        and not _TECH_BUSINESS_RE.search(blob)
    ):
        candidate["primary_block"] = "city_watch"
        reasons.append("section_routing:property_not_it")
    if (
        block in {"last_24h", "today_focus"}
        and _NATIONAL_NO_GM_RE.search(blob)
        and not _GM_ANCHOR_RE.search(blob)
    ):
        candidate["primary_block"] = "city_watch"
        reasons.append("section_routing:national_without_gm_demoted")
    if reasons:
        candidate["section_routing_quality"] = reasons
        existing = str(candidate.get("reason") or "").strip()
        note = "Validator: corrected section routing before translation (" + ", ".join(reasons) + ")."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return reasons


def _should_route_to_transport(candidate: dict, blob: str, source_label: str) -> bool:
    """Return True only for real public-transport/service disruption cards.

    This deliberately avoids generic terms such as "service", "route",
    "station", "works", or "transport system" on their own. Those words occur
    in courts, politics, business PR and development stories; routing them to
    the transport block caused non-transport lines to leak into the public
    transport section before the pre-send judge caught the issue.
    """
    category = str(candidate.get("category") or "").lower()
    if category in _TRANSPORT_CATEGORIES:
        return True
    if source_label.strip().lower() in _TRANSPORT_SOURCE_LABELS:
        return True
    if category in _NEVER_AUTO_TRANSPORT_CATEGORIES:
        return False
    return bool(_TRANSPORT_SECTION_RE.search(blob) and _TRANSPORT_IMPACT_RE.search(blob))


def _hold_sensitive_thin_or_failed_enrichment(candidate: dict) -> bool:
    """Sensitive news must not be translated from a teaser.

    If enrichment failed or the evidence packet is too thin, hold the row
    before translation. This is a row-level hold, not a release block.
    """
    if not candidate.get("include"):
        return False
    category = str(candidate.get("category") or "")
    if category not in {"media_layer", "gmp", "council", "public_services", "city_news", "tech_business", "football"}:
        return False
    blob = _candidate_blob(candidate)
    if not _SENSITIVE_EVIDENCE_RE.search(blob):
        return False
    health = candidate.get("enrichment_health") if isinstance(candidate.get("enrichment_health"), dict) else {}
    failed = bool(health.get("failed"))
    thin = bool(health.get("thin"))
    if not failed and not thin:
        return False
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    has_place = bool(entities.get("boroughs") or entities.get("districts") or _GM_ANCHOR_RE.search(blob))
    if (
        thin
        and not failed
        and has_place
        and _SENSITIVE_INCIDENT_DETAIL_RE.search(blob)
        and _SENSITIVE_FOLLOWUP_ACTION_RE.search(blob)
    ):
        return False
    candidate["editorial_status"] = "held_for_enrichment"
    _append_reject(
        candidate,
        "sensitive_thin_or_failed_enrichment",
        "Validator: sensitive/crime/court item has failed or thin enrichment; held before translation instead of publishing a teaser.",
    )
    return True


_EVENT_LIKE_TERMS = (
    "festival",
    "concert",
    "workshop",
    "exhibition",
    "screening",
    "show",
    "performance",
    "market",
    "fair",
    "gig",
    "tickets",
    "what's on",
    "whats on",
)
_SOLD_OUT_EVENT_RE = re.compile(
    r"\b(?:sold\s*out|fully\s*booked|no\s+(?:tickets|spaces|places)\s+(?:left|available)|"
    r"tickets?\s+(?:are\s+)?(?:sold\s*out|unavailable)|распродан[оаы]?|мест\s+нет)\b",
    re.IGNORECASE,
)
_MARKET_FAIR_WEEKEND_RE = re.compile(
    r"\b(?:car\s*boot|makers?\s+market|artisan\s+market|farmers?\s+market|"
    r"flea\s+market|vintage\s+market|food\s+market|market|fair|fayre|ярмарк|рынок)\b",
    re.IGNORECASE,
)
_ROUTINE_MARKET_RECURRENCE_RE = re.compile(
    r"\b(?:every|weekly|monthly|each\s+(?:week|month)|"
    r"(?:first|second|third|fourth|last)\s+(?:saturdays?|sundays?|weekends?)|"
    r"кажд(?:ую|ый|ое)|еженедельн|ежемесячн|по\s+(?:субботам|воскресеньям|выходным))\b",
    re.IGNORECASE,
)
_RARE_MARKET_OR_FESTIVAL_RE = re.compile(
    r"\b(?:annual|yearly|once\s+a\s+year|biannual|twice\s+a\s+year|"
    r"festival|food\s+festival|bbq|barbecue|beer\s+festival|street\s+food|"
    r"music|live\s+music|artists?|performance|special|launch|anniversary|"
    r"ежегодн|раз\s+в\s+год|фестиваль|барбекю|уличн\w+\s+ед|живая\s+музык)\b",
    re.IGNORECASE,
)
_COURT_ROUNDUP_RE = re.compile(
    r"\b(?:locked\s+up\s+this\s+week|jailed\s+this\s+week|this\s+week\s+in\s+court|"
    r"among\s+(?:those|the\s+criminals)\s+(?:locked\s+up|jailed)|"
    r"courts?\s+round-?up|sentenced\s+this\s+week)\b",
    re.IGNORECASE,
)
_COUNCIL_ADMIN_ONLY_RE = re.compile(
    r"\b(?:appoints?\s+(?:new\s+)?cabinet|cabinet\s+appointments?|"
    r"confirmed?\s+(?:.*\b)?(?:leader|deputy\s+leader)|"
    r"remain(?:s|ed)?\s+(?:as\s+)?(?:leader|deputy\s+leader))\b",
    re.IGNORECASE,
)
_COUNCIL_READER_IMPACT_RE = re.compile(
    r"\b(?:homes?|housing|homeless|rent|council\s+tax|budget|consultation|deadline|"
    r"school|care|cqc|fire\s+safety|public\s+safety|road|transport|bins?|"
    r"service|library|market|licen[cs]e|planning|approved|rejected|jobs?|"
    r"funding|investment|closure|reopen|open(?:ing)?|works?)\b",
    re.IGNORECASE,
)
_RELATIVE_UNDATED_TERMS = (
    "next month",
    "coming soon",
    "later this year",
    "this summer",
    "this autumn",
    "this winter",
    "this spring",
)
_MONTHS = (
    "jan", "january", "feb", "february", "mar", "march", "apr", "april",
    "may", "jun", "june", "jul", "july", "aug", "august", "sep", "sept",
    "september", "oct", "october", "nov", "november", "dec", "december",
)
_CONCRETE_DATE_RE = re.compile(
    r"\b(?:20\d{2}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}(?:st|nd|rd|th)?\s+"
    r"(?:" + "|".join(_MONTHS) + r")(?:\s+20\d{2})?)\b",
    re.IGNORECASE,
)
_EDITORIAL_DAY_MONTH_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month>january|february|march|april|may|"
    r"june|july|august|september|october|november|december)\b",
    re.IGNORECASE,
)
_EDITORIAL_MONTH_DAY_RE = re.compile(
    r"\b(?P<month>january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?\b",
    re.IGNORECASE,
)


def _candidate_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_url")
    )


_IMPLICIT_WEEKEND_URL_PATTERNS = (
    "this-weekend", "this_weekend", "weekend-in-",
    "things-to-do-this-week", "weekend-events",
)


def _is_implicit_weekend_aggregator(candidate: dict) -> bool:
    """Source URLs like '.../things-to-do-this-weekend-in-manchester' list
    only events for the current weekend by definition — even if an
    individual card has no explicit date in title. Treat as dated.
    """
    url = str(candidate.get("source_url") or "").lower()
    return any(pat in url for pat in _IMPLICIT_WEEKEND_URL_PATTERNS)


def _has_future_or_concrete_date(candidate: dict) -> bool:
    summary = str(candidate.get("summary") or "")
    if _summary_field_datetime(summary, "event_date") is not None:
        return True
    if _is_implicit_weekend_aggregator(candidate):
        return True
    published_at = str(candidate.get("published_at") or "")
    if published_at:
        try:
            if datetime.fromisoformat(published_at.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date() >= now_london().date():
                return True
        except ValueError:
            pass
    return bool(_CONCRETE_DATE_RE.search(_candidate_blob(candidate)))


def _has_computable_market_schedule(candidate: dict) -> bool:
    lowered = _candidate_blob(candidate).lower()
    market_like = "market" in lowered or "car boot" in lowered
    if not market_like:
        return False
    return bool(
        re.search(
            r"\b(?:every|each|all|most|weekly|first|1st|second|2nd|third|3rd|last)\s+"
            r"(?:saturday|sunday|weekend|month)\b|"
            r"\bruns?\s+(?:on\s+)?(?:saturdays?|sundays?|weekends?|bank holiday mondays?)\b|"
            r"\bopen(?:ing)?\s+(?:hours?\s+)?(?:on\s+)?(?:saturdays?|sundays?|weekends?)\b|"
            r"\b(?:saturdays?|sundays?|weekends?)\b.{0,80}\b(?:open|trading|market|car boot)\b",
            lowered,
        )
    )


_PAST_DATE_MONTH_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month>january|february|march|april|may|"
    r"june|july|august|september|october|november|december|"
    r"января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
    re.IGNORECASE,
)
_MONTH_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    # Russian month names appear in LLM-rewritten draft_line / Russian
    # source titles. Match them with the same year-resolution heuristic
    # as English so a "5 апреля" reference in a 22 May digest is treated
    # the same as "5 April" — i.e. a date that has already passed.
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


# Recurring-event patterns. If a card with only past dates also carries
# one of these signals, the underlying event is still active and we
# should keep the card (and let the rewriter say "каждое воскресенье"
# instead of the dead start date). Covers both market-style recurrence
# already detected by _has_computable_market_schedule and broader cases
# like "season runs until 30 September" / "сезон до 30 сентября" /
# "every Sunday" / "каждое воскресенье" / "weekly" / "monthly" /
# "runs until DD month" / "идёт до DD месяц".
_RECURRENCE_PATTERN_RE = re.compile(
    r"\b(?:"
    r"every\s+(?:sunday|saturday|weekend|monday|tuesday|wednesday|thursday|friday|week|month|day)|"
    r"weekly|monthly|each\s+(?:week|month|sunday|saturday|weekend)|"
    r"runs?\s+(?:until|through|to)\s+\d|season\s+runs?|"
    r"season\s+(?:until|through|to)\s+\d|"
    r"открыт(?:а|о|ы)?\s+с\s+\d|сезон\s+(?:до|по|с)\s+\d|"
    r"каждое\s+(?:воскресенье|субботу|воскресение)|"
    r"каждую\s+(?:субботу|неделю|пятницу|пятницу)|"
    r"еженедельно|ежемесячно|"
    r"работает\s+(?:до|с|по)\s+\d|идёт\s+до\s+\d|идет\s+до\s+\d|"
    r"проходит\s+(?:до|каждое|каждую|еженедельно)"
    r")\b",
    re.IGNORECASE,
)


def _has_recurrence_pattern(candidate: dict) -> bool:
    """True when card mentions a recurring schedule or active season.

    Used by _exclude_stale_event: if the only mentioned date is in the
    past but the card also says "every Sunday" or "season runs until
    30 September", the event is still active — don't drop, mark
    is_recurring so the rewriter knows to say "каждое воскресенье"
    rather than the dead start date.

    Already-existing _has_computable_market_schedule catches the
    market/car-boot subset; this is the general one.
    """
    if _has_computable_market_schedule(candidate):
        return True
    blob = _candidate_blob(candidate)
    return bool(_RECURRENCE_PATTERN_RE.search(blob))


def _append_reject(candidate: dict, code: str, note: str) -> None:
    candidate["include"] = False
    candidate["reject_reasons"] = sorted(set(
        [str(r) for r in candidate.get("reject_reasons") or [] if str(r).strip()]
        + [code]
    ))
    existing = str(candidate.get("reason") or "").strip()
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    mark_reject_second_opinion(candidate, code)


def _explicit_dates_from_blob(candidate: dict) -> list[date]:
    today = now_london().date()
    out: list[date] = []
    summary = str(candidate.get("summary") or "")
    for field in ("event_date", "public_onsale"):
        dt = _summary_field_datetime(summary, field)
        if dt is not None:
            out.append(dt.date())
    blob = _candidate_blob(candidate)
    for match in list(_EDITORIAL_DAY_MONTH_RE.finditer(blob)) + list(_EDITORIAL_MONTH_DAY_RE.finditer(blob)):
        month = _MONTH_NUM.get(match.group("month").lower())
        if not month:
            continue
        try:
            parsed = date(today.year, month, int(match.group("day")))
        except ValueError:
            continue
        if parsed < today.replace(day=1):
            parsed = parsed.replace(year=parsed.year + 1)
        out.append(parsed)
    return out


def _published_day(candidate: dict) -> date | None:
    raw = str(candidate.get("published_at") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
    except ValueError:
        return None


_NEWS_UPDATE_MARKERS = re.compile(
    r"\b("
    r"today|this morning|this afternoon|yesterday|latest|update|updated|"
    r"sentenced|jailed|convicted|verdict|charged|arrested|appeal|"
    r"approved|rejected|confirmed|announced|launched|opened|closed|"
    r"warning|disruption|strike|closure"
    r")\b",
    re.IGNORECASE,
)


_SOFT_CIVIC_PR_RE = re.compile(
    r"\b("
    r"billie\s+bee|world\s+bee\s+day|beeline\s+for\s+summer\s+holidays|"
    r"lord\s+mayor|mayor-making|"
    r"creative\s+health\s+leads?\s+programme|selected\s+to\s+get\s+creative\s+with\s+approach\s+to\s+health|"
    r"community\s+champions?|pride\s+in\s+place|"
    r"becomes?\s+(?:an?\s+)?(?:ams\s+)?fellow|academy\s+of\s+medical\s+sciences|"
    r"award(?:ed|s)?|shortlisted|celebrat(?:e|ing|ion)"
    r")\b",
    re.IGNORECASE,
)

_LOW_VALUE_LIFESTYLE_RE = re.compile(
    r"\b("
    r"race\s+across\s+the\s+world|bbc\s+show|reality\s+show|"
    r"coronation\s+street\s+star|soap\s+star|tv\s+star|"
    r"celebrity|influencer"
    r")\b",
    re.IGNORECASE,
)

_LOW_VALUE_FOOTBALL_RE = re.compile(
    r"\b("
    r"connection\s+with\s+our\s+fans|farewell\s+interview|"
    r"fan\s+of\s+the\s+club\s+for\s+the\s+rest\s+of\s+my\s+life|"
    r"bespoke\s+shirts?|programme\s+cover|training\s+gallery"
    r")\b",
    re.IGNORECASE,
)

_STRONG_NON_GM_RE = re.compile(
    r"\b("
    r"warrington|cheshire|liverpool|london|leeds|sheffield|birmingham|"
    r"blackpool|blackburn|burnley|lancaster|chester|crewe|bradford|"
    r"wakefield|nottingham|leicester|newcastle|"
    # "Preston Crown Court" is a court name, not a GM location; only treat
    # Preston as non-GM when it reads as the city, not a person/court.
    r"preston\s+(?:city|town|north|new\s+road)|"
    r"texas|america|usa|united\s+states"
    r")\b",
    re.IGNORECASE,
)

_LOCAL_SIGNAL_RE = re.compile(
    r"\b("
    r"manchester|salford|stockport|trafford|tameside|rochdale|oldham|"
    r"wigan|bolton|bury|altrincham|ashton-under-lyne|ashton under lyne|"
    r"prestwich|eccles|burnage|romiley|swinton|ancoats|cheadle|didsbury|"
    r"chorlton|fallowfield|rusholme|harpurhey|openshaw|wythenshawe|"
    r"old trafford|gmca|gmp|tfgm|metrolink|bee network"
    r")\b",
    re.IGNORECASE,
)


def _news_text_without_publisher_chrome(candidate: dict) -> str:
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead")
    )
    text = re.sub(r"\bnews\s+greater\s+manchester\s+news\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bgreater\s+manchester\s+news\b", " ", text, flags=re.IGNORECASE)
    return text


_TOUR_ANNOUNCE_RE = re.compile(
    r"\b(?:announce[sd]?|unveil(?:s|ed)?|reveal(?:s|ed)?)\b[^.]{0,40}\b(?:uk\s+)?tour\b|"
    r"\b(?:uk|headline|world|arena)\s+tour\b|\btour\s+dates?\b|\btickets?\s+(?:on\s+sale|go\s+on\s+sale)\b",
    re.IGNORECASE,
)


def _reroute_tour_announcement(candidate: dict) -> bool:
    """A tour / on-sale announcement is a ticket lead, not fresh city news. On
    2026-06-04 "Amble Announce UK Tour 2026 Including Manchester Apollo" sat in
    «Свежие новости» (media_layer/last_24h). Move such items into the
    forward-looking announcements lane so news stays news and tickets sit with
    tickets. Only fires for news-lane culture items, never touches hard news."""
    if str(candidate.get("category") or "") not in {"media_layer", "culture_weekly"}:
        return False
    if str(candidate.get("primary_block") or "") not in {"last_24h", "city_watch"}:
        return False
    blob = " ".join(str(candidate.get(f) or "") for f in ("title", "summary", "lead"))
    if not _TOUR_ANNOUNCE_RE.search(blob):
        return False
    candidate["primary_block"] = "future_announcements"
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: tour/on-sale announcement rerouted from news to announcements lane."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


def _exclude_non_gm_news(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    if str(candidate.get("category") or "") not in {"media_layer", "gmp", "public_services", "tech_business"}:
        return False
    text = _news_text_without_publisher_chrome(candidate)
    if not _STRONG_NON_GM_RE.search(text):
        return False
    if _LOCAL_SIGNAL_RE.search(text):
        return False
    _append_reject(
        candidate,
        "not_gm",
        "Validator: story is centred outside Greater Manchester; publisher section chrome was ignored.",
    )
    return True


# ---------------------------------------------------------------------
# S4 — weak-item predicates with hard protection against "killed a real
# news story".
#
# Each predicate rejects ONLY if all three conditions hold:
#   1. The text matches the predicate's class signature.
#   2. There is no "news anchor" in the candidate (no fresh date,
#      no GM-location entity, no name-other-than-the-subject, no
#      money/casualty figure, no crime/civic verb).
#   3. There is no follow-up keyword indicating a current event
#      (charged / убил / arrested / opens / launches / closed).
# A single anchor wins and the candidate passes — "gangster from the
# 90s killed someone yesterday" is news, not historical filler.

_CELEBRITY_SIGHTING_RE = re.compile(
    r"\b(?:visited|popped\s+in|stopped\s+by|stopped\s+off|signed\s+(?:records|copies|autographs)|"
    r"posed\s+with|took\s+(?:photos|selfies)\s+with|"
    r"surprised\s+(?:fans|customers|staff)\s+at|"
    r"зашёл\s+в|зашла\s+в|посетил|посетила|"
    r"расписал(?:ся|ась)|сфотографировал(?:ся|ась))\b",
    re.IGNORECASE,
)
_MOTIVATIONAL_OVERCAME_RE = re.compile(
    r"\b(?:overcame|inspires|inspiring|turned\s+(?:his|her|their)\s+life|"
    r"first\s+job|too\s+stupid|question(?:ed)?\s+if\s+i\s+was|"
    r"dream(?:ed|t)?s?\s+of\s+(?:making|becoming|a\s+career)|"
    r"next\s+big\s+name|proper\s+career|classic\s+sports\s+story|getting\s+an\s+injury|"
    r"pandemic\s+(?:inspired|sparked)|turning\s+point|not\s+knowing\s+what\s+i\s+wanted|"
    r"from\s+(?:failure|nothing|homeless)|"
    r"after\s+being\s+(?:diagnosed|told|rejected)|"
    r"now\s+(?:helps|inspires|leads|teaches|runs)|"
    r"after\s+failing|despite\s+(?:dyslexia|adhd|autism|setback)|"
    r"стал(?:а)?\s+успешн(?:ым|ой)|преодоле(?:л|ла|ли)|"
    r"вдохновля(?:ет|ют)\s+(?:других|молод|других\s+людей)|"
    r"провалил(?:а)?\s+(?:экзамен|школу|собеседование))\b",
    re.IGNORECASE,
)
_HISTORICAL_NO_NEWS_RE = re.compile(
    r"\b(?:in\s+the\s+(?:80s|90s|2000s|seventies|eighties|nineties)|"
    r"one\s+of\s+(?:the\s+)?(?:most|biggest)\s+(?:notorious|feared|famous)|"
    r"was\s+(?:once|formerly|previously)\s+known|"
    r"became\s+famous\s+(?:in|during)|"
    r"гангстер(?:ов)?\s+(?:80|90|2000)-х|"
    r"в\s+(?:80|90|2000)-х\s+(?:годах)?|"
    r"один\s+из\s+(?:самых)?\s+(?:известных|страшных|знаменитых)\s+(?:гангстеров|преступников))\b",
    re.IGNORECASE,
)

# "News anchor" signals — at least one means the card carries a current
# story worth publishing. Designed to be permissive on purpose: the
# whole point of S4 is to leave news alone and only kill pure filler.
_NEWS_ANCHOR_VERBS_RE = re.compile(
    r"\b(?:killed|murdered|stabbed|shot|charged|arrested|jailed|sentenced|"
    r"convicted|cleared|fined|fired|sacked|opens|opening|opened|closes|"
    r"closing|closed|launches|launching|launched|approves|approved|"
    r"rejects|rejected|appeal|appealed|inquest|trial|verdict|"
    r"убил(?:а)?|зарезал(?:а)?|застрелил(?:а)?|арестован(?:а)?|"
    r"задержан(?:а)?|осуждён(?:а)?|приговорён(?:а)?|обвинён(?:а)?|"
    r"открывает(?:ся)?|откро(?:ет|ется)|закрывает(?:ся)?|"
    r"одобр(?:ил|ила|или|ен)|отклон(?:ил|ила|или|ён)|"
    r"приговор|суд|расследовани[ея]|инквест|инцидент|"
    r"погиб(?:ла)?|пострадал(?:а|и))\b",
    re.IGNORECASE,
)
_RECENT_DATE_HINT_RE = re.compile(
    r"\b(?:yesterday|today|this\s+morning|last\s+night|earlier\s+this\s+week|"
    r"вчера|сегодня|утром|накануне|на\s+днях|на\s+этой\s+неделе)\b",
    re.IGNORECASE,
)
_MONEY_OR_CASUALTY_RE = re.compile(
    r"£\s*\d|\b\d+\s+(?:people|residents|victims|пострадавш|жертв|погибш|раненых)\b|"
    r"\b(?:£|\$)\d{2,}\b",
    re.IGNORECASE,
)


def _has_news_anchor(candidate: dict) -> bool:
    """A card has a 'news anchor' when it carries a STRONG signal of
    current news:
      - crime / civic / business verb (charged / убил / opens /
        approved / fined / sentenced)
      - a money sum (£250k) or a casualty count (3 пострадавших)

    Note: weak signals like "yesterday", a borough name on its own, a
    district or a station are NOT enough — every celebrity-sighting
    and historical-gangster card mentions a Greater Manchester
    borough, and that's exactly the noise S4 needs to remove. Crime
    + borough together still pass via the verb match.
    """
    blob = _candidate_blob(candidate)
    if _NEWS_ANCHOR_VERBS_RE.search(blob):
        return True
    if _MONEY_OR_CASUALTY_RE.search(blob):
        return True
    return False


def _exclude_celebrity_sighting(candidate: dict) -> bool:
    """User feedback: «Ian Brown зашёл в магазин — непонятно».

    Cards where a known figure visits / signs / poses, without any
    news angle (no crime, no opening/closing, no fresh date, no
    money/casualty figure) — drop. If anchor present (e.g. the same
    person at a charity event that opened today in Bolton), pass.
    """
    if not candidate.get("include"):
        return False
    blob = _candidate_blob(candidate)
    if not _CELEBRITY_SIGHTING_RE.search(blob):
        return False
    if _has_news_anchor(candidate):
        return False
    _append_reject(
        candidate,
        "celebrity_sighting",
        "Validator: celebrity-sighting card has no news angle "
        "(no crime/civic verb, no fresh date, no money/casualty, no GM borough).",
    )
    return True


def _exclude_motivational_human_interest(candidate: dict) -> bool:
    """User feedback: «второй день получаю такие новости вчера было
    про какого кто экзамен завалили и стал успешным, нахера мне это?».

    Drops "X overcame Y, now is Z" inspirational cards UNLESS they
    carry an anchor (opens an office in Bolton on 28 May, runs a
    workshop on a specific date, etc.).
    """
    if not candidate.get("include"):
        return False
    blob = _candidate_blob(candidate)
    if not _MOTIVATIONAL_OVERCAME_RE.search(blob):
        return False
    if _has_news_anchor(candidate):
        return False
    _append_reject(
        candidate,
        "motivational_human_interest",
        "Validator: motivational human-interest card has no GM action "
        "anchor (no event date, no opening/launch, no GM borough).",
    )
    return True


def _exclude_by_editorial_contract(candidate: dict) -> bool:
    """Single editorial-contract gate for systemic rejects.

    This does not replace the older targeted predicates; it catches the
    pattern-level failures that keep recurring under new wording: pure
    motivational profiles, old-existing food/opening cards, and stale
    public-service notices. All are auditable through
    candidate.editorial_contract rather than hidden in writer prose.
    """
    if not candidate.get("include"):
        return False
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    reject_reason = str(contract.get("reject_reason") or "").strip()
    if not reject_reason:
        tier = str(contract.get("publish_tier") or "").strip()
        event_shape = str(contract.get("event_shape") or "").strip()
        block = str(candidate.get("primary_block") or "")
        category = str(candidate.get("category") or "")
        if (
            tier == "filler"
            and block in {"last_24h", "today_focus", "city_watch"}
            and category in {"media_layer", "city_news", "gmp"}
        ):
            reject_reason = "editorial_filler"
        elif event_shape == "bookable_activity" and (
            block == "weekend_activities"
            or (
                block == "next_7_days"
                and "designmynight" in str(candidate.get("source_label") or "").lower()
            )
        ):
            reject_reason = "bookable_activity_filler"
        else:
            return False
    story_type = str(contract.get("story_type") or "")
    code = reject_reason
    if story_type == "human_interest":
        code = "motivational_human_interest"
    note = (
        "Validator: editorial_contract rejected item "
        f"(story_type={story_type}, anchor={contract.get('anchor_type')}, tier={contract.get('publish_tier')})."
    )
    _append_reject(candidate, code, note)
    return True


def _demote_optional_top_news_by_contract(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    if str(contract.get("publish_tier") or "") != "optional":
        return False
    if str(candidate.get("primary_block") or "") not in {"last_24h", "today_focus"}:
        return False
    if str(candidate.get("category") or "") not in {"media_layer", "gmp", "city_news"}:
        return False
    candidate["primary_block"] = "city_watch"
    candidate["quality_warnings"] = sorted(set(
        [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
        + ["optional_top_news_demoted_to_city_watch"]
    ))
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: optional editorial_contract item demoted out of top news sections."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


def _exclude_road_only_transport(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    if str(candidate.get("primary_block") or "") != "transport" and str(candidate.get("category") or "") != "transport":
        return False
    if str(candidate.get("transport_mode") or "") != "road":
        return False
    _append_reject(
        candidate,
        "road_only_transport",
        "Validator: road-only TfGM alert is not public transport coverage.",
    )
    return True


def _exclude_historical_no_news_angle(candidate: dict) -> bool:
    """User feedback: «Salford Винни Клей 90-х годов — уже было и
    зачем мне эта новость про город сейчас».

    Historical archive stories without a fresh news hook are dropped.
    Protection: if the card mentions "yesterday killed" / "charged
    today" / a fresh court date / a current arrest, it passes — that
    IS news even if the subject is a 90s figure.
    """
    if not candidate.get("include"):
        return False
    blob = _candidate_blob(candidate)
    if not _HISTORICAL_NO_NEWS_RE.search(blob):
        return False
    if _has_news_anchor(candidate):
        return False
    _append_reject(
        candidate,
        "historical_no_news_angle",
        "Validator: historical archive story has no current news hook "
        "(no fresh date, no crime verb, no civic action).",
    )
    return True


def _exclude_low_value_news(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    text = _news_text_without_publisher_chrome(candidate)
    if category in {"media_layer", "council", "public_services"} and _SOFT_CIVIC_PR_RE.search(text):
        _append_reject(
            candidate,
            "weak_value_civic_pr",
            "Validator: soft civic/award/awareness item has no practical reader value for the morning issue.",
        )
        return True
    if category == "media_layer" and block in {"last_24h", "today_focus", "city_watch"} and _LOW_VALUE_LIFESTYLE_RE.search(text):
        _append_reject(
            candidate,
            "weak_value_lifestyle",
            "Validator: entertainment/lifestyle item is only loosely local and has no practical GM impact.",
        )
        return True
    if category == "football" and _LOW_VALUE_FOOTBALL_RE.search(text):
        _append_reject(
            candidate,
            "weak_value_football_pr",
            "Validator: football item is club PR/farewell filler rather than match, transfer, injury, or ticket news.",
        )
        return True
    return False


def _exclude_stale_undated_news_from_text(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    if str(candidate.get("category") or "") not in {"media_layer", "gmp", "council", "public_services", "city_news"}:
        return False
    if _published_day(candidate) is not None:
        return False
    dates = _explicit_dates_from_blob(candidate)
    if not dates:
        return False
    today = now_london().date()
    if max(dates) >= today:
        return False
    age_days = (today - max(dates)).days
    if age_days <= 7:
        return False
    _append_reject(
        candidate,
        "stale_undated_news",
        f"Validator: undated news item only mentions old dates (latest {max(dates).isoformat()}).",
    )
    return True


def _exclude_wrong_food_opening_category(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    if str(candidate.get("category") or "") != "food_openings" and str(candidate.get("primary_block") or "") != "openings":
        return False
    text = _candidate_blob(candidate).lower()
    if "coronation street experience" in text:
        _append_reject(
            candidate,
            "wrong_openings_category",
            "Validator: visitor attraction is not a food/opening/market item.",
        )
        return True
    food_or_market = re.search(
        r"\b(food|restaurant|cafe|caf[eé]|coffee|bar|pub|brewery|beer|pizza|"
        r"kitchen|dining|bakery|market|makers|opening|opened|opens)\b",
        text,
        re.IGNORECASE,
    )
    if food_or_market:
        return False
    _append_reject(
        candidate,
        "wrong_openings_category",
        "Validator: openings block item is not food, drink, market, or a new venue opening.",
    )
    return True


# #5 Beyond this age, a news item is dropped no matter what — no "new phase"
# wording, no source exemption. 14d keeps genuinely-developing week-old stories
# while killing recycled press releases (GMMH 15 May appointment).
_HARD_STALE_AGE_DAYS = 14

# The age cutoff is about NEWS, which decays with publication age. Tickets,
# weekend/upcoming events and Russian-speaking gigs live by their EVENT date,
# not when the listing was published — an announcement weeks ahead is the whole
# point. These are never aged out (belt-and-suspenders on top of the category
# gate, plus a future-event-date check for anything event-like that slipped
# into a news category).
_AGE_EXEMPT_CATEGORIES = {
    "venues_tickets", "culture_weekly", "russian_speaking_events", "diaspora_events", "professional_events",
}
_AGE_EXEMPT_BLOCKS = {
    "ticket_radar", "outside_gm_tickets", "weekend_activities", "next_7_days",
    "future_announcements", "russian_events", "professional_events",
}


def _is_forward_looking_event(candidate: dict) -> bool:
    """True for tickets/events/Russian gigs (by category or block), or an item
    carrying a STRUCTURED future event date.

    Deliberately does NOT trust free-text dates parsed from the body — those
    pick up year-rollover artifacts ("15 May" → 2027) and incidental future
    mentions in old news, which would let stale press releases escape #5.
    """
    if str(candidate.get("category") or "") in _AGE_EXEMPT_CATEGORIES:
        return True
    if str(candidate.get("primary_block") or "") in _AGE_EXEMPT_BLOCKS:
        return True
    ev = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(ev.get("date_start") or ev.get("date") or "").strip()
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date() >= now_london().date()
        except ValueError:
            return False
    return False


def _exclude_stale_news_without_new_phase(candidate: dict) -> bool:
    """Drop old city/news items unless the text carries a clear new phase."""
    if not candidate.get("include"):
        return False
    if str(candidate.get("primary_block") or "") in {"weather", "transport"}:
        return False
    if str(candidate.get("category") or "") not in {"media_layer", "gmp", "council", "public_services", "city_news", "tech_business", "football"}:
        return False
    # Never age out tickets/events/Russian-speaking gigs or anything with an
    # upcoming event date — they have their own date logic.
    if _is_forward_looking_event(candidate):
        return False
    pub_day = _published_day(candidate)
    if pub_day is None:
        return False
    age_days = (now_london().date() - pub_day).days
    if age_days <= 7:
        return False
    # #5 Hard age cutoff — independent of source AND of any "new phase" marker.
    # A press release written 19 days ago still says "today/announced", which
    # used to let it escape via _NEWS_UPDATE_MARKERS (GMMH CEO appointment from
    # 15 May shipped on 3 June). Past this cutoff the date wins, no exceptions.
    if age_days > _HARD_STALE_AGE_DAYS:
        _append_reject(
            candidate,
            "stale_hard_age",
            f"Validator: news item is {age_days} days old (hard cutoff {_HARD_STALE_AGE_DAYS}d), dropped regardless of update wording.",
        )
        return True
    if _NEWS_UPDATE_MARKERS.search(_candidate_blob(candidate)):
        return False
    _append_reject(
        candidate,
        "stale_no_new_phase",
        f"Validator: news item is {age_days} days old and has no clear new phase for today's digest.",
    )
    return True


def _exclude_bad_food_opening_timing(candidate: dict) -> bool:
    """Food/openings are daily-value only: recent openings or this-week promos."""
    if not candidate.get("include"):
        return False
    if str(candidate.get("category") or "") != "food_openings" and str(candidate.get("primary_block") or "") != "openings":
        return False
    today = now_london().date()
    explicit_dates = _explicit_dates_from_blob(candidate)
    if explicit_dates:
        latest = max(explicit_dates)
        earliest = min(explicit_dates)
        if latest < today - timedelta(days=3):
            _append_reject(
                candidate,
                "stale_opening",
                f"Validator: food/opening date {latest.isoformat()} is more than 3 days old.",
            )
            return True
        if earliest > today + timedelta(days=30):
            _append_reject(
                candidate,
                "future_opening_too_early",
                f"Validator: food/opening date {earliest.isoformat()} is more than 30 days away.",
            )
            return True
        return False
    pub_day = _published_day(candidate)
    if pub_day is not None and (today - pub_day).days > 7:
        _append_reject(
            candidate,
            "stale_opening",
            f"Validator: undated food/opening article is {(today - pub_day).days} days old.",
        )
        return True
    return False


_BOOK_AUTHOR_MARKERS_RE = re.compile(
    r"\b(?:bestseller|best-?selling|memoir|autobiography|novelist|"
    r"novel|book\s+(?:hits|launch|tour|signing|release)|her\s+book|his\s+book|"
    r"(?:author|writer)\s+(?:of|hits|launches|releases|signs)|"
    r"автор(?:ом)?\s+(?:книги|бестселлера|мемуаров)|"
    r"книга|бестселлер|мемуары|роман|выпустил(?:а)?\s+книгу|"
    r"опубликовал(?:а)?\s+книгу)\b",
    re.IGNORECASE,
)
_TECH_MARKERS_RE = re.compile(
    r"\b(?:tech|software|startup|стартап|SaaS|AI|ML|fintech|"
    r"deeptech|biotech|cyber|cybersecurity|app\s+launch|platform\s+launch|"
    r"funding\s+round|series\s+[a-c]|seed\s+round|venture|"
    r"разработчик|программист|алгоритм|искусственн(?:ый|ого)\s+интеллект)\b",
    re.IGNORECASE,
)
# A concrete business action makes a tech/business card newsworthy.
_BUSINESS_ACTION_RE = re.compile(
    r"\b(?:jobs?|hiring|hire[ds]?|recruit\w*|investment|invest(?:s|ed|ing)?|"
    r"funding|raised|raise[ds]?|seed|series\s+[a-d]|grant|contract|deal|"
    r"acquisition|acquir(?:e|ed|es)|merger|takeover|"
    r"open(?:s|ed|ing)?|launch(?:es|ed|ing)?|clos(?:e|es|ed|ing|ure)|"
    r"relocat\w*|expand\w*|expansion|headquarters|\bhq\b|factory|warehouse|"
    r"\boffice\b|\bstore\b|plant|turnover|revenue|profit|loss|"
    r"redundanc\w*|layoffs?|administration|"
    r"£\d|\$\d|\d+\s*(?:jobs|roles|million|m\b|bn\b))\b",
    re.IGNORECASE,
)
_BUSINESS_IMPACT_RE = re.compile(
    r"\b(?:jobs?|roles?|investment|invest(?:s|ed|ing)?|funding|raised|"
    r"raise[ds]?|seed|series\s+[a-d]|grant|contract|deal|acquisition|"
    r"acquir(?:e|ed|es)|merger|takeover|open(?:s|ed|ing)?|launch(?:es|ed|ing)?|"
    r"clos(?:e|es|ed|ing|ure)|relocat\w*|headquarters|"
    r"\bhq\b|factory|warehouse|\boffice\b|\bstore\b|plant|turnover|"
    r"revenue|profit|loss|redundanc\w*|layoffs?|administration|"
    r"£\d|\$\d|\d+\s*(?:jobs|roles|million|m\b|bn\b))\b",
    re.IGNORECASE,
)
_BUSINESS_PERSONNEL_PR_RE = re.compile(
    r"\b(?:appoint(?:s|ed|ment)?|joins?\s+as|named\s+(?:as\s+)?|promot(?:es|ed|ion)|"
    r"new\s+(?:partner|director|chief|ceo|cfo|cto|coo|head\s+of|tax\s+partner)|"
    r"(?:partner|director|chief|ceo|cfo|cto|coo|head\s+of)\s+(?:appointment|hire|joins?))\b",
    re.IGNORECASE,
)
# Pure PR with no business action: anniversary, awards, campaigns, version
# milestones, founder back-stories.
_BUSINESS_PR_ONLY_RE = re.compile(
    r"\b(?:anniversary|years?\s+(?:in\s+business|of\s+(?:business|trading))|"
    r"celebrat\w*|award[s]?\b|wins?\s+award|shortlist\w*|nominat\w*|campaign|"
    r"\bv\d{1,2}\.\d|founder'?s?\s+story|community\s+champion|recogni[sz]\w*|"
    r"proud\s+to|годовщин\w*|юбилей|награ\w*|кампани\w*)\b",
    re.IGNORECASE,
)


def _exclude_book_author_in_tech_business(candidate: dict) -> bool:
    """tech_business cards about book authors/bestsellers are not tech news.

    Lindsey Meredith ("AUTHORity" beats Seth Godin on Amazon) is a book
    industry story, not an IT/business story. The Bdaily routing puts
    her in tech_business because the source is a business-news outlet,
    but the content has nothing to do with tech, software, or startups.

    Reject when ALL of:
      - category is tech_business or food_openings (similar misroute risk)
      - title+summary mentions a book/author marker
      - title+summary does NOT mention a tech/startup marker

    Local event hooks (book signing 30 May at HOME) are intentionally
    NOT routed here yet — those would belong in culture_weekly via a
    proper reroute, which we'll add in S4 when we touch the broader
    weak-item / human-interest filter.
    """
    if not candidate.get("include"):
        return False
    category = str(candidate.get("category") or "")
    if category not in {"tech_business", "food_openings"}:
        return False
    blob = _candidate_blob(candidate)
    if not _BOOK_AUTHOR_MARKERS_RE.search(blob):
        return False
    if _TECH_MARKERS_RE.search(blob):
        # Card mentions both — likely a tech-author crossover; let it
        # through and rely on other gates.
        return False
    _append_reject(
        candidate,
        "book_author_misrouted",
        "Validator: book/author story misrouted to tech_business — "
        "no tech/startup signal, no local-event hook.",
    )
    return True


def _exclude_pr_only_tech_business(candidate: dict) -> bool:
    """tech/business needs a concrete business action — jobs, investment,
    opening/closure, a contract, or local impact. Anniversary/award/campaign/
    version-milestone PR with none of those is not news (owner 2026-06-13:
    Manchester Digital V25.0 anniversary was correctly dropped as PR; IT/
    business publishes only on a concrete action)."""
    if not candidate.get("include"):
        return False
    if str(candidate.get("category") or "") != "tech_business":
        return False
    blob = _candidate_blob(candidate)
    if _BUSINESS_PERSONNEL_PR_RE.search(blob) and not _BUSINESS_IMPACT_RE.search(blob):
        _append_reject(
            candidate,
            "tech_business_personnel_pr",
            "Validator: personnel/partner appointment without concrete business "
            "impact — no jobs, investment, office, launch, contract, or financial action.",
        )
        return True
    if not _BUSINESS_PR_ONLY_RE.search(blob):
        return False
    if _BUSINESS_ACTION_RE.search(blob):
        # PR wrapper around a real action (e.g. "celebrates 25 years and opens
        # a second office, 40 new jobs") — keep it.
        return False
    _append_reject(
        candidate,
        "tech_business_pr_only",
        "Validator: tech/business PR (anniversary/award/campaign) with no "
        "concrete business action — jobs, investment, opening/closure, or contract.",
    )
    return True


_SOLD_OUT_RE = re.compile(
    r"\b(?:sold[\s-]?out|распродан\w*|билеты\s+(?:уже\s+)?распродан\w*)\b", re.IGNORECASE
)
_RESALE_RE = re.compile(
    r"\b(?:resale|re-?sale|waiting\s+list|returns?\b|перепродаж\w*|лист\s+ожидани\w*|"
    r"возврат\w*|освобод\w*\s+мест)\b",
    re.IGNORECASE,
)


def _exclude_sold_out_event(candidate: dict) -> bool:
    """A sold-out event with no resale/returns/waiting-list is not actionable —
    do not publish it as an upcoming pick (owner 2026-06-15: Lowry «Babies
    Playtime — билеты сейчас распроданы» in the next-7-days afisha)."""
    if not candidate.get("include"):
        return False
    if str(candidate.get("primary_block") or "") not in {
        "weekend_activities", "next_7_days", "future_announcements", "russian_events"
    }:
        return False
    blob = _candidate_blob(candidate)
    if _SOLD_OUT_RE.search(blob) and not _RESALE_RE.search(blob):
        _append_reject(
            candidate,
            "event_sold_out",
            "Validator: event is sold out with no resale/returns — no reader action.",
        )
        return True
    return False


def _is_market_fair_weekend_candidate(candidate: dict) -> bool:
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    if str(protected.get("lane") or "") in {"weekend_market", "recurring_market"}:
        return True
    blob = _candidate_blob(candidate)
    return bool(_MARKET_FAIR_WEEKEND_RE.search(blob))


def _is_routine_market_weekend_candidate(candidate: dict) -> bool:
    if not _is_market_fair_weekend_candidate(candidate):
        return False
    blob = _candidate_blob(candidate)
    if _RARE_MARKET_OR_FESTIVAL_RE.search(blob):
        return False
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    if str(protected.get("lane") or "") in {"weekend_market", "recurring_market"}:
        return True
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    return bool(event.get("is_recurring") or _ROUTINE_MARKET_RECURRENCE_RE.search(blob))


def _event_future_dates(candidate: dict) -> list[date]:
    today = now_london().date()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    dates: list[date] = []
    event_dt = _summary_field_datetime(str(candidate.get("summary") or ""), "event_date")
    if event_dt is not None:
        dates.append(event_dt.date())
    for iso_field in (
        str(event.get("date_start") or "").strip(),
        str(event.get("date") or "").strip(),
        str(event.get("date_iso") or "").strip(),
    ):
        if not iso_field:
            continue
        try:
            dates.append(datetime.fromisoformat(iso_field.replace("Z", "+00:00")).date())
        except (TypeError, ValueError):
            try:
                dates.append(date.fromisoformat(iso_field))
            except (TypeError, ValueError):
                pass
    if not dates:
        dates.extend(_explicit_dates_from_blob(candidate))
    return sorted({d for d in dates if d >= today})


def _reroute_market_planning_to_weekend(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    if str(candidate.get("primary_block") or "") != "next_7_days":
        return False
    if not _is_routine_market_weekend_candidate(candidate):
        return False
    today = now_london().date()
    future_dates = _event_future_dates(candidate)
    recurring = _has_recurrence_pattern(candidate)
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if future_dates:
        nearest = future_dates[0]
        if (nearest - today).days > 7:
            return False
    elif not (recurring or event.get("is_recurring")):
        return False
    candidate["primary_block"] = "weekend_activities"
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: market/car boot/fair belongs in weekend_activities, not next_7_days."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


def _exclude_sold_out_event(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    if category not in {"culture_weekly", "venues_tickets", "russian_speaking_events"} and block not in _EVENT_BLOCKS:
        return False
    if not _SOLD_OUT_EVENT_RE.search(_candidate_blob(candidate)):
        return False
    _append_reject(
        candidate,
        "event_sold_out",
        "Validator: sold-out event is not useful in the public digest.",
    )
    return True


def _exclude_court_roundup_listicle(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    if str(candidate.get("primary_block") or "") not in {"last_24h", "today_focus", "city_watch"}:
        return False
    blob = _candidate_blob(candidate)
    if not _COURT_ROUNDUP_RE.search(blob):
        return False
    _append_reject(
        candidate,
        "court_roundup_listicle",
        "Validator: court roundup listicle mixes several cases; publish a standalone case instead.",
    )
    return True


def _exclude_council_admin_without_impact(candidate: dict) -> bool:
    if not candidate.get("include"):
        return False
    if str(candidate.get("primary_block") or "") not in {"city_watch", "today_focus", "last_24h"}:
        return False
    blob = _candidate_blob(candidate)
    if not _COUNCIL_ADMIN_ONLY_RE.search(blob):
        return False
    if _COUNCIL_READER_IMPACT_RE.search(blob):
        return False
    _append_reject(
        candidate,
        "council_admin_no_reader_impact",
        "Validator: council leadership/admin item has no concrete reader impact.",
    )
    return True


def _exclude_stale_event(candidate: dict) -> bool:
    """Drop event candidates whose only date is already in the past.

    Catches stale aggregator listings like "Urmston Artisan Market 2 мая"
    surfacing in a 16 May digest — the LLM faithfully reproduced the
    title without realising the date had passed.

    Two date sources, in priority order:
      1. summary's event_date=YYYY-MM-DD field (set by Eventbrite/Ticketmaster
         parsers) — authoritative.
      2. First "<day> <month>" mention anywhere in title/summary/lead/
         evidence/source_url. Resolve year to current; if past, drop.

    Only fires for event-block candidates so we don't accidentally
    silence council news that mentions historical dates.
    """
    if not candidate.get("include"):
        return False
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    event_like = (
        category in {"culture_weekly", "venues_tickets", "russian_speaking_events"}
        or block in _EVENT_BLOCKS
    )
    if not event_like:
        return False

    today = now_london().date()

    # 1) authoritative structured event object from event_extraction.
    # RNCM and several venue parsers put the date into candidate["event"],
    # not into summary's event_date=... field; missing this let a 3 Jan
    # 2026 event ship in a 27 May 2026 "next 7 days" section.
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw_event_date = str(event.get("date_start") or event.get("date") or "").strip()
    if raw_event_date:
        try:
            parsed_event_date = datetime.fromisoformat(raw_event_date.replace("Z", "+00:00")).date()
        except ValueError:
            parsed_event_date = None
        if parsed_event_date is not None and parsed_event_date < today:
            if _has_recurrence_pattern(candidate):
                event["is_recurring"] = True
                return False
            candidate["include"] = False
            existing = str(candidate.get("reason") or "").strip()
            note = f"Validator: structured event date {parsed_event_date.isoformat()} is in the past."
            candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
            return True

    # 2) authoritative structured summary field
    summary = str(candidate.get("summary") or "")
    event_dt = _summary_field_datetime(summary, "event_date")
    if event_dt is not None and event_dt.date() < today:
        # Recurring schedules: don't reject — mark is_recurring so the
        # rewriter says "каждое воскресенье" rather than the dead start
        # date. Bowlee Car Boot Sale's start_date is 5 April but the
        # market runs every Sunday until September; that's still useful.
        if _has_recurrence_pattern(candidate):
            event = candidate.get("event")
            if isinstance(event, dict):
                event["is_recurring"] = True
            return False
        candidate["include"] = False
        existing = str(candidate.get("reason") or "").strip()
        note = f"Validator: event_date {event_dt.date().isoformat()} is in the past."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True

    # 3) heuristic "<day> <month>" date in any blob field
    blob = _candidate_blob(candidate)
    candidates_dates: list = []
    for m in _PAST_DATE_MONTH_RE.finditer(blob):
        try:
            day = int(m.group("day"))
        except ValueError:
            continue
        month = _MONTH_NUM[m.group("month").lower()]
        # Resolve year: closest future year if past in current year would be
        # >180 days back, else current year.
        try:
            this_year = today.replace(year=today.year).replace(month=month, day=day)
        except ValueError:
            continue
        if this_year < today and (today - this_year).days > 180:
            this_year = this_year.replace(year=today.year + 1)
        candidates_dates.append(this_year)

    if not candidates_dates:
        return False
    # If EVERY mentioned date is past, drop. Otherwise let it through —
    # presence of a future date means the card has something to offer.
    if all(d < today for d in candidates_dates):
        # Recurring schedules with a past start date: keep, mark recurring.
        if _has_recurrence_pattern(candidate):
            event = candidate.get("event")
            if isinstance(event, dict):
                event["is_recurring"] = True
            return False
        candidate["include"] = False
        existing = str(candidate.get("reason") or "").strip()
        latest_past = max(candidates_dates).isoformat()
        note = f"Validator: all event dates are in the past (last seen {latest_past})."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True
    return False


def _demote_distant_weekend_event(candidate: dict) -> bool:
    """Hard cutoff for «Выходные в GM»: only events within next 3 days stay.

    Without this, an Eventbrite listing for 6 June at today=22 May ends
    up under "Weekend in GM" simply because its source primary_block is
    weekend_activities. Demote based on the earliest dated occurrence:

    - in [today, today+3]:  keep in weekend_activities.
    - in [today+4, today+7]: demote to next_7_days.
    - in [today+8, today+30]: demote to future_announcements.
    - beyond today+30:      drop (no actionable horizon).
    - no dated occurrence:  keep (recurring or implicit-weekend
                            aggregator already handled by other gates).

    Recurring events (is_recurring=True from _exclude_stale_event):
    keep in weekend_activities only if the next sat/sun is within 3 days,
    otherwise demote to next_7_days so it resurfaces closer to the
    weekend.
    """
    if not candidate.get("include"):
        return False
    if str(candidate.get("primary_block") or "") != "weekend_activities":
        return False

    today = now_london().date()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}

    # A concrete future STRUCTURED date wins over recurrence detection: a
    # "one-off … event_date=2026-06-29" card is about that date even when
    # _exclude_stale_event flipped is_recurring=True on weak "Saturday market"
    # wording (owner 2026-06-13: a single far-future market must leave the
    # weekend board, not be kept by a misfired recurrence flag).
    _structured_dates: list[date] = []
    _ev_dt = _summary_field_datetime(str(candidate.get("summary") or ""), "event_date")
    if _ev_dt is not None:
        _structured_dates.append(_ev_dt.date())
    for _iso_field in (
        str(event.get("date_start") or "").strip(),
        str(event.get("date") or "").strip(),
        str(event.get("date_iso") or "").strip(),
    ):
        if not _iso_field:
            continue
        try:
            _structured_dates.append(datetime.fromisoformat(_iso_field.replace("Z", "+00:00")).date())
        except (TypeError, ValueError):
            try:
                _structured_dates.append(date.fromisoformat(_iso_field))
            except (TypeError, ValueError):
                pass
    # Only a NEAR-TERM structured date (within the 30-day actionable horizon)
    # overrides recurrence. A far-future structured date is almost always a
    # rolled-forward recurrence start ("5 April" → 2027-04-05), not a real
    # one-off, so it must NOT defeat the recurring branch below.
    _has_near_structured_future = any(today <= d <= today + timedelta(days=30) for d in _structured_dates)

    # If _exclude_stale_event has explicitly tagged the card as recurring
    # (past start date + "every Sunday" / "сезон до"), trust that and go
    # straight to the recurring branch — unless there is a concrete near-term
    # structured date (a genuine one-off). Without this short-circuit,
    # _explicit_dates_from_blob would auto-roll the past start date
    # ("5 April") to next year (2027-04-05) and the concrete-date
    # branch would drop the card as "318 days out".
    if event.get("is_recurring") is True and not _has_near_structured_future:
        days_to_sat = (5 - today.weekday()) % 7
        days_to_sun = (6 - today.weekday()) % 7
        nearest_weekend_day = min(days_to_sat, days_to_sun)
        if nearest_weekend_day <= 3:
            return False
        if _is_routine_market_weekend_candidate(candidate) and nearest_weekend_day <= 7:
            existing = str(candidate.get("reason") or "").strip()
            note = (
                "Validator: recurring market/car boot/fair stays in "
                "weekend_activities for weekend planning."
            )
            candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
            return False
        candidate["primary_block"] = "next_7_days"
        existing = str(candidate.get("reason") or "").strip()
        note = (
            f"Demoted from weekend_activities: recurring event's next "
            f"weekend occurrence is {nearest_weekend_day} days out."
        )
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True

    # Otherwise: concrete future date wins over recurrence detection.
    # A card that explicitly says "6 June" is about that specific
    # Saturday, not about "every Sunday" — even if the phrase
    # "Saturday market" appears in the summary and trips the
    # recurrence heuristic.
    dates: list[date] = []
    summary = str(candidate.get("summary") or "")
    event_dt = _summary_field_datetime(summary, "event_date")
    if event_dt is not None:
        dates.append(event_dt.date())
    for iso_field in (
        str(event.get("date_start") or "").strip(),
        str(event.get("date") or "").strip(),
        str(event.get("date_iso") or "").strip(),
    ):
        if not iso_field:
            continue
        try:
            dates.append(datetime.fromisoformat(iso_field.replace("Z", "+00:00")).date())
        except (TypeError, ValueError):
            try:
                dates.append(date.fromisoformat(iso_field))
            except (TypeError, ValueError):
                pass
    # Structured dates are the source of truth. Only use loose title/body
    # date mentions when the collector did not give us event_date/date_iso;
    # otherwise a stale or decorative title date can wrongly keep a far-out
    # event in the weekend section.
    if not dates:
        dates.extend(_explicit_dates_from_blob(candidate))

    future_dates = [d for d in dates if d >= today]
    if future_dates:
        earliest = min(future_dates)
        days_out = (earliest - today).days
        if days_out <= 3:
            return False
        if _is_routine_market_weekend_candidate(candidate) and days_out <= 7:
            existing = str(candidate.get("reason") or "").strip()
            note = (
                "Validator: market/car boot/fair stays in weekend_activities "
                "for weekend planning."
            )
            candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
            return False
        if days_out <= 7:
            candidate["primary_block"] = "next_7_days"
            target = "next_7_days"
        elif days_out <= 30:
            candidate["primary_block"] = "future_announcements"
            target = "future_announcements"
        else:
            candidate["include"] = False
            existing = str(candidate.get("reason") or "").strip()
            note = (
                f"Validator: weekend_activities item dated {earliest.isoformat()} "
                f"is {days_out} days out — beyond the 30-day actionable horizon."
            )
            candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
            return True
        existing = str(candidate.get("reason") or "").strip()
        note = (
            f"Demoted from weekend_activities to {target}: earliest date "
            f"{earliest.isoformat()} is {days_out} days out."
        )
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True

    # No concrete future date. Fall back to recurrence detection — if
    # the card describes a recurring market/season ("every Sunday",
    # "сезон до сентября"), check whether the next weekend day is
    # within the 3-day window. Otherwise leave the card alone.
    explicit_recurring = event.get("is_recurring")
    if explicit_recurring is False:
        return False
    recurring = bool(explicit_recurring) or _has_recurrence_pattern(candidate)
    if not recurring:
        return False
    days_to_sat = (5 - today.weekday()) % 7
    days_to_sun = (6 - today.weekday()) % 7
    nearest_weekend_day = min(days_to_sat, days_to_sun)
    if nearest_weekend_day <= 3:
        return False
    if _is_routine_market_weekend_candidate(candidate) and nearest_weekend_day <= 7:
        existing = str(candidate.get("reason") or "").strip()
        note = (
            "Validator: recurring market/car boot/fair stays in "
            "weekend_activities for weekend planning."
        )
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return False
    candidate["primary_block"] = "next_7_days"
    existing = str(candidate.get("reason") or "").strip()
    note = (
        f"Demoted from weekend_activities: recurring event's next "
        f"weekend occurrence is {nearest_weekend_day} days out."
    )
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


def _exclude_undated_event_like_candidate(candidate: dict) -> bool:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    lowered = _candidate_blob(candidate).lower()
    event_like = (
        category in {"culture_weekly", "venues_tickets", "russian_speaking_events"}
        or block in _EVENT_BLOCKS
    )
    if not event_like:
        return False
    if _has_future_or_concrete_date(candidate) or _has_computable_market_schedule(candidate):
        return False
    if not any(term in lowered for term in _EVENT_LIKE_TERMS + _RELATIVE_UNDATED_TERMS):
        return False
    report = event_quality_report(candidate)
    if report.get("is_event"):
        report["severity"] = "hard"
        candidate["event_quality"] = report
    candidate["include"] = False
    candidate["reject_reasons"] = sorted(set(
        [str(r) for r in candidate.get("reject_reasons") or [] if str(r).strip()]
        + ["no_date"]
    ))
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: event-like candidate has no concrete upcoming date."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


def _exclude_under_specified_event(candidate: dict) -> bool:
    """Apply the event quality gate to the live pipeline.

    The older validator only blocked stale/undated event-like candidates.
    This enforces the product rule with two severities: missing date/source
    is hard because the reader cannot act safely; missing borough/price is
    soft because official event pages are still useful even when sparse.
    """
    if not candidate.get("include"):
        return False
    report = event_quality_report(candidate)
    candidate["event_quality"] = report
    if not report.get("is_event") or report.get("ok"):
        return False

    reasons = event_quality_reject_reasons(candidate)
    missing = {str(item) for item in report.get("missing", [])}
    hard_missing = missing & {"date", "source"}
    if not hard_missing:
        warnings = sorted(set(str(r) for r in reasons if str(r).strip()))
        candidate["event_quality_warnings"] = warnings
        candidate["event_quality"]["severity"] = "soft"
        if warnings:
            candidate["quality_warnings"] = sorted(set(
                [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                + [f"event_quality:{r}" for r in warnings]
            ))
        return False

    candidate["event_quality"]["severity"] = "hard"
    candidate["include"] = False
    candidate["reject_reasons"] = sorted(set(
        [str(r) for r in candidate.get("reject_reasons") or [] if str(r).strip()]
        + reasons
    ))
    missing_text = ", ".join(str(item) for item in report.get("missing", []))
    existing = str(candidate.get("reason") or "").strip()
    note = f"Validator: event quality gate failed ({missing_text})."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


# Hosts that gate full article bodies behind a subscription. RSS / preview
# fetches return only a teaser, so any candidate from these hosts must carry
# substantive evidence_text from the preview itself. The detector below drops
# them when the preview body is too thin to write a self-contained card.
_PAYWALL_HOSTS = frozenset({
    "manchestermill.co.uk",
    "www.manchestermill.co.uk",
    "thelead.uk",
    "www.thelead.uk",
    "prolificnorth.co.uk",
    "www.prolificnorth.co.uk",
})

_PAYWALL_STUB_MARKERS = (
    "subscribe to continue",
    "sign in to continue",
    "join us to continue",
    "subscribe to read",
    "become a member",
    "members only",
    "log in to read",
    "this is a members",
    "this article is for paying",
    "the rest of this article",
    "support our journalism",
    "this story is for subscribers",
    "to keep reading",
    "to read more",
)


def _exclude_paywall_stub(candidate: dict) -> bool:
    """Drop premium-source candidates whose preview body is just a teaser.

    Cheap deterministic check that runs before the rewrite stage and saves
    LLM tokens on cards that would inevitably read as a vague rehash.
    """
    if not candidate.get("include"):
        return False
    url = str(candidate.get("source_url") or "")
    host = parse.urlsplit(url).netloc.lower()
    is_paywall_host = host in _PAYWALL_HOSTS
    evidence_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("evidence_text", "summary", "lead")
    )
    lowered = evidence_blob.lower()
    has_paywall_stub = any(marker in lowered for marker in _PAYWALL_STUB_MARKERS)
    if not (is_paywall_host or has_paywall_stub):
        return False
    # Paywall hosts: require at least ~220 chars of preview text to pass.
    # Anything shorter is a teaser the LLM can only pad out into vagueness.
    meaningful = len(re.sub(r"\s+", " ", evidence_blob).strip())
    if is_paywall_host and meaningful < 220:
        candidate["include"] = False
        existing = str(candidate.get("reason") or "").strip()
        note = f"Validator: paywall host {host} returned only {meaningful}c of preview body — full text not accessible."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True
    if has_paywall_stub:
        candidate["include"] = False
        existing = str(candidate.get("reason") or "").strip()
        note = "Validator: evidence_text contains paywall stub markers, full text not accessible."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True
    return False


def _exclude_thin_evidence_candidate(candidate: dict) -> bool:
    category = str(candidate.get("category") or "")
    if category not in {"media_layer", "gmp", "council", "public_services", "city_news", "tech_business", "football"}:
        return False
    blob = _candidate_blob(candidate)
    words = re.findall(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё'-]{2,}", blob)
    has_detail = bool(
        re.search(r"\b\d", blob)
        or re.search(r"£\s*\d", blob)
        or re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", blob)
    )
    if len(words) >= 22 or has_detail:
        return False
    candidate["include"] = False
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: evidence is too thin for a self-contained draft_line."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


def _apply_specificity_review(candidate: dict) -> None:
    """C3/C4: keep useful items, but make unclear crime/property items auditable.

    Collector enrichment has already run before validation. Here we use the
    enriched text rather than immediately rejecting: first mark borderline and
    demote to city_watch; reject only when even the enriched fields are too
    thin to tell the reader what happened / where.
    """
    if not candidate.get("include"):
        return

    reviews = {
        "crime": crime_specificity_review(candidate),
        "property": property_specificity_review(candidate),
    }
    applied = {name: review for name, review in reviews.items() if review.get("applies")}
    if not applied:
        return

    candidate["specificity_review"] = applied
    hard_reasons: list[str] = []
    borderline_reasons: list[str] = []
    for name, review in applied.items():
        severity = str(review.get("severity") or "none")
        missing = ",".join(str(item) for item in (review.get("missing") or []))
        if severity == "hard":
            hard_reasons.append(f"{name}_too_unclear:{missing}")
        elif severity == "borderline":
            borderline_reasons.append(f"{name}_borderline:{missing}")

    if hard_reasons:
        candidate["include"] = False
        candidate["reject_reasons"] = sorted(set(
            [str(r) for r in candidate.get("reject_reasons") or [] if str(r).strip()]
            + hard_reasons
        ))
        existing = str(candidate.get("reason") or "").strip()
        note = "Validator: enriched item is still too unclear for a self-contained digest card."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return

    if borderline_reasons:
        # Protected hard news with a formal news_anchor must NOT be sent
        # to the borderline pool. On 2026-05-27 we held "Man arrested
        # over Manchester synagogue attack" and "Guns, samurai sword
        # and cocaine seized in Wigan raids" — both had
        # protected_lane=public_safety and news_anchor=True but a
        # specificity-borderline tag still demoted them to city_watch
        # and into the quarantine queue.
        lane = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
        anchor = candidate.get("news_anchor") if isinstance(candidate.get("news_anchor"), dict) else {}
        if lane.get("protected") and anchor.get("has_news_anchor"):
            # Record the soft warnings for the audit trail but do not
            # flip editorial_status and do not demote out of the top
            # blocks. Specificity gaps here are an enrichment problem,
            # not a publishability problem.
            candidate["quality_warnings"] = sorted(set(
                [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                + [f"{r}__protected_override" for r in borderline_reasons]
            ))
            candidate["specificity_review_protected_override"] = True
            return

        candidate["editorial_status"] = "borderline"
        candidate["quality_warnings"] = sorted(set(
            [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
            + borderline_reasons
        ))
        # Borderline specificity should not get top billing. Keep it
        # reviewable but demote from today/24h blocks into city radar.
        if str(candidate.get("primary_block") or "") in {"today_focus", "last_24h"}:
            candidate["primary_block"] = "city_watch"
            existing = str(candidate.get("reason") or "").strip()
            note = "Validator: demoted to city_watch after specificity review."
            candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note


def _manual_override(candidate: dict, state_dir: Path) -> str:
    fp = str(candidate.get("fingerprint") or "").strip()
    if not fp:
        return ""
    path = state_dir / "manual_candidate_overrides.json"
    payload = read_json(path, {"force_include": [], "force_exclude": []}) if path.exists() else {}
    force_include = {str(item) for item in payload.get("force_include") or []}
    force_exclude = {str(item) for item in payload.get("force_exclude") or []}
    if fp in force_exclude:
        candidate["include"] = False
        candidate["editorial_status"] = "manual_excluded"
        candidate["reject_reasons"] = sorted(set(
            [str(r) for r in candidate.get("reject_reasons") or [] if str(r).strip()]
            + ["manual_exclude"]
        ))
        return "force_exclude"
    if fp in force_include:
        candidate["include"] = True
        candidate["editorial_status"] = "approved"
        candidate["manual_override"] = "force_include"
        candidate["quality_warnings"] = [
            str(r) for r in candidate.get("quality_warnings") or []
            if not str(r).startswith(("crime_borderline", "property_borderline", "event_schema_missing"))
        ]
        return "force_include"
    return ""


# Anchor types that legitimately re-appear day after day. Conservative
# on purpose: transport service status, weather, dated events with a
# concrete future date, on-sale ticket windows. Everything else (including
# anchors like ``new_phase`` and ``local_action`` that LLMs assign liberally
# to repeats) is fair game for cross-day stale-rehash blocking.
_ALWAYS_PUBLISHABLE_REPEAT_ANCHORS = frozenset({
    "service_status",       # ongoing transport / road / utility disruption
    "today_weather",        # daily weather card
    "dated_event",          # event with a concrete future date
    "ticket_opportunity",   # on-sale / presale window
    "ongoing_disruption",   # explicit ongoing flag
})


def _exclude_cross_day_rehash(candidate: dict, state_dir: Path) -> bool:
    """Block items whose fingerprint already shipped in the digest within
    the past few days.

    Catches "ЭТО ДАЕТСЯ УЖЕ 3 ДЕНЬ" complaints from 2026-05-25: GRUB
    Stretford foodhall (4 days in a row), Rochdale historic bridge
    (3 days), Pelican Inn Timperley (3 days). All have why_now and an
    editorial_contract but the contract's anchor_type (local_action,
    planning) is not on the always-publishable list, so a same-fingerprint
    repeat from a previous day is a stale rehash and must not visible.

    The check reads daily_index/YYYY-MM-DD.jsonl files for the last
    ``repeat_ttl_days`` days plus one. Days are skipped silently if the
    file is missing. The block is a hard reject — owner's rule "never
    block the release" still applies because the digest ships regardless
    (just without this item).
    """
    if not candidate.get("include"):
        return False
    fingerprint = str(candidate.get("fingerprint") or "").strip()
    if not fingerprint:
        return False
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    anchor = str(contract.get("anchor_type") or "")
    if anchor in _ALWAYS_PUBLISHABLE_REPEAT_ANCHORS:
        return False
    # Operational blocks (transport, weather) self-manage rotation via
    # transport_fill and synthetic_freshness; never gate them here.
    block = str(candidate.get("primary_block") or "")
    if block in {"transport", "weather"}:
        return False
    if block == "weekend_activities" and _has_recurrence_pattern(candidate):
        today_date = now_london().date()
        days_to_sat = (5 - today_date.weekday()) % 7
        days_to_sun = (6 - today_date.weekday()) % 7
        if min(days_to_sat, days_to_sun) <= 7:
            # A recurring market/fair is not a stale repeat just because
            # yesterday's digest also mentioned the same page. The actionable
            # occurrence is the next weekend instance; keep it eligible across
            # the planning week and let writer caps/budget decide visibility.
            return False

    policy = contract.get("section_policy") if isinstance(contract.get("section_policy"), dict) else {}
    try:
        ttl_days = max(1, int(policy.get("repeat_ttl_days") or 1))
    except (TypeError, ValueError):
        ttl_days = 1
    # bookable_listing / future event TTL override: a concert on Saturday
    # mentioned in Monday's digest is STILL the same valid concert on
    # Tuesday — the default ttl=1d would reject it as a rehash. While
    # the event itself is still in the future (and within 14 days), the
    # cross-day rehash window must follow the event, not the original
    # post date. Closes the 2026-05-27 Lowry Boys / Cherryholt loss.
    if anchor == "bookable_listing":
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        event_day_str = str(event.get("date_start") or event.get("date") or "").strip()
        if event_day_str:
            try:
                event_day = datetime.strptime(event_day_str[:10], "%Y-%m-%d").date()
            except ValueError:
                event_day = None
            if event_day is not None:
                today_date = now_london().date()
                if today_date <= event_day <= today_date + timedelta(days=14):
                    # Event is still upcoming within 14 days — do not
                    # block as a cross-day rehash, the concert/market
                    # itself is still happening.
                    return False
        # Older/undated bookable_listings fall through to the default ttl.
    # Look back ttl_days + 1 to catch the day before yesterday for
    # short-TTL items (default 1d means "yesterday").
    lookback = ttl_days + 1

    today = now_london().date()
    daily_dir = state_dir / "daily_index"
    if not daily_dir.exists():
        return False

    for offset in range(1, lookback + 1):
        check_day = (today - timedelta(days=offset)).isoformat()
        path = daily_dir / f"{check_day}.jsonl"
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if str(rec.get("fingerprint") or "") != fingerprint:
                continue
            if not rec.get("included"):
                continue
            phase = classify_change_phase(candidate)
            previous_phase = classify_change_phase(rec)
            if phase and phase != previous_phase:
                candidate["change_phase"] = phase
                candidate["change_type"] = "new_phase"
                existing = str(candidate.get("reason") or "").strip()
                note = (
                    f"Validator: same fingerprint was published on {check_day}, "
                    f"but today has a new phase ({phase})."
                )
                candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
                candidate["quality_warnings"] = sorted(set(
                    [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                    + ["cross_day_same_url_new_phase"]
                ))
                return False
            # Hit — same fingerprint already shipped on a previous day.
            candidate["include"] = False
            candidate["change_type"] = "same_story_rehash"
            existing = str(candidate.get("reason") or "").strip()
            note = (
                f"Validator: cross-day rehash — fingerprint already shipped on {check_day} "
                f"(anchor={anchor or 'unknown'}, ttl={ttl_days}d)."
            )
            candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
            candidate["reject_reasons"] = sorted(set(
                [str(r) for r in candidate.get("reject_reasons") or [] if str(r).strip()]
                + ["cross_day_rehash"]
            ))
            return True
    return False


def _apply_why_now_gate(candidate: dict, *, manual_override: str = "") -> None:
    """Q1: make today's reason explicit before the writer sees the item."""
    why_now = infer_why_now(candidate)
    candidate["why_now"] = why_now
    if manual_override == "force_include" or not candidate.get("include"):
        return
    if why_now == "stale":
        _append_reject(
            candidate,
            "why_now_stale",
            "Validator: no publishable reason for today's morning issue; item is stale/no-change.",
        )
        return
    if not why_now_is_publishable(why_now):
        candidate["editorial_status"] = "borderline"
        candidate["quality_warnings"] = sorted(set(
            [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
            + [f"why_now_unclear:{why_now}"]
        ))
        if str(candidate.get("primary_block") or "") in {"today_focus", "last_24h"}:
            candidate["primary_block"] = "city_watch"
        existing = str(candidate.get("reason") or "").strip()
        note = "Validator: held for manual review because why-now value is unclear."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note


def validate_candidates(project_root: Path) -> StageResult:
    stage_started = time.monotonic()
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    report_path = state_dir / "candidate_validation_report.json"

    payload = read_json(candidates_path, {"candidates": []})
    candidates = payload.get("candidates", [])
    errors: list[str] = []
    items: list[dict] = []

    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict):
            errors.append(f"Candidate #{index} is not an object.")
            continue

        # E2: enrichment happens before any validator gate. This prevents
        # false "no date / no venue / unclear" decisions based on raw RSS
        # snippets when the body or structured source fields already contain
        # the missing facts.
        enrich_candidate_entities(candidate)
        enrich_candidate_event(candidate)

        validation_errors: list[str] = []
        url = clean_url(str(candidate.get("source_url") or "").strip())
        candidate["source_url"] = url
        label = str(candidate.get("source_label") or "").strip()

        if candidate.get("include"):
            if not url:
                validation_errors.append("Missing source_url.")
            if not label:
                validation_errors.append("Missing source_label.")
            if not str(candidate.get("title") or "").strip():
                validation_errors.append("Missing title.")
            if not str(candidate.get("primary_block") or "").strip():
                validation_errors.append("Missing primary_block.")

        lowered = url.lower()
        if "/amp/" in lowered:
            validation_errors.append("AMP URL is forbidden.")
        if _is_search_url(url):
            validation_errors.append("Search URL is forbidden.")
        if candidate.get("include") and _is_topic_or_index_url(url):
            candidate["include"] = False
            candidate["reason"] = str(candidate.get("reason") or "").rstrip() + " | Validator: topic/index URL, not a standalone item."
        # NOTE: previously weekend_activities candidates were dropped here unless
        # title+path carried a date token. evidence_text / summary were ignored,
        # so venue events that carry the date in the page body were lost en masse.
        # _exclude_undated_event_like_candidate below covers the same intent but
        # reads the full candidate blob, so we let it handle the date check.
        # Tag transport candidates with mode + Russian-facing operator so the
        # rewriter never has to infer "Автобус:" vs "Metrolink:" from a
        # TfGM roadworks bulletin. Idempotent and safe for non-transport.
        if candidate.get("include"):
            apply_professional_event_match(candidate, project_root)
        classify_transport_candidate(candidate)
        attach_change_phase(candidate)
        attach_editorial_contract(candidate)
        apply_story_intelligence(candidate)
        if candidate.get("include"):
            _apply_section_routing_quality(candidate)
        manual = _manual_override(candidate, state_dir)
        if candidate.get("include") and manual != "force_include":
            _exclude_cross_day_rehash(candidate, state_dir)
        if candidate.get("include") and manual != "force_include":
            _exclude_road_only_transport(candidate)
        if candidate.get("include"):
            _reclassify_outside_gm_when_local_venue(candidate)
        if candidate.get("include"):
            _reclassify_gm_when_outside_venue(candidate)
        if candidate.get("include"):
            _exclude_stale_ticket_onsale(candidate)
        if candidate.get("include"):
            _ensure_default_ticket_type(candidate)
        if candidate.get("include"):
            _reroute_tour_announcement(candidate)
        if candidate.get("include"):
            _exclude_non_gm_news(candidate)
        if candidate.get("include"):
            _exclude_low_value_news(candidate)
        if candidate.get("include"):
            _exclude_celebrity_sighting(candidate)
        if candidate.get("include"):
            _exclude_motivational_human_interest(candidate)
        if candidate.get("include"):
            _exclude_historical_no_news_angle(candidate)
        if candidate.get("include"):
            _exclude_book_author_in_tech_business(candidate)
        if candidate.get("include"):
            _exclude_pr_only_tech_business(candidate)
        if candidate.get("include"):
            _exclude_sold_out_event(candidate)
        if candidate.get("include"):
            _exclude_court_roundup_listicle(candidate)
        if candidate.get("include"):
            _exclude_council_admin_without_impact(candidate)
        if candidate.get("include"):
            _exclude_stale_undated_news_from_text(candidate)
        if candidate.get("include") and manual != "force_include":
            _demote_optional_top_news_by_contract(candidate)
        if candidate.get("include") and manual != "force_include":
            _exclude_by_editorial_contract(candidate)
        if candidate.get("include"):
            _exclude_paywall_stub(candidate)
        if candidate.get("include") and manual != "force_include":
            _hold_sensitive_thin_or_failed_enrichment(candidate)
        if candidate.get("include"):
            _exclude_stale_news_without_new_phase(candidate)
        if candidate.get("include"):
            _exclude_wrong_food_opening_category(candidate)
        if candidate.get("include"):
            _exclude_bad_food_opening_timing(candidate)
        if candidate.get("include"):
            _apply_specificity_review(candidate)
        if candidate.get("include"):
            _exclude_stale_event(candidate)
        if candidate.get("include"):
            _reroute_market_planning_to_weekend(candidate)
        if candidate.get("include"):
            _demote_distant_weekend_event(candidate)
        if candidate.get("include"):
            _exclude_undated_event_like_candidate(candidate)
        if candidate.get("include"):
            _exclude_under_specified_event(candidate)
        if candidate.get("include"):
            _exclude_sold_out_event(candidate)
        completeness = event_schema_completeness(candidate)
        if completeness.get("applies"):
            candidate["event_schema_completeness"] = completeness
            # Only required fields (date_start, venue) hold an event. Missing
            # price/booking_url/borough are still recorded in completeness for
            # reporting but no longer route the item to the borderline queue.
            required_missing = completeness.get("required_missing") or []
            if required_missing and candidate.get("include"):
                candidate["quality_warnings"] = sorted(set(
                    [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                    + [f"event_schema_missing:{','.join(str(m) for m in required_missing)}"]
                ))
        if manual == "force_include":
            candidate["include"] = True
            candidate["editorial_status"] = "approved"
        attach_editorial_contract(candidate)
        _apply_why_now_gate(candidate, manual_override=manual)
        attach_reader_action(candidate)
        attach_editorial_contract(candidate)
        apply_story_intelligence(candidate)
        if candidate.get("event_page_type") in {"homepage", "aggregator"}:
            validation_errors.append("Event candidate must use an official event page.")

        if candidate.get("source_trial") and candidate.get("include") and manual != "force_include":
            candidate["include"] = False
            candidate["trial_status"] = "validated_not_publishable"
            candidate["editorial_status"] = "trial"
            existing = str(candidate.get("reason") or "").strip()
            note = "Validator: source is in trial mode, so candidate is measured but not published."
            candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note

        attach_reader_value(candidate)
        attach_scoring_trace(candidate)

        candidate["validation_errors"] = validation_errors
        candidate["validated"] = not validation_errors
        items.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "validated": not validation_errors,
                "validation_errors": validation_errors,
                "event_quality": candidate.get("event_quality"),
                "specificity_review": candidate.get("specificity_review"),
                "event_schema_completeness": candidate.get("event_schema_completeness"),
                "why_now": candidate.get("why_now") or "",
                "change_phase": candidate.get("change_phase") or "",
                "reader_action_type": candidate.get("reader_action_type") or "",
                "source_trial": bool(candidate.get("source_trial")),
                "trial_status": candidate.get("trial_status") or "",
                "editorial_contract": candidate.get("editorial_contract") or {},
                "rubric_contract": candidate.get("rubric_contract") or {},
                "news_anchor": candidate.get("news_anchor") or {},
                "protected_lane": candidate.get("protected_lane") or {},
                "english_judge": candidate.get("english_judge") or {},
                "second_opinion_required": bool(candidate.get("second_opinion_required")),
                "reject_reasons": candidate.get("reject_reasons") or [],
                "quality_warnings": candidate.get("quality_warnings") or [],
                "editorial_status": candidate.get("editorial_status") or "",
            }
        )

        if candidate.get("include") and validation_errors:
            errors.append(f"Candidate #{index} failed validation.")

    practical_backfill = apply_practical_backfill(candidates)
    if practical_backfill:
        for candidate in candidates:
            if isinstance(candidate, dict):
                attach_reader_action(candidate)
                attach_editorial_contract(candidate)
    city_intelligence = annotate_city_intelligence(candidates)
    payload["run_at_london"] = now_london().isoformat()
    payload["run_date_london"] = today_london()
    pipeline_run_id = pipeline_run_id_from(payload)
    write_json(candidates_path, payload)
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "city_intelligence": city_intelligence,
            "practical_backfill": practical_backfill,
            "trial_candidates": sum(1 for c in candidates if isinstance(c, dict) and c.get("source_trial")),
            "items": items,
            "duration_seconds": round(time.monotonic() - stage_started, 3),
        },
    )

    return StageResult(
        not errors,
        "Candidate validation completed." if not errors else "Candidate validation found blocking errors.",
        report_path,
    )
