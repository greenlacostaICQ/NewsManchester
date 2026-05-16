from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from urllib import parse

from news_digest.pipeline.common import clean_url, now_london, pipeline_run_id_from, read_json, today_london, write_json
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
    if "ticket_signal=onsale" not in summary.lower():
        return False
    onsale_at = _summary_field_datetime(summary, "public_onsale")
    if onsale_at is None or onsale_at >= now_london():
        return False
    candidate["include"] = False
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: public_onsale is already in the past."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


_EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
}
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


def _candidate_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_url")
    )


def _has_future_or_concrete_date(candidate: dict) -> bool:
    summary = str(candidate.get("summary") or "")
    if _summary_field_datetime(summary, "event_date") is not None:
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
    return "market" in lowered and bool(
        re.search(
            r"\b(?:every|first|1st|second|2nd|third|3rd|last)\s+(?:saturday|sunday|weekend|month)\b",
            lowered,
        )
    )


_PAST_DATE_MONTH_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month>january|february|march|april|may|"
    r"june|july|august|september|october|november|december)\b",
    re.IGNORECASE,
)
_MONTH_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


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

    # 1) authoritative structured date
    summary = str(candidate.get("summary") or "")
    event_dt = _summary_field_datetime(summary, "event_date")
    if event_dt is not None and event_dt.date() < today:
        candidate["include"] = False
        existing = str(candidate.get("reason") or "").strip()
        note = f"Validator: event_date {event_dt.date().isoformat()} is in the past."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True

    # 2) heuristic "<day> <month>" date in any blob field
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
        candidate["include"] = False
        existing = str(candidate.get("reason") or "").strip()
        latest_past = max(candidates_dates).isoformat()
        note = f"Validator: all event dates are in the past (last seen {latest_past})."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        return True
    return False


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
    candidate["include"] = False
    existing = str(candidate.get("reason") or "").strip()
    note = "Validator: event-like candidate has no concrete upcoming date."
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


def validate_candidates(project_root: Path) -> StageResult:
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
        classify_transport_candidate(candidate)
        if candidate.get("include"):
            _exclude_stale_ticket_onsale(candidate)
        if candidate.get("include"):
            _exclude_paywall_stub(candidate)
        if candidate.get("include"):
            _exclude_stale_event(candidate)
        if candidate.get("include"):
            _exclude_undated_event_like_candidate(candidate)
        if candidate.get("event_page_type") in {"homepage", "aggregator"}:
            validation_errors.append("Event candidate must use an official event page.")

        candidate["validation_errors"] = validation_errors
        candidate["validated"] = not validation_errors
        items.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "validated": not validation_errors,
                "validation_errors": validation_errors,
            }
        )

        if candidate.get("include") and validation_errors:
            errors.append(f"Candidate #{index} failed validation.")

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
            "items": items,
        },
    )

    return StageResult(
        not errors,
        "Candidate validation completed." if not errors else "Candidate validation found blocking errors.",
        report_path,
    )
