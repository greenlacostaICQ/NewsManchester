from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from urllib import parse

from news_digest.pipeline.common import clean_url, now_london, pipeline_run_id_from, read_json, today_london, write_json
from news_digest.pipeline.editorial_quality import (
    evaluate_editorial_rubric,
    included_rubric_red_flags,
    reader_value_components,
    reader_value_report,
    reader_value_score,
    rubric_summary,
)
from news_digest.pipeline.reject_reasons import (
    add_reject_reason,
    ensure_reject_reason,
    ensure_reject_reasons,
    reject_reason_counts,
    reject_reasons,
)


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
    add_reject_reason(candidate, "expired")
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
    add_reject_reason(candidate, "no_date")
    return True


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
    add_reject_reason(candidate, "source_thin")
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
            add_reject_reason(candidate, "invalid_url")
        if _is_search_url(url):
            validation_errors.append("Search URL is forbidden.")
            add_reject_reason(candidate, "invalid_url")
        if candidate.get("include") and _is_topic_or_index_url(url):
            candidate["include"] = False
            candidate["reason"] = str(candidate.get("reason") or "").rstrip() + " | Validator: topic/index URL, not a standalone item."
            add_reject_reason(candidate, "invalid_url")
        if candidate.get("include"):
            _exclude_stale_ticket_onsale(candidate)
        if candidate.get("include"):
            _exclude_undated_event_like_candidate(candidate)
        if candidate.get("include"):
            _exclude_thin_evidence_candidate(candidate)
        if candidate.get("event_page_type") in {"homepage", "aggregator"}:
            validation_errors.append("Event candidate must use an official event page.")
            add_reject_reason(candidate, "invalid_url")

        candidate["validation_errors"] = validation_errors
        candidate["validated"] = not validation_errors
        ensure_reject_reason(candidate)
        candidate["editorial_rubric"] = evaluate_editorial_rubric(candidate)
        candidate["reader_value_components"] = reader_value_components(candidate)
        candidate["reader_value_score"] = reader_value_score(candidate)
        items.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "validated": not validation_errors,
                "validation_errors": validation_errors,
                "include": bool(candidate.get("include")),
                "reject_reasons": reject_reasons(candidate),
                "editorial_rubric": candidate.get("editorial_rubric"),
                "reader_value_score": candidate.get("reader_value_score"),
                "reader_value_components": candidate.get("reader_value_components"),
            }
        )

        if candidate.get("include") and validation_errors:
            errors.append(f"Candidate #{index} failed validation.")

    payload["run_at_london"] = now_london().isoformat()
    payload["run_date_london"] = today_london()
    pipeline_run_id = pipeline_run_id_from(payload)
    ensure_reject_reasons(candidates)
    write_json(candidates_path, payload)
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "reject_reason_counts": reject_reason_counts(candidates),
            "editorial_rubric_summary": rubric_summary(candidates),
            "included_rubric_red_flags": included_rubric_red_flags(candidates),
            "reader_value_report": reader_value_report(candidates),
            "items": items,
        },
    )

    return StageResult(
        not errors,
        "Candidate validation completed." if not errors else "Candidate validation found blocking errors.",
        report_path,
    )
