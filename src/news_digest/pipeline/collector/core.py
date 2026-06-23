"""Collector entry points.

`collect_digest` is the top-level loop: iterate SOURCES, fetch each one
(primary + fallback), extract candidates, route, fall back where blocks
would be empty, write `collector_report.json` and `candidates.json`.

`initialize_collector_state` writes a stub report+history bundle so the
rest of the pipeline can start from a known shape on a fresh checkout.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
import json
from pathlib import Path
import os
import re
import time
from urllib import parse

from news_digest.pipeline.common import (
    REQUIRED_SCAN_CATEGORIES,
    new_pipeline_run_id,
    now_london,
    today_london,
    write_json,
)
from news_digest.pipeline.entity_extraction import enrich_candidates_entities
from news_digest.pipeline.event_extraction import enrich_candidates_events
from news_digest.pipeline.history import ensure_history_files
from news_digest.pipeline.story_intelligence import (
    apply_cheap_dedup_before_enrich,
    attach_story_clusters,
    attach_story_intelligence,
)

from .extract import _extract_source_candidates
from .fallbacks import (
    _last_24h_fallback_candidates,
    _transport_fallback_candidates,
    _weather_candidate,
)
from .fetch import NotModified, _fetch_source_body, _fetch_text, load_fetch_cache, save_fetch_cache
from .dates import _parse_datetime_value
from .routing import _promote_to_today_focus, _reroute_media_transit_to_transport
from .sources import SOURCES
from .summary import _looks_like_active_disruption


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


# 89 sources today; widening the pool from 6 → 12 halves the rounds.
# Cloudflare-protected sources still go through the curl_cffi cascade
# inside _fetch_source_body, so per-source resilience is unchanged.
_COLLECTOR_MAX_WORKERS = 12
_SENSITIVE_QUERY_KEYS = {"apikey", "api_key", "key", "token", "access_token"}
_HARD_NEWS_SOURCES = {
    "BBC Manchester",
    "BBC Manchester Web",
    "BBC Manchester public safety fallback",
    "MEN",
    "MEN Latest News",
    "MEN News Sitemap",
    "About Manchester News",
}
_OFFICIAL_BACKGROUND_SOURCES = {
    "GMCA",
    "Manchester Council",
    "Stockport Council",
    "Oldham Council",
    "Rochdale Council",
    "Bolton Council",
    "Bury Council",
    "Wigan Council",
    "Trafford Council",
    "Salford Council",
    "Tameside Council",
}
_EVENT_SOURCE_TYPES = {
    "html_eventbrite",
    "html_eventbrite_events",
    "html_page_event",
    "html_visitmanchester_events",
    "html_phm_events",
    "html_the_manc_weekly_events",
    "html_sectioned_event_guide",
    "html_designmynight",
    "html_eventfirst",
    "html_kontramarka",
}


def _redact_sensitive_url(value: str) -> str:
    raw = str(value or "")
    if not raw:
        return raw
    parsed = parse.urlsplit(raw)
    if not parsed.query:
        return raw
    query = parse.parse_qsl(parsed.query, keep_blank_values=True)
    redacted = [
        (key, "***" if key.lower() in _SENSITIVE_QUERY_KEYS else val)
        for key, val in query
    ]
    return parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, parse.urlencode(redacted), parsed.fragment)
    )


def _redact_sensitive_text(value: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        raw_url = match.group(0)
        suffix = ":" if raw_url.endswith(":") else ""
        return _redact_sensitive_url(raw_url.rstrip(":")) + suffix

    return re.sub(r"https?://\S+", _replace, str(value or ""))


def _source_contract(source) -> str:
    explicit = str(getattr(source, "source_contract", "") or "").strip()
    if explicit:
        return explicit
    if source.source_type == "json_ticketmaster":
        return "ticket_api"
    if source.report_category == "transport":
        return "transport_live"
    if source.name in _HARD_NEWS_SOURCES:
        return "hard_news_daily"
    if source.name in _OFFICIAL_BACKGROUND_SOURCES:
        return "official_background"
    if source.report_category == "media_layer":
        return "news_periodic"
    if source.report_category == "venues_tickets":
        return "venue_calendar"
    if source.report_category in {"culture_weekly", "diaspora_events", "professional_events"} or source.source_type in _EVENT_SOURCE_TYPES:
        return "event_calendar"
    if source.report_category == "food_openings":
        return "openings_watch"
    if source.report_category == "football":
        return "football_official"
    if source.report_category == "tech_business":
        return "business_news"
    if source.report_category == "public_services":
        return "official_background"
    return "generic"


def _ticketmaster_page_url(url: str, page: int) -> str:
    parsed = parse.urlsplit(url)
    query = parse.parse_qsl(parsed.query, keep_blank_values=True)
    updated: list[tuple[str, str]] = []
    replaced = False
    for key, value in query:
        if key == "page":
            updated.append((key, str(page)))
            replaced = True
        else:
            updated.append((key, value))
    if not replaced:
        updated.append(("page", str(page)))
    return parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parse.urlencode(updated), parsed.fragment))


def _ticketmaster_pagination_limit(source) -> int:
    name = str(getattr(source, "name", "") or "").lower()
    if "ticketmaster uk major upcoming" in name:
        return 5
    if "ticketmaster uk major onsale" in name:
        return 3
    return 1


def _fetch_ticketmaster_paginated_body(source, body: str, fetched_url: str) -> tuple[str, list[str]]:
    """Merge extra Ticketmaster pages for UK-wide artist-watch sources.

    A single countrywide Ticketmaster request sorted by date only returns the
    first page, not "all UK". Stars at open-air/heritage venues can sit on a
    later page and never reach notability scoring.
    """
    limit = _ticketmaster_pagination_limit(source)
    if limit <= 1:
        return body, []
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body, ["Ticketmaster pagination skipped: first page was not JSON."]
    page_info = payload.get("page") if isinstance(payload.get("page"), dict) else {}
    try:
        total_pages = int(page_info.get("totalPages") or 1)
        current_page = int(page_info.get("number") or 0)
    except (TypeError, ValueError):
        total_pages = 1
        current_page = 0
    max_page_exclusive = min(total_pages, current_page + limit)
    if max_page_exclusive <= current_page + 1:
        return body, []
    embedded = payload.setdefault("_embedded", {})
    events = embedded.setdefault("events", [])
    if not isinstance(events, list):
        return body, ["Ticketmaster pagination skipped: first page events payload was not a list."]
    warnings: list[str] = []
    seen_ids = {str(event.get("id") or event.get("url") or "") for event in events if isinstance(event, dict)}
    for page_num in range(current_page + 1, max_page_exclusive):
        page_url = _ticketmaster_page_url(fetched_url, page_num)
        try:
            page_payload = json.loads(_fetch_text(page_url))
        except Exception as exc:  # noqa: BLE001 - keep the source usable if one page fails.
            warnings.append(f"Ticketmaster pagination page {page_num} failed: {_redact_sensitive_text(str(exc))}")
            continue
        page_events = ((page_payload.get("_embedded") or {}).get("events") or [])
        if not isinstance(page_events, list):
            continue
        added = 0
        for event in page_events:
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("id") or event.get("url") or "")
            if event_id and event_id in seen_ids:
                continue
            if event_id:
                seen_ids.add(event_id)
            events.append(event)
            added += 1
        warnings.append(f"Ticketmaster pagination page {page_num}: added {added} event(s).")
    return json.dumps(payload), warnings


def _candidate_has_upcoming_date(candidate: dict) -> bool:
    parsed = _parse_datetime_value(str(candidate.get("published_at") or ""))
    if parsed is None:
        return False
    now = now_london()
    return (now - timedelta(days=1)) <= parsed <= (now + timedelta(days=540))


def _coverage_signal_count(source, candidates: list[dict]) -> tuple[int, str]:
    contract = _source_contract(source)
    if contract == "hard_news_daily":
        return (
            sum(1 for candidate in candidates if candidate.get("freshness_status") == "fresh_24h"),
            "fresh published items",
        )
    if contract in {"event_calendar", "venue_calendar", "ticket_api"}:
        dated = sum(1 for candidate in candidates if _candidate_has_upcoming_date(candidate))
        return (dated or len(candidates), "upcoming dated items" if dated else "items")
    if contract == "transport_live":
        return (len(candidates), "live/known transport items")
    return (len(candidates), "items")


def _default_report() -> dict:
    payload = {
        "run_at_london": now_london().isoformat(),
        "run_date_london": today_london(),
        "stage_status": "incomplete",
        "total_duration_seconds": 0.0,
        "max_workers": _COLLECTOR_MAX_WORKERS,
        "categories": {
            key: {
                "checked": False,
                "sources": [],
                "source_health": [],
                "candidate_count": 0,
                "publishable_count": 0,
                "dated_candidate_count": 0,
                "fresh_last_24h_count": 0,
                "usable_for_release": False,
                "notes": "",
                "errors": [],
                "duration_seconds": 0.0,
            }
            for key in REQUIRED_SCAN_CATEGORIES
        },
    }
    payload["categories"]["public_services"]["active_disruption_today"] = False
    return payload


def _source_health_template(source) -> dict:
    return {
        "name": source.name,
        "url": source.url,
        "source_contract": _source_contract(source),
        "trial": bool(getattr(source, "trial", False)),
        "checked": False,
        "fetched": False,
        "not_modified": False,
        "candidate_count": 0,
        "publishable_count": 0,
        "dated_candidate_count": 0,
        "fresh_last_24h_count": 0,
        "coverage_signal_count": 0,
        "coverage_signal_label": "",
        "usable_for_release": False,
        "fallback_used": False,
        "failure_class": "",
        "reliability_ladder_step": "",
        "recommended_next_action": "",
        "errors": [],
        "warnings": [],
        "duration_seconds": 0.0,
        "fetch_duration_seconds": 0.0,
        "extract_duration_seconds": 0.0,
    }


def _classify_source_failure(
    *,
    fetched: bool,
    not_modified: bool,
    candidate_count: int,
    errors: list[str],
    warnings: list[str],
) -> tuple[str, str, str]:
    """Return error taxonomy + next reliability step.

    The operational order is deliberate: understand the failure first, fix the
    parser/pagination when the page fetched, and only then escalate to curl_cffi
    or proxy for proven access/WAF failures.
    """
    if not_modified:
        return ("healthy_not_modified", "none", "no action; source reached and unchanged")
    text = " ".join(errors + warnings).lower()
    if errors:
        if any(token in text for token in ("403", "forbidden", "cloudflare", "waf")):
            return ("fetch_403_waf", "curl_cffi_candidate", "prove WAF/403, then try curl_cffi for this source only")
        if any(token in text for token in ("429", "too many requests", "rate limit")):
            return ("fetch_rate_limited", "backoff_or_schedule", "reduce request pressure or add per-source backoff")
        if any(token in text for token in ("timed out", "timeout", "read timed out")):
            return ("fetch_timeout", "fetch_tuning", "check timeout, fallback URL, and source availability")
        if any(token in text for token in ("name or service", "nodename", "dns", "temporary failure")):
            return ("fetch_dns", "source_url_check", "verify source URL/domain before transport workaround")
        if any(token in text for token in ("404", "not found", "410", "gone")):
            return ("source_url_dead", "source_registry_fix", "replace or disable dead URL")
        return ("fetch_error_unknown", "error_taxonomy", "classify the error before adding transport workarounds")
    if fetched and candidate_count == 0:
        if any(token in text for token in ("pagination", "page 2", "next page")):
            return ("pagination_gap", "pagination_fix", "extend parser pagination for this source")
        return ("parser_or_filter_empty", "parser_fix", "inspect HTML/feed shape and source filter before curl/proxy")
    if fetched:
        return ("healthy_with_candidates", "none", "no reliability action")
    return ("not_checked", "error_taxonomy", "source was not checked")


def _collect_single_source(source) -> tuple[dict, list[dict]]:
    source_health = _source_health_template(source)
    started_at = time.perf_counter()
    try:
        if source.source_type == "json_ticketmaster" and not os.environ.get("TICKETMASTER_API_KEY", "").strip():
            raise RuntimeError("missing TICKETMASTER_API_KEY for Ticketmaster API source")
        if source.source_type == "json_skiddle" and not os.environ.get("SKIDDLE_API_KEY", "").strip():
            raise RuntimeError("missing SKIDDLE_API_KEY for Skiddle API source")
        fetch_started_at = time.perf_counter()
        try:
            body, fetched_url, attempt_log = _fetch_source_body(source)
        except NotModified:
            # 304 — source's feed is byte-identical to last fetch. Skip
            # parsing: every item it would yield is already covered by
            # published_facts.json (or yesterday's candidates) and dedupe
            # would drop them anyway. Mark as a healthy non-error state
            # distinct from a real fetch failure.
            source_health["fetch_duration_seconds"] = round(time.perf_counter() - fetch_started_at, 3)
            source_health["checked"] = True
            source_health["fetched"] = True
            source_health["not_modified"] = True
            source_health["fetched_url"] = _redact_sensitive_url(source.url)
            # Treat not_modified as usable: the source IS reachable; we
            # just have no new items today. Without this, transport-style
            # health flips to "unhealthy" on quiet days.
            source_health["usable_for_release"] = True
            source_health["warnings"].append("304 Not Modified — no new content since last fetch")
            cls, step, action = _classify_source_failure(
                fetched=True,
                not_modified=True,
                candidate_count=0,
                errors=[],
                warnings=source_health["warnings"],
            )
            source_health["failure_class"] = cls
            source_health["reliability_ladder_step"] = step
            source_health["recommended_next_action"] = action
            return source_health, []
        source_health["fetch_duration_seconds"] = round(time.perf_counter() - fetch_started_at, 3)
        if attempt_log:
            source_health["fallback_used"] = True
            source_health["warnings"].append(
                f"primary URL failed; switched to fallback {fetched_url}"
            )
        source_health["fetched_url"] = _redact_sensitive_url(fetched_url)
        for attempt_note in attempt_log:
            source_health["warnings"].append(f"attempt failed: {_redact_sensitive_text(attempt_note)}")
        if source.source_type == "json_ticketmaster":
            body, pagination_warnings = _fetch_ticketmaster_paginated_body(source, body, fetched_url)
            source_health["warnings"].extend(pagination_warnings)

        extract_started_at = time.perf_counter()
        source_candidates = _extract_source_candidates(source, body)
        source_health["extract_duration_seconds"] = round(time.perf_counter() - extract_started_at, 3)
        source_health["checked"] = True
        source_health["fetched"] = True
        source_health["candidate_count"] = len(source_candidates)
        source_health["publishable_count"] = sum(
            1 for candidate in source_candidates if candidate.get("include")
        )
        source_health["dated_candidate_count"] = sum(
            1 for candidate in source_candidates if candidate.get("published_at")
        )
        source_health["fresh_last_24h_count"] = sum(
            1 for candidate in source_candidates if candidate.get("freshness_status") == "fresh_24h"
        )
        signal_count, signal_label = _coverage_signal_count(source, source_candidates)
        source_health["coverage_signal_count"] = signal_count
        source_health["coverage_signal_label"] = signal_label
        source_health["usable_for_release"] = bool(signal_count) or source.report_category in {"transport"}
        if not source_candidates:
            message = f"{source.name}: fetched successfully but no candidate links passed filters"
            source_health["warnings"].append(message)
        cls, step, action = _classify_source_failure(
            fetched=True,
            not_modified=False,
            candidate_count=len(source_candidates),
            errors=[],
            warnings=source_health["warnings"],
        )
        source_health["failure_class"] = cls
        source_health["reliability_ladder_step"] = step
        source_health["recommended_next_action"] = action
        return source_health, source_candidates
    except Exception as exc:  # noqa: BLE001 - errors must be surfaced in collector_report.
        source_health["checked"] = True
        source_health["errors"].append(str(exc))
        cls, step, action = _classify_source_failure(
            fetched=False,
            not_modified=False,
            candidate_count=0,
            errors=source_health["errors"],
            warnings=source_health["warnings"],
        )
        source_health["failure_class"] = cls
        source_health["reliability_ladder_step"] = step
        source_health["recommended_next_action"] = action
        return source_health, []
    finally:
        source_health["duration_seconds"] = round(time.perf_counter() - started_at, 3)


def initialize_collector_state(project_root: Path, *, overwrite: bool = False) -> StageResult:
    state_dir = project_root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    ensure_history_files(state_dir)

    report_path = state_dir / "collector_report.json"
    if overwrite or not report_path.exists():
        write_json(report_path, _default_report())

    return StageResult(True, f"Collector state initialized at {report_path}.", report_path)


def collect_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    ensure_history_files(state_dir)

    collect_started_at = time.perf_counter()
    pipeline_run_id = new_pipeline_run_id()
    report = _default_report()
    report["pipeline_run_id"] = pipeline_run_id
    candidates: list[dict] = [_weather_candidate()]
    for source in SOURCES:
        report["categories"][source.report_category]["sources"].append(source.name)

    # Load HTTP-validator cache once; fetchers add conditional headers
    # and update the cache via module-level state. Flushed at the end
    # of the run regardless of whether parsing succeeds.
    load_fetch_cache(state_dir)

    with ThreadPoolExecutor(max_workers=_COLLECTOR_MAX_WORKERS) as executor:
        source_results = list(executor.map(_collect_single_source, SOURCES))

    save_fetch_cache(state_dir)

    for source, (source_health, source_candidates) in zip(SOURCES, source_results, strict=True):
        category_report = report["categories"][source.report_category]
        category_report["source_health"].append(source_health)
        category_report["checked"] = True
        category_report["duration_seconds"] += source_health["duration_seconds"]
        category_report["candidate_count"] += len(source_candidates)
        category_report["publishable_count"] += sum(
            1 for candidate in source_candidates if candidate.get("include")
        )
        category_report["dated_candidate_count"] += sum(
            1 for candidate in source_candidates if candidate.get("published_at")
        )
        category_report["fresh_last_24h_count"] += sum(
            1 for candidate in source_candidates if candidate.get("freshness_status") == "fresh_24h"
        )
        if source_health["errors"]:
            category_report["errors"].append(f"{source.name}: {source_health['errors'][0]}")
        elif source_health.get("not_modified"):
            # 304 — valid healthy state. Skip both the "filter rejected
            # everything" warning and any error log; the validators in
            # fetch_cache.json carry over so tomorrow re-sends them.
            pass
        elif not source_candidates and source_health.get("source_contract") in {"hard_news_daily", "ticket_api"}:
            category_report["errors"].append(
                f"{source.name}: fetched successfully but no candidate links passed filters"
            )
        candidates.extend(source_candidates)

    for category in report["categories"].values():
        if category["checked"]:
            fetched_sources = sum(
                1 for item in category.get("source_health", []) if isinstance(item, dict) and item.get("fetched")
            )
            total_sources = len(category["sources"])
            category["duration_seconds"] = round(float(category.get("duration_seconds") or 0.0), 3)
            category["notes"] = (
                f"Fetched {fetched_sources}/{total_sources} source(s); "
                f"{category['publishable_count']} publishable candidate(s), "
                f"{category['dated_candidate_count']} dated candidate(s); "
                f"{category['duration_seconds']:.3f}s total source time."
            )
            category["usable_for_release"] = any(
                isinstance(item, dict) and item.get("usable_for_release")
                for item in category.get("source_health", [])
            )
        else:
            category["notes"] = "No source in this category fetched successfully."
            category["usable_for_release"] = False

    report["categories"]["public_services"]["active_disruption_today"] = any(
        isinstance(candidate, dict)
        and candidate.get("include")
        and candidate.get("category") == "public_services"
        and str(candidate.get("freshness_status") or "") != "stale"
        and _looks_like_active_disruption(str(candidate.get("title") or ""))
        for candidate in candidates
    )
    candidates.extend(_transport_fallback_candidates(report))
    _reroute_media_transit_to_transport(candidates)
    _promote_to_today_focus(candidates)
    candidates.extend(_last_24h_fallback_candidates(candidates))
    cheap_dedup_summary = apply_cheap_dedup_before_enrich(candidates)
    enrich_candidates_entities(candidates)
    # I3: structured event facts. Must run AFTER entity enrichment so
    # extract_event() can reuse entities.venues / entities.boroughs.
    enrich_candidates_events(candidates)
    story_cluster_summary = attach_story_clusters(candidates)
    attach_story_intelligence(candidates)
    report["story_intelligence"] = {
        "cheap_dedup_before_enrich": cheap_dedup_summary,
        "story_clusters": story_cluster_summary,
    }

    checked_all = all(
        bool(report["categories"][key]["checked"])
        for key in REQUIRED_SCAN_CATEGORIES
    )
    report["stage_status"] = "complete" if checked_all else "incomplete"
    report["total_duration_seconds"] = round(time.perf_counter() - collect_started_at, 3)

    candidates_payload = {
        "pipeline_run_id": pipeline_run_id,
        "run_at_london": now_london().isoformat(),
        "run_date_london": today_london(),
        "stage_status": "complete" if candidates else "incomplete",
        "candidates": candidates,
    }

    report_path = state_dir / "collector_report.json"
    write_json(report_path, report)
    write_json(state_dir / "candidates.json", candidates_payload)

    if checked_all:
        return StageResult(True, f"Collector fetched {len(candidates)} candidate(s).", report_path)

    incomplete = [
        label
        for key, label in REQUIRED_SCAN_CATEGORIES.items()
        if not bool(report["categories"][key]["checked"])
    ]
    return StageResult(False, "Collector incomplete: " + ", ".join(incomplete), report_path)
