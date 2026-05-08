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
from pathlib import Path
import time

from news_digest.pipeline.common import (
    REQUIRED_SCAN_CATEGORIES,
    now_london,
    today_london,
    write_json,
)
from news_digest.pipeline.history import ensure_history_files

from .extract import _extract_source_candidates
from .fallbacks import (
    _last_24h_fallback_candidates,
    _transport_fallback_candidates,
    _weather_candidate,
)
from .fetch import _fetch_source_body
from .routing import _promote_to_today_focus
from .sources import SOURCES
from .summary import _looks_like_active_disruption


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


_COLLECTOR_MAX_WORKERS = 6


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
        "checked": False,
        "fetched": False,
        "candidate_count": 0,
        "publishable_count": 0,
        "dated_candidate_count": 0,
        "fresh_last_24h_count": 0,
        "usable_for_release": False,
        "errors": [],
        "warnings": [],
        "duration_seconds": 0.0,
        "fetch_duration_seconds": 0.0,
        "extract_duration_seconds": 0.0,
    }


def _collect_single_source(source) -> tuple[dict, list[dict]]:
    source_health = _source_health_template(source)
    started_at = time.perf_counter()
    try:
        fetch_started_at = time.perf_counter()
        body, fetched_url, attempt_log = _fetch_source_body(source)
        source_health["fetch_duration_seconds"] = round(time.perf_counter() - fetch_started_at, 3)
        if fetched_url != source.url:
            source_health["warnings"].append(
                f"primary URL failed; switched to fallback {fetched_url}"
            )
        source_health["fetched_url"] = fetched_url
        for attempt_note in attempt_log:
            source_health["warnings"].append(f"attempt failed: {attempt_note}")

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
        source_health["usable_for_release"] = bool(source_candidates) or source.report_category in {
            "transport"
        }
        if not source_candidates:
            message = f"{source.name}: fetched successfully but no candidate links passed filters"
            source_health["warnings"].append(message)
        return source_health, source_candidates
    except Exception as exc:  # noqa: BLE001 - errors must be surfaced in collector_report.
        source_health["checked"] = True
        source_health["errors"].append(str(exc))
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
    report = _default_report()
    candidates: list[dict] = [_weather_candidate()]
    for source in SOURCES:
        report["categories"][source.report_category]["sources"].append(source.name)

    with ThreadPoolExecutor(max_workers=_COLLECTOR_MAX_WORKERS) as executor:
        source_results = list(executor.map(_collect_single_source, SOURCES))

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
        if source_health.get("fetched_url") and source_health["fetched_url"] != source.url:
            category_report["errors"].append(
                f"{source.name}: primary URL failed, fallback used ({source_health['fetched_url']})"
            )
        if source_health["errors"]:
            category_report["errors"].append(f"{source.name}: {source_health['errors'][0]}")
        elif not source_candidates:
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
        and candidate.get("category") == "public_services"
        and _looks_like_active_disruption(str(candidate.get("title") or ""))
        for candidate in candidates
    )
    candidates.extend(_transport_fallback_candidates(report))
    _promote_to_today_focus(candidates)
    candidates.extend(_last_24h_fallback_candidates(candidates))

    checked_all = all(
        bool(report["categories"][key]["checked"])
        for key in REQUIRED_SCAN_CATEGORIES
    )
    report["stage_status"] = "complete" if checked_all else "incomplete"
    report["total_duration_seconds"] = round(time.perf_counter() - collect_started_at, 3)

    candidates_payload = {
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
