from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib import parse

from news_digest.pipeline.common import clean_url, now_london, read_json, today_london, write_json


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
    write_json(candidates_path, payload)
    write_json(
        report_path,
        {
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
