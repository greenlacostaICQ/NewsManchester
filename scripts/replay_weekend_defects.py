#!/usr/bin/env python3
"""Replay the July weekend-inventory loss modes against current code."""
from __future__ import annotations

import json
from pathlib import Path
import re
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from news_digest.pipeline.collector.extract import _extract_source_candidates  # noqa: E402
from news_digest.pipeline.collector.sources import SourceDef  # noqa: E402
from news_digest.pipeline.dedupe import _topic_published_matches  # noqa: E402
from news_digest.pipeline.weekend_inventory import weekend_occurrence_date  # noqa: E402
from news_digest.pipeline.writer import (  # noqa: E402
    _build_weekend_event_fallback_line,
    _collapse_weekend_duplicate_events,
    _is_outside_current_weekend_candidate,
    _line_has_conflicting_event_date,
)


REPLAY_DAYS = ("2026-07-07", "2026-07-08", "2026-07-09")
DATE_LOSS_MARKERS = (
    "Outside current weekend window",
    "event date in draft_line conflicts",
    "structured event date",
)
RECURRING_SCHEDULE_RE = re.compile(
    r"dates?:\s*(?:from\s+|every\s+)?(?:saturdays?|sundays?|bank\s+holiday\s+mondays?)|"
    r"\b(?:every|each|all|most|weekly)\s+(?:saturdays?|sundays?)|"
    r"\b(?:saturdays?|sundays?)\s+(?:weekly|every\s+week)\b",
    re.IGNORECASE,
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _candidate_from_rows(audit: dict[str, Any], daily: dict[str, Any] | None) -> dict[str, Any]:
    daily = daily or {}
    evidence = daily.get("evidence_packet") if isinstance(daily.get("evidence_packet"), dict) else {}
    audit_input = audit.get("input") if isinstance(audit.get("input"), dict) else {}
    scoring = daily.get("scoring_trace") if isinstance(daily.get("scoring_trace"), dict) else {}
    scoring_contract = scoring.get("editorial_contract") if isinstance(scoring.get("editorial_contract"), dict) else {}
    contract = daily.get("editorial_contract") if isinstance(daily.get("editorial_contract"), dict) else None
    contract = contract or evidence.get("editorial_contract") or scoring_contract or {}
    event = daily.get("event") if isinstance(daily.get("event"), dict) else None
    event = event or evidence.get("event") or {}
    candidate = {
        "fingerprint": str(audit.get("fingerprint") or daily.get("fingerprint") or ""),
        "title": str(audit.get("title") or daily.get("title") or evidence.get("title") or ""),
        "source_label": str(audit.get("source_label") or daily.get("source_label") or evidence.get("source_label") or ""),
        "source_url": str(audit.get("source_url") or daily.get("url") or evidence.get("source_url") or ""),
        "category": str(audit.get("category") or daily.get("category") or evidence.get("category") or ""),
        "primary_block": str(audit.get("primary_block") or daily.get("primary_block") or evidence.get("primary_block") or ""),
        "published_at": str(audit_input.get("published_at") or evidence.get("published_at") or ""),
        "lead": str(audit_input.get("lead") or evidence.get("lead") or ""),
        "summary": str(audit_input.get("summary") or evidence.get("summary") or ""),
        "evidence_text": str(audit_input.get("evidence_text") or evidence.get("evidence_text") or ""),
        "event": event,
        "editorial_contract": contract,
        "include": bool(audit.get("include") or daily.get("included")),
        "section_board_score": float(daily.get("section_board_score") or 0.0),
    }
    topic_key = str(contract.get("topic_key") or "").strip()
    if topic_key:
        candidate["repeat_story_key"] = topic_key
    return candidate


def _daily_by_fingerprint(day: str) -> dict[str, dict[str, Any]]:
    rows = _read_jsonl(PROJECT_ROOT / "data" / "state" / "daily_index" / f"{day}.jsonl")
    return {str(row.get("fingerprint") or ""): row for row in rows if row.get("fingerprint")}


def _published_facts_by_title() -> list[dict[str, Any]]:
    path = PROJECT_ROOT / "data" / "state" / "published_facts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [row for row in payload.get("facts", []) if isinstance(row, dict)]


def _previous_from_reason(reason: str, published: list[dict[str, Any]]) -> dict[str, Any] | None:
    match = re.search(r"как «(.+?)»", reason)
    if not match:
        return None
    needle = match.group(1).lower()
    for fact in published:
        if needle and needle in str(fact.get("title") or "").lower():
            return fact
    return None


def _date_loss_after(candidate: dict[str, Any], reason: str) -> bool:
    occurrence = weekend_occurrence_date(candidate)
    if occurrence is None:
        return True
    line = _build_weekend_event_fallback_line(candidate)
    if "Outside current weekend window" in reason:
        return _is_outside_current_weekend_candidate(candidate, line)
    if "event date in draft_line conflicts" in reason:
        return _line_has_conflicting_event_date(candidate, line)
    if "structured event date" in reason:
        return False
    return False


def _has_recurring_schedule_signal(candidate: dict[str, Any]) -> bool:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    blob = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            event.get("date_text"),
        )
    )
    return bool(RECURRING_SCHEDULE_RE.search(blob))


def _duplicate_replay(rendered: list[dict[str, Any]]) -> tuple[int, int, list[str]]:
    if not rendered:
        return 0, 0, []
    lines: list[str] = []
    srcs: list[str] = []
    fps: list[str] = []
    scores: list[float] = []
    titles: list[str] = []
    candidate_by_fp: dict[str, dict[str, Any]] = {}
    for candidate in rendered:
        fp = str(candidate.get("fingerprint") or "")
        line = _build_weekend_event_fallback_line(candidate) or f"• {candidate.get('title')}"
        lines.append(line)
        srcs.append(str(candidate.get("source_label") or ""))
        fps.append(fp)
        scores.append(float(candidate.get("section_board_score") or 0.0))
        titles.append(str(candidate.get("title") or ""))
        candidate_by_fp[fp] = candidate
    _, _, _, _, _, dropped = _collapse_weekend_duplicate_events(lines, srcs, fps, scores, titles, candidate_by_fp)
    return len(dropped), 0, [str(item.get("title") or item.get("fingerprint") or "") for item in dropped]


def _challenge_after_count() -> int:
    source = SourceDef(
        name="Replay Challenge Page",
        report_category="culture_weekly",
        candidate_category="culture_weekly",
        url="https://example.test/markets",
        primary_block="weekend_activities",
        source_type="html",
    )
    body = "<html><title>One moment, please...</title><body>Please wait while your request is being verified</body></html>"
    return len(_extract_source_candidates(source, body))


def main() -> int:
    published = _published_facts_by_title()
    challenge_after = _challenge_after_count()
    totals = {
        "before_date": 0,
        "after_date": 0,
        "before_topic": 0,
        "after_topic": 0,
        "before_dupes": 0,
        "after_dupes": 0,
        "before_challenge": 0,
        "after_challenge": 0,
    }
    print("date | date-loss before->after | false-topic before->after | duplicate rows before->after | challenge candidates before->after")
    print("-" * 118)
    for day in REPLAY_DAYS:
        daily = _daily_by_fingerprint(day)
        audits = [
            row for row in _read_jsonl(PROJECT_ROOT / "data" / "state" / "audit_trail" / f"{day}.jsonl")
            if row.get("primary_block") == "weekend_activities"
        ]
        candidates = [
            _candidate_from_rows(row, daily.get(str(row.get("fingerprint") or "")))
            for row in audits
        ]
        date_losses = [
            (row, candidate)
            for row, candidate in zip(audits, candidates)
            if any(marker in str(row.get("final_reason") or "") for marker in DATE_LOSS_MARKERS)
            and _has_recurring_schedule_signal(candidate)
        ]
        topic_losses = [
            (row, candidate)
            for row, candidate in zip(audits, candidates)
            if "Повтор темы без новой фазы" in str(row.get("final_reason") or "")
        ]
        rendered = [
            candidate for row, candidate in zip(audits, candidates)
            if str(row.get("final_disposition") or "") == "rendered"
        ]
        before_dupes, after_dupes, dupe_titles = _duplicate_replay(rendered)
        before_challenge = sum(
            1 for row in audits
            if "one moment, please" in str(row.get("title") or "").lower()
            or "request is being verified" in json.dumps(row.get("input") or {}).lower()
        )
        after_date = sum(
            1 for row, candidate in date_losses
            if _date_loss_after(candidate, str(row.get("final_reason") or ""))
        )
        after_topic = 0
        for row, candidate in topic_losses:
            previous = _previous_from_reason(str(row.get("final_reason") or ""), published)
            topic_key = str(candidate.get("repeat_story_key") or "")
            if previous and topic_key and _topic_published_matches(candidate, {topic_key: [previous]}):
                after_topic += 1
        totals["before_date"] += len(date_losses)
        totals["after_date"] += after_date
        totals["before_topic"] += len(topic_losses)
        totals["after_topic"] += after_topic
        totals["before_dupes"] += before_dupes
        totals["after_dupes"] += after_dupes
        totals["before_challenge"] += before_challenge
        totals["after_challenge"] += challenge_after if before_challenge else 0
        dupe_note = f" ({', '.join(dupe_titles[:3])})" if dupe_titles else ""
        print(
            f"{day} | "
            f"{len(date_losses)}->{after_date} | "
            f"{len(topic_losses)}->{after_topic} | "
            f"{before_dupes}->{after_dupes}{dupe_note} | "
            f"{before_challenge}->{challenge_after if before_challenge else 0}"
        )
    print("-" * 118)
    print(
        "TOTAL | "
        f"{totals['before_date']}->{totals['after_date']} | "
        f"{totals['before_topic']}->{totals['after_topic']} | "
        f"{totals['before_dupes']}->{totals['after_dupes']} | "
        f"{totals['before_challenge']}->{totals['after_challenge']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
