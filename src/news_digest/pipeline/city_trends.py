from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from news_digest.pipeline.city_intelligence import (
    GM_BOROUGHS,
    candidate_boroughs,
    candidate_topic_tags,
)
from news_digest.pipeline.common import now_london, read_json, write_json
from news_digest.pipeline.entity_extraction import extract_entities
from news_digest.pipeline.event_extraction import extract_event, is_event_candidate


CITY_TRENDS_SCHEMA_VERSION = 1
TREND_WINDOWS_DAYS: tuple[int, ...] = (1, 3, 7, 30)
CITY_HISTORY_FILENAME = "city_intelligence_history.json"
WEEKLY_ROLLUP_FILENAME = "weekly_city_rollup.json"

_ENTITY_BUCKET_TYPES = {
    "boroughs": "borough",
    "districts": "district",
    "stations": "station",
    "councils": "council",
    "venues": "venue",
    "clubs": "club",
    "companies": "company",
}
_ENTITY_STOPLIST = {
    "bbc",
    "bbc manchester",
    "itv",
    "itv granada",
    "men",
    "the mill",
    "ticketmaster",
}


def _history_path(state_dir: Path) -> Path:
    return state_dir / CITY_HISTORY_FILENAME


def _rollup_path(state_dir: Path) -> Path:
    return state_dir / WEEKLY_ROLLUP_FILENAME


def load_city_history(state_dir: Path) -> list[dict]:
    path = _history_path(state_dir)
    if not path.exists():
        return []
    try:
        payload = read_json(path, [])
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _rendered_set(rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None) -> set[str]:
    if isinstance(rendered_fingerprints, dict):
        return {str(fp) for fp in (rendered_fingerprints.get("rendered_candidate_fingerprints") or [])}
    return {str(fp) for fp in (rendered_fingerprints or [])}


def _selected_candidates(
    candidates: Iterable[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None,
) -> tuple[list[dict], str]:
    rendered = _rendered_set(rendered_fingerprints)
    if rendered:
        return [
            c for c in candidates
            if isinstance(c, dict) and str(c.get("fingerprint") or "") in rendered
        ], "rendered"
    return [c for c in candidates if isinstance(c, dict) and c.get("include")], "included"


def _count_topic_tags(candidates: list[dict]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for candidate in candidates:
        tags = candidate.get("topic_tags")
        if not isinstance(tags, list) or not tags:
            tags = candidate_topic_tags(candidate)
        for tag in {str(tag) for tag in tags if str(tag or "").strip()}:
            counts[tag] += 1
    return counts


def _entity_key(entity_type: str, name: str) -> str:
    return f"{entity_type}:{name}"


def _split_entity_key(key: str) -> tuple[str, str]:
    if ":" not in key:
        return "entity", key
    entity_type, name = key.split(":", 1)
    return entity_type, name


def _count_entities(candidates: list[dict]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for candidate in candidates:
        entities = candidate.get("entities")
        if not isinstance(entities, dict):
            entities = extract_entities(candidate)
        seen: set[str] = set()
        for bucket, entity_type in _ENTITY_BUCKET_TYPES.items():
            values = entities.get(bucket) if isinstance(entities, dict) else []
            if not isinstance(values, list):
                continue
            for value in values:
                name = str(value or "").strip()
                if not name or name.lower() in _ENTITY_STOPLIST:
                    continue
                seen.add(_entity_key(entity_type, name))
        for key in seen:
            counts[key] += 1
    return counts


def _count_boroughs(candidates: list[dict]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for candidate in candidates:
        for borough in candidate_boroughs(candidate):
            counts[borough] += 1
    return counts


def _event_summary(candidates: list[dict]) -> dict[str, object]:
    by_borough: Counter[str] = Counter()
    by_venue: Counter[str] = Counter()
    by_date: Counter[str] = Counter()
    event_count = 0
    dated_count = 0
    undated_count = 0
    for candidate in candidates:
        event = candidate.get("event")
        if not isinstance(event, dict):
            event = extract_event(candidate)
        if not event.get("is_event") and not is_event_candidate(candidate):
            continue
        event_count += 1
        borough = str(event.get("borough") or "").strip()
        venue = str(event.get("venue") or "").strip()
        date_value = str(event.get("date") or "").strip()
        if borough:
            by_borough[borough] += 1
        for fallback_borough in candidate_boroughs(candidate):
            if not borough:
                by_borough[fallback_borough] += 1
                break
        if venue:
            by_venue[venue] += 1
        if date_value:
            by_date[date_value] += 1
            dated_count += 1
        else:
            undated_count += 1
    return {
        "event_count": event_count,
        "dated_event_count": dated_count,
        "undated_event_count": undated_count,
        "by_borough": dict(sorted(by_borough.items())),
        "by_venue": dict(by_venue.most_common(12)),
        "by_date": dict(sorted(by_date.items())),
    }


def _risk_snapshot(report_payload: dict | None) -> dict[str, object]:
    report = report_payload if isinstance(report_payload, dict) else {}
    source_status = report.get("source_status") or {}
    source_counts = source_status.get("counts") or {}
    digest_health = report.get("digest_health") or {}
    city_intelligence = report.get("city_intelligence") or {}
    borough_coverage = city_intelligence.get("borough_coverage") or {}
    warnings = [str(w) for w in (report.get("warnings") or []) if str(w).strip()]
    return {
        "release_decision": report.get("release_decision") or "",
        "digest_risk_level": digest_health.get("risk_level") or "",
        "warning_count": len(warnings),
        "source_failed_count": int(source_counts.get("failed") or 0),
        "zero_yield_sources": int(source_counts.get("zero_yield") or 0),
        "lost_leads": len(report.get("lost_leads") or []),
        "section_underflow": len(report.get("section_underflow") or []),
        "borough_skew_flags": list(borough_coverage.get("skew_flags") or []),
    }


def _counter_payload(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def build_daily_city_snapshot(
    *,
    run_date_london: str,
    report_payload: dict | None,
    candidates: list[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None = None,
    trend_detection: dict | None = None,
) -> dict[str, object]:
    selected, basis = _selected_candidates(candidates, rendered_fingerprints)
    return {
        "schema_version": CITY_TRENDS_SCHEMA_VERSION,
        "run_date_london": run_date_london,
        "run_at_london": now_london().isoformat(),
        "basis": basis,
        "item_count": len(selected),
        "topics": _counter_payload(_count_topic_tags(selected)),
        "entities": _counter_payload(_count_entities(selected)),
        "boroughs": _counter_payload(_count_boroughs(selected)),
        "events": _event_summary(selected),
        "risks": _risk_snapshot(report_payload),
        "trend_detection": trend_detection or {},
    }


def _parse_day(value: object) -> datetime | None:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d")
    except (TypeError, ValueError):
        return None


def _history_before(history: list[dict], current_day: str) -> list[dict]:
    return [
        item for item in history
        if str(item.get("run_date_london") or "") < current_day
    ]


def _sum_counter_from_history(history: list[dict], field: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for item in history:
        values = item.get(field) or {}
        if not isinstance(values, dict):
            continue
        for key, count in values.items():
            counts[str(key)] += int(count or 0)
    return counts


def _window_history(history: list[dict], current_day: str, days: int) -> list[dict]:
    current = _parse_day(current_day)
    if current is None:
        return []
    start = current - timedelta(days=days)
    out: list[dict] = []
    for item in history:
        day = _parse_day(item.get("run_date_london"))
        if day is None:
            continue
        if start <= day < current:
            out.append(item)
    return out


def _growth_rows(
    current: Counter[str],
    previous_history: list[dict],
    field: str,
    *,
    limit: int = 20,
    entity_rows: bool = False,
) -> list[dict[str, object]]:
    previous = _sum_counter_from_history(previous_history, field)
    previous_days = max(1, len(previous_history))
    keys = set(current) | set(previous)
    rows: list[dict[str, object]] = []
    for key in keys:
        current_count = int(current.get(key) or 0)
        previous_total = int(previous.get(key) or 0)
        previous_avg = previous_total / previous_days
        delta = current_count - previous_avg
        ratio = None if previous_avg == 0 else current_count / previous_avg
        row: dict[str, object] = {
            "key": key,
            "current_count": current_count,
            "previous_window_total": previous_total,
            "previous_window_avg": round(previous_avg, 3),
            "delta_vs_avg": round(delta, 3),
            "ratio_vs_avg": round(ratio, 3) if ratio is not None else None,
            "is_new": current_count > 0 and previous_total == 0,
        }
        if entity_rows:
            entity_type, name = _split_entity_key(key)
            row["entity_type"] = entity_type
            row["name"] = name
        rows.append(row)
    rows.sort(key=lambda row: (-float(row["delta_vs_avg"]), -int(row["current_count"]), str(row["key"])))
    return rows[:limit]


def build_trend_detection(
    state_dir: Path,
    *,
    run_date_london: str,
    candidates: list[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None = None,
) -> dict[str, object]:
    history = _history_before(load_city_history(state_dir), run_date_london)
    selected, basis = _selected_candidates(candidates, rendered_fingerprints)
    current_topics = _count_topic_tags(selected)
    current_entities = _count_entities(selected)
    windows: dict[str, object] = {}
    for days in TREND_WINDOWS_DAYS:
        previous = _window_history(history, run_date_london, days)
        windows[f"{days}d"] = {
            "previous_days_available": len(previous),
            "topics": _growth_rows(current_topics, previous, "topics"),
            "entities": _growth_rows(current_entities, previous, "entities", entity_rows=True),
        }
    return {
        "schema_version": CITY_TRENDS_SCHEMA_VERSION,
        "basis": basis,
        "run_date_london": run_date_london,
        "windows": windows,
        "rising_topics": windows["7d"]["topics"][:8],
        "rising_entities": windows["7d"]["entities"][:8],
    }


def append_city_intelligence_history(
    state_dir: Path,
    *,
    report_payload: dict,
    candidates: list[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None = None,
    trend_detection: dict | None = None,
) -> Path:
    state_dir.mkdir(parents=True, exist_ok=True)
    run_date = str(report_payload.get("run_date_london") or now_london().strftime("%Y-%m-%d"))
    snapshot = build_daily_city_snapshot(
        run_date_london=run_date,
        report_payload=report_payload,
        candidates=candidates,
        rendered_fingerprints=rendered_fingerprints,
        trend_detection=trend_detection,
    )
    history = [
        item for item in load_city_history(state_dir)
        if str(item.get("run_date_london") or "") != run_date
    ]
    history.append(snapshot)
    history.sort(key=lambda item: str(item.get("run_date_london") or ""))
    history = history[-60:]
    path = _history_path(state_dir)
    write_json(path, history)
    return path


def _top_counter_rows(counter: Counter[str], limit: int = 12) -> list[dict[str, object]]:
    return [
        {"name": name, "count": count}
        for name, count in counter.most_common(limit)
    ]


def _entity_rows(counter: Counter[str], limit: int = 12) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for key, count in counter.most_common(limit):
        entity_type, name = _split_entity_key(key)
        rows.append({"entity_type": entity_type, "name": name, "count": count})
    return rows


def build_weekly_city_rollup(
    state_dir: Path,
    *,
    end_date_london: str | None = None,
) -> dict[str, object]:
    history = load_city_history(state_dir)
    if not history:
        rollup = {
            "schema_version": CITY_TRENDS_SCHEMA_VERSION,
            "generated_at_london": now_london().isoformat(),
            "period": {"start": None, "end": end_date_london, "days": 0},
            "errors": ["city_intelligence_history.json is empty"],
        }
        write_json(_rollup_path(state_dir), rollup)
        return rollup

    end_date = end_date_london or str(history[-1].get("run_date_london") or now_london().strftime("%Y-%m-%d"))
    end_dt = _parse_day(end_date)
    if end_dt is None:
        end_dt = _parse_day(str(history[-1].get("run_date_london") or "")) or now_london().replace(tzinfo=None)
        end_date = end_dt.strftime("%Y-%m-%d")
    start_dt = end_dt - timedelta(days=6)
    selected = [
        item for item in history
        if (day := _parse_day(item.get("run_date_london"))) is not None and start_dt <= day <= end_dt
    ]
    topic_counts = _sum_counter_from_history(selected, "topics")
    entity_counts = _sum_counter_from_history(selected, "entities")
    borough_counts = _sum_counter_from_history(selected, "boroughs")

    event_count = 0
    dated_event_count = 0
    undated_event_count = 0
    events_by_borough: Counter[str] = Counter()
    events_by_venue: Counter[str] = Counter()
    events_by_date: Counter[str] = Counter()
    risk_counts: Counter[str] = Counter()
    borough_skew_days: list[dict[str, object]] = []
    for item in selected:
        events = item.get("events") or {}
        if isinstance(events, dict):
            event_count += int(events.get("event_count") or 0)
            dated_event_count += int(events.get("dated_event_count") or 0)
            undated_event_count += int(events.get("undated_event_count") or 0)
            for key, count in (events.get("by_borough") or {}).items():
                events_by_borough[str(key)] += int(count or 0)
            for key, count in (events.get("by_venue") or {}).items():
                events_by_venue[str(key)] += int(count or 0)
            for key, count in (events.get("by_date") or {}).items():
                events_by_date[str(key)] += int(count or 0)
        risks = item.get("risks") or {}
        if isinstance(risks, dict):
            if str(risks.get("release_decision") or "") != "pass":
                risk_counts["failed_release_days"] += 1
            if str(risks.get("digest_risk_level") or "") not in {"", "healthy"}:
                risk_counts[f"digest_{risks.get('digest_risk_level')}"] += 1
            risk_counts["warnings"] += int(risks.get("warning_count") or 0)
            risk_counts["failed_sources"] += int(risks.get("source_failed_count") or 0)
            risk_counts["zero_yield_sources"] += int(risks.get("zero_yield_sources") or 0)
            risk_counts["lost_leads"] += int(risks.get("lost_leads") or 0)
            risk_counts["section_underflow"] += int(risks.get("section_underflow") or 0)
            skew_flags = risks.get("borough_skew_flags") or []
            if skew_flags:
                borough_skew_days.append(
                    {
                        "run_date_london": item.get("run_date_london"),
                        "flags": list(skew_flags)[:4],
                    }
                )

    latest_trends = (selected[-1].get("trend_detection") if selected else {}) or {}
    rollup = {
        "schema_version": CITY_TRENDS_SCHEMA_VERSION,
        "generated_at_london": now_london().isoformat(),
        "period": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_date,
            "days": len(selected),
        },
        "basis": sorted({str(item.get("basis") or "") for item in selected if item.get("basis")}),
        "totals": {
            "runs": len(selected),
            "items": sum(int(item.get("item_count") or 0) for item in selected),
        },
        "topics": _top_counter_rows(topic_counts, 15),
        "entities": _entity_rows(entity_counts, 15),
        "boroughs": [
            {"name": borough, "count": int(borough_counts.get(borough) or 0)}
            for borough in GM_BOROUGHS
        ],
        "events": {
            "event_count": event_count,
            "dated_event_count": dated_event_count,
            "undated_event_count": undated_event_count,
            "by_borough": _top_counter_rows(events_by_borough, 10),
            "by_venue": _top_counter_rows(events_by_venue, 10),
            "by_date": _top_counter_rows(events_by_date, 10),
        },
        "risks": {
            "counts": dict(risk_counts),
            "borough_skew_days": borough_skew_days,
        },
        "trends": {
            "rising_topics": latest_trends.get("rising_topics") or [],
            "rising_entities": latest_trends.get("rising_entities") or [],
        },
        "errors": [],
    }
    write_json(_rollup_path(state_dir), rollup)
    return rollup


def weekly_city_rollup_text(rollup: dict) -> str:
    period = rollup.get("period") or {}
    lines = [
        f"🏙️ Weekly city rollup — {period.get('start') or '?'} → {period.get('end') or '?'}",
        f"Runs: {(rollup.get('totals') or {}).get('runs', 0)}  •  Items: {(rollup.get('totals') or {}).get('items', 0)}",
    ]
    errors = rollup.get("errors") or []
    if errors:
        lines.append("Problems:")
        lines.extend(f"• {err}" for err in errors)
        return "\n".join(lines)

    topics = rollup.get("topics") or []
    if topics:
        lines.append("\nТемы недели:")
        for row in topics[:8]:
            lines.append(f"• {row.get('name')}: {row.get('count')}")

    boroughs = [
        row for row in (rollup.get("boroughs") or [])
        if int(row.get("count") or 0) > 0
    ]
    if boroughs:
        boroughs.sort(key=lambda row: (-int(row.get("count") or 0), str(row.get("name") or "")))
        lines.append("\nBorough coverage:")
        lines.append(", ".join(f"{row.get('name')}: {row.get('count')}" for row in boroughs[:10]))

    events = rollup.get("events") or {}
    lines.append(
        f"\nСобытия: {events.get('event_count', 0)} "
        f"(dated: {events.get('dated_event_count', 0)}, без даты: {events.get('undated_event_count', 0)})"
    )
    venues = events.get("by_venue") or []
    if venues:
        lines.append("Top venues: " + ", ".join(f"{row.get('name')} ({row.get('count')})" for row in venues[:5]))

    rising_topics = (rollup.get("trends") or {}).get("rising_topics") or []
    if rising_topics:
        lines.append("\nРастущие темы:")
        for row in rising_topics[:5]:
            marker = "new" if row.get("is_new") else f"+{row.get('delta_vs_avg')}"
            lines.append(f"• {row.get('key')}: {row.get('current_count')} ({marker})")

    risks = rollup.get("risks") or {}
    counts = risks.get("counts") or {}
    lines.append("\nРиски:")
    lines.append(
        "• warnings: "
        f"{counts.get('warnings', 0)}, failed sources: {counts.get('failed_sources', 0)}, "
        f"lost leads: {counts.get('lost_leads', 0)}, underflows: {counts.get('section_underflow', 0)}"
    )
    skew_days = risks.get("borough_skew_days") or []
    if skew_days:
        lines.append(f"• borough skew days: {len(skew_days)}")
    return "\n".join(lines)
