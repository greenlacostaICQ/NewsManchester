from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re

from news_digest.pipeline.common import (
    canonical_url_identity,
    fingerprint_for_candidate,
    normalize_title,
    now_london,
    today_london,
)
from news_digest.pipeline.reject_reasons import reject_reasons


DAILY_INDEX_FILENAME = "daily_index.jsonl"
DAILY_INDEX_LOOKBACK_DAYS = 3

_BOROUGH_TERMS = {
    "manchester": ("manchester", "city centre", "piccadilly", "deansgate", "ancoats", "northern quarter", "oxford road"),
    "salford": ("salford",),
    "trafford": ("trafford", "stretford", "altrincham"),
    "stockport": ("stockport",),
    "tameside": ("tameside", "ashton"),
    "oldham": ("oldham",),
    "rochdale": ("rochdale",),
    "bury": ("bury",),
    "bolton": ("bolton",),
    "wigan": ("wigan",),
}
_TITLE_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "after",
    "into",
    "this",
    "that",
    "you",
    "your",
    "manchester",
    "greater",
}


def daily_index_path(state_dir: Path) -> Path:
    return state_dir / DAILY_INDEX_FILENAME


def ensure_daily_index(state_dir: Path) -> Path:
    path = daily_index_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")
    return path


def _candidate_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text", "source_url", "source_label")
    )


def borough_for_candidate(candidate: dict) -> str:
    blob = _candidate_blob(candidate).lower()
    found = [
        borough
        for borough, terms in _BOROUGH_TERMS.items()
        if any(re.search(rf"\b{re.escape(term)}\b", blob) for term in terms)
    ]
    return found[0] if found else ""


def source_family(source_label: object, source_url: object = "") -> str:
    label = str(source_label or "").strip().lower()
    if label:
        label = re.sub(r"\b(?:manchester|greater|gm|news|live|official)\b", " ", label)
        label = re.sub(r"[^a-z0-9]+", " ", label)
        return re.sub(r"\s+", " ", label).strip() or str(source_label or "").strip().lower()
    host_path = canonical_url_identity(str(source_url or ""))
    host = host_path.split("/", 1)[0]
    return host


def entity_tokens(value: object) -> list[str]:
    text = str(value or "")
    tokens = {
        token.lower()
        for token in re.findall(r"\b(?:[A-Z][a-z]{2,}|[A-Z]{2,}|\d{2,})\b", text)
        if token.lower() not in _TITLE_STOPWORDS
    }
    return sorted(tokens)[:12]


def title_tokens(value: object) -> set[str]:
    words = re.findall(r"[a-zA-Zа-яёА-ЯЁ][a-zA-Zа-яёА-ЯЁ'-]*", str(value or "").lower())
    return {word for word in words if len(word) >= 3 and word not in _TITLE_STOPWORDS}


def _jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def daily_index_record(candidate: dict, *, pipeline_run_id: str = "") -> dict[str, object]:
    fingerprint = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
    reasons = reject_reasons(candidate)
    return {
        "run_at_london": now_london().isoformat(),
        "run_date_london": today_london(),
        "pipeline_run_id": pipeline_run_id,
        "title": candidate.get("title"),
        "url": candidate.get("source_url"),
        "canonical_url": canonical_url_identity(str(candidate.get("source_url") or "")),
        "fingerprint": fingerprint,
        "source": candidate.get("source_label"),
        "source_family": source_family(candidate.get("source_label"), candidate.get("source_url")),
        "category": candidate.get("category"),
        "primary_block": candidate.get("primary_block"),
        "borough": borough_for_candidate(candidate),
        "entities": entity_tokens(candidate.get("title")),
        "normalized_title": normalize_title(str(candidate.get("title") or "")),
        "included": bool(candidate.get("include")),
        "reject_reason": reasons[0] if reasons else "",
        "reject_reasons": reasons,
        "change_type": str(candidate.get("change_type") or "new_story"),
    }


def append_daily_index(state_dir: Path, candidates: list[dict], *, pipeline_run_id: str = "") -> dict[str, object]:
    path = ensure_daily_index(state_dir)
    records = [
        daily_index_record(candidate, pipeline_run_id=pipeline_run_id)
        for candidate in candidates
        if isinstance(candidate, dict)
    ]
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    counts = Counter(str(record.get("change_type") or "new_story") for record in records)
    return {
        "path": str(path),
        "appended_records": len(records),
        "change_type_counts": dict(sorted(counts.items())),
    }


def _parse_day(value: object) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def load_recent_daily_index(state_dir: Path, *, lookback_days: int = DAILY_INDEX_LOOKBACK_DAYS) -> list[dict]:
    path = ensure_daily_index(state_dir)
    today = now_london().date()
    cutoff = today - timedelta(days=lookback_days)
    entries: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict):
            continue
        day = _parse_day(entry.get("run_date_london"))
        if day is None or day >= today or day < cutoff:
            continue
        entries.append(entry)
    return entries


def compare_candidate_to_daily_index(candidate: dict, entries: list[dict], *, limit: int = 3) -> list[dict]:
    fingerprint = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
    canonical = canonical_url_identity(str(candidate.get("source_url") or ""))
    normalized = normalize_title(str(candidate.get("title") or ""))
    family = source_family(candidate.get("source_label"), candidate.get("source_url"))
    borough = borough_for_candidate(candidate)
    tokens = title_tokens(candidate.get("title"))
    entities = set(entity_tokens(candidate.get("title")))

    matches: list[dict] = []
    for entry in entries:
        reasons: list[str] = []
        score = 0.0
        if fingerprint and fingerprint == str(entry.get("fingerprint") or ""):
            reasons.append("fingerprint")
            score = max(score, 1.0)
        if canonical and canonical == str(entry.get("canonical_url") or ""):
            reasons.append("canonical_url")
            score = max(score, 0.98)
        if normalized and normalized == str(entry.get("normalized_title") or ""):
            reasons.append("normalized_title")
            score = max(score, 0.95)

        previous_tokens = title_tokens(entry.get("title"))
        overlap = _jaccard(tokens, previous_tokens)
        same_family = family and family == str(entry.get("source_family") or "")
        same_borough = bool(borough and borough == str(entry.get("borough") or ""))
        previous_entities = set(entry.get("entities") or [])
        entity_overlap = sorted(entities & previous_entities)

        if same_family and same_borough and overlap >= 0.5:
            reasons.append("source_family_title_borough")
            score = max(score, overlap)
        if same_borough and len(entity_overlap) >= 2 and overlap >= 0.25:
            reasons.append("entities_borough")
            score = max(score, 0.65 + min(overlap, 0.3))

        if not reasons:
            continue
        matches.append(
            {
                "match_type": reasons[0],
                "match_reasons": reasons,
                "score": round(score, 3),
                "run_date_london": entry.get("run_date_london"),
                "fingerprint": entry.get("fingerprint"),
                "title": entry.get("title"),
                "source": entry.get("source"),
                "borough": entry.get("borough"),
                "included": bool(entry.get("included")),
                "change_type": entry.get("change_type"),
                "title_overlap": round(overlap, 3),
            }
        )
    matches.sort(key=lambda item: float(item.get("score") or 0), reverse=True)
    return matches[:limit]


def change_type_from_daily_matches(candidate: dict, matches: list[dict]) -> str:
    existing = str(candidate.get("change_type") or "").strip()
    if existing:
        return existing
    if not matches:
        return "new_story"
    first = matches[0]
    match_type = str(first.get("match_type") or "")
    if match_type in {"fingerprint", "canonical_url", "normalized_title", "source_family_title_borough"}:
        return "same_story_rehash"
    if match_type == "entities_borough":
        return "same_story_new_facts"
    return "follow_up"


def apply_daily_index_comparison(candidates: list[dict], entries: list[dict]) -> dict[str, object]:
    compared = 0
    matched = 0
    counts: Counter[str] = Counter()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        compared += 1
        matches = compare_candidate_to_daily_index(candidate, entries)
        candidate["daily_index_matches"] = matches
        candidate["change_type"] = change_type_from_daily_matches(candidate, matches)
        counts[str(candidate.get("change_type") or "new_story")] += 1
        if matches:
            matched += 1
            candidate["matched_daily_index_fingerprint"] = matches[0].get("fingerprint")
            candidate["matched_daily_index_date"] = matches[0].get("run_date_london")
    return {
        "lookback_days": DAILY_INDEX_LOOKBACK_DAYS,
        "loaded_snapshot_records": len(entries),
        "compared_candidates": compared,
        "matched_candidates": matched,
        "change_type_counts": dict(sorted(counts.items())),
    }
