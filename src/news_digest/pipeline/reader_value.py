"""Manual reader-value validation for candidate scoring.

The labels in data/validation/reader_value_labels.json are deliberately
hand-maintained. They are not training data for an LLM; they are a small
editorial benchmark that tells us whether the reader-value score is
roughly aligned with human judgement.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re

from news_digest.pipeline.common import read_json, write_json

LABELS_PATH = Path("data") / "validation" / "reader_value_labels.json"
VALID_LABELS = ("useful", "neutral", "should_not_include")
MIN_LABELS = 30
MAX_LABELS = 50

_BASE_CATEGORY_SCORE = {
    "weather": 70,
    "transport": 70,
    "media_layer": 35,
    "public_services": 45,
    "council": 38,
    "culture_weekly": 35,
    "venues_tickets": 35,
    "food_openings": 42,
    "tech_business": 30,
    "football": 35,
    "russian_speaking_events": 35,
}

_BLOCK_BONUS = {
    "weather": 20,
    "transport": 20,
    "lead_story": 25,
    "today_focus": 20,
    "last_24h": 12,
    "weekend_activities": 14,
    "next_7_days": 5,
    "ticket_radar": 6,
    "openings": 10,
    "city_watch": 0,
    "tech_business": 0,
    "outside_gm_tickets": -15,
    "future_announcements": -10,
}

_HIGH_VALUE_TITLE_RE = re.compile(
    r"\b("
    r"police|murder|stab|stabbing|injured|dead|death|jailed|assault|"
    r"election|by-election|burnham|mayor|council|renters|housing|affordable|"
    r"billion|masterplan|projects|die|fight over|hopwood|"
    r"market opens|opens|re-open|closure|disruption|diversion|"
    r"school|nhs|mental health|fine|fly-tipping"
    r")\b",
    re.IGNORECASE,
)

_LOW_VALUE_TITLE_RE = re.compile(
    r"\b("
    r"quiz|rich list|job:|jobs|watch the world cup|best places|"
    r"invest in manchester|roundtable|award win|training and support|"
    r"food halls and markets|photo id|remember you will need|"
    r"contact theatre|palace theatre tour"
    r")\b",
    re.IGNORECASE,
)


def load_reader_value_labels(project_root: Path) -> dict:
    return read_json(project_root / LABELS_PATH, {"schema_version": 1, "labels": []})


def validate_reader_value_labels(payload: dict) -> list[str]:
    labels = payload.get("labels") if isinstance(payload, dict) else None
    errors: list[str] = []
    if not isinstance(labels, list):
        return ["labels must be a list"]
    if len(labels) < MIN_LABELS or len(labels) > MAX_LABELS:
        errors.append(f"labels must contain {MIN_LABELS}-{MAX_LABELS} items; got {len(labels)}")

    seen: set[str] = set()
    counts: Counter[str] = Counter()
    required = {"fingerprint", "title", "source_label", "category", "primary_block", "label", "rationale"}
    for index, item in enumerate(labels, start=1):
        if not isinstance(item, dict):
            errors.append(f"label #{index} is not an object")
            continue
        missing = sorted(field for field in required if not str(item.get(field) or "").strip())
        if missing:
            errors.append(f"label #{index} missing required field(s): {', '.join(missing)}")
        fingerprint = str(item.get("fingerprint") or "")
        if fingerprint in seen:
            errors.append(f"duplicate fingerprint: {fingerprint}")
        seen.add(fingerprint)
        label = str(item.get("label") or "")
        if label not in VALID_LABELS:
            errors.append(f"label #{index} has invalid label {label!r}")
        counts[label] += 1

    for label in VALID_LABELS:
        if counts[label] < 5:
            errors.append(f"need at least 5 {label!r} labels; got {counts[label]}")
    return errors


def reader_value_score(item: dict) -> int:
    category = str(item.get("category") or "")
    block = str(item.get("primary_block") or "")
    change_type = str(item.get("change_type") or "")
    title = str(item.get("title") or "")
    reject_reason = str(item.get("reject_reason") or "")
    if not reject_reason:
        # Live candidates carry the drop justification in `reason`; the
        # labels benchmark uses `reject_reason`. Treat both interchangeably
        # so the score is meaningful on in-flight candidates too.
        reject_reason = str(item.get("reason") or "")
    text = f"{title} {reject_reason}"

    score = _BASE_CATEGORY_SCORE.get(category, 35)
    score += _BLOCK_BONUS.get(block, 0)

    # Labels benchmark snapshots `included` (past-tense, what shipped);
    # live candidates use `include` (current decision). Honor both.
    included = item.get("included")
    if included is None:
        included = item.get("include")
    if included:
        score += 8
    else:
        score -= 5

    if change_type in {"same_story_new_facts", "new_phase", "follow_up"}:
        score += 12
    elif change_type == "new_story":
        score += 5
    elif change_type in {"no_change", "same_story_rehash"}:
        score -= 28
    elif change_type == "reminder":
        score -= 8

    if _HIGH_VALUE_TITLE_RE.search(title):
        score += 14
    if _LOW_VALUE_TITLE_RE.search(title):
        score -= 18

    lowered_reason = reject_reason.lower()
    if "same story kept from stronger source" in lowered_reason:
        score = min(score, 35)
    if "duplicate" in lowered_reason or "дублик" in lowered_reason:
        score -= 18
    if "no_change" in change_type or "без новых фактов" in lowered_reason or "повтор сюжета" in lowered_reason:
        score -= 22
    if "evergreen" in lowered_reason or "листинг" in lowered_reason:
        score -= 24
    if "устарев" in lowered_reason or "неактуальна" in lowered_reason:
        score -= 24
    if "ваканс" in lowered_reason or "job advert" in lowered_reason:
        score -= 24
    if "pr" in lowered_reason or "чистый pr" in lowered_reason or "ребрендинг" in lowered_reason:
        score -= 24
    if "outside current weekend" in lowered_reason or "expired event" in lowered_reason:
        score -= 18
    if "не относится к greater manchester" in lowered_reason or "не в greater manchester" in lowered_reason:
        score -= 35
    if "missing draft_line" in lowered_reason:
        score -= 8
    if "pending dedupe review" in lowered_reason:
        score -= 10
    if "no concrete upcoming date" in lowered_reason or "has no concrete upcoming date" in lowered_reason:
        score -= 22

    if re.search(r"\b(london|liverpool)\b", text, flags=re.IGNORECASE) and block == "outside_gm_tickets":
        score -= 20

    return max(0, min(100, int(score)))


def predicted_label(score: int) -> str:
    if score >= 75:
        return "useful"
    if score >= 38:
        return "neutral"
    return "should_not_include"


def attach_reader_value(candidate: dict) -> dict:
    """Stamp reader_value_score + reader_value_label onto a live candidate.

    Called by validate_candidates and curator_pass so the score is
    visible in candidates.json (and downstream stages can sort by it
    without recomputing).
    """

    if not isinstance(candidate, dict):
        return candidate
    score = reader_value_score(candidate)
    candidate["reader_value_score"] = score
    candidate["reader_value_label"] = predicted_label(score)
    return candidate


def evaluate_reader_value_labels(project_root: Path) -> dict:
    payload = load_reader_value_labels(project_root)
    errors = validate_reader_value_labels(payload)
    labels = payload.get("labels") if isinstance(payload, dict) else []
    results: list[dict] = []
    confusion: dict[str, Counter[str]] = {label: Counter() for label in VALID_LABELS}
    for item in labels if isinstance(labels, list) else []:
        if not isinstance(item, dict):
            continue
        score = reader_value_score(item)
        predicted = predicted_label(score)
        actual = str(item.get("label") or "")
        if actual in confusion:
            confusion[actual][predicted] += 1
        results.append(
            {
                "fingerprint": item.get("fingerprint"),
                "title": item.get("title"),
                "label": actual,
                "predicted_label": predicted,
                "reader_value_score": score,
                "included": item.get("included"),
                "category": item.get("category"),
                "primary_block": item.get("primary_block"),
            }
        )

    total = len(results)
    correct = sum(1 for r in results if r["label"] == r["predicted_label"])
    dangerous_false_positive = sum(
        1
        for r in results
        if r["label"] == "should_not_include" and r["predicted_label"] in {"useful", "neutral"}
    )
    useful_recall_denominator = sum(1 for r in results if r["label"] == "useful")
    useful_recall_hits = sum(
        1 for r in results if r["label"] == "useful" and r["predicted_label"] == "useful"
    )
    summary = {
        "label_count": total,
        "accuracy": round(correct / total, 3) if total else 0.0,
        "useful_recall": round(useful_recall_hits / useful_recall_denominator, 3)
        if useful_recall_denominator
        else 0.0,
        "dangerous_false_positive_count": dangerous_false_positive,
        "label_counts": dict(Counter(str(r["label"]) for r in results)),
        "confusion": {label: dict(confusion[label]) for label in VALID_LABELS},
    }
    return {
        "schema_version": 1,
        "errors": errors,
        "summary": summary,
        "results": results,
    }


def write_reader_value_validation_report(project_root: Path) -> Path:
    report = evaluate_reader_value_labels(project_root)
    out = project_root / "data" / "state" / "reader_value_validation_report.json"
    write_json(out, report)
    return out
