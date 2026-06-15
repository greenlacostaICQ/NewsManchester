"""Profile-aware matching for free professional events.

This module is intentionally deterministic. It does not decide prose style;
it decides whether a business/tech/university event is worth showing to the
owner before the writer turns it into a Russian card.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
from typing import Any


PROFILE_ENV_JSON = "BUSINESS_EVENT_PROFILE_JSON"
PROFILE_ENV_PATH = "BUSINESS_EVENT_PROFILE_PATH"
MATCH_MODEL_VERSION = "professional_event_match_v1"

HIGH_VALUE_TOPICS = (
    "ai", "agentic ai", "artificial intelligence", "machine learning",
    "digital transformation", "transformation leadership", "fintech",
    "banking", "payments", "open banking", "saas", "product",
    "growth", "retention", "consumer app", "enterprise technology",
    "cloud", "data", "analytics", "crm", "automation", "api",
    "startup", "scaleup", "funding", "investor", "pitch",
    "university-industry", "industry partnership", "innovation",
    "board", "advisory", "fractional",
)
ENGLISH_PRACTICE_TOPICS = (
    "networking", "meetup", "workshop", "roundtable", "breakfast",
    "lunch", "founder", "startup", "business", "innovation",
    "community", "seminar", "skills", "training", "masterclass",
)
MAJOR_EVENT_TOPICS = (
    "conference", "expo", "summit", "festival", "showcase",
    "trade show", "delegate", "keynote", "multi-track", "exhibition",
    "dtx", "ucx", "manchester central",
)
STRONG_HIGH_VALUE_TOPICS = (
    "ai", "agentic ai", "artificial intelligence", "machine learning",
    "digital transformation", "fintech", "banking", "payments",
    "open banking", "saas", "enterprise technology", "cloud", "data",
    "funding", "investor", "university-industry", "industry partnership",
    "board", "advisory", "fractional",
)
FREE_ACCESS_PATTERNS = (
    r"\bfree\s+(?:event|ticket|entry|admission|delegate\s+pass|to\s+attend)\b",
    r"\bfree\b",
    r"\bfree\s+general\s+admission\b",
    r"\bgeneral\s+admission\b",
    r"\bcomplimentary\b",
    r"\bno\s+cost\b",
    r"\bfree\s+for\s+(?:business|bank|banks|end\s+users?|enterprise|"
    r"eligible|delegates?|members?|representatives?)\b",
    r"\bfree\s+to\s+(?:business|eligible|attend|members?|delegates?)\b",
)
PAID_ONLY_PATTERNS = (
    r"\bfrom\s+£\s?\d",
    r"\btickets?\s+(?:from|cost|priced)\s+£\s?\d",
    r"\bpaid\s+(?:event|ticket)\b",
    r"\bnon-member\s+price\b",
)
SOLD_OUT_PATTERNS = (
    r"\bsold\s*out\b", r"\bfully\s*booked\b", r"\bno\s+(?:places|spaces|tickets)\s+left\b",
)
LOW_FIT_PATTERNS = (
    r"\bstudent[s-]?only\b",
    r"\bundergraduate\b",
    r"\bpure\s+sales\b",
    r"\bvendor\s+demo\b",
)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_business_event_profile(project_root: Path | None = None) -> dict[str, Any]:
    raw = os.getenv(PROFILE_ENV_JSON, "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}

    path_raw = os.getenv(PROFILE_ENV_PATH, "").strip()
    paths = []
    if path_raw:
        paths.append(Path(path_raw).expanduser())
    root = project_root or _project_root()
    paths.append(root / "data" / "private" / "business_event_profile.json")
    for path in paths:
        try:
            if path.exists():
                parsed = json.loads(path.read_text(encoding="utf-8"))
                return parsed if isinstance(parsed, dict) else {}
        except (OSError, json.JSONDecodeError):
            continue
    return {}


def _blob(candidate: dict[str, Any]) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    fields = [
        candidate.get("title"),
        candidate.get("summary"),
        candidate.get("lead"),
        candidate.get("evidence_text"),
        candidate.get("source_label"),
        candidate.get("source_url"),
        event.get("event_name"),
        event.get("venue"),
        event.get("price"),
    ]
    return " ".join(str(v or "") for v in fields).lower()


def _count_terms(blob: str, terms: tuple[str, ...] | list[str]) -> int:
    return sum(1 for term in terms if str(term or "").lower() in blob)


def _free_access(blob: str, source_label: str) -> tuple[str, str]:
    if any(re.search(pattern, blob, re.IGNORECASE) for pattern in SOLD_OUT_PATTERNS):
        return "sold_out", "мест нет"
    has_free = any(re.search(pattern, blob, re.IGNORECASE) for pattern in FREE_ACCESS_PATTERNS)
    has_paid = any(re.search(pattern, blob, re.IGNORECASE) for pattern in PAID_ONLY_PATTERNS)
    if has_free and re.search(r"\bfree\s+for\b|\bfree\s+to\b", blob, re.IGNORECASE):
        return "eligible_free", "бесплатно для подходящих представителей бизнеса"
    if has_free:
        return "free", "бесплатная регистрация"
    if "business growth hub" in source_label.lower():
        return "likely_free", "обычно бесплатное business-support событие; цену нужно сверить"
    if has_paid:
        return "paid", "платное событие"
    return "unknown", "бесплатный доступ не подтверждён"


def _profile_terms(profile: dict[str, Any], key: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
    matching = profile.get("event_matching") if isinstance(profile.get("event_matching"), dict) else {}
    value = matching.get(key)
    if isinstance(value, list):
        terms = tuple(str(item).lower() for item in value if str(item or "").strip())
        return tuple(dict.fromkeys((*fallback, *terms)))
    return fallback


def score_professional_event(candidate: dict[str, Any], project_root: Path | None = None) -> dict[str, Any]:
    profile = load_business_event_profile(project_root)
    blob = _blob(candidate)
    source_label = str(candidate.get("source_label") or "")
    access_status, access_reason = _free_access(blob, source_label)
    high_terms = _profile_terms(profile, "high_fit_topics", HIGH_VALUE_TOPICS)
    medium_terms = _profile_terms(profile, "medium_fit_topics", ENGLISH_PRACTICE_TOPICS)
    english_terms = _profile_terms(profile, "english_practice_good_fit", ENGLISH_PRACTICE_TOPICS)
    major_terms = _profile_terms(profile, "major_conference_or_expo_signals", MAJOR_EVENT_TOPICS)

    high_hits = _count_terms(blob, high_terms)
    strong_high_hits = _count_terms(blob, STRONG_HIGH_VALUE_TOPICS)
    medium_hits = _count_terms(blob, medium_terms)
    english_hits = _count_terms(blob, english_terms)
    major_hits = _count_terms(blob, major_terms)
    low_fit = any(re.search(pattern, blob, re.IGNORECASE) for pattern in LOW_FIT_PATTERNS)
    in_person = not re.search(r"\bwebinar|online\s+only|virtual\b", blob, re.IGNORECASE)

    score = 0
    if access_status in {"free", "eligible_free", "likely_free"}:
        score += 35
    elif access_status == "unknown":
        score -= 15
    else:
        score -= 80
    score += min(high_hits, 5) * 11
    score += min(medium_hits, 4) * 5
    score += min(english_hits, 4) * 7
    score += min(major_hits, 4) * 12
    if in_person:
        score += 8
    if low_fit:
        score -= 35

    major = major_hits >= 1
    if major and score >= 65:
        event_level = "major_conference_or_expo"
    elif high_hits >= 2 and strong_high_hits >= 1 and score >= 65:
        event_level = "high_value_professional"
    elif english_hits >= 1 and in_person and score >= 55:
        event_level = "english_practice_networking"
    else:
        event_level = "reject"

    publish = event_level != "reject" and access_status in {"free", "eligible_free", "likely_free"} and score >= 55
    if access_status == "likely_free" and score < 75:
        publish = False

    gets: list[str] = []
    if high_hits:
        gets.append("профессиональный сигнал по AI/product/fintech/digital transformation")
    if english_hits and in_person:
        gets.append("спокойная практика профессионального английского")
    if major:
        gets.append("плотный нетворк большой конференции или экспо")
    if "university" in blob or "university-industry" in blob:
        gets.append("связь университетов и бизнеса")
    if not gets:
        gets.append("локальный business networking")

    return {
        "model": MATCH_MODEL_VERSION,
        "publish": publish,
        "fit_score": max(0, min(100, int(score))),
        "event_level": event_level,
        "major_conference_or_expo": major,
        "free_access_status": access_status,
        "free_access_reason": access_reason,
        "why_this_fits_aleksei": "; ".join(gets[:2]),
        "what_he_gets_from_it": gets[:4],
        "english_practice_value": bool(english_hits and in_person),
        "recommended_action": "register" if publish and score >= 70 else ("consider" if publish else "skip"),
        "signals": {
            "high_value_hits": high_hits,
            "strong_high_value_hits": strong_high_hits,
            "medium_hits": medium_hits,
            "english_practice_hits": english_hits,
            "major_event_hits": major_hits,
            "in_person": in_person,
        },
    }


def apply_professional_event_match(candidate: dict[str, Any], project_root: Path | None = None) -> dict[str, Any]:
    if str(candidate.get("category") or "") != "professional_events":
        return candidate
    match = score_professional_event(candidate, project_root)
    candidate["professional_event_match"] = match
    candidate["reader_action_type"] = "book_or_buy" if match.get("recommended_action") == "register" else "plan_ahead"
    candidate["english_editorial_score"] = max(float(candidate.get("english_editorial_score") or 0), float(match.get("fit_score") or 0))
    if not match.get("publish"):
        candidate["include"] = False
        candidate["reason"] = (
            str(candidate.get("reason") or "").rstrip()
            + f" | Professional event match: {match.get('event_level')} / {match.get('free_access_status')} / score {match.get('fit_score')}."
        ).strip()
    return candidate
