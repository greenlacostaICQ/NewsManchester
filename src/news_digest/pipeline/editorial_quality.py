from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
import re
from typing import Iterable

from news_digest.pipeline.common import now_london


RUBRIC_FIELDS = (
    "new",
    "local",
    "specific",
    "useful",
    "actionable",
    "not_pr",
    "not_evergreen",
    "not_english_prose",
)

_GM_TERMS = (
    "greater manchester",
    "gm",
    "manchester",
    "salford",
    "trafford",
    "stockport",
    "tameside",
    "oldham",
    "rochdale",
    "bury",
    "bolton",
    "wigan",
    "altrincham",
    "stretford",
    "ashton",
    "eccles",
)
_LOCAL_SOURCE_HOSTS = (
    "manchester",
    "gmp.police",
    "tfgm",
    "greatermanchester",
    "salford",
    "stockport",
    "trafford",
    "tameside",
    "oldham",
    "rochdale",
    "bolton",
    "bury",
    "wigan",
)
_EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
}
_OPERATIONAL_BLOCKS = {"weather", "transport", "today_focus", "last_24h"}
_HIGH_URGENCY_BLOCKS = {"weather", "transport", "today_focus", "last_24h"}
_NEAR_TERM_EVENT_BLOCKS = {"weekend_activities", "next_7_days", "ticket_radar", "russian_events"}
_IMPACT_CATEGORIES = {"media_layer", "gmp", "public_services", "city_news", "council", "transport"}
_IMPACT_TERMS = (
    "death",
    "died",
    "murder",
    "court",
    "police",
    "fire",
    "crash",
    "closed",
    "closure",
    "disruption",
    "warning",
    "strike",
    "council",
    "gmca",
    "homes",
    "jobs",
    "school",
    "hospital",
    "evacuat",
    "£",
)
_ACTION_VERBS_RU = (
    "проверьте",
    "закладывайте",
    "сверьте",
    "уточните",
    "не откладывайте",
    "убедитесь",
    "следите",
    "держите в планах",
)
_ACTION_TERMS_EN = (
    "check",
    "plan",
    "allow extra",
    "book",
    "tickets",
    "apply",
    "register",
    "avoid",
    "closed",
    "disruption",
    "warning",
)
_MORNING_RELEVANCE_BLOCKS = {"weather", "transport", "today_focus"}
_MORNING_RELEVANCE_TERMS = (
    "today",
    "tonight",
    "this morning",
    "tomorrow morning",
    "deadline",
    "closes today",
    "last day",
    "warning",
    "closure",
    "closed",
    "strike",
    "industrial action",
    "cancel",
    "replacement bus",
    "сегодня",
    "утром",
    "дедлайн",
    "последний день",
    "предупреждение",
    "закрыт",
    "закрытие",
    "забастов",
)
_COMMUTER_SUBJECT_TERMS = (
    "metrolink",
    "tram",
    "trams",
    "bus",
    "buses",
    "rail",
    "train",
    "station",
    "airport",
    "m60",
    "m62",
    "road",
    "route",
    "service",
    "bee network",
    "трамва",
    "автобус",
    "поезд",
    "станци",
    "аэропорт",
    "маршрут",
)
_COMMUTER_DISRUPTION_TERMS = (
    "disruption",
    "delay",
    "delays",
    "cancel",
    "cancelled",
    "closed",
    "closure",
    "strike",
    "replacement bus",
    "works",
    "suspended",
    "diverted",
    "warning",
    "rain",
    "snow",
    "wind",
    "ice",
    "fog",
    "задерж",
    "отмен",
    "закрыт",
    "забастов",
    "ремонт",
    "замещающ",
    "дожд",
    "снег",
    "ветер",
    "туман",
)
_PR_TERMS = (
    "award-winning",
    "named best",
    "best places",
    "sponsored",
    "affiliate",
    "discount",
    "promo",
    "promotion",
    "deal",
    "partnership",
    "appointed",
    "shortlisted",
    "celebrates",
)
_EVERGREEN_TERMS = (
    "things to do",
    "best places",
    "where to",
    "guide to",
    "everything you need to know",
    "top 10",
    "10 best",
    "hidden gems",
    "must-visit",
)
_DETAIL_RE = re.compile(
    r"\b(?:\d{1,2}:\d{2}|£\s*\d|\d+\s*(?:people|homes|jobs|routes|services|days|weeks)|"
    r"\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*|"
    r"20\d{2}[/-]\d{1,2}[/-]\d{1,2})\b",
    re.IGNORECASE,
)
_CYRILLIC_RE = re.compile(r"[а-яё]", re.IGNORECASE)
_LATIN_WORD_RE = re.compile(r"[A-Za-z][A-Za-z'’-]+")


def _candidate_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text", "source_url")
    )


def _published_at(candidate: dict) -> datetime | None:
    raw = str(candidate.get("published_at") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(now_london().tzinfo)
    except ValueError:
        return None


def _has_recent_signal(candidate: dict) -> bool:
    block = str(candidate.get("primary_block") or "")
    if block in _OPERATIONAL_BLOCKS:
        return True
    published = _published_at(candidate)
    if published is None:
        return bool(_DETAIL_RE.search(_candidate_blob(candidate)))
    return published >= now_london() - timedelta(days=7)


def _hours_since_published(candidate: dict) -> float | None:
    published = _published_at(candidate)
    if published is None:
        return None
    return max((now_london() - published).total_seconds() / 3600, 0.0)


def _has_local_signal(candidate: dict) -> bool:
    blob = _candidate_blob(candidate).lower()
    source_url = str(candidate.get("source_url") or "").lower()
    if any(re.search(rf"\b{re.escape(term)}\b", blob) for term in _GM_TERMS):
        return True
    return any(term in source_url for term in _LOCAL_SOURCE_HOSTS)


def _has_specific_signal(candidate: dict) -> bool:
    blob = _candidate_blob(candidate)
    words = re.findall(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё'-]{2,}", blob)
    proper_name = bool(re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", blob))
    return len(words) >= 22 and (proper_name or bool(_DETAIL_RE.search(blob)))


def _has_action_signal(candidate: dict) -> bool:
    practical = str(candidate.get("practical_angle") or "").lower()
    blob = _candidate_blob(candidate).lower()
    block = str(candidate.get("primary_block") or "")
    if any(term in practical for term in _ACTION_VERBS_RU):
        return True
    if block in {"transport", "weather"}:
        return True
    return any(term in blob for term in _ACTION_TERMS_EN)


def _not_pr(candidate: dict) -> bool:
    blob = _candidate_blob(candidate).lower()
    if any(term in blob for term in _PR_TERMS):
        has_hard_fact = bool(_DETAIL_RE.search(blob) or re.search(r"\b(open|opens|closed|closing|launched|created|jobs|investment)\b", blob))
        return has_hard_fact
    return True


def _not_evergreen(candidate: dict) -> bool:
    blob = _candidate_blob(candidate).lower()
    block = str(candidate.get("primary_block") or "")
    if not any(term in blob for term in _EVERGREEN_TERMS):
        return True
    return block in _EVENT_BLOCKS and bool(_DETAIL_RE.search(blob))


def _not_english_prose(candidate: dict) -> bool:
    # Source fields are often English by design. Only judge the generated line
    # once it exists, otherwise the rewrite stage has not had a chance to work.
    line = str(candidate.get("draft_line") or "").strip()
    if not line:
        return True
    if _CYRILLIC_RE.search(line):
        return True
    latin_words = _LATIN_WORD_RE.findall(line)
    return len(latin_words) < 8


def evaluate_editorial_rubric(candidate: dict) -> dict[str, bool]:
    block = str(candidate.get("primary_block") or "")
    rubric = {
        "new": _has_recent_signal(candidate),
        "local": _has_local_signal(candidate),
        "specific": _has_specific_signal(candidate),
        "useful": False,
        "actionable": _has_action_signal(candidate),
        "not_pr": _not_pr(candidate),
        "not_evergreen": _not_evergreen(candidate),
        "not_english_prose": _not_english_prose(candidate),
    }
    rubric["useful"] = rubric["specific"] or rubric["actionable"] or block in _OPERATIONAL_BLOCKS
    return rubric


def _impact_score(candidate: dict) -> int:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    blob = _candidate_blob(candidate).lower()
    score = 0
    if category in _IMPACT_CATEGORIES or block in _HIGH_URGENCY_BLOCKS:
        score += 10
    if any(term in blob for term in _IMPACT_TERMS):
        score += 7
    if re.search(r"\b\d{2,}\s*(?:people|homes|jobs|services|routes|pupils|patients)\b", blob):
        score += 3
    return min(score, 20)


def _morning_relevance_score(candidate: dict) -> int:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    blob = _candidate_blob(candidate).lower()
    score = 0
    if block in _MORNING_RELEVANCE_BLOCKS or category in {"weather", "transport"}:
        score += 7
    if any(term in blob for term in _MORNING_RELEVANCE_TERMS):
        score += 5
    if block in _NEAR_TERM_EVENT_BLOCKS and any(term in blob for term in ("today", "tonight", "сегодня")):
        score += 3
    return min(score, 12)


def _commuter_boost_score(candidate: dict) -> int:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    blob = _candidate_blob(candidate).lower()
    subject_match = block == "transport" or category == "transport" or any(term in blob for term in _COMMUTER_SUBJECT_TERMS)
    disruption_match = any(term in blob for term in _COMMUTER_DISRUPTION_TERMS)
    if subject_match and disruption_match:
        return 10
    if block == "weather" and disruption_match:
        return 5
    return 0


def reader_value_components(candidate: dict) -> dict[str, int]:
    rubric = candidate.get("editorial_rubric")
    if not isinstance(rubric, dict):
        rubric = evaluate_editorial_rubric(candidate)

    block = str(candidate.get("primary_block") or "")
    reasons = {str(reason) for reason in candidate.get("reject_reasons", []) if str(reason)}
    hours_since = _hours_since_published(candidate)

    novelty = 15 if rubric.get("new") else 0
    if hours_since is not None and hours_since <= 36:
        novelty += 5
    novelty = min(novelty, 20)

    local_relevance = 20 if rubric.get("local") else 0
    if not rubric.get("local") and any(term in str(candidate.get("source_url") or "").lower() for term in _LOCAL_SOURCE_HOSTS):
        local_relevance = 10

    specificity = 15 if rubric.get("specific") else (7 if _DETAIL_RE.search(_candidate_blob(candidate)) else 0)

    urgency = 0
    if block in _HIGH_URGENCY_BLOCKS:
        urgency = 15
    elif block in _NEAR_TERM_EVENT_BLOCKS:
        urgency = 10
    elif block == "future_announcements":
        urgency = 4
    if hours_since is not None and hours_since <= 24:
        urgency = min(15, urgency + 3)

    actionability = 15 if rubric.get("actionable") else 0
    impact = _impact_score(candidate)
    morning_relevance = _morning_relevance_score(candidate)
    commuter_boost = _commuter_boost_score(candidate)

    pr_penalty = 25 if not rubric.get("not_pr") or "pr" in reasons else 0
    repeat_penalty = 20 if reasons.intersection({"duplicate", "no_change"}) else 0
    vagueness_penalty = 0
    if not rubric.get("specific"):
        vagueness_penalty += 8
    if not rubric.get("useful"):
        vagueness_penalty += 5
    if not rubric.get("actionable") and block in _NEAR_TERM_EVENT_BLOCKS | _HIGH_URGENCY_BLOCKS:
        vagueness_penalty += 4

    return {
        "novelty": novelty,
        "local_relevance": local_relevance,
        "specificity": specificity,
        "urgency": urgency,
        "actionability": actionability,
        "impact": impact,
        "morning_relevance": morning_relevance,
        "commuter_boost": commuter_boost,
        "pr_penalty": pr_penalty,
        "repeat_penalty": repeat_penalty,
        "vagueness_penalty": min(vagueness_penalty, 17),
    }


def reader_value_score(candidate: dict) -> int:
    components = reader_value_components(candidate)
    raw_score = (
        components["novelty"]
        + components["local_relevance"]
        + components["specificity"]
        + components["urgency"]
        + components["actionability"]
        + components["impact"]
        + components["morning_relevance"]
        + components["commuter_boost"]
        - components["pr_penalty"]
        - components["repeat_penalty"]
        - components["vagueness_penalty"]
    )
    return max(0, min(100, int(raw_score)))


def apply_editorial_quality(candidates: Iterable[dict]) -> None:
    for candidate in candidates:
        if isinstance(candidate, dict):
            candidate["editorial_rubric"] = evaluate_editorial_rubric(candidate)
            candidate["reader_value_components"] = reader_value_components(candidate)
            candidate["reader_value_score"] = reader_value_score(candidate)


def candidate_reader_value(candidate: dict) -> int:
    value = candidate.get("reader_value_score")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return reader_value_score(candidate)


def rubric_red_flags(candidate: dict) -> list[str]:
    rubric = candidate.get("editorial_rubric")
    if not isinstance(rubric, dict):
        rubric = evaluate_editorial_rubric(candidate)
    return [field for field in RUBRIC_FIELDS if rubric.get(field) is False]


def included_rubric_red_flags(candidates: Iterable[dict], *, limit: int = 50) -> list[dict]:
    flagged: list[dict] = []
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        flags = rubric_red_flags(candidate)
        if not flags:
            continue
        flagged.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "category": candidate.get("category"),
                "primary_block": candidate.get("primary_block"),
                "red_flags": flags,
            }
        )
    return flagged[:limit]


def rubric_summary(candidates: Iterable[dict]) -> dict[str, object]:
    total = 0
    included = 0
    included_flagged = 0
    flag_counts: Counter[str] = Counter()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        total += 1
        flags = rubric_red_flags(candidate)
        flag_counts.update(flags)
        if candidate.get("include"):
            included += 1
            if flags:
                included_flagged += 1
    return {
        "candidates": total,
        "included_candidates": included,
        "included_with_red_flags": included_flagged,
        "red_flag_counts": dict(sorted(flag_counts.items())),
    }


def _score_report_item(candidate: dict) -> dict:
    return {
        "fingerprint": candidate.get("fingerprint"),
        "title": candidate.get("title"),
        "category": candidate.get("category"),
        "primary_block": candidate.get("primary_block"),
        "reader_value_score": candidate_reader_value(candidate),
        "reader_value_components": candidate.get("reader_value_components") or reader_value_components(candidate),
    }


def reader_value_report(candidates: Iterable[dict], *, limit: int = 10) -> dict[str, object]:
    included = [candidate for candidate in candidates if isinstance(candidate, dict) and candidate.get("include")]
    scored = sorted(included, key=candidate_reader_value, reverse=True)
    if not scored:
        return {"included_candidates": 0, "average_score": 0, "top": [], "bottom": []}
    average = round(sum(candidate_reader_value(candidate) for candidate in scored) / len(scored), 1)
    bottom = sorted(scored, key=candidate_reader_value)[:limit]
    return {
        "included_candidates": len(scored),
        "average_score": average,
        "top": [_score_report_item(candidate) for candidate in scored[:limit]],
        "bottom": [_score_report_item(candidate) for candidate in bottom],
    }
