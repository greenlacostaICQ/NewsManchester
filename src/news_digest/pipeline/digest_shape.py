from __future__ import annotations

from collections import Counter
import re
from typing import Iterable


CITY_PUBLIC_CATEGORIES = {"media_layer", "gmp", "public_services", "city_news", "council"}
EVENT_CATEGORIES = {"culture_weekly", "venues_tickets", "russian_speaking_events", "food_openings"}
EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "openings",
}
CRIME_TERMS = (
    "police",
    "court",
    "crime",
    "murder",
    "stab",
    "assault",
    "arrest",
    "jail",
    "jailed",
    "sentence",
    "sentenced",
    "fire",
    "crash",
    "collision",
    "death",
    "died",
)
CITY_CENTRE_TERMS = (
    "city centre",
    "city-centre",
    "piccadilly",
    "deansgate",
    "northern quarter",
    "spinningfields",
    "ancoats",
    "market street",
    "oxford road",
    "castlefield",
)
BOROUGH_TERMS = {
    "manchester": ("manchester", "city centre", "piccadilly", "deansgate", "ancoats", "northern quarter"),
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


def _blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text", "draft_line", "source_url")
    ).lower()


def _share(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total, 3)


def _is_crime_or_incident(candidate: dict) -> bool:
    category = str(candidate.get("category") or "")
    if category == "gmp":
        return True
    blob = _blob(candidate)
    return any(term in blob for term in CRIME_TERMS)


def _is_event(candidate: dict) -> bool:
    return (
        str(candidate.get("category") or "") in EVENT_CATEGORIES
        or str(candidate.get("primary_block") or "") in EVENT_BLOCKS
    )


def _is_city_centre(candidate: dict) -> bool:
    blob = _blob(candidate)
    return any(term in blob for term in CITY_CENTRE_TERMS)


def _boroughs(candidate: dict) -> list[str]:
    blob = _blob(candidate)
    found: list[str] = []
    for borough, terms in BOROUGH_TERMS.items():
        if any(re.search(rf"\b{re.escape(term)}\b", blob) for term in terms):
            found.append(borough)
    return found


def rendered_candidates(candidates: Iterable[dict], rendered_fingerprints: Iterable[str]) -> list[dict]:
    rendered = {str(fingerprint).strip() for fingerprint in rendered_fingerprints if str(fingerprint).strip()}
    return [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("include")
        and str(candidate.get("fingerprint") or "").strip() in rendered
    ]


def digest_shape_report(candidates: Iterable[dict], rendered_fingerprints: Iterable[str]) -> dict[str, object]:
    visible_candidates = rendered_candidates(candidates, rendered_fingerprints)
    visible_count = len(visible_candidates)
    category_counts = Counter(str(candidate.get("category") or "unknown") for candidate in visible_candidates)
    block_counts = Counter(str(candidate.get("primary_block") or "unknown") for candidate in visible_candidates)
    source_counts = Counter(str(candidate.get("source_label") or "unknown") for candidate in visible_candidates)

    topic_counts = {
        "city_public": sum(1 for candidate in visible_candidates if str(candidate.get("category") or "") in CITY_PUBLIC_CATEGORIES),
        "crime_incident": sum(1 for candidate in visible_candidates if _is_crime_or_incident(candidate)),
        "football": sum(1 for candidate in visible_candidates if str(candidate.get("category") or "") == "football"),
        "events": sum(1 for candidate in visible_candidates if _is_event(candidate)),
        "city_centre": sum(1 for candidate in visible_candidates if _is_city_centre(candidate)),
    }
    borough_counts: Counter[str] = Counter()
    for candidate in visible_candidates:
        borough_counts.update(_boroughs(candidate))

    top_source, top_source_count = ("", 0)
    if source_counts:
        top_source, top_source_count = source_counts.most_common(1)[0]

    guardrails: list[dict[str, object]] = []

    def add_guardrail(name: str, status: str, value: object, limit: object, message: str) -> None:
        guardrails.append(
            {
                "name": name,
                "status": status,
                "value": value,
                "limit": limit,
                "message": message,
            }
        )

    if visible_count < 12:
        add_guardrail("volume_min", "warn", visible_count, ">=12", "Digest has fewer than 12 visible items.")
    elif visible_count > 24:
        add_guardrail("volume_max", "warn", visible_count, "<=24", "Digest has more than 24 visible items.")
    else:
        add_guardrail("volume", "pass", visible_count, "12-24", "Digest volume is in target range.")

    if visible_count >= 8 and topic_counts["city_public"] < 2:
        add_guardrail("city_public_min", "warn", topic_counts["city_public"], ">=2", "City/public-affairs coverage is thin.")

    if visible_count >= 12:
        limits = {
            "crime_incident": (0.35, "Crime/incident coverage dominates the issue."),
            "football": (0.20, "Football coverage dominates the issue."),
            "events": (0.45, "Events coverage dominates the issue."),
            "city_centre": (0.40, "City-centre coverage dominates the issue."),
        }
        for key, (limit, message) in limits.items():
            share = _share(topic_counts[key], visible_count)
            if share > limit:
                add_guardrail(f"{key}_share", "warn", share, f"<={limit}", message)

        top_source_share = _share(top_source_count, visible_count)
        if top_source_share > 0.35:
            add_guardrail(
                "top_source_share",
                "warn",
                {"source": top_source, "share": top_source_share},
                "<=0.35",
                "One source dominates the issue.",
            )
        if len(source_counts) < 5:
            add_guardrail("source_diversity", "warn", len(source_counts), ">=5", "Too few sources represented.")
        borough_mentions = sum(borough_counts.values())
        if borough_mentions >= 6 and len(borough_counts) < 3:
            add_guardrail("district_diversity", "warn", dict(sorted(borough_counts.items())), ">=3 boroughs", "Borough spread is narrow.")

    return {
        "visible_count": visible_count,
        "category_counts": dict(sorted(category_counts.items())),
        "block_counts": dict(sorted(block_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "top_source": {"source": top_source, "count": top_source_count, "share": _share(top_source_count, visible_count)},
        "topic_counts": topic_counts,
        "topic_shares": {key: _share(value, visible_count) for key, value in topic_counts.items()},
        "borough_counts": dict(sorted(borough_counts.items())),
        "guardrails": guardrails,
        "warnings": [guardrail for guardrail in guardrails if guardrail.get("status") == "warn"],
    }
