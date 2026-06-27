"""Profile-aware matching for free professional events.

The deterministic score is the cheap first pass. A compact LLM pass then
compares publishable professional events with the owner's CV/profile so the
block is not just "business keywords", but "worth Aleksei's time".
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
LLM_MATCH_MODEL_VERSION = "professional_event_llm_cv_match_v1"
LLM_MATCH_MAX_CANDIDATES = int(os.getenv("PROFESSIONAL_EVENT_LLM_MATCH_MAX", "16"))

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
            "low_fit": low_fit,
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
        # Hard commercial/sold-out exclusions are safe deterministically.
        # Low-score-but-free events stay alive until the LLM CV-match can say
        # whether the title is actually useful for the owner.
        if match.get("free_access_status") in {"paid", "sold_out"}:
            candidate["include"] = False
            candidate["reason"] = (
                str(candidate.get("reason") or "").rstrip()
                + f" | Professional event match: {match.get('event_level')} / {match.get('free_access_status')} / score {match.get('fit_score')}."
            ).strip()
        else:
            candidate["professional_match_status"] = "needs_llm_cv_match"
            candidate["quality_warnings"] = sorted(set(
                [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                + ["professional_llm_cv_match_required"]
            ))
    return candidate


def _event_field(candidate: dict[str, Any], key: str) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    return str(event.get(key) or "").strip()


# GM professional sources are GM-local by curation, so a parsed venue string is
# not required to satisfy the "place or online" contract — the event happens in
# Greater Manchester. Requiring a *specific* venue string was the real reason
# CV eligible sat at 1/42: dated GM Chamber / Manchester Digital events have a
# date + booking URL but no venue token parsed off their listing page.
_GM_PLACE_TOKENS = (
    "greater manchester", "manchester", "salford", "bury", "rochdale",
    "oldham", "stockport", "tameside", "trafford", "wigan",
)
_GM_PROFESSIONAL_SOURCE_TOKENS = (
    "chamber", "manchester digital", "growth hub", "pro-manchester",
    "promanchester", "university of manchester", "compiledmcr",
    "manchester central", "midas",
)
_ONLINE_TOKENS = ("online", "webinar", "virtual", "remote", "livestream", "zoom", "teams")


def _has_place_or_online(candidate: dict[str, Any], event: dict[str, Any]) -> bool:
    if str(event.get("venue") or "").strip() or str(event.get("borough") or "").strip():
        return True
    blob = _blob(candidate)
    if any(tok in blob for tok in _ONLINE_TOKENS):
        return True
    if any(tok in blob for tok in _GM_PLACE_TOKENS):
        return True
    source = str(candidate.get("source_label") or "").lower()
    return any(tok in source for tok in _GM_PROFESSIONAL_SOURCE_TOKENS)


def _professional_event_has_minimum_facts(candidate: dict[str, Any]) -> bool:
    from news_digest.pipeline.event_extraction import event_date_is_trustworthy  # noqa: PLC0415

    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    name = str(event.get("event_name") or candidate.get("title") or "").strip()
    url = str(event.get("booking_url") or candidate.get("source_url") or "").strip()
    if not (name and url):
        return False
    # A trustworthy, concrete date is the discriminator that keeps generic
    # programme / membership pages (no date, or a stray far-future month/day)
    # out of the protected professional block.
    if not event_date_is_trustworthy(candidate):
        return False
    return _has_place_or_online(candidate, event)


def _profile_for_prompt(project_root: Path | None = None) -> dict[str, object]:
    profile = load_business_event_profile(project_root)
    if profile:
        return profile
    return {
        "role": "CPO/CDTO / product and digital transformation leader",
        "strong_fit": ["fintech", "SaaS", "AI/ML", "data", "product", "digital transformation", "board/advisory"],
        "secondary_value": ["UK networking", "Manchester business network", "English professional practice"],
        "low_fit": ["student-only", "pure vendor demo", "paid dinner without clear professional value"],
    }


def _llm_payload(candidate: dict[str, Any]) -> dict[str, object]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    deterministic = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
    return {
        "id": str(candidate.get("fingerprint") or candidate.get("source_url") or candidate.get("title") or "")[:220],
        "title": str(event.get("event_name") or candidate.get("title") or "")[:220],
        "date": str(event.get("date_start") or event.get("date") or event.get("date_text") or "")[:80],
        "venue": str(event.get("venue") or "")[:160],
        "price_or_access": str(event.get("price") or deterministic.get("free_access_reason") or "")[:180],
        "booking_url": str(event.get("booking_url") or candidate.get("source_url") or "")[:260],
        "source": str(candidate.get("source_label") or "")[:120],
        "summary": str(candidate.get("summary") or candidate.get("lead") or candidate.get("evidence_text") or "")[:900],
        "deterministic_score": deterministic.get("fit_score"),
        "deterministic_reason": deterministic.get("why_this_fits_aleksei") or "",
    }


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[float, int, str]:
    match = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
    score = float(match.get("fit_score") or 0)
    level_bonus = {
        "major_conference_or_expo": 30,
        "high_value_professional": 20,
        "english_practice_networking": 10,
    }.get(str(match.get("event_level") or ""), 0)
    complete = 1 if _professional_event_has_minimum_facts(candidate) else 0
    return (score + level_bonus, complete, str(candidate.get("title") or ""))


def _drop_pending_llm_candidates(candidates: list[dict[str, Any]], reason: str) -> int:
    dropped = 0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if candidate.get("professional_match_status") != "needs_llm_cv_match":
            continue
        candidate["include"] = False
        candidate["reason"] = (
            str(candidate.get("reason") or "").rstrip()
            + f" | Professional LLM CV match: {reason}."
        ).strip()
        dropped += 1
    return dropped


def apply_professional_event_llm_matches(
    candidates: list[dict[str, Any]],
    project_root: Path | None = None,
    *,
    max_candidates: int | None = None,
) -> dict[str, Any]:
    """Run the actual model-based CV fit check on a compact professional board.

    This deliberately happens after deterministic enrichment/scoring, not per
    source item. The model sees only professional candidates with event facts,
    returns go/consider/skip, and the result is written back to candidates.
    """
    professional = [
        c for c in candidates
        if isinstance(c, dict)
        and str(c.get("category") or "") == "professional_events"
        and c.get("include")
        and _professional_event_has_minimum_facts(c)
    ]
    professional.sort(key=_candidate_sort_key, reverse=True)
    limit = max_candidates if max_candidates is not None else LLM_MATCH_MAX_CANDIDATES
    selected = professional[: max(0, limit)]
    not_sent = professional[max(0, limit):]
    report: dict[str, Any] = {
        "model_version": LLM_MATCH_MODEL_VERSION,
        "eligible": len(professional),
        "sent": len(selected),
        "not_sent": len(not_sent),
        "applied": 0,
        "skipped": 0,
        "status": "skipped_no_candidates" if not selected else "pending",
    }
    if not_sent:
        report["dropped_not_sent_pending"] = _drop_pending_llm_candidates(not_sent, "not evaluated inside morning CV-match cap")
    if not selected:
        return report

    try:
        from openai import OpenAI  # noqa: PLC0415
        from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415
        from news_digest.pipeline.model_routing import (
            chat_completion_options_for_route,
            resolve_model_route,
            sdk_retries_for_route,
        )
    except ImportError as exc:
        report["dropped_pending"] = int(report.get("dropped_pending") or 0) + _drop_pending_llm_candidates(selected, "model unavailable")
        report.update({"status": "skipped_import_error", "error": f"{exc.__class__.__name__}: {exc}"})
        return report

    routes = [route for route in resolve_model_route("professional_cv_match") if route.api_key]
    if not routes:
        report["dropped_pending"] = int(report.get("dropped_pending") or 0) + _drop_pending_llm_candidates(selected, "OPENAI_API_KEY unavailable")
        report.update({"status": "skipped_no_api_key"})
        return report
    route = routes[0]
    payload = {
        "profile": _profile_for_prompt(project_root),
        "events": [_llm_payload(c) for c in selected],
    }
    system_prompt = (
        "Ты оцениваешь бесплатные business/tech события под конкретный профиль владельца дайджеста. "
        "Нужно выбрать не просто события с business-словами, а те, куда ему реально стоит пойти: "
        "CPO/CDTO, fintech/SaaS, AI/ML, product, digital transformation, board/advisory, UK networking, "
        "практика профессионального английского. Верни строгий JSON: "
        "{\"items\":[{\"id\":\"...\",\"fit\":\"go|consider|skip\",\"score\":0-100,"
        "\"why\":\"одно конкретное предложение\",\"action\":\"register|consider|skip\","
        "\"free_access\":true|false,\"reason\":\"кратко\"}]}."
        "Если нет даты, места/online или понятного доступа, fit=skip. Не выдумывай факты."
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    try:
        client = OpenAI(
            api_key=route.api_key,
            base_url=route.base_url,
            timeout=route.timeout_seconds or 35,
            max_retries=sdk_retries_for_route(provider=route.provider, model=route.model, base_url=route.base_url),
        )
        response = client.chat.completions.create(
            model=route.model,
            messages=messages,
            temperature=0.1,
            max_tokens=min(6000, 360 * len(selected) + 900),
            response_format={"type": "json_object"},
            **chat_completion_options_for_route(provider=route.provider, model=route.model, base_url=route.base_url),
        )
        record_call_from_response(
            response=response,
            stage="validate",
            provider=route.provider_label,
            model=route.model,
            prompt_name="professional_cv_match",
            messages=messages,
            max_tokens=min(6000, 360 * len(selected) + 900),
        )
        parsed = json.loads(str(response.choices[0].message.content or "{}"))
    except Exception as exc:  # noqa: BLE001
        report["dropped_pending"] = int(report.get("dropped_pending") or 0) + _drop_pending_llm_candidates(selected, "model call failed")
        report.update({
            "status": "failed",
            "provider": route.provider_label,
            "model": route.model,
            "error": f"{exc.__class__.__name__}: {exc}",
        })
        return report

    rows = parsed.get("items") if isinstance(parsed, dict) else []
    if not isinstance(rows, list):
        report["dropped_pending"] = int(report.get("dropped_pending") or 0) + _drop_pending_llm_candidates(selected, "model response could not be parsed")
        report.update({"status": "parse_failed", "raw_type": type(parsed).__name__})
        return report
    by_id = {str(_llm_payload(c)["id"]): c for c in selected}
    for row in rows:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("id") or "")
        candidate = by_id.get(cid)
        if not candidate:
            continue
        fit = str(row.get("fit") or "").strip().lower()
        if fit not in {"go", "consider", "skip"}:
            fit = "consider" if bool(row.get("free_access")) else "skip"
        try:
            score = max(0, min(100, int(row.get("score") or 0)))
        except (TypeError, ValueError):
            score = 0
        llm_match = {
            "model": LLM_MATCH_MODEL_VERSION,
            "provider": route.provider_label,
            "route_role": route.role,
            "fit": fit,
            "score": score,
            "why": str(row.get("why") or row.get("reason") or "").strip(),
            "action": str(row.get("action") or ("register" if fit == "go" else fit)).strip(),
            "free_access": bool(row.get("free_access")),
            "reason": str(row.get("reason") or "").strip(),
        }
        candidate["professional_llm_match"] = llm_match
        match = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
        match = dict(match)
        match.update({
            "model": f"{MATCH_MODEL_VERSION}+{LLM_MATCH_MODEL_VERSION}",
            "publish": fit in {"go", "consider"},
            "fit_score": max(int(match.get("fit_score") or 0), score),
            "llm_fit": fit,
            "why_this_fits_aleksei": llm_match["why"] or match.get("why_this_fits_aleksei") or "",
            "recommended_action": "register" if fit == "go" else ("consider" if fit == "consider" else "skip"),
        })
        candidate["professional_event_match"] = match
        candidate["professional_match_status"] = "llm_cv_matched"
        candidate["reader_action_type"] = "book_or_buy" if match["recommended_action"] == "register" else "plan_ahead"
        candidate["english_editorial_score"] = max(float(candidate.get("english_editorial_score") or 0), float(match.get("fit_score") or 0))
        if fit == "skip":
            candidate["include"] = False
            candidate["reason"] = (
                str(candidate.get("reason") or "").rstrip()
                + f" | Professional LLM CV match: skip — {llm_match['reason'] or llm_match['why']}."
            ).strip()
            report["skipped"] = int(report.get("skipped") or 0) + 1
        else:
            report["applied"] = int(report.get("applied") or 0) + 1
    report.update({"status": "ok", "provider": route.provider_label, "model": route.model})
    return report
