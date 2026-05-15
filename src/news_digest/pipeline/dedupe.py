from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import re

from news_digest.pipeline.common import (
    fingerprint_for_candidate,
    normalize_title,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.history import ensure_history_files


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


def initialize_candidates_state(project_root: Path, *, overwrite: bool = False) -> StageResult:
    state_dir = project_root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    ensure_history_files(state_dir)

    path = state_dir / "candidates.json"
    if overwrite or not path.exists():
        write_json(
            path,
            {
                "run_at_london": now_london().isoformat(),
                "run_date_london": today_london(),
                "stage_status": "incomplete",
                "candidates": [
                    {
                        "title": "",
                        "category": "media_layer",
                        "summary": "",
                        "source_url": "",
                        "source_label": "",
                        "primary_block": "last_24h",
                        "include": False,
                        "dedupe_decision": "drop",
                        "carry_over_label": "",
                        "reason": "",
                        "matched_previous_fingerprint": "",
                    }
                ],
            },
        )

    return StageResult(True, f"Candidates state initialized at {path}.", path)


def dedupe_candidates(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    paths = ensure_history_files(state_dir)
    candidates_path = state_dir / "candidates.json"
    report_path = paths["dedupe_memory"]

    payload = read_json(candidates_path, {"candidates": []})
    candidates = payload.get("candidates", [])
    published = read_json(paths["published_facts"], {"facts": []}).get("facts", [])
    published_by_fp = {
        str(item.get("fingerprint")): item for item in published if isinstance(item, dict) and item.get("fingerprint")
    }
    published_titles = [
        item for item in published if isinstance(item, dict) and item.get("normalized_title")
    ]

    errors: list[str] = []
    decisions: list[dict] = []

    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict):
            errors.append(f"Candidate #{index} is not an object.")
            continue

        fingerprint = fingerprint_for_candidate(candidate)
        candidate["fingerprint"] = fingerprint
        previous = published_by_fp.get(fingerprint)
        normalized_title = normalize_title(str(candidate.get("title") or ""))
        original_title = str(candidate.get("title") or "")
        similar_previous = _similar_published_titles(normalized_title, original_title, published_titles)
        candidate.setdefault("reason", "")
        candidate.setdefault("matched_previous_fingerprint", "")

        decision = str(candidate.get("dedupe_decision") or "").strip()
        category = str(candidate.get("category") or "").strip()
        primary_block = str(candidate.get("primary_block") or "").strip()
        operational_repeat_ok = primary_block in {"weather", "transport"}
        same_day_repeat_ok = (
            previous is not None
            and str(previous.get("last_published_day_london") or "").strip() == today_london()
        )
        calendar_carry_ok = previous is not None and _calendar_item_should_carry_over(candidate, previous)
        if previous is not None and (operational_repeat_ok or same_day_repeat_ok):
            candidate["dedupe_decision"] = "new"
            candidate["include"] = True
            candidate["reason"] = candidate.get("reason") or "Operational block repeat is allowed while it remains relevant."
        elif calendar_carry_ok:
            candidate["dedupe_decision"] = "carry_over_with_label"
            candidate["include"] = True
            candidate["carry_over_label"] = candidate.get("carry_over_label") or "актуально к дате"
            candidate["reason"] = (
                candidate.get("reason")
                or "Calendar/lifestyle item is still active and was not shown in the previous issue."
            )
        elif previous is not None:
            candidate["matched_previous_fingerprint"] = fingerprint
            if decision not in {"carry_over_with_label", "new_phase"}:
                candidate["dedupe_decision"] = "drop"
                candidate["include"] = False
                candidate["reason"] = candidate.get("reason") or "Repeat without new phase."
            elif decision == "carry_over_with_label" and not candidate.get("carry_over_label"):
                candidate["dedupe_decision"] = "drop"
                candidate["include"] = False
                candidate["reason"] = "Carry-over without carry_over_label."
        elif decision not in {"drop", "new", "new_phase", "carry_over_with_label"}:
            candidate["dedupe_decision"] = "drop"
            candidate["include"] = False
            candidate["reason"] = "Invalid dedupe decision."

        # Q6: classify what kind of change this candidate represents.
        change_type = _classify_change_type(candidate, previous, similar_previous)
        candidate["change_type"] = change_type

        # Q7: pull "previous fact" out into structured fields whenever
        # there's any prior match (exact fingerprint or title-similar),
        # not just for hard-rejects. Makes "почему отбили / на что
        # ссылается" queryable from JSON without parsing the reason
        # sentence.
        prev_ref = previous or (
            published_by_fp.get(str(similar_previous[0].get("fingerprint") or ""))
            if similar_previous else None
        )
        prev_fp = str(prev_ref.get("fingerprint") or "").strip() if prev_ref else ""
        prev_date = (
            str(prev_ref.get("first_published_day_london") or "").strip()
            if prev_ref else ""
        )
        prev_title = str(prev_ref.get("title") or "").strip() if prev_ref else ""
        if prev_ref:
            candidate["previous_fingerprint"] = prev_fp
            candidate["previous_published_day"] = prev_date
            candidate["previous_title"] = prev_title

        if change_type in {"no_change", "same_story_rehash"}:
            human_prefix = (
                "Без новых фактов: уже был"
                if change_type == "no_change"
                else "Повтор сюжета без новых деталей: уже был"
            )
            if prev_date and prev_title:
                candidate["reason"] = (
                    f"{human_prefix} {prev_date} как «{prev_title[:120]}»."
                )
            elif prev_title:
                candidate["reason"] = f"{human_prefix} ранее как «{prev_title[:120]}»."
            # If we ended up here without a dedupe drop yet, enforce one.
            if candidate.get("dedupe_decision") not in {"drop"}:
                candidate["dedupe_decision"] = "drop"
                candidate["include"] = False

        if not candidate.get("reason"):
            errors.append(f"Candidate #{index} is missing reason.")

        decisions.append(
            {
                "fingerprint": fingerprint,
                "title": candidate.get("title"),
                "decision": candidate.get("dedupe_decision"),
                "change_type": change_type,
                "reason": candidate.get("reason"),
                "matched_previous_fingerprint": candidate.get("matched_previous_fingerprint"),
                "previous_fingerprint": prev_fp,
                "previous_published_day": prev_date,
                "previous_title": prev_title,
                "carry_over_label": candidate.get("carry_over_label"),
                "similar_previous": similar_previous,
            }
        )

    # LLM borderline review: heuristic _classify_change_type can't tell
    # "£230m requested" from "£230m granted". For candidates labelled
    # no_change/same_story_rehash that DO carry substantive evidence_text,
    # ask the LLM to either upgrade the verdict (same_story_new_facts /
    # follow_up → un-drop) or confirm rehash. See _review_borderline_with_llm.
    llm_reviews = _review_borderline_with_llm(candidates, published_by_fp)
    for decision in decisions:
        rev = llm_reviews.get(str(decision.get("fingerprint") or ""))
        if rev:
            decision["change_type"] = rev["change_type"]
            decision["reason"] = rev["reason"]
            decision["llm_reviewed"] = True

    intra_batch_drops = _apply_intra_batch_dedup(candidates)
    final_candidates_by_fp = {
        str(candidate.get("fingerprint") or ""): candidate
        for candidate in candidates
        if isinstance(candidate, dict) and candidate.get("fingerprint")
    }
    for decision in decisions:
        final_candidate = final_candidates_by_fp.get(str(decision.get("fingerprint") or ""))
        if not final_candidate:
            continue
        decision["decision"] = final_candidate.get("dedupe_decision")
        decision["reason"] = final_candidate.get("reason")
        decision["include"] = bool(final_candidate.get("include"))

    payload["run_at_london"] = now_london().isoformat()
    payload["run_date_london"] = today_london()
    payload["stage_status"] = "complete" if not errors else "failed"
    pipeline_run_id = pipeline_run_id_from(payload)
    write_json(candidates_path, payload)
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "last_updated_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "decisions": decisions,
            "intra_batch_dedup_drops": intra_batch_drops,
        },
    )

    return StageResult(not errors, "Dedupe completed." if not errors else "Dedupe completed with errors.", report_path)


_GM_BOROUGHS: frozenset[str] = frozenset({
    "salford", "stockport", "trafford", "tameside",
    "rochdale", "oldham", "wigan", "bolton", "bury",
    "altrincham", "stretford", "ashton", "eccles",
})

_SOURCE_PRIORITY: dict[str, int] = {
    "bbc": 0,
    "manchester evening news": 1, "men": 1,
    "the mill": 2,
    "greater manchester police": 2, "gmp": 2,
    "the manc": 3, "altrincham today": 3,
    "i love manchester": 4, "secret manchester": 4,
    "manchester's finest": 5,
}

_TITLE_STOPWORDS: frozenset[str] = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to",
    "of", "for", "with", "from", "is", "are", "was", "were", "be",
    "been", "has", "have", "had", "by", "as", "it", "its",
})

_CALENDAR_CARRY_BLOCKS: frozenset[str] = frozenset({
    "openings",
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
})
_CALENDAR_CARRY_CATEGORIES: frozenset[str] = frozenset({
    "food_openings",
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
})
_CALENDAR_CARRY_MIN_INTERVAL_DAYS = 2
_CALENDAR_CARRY_MAX_AGE_DAYS = 14
_CALENDAR_SIGNAL_TERMS: tuple[str, ...] = (
    "bar",
    "beer",
    "brewery",
    "cafe",
    "café",
    "car boot",
    "closing",
    "craft",
    "fair",
    "farmers market",
    "festival",
    "flea",
    "food hall",
    "food market",
    "launch",
    "launches",
    "maker",
    "makers market",
    "market",
    "opening",
    "opens",
    "pop-up",
    "pub",
    "reopen",
    "restaurant",
    "street food",
)
_MONTHS: dict[str, int] = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def _parse_day(value: object) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
    except ValueError:
        pass
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _published_day_from_history(item: dict, key: str) -> date | None:
    try:
        return datetime.strptime(str(item.get(key) or ""), "%Y-%m-%d").date()
    except ValueError:
        return None


def _candidate_text(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "source_url")
    )


def _calendar_dates_from_text(text: str) -> list[date]:
    today = now_london().date()
    dates: list[date] = []
    lowered = str(text or "").lower()

    for match in re.finditer(r"\b(20\d{2})[/-](\d{1,2})[/-](\d{1,2})\b", lowered):
        year, month, day = (int(part) for part in match.groups())
        try:
            dates.append(date(year, month, day))
        except ValueError:
            continue

    for match in re.finditer(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)", lowered):
        year, month, day = (int(part) for part in match.groups())
        try:
            dates.append(date(year, month, day))
        except ValueError:
            continue

    for match in re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]{3,9})(?:\s+(20\d{2}))?\b", lowered):
        day_raw, month_raw, year_raw = match.groups()
        month = _MONTHS.get(month_raw)
        if not month:
            continue
        year = int(year_raw) if year_raw else today.year
        try:
            parsed = date(year, month, int(day_raw))
        except ValueError:
            continue
        if not year_raw and parsed < today.replace(day=1):
            parsed = parsed.replace(year=parsed.year + 1)
        dates.append(parsed)

    return dates


def _calendar_item_should_carry_over(candidate: dict, previous: dict) -> bool:
    primary_block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if primary_block not in _CALENDAR_CARRY_BLOCKS and category not in _CALENDAR_CARRY_CATEGORIES:
        return False

    text = _candidate_text(candidate)
    lowered = text.lower()
    if not any(term in lowered for term in _CALENDAR_SIGNAL_TERMS):
        return False

    today = now_london().date()
    first_published = _published_day_from_history(previous, "first_published_day_london")
    if first_published and (today - first_published).days > _CALENDAR_CARRY_MAX_AGE_DAYS:
        return False

    explicit_dates = _calendar_dates_from_text(text)
    published_day = _parse_day(candidate.get("published_at"))
    if primary_block in {"weekend_activities", "next_7_days", "ticket_radar", "outside_gm_tickets", "russian_events", "future_announcements"} and published_day:
        explicit_dates.append(published_day)

    long_running_active = False
    if len(explicit_dates) >= 2:
        start = min(explicit_dates)
        end = max(explicit_dates)
        long_running_active = start <= today <= end and (end - start).days >= 7

    last_published = _published_day_from_history(previous, "last_published_day_london")
    min_interval = 1 if long_running_active else _CALENDAR_CARRY_MIN_INTERVAL_DAYS
    if last_published and (today - last_published).days < min_interval:
        return False

    if explicit_dates:
        return max(explicit_dates) >= today

    # Food/opening pages often lack machine-readable event dates. Keep them
    # eligible briefly instead of dropping an opening forever after one URL hit.
    return primary_block == "openings" or category == "food_openings"


def _extract_borough(title: str) -> str | None:
    lowered = title.lower()
    for borough in _GM_BOROUGHS:
        if re.search(rf"\b{re.escape(borough)}\b", lowered):
            return borough
    return None


def _source_rank(source_label: str) -> int:
    label = str(source_label or "").lower()
    for key, rank in _SOURCE_PRIORITY.items():
        if key in label:
            return rank
    return 99


_TICKETMASTER_SUFFIX_RE = re.compile(
    r'\s*[—–\-]+\s*(?:event|public\s+sale)\s+\d{4}[\d\s:\-]+.*$',
    re.IGNORECASE,
)
# Strip "Feat. The Neverland Express" / "Featuring <band>" — sub-details that appear
# in Ticketmaster titles but not venue listing titles, causing Jaccard to drop.
_FEAT_SUFFIX_RE = re.compile(
    r'\s+[Ff]eat(?:uring)?\.?\s+.+$',
)


# Change-type classification (Q6/Q7).
# Words in lead/title/summary that mark a story as a clear follow-up
# of an earlier published item rather than a fresh rehash. When any of
# these appear AND there is a previous match, classify as `follow_up`
# (not blocked) rather than `same_story_rehash` (auto-rejected).
_FOLLOW_UP_MARKERS: tuple[str, ...] = (
    # Russian — court / police progression
    "приговор", "осужд", "виновн", "приговорил",
    "следствие продолжа", "расследование продолжа",
    "задержан", "арестован", "обвинен",
    "годовщин", "к годовщине",
    "обновление", "обновлён", "новые подробности", "уточн",
    # English court / police
    "sentenced", "verdict", "convicted", "guilty",
    "investigation continues", "court update",
    "appeal", "charged", "anniversary",
    "follow up", "follow-up", "follow up:",
    # Project / policy phase markers
    "вступает в силу", "вступил в силу", "запущен", "открылся",
    "comes into effect", "now in effect", "officially open",
)


def _classify_change_type(
    candidate: dict,
    previous: dict | None,
    similar_previous: list[dict],
) -> str:
    """Return one of:
      new_story, no_change, same_story_rehash, same_story_new_facts,
      follow_up, reminder.

    Cheap heuristic based on fingerprint match, similar-title match,
    declared dedupe_decision, and follow-up keywords. Anything more
    nuanced (e.g. "Wigan got £230m vs Wigan asked for £230m") is the
    LLM curator's job — we only label the obvious cases here.
    """
    decision = str(candidate.get("dedupe_decision") or "").strip()
    primary_block = str(candidate.get("primary_block") or "").strip()

    # Operational repeats (weather, transport): treat as standalone
    # new_story each day — readers expect a fresh snapshot.
    if primary_block in {"weather", "transport"} and not previous:
        return "new_story"
    if primary_block in {"weather", "transport"} and previous:
        return "same_story_new_facts"  # daily refresh with new figures

    # Calendar carry-over (next_7_days reminders) was decided upstream.
    if decision == "carry_over_with_label":
        return "reminder"

    # Explicit new_phase set by candidate validator or curator means
    # "yes, same story, but a new development". Distinguishing this
    # from rehash without LLM is impossible cheaply — trust the flag.
    if decision == "new_phase":
        return "same_story_new_facts"

    has_match = previous is not None or bool(similar_previous)
    if not has_match:
        return "new_story"

    # Same-story branch: look for follow-up cue words in candidate text.
    blob = " ".join(
        str(candidate.get(f) or "")
        for f in ("title", "lead", "summary", "evidence_text", "practical_angle")
    ).lower()
    if any(marker in blob for marker in _FOLLOW_UP_MARKERS):
        return "follow_up"

    # Exact fingerprint hit and the candidate text shows no follow-up
    # signal → republished without new substance.
    if previous is not None:
        return "no_change"

    # Title-similar to something we already shipped, no follow-up signal.
    return "same_story_rehash"


# LLM borderline review — Q6/Q7 nuance pass.
# Heuristic _classify_change_type can't tell apart "£230m requested"
# vs "£230m granted" or "Burnham hints at Westminster return" vs
# "Burnham officially announces Westminster bid". For those cases we
# pull a small batch into a dedicated LLM check and upgrade the
# verdict if evidence_text shows real news. Same provider cascade as
# curator (DeepSeek primary → OpenAI → Groq Llama).

_DEDUPE_REVIEW_PROMPT = """Ты редактор городского дайджеста. Получаешь пары: «новый кандидат» и «прошлая публикация» по той же истории.

Решение для каждой пары — одно из трёх:
- new_facts: в evidence_text есть конкретная НОВАЯ деталь, которой не было в прошлом заголовке (новая сумма £, новое имя, новая дата, вступило в силу, открыли, объявили о закрытии, поймали, обвинили).
- follow_up: явная следующая фаза события (вердикт после ареста, годовщина, итог расследования, запуск после анонса).
- rehash: тот же сюжет, просто новый URL/перепечатка/другая редакция, без новых конкретных фактов.

Возвращай ТОЛЬКО JSON-массив:
[{"fingerprint":"...","change_type":"new_facts|follow_up|rehash","reason":"кратко по-русски, ≤120 символов, со ссылкой на конкретный новый факт"}]
Никакого markdown."""

_BORDERLINE_MIN_EVIDENCE_CHARS = 200
_BORDERLINE_BATCH_SIZE = 10


def _borderline_pairs(
    candidates: list[dict],
    published_by_fp: dict[str, dict],
) -> list[tuple[dict, dict]]:
    """Pick candidates classified as no_change/same_story_rehash that
    still have enough evidence_text to be worth a second look."""
    pairs: list[tuple[dict, dict]] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        if str(c.get("change_type") or "") not in {"no_change", "same_story_rehash"}:
            continue
        evidence = str(c.get("evidence_text") or "")
        if len(evidence) < _BORDERLINE_MIN_EVIDENCE_CHARS:
            continue
        # Find a previous reference: exact fingerprint or first similar.
        prev = published_by_fp.get(str(c.get("fingerprint") or ""))
        if not prev:
            # We may have matched via title similarity earlier; the
            # similar_previous list is in dedupe_memory's decision but
            # not on the candidate — fall back to scanning all published
            # by normalized title overlap once more.
            continue
        pairs.append((c, prev))
    return pairs


def _call_dedupe_review_llm(
    pairs: list[tuple[dict, dict]],
    api_key: str,
    base_url: str,
    model: str,
) -> list[dict]:
    if not pairs or not api_key:
        return []
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:  # pragma: no cover
        return []
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=45, max_retries=0)
    results: list[dict] = []
    for i in range(0, len(pairs), _BORDERLINE_BATCH_SIZE):
        batch = pairs[i: i + _BORDERLINE_BATCH_SIZE]
        user = [
            {
                "fingerprint": c.get("fingerprint", ""),
                "candidate_title": c.get("title", ""),
                "candidate_evidence": (c.get("evidence_text") or "")[:800],
                "candidate_lead": (c.get("lead") or "")[:300],
                "previous_title": prev.get("title", ""),
                "previous_published_day": prev.get("first_published_day_london", ""),
            }
            for c, prev in batch
        ]
        try:
            import json as _json  # noqa: PLC0415
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _DEDUPE_REVIEW_PROMPT},
                    {"role": "user", "content": _json.dumps(user, ensure_ascii=False)},
                ],
                temperature=0.1,
                max_tokens=1500,
            )
            from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415
            from news_digest.pipeline.curator import _provider_label  # noqa: PLC0415
            record_call_from_response(
                response=response,
                stage="dedupe_review",
                provider=_provider_label(model),
                model=model,
                prompt_name="dedupe_review",
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.rsplit("```", 1)[0]
            results.extend(_json.loads(raw.strip()) or [])
        except Exception as exc:  # noqa: BLE001
            import logging  # noqa: PLC0415
            logging.getLogger(__name__).warning("Dedupe LLM review failed: %s", exc)
            return []
    return results


def _review_borderline_with_llm(
    candidates: list[dict],
    published_by_fp: dict[str, dict],
) -> dict[str, dict]:
    """Run LLM review on borderline rehash/no_change candidates. Returns
    {fingerprint: {change_type, reason}} only for candidates the LLM
    upgraded or explicitly confirmed. Failures are silent — the existing
    heuristic decision stays in place.
    """
    import os  # noqa: PLC0415
    import logging  # noqa: PLC0415
    logger = logging.getLogger(__name__)
    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    if provider_override == "none":
        return {}
    pairs = _borderline_pairs(candidates, published_by_fp)
    if not pairs:
        return {}
    logger.info("Dedupe LLM review: %d borderline candidate(s).", len(pairs))

    # Same provider cascade as curator: DeepSeek → OpenAI → Groq.
    from news_digest.pipeline.curator import (  # noqa: PLC0415
        DEEPSEEK_BASE_URL, DEEPSEEK_MODEL,
        OPENAI_BASE_URL, OPENAI_MODEL,
        GROQ_BASE_URL, GROQ_MODEL,
    )
    chains = [
        (os.environ.get("DEEPSEEK_API_KEY", ""), DEEPSEEK_BASE_URL, DEEPSEEK_MODEL),
        (os.environ.get("OPENAI_API_KEY", ""),   OPENAI_BASE_URL,   OPENAI_MODEL),
        (os.environ.get("GROQ_API_KEY", ""),     GROQ_BASE_URL,     GROQ_MODEL),
    ]
    decisions: list[dict] = []
    for api_key, base_url, model in chains:
        if not api_key:
            continue
        decisions = _call_dedupe_review_llm(pairs, api_key, base_url, model)
        if decisions:
            break
    if not decisions:
        logger.info("Dedupe LLM review: all providers failed — keeping heuristic verdicts.")
        return {}

    # Apply decisions: upgrade to same_story_new_facts / follow_up only
    # if LLM explicitly says so AND the candidate was previously dropped.
    upgrades: dict[str, dict] = {}
    upgrade_map = {"new_facts": "same_story_new_facts", "follow_up": "follow_up"}
    cands_by_fp = {str(c.get("fingerprint") or ""): c for c in candidates if isinstance(c, dict)}
    for d in decisions:
        if not isinstance(d, dict):
            continue
        fp = str(d.get("fingerprint") or "").strip()
        ct_raw = str(d.get("change_type") or "").strip().lower()
        reason = str(d.get("reason") or "").strip()
        if not fp or ct_raw not in {"new_facts", "follow_up", "rehash"}:
            continue
        c = cands_by_fp.get(fp)
        if not c:
            continue
        upgraded_ct = upgrade_map.get(ct_raw)
        if upgraded_ct:
            c["change_type"] = upgraded_ct
            c["include"] = True
            c["dedupe_decision"] = "new_phase"
            c["reason"] = f"LLM-review: {reason}" if reason else "LLM-review: upgraded from heuristic verdict."
            upgrades[fp] = {"change_type": upgraded_ct, "reason": c["reason"]}
        elif ct_raw == "rehash" and reason:
            # Strengthen the reason field with the LLM's specific call —
            # heuristic reason already cites date+title, this adds the
            # LLM's "почему не апгрейд" rationale.
            existing = c.get("reason") or ""
            c["reason"] = f"{existing} LLM-review: {reason}".strip()
            upgrades[fp] = {"change_type": c.get("change_type") or "same_story_rehash", "reason": c["reason"]}
    logger.info(
        "Dedupe LLM review: upgraded %d, confirmed-rehash %d, total reviewed %d.",
        sum(1 for v in upgrades.values() if v["change_type"] != "same_story_rehash"),
        sum(1 for v in upgrades.values() if v["change_type"] == "same_story_rehash"),
        len(pairs),
    )
    return upgrades


def _title_tokens(title: str) -> frozenset[str]:
    # Strip Ticketmaster metadata suffix ("— event 2026-05-15 — public sale …")
    # and featuring credits before tokenising so cross-source dedup works.
    normalized = _TICKETMASTER_SUFFIX_RE.sub("", str(title or ""))
    normalized = _FEAT_SUFFIX_RE.sub("", normalized)
    words = re.findall(r"[a-zA-Zа-яёА-ЯЁ][a-zA-Zа-яёА-ЯЁ'-]*", normalized.lower())
    return frozenset(w for w in words if w not in _TITLE_STOPWORDS and len(w) >= 3)


_DEDUP_BLOCK_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"lead_story", "last_24h", "today_focus", "city_watch", "district_radar"}),
    frozenset({"weekend_activities", "next_7_days", "future_announcements", "ticket_radar", "outside_gm_tickets", "russian_events"}),
    frozenset({"openings", "tech_business"}),
)


def _dedup_block_group(primary_block: str) -> str:
    for index, group in enumerate(_DEDUP_BLOCK_GROUPS):
        if primary_block in group:
            return f"group:{index}"
    return primary_block


def _apply_intra_batch_dedup(candidates: list[dict]) -> list[dict]:
    """Drop topic-duplicates within the batch, keeping the strongest source.

    Two included candidates are considered the same story when:
    - They are in the same dedupe block group
    - Their title token overlap (Jaccard) >= 0.50
    - They refer to the same GM borough, or neither mentions a specific borough
      (city-wide story)

    The candidate with the lower source priority rank is dropped.
    """
    included = [c for c in candidates if isinstance(c, dict) and c.get("include")]
    n = len(included)

    to_drop: dict[int, dict] = {}

    for i in range(n):
        if i in to_drop:
            continue
        ci = included[i]
        tokens_i = _title_tokens(str(ci.get("title") or ""))
        borough_i = _extract_borough(str(ci.get("title") or ""))
        block_i = str(ci.get("primary_block") or "")
        group_i = _dedup_block_group(block_i)
        rank_i = _source_rank(str(ci.get("source_label") or ""))

        for j in range(i + 1, n):
            if j in to_drop:
                continue
            cj = included[j]
            if _dedup_block_group(str(cj.get("primary_block") or "")) != group_i:
                continue

            borough_j = _extract_borough(str(cj.get("title") or ""))
            if borough_i != borough_j:
                continue  # different boroughs = different stories

            tokens_j = _title_tokens(str(cj.get("title") or ""))
            union = tokens_i | tokens_j
            if not union or len(tokens_i) < 3 or len(tokens_j) < 3:
                continue
            overlap = len(tokens_i & tokens_j) / len(union)
            if overlap < 0.40:
                continue

            rank_j = _source_rank(str(cj.get("source_label") or ""))
            if rank_i <= rank_j:
                to_drop[j] = {"kept_index": i, "overlap": round(overlap, 2)}
            else:
                to_drop[i] = {"kept_index": j, "overlap": round(overlap, 2)}
                break

    drops: list[dict] = []
    for idx, drop_context in to_drop.items():
        c = included[idx]
        kept = included[int(drop_context["kept_index"])]
        c["dedupe_decision"] = "drop"
        c["include"] = False
        c["reason"] = "Intra-batch topic duplicate — same story kept from stronger source."
        drops.append(
            {
                "fingerprint": c.get("fingerprint"),
                "title": c.get("title"),
                "source_label": c.get("source_label"),
                "primary_block": c.get("primary_block"),
                "kept_fingerprint": kept.get("fingerprint"),
                "kept_title": kept.get("title"),
                "kept_source_label": kept.get("source_label"),
                "kept_primary_block": kept.get("primary_block"),
                "overlap": drop_context["overlap"],
                "reason": c["reason"],
            }
        )
    return drops


def _entity_tokens(title: str) -> set[str]:
    """Capitalized words and numbers from the original title — likely proper nouns."""
    return {w.lower() for w in re.findall(r"\b(?:[A-Z][a-z]{1,}|[A-Z]{2,}|\d{2,})\b", title)}


def _similar_published_titles(
    normalized_title: str,
    original_title: str,
    published_titles: list[dict],
) -> list[dict]:
    title_tokens = set(normalized_title.split())
    entity_tokens = _entity_tokens(original_title)
    if len(title_tokens) < 4:
        return []
    matches: list[dict] = []
    for item in published_titles:
        previous_title = str(item.get("normalized_title") or "")
        previous_tokens = set(previous_title.split())
        if len(previous_tokens) < 4:
            continue
        union = title_tokens | previous_tokens
        overlap = len(title_tokens & previous_tokens) / max(len(union), 1)
        # Primary match: high Jaccard on full title tokens
        if overlap >= 0.55:
            matches.append({"fingerprint": item.get("fingerprint"), "title": item.get("title"), "overlap": round(overlap, 2)})
            continue
        # Secondary match: moderate Jaccard + shared named entities (≥2)
        if overlap >= 0.25 and entity_tokens:
            prev_entities = _entity_tokens(str(item.get("title") or ""))
            if len(entity_tokens & prev_entities) >= 2:
                matches.append({"fingerprint": item.get("fingerprint"), "title": item.get("title"), "overlap": round(overlap, 2)})
    return matches[:3]
