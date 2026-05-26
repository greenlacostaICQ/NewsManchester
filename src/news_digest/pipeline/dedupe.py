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
from news_digest.pipeline.editorial_contracts import (
    attach_editorial_contract,
    history_window_days_for_contract,
    is_specific_topic_key,
    lifecycle_repeat_review,
    topic_key_for_candidate,
)
from news_digest.pipeline.entity_extraction import enrich_candidate_entities
from news_digest.pipeline.event_extraction import enrich_candidate_event
from news_digest.pipeline.history import ensure_history_files
from news_digest.pipeline.story_intelligence import (
    attach_evidence_packet,
    attach_story_intelligence,
    attach_story_clusters,
    history_match_records,
    new_facts_diff,
)
from news_digest.pipeline.semantic_dedupe import (
    EMBEDDING_VERSION,
    anchor_tokens,
    cosine_similarity,
    has_new_fact_signal,
    semantic_embedding,
)


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
    published_by_topic: dict[str, list[dict]] = {}
    for item in published:
        if not isinstance(item, dict):
            continue
        topic_key = topic_key_for_candidate(item)
        if is_specific_topic_key(topic_key):
            published_by_topic.setdefault(topic_key, []).append(item)

    errors: list[str] = []
    decisions: list[dict] = []

    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict):
            errors.append(f"Candidate #{index} is not an object.")
            continue
        enrich_candidate_entities(candidate)
        # I3: event facts depend on entities — must run AFTER entity pass.
        enrich_candidate_event(candidate)
        attach_editorial_contract(candidate)

        fingerprint = fingerprint_for_candidate(candidate)
        candidate["fingerprint"] = fingerprint
        previous = published_by_fp.get(fingerprint)
        normalized_title = normalize_title(str(candidate.get("title") or ""))
        original_title = str(candidate.get("title") or "")
        title_similar_previous = _similar_published_titles(candidate, normalized_title, original_title, published_titles)
        semantic_previous = _semantic_published_matches(candidate, published_titles)
        people_previous = _people_published_matches(candidate, published_titles)
        topic_previous = _topic_published_matches(candidate, published_by_topic)
        similar_previous = _merge_previous_matches(
            title_similar_previous, semantic_previous, people_previous, topic_previous,
        )
        candidate["history_window_days"] = _history_window_days(candidate)
        candidate["history_matches"] = history_match_records(similar_previous)
        attach_evidence_packet(candidate, history_matches=candidate["history_matches"])
        if topic_previous:
            candidate["topic_lifecycle_match"] = {
                "topic_key": topic_key_for_candidate(candidate),
                "previous_fingerprint": topic_previous[0].get("fingerprint"),
                "previous_title": topic_previous[0].get("title"),
                "previous_published_day": (
                    topic_previous[0].get("last_published_day_london")
                    or topic_previous[0].get("first_published_day_london")
                    or ""
                ),
            }
        if semantic_previous:
            candidate["semantic_dedupe_match"] = (
                "embedding_only" if not title_similar_previous else "embedding_and_title"
            )
            candidate["semantic_dedupe_score"] = semantic_previous[0].get("overlap")
        if people_previous:
            # S2: surface the people match separately so the support
            # report can show "blocked because Erica de Souza Correa
            # was already published 2 days ago".
            top_person_match = people_previous[0]
            candidate["people_dedupe_match"] = {
                "matched_person_today": top_person_match.get("matched_person_today"),
                "matched_person_previously": top_person_match.get("matched_person_previously"),
                "previous_fingerprint": top_person_match.get("fingerprint"),
                "previous_title": top_person_match.get("title"),
                "shared_tokens": top_person_match.get("shared_tokens"),
            }
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
            candidate["reason"] = candidate.get("reason") or (
                "Same-day rerun repeat is allowed while correcting today's issue."
                if same_day_repeat_ok
                else "Operational block repeat is allowed while it remains relevant."
            )
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

        prev_ref = previous or (
            published_by_fp.get(str(similar_previous[0].get("fingerprint") or ""))
            if similar_previous else None
        )

        # Q6: classify what kind of change this candidate represents.
        change_type = _classify_change_type(candidate, previous, similar_previous, prev_ref)
        candidate["change_type"] = change_type
        if (
            change_type in {"same_story_new_facts", "follow_up"}
            and prev_ref is not None
            and not (same_day_repeat_ok or operational_repeat_ok)
            and str(candidate.get("dedupe_decision") or "") == "drop"
        ):
            candidate["dedupe_decision"] = "new_phase"
            candidate["include"] = True
            candidate["reason"] = (
                "Same story, but semantic dedupe found concrete new facts."
                if change_type == "same_story_new_facts"
                else "Follow-up to a previous story with a new phase."
            )

        lifecycle_review = (
            lifecycle_repeat_review(candidate, prev_ref)
            if prev_ref is not None and not (same_day_repeat_ok or operational_repeat_ok)
            else {"repeat": False}
        )
        if lifecycle_review.get("repeat"):
            candidate["dedupe_decision"] = "drop"
            candidate["include"] = False
            candidate["change_type"] = "same_story_rehash"
            candidate["topic_lifecycle_repeat"] = lifecycle_review
            candidate["reason"] = (
                "Повтор темы без новой фазы: уже был"
                f" {prev_ref.get('last_published_day_london') or prev_ref.get('first_published_day_london') or 'ранее'}"
                f" как «{str(prev_ref.get('title') or '')[:120]}»."
            )

        # Q7: pull "previous fact" out into structured fields whenever
        # there's any prior match (exact fingerprint or title-similar),
        # not just for hard-rejects. Makes "почему отбили / на что
        # ссылается" queryable from JSON without parsing the reason
        # sentence.
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
            # S2: when the previous match came from a people-entity hit
            # (BBC vs MEN vs The Manc on the same victim), call that out
            # explicitly so the support report can group these blocks.
            person_tag = ""
            if isinstance(candidate.get("people_dedupe_match"), dict):
                pm = candidate["people_dedupe_match"]
                today_name = pm.get("matched_person_today")
                if today_name:
                    person_tag = f" — та же фигурант(а) {today_name}"
                    candidate["cross_day_entity_repeat"] = True
            if prev_date and prev_title:
                candidate["reason"] = (
                    f"{human_prefix} {prev_date} как «{prev_title[:120]}»{person_tag}."
                )
            elif prev_title:
                candidate["reason"] = f"{human_prefix} ранее как «{prev_title[:120]}»{person_tag}."
            # If we ended up here without a dedupe drop yet, enforce one —
            # UNLESS the same-day rerun exemption already accepted this
            # candidate (operational/manual rerun reading earlier in this
            # function). Same-day reruns should always re-include items
            # published earlier today so a manually-triggered second
            # digest at 14:00 doesn't lose the morning news.
            if same_day_repeat_ok or operational_repeat_ok:
                # Keep include=True and overwrite the misleading "no facts"
                # reason with the rerun rationale.
                candidate["dedupe_decision"] = "new"
                candidate["include"] = True
                candidate["reason"] = (
                    "Same-day rerun repeat is allowed while correcting today's issue."
                    if same_day_repeat_ok
                    else "Operational block repeat is allowed while it remains relevant."
                )
            elif candidate.get("dedupe_decision") not in {"drop"}:
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
                "topic_key": topic_key_for_candidate(candidate),
                "topic_lifecycle_repeat": candidate.get("topic_lifecycle_repeat") or {},
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

    story_cluster_summary = attach_story_clusters(candidates)
    attach_story_intelligence(candidates)
    intra_batch_drops = _apply_intra_batch_dedup(candidates)

    # I1: embeddings-based semantic dedup pass. Runs AFTER the
    # deterministic Jaccard/entity pass so it only sees survivors,
    # and AFTER the LLM borderline review so its borderline list is
    # an honest "even with both passes, these still look similar".
    # No-ops gracefully when OPENAI_API_KEY is unset.
    try:
        from news_digest.pipeline.semantic_dedupe import run_semantic_pass  # noqa: PLC0415

        semantic_result = run_semantic_pass(
            candidates=candidates,
            published_facts=published,
            state_dir=state_dir,
        ).to_dict()
    except Exception as exc:  # noqa: BLE001 — never block dedupe on semantic
        import logging  # noqa: PLC0415
        logging.getLogger(__name__).warning("semantic dedup pass failed: %s", exc)
        semantic_result = {"enabled": False, "error": str(exc)}
    semantic_guard = _apply_semantic_drop_guard(candidates)

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
        decision["change_type"] = final_candidate.get("change_type")
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
            "semantic_embedding_version": EMBEDDING_VERSION,
            "semantic_match_count": sum(
                1 for c in candidates
                if isinstance(c, dict) and str(c.get("semantic_dedupe_match") or "").startswith("embedding")
            ),
            "semantic_guard": semantic_guard,
            "story_clusters": story_cluster_summary,
            "intra_batch_dedup_drops": intra_batch_drops,
            "semantic_dedup_summary": semantic_result,
        },
    )

    return StageResult(not errors, "Dedupe completed." if not errors else "Dedupe completed with errors.", report_path)


_GM_BOROUGHS: frozenset[str] = frozenset({
    "salford", "stockport", "trafford", "tameside",
    "rochdale", "oldham", "wigan", "bolton", "bury",
    "altrincham", "stretford", "ashton", "eccles",
})

# Legacy substring-based media ranking. Kept ONLY as a same-tier
# tie-breaker when neither candidate's source label is in the
# category-aware registry maintained by source_selection.py (I4).
# Source labels that ARE in source_selection.SOURCE_TIER win first.
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
    # Ticket/event packaging terms. These are not the event identity and
    # should not make unrelated listings look similar.
    "ticket", "tickets", "event", "public", "sale", "venue", "premium",
    "live", "concert", "concerts",
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


def _history_window_days(candidate: dict) -> int:
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    if not contract:
        attach_editorial_contract(candidate)
        contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    section_policy = contract.get("section_policy") if isinstance(contract.get("section_policy"), dict) else {}
    raw = section_policy.get("history_window_days")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = history_window_days_for_contract(
            str(contract.get("story_type") or ""),
            str(contract.get("event_shape") or ""),
            str(contract.get("anchor_type") or ""),
        )
    return max(1, min(45, value))


def _published_item_day(item: dict) -> date | None:
    for key in ("last_published_day_london", "first_published_day_london", "published_day_london"):
        parsed = _published_day_from_history(item, key)
        if parsed:
            return parsed
    return _parse_day(item.get("published_at"))


def _within_history_window(candidate: dict, item: dict) -> bool:
    published_day = _published_item_day(item)
    if published_day is None:
        return True
    age_days = (now_london().date() - published_day).days
    return age_days <= _history_window_days(candidate)


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


def _topic_published_matches(candidate: dict, published_by_topic: dict[str, list[dict]]) -> list[dict]:
    topic_key = topic_key_for_candidate(candidate)
    if not is_specific_topic_key(topic_key):
        return []
    matches = [
        item for item in (published_by_topic.get(topic_key) or [])
        if isinstance(item, dict) and _within_history_window(candidate, item)
    ]
    if not matches:
        return []

    def _recent_key(item: dict) -> str:
        return str(
            item.get("last_published_day_london")
            or item.get("first_published_day_london")
            or item.get("published_day_london")
            or ""
        )

    matches.sort(key=_recent_key, reverse=True)
    out: list[dict] = []
    for item in matches[:5]:
        copy = dict(item)
        copy["overlap"] = 1.0
        copy["match_type"] = "topic_lifecycle"
        out.append(copy)
    return out


def _extract_borough(title: str) -> str | None:
    lowered = title.lower()
    for borough in _GM_BOROUGHS:
        if re.search(rf"\b{re.escape(borough)}\b", lowered):
            return borough
    return None


def _source_rank(source_label: str, category: str = "") -> int:
    """Lower rank = better source. I4-aware.

    Delegates to ``source_selection.source_rank`` (category + tier
    aware) for any source label that appears in that registry. Falls
    back to the legacy substring map only for the small set of media
    sources whose labels are exact substrings of the source_label —
    this keeps regressions impossible for ranges already covered by
    the test suite while the I4 registry catches everything else.
    """
    from news_digest.pipeline.source_selection import SOURCE_TIER, source_rank

    label = str(source_label or "")
    if label in SOURCE_TIER or category:
        # Registered in I4 OR we have a category to drive scoring.
        return source_rank(label, category)
    lowered = label.lower()
    for key, rank in _SOURCE_PRIORITY.items():
        if key in lowered:
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
    previous_ref: dict | None = None,
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

    if (
        previous is not None
        and str(previous.get("last_published_day_london") or "").strip() == today_london()
    ):
        return "same_story_new_facts"

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

    if previous_ref is not None and has_new_fact_signal(candidate, previous_ref):
        return "same_story_new_facts"

    if previous_ref is not None:
        diff = new_facts_diff(candidate, previous_ref)
        candidate["new_facts_diff"] = diff
        if diff.get("has_new_facts"):
            types = set(diff.get("new_fact_types") or [])
            if "stages" in types:
                return "follow_up"
            return "same_story_new_facts"

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
            prev = published_by_fp.get(str(c.get("previous_fingerprint") or ""))
        if not prev:
            prev = published_by_fp.get(str(c.get("matched_previous_fingerprint") or ""))
        if not prev:
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
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=20, max_retries=0)
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
            from news_digest.pipeline.model_routing import provider_label_for_model  # noqa: PLC0415
            record_call_from_response(
                response=response,
                stage="dedupe_review",
                provider=provider_label_for_model(model),
                model=model,
                prompt_name="dedupe_review",
                messages=[
                    {"role": "system", "content": _DEDUPE_REVIEW_PROMPT},
                    {"role": "user", "content": _json.dumps(user, ensure_ascii=False)},
                ],
                max_tokens=1500,
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

    from news_digest.pipeline.model_routing import resolve_model_route  # noqa: PLC0415

    chains = resolve_model_route(
        "dedupe_review",
        provider_override=provider_override,
        base_url_override=os.environ.get("LLM_BASE_URL", "").strip(),
        model_override=os.environ.get("LLM_MODEL", "").strip(),
    )
    from news_digest.pipeline import provider_health  # noqa: PLC0415
    decisions: list[dict] = []
    for step in chains:
        if not step.api_key:
            continue
        if provider_health.is_dead(step.provider):
            logger.info(
                "Dedupe review: skipping %s — circuit breaker tripped earlier this run.",
                step.provider_label,
            )
            continue
        decisions = _call_dedupe_review_llm(pairs, step.api_key, step.base_url, step.model)
        if decisions:
            provider_health.record_success(step.provider)
            break
        provider_health.record_failure(step.provider)
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


# Named-entity dedup signature. Catches stories like:
#   "Labour allows Andy Burnham to run for selection"
#   "Burnham makes his move with a Makerfield gambit"
#   "Burnham out, Reform in?"
# These are about the same political event but have only 1-2 tokens in
# common — Jaccard on tokens misses them. By extracting capitalised
# 2-word person names and unique single tokens like "Burnham" /
# "Makerfield" we can detect the shared subject and dedupe.
_ENTITY_RE = re.compile(
    r"\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?)\b"
)
# Drop generic English capitalised words that aren't entities but appear
# in many headlines (start-of-sentence words, generic place mentions).
# Note: "city" and "united" are intentionally NOT here — Man City / Man
# United headlines often drop the prefix ("City unveil new kit", "United
# sign Mainoo"), and we need the bare token to match across BBC/MEN
# coverage of the same story.
_ENTITY_STOPWORDS = frozenset({
    "manchester", "greater", "police", "council", "labour", "tory",
    "conservative", "reform", "court", "the", "new", "what", "how", "why",
    "when", "where", "labour", "liberal", "democrats", "green", "party",
    "and", "for", "with", "from", "about", "after", "before", "during",
    "today", "yesterday", "weekend", "week", "month", "year",
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
    "north", "south", "east", "west", "central", "north-west", "north-east",
    # Generic event/category words are not distinctive entities. Treating
    # "Festival" as a strong shared entity made Manchester Flower Festival
    # collapse into unrelated Manchester International/Folk/Jazz Festival
    # items even with zero title-token overlap.
    "event", "events", "festival", "festivals", "show", "trail", "tour",
    "workshop", "workshops", "market", "markets", "tickets", "ticket",
    "live", "concert", "concerts", "venue", "premium", "film", "orchestra",
    "tribute", "future", "vintage",
    # Generic news verbs that aren't entities even if capitalised at
    # start of headline.
    "premier", "league", "premier league",
})


def _title_entities(title: str) -> frozenset[str]:
    """Extract distinctive proper-noun entities (people, places, orgs).

    Returns lowercased entities, including 2-word names ("andy burnham"),
    so "Burnham" and "Andy Burnham" are both captured.
    """
    text = _TICKETMASTER_SUFFIX_RE.sub("", str(title or ""))
    text = _FEAT_SUFFIX_RE.sub("", text)
    entities = set()
    for m in _ENTITY_RE.finditer(text):
        name = m.group(1).lower()
        name_words = name.split()
        # Include both the full match and individual words for partial overlap.
        if name not in _ENTITY_STOPWORDS and any(word not in _ENTITY_STOPWORDS for word in name_words):
            entities.add(name)
        for word in name_words:
            if word not in _ENTITY_STOPWORDS and len(word) >= 4:
                entities.add(word)
    return frozenset(entities)


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


_MARKET_LISTING_RE = re.compile(r"\b(?:market|car boot|makers market|artisan market|flea market)\b", re.IGNORECASE)
_GENERIC_MARKET_TITLE_RE = re.compile(r"\b(?:casual trading|market|markets|what'?s on|events?)\b", re.IGNORECASE)


def _is_market_listing(candidate: dict) -> bool:
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    return bool(_MARKET_LISTING_RE.search(blob))


def _market_identity_tokens(candidate: dict) -> set[str]:
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("source_label", "title", "summary", "evidence_text")
    ).lower()
    tokens: set[str] = set()
    for phrase in re.findall(
        r"\b(?:new smithfield|bowlee|barton|burnage|altrincham|northern quarter|"
        r"stockport|urmston|chorlton|levenshulme|first street|makers?|artisan|"
        r"aerodrome|community park|market house)\b",
        text,
    ):
        tokens.add(re.sub(r"\s+", " ", phrase).strip())
    source_label = str(candidate.get("source_label") or "").strip().lower()
    if source_label and not _GENERIC_MARKET_TITLE_RE.fullmatch(source_label):
        tokens.add(source_label)
    return tokens


def _distinct_market_listing_pair(first: dict, second: dict) -> bool:
    if not (_is_market_listing(first) and _is_market_listing(second)):
        return False
    first_ids = _market_identity_tokens(first)
    second_ids = _market_identity_tokens(second)
    if not first_ids or not second_ids:
        return False
    return first_ids.isdisjoint(second_ids)


_EVENT_DEDUPE_BLOCKS = frozenset({
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
})
_EVENT_DEDUPE_CATEGORIES = frozenset({
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "diaspora_events",
})
_RANGE_DATE_RE = re.compile(r"(?:\d{1,2}\s*[–—-]\s*\d{1,2}|(?:to|until)\s+\d{1,2})", re.IGNORECASE)
_PAGE_CHROME_VENUES = frozenset({
    "palace theatre",
    "opera house",
    "manchester opera house",
})


def _event_candidate_quality(candidate: dict) -> int:
    """Tie-break duplicate event listings by usable event facts.

    Source authority still wins first. This only decides between sources
    with the same authority score, where "first URL wins" can keep a
    page-chrome scrape over a cleaner organiser/news page.
    """
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block not in _EVENT_DEDUPE_BLOCKS and category not in _EVENT_DEDUPE_CATEGORIES:
        return 0

    event = candidate.get("event")
    if not isinstance(event, dict):
        event = {}
    title = str(candidate.get("title") or "")
    summary = str(candidate.get("summary") or "")
    lead = str(candidate.get("lead") or "")
    evidence = str(candidate.get("evidence_text") or "")
    front_text = f"{title} {summary} {lead}".lower()

    score = 0
    event_name = str(event.get("event_name") or "").strip()
    if len(event_name) >= 8 and event_name.lower() not in {"the", "event", "events"}:
        score += 4
    elif event_name:
        score -= 4

    date_text = str(event.get("date_text") or "")
    if event.get("date_start"):
        score += 2
    if event.get("date_end") or _RANGE_DATE_RE.search(date_text):
        score += 3
    if event.get("borough"):
        score += 1
    if event.get("price"):
        score += 2
    if event.get("booking_url"):
        score += 1

    venue = str(event.get("venue") or "").strip()
    if venue:
        score += 2
        if venue.lower() in _PAGE_CHROME_VENUES and venue.lower() not in front_text:
            score -= 5

    if len(summary) >= 140:
        score += 1
    if len(evidence) >= 700:
        score += 1
    return score


def _prefer_dedupe_candidate(first: dict, second: dict, first_rank: int, second_rank: int) -> bool:
    """Return True when the first candidate should survive a duplicate pair."""
    if first_rank != second_rank:
        return first_rank < second_rank
    first_quality = _event_candidate_quality(first)
    second_quality = _event_candidate_quality(second)
    if first_quality != second_quality:
        return first_quality > second_quality
    return True


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
    for candidate in included:
        attach_editorial_contract(candidate)
    n = len(included)

    to_drop: dict[int, dict] = {}

    for i in range(n):
        if i in to_drop:
            continue
        ci = included[i]
        tokens_i = _title_tokens(str(ci.get("title") or ""))
        entities_i = _title_entities(str(ci.get("title") or ""))
        borough_i = _extract_borough(str(ci.get("title") or ""))
        block_i = str(ci.get("primary_block") or "")
        group_i = _dedup_block_group(block_i)
        is_event_ticket_group = group_i == _dedup_block_group("ticket_radar")
        topic_i = topic_key_for_candidate(ci)
        cluster_i = str(ci.get("story_cluster_key") or "")
        rank_i = _source_rank(
            str(ci.get("source_label") or ""),
            str(ci.get("category") or ""),
        )

        for j in range(i + 1, n):
            if j in to_drop:
                continue
            cj = included[j]
            if _dedup_block_group(str(cj.get("primary_block") or "")) != group_i:
                continue

            cluster_j = str(cj.get("story_cluster_key") or "")
            if cluster_i and cluster_i == cluster_j:
                rank_j = _source_rank(
                    str(cj.get("source_label") or ""),
                    str(cj.get("category") or ""),
                )
                if _prefer_dedupe_candidate(ci, cj, rank_i, rank_j):
                    to_drop[j] = {
                        "kept_index": i,
                        "overlap": 1.0,
                        "story_cluster_key": cluster_i,
                    }
                else:
                    to_drop[i] = {
                        "kept_index": j,
                        "overlap": 1.0,
                        "story_cluster_key": cluster_i,
                    }
                    break
                continue

            topic_j = topic_key_for_candidate(cj)
            if topic_i and topic_i == topic_j and is_specific_topic_key(topic_i):
                rank_j = _source_rank(
                    str(cj.get("source_label") or ""),
                    str(cj.get("category") or ""),
                )
                if _prefer_dedupe_candidate(ci, cj, rank_i, rank_j):
                    to_drop[j] = {
                        "kept_index": i,
                        "overlap": 1.0,
                        "topic_key": topic_i,
                    }
                else:
                    to_drop[i] = {
                        "kept_index": j,
                        "overlap": 1.0,
                        "topic_key": topic_i,
                    }
                    break
                continue

            borough_j = _extract_borough(str(cj.get("title") or ""))
            if borough_i != borough_j:
                continue  # different boroughs = different stories
            if _distinct_market_listing_pair(ci, cj):
                continue  # same generic listing wording, different market/venue

            tokens_j = _title_tokens(str(cj.get("title") or ""))
            entities_j = _title_entities(str(cj.get("title") or ""))

            # FAST PATH — shared distinctive entity. If two titles both
            # mention the same proper noun (Burnham, Mainoo, Manchester
            # United, Trafford Centre, City), treat as same story even
            # when token Jaccard is low. Catches the classic political-
            # story case where each headline picks different verbs around
            # the same subject.
            shared_entities = entities_i & entities_j
            if shared_entities and not is_event_ticket_group:
                # "Strong" signal — any of:
                #  - multi-word entity ("Andy Burnham", "Trafford Centre")
                #  - 5+ char single word ("Mainoo", "Burnham")
                #  - short word (4+ chars) that appears INSIDE a multi-word
                #    entity in either title ("city" inside "manchester city")
                multi_word_i = [e for e in entities_i if " " in e]
                multi_word_j = [e for e in entities_j if " " in e]
                strong = False
                for e in shared_entities:
                    if " " in e or len(e) >= 5:
                        strong = True
                        break
                    if len(e) >= 4:
                        for mw in multi_word_i + multi_word_j:
                            if e in mw.split():
                                strong = True
                                break
                    if strong:
                        break
                if strong:
                    rank_j = _source_rank(
                        str(cj.get("source_label") or ""),
                        str(cj.get("category") or ""),
                    )
                    if _prefer_dedupe_candidate(ci, cj, rank_i, rank_j):
                        to_drop[j] = {"kept_index": i, "overlap": 0.0, "shared_entity": ",".join(sorted(shared_entities))[:60]}
                    else:
                        to_drop[i] = {"kept_index": j, "overlap": 0.0, "shared_entity": ",".join(sorted(shared_entities))[:60]}
                        break
                    continue

            union = tokens_i | tokens_j
            if not union or len(tokens_i) < 3 or len(tokens_j) < 3:
                continue
            overlap = len(tokens_i & tokens_j) / len(union)
            if overlap < 0.40:
                continue

            rank_j = _source_rank(
                str(cj.get("source_label") or ""),
                str(cj.get("category") or ""),
            )
            if _prefer_dedupe_candidate(ci, cj, rank_i, rank_j):
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
                "topic_key": drop_context.get("topic_key") or "",
                "story_cluster_key": drop_context.get("story_cluster_key") or "",
                "reason": c["reason"],
            }
        )
    return drops


_SEMANTIC_ONLY_DROP_CAP_SHARE = 0.20
_SEMANTIC_ONLY_DROP_MIN_CAP = 3


def _apply_semantic_drop_guard(candidates: list[dict]) -> dict[str, object]:
    """Fail-open guard for embedding-only cross-day drops.

    Title/fingerprint dedupe is mature. Embedding-only dedupe is useful but
    riskier, so if it would remove a large share of the eligible issue we
    keep those candidates and report the guard instead of shrinking the day.
    """
    eligible = [
        c for c in candidates
        if isinstance(c, dict)
        and str(c.get("primary_block") or "") not in {"weather", "transport"}
    ]
    semantic_only_drops = [
        c for c in eligible
        if c.get("semantic_dedupe_match") == "embedding_only"
        and c.get("dedupe_decision") == "drop"
        and c.get("change_type") in {"no_change", "same_story_rehash"}
    ]
    limit = max(_SEMANTIC_ONLY_DROP_MIN_CAP, int(len(eligible) * _SEMANTIC_ONLY_DROP_CAP_SHARE))
    if len(semantic_only_drops) <= limit:
        return {
            "triggered": False,
            "embedding_only_drops": len(semantic_only_drops),
            "limit": limit,
        }

    restored_candidates: list[dict[str, object]] = []
    for candidate in semantic_only_drops:
        restored_candidates.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "source_label": candidate.get("source_label"),
                "primary_block": candidate.get("primary_block"),
                "semantic_match_sim": candidate.get("semantic_match_sim"),
                "semantic_match_fingerprint": candidate.get("semantic_match_fingerprint"),
                "previous_reason": candidate.get("reason"),
            }
        )
        candidate["include"] = True
        candidate["dedupe_decision"] = "new"
        candidate["change_type"] = "new_story"
        candidate["reason"] = (
            "Semantic dedupe guard: kept because embedding-only drops exceeded "
            "the daily safety cap; review manually."
        )
    return {
        "triggered": True,
        "embedding_only_drops": len(semantic_only_drops),
        "limit": limit,
        "restored": len(semantic_only_drops),
        "restored_candidates": restored_candidates,
    }


def _entity_tokens(title: str) -> set[str]:
    """Capitalized words and numbers from the original title — likely proper nouns."""
    return {w.lower() for w in re.findall(r"\b(?:[A-Z][a-z]{1,}|[A-Z]{2,}|\d{2,})\b", title)}


def _similar_published_titles(
    candidate: dict | str | None = None,
    normalized_title: str = "",
    original_title: str | list[dict] = "",
    published_titles: list[dict] | None = None,
) -> list[dict]:
    # Backward-compatible private helper signature used by older tests:
    # _similar_published_titles(normalized_title, original_title, published_titles)
    if published_titles is None:
        published_titles = original_title if isinstance(original_title, list) else []
        original_title = str(normalized_title or "")
        normalized_title = str(candidate or "")
        candidate = {}
    title_tokens = set(_title_tokens(original_title)) or set(normalized_title.split())
    entity_tokens = _entity_tokens(original_title)
    if len(title_tokens) < 2:
        return []
    matches: list[dict] = []
    for item in published_titles:
        previous_tokens = set(_title_tokens(str(item.get("title") or ""))) or set(
            str(item.get("normalized_title") or "").split()
        )
        if isinstance(candidate, dict) and candidate and not _within_history_window(candidate, item):
            continue
        if len(previous_tokens) < 2:
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


_SEMANTIC_MATCH_THRESHOLD = 0.38
_SEMANTIC_MATCH_HIGH_THRESHOLD = 0.55
_SEMANTIC_MIN_SHARED_ANCHORS = 2
_SEMANTIC_SKIP_BLOCKS = frozenset({
    "weather",
    "transport",
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
})


def _published_embedding(item: dict) -> list[float]:
    stored = item.get("semantic_embedding")
    version = str(item.get("semantic_embedding_version") or "")
    if (
        version == EMBEDDING_VERSION
        and isinstance(stored, list)
        and all(isinstance(v, (int, float)) for v in stored)
    ):
        return [float(v) for v in stored]
    return semantic_embedding(item)


def _semantic_published_matches(candidate: dict, published_items: list[dict]) -> list[dict]:
    block = str(candidate.get("primary_block") or "")
    if block in _SEMANTIC_SKIP_BLOCKS:
        return []
    candidate_embedding = semantic_embedding(candidate)
    candidate_anchors = anchor_tokens(candidate)
    if len(candidate_anchors) < _SEMANTIC_MIN_SHARED_ANCHORS:
        return []

    matches: list[dict] = []
    for item in published_items:
        if not isinstance(item, dict):
            continue
        if not _within_history_window(candidate, item):
            continue
        previous_block = str(item.get("primary_block") or "")
        previous_category = str(item.get("category") or "")
        candidate_category = str(candidate.get("category") or "")
        if previous_block in _SEMANTIC_SKIP_BLOCKS:
            continue
        if (
            _dedup_block_group(previous_block) != _dedup_block_group(block)
            and previous_category != candidate_category
        ):
            continue
        previous_anchors = anchor_tokens(item)
        shared_anchors = candidate_anchors & previous_anchors
        if len(shared_anchors) < _SEMANTIC_MIN_SHARED_ANCHORS:
            continue
        score = cosine_similarity(candidate_embedding, _published_embedding(item))
        if score < _SEMANTIC_MATCH_THRESHOLD:
            continue
        if score < _SEMANTIC_MATCH_HIGH_THRESHOLD and len(shared_anchors) < 3:
            continue
        matches.append(
            {
                "fingerprint": item.get("fingerprint"),
                "title": item.get("title"),
                "overlap": round(score, 3),
                "match_type": "semantic_embedding",
                "shared_anchors": sorted(shared_anchors)[:8],
            }
        )
    matches.sort(key=lambda m: float(m.get("overlap") or 0.0), reverse=True)
    return matches[:3]


def _merge_previous_matches(*match_lists: list[dict]) -> list[dict]:
    """Merge any number of match-lists (title / semantic / people),
    keeping the strongest overlap per fingerprint.
    """
    by_fp: dict[str, dict] = {}
    for match_list in match_lists:
        for match in match_list:
            fp = str(match.get("fingerprint") or "")
            if not fp:
                continue
            existing = by_fp.get(fp)
            if not existing or float(match.get("overlap") or 0.0) > float(existing.get("overlap") or 0.0):
                by_fp[fp] = dict(match)
    return sorted(by_fp.values(), key=lambda m: float(m.get("overlap") or 0.0), reverse=True)[:5]


# Cyrillic → Latin transliteration so "Эрика" matches "Erica" cross-day
# when one outlet runs the story in Russian and another in English.
# Approximate ISO-9 / BGN-style; we only need enough fidelity that token
# overlap > 1 — not a publishable transliteration.
_CYR_TO_LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
    "ё": "e", "ж": "zh", "з": "z", "и": "i", "й": "i", "к": "k",
    "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts",
    "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
}


def _transliterate_cyr(token: str) -> str:
    return "".join(_CYR_TO_LAT.get(ch, ch) for ch in token)


def _normalise_person_tokens(name: str) -> set[str]:
    """Lowercase + drop short particles. Returns the set of tokens used
    to compare two people-name strings.

    Two scripts can describe the same person — "Эрика де Соуза Корреа"
    (Russian) and "Erica de Souza Correa" (English) refer to the same
    victim. We strip Russian case suffixes, then also emit a
    transliterated Latin copy of every Cyrillic-origin token so the set
    overlap works across languages.
    """
    parts: set[str] = set()
    for raw in re.split(r"\s+", name.strip().lower()):
        token = raw.strip("., '\"")
        if not token or len(token) < 3:
            continue
        if token in {"de", "van", "von", "der", "of", "the", "la", "le",
                     "del", "di", "де", "фон", "ван", "ди", "ла", "ле"}:
            continue
        # Strip trailing case suffix — Cyrillic OR Latin vowel.
        # Both 'Эрика' and 'Erica' lose their trailing -а/-a so they
        # land on the same stem cross-language.
        for suf in ("ой", "ом", "ах", "ам", "ами"):
            if token.endswith(suf) and len(token) > 4:
                token = token[: -len(suf)]
                break
        else:
            if len(token) > 4 and token[-1] in "аеиоуыэюяёaeiouy":
                token = token[:-1]
        parts.add(token)
        # Cross-language transliteration: if the token contains Cyrillic
        # letters, emit a Latin copy so it can overlap with English
        # forms of the same name. Also emit a k→c variant because real
        # English spelling often uses 'c' where strict transliteration
        # gives 'k' (Корреа → Korrea vs Correa).
        if any(ch in _CYR_TO_LAT for ch in token):
            lat = _transliterate_cyr(token)
            if lat and lat != token:
                if len(lat) > 4 and lat[-1] in "aeiouy":
                    lat = lat[:-1]
                parts.add(lat)
                if "k" in lat:
                    parts.add(lat.replace("k", "c"))
        else:
            # Latin-input token: also emit k↔c alternates so an English
            # name with 'k' lines up with a Cyrillic-transliterated 'c'.
            if "k" in token:
                parts.add(token.replace("k", "c"))
            elif "c" in token:
                parts.add(token.replace("c", "k"))
    return parts


def _people_published_matches(
    candidate: dict, published_items: list[dict],
) -> list[dict]:
    """Find published items that share a real person with the candidate.

    Two people-names match when their normalised token sets share at
    least 2 tokens — enough to align "Эрика де Соуза Корреа" with
    "Эрики де Соуза Корреа" (different cases) or "Erica de Souza
    Correa" with "Erica Souza" (truncated). Single common surnames
    like "Smith" never reach the threshold alone.
    """
    block = str(candidate.get("primary_block") or "")
    if block in _SEMANTIC_SKIP_BLOCKS:
        return []
    entities = candidate.get("entities")
    if not isinstance(entities, dict):
        return []
    candidate_names = entities.get("people") or []
    if not candidate_names:
        return []
    candidate_token_sets = [_normalise_person_tokens(name) for name in candidate_names]
    candidate_token_sets = [tokens for tokens in candidate_token_sets if len(tokens) >= 2]
    if not candidate_token_sets:
        return []

    matches: list[dict] = []
    for item in published_items:
        if not isinstance(item, dict):
            continue
        if not _within_history_window(candidate, item):
            continue
        previous_entities = item.get("entities")
        if not isinstance(previous_entities, dict):
            continue
        previous_names = previous_entities.get("people") or []
        if not previous_names:
            continue
        best_shared: tuple[str, str, int] | None = None
        for cand_name, cand_tokens in zip(candidate_names, candidate_token_sets):
            for prev_name in previous_names:
                prev_tokens = _normalise_person_tokens(prev_name)
                if len(prev_tokens) < 2:
                    continue
                shared = cand_tokens & prev_tokens
                if len(shared) >= 2 and (best_shared is None or len(shared) > best_shared[2]):
                    best_shared = (cand_name, prev_name, len(shared))
        if best_shared is None:
            continue
        # Score scales 0.78–0.95 depending on token-overlap strength so
        # the people match competes fairly with semantic overlap when
        # merge_previous_matches dedupes by fingerprint.
        score = min(0.95, 0.75 + 0.05 * best_shared[2])
        matches.append(
            {
                "fingerprint": item.get("fingerprint"),
                "title": item.get("title"),
                "overlap": round(score, 3),
                "match_type": "people_entity",
                "matched_person_today": best_shared[0],
                "matched_person_previously": best_shared[1],
                "shared_tokens": best_shared[2],
            }
        )
    matches.sort(key=lambda m: float(m.get("overlap") or 0.0), reverse=True)
    return matches[:3]
