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

        if not candidate.get("reason"):
            errors.append(f"Candidate #{index} is missing reason.")

        decisions.append(
            {
                "fingerprint": fingerprint,
                "title": candidate.get("title"),
                "decision": candidate.get("dedupe_decision"),
                "reason": candidate.get("reason"),
                "matched_previous_fingerprint": candidate.get("matched_previous_fingerprint"),
                "carry_over_label": candidate.get("carry_over_label"),
                "similar_previous": similar_previous,
            }
        )

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
