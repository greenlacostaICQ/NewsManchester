from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import html
import logging
from pathlib import Path
import re

logger = logging.getLogger(__name__)

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    SECTION_MAX_ITEMS,
    SECTION_MAX_PER_SOURCE,
    is_placeholder_practical_angle,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.toponyms import restore_english_toponyms
from news_digest.pipeline.place_names import preserve_place_names


MODEL_WRITTEN_CATEGORIES = {"media_layer", "gmp", "council", "public_services", "food_openings"}
REQUIRE_DRAFT_LINE_CATEGORIES = MODEL_WRITTEN_CATEGORIES | {
    "transport",
    "venues_tickets",
    "russian_speaking_events",
    "culture_weekly",
    "football",
    "tech_business",
    "city_news",
}
# Categories that should render as 350–450 char multi-sentence cards rather
# than single-line headlines. Transport / weather / billet are explicitly
# excluded — they're shorter by design.
LONG_FORMAT_CATEGORIES = {
    "media_layer",
    "gmp",
    "council",
    "public_services",
    "city_news",
    "food_openings",
    "tech_business",
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "football",
}
LONG_FORMAT_MIN_CHARS = 150
LONG_FORMAT_MIN_SENTENCES = 2
SHORT_TICKET_BLOCKS = {"ticket_radar", "outside_gm_tickets"}
TODAY_FOCUS_SECTION = "Что важно сегодня"
TODAY_FOCUS_BACKFILL_SECTIONS = (
    "Общественный транспорт сегодня",
    "Что произошло за 24 часа",
    "Городской радар",
)
TODAY_FOCUS_BACKFILL_TARGET = 2
TODAY_FOCUS_MIN_SOURCE_REMAINING = {
    "Что произошло за 24 часа": 1,
}
_BAD_EDITORIAL_PROSE_MARKERS = (
    "ticket office",
    "слот входа",
    "госпитальн",
    "кадровый и дисциплинарный кейс",
    "заметный кейс",
    "новая фаза истории",
    "сетка влияния",
    "следить компаниям",
    "business-impact",
    "лучше взять зонт",
    "лучше прихватить зонт",
    "не забудьте зонт",
    "прихватите зонт",
    "live alert",
    "live disruption",
    "forecast",
    "attractions",
    "highlights",
    "matchday",
    "check before",
    "опубликовал важное обновление",
    "появилось новое обновление",
    "судебное обновление",
    "новое судебное",
    "футбольное обновление",
    "перепроверьте",
    "убедитесь сами",
    "читайте подробнее",
    "подробности ниже",
    # PR filler endings from LLM padding
    "обогатит",
    "центр притяжения",
    "новая достопримечательность",
    "другие детали не сообщаются",
    "подробности не раскрываются",
    "остаётся нерешённой",
    "привлечёт внимание",
    "вступило в силу.",
    "билеты и даты уточняйте",
    "время и дату уточняйте",
    "дату и время уточняйте",
    "уточните даты",
)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path
    draft_path: Path


def _title_line() -> str:
    now = now_london()
    return f"<b>Greater Manchester Brief — {now.strftime('%Y-%m-%d, %H:%M')}</b>"


def _normalize_text_key(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9а-яё]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _summary_is_useful(summary: str, headline: str) -> bool:
    cleaned = str(summary or "").strip()
    if not cleaned:
        return False
    if _normalize_text_key(cleaned) == _normalize_text_key(headline):
        return False
    if len(cleaned) < 28:
        return False
    return True


def _backfill_today_focus(
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
) -> int:
    if sections.get(TODAY_FOCUS_SECTION):
        return 0

    moved = 0
    sections.setdefault(TODAY_FOCUS_SECTION, [])
    section_sources.setdefault(TODAY_FOCUS_SECTION, [])
    section_scores.setdefault(TODAY_FOCUS_SECTION, [])

    for source_section in TODAY_FOCUS_BACKFILL_SECTIONS:
        lines = sections.get(source_section) or []
        sources = section_sources.get(source_section) or []
        scores = section_scores.get(source_section) or []
        min_remaining = TODAY_FOCUS_MIN_SOURCE_REMAINING.get(source_section, 0)
        while lines and moved < TODAY_FOCUS_BACKFILL_TARGET and len(lines) > min_remaining:
            sections[TODAY_FOCUS_SECTION].append(lines.pop(0))
            section_sources[TODAY_FOCUS_SECTION].append(sources.pop(0) if sources else "")
            section_scores[TODAY_FOCUS_SECTION].append(scores.pop(0) if scores else 0.0)
            moved += 1
        sections[source_section] = lines
        section_sources[source_section] = sources
        section_scores[source_section] = scores
        if moved >= TODAY_FOCUS_BACKFILL_TARGET:
            break

    if not sections.get(TODAY_FOCUS_SECTION):
        sections.pop(TODAY_FOCUS_SECTION, None)
        section_sources.pop(TODAY_FOCUS_SECTION, None)
        section_scores.pop(TODAY_FOCUS_SECTION, None)
    return moved


def _contains_cyrillic(value: str) -> bool:
    return bool(re.search(r"[а-яё]", str(value or ""), flags=re.IGNORECASE))


def _looks_like_untranslated_english(value: str) -> bool:
    text = str(value or "").strip()
    if not text or _contains_cyrillic(text):
        return False
    latin_words = re.findall(r"[A-Za-z][A-Za-z'’-]+", text)
    if len(latin_words) < 8:
        return False
    stopwords = {
        "the", "and", "for", "with", "from", "after", "following", "into", "across",
        "will", "have", "has", "had", "that", "this", "they", "their", "about", "said",
        "says", "into", "over", "under", "following", "response", "operators",
    }
    stopword_hits = sum(1 for word in latin_words if word.lower() in stopwords)
    return stopword_hits >= 2


def _source_anchor(source_url: str, source_label: str) -> str:
    return f'<a href="{html.escape(source_url, quote=True)}">{html.escape(source_label)}</a>'


def _attach_source_anchor(line: str, source_url: str, source_label: str) -> str:
    text = str(line or "").strip()
    if "<a " in text.lower():
        return text
    label = str(source_label or "").strip()
    label_lower = label.lower()
    # Normalise by stripping trailing punctuation before checking — handles both
    # "...Met Office" and "...Met Office." (period added by LLM or practical angle).
    if label and text.lower().rstrip(" .").endswith(label_lower):
        base = text.rstrip(" .")
        # Only strip trailing spaces (not periods) so the sentence period before
        # the label is preserved: "...зонт обязателен. Met Office" → "...зонт обязателен."
        text = base[: len(base) - len(label)].rstrip(" ")
    return f"{text} {_source_anchor(source_url, source_label)}".strip()


_SUMMER_MONTHS = frozenset({6, 7, 8})
_HEAVY_SNOW_PATTERN = re.compile(
    r"\b(?:heavy\s+snow|blizzard|snowstorm|snowfall|снегопад|метель|снежная\s+буря)\b",
    re.IGNORECASE,
)
_EXTREME_TEMP_PATTERN = re.compile(r"\b([1-9]\d)\s*°[Cc]\b")
_EVENT_BLOCKS = {"weekend_activities", "next_7_days", "ticket_radar", "outside_gm_tickets", "russian_events", "future_announcements"}
_MONTHS = {
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


def _sanity_flags(candidate: dict, line: str) -> list[str]:
    flags: list[str] = []
    month = now_london().month
    if month in _SUMMER_MONTHS and _HEAVY_SNOW_PATTERN.search(line):
        flags.append("Seasonal impossibility: heavy snow in summer month.")
    for m in _EXTREME_TEMP_PATTERN.finditer(line):
        temp = int(m.group(1))
        if temp > 38 or temp < 0 and month in _SUMMER_MONTHS:
            flags.append(f"Implausible Manchester temperature: {m.group()}.")
    return flags


def _parse_day(value: object) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
    except ValueError:
        return None


def _future_date_signal(text: str) -> bool:
    today = now_london().date()
    lowered = str(text or "").lower()
    dates: list[date] = []
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
            dates.append(date(year, month, int(day_raw)))
        except ValueError:
            continue
    return bool(dates and max(dates) >= today)


def _is_expired_event_candidate(candidate: dict, line: str = "") -> bool:
    if str(candidate.get("primary_block") or "") not in _EVENT_BLOCKS:
        return False
    event_day = _parse_day(candidate.get("published_at"))
    if not event_day or event_day >= now_london().date():
        return False
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("source_url"),
            line,
        )
    )
    return not _future_date_signal(text)


# Canonical money normaliser: maps £150m, £150 million, £150млн,
# £150 миллионов, £150мн all to (150.0, "m"). Used by the hallucination
# check so the writer doesn't reject its own LLM lines that translate
# "£230m" to "£230млн" — the previous string comparison flagged those
# as missing from evidence and silently lost real leads (Wigan £230m,
# Metrolink £150m, council £11.8m, …).
_MONEY_TOKEN_RE = re.compile(
    r"£\s*(\d[\d.,]*)\s*"
    r"(k|m|bn|млн|млрд|тыс|миллионов?|миллиардов?|тысяч)?",
    re.IGNORECASE,
)
_UNIT_MAP = {
    "":         "",
    "k":        "k",
    "тыс":      "k",
    "тысяч":    "k",
    "m":        "m",
    "млн":      "m",
    "миллион":  "m",
    "миллионов":"m",
    "bn":       "bn",
    "млрд":     "bn",
    "миллиард": "bn",
    "миллиардов":"bn",
}


def _normalize_money(amount_str: str, unit_str: str) -> tuple[float, str] | None:
    """Return (amount, canonical_unit) or None if the token doesn't parse.
    Handles £230m / £230млн / £230 million / £230 миллионов as the same
    canonical (230.0, 'm')."""
    s = amount_str.replace(",", ".").replace(" ", "")
    try:
        amount = float(s)
    except ValueError:
        return None
    unit_key = (unit_str or "").lower().strip()
    canonical = _UNIT_MAP.get(unit_key, "")
    return (amount, canonical)


def _extract_money(text: str) -> set[tuple[float, str]]:
    """Pull every £-amount out of `text` as a set of canonical tuples."""
    found: set[tuple[float, str]] = set()
    for m in _MONEY_TOKEN_RE.finditer(text or ""):
        norm = _normalize_money(m.group(1), m.group(2) or "")
        if norm is not None:
            found.add(norm)
    return found


def _money_amounts_match(line_amount: float, evidence_amounts: set[tuple[float, str]]) -> bool:
    """Return True if `line_amount` reasonably equals any evidence amount.

    Allowed editorial freedom (and ONLY this):
      1. Exact match (any unit).
      2. LLM rounded a *fractional* evidence value to the nearest whole.
         Only fires when the evidence value has a non-zero fractional
         part. "£11.8m → £12 млн" passes; "£100m → £105 млн" doesn't,
         because £100m has no fraction to round.
    """
    for ea, _ in evidence_amounts:
        if abs(line_amount - ea) < 0.01:
            return True
        has_fraction = abs(ea - round(ea)) > 0.001
        if has_fraction and abs(line_amount - round(ea)) < 0.01:
            return True
    return False


def _hallucination_flags(candidate: dict, line: str) -> list[str]:
    """Flag £-sums in `line` that don't appear in upstream
    evidence/title/summary/lead. Normalised comparison via _extract_money
    so £230m ↔ £230млн match; also accepts editorial rounding via
    _money_amounts_match (so £11.8m → £12 млн doesn't trip).
    """
    evidence_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    )
    flags: list[str] = []
    line_amounts = _extract_money(line)
    if not line_amounts:
        return flags
    evidence_amounts = _extract_money(evidence_blob)
    for amount, unit in line_amounts:
        if (amount, unit) in evidence_amounts:
            continue
        if _money_amounts_match(amount, evidence_amounts):
            continue
        flags.append(f"Pound amount £{amount:g}{unit} not present in evidence_text.")
        break
    return flags


# Source-tier weights for «Городской радар» ordering. Higher = surfaces first.
# Cap of 12 truncates the tail, so anything below ~30 is effectively cut.
_CITY_WATCH_SOURCE_WEIGHTS: dict[str, int] = {
    # GM-wide political authority — highest editorial priority.
    "GMCA": 120,
    "Manchester Council": 100,
    "Salford Council": 95,
    "Stockport Council": 95,
    "Trafford Council": 95,
    "Oldham Council": 90,
    "Rochdale Council": 90,
    "Bolton Council": 90,
    "Bury Council": 90,
    "Tameside Council": 90,
    "Wigan Council": 90,
    # Independent local journalism with reporting (not press releases).
    "The Mill": 110,
    "The Manc": 85,
    "Manchester Mill": 110,
    "I Love Manchester": 60,
    # NHS / emergency services.
    "GMMH": 70,
    "GMP": 80,
    # Universities — institutional PR, usually low signal for residents.
    "University of Manchester": 25,
    "University of Salford": 25,
    "Manchester Metropolitan University": 25,
}
_CITY_WATCH_DEFAULT_WEIGHT = 50


def _city_watch_score(candidate: dict) -> float:
    """Editorial priority for «Городской радар» (higher = surfaces first).

    Combines source-tier weight with content signals: presence of GM boroughs,
    £-sums, dates, named people. Penalises academic / generic press-release
    language so university feeds don't crowd out actual city news.
    """
    source_label = str(candidate.get("source_label") or "").strip()
    score = float(_CITY_WATCH_SOURCE_WEIGHTS.get(source_label, _CITY_WATCH_DEFAULT_WEIGHT))

    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    ).lower()

    # Borough mentions — real GM signal.
    borough_hits = sum(
        1
        for borough in ("manchester", "salford", "trafford", "stockport", "tameside",
                         "oldham", "rochdale", "bury", "bolton", "wigan")
        if borough in blob
    )
    score += min(borough_hits, 3) * 5

    # Concrete signals readers care about: £ amounts, dates, percentages.
    if re.search(r"£\s*\d", blob):
        score += 15
    if re.search(r"\b(?:january|february|march|april|may|june|july|august|"
                 r"september|october|november|december|января|февраля|марта|"
                 r"апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b", blob):
        score += 8
    if re.search(r"\b\d{1,3}%\b|\b\d{4,6}\s+(?:residents|people|жител)", blob):
        score += 10

    # Academic / generic PR markers — drop these to the bottom.
    academic_markers = (
        "research", "researcher", "электрон", "graphene", "lecture",
        "vice-chancellor", "chancellor", "academic", "professor", "phd",
        "campus", "students meet", "submit your taught course",
        "datadobi", "storage optimisation",
    )
    if any(marker in blob for marker in academic_markers):
        score -= 35

    # Generic council PR with no specific news beat.
    generic_pr_markers = (
        "named greater manchester town of culture",
        "community champions",
        "capital grant winners",
        "parting gifts",
        "celebration",
        "tea party",
        "lord mayor",
    )
    if any(marker in blob for marker in generic_pr_markers):
        score -= 10

    # Title length under 50 chars often means slogany PR header.
    title = str(candidate.get("title") or "")
    if len(title) < 30:
        score -= 5

    # Evidence depth — long evidence_text usually means a real article.
    evidence_len = len(str(candidate.get("evidence_text") or ""))
    if evidence_len >= 600:
        score += 10
    elif evidence_len < 200:
        score -= 8

    return score


def _draft_line_quality_errors(candidate: dict, line: str) -> list[str]:
    text = str(line or "").strip()
    errors: list[str] = []
    if not text:
        return ["Missing draft_line."]
    if not text.startswith("• "):
        errors.append("draft_line must start with bullet marker.")
    if "<a " in text.lower():
        errors.append("draft_line must not include source anchor HTML.")
    if re.search(r"\*\*.+?\*\*", text) or re.search(r"(?<!\*)\*(?!\s).+?(?<!\s)\*(?!\*)", text):
        errors.append("draft_line must not use Markdown emphasis markers.")
    if not _contains_cyrillic(text):
        errors.append("draft_line must contain normal Russian prose.")
    normalized = re.sub(r"\s+", " ", text)
    if len(normalized) < 45:
        errors.append("draft_line is too short to be a self-contained item.")
    category = str(candidate.get("category") or "").strip()
    sentence_count = len(re.findall(r"[.!?]", text))
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and sentence_count < 1:
        errors.append("draft_line must contain at least one complete sentence.")
    block_key = str(candidate.get("primary_block") or "").strip()
    if category in LONG_FORMAT_CATEGORIES and block_key not in SHORT_TICKET_BLOCKS:
        if len(normalized) < LONG_FORMAT_MIN_CHARS:
            errors.append(
                f"draft_line for long-format category needs ≥{LONG_FORMAT_MIN_CHARS} chars (got {len(normalized)})."
            )
        if sentence_count < LONG_FORMAT_MIN_SENTENCES:
            errors.append(
                f"draft_line for long-format category needs ≥{LONG_FORMAT_MIN_SENTENCES} sentences (got {sentence_count})."
            )
    lowered = text.lower()
    for marker in _BAD_EDITORIAL_PROSE_MARKERS:
        if marker in lowered:
            errors.append(f"draft_line contains bad editorial prose marker: {marker}.")
            break
    errors.extend(_sanity_flags(candidate, text))
    errors.extend(_hallucination_flags(candidate, text))
    # Thin-evidence + long-draft = LLM padded a teaser into a vague card.
    # We only check long-format categories (city news / events / business etc.) —
    # transport / weather are intentionally short. Football already has its own
    # "return draft_line=\"\"" rule in the prompt.
    if category in LONG_FORMAT_CATEGORIES and category != "football":
        evidence = str(candidate.get("evidence_text") or candidate.get("summary") or candidate.get("lead") or "")
        evidence_meaningful = len(re.sub(r"\s+", " ", evidence).strip())
        draft_len = len(normalized)
        # Concrete signals: numbers, £-amount, date, capitalised proper noun pair.
        has_concrete = bool(
            re.search(r"\b\d{2,}", text)
            or re.search(r"£\s*\d", text)
            or re.search(r"\b(?:января|февраля|марта|апреля|мая|июня|июля|"
                         r"августа|сентября|октября|ноября|декабря)\b", text, re.IGNORECASE)
            or re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", text)
        )
        if evidence_meaningful < 150 and draft_len > 220 and not has_concrete:
            errors.append(
                f"draft_line padded from thin evidence "
                f"(evidence={evidence_meaningful}c, draft={draft_len}c, no concrete signal)."
            )
    return errors


def write_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "writer_report.json"

    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])
    sections = {heading: [] for heading in PRIMARY_BLOCKS.values()}
    # Parallel list of source_labels per section (same indices as sections[*]).
    # Used to apply SECTION_MAX_PER_SOURCE caps at render time.
    section_sources: dict[str, list[str]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    # Editorial priority score per line — populated only for «Городской радар»
    # where we re-sort candidates before truncation so the cap drops the
    # weakest items (PR releases) rather than whatever happened to come last.
    section_scores: dict[str, list[float]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    errors: list[str] = []
    warnings: list[str] = []
    quality_counts = {
        "included_candidates": 0,
        "rendered_candidates": 0,
        "blocked_for_quality": 0,
        "held_for_editorial_quality": 0,
        "dropped_missing_draft_line": 0,
        "dropped_english_passthrough": 0,
        "dropped_low_quality": 0,
    }
    rendered_candidate_fingerprints: list[str] = []
    dropped_candidates: list[dict[str, object]] = []

    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        quality_counts["included_candidates"] += 1
        if candidate.get("validation_errors"):
            errors.append(f"Candidate #{index} is include=true but still has validation_errors.")
            quality_counts["blocked_for_quality"] += 1
            continue
        if not candidate.get("source_url") or not candidate.get("source_label"):
            errors.append(f"Candidate #{index} is include=true but missing source reference.")
            quality_counts["blocked_for_quality"] += 1
            continue
        # practical_angle is no longer a hard gate: the new long-format prompts
        # derive the "so what" sentence directly from evidence_text, so an
        # empty / placeholder practical_angle should not block rendering.
        practical_angle = str(candidate.get("practical_angle") or "").strip()
        if not practical_angle:
            warnings.append(f"Candidate #{index}: empty practical_angle (kept).")
        elif is_placeholder_practical_angle(practical_angle):
            warnings.append(f"Candidate #{index}: placeholder practical_angle (kept).")
        if str(candidate.get("primary_block") or "") == "last_24h" and not str(candidate.get("published_at") or "").strip():
            errors.append(f"Candidate #{index} is in last_24h without published_at.")
            quality_counts["blocked_for_quality"] += 1
            continue
        if _is_expired_event_candidate(candidate):
            warnings.append(f"Candidate #{index} dropped: expired event date.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["Expired event date."],
                }
            )
            continue

        block_key = str(candidate.get("primary_block") or "").strip()
        section_name = PRIMARY_BLOCKS.get(block_key)
        if not section_name:
            errors.append(f"Candidate #{index} has unknown primary_block: {block_key!r}.")
            quality_counts["blocked_for_quality"] += 1
            continue

        line = str(candidate.get("draft_line") or "").strip()
        title = str(candidate.get("title") or "").strip()
        lead = str(candidate.get("lead") or "").strip()
        summary = str(candidate.get("summary") or "").strip()
        source_label = str(candidate.get("source_label") or "").strip()
        source_url = str(candidate.get("source_url") or "").strip()
        category = str(candidate.get("category") or "").strip()

        if _normalize_text_key(lead) and _normalize_text_key(lead) == _normalize_text_key(summary):
            summary = ""

        english_detected = False
        if category in {"media_layer", "gmp", "public_services", "city_news", "council", "transport", "venues_tickets", "russian_speaking_events", "culture_weekly", "football", "tech_business", "food_openings"}:
            english_fields = [field for field in (lead, summary, title) if _looks_like_untranslated_english(field)]
            if english_fields:
                english_detected = True

        if not line and category == "transport":
            # Tier 4 transport safety net: never drop a transport alert.
            # If transport_fill couldn't extract structure AND LLM tier-3
            # returned empty, fall back to a minimal title-based stub so
            # the reader still sees that something is happening.
            stub_title = re.sub(r"\s+", " ", title).strip()
            # Take the first phrase up to the first dash / pipe / period.
            first_phrase = re.split(r"\s+[-–|]\s+|\.\s+", stub_title, maxsplit=1)[0]
            first_phrase = first_phrase[:120].rstrip()
            label = source_label or "Транспорт"
            line = f"• {label}: {first_phrase} — подробности в источнике."
            warnings.append(f"Candidate #{index}: transport tier-4 stub used (no extractor/LLM draft_line).")
            logger.info("TIER4 transport stub | %s | %s", block_key, first_phrase[:80])

        if not line:
            if category in REQUIRE_DRAFT_LINE_CATEGORIES:
                warnings.append(f"Candidate #{index} dropped: no model draft_line for {category!r}.")
                logger.info("DROP no_draft_line | %s | %s | %s", category, block_key, title[:80])
                quality_counts["dropped_missing_draft_line"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": ["Missing draft_line."],
                    }
                )
                continue
            if english_detected:
                warnings.append(f"Candidate #{index} dropped: English passthrough without translation.")
                logger.info("DROP english_passthrough | %s | %s | %s", category, block_key, title[:80])
                quality_counts["dropped_english_passthrough"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": ["Untranslated English."],
                    }
                )
                continue
            headline = lead or title or summary
            rendered_parts: list[str] = []
            if headline:
                rendered_parts.append(html.escape(headline.rstrip(".")) + ".")
            if _summary_is_useful(summary, headline):
                rendered_parts.append(html.escape(summary.rstrip(".")) + ".")
            line = "• " + " ".join(rendered_parts).strip()

        draft_line_errors = _draft_line_quality_errors(candidate, line)
        if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
            warnings.append(
                f"Candidate #{index} dropped: draft_line quality issues ({'; '.join(draft_line_errors)})."
            )
            logger.info("DROP low_quality | %s | %s | %s | %s", category, block_key, title[:80], "; ".join(draft_line_errors))
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": title,
                    "category": category,
                    "primary_block": block_key,
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": draft_line_errors,
                }
            )
            continue

        # Scrub LLM placeholder genres like "Madison Beer, жанр не указан" /
        # "Avatar, другой жанр" — the rewrite prompt says "не выдумывай жанр"
        # but gpt-4o-mini still tacks these phrases on. Strip them post-hoc.
        line = re.sub(r",\s*(?:жанр\s+не\s+указан|другой\s+жанр|жанр\s+не\s+определ[её]н|жанр\s+неизвестен)\s*(?=[.!?]|$)", "", line, flags=re.IGNORECASE)
        # Restore English spellings for GM toponyms (Altrincham, Bury, Wigan, ...).
        line = restore_english_toponyms(line)
        if candidate.get("is_lead"):
            # Lead story: no bullet, bold first sentence, placed in main_story block
            line = line.lstrip("• ").strip()
            sentences = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)
            if len(sentences) == 2:
                line = f"<b>{sentences[0]}</b> {sentences[1]}"
            else:
                line = f"<b>{line}</b>"
            line = preserve_place_names(line)
            line = _attach_source_anchor(line, source_url, source_label)
            sections.setdefault("Главная история дня", []).insert(0, line)
            section_sources.setdefault("Главная история дня", []).insert(0, source_label)
            section_scores.setdefault("Главная история дня", []).insert(0, 0.0)
        else:
            if not line.startswith("• "):
                line = f"• {line}"
            line = preserve_place_names(line)
            line = _attach_source_anchor(line, source_url, source_label)
            sections[section_name].append(line)
            section_sources[section_name].append(source_label)
            score = _city_watch_score(candidate) if section_name == "Городской радар" else 0.0
            section_scores[section_name].append(score)
        quality_counts["rendered_candidates"] += 1
        fingerprint = str(candidate.get("fingerprint") or "").strip()
        if fingerprint:
            rendered_candidate_fingerprints.append(fingerprint)

    missing_draft_count = quality_counts["dropped_missing_draft_line"]
    if missing_draft_count:
        warnings.append(
            f"Writer dropped {missing_draft_count} included candidate(s) with missing draft_line — digest continues."
        )

    backfilled_today_focus = _backfill_today_focus(sections, section_sources, section_scores)
    if backfilled_today_focus:
        warnings.append(
            f"Writer backfilled «{TODAY_FOCUS_SECTION}» with {backfilled_today_focus} item(s) from other practical sections."
        )

    rendered: list[str] = [_title_line(), ""]

    # "Выходные в GM" показываем только с четверга (weekday >= 3)
    london_weekday = now_london().weekday()  # 0=Пн … 6=Вс
    show_weekend = london_weekday >= 3

    ordered_sections = [
        "Погода",
        "Главная история дня",
        "Что произошло за 24 часа",
        "Общественный транспорт сегодня",
        "Что важно сегодня",
        *(["Выходные в GM"] if show_weekend else []),
        "Городской радар",
        "Что важно в ближайшие 7 дней",
        "Дальние анонсы",
        "Билеты / Ticket Radar",
        "Крупные концерты вне GM",
        "Русскоязычные концерты и стендап UK",
        "Еда, открытия и рынки",
        "IT и бизнес",
        "Футбол",
        "Радар по районам",
    ]
    section_counts: dict[str, int] = {}
    for section_name in ordered_sections:
        lines = sections.get(section_name, [])
        if not lines:
            continue
        srcs = section_sources.get(section_name, [])
        scores = section_scores.get(section_name, [])
        # Re-rank «Городской радар» by editorial score so the 12-item cap
        # keeps GMCA / council news and drops university PR / electron-spin
        # research, rather than truncating in arbitrary source-registry order.
        if section_name == "Городской радар" and scores:
            triples = sorted(
                zip(lines, srcs + [""] * (len(lines) - len(srcs)),
                    scores + [0.0] * (len(lines) - len(scores))),
                key=lambda triple: triple[2],
                reverse=True,
            )
            lines = [t[0] for t in triples]
            srcs = [t[1] for t in triples]
        per_source_cap = SECTION_MAX_PER_SOURCE.get(section_name)
        if per_source_cap:
            src_counts: dict[str, int] = {}
            filtered: list[str] = []
            for idx, ln in enumerate(lines):
                src = srcs[idx] if idx < len(srcs) else ""
                if src_counts.get(src, 0) >= per_source_cap:
                    continue
                src_counts[src] = src_counts.get(src, 0) + 1
                filtered.append(ln)
            lines = filtered
        cap = SECTION_MAX_ITEMS.get(section_name)
        if cap:
            lines = lines[:cap]
        # Per-source / per-section caps can filter every remaining line —
        # don't emit a bare section header in that case, the release gate
        # rejects empty low-signal blocks.
        if not lines:
            section_counts[section_name] = 0
            continue
        section_counts[section_name] = len(lines)
        rendered.append(f"<b>{section_name}</b>")
        rendered.extend(lines)
        rendered.append("")

    draft_path.write_text("\n".join(rendered).strip() + "\n", encoding="utf-8")
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "warnings": warnings,
            "quality_counts": quality_counts,
            "section_counts": section_counts,
            "backfilled_today_focus": backfilled_today_focus,
            "rendered_candidate_fingerprints": rendered_candidate_fingerprints,
            "dropped_candidates": dropped_candidates,
            "draft_path": str(draft_path.resolve()),
        },
    )
    return StageResult(
        not errors,
        "Writer stage completed." if not errors else "Writer stage found blocking issues.",
        report_path,
        draft_path,
    )
