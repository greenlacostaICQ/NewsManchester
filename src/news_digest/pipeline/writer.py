from __future__ import annotations

from dataclasses import dataclass
import html
from pathlib import Path
import re

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    is_placeholder_practical_angle,
    now_london,
    read_json,
    today_london,
    write_json,
)


MODEL_WRITTEN_CATEGORIES = {"media_layer", "gmp", "council", "public_services", "food_openings"}
REQUIRE_DRAFT_LINE_CATEGORIES = MODEL_WRITTEN_CATEGORIES | {
    "transport",
    "venues_tickets",
    "culture_weekly",
    "football",
    "tech_business",
    "city_news",
}


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
    lowered = text.lower()
    label_lower = label.lower()
    if label and lowered.endswith(label_lower):
        text = text[: len(text) - len(label)].rstrip(" .")
    return f"{text} {_source_anchor(source_url, source_label)}".strip()


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
    if len(re.sub(r"\s+", " ", text)) < 45:
        errors.append("draft_line is too short to be a self-contained item.")
    category = str(candidate.get("category") or "").strip()
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and len(re.findall(r"[.!?]", text)) < 1:
        errors.append("draft_line must contain at least one complete sentence.")
    return errors


def write_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "writer_report.json"

    payload = read_json(candidates_path, {"candidates": []})
    candidates = payload.get("candidates", [])
    sections = {heading: [] for heading in PRIMARY_BLOCKS.values()}
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
        practical_angle = str(candidate.get("practical_angle") or "").strip()
        if not practical_angle:
            errors.append(f"Candidate #{index} is include=true but missing practical_angle.")
            quality_counts["blocked_for_quality"] += 1
            continue
        if is_placeholder_practical_angle(practical_angle):
            warnings.append(f"Candidate #{index} held: placeholder practical_angle ({practical_angle[:60]!r}).")
            quality_counts["held_for_editorial_quality"] += 1
            continue
        if str(candidate.get("primary_block") or "") == "last_24h" and not str(candidate.get("published_at") or "").strip():
            errors.append(f"Candidate #{index} is in last_24h without published_at.")
            quality_counts["blocked_for_quality"] += 1
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
        if category in {"media_layer", "gmp", "public_services", "city_news", "council", "transport", "venues_tickets", "culture_weekly", "football", "tech_business", "food_openings"}:
            english_fields = [field for field in (lead, summary, title) if _looks_like_untranslated_english(field)]
            if english_fields:
                english_detected = True

        if not line:
            if category in REQUIRE_DRAFT_LINE_CATEGORIES:
                warnings.append(f"Candidate #{index} dropped: no model draft_line for {category!r}.")
                quality_counts["dropped_missing_draft_line"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "reasons": ["Missing draft_line."],
                    }
                )
                continue
            if english_detected:
                warnings.append(f"Candidate #{index} dropped: English passthrough without translation.")
                quality_counts["dropped_english_passthrough"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
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
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": title,
                    "category": category,
                    "primary_block": block_key,
                    "reasons": draft_line_errors,
                }
            )
            continue

        if not line.startswith("• "):
            line = f"• {line}"
        line = _attach_source_anchor(line, source_url, source_label)
        sections[section_name].append(line)
        quality_counts["rendered_candidates"] += 1
        fingerprint = str(candidate.get("fingerprint") or "").strip()
        if fingerprint:
            rendered_candidate_fingerprints.append(fingerprint)

    rendered: list[str] = [_title_line(), ""]

    # "Выходные в GM" показываем только с четверга (weekday >= 3)
    london_weekday = now_london().weekday()  # 0=Пн … 6=Вс
    show_weekend = london_weekday >= 3

    ordered_sections = [
        "Погода",
        "Главная история дня",
        "Что произошло за 24 часа",
        "Транспорт и сбои",
        "Что важно сегодня",
        *(["Выходные в GM"] if show_weekend else []),
        "Городской радар",
        "Что важно в ближайшие 7 дней",
        "Дальние анонсы",
        "Билеты / Ticket Radar",
        "Открытия и еда",
        "IT и бизнес",
        "Футбол",
        "Радар по районам",
    ]
    # Section-level caps
    SECTION_MAX_ITEMS: dict[str, int] = {
        "Футбол": 3,
        "IT и бизнес": 5,
        "Выходные в GM": 6,
    }

    for section_name in ordered_sections:
        lines = sections.get(section_name, [])
        if not lines:
            continue
        cap = SECTION_MAX_ITEMS.get(section_name)
        if cap:
            lines = lines[:cap]
        rendered.append(f"<b>{section_name}</b>")
        rendered.extend(lines)
        rendered.append("")

    draft_path.write_text("\n".join(rendered).strip() + "\n", encoding="utf-8")
    write_json(
        report_path,
        {
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "warnings": warnings,
            "quality_counts": quality_counts,
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
