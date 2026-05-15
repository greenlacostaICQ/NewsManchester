from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    REQUIRED_BLOCKS,
    REQUIRED_SCAN_CATEGORIES,
    SECTION_MIN_ITEMS,
    extract_sections,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)


BANNED_PLACEHOLDER_MARKERS = [
    "[link]",
    "[todo]",
    "[source]",
]

BANNED_AUTHOR_VOICE = [
    "я не вижу",
    "я не нашёл",
    "у меня нет подтверждения",
    "для вашего слоя",
    "нужен редакторский rewrite",
]

BAD_EDITORIAL_PROSE = [
    "ticket office",
    "слот входа",
    "госпитальн",
    "кадровый и дисциплинарный кейс",
    "заметный кейс",
    "новая фаза истории",
    "сетка влияния",
    "следить компаниям",
    "business-impact",
    # Weather clichés
    "лучше взять зонт",
    "лучше прихватить зонт",
    "не забудьте зонт",
    "прихватите зонт",
    # English words / phrases that slip through translation
    "live alert",
    "live disruption",
    "forecast",
    "attractions",
    "highlights",
    "matchday",
    "check before",
    # Passive filler / vague council-speak
    "опубликовал важное обновление",
    "появилось новое обновление",
    "судебное обновление",
    "новое судебное",
    "футбольное обновление",
    # Prompts to the reader that belong to the author, not the digest
    "перепроверьте",
    "убедитесь сами",
    "читайте подробнее",
    "подробности ниже",
    "обогатит",
    "центр притяжения",
    "новая достопримечательность",
    "другие детали не сообщаются",
    "подробности не раскрываются",
    "решение вступило в силу",
    "остаётся нерешённой",
    "привлечёт внимание",
    "достопримечательност",
    "готовые к изменению климата",
    "sponge park",
    "обещает стать",
    "жители в шоке",
    "эмоциональное прощание",
    "это событие подчеркивает",
    "отличный повод",
    "билеты и даты уточняйте",
    "время и дату уточняйте",
    "дату и время уточняйте",
    "уточните даты",
    "уточняйте",
    "booking fee",
    "under-30s",
    "claimants",
    "soft refreshments",
    "guided writing session",
    "civic reception",
    "takeaway",
]

ENGLISH_PROSE_PATTERN = re.compile(
    r"\b(?:the|and|for|with|from|after|following|across|response|operators|said|says|their)\b",
    re.IGNORECASE,
)

FAIL_CLOSED_SUMMARY = (
    "Digest release is blocked until collector, dedupe, validator, writer and gate inputs pass."
)
RELEASE_GATE_VERSION = 3


@dataclass(slots=True)
class ReleaseResult:
    ok: bool
    message: str
    report_path: Path
    output_path: Path


def _visible_text_from_html(html_text: str) -> str:
    """Return digest text without URLs or tags for prose-only checks."""
    text = re.sub(
        r"<a\b[^>]*>(.*?)</a>",
        lambda match: match.group(1),
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def initialize_release_inputs(project_root: Path, *, overwrite: bool = False) -> dict[str, Path]:
    state_dir = project_root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    current_day_london = today_london()

    collector_path = state_dir / "collector_report.json"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"

    collector_template = {
        "run_date_london": current_day_london,
        "categories": {
            key: {
                "checked": False,
                "sources": [],
                "source_health": [],
                "candidate_count": 0,
                "publishable_count": 0,
                "dated_candidate_count": 0,
                "fresh_last_24h_count": 0,
                "notes": "",
                "errors": [],
            }
            for key in REQUIRED_SCAN_CATEGORIES
        },
    }
    collector_template["categories"]["public_services"]["active_disruption_today"] = False

    candidates_template = {
        "run_date_london": current_day_london,
        "candidates": [
            {
                "title": "",
                "category": "media_layer",
                "summary": "",
                "source_url": "",
                "source_label": "",
                "include": False,
                "dedupe_decision": "drop",
                "carry_over_label": "",
            }
        ],
    }

    if overwrite or not collector_path.exists():
        write_json(collector_path, collector_template)
    if overwrite or not candidates_path.exists():
        write_json(candidates_path, candidates_template)
    if overwrite or not draft_path.exists():
        draft_path.write_text(
            "<b>Greater Manchester Brief — draft</b>\n\n"
            "<b>Погода</b>\n"
            "• \n\n"
            "<b>Коротко</b>\n"
            "• \n\n"
            "<b>Общественный транспорт сегодня</b>\n"
            "• \n\n"
            "<b>Что важно сегодня</b>\n"
            "• \n\n"
            "<b>Что произошло за 24 часа</b>\n"
            "• \n\n"
            "<b>Что важно в ближайшие 7 дней</b>\n"
            "• \n\n"
            "<b>Билеты / Ticket Radar</b>\n"
            "• \n",
            encoding="utf-8",
        )

    return {
        "collector_report": collector_path,
        "candidates": candidates_path,
        "draft_digest": draft_path,
    }


def _load_optional_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return read_json(path)


def _validate_scan_report(scan_report: dict | None, current_day_london: str, errors: list[str]) -> None:
    if scan_report is None:
        errors.append("Missing data/state/collector_report.json.")
        return

    if scan_report.get("run_date_london") != current_day_london:
        errors.append(
            f"Collector report is stale: {scan_report.get('run_date_london')} != {current_day_london}."
        )

    categories = scan_report.get("categories")
    if not isinstance(categories, dict):
        errors.append("Collector report does not contain a categories object.")
        return

    # Categories that must be both checked AND usable for release.
    # transport: excluded — clean-network day is valid.
    # gmp: excluded — BBC Manchester public-safety fallback covers it when GMP server is down.
    # venues_tickets, football: low-signal; source timeouts must not block the whole digest.
    REQUIRED_USABLE_CATEGORIES = {k for k in REQUIRED_SCAN_CATEGORIES if k not in {"transport", "gmp", "venues_tickets", "football"}}

    for key, label in REQUIRED_SCAN_CATEGORIES.items():
        category = categories.get(key)
        if not isinstance(category, dict) or not category.get("checked"):
            errors.append(f"Broad scan incomplete: {label} was not marked as checked.")
            continue
        if key in REQUIRED_USABLE_CATEGORIES and not category.get("usable_for_release"):
            errors.append(f"Scan category {label} was checked but not usable for release (all sources failed or timed out).")


def _validate_candidates(
    candidates_report: dict | None,
    current_day_london: str,
    errors: list[str],
) -> dict[str, list[dict]]:
    if candidates_report is None:
        errors.append("Missing data/state/candidates.json.")
        return {"included_candidates": []}

    if candidates_report.get("run_date_london") != current_day_london:
        errors.append(
            f"Candidates report is stale: {candidates_report.get('run_date_london')} != {current_day_london}."
        )

    candidates = candidates_report.get("candidates")
    if not isinstance(candidates, list):
        errors.append("Candidates report does not contain a candidates list.")
        return {"included_candidates": []}

    included_candidates: list[dict] = []
    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict):
            errors.append(f"Candidate #{index} is not an object.")
            continue
        if not candidate.get("source_url") or not candidate.get("source_label"):
            errors.append(f"Candidate #{index} is missing source_url or source_label.")
        decision = candidate.get("dedupe_decision")
        if decision not in {"drop", "carry_over_with_label", "new_phase", "new"}:
            errors.append(f"Candidate #{index} has invalid dedupe_decision: {decision!r}.")
        if decision == "carry_over_with_label" and not candidate.get("carry_over_label"):
            errors.append(f"Candidate #{index} carry-over is missing carry_over_label.")
        if candidate.get("include") is True and decision != "drop":
            included_candidates.append(candidate)

    if not included_candidates:
        errors.append("No included candidates survived dedupe.")

    return {"included_candidates": included_candidates}


def _validate_curator_report(
    curator_report: dict | None,
    current_day_london: str,
    errors: list[str],
    warnings: list[str],
) -> None:
    if curator_report is None:
        errors.append("Missing data/state/curator_report.json.")
        return

    if curator_report.get("run_date_london") != current_day_london:
        errors.append(
            f"Curator report is stale: {curator_report.get('run_date_london')} != {current_day_london}."
        )
        return

    status = curator_report.get("status")
    if status == "skipped":
        reason = curator_report.get("reason", "unknown")
        if reason in {"all providers failed", "LLM_PROVIDER=none"}:
            warnings.append(f"Curator skipped ({reason}) — existing include flags kept.")
        else:
            errors.append(f"Curator skipped unexpectedly: {reason!r}.")
    elif status != "complete":
        errors.append(f"Curator report is not complete: {status!r}.")
    else:
        reviewed = curator_report.get("reviewed")
        if not isinstance(reviewed, int) or reviewed <= 0:
            errors.append("Curator report did not review any included candidates.")


def _validate_stage_reports(
    writer_report: dict | None,
    editor_report: dict | None,
    errors: list[str],
) -> set[str]:
    rendered_fingerprints: set[str] = set()
    if writer_report is None:
        errors.append("Missing data/state/writer_report.json.")
    else:
        if writer_report.get("stage_status") != "complete":
            errors.append("Writer report is not complete.")
        rendered_fingerprints = {
            str(item)
            for item in writer_report.get("rendered_candidate_fingerprints", [])
            if str(item).strip()
        }

    if editor_report is None:
        errors.append("Missing data/state/editor_report.json.")
    else:
        if editor_report.get("stage_status") != "complete":
            errors.append("Editor report is not complete.")

    return rendered_fingerprints


def _validate_pipeline_run_consistency(
    *,
    collector_report: dict | None,
    candidates_report: dict | None,
    curator_report: dict | None,
    llm_rewrite_report: dict | None,
    writer_report: dict | None,
    editor_report: dict | None,
    errors: list[str],
    warnings: list[str],
) -> str:
    expected = pipeline_run_id_from(collector_report)
    if not expected:
        errors.append("Collector report is missing pipeline_run_id; run collect-digest with the current code.")
        return ""

    required_inputs = {
        "candidates": candidates_report,
        "writer_report": writer_report,
        "editor_report": editor_report,
    }
    optional_inputs = {
        "curator_report": curator_report,
        "llm_rewrite_report": llm_rewrite_report,
    }

    for label, payload in required_inputs.items():
        actual = pipeline_run_id_from(payload)
        if not actual:
            errors.append(f"{label} is missing pipeline_run_id; rerun the full pipeline from collect-digest.")
        elif actual != expected:
            errors.append(
                f"{label} belongs to a different pipeline run ({actual}) than collector_report ({expected})."
            )

    for label, payload in optional_inputs.items():
        if payload is None:
            errors.append(f"Missing data/state/{label}.json.")
            continue
        actual = pipeline_run_id_from(payload)
        if not actual:
            errors.append(f"{label} is missing pipeline_run_id; rerun the full pipeline from collect-digest.")
        elif actual != expected:
            errors.append(
                f"{label} belongs to a different pipeline run ({actual}) than collector_report ({expected})."
            )

    if llm_rewrite_report is not None:
        status = str(llm_rewrite_report.get("stage_status") or "")
        if status not in {"complete", "degraded"}:
            errors.append(f"LLM rewrite report is not complete/degraded: {status!r}.")
        elif status == "degraded":
            warnings.append("LLM rewrite was degraded; writer/release quality gates handled the remaining candidates.")

    return expected


def _validate_draft(
    draft_path: Path,
    scan_report: dict | None,
    included_candidates: list[dict],
    rendered_fingerprints: set[str],
    current_day_london: str,
    errors: list[str],
) -> None:
    if not draft_path.exists():
        errors.append(f"Missing draft digest: {draft_path}.")
        return

    html_text = draft_path.read_text(encoding="utf-8")
    header_match = re.search(
        r"<b>Greater Manchester Brief — (\d{4}-\d{2}-\d{2}), \d{2}:\d{2}</b>",
        html_text,
    )
    if not header_match or header_match.group(1) != current_day_london:
        errors.append(f"Draft digest header does not contain today's date {current_day_london}.")

    for marker in BANNED_PLACEHOLDER_MARKERS:
        if marker in html_text:
            errors.append(f"Draft digest contains placeholder marker: {marker}.")

    lower_text = html_text.lower()
    visible_lower_text = _visible_text_from_html(html_text).lower()
    for marker in BANNED_AUTHOR_VOICE:
        if marker in visible_lower_text:
            errors.append(f"Draft digest contains author voice marker: {marker}.")
    for marker in BAD_EDITORIAL_PROSE:
        if marker in visible_lower_text:
            errors.append(f"Draft digest contains bad editorial prose marker: {marker}.")
    if "/amp/" in lower_text:
        errors.append("Draft digest contains an /amp/ URL.")
    if "<a " not in lower_text:
        errors.append("Draft digest contains no HTML source links.")

    sections = extract_sections(html_text)
    for block in REQUIRED_BLOCKS:
        if block not in sections:
            errors.append(f"Draft digest is missing required block: {block}.")
        elif not [line for line in sections.get(block, []) if line.strip() != "•"]:
            errors.append(f"Draft digest has no substantive item in required block: {block}.")

    visible_item_count = sum(
        1
        for lines in sections.values()
        for line in lines
        if line.strip() and line.strip() != "•"
    )
    collected_candidate_count = 0
    if isinstance(scan_report, dict):
        categories = scan_report.get("categories", {})
        if isinstance(categories, dict):
            collected_candidate_count = sum(
                int(category.get("candidate_count") or 0)
                for category in categories.values()
                if isinstance(category, dict)
            )
    if collected_candidate_count >= 40 and visible_item_count < 12:
        errors.append(
            "Draft digest is too thin for a full scan: "
            f"{visible_item_count} visible item(s) from {collected_candidate_count} collected candidate(s)."
        )

    weather_lines = sections.get("Погода", [])
    if not weather_lines or not re.search(r"\d", " ".join(weather_lines)):
        errors.append("Weather block is missing digits.")

    for section_name, lines in sections.items():
        for line in lines:
            body = re.sub(r"<[^>]+>", " ", line)
            body = re.sub(r"\s+", " ", body).strip()
            if not body:
                continue
            chunks = [chunk.strip(" .") for chunk in re.split(r"(?<=[.!?])\s+", body) if chunk.strip()]
            if len(chunks) >= 2 and chunks[0].lower() == chunks[1].lower():
                errors.append(f"Draft digest contains repeated sentence in section {section_name}.")
                break

    # Strip quoted spans (Russian guillemets and ASCII/curly double quotes)
    # before scanning for the standalone word "нет" — model-refusal markers
    # like "у меня нет данных" always appear in plain prose, while the
    # legitimate use is almost always a quoted slogan (e.g. campaign
    # «сказать "нет" кредитным акулам»).
    _QUOTED_SPAN_RE = re.compile(r'[«"„“][^»"”„“]{0,80}[»"”]')

    def _has_refusal_marker(line: str) -> bool:
        stripped = _QUOTED_SPAN_RE.sub("", line)
        if re.search(r"(?<![а-яёА-ЯЁ])нет(?![а-яёА-ЯЁ])", stripped, re.IGNORECASE):
            return True
        return "не добавляю" in stripped.lower()

    for block in LOW_SIGNAL_BLOCKS:
        lines = sections.get(block, [])
        if lines and any(_has_refusal_marker(line) for line in lines):
            errors.append(f"Low-signal block should be hidden instead of printed empty: {block}.")

    last_24h_lines = sections.get("Что произошло за 24 часа", [])
    fresh_last_24h_candidates = [
        candidate
        for candidate in included_candidates
        if candidate.get("category") in {"media_layer", "gmp", "public_services", "city_news"}
        and candidate.get("primary_block") == "last_24h"
        and candidate.get("freshness_status") == "fresh_24h"
    ]
    if len(fresh_last_24h_candidates) >= 3 and len(last_24h_lines) < 3:
        errors.append("Last 24h block has fewer than 3 items despite available fresh city/news candidates.")

    city_candidates = [
        candidate
        for candidate in included_candidates
        if candidate.get("category") in {"media_layer", "gmp", "public_services", "city_news", "council"}
    ]
    if not city_candidates:
        errors.append("Draft digest has no included city/public-affairs candidates.")
    if len(city_candidates) >= 2:
        city_hits = 0
        for candidate in city_candidates:
            fingerprint = str(candidate.get("fingerprint") or "").strip()
            if fingerprint and fingerprint in rendered_fingerprints:
                city_hits += 1
        if city_hits < 2:
            errors.append(
                "Draft digest is skewed away from city news: fewer than 2 included city/public-affairs candidates are visible."
            )

    active_disruption = False
    if isinstance(scan_report, dict):
        public_services = scan_report.get("categories", {}).get("public_services", {})
        active_disruption = bool(public_services.get("active_disruption_today"))
    if active_disruption:
        # Check by fingerprint (robust) — title-match is unreliable when LLM rewrites draft_line
        ps_fingerprints = {
            str(c.get("fingerprint") or "").strip()
            for c in included_candidates
            if c.get("category") == "public_services" and str(c.get("fingerprint") or "").strip()
        }
        if ps_fingerprints and not ps_fingerprints.intersection(rendered_fingerprints):
            errors.append(
                "Active public-services disruption is marked for today but not visible in the digest."
            )


# Sections that announce things-to-attend. A bullet here without a date
# marker is almost always an unhelpful "concert sometime" line. We tolerate
# up to ~half before flagging.
_EVENT_SECTIONS_FOR_DATE_CHECK = frozenset({
    "Что важно в ближайшие 7 дней",
    "Билеты / Ticket Radar",
    "Выходные в GM",
    "Крупные концерты вне GM",
    "Дальние анонсы",
    "Русскоязычные концерты и стендап UK",
})

# Date markers: Russian/ASCII numeric dates, Russian month names, weekday
# accusative forms ("в субботу"), "сегодня"/"завтра"/"послезавтра", or a
# year. Anchored on word boundaries via the surrounding regex.
_DATE_MARKER_RE = re.compile(
    r"\b\d{1,2}\s*(?:января|февраля|марта|апреля|мая|июня|июля|"
    r"августа|сентября|октября|ноября|декабря)\b"
    r"|\b\d{1,2}[/.\-]\d{1,2}\b"
    r"|\bв\s+(?:понедельник|вторник|среду|четверг|пятницу|субботу|воскресенье)\b"
    r"|\b(?:сегодня|завтра|послезавтра)\b"
    r"|\b20\d{2}\b",
    re.IGNORECASE,
)


def _evaluate_digest_health(
    writer_report: dict | None,
    curator_report: dict | None,
    sections: dict[str, list[str]],
) -> dict[str, object]:
    """Q9: aggregate quality signals into a single health verdict.

    severity 3 = severe regression (e.g. all news sections thin, fewer
                 than 15 items rendered) — surfaces as 🔴 unhealthy
    severity 2 = clear risk (no weather, undated events) — at_risk
    severity 1 = soft risk (transport empty, few items 15-25)

    Observational only: the verdict never blocks release. The operator's
    rule is "ship and flag" — a flagged digest is more useful than no
    digest. The Telegram admin alert escalates the icon to 🔴 when
    unhealthy so the day stands out at a glance.
    """
    signals: list[dict[str, object]] = []
    qc = (writer_report or {}).get("quality_counts") or {}
    sc = (writer_report or {}).get("section_counts") or {}
    rendered = int(qc.get("rendered_candidates") or 0)
    included = int(qc.get("included_candidates") or 0)

    if rendered < 15:
        signals.append({
            "name": "too_few_items",
            "severity": 3,
            "detail": f"Only {rendered} item(s) rendered — below the 15-item floor.",
        })
    elif rendered < 25:
        signals.append({
            "name": "few_items",
            "severity": 1,
            "detail": f"Only {rendered} item(s) rendered (typical day 30–50).",
        })

    if sc.get("Погода", 0) == 0:
        signals.append({
            "name": "weather_empty",
            "severity": 2,
            "detail": "Weather section empty — Met Office collector likely down.",
        })

    if sc.get("Общественный транспорт сегодня", 0) == 0:
        signals.append({
            "name": "transport_empty",
            "severity": 1,
            "detail": "Transport section empty — no TfGM/Metrolink alerts surfaced.",
        })

    n_24h = int(sc.get("Что произошло за 24 часа", 0) or 0)
    n_today = int(sc.get("Что важно сегодня", 0) or 0)
    n_radar = int(sc.get("Городской радар", 0) or 0)
    if n_24h < 5 and n_today < 3 and n_radar < 5:
        signals.append({
            "name": "all_news_thin",
            "severity": 3,
            "detail": (
                f"All news sections thin: 24h={n_24h}, today={n_today}, "
                f"radar={n_radar} — possible coverage breakdown."
            ),
        })

    no_date = 0
    total_events = 0
    for sec in _EVENT_SECTIONS_FOR_DATE_CHECK:
        for line in sections.get(sec, []):
            visible = re.sub(r"<[^>]+>", " ", str(line))
            if not visible.strip() or visible.strip() == "•":
                continue
            total_events += 1
            if not _DATE_MARKER_RE.search(visible):
                no_date += 1
    if total_events >= 3 and no_date / total_events > 0.5:
        signals.append({
            "name": "events_without_dates",
            "severity": 2,
            "detail": (
                f"{no_date}/{total_events} event item(s) lack a date marker — "
                "readers can't act on undated event listings."
            ),
        })

    if curator_report:
        semantic_dropped = int(curator_report.get("semantic_dropped") or 0)
        if semantic_dropped >= 10:
            signals.append({
                "name": "high_semantic_duplicates",
                "severity": 1,
                "detail": (
                    f"{semantic_dropped} semantic duplicate(s) dropped by curator — "
                    "input feeds are unusually noisy today."
                ),
            })

    if included >= 20 and rendered < included * 0.4:
        signals.append({
            "name": "low_writer_yield",
            "severity": 2,
            "detail": (
                f"Writer rendered {rendered} of {included} included candidates "
                f"({rendered / included:.0%}) — quality gates may be too strict."
            ),
        })

    score = sum(int(s["severity"]) for s in signals)
    has_severe = any(int(s["severity"]) >= 3 for s in signals)
    if has_severe:
        level = "unhealthy"
    elif score >= 2:
        level = "at_risk"
    else:
        level = "healthy"

    return {
        "risk_level": level,
        "risk_score": score,
        "signals": signals,
    }


def _summarise_change_types(state_dir: Path) -> dict[str, object]:
    """Count change_type buckets from dedupe_memory.json (Q6/Q7).
    Surface counts + a handful of concrete rejected examples so the
    "почему отбили этот сюжет" question is answerable from one file."""
    path = state_dir / "dedupe_memory.json"
    counts: dict[str, int] = {
        "new_story": 0,
        "same_story_new_facts": 0,
        "same_story_rehash": 0,
        "follow_up": 0,
        "reminder": 0,
        "no_change": 0,
        "unclassified": 0,
    }
    auto_rejected_examples: list[dict[str, object]] = []
    if not path.exists():
        return {"counts": counts, "auto_rejected_examples": auto_rejected_examples}
    try:
        report = read_json(path)
    except Exception:  # noqa: BLE001
        return {"counts": counts, "auto_rejected_examples": auto_rejected_examples}
    for d in report.get("decisions") or []:
        if not isinstance(d, dict):
            continue
        ct = str(d.get("change_type") or "").strip() or "unclassified"
        counts[ct] = counts.get(ct, 0) + 1
        if ct in {"no_change", "same_story_rehash"} and len(auto_rejected_examples) < 10:
            auto_rejected_examples.append(
                {
                    "change_type": ct,
                    "title": d.get("title"),
                    "reason": d.get("reason"),
                    "previous_fingerprint": d.get("previous_fingerprint") or d.get("matched_previous_fingerprint"),
                    "previous_published_day": d.get("previous_published_day"),
                    "previous_title": d.get("previous_title"),
                }
            )
    return {"counts": counts, "auto_rejected_examples": auto_rejected_examples}


def _prompts_snapshot() -> list[dict[str, str]]:
    from news_digest.pipeline.prompts_meta import snapshot as _ps  # noqa: PLC0415
    return _ps()


def _aggregate_cost(state_dir: Path) -> dict:
    """Sum per-stage cost_*.json into a single daily total. Tolerates
    missing stage files (e.g. LLM_PROVIDER=none disables llm_rewrite)."""
    from news_digest.pipeline.cost_tracker import summarise, CallRecord  # noqa: PLC0415
    records: list[CallRecord] = []
    for stage_file in state_dir.glob("cost_*.json"):
        try:
            payload = read_json(stage_file)
        except Exception:  # noqa: BLE001
            continue
        for r in payload.get("records") or []:
            records.append(
                CallRecord(
                    stage=str(r.get("stage") or ""),
                    provider=str(r.get("provider") or ""),
                    model=str(r.get("model") or ""),
                    prompt_name=str(r.get("prompt_name") or ""),
                    prompt_tokens=int(r.get("prompt_tokens") or 0),
                    completion_tokens=int(r.get("completion_tokens") or 0),
                    cost_usd=float(r.get("cost_usd") or 0.0),
                )
            )
    return summarise(records)


def _detect_prompt_drift(
    curator_report: dict | None,
    llm_rewrite_report: dict | None,
    state_dir: Path,
) -> list[dict[str, str]]:
    """If prompt text changed (new hash) but semver stayed the same vs
    yesterday — flag silent drift. Reads previous prompt hashes from
    `cost_history.json`."""
    current = {p["name"]: p for p in _prompts_snapshot()}
    history_path = state_dir / "cost_history.json"
    if not history_path.exists():
        return []
    try:
        history = read_json(history_path) or []
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(history, list) or not history:
        return []
    last = history[-1]
    last_prompts = {p["name"]: p for p in last.get("prompt_versions") or []}
    drift: list[dict[str, str]] = []
    for name, cur in current.items():
        prev = last_prompts.get(name)
        if not prev:
            continue
        if cur["hash"] != prev["hash"] and cur["version"] == prev["version"]:
            drift.append(
                {
                    "name": name,
                    "version": cur["version"],
                    "old_hash": prev["hash"],
                    "new_hash": cur["hash"],
                }
            )
    return drift


def _check_budget(state_dir: Path, today_cost: float, current_day_london: str) -> str | None:
    """Compare today's total cost against 1.5× the rolling 7-day average.
    Skip the check until we have at least 3 historical days."""
    history_path = state_dir / "cost_history.json"
    if not history_path.exists():
        return None
    try:
        history = read_json(history_path) or []
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(history, list):
        return None
    recent = [
        float(e.get("total_cost_usd") or 0.0)
        for e in history[-7:]
        if isinstance(e, dict) and e.get("run_date_london") != current_day_london
    ]
    if len(recent) < 3:
        return None
    avg = sum(recent) / len(recent)
    if avg <= 0:
        return None
    threshold = avg * 1.5
    if today_cost > threshold:
        return (
            f"Budget: today's cost ${today_cost:.4f} > 1.5× of 7-day average "
            f"${avg:.4f} (threshold ${threshold:.4f}). Investigate batch size, "
            "provider fallback, or model swap."
        )
    return None


def _append_cost_history(
    state_dir: Path,
    current_day_london: str,
    cost_summary: dict,
    prompt_versions: list[dict[str, str]],
) -> None:
    """Append today's totals to data/state/cost_history.json. Keeps the
    last 60 entries so the file does not grow unbounded."""
    history_path = state_dir / "cost_history.json"
    history: list = []
    if history_path.exists():
        try:
            loaded = read_json(history_path)
            if isinstance(loaded, list):
                history = loaded
        except Exception:  # noqa: BLE001
            history = []
    history = [e for e in history if not (isinstance(e, dict) and e.get("run_date_london") == current_day_london)]
    history.append(
        {
            "run_date_london": current_day_london,
            "run_at_london": now_london().isoformat(),
            "total_cost_usd": cost_summary.get("total_cost_usd", 0.0),
            "total_calls": cost_summary.get("total_calls", 0),
            "by_stage": cost_summary.get("by_stage", {}),
            "by_provider": cost_summary.get("by_provider", {}),
            "by_model": cost_summary.get("by_model", {}),
            "prompt_versions": prompt_versions,
        }
    )
    history = history[-60:]
    write_json(history_path, history)


def build_release(project_root: Path) -> ReleaseResult:
    state_dir = project_root / "data" / "state"
    outgoing_dir = project_root / "data" / "outgoing"
    outgoing_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    current_day_london = today_london()
    draft_path = state_dir / "draft_digest.html"
    output_path = outgoing_dir / "current_digest.html"
    report_path = state_dir / "release_report.json"

    scan_report = _load_optional_json(state_dir / "collector_report.json")
    candidates_report = _load_optional_json(state_dir / "candidates.json")
    curator_report = _load_optional_json(state_dir / "curator_report.json")
    llm_rewrite_report = _load_optional_json(state_dir / "llm_rewrite_report.json")
    writer_report = _load_optional_json(state_dir / "writer_report.json")
    editor_report = _load_optional_json(state_dir / "editor_report.json")

    errors: list[str] = []
    warnings: list[str] = []
    _validate_scan_report(scan_report, current_day_london, errors)
    candidate_context = _validate_candidates(candidates_report, current_day_london, errors)
    _validate_curator_report(curator_report, current_day_london, errors, warnings)
    rendered_fingerprints = _validate_stage_reports(writer_report, editor_report, errors)
    pipeline_run_id = _validate_pipeline_run_consistency(
        collector_report=scan_report,
        candidates_report=candidates_report,
        curator_report=curator_report,
        llm_rewrite_report=llm_rewrite_report,
        writer_report=writer_report,
        editor_report=editor_report,
        errors=errors,
        warnings=warnings,
    )
    lost_leads: list[dict[str, object]] = []
    section_underflow: list[dict[str, object]] = []
    if writer_report:
        qc = writer_report.get("quality_counts") or {}
        english = int(qc.get("dropped_english_passthrough") or 0)
        no_draft = int(qc.get("dropped_missing_draft_line") or 0)
        low_quality = int(qc.get("dropped_low_quality") or 0)
        included = int(qc.get("included_candidates") or 0)
        rendered = int(qc.get("rendered_candidates") or 0)
        if english > 0:
            warnings.append(f"Quality: {english} candidate(s) dropped for English passthrough — translation may be failing.")
        if no_draft > 2:
            warnings.append(f"Quality: {no_draft} candidate(s) dropped for missing draft_line — LLM rewrite yield is low.")
        if low_quality > max(3, included // 5):
            warnings.append(
                f"Quality: writer dropped many low-quality draft_lines: {low_quality} of {included} included candidates."
            )
        if included >= 15 and rendered < 8:
            warnings.append(f"Quality: heavy filtering — {rendered} rendered from {included} included candidates.")

        # Sticky leads: curator-marked leads that writer dropped are
        # visible regressions, not silent quality drops. Surface every
        # one in release_report and as a per-lead warning.
        for drop in writer_report.get("dropped_candidates") or []:
            if not isinstance(drop, dict) or not drop.get("is_lead"):
                continue
            lost_leads.append(
                {
                    "fingerprint": drop.get("fingerprint"),
                    "title": drop.get("title"),
                    "category": drop.get("category"),
                    "primary_block": drop.get("primary_block"),
                    "reasons": drop.get("reasons") or [],
                }
            )
            warnings.append(
                "Lost lead: curator-marked lead dropped by writer "
                f"({drop.get('category') or 'unknown'}) — {'; '.join(drop.get('reasons') or ['no reason recorded'])}"
            )

        # Section underflow: spot days when writer filtering pushed a
        # section below its target minimum. We only flag underflow when
        # writer actually dropped candidates that would have lived in
        # that section — a thin section with zero drops just means there
        # was no news today, which is not a signal worth alerting on.
        sec_counts = writer_report.get("section_counts") or {}
        dropped_per_section: dict[str, int] = {}
        for drop in writer_report.get("dropped_candidates") or []:
            if not isinstance(drop, dict):
                continue
            section_name = PRIMARY_BLOCKS.get(str(drop.get("primary_block") or ""))
            if section_name:
                dropped_per_section[section_name] = dropped_per_section.get(section_name, 0) + 1
        for section_name, minimum in SECTION_MIN_ITEMS.items():
            actual = int(sec_counts.get(section_name) or 0)
            dropped_here = dropped_per_section.get(section_name, 0)
            if actual < minimum and dropped_here > 0:
                section_underflow.append(
                    {
                        "section": section_name,
                        "actual": actual,
                        "minimum": minimum,
                        "dropped_by_writer": dropped_here,
                    }
                )
                warnings.append(
                    f"Section underflow: «{section_name}» shipped {actual} items "
                    f"(min={minimum}) while writer dropped {dropped_here} candidate(s) "
                    f"that targeted this section — quality gates may be too strict."
                )
    change_type_summary = _summarise_change_types(state_dir)

    cost_summary = _aggregate_cost(state_dir)
    if cost_summary["unknown_priced_models"]:
        warnings.append(
            "Cost: unknown-priced model(s) used — add to PRICING_PER_MTOKEN: "
            f"{', '.join(cost_summary['unknown_priced_models'])}"
        )

    prompt_drift = _detect_prompt_drift(curator_report, llm_rewrite_report, state_dir)
    if prompt_drift:
        for pd in prompt_drift:
            warnings.append(
                f"Prompt drift: «{pd['name']}» hash changed to {pd['new_hash']} "
                f"but version stayed {pd['version']} — bump semver if intent changed."
            )

    budget_alert = _check_budget(state_dir, cost_summary["total_cost_usd"], current_day_london)
    if budget_alert:
        warnings.append(budget_alert)

    _validate_draft(
        draft_path=draft_path,
        scan_report=scan_report,
        included_candidates=candidate_context["included_candidates"],
        rendered_fingerprints=rendered_fingerprints,
        current_day_london=current_day_london,
        errors=errors,
    )

    # Q9: Bad Digest Detector. Compute health verdict from writer + curator
    # signals plus a date-marker scan over event sections. The detector is
    # observational only — even a severity-3 (unhealthy) verdict goes out
    # as warnings, never errors, because operator preference is "ship and
    # flag" rather than "block and notify". The 🔴 icon in the Telegram
    # admin message + risk_level=unhealthy in release_report still make
    # the day stand out.
    if draft_path.exists():
        try:
            health_sections = extract_sections(draft_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            health_sections = {}
    else:
        health_sections = {}
    digest_health = _evaluate_digest_health(writer_report, curator_report, health_sections)
    for sig in digest_health["signals"]:
        prefix = "Severe" if int(sig["severity"]) >= 3 else "Risk"
        warnings.append(f"Digest health [{prefix}] {sig['name']}: {sig['detail']}")

    ok = not errors
    published_facts_updated = False
    if ok:
        shutil.copyfile(draft_path, output_path)
        message = f"Release passed. Promoted {draft_path} to {output_path}."
    else:
        message = FAIL_CLOSED_SUMMARY

    report_payload = {
        "release_gate_version": RELEASE_GATE_VERSION,
        "pipeline_run_id": pipeline_run_id,
        "run_at_london": now_london().isoformat(),
        "run_date_london": current_day_london,
        "release_decision": "pass" if ok else "fail",
        "message": message,
        "errors": errors,
        "warnings": warnings,
        "lost_leads": lost_leads,
        "section_underflow": section_underflow,
        "cost_summary": cost_summary,
        "change_type_summary": change_type_summary,
        "digest_health": digest_health,
        "prompt_versions": _prompts_snapshot(),
        "prompt_drift": prompt_drift,
        "published_facts_updated": published_facts_updated,
        "inputs": {
            "collector_report": str((state_dir / "collector_report.json").resolve()),
            "candidates": str((state_dir / "candidates.json").resolve()),
            "curator_report": str((state_dir / "curator_report.json").resolve()),
            "llm_rewrite_report": str((state_dir / "llm_rewrite_report.json").resolve()),
            "writer_report": str((state_dir / "writer_report.json").resolve()),
            "editor_report": str((state_dir / "editor_report.json").resolve()),
            "draft_digest": str(draft_path.resolve()),
        },
        "output_path": str(output_path.resolve()),
    }
    write_json(report_path, report_payload)

    # Snapshot every successful gate to a separate file so a later failed
    # debug run does not erase the proof that the morning gate passed.
    # `release_report.json` always reflects the latest run (could be
    # fail); `last_passed_release_report.json` always reflects the most
    # recent successful gate. Operational diagnosis ("did today actually
    # ship?") should consult `delivery_state.json` for the canonical
    # answer; this file just preserves the gate's audit trail.
    if ok:
        write_json(state_dir / "last_passed_release_report.json", report_payload)
        _append_cost_history(state_dir, current_day_london, cost_summary, _prompts_snapshot())

    return ReleaseResult(ok=ok, message=message, report_path=report_path, output_path=output_path)
