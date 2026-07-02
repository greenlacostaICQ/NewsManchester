from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import html
import json
import logging
from pathlib import Path
import re
import shutil
import time

logger = logging.getLogger(__name__)

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    REQUIRED_BLOCKS,
    REQUIRED_SCAN_CATEGORIES,
    SECTION_MIN_ITEMS,
    canonical_url_identity,
    extract_sections,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.transport_language import (
    repair_transport_line_language,
    transport_public_contract_errors,
)
from news_digest.pipeline.city_intelligence import summarise_city_intelligence
from news_digest.pipeline.city_trends import (
    append_city_intelligence_history,
    build_trend_detection,
)
from news_digest.pipeline.inventory import (
    aggregate_category_health,
    categories_needing_live_fallback,
    verify_collect_conservation,
    verify_dispositions,
)
from news_digest.pipeline.reader_value import validate_reader_value_labels
from news_digest.pipeline.repeat_policy import visible_repeat_verdict
from news_digest.pipeline.story_intelligence import (
    AUDIT_TRAIL_SCHEMA_VERSION,
    COST_LATENCY_BUDGETS,
    MODEL_BAKEOFF_SPEC,
    apply_story_intelligence,
    backup_pool_record,
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
            "<b>Свежие новости</b>\n"
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
    # public_services: excluded — it is an official-background layer; a single
    # NHS trust media-centre outage must warn, not block, unless a rendered
    # active disruption is actually required elsewhere.
    # venues_tickets, football: low-signal; source timeouts must not block the whole digest.
    REQUIRED_USABLE_CATEGORIES = {
        k
        for k in REQUIRED_SCAN_CATEGORIES
        if k not in {"transport", "gmp", "public_services", "venues_tickets", "football"}
    }

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
    writer_report: dict | None,
    current_day_london: str,
    errors: list[str],
    warnings: list[str],
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

    repaired_html, contract_repair = _repair_public_html_contracts(html_text)
    if contract_repair.get("fixed_count") and repaired_html != html_text:
        draft_path.write_text(repaired_html, encoding="utf-8")
        html_text = repaired_html
        warnings.append(
            "Release recovered "
            f"{contract_repair.get('fixed_count')} public-contract line(s); digest continues."
        )

    sections = extract_sections(html_text)
    errors.extend(public_html_contract_errors(html_text))
    for block in REQUIRED_BLOCKS:
        has_candidates_for_block = any(
            PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or "")) == block
            for candidate in included_candidates
            if isinstance(candidate, dict) and candidate.get("include")
        )
        today_focus_board = (writer_report or {}).get("today_focus_board")
        today_focus_has_no_eligible_rows = (
            block == "Что важно сегодня"
            and isinstance(today_focus_board, dict)
            and int(today_focus_board.get("eligible_candidates") or 0) == 0
            and int(today_focus_board.get("rendered_candidates") or 0) == 0
        )
        today_focus_warning_reason = ""
        if today_focus_has_no_eligible_rows:
            today_focus_warning_reason = str(today_focus_board.get("underflow_reason") or "no_eligible_practical_items")
        if block not in sections:
            if block == "Что важно сегодня" and (not has_candidates_for_block or today_focus_has_no_eligible_rows):
                if today_focus_has_no_eligible_rows:
                    warnings.append(
                        "Draft has no «Что важно сегодня» section because writer found no eligible "
                        f"practical today-focus item ({today_focus_warning_reason})."
                    )
                else:
                    warnings.append("Draft has no «Что важно сегодня» section because no today_focus candidates survived.")
            else:
                errors.append(f"Draft digest is missing required block: {block}.")
        elif not [line for line in sections.get(block, []) if line.strip() != "•"]:
            if block == "Что важно сегодня" and (not has_candidates_for_block or today_focus_has_no_eligible_rows):
                if today_focus_has_no_eligible_rows:
                    warnings.append(
                        "Draft has empty «Что важно сегодня» section because writer found no eligible "
                        f"practical today-focus item ({today_focus_warning_reason})."
                    )
                else:
                    warnings.append("Draft has empty «Что важно сегодня» section because no today_focus candidates survived.")
            else:
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
        # O2: if the synthetic weather candidate is flagged stale (Met
        # Office + Open-Meteo both unreachable after refetch×2), the
        # placeholder line has no digits by design. Downgrade to a
        # warning instead of blocking the release — the digest still
        # ships, just with an honest "data unavailable" weather block.
        stale_weather = any(
            isinstance(c, dict)
            and c.get("primary_block") == "weather"
            and c.get("synthetic_stale")
            for c in included_candidates
        )
        if stale_weather:
            warnings.append(
                "Weather block has no digits — synthetic source flagged stale "
                "(Met Office + Open-Meteo both unreachable after refetch×2). "
                "Shipping placeholder."
            )
        else:
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
            # NEVER block the whole release because a single low-signal block
            # (e.g. Городской радар) carried a model-refusal line — that killed
            # the entire 2026-06-13 digest (no news delivered) over a cosmetic
            # issue in a minor section. Warn instead; the offending line should
            # be dropped upstream in the writer (follow-up), but the issue must
            # always ship rather than be lost.
            warnings.append(f"Low-signal block has a model-refusal line (should be dropped/hidden): {block}.")

    last_24h_lines = sections.get("Свежие новости", [])
    fresh_last_24h_candidates = [
        candidate
        for candidate in included_candidates
        if candidate.get("category") in {"media_layer", "gmp", "public_services", "city_news"}
        and candidate.get("primary_block") == "last_24h"
        and candidate.get("freshness_status") == "fresh_24h"
    ]
    if len(fresh_last_24h_candidates) >= 3 and len(last_24h_lines) < 3:
        warnings.append("Last 24h block has fewer than 3 items despite available fresh city/news candidates.")

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


def public_html_contract_errors(html_text: str) -> list[str]:
    """Rendered-HTML replay guard for historical public-output failures."""
    visible_text = _visible_text_from_html(html_text)
    lowered = visible_text.lower()
    errors: list[str] = []
    forbidden_markers = {
        "локальный радар": "weather_local_radar",
        "почему в радаре": "ticket_machine_explanation",
        "билеты и детали берите": "ticket_generic_cta",
        "проверьте время": "missing_event_time_cta",
        "проверьте дату": "missing_event_date_cta",
        "this website makes extensive use of javascript": "source_chrome_passthrough",
    }
    for marker, code in forbidden_markers.items():
        if marker in lowered:
            errors.append(f"Rendered HTML violates public contract: {code}.")
    sections = extract_sections(html_text)
    for line in sections.get("Общественный транспорт сегодня", []):
        for issue in transport_public_contract_errors(line):
            errors.append(f"Rendered HTML violates public contract: {issue}.")
    ticket_lines = [
        line for line in sections.get("Билеты / Ticket Radar", [])
        if line.strip() and line.strip() != "•"
    ]
    if len(ticket_lines) > 15:
        errors.append(f"Rendered HTML violates public contract: ticket_radar_over_cap ({len(ticket_lines)} > 15).")
    return errors


def _repair_public_html_contracts(html_text: str) -> tuple[str, dict[str, object]]:
    current_section = ""
    changed_lines: list[dict[str, object]] = []
    output: list[str] = []
    marker_replacements = [
        (re.compile(r"\bпроверьте\s+время\b", re.IGNORECASE), "сверьте детали"),
        (re.compile(r"\bпроверьте\s+дату\b", re.IGNORECASE), "сверьте дату"),
        (re.compile(r"\bбилеты\s+и\s+детали\s+берите\b", re.IGNORECASE), "детали смотрите"),
        (re.compile(r"\bпочему\s+в\s+радаре[:：]?\s*", re.IGNORECASE), ""),
        (re.compile(r"\bлокальный\s+радар[:：]?\s*", re.IGNORECASE), ""),
        (
            re.compile(r"this website makes extensive use of javascript\.?", re.IGNORECASE),
            "",
        ),
    ]
    for line in html_text.splitlines():
        header_match = re.fullmatch(r"<b>(.*?)</b>", line.strip())
        if header_match:
            current_section = html.unescape(header_match.group(1))
            output.append(line)
            continue
        fixed_line = line
        generic_reasons: list[str] = []
        for pattern, replacement in marker_replacements:
            if pattern.search(fixed_line):
                fixed_line = pattern.sub(replacement, fixed_line)
                generic_reasons.append("generic_public_contract_phrase")
        if generic_reasons and fixed_line != line:
            fixed_line = re.sub(r"\s{2,}", " ", fixed_line)
            fixed_line = re.sub(r"\s+([,.;:])", r"\1", fixed_line)
            changed_lines.append({
                "section": current_section,
                "reasons": generic_reasons,
                "before": line[:240],
                "after": fixed_line[:240],
            })
            line = fixed_line
        if current_section == "Общественный транспорт сегодня" and line.lstrip().startswith("•"):
            fixed, reasons = repair_transport_line_language(line)
            if reasons and fixed != line:
                changed_lines.append({
                    "section": current_section,
                    "reasons": reasons,
                    "before": line[:240],
                    "after": fixed[:240],
                })
                line = fixed
        output.append(line)
    trailing_newline = "\n" if html_text.endswith("\n") else ""
    return "\n".join(output) + trailing_newline, {
        "attempted": True,
        "fixed_count": len(changed_lines),
        "fixed_lines": changed_lines,
    }


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

# Date markers (S3 expanded): numeric dates, Russian month names, weekday
# forms ("в субботу"), "сегодня"/"завтра", year, range markers ("до 24
# мая", "с 15 по 24"), AND recurring/permanent phrases that are valid
# time anchors for recurring events ("каждое воскресенье", "работает
# постоянно", "по выходным"). Anchored on word boundaries.
_DATE_MARKER_RE = re.compile(
    r"\b\d{1,2}\s*(?:января|февраля|марта|апреля|мая|июня|июля|"
    r"августа|сентября|октября|ноября|декабря)\b"
    r"|\b\d{1,2}[/.\-]\d{1,2}\b"
    r"|\bв\s+(?:понедельник|вторник|среду|четверг|пятницу|субботу|воскресенье)\b"
    r"|\b(?:сегодня|завтра|послезавтра)\b"
    r"|\b20\d{2}\b"
    # Recurring / permanent markers — also valid time anchors.
    r"|\bкажд(?:ое|ую|ый|ого|ой)\s+(?:воскресенье|субботу|неделю|месяц|"
    r"день|вечер|пятницу|понедельник|вторник|среду|четверг)\b"
    r"|\bеженедельн\w*\b|\bежемесячн\w*\b"
    r"|\bпостоянно\s+работает\b|\bработает\s+(?:постоянно|по\s+выходным|"
    r"каждый\s+день|круглогодично)\b"
    r"|\bпо\s+выходным\b|\bв\s+будни\b"
    # Range / open-ended ("идёт до 24 мая", "с 15 мая", "до конца сентября").
    r"|\bдо\s+(?:конца\s+)?(?:января|февраля|марта|апреля|мая|июня|июля|"
    r"августа|сентября|октября|ноября|декабря)\b"
    r"|\bидёт\s+до\b|\bидет\s+до\b",
    re.IGNORECASE,
)


def _evaluate_digest_health(
    writer_report: dict | None,
    curator_report: dict | None,
    sections: dict[str, list[str]],
) -> dict[str, object]:
    """Q9: aggregate quality signals into a single health verdict.

    severity 3 = severe regression (e.g. all news sections thin, fewer
                 than 12 items rendered) — surfaces as 🔴 unhealthy
    severity 2 = clear risk (no weather, undated events, bloated issue) — at_risk
    severity 1 = soft risk (transport empty, few items 12-13)

    These signals are warning-only. The release gate blocks broken inputs
    and invalid Telegram HTML, but editorial risk should ship with a loud
    review report instead of silently suppressing the morning issue.
    """
    signals: list[dict[str, object]] = []
    qc = (writer_report or {}).get("quality_counts") or {}
    sc = (writer_report or {}).get("section_counts") or {}
    final_counts = {
        name: len([line for line in lines if str(line).strip() and str(line).strip() != "•"])
        for name, lines in (sections or {}).items()
    }

    def _section_count(section_name: str) -> int:
        if final_counts:
            return int(final_counts.get(section_name) or 0)
        return int(sc.get(section_name) or 0)

    rendered = int(qc.get("rendered_candidates") or 0)
    included = int(qc.get("included_candidates") or 0)

    if rendered < 12:
        signals.append({
            "name": "too_few_items",
            "severity": 3,
            "detail": f"Only {rendered} item(s) rendered — below the 12-item hard floor.",
        })
    elif rendered < 14:
        signals.append({
            "name": "few_items",
            "severity": 1,
            "detail": f"Only {rendered} item(s) rendered (target day 14–45).",
        })
    elif rendered > 45:
        signals.append({
            "name": "too_many_items",
            "severity": 2,
            "detail": f"{rendered} item(s) rendered — above the 45-item editorial cap target.",
        })

    if _section_count("Погода") == 0:
        signals.append({
            "name": "weather_empty",
            "severity": 2,
            "detail": "Weather section empty — Met Office collector likely down.",
        })

    if _section_count("Общественный транспорт сегодня") == 0:
        signals.append({
            "name": "transport_empty",
            "severity": 1,
            "detail": "Transport section empty — no TfGM/Metrolink alerts surfaced.",
        })

    n_24h = _section_count("Свежие новости")
    n_today = _section_count("Что важно сегодня")
    n_radar = _section_count("Городской радар")
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

    not_selected_tickets = int(qc.get("dropped_ticket_not_selected") or 0)
    held_editorial = int(qc.get("held_for_editorial_quality") or 0)
    selected_for_publication = max(0, included - not_selected_tickets - held_editorial)
    if selected_for_publication >= 20 and rendered < selected_for_publication * 0.75:
        signals.append({
            "name": "low_selected_writer_yield",
            "severity": 2,
            "detail": (
                f"Writer rendered {rendered} of {selected_for_publication} selected publishable candidates "
                f"({rendered / selected_for_publication:.0%}); inventory/backup/manual-review items are excluded."
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


# Categories where "fresh in last 24h" is the right liveness signal —
# these are news feeds expected to publish daily. For evergreen feeds
# (venues, culture, diaspora, food openings) candidates accumulate
# over weeks and fresh-24h=0 is the normal case, not staleness.
_FRESHNESS_SENSITIVE_CATEGORIES = frozenset({
    "media_layer", "gmp", "transport", "public_services", "city_news",
    "football", "tech_business",
})


def _classify_source_status(entry: dict, category: str) -> tuple[str, str]:
    """R1: one-word status per source + short human-readable detail."""
    errors = list(entry.get("errors") or [])
    warnings = list(entry.get("warnings") or [])
    fetched = bool(entry.get("fetched"))
    cands = int(entry.get("candidate_count") or 0)
    fresh = int(entry.get("fresh_last_24h_count") or 0)
    contract = str(entry.get("source_contract") or "")
    signal = int(entry.get("coverage_signal_count") or 0)
    signal_label = str(entry.get("coverage_signal_label") or "coverage item(s)")
    if not fetched or errors:
        return "failed", (errors[0] if errors else "fetch failed")[:140]
    if entry.get("not_modified"):
        return "stale", "source reachable; no changed content since last run (304 Not Modified)"
    if cands == 0:
        return "empty", "fetched but no candidate links parsed"
    if contract == "hard_news_daily" and fresh == 0:
        return "stale", f"{cands} item(s) but 0 fresh in last 24h — feed is dormant"
    if contract in {"event_calendar", "venue_calendar", "ticket_api"}:
        if signal == 0:
            return "empty", f"{cands} item(s) but 0 usable {signal_label}"
        if warnings:
            return "partial", "; ".join(warnings)[:140]
        return "ok", f"{signal} {signal_label}"
    if category in _FRESHNESS_SENSITIVE_CATEGORIES and fresh == 0 and contract in {"transport_live"}:
        return "stale", f"{cands} item(s) but 0 fresh in last 24h — feed is dormant"
    if warnings:
        return "partial", "; ".join(warnings)[:140]
    return "ok", f"{cands} item(s)" + (f", {fresh} fresh in last 24h" if fresh else "")


def _summarise_synthetic_freshness(candidates_report: dict | None) -> dict[str, object]:
    """O2: surface every synthetic candidate's freshness state.

    Returns:
        - ``total`` — count of candidates with ``synthetic=True``.
        - ``stale_count`` — how many of those are ``synthetic_stale=True``.
        - ``stale_sources`` — sorted unique ``source_label`` of stale items.
        - ``degraded_count`` — how many synthetic items shipped with degraded
          placeholder facts rather than freshly fetched source facts.
        - ``items[]`` — per-candidate triplet of (source_label,
          data_fetched_at, attempts) for the release_report drilldown.
    """
    items: list[dict[str, object]] = []
    stale_sources: set[str] = set()
    degraded_sources: set[str] = set()
    total = 0
    stale_count = 0
    degraded_count = 0
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict) or not candidate.get("synthetic"):
            continue
        total += 1
        is_stale = bool(candidate.get("synthetic_stale"))
        weather_facts = candidate.get("weather_facts") if isinstance(candidate.get("weather_facts"), dict) else {}
        is_degraded = bool(
            candidate.get("synthetic_degraded")
            or is_stale
            or weather_facts.get("degraded")
            or weather_facts.get("placeholder")
        )
        source_label = str(candidate.get("source_label") or "")
        if is_stale:
            stale_count += 1
            if source_label:
                stale_sources.add(source_label)
        if is_degraded:
            degraded_count += 1
            if source_label:
                degraded_sources.add(source_label)
        item = {
            "source_label": source_label,
            "primary_block": str(candidate.get("primary_block") or ""),
            "synthetic_stale": is_stale,
            "degraded": is_degraded,
            "data_fetched_at": candidate.get("data_fetched_at"),
            "fetch_attempts": int(candidate.get("synthetic_fetch_attempts") or 0),
        }
        if weather_facts:
            item["weather_facts"] = {
                "status": weather_facts.get("status"),
                "source": weather_facts.get("source"),
                "hourly": weather_facts.get("hourly") or [],
                "morning_temp_c": weather_facts.get("morning_temp_c"),
                "min_temp_c": weather_facts.get("min_temp_c"),
                "max_temp_c": weather_facts.get("max_temp_c"),
                "rain_probability_max": weather_facts.get("rain_probability_max"),
                "warnings": weather_facts.get("warnings") or [],
                "placeholder": bool(weather_facts.get("placeholder")),
                "degraded": bool(weather_facts.get("degraded")),
            }
        items.append(item)
    return {
        "total": total,
        "stale_count": stale_count,
        "stale_sources": sorted(stale_sources),
        "degraded_count": degraded_count,
        "degraded_sources": sorted(degraded_sources),
        "items": items,
    }


def _count_per_source_yield(
    candidates_report: dict | None,
    rendered_fingerprints: set[str] | list[str] | dict | None,
    writer_report: dict | None = None,
) -> dict[str, dict[str, object]]:
    """O1: count, per source_label, how many candidates survived each
    downstream stage. Two columns matter for editorial review:
      - curated: include=True after curator (= what went into writer)
      - rendered: curated AND fingerprint appears in writer's
                  rendered_candidate_fingerprints (= what shipped in HTML).

    rendered ≤ curated by construction. A source with curated>0 and
    rendered=0 is a flag — its material was killed late (writer quality
    gate, editor balance trim) and may need attention.
    """
    if isinstance(rendered_fingerprints, dict):
        rendered_set = set(rendered_fingerprints.get("rendered_candidate_fingerprints") or ())
    else:
        rendered_set = set(rendered_fingerprints or ())
    yields: dict[str, dict[str, object]] = {}
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        source_label = str(candidate.get("source_label") or "").strip()
        if not source_label:
            continue
        record = yields.setdefault(
            source_label,
            {
                "curated": 0,
                "rendered": 0,
                "reject_reasons": {},
                "loss_funnel": {
                    "candidate_pool": 0,
                    "rejected_before_writer": 0,
                    "backup_before_rewrite": 0,
                    "included_after_curation": 0,
                    "included_missing_draft_line": 0,
                    "writer_dropped": 0,
                    "rendered": 0,
                },
            },
        )
        funnel = record.get("loss_funnel")
        if isinstance(funnel, dict):
            funnel["candidate_pool"] = int(funnel.get("candidate_pool") or 0) + 1
        if not candidate.get("include"):
            if isinstance(funnel, dict):
                key = "backup_before_rewrite" if str(candidate.get("rewrite_shortlist_status") or "") == "backup_before_rewrite" else "rejected_before_writer"
                funnel[key] = int(funnel.get(key) or 0) + 1
            reasons = [str(r) for r in (candidate.get("reject_reasons") or []) if str(r).strip()]
            if not reasons:
                reason = str(candidate.get("reason") or "").strip()
                reasons = [reason[:90] or "rejected_before_writer"]
            reason_counts = record["reject_reasons"]
            if isinstance(reason_counts, dict):
                for reason in reasons:
                    reason_counts[reason] = int(reason_counts.get(reason) or 0) + 1
            continue
        record["curated"] = int(record["curated"]) + 1
        if isinstance(funnel, dict):
            funnel["included_after_curation"] = int(funnel.get("included_after_curation") or 0) + 1
            if not str(candidate.get("draft_line") or "").strip():
                funnel["included_missing_draft_line"] = int(funnel.get("included_missing_draft_line") or 0) + 1
        fp = str(candidate.get("fingerprint") or "")
        if fp and fp in rendered_set:
            record["rendered"] = int(record["rendered"]) + 1
            if isinstance(funnel, dict):
                funnel["rendered"] = int(funnel.get("rendered") or 0) + 1
    cand_by_fp = {
        str(c.get("fingerprint") or ""): c
        for c in (candidates_report or {}).get("candidates") or []
        if isinstance(c, dict)
    }
    for drop in (writer_report or {}).get("dropped_candidates") or []:
        if not isinstance(drop, dict):
            continue
        fp = str(drop.get("fingerprint") or "")
        cand = cand_by_fp.get(fp) or {}
        source_label = str(cand.get("source_label") or drop.get("source_label") or "").strip()
        if not source_label:
            continue
        record = yields.setdefault(
            source_label,
            {
                "curated": 0,
                "rendered": 0,
                "reject_reasons": {},
                "loss_funnel": {
                    "candidate_pool": 0,
                    "rejected_before_writer": 0,
                    "backup_before_rewrite": 0,
                    "included_after_curation": 0,
                    "included_missing_draft_line": 0,
                    "writer_dropped": 0,
                    "rendered": 0,
                },
            },
        )
        funnel = record.get("loss_funnel")
        if isinstance(funnel, dict):
            funnel["writer_dropped"] = int(funnel.get("writer_dropped") or 0) + 1
        reason_counts = record["reject_reasons"]
        if not isinstance(reason_counts, dict):
            continue
        for reason in (drop.get("reasons") or ["writer_drop"]):
            label = str(reason or "writer_drop").strip()[:90]
            reason_counts[label] = int(reason_counts.get(label) or 0) + 1
    return yields


def _source_funnel_human(name: str, row: dict[str, object]) -> dict[str, object]:
    funnel = row.get("loss_funnel") if isinstance(row.get("loss_funnel"), dict) else {}
    raw = int(row.get("raw_count") or row.get("candidate_count") or funnel.get("source_raw_count") or funnel.get("candidate_pool") or 0)
    rejected = int(funnel.get("rejected_before_writer") or 0)
    backup = int(funnel.get("backup_before_rewrite") or 0)
    curated = int(row.get("curated_count") or row.get("accepted_count") or funnel.get("included_after_curation") or 0)
    missing = int(funnel.get("included_missing_draft_line") or 0)
    writer_dropped = int(funnel.get("writer_dropped") or 0)
    rendered = int(row.get("rendered_count") or funnel.get("rendered") or 0)
    duplicate_like = 0
    for reason, count in (row.get("reject_reasons") or {}).items():
        if "duplicate" in str(reason).lower() or "same story" in str(reason).lower() or "повтор" in str(reason).lower():
            duplicate_like += int(count or 0)

    lines = [
        f"Собрали: {raw}",
        f"Повторы / уже было: {duplicate_like}",
        f"Отклонено до редактора: {rejected}",
        f"Ушло в резерв до перевода: {backup}",
        f"Выбрано редактором: {curated}",
        f"Не получилось написать нормальную строку: {missing + writer_dropped}",
        f"Попало в выпуск: {rendered}",
    ]

    name_l = str(name or "").lower()
    if raw == 0:
        conclusion = "источник сегодня не дал материалов"
        action = "проверить, это нормальная тишина источника или проблема парсера"
    elif "ticketmaster" in name_l and rendered >= 8:
        conclusion = "источник живой, но билетная политика пропустила слишком много видимых строк"
        action = "не отключать источник; отбирать по реальному поводу и notability, а старые B-tier продажи держать в резерве"
    elif rendered > 0 and rendered >= max(1, curated // 2):
        conclusion = "источник даёт полезный вклад в выпуск"
        action = "оставить как есть; смотреть только на качество отдельных строк"
    elif duplicate_like >= max(2, raw // 2):
        conclusion = "источник живой, но в основном даёт дубли"
        action = "не отключать; усилить dedupe и не давать дублю вытеснять лучший источник"
    elif curated > 0 and rendered == 0 and (missing + writer_dropped) > 0:
        conclusion = "источник даёт кандидатов, но они теряются на написании строки"
        action = "проверить draft_line/rewrite для этого типа материалов"
    elif curated == 0 and raw > 0:
        conclusion = "источник живой, но материалы не проходят редакционный отбор"
        action = "проверить, это шумный источник или фильтр слишком строгий"
    else:
        conclusion = "источник требует ручной проверки по воронке"
        action = "смотреть примеры rejected/backup кандидатов"
    return {
        "template": lines,
        "conclusion": conclusion,
        "action": action,
        "one_line": f"{name}: {conclusion}; {action}.",
    }


def _source_loss_stage(row: dict[str, object]) -> dict[str, str]:
    """W10: attribute a zero-contribution source to the funnel stage where it
    lost everything — fetched → parsed → selected → written → visible. The loss
    stage is the FIRST step whose count fell to zero, so a "84 sources at 0" list
    stops being opaque: each one names where (and why) it dropped out. Empty dict
    for a source that reached the digest (rendered_count > 0), so only zero-yield
    rows are annotated and healthy ones add no noise.
    """
    raw = int(row.get("raw_count") or row.get("candidate_count") or 0)
    curated = int(row.get("curated_count") or row.get("accepted_count") or 0)
    rendered = int(row.get("rendered_count") or 0)
    if rendered > 0:
        return {}
    if int(row.get("failure_count") or 0) > 0 and raw == 0:
        klass = str(row.get("failure_class") or "").strip()
        return {"stage": "fetched", "reason": f"fetch failed ({klass})" if klass else "fetch failed"}
    if raw == 0:
        return {"stage": "parsed", "reason": "fetched OK but parsed 0 candidates (silent feed or parser drift)"}
    if curated == 0:
        reasons = row.get("reject_reasons") if isinstance(row.get("reject_reasons"), dict) else {}
        top = max(reasons.items(), key=lambda kv: int(kv[1] or 0))[0] if reasons else "rejected by validator/curator"
        return {"stage": "selected", "reason": f"{raw} candidate(s), 0 selected — top reason: {top}"}
    return {"stage": "written", "reason": f"{curated} selected but 0 reached visible HTML (lost at writer/editor draft_line)"}


def _weekend_source_coverage_report(sources: list[dict[str, object]]) -> dict[str, object]:
    """Weekend source-level audit.

    `weekend_inventory_loss_trace` explains candidate-level losses after an
    item exists. This report catches the earlier blind spot: trusted Weekend
    sources that fetched OK but yielded no usable visitor inventory at all.
    """
    watched = [
        row for row in sources
        if isinstance(row, dict)
        and row.get("category") != "synthetic"
        and not row.get("trial")
        and str(row.get("primary_block") or "") == "weekend_activities"
        and str(row.get("source_contract") or "") == "event_calendar"
    ]
    incidents: list[dict[str, object]] = []
    rendered_sources = 0
    for row in watched:
        raw = int(row.get("candidate_count") or row.get("raw_count") or 0)
        rendered = int(row.get("rendered_count") or 0)
        coverage_signal = int(row.get("coverage_signal_count") or 0)
        if rendered > 0:
            rendered_sources += 1
            continue
        if not row.get("fetched") or int(row.get("failure_count") or 0) > 0:
            stage = "fetch_failed"
            reason = str(row.get("loss_reason") or row.get("detail") or "source did not fetch cleanly")
        elif raw == 0:
            stage = "parser_empty"
            reason = "trusted Weekend source fetched OK but parsed 0 candidates"
        elif coverage_signal == 0:
            stage = "date_not_recovered"
            reason = "source produced candidates but no usable dated Weekend signal"
        else:
            stage = str(row.get("loss_stage") or "not_rendered")
            reason = str(row.get("loss_reason") or "source produced candidates but none reached visible Weekend output")
        incidents.append(
            {
                "name": str(row.get("name") or ""),
                "stage": stage,
                "reason": reason[:180],
                "candidate_count": raw,
                "coverage_signal_count": coverage_signal,
                "curated_count": int(row.get("curated_count") or 0),
                "rendered_count": rendered,
                "url": str(row.get("url") or ""),
                "recommended_next_action": str(row.get("recommended_next_action") or ""),
            }
        )
    counts: dict[str, int] = {}
    for item in incidents:
        stage = str(item.get("stage") or "unknown")
        counts[stage] = counts.get(stage, 0) + 1
    return {
        "schema_version": 1,
        "checked_sources": len(watched),
        "rendered_sources": rendered_sources,
        "incident_count": len(incidents),
        "counts_by_stage": counts,
        "incidents": incidents[:80],
    }


def _summarise_source_health(
    scan_report: dict | None,
    candidates_report: dict | None = None,
    rendered_fingerprints: set[str] | list[str] | dict | None = None,
    writer_report: dict | None = None,
) -> dict[str, object]:
    """R1: per-source status table + counts. Reads collector_report.json.

    O1 extension: each source row also carries `curated_count` and
    `rendered_count` so editorial review can see — at a glance — which
    sources contributed material that actually shipped vs. which were
    killed downstream. Synthetic sources that appear on candidates but
    not in collector_report (Met Office weather, transport-fill
    reminders) are appended as `category="synthetic"` rows so the table
    is complete.
    """
    counts: dict[str, int] = {"ok": 0, "partial": 0, "stale": 0, "empty": 0, "failed": 0}
    sources: list[dict[str, object]] = []
    yields = _count_per_source_yield(candidates_report, rendered_fingerprints, writer_report)
    seen_names: set[str] = set()

    if scan_report:
        for cat_name, cat in (scan_report.get("categories") or {}).items():
            if not isinstance(cat, dict):
                continue
            for entry in cat.get("source_health") or []:
                if not isinstance(entry, dict):
                    continue
                status, detail = _classify_source_status(entry, str(cat_name))
                counts[status] = counts.get(status, 0) + 1
                name = str(entry.get("name") or "")
                seen_names.add(name)
                row_yield = yields.get(name) or {"curated": 0, "rendered": 0, "reject_reasons": {}, "loss_funnel": {}}
                raw_count = int(entry.get("candidate_count") or 0)
                accepted_count = int(row_yield["curated"])
                loss_funnel = dict(row_yield.get("loss_funnel") or {})
                loss_funnel["source_raw_count"] = raw_count
                row = {
                    "name": name,
                    "category": str(cat_name),
                    "status": status,
                    "detail": detail,
                    "raw_count": raw_count,
                    "accepted_count": accepted_count,
                    "rejected_count": max(raw_count - accepted_count, 0),
                    "rendered_count": int(row_yield["rendered"]),
                    "reject_reasons": row_yield.get("reject_reasons") or {},
                    "loss_funnel": loss_funnel,
                    "failure_count": len(list(entry.get("errors") or [])),
                    "failure_class": str(entry.get("failure_class") or ""),
                    "reliability_ladder_step": str(entry.get("reliability_ladder_step") or ""),
                    "recommended_next_action": str(entry.get("recommended_next_action") or ""),
                    "fetched": bool(entry.get("fetched")),
                    "primary_block": str(entry.get("primary_block") or ""),
                    "url": str(entry.get("url") or ""),
                    "candidate_count": raw_count,
                    "fresh_last_24h_count": int(entry.get("fresh_last_24h_count") or 0),
                    "source_contract": str(entry.get("source_contract") or ""),
                    "trial": bool(entry.get("trial")),
                    "coverage_signal_count": int(entry.get("coverage_signal_count") or 0),
                    "coverage_signal_label": str(entry.get("coverage_signal_label") or ""),
                    "curated_count": int(row_yield["curated"]),
                }
                row["human_funnel"] = _source_funnel_human(name, row)
                sources.append(row)

    # Append synthetic sources (Met Office weather, transport_fill
    # reminders) that bypass the core collector. Status is derived from
    # yield alone: rendered>0 ⇒ ok, curated>0 but rendered=0 ⇒ partial,
    # everything else ⇒ empty.
    for name, row in sorted(yields.items()):
        if name in seen_names:
            continue
        if row["rendered"] > 0:
            status = "ok"
            detail = f"synthetic: {row['rendered']} item(s) rendered"
        elif row["curated"] > 0:
            status = "partial"
            detail = f"synthetic: {row['curated']} curated but 0 rendered"
        else:
            status = "empty"
            detail = "synthetic: no candidates survived"
        counts[status] = counts.get(status, 0) + 1
        source_row = {
            "name": name,
            "category": "synthetic",
            "status": status,
            "detail": detail,
            "raw_count": int(row["curated"]),
            "accepted_count": int(row["curated"]),
            "rejected_count": 0,
            "rendered_count": int(row["rendered"]),
            "reject_reasons": row.get("reject_reasons") or {},
            "loss_funnel": row.get("loss_funnel") or {},
            "failure_count": 0,
            "failure_class": "",
            "reliability_ladder_step": "",
            "recommended_next_action": "",
            "fetched": True,
            "primary_block": "",
            "url": "",
            "candidate_count": int(row["curated"]),
            "fresh_last_24h_count": 0,
            "curated_count": int(row["curated"]),
        }
        source_row["human_funnel"] = _source_funnel_human(name, source_row)
        sources.append(source_row)

    # Zero-yield sources: fetched OK but contributed nothing past the
    # curator. Surface as a top-level counter so the after-run summary
    # can flag silent waste (we kept fetching the feed but its output
    # never reached the digest).
    zero_yield = sum(
        1
        for row in sources
        if row.get("category") != "synthetic"
        and not row.get("trial")
        and int(row.get("candidate_count") or 0) > 0
        and int(row.get("rendered_count") or 0) == 0
    )
    counts["zero_yield"] = zero_yield

    # "No contribution" sources read as panic when lumped into one number.
    # Split them so the summary distinguishes a real fault (fetch_failed)
    # from a quiet feed (idle_no_items) from material the editor cut
    # (all_rejected). idle_no_items is normal and expected on most days.
    no_contribution = {"fetch_failed": 0, "all_rejected": 0, "idle_no_items": 0}
    for row in sources:
        if row.get("category") == "synthetic":
            continue
        if row.get("trial"):
            continue
        if int(row.get("rendered_count") or 0) > 0:
            continue
        if int(row.get("failure_count") or 0) > 0:
            no_contribution["fetch_failed"] += 1
        elif int(row.get("candidate_count") or 0) > 0:
            no_contribution["all_rejected"] += 1
        else:
            no_contribution["idle_no_items"] += 1
    counts["no_contribution_breakdown"] = no_contribution

    # W10: stamp the loss stage + reason onto every zero-contribution source so
    # each "source at 0" is debuggable on its own row, and aggregate the stages.
    zero_contribution_by_stage: dict[str, int] = {}
    for row in sources:
        if row.get("category") == "synthetic" or row.get("trial"):
            continue
        if int(row.get("rendered_count") or 0) > 0:
            continue
        stage_info = _source_loss_stage(row)
        if not stage_info:
            continue
        row["loss_stage"] = stage_info["stage"]
        row["loss_reason"] = stage_info["reason"]
        zero_contribution_by_stage[stage_info["stage"]] = zero_contribution_by_stage.get(stage_info["stage"], 0) + 1
    counts["zero_contribution_by_stage"] = zero_contribution_by_stage
    weekend_source_coverage = _weekend_source_coverage_report(sources)
    counts["weekend_source_coverage_incidents"] = int(weekend_source_coverage["incident_count"])

    return {"counts": counts, "sources": sources, "weekend_source_coverage": weekend_source_coverage}


def _summarise_transport_coverage(
    scan_report: dict | None,
    candidates_report: dict | None,
    rendered_fingerprints: set[str] | list[str] | dict | None,
) -> dict[str, object]:
    rendered_set = set(rendered_fingerprints or ())
    category = ((scan_report or {}).get("categories") or {}).get("transport") or {}
    checked = bool(category.get("checked"))
    source_flags = {"tfgm_checked": False, "metrolink_checked": False, "national_rail_checked": False}
    rows: list[dict[str, object]] = []
    for entry in category.get("source_health") or []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "")
        lowered = name.lower()
        if "tfgm" in lowered:
            source_flags["tfgm_checked"] = True
        if "metrolink" in lowered:
            source_flags["metrolink_checked"] = True
        if "national rail" in lowered:
            source_flags["national_rail_checked"] = True
        rows.append(
            {
                "name": name,
                "status": "failed" if entry.get("errors") else "checked",
                "raw_count": int(entry.get("candidate_count") or 0),
                "errors": entry.get("errors") or [],
            }
        )
    transport_candidates = [
        c for c in (candidates_report or {}).get("candidates") or []
        if isinstance(c, dict) and str(c.get("primary_block") or "") == "transport"
    ]
    found = [c for c in transport_candidates if c.get("include")]
    if any(
        re.search(
            r"\b(metrolink|tram|trams|rochdale line|airport line|bury line|"
            r"ashton line|eccles line|east didsbury line|piccadilly tram stop)\b",
            " ".join(
                str(c.get(field) or "")
                for field in ("title", "summary", "lead", "draft_line", "source_label")
            ).lower(),
        )
        for c in found
    ):
        source_flags["metrolink_checked"] = True
    rendered = [c for c in found if str(c.get("fingerprint") or "") in rendered_set]
    if rendered:
        verdict = "disruptions_rendered"
    elif found:
        verdict = "found_not_rendered"
    elif checked and rows and not any(row["status"] == "failed" for row in rows):
        verdict = "checked_no_disruptions"
    elif checked:
        verdict = "partially_checked"
    else:
        verdict = "not_checked"
    return {
        "checked": checked,
        **source_flags,
        "disruptions_found": len(found),
        "disruptions_rendered": len(rendered),
        "verdict": verdict,
        "sources": rows,
    }


def _summarise_diaspora_diagnostics(scan_report: dict | None, source_status: dict) -> dict[str, object]:
    category = ((scan_report or {}).get("categories") or {}).get("diaspora_events") or {}
    rows = [
        row for row in (source_status.get("sources") or [])
        if isinstance(row, dict) and row.get("category") == "diaspora_events"
    ]
    raw = sum(int(row.get("raw_count") or row.get("candidate_count") or 0) for row in rows)
    accepted = sum(int(row.get("accepted_count") or row.get("curated_count") or 0) for row in rows)
    rendered = sum(int(row.get("rendered_count") or 0) for row in rows)
    if rendered:
        verdict = "rendered"
    elif accepted:
        verdict = "accepted_not_rendered"
    elif raw:
        verdict = "fetched_but_filtered"
    elif category.get("checked"):
        verdict = "checked_empty"
    else:
        verdict = "not_checked"
    return {
        "checked": bool(category.get("checked")),
        "raw_count": raw,
        "accepted_count": accepted,
        "rendered_count": rendered,
        "verdict": verdict,
        "sources": rows,
        "source_expansion_note": (
            "RuPub/Telegram/VK should be added only through stable public event pages or an explicit RSS/API bridge; "
            "do not scrape private social feeds directly in the daily release path."
        ),
    }


def _candidate_by_source_url(candidates_report: dict | None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        url = str(candidate.get("source_url") or "").strip()
        if url:
            out[url] = candidate
            ident = canonical_url_identity(url)
            if ident:
                out[ident] = candidate
    return out


def _candidate_for_rendered_url(by_url: dict[str, dict], url: object) -> dict | None:
    raw = html.unescape(str(url or "")).strip()
    if not raw:
        return None
    return by_url.get(raw) or by_url.get(canonical_url_identity(raw))


def _published_by_fingerprint(published_facts: dict | None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    facts = (published_facts or {}).get("facts") if isinstance(published_facts, dict) else []
    for fact in facts or []:
        if not isinstance(fact, dict):
            continue
        fp = str(fact.get("fingerprint") or "").strip()
        if fp:
            out[fp] = fact
    return out


def _rendered_html_lines(html_text: str) -> list[dict[str, object]]:
    lines: list[dict[str, object]] = []
    for line in html_text.splitlines():
        raw = line.strip()
        if not raw.startswith("•"):
            continue
        urls = re.findall(r'<a\b[^>]*href=["\']([^"\']+)["\']', raw, flags=re.IGNORECASE)
        visible = _visible_text_from_html(raw)
        lines.append({"html": raw, "visible_text": visible, "urls": urls})
    return lines


def _classify_visible_repeat_policy(
    html_text: str,
    candidates_report: dict | None,
    published_facts: dict | None,
) -> dict[str, object]:
    by_url = _candidate_by_source_url(candidates_report)
    previous_by_fp = _published_by_fingerprint(published_facts)
    bad: list[dict[str, object]] = []
    checked = 0
    allowed = 0
    for row in _rendered_html_lines(html_text):
        matched: list[dict] = []
        seen_fps: set[str] = set()
        for url in row["urls"]:
            candidate = _candidate_for_rendered_url(by_url, url)
            if not candidate:
                continue
            fp = str(candidate.get("fingerprint") or "")
            if fp in seen_fps:
                continue
            seen_fps.add(fp)
            matched.append(candidate)
        for candidate in matched:
            fp = str(candidate.get("fingerprint") or "")
            previous = previous_by_fp.get(fp)
            if not previous:
                continue
            checked += 1
            verdict = visible_repeat_verdict(candidate, previous)
            if verdict.allow:
                allowed += 1
                continue
            bad.append(
                {
                    "fingerprint": fp,
                    "title": candidate.get("title"),
                    "source_label": candidate.get("source_label"),
                    "primary_block": candidate.get("primary_block"),
                    "category": candidate.get("category"),
                    "visible_text": row["visible_text"],
                    "repeat_policy": verdict.as_dict(),
                }
            )
    return {
        "counts": {
            "visible_lines": len(_rendered_html_lines(html_text)),
            "checked_exact_previous": checked,
            "allowed_visible_repeats": allowed,
            "bad_visible_repeats": len(bad),
        },
        "bad_visible_repeats": bad[:30],
    }


def _quarantine_repeat_rendered_html_items(
    html_text: str,
    candidates_report: dict | None,
    repeat_review: dict[str, object],
) -> tuple[str, dict[str, object]]:
    bad_items = repeat_review.get("bad_visible_repeats") if isinstance(repeat_review, dict) else []
    bad_fps = {
        str(item.get("fingerprint") or "")
        for item in bad_items or []
        if isinstance(item, dict) and item.get("fingerprint")
    }
    by_url = _candidate_by_source_url(candidates_report)
    bad_url_count = sum(
        1
        for candidate in by_url.values()
        if str(candidate.get("fingerprint") or "") in bad_fps
    )
    kept: list[str] = []
    removed: list[dict[str, object]] = []
    for line in html_text.splitlines():
        raw = line.strip()
        if not raw.startswith("•"):
            kept.append(line)
            continue
        urls = re.findall(r'<a\b[^>]*href=["\']([^"\']+)["\']', raw, flags=re.IGNORECASE)
        if any(
            (candidate := _candidate_for_rendered_url(by_url, url)) is not None
            and str(candidate.get("fingerprint") or "") in bad_fps
            for url in urls
        ):
            removed.append(
                {
                    "visible_text": re.sub(r"\s+", " ", _visible_text_from_html(raw)).strip()[:240],
                    "urls": urls,
                    "matched_by": "url",
                }
            )
            continue
        kept.append(line)
    return "\n".join(kept).strip() + ("\n" if kept else ""), {
        "attempted": bool(bad_fps),
        "bad_fingerprints": sorted(bad_fps),
        "bad_url_count": bad_url_count,
        "removed_count": len(removed),
        "removed_items": removed[:30],
    }


def _classify_rendered_html_quality(html_text: str, candidates_report: dict | None) -> dict[str, object]:
    """A2: inspect what actually reached Telegram HTML, not candidates.json."""
    by_url = _candidate_by_source_url(candidates_report)
    bad: list[dict[str, object]] = []
    for row in _rendered_html_lines(html_text):
        matched = [
            candidate
            for candidate in (_candidate_for_rendered_url(by_url, url) for url in row["urls"])
            if candidate is not None
        ]
        for candidate in matched:
            reasons: list[str] = []
            if candidate.get("editorial_status") == "borderline" and candidate.get("manual_override") != "force_include":
                reasons.append("borderline_visible")
            if candidate.get("reject_reasons") and not candidate.get("include"):
                reasons.append("rejected_candidate_visible")
            if candidate.get("quality_warnings") and any(
                str(w).startswith(("crime_borderline", "property_borderline"))
                for w in candidate.get("quality_warnings") or []
            ):
                visible_text = str(row["visible_text"] or "")
                if _visible_line_still_unclear_after_repair(visible_text):
                    reasons.append("unclear_candidate_visible")
            if reasons:
                bad.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": candidate.get("title"),
                        "source_label": candidate.get("source_label"),
                        "reasons": reasons,
                        "visible_text": row["visible_text"],
                    }
                )
    return {
        "counts": {
            "visible_lines": len(_rendered_html_lines(html_text)),
            "bad_visible_items": len(bad),
        },
        "bad_visible_items": bad[:20],
    }


def _quarantine_bad_rendered_html_items(
    html_text: str,
    candidates_report: dict | None,
    rendered_html_review: dict[str, object],
) -> tuple[str, dict[str, object]]:
    bad_items = rendered_html_review.get("bad_visible_items") if isinstance(rendered_html_review, dict) else []
    bad_fps = {
        str(item.get("fingerprint") or "")
        for item in bad_items or []
        if isinstance(item, dict) and item.get("fingerprint")
    }
    bad_visible_texts = {
        re.sub(r"\s+", " ", str(item.get("visible_text") or "")).strip()
        for item in bad_items or []
        if isinstance(item, dict) and item.get("visible_text")
    }
    by_url = _candidate_by_source_url(candidates_report)
    bad_urls = {
        url
        for url, candidate in by_url.items()
        if str(candidate.get("fingerprint") or "") in bad_fps
    }
    kept: list[str] = []
    removed: list[dict[str, object]] = []
    for line in html_text.splitlines():
        raw = line.strip()
        if not raw.startswith("•"):
            kept.append(line)
            continue
        urls = re.findall(r'<a\b[^>]*href=["\']([^"\']+)["\']', raw, flags=re.IGNORECASE)
        visible_text = re.sub(r"\s+", " ", _visible_text_from_html(raw)).strip()
        matched_by_url = any(url in bad_urls for url in urls)
        matched_by_text = visible_text in bad_visible_texts
        if matched_by_url or matched_by_text:
            removed.append(
                {
                    "visible_text": visible_text[:240],
                    "urls": urls,
                    "matched_by": "url" if matched_by_url else "visible_text",
                }
            )
            continue
        kept.append(line)
    report = {
        "attempted": bool(bad_fps or bad_visible_texts),
        "bad_fingerprints": sorted(bad_fps),
        "bad_url_count": len(bad_urls),
        "removed_count": len(removed),
        "removed_items": removed[:20],
    }
    return "\n".join(kept).strip() + ("\n" if kept else ""), report


def _force_remove_bad_rendered_lines(
    html_text: str,
    rendered_html_review: dict[str, object],
) -> tuple[str, dict[str, object]]:
    """Last-resort, text-tolerant removal of any visible line still flagged
    bad after the precise URL/exact-text quarantine. A residual bad row must
    never block the whole issue — drop it and ship the rest."""
    bad_items = rendered_html_review.get("bad_visible_items") if isinstance(rendered_html_review, dict) else []
    keys: list[str] = []
    for item in bad_items or []:
        if isinstance(item, dict):
            text = re.sub(r"\s+", " ", str(item.get("visible_text") or "")).strip().lower()
            if len(text) >= 20:
                keys.append(text[:60])
    if not keys:
        return html_text, {"removed_count": 0}
    kept: list[str] = []
    removed = 0
    for line in html_text.splitlines():
        raw = line.strip()
        if raw.startswith("•"):
            vis = re.sub(r"\s+", " ", _visible_text_from_html(raw)).strip().lower()
            if vis and any(key in vis or vis[:60] in key for key in keys):
                removed += 1
                continue
        kept.append(line)
    return "\n".join(kept).strip() + ("\n" if kept else ""), {"removed_count": removed}


def _visible_line_still_unclear_after_repair(visible_text: str) -> bool:
    text = re.sub(r"\s+", " ", str(visible_text or "")).strip()
    if len(text) < 95:
        return True
    lowered = text.lower()
    vague_markers = (
        "появилось обновление",
        "есть новый городской контекст",
        "это важный сигнал",
        "это заметный кейс",
        "подробнее пока неясно",
        "данные уточняются",
    )
    if any(marker in lowered for marker in vague_markers):
        return True
    return False


def _borderline_queue(candidates_report: dict | None, writer_report: dict | None) -> dict[str, object]:
    writer_hold = {
        str(drop.get("fingerprint") or "")
        for drop in (writer_report or {}).get("dropped_candidates") or []
        if isinstance(drop, dict)
        and any("manual review" in str(reason).lower() for reason in (drop.get("reasons") or []))
    }
    items: list[dict[str, object]] = []
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if candidate.get("editorial_status") != "borderline" and fp not in writer_hold:
            continue
        if candidate.get("manual_override") == "force_include":
            continue
        # Derive a reason_code so the audit-trail / Telegram report can group
        # the borderline pool. Without this the queue previously stored
        # everything as "no_reason" which violated E16 (audit per story).
        warnings = [str(w) for w in (candidate.get("quality_warnings") or []) if str(w).strip()]
        ej = candidate.get("english_judge") if isinstance(candidate.get("english_judge"), dict) else {}
        ej_codes = [str(c) for c in (ej.get("reason_codes") or []) if str(c).strip()]
        reason_code = next(
            (w.split(":")[0] for w in warnings if w),
            next(iter(ej_codes), None) or "borderline_unspecified",
        )
        items.append(
            {
                "fingerprint": fp,
                "title": candidate.get("title"),
                "source_label": candidate.get("source_label"),
                "primary_block": candidate.get("primary_block"),
                "category": candidate.get("category"),
                "reason_code": reason_code,
                "reason": candidate.get("reason") or "",
                "quality_warnings": warnings,
                "english_judge_reason_codes": ej_codes,
                "specificity_review": candidate.get("specificity_review") or {},
                "event_schema_completeness": candidate.get("event_schema_completeness") or {},
                "manual_include_hint": f'Add "{fp}" to data/state/manual_candidate_overrides.json force_include[]',
            }
        )
    by_reason: dict[str, int] = {}
    for it in items:
        rc = str(it.get("reason_code") or "borderline_unspecified")
        by_reason[rc] = by_reason.get(rc, 0) + 1
    return {
        "counts": {"borderline": len(items), "by_reason": by_reason},
        "items": items[:30],
    }


def _quality_scorecard(
    *,
    state_dir: Path,
    current_day_london: str,
    candidates_report: dict | None,
    writer_report: dict | None,
    rendered_fingerprints: set[str],
    source_status: dict,
    published_review: dict,
    transport_coverage: dict,
) -> dict[str, object]:
    candidates = [c for c in (candidates_report or {}).get("candidates") or [] if isinstance(c, dict)]
    rendered = [c for c in candidates if str(c.get("fingerprint") or "") in rendered_fingerprints]
    full_count = len(candidates)
    visible_count = len(rendered)
    source_counts = Counter(str(c.get("source_label") or "") for c in rendered if c.get("source_label"))
    top_sources = [
        {"source_label": name, "count": count, "share": round(count / visible_count, 3) if visible_count else 0}
        for name, count in source_counts.most_common(3)
    ]
    stale_or_bad = int((published_review.get("counts") or {}).get("suspiciously_published") or 0)
    unclear_visible = sum(
        1 for c in rendered
        if c.get("editorial_status") == "borderline"
        or any(str(w).startswith(("crime_borderline", "property_borderline")) for w in c.get("quality_warnings") or [])
    )
    repeat_visible = sum(1 for c in rendered if str(c.get("change_type") or "") in {"no_change", "same_story_rehash"})
    tickets = [c for c in candidates if str(c.get("primary_block") or "") in {"ticket_radar", "future_announcements", "next_7_days"} and str(c.get("category") or "") == "venues_tickets"]
    rendered_ticket_fps = {str(c.get("fingerprint") or "") for c in rendered if str(c.get("category") or "") == "venues_tickets"}
    from news_digest.pipeline.editorial_contracts import classify_ticket_type  # noqa: PLC0415

    ticket_types: dict[str, dict[str, int]] = {}
    for c in tickets:
        # ticket_type is only stamped on include=True candidates (the validator
        # runs _ensure_default_ticket_type behind the include gate). Deduped /
        # rejected repeats (e.g. "6LACK", "Jamie Webster" already seen) reach
        # the funnel with no ticket_type and used to pile into a misleading
        # "unknown" bucket (11 on 2026-06-01 — all just dedup losers, not a
        # classifier failure). Classify them on the fly so the funnel reads true.
        t = str(c.get("ticket_type") or "").strip() or classify_ticket_type(c)
        row = ticket_types.setdefault(t, {"fetched": 0, "published": 0})
        row["fetched"] += 1
        if str(c.get("fingerprint") or "") in rendered_ticket_fps:
            row["published"] += 1
    metric_design = [
        "top_size_vs_full_feed",
        "published_stale_unclear_repeat_share",
        "top_3_source_diversity",
        "ticket_funnel_by_type",
        "transport_checked_vs_rendered",
        "seven_day_trend",
    ]
    history_path = state_dir / "quality_scorecard_history.json"
    history = read_json(history_path, {"days": []}) if history_path.exists() else {"days": []}
    days = [d for d in history.get("days") or [] if isinstance(d, dict) and d.get("date") != current_day_london]
    today_row = {
        "date": current_day_london,
        "visible_count": visible_count,
        "full_count": full_count,
        "suspicious_published": stale_or_bad,
        "unclear_visible": unclear_visible,
        "repeat_visible": repeat_visible,
    }
    days.append(today_row)
    days = days[-14:]
    write_json(history_path, {"days": days})
    week = days[-7:]
    avg_visible = round(sum(int(d.get("visible_count") or 0) for d in week) / len(week), 2) if week else 0
    return {
        "metric_design": metric_design,
        "today": {
            "visible_count": visible_count,
            "full_count": full_count,
            "top_size_vs_full_feed": round(visible_count / full_count, 3) if full_count else 0,
            "suspicious_published": stale_or_bad,
            "unclear_visible": unclear_visible,
            "repeat_visible": repeat_visible,
            "top_sources": top_sources,
            "ticket_types": ticket_types,
            "transport": transport_coverage,
            "source_zero_yield": int((source_status.get("counts") or {}).get("zero_yield") or 0),
        },
        "seven_day_trend": {
            "days": week,
            "avg_visible_count": avg_visible,
        },
    }


def _update_feedback_items(
    state_dir: Path,
    current_day_london: str,
    candidates_report: dict | None,
    rendered_fingerprints: set[str],
) -> dict[str, object]:
    path = state_dir / "personalization_feedback.json"
    payload = read_json(path, {"items": []}) if path.exists() else {"items": []}
    existing = {
        (str(item.get("date") or ""), str(item.get("fingerprint") or "")): item
        for item in payload.get("items") or []
        if isinstance(item, dict)
    }
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if not fp or fp not in rendered_fingerprints:
            continue
        key = (current_day_london, fp)
        item = existing.get(key, {})
        item.update(
            {
                "date": current_day_london,
                "fingerprint": fp,
                "title": candidate.get("title") or "",
                "source_label": candidate.get("source_label") or "",
                "category": candidate.get("category") or "",
                "primary_block": candidate.get("primary_block") or "",
                "scoring_trace": candidate.get("scoring_trace") or {},
                "reaction": item.get("reaction"),
                "reaction_source": item.get("reaction_source"),
                "reaction_at_london": item.get("reaction_at_london"),
            }
        )
        existing[key] = item
    rows = sorted(existing.values(), key=lambda item: (str(item.get("date") or ""), str(item.get("fingerprint") or "")))
    write_json(path, {"schema_version": 1, "items": rows[-1000:]})
    pending = sum(1 for item in rows if not item.get("reaction"))
    labelled = len(rows) - pending
    return {
        "path": str(path.resolve()),
        "rendered_items_recorded_today": len([item for item in rows if item.get("date") == current_day_london]),
        "total_items": len(rows),
        "labelled_items": labelled,
        "pending_items": pending,
    }


# R2: suspicious-reject classification.
# Patterns in writer drop reasons that almost certainly point to an
# LLM-formatting glitch rather than genuine low quality. £230m → £230млн
# tripping the evidence-substring check is a classic example.
_SUSPICIOUS_DROP_REASON_RE = re.compile(
    r"Pound amount\s+'?[^']+'?\s+not present in evidence_text",
    re.IGNORECASE,
)
# Curator drops worded as "evergreen без даты" — suspicious if the
# evidence_text actually contains a concrete date marker.
_DATE_HINT_IN_EVIDENCE = re.compile(
    r"\b\d{1,2}\s*(?:января|февраля|марта|апреля|мая|июня|июля|"
    r"августа|сентября|октября|ноября|декабря)\b"
    r"|\b\d{1,2}[/.\-]\d{1,2}\b"
    r"|\b(?:сегодня|завтра|послезавтра)\b"
    r"|\bв\s+(?:понедельник|вторник|среду|четверг|пятницу|субботу|воскресенье)\b",
    re.IGNORECASE,
)
_PREMIUM_SOURCE_PRIORITY = frozenset({
    "BBC Manchester",
    "BBC Manchester Web",
    "MEN",
    "MEN Latest News",
    "MEN News Sitemap",
    "ITV Granada Greater Manchester",
    "Place North West",
    "About Manchester News",
    "The Mill",
    "The Manc",
    "Manchester Council",
    "GMCA",
})

_PUBLISHED_UPDATE_MARKERS = re.compile(
    r"\b(today|this morning|this afternoon|yesterday|latest|update|updated|"
    r"sentenced|jailed|convicted|verdict|charged|arrested|appeal|"
    r"approved|rejected|confirmed|announced|launched|opened|closed|"
    r"warning|disruption|strike|closure)\b",
    re.IGNORECASE,
)


def _parse_candidate_day(raw: object) -> date | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _event_days(candidate: dict) -> list[date]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    days: list[date] = []
    for key in ("date_start", "date_end", "date"):
        parsed = _parse_candidate_day(event.get(key))
        if parsed:
            days.append(parsed)
    summary = str(candidate.get("summary") or "")
    for key in ("event_date", "public_onsale"):
        match = re.search(rf"\b{key}=(\d{{4}}-\d{{2}}-\d{{2}})", summary)
        if match:
            parsed = _parse_candidate_day(match.group(1))
            if parsed:
                days.append(parsed)
    return days


_EVENT_MISS_BLOCKS = frozenset({
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
})
_EVENT_MISS_CATEGORIES = frozenset({
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "diaspora_events",
})
_HIGH_VALUE_EVENT_RE = re.compile(
    r"\b(?:festival|exhibition|concert|gig|market|makers?|fair|trail|"
    r"theatre|play|comedy|stand-?up|workshop|talk|bank holiday|free)\b",
    re.IGNORECASE,
)
_LOW_VALUE_TICKET_TYPE = frozenset({"old_public_sale", "regular_upcoming"})
_EVENT_TITLE_STOPWORDS = frozenset({
    "the", "and", "for", "with", "from", "this", "that", "manchester",
    "festival", "event", "events", "show", "tickets", "ticket", "returns",
})


def _dedupe_drop_map(dedupe_memory: dict | None) -> dict[str, dict]:
    if not isinstance(dedupe_memory, dict):
        return {}
    drops: dict[str, dict] = {}
    for drop in dedupe_memory.get("intra_batch_dedup_drops") or []:
        if not isinstance(drop, dict):
            continue
        fp = str(drop.get("fingerprint") or "")
        if fp:
            drops[fp] = drop
    return drops


def _event_titles_look_same(first: str, second: str) -> bool:
    first_tokens = {
        token for token in re.findall(r"[a-zA-Z][a-zA-Z'-]{2,}", str(first or "").lower())
        if token not in _EVENT_TITLE_STOPWORDS
    }
    second_tokens = {
        token for token in re.findall(r"[a-zA-Z][a-zA-Z'-]{2,}", str(second or "").lower())
        if token not in _EVENT_TITLE_STOPWORDS
    }
    if not first_tokens or not second_tokens:
        return False
    return bool(first_tokens & second_tokens)


def _event_miss_score(candidate: dict, today: date) -> tuple[int, int | None]:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block not in _EVENT_MISS_BLOCKS and category not in _EVENT_MISS_CATEGORIES:
        return 0, None

    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("source_label", "title", "summary", "lead", "evidence_text")
    )
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    days = sorted(day for day in _event_days(candidate) if day >= today - timedelta(days=1))
    days_out = (days[0] - today).days if days else None

    score = 0
    if days_out is not None:
        if 0 <= days_out <= 3:
            score += 5
        elif days_out <= 7:
            score += 4
        elif days_out <= 30:
            score += 1
        else:
            score -= 3
    if _HIGH_VALUE_EVENT_RE.search(blob):
        score += 2
    if str(event.get("price") or "").strip() or re.search(r"\bfree\b|£\s*\d", blob, re.IGNORECASE):
        score += 1
    if str(event.get("date_text") or "").strip() and re.search(r"\d{1,2}\s*[–—-]\s*\d{1,2}", str(event.get("date_text") or "")):
        score += 1
    if str(event.get("borough") or "").strip():
        score += 1

    source_label = str(candidate.get("source_label") or "")
    try:
        from news_digest.pipeline.source_selection import source_tier

        if source_tier(source_label) <= 2:
            score += 2
    except Exception:  # noqa: BLE001 - diagnostics must not break release
        pass

    ticket_type = str(candidate.get("ticket_type") or "")
    if category == "venues_tickets" and ticket_type in _LOW_VALUE_TICKET_TYPE:
        score -= 4
    if re.search(r"\b(?:weekly|every)\b", blob, re.IGNORECASE) and (days_out is None or days_out > 7):
        score -= 3
    return score, days_out


def _event_miss_review(
    candidates_report: dict | None,
    writer_report: dict | None,
    rendered_fingerprints: set[str],
    current_day_london: str,
    dedupe_memory: dict | None = None,
) -> dict[str, object]:
    """Find high-value events/tickets that were collected but not published.

    This is the firewall for the Flower Festival class of failure: source
    coverage succeeded, but a later stage silently removed a useful event.
    """
    try:
        today = datetime.strptime(current_day_london, "%Y-%m-%d").date()
    except ValueError:
        today = datetime.strptime(today_london(), "%Y-%m-%d").date()

    rendered_set = {str(fp) for fp in rendered_fingerprints if fp}
    writer_drops = {
        str(item.get("fingerprint") or ""): item
        for item in ((writer_report or {}).get("dropped_candidates") or [])
        if isinstance(item, dict)
    }
    dedupe_drops = _dedupe_drop_map(dedupe_memory)
    candidates_by_fp = {
        str(candidate.get("fingerprint") or ""): candidate
        for candidate in (candidates_report or {}).get("candidates") or []
        if isinstance(candidate, dict)
    }

    items: list[dict[str, object]] = []
    critical: list[dict[str, object]] = []
    counts = Counter()
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if not fp or fp in rendered_set:
            continue
        score, days_out = _event_miss_score(candidate, today)
        if score < 7:
            continue

        reason = str(candidate.get("reason") or "")
        drop = writer_drops.get(fp)
        dedupe_drop = dedupe_drops.get(fp, {})
        kept_fp = str(dedupe_drop.get("kept_fingerprint") or "")
        kept_candidate = candidates_by_fp.get(kept_fp) or {}
        kept_title = str(dedupe_drop.get("kept_title") or kept_candidate.get("title") or "")
        covered_by_rendered_duplicate = bool(
            kept_fp
            and kept_fp in rendered_set
            and _event_titles_look_same(str(candidate.get("title") or ""), kept_title)
        )

        if covered_by_rendered_duplicate:
            verdict = "covered_by_rendered_duplicate"
        elif dedupe_drop:
            verdict = "dedupe_lost_event"
        elif fp in writer_drops:
            verdict = "writer_dropped_event"
            reason = "; ".join(str(r) for r in (drop.get("reasons") or [])) or reason
        elif candidate.get("include"):
            verdict = "selected_but_not_published"
        else:
            verdict = "rejected_high_value_event"
        reason_text = reason or "; ".join(str(r) for r in (candidate.get("reject_reasons") or []))
        reason_l = reason_text.lower()
        if verdict == "covered_by_rendered_duplicate":
            alert_class = "duplicate_or_covered"
        elif verdict == "dedupe_lost_event" and kept_fp:
            alert_class = "possible_miss"
        elif "ticket not selected" in reason_l or "not selected" in reason_l:
            alert_class = "not_selected_by_design"
        elif "outside global translation board" in reason_l:
            alert_class = "not_selected_capacity"
        elif "held for manual review" in reason_l or "borderline editorial status" in reason_l:
            alert_class = "manual_review"
        elif "past" in reason_l or "expired" in reason_l or "all event dates are in the past" in reason_l:
            alert_class = "stale_or_expired"
        elif verdict in {"writer_dropped_event", "selected_but_not_published"}:
            alert_class = "possible_miss"
        else:
            alert_class = "review_only"

        record = {
            "fingerprint": fp,
            "title": candidate.get("title") or "",
            "source_label": candidate.get("source_label") or "",
            "primary_block": candidate.get("primary_block") or "",
            "category": candidate.get("category") or "",
            "score": score,
            "days_out": days_out,
            "verdict": verdict,
            "alert_class": alert_class,
            "reason": reason_text,
            "kept_fingerprint": kept_fp,
            "kept_title": kept_title,
            "kept_source_label": dedupe_drop.get("kept_source_label") or "",
        }
        items.append(record)
        counts[verdict] += 1
        counts[f"alert_{alert_class}"] += 1
        # Conservative fail condition: a high-confidence event in the
        # next week disappeared without a rendered duplicate covering it.
        if alert_class == "possible_miss" and days_out is not None and 0 <= days_out <= 7:
            critical.append(record)

    items = sorted(items, key=lambda item: (int(item.get("days_out") if item.get("days_out") is not None else 999), -int(item.get("score") or 0), str(item.get("title") or "")))
    return {
        "counts": {
            "high_value_not_published": len(items),
            "critical_misses": len(critical),
            **dict(counts),
        },
        "critical_misses": critical[:20],
        "items": items[:50],
    }


def _disposition_for_candidate(
    candidate: dict,
    *,
    rendered_set: set[str],
    writer_drops: dict[str, dict],
    dedupe_drops: dict[str, dict],
) -> tuple[str, str]:
    fp = str(candidate.get("fingerprint") or "")
    if fp in rendered_set:
        return "rendered", ""
    if fp in writer_drops:
        drop = writer_drops[fp]
        return "writer_dropped", "; ".join(str(r) for r in (drop.get("reasons") or []))
    if fp in dedupe_drops:
        drop = dedupe_drops[fp]
        return "dedupe_dropped", str(drop.get("reason") or "dedupe drop")
    if candidate.get("include"):
        return "selected_not_published", ""
    return "rejected", str(candidate.get("reason") or "; ".join(str(r) for r in (candidate.get("reject_reasons") or [])))


def _is_independent_high_value_signal(candidate: dict) -> bool:
    if not isinstance(candidate, dict):
        return False
    apply_story_intelligence(candidate)
    lane = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    anchor = candidate.get("news_anchor") if isinstance(candidate.get("news_anchor"), dict) else {}
    judge = candidate.get("english_judge") if isinstance(candidate.get("english_judge"), dict) else {}
    score = float(candidate.get("section_board_score") or 0.0)
    if candidate.get("second_opinion_required") or candidate.get("backup_candidate"):
        return True
    if lane.get("protected"):
        return True
    if anchor.get("has_news_anchor") and score >= 80:
        return True
    return judge.get("decision") == "publish_candidate" and score >= 90


def _final_loss_check(
    *,
    candidates_report: dict | None,
    writer_report: dict | None,
    rendered_fingerprints: set[str],
    dedupe_memory: dict | None,
) -> dict[str, object]:
    """Warning-only check for valuable candidates that disappeared late.

    Event miss review is event-specific; this covers transport, public
    safety, civic/planning, Russian events, tickets and any anchored story
    with a strong independent score.
    """
    rendered_set = {str(fp) for fp in rendered_fingerprints if fp}
    writer_drops = {
        str(item.get("fingerprint") or ""): item
        for item in ((writer_report or {}).get("dropped_candidates") or [])
        if isinstance(item, dict)
    }
    dedupe_drops = _dedupe_drop_map(dedupe_memory)
    items: list[dict[str, object]] = []
    counts = Counter()
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if not fp or fp in rendered_set:
            continue
        if not _is_independent_high_value_signal(candidate):
            continue
        disposition, reason = _disposition_for_candidate(
            candidate,
            rendered_set=rendered_set,
            writer_drops=writer_drops,
            dedupe_drops=dedupe_drops,
        )
        reason_l = str(reason or "").lower()
        category = str(candidate.get("category") or "")
        if disposition == "rendered":
            alert_class = "rendered"
        elif "ticket not selected" in reason_l or "not selected" in reason_l:
            alert_class = "not_selected_by_design"
        elif disposition == "dedupe_dropped" and any(token in reason_l for token in ("duplicate", "same story", "multi-night", "merged")):
            alert_class = "duplicate_or_covered"
        elif "held for manual review" in reason_l or "borderline editorial status" in reason_l:
            alert_class = "manual_review"
        elif category == "venues_tickets" and disposition in {"writer_dropped", "selected_not_published"}:
            alert_class = "ticket_inventory_not_rendered"
        elif disposition in {"writer_dropped", "selected_not_published", "dedupe_dropped"}:
            alert_class = "possible_miss"
        else:
            alert_class = "review_only"
        lane = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
        enrichment = candidate.get("enrichment_health") if isinstance(candidate.get("enrichment_health"), dict) else {}
        frame = candidate.get("story_frame") if isinstance(candidate.get("story_frame"), dict) else {}
        recovery_trace = candidate.get("recovery_trace") if isinstance(candidate.get("recovery_trace"), list) else []
        recovery_plan = candidate.get("recovery_plan") if isinstance(candidate.get("recovery_plan"), dict) else {}
        record = {
            "fingerprint": fp,
            "title": candidate.get("title") or "",
            "source_label": candidate.get("source_label") or "",
            "category": candidate.get("category") or "",
            "primary_block": candidate.get("primary_block") or "",
            "disposition": disposition,
            "alert_class": alert_class,
            "reason": reason,
            "human_reason": (
                f"Не дошло до выпуска: {reason}. "
                f"Не хватило: {', '.join(frame.get('missing_facts') or []) or 'явных недостающих фактов нет'}."
            ),
            "story_frame": frame,
            "missing_facts": frame.get("missing_facts") or [],
            "recovery_trace": recovery_trace,
            "recovery_plan": recovery_plan,
            "protected_lanes": lane.get("lanes") or [],
            "section_board_score": candidate.get("section_board_score"),
            "false_negative_risk": (candidate.get("english_judge") or {}).get("false_negative_risk") if isinstance(candidate.get("english_judge"), dict) else "",
            "enrichment_warning": bool(enrichment.get("warning")),
            "manual_include_hint": f'Add "{fp}" to data/state/manual_candidate_overrides.json force_include[]',
        }
        items.append(record)
        counts[disposition] += 1
        counts[f"alert_{alert_class}"] += 1

    items = sorted(
        items,
        key=lambda item: (
            0 if item.get("enrichment_warning") else 1,
            -float(item.get("section_board_score") or 0.0),
            str(item.get("title") or ""),
        ),
    )
    critical = [
        item for item in items
        if item.get("alert_class") == "possible_miss"
    ]
    return {
        "schema_version": 1,
        "counts": {
            "high_value_not_rendered": len(items),
            "critical_losses": len(critical),
            **dict(counts),
        },
        "critical_losses": critical[:30],
        "items": items[:80],
    }


def _write_backup_pool(
    *,
    state_dir: Path,
    current_day_london: str,
    candidates_report: dict | None,
    writer_report: dict | None,
    rendered_fingerprints: set[str],
    final_loss_check: dict,
) -> dict[str, object]:
    path = state_dir / "backup_pool.json"
    existing_items: list[dict] = []
    if path.exists():
        try:
            existing = read_json(path, {"items": []})
            existing_items = [item for item in existing.get("items") or [] if isinstance(item, dict)]
        except Exception:  # noqa: BLE001
            existing_items = []

    today = datetime.strptime(current_day_london, "%Y-%m-%d").date()
    by_fp: dict[str, dict] = {}
    for item in existing_items:
        expires = str(item.get("expires_on_london") or "")
        try:
            if datetime.strptime(expires, "%Y-%m-%d").date() < today:
                continue
        except ValueError:
            continue
        fp = str(item.get("fingerprint") or "")
        if fp:
            by_fp[fp] = item

    loss_by_fp = {
        str(item.get("fingerprint") or ""): item
        for item in (final_loss_check.get("items") or [])
        if isinstance(item, dict)
    }
    rendered_set = {str(fp) for fp in rendered_fingerprints if fp}
    writer_drop_fps = {
        str(item.get("fingerprint") or "")
        for item in ((writer_report or {}).get("dropped_candidates") or [])
        if isinstance(item, dict)
    }
    for candidate in (candidates_report or {}).get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if not fp or fp in rendered_set:
            continue
        apply_story_intelligence(candidate)
        loss = loss_by_fp.get(fp)
        include_in_backup = bool(
            loss
            or candidate.get("backup_candidate")
            or candidate.get("second_opinion_required")
            or fp in writer_drop_fps
            or ((candidate.get("english_judge") or {}).get("decision") == "backup_candidate")
        )
        if not include_in_backup:
            continue
        reason = str((loss or {}).get("reason") or candidate.get("reason") or "")
        record = backup_pool_record(candidate, reason=reason, current_day_london=current_day_london)
        record["latest_disposition"] = (loss or {}).get("disposition") or ("writer_dropped" if fp in writer_drop_fps else "not_rendered")
        by_fp[fp] = record

    items = sorted(
        by_fp.values(),
        key=lambda item: (
            str(item.get("expires_on_london") or ""),
            -float(item.get("section_board_score") or 0.0),
            str(item.get("title") or ""),
        ),
    )
    payload = {
        "schema_version": 1,
        "updated_on_london": current_day_london,
        "counts": {
            "active": len(items),
            "expires_today": sum(1 for item in items if item.get("expires_on_london") == current_day_london),
        },
        "items": items[:250],
    }
    write_json(path, payload)
    return {**payload, "path": str(path.resolve())}


def _write_audit_trail(
    *,
    state_dir: Path,
    current_day_london: str,
    candidates_report: dict | None,
    writer_report: dict | None,
    rendered_fingerprints: set[str],
    dedupe_memory: dict | None,
    release_decision: str,
) -> str | None:
    candidates = [c for c in (candidates_report or {}).get("candidates") or [] if isinstance(c, dict)]
    if not candidates:
        return None
    audit_dir = state_dir / "audit_trail"
    audit_dir.mkdir(parents=True, exist_ok=True)
    path = audit_dir / f"{current_day_london}.jsonl"
    rendered_set = {str(fp) for fp in rendered_fingerprints if fp}
    writer_drops = {
        str(item.get("fingerprint") or ""): item
        for item in ((writer_report or {}).get("dropped_candidates") or [])
        if isinstance(item, dict)
    }
    dedupe_drops = _dedupe_drop_map(dedupe_memory)
    lines: list[str] = []
    for candidate in candidates:
        apply_story_intelligence(candidate)
        fp = str(candidate.get("fingerprint") or "")
        disposition, reason = _disposition_for_candidate(
            candidate,
            rendered_set=rendered_set,
            writer_drops=writer_drops,
            dedupe_drops=dedupe_drops,
        )
        record = {
            "schema_version": AUDIT_TRAIL_SCHEMA_VERSION,
            "run_date_london": current_day_london,
            "release_decision": release_decision,
            "fingerprint": fp,
            "title": candidate.get("title") or "",
            "source_label": candidate.get("source_label") or "",
            "source_url": candidate.get("source_url") or "",
            "category": candidate.get("category") or "",
            "primary_block": candidate.get("primary_block") or "",
            "input": {
                "published_at": candidate.get("published_at") or "",
                "lead": candidate.get("lead") or "",
                "summary": candidate.get("summary") or "",
                "evidence_text": str(candidate.get("evidence_text") or "")[:1800],
            },
            "judge": {
                "rubric_contract": candidate.get("rubric_contract") or {},
                "news_anchor": candidate.get("news_anchor") or {},
                "protected_lane": candidate.get("protected_lane") or {},
                "english_judge": candidate.get("english_judge") or {},
                "section_board_score": candidate.get("section_board_score"),
                "enrichment_health": candidate.get("enrichment_health") or {},
            },
            "final_disposition": disposition,
            "final_reason": reason,
            "include": bool(candidate.get("include")),
            "backup_candidate": bool(candidate.get("backup_candidate")),
            "second_opinion_required": bool(candidate.get("second_opinion_required")),
            "change_type": candidate.get("change_type") or "",
        }
        lines.append(json.dumps(record, ensure_ascii=False))
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    cutoff = datetime.strptime(current_day_london, "%Y-%m-%d").date() - timedelta(days=7)
    for file in audit_dir.glob("*.jsonl"):
        try:
            file_day = datetime.strptime(file.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if file_day < cutoff:
            file.unlink(missing_ok=True)
    return str(path.resolve())


def _visible_fingerprints_from_html(digest_html: str, candidates: list[dict]) -> set[str]:
    by_url: dict[str, str] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "").strip()
        url = str(candidate.get("source_url") or "").strip()
        if not fp or not url:
            continue
        ident = canonical_url_identity(url)
        if ident:
            by_url.setdefault(ident, fp)
    visible: set[str] = set()
    for href in re.findall(r'<a\s+[^>]*href="([^"]+)"', digest_html, flags=re.IGNORECASE):
        fp = by_url.get(canonical_url_identity(html.unescape(href)))
        if fp:
            visible.add(fp)
    return visible


def _candidate_selection_score(candidate: dict) -> float:
    for field in ("section_board_score", "reader_value_score", "source_score"):
        try:
            return float(candidate.get(field) or 0.0)
        except (TypeError, ValueError):
            continue
    return 0.0


def _candidate_selection_reason(candidate: dict, final_reason: str) -> str:
    for field in (
        "publish_plan_reason",
        "digest_selection_reason",
        "rewrite_shortlist_reason",
        "section_board_reason",
        "reason",
    ):
        value = str(candidate.get(field) or "").strip()
        if value:
            return value[:280]
    return str(final_reason or "")[:280]


def _read_jsonl_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _write_final_selection_report(
    *,
    state_dir: Path,
    current_day_london: str,
    candidates_report: dict | None,
    writer_report: dict | None,
    rendered_fingerprints: set[str],
    dedupe_memory: dict | None,
    final_html: str,
) -> dict[str, object]:
    """Human-readable final choice/loss table per block after HTML recovery."""
    path = state_dir / "final_selection_report.json"
    candidates = [c for c in (candidates_report or {}).get("candidates") or [] if isinstance(c, dict)]
    source_run_log_rows = _read_jsonl_rows(state_dir / "source_run_log.jsonl")
    category_health = aggregate_category_health(source_run_log_rows)
    collect_conservation = verify_collect_conservation(source_run_log_rows, len(candidates))
    html_visible = _visible_fingerprints_from_html(final_html, candidates)
    rendered_set = {str(fp) for fp in rendered_fingerprints if fp}
    visible_set = set(rendered_set) | html_visible
    # 8.7 no-loss: every captured item has exactly one disposition, computed
    # against the truly-visible set (rendered + HTML-recovered).
    disposition_conservation = verify_dispositions(candidates, visible_set)
    writer_drops = {
        str(item.get("fingerprint") or ""): item
        for item in ((writer_report or {}).get("dropped_candidates") or [])
        if isinstance(item, dict)
    }
    dedupe_drops = _dedupe_drop_map(dedupe_memory)
    sections: dict[str, dict[str, object]] = {}
    totals = Counter()
    for candidate in candidates:
        apply_story_intelligence(candidate)
        fp = str(candidate.get("fingerprint") or "").strip()
        block = str(candidate.get("primary_block") or "unknown")
        section_name = PRIMARY_BLOCKS.get(block, block)
        disposition, reason = _disposition_for_candidate(
            candidate,
            rendered_set=visible_set,
            writer_drops=writer_drops,
            dedupe_drops=dedupe_drops,
        )
        html_visible_only = bool(fp and fp in html_visible and fp not in rendered_set)
        score = _candidate_selection_score(candidate)
        final_status = "visible_after_repair" if html_visible_only else "visible" if fp in visible_set else disposition
        selection_status = (
            candidate.get("publish_plan_status")
            or candidate.get("digest_selection_verdict")
            or ("selected" if candidate.get("include") else "not_selected")
        )
        reserve_reason = ""
        if candidate.get("recoverable_reserve") or candidate.get("public_reserve"):
            reserve_reason = str(candidate.get("digest_selection_reason") or candidate.get("publish_plan_reason") or "same-block reserve")
        row = {
            "fingerprint": fp,
            "title": str(candidate.get("title") or "")[:180],
            "source_label": str(candidate.get("source_label") or ""),
            "score": score,
            "reader_value_score": candidate.get("reader_value_score"),
            "section_board_score": candidate.get("section_board_score"),
            "selection_status": selection_status,
            "final_status": final_status,
            "final_reason": _candidate_selection_reason(candidate, reason),
            "reserve_reason": reserve_reason[:240],
            "repeat_decision": candidate.get("change_type") or candidate.get("dedupe_decision") or "",
            "include": bool(candidate.get("include")),
            "recoverable_reserve": bool(candidate.get("recoverable_reserve") or candidate.get("public_reserve")),
            "html_visible_only_after_repair": html_visible_only,
        }
        section = sections.setdefault(
            section_name,
            {
                "primary_blocks": sorted({block}),
                "top": [],
                "visible": [],
                "lost_or_rejected": [],
                "counts": Counter(),
            },
        )
        blocks = set(section.get("primary_blocks") or [])
        blocks.add(block)
        section["primary_blocks"] = sorted(blocks)
        counts = section["counts"]
        assert isinstance(counts, Counter)
        counts[str(final_status)] += 1
        totals[str(final_status)] += 1
        top_items = section["top"]
        assert isinstance(top_items, list)
        top_items.append(row)
        if final_status in {"visible", "visible_after_repair"}:
            visible_items = section["visible"]
            assert isinstance(visible_items, list)
            visible_items.append(row)
        else:
            lost_items = section["lost_or_rejected"]
            assert isinstance(lost_items, list)
            lost_items.append(row)

    for section in sections.values():
        for key in ("top", "visible", "lost_or_rejected"):
            rows = section.get(key)
            if isinstance(rows, list):
                rows.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("title") or "")))
                section[key] = rows[:20] if key == "top" else rows[:30]
        counts = section.get("counts")
        if isinstance(counts, Counter):
            section["counts"] = dict(counts)
    payload = {
        "schema_version": 1,
        "run_date_london": current_day_london,
        "created_at_london": now_london().isoformat(),
        "policy": (
            "Final per-block table after editor, visible recovery and final HTML. "
            "Shows who ranked high, who became visible, who was replaced/recovered, "
            "and why a candidate was lost or rejected."
        ),
        "totals": dict(totals),
        "sections": dict(sorted(sections.items())),
        "category_health": category_health,
        "live_fallback_categories": categories_needing_live_fallback(category_health),
        "collect_conservation": collect_conservation,
        "disposition_conservation": disposition_conservation,
    }
    write_json(path, payload)
    return {
        "path": str(path.resolve()),
        "schema_version": payload["schema_version"],
        "totals": payload["totals"],
        "section_count": len(sections),
        "collect_conservation": collect_conservation,
        "disposition_conservation": disposition_conservation,
    }


def _classify_published_candidates(
    candidates_report: dict | None,
    rendered_fingerprints: set[str],
) -> dict[str, object]:
    """Surface visible items that look editorially wrong after all gates.

    Reject review catches false negatives. This catches false positives:
    stale news, stale food openings, and far-future food openings that
    reached the HTML anyway.
    """
    today = datetime.strptime(today_london(), "%Y-%m-%d").date()
    suspicious: list[dict[str, object]] = []
    for c in (candidates_report or {}).get("candidates") or []:
        if not isinstance(c, dict):
            continue
        fp = str(c.get("fingerprint") or "")
        if fp not in rendered_fingerprints:
            continue
        category = str(c.get("category") or "")
        block = str(c.get("primary_block") or "")
        title = str(c.get("title") or "")
        blob = " ".join(str(c.get(field) or "") for field in ("title", "summary", "lead", "evidence_text"))
        reasons: list[str] = []

        if category in {"media_layer", "gmp", "council", "public_services", "city_news", "tech_business", "football"} and block not in {"weather", "transport"}:
            pub_day = _parse_candidate_day(c.get("published_at"))
            if pub_day is not None and (today - pub_day).days > 7 and not _PUBLISHED_UPDATE_MARKERS.search(blob):
                reasons.append(f"news item is {(today - pub_day).days} days old without a clear new phase")

        if category == "food_openings" or block == "openings":
            days = _event_days(c)
            if days and max(days) < today - timedelta(days=3):
                reasons.append(f"food/opening date {max(days).isoformat()} is more than 3 days old")
            if days and min(days) > today + timedelta(days=30):
                reasons.append(f"food/opening date {min(days).isoformat()} is more than 30 days away")
            pub_day = _parse_candidate_day(c.get("published_at"))
            if not days and pub_day is not None and (today - pub_day).days > 7:
                reasons.append(f"undated food/opening article is {(today - pub_day).days} days old")

        if reasons:
            suspicious.append(
                {
                    "fingerprint": fp,
                    "title": title,
                    "source_label": c.get("source_label"),
                    "category": category,
                    "primary_block": block,
                    "reasons": reasons,
                }
            )

    return {
        "counts": {
            "suspiciously_published": len(suspicious),
            "warning_visible_items": len(suspicious),
        },
        "suspiciously_published": suspicious[:20],
    }


def _classify_rejected_candidates(
    writer_report: dict | None,
    curator_report: dict | None,
    candidates_report: dict | None,
) -> dict[str, object]:
    """R2: split rejected candidates into correctly_rejected / borderline /
    suspiciously_rejected. Suspicious gets the strongest visibility — those
    are likely false negatives we'd want to review by hand.

    R3.3: any high reader_value_score drop is at least borderline — and
    drops the model predicts as ``useful`` are surfaced as suspicious so a
    regex-only classifier can't silently throw out items the score model
    rates as worth shipping.
    """
    counts: dict[str, int] = {
        "correctly_rejected": 0,
        "borderline": 0,
        "suspiciously_rejected": 0,
    }
    suspicious: list[dict[str, object]] = []
    borderline: list[dict[str, object]] = []

    candidates = (candidates_report or {}).get("candidates") or []
    cand_by_fp: dict[str, dict] = {
        str(c.get("fingerprint") or ""): c for c in candidates if isinstance(c, dict)
    }

    def _score_for(fp: str) -> tuple[int | None, str | None]:
        cand = cand_by_fp.get(fp) or {}
        score = cand.get("reader_value_score")
        if not isinstance(score, (int, float)):
            return None, None
        return int(score), str(cand.get("reader_value_label") or "")

    # ── writer drops ──────────────────────────────────────────────────────
    for drop in (writer_report or {}).get("dropped_candidates") or []:
        if not isinstance(drop, dict):
            continue
        reasons = drop.get("reasons") or []
        is_lead = bool(drop.get("is_lead"))
        suspicious_hit = is_lead or any(
            _SUSPICIOUS_DROP_REASON_RE.search(str(r) or "") for r in reasons
        )
        fp = str(drop.get("fingerprint") or "")
        score, label = _score_for(fp)
        # A high editorial value score lifts a drop into review even if no
        # regex tripped: a "useful"-class candidate should not get silently
        # dropped at the writer stage.
        score_suspicious = score is not None and (score >= 75 or label == "useful")
        score_borderline = score is not None and score >= 60 and not score_suspicious
        record = {
            "stage": "writer",
            "title": drop.get("title"),
            "category": drop.get("category"),
            "primary_block": drop.get("primary_block"),
            "reasons": reasons,
            "is_lead": is_lead,
            "reader_value_score": score,
            "reader_value_label": label,
        }
        if suspicious_hit or score_suspicious:
            counts["suspiciously_rejected"] += 1
            if len(suspicious) < 15:
                suspicious.append(record)
        elif score_borderline:
            counts["borderline"] += 1
            if len(borderline) < 10:
                borderline.append(record)
        else:
            counts["correctly_rejected"] += 1

    # ── validator drops ───────────────────────────────────────────────────
    # On 2026-05-27 the reject_review showed 119+2+2 = 123 classified
    # rejects but the digest had 214 rejected items total. The 91 gap
    # was candidates dropped by validator stage (cross_day_rehash,
    # non_gm, property_listing, old_existing_food, expired_event, etc.)
    # which never made it into writer.dropped_candidates or
    # curator.decisions. C4 fix: classify them here too so every drop
    # is auditable.
    fp_seen: set[str] = set()
    for drop in (writer_report or {}).get("dropped_candidates") or []:
        if isinstance(drop, dict):
            fp_seen.add(str(drop.get("fingerprint") or ""))
    for dec in (curator_report or {}).get("decisions") or []:
        if isinstance(dec, dict) and not dec.get("include"):
            fp_seen.add(str(dec.get("fingerprint") or ""))

    for cand in candidates:
        if not isinstance(cand, dict):
            continue
        if cand.get("include"):
            continue
        fp = str(cand.get("fingerprint") or "")
        if not fp or fp in fp_seen:
            continue
        reject_reasons = [str(r) for r in (cand.get("reject_reasons") or []) if str(r).strip()]
        reason = str(cand.get("reason") or "")
        score, label = _score_for(fp)
        score_suspicious = score is not None and (score >= 75 or label == "useful")
        score_borderline = score is not None and score >= 60 and not score_suspicious
        record = {
            "stage": "validator",
            "title": cand.get("title"),
            "source_label": cand.get("source_label"),
            "primary_block": cand.get("primary_block"),
            "category": cand.get("category"),
            "reject_reasons": reject_reasons[:6],
            "reason": reason[:240],
            "reader_value_score": score,
            "reader_value_label": label,
        }
        if score_suspicious:
            record["why_flagged"] = f"Reader-value score={score} ({label}) — validator drop worth review."
            counts["suspiciously_rejected"] += 1
            if len(suspicious) < 15:
                suspicious.append(record)
        elif score_borderline:
            record["why_flagged"] = f"Reader-value score={score} ({label}) — validator drop worth review."
            counts["borderline"] += 1
            if len(borderline) < 10:
                borderline.append(record)
        else:
            counts["correctly_rejected"] += 1

    # ── curator drops ─────────────────────────────────────────────────────
    # Pull candidates with their evidence_text so we can re-check curator
    # drops for date hints (the "evergreen" justification is wrong if a
    # concrete date is actually in evidence).
    for dec in (curator_report or {}).get("decisions") or []:
        if not isinstance(dec, dict) or dec.get("include"):
            continue
        fp = str(dec.get("fingerprint") or "")
        cand = cand_by_fp.get(fp) or {}
        reason = str(dec.get("reason") or "")
        source_label = str(cand.get("source_label") or "")
        evidence = str(cand.get("evidence_text") or "")
        score, label = _score_for(fp)
        score_suspicious = score is not None and (score >= 75 or label == "useful")
        score_borderline = score is not None and score >= 60 and not score_suspicious
        suspicious_reason = False
        why = ""
        if "evergreen" in reason.lower() and _DATE_HINT_IN_EVIDENCE.search(evidence):
            suspicious_reason = True
            why = "Curator pометил evergreen, но в evidence есть конкретная дата."
        elif source_label in _PREMIUM_SOURCE_PRIORITY and "дубл" not in reason.lower():
            # Premium source drop that isn't a dedup → at least borderline.
            why = f"Premium-источник {source_label} отбит без явной дедупликации."
        if score_suspicious and not why:
            why = f"Reader-value score={score} ({label}) — curator drop worth review."
        elif score_borderline and not why:
            why = f"Reader-value score={score} ({label}) — curator drop worth review."
        # curator decisions don't carry the title — pull it from the
        # paired candidate so the admin report shows something useful.
        record = {
            "stage": "curator",
            "title": dec.get("title") or cand.get("title"),
            "source_label": source_label,
            "reason": reason,
            "why_flagged": why,
            "reader_value_score": score,
            "reader_value_label": label,
        }
        if suspicious_reason or score_suspicious:
            counts["suspiciously_rejected"] += 1
            if len(suspicious) < 15:
                suspicious.append(record)
        elif why or score_borderline:
            counts["borderline"] += 1
            if len(borderline) < 10:
                borderline.append(record)
        else:
            counts["correctly_rejected"] += 1

    return {
        "counts": counts,
        "suspiciously_rejected": suspicious,
        "borderline": borderline,
    }


def _semantic_dedup_counts_from_memory(state_dir: Path) -> dict[str, int]:
    """I1: pull intra/cross-day/borderline counts from dedupe_memory.json
    so the after_run_summary dashboard surfaces them in one place."""
    path = state_dir / "dedupe_memory.json"
    if not path.exists():
        return {"intra_drops": 0, "cross_day_drops": 0, "borderline": 0, "enabled": False}
    try:
        report = read_json(path)
    except Exception:  # noqa: BLE001
        return {"intra_drops": 0, "cross_day_drops": 0, "borderline": 0, "enabled": False}
    summary = report.get("semantic_dedup_summary") or {}
    guard = report.get("semantic_guard") or {}
    return {
        "intra_drops": int(summary.get("intra_drop_count") or 0),
        "cross_day_drops": int(summary.get("cross_day_drop_count") or 0),
        "borderline": int(summary.get("borderline_count") or 0),
        "enabled": bool(summary.get("enabled")),
        "restored": int(guard.get("restored") or 0),
        "restored_candidates": guard.get("restored_candidates") or [],
    }


def _build_after_run_summary(
    digest_health: dict,
    source_status: dict,
    reject_review: dict,
    writer_report: dict | None,
    lost_leads: list,
    section_underflow: list,
    synthetic_freshness: dict | None = None,
    semantic_dedup: dict | None = None,
    city_intelligence: dict | None = None,
    trend_detection: dict | None = None,
    event_miss_review: dict | None = None,
) -> dict[str, object]:
    """R3: compact post-run dashboard. One block, query-once."""
    rendered = int(((writer_report or {}).get("quality_counts") or {}).get("rendered_candidates") or 0)
    sd = semantic_dedup or {}
    ci = city_intelligence or {}
    topic_clusters = ci.get("topic_clusters") or {}
    borough_coverage = ci.get("borough_coverage") or {}
    borough_counts = borough_coverage.get("counts") or {}
    dominant = borough_coverage.get("dominant_borough") or {}
    skew_flags = borough_coverage.get("skew_flags") or []
    td = trend_detection or {}
    em = event_miss_review or {}
    em_counts = em.get("counts") or {}
    rising_topics = td.get("rising_topics") or []
    rising_entities = td.get("rising_entities") or []
    return {
        "useful_items": rendered,
        "digest_risk_level": digest_health.get("risk_level"),
        "digest_risk_score": digest_health.get("risk_score"),
        "broken_sources": source_status["counts"].get("failed", 0) + source_status["counts"].get("empty", 0),
        "stale_sources": source_status["counts"].get("stale", 0),
        "zero_yield_sources": source_status["counts"].get("zero_yield", 0),
        "stale_synthetic_items": int((synthetic_freshness or {}).get("stale_count") or 0),
        "suspiciously_rejected": reject_review["counts"].get("suspiciously_rejected", 0),
        "borderline_rejected": reject_review["counts"].get("borderline", 0),
        "semantic_dedup_enabled": bool(sd.get("enabled")),
        "semantic_intra_drops": int(sd.get("intra_drops") or 0),
        "semantic_cross_day_drops": int(sd.get("cross_day_drops") or 0),
        "semantic_borderline": int(sd.get("borderline") or 0),
        "semantic_restored_by_guard": int(sd.get("restored") or 0),
        "topic_cluster_count": int(topic_clusters.get("cluster_count") or 0),
        "clustered_candidates": int(topic_clusters.get("clustered_candidate_count") or 0),
        "included_borough_count": int(borough_counts.get("covered_boroughs_included") or 0),
        "rendered_borough_count": int(borough_counts.get("covered_boroughs_rendered") or 0),
        "dominant_borough": dominant.get("borough") if isinstance(dominant, dict) else None,
        "borough_skew_flags": len(skew_flags) if isinstance(skew_flags, list) else 0,
        "rising_topics_7d": len(rising_topics) if isinstance(rising_topics, list) else 0,
        "rising_entities_7d": len(rising_entities) if isinstance(rising_entities, list) else 0,
        "critical_event_misses": int(em_counts.get("critical_misses") or 0),
        "high_value_events_not_published": int(em_counts.get("high_value_not_published") or 0),
        "lost_leads": len(lost_leads or []),
        "section_underflow": len(section_underflow or []),
    }


# S5 — first sentence anti-patterns for news cards. We don't enforce a
# perfect lead, just block the obvious cases that broke 22 May digest:
# quote-lead and narrative-житель/жительница-lead. Warning-only.
_QUOTE_LEAD_RE = re.compile(r'^\s*•?\s*[«"„]', re.UNICODE)
_NARRATIVE_LEAD_RE = re.compile(
    r"^\s*•?\s*"
    r"(?:<[^>]+>\s*)?"          # optional <b>...</b> wrapper for borough
    r"(?:[А-ЯЁA-Z][а-яёa-z]+:\s*)?"  # optional "Bolton:" prefix
    r"(?:местн(?:ая|ый)\s+жит(?:ель|ельница)|"
    r"(?:многие|местные)\s+жители|"
    r"жительница\s+[А-ЯЁA-Z]|"
    r"(?:одна\s+из|туристическ)|"
    r"a\s+local\s+(?:resident|woman|man)|local\s+resident)",
    re.IGNORECASE | re.UNICODE,
)
# Cards in these sections get the lead-quality check applied.
_NEWS_LEAD_BLOCKS = frozenset({
    "last_24h", "today_focus", "city_watch", "transport",
})


def _summarise_news_lead_quality(
    candidates_report: dict | None,
    rendered_fingerprints: set[str],
) -> dict[str, object]:
    """S5: warn when a published news card opens with a quote, a
    "местная жительница" narrative, or a generic non-fact phrase.

    Warning-only — the digest still ships per the never-block rule.
    The point is to surface in the Telegram admin report which cards
    lost their lead-first structure, so the reader doesn't have to
    write the complaint.
    """
    counts = {"checked": 0, "quote_lead": 0, "narrative_lead": 0}
    issues: list[dict[str, object]] = []
    if not isinstance(candidates_report, dict):
        return {"counts": counts, "issues": issues}
    for candidate in candidates_report.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if fp not in rendered_fingerprints:
            continue
        block = str(candidate.get("primary_block") or "")
        if block not in _NEWS_LEAD_BLOCKS:
            continue
        category = str(candidate.get("category") or "")
        if category not in {"media_layer", "gmp", "council", "public_services",
                            "city_news", "tech_business", "transport"}:
            continue
        counts["checked"] += 1
        draft_line = str(candidate.get("draft_line") or "")
        # Strip HTML tags but keep the first 200 chars so we look at the
        # opening clause, not the whole card.
        visible = re.sub(r"<[^>]+>", " ", draft_line)
        opener = visible[:200]
        if _QUOTE_LEAD_RE.search(opener):
            counts["quote_lead"] += 1
            issues.append({
                "fingerprint": fp,
                "title": str(candidate.get("title") or "")[:140],
                "primary_block": block,
                "issue": "quote_lead",
                "detail": "карточка начинается с прямой цитаты, не с факта",
            })
            continue
        if _NARRATIVE_LEAD_RE.search(opener):
            counts["narrative_lead"] += 1
            issues.append({
                "fingerprint": fp,
                "title": str(candidate.get("title") or "")[:140],
                "primary_block": block,
                "issue": "narrative_lead",
                "detail": "карточка начинается с «местного жителя/жительницы», не с факта",
            })
    return {"counts": counts, "issues": issues[:20]}


def _summarise_event_completeness(
    candidates_report: dict | None,
    rendered_fingerprints: set[str],
    sections: dict[str, list[str]] | None,
) -> dict[str, object]:
    """S3: surface published event cards that lost their date or venue
    on the rewrite path.

    For each rendered candidate whose primary_block is an event block:
      - if the draft_line has no date marker AT ALL (even though the
        extracted event has a date_iso or date_text), flag as
        "lost_date" — the rewriter dropped the time anchor.
      - if the candidate has an extracted event.venue but the venue
        name does not appear anywhere in the draft_line, flag as
        "lost_venue".

    Warning-only — the digest still ships per the never-block rule.
    Pure surfacing so the support report shows "events shipped without
    when/where".
    """
    counts = {"checked": 0, "missing_date": 0, "missing_venue": 0}
    issues: list[dict[str, object]] = []
    if not isinstance(candidates_report, dict):
        return {"counts": counts, "issues": issues}
    event_blocks = _EVENT_SECTIONS_FOR_DATE_CHECK
    # Map block-id -> section name; only blocks that end up in event sections.
    event_block_ids = {
        block_id for block_id, sec_name in PRIMARY_BLOCKS.items()
        if sec_name in event_blocks
    }
    for candidate in candidates_report.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if fp not in rendered_fingerprints:
            continue
        block = str(candidate.get("primary_block") or "")
        if block not in event_block_ids:
            continue
        counts["checked"] += 1
        draft_line = str(candidate.get("draft_line") or "")
        visible = re.sub(r"<[^>]+>", " ", draft_line)
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        has_extracted_date = bool(
            event.get("date_iso") or event.get("date_text") or event.get("is_recurring")
        )
        has_date_in_text = bool(_DATE_MARKER_RE.search(visible))
        if has_extracted_date and not has_date_in_text:
            counts["missing_date"] += 1
            issues.append({
                "fingerprint": fp,
                "title": str(candidate.get("title") or "")[:140],
                "primary_block": block,
                "issue": "missing_date",
                "detail": "event has extracted date but draft_line has no time anchor",
            })
        venue = str(event.get("venue") or "").strip()
        if venue and len(venue) >= 4:
            # Simple containment check — case-insensitive.
            if venue.lower() not in visible.lower():
                counts["missing_venue"] += 1
                issues.append({
                    "fingerprint": fp,
                    "title": str(candidate.get("title") or "")[:140],
                    "primary_block": block,
                    "issue": "missing_venue",
                    "detail": f"event.venue=«{venue}» not in draft_line",
                })
    return {"counts": counts, "issues": issues[:30]}


def _summarise_cross_day_recurrence(candidates_report: dict | None) -> dict[str, object]:
    """S2: surface candidates blocked because the same person/incident
    was published in the previous days.

    Each entry includes both names (today's surface form + the form
    matched in published_facts), the previous title and date so the
    Telegram admin can recognise the block — e.g. "Эрика де Соуза
    Корреа уже была 21 мая как «Семья 17-летней Эрики де Соуза Корреа
    выразила скорбь»".
    """
    blocked: list[dict[str, object]] = []
    if not isinstance(candidates_report, dict):
        return {"counts": {"blocked": 0}, "blocked": blocked}
    for candidate in candidates_report.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        if not candidate.get("cross_day_entity_repeat"):
            continue
        match = candidate.get("people_dedupe_match") if isinstance(candidate.get("people_dedupe_match"), dict) else {}
        blocked.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": str(candidate.get("title") or "")[:160],
                "source_label": candidate.get("source_label"),
                "matched_person_today": match.get("matched_person_today"),
                "matched_person_previously": match.get("matched_person_previously"),
                "previous_fingerprint": match.get("previous_fingerprint"),
                "previous_title": (candidate.get("previous_title") or match.get("previous_title") or "")[:160],
                "previous_published_day": candidate.get("previous_published_day"),
                "change_type": candidate.get("change_type"),
            }
        )
    return {
        "counts": {"blocked": len(blocked)},
        "blocked": blocked[:20],
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
        if stage_file.name == "cost_history.json":
            continue
        try:
            payload = read_json(stage_file)
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(payload, dict):
            continue
        for r in payload.get("records") or []:
            if not isinstance(r, dict):
                continue
            records.append(
                CallRecord(
                    stage=str(r.get("stage") or ""),
                    provider=str(r.get("provider") or ""),
                    model=str(r.get("model") or ""),
                    prompt_name=str(r.get("prompt_name") or ""),
                    prompt_version=str(r.get("prompt_version") or ""),
                    prompt_tokens=int(r.get("prompt_tokens") or 0),
                    completion_tokens=int(r.get("completion_tokens") or 0),
                    estimated_prompt_tokens=int(r.get("estimated_prompt_tokens") or r.get("prompt_tokens") or 0),
                    estimated_completion_tokens=int(r.get("estimated_completion_tokens") or r.get("completion_tokens") or 0),
                    cost_usd=float(r.get("cost_usd") or 0.0),
                    estimated_cost_usd=float(r.get("estimated_cost_usd") or r.get("cost_usd") or 0.0),
                    usage_source=str(r.get("usage_source") or "actual"),
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


def _cost_latency_budget_report(
    *,
    cost_summary: dict,
    scan_report: dict | None,
    writer_report: dict | None,
    editor_report: dict | None,
) -> dict[str, object]:
    total_calls = int(cost_summary.get("total_calls") or 0)
    prompt_tokens = int(cost_summary.get("total_prompt_tokens") or cost_summary.get("total_estimated_prompt_tokens") or 0)
    completion_tokens = int(cost_summary.get("total_completion_tokens") or cost_summary.get("total_estimated_completion_tokens") or 0)
    estimated_tokens = int(
        cost_summary.get("total_estimated_tokens")
        or (
            int(cost_summary.get("total_estimated_prompt_tokens") or 0)
            + int(cost_summary.get("total_estimated_completion_tokens") or 0)
        )
        or (prompt_tokens + completion_tokens)
        or 0
    )
    total_cost = float(cost_summary.get("total_cost_usd") or 0.0)
    timings: dict[str, float] = {}
    for name, report in (
        ("collect", scan_report),
        ("write", writer_report),
        ("edit", editor_report),
    ):
        if not isinstance(report, dict):
            continue
        seconds = report.get("duration_seconds") or report.get("elapsed_seconds")
        try:
            if seconds is not None:
                timings[name] = float(seconds)
        except (TypeError, ValueError):
            continue
    wall_time = sum(timings.values()) if timings else None
    budgets = COST_LATENCY_BUDGETS
    breaches: list[str] = []
    if total_calls > int(budgets["max_total_calls"]):
        breaches.append("max_total_calls")
    if estimated_tokens > int(budgets["max_total_estimated_tokens"]):
        breaches.append("max_total_estimated_tokens")
    if total_cost > float(budgets["max_total_cost_usd"]):
        breaches.append("max_total_cost_usd")
    if wall_time is not None and wall_time > float(budgets["hard_wall_time_warning_seconds"]):
        breaches.append("hard_wall_time_warning_seconds")
    return {
        "schema_version": 1,
        "warning_only": True,
        "budgets": budgets,
        "actual": {
            "total_calls": total_calls,
            "estimated_tokens": estimated_tokens,
            "total_cost_usd": total_cost,
            "known_stage_wall_time_seconds": wall_time,
            "stage_timings": timings,
        },
        "breaches": breaches,
        "status": "warning" if breaches else "ok",
    }


def _stage_seconds(report: dict | None) -> float:
    if not isinstance(report, dict):
        return 0.0
    for key in ("duration_seconds", "elapsed_seconds", "rewrite_seconds", "total_duration_seconds"):
        try:
            value = float(report.get(key) or 0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return round(value, 3)
    timings = report.get("timings") if isinstance(report.get("timings"), dict) else {}
    try:
        return round(float(timings.get("total_seconds") or 0), 3)
    except (TypeError, ValueError):
        return 0.0


def _build_speed_report(
    *,
    state_dir: Path,
    scan_report: dict | None,
    curator_report: dict | None,
    llm_rewrite_report: dict | None,
    writer_report: dict | None,
    editor_report: dict | None,
) -> dict[str, object]:
    dedupe_report = _load_optional_json(state_dir / "dedupe_memory.json")
    validator_report = _load_optional_json(state_dir / "candidate_validation_report.json")
    stages = {
        "collect": _stage_seconds(scan_report),
        "dedupe": _stage_seconds(dedupe_report),
        "validate": _stage_seconds(validator_report),
        "curator": _stage_seconds(curator_report),
        "llm_rewrite": _stage_seconds(llm_rewrite_report),
        "write": _stage_seconds(writer_report),
        "edit": _stage_seconds(editor_report),
    }
    bottlenecks = [
        {"stage": name, "duration_seconds": seconds}
        for name, seconds in sorted(stages.items(), key=lambda item: item[1], reverse=True)
        if seconds > 0
    ]
    llm_summary = (
        llm_rewrite_report.get("diagnostics_summary")
        if isinstance(llm_rewrite_report, dict) and isinstance(llm_rewrite_report.get("diagnostics_summary"), dict)
        else {}
    )
    llm_actual = {
        "batch_count": int(llm_summary.get("batch_count") or 0),
        "sent": int(llm_summary.get("sent") or 0),
        "accepted": int(llm_summary.get("accepted") or 0),
        "truncated_responses": int(llm_summary.get("truncated_responses") or 0),
        "timeout_errors": int(llm_summary.get("timeout_errors") or 0),
        "queue_wait_seconds": llm_summary.get("queue_wait_seconds") or {},
        "api_seconds": llm_summary.get("api_seconds") or {},
        "completion_tokens_per_item": llm_summary.get("completion_tokens_per_item") or {},
        "max_tokens": llm_summary.get("max_tokens") or {},
        "by_prompt": llm_summary.get("by_prompt") or {},
        "by_provider": llm_summary.get("by_provider") or {},
    }
    total_known = round(sum(stages.values()), 3)
    status = "attention" if llm_actual["timeout_errors"] or llm_actual["truncated_responses"] else "ok"
    return {
        "schema_version": 1,
        "run_at_london": now_london().isoformat(),
        "run_date_london": today_london(),
        "status": status,
        "total_known_stage_seconds": total_known,
        "stage_seconds": stages,
        "bottlenecks": bottlenecks[:8],
        "dedupe_timings": (dedupe_report or {}).get("timings") if isinstance(dedupe_report, dict) else {},
        "validate_timings": (validator_report or {}).get("timings") if isinstance(validator_report, dict) else {},
        "llm": llm_actual,
        "token_budget_history": (
            llm_rewrite_report.get("token_budget_history")
            if isinstance(llm_rewrite_report, dict)
            else {}
        ),
        "batching_strategy": (
            llm_rewrite_report.get("batching_strategy")
            if isinstance(llm_rewrite_report, dict)
            else {}
        ),
        "policy": "Observation-only speed report: no release blocking, no quality cuts. Optimise by parser/cache/token p95 evidence before changing editorial floors.",
    }


# ── Cancel-proof observability (backlog item 1) ───────────────────────────
#
# The pipeline runs as a chain of SEPARATE processes (collect → dedupe →
# validate → … → write → edit → build). speed_report.json and
# final_selection_report.json used to be written only by the build/release
# gate, so a run killed BEFORE build left nothing on disk and forensics had to
# be reconstructed from per-file mtimes. These helpers flush incrementally
# after every stage, so at any kill point on disk we have: per-stage timings,
# a per-source run log, and a provisional selection snapshot. final_selection_
# report.json stays the authoritative post-render table. All writes are guarded
# — observability must never abort the issue.

_STAGE_REPORT_FILES: dict[str, str] = {
    "collect": "collector_report.json",
    "dedupe": "dedupe_memory.json",
    "validate": "candidate_validation_report.json",
    "curator": "curator_report.json",
    "transport_fill": "transport_fill_report.json",
    "llm_rewrite": "llm_rewrite_report.json",
    "write": "writer_report.json",
    "edit": "editor_report.json",
}

# Stages after which the selection set is meaningful enough to snapshot.
_SELECTION_SNAPSHOT_STAGES = {
    "collect",
    "dedupe",
    "validate",
    "curator",
    "transport_fill",
    "llm_rewrite",
    "write",
    "edit",
}


def _candidate_has_public_text(candidate: dict) -> bool:
    """A candidate is renderable only if it already carries public text."""
    for key in ("draft_line", "render_line", "public_line"):
        if str(candidate.get(key) or "").strip():
            return True
    return False


def _refresh_speed_report_snapshot(state_dir: Path) -> None:
    """Rebuild speed_report.json from whatever stage reports exist on disk.
    Provisional until build writes the final one; always reflects completed
    stages so a kill mid-run still leaves accurate timings."""
    speed_report = _build_speed_report(
        state_dir=state_dir,
        scan_report=_load_optional_json(state_dir / "collector_report.json"),
        curator_report=_load_optional_json(state_dir / "curator_report.json"),
        llm_rewrite_report=_load_optional_json(state_dir / "llm_rewrite_report.json"),
        writer_report=_load_optional_json(state_dir / "writer_report.json"),
        editor_report=_load_optional_json(state_dir / "editor_report.json"),
    )
    speed_report["provisional"] = True
    write_json(state_dir / "speed_report.json", speed_report)


def _append_stage_timing(state_dir: Path, stage: str) -> None:
    """Append one append-only line per finished stage to stage_timings.jsonl."""
    report_name = _STAGE_REPORT_FILES.get(stage)
    report = _load_optional_json(state_dir / report_name) if report_name else None
    record = {
        "stage": stage,
        "duration_seconds": _stage_seconds(report),
        "stage_status": str((report or {}).get("stage_status") or ("complete" if report else "unknown")),
        "finished_at_london": now_london().isoformat(),
        "run_date_london": today_london(),
    }
    with (state_dir / "stage_timings.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_source_run_log(state_dir: Path) -> int:
    """Flatten collector_report categories→source_health into source_run_log.jsonl
    (one line per source: found/enriched/dropped/failed/duration). Cancel-proof
    'which source did what' without parsing the big collector report."""
    collector_report = _load_optional_json(state_dir / "collector_report.json")
    categories = collector_report.get("categories") if isinstance(collector_report, dict) else None
    if not isinstance(categories, dict):
        return 0
    run_date = str(collector_report.get("run_date_london") or today_london())
    rows: list[dict[str, object]] = []
    for category, payload in categories.items():
        sources = (payload or {}).get("source_health") if isinstance(payload, dict) else None
        if not isinstance(sources, list):
            continue
        for source in sources:
            if not isinstance(source, dict):
                continue
            found = int(source.get("candidate_count") or 0)
            enriched = int(source.get("publishable_count") or 0)
            rows.append({
                "run_date_london": run_date,
                "category": str(category),
                "name": str(source.get("name") or ""),
                "url": str(source.get("url") or ""),
                "checked": bool(source.get("checked")),
                "fetched": bool(source.get("fetched")),
                "not_modified": bool(source.get("not_modified")),
                "found": found,
                "enriched": enriched,
                "dated": int(source.get("dated_candidate_count") or 0),
                "fresh_24h": int(source.get("fresh_last_24h_count") or 0),
                "dropped_at_collect": max(found - enriched, 0),
                "failure_class": str(source.get("failure_class") or ""),
                "errors": len(source.get("errors") or []),
                "warnings": len(source.get("warnings") or []),
                "duration_seconds": float(source.get("duration_seconds") or 0.0),
                "fetch_seconds": float(source.get("fetch_duration_seconds") or 0.0),
                "extract_seconds": float(source.get("extract_duration_seconds") or 0.0),
            })
    with (state_dir / "source_run_log.jsonl").open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(rows)


def _write_selection_snapshot(state_dir: Path, stage: str) -> dict[str, object]:
    """Provisional pre-build selection table from candidates.json. Sibling of
    _write_final_selection_report but without rendered HTML — answers 'who is
    selected and why' at the moment <stage> finished, even if the run is killed
    before the writer/build produces the final report."""
    candidates_payload = _load_optional_json(state_dir / "candidates.json") or {}
    candidates = [c for c in (candidates_payload.get("candidates") or []) if isinstance(c, dict)]
    _snapshot_rows = _read_jsonl_rows(state_dir / "source_run_log.jsonl")
    _snapshot_category_health = aggregate_category_health(_snapshot_rows)
    sections: dict[str, dict[str, object]] = {}
    totals: Counter = Counter()
    for candidate in candidates:
        block = str(candidate.get("primary_block") or "unknown")
        section_name = PRIMARY_BLOCKS.get(block, block)
        status = str(
            candidate.get("publish_plan_status")
            or candidate.get("digest_selection_verdict")
            or ("selected" if candidate.get("include") else "not_selected")
        )
        totals[status] += 1
        section = sections.setdefault(section_name, {"counts": Counter(), "items": []})
        counts = section["counts"]
        assert isinstance(counts, Counter)
        counts[status] += 1
        items = section["items"]
        assert isinstance(items, list)
        if status not in {"not_selected", "drop"} or len(items) < 30:
            items.append({
                "fingerprint": str(candidate.get("fingerprint") or ""),
                "title": str(candidate.get("title") or "")[:180],
                "source_label": str(candidate.get("source_label") or ""),
                "status": status,
                "include": bool(candidate.get("include")),
                "has_public_text": _candidate_has_public_text(candidate),
                "recoverable_reserve": bool(candidate.get("recoverable_reserve") or candidate.get("public_reserve")),
                "reason": str(candidate.get("digest_selection_reason") or candidate.get("publish_plan_reason") or "")[:240],
            })
    for section in sections.values():
        counts = section.get("counts")
        if isinstance(counts, Counter):
            section["counts"] = dict(counts)
    payload = {
        "schema_version": 1,
        "provisional": True,
        "after_stage": stage,
        "run_date_london": today_london(),
        "created_at_london": now_london().isoformat(),
        "policy": (
            "Provisional pre-build selection snapshot. The authoritative per-block "
            "choice/loss table is final_selection_report.json, written after HTML render."
        ),
        "totals": dict(totals),
        "sections": dict(sorted(sections.items())),
        "category_health": _snapshot_category_health,
        "live_fallback_categories": categories_needing_live_fallback(_snapshot_category_health),
        "collect_conservation": verify_collect_conservation(_snapshot_rows, len(candidates)),
    }
    write_json(state_dir / "selection_snapshot.json", payload)
    return {"candidate_count": len(candidates), "totals": dict(totals)}


def _repeat_match_types(candidate: dict) -> list[str]:
    types: set[str] = set()
    if candidate.get("history_matches"):
        for match in candidate.get("history_matches") or []:
            if isinstance(match, dict):
                mt = str(match.get("match_type") or "fingerprint").strip() or "fingerprint"
                types.add(mt)
    if candidate.get("semantic_dedupe_match") or candidate.get("semantic_dedupe_score") is not None:
        types.add("semantic_embedding")
    if candidate.get("topic_lifecycle_match") or candidate.get("topic_lifecycle_repeat"):
        types.add("topic_lifecycle")
    if candidate.get("people_dedupe_match"):
        types.add("people_entity")
    return sorted(types)


def _write_repeat_policy_report(
    state_dir: Path,
    *,
    current_day_london: str,
    candidates_report: dict | None,
    rendered_fingerprints: set[str],
    published_facts: dict | None,
    visible_repeat_review: dict[str, object],
    visible_repeat_quarantine: dict[str, object],
) -> dict[str, object]:
    candidates = [c for c in (candidates_report or {}).get("candidates") or [] if isinstance(c, dict)]
    previous_by_fp = _published_by_fingerprint(published_facts)
    rows: list[dict[str, object]] = []
    counts: Counter = Counter()
    by_block: dict[str, Counter] = defaultdict(Counter)
    by_match_type: Counter = Counter()
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "")
        block = str(candidate.get("primary_block") or "unknown")
        status = "rendered" if fp in rendered_fingerprints else ("included" if candidate.get("include") else "not_included")
        previous = previous_by_fp.get(fp)
        verdict = visible_repeat_verdict(candidate, previous).as_dict() if previous else {
            "allow": True,
            "repeat_class": "new",
            "reason": "no_previous_match",
        }
        match_types = _repeat_match_types(candidate)
        if previous and "fingerprint" not in match_types:
            match_types.append("fingerprint")
        for mt in match_types:
            by_match_type[mt] += 1
        counts["candidates"] += 1
        counts[status] += 1
        if previous:
            counts["exact_previous"] += 1
        if any("semantic" in mt or "embedding" in mt for mt in match_types):
            counts["vector_or_semantic_matches"] += 1
        if status == "rendered" and previous:
            key = "visible_repeat_allowed" if verdict.get("allow") else "visible_repeat_rejected"
            counts[key] += 1
            by_block[block][key] += 1
        if previous or match_types or status == "rendered":
            rows.append(
                {
                    "fingerprint": fp,
                    "title": str(candidate.get("title") or "")[:180],
                    "source_label": candidate.get("source_label"),
                    "primary_block": block,
                    "category": candidate.get("category"),
                    "status": status,
                    "include": bool(candidate.get("include")),
                    "change_type": candidate.get("change_type"),
                    "dedupe_decision": candidate.get("dedupe_decision"),
                    "match_types": sorted(set(match_types)),
                    "repeat_policy": verdict,
                    "previous_title": str((previous or {}).get("title") or "")[:180],
                    "published_count": (previous or {}).get("published_count"),
                }
            )
    payload = {
        "schema_version": 1,
        "run_date_london": current_day_london,
        "created_at_london": now_london().isoformat(),
        "policy": (
            "Repeat-policy audit after writer. Exact fingerprint repeats are checked "
            "against typed repeat exceptions; vector/semantic match counts are surfaced "
            "for review, not used as a final HTML removal key."
        ),
        "counts": dict(counts),
        "by_block": {block: dict(counter) for block, counter in sorted(by_block.items())},
        "by_match_type": dict(by_match_type),
        "visible_repeat_review": visible_repeat_review,
        "visible_repeat_quarantine": visible_repeat_quarantine,
        "items": rows[:400],
    }
    path = state_dir / "repeat_policy_report.json"
    write_json(path, payload)
    return {
        "path": str(path.resolve()),
        "schema_version": payload["schema_version"],
        "counts": payload["counts"],
        "by_match_type": payload["by_match_type"],
    }


def flush_stage_observability(project_root: Path, stage: str) -> dict[str, object]:
    """Cancel-proof incremental flush, called by the orchestrator after each
    pipeline stage. Refreshes per-stage timings + provisional speed report, the
    per-source run log (after collect) and the provisional selection snapshot
    (after selection-affecting stages). Never raises — observability must not
    block the issue."""
    state_dir = project_root / "data" / "state"
    result: dict[str, object] = {"stage": stage}
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("observability: state dir unavailable (%s)", exc)
        return result
    for label, fn in (
        ("stage_timings", lambda: _append_stage_timing(state_dir, stage)),
        ("speed_report", lambda: _refresh_speed_report_snapshot(state_dir)),
    ):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("observability: %s flush failed after %s (%s)", label, stage, exc)
    if stage == "collect":
        try:
            result["source_run_log_rows"] = _write_source_run_log(state_dir)
        except Exception as exc:  # noqa: BLE001
            logger.warning("observability: source_run_log flush failed (%s)", exc)
    if stage in _SELECTION_SNAPSHOT_STAGES:
        try:
            result["selection_snapshot"] = _write_selection_snapshot(state_dir, stage)
        except Exception as exc:  # noqa: BLE001
            logger.warning("observability: selection_snapshot flush failed after %s (%s)", stage, exc)
    return result


def _model_bakeoff_readiness(project_root: Path) -> dict[str, object]:
    labels_path = project_root / "data" / "validation" / "reader_value_labels.json"
    label_count = 0
    label_errors: list[str] = []
    if labels_path.exists():
        try:
            labels_payload = read_json(labels_path, {"labels": []})
            labels = labels_payload.get("labels") if isinstance(labels_payload, dict) else []
            label_count = len(labels) if isinstance(labels, list) else 0
            label_errors = validate_reader_value_labels(labels_payload)
        except Exception as exc:  # noqa: BLE001
            label_errors = [f"reader_value_labels unreadable: {exc}"]
    else:
        label_errors = ["reader_value_labels.json missing"]
    ready = label_count >= 30 and not label_errors
    return {
        **MODEL_BAKEOFF_SPEC,
        "validation_set": {
            "path": str(labels_path),
            "label_count": label_count,
            "ready": ready,
            "errors": label_errors,
        },
        "status": "ready" if ready else "needs_labels",
    }


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


def _write_outgoing_metadata(
    metadata_path: Path,
    *,
    report_payload: dict,
    prompt_versions: list[dict[str, str]],
) -> None:
    """Write sidecar metadata for the published digest."""
    payload = {
        "schema_version": 1,
        "release_gate_version": report_payload.get("release_gate_version"),
        "pipeline_run_id": report_payload.get("pipeline_run_id"),
        "run_at_london": report_payload.get("run_at_london"),
        "run_date_london": report_payload.get("run_date_london"),
        "release_decision": report_payload.get("release_decision"),
        "output_path": report_payload.get("output_path"),
        "prompt_versions": prompt_versions,
        "model_routing_policy": report_payload.get("model_routing_policy") or {},
        "prompt_drift": report_payload.get("prompt_drift") or [],
        "cost_summary": report_payload.get("cost_summary") or {},
    }
    write_json(metadata_path, payload)


def build_release(project_root: Path) -> ReleaseResult:
    stage_started = time.monotonic()
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
            if section_name == "Выходные в GM":
                try:
                    weekday = datetime.strptime(current_day_london, "%Y-%m-%d").weekday()
                except ValueError:
                    weekday = now_london().weekday()
                # Weekend planning is intentionally active only Thu-Sun.
                # A Monday-Wednesday empty block is normal and should not
                # produce a false editorial alarm.
                if weekday not in {3, 4, 5, 6}:
                    continue
            actual = int(sec_counts.get(section_name) or 0)
            dropped_here = dropped_per_section.get(section_name, 0)
            if section_name == "Что важно сегодня" and actual < minimum:
                board = writer_report.get("today_focus_board") if isinstance(writer_report.get("today_focus_board"), dict) else {}
                if board:
                    section_underflow.append(
                        {
                            "section": section_name,
                            "actual": actual,
                            "minimum": minimum,
                            "dropped_by_writer": dropped_here,
                            "eligible_candidates": board.get("eligible_candidates"),
                            "underflow_reason": board.get("underflow_reason") or "",
                        }
                    )
                    warnings.append(
                        f"Section underflow: «{section_name}» shipped {actual} items "
                        f"(min={minimum}); board saw {board.get('eligible_candidates', 0)} "
                        f"eligible practical candidate(s), reason={board.get('underflow_reason') or 'unknown'}."
                    )
                    continue
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
    cross_day_recurrence = _summarise_cross_day_recurrence(candidates_report)
    event_completeness = _summarise_event_completeness(
        candidates_report, rendered_fingerprints, None,
    )
    news_lead_quality = _summarise_news_lead_quality(
        candidates_report, rendered_fingerprints,
    )

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
    cost_latency_budget = _cost_latency_budget_report(
        cost_summary=cost_summary,
        scan_report=scan_report,
        writer_report=writer_report,
        editor_report=editor_report,
    )
    if cost_latency_budget["status"] == "warning":
        warnings.append(
            "Cost/latency budget: warning threshold breached — "
            f"{', '.join(cost_latency_budget.get('breaches') or [])}; see release_report.cost_latency_budget."
        )
    model_bakeoff = _model_bakeoff_readiness(project_root)
    if model_bakeoff["status"] != "ready":
        warnings.append(
            "Model bake-off: validation set is not ready for a safe model switch — "
            "see release_report.model_bakeoff."
        )

    _validate_draft(
        draft_path=draft_path,
        scan_report=scan_report,
        included_candidates=candidate_context["included_candidates"],
        rendered_fingerprints=rendered_fingerprints,
        writer_report=writer_report,
        current_day_london=current_day_london,
        errors=errors,
        warnings=warnings,
    )

    # Q9/S1/A4: Bad Digest Detector. Editorial failures are warning-only:
    # the digest should ship with an explicit review report unless a
    # technical release invariant (HTML/date/required stage contract) is
    # broken. This preserves the operator rule: do not silently block a
    # morning issue when we can still send and explain the risk.
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

    # R1 + O1: per-source status table with curated/rendered yield columns.
    source_status = _summarise_source_health(
        scan_report,
        candidates_report=candidates_report,
        rendered_fingerprints=rendered_fingerprints,
        writer_report=writer_report,
    )
    # Phase 1: emit the per-source funnel (raw → curated → rendered) as its own
    # report so "fetched OK / 0 rendered" is a visible red signal instead of
    # being buried in the release report. Reporting must never block release.
    try:
        write_json(
            state_dir / "source_health_funnel.json",
            {
                "run_date_london": today_london(),
                "sources": [
                    {
                        "name": row.get("name"),
                        "category": row.get("category"),
                        "status": row.get("status"),
                        "raw": (row.get("loss_funnel") or {}).get("source_raw_count"),
                        "rendered": (row.get("loss_funnel") or {}).get("rendered"),
                        "loss_funnel": row.get("loss_funnel", {}),
                    }
                    for row in (source_status or [])
                    if isinstance(row, dict)
                ],
            },
        )
    except Exception:  # noqa: BLE001 - reporting must never block release
        pass
    # Phase 2 #2: append today's source health to the rolling jsonl log so the
    # anomaly check can compare against the trailing window. Never blocks.
    source_anomalies: list[dict] = []
    dead_parsers: list[dict] = []
    try:
        from news_digest.pipeline import source_health_history as _shh
        from news_digest.pipeline import source_anomaly as _sa

        _shh.append_row(
            state_dir,
            _shh.build_row(
                run_date_london=current_day_london,
                pipeline_run_id=pipeline_run_id,
                run_at_london=now_london().isoformat(),
                source_status=source_status,
            ),
        )
        _history = _shh.load_history(state_dir)
        source_anomalies = _sa.detect_source_anomalies(_history)
        if source_anomalies:
            warnings.append(
                f"Source anomaly: {len(source_anomalies)} source(s) dropped sharply vs their "
                "7-day median — see release_report.source_anomalies."
            )
        # Phase 2 #6: sources that fetch OK but parse nothing every day are
        # broken parsers needing a dedicated per-source extractor. Naming them
        # turns "27 dead feeds" into an actionable list. Warning-only.
        dead_parsers = _sa.detect_dead_parsers(_history)
        if dead_parsers:
            _names = ", ".join(d["name"] for d in dead_parsers[:5])
            warnings.append(
                f"Dead parsers: {len(dead_parsers)} source(s) fetched but parsed 0 items all "
                f"week — need a per-source extractor ({_names}). See release_report.dead_parsers."
            )
    except Exception as exc:  # noqa: BLE001 - logging must never block the release
        logger.warning("source health history/anomaly step failed: %s", exc)
    if source_status["counts"].get("failed", 0) >= 3:
        warnings.append(
            f"Source health: {source_status['counts']['failed']} source(s) failed today — "
            "check release_report.source_status for the list."
        )
    # O1: zero-yield sources — fetched OK but nothing rendered. Worth
    # noticing when many feeds silently underperform, e.g. half the
    # culture sources contributing zero to the digest. Threshold is
    # intentionally loose (warnings only, never blocks the release).
    zero_yield = int(source_status["counts"].get("zero_yield") or 0)
    if zero_yield >= 10:
        warnings.append(
            f"Source yield: {zero_yield} source(s) fetched today but contributed nothing to "
            "the digest — review release_report.source_status (sort by curated_count=0)."
        )
    transport_coverage = _summarise_transport_coverage(
        scan_report=scan_report,
        candidates_report=candidates_report,
        rendered_fingerprints=rendered_fingerprints,
    )
    if transport_coverage["verdict"] == "found_not_rendered":
        warnings.append(
            "Transport coverage: disruption candidates were found but none rendered — "
            "review release_report.transport_coverage."
        )
    elif transport_coverage["verdict"] == "not_checked":
        warnings.append("Transport coverage: transport sources were not checked.")

    diaspora_diagnostics = _summarise_diaspora_diagnostics(scan_report, source_status)
    if diaspora_diagnostics["verdict"] in {"checked_empty", "fetched_but_filtered", "accepted_not_rendered"}:
        warnings.append(
            "Diaspora diagnostics: russian_events block has no rendered item — "
            f"{diaspora_diagnostics['verdict']}; review release_report.diaspora_diagnostics."
        )

    # O2: synthetic freshness gate — any candidate flagged
    # synthetic_stale=True went out as a placeholder because its upstream
    # source was unreachable after refetch×2 (weather) or its persisted
    # state hasn't been re-confirmed in 14+ days (transport reminder).
    synthetic_freshness = _summarise_synthetic_freshness(candidates_report)
    if synthetic_freshness["stale_count"]:
        names = ", ".join(synthetic_freshness["stale_sources"]) or "unknown"
        warnings.append(
            f"Synthetic freshness: {synthetic_freshness['stale_count']} stale synthetic item(s) "
            f"shipping with placeholder data — {names}. See release_report.synthetic_freshness."
        )

    # R2: rejected candidate review with borderline / suspicious flags.
    reject_review = _classify_rejected_candidates(
        writer_report=writer_report,
        curator_report=curator_report,
        candidates_report=candidates_report,
    )
    if reject_review["counts"].get("suspiciously_rejected", 0) > 0:
        warnings.append(
            f"Rejected review: {reject_review['counts']['suspiciously_rejected']} "
            "suspiciously rejected candidate(s) — see release_report.reject_review."
        )
    published_review = _classify_published_candidates(
        candidates_report=candidates_report,
        rendered_fingerprints=rendered_fingerprints,
    )
    if published_review["counts"].get("suspiciously_published", 0) > 0:
        warnings.append(
            f"Published review: {published_review['counts']['suspiciously_published']} "
            "suspicious visible candidate(s) shipped with warning; see release_report.published_review."
        )
    published_facts = read_json(state_dir / "published_facts.json", {"facts": []}) if (state_dir / "published_facts.json").exists() else {"facts": []}
    visible_repeat_review = {"counts": {"visible_lines": 0, "bad_visible_repeats": 0}, "bad_visible_repeats": []}
    visible_repeat_quarantine: dict[str, object] = {"attempted": False, "removed_count": 0}
    if draft_path.exists():
        repeat_html_text = draft_path.read_text(encoding="utf-8")
        visible_repeat_review = _classify_visible_repeat_policy(
            repeat_html_text,
            candidates_report,
            published_facts,
        )
        if int((visible_repeat_review.get("counts") or {}).get("bad_visible_repeats") or 0) > 0:
            sanitized_html, visible_repeat_quarantine = _quarantine_repeat_rendered_html_items(
                repeat_html_text,
                candidates_report,
                visible_repeat_review,
            )
            if int(visible_repeat_quarantine.get("removed_count") or 0) > 0:
                draft_path.write_text(sanitized_html, encoding="utf-8")
                warnings.append(
                    "Repeat policy: removed "
                    f"{visible_repeat_quarantine.get('removed_count')} exact previously-published visible item(s); "
                    "digest continues."
                )
                visible_repeat_review = _classify_visible_repeat_policy(
                    sanitized_html,
                    candidates_report,
                    published_facts,
                )
            else:
                warnings.append(
                    "Repeat policy: suspicious visible repeat(s) detected but no exact HTML line was removed; "
                    "see release_report.visible_repeat_review."
                )
    repeat_policy_report: dict[str, object] = {}
    try:
        repeat_policy_report = _write_repeat_policy_report(
            state_dir,
            current_day_london=current_day_london,
            candidates_report=candidates_report,
            rendered_fingerprints=rendered_fingerprints,
            published_facts=published_facts,
            visible_repeat_review=visible_repeat_review,
            visible_repeat_quarantine=visible_repeat_quarantine,
        )
    except Exception as exc:  # noqa: BLE001 - observability must not block release
        logger.warning("repeat policy report failed: %s", exc)
        warnings.append("Repeat policy report: snapshot failed; check release logs.")
    rendered_html_review = {"counts": {"visible_lines": 0, "bad_visible_items": 0}, "bad_visible_items": []}
    rendered_html_quarantine: dict[str, object] = {"attempted": False, "removed_count": 0}
    if draft_path.exists():
        draft_html_text = draft_path.read_text(encoding="utf-8")
        rendered_html_review = _classify_rendered_html_quality(draft_html_text, candidates_report)
        if rendered_html_review["counts"].get("bad_visible_items", 0) > 0:
            sanitized_html, rendered_html_quarantine = _quarantine_bad_rendered_html_items(
                draft_html_text,
                candidates_report,
                rendered_html_review,
            )
            if int(rendered_html_quarantine.get("removed_count") or 0) > 0:
                draft_path.write_text(sanitized_html, encoding="utf-8")
                warnings.append(
                    "Rendered HTML review quarantined "
                    f"{rendered_html_quarantine.get('removed_count')} rejected/borderline visible item(s); "
                    "digest continues."
                )
                rendered_html_review = _classify_rendered_html_quality(sanitized_html, candidates_report)
    if rendered_html_review["counts"].get("bad_visible_items", 0) > 0:
        # Degradation, not fail-closed: force-remove any residual flagged row
        # and ship the issue anyway. One bad content row must never block the
        # whole digest — only technical inconsistency (missing/unpromoted build,
        # stale date/marker) may block the send (owner 2026-06-16).
        if draft_path.exists():
            forced_html, forced_report = _force_remove_bad_rendered_lines(
                draft_path.read_text(encoding="utf-8"), rendered_html_review
            )
            if int(forced_report.get("removed_count") or 0) > 0:
                draft_path.write_text(forced_html, encoding="utf-8")
                rendered_html_review = _classify_rendered_html_quality(forced_html, candidates_report)
                rendered_html_quarantine["force_removed_count"] = forced_report.get("removed_count")
        warnings.append(
            "Rendered HTML review force-removed residual bad visible item(s); "
            "digest continues (degradation, not block). See release_report.rendered_html_review."
        )
    dedupe_memory = read_json(state_dir / "dedupe_memory.json", {}) if (state_dir / "dedupe_memory.json").exists() else {}
    event_miss_review = _event_miss_review(
        candidates_report=candidates_report,
        writer_report=writer_report,
        rendered_fingerprints=rendered_fingerprints,
        current_day_london=current_day_london,
        dedupe_memory=dedupe_memory,
    )
    critical_event_misses = int((event_miss_review.get("counts") or {}).get("critical_misses") or 0)
    if critical_event_misses:
        warnings.append(
            f"Event miss review: {critical_event_misses} event/ticket candidate(s) look like possible real misses "
            "after excluding deliberate skips, duplicates, stale items and manual-review holds — see release_report.event_miss_review."
        )
    final_loss_check = _final_loss_check(
        candidates_report=candidates_report,
        writer_report=writer_report,
        rendered_fingerprints=rendered_fingerprints,
        dedupe_memory=dedupe_memory,
    )
    critical_losses = int((final_loss_check.get("counts") or {}).get("critical_losses") or 0)
    if critical_losses:
        warnings.append(
            f"Final loss check: {critical_losses} protected/high-value candidate(s) look like possible real misses "
            "after excluding deliberate ticket skips, duplicates and manual-review holds — see release_report.final_loss_check."
        )
    backup_pool = _write_backup_pool(
        state_dir=state_dir,
        current_day_london=current_day_london,
        candidates_report=candidates_report,
        writer_report=writer_report,
        rendered_fingerprints=rendered_fingerprints,
        final_loss_check=final_loss_check,
    )
    if int((backup_pool.get("counts") or {}).get("active") or 0):
        warnings.append(
            f"Backup pool: {backup_pool['counts']['active']} candidate(s) retained with TTL for recovery."
        )
    borderline_queue = _borderline_queue(candidates_report, writer_report)
    if borderline_queue["counts"].get("borderline", 0):
        warnings.append(
            f"Borderline queue: {borderline_queue['counts']['borderline']} candidate(s) held for manual review."
        )

    semantic_dedup_counts = _semantic_dedup_counts_from_memory(state_dir)
    if int(semantic_dedup_counts.get("restored") or 0) > 0:
        warnings.append(
            "Semantic dedupe guard restored "
            f"{semantic_dedup_counts['restored']} embedding-only drop(s) — "
            "review release_report.after_run_summary and dedupe_memory.semantic_guard."
        )
    city_intelligence = summarise_city_intelligence(
        candidates_report.get("candidates", []) if isinstance(candidates_report, dict) else [],
        rendered_fingerprints=rendered_fingerprints,
    )
    for flag in (city_intelligence.get("borough_coverage") or {}).get("skew_flags") or []:
        warnings.append(f"Borough coverage: {flag}")
    trend_detection = build_trend_detection(
        state_dir,
        run_date_london=current_day_london,
        candidates=candidates_report.get("candidates", []) if isinstance(candidates_report, dict) else [],
        rendered_fingerprints=rendered_fingerprints,
    )
    quality_scorecard = _quality_scorecard(
        state_dir=state_dir,
        current_day_london=current_day_london,
        candidates_report=candidates_report,
        writer_report=writer_report,
        rendered_fingerprints=rendered_fingerprints,
        source_status=source_status,
        published_review=published_review,
        transport_coverage=transport_coverage,
    )
    feedback_capture = _update_feedback_items(
        state_dir=state_dir,
        current_day_london=current_day_london,
        candidates_report=candidates_report,
        rendered_fingerprints=rendered_fingerprints,
    )

    # R3: after-run summary, single compact dashboard block.
    after_run_summary = _build_after_run_summary(
        digest_health=digest_health,
        source_status=source_status,
        reject_review=reject_review,
        writer_report=writer_report,
        lost_leads=lost_leads,
        section_underflow=section_underflow,
        synthetic_freshness=synthetic_freshness,
        semantic_dedup=semantic_dedup_counts,
        city_intelligence=city_intelligence,
        trend_detection=trend_detection,
        event_miss_review=event_miss_review,
    )
    speed_report = _build_speed_report(
        state_dir=state_dir,
        scan_report=scan_report,
        curator_report=curator_report,
        llm_rewrite_report=llm_rewrite_report,
        writer_report=writer_report,
        editor_report=editor_report,
    )
    try:
        write_json(state_dir / "speed_report.json", speed_report)
    except Exception as exc:  # noqa: BLE001
        logger.warning("speed report snapshot failed: %s", exc)

    # S4: visible-HTML contract. Reconcile the shipped draft against the
    # per-section minimums + lead contract and run bounded recovery BEFORE
    # promotion (owner refinement #3: validate the draft, not the already-
    # promoted outgoing). Guarded — a reconciler error must never abort the
    # issue (never-block); the issue still ships.
    visible_contract: dict[str, object] = {"enabled": False}
    try:
        from news_digest.pipeline.release_reconcile import reconcile_visible_html  # noqa: PLC0415

        visible_contract = reconcile_visible_html(
            draft_path,
            [c for c in (candidates_report.get("candidates") or []) if isinstance(c, dict)],
            (writer_report or {}).get("section_counts") or {},
        )
        write_json(state_dir / "visible_contract_report.json", visible_contract)
        if not (visible_contract.get("control_assertion") or {}).get("ok", True):
            warnings.append(
                "Visible contract: writer section counts diverge from the shipped HTML "
                "— see visible_contract_report.json (RC1)."
            )
        if not visible_contract.get("lead_visible", True):
            warnings.append("Visible contract: lead block «Главная история дня» is not visible in the shipped HTML.")
        if visible_contract.get("inserted_total"):
            warnings.append(
                f"Visible contract: recovered {visible_contract['inserted_total']} line(s) into thin section(s) "
                "from the recoverable reserve before promotion."
            )
        for short in visible_contract.get("still_under_minimum") or []:
            warnings.append(
                f"Visible contract: «{short['section']}» shipped {short['actual']}/{short['minimum']} "
                f"— {short['reason']} (honest shortfall, issue still ships)."
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("visible-HTML reconciler failed (issue still ships): %s", exc)
        # The report must ALWAYS exist (RC1): a missing file used to read as
        # "no divergence". Write an explicit degraded marker instead.
        visible_contract = {"enabled": False, "error": str(exc)}
        try:
            write_json(state_dir / "visible_contract_report.json", visible_contract)
        except Exception:  # noqa: BLE001
            pass

    # A known visible failure must not ship as a silent "pass". The issue still
    # ships (never-block), but the decision tells the truth: "ship_degraded".
    # Technical errors still fail-close. (P0-A.)
    ok = not errors
    visible_contract_failed = bool(visible_contract.get("enabled")) and not (
        (visible_contract.get("control_assertion") or {}).get("ok", True)
    )
    repeat_policy_removed = int(visible_repeat_quarantine.get("removed_count") or 0) > 0
    unrepaired_bad_visible_lines = int(
        (rendered_html_review.get("counts") or {}).get("bad_visible_items", 0) or 0
    ) > 0
    if not ok:
        release_decision = "fail"
    elif visible_contract_failed or unrepaired_bad_visible_lines or repeat_policy_removed:
        release_decision = "ship_degraded"
    else:
        release_decision = "pass"
    if ok:
        shutil.copyfile(draft_path, output_path)
        message = f"Release passed. Promoted {draft_path} to {output_path}."
    else:
        message = FAIL_CLOSED_SUMMARY

    audit_trail_path: str | None = None
    try:
        audit_trail_path = _write_audit_trail(
            state_dir=state_dir,
            current_day_london=current_day_london,
            candidates_report=candidates_report,
            writer_report=writer_report,
            rendered_fingerprints=rendered_fingerprints,
            dedupe_memory=dedupe_memory,
            release_decision=release_decision,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("audit trail snapshot failed: %s", exc)
        warnings.append("Audit trail: snapshot failed; check release logs.")

    final_selection_report: dict[str, object] = {}
    try:
        final_selection_report = _write_final_selection_report(
            state_dir=state_dir,
            current_day_london=current_day_london,
            candidates_report=candidates_report,
            writer_report=writer_report,
            rendered_fingerprints=rendered_fingerprints,
            dedupe_memory=dedupe_memory,
            final_html=draft_path.read_text(encoding="utf-8") if draft_path.exists() else "",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("final selection report failed: %s", exc)
        warnings.append("Final selection report: snapshot failed; check release logs.")

    report_payload = {
        "release_gate_version": RELEASE_GATE_VERSION,
        "pipeline_run_id": pipeline_run_id,
        "run_at_london": now_london().isoformat(),
        "run_date_london": current_day_london,
        "release_decision": release_decision,
        "message": message,
        "errors": errors,
        "warnings": warnings,
        "lost_leads": lost_leads,
        "section_underflow": section_underflow,
        "cost_summary": cost_summary,
        "llm_rewrite_diagnostics": {
            "diagnostics_summary": (llm_rewrite_report or {}).get("diagnostics_summary") if isinstance(llm_rewrite_report, dict) else {},
            "token_budget_history": (llm_rewrite_report or {}).get("token_budget_history") if isinstance(llm_rewrite_report, dict) else {},
            "batching_strategy": (llm_rewrite_report or {}).get("batching_strategy") if isinstance(llm_rewrite_report, dict) else {},
            "concurrency_policy": (llm_rewrite_report or {}).get("concurrency_policy") if isinstance(llm_rewrite_report, dict) else {},
            "batch_api_policy": (llm_rewrite_report or {}).get("batch_api_policy") if isinstance(llm_rewrite_report, dict) else {},
        },
        "cost_latency_budget": cost_latency_budget,
        "model_bakeoff": model_bakeoff,
        "change_type_summary": change_type_summary,
        "cross_day_recurrence": cross_day_recurrence,
        "event_completeness": event_completeness,
        "news_lead_quality": news_lead_quality,
        "digest_health": digest_health,
        "source_status": source_status,
        "source_anomalies": source_anomalies,
        "dead_parsers": dead_parsers,
        "transport_coverage": transport_coverage,
        "diaspora_diagnostics": diaspora_diagnostics,
        "reject_review": reject_review,
        "published_review": published_review,
        "visible_repeat_review": visible_repeat_review,
        "visible_repeat_quarantine": visible_repeat_quarantine,
        "repeat_policy_report": repeat_policy_report,
        "rendered_html_review": rendered_html_review,
        "rendered_html_quarantine": rendered_html_quarantine,
        "event_miss_review": event_miss_review,
        "final_loss_check": final_loss_check,
        "backup_pool": backup_pool,
        "borderline_queue": borderline_queue,
        "quality_scorecard": quality_scorecard,
        "feedback_capture": feedback_capture,
        "speed_report": speed_report,
        "synthetic_freshness": synthetic_freshness,
        "city_intelligence": city_intelligence,
        "trend_detection": trend_detection,
        "after_run_summary": after_run_summary,
        "prompt_versions": _prompts_snapshot(),
        "prompt_drift": prompt_drift,
        "audit_trail_path": audit_trail_path,
        "final_selection_report": final_selection_report,
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
        "duration_seconds": round(time.monotonic() - stage_started, 3),
    }
    write_json(report_path, report_payload)

    # A0 — Daily Index Snapshot: append-only JSONL covering every
    # candidate (published + rejected) so we can replay "what did we
    # see on day X, and what did we choose to publish/drop". Written
    # regardless of gate outcome so we never lose a day's record.
    try:
        from news_digest.pipeline.history import write_daily_index_snapshot  # noqa: PLC0415
        write_daily_index_snapshot(project_root)
    except Exception as exc:  # noqa: BLE001
        # Snapshot is observational; never break the release on its failure.
        logger.warning("daily_index snapshot failed: %s", exc)
    try:
        append_city_intelligence_history(
            state_dir,
            report_payload=report_payload,
            candidates=candidates_report.get("candidates", []) if isinstance(candidates_report, dict) else [],
            rendered_fingerprints=rendered_fingerprints,
            trend_detection=trend_detection,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("city intelligence history snapshot failed: %s", exc)

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
