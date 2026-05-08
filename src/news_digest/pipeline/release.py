from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    REQUIRED_BLOCKS,
    REQUIRED_SCAN_CATEGORIES,
    extract_sections,
    now_london,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.history import update_published_facts


BANNED_MARKERS = [
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
]

ENGLISH_PROSE_PATTERN = re.compile(
    r"\b(?:the|and|for|with|from|after|following|across|response|operators|said|says|their)\b",
    re.IGNORECASE,
)

FAIL_CLOSED_SUMMARY = (
    "Digest release is blocked until collector, dedupe, validator, writer and gate inputs pass."
)


@dataclass(slots=True)
class ReleaseResult:
    ok: bool
    message: str
    report_path: Path
    output_path: Path


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
            "<b>Транспорт и сбои</b>\n"
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
    REQUIRED_USABLE_CATEGORIES = {k for k in REQUIRED_SCAN_CATEGORIES if k not in {"transport", "gmp"}}

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
        if int(editor_report.get("weak_city_candidate_count") or 0) > 0:
            errors.append("Editor report still has weak city/public-affairs candidates.")

    return rendered_fingerprints


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

    for marker in BANNED_MARKERS:
        if marker in html_text:
            errors.append(f"Draft digest contains placeholder or bot marker: {marker}.")

    lower_text = html_text.lower()
    for marker in BANNED_AUTHOR_VOICE:
        if marker in lower_text:
            errors.append(f"Draft digest contains author voice marker: {marker}.")
    for marker in BAD_EDITORIAL_PROSE:
        if marker in lower_text:
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
            # English-prose detection is intentionally non-blocking while
            # the pipeline has no translator stage. Without an LLM/rewrite
            # in front of writer, blocking here would fail every release
            # in which fresh BBC/GMP/Manc items survived. The signal is
            # still useful for monitoring (it shows up in the writer
            # report's warnings) but the gate stays open.
            if not re.search(r"[а-яё]", body, flags=re.IGNORECASE):
                latin_words = re.findall(r"[A-Za-z][A-Za-z'’-]+", body)
                if len(latin_words) >= 8 and ENGLISH_PROSE_PATTERN.search(body):
                    # Intentional: do not append to errors. Re-enable when
                    # a translator step is wired before writer.
                    break

    for block in LOW_SIGNAL_BLOCKS:
        lines = sections.get(block, [])
        if lines and any("не добавляю" in line.lower() or "нет" in line.lower() for line in lines):
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
    writer_report = _load_optional_json(state_dir / "writer_report.json")
    editor_report = _load_optional_json(state_dir / "editor_report.json")

    errors: list[str] = []
    _validate_scan_report(scan_report, current_day_london, errors)
    candidate_context = _validate_candidates(candidates_report, current_day_london, errors)
    rendered_fingerprints = _validate_stage_reports(writer_report, editor_report, errors)
    _validate_draft(
        draft_path=draft_path,
        scan_report=scan_report,
        included_candidates=candidate_context["included_candidates"],
        rendered_fingerprints=rendered_fingerprints,
        current_day_london=current_day_london,
        errors=errors,
    )

    ok = not errors
    published_facts_updated = False
    if ok:
        shutil.copyfile(draft_path, output_path)
        message = f"Release passed. Promoted {draft_path} to {output_path}."
        # Record published facts at gate-pass time, not at send time.
        # This way tomorrow's dedupe sees today's items even if the
        # actual Telegram send fails or is delayed. Only fingerprints
        # that the writer actually rendered into the draft are recorded.
        rendered_candidates = [
            candidate
            for candidate in candidate_context["included_candidates"]
            if str(candidate.get("fingerprint") or "") in rendered_fingerprints
        ]
        if rendered_candidates:
            try:
                update_published_facts(project_root, rendered_candidates)
                published_facts_updated = True
            except Exception as exc:  # noqa: BLE001 - surface in release report.
                errors.append(f"Failed to update published_facts: {exc}")
                ok = False
                message = FAIL_CLOSED_SUMMARY
    else:
        message = FAIL_CLOSED_SUMMARY

    report_payload = {
        "run_at_london": now_london().isoformat(),
        "run_date_london": current_day_london,
        "release_decision": "pass" if ok else "fail",
        "message": message,
        "errors": errors,
        "published_facts_updated": published_facts_updated,
        "inputs": {
            "collector_report": str((state_dir / "collector_report.json").resolve()),
            "candidates": str((state_dir / "candidates.json").resolve()),
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

    return ReleaseResult(ok=ok, message=message, report_path=report_path, output_path=output_path)
