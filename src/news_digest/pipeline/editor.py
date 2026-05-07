from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    REQUIRED_BLOCKS,
    extract_sections,
    is_placeholder_practical_angle,
    now_london,
    read_json,
    today_london,
    write_json,
)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path
    draft_path: Path


def _unique_preserving_order(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for line in lines:
        key = line.strip()
        if key in seen:
            continue
        seen.add(key)
        result.append(line)
    return result


def edit_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "editor_report.json"

    draft_text = draft_path.read_text(encoding="utf-8") if draft_path.exists() else ""
    sections = extract_sections(draft_text)
    payload = read_json(candidates_path, {"candidates": []})
    included_candidates = [
        candidate
        for candidate in payload.get("candidates", [])
        if isinstance(candidate, dict) and candidate.get("include")
    ]

    errors: list[str] = []
    warnings: list[str] = []

    city_candidates = [
        candidate
        for candidate in included_candidates
        if candidate.get("category") in {"media_layer", "gmp", "public_services", "city_news", "council"}
    ]
    soft_candidates = [
        candidate
        for candidate in included_candidates
        if candidate.get("category") in {"venues_tickets", "ticket_radar", "culture_weekly", "football"}
    ]

    normalized_sections: dict[str, list[str]] = {}
    seen_lines_to_section: dict[str, str] = {}
    duplicate_collisions = 0
    for section_name, lines in sections.items():
        deduped = _unique_preserving_order(lines)
        filtered: list[str] = []
        if len(deduped) < len(lines):
            warnings.append(f"Removed duplicate lines in section {section_name}.")
        for line in deduped:
            key = line.strip()
            previous_section = seen_lines_to_section.get(key)
            if previous_section and previous_section != section_name:
                duplicate_collisions += 1
                warnings.append(
                    f"Removed cross-section duplicate from {section_name}; already present in {previous_section}."
                )
                continue
            seen_lines_to_section[key] = section_name
            filtered.append(line)
        normalized_sections[section_name] = filtered

    if not city_candidates:
        errors.append("No city/public-affairs candidates are included.")
    elif len(soft_candidates) > len(city_candidates) * 2:
        errors.append("Draft is overly skewed toward soft items compared with city/public-affairs coverage.")

    weak_city_candidates = [
        candidate
        for candidate in city_candidates
        if is_placeholder_practical_angle(str(candidate.get("practical_angle") or ""))
    ]
    if city_candidates and len(weak_city_candidates) == len(city_candidates):
        errors.append("All city/public-affairs candidates still rely on placeholder practical angles.")

    # "Коротко" больше не требуется — убрана из дайджеста
    required_to_check = [b for b in REQUIRED_BLOCKS if b != "Коротко"]
    for block in required_to_check:
        if block not in normalized_sections:
            errors.append(f"Required block missing after editor pass: {block}.")

    rendered: list[str] = []
    if draft_text:
        first_line = draft_text.splitlines()[0].strip()
        rendered.append(first_line)
        rendered.append("")
    for section_name, lines in normalized_sections.items():
        real_lines = [line for line in lines if line.strip() and line.strip() != "•"]
        if not real_lines:
            continue
        rendered.append(f"<b>{section_name}</b>")
        rendered.extend(real_lines)
        rendered.append("")
    if rendered:
        draft_path.write_text("\n".join(rendered).strip() + "\n", encoding="utf-8")

    write_json(
        report_path,
        {
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "warnings": warnings,
            "city_candidate_count": len(city_candidates),
            "soft_candidate_count": len(soft_candidates),
            "weak_city_candidate_count": len(weak_city_candidates),
            "duplicate_collisions": duplicate_collisions,
            "draft_path": str(draft_path.resolve()),
        },
    )

    return StageResult(
        not errors,
        "Editor stage completed." if not errors else "Editor stage found blocking issues.",
        report_path,
        draft_path,
    )
