from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    REQUIRED_BLOCKS,
    extract_sections,
    is_placeholder_practical_angle,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)


MIN_CITY_PRACTICAL_ANGLE_LENGTH = 40
MAX_WEAK_CITY_CANDIDATE_SHARE = 0.5
PRE_SEND_RUSSIAN_EDITOR_MODEL = "gpt-4o"

PRE_SEND_RUSSIAN_EDITOR_PROMPT = """Ты выпускающий редактор русского Telegram-дайджеста Greater Manchester.
Тебе дают уже видимые строки выпуска. Исправь только русский язык и очевидные редакторские дефекты. Не добавляй новых фактов.

Верни JSON-объект: {"items":[{"index":0,"status":"ok|fixed","line":"...","reason":"..."}]}.

Правила:
- Сохраняй bullet "• " и HTML-теги/ссылки, если они есть.
- Не меняй даты, числа, имена, районы, площадки, источники и ссылки.
- Исправляй кальку и плохой русский: "защита от дождя", "возрастелет", "в возрасте лет", "перевернулся на крыше", "спасён с высоты", "инцидент был успешно разрешен".
- Погода должна звучать по-человечески: "возьмите зонт", "дождевик", "планируйте пересадки с запасом"; не пиши "защита от дождя".
- Если факта не хватает, не выдумывай возраст/место/причину. Лучше убери битую фразу.
- Футбол и билеты не должны звучать как машинная оценка: убирай "это важная информация" и "интересная информация", заменяй на конкретный смысл из строки.
- Если строка нормальная, верни её без изменений со status="ok".
"""

_BAD_RUSSIAN_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"держите\s+защиту\s+от\s+дождя\s+под\s+рукой", re.IGNORECASE), "возьмите зонт"),
    (re.compile(r"защиту\s+от\s+дождя", re.IGNORECASE), "зонт или дождевик"),
    (re.compile(r"\bв\s+возрасте\s*лет\b", re.IGNORECASE), ""),
    (re.compile(r"\bвозрасте\s*лет\b", re.IGNORECASE), ""),
    (re.compile(r"\bвозрастелет\b", re.IGNORECASE), ""),
    (re.compile(r"автомобиль\s+перевернулся\s+на\s+крыше", re.IGNORECASE), "автомобиль перевернулся и оказался на крыше"),
    (re.compile(r"машина\s+перевернулась\s+на\s+крыше", re.IGNORECASE), "машина перевернулась и оказалась на крыше"),
    (re.compile(r"\bспас[её]н\s+с\s+высоты\b", re.IGNORECASE), "снят с высоты спасателями"),
    (re.compile(r"\bспасли\s+с\s+высоты\b", re.IGNORECASE), "сняли с высоты"),
    (re.compile(r"\bинцидент\s+был\s+успешно\s+разреш[её]н\.?", re.IGNORECASE), ""),
    (re.compile(r"\bэто\s+важная\s+информация\s+для\s+", re.IGNORECASE), "это важно для "),
    (re.compile(r"\bэто\s+интересная\s+информация\s+для\s+", re.IGNORECASE), "это может быть полезно для "),
)

_BAD_RUSSIAN_DETECTORS: tuple[re.Pattern[str], ...] = (
    re.compile(r"защит[ау]\s+от\s+дождя", re.IGNORECASE),
    re.compile(r"\b(?:в\s+)?возрасте\s*лет\b", re.IGNORECASE),
    re.compile(r"\bвозрастелет\b", re.IGNORECASE),
    re.compile(r"перевернул[асься]+\s+на\s+крыше", re.IGNORECASE),
    re.compile(r"спас[её]н\s+с\s+высоты", re.IGNORECASE),
    re.compile(r"инцидент\s+был\s+успешно\s+разреш[её]н", re.IGNORECASE),
    re.compile(r"это\s+(?:важная|интересная)\s+информация", re.IGNORECASE),
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


def _is_weak_city_candidate(candidate: dict) -> bool:
    practical_angle = str(candidate.get("practical_angle") or "").strip()
    if is_placeholder_practical_angle(practical_angle):
        return True
    return len(practical_angle) < MIN_CITY_PRACTICAL_ANGLE_LENGTH


def _has_included_candidates_for_section(candidates: list[dict], section_name: str) -> bool:
    return any(
        PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or "")) == section_name
        for candidate in candidates
        if isinstance(candidate, dict) and candidate.get("include")
    )


def _polish_russian_line_rules(line: str) -> tuple[str, list[str]]:
    fixed = str(line or "")
    reasons: list[str] = []
    for pattern, replacement in _BAD_RUSSIAN_PATTERNS:
        if pattern.search(fixed):
            fixed = pattern.sub(replacement, fixed)
            reasons.append(pattern.pattern)
    fixed = re.sub(r"\s{2,}", " ", fixed)
    fixed = re.sub(r"\s+([,.;:])", r"\1", fixed)
    fixed = re.sub(r"•\s*,\s*", "• ", fixed)
    fixed = re.sub(r"\.\s*\.", ".", fixed)
    fixed = re.sub(
        r"^(•\s+)([а-яё])",
        lambda match: match.group(1) + match.group(2).upper(),
        fixed,
    )
    fixed = fixed.strip()
    return fixed, reasons


def _line_needs_russian_editor(line: str) -> bool:
    text = str(line or "")
    return any(pattern.search(text) for pattern in _BAD_RUSSIAN_DETECTORS)


def _line_preserves_links(original: str, fixed: str) -> bool:
    original_links = re.findall(r"<a\s+[^>]*href=", str(original or ""), flags=re.IGNORECASE)
    fixed_links = re.findall(r"<a\s+[^>]*href=", str(fixed or ""), flags=re.IGNORECASE)
    return len(original_links) == len(fixed_links)


def _visible_line_items(sections: dict[str, list[str]]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    index = 0
    for section_name, lines in sections.items():
        for line in lines:
            if not line.strip() or line.strip() == "•":
                continue
            items.append({"index": index, "section": section_name, "line": line})
            index += 1
    return items


def _call_pre_send_russian_editor(items: list[dict[str, object]], api_key: str) -> tuple[dict[int, str], dict[str, object]]:
    if not items or not api_key:
        return {}, {"status": "skipped_missing_api_key" if not api_key else "skipped_no_items"}
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        return {}, {"status": "skipped_missing_openai_package"}
    from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415

    client = OpenAI(api_key=api_key, timeout=60, max_retries=1)
    messages = [
        {"role": "system", "content": PRE_SEND_RUSSIAN_EDITOR_PROMPT},
        {"role": "user", "content": json.dumps({"items": items}, ensure_ascii=False)},
    ]
    max_tokens = min(12000, 280 * len(items) + 1200)
    try:
        response = client.chat.completions.create(
            model=PRE_SEND_RUSSIAN_EDITOR_MODEL,
            messages=messages,
            temperature=0.1,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )
    except Exception as exc:  # noqa: BLE001
        return {}, {"status": "failed", "error": f"{exc.__class__.__name__}: {exc}"}
    record_call_from_response(
        response=response,
        stage="editor",
        provider="OpenAI",
        model=PRE_SEND_RUSSIAN_EDITOR_MODEL,
        prompt_name="pre_send_russian_editor",
        messages=messages,
        max_tokens=max_tokens,
    )
    raw = str(response.choices[0].message.content or "").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return {}, {"status": "parse_failed", "error": f"{exc.__class__.__name__}: {exc}", "raw_excerpt": raw[:400]}
    rows = parsed.get("items") if isinstance(parsed, dict) else parsed
    if not isinstance(rows, list):
        return {}, {"status": "parse_failed", "error": "JSON root has no items list", "raw_excerpt": raw[:400]}
    fixes: dict[int, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            index = int(row.get("index"))
        except (TypeError, ValueError):
            continue
        line = str(row.get("line") or "").strip()
        if line.startswith("• "):
            fixes[index] = line
    return fixes, {"status": "ok", "items_sent": len(items), "items_returned": len(fixes)}


def _pre_send_polish_sections(sections: dict[str, list[str]], warnings: list[str]) -> tuple[dict[str, list[str]], dict[str, object]]:
    rule_fixed = 0
    model_fixed = 0
    remaining_bad = 0
    polished: dict[str, list[str]] = {}
    for section_name, lines in sections.items():
        new_lines: list[str] = []
        for line in lines:
            fixed, reasons = _polish_russian_line_rules(line)
            if reasons and fixed != line:
                rule_fixed += 1
            new_lines.append(fixed)
        polished[section_name] = new_lines

    items = _visible_line_items(polished)
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    model_fixes, model_report = _call_pre_send_russian_editor(items, api_key)
    if model_report.get("status") not in {"ok", "skipped_no_items"}:
        warnings.append(f"Pre-send Russian editor skipped/failed: {model_report.get('status')} {model_report.get('error') or ''}".strip())
    if model_fixes:
        by_index = {int(item["index"]): item for item in items}
        for index, fixed_line in model_fixes.items():
            item = by_index.get(index)
            if not item:
                continue
            original = str(item.get("line") or "")
            if not _line_preserves_links(original, fixed_line):
                continue
            if fixed_line != original:
                item["line"] = fixed_line
                model_fixed += 1
        rebuilt: dict[str, list[str]] = {}
        for item in items:
            rebuilt.setdefault(str(item.get("section") or ""), []).append(str(item.get("line") or ""))
        # Preserve empty sections that had no visible lines.
        for section_name in polished:
            rebuilt.setdefault(section_name, [])
        polished = rebuilt

    bad_examples: list[str] = []
    for item in _visible_line_items(polished):
        line = str(item.get("line") or "")
        if _line_needs_russian_editor(line):
            remaining_bad += 1
            if len(bad_examples) < 8:
                bad_examples.append(line[:240])

    return polished, {
        "enabled": True,
        "rules_fixed": rule_fixed,
        "model_fixed": model_fixed,
        "remaining_bad": remaining_bad,
        "bad_examples": bad_examples,
        "model": PRE_SEND_RUSSIAN_EDITOR_MODEL,
        "model_report": model_report,
    }


def edit_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "editor_report.json"

    draft_text = draft_path.read_text(encoding="utf-8") if draft_path.exists() else ""
    sections = extract_sections(draft_text)
    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
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
        warnings.append(
            "Draft is skewed toward soft items compared with city/public-affairs coverage: "
            f"{len(soft_candidates)} soft vs {len(city_candidates)} city/public-affairs candidate(s)."
        )

    weak_city_candidates = [
        candidate
        for candidate in city_candidates
        if _is_weak_city_candidate(candidate)
    ]
    weak_city_share = len(weak_city_candidates) / len(city_candidates) if city_candidates else 0
    if city_candidates and weak_city_share > MAX_WEAK_CITY_CANDIDATE_SHARE:
        warnings.append(
            "City/public-affairs candidates need editorial rewrite or candidate-level drop: "
            f"({len(weak_city_candidates)}/{len(city_candidates)})."
        )

    normalized_sections, russian_editor_report = _pre_send_polish_sections(normalized_sections, warnings)
    if int(russian_editor_report.get("remaining_bad") or 0) > 0:
        warnings.append(
            "Pre-send Russian editor still sees "
            f"{russian_editor_report.get('remaining_bad')} suspicious line(s); see editor_report.pre_send_russian_editor."
        )

    # "Коротко" больше не требуется — убрана из дайджеста
    required_to_check = [b for b in REQUIRED_BLOCKS if b != "Коротко"]
    for block in required_to_check:
        if block not in normalized_sections:
            if block == "Что важно сегодня" and not _has_included_candidates_for_section(included_candidates, block):
                warnings.append("No included today_focus candidates; omitted «Что важно сегодня» instead of blocking release.")
            else:
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

    try:
        from news_digest.pipeline.cost_tracker import dump_stage, snapshot, summarise  # noqa: PLC0415
        dump_stage(state_dir, "editor")
        cost_summary = summarise(snapshot(stage="editor"))
    except Exception:  # noqa: BLE001
        cost_summary = {}

    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "warnings": warnings,
            "city_candidate_count": len(city_candidates),
            "soft_candidate_count": len(soft_candidates),
            "weak_city_candidate_count": len(weak_city_candidates),
            "weak_city_candidate_share": round(weak_city_share, 3),
            "pre_send_russian_editor": russian_editor_report,
            "cost_summary": cost_summary,
            "min_city_practical_angle_length": MIN_CITY_PRACTICAL_ANGLE_LENGTH,
            "max_weak_city_candidate_share": MAX_WEAK_CITY_CANDIDATE_SHARE,
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
