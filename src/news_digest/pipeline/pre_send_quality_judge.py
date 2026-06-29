"""Pre-send quality judge for the production digest.

This is the strong-model final reader before Telegram delivery. The bulk
pipeline remains mini-first; this stage reads only the already-built digest
and compact evidence for rendered items, then decides whether the issue is
safe to send.
"""
from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from news_digest.pipeline.common import canonical_url_identity, pipeline_run_id_from, read_json, today_london, write_json
from news_digest.pipeline.fact_lock import FACT_LOCK_VERSION, iter_fact_texts, unsupported_fact_tokens
from news_digest.pipeline.model_routing import resolve_model_route, sdk_retries_for_route


logger = logging.getLogger(__name__)


PROMPT_VERSION = "v3"
REPORT_NAME = "pre_send_quality_report.json"
ALLOWED_TO_SEND = {"pass", "warn"}
BLOCKING_DECISIONS = {"repair_required", "block"}


SYSTEM_PROMPT = """Ты старший редактор и fact-check судья русскоязычного утреннего дайджеста Greater Manchester.

Твоя задача — решить, можно ли отправлять уже собранный выпуск читателям в Telegram.

Проверяй именно финальный выпуск, а не широкий исходный пул:
1. смысловая верность: текст не меняет субъект, роль, обвинение, статус дела;
2. crime/court/sensitive: особенно строго проверяй роли, возраст, обвиняемый/жертва/свидетель, "обвиняется" vs "осуждён";
3. события/афиша: venue не должен стать artist, дата/окно даты не должны противоречить evidence;
4. география: не выдавать не-GM за Greater Manchester без явного контекста;
5. русский текст: нет непереведённых бытовых английских слов, машинной кальки, абсурда;
6. практическая польза: карточка должна быть понятной без открытия ссылки;
7. продуктовая полнота: проверь product_completeness — не схлопнулись ли «Свежие новости»,
   «Футбол», «Выходные в GM», не доминируют ли билеты/концерты над core news, не слишком ли
   много выбранных кандидатов потеряно между rewrite/writer/render;
8. не блокируй выпуск за стиль, если смысл безопасен.

Decision:
- "pass": критических проблем нет.
- "warn": есть мелкие стилистические/плотностные замечания, но выпуск можно отправлять.
- "repair_required": есть конкретные строки, которые нельзя отправлять без правки/удаления.
- "block": выпуск в целом небезопасен или почти пустой; это редкий случай.

Верни ТОЛЬКО JSON без markdown:
{
  "decision": "pass|warn|repair_required|block",
  "confidence": 0.0-1.0,
  "critical_errors": [
    {
      "line_index": 1,
      "section": "...",
      "problem": "...",
      "risk": "factual|legal|sensitive|geo|date|translation|format",
      "suggested_action": "repair|strip"
    }
  ],
  "actions": [
    {
      "line_index": 1,
      "section": "...",
      "action": "keep|patch|replace|strip",
      "replacement_text": "• ...",
      "reason": "...",
      "risk": "factual|legal|sensitive|geo|date|translation|format|product"
    }
  ],
  "warnings": ["..."],
  "notes": "до 240 символов"
}

Если сомневаешься, предпочти "repair_required" только для реально опасной смысловой ошибки. Не требуй переписывать выпуск ради вкуса.
Если проблема продуктовая, но выпуск всё ещё можно отправить как degraded issue, ставь "warn" и явно назови провал блока.
actions — это не комментарии, а конкретные редакторские действия. Для безопасной строки ставь keep только если она упомянута в critical_errors; не перечисляй весь выпуск. Для patch/replace replacement_text должен начинаться с «• » и опираться только на rendered_candidates/facts.
"""


@dataclass(frozen=True)
class PreSendQualityResult:
    status: str
    decision: str
    can_send: bool
    reason: str
    model: str = ""
    provider: str = ""
    prompt_version: str = PROMPT_VERSION
    run_date_london: str = ""
    pipeline_run_id: str = ""
    digest_sha256: str = ""
    duration_seconds: float = 0.0
    confidence: float | None = None
    critical_errors: list[dict[str, Any]] | None = None
    actions: list[dict[str, Any]] | None = None
    warnings: list[str] | None = None
    product_completeness: dict[str, Any] | None = None
    deterministic_post_check: dict[str, Any] | None = None
    repair_executor: dict[str, Any] | None = None
    notes: str = ""
    raw: dict[str, Any] | None = None


def digest_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _strip_tags(text: str) -> str:
    text = re.sub(r"<a\s+[^>]*>(.*?)</a>", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"</?(?:b|i|strong|em)>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _digest_line_slots_from_html(digest_html: str) -> list[dict[str, Any]]:
    section = ""
    lines: list[dict[str, Any]] = []
    for raw_index, raw_line in enumerate(digest_html.splitlines()):
        line = raw_line.strip()
        if not line:
            continue
        header_match = re.fullmatch(r"<b>(.*?)</b>", line)
        if header_match and not _strip_tags(line).startswith("Greater Manchester Brief"):
            section = _strip_tags(header_match.group(1))
            continue
        plain = _strip_tags(line)
        if not plain or plain.startswith("Greater Manchester Brief"):
            continue
        is_item = plain.startswith("•") or bool(section)
        if not is_item:
            continue
        lines.append(
            {
                "line_index": len(lines) + 1,
                "section": section,
                "text": plain[:900],
                "html": line,
                "raw_index": raw_index,
            }
        )
    # The judge model must see the WHOLE issue, not just the first 60 lines —
    # otherwise tail defects (e.g. a broken line deep in the ticket list) are
    # invisible to it. 250 covers any realistic issue with headroom.
    return lines[:250]


def digest_lines_from_html(digest_html: str) -> list[dict[str, Any]]:
    return [
        {"line_index": item["line_index"], "section": item.get("section") or "", "text": item.get("text") or ""}
        for item in _digest_line_slots_from_html(digest_html)
    ]


def _line_url_identity(line: str) -> str:
    match = re.search(r'<a\s+[^>]*href="([^"]+)"', str(line or ""), flags=re.IGNORECASE)
    if not match:
        return ""
    return canonical_url_identity(html.unescape(match.group(1)))


def _candidate_index(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        url_key = canonical_url_identity(str(candidate.get("source_url") or ""))
        if url_key:
            index.setdefault(url_key, candidate)
        fp = str(candidate.get("fingerprint") or "").strip()
        if fp:
            index.setdefault(fp, candidate)
    return index


def _rendered_candidates(project_root: Path) -> list[dict[str, Any]]:
    state_dir = project_root / "data" / "state"
    writer_report = read_json(state_dir / "writer_report.json", {})
    rendered = {
        str(fp).strip()
        for fp in writer_report.get("rendered_candidate_fingerprints", [])
        if str(fp).strip()
    }
    candidates_payload = read_json(state_dir / "candidates.json", {"candidates": []})
    candidates = candidates_payload.get("candidates") or []
    if not rendered:
        return []
    summary: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        fingerprint = str(candidate.get("fingerprint") or "").strip()
        if fingerprint not in rendered:
            continue
        summary.append(
            {
                "fingerprint": fingerprint,
                "title": str(candidate.get("title") or "")[:220],
                "source_label": str(candidate.get("source_label") or ""),
                "source_url": str(candidate.get("source_url") or ""),
                "primary_block": str(candidate.get("primary_block") or ""),
                "category": str(candidate.get("category") or ""),
                "summary": str(candidate.get("summary") or "")[:360],
                "lead": str(candidate.get("lead") or "")[:360],
                "evidence_text": str(candidate.get("evidence_text") or "")[:700],
                "event": candidate.get("event") if isinstance(candidate.get("event"), dict) else {},
                "structured_event_hint": candidate.get("structured_event_hint") if isinstance(candidate.get("structured_event_hint"), dict) else {},
                "practical_angle": str(candidate.get("practical_angle") or "")[:260],
                "draft_line": _strip_tags(str(candidate.get("draft_line") or ""))[:700],
                "is_lead": bool(candidate.get("is_lead")),
                "protected_lane": str(candidate.get("protected_lane") or ""),
            }
        )
    # Match digest_lines_from_html: the judge must see metadata for the WHOLE
    # issue, not the first 60 rendered candidates — otherwise tail items (deep in
    # the ticket list) have no metadata for the model to cross-check. 250 covers
    # any realistic issue with headroom.
    return summary[:250]


def _product_completeness_context(project_root: Path, digest_lines: list[dict[str, Any]]) -> dict[str, Any]:
    state_dir = project_root / "data" / "state"
    writer_report = read_json(state_dir / "writer_report.json", {})
    release_report = read_json(state_dir / "release_report.json", {})
    # S4 / RC1: measure the SHIPPED HTML, not writer intent. The editor mutates
    # the draft after the writer reports its counts (drops the lead, trims
    # outside-GM), so writer_report.section_counts describes a digest that was
    # never sent. Count from the FULL draft HTML via extract_sections (not the
    # 60-line judge sample, which under-counts long ticket tails). Fall back to
    # the parsed lines, then writer counts, only if the HTML is unavailable.
    section_counts: dict[str, int] = {}
    draft_html_path = state_dir / "draft_digest.html"
    if draft_html_path.exists():
        from news_digest.pipeline.common import extract_sections  # noqa: PLC0415
        section_counts = {
            section: len(lines)
            for section, lines in extract_sections(draft_html_path.read_text(encoding="utf-8")).items()
        }
    if not section_counts:
        for line in digest_lines:
            section = str(line.get("section") or "")
            if section:
                section_counts[section] = section_counts.get(section, 0) + 1
    if not section_counts:
        section_counts = dict(writer_report.get("section_counts") or {})
    ticket_sections = {"Билеты / Ticket Radar", "Крупные концерты вне GM", "Русскоязычные концерты и стендап UK"}
    ticket_items = sum(int(section_counts.get(section) or 0) for section in ticket_sections)
    core_sections = {
        "Свежие новости": 3,
        "Футбол": 1,
        "Выходные в GM": 3,
        "Что важно сегодня": 2,
        "Общественный транспорт сегодня": 1,
    }
    core_counts = {section: int(section_counts.get(section) or 0) for section in core_sections}
    alerts: list[str] = []
    for section, floor in core_sections.items():
        count = core_counts[section]
        if count < floor:
            alerts.append(f"{section}: {count} item(s), emergency floor {floor}")
    core_total = sum(core_counts.values())
    if ticket_items > max(6, core_total):
        alerts.append(f"ticket dominance: {ticket_items} ticket/concert item(s) vs {core_total} core item(s)")
    qc = writer_report.get("quality_counts") or {}
    included = int(qc.get("included_candidates") or 0)
    rendered = int(qc.get("rendered_candidates") or 0)
    if included >= 15 and rendered and rendered / max(1, included) < 0.35:
        alerts.append(f"low writer yield: {rendered}/{included} included candidates rendered")
    source_status = release_report.get("source_status") or {}
    failed_sources = int((source_status.get("counts") or {}).get("failed") or 0)
    if failed_sources >= 3:
        alerts.append(f"source failures: {failed_sources}")
    return {
        "section_counts": section_counts,
        "core_counts": core_counts,
        "ticket_items": ticket_items,
        "core_items": core_total,
        "writer_quality_counts": qc,
        "section_underflow": release_report.get("section_underflow") or [],
        "source_health_counts": (source_status.get("counts") or {}),
        "alerts": alerts,
    }


_DATE_OR_NUMBER_RE = re.compile(
    r"\b(?:\d{1,2}:\d{2}|\d{1,2}/\d{1,2}/20\d{2}|20\d{2}-\d{2}-\d{2}|"
    r"\d+(?:[.,]\d+)?%?|£\d+(?:[.,]\d+)?[mk]?)\b",
    re.IGNORECASE,
)


def _deterministic_action_post_check(
    actions: list[dict[str, Any]],
    digest_lines: list[dict[str, Any]],
    rendered_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    line_count = len(digest_lines)
    seen_targets: set[int] = set()
    fact_blob = " ".join(
        " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "practical_angle", "draft_line"))
        for candidate in rendered_candidates
        if isinstance(candidate, dict)
    )
    fact_tokens = set(_DATE_OR_NUMBER_RE.findall(fact_blob))
    seen_line_text: dict[str, int] = {}
    untranslated_re = re.compile(
        r"\b(?:councillors|greengrocer|baby-and-carer|cask ale|dining room|"
        r"pub menu|slot|disruptions|opening)\b",
        re.IGNORECASE,
    )
    for line in digest_lines:
        text = str(line.get("text") or "").strip()
        norm = re.sub(r"\W+", " ", text.lower()).strip()
        if norm and norm in seen_line_text:
            warnings.append(f"possible duplicate digest line: {seen_line_text[norm]} and {line.get('line_index')}")
        else:
            try:
                seen_line_text[norm] = int(line.get("line_index") or 0)
            except (TypeError, ValueError):
                seen_line_text[norm] = 0
        if untranslated_re.search(text):
            warnings.append(f"possible untranslated residue on line {line.get('line_index')}: {text[:120]}")
    for action in actions:
        try:
            line_index = int(action.get("line_index") or 0)
        except (TypeError, ValueError):
            line_index = 0
        action_name = str(action.get("action") or "").strip().lower()
        if line_index < 1 or line_index > line_count:
            errors.append(f"action target out of range: {line_index}")
            continue
        if line_index in seen_targets:
            warnings.append(f"multiple actions target line {line_index}")
        seen_targets.add(line_index)
        replacement = str(action.get("replacement_text") or "").strip()
        if action_name in {"patch", "replace"}:
            if not replacement.startswith("• "):
                errors.append(f"{action_name} for line {line_index} does not start with bullet")
            replacement_tokens = set(_DATE_OR_NUMBER_RE.findall(replacement))
            unknown_tokens = sorted(token for token in replacement_tokens if token not in fact_tokens)
            if unknown_tokens:
                warnings.append(f"{action_name} for line {line_index} has unchecked number/date token(s): {', '.join(unknown_tokens[:6])}")
    return {
        "errors": errors,
        "warnings": warnings[:20],
        "can_apply_actions": not errors,
        "action_count": len(actions),
    }


def _action_rows(actions: list[dict[str, Any]], critical_errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for action in actions:
        if not isinstance(action, dict):
            continue
        try:
            line_index = int(action.get("line_index") or 0)
        except (TypeError, ValueError):
            line_index = 0
        if line_index <= 0:
            continue
        rows.append(dict(action))
        seen.add(line_index)
    for error in critical_errors:
        if not isinstance(error, dict):
            continue
        try:
            line_index = int(error.get("line_index") or 0)
        except (TypeError, ValueError):
            line_index = 0
        if line_index <= 0 or line_index in seen:
            continue
        suggested = str(error.get("suggested_action") or "repair").strip().lower()
        rows.append(
            {
                "line_index": line_index,
                "section": error.get("section") or "",
                "action": "strip" if suggested == "strip" else "replace",
                "replacement_text": "",
                "reason": error.get("problem") or "critical pre-send issue",
                "risk": error.get("risk") or "",
            }
        )
        seen.add(line_index)
    return rows[:30]


def _candidate_fact_values(candidate: dict[str, Any] | None, *, include_original_line: str = "") -> list[Any]:
    values: list[Any] = []
    if include_original_line:
        values.append(include_original_line)
    if isinstance(candidate, dict):
        values.extend(iter_fact_texts(candidate))
    return values


def _replacement_with_link(replacement: str, original_line: str, candidate: dict[str, Any] | None = None) -> str:
    line = str(replacement or "").strip()
    if not line:
        return ""
    if not line.startswith("• "):
        line = f"• {line.lstrip('• ').strip()}"
    if re.search(r"<a\s+[^>]*href=", line, flags=re.IGNORECASE):
        return line
    original_link = re.search(r'(<a\s+[^>]*href="[^"]+"[^>]*>.*?</a>)', str(original_line or ""), flags=re.IGNORECASE | re.DOTALL)
    if original_link:
        return f"{line} {original_link.group(1)}"
    if isinstance(candidate, dict):
        url = str(candidate.get("source_url") or "").strip()
        if url:
            label = html.escape(str(candidate.get("source_label") or "источник"))
            return f'{line} <a href="{html.escape(url, quote=True)}">{label}</a>'
    return line


def _fact_lock_errors_for_replacement(
    replacement: str,
    *,
    candidate: dict[str, Any] | None,
    original_line: str,
    allow_original_line_facts: bool,
) -> list[str]:
    allowed = _candidate_fact_values(
        candidate,
        include_original_line=original_line if allow_original_line_facts else "",
    )
    if not allowed:
        allowed = [original_line]
    return unsupported_fact_tokens(replacement, allowed)


def _deterministic_rewrite_from_candidate(
    candidate: dict[str, Any] | None,
    original_line: str,
    stats: dict[str, Any],
) -> str:
    if not isinstance(candidate, dict):
        return ""
    stats["enrich_attempted"] = int(stats.get("enrich_attempted") or 0) + 1
    c_work = dict(candidate)
    refetch_stats = stats.setdefault(
        "refetch",
        {"attempted": 0, "improved": 0, "failed": 0, "empty_or_not_better": 0, "skipped": 0},
    )
    if isinstance(refetch_stats, dict):
        try:
            from news_digest.pipeline.editor import _candidate_full_evidence_text  # noqa: PLC0415

            evidence_text, evidence_source = _candidate_full_evidence_text(c_work, refetch_stats)
        except Exception:  # noqa: BLE001
            evidence_text, evidence_source = "", ""
        if evidence_text:
            c_work["evidence_text"] = evidence_text
            packet = c_work.get("evidence_packet") if isinstance(c_work.get("evidence_packet"), dict) else {}
            packet = dict(packet)
            packet["evidence_text"] = evidence_text
            packet["evidence_source"] = evidence_source
            c_work["evidence_packet"] = packet
            stats["enrich_improved"] = int(stats.get("enrich_improved") or 0) + 1
    try:
        from news_digest.pipeline.editor import _line_needs_russian_editor, _polish_russian_line_rules  # noqa: PLC0415
        from news_digest.pipeline.writer import _final_replacement_line  # noqa: PLC0415

        line = _final_replacement_line(c_work)
        if line and not line.startswith("• "):
            line = f"• {line}"
        if line:
            line, _ = _polish_russian_line_rules(line)
        line = _replacement_with_link(line, original_line, c_work)
        if not line or _line_needs_russian_editor(line):
            stats["deterministic_rewrite_rejected"] = int(stats.get("deterministic_rewrite_rejected") or 0) + 1
            return ""
    except Exception:  # noqa: BLE001
        stats["deterministic_rewrite_failed"] = int(stats.get("deterministic_rewrite_failed") or 0) + 1
        return ""
    unsupported = _fact_lock_errors_for_replacement(
        line,
        candidate=c_work,
        original_line=original_line,
        allow_original_line_facts=False,
    )
    if unsupported:
        stats["deterministic_fact_lock_rejected"] = int(stats.get("deterministic_fact_lock_rejected") or 0) + 1
        return ""
    stats["deterministic_rewrite_built"] = int(stats.get("deterministic_rewrite_built") or 0) + 1
    return line


def _reserve_replacement_line(
    section_name: str,
    candidates: list[dict[str, Any]],
    rendered_urls: set[str],
    rendered_story_keys: set[str],
    stats: dict[str, Any],
) -> str:
    try:
        from news_digest.pipeline.editor import _same_section_reserve_line  # noqa: PLC0415

        line = _same_section_reserve_line(section_name, candidates, rendered_urls, rendered_story_keys, stats)
    except Exception:  # noqa: BLE001
        line = ""
    if line:
        stats["reserve_replacement_used"] = int(stats.get("reserve_replacement_used") or 0) + 1
    return line


def _html_lines_for_section(html_lines: list[str], section_name: str) -> list[int]:
    current = ""
    indexes: list[int] = []
    for idx, raw_line in enumerate(html_lines):
        line = raw_line.strip()
        header_match = re.fullmatch(r"<b>(.*?)</b>", line)
        if header_match and not _strip_tags(line).startswith("Greater Manchester Brief"):
            current = _strip_tags(header_match.group(1))
            continue
        if current == section_name and _strip_tags(line).startswith("•"):
            indexes.append(idx)
    return indexes


def _ensure_transport_fallback_if_empty(html_lines: list[str], touched_sections: set[str], stats: dict[str, Any]) -> None:
    section = "Общественный транспорт сегодня"
    if section not in touched_sections:
        return
    if _html_lines_for_section(html_lines, section):
        return
    try:
        from news_digest.pipeline.editor import _transport_status_fallback_line  # noqa: PLC0415

        fallback = _transport_status_fallback_line()
    except Exception:  # noqa: BLE001
        fallback = (
            '• Транспорт: конкретных подтверждённых сбоев в выпуск не попало. '
            'Перед поездкой проверьте страницу статуса TfGM. '
            '<a href="https://tfgm.com/travel-updates">TfGM</a>'
        )
    for idx, raw_line in enumerate(html_lines):
        if re.fullmatch(r"<b>Общественный транспорт сегодня</b>", raw_line.strip()):
            html_lines.insert(idx + 1, fallback)
            stats["transport_status_fallback_inserted"] = int(stats.get("transport_status_fallback_inserted") or 0) + 1
            return


def _apply_repair_executor(
    *,
    project_root: Path,
    digest_html: str,
    actions: list[dict[str, Any]],
    critical_errors: list[dict[str, Any]],
    deterministic_post_check: dict[str, Any],
    dry_run: bool,
) -> tuple[str, dict[str, Any]]:
    rows = _action_rows(actions, critical_errors)
    report: dict[str, Any] = {
        "enabled": True,
        "dry_run": dry_run,
        "fact_lock_version": FACT_LOCK_VERSION,
        "requested": len(rows),
        "attempted": 0,
        "applied": 0,
        "model_patch_applied": 0,
        "deterministic_rewrite_used": 0,
        "reserve_replacement_used": 0,
        "stripped": 0,
        "unresolved": 0,
        "fact_lock_rejected": 0,
        "enrich_attempted": 0,
        "post_check_errors": deterministic_post_check.get("errors") or [],
        "actions": [],
    }
    if not rows:
        report["status"] = "skipped_no_actions"
        return digest_html, report

    state_dir = project_root / "data" / "state"
    candidates_payload = read_json(state_dir / "candidates.json", {"candidates": []})
    candidates = [c for c in candidates_payload.get("candidates") or [] if isinstance(c, dict)]
    candidates_by_key = _candidate_index(candidates)
    slots = _digest_line_slots_from_html(digest_html)
    slot_by_index = {int(slot.get("line_index") or 0): slot for slot in slots}
    html_lines = digest_html.splitlines()
    rendered_urls = {
        _line_url_identity(str(slot.get("html") or ""))
        for slot in slots
        if str(slot.get("html") or "").strip()
    }
    rendered_urls.discard("")
    try:
        from news_digest.pipeline.editor import _line_needs_russian_editor, _line_story_identity_key, _line_preserves_links  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        _line_needs_russian_editor = lambda line: False  # type: ignore[assignment]
        _line_preserves_links = lambda original, fixed: True  # type: ignore[assignment]

        def _line_story_identity_key(line: str, candidates_by_key: dict[str, dict[str, Any]]) -> str:  # type: ignore[no-redef]
            return ""

    rendered_story_keys = {
        _line_story_identity_key(str(slot.get("html") or ""), candidates_by_key)
        for slot in slots
        if str(slot.get("html") or "").strip()
    }
    rendered_story_keys.discard("")
    touched_sections: set[str] = set()

    for row in rows:
        try:
            line_index = int(row.get("line_index") or 0)
        except (TypeError, ValueError):
            line_index = 0
        slot = slot_by_index.get(line_index)
        action_record = {
            "line_index": line_index,
            "section": row.get("section") or (slot or {}).get("section") or "",
            "requested_action": row.get("action") or "",
            "reason": row.get("reason") or "",
            "outcome": "",
        }
        if not slot:
            action_record["outcome"] = "skipped_missing_line"
            report["actions"].append(action_record)
            report["unresolved"] = int(report.get("unresolved") or 0) + 1
            continue
        raw_index = int(slot.get("raw_index") or 0)
        original = str(slot.get("html") or "")
        section_name = str(slot.get("section") or row.get("section") or "")
        touched_sections.add(section_name)
        action_name = str(row.get("action") or "").strip().lower()
        if action_name == "keep":
            action_record["outcome"] = "kept"
            report["actions"].append(action_record)
            continue
        report["attempted"] = int(report.get("attempted") or 0) + 1
        candidate = candidates_by_key.get(_line_url_identity(original))
        replacement = ""
        model_replacement = str(row.get("replacement_text") or "").strip()
        if action_name in {"patch", "replace"} and model_replacement:
            model_line = _replacement_with_link(model_replacement, original, candidate)
            unsupported = _fact_lock_errors_for_replacement(
                model_line,
                candidate=candidate,
                original_line=original,
                allow_original_line_facts=False,
            )
            if unsupported:
                action_record["model_replacement_rejected"] = f"fact_lock: {', '.join(unsupported[:6])}"
                report["fact_lock_rejected"] = int(report.get("fact_lock_rejected") or 0) + 1
            elif not _line_preserves_links(original, model_line):
                action_record["model_replacement_rejected"] = "link_mismatch"
            elif _line_needs_russian_editor(model_line):
                action_record["model_replacement_rejected"] = "still_needs_editor"
            else:
                replacement = model_line
                action_record["outcome"] = "model_patch_applied"
                report["model_patch_applied"] = int(report.get("model_patch_applied") or 0) + 1

        if not replacement and action_name != "strip":
            replacement = _deterministic_rewrite_from_candidate(candidate, original, report)
            if replacement:
                action_record["outcome"] = "deterministic_rewrite"

        if not replacement:
            reserve_stats = report.setdefault("reserve_stats", {"enriched_rewrite_attempts": 0, "enriched_rewrite_used": 0})
            replacement = _reserve_replacement_line(
                section_name,
                candidates,
                rendered_urls,
                rendered_story_keys,
                reserve_stats if isinstance(reserve_stats, dict) else {},
            )
            if replacement:
                action_record["outcome"] = "reserve_replacement"

        if replacement:
            if not dry_run:
                html_lines[raw_index] = replacement
            report["applied"] = int(report.get("applied") or 0) + 1
            if action_record["outcome"] == "deterministic_rewrite":
                report["deterministic_rewrite_used"] = int(report.get("deterministic_rewrite_used") or 0) + 1
            elif action_record["outcome"] == "reserve_replacement":
                report["reserve_replacement_used"] = int(report.get("reserve_replacement_used") or 0) + 1
            report["actions"].append(action_record)
            continue

        if not dry_run:
            html_lines[raw_index] = ""
        action_record["outcome"] = "stripped_honest_shortfall"
        report["stripped"] = int(report.get("stripped") or 0) + 1
        report["applied"] = int(report.get("applied") or 0) + 1
        report["actions"].append(action_record)

    if not dry_run:
        _ensure_transport_fallback_if_empty(html_lines, touched_sections, report)
    unresolved = int(report.get("unresolved") or 0)
    report["status"] = "applied" if int(report.get("applied") or 0) and not unresolved else "partial" if int(report.get("applied") or 0) else "unresolved"
    return "\n".join(html_lines).strip(), report


def _parse_reply(raw: str) -> dict[str, Any] | None:
    text = (raw or "").strip()
    if not text:
        return None
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    if match:
        text = match.group(1)
    else:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if match:
            text = match.group(0)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _normalise_result(parsed: dict[str, Any], *, fallback_reason: str = "") -> tuple[str, bool, str, float | None, list[dict[str, Any]], list[dict[str, Any]], list[str], str]:
    decision = str(parsed.get("decision") or "").strip().lower()
    if decision not in ALLOWED_TO_SEND | BLOCKING_DECISIONS:
        decision = "repair_required"
        reason = fallback_reason or "judge returned an unknown decision"
    else:
        reason = ""
    try:
        confidence = float(parsed.get("confidence"))
    except (TypeError, ValueError):
        confidence = None
    if confidence is not None:
        confidence = max(0.0, min(1.0, confidence))
    raw_errors = parsed.get("critical_errors") if isinstance(parsed.get("critical_errors"), list) else []
    critical_errors = [err for err in raw_errors if isinstance(err, dict)][:12]
    raw_actions = parsed.get("actions") if isinstance(parsed.get("actions"), list) else []
    actions: list[dict[str, Any]] = []
    for action in raw_actions:
        if not isinstance(action, dict):
            continue
        action_name = str(action.get("action") or "").strip().lower()
        if action_name not in {"keep", "patch", "replace", "strip"}:
            continue
        cleaned = dict(action)
        cleaned["action"] = action_name
        cleaned["replacement_text"] = str(cleaned.get("replacement_text") or "")[:900]
        cleaned["reason"] = str(cleaned.get("reason") or "")[:260]
        actions.append(cleaned)
        if len(actions) >= 20:
            break
    raw_warnings = parsed.get("warnings") if isinstance(parsed.get("warnings"), list) else []
    warnings = [str(item)[:260] for item in raw_warnings if str(item).strip()][:12]
    notes = str(parsed.get("notes") or "")[:320]
    can_send = decision in ALLOWED_TO_SEND
    if not reason:
        reason = "quality judge passed" if can_send else "quality judge found blocking defects"
    return decision, can_send, reason, confidence, critical_errors, actions, warnings, notes


def _pipeline_run_id(project_root: Path) -> str:
    state_dir = project_root / "data" / "state"
    for filename in ("release_report.json", "llm_rewrite_report.json", "writer_report.json"):
        payload = read_json(state_dir / filename, {})
        run_id = pipeline_run_id_from(payload)
        if run_id:
            return run_id
    return ""


def _write_report(project_root: Path, result: PreSendQualityResult) -> Path:
    path = project_root / "data" / "state" / REPORT_NAME
    write_json(path, asdict(result))
    release_path = project_root / "data" / "state" / "release_report.json"
    release_report = read_json(release_path, {})
    if isinstance(release_report, dict) and release_report:
        release_report["pre_send_quality_judge"] = asdict(result)
        repair = result.repair_executor if isinstance(result.repair_executor, dict) else {}
        repair_applied = int(repair.get("applied") or 0) if repair else 0
        repair_unresolved = int(repair.get("unresolved") or 0) if repair else 0
        if repair:
            release_report["pre_send_repair_executor"] = repair
        if result.decision == "warn" or repair_applied or repair_unresolved:
            if release_report.get("release_decision") == "pass":
                release_report["release_decision"] = "ship_degraded"
            warnings = release_report.setdefault("warnings", [])
            if isinstance(warnings, list):
                warnings.append(
                    "Pre-send repair executor: "
                    f"applied={repair_applied}, stripped={repair.get('stripped', 0) if repair else 0}, "
                    f"unresolved={repair_unresolved}."
                )
        try:
            write_json(release_path, release_report)
        except OSError as exc:
            logger.warning("pre-send quality judge: could not stamp release_report: %s", exc)
    return path


def evaluate_pre_send_quality(
    project_root: Path,
    *,
    api_key: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    start = time.monotonic()
    state_dir = project_root / "data" / "state"
    digest_path = project_root / "data" / "outgoing" / "current_digest.html"
    run_date = today_london()
    pipeline_run_id = _pipeline_run_id(project_root)

    if not digest_path.exists():
        result = PreSendQualityResult(
            status="failed",
            decision="block",
            can_send=False,
            reason="current_digest.html missing",
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            duration_seconds=round(time.monotonic() - start, 3),
        )
        _write_report(project_root, result)
        return asdict(result)

    digest_html = digest_path.read_text(encoding="utf-8")
    sha = digest_hash(digest_html)
    digest_lines = digest_lines_from_html(digest_html)
    rendered_candidates = _rendered_candidates(project_root)
    product_completeness = _product_completeness_context(project_root, digest_lines)

    if dry_run:
        result = PreSendQualityResult(
            status="dry_run",
            decision="warn",
            can_send=True,
            reason=f"dry run: {len(digest_lines)} digest lines and {len(rendered_candidates)} rendered candidates ready for judge",
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            warnings=[],
            critical_errors=[],
            product_completeness=product_completeness,
        )
        _write_report(project_root, result)
        return asdict(result)

    route = resolve_model_route("pre_send_quality")
    if not route:
        result = PreSendQualityResult(
            status="failed",
            decision="block",
            can_send=False,
            reason="pre_send_quality model route is not configured",
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            critical_errors=[],
            warnings=[],
            product_completeness=product_completeness,
        )
        _write_report(project_root, result)
        return asdict(result)
    step = route[0]
    key = api_key if api_key is not None else step.api_key
    if not key:
        result = PreSendQualityResult(
            status="failed",
            decision="block",
            can_send=False,
            reason=f"{step.api_key_env} is not set for required pre-send quality judge",
            model=step.model,
            provider=step.provider,
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            critical_errors=[],
            warnings=[],
            product_completeness=product_completeness,
        )
        _write_report(project_root, result)
        return asdict(result)

    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        result = PreSendQualityResult(
            status="failed",
            decision="block",
            can_send=False,
            reason="openai package is not installed",
            model=step.model,
            provider=step.provider,
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            critical_errors=[],
            warnings=[],
            product_completeness=product_completeness,
        )
        _write_report(project_root, result)
        return asdict(result)

    payload = {
        "run_date_london": run_date,
        "pipeline_run_id": pipeline_run_id,
        "digest_sha256": sha,
        "digest_lines": digest_lines,
        "rendered_candidates": rendered_candidates,
        "product_completeness": product_completeness,
    }
    user_content = json.dumps(payload, ensure_ascii=False)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
    max_tokens = 1200
    try:
        client = OpenAI(
            api_key=key,
            base_url=step.base_url,
            timeout=step.timeout_seconds or 75,
            max_retries=sdk_retries_for_route(provider=step.provider, model=step.model, base_url=step.base_url),
        )
        response = client.chat.completions.create(
            model=step.model,
            messages=messages,
            temperature=0.0,
            max_tokens=max_tokens,
        )
        raw_text = response.choices[0].message.content or ""
        try:
            from news_digest.pipeline.cost_tracker import dump_stage, record_call_from_response  # noqa: PLC0415

            record_call_from_response(
                response=response,
                stage="pre_send_quality_judge",
                provider=step.provider,
                model=step.model,
                prompt_name="pre_send_quality_judge",
                messages=messages,
                max_tokens=max_tokens,
            )
            dump_stage(state_dir, "pre_send_quality_judge")
        except Exception as exc:  # noqa: BLE001
            logger.warning("pre-send quality judge: cost tracking failed: %s", exc)
    except Exception as exc:  # noqa: BLE001
        result = PreSendQualityResult(
            status="failed",
            decision="block",
            can_send=False,
            reason=f"pre-send quality judge LLM call failed: {exc}",
            model=step.model,
            provider=step.provider,
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            critical_errors=[],
            warnings=[],
            product_completeness=product_completeness,
        )
        _write_report(project_root, result)
        return asdict(result)

    parsed = _parse_reply(raw_text)
    if parsed is None:
        result = PreSendQualityResult(
            status="failed",
            decision="block",
            can_send=False,
            reason="pre-send quality judge returned no parseable JSON",
            model=step.model,
            provider=step.provider,
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            critical_errors=[],
            warnings=[],
            product_completeness=product_completeness,
        )
        _write_report(project_root, result)
        return asdict(result)

    decision, can_send, reason, confidence, critical_errors, actions, warnings, notes = _normalise_result(parsed)
    deterministic_post_check = _deterministic_action_post_check(actions, digest_lines, rendered_candidates)
    repair_executor: dict[str, Any] | None = None
    final_sha = sha
    if actions or critical_errors:
        repaired_html, repair_executor = _apply_repair_executor(
            project_root=project_root,
            digest_html=digest_html,
            actions=actions,
            critical_errors=critical_errors,
            deterministic_post_check=deterministic_post_check,
            dry_run=dry_run,
        )
        if repaired_html != digest_html and not dry_run:
            digest_path.write_text(repaired_html + "\n", encoding="utf-8")
            digest_html = repaired_html + "\n"
            final_sha = digest_hash(digest_html)
            digest_lines = digest_lines_from_html(digest_html)
            product_completeness = _product_completeness_context(project_root, digest_lines)
        if repair_executor and int(repair_executor.get("applied") or 0):
            decision = "warn"
            can_send = True
            reason = (
                "pre-send repair executor applied "
                f"{repair_executor.get('applied')} repair(s); issue ships with honest degradation report"
            )
        elif decision in BLOCKING_DECISIONS:
            # Content defects should not silently cancel the daily send. If the
            # executor could not act, surface it as degraded output instead.
            decision = "warn"
            can_send = True
            reason = "pre-send judge found issues, but no executable repair was available; shipping degraded with report"
    result = PreSendQualityResult(
        status="ok",
        decision=decision,
        can_send=can_send,
        reason=reason,
        model=step.model,
        provider=step.provider,
        run_date_london=run_date,
        pipeline_run_id=pipeline_run_id,
        digest_sha256=final_sha,
        duration_seconds=round(time.monotonic() - start, 3),
        confidence=confidence,
        critical_errors=critical_errors,
        actions=actions,
        warnings=warnings,
        product_completeness=product_completeness,
        deterministic_post_check=deterministic_post_check,
        repair_executor=repair_executor,
        notes=notes,
        raw=parsed,
    )
    _write_report(project_root, result)
    return asdict(result)


def quality_gate_error_for_digest(project_root: Path, digest_path: Path) -> str:
    """Return a blocking reason if current_digest.html lacks a fresh pass."""
    current_digest = (project_root / "data" / "outgoing" / "current_digest.html").resolve()
    try:
        resolved = digest_path.resolve()
    except OSError:
        resolved = digest_path
    if resolved != current_digest:
        return ""
    if not digest_path.exists():
        return "current_digest.html missing"
    html = digest_path.read_text(encoding="utf-8")
    sha = digest_hash(html)
    report = read_json(project_root / "data" / "state" / REPORT_NAME, {})
    if not report:
        return "pre-send quality judge has not run for current_digest.html"
    if str(report.get("digest_sha256") or "") != sha:
        return "pre-send quality judge report is stale for current_digest.html"
    if report.get("can_send") is not True or str(report.get("decision") or "") not in ALLOWED_TO_SEND:
        return f"pre-send quality judge blocked send: {report.get('decision') or report.get('reason') or 'unknown'}"
    today = today_london()
    if str(report.get("run_date_london") or "") != today:
        return f"pre-send quality judge report is for {report.get('run_date_london')}, not {today}"
    return ""
