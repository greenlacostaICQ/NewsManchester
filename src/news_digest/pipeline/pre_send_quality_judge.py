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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from news_digest.pipeline.common import (
    canonical_url_identity,
    extract_sections,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.fact_completeness import (
    FACT_COMPLETENESS_VERSION,
    line_satisfies_concept,
    translation_completeness_review,
)
from news_digest.pipeline.fact_lock import FACT_LOCK_VERSION, iter_fact_texts, unsupported_fact_tokens
from news_digest.pipeline.model_routing import resolve_model_route, sdk_retries_for_route


logger = logging.getLogger(__name__)


PROMPT_VERSION = "v4"
REPORT_NAME = "pre_send_quality_report.json"
ALLOWED_TO_SEND = {"pass", "warn"}
BLOCKING_DECISIONS = {"repair_required", "block"}
PRE_SEND_JUDGE_CHUNK_LINES = 12
PRE_SEND_JUDGE_MAX_WORKERS = 3
PRE_SEND_JUDGE_MAX_TPM = 27000.0
PRE_SEND_JUDGE_MAP_MAX_TOKENS = 900
PRE_SEND_JUDGE_REDUCE_MAX_TOKENS = 900
_JUDGE_TOKEN_LIMITER = None


SYSTEM_PROMPT = """Ты старший редактор и fact-check судья русскоязычного утреннего дайджеста Greater Manchester.

Твоя задача — найти строки, которые нужно исправить перед отправкой уже собранного выпуска
читателям в Telegram. Ошибка отдельной строки никогда не отменяет весь выпуск: для неё нужно
дать patch/replace/strip, а технический контроллер применит исправление, запасного или снимет
только эту строку.

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
- "block": зарезервировано для технически отсутствующего/нечитаемого выпуска; не используй
  его для ошибок отдельных строк или продуктовой полноты.

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


MAP_SYSTEM_PROMPT = SYSTEM_PROMPT + """

Режим MAP: тебе дали один фрагмент финального выпуска. Проверяй только строки
этого фрагмента. Не делай выводов о балансе всего выпуска — этим занимается
reduce-судья. Если строка безопасна, не добавляй action. line_index — глобальный
номер строки в выпуске, используй его без изменения.
"""


REDUCE_SYSTEM_PROMPT = SYSTEM_PROMPT + """

Режим REDUCE: тебе дали краткий контур всего выпуска и результаты chunk-судей.
Проверяй только глобальные проблемы: баланс секций, дубли между чанками,
доминирование optional-блоков, missing lead/must_show, и failed chunks. Не
переписывай отдельные строки, если для них не хватает facts; line actions давай
только для очевидных глобальных дублей или форматных дефектов из digest_outline.
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
    translation_completeness: dict[str, Any] | None = None
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


def _compact_text(value: object, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _compact_event(event: object, hint: object) -> dict[str, Any]:
    event_dict = event if isinstance(event, dict) else {}
    hint_dict = hint if isinstance(hint, dict) else {}
    out: dict[str, Any] = {}
    for key in (
        "event_name",
        "date_start",
        "date",
        "date_text",
        "venue",
        "borough",
        "price",
        "booking_url",
    ):
        value = event_dict.get(key) or hint_dict.get(key)
        if str(value or "").strip():
            out[key] = str(value)[:220]
    return out


def _compact_candidate_for_judge(candidate: dict[str, Any]) -> dict[str, Any]:
    """Small, line-checkable candidate facts for judge map chunks.

    The old judge payload copied summary + lead + 700 chars of evidence +
    700 chars of draft for every rendered card, which made a 57-item issue
    exceed a 30k TPM tier. Keep only identifiers plus the source facts a judge
    needs to verify the visible line.
    """
    event = _compact_event(candidate.get("event"), candidate.get("structured_event_hint"))
    fact_bits: list[str] = []
    if event:
        fact_bits.append(
            "; ".join(f"{key}={value}" for key, value in event.items() if str(value).strip())
        )
    for field in ("evidence_text", "practical_angle"):
        value = _compact_text(candidate.get(field), 320 if field == "evidence_text" else 180)
        if value:
            fact_bits.append(value)
    return {
        "fingerprint": str(candidate.get("fingerprint") or "").strip(),
        "title": _compact_text(candidate.get("title"), 220),
        "source_label": _compact_text(candidate.get("source_label"), 80),
        "source_url": str(candidate.get("source_url") or "")[:260],
        "primary_block": str(candidate.get("primary_block") or ""),
        "category": str(candidate.get("category") or ""),
        "compact_facts": _compact_text(" | ".join(fact_bits), 520),
        # Reverse fact-completeness must read the actual story claim, not the
        # first navigation/header words scraped into evidence_text. The latter
        # produced a real false positive when an unrelated Prolific North nav
        # item contained "grooming" beside a parking-platform story.
        "source_claim": _compact_text(candidate.get("summary") or candidate.get("lead"), 360),
        "event": event,
        "is_lead": bool(candidate.get("is_lead")),
        "protected_lane": candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {},
    }


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
        summary.append(_compact_candidate_for_judge(candidate))
    # Match digest_lines_from_html: the judge must see metadata for the WHOLE
    # issue, not the first 60 rendered candidates — otherwise tail items (deep in
    # the ticket list) have no metadata for the model to cross-check. 250 covers
    # any realistic issue with headroom.
    return summary[:250]


def _rendered_candidates_by_url(rendered_candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for candidate in rendered_candidates:
        url_key = canonical_url_identity(str(candidate.get("source_url") or ""))
        if url_key:
            out.setdefault(url_key, candidate)
    return out


def _line_payload_for_judge(slot: dict[str, Any], rendered_by_url: dict[str, dict[str, Any]]) -> dict[str, Any]:
    html_line = str(slot.get("html") or "")
    candidate = rendered_by_url.get(_line_url_identity(html_line)) or {}
    payload = {
        "line_index": int(slot.get("line_index") or 0),
        "section": str(slot.get("section") or ""),
        "text": _compact_text(slot.get("text"), 650),
    }
    if candidate:
        payload["candidate"] = candidate
    return payload


def _chunk_digest_slots(
    slots: list[dict[str, Any]],
    rendered_by_url: dict[str, dict[str, Any]],
    *,
    max_lines: int = PRE_SEND_JUDGE_CHUNK_LINES,
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    by_section: list[tuple[str, list[dict[str, Any]]]] = []
    current_section = ""
    current: list[dict[str, Any]] = []
    for slot in slots:
        section = str(slot.get("section") or "")
        if current and section != current_section:
            by_section.append((current_section, current))
            current = []
        current_section = section
        current.append(slot)
    if current:
        by_section.append((current_section, current))

    chunk_no = 1
    for section, rows in by_section:
        for start in range(0, len(rows), max_lines):
            part = rows[start:start + max_lines]
            lines = [_line_payload_for_judge(slot, rendered_by_url) for slot in part]
            line_indexes = [int(line.get("line_index") or 0) for line in lines]
            chunks.append(
                {
                    "chunk_id": f"chunk-{chunk_no:02d}",
                    "sections": sorted({section} if section else {str(line.get("section") or "") for line in lines}),
                    "line_range": [min(line_indexes), max(line_indexes)] if line_indexes else [],
                    "digest_lines": lines,
                }
            )
            chunk_no += 1
    return chunks


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
        "Что важно сегодня": 2,
        "Общественный транспорт сегодня": 1,
    }
    if now_london().weekday() >= 3:
        core_sections["Выходные в GM"] = 3
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
        " ".join(str(candidate.get(field) or "") for field in ("title", "compact_facts", "source_label", "source_url"))
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


def _deterministic_html_scan(slots: list[dict[str, Any]]) -> dict[str, Any]:
    """0040: model-free safety net. When the judge LLM cannot produce a verdict
    (rate limit, timeout, unparseable, all chunks failed) the failure report was
    previously a blind zero — the issue shipped with no final check at all
    (2026-07-01: 429, empty critical_errors). This scans the shipped HTML for
    mechanical defects so the report carries real findings even with no model.

    Report-only: it does not mutate the HTML (repair stays with the executor).
    Detects broken hrefs (raw HTML/whitespace — the CONEXEN class), empty
    generic call-to-action tails, and Latin/Cyrillic glued words per line.
    """
    from news_digest.pipeline.editorial_contracts import classify_prose_defects  # noqa: PLC0415
    from news_digest.pipeline.writer import _mixed_latin_cyrillic_words  # noqa: PLC0415

    findings: list[dict[str, Any]] = []
    for slot in slots:
        idx = int(slot.get("line_index") or 0)
        html_line = str(slot.get("html") or "")
        text = str(slot.get("text") or "")
        for href in re.findall(r'href="([^"]*)"', html_line):
            if re.search(r"[<>\s]|&lt;|&gt;", href):
                findings.append({"line_index": idx, "type": "broken_href", "detail": href[:80]})
        for defect in classify_prose_defects(text):
            findings.append({
                "line_index": idx,
                "type": str(defect.get("code") or "prose_defect"),
                "severity": str(defect.get("severity") or "repair"),
                "detail": str(defect.get("marker") or "")[:80],
            })
        mixed = _mixed_latin_cyrillic_words(text)
        if mixed:
            findings.append({"line_index": idx, "type": "mixed_script", "detail": str(mixed[0])[:80]})
    return {
        "status": "ran",
        "mode": "model_unavailable_fallback",
        "defect_count": len(findings),
        "findings": findings[:60],
    }


def _completeness_source_blob(candidate: dict[str, Any]) -> str:
    """English source headline blob for the reverse fact-lock check.

    Kept to title + compact facts (which already carry the event fields and the
    first ~320 chars of evidence): a grave severity concept lives in the
    headline of a crime story, so this is enough to decide presence without
    re-reading the full article.
    """
    source_claim = str(
        candidate.get("source_claim")
        or candidate.get("summary")
        or candidate.get("lead")
        or ""
    ).strip()
    if not source_claim:
        source_claim = str(candidate.get("compact_facts") or "").strip()
    return " ".join(
        part for part in (str(candidate.get("title") or "").strip(), source_claim) if part
    ).strip()


def _deterministic_completeness_scan(
    slots: list[dict[str, Any]],
    rendered_by_url: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """0043: reverse fact-lock — flag grave source facts dropped from the line.

    Deterministic omission net for sensitive / hard-news lines. A grave severity
    concept present in the English source but absent (in any Russian rendering)
    from the shipped line is a critical omission → emitted as a repair-worthy
    critical error so the executor rewrites from the candidate or pulls the line.
    Dropped number/date facts are warning-only (the digest may compress).
    """
    critical_errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    critical_omissions: list[dict[str, Any]] = []
    noncritical_omissions: list[dict[str, Any]] = []
    checked = 0
    applicable = 0
    unmatched = 0
    for slot in slots:
        html_line = str(slot.get("html") or "")
        candidate = rendered_by_url.get(_line_url_identity(html_line))
        if not candidate:
            unmatched += 1
            continue
        checked += 1
        review = translation_completeness_review(
            _completeness_source_blob(candidate), str(slot.get("text") or "")
        )
        if not review.get("applies"):
            continue
        applicable += 1
        idx = int(slot.get("line_index") or 0)
        section = str(slot.get("section") or "")
        source_url_key = canonical_url_identity(str(candidate.get("source_url") or ""))
        for miss in review.get("missing_critical", []):
            concept = str(miss.get("concept") or "")
            critical_omissions.append({"line_index": idx, "source_url_key": source_url_key, **miss})
            critical_errors.append(
                {
                    "line_index": idx,
                    "section": section,
                    "risk": "translation",
                    "problem": (
                        f"critical source fact dropped in translation: "
                        f"'{miss.get('source_hit')}' ({concept}/{miss.get('obligation')}) "
                        f"has no Russian rendering in the shipped line"
                    ),
                    "suggested_action": "repair",
                    "completeness_concept": concept,
                }
            )
        dropped = review.get("missing_noncritical") or []
        if dropped:
            noncritical_omissions.append({"line_index": idx, "dropped": dropped[:6]})
            warnings.append(
                f"line {idx} drops source number/date fact(s): {', '.join(dropped[:6])}"
            )
    return {
        "version": FACT_COMPLETENESS_VERSION,
        "checked_lines": checked,
        "matched_lines": checked,
        "applicable_lines": applicable,
        "unmatched_lines": unmatched,
        "critical_omission_count": len(critical_omissions),
        "critical_omissions": critical_omissions[:40],
        "noncritical_omissions": noncritical_omissions[:40],
        "critical_errors": critical_errors,
        "warnings": warnings[:20],
        "recovered": 0,
        "pulled_for_rework": 0,
        "still_missing": len(critical_omissions),
    }


def _recount_completeness_recovery(
    completeness: dict[str, Any],
    digest_html_after: str,
) -> None:
    """After repair, count how many flagged omissions now render the concept."""
    omissions = completeness.get("critical_omissions") or []
    if not omissions:
        return
    slots = _digest_line_slots_from_html(digest_html_after)
    text_by_index = {int(s.get("line_index") or 0): str(s.get("text") or "") for s in slots}
    url_by_index = {
        int(s.get("line_index") or 0): _line_url_identity(str(s.get("html") or ""))
        for s in slots
    }
    recovered = 0
    pulled_for_rework = 0
    still_missing = 0
    for miss in omissions:
        idx = int(miss.get("line_index") or 0)
        concept = str(miss.get("concept") or "")
        text = text_by_index.get(idx, "")
        original_url = str(miss.get("source_url_key") or "")
        current_url = url_by_index.get(idx, "")
        # A stripped/removed line renders no text: the fact is not recovered but
        # the neutered line no longer ships — count it as pulled, not recovered.
        if text and original_url and current_url and current_url != original_url:
            pulled_for_rework += 1
        elif text and line_satisfies_concept(concept, text):
            recovered += 1
        else:
            if text:
                still_missing += 1
            else:
                pulled_for_rework += 1
    completeness["recovered"] = recovered
    completeness["pulled_for_rework"] = pulled_for_rework
    completeness["still_missing"] = still_missing


def _action_rows(actions: list[dict[str, Any]], critical_errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_index: dict[int, dict[str, Any]] = {}
    for action in actions:
        if not isinstance(action, dict):
            continue
        try:
            line_index = int(action.get("line_index") or 0)
        except (TypeError, ValueError):
            line_index = 0
        if line_index <= 0:
            continue
        copied = dict(action)
        rows.append(copied)
        by_index[line_index] = copied
    for error in critical_errors:
        if not isinstance(error, dict):
            continue
        try:
            line_index = int(error.get("line_index") or 0)
        except (TypeError, ValueError):
            line_index = 0
        if line_index <= 0:
            continue
        if line_index in by_index:
            target = by_index[line_index]
            target["critical_error"] = True
            target.setdefault("critical_problem", error.get("problem") or "critical pre-send issue")
            if not target.get("risk"):
                target["risk"] = error.get("risk") or ""
            if error.get("completeness_concept"):
                target["completeness_concept"] = error.get("completeness_concept")
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
                "critical_error": True,
                "completeness_concept": error.get("completeness_concept") or "",
            }
        )
        by_index[line_index] = rows[-1]
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
        source_label = re.sub(r"<[^>]+>", " ", original_link.group(1))
        source_label = re.sub(r"\s+", " ", html.unescape(source_label)).strip()
        if source_label:
            line = re.sub(
                rf"(?:\s+|\.\s*){re.escape(source_label)}\.?\s*$",
                "",
                line,
                flags=re.IGNORECASE,
            ).rstrip()
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


def _enrich_candidate_for_repair(
    candidate: dict[str, Any] | None,
    stats: dict[str, Any],
) -> dict[str, Any] | None:
    if not isinstance(candidate, dict):
        return None
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
    return c_work


def _deterministic_rewrite_from_candidate(
    candidate: dict[str, Any] | None,
    original_line: str,
    stats: dict[str, Any],
) -> str:
    if not isinstance(candidate, dict):
        return ""
    c_work = dict(candidate)
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


_LEAD_SECTION_TITLE = "Главная история дня"
# Blocks whose lead is a weak "main story of the day" — a transport disruption or
# a weather line should never be promoted into «Главная история дня».
_WEAK_LEAD_RE = re.compile(
    r"\b(?:delay|delays|cancel|cancelled|disrupt|diversion|replacement\s+bus|"
    r"roadworks?|road\s+closed|weather|forecast|temperature|°)\b",
    re.IGNORECASE,
)


def _strip_empty_section_headings(html_text: str) -> str:
    """Drop a section heading that has no content beneath it before shipping.

    The repair executor can strip a line as an honest shortfall (e.g. an
    unsupported lead), and some sections collect nothing on a quiet day. The
    shortfall stays in the release report, but the reader-facing HTML must not
    carry a bare «<b>Section</b>» with nothing under it. The brief title and any
    section that still has a bullet or a bold lead line are kept untouched."""
    heading_re = re.compile(r"^\s*<b>[^<]+</b>\s*$")
    lines = html_text.splitlines()
    keep = [True] * len(lines)
    for i, line in enumerate(lines):
        if not heading_re.match(line) or line.strip().startswith("<b>Greater Manchester Brief"):
            continue
        has_content = False
        j = i + 1
        while j < len(lines) and not heading_re.match(lines[j]):
            if lines[j].strip():
                has_content = True
                break
            j += 1
        if has_content:
            continue
        keep[i] = False
        k = i + 1
        while k < len(lines) and not heading_re.match(lines[k]) and not lines[k].strip():
            keep[k] = False
            k += 1
    trailing = "\n" if html_text.endswith("\n") else ""
    return "\n".join(l for l, k in zip(lines, keep) if k).rstrip("\n") + trailing


def _strip_empty_endings_in_html(html_text: str) -> tuple[str, int]:
    """Last-resort strip of boilerplate call-to-action endings on the shipped
    HTML ("сверьте часы и условия перед поездкой", "уточняйте на странице
    перевозчика").

    The editor's `_apply_empty_ending_post_check` only sees `polished`; weekend,
    transport and reserve-backfilled lines reach the digest by other paths and
    keep their filler. This ship-time pass runs over every rendered bullet so the
    filler cannot survive regardless of which stage produced the line.
    """
    try:
        from news_digest.pipeline.editor import _strip_empty_editor_ending  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return html_text, 0
    lines = html_text.splitlines()
    stripped = 0
    for idx, raw in enumerate(lines):
        if not raw.strip().startswith("•"):
            continue
        fixed, reason = _strip_empty_editor_ending(raw, strip_short=True)
        if fixed != raw and reason in {"empty_generic_ending_stripped", "empty_ending_stripped_short_line"}:
            lines[idx] = fixed
            stripped += 1
    trailing = "\n" if html_text.endswith("\n") else ""
    return "\n".join(lines).rstrip("\n") + trailing, stripped


def _repair_group_key(row: dict[str, Any]) -> str:
    for field in ("repair_group_id", "issue_id", "group_id"):
        value = str(row.get(field) or "").strip()
        if value:
            return value
    reason = re.sub(r"\s+", " ", str(row.get("reason") or row.get("critical_problem") or "")).strip().lower()
    if re.search(r"\bduplicate\b|дубли", reason):
        reason = reason.split(".", 1)[0]
        reason = re.sub(r"\b\d+\b", "#", reason)
    return f"{str(row.get('section') or '').strip().lower()}::{reason or row.get('line_index')}"


def _known_factual_error(row: dict[str, Any]) -> bool:
    if row.get("critical_error"):
        return True
    blob = f"{row.get('risk') or ''} {row.get('reason') or ''} {row.get('critical_problem') or ''}".lower()
    return bool(
        re.search(
            r"fact[_ -]?(?:integrity|error|lock)|factual|unsupported|fabricat|hallucin|wrong[- ]artist|"
            r"неправильн[^.]{0,80}артист|"
            r"не подтвержд|выдум|не соответствует источник|критическ\w*\s+факт",
            blob,
        )
    )


_RU_EVENT_MONTH_STEMS = {
    1: "январ",
    2: "феврал",
    3: "март",
    4: "апрел",
    5: "ма[йя]",
    6: "июн",
    7: "июл",
    8: "август",
    9: "сентябр",
    10: "октябр",
    11: "ноябр",
    12: "декабр",
}


def _line_has_expected_event_date(candidate: dict[str, Any] | None, line: str) -> bool:
    if not isinstance(candidate, dict):
        return False
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("date_start") or event.get("date") or "")[:10]
    match = re.fullmatch(r"(20\d{2})-(\d{2})-(\d{2})", raw)
    if not match:
        return False
    year, month, day = (int(value) for value in match.groups())
    stem = _RU_EVENT_MONTH_STEMS.get(month)
    if not stem:
        return False
    visible = _strip_tags(line).lower()
    if not re.search(rf"\b0?{day}\s+{stem}[а-яё]*\b", visible):
        return False
    try:
        from news_digest.pipeline.writer import _line_has_conflicting_event_date  # noqa: PLC0415

        if _line_has_conflicting_event_date(candidate, visible):
            return False
    except Exception:  # noqa: BLE001
        pass
    # A displayed year, when present, must agree with the structured event.
    displayed_years = {int(value) for value in re.findall(r"\b20\d{2}\b", visible)}
    return not displayed_years or displayed_years == {year}


def _repair_request_already_satisfied(
    row: dict[str, Any],
    original_line: str,
    candidate: dict[str, Any] | None,
) -> bool:
    """Reject a self-contradictory model complaint without rewriting a good row."""
    concept = str(row.get("completeness_concept") or "").strip()
    if concept and line_satisfies_concept(concept, _strip_tags(original_line)):
        return True
    risk_blob = f"{row.get('risk') or ''} {row.get('reason') or ''} {row.get('critical_problem') or ''}".lower()
    return bool(
        re.search(r"\bdate\b|дат", risk_blob)
        and _line_has_expected_event_date(candidate, original_line)
    )


def _repair_line_postcheck_errors(
    row: dict[str, Any],
    line: str,
    candidate: dict[str, Any] | None,
    *,
    apply_requested_concept: bool,
) -> list[str]:
    errors: list[str] = []
    plain = _strip_tags(line)
    concept = str(row.get("completeness_concept") or "") if apply_requested_concept else ""
    if concept and not line_satisfies_concept(concept, plain):
        errors.append(f"critical concept still absent: {concept}")
    reason_blob = str(row.get("reason") or row.get("critical_problem") or "").lower()
    if re.search(r"wrong[- ]artist|неправильн[^.]{0,80}артист", reason_blob):
        expected = _expected_primary_artist(candidate)
        rendered = re.sub(r"[^a-z0-9а-яё]+", " ", plain.lower()).strip()
        if not expected or expected not in rendered:
            errors.append(f"expected primary artist still absent: {expected or 'unknown'}")
    risk_blob = f"{row.get('risk') or ''} {reason_blob}".lower()
    event = candidate.get("event") if isinstance(candidate, dict) and isinstance(candidate.get("event"), dict) else {}
    if re.search(r"\bdate\b|дат", risk_blob) and str(event.get("date_start") or event.get("date") or ""):
        if not _line_has_expected_event_date(candidate, line):
            errors.append("structured event date is not rendered correctly")
    return errors


def _candidate_own_completeness_errors(candidate: dict[str, Any] | None, line: str) -> list[str]:
    if not isinstance(candidate, dict):
        return []
    review = translation_completeness_review(_completeness_source_blob(candidate), _strip_tags(line))
    return [
        f"replacement drops {str(row.get('concept') or 'critical concept')}"
        for row in review.get("missing_critical") or []
        if isinstance(row, dict)
    ]


def _plan_slot_id_for_line(
    execution: dict[str, Any],
    candidate: dict[str, Any] | None,
    original_line: str,
    section: str,
) -> str:
    candidate_slot = str((candidate or {}).get("plan_slot_id") or "")
    slots = execution.get("slots") if isinstance(execution.get("slots"), dict) else {}
    if candidate_slot and candidate_slot in slots:
        return candidate_slot
    url_identity = _line_url_identity(original_line)
    fingerprint = str((candidate or {}).get("fingerprint") or "")
    for slot_id, payload in slots.items():
        if not isinstance(payload, dict):
            continue
        if section and str(payload.get("section") or "") != section:
            continue
        final_url = canonical_url_identity(str(payload.get("final_html_url") or ""))
        if url_identity and final_url == url_identity:
            return str(slot_id)
        if fingerprint and fingerprint in {
            str(payload.get("final_fingerprint") or ""),
            str(payload.get("original_fingerprint") or ""),
        }:
            return str(slot_id)
    return ""


def _ticket_duplicate_identity(candidate: dict[str, Any] | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    artist = str(notability.get("artist") or event.get("event_name") or "")
    date_value = str(event.get("date_start") or event.get("date") or "")[:10]
    venue = str(event.get("venue") or "")
    norm = lambda value: re.sub(r"[^a-z0-9а-яё]+", " ", str(value).lower()).strip()
    if artist and date_value and venue:
        return f"ticket:{norm(artist)}|{date_value}|{norm(venue)}"
    try:
        from news_digest.pipeline.editor import _candidate_story_identity_key  # noqa: PLC0415

        return _candidate_story_identity_key(candidate)
    except Exception:  # noqa: BLE001
        return ""


def _expected_primary_artist(candidate: dict[str, Any] | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    value = str(event.get("event_name") or candidate.get("title") or "")
    value = re.split(r"\s+—\s+event\b", value, maxsplit=1, flags=re.IGNORECASE)[0]
    value = re.sub(r"^.*?presents\s*[-–:]\s*", "", value, flags=re.IGNORECASE)
    value = re.split(r"\s+(?:ft\.?|featuring)\s+", value, maxsplit=1, flags=re.IGNORECASE)[0]
    return re.sub(r"[^a-z0-9а-яё]+", " ", value.lower()).strip()


def _finalize_repair_report(
    project_root: Path,
    digest_html: str,
    report: dict[str, Any],
    *,
    persist_execution: bool = True,
) -> None:
    """Post-check every repair operation, including single-line operations."""
    state_dir = project_root / "data" / "state"
    candidates_payload = read_json(state_dir / "candidates.json", {"candidates": []})
    candidates = [row for row in candidates_payload.get("candidates") or [] if isinstance(row, dict)]
    candidates_by_key = _candidate_index(candidates)
    final_slots = _digest_line_slots_from_html(digest_html)
    final_by_url = {
        _line_url_identity(str(slot.get("html") or "")): slot
        for slot in final_slots
        if _line_url_identity(str(slot.get("html") or ""))
    }
    from news_digest.pipeline.editorial_contracts import classify_prose_defects  # noqa: PLC0415
    try:
        from news_digest.pipeline.editor import _line_needs_russian_editor  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        _line_needs_russian_editor = lambda line: False  # type: ignore[assignment]

    final_identity_counts: dict[str, int] = {}
    for slot in final_slots:
        candidate = candidates_by_key.get(_line_url_identity(str(slot.get("html") or "")))
        identity = _ticket_duplicate_identity(candidate)
        if identity:
            final_identity_counts[identity] = final_identity_counts.get(identity, 0) + 1

    operations = report.get("operations") if isinstance(report.get("operations"), list) else []
    unresolved = 0
    blocking_unresolved = 0
    resolved = 0
    try:
        from news_digest.pipeline.plan_execution import (  # noqa: PLC0415
            load_execution,
            record_repair,
            save_execution,
        )

        execution = load_execution(state_dir)
    except Exception:  # noqa: BLE001
        execution = {}

    for operation in operations:
        actions = operation.get("actions") if isinstance(operation.get("actions"), list) else []
        checks: list[dict[str, Any]] = []
        operation_ok = True
        duplicate_identities: set[str] = set()
        for action in actions:
            original_url = str(action.get("original_url_identity") or "")
            plan_slot_id = str(action.get("plan_slot_id") or "")
            candidate = candidates_by_key.get(str(action.get("candidate_fingerprint") or "")) or candidates_by_key.get(original_url)
            current_slot = final_by_url.get(original_url)
            execution_row = ((execution.get("slots") or {}).get(plan_slot_id) or {}) if plan_slot_id else {}
            final_fp = str(execution_row.get("final_fingerprint") or "")
            final_candidate = candidates_by_key.get(final_fp) if final_fp else candidate
            if final_candidate:
                final_url = canonical_url_identity(str(final_candidate.get("source_url") or ""))
                current_slot = final_by_url.get(final_url) or current_slot
            method = str(action.get("method") or "")
            passed = False
            detail = ""
            if method in {"keep", "verified_existing_fact"}:
                passed = current_slot is not None and str(current_slot.get("section") or "") == str(action.get("section") or "")
                detail = (
                    "existing fact verified"
                    if passed and method == "verified_existing_fact"
                    else ("no repair requested" if passed else "kept line missing or moved")
                )
            elif method == "removed":
                passed = str(execution_row.get("status") or "") == "removed" and original_url not in final_by_url
                detail = "slot removed and source absent" if passed else "removed source is still visible"
            elif current_slot is None:
                detail = "final line missing"
            elif str(current_slot.get("section") or "") != str(action.get("section") or ""):
                detail = "line moved to another section"
            else:
                current_html = str(current_slot.get("html") or "")
                passed = method in {"model_patch", "deterministic_rewrite", "reserve_replacement"}
                if method in {"model_patch", "deterministic_rewrite"} and current_html == str(action.get("original_html") or ""):
                    passed = False
                    detail = "line did not change"
                if passed and (
                    classify_prose_defects(current_html)
                    or _line_needs_russian_editor(current_html)
                ):
                    passed = False
                    detail = "shared prose policy still fails"
            if current_slot is not None and method not in {"removed", "keep", "verified_existing_fact"}:
                current_html = str(current_slot.get("html") or "")
                post_errors = _repair_line_postcheck_errors(
                    action,
                    current_html,
                    final_candidate,
                    apply_requested_concept=method != "reserve_replacement",
                )
                if method == "reserve_replacement":
                    post_errors.extend(_candidate_own_completeness_errors(final_candidate, current_html))
                if post_errors:
                    passed = False
                    detail = "; ".join(post_errors[:4])
            identity = _ticket_duplicate_identity(candidate)
            if identity and re.search(r"\bduplicate\b|дубли", str(action.get("reason") or "").lower()):
                duplicate_identities.add(identity)
            checks.append({"line_index": action.get("line_index"), "passed": passed, "detail": detail or "post-check passed"})
            operation_ok = operation_ok and passed

        for identity in duplicate_identities:
            count = int(final_identity_counts.get(identity, 0))
            duplicate_ok = count <= 1
            checks.append({"check": "duplicate_identity", "identity": identity, "visible_count": count, "passed": duplicate_ok})
            operation_ok = operation_ok and duplicate_ok

        outcome = "resolved_in_place" if operation_ok else "unresolved"
        operation["outcome"] = outcome
        operation["post_checks"] = checks
        operation["known_factual_error"] = any(_known_factual_error(action) for action in actions)
        if operation_ok:
            resolved += 1
        else:
            unresolved += 1
            if operation["known_factual_error"]:
                blocking_unresolved += 1
        for action in actions:
            action["outcome"] = outcome
            action["post_check_passed"] = operation_ok
            slot_id = str(action.get("plan_slot_id") or "")
            if slot_id and execution and persist_execution:
                record_repair(
                    execution,
                    slot_id,
                    status=outcome,
                    reason=str(action.get("reason") or operation.get("group_key") or ""),
                )

    prose_findings = []
    for slot in final_slots:
        line = str(slot.get("html") or "")
        shared_findings = classify_prose_defects(line)
        if shared_findings:
            for finding in shared_findings:
                prose_findings.append(
                    {
                        "line_index": slot.get("line_index"),
                        "section": slot.get("section") or "",
                        "outcome": "unresolved",
                        **finding,
                    }
                )
        elif _line_needs_russian_editor(line):
            prose_findings.append(
                {
                    "line_index": slot.get("line_index"),
                    "section": slot.get("section") or "",
                    "outcome": "unresolved",
                    "code": "language_policy_defect",
                    "severity": "warning",
                }
            )
    report["resolved_in_place"] = resolved
    report["unresolved"] = unresolved
    report["blocking_unresolved"] = blocking_unresolved
    report["final_prose_policy"] = {
        "checked_lines": len(final_slots),
        "resolved_in_place": len(final_slots) - len(prose_findings),
        "unresolved": len(prose_findings),
        "findings": prose_findings[:60],
    }
    report["status"] = "resolved_in_place" if not unresolved and not prose_findings else "unresolved"
    if execution and persist_execution:
        save_execution(state_dir, execution)


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
        "blocking_unresolved": 0,
        "resolved_in_place": 0,
        "fact_lock_rejected": 0,
        "enrich_attempted": 0,
        "post_check_errors": deterministic_post_check.get("errors") or [],
        "actions": [],
        "operations": [],
    }
    if not rows:
        _finalize_repair_report(project_root, digest_html, report, persist_execution=not dry_run)
        return digest_html, report

    state_dir = project_root / "data" / "state"
    candidates_payload = read_json(state_dir / "candidates.json", {"candidates": []})
    candidates = [c for c in candidates_payload.get("candidates") or [] if isinstance(c, dict)]
    candidates_by_key = _candidate_index(candidates)
    try:
        from news_digest.pipeline.plan_execution import load_execution  # noqa: PLC0415

        execution_for_lookup = load_execution(state_dir)
    except Exception:  # noqa: BLE001
        execution_for_lookup = {}
    slots = _digest_line_slots_from_html(digest_html)
    slot_by_index = {int(slot.get("line_index") or 0): slot for slot in slots}
    html_lines = digest_html.splitlines()
    try:
        from news_digest.pipeline.editor import _line_needs_russian_editor, _line_preserves_links  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        _line_needs_russian_editor = lambda line: False  # type: ignore[assignment]
        _line_preserves_links = lambda original, fixed: True  # type: ignore[assignment]

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
            "risk": row.get("risk") or "",
            "critical_error": bool(row.get("critical_error")),
            "critical_problem": row.get("critical_problem") or "",
            "completeness_concept": row.get("completeness_concept") or "",
            "outcome": "",
            "method": "",
        }
        if not slot:
            action_record["outcome"] = "skipped_missing_line"
            report["actions"].append(action_record)
            report["unresolved"] = int(report.get("unresolved") or 0) + 1
            continue
        raw_index = int(slot.get("raw_index") or 0)
        original = str(slot.get("html") or "")
        section_name = str(slot.get("section") or row.get("section") or "")
        action_name = str(row.get("action") or "").strip().lower()
        candidate = candidates_by_key.get(_line_url_identity(original))
        repair_candidate = _enrich_candidate_for_repair(candidate, report) if action_name != "keep" else candidate
        plan_slot_id = _plan_slot_id_for_line(
            execution_for_lookup,
            candidate,
            original,
            section_name,
        )
        action_record.update(
            {
                "section": section_name,
                "plan_slot_id": plan_slot_id,
                "candidate_fingerprint": str((candidate or {}).get("fingerprint") or ""),
                "original_url_identity": _line_url_identity(original),
                "original_html": original,
            }
        )
        if action_name == "keep" and not _known_factual_error(row):
            action_record["method"] = "keep"
            report["actions"].append(action_record)
            continue
        if action_name == "keep":
            # A model cannot preserve a line it simultaneously marks as a
            # known factual defect. Run the normal recovery ladder instead.
            action_record["keep_rejected"] = "known_factual_error"
            action_name = "replace"
        if action_name != "strip" and _repair_request_already_satisfied(row, original, repair_candidate):
            action_record["method"] = "verified_existing_fact"
            action_record["verification"] = "structured fact already rendered correctly"
            report["false_positive_existing_fact"] = int(report.get("false_positive_existing_fact") or 0) + 1
            report["actions"].append(action_record)
            continue
        report["attempted"] = int(report.get("attempted") or 0) + 1
        replacement = ""
        model_replacement = str(row.get("replacement_text") or "").strip()
        if action_name in {"patch", "replace"} and model_replacement:
            model_line = _replacement_with_link(model_replacement, original, repair_candidate)
            unsupported = _fact_lock_errors_for_replacement(
                model_line,
                candidate=repair_candidate,
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
                post_errors = _repair_line_postcheck_errors(
                    row,
                    model_line,
                    repair_candidate,
                    apply_requested_concept=True,
                )
                if post_errors:
                    action_record["model_replacement_rejected"] = f"post_check: {', '.join(post_errors[:4])}"
                    report["model_post_check_rejected"] = int(report.get("model_post_check_rejected") or 0) + 1
                else:
                    replacement = model_line
                    action_record["method"] = "model_patch"
                    report["model_patch_applied"] = int(report.get("model_patch_applied") or 0) + 1

        if not replacement and action_name != "strip":
            replacement = _deterministic_rewrite_from_candidate(repair_candidate, original, report)
            if replacement:
                post_errors = _repair_line_postcheck_errors(
                    row,
                    replacement,
                    repair_candidate,
                    apply_requested_concept=True,
                )
                if post_errors:
                    action_record["deterministic_rewrite_rejected"] = f"post_check: {', '.join(post_errors[:4])}"
                    report["deterministic_post_check_rejected"] = int(report.get("deterministic_post_check_rejected") or 0) + 1
                    replacement = ""
                else:
                    action_record["method"] = "deterministic_rewrite"

        if not replacement and action_name != "strip":
            # Этап 3: единственный источник замен — цепочка запасных слота.
            if plan_slot_id:
                from news_digest.pipeline.plan_execution import (  # noqa: PLC0415
                    load_execution,
                    record_outcome,
                    save_execution,
                )
                from news_digest.pipeline.writer import produce_replacement_for_slot  # noqa: PLC0415

                for _attempt in range(4):
                    candidate_line = produce_replacement_for_slot(state_dir, plan_slot_id, stage="judge")
                    if not candidate_line:
                        break
                    current_execution = load_execution(state_dir)
                    current_row = ((current_execution.get("slots") or {}).get(plan_slot_id) or {})
                    backup_fp = str(current_row.get("final_fingerprint") or "")
                    backup_candidate = candidates_by_key.get(backup_fp)
                    post_errors = _repair_line_postcheck_errors(
                        row,
                        candidate_line,
                        backup_candidate,
                        apply_requested_concept=False,
                    ) + _candidate_own_completeness_errors(backup_candidate, candidate_line)
                    if not post_errors:
                        replacement = candidate_line
                        action_record["method"] = "reserve_replacement"
                        action_record["replacement_fingerprint"] = backup_fp
                        break
                    record_outcome(
                        current_execution,
                        plan_slot_id,
                        status="",
                        failed_fingerprint=backup_fp,
                        reason=f"judge_post_check:{', '.join(post_errors[:3])}",
                        stage="judge",
                    )
                    save_execution(state_dir, current_execution)
                    action_record.setdefault("reserve_rejections", []).append(
                        {"fingerprint": backup_fp, "errors": post_errors[:4]}
                    )
                    report["reserve_post_check_rejected"] = int(report.get("reserve_post_check_rejected") or 0) + 1

        if replacement:
            if not dry_run:
                html_lines[raw_index] = replacement
            report["applied"] = int(report.get("applied") or 0) + 1
            if action_record["method"] == "deterministic_rewrite":
                report["deterministic_rewrite_used"] = int(report.get("deterministic_rewrite_used") or 0) + 1
            elif action_record["method"] == "reserve_replacement":
                report["reserve_replacement_used"] = int(report.get("reserve_replacement_used") or 0) + 1
            report["actions"].append(action_record)
            continue

        if action_name != "strip" and not _known_factual_error(row):
            # A stylistic/fix request may remain visible, but it is explicitly
            # unresolved. It is never called repaired merely because a floor is
            # at risk.
            report["actions"].append(action_record)
            continue
        if action_name != "strip" and _known_factual_error(row) and not plan_slot_id:
            # Never create a technical plan mismatch merely to remove a quality
            # defect whose slot could not be identified. Keep it visible,
            # report it honestly, and let delivery continue as degraded.
            action_record["outcome"] = "unresolved_no_plan_slot"
            report["actions"].append(action_record)
            continue

        reason_blob = f"{row.get('risk') or ''} {row.get('reason') or ''}".lower()
        removal_reason = "duplicate_after_plan" if re.search(r"\bduplicate\b|дубли", reason_blob) else (
            "fact_lock_failed" if _known_factual_error(row) else "unrenderable_line"
        )
        if not dry_run:
            html_lines[raw_index] = ""
            try:
                from news_digest.pipeline.plan_execution import (  # noqa: PLC0415
                    load_execution,
                    record_outcome,
                    save_execution,
                )

                _exec = load_execution(state_dir)
                if plan_slot_id:
                    record_outcome(_exec, plan_slot_id, status="removed", reason=removal_reason, stage="judge")
                    save_execution(state_dir, _exec)
            except Exception:  # noqa: BLE001
                pass
        action_record["method"] = "removed"
        action_record["removal_reason"] = removal_reason
        report["stripped"] = int(report.get("stripped") or 0) + 1
        report["applied"] = int(report.get("applied") or 0) + 1
        report["actions"].append(action_record)

    operations_by_key: dict[str, dict[str, Any]] = {}
    for action in report["actions"]:
        key = _repair_group_key(action)
        operation = operations_by_key.setdefault(
            key,
            {"group_key": key, "actions": [], "outcome": "", "post_checks": []},
        )
        operation["actions"].append(action)
    report["operations"] = list(operations_by_key.values())
    repaired_html = "\n".join(html_lines).strip()
    _finalize_repair_report(project_root, repaired_html, report, persist_execution=not dry_run)
    return repaired_html, report


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


_DECISION_RANK = {"pass": 0, "warn": 1, "repair_required": 2, "block": 3}


def _judge_token_limiter():
    global _JUDGE_TOKEN_LIMITER
    if _JUDGE_TOKEN_LIMITER is None:
        from news_digest.pipeline.llm_rewrite import _TokenRateLimiter  # noqa: PLC0415

        max_tpm = max(2000.0, float(os.environ.get("PRE_SEND_JUDGE_MAX_TPM", PRE_SEND_JUDGE_MAX_TPM)))
        _JUDGE_TOKEN_LIMITER = _TokenRateLimiter(max_tpm)
    return _JUDGE_TOKEN_LIMITER


def _estimate_messages_tokens(messages: list[dict[str, str]], max_tokens: int) -> int:
    # Conservative enough for pacing: OpenAI-compatible chat prompts in this
    # repo average around 3.5-4 chars/token; use /3.5 plus reserved output.
    chars = sum(len(str(message.get("content") or "")) for message in messages)
    return max(1, int(chars / 3.5) + int(max_tokens or 0))


def _call_judge_payload(
    *,
    client: object,
    step: object,
    system_prompt: str,
    payload: dict[str, Any],
    max_tokens: int,
    prompt_name: str,
) -> dict[str, Any]:
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    estimated_tokens = _estimate_messages_tokens(messages, max_tokens)
    started = time.monotonic()
    try:
        _judge_token_limiter().acquire(estimated_tokens)
        response = client.chat.completions.create(
            model=step.model,
            messages=messages,
            temperature=0.0,
            max_tokens=max_tokens,
        )
        raw_text = response.choices[0].message.content or ""
        try:
            from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415

            record_call_from_response(
                response=response,
                stage="pre_send_quality_judge",
                provider=step.provider,
                model=step.model,
                prompt_name=prompt_name,
                messages=messages,
                max_tokens=max_tokens,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("pre-send quality judge: cost tracking failed: %s", exc)
        parsed = _parse_reply(raw_text)
        if parsed is None:
            return {
                "status": "parse_failed",
                "error": "no parseable JSON",
                "raw_excerpt": raw_text[:500],
                "estimated_tokens": estimated_tokens,
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        return {
            "status": "ok",
            "parsed": parsed,
            "estimated_tokens": estimated_tokens,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "error": f"{exc.__class__.__name__}: {exc}",
            "estimated_tokens": estimated_tokens,
            "duration_seconds": round(time.monotonic() - started, 3),
        }


def _chunk_summary(report: dict[str, Any]) -> dict[str, Any]:
    parsed = report.get("parsed") if isinstance(report.get("parsed"), dict) else {}
    return {
        "chunk_id": report.get("chunk_id"),
        "status": report.get("status"),
        "line_range": report.get("line_range") or [],
        "sections": report.get("sections") or [],
        "decision": parsed.get("decision") if parsed else "",
        "critical_error_count": len(parsed.get("critical_errors") or []) if parsed else 0,
        "action_count": len(parsed.get("actions") or []) if parsed else 0,
        "warning_count": len(parsed.get("warnings") or []) if parsed else 0,
        "error": report.get("error") or "",
        "estimated_tokens": report.get("estimated_tokens") or 0,
        "duration_seconds": report.get("duration_seconds") or 0,
    }


def _combine_map_reduce_results(
    chunk_reports: list[dict[str, Any]],
    reduce_report: dict[str, Any] | None,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    reports = list(chunk_reports)
    if reduce_report:
        reports.append(reduce_report)
    successful = [r for r in reports if r.get("status") == "ok" and isinstance(r.get("parsed"), dict)]
    failed = [r for r in reports if r.get("status") != "ok"]

    if not successful:
        combined = {
            "decision": "block",
            "confidence": 0.0,
            "critical_errors": [],
            "actions": [],
            "warnings": [f"all judge calls failed: {len(failed)} failure(s)"],
            "notes": "pre-send judge failed before producing usable verdicts",
        }
        return "failed", combined, {"chunks": [_chunk_summary(r) for r in chunk_reports], "reduce": _chunk_summary(reduce_report or {})}

    decision = "pass"
    confidence_values: list[float] = []
    critical_errors: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    notes: list[str] = []
    for report in successful:
        parsed = report["parsed"]
        parsed_decision = str(parsed.get("decision") or "repair_required").strip().lower()
        if _DECISION_RANK.get(parsed_decision, 2) > _DECISION_RANK.get(decision, 0):
            decision = parsed_decision if parsed_decision in _DECISION_RANK else "repair_required"
        try:
            confidence_values.append(float(parsed.get("confidence")))
        except (TypeError, ValueError):
            pass
        critical_errors.extend(row for row in (parsed.get("critical_errors") or []) if isinstance(row, dict))
        actions.extend(row for row in (parsed.get("actions") or []) if isinstance(row, dict))
        warnings.extend(str(row)[:260] for row in (parsed.get("warnings") or []) if str(row).strip())
        note = str(parsed.get("notes") or "").strip()
        if note:
            notes.append(note[:160])

    if failed and _DECISION_RANK.get(decision, 0) < _DECISION_RANK["warn"]:
        decision = "warn"
    for report in failed:
        warnings.append(
            f"judge {report.get('chunk_id') or report.get('mode') or 'call'} failed: "
            f"{report.get('status')} {report.get('error') or ''}".strip()
        )
    status = "partial" if failed else "ok"
    combined = {
        "decision": decision,
        "confidence": min(confidence_values) if confidence_values else (0.65 if failed else 0.8),
        "critical_errors": critical_errors[:24],
        "actions": actions[:40],
        "warnings": warnings[:30],
        "notes": "; ".join(notes)[:320] if notes else ("partial judge verdict" if failed else "map/reduce judge verdict"),
    }
    raw = {
        "mode": "map_reduce",
        "status": status,
        "chunk_count": len(chunk_reports),
        "failed_chunk_count": sum(1 for r in chunk_reports if r.get("status") != "ok"),
        "chunks": [_chunk_summary(r) for r in chunk_reports],
        "reduce": _chunk_summary(reduce_report or {}),
    }
    return status, combined, raw


def _run_map_reduce_judge(
    *,
    client: object,
    step: object,
    state_dir: Path,
    run_date: str,
    pipeline_run_id: str,
    sha: str,
    slots: list[dict[str, Any]],
    rendered_candidates: list[dict[str, Any]],
    product_completeness: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    rendered_by_url = _rendered_candidates_by_url(rendered_candidates)
    chunks = _chunk_digest_slots(slots, rendered_by_url)
    chunk_reports: list[dict[str, Any]] = []

    def _call_chunk(chunk: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "mode": "map",
            "run_date_london": run_date,
            "pipeline_run_id": pipeline_run_id,
            "digest_sha256": sha,
            "chunk": chunk,
        }
        report = _call_judge_payload(
            client=client,
            step=step,
            system_prompt=MAP_SYSTEM_PROMPT,
            payload=payload,
            max_tokens=PRE_SEND_JUDGE_MAP_MAX_TOKENS,
            prompt_name="pre_send_quality_judge_map",
        )
        report.update({
            "mode": "map",
            "chunk_id": chunk.get("chunk_id"),
            "sections": chunk.get("sections") or [],
            "line_range": chunk.get("line_range") or [],
        })
        return report

    max_workers = max(1, int(os.environ.get("PRE_SEND_JUDGE_MAX_WORKERS", PRE_SEND_JUDGE_MAX_WORKERS)))
    max_workers = min(max_workers, len(chunks) or 1)
    if max_workers <= 1:
        chunk_reports = [_call_chunk(chunk) for chunk in chunks]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {executor.submit(_call_chunk, chunk): idx for idx, chunk in enumerate(chunks)}
            ordered: list[dict[str, Any] | None] = [None] * len(chunks)
            for future in as_completed(future_to_index):
                ordered[future_to_index[future]] = future.result()
            chunk_reports = [report for report in ordered if isinstance(report, dict)]

    digest_outline = [
        {
            "line_index": int(slot.get("line_index") or 0),
            "section": str(slot.get("section") or ""),
            "text": _compact_text(slot.get("text"), 260),
        }
        for slot in slots
    ]
    reduce_payload = {
        "mode": "reduce",
        "run_date_london": run_date,
        "pipeline_run_id": pipeline_run_id,
        "digest_sha256": sha,
        "product_completeness": product_completeness,
        "digest_outline": digest_outline,
        "rendered_leads_and_protected": [
            {
                "fingerprint": c.get("fingerprint"),
                "title": c.get("title"),
                "primary_block": c.get("primary_block"),
                "is_lead": c.get("is_lead"),
                "protected_lane": c.get("protected_lane") or {},
            }
            for c in rendered_candidates
            if c.get("is_lead") or c.get("protected_lane")
        ][:40],
        "chunk_summaries": [_chunk_summary(report) for report in chunk_reports],
    }
    reduce_report = _call_judge_payload(
        client=client,
        step=step,
        system_prompt=REDUCE_SYSTEM_PROMPT,
        payload=reduce_payload,
        max_tokens=PRE_SEND_JUDGE_REDUCE_MAX_TOKENS,
        prompt_name="pre_send_quality_judge_reduce",
    )
    reduce_report.update({"mode": "reduce", "chunk_id": "reduce"})

    try:
        from news_digest.pipeline.cost_tracker import dump_stage  # noqa: PLC0415

        dump_stage(state_dir, "pre_send_quality_judge")
    except Exception as exc:  # noqa: BLE001
        logger.warning("pre-send quality judge: cost dump failed: %s", exc)

    status, combined, raw = _combine_map_reduce_results(chunk_reports, reduce_report)
    raw["max_workers"] = max_workers
    raw["chunk_line_target"] = PRE_SEND_JUDGE_CHUNK_LINES
    raw["max_tpm"] = max(2000.0, float(os.environ.get("PRE_SEND_JUDGE_MAX_TPM", PRE_SEND_JUDGE_MAX_TPM)))
    return status, combined, raw


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
        blocking_unresolved = int(repair.get("blocking_unresolved") or 0) if repair else 0
        if repair:
            release_report["pre_send_repair_executor"] = repair
        if result.decision == "warn" or repair_applied or repair_unresolved or blocking_unresolved:
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
    digest_slots = _digest_line_slots_from_html(digest_html)
    digest_lines = [
        {"line_index": item["line_index"], "section": item.get("section") or "", "text": item.get("text") or ""}
        for item in digest_slots
    ]
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
            decision="warn",
            can_send=True,
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
            decision="warn",
            can_send=True,
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
            decision="warn",
            can_send=True,
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

    try:
        client = OpenAI(
            api_key=key,
            base_url=step.base_url,
            timeout=step.timeout_seconds or 75,
            max_retries=sdk_retries_for_route(provider=step.provider, model=step.model, base_url=step.base_url),
        )
        judge_status, parsed, map_reduce_raw = _run_map_reduce_judge(
            client=client,
            step=step,
            state_dir=state_dir,
            run_date=run_date,
            pipeline_run_id=pipeline_run_id,
            sha=sha,
            slots=digest_slots,
            rendered_candidates=rendered_candidates,
            product_completeness=product_completeness,
        )
    except Exception as exc:  # noqa: BLE001
        fallback_scan = _deterministic_html_scan(digest_slots)
        result = PreSendQualityResult(
            status="failed",
            decision="warn",
            can_send=True,
            reason=f"pre-send quality judge LLM call failed: {exc}",
            model=step.model,
            provider=step.provider,
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            critical_errors=[],
            warnings=[
                f"deterministic fallback: {f.get('type')} on line {f.get('line_index')} — {f.get('detail')}"
                for f in fallback_scan.get("findings", [])
            ],
            product_completeness=product_completeness,
            deterministic_post_check={"model_unavailable_fallback": fallback_scan},
        )
        _write_report(project_root, result)
        return asdict(result)

    if parsed is None:
        fallback_scan = _deterministic_html_scan(digest_slots)
        result = PreSendQualityResult(
            status="failed",
            decision="warn",
            can_send=True,
            reason="pre-send quality judge returned no parseable JSON",
            model=step.model,
            provider=step.provider,
            run_date_london=run_date,
            pipeline_run_id=pipeline_run_id,
            digest_sha256=sha,
            duration_seconds=round(time.monotonic() - start, 3),
            critical_errors=[],
            warnings=[
                f"deterministic fallback: {f.get('type')} on line {f.get('line_index')} — {f.get('detail')}"
                for f in fallback_scan.get("findings", [])
            ],
            product_completeness=product_completeness,
            deterministic_post_check={"model_unavailable_fallback": fallback_scan},
        )
        _write_report(project_root, result)
        return asdict(result)

    decision, can_send, reason, confidence, critical_errors, actions, warnings, notes = _normalise_result(parsed)
    deterministic_post_check = _deterministic_action_post_check(actions, digest_lines, rendered_candidates)
    # 0043: reverse fact-lock — deterministic net for grave source facts dropped
    # in translation. Its critical errors join the LLM's so the repair executor
    # rewrites/pulls the neutered line; its scalar drops are warning-only.
    translation_completeness = _deterministic_completeness_scan(
        digest_slots, _rendered_candidates_by_url(rendered_candidates)
    )
    critical_errors.extend(translation_completeness.pop("critical_errors", []))
    warnings.extend(translation_completeness.pop("warnings", []))
    # 0040: when no chunk produced a usable verdict, the action-based post-check
    # above sees an empty action list and reports nothing. Fall back to a direct
    # model-free scan of the shipped HTML so the failure report is not blind.
    if judge_status == "failed":
        fallback_scan = _deterministic_html_scan(digest_slots)
        deterministic_post_check = {**(deterministic_post_check or {}), "model_unavailable_fallback": fallback_scan}
        for finding in fallback_scan.get("findings", []):
            warnings.append(
                f"deterministic fallback: {finding.get('type')} on line "
                f"{finding.get('line_index')} — {finding.get('detail')}"
            )
    repair_executor: dict[str, Any] | None = None
    final_sha = sha
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
        _recount_completeness_recovery(translation_completeness, digest_html)
    # Этап 3: пост-ремонтная «гигиена» больше не меняет состав. Разрешены
    # только словесные правки: срез пустых хвостов внутри строки и снятие
    # шапки секции, оставшейся без строк после fact-integrity снятий.
    if not dry_run:
        hygiene_html, endings_stripped = _strip_empty_endings_in_html(digest_html)
        if endings_stripped and repair_executor is not None:
            repair_executor["empty_endings_stripped_at_ship"] = int(
                repair_executor.get("empty_endings_stripped_at_ship") or 0
            ) + endings_stripped
        hygiene_html = _strip_empty_section_headings(hygiene_html)
        if hygiene_html != digest_html:
            digest_path.write_text(hygiene_html, encoding="utf-8")
            digest_html = hygiene_html
            final_sha = digest_hash(digest_html)
            digest_lines = digest_lines_from_html(digest_html)
            product_completeness = _product_completeness_context(project_root, digest_lines)
    if repair_executor is not None:
        # Hygiene is the last prose mutation. Re-run every operation check and
        # the shared prose policy on the exact bytes that may be sent.
        _finalize_repair_report(project_root, digest_html, repair_executor)
        blocking_unresolved = int(repair_executor.get("blocking_unresolved") or 0)
        unresolved = int(repair_executor.get("unresolved") or 0)
        prose_unresolved = int((repair_executor.get("final_prose_policy") or {}).get("unresolved") or 0)
        if blocking_unresolved:
            decision = "warn"
            can_send = True
            reason = (
                f"pre-send repair left {blocking_unresolved} known factual operation(s) unresolved; "
                "delivery continues as degraded"
            )
        elif unresolved or prose_unresolved:
            decision = "warn"
            can_send = True
            reason = (
                "pre-send repair completed with honest unresolved non-factual findings: "
                f"operations={unresolved}, prose={prose_unresolved}"
            )
        elif int(repair_executor.get("applied") or 0):
            decision = "warn"
            can_send = True
            reason = (
                "pre-send repair executor applied "
                f"{repair_executor.get('applied')} repair(s); every operation passed its post-check"
            )
        elif decision in BLOCKING_DECISIONS:
            decision = "warn"
            can_send = True
            reason = "pre-send judge requested a hold without an executable repair; delivery continues as degraded"
    result = PreSendQualityResult(
        status=judge_status,
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
        translation_completeness=translation_completeness,
        repair_executor=repair_executor,
        notes=notes,
        raw={**parsed, "map_reduce": map_reduce_raw},
    )
    _write_report(project_root, result)
    return asdict(result)


def quality_gate_error_for_digest(project_root: Path, digest_path: Path) -> str:
    """Quality reports are advisory; only a missing digest is undeliverable.

    Kept as a compatibility surface for callers/tests. Freshness, model failure,
    and unresolved findings are written to reports and warnings, but the global
    release contract forbids using them to hold an otherwise technical-valid
    issue.
    """
    current_digest = (project_root / "data" / "outgoing" / "current_digest.html").resolve()
    try:
        resolved = digest_path.resolve()
    except OSError:
        resolved = digest_path
    if resolved != current_digest:
        return ""
    if not digest_path.exists():
        return "current_digest.html missing"
    return ""
