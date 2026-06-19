"""Pre-send quality judge for the production digest.

This is the strong-model final reader before Telegram delivery. The bulk
pipeline remains mini-first; this stage reads only the already-built digest
and compact evidence for rendered items, then decides whether the issue is
safe to send.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from news_digest.pipeline.common import pipeline_run_id_from, read_json, today_london, write_json
from news_digest.pipeline.model_routing import resolve_model_route, sdk_retries_for_route


logger = logging.getLogger(__name__)


PROMPT_VERSION = "v2"
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
  "warnings": ["..."],
  "notes": "до 240 символов"
}

Если сомневаешься, предпочти "repair_required" только для реально опасной смысловой ошибки. Не требуй переписывать выпуск ради вкуса.
Если проблема продуктовая, но выпуск всё ещё можно отправить как degraded issue, ставь "warn" и явно назови провал блока.
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
    warnings: list[str] | None = None
    product_completeness: dict[str, Any] | None = None
    notes: str = ""
    raw: dict[str, Any] | None = None


def digest_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _strip_tags(text: str) -> str:
    text = re.sub(r"<a\s+[^>]*>(.*?)</a>", r"\1", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"</?(?:b|i|strong|em)>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def digest_lines_from_html(digest_html: str) -> list[dict[str, Any]]:
    section = ""
    lines: list[dict[str, Any]] = []
    for raw_line in digest_html.splitlines():
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
            }
        )
    return lines[:60]


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
                "primary_block": str(candidate.get("primary_block") or ""),
                "category": str(candidate.get("category") or ""),
                "summary": str(candidate.get("summary") or "")[:360],
                "lead": str(candidate.get("lead") or "")[:360],
                "practical_angle": str(candidate.get("practical_angle") or "")[:260],
                "draft_line": _strip_tags(str(candidate.get("draft_line") or ""))[:700],
                "is_lead": bool(candidate.get("is_lead")),
                "protected_lane": str(candidate.get("protected_lane") or ""),
            }
        )
    return summary[:60]


def _product_completeness_context(project_root: Path, digest_lines: list[dict[str, Any]]) -> dict[str, Any]:
    state_dir = project_root / "data" / "state"
    writer_report = read_json(state_dir / "writer_report.json", {})
    release_report = read_json(state_dir / "release_report.json", {})
    section_counts = dict(writer_report.get("section_counts") or {})
    if not section_counts:
        for line in digest_lines:
            section = str(line.get("section") or "")
            if section:
                section_counts[section] = section_counts.get(section, 0) + 1
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


def _normalise_result(parsed: dict[str, Any], *, fallback_reason: str = "") -> tuple[str, bool, str, float | None, list[dict[str, Any]], list[str], str]:
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
    raw_warnings = parsed.get("warnings") if isinstance(parsed.get("warnings"), list) else []
    warnings = [str(item)[:260] for item in raw_warnings if str(item).strip()][:12]
    notes = str(parsed.get("notes") or "")[:320]
    can_send = decision in ALLOWED_TO_SEND
    if not reason:
        reason = "quality judge passed" if can_send else "quality judge found blocking defects"
    return decision, can_send, reason, confidence, critical_errors, warnings, notes


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
    max_tokens = 900
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

    decision, can_send, reason, confidence, critical_errors, warnings, notes = _normalise_result(parsed)
    result = PreSendQualityResult(
        status="ok",
        decision=decision,
        can_send=can_send,
        reason=reason,
        model=step.model,
        provider=step.provider,
        run_date_london=run_date,
        pipeline_run_id=pipeline_run_id,
        digest_sha256=sha,
        duration_seconds=round(time.monotonic() - start, 3),
        confidence=confidence,
        critical_errors=critical_errors,
        warnings=warnings,
        product_completeness=product_completeness,
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
