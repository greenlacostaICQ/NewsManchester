"""Этап 3: финальная сверка перед отправкой — «выпуск = план?».

Запускается ПОСЛЕ предsend-судьи (последнего, кто правит слова) и до
send-file. Сравнивает ФИНАЛЬНЫЙ отправляемый HTML с неизменяемым планом
и отчётом исполнения.

Правило блокировки (согласовано): контентные расхождения НИКОГДА не
отменяют выпуск — они уходят предупреждениями и ship_degraded; отправку
блокируют только технические дефекты артефакта:
  * плана нет или он от другого pipeline_run_id;
  * шапка выпуска не за сегодняшний день;
  * HTML пуст или без единой ссылки-источника.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path

from news_digest.pipeline.common import (
    canonical_url_identity,
    extract_sections,
    now_london,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.plan_execution import load_execution, load_plan, plan_slots

REPORT_NAME = "verify_digest_plan_report.json"
_HREF_RE = re.compile(r'href="([^"]+)"')
_MASTHEAD_RE = re.compile(r"^<b>Greater Manchester Brief — (\d{4}-\d{2}-\d{2})")


@dataclass
class VerifyResult:
    ok: bool
    message: str
    report_path: Path


def _candidate_url_identity(candidate: dict | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    return canonical_url_identity(str(candidate.get("source_url") or ""))


def run_verify_digest_plan(project_root: Path, digest_path: Path | None = None) -> VerifyResult:
    state_dir = project_root / "data" / "state"
    report_path = state_dir / REPORT_NAME
    digest_path = digest_path or (project_root / "data" / "outgoing" / "current_digest.html")

    technical_errors: list[str] = []
    warnings: list[str] = []
    divergences: list[dict[str, object]] = []

    html_text = digest_path.read_text(encoding="utf-8") if digest_path.exists() else ""
    plan = load_plan(state_dir)
    execution = load_execution(state_dir)
    payload = read_json(state_dir / "candidates.json", {"candidates": []})
    by_fp = {
        str(c.get("fingerprint") or ""): c
        for c in payload.get("candidates", [])
        if isinstance(c, dict)
    }

    # --- Технический гейт (единственное, что блокирует отправку) -----------
    if not html_text.strip():
        technical_errors.append("Final digest HTML is missing or empty.")
    elif "<a " not in html_text.lower():
        technical_errors.append("Final digest HTML contains no source links.")
    if not plan or not plan_slots(plan):
        technical_errors.append("release_plan.json is missing or has no slots.")
    else:
        plan_run = str(plan.get("pipeline_run_id") or "")
        exec_run = str(execution.get("pipeline_run_id") or "")
        if plan_run and exec_run and plan_run != exec_run:
            technical_errors.append(
                f"Plan/run mismatch: release_plan {plan_run} vs plan_execution {exec_run}."
            )
        plan_day = str(plan.get("run_date_london") or "")
        if plan_day and plan_day != today_london():
            technical_errors.append(f"release_plan is for {plan_day}, today is {today_london()}.")
    masthead = _MASTHEAD_RE.match(html_text.splitlines()[0].strip() if html_text else "")
    if html_text and not masthead:
        technical_errors.append("Masthead line is missing from the final HTML.")
    elif masthead and masthead.group(1) != today_london():
        technical_errors.append(
            f"Masthead date {masthead.group(1)} is not today ({today_london()}) — stale artifact."
        )

    # --- Контентная сверка (warnings, никогда не блокирует) ----------------
    visible_idents = {canonical_url_identity(u) for u in _HREF_RE.findall(html_text)}
    visible_idents.discard("")
    sections = extract_sections(html_text)
    lead_visible = bool(sections.get("Главная история дня"))

    slot_rows = list((execution.get("slots") or {}).values())
    planned_ident_by_slot: dict[str, str] = {}
    accounted_idents: set[str] = set()
    for slot in plan_slots(plan):
        slot_id = str(slot.get("slot_id") or "")
        for fp in [slot.get("primary_fingerprint"), *(slot.get("backup_fingerprints") or [])]:
            ident = _candidate_url_identity(by_fp.get(str(fp or "")))
            if ident:
                accounted_idents.add(ident)
        planned_ident_by_slot[slot_id] = _candidate_url_identity(
            by_fp.get(str(slot.get("primary_fingerprint") or ""))
        )
    lead_plan = plan.get("lead") if isinstance(plan.get("lead"), dict) else {}
    for fp in [lead_plan.get("primary_fingerprint"), *(lead_plan.get("understudy_fingerprints") or [])]:
        ident = _candidate_url_identity(by_fp.get(str(fp or "")))
        if ident:
            accounted_idents.add(ident)

    shown = replaced = removed = unfilled = 0
    for row in slot_rows:
        if not isinstance(row, dict):
            continue
        slot_id = str(row.get("slot_id") or "")
        status = str(row.get("status") or "")
        final_fp = str(row.get("final_fingerprint") or "")
        final_ident = _candidate_url_identity(by_fp.get(final_fp))
        if status in {"shown", "replaced"}:
            shown += status == "shown"
            replaced += status == "replaced"
            if final_ident and final_ident not in visible_idents:
                divergences.append(
                    {
                        "slot_id": slot_id,
                        "kind": "planned_line_missing_from_final_html",
                        "section": row.get("section"),
                        "detail": f"status={status}, но строки слота нет в финальном HTML",
                    }
                )
        elif status == "removed":
            removed += 1
            if final_ident and final_ident in visible_idents:
                divergences.append(
                    {
                        "slot_id": slot_id,
                        "kind": "removed_line_still_visible",
                        "section": row.get("section"),
                        "detail": row.get("replacement_reason"),
                    }
                )
            if not str(row.get("replacement_reason") or "").strip():
                divergences.append(
                    {"slot_id": slot_id, "kind": "removed_without_coded_reason", "section": row.get("section")}
                )
        else:
            unfilled += 1
            divergences.append({"slot_id": slot_id, "kind": f"slot_status_{status or 'unknown'}"})

    foreign_lines = sorted(visible_idents - accounted_idents)
    for ident in foreign_lines[:20]:
        divergences.append({"kind": "line_outside_plan", "url_identity": ident})

    empty_bullets = [ln for ln in html_text.splitlines() if ln.strip() in {"•", "• "}]
    if empty_bullets:
        divergences.append({"kind": "empty_bullets", "count": len(empty_bullets)})
    if not lead_visible:
        divergences.append({"kind": "lead_not_visible"})

    for d in divergences:
        warnings.append(f"verify: {d.get('kind')} — {d.get('slot_id') or d.get('detail') or d.get('url_identity') or ''}")

    ok = not technical_errors
    ship_degraded = bool(divergences)
    write_json(
        report_path,
        {
            "schema_version": 1,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "pipeline_run_id": str(plan.get("pipeline_run_id") or ""),
            "digest_path": str(digest_path),
            "ok_technical": ok,
            "ship_degraded": ship_degraded,
            "technical_errors": technical_errors,
            "counts": {
                "slots": len(slot_rows),
                "shown": shown,
                "replaced": replaced,
                "removed": removed,
                "unfilled": unfilled,
                "visible_source_links": len(visible_idents),
                "lines_outside_plan": len(foreign_lines),
                "empty_bullets": len(empty_bullets),
            },
            "lead_visible": lead_visible,
            "divergences": divergences[:120],
            "warnings": warnings[:120],
            "policy": (
                "Контентные расхождения не отменяют выпуск (ship_degraded + warning); "
                "технически негодный артефакт не отправляется."
            ),
        },
    )
    if technical_errors:
        return VerifyResult(False, "; ".join(technical_errors)[:300], report_path)
    message = (
        f"Plan conformance: {shown} shown, {replaced} replaced, {removed} removed"
        + (f", {len(divergences)} divergence(s) — ship_degraded" if divergences else " — clean")
    )
    return VerifyResult(True, message, report_path)
