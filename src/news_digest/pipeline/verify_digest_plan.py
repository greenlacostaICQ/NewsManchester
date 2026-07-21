"""Этап 3: финальная сверка перед отправкой — «выпуск = план?».

Запускается ПОСЛЕ предsend-судьи (последнего, кто правит слова) и до
send-file. Сравнивает ФИНАЛЬНЫЙ отправляемый HTML с неизменяемым планом
и отчётом исполнения.

Плановый недобор и кодифицированные снятия уходят в ship_degraded. Ошибка
исполнения плана или известная фактическая ошибка после ремонта блокирует
отправку:
  * плана нет или он от другого pipeline_run_id;
  * шапка выпуска не за сегодняшний день;
  * HTML пуст или без единой ссылки-источника.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path

from news_digest.pipeline.common import (
    extract_sections,
    now_london,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.plan_execution import (
    build_final_execution_report,
    load_execution,
    load_plan,
    plan_slots,
)

REPORT_NAME = "verify_digest_plan_report.json"
_HREF_RE = re.compile(r'href="([^"]+)"')
_MASTHEAD_RE = re.compile(r"^<b>Greater Manchester Brief — (\d{4}-\d{2}-\d{2})")


@dataclass
class VerifyResult:
    ok: bool
    message: str
    report_path: Path


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
        # Fail-closed: сверка без отчёта исполнения — это не «чисто», это
        # «мы не знаем, что отправляем». Блокирует отправку.
        from news_digest.pipeline.plan_execution import execution_path  # noqa: PLC0415

        exec_slots = execution.get("slots") or {}
        if not execution_path(state_dir).exists() or not exec_slots:
            technical_errors.append("plan_execution_report.json is missing or empty — исполнение плана неизвестно.")
        plan_run = str(plan.get("pipeline_run_id") or "")
        exec_run = str(execution.get("pipeline_run_id") or "")
        if plan_run != exec_run:
            technical_errors.append(
                f"Plan/run mismatch: release_plan {plan_run!r} vs plan_execution {exec_run!r}."
            )
        plan_day = str(plan.get("run_date_london") or "")
        if plan_day and plan_day != today_london():
            technical_errors.append(f"release_plan is for {plan_day}, today is {today_london()}.")
        exec_day = str(execution.get("run_date_london") or "")
        if exec_day and exec_day != today_london():
            technical_errors.append(f"plan_execution is for {exec_day}, today is {today_london()}.")
        expected_rows = len(plan_slots(plan)) + (
            1 if str((plan.get("lead") or {}).get("primary_fingerprint") or "") else 0
        )
        if exec_slots and len(exec_slots) != expected_rows:
            technical_errors.append(
                f"Execution covers {len(exec_slots)} slot(s), plan expects {expected_rows} — исполнение неполно."
            )
        bad_statuses = sorted({
            str((row or {}).get("status") or "unknown")
            for row in exec_slots.values()
            if str((row or {}).get("status") or "") not in {"shown", "replaced", "removed"}
        })
        if bad_statuses:
            technical_errors.append(
                f"Execution has unfinished slot status(es): {', '.join(bad_statuses)} — конвейер не дошёл до конца."
            )
    # Структура Telegram-HTML: битые теги ломают отправку/рендер — технический брак.
    if html_text:
        open_a = len(re.findall(r"<a\s", html_text))
        close_a = html_text.count("</a>")
        open_b = html_text.count("<b>")
        close_b = html_text.count("</b>")
        if open_a != close_a:
            technical_errors.append(f"Telegram HTML broken: <a>={open_a} vs </a>={close_a}.")
        if open_b != close_b:
            technical_errors.append(f"Telegram HTML broken: <b>={open_b} vs </b>={close_b}.")
    masthead = _MASTHEAD_RE.match(html_text.splitlines()[0].strip() if html_text else "")
    if html_text and not masthead:
        technical_errors.append("Masthead line is missing from the final HTML.")
    elif masthead and masthead.group(1) != today_london():
        technical_errors.append(
            f"Masthead date {masthead.group(1)} is not today ({today_london()}) — stale artifact."
        )

    # --- Сверка исполнения: каждая HTML-строка потребляется одним слотом ----
    sections = extract_sections(html_text)
    lead_visible = bool(sections.get("Главная история дня"))
    final_selection = build_final_execution_report(state_dir, html_text, write=True)
    divergences.extend(final_selection.get("divergences") or [])
    final_counts = final_selection.get("counts") or {}
    shown = int(final_counts.get("shown") or 0)
    replaced = int(final_counts.get("replaced") or 0)
    removed = int(final_counts.get("removed") or 0)
    unfilled = max(0, int(final_counts.get("slots") or 0) - shown - replaced - removed)

    # These are technical composition defects: unlike an honest coded removal,
    # they mean the HTML is not a faithful execution of the immutable plan.
    blocking_plan_kinds = {
        "planned_line_missing_from_final_html",
        "slot_rendered_in_wrong_section",
        "removed_line_still_visible",
        "line_outside_plan",
        "html_line_duplicated",
        "final_report_row_count_mismatch",
    }
    for divergence in divergences:
        if str(divergence.get("kind") or "") in blocking_plan_kinds:
            technical_errors.append(
                "Plan execution mismatch: "
                f"{divergence.get('kind')} ({divergence.get('slot_id') or divergence.get('url') or ''})."
            )
    for row in (execution.get("slots") or {}).values():
        if isinstance(row, dict) and str(row.get("status") or "") == "removed" and not str(row.get("replacement_reason") or "").strip():
            technical_errors.append(f"Removed slot {row.get('slot_id') or '?'} has no coded reason.")

    empty_bullets = [ln for ln in html_text.splitlines() if ln.strip() in {"•", "• "}]
    if empty_bullets:
        divergences.append({"kind": "empty_bullets", "count": len(empty_bullets)})
    if not lead_visible:
        divergences.append({"kind": "lead_not_visible"})

    actual_section_counts = {section: len(lines) for section, lines in sections.items()}
    shortfalls = final_selection.get("sections") or {}
    for section, summary in shortfalls.items():
        if not isinstance(summary, dict):
            continue
        if int(summary.get("planned_shortfall") or 0):
            divergences.append({"kind": "planned_shortfall", "section": section, **summary})
        if int(summary.get("execution_loss") or 0):
            divergences.append({"kind": "execution_loss", "section": section, **summary})

    quality_report = read_json(state_dir / "pre_send_quality_report.json", {})
    repair_report = quality_report.get("repair_executor") if isinstance(quality_report, dict) else {}
    repair_report = repair_report if isinstance(repair_report, dict) else {}
    blocking_unresolved = int(repair_report.get("blocking_unresolved") or 0)
    if blocking_unresolved:
        technical_errors.append(
            f"Pre-send repair has {blocking_unresolved} unresolved known factual error operation(s)."
        )

    a_tier_rows = [
        candidate for candidate in by_fp.values()
        if str(candidate.get("a_tier_policy_status") or "") == "must_show"
    ]
    a_tier_visible = []
    a_tier_missing = []
    final_fps = {
        str((row.get("final_candidate") or {}).get("fingerprint") or "")
        for row in final_selection.get("final_rows") or []
        if isinstance(row, dict)
    }
    for candidate in a_tier_rows:
        fp = str(candidate.get("fingerprint") or "")
        if fp in final_fps:
            a_tier_visible.append(fp)
        else:
            a_tier_missing.append(fp)
            divergences.append(
                {
                    "kind": "a_tier_missing_from_final_html",
                    "fingerprint": fp,
                    "section": candidate.get("plan_section") or candidate.get("primary_block"),
                    "detail": str(candidate.get("title") or "")[:140],
                }
            )

    for d in divergences:
        warnings.append(f"verify: {d.get('kind')} — {d.get('slot_id') or d.get('detail') or d.get('url_identity') or ''}")

    ok = not technical_errors
    ship_degraded = bool(divergences or int(repair_report.get("unresolved") or 0)) and not technical_errors
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
                "slots": int(final_counts.get("slots") or 0),
                "shown": shown,
                "replaced": replaced,
                "removed": removed,
                "unfilled": unfilled,
                "visible_source_links": len(_HREF_RE.findall(html_text)),
                "lines_outside_plan": int(final_counts.get("lines_outside_plan") or 0),
                "empty_bullets": len(empty_bullets),
            },
            "actual_section_counts": actual_section_counts,
            "a_tier_conservation": {
                "eligible": len(a_tier_rows),
                "visible": len(a_tier_visible),
                "missing": a_tier_missing,
                "conserved": not a_tier_missing,
            },
            "shortfalls": shortfalls,
            "final_selection_report": {
                "path": str((state_dir / "final_selection_report.json").resolve()),
                "schema_version": final_selection.get("schema_version"),
                "counts": final_counts,
            },
            "lead_visible": lead_visible,
            "divergences": divergences[:120],
            "warnings": warnings[:120],
            "policy": (
                "Плановый недобор и кодифицированные снятия дают ship_degraded; "
                "строка вне слота/блока, повторное использование HTML-строки и unresolved fact error блокируют отправку."
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
