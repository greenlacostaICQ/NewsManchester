"""Этап 3: финальная сверка перед отправкой — «выпуск = план?».

Запускается ПОСЛЕ предsend-судьи (последнего, кто правит слова) и до
send-file. Сравнивает ФИНАЛЬНЫЙ отправляемый HTML с неизменяемым планом
и отчётом исполнения.

Плановый недобор, кодифицированные снятия и unresolved quality findings уходят
в ship_degraded. Отправку блокирует только техническая ошибка исполнения:
  * плана нет или он от другого pipeline_run_id;
  * шапка выпуска не за сегодняшний день;
  * HTML пуст или без единой ссылки-источника.
"""
from __future__ import annotations

from dataclasses import dataclass
import html
import re
from pathlib import Path

from news_digest.pipeline.common import (
    candidates_by_fingerprint,
    extract_sections,
    now_london,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.plan_execution import (
    REMOVAL_REASONS,
    build_final_execution_report,
    load_execution,
    load_plan,
    plan_slots,
)
from news_digest.pipeline.editorial_contracts import classify_prose_defects

REPORT_NAME = "verify_digest_plan_report.json"
_HREF_RE = re.compile(r'href="([^"]+)"')
_MASTHEAD_RE = re.compile(r"^<b>Greater Manchester Brief — (\d{4}-\d{2}-\d{2})")


@dataclass
class VerifyResult:
    ok: bool
    message: str
    report_path: Path


def _final_event_completeness(
    final_rows: list[dict[str, object]],
    candidates: dict[str, dict],
    html_text: str,
) -> dict[str, object]:
    """Check the actual final HTML row consumed by each event slot."""
    from news_digest.pipeline.release import (  # noqa: PLC0415
        _DATE_MARKER_RE,
        _EVENT_SECTIONS_FOR_DATE_CHECK,
    )

    html_lines = html_text.splitlines()
    counts = {"checked": 0, "missing_date": 0, "missing_venue": 0}
    issues: list[dict[str, object]] = []
    for row in final_rows:
        if not isinstance(row, dict):
            continue
        section = str(row.get("final_html_section") or row.get("planned_section") or "")
        if section not in _EVENT_SECTIONS_FOR_DATE_CHECK:
            continue
        ref = row.get("final_candidate") if isinstance(row.get("final_candidate"), dict) else {}
        fingerprint = str(ref.get("fingerprint") or "")
        candidate = candidates.get(fingerprint) or {}
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        line_number = int(row.get("final_html_line") or 0)
        rendered_html = html_lines[line_number - 1] if 0 < line_number <= len(html_lines) else ""
        visible = html.unescape(re.sub(r"<[^>]+>", " ", rendered_html))
        visible = re.sub(r"\s+", " ", visible).strip()
        counts["checked"] += 1

        has_date_fact = bool(
            event.get("date_iso")
            or event.get("date_text")
            or event.get("date")
            or event.get("date_start")
            or event.get("next_occurrence")
            or event.get("is_recurring")
        )
        if has_date_fact and not _DATE_MARKER_RE.search(visible):
            counts["missing_date"] += 1
            issues.append(
                {
                    "slot_id": row.get("slot_id") or "",
                    "fingerprint": fingerprint,
                    "final_html_line": line_number,
                    "issue": "missing_date",
                    "rendered_text": visible[:240],
                }
            )

        venue = str(event.get("venue") or "").strip()
        if venue and len(venue) >= 4:
            variants = {
                venue,
                venue.split(",", 1)[0].strip(),
                re.sub(r"\s*\([^)]*\)\s*", " ", venue).strip(),
            }
            normal_visible = re.sub(r"[^a-zа-яё0-9]+", " ", visible.lower()).strip()
            venue_visible = any(
                re.sub(r"[^a-zа-яё0-9]+", " ", variant.lower()).strip() in normal_visible
                for variant in variants
                if len(variant) >= 4
            )
            if not venue_visible:
                counts["missing_venue"] += 1
                issues.append(
                    {
                        "slot_id": row.get("slot_id") or "",
                        "fingerprint": fingerprint,
                        "final_html_line": line_number,
                        "issue": "missing_venue",
                        "expected_venue": venue,
                        "rendered_text": visible[:240],
                    }
                )
    return {
        "scope": "final_html_slot_fingerprint",
        "counts": counts,
        "issues": issues[:60],
    }


def _final_source_funnel(
    scan_report: dict,
    candidates: list[dict],
    plan: dict,
    writer_report: dict,
    final_selection: dict,
) -> dict[str, object]:
    """One monotonic per-source funnel ending at the verified HTML."""
    rows: dict[str, dict[str, object]] = {}

    def _row(name: str) -> dict[str, object]:
        return rows.setdefault(
            name,
            {
                "name": name,
                "raw": 0,
                "curated": 0,
                "ranked": 0,
                "planned": 0,
                "written": 0,
                "final": 0,
            },
        )

    for category in (scan_report.get("categories") or {}).values():
        if not isinstance(category, dict):
            continue
        for source in category.get("source_health") or []:
            if not isinstance(source, dict):
                continue
            name = str(source.get("name") or "").strip()
            if name:
                record = _row(name)
                record["raw"] = int(record["raw"]) + int(source.get("candidate_count") or 0)

    by_fp = {
        str(candidate.get("fingerprint") or ""): candidate
        for candidate in candidates
        if isinstance(candidate, dict) and str(candidate.get("fingerprint") or "")
    }
    planned_fps: set[str] = set()
    for slot in plan_slots(plan):
        planned_fps.add(str(slot.get("primary_fingerprint") or ""))
        planned_fps.update(str(fp) for fp in (slot.get("backup_fingerprints") or []) if str(fp))
    lead = plan.get("lead") if isinstance(plan.get("lead"), dict) else {}
    planned_fps.add(str(lead.get("primary_fingerprint") or ""))
    planned_fps.update(str(fp) for fp in (lead.get("understudy_fingerprints") or []) if str(fp))
    planned_fps.discard("")
    written_fps = set(writer_report.get("rendered_candidate_fingerprints") or [])
    final_fps = {
        str((row.get("final_candidate") or {}).get("fingerprint") or "")
        for row in final_selection.get("final_rows") or []
        if isinstance(row, dict) and isinstance(row.get("final_candidate"), dict)
    }

    for fingerprint, candidate in by_fp.items():
        name = str(candidate.get("source_label") or "").strip()
        if not name:
            continue
        record = _row(name)
        curated = bool(
            candidate.get("curated_for_rank")
            if "curated_for_rank" in candidate
            else candidate.get("include") or candidate.get("digest_selection_verdict")
        )
        ranked = str(candidate.get("digest_selection_verdict") or "") in {"selected", "reserve"}
        if curated:
            record["curated"] = int(record["curated"]) + 1
        if ranked:
            record["ranked"] = int(record["ranked"]) + 1
        if fingerprint in planned_fps:
            record["planned"] = int(record["planned"]) + 1
        if fingerprint in written_fps:
            record["written"] = int(record["written"]) + 1
        if fingerprint in final_fps:
            record["final"] = int(record["final"]) + 1

    loss_counts: dict[str, int] = {}
    for record in rows.values():
        if int(record["final"]) > 0:
            record["loss_stage"] = ""
            record["loss_reason"] = ""
            continue
        if int(record["curated"]) == 0:
            stage, reason = "curated", "raw candidates did not survive curation"
        elif int(record["ranked"]) == 0:
            stage, reason = "ranked", "curated candidates did not survive rank selection"
        elif int(record["planned"]) == 0:
            stage, reason = "planned", "ranked candidates were not selected by planner"
        elif int(record["written"]) == 0:
            stage, reason = "written", "planned candidates did not reach writer HTML"
        else:
            stage, reason = "final", "written candidates were removed after writing"
        record["loss_stage"] = stage
        record["loss_reason"] = reason
        loss_counts[stage] = loss_counts.get(stage, 0) + 1
    return {
        "scope": "verified_final_html",
        "columns": ["raw", "curated", "ranked", "planned", "written", "final"],
        "loss_counts": loss_counts,
        "sources": sorted(rows.values(), key=lambda row: str(row.get("name") or "")),
    }


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
    candidates = payload.get("candidates", [])
    by_fp = candidates_by_fingerprint(candidates)

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
        if not isinstance(row, dict) or str(row.get("status") or "") != "removed":
            continue
        reason = str(row.get("replacement_reason") or "").strip()
        if reason not in REMOVAL_REASONS:
            technical_errors.append(
                f"Removed slot {row.get('slot_id') or '?'} has invalid coded reason: {reason or 'missing'}."
            )

    empty_bullets = [ln for ln in html_text.splitlines() if ln.strip() in {"•", "• "}]
    if empty_bullets:
        divergences.append({"kind": "empty_bullets", "count": len(empty_bullets)})
    if not lead_visible:
        divergences.append({"kind": "lead_not_visible"})
    prose_findings: list[dict[str, object]] = []
    for line_index, line in enumerate(html_text.splitlines(), start=1):
        if not line.strip().startswith("•"):
            continue
        for finding in classify_prose_defects(line):
            prose_findings.append({"line_index": line_index, **finding})
            divergences.append({
                "kind": "prose_policy_defect",
                "line_index": line_index,
                **finding,
            })

    actual_section_counts = {section: len(lines) for section, lines in sections.items()}
    shortfalls = final_selection.get("sections") or {}
    for section, summary in shortfalls.items():
        if not isinstance(summary, dict):
            continue
        if int(summary.get("planned_shortfall") or 0):
            divergences.append({"kind": "planned_shortfall", "section": section, **summary})
        if int(summary.get("execution_loss") or 0):
            divergences.append({"kind": "execution_loss", "section": section, **summary})

    event_completeness = _final_event_completeness(
        final_selection.get("final_rows") or [],
        by_fp,
        html_text,
    )
    source_funnel = _final_source_funnel(
        read_json(state_dir / "collector_report.json", {}),
        candidates,
        plan,
        read_json(state_dir / "writer_report.json", {}),
        final_selection,
    )

    quality_report = read_json(state_dir / "pre_send_quality_report.json", {})
    repair_report = quality_report.get("repair_executor") if isinstance(quality_report, dict) else {}
    repair_report = repair_report if isinstance(repair_report, dict) else {}
    blocking_unresolved = int(repair_report.get("blocking_unresolved") or 0)
    if blocking_unresolved:
        divergences.append(
            {
                "kind": "unresolved_known_factual",
                "count": blocking_unresolved,
                "detail": (
                    f"{blocking_unresolved} known factual repair operation(s) remain unresolved; "
                    "quality degradation never blocks delivery"
                ),
            }
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
            "schema_version": 2,
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
            "event_completeness": event_completeness,
            "source_funnel": source_funnel,
            "prose_policy": {
                "checked_lines": sum(1 for line in html_text.splitlines() if line.strip().startswith("•")),
                "defect_count": len(prose_findings),
                "findings": prose_findings[:60],
            },
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
                "quality findings никогда не блокируют выпуск; только отсутствие/чужой run, "
                "битый HTML или техническое расхождение плана блокируют отправку."
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
