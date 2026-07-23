"""Этап 3: исполнение слот-плана — единый recovery-контроллер.

Один модуль обслуживает писателя, pre-send repair executor и финальный verify:

* читает неизменяемый ``release_plan.json`` (его пишет только plan-digest);
* ведёт ``plan_execution_report.json`` — что случилось с каждым слотом
  (shown / replaced / removed), кем и почему;
* выдаёт следующего запасного из цепочки слота с перепроверкой срока и
  обязательных полей (порядок цепочки НЕ пересчитывается);
* держит общий бюджет попыток ремонта на выпуск.

Ни одна функция здесь не меняет план. Состав меняется только заменой на
запасного из цепочки слота или снятием слота по кодифицированной причине.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import html
from pathlib import Path
import re
from typing import Any

from news_digest.pipeline.common import (
    candidates_by_fingerprint,
    canonical_url_identity,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json_atomic,
)

# Кодифицированные причины снятия строки (единственные допустимые).
REMOVAL_REASONS = frozenset(
    {
        "unsupported_fact",
        "missing_required_facts",
        "expired_after_plan",
        "source_invalidated",
        "fact_lock_failed",
        "duplicate_after_plan",
        "unrenderable_line",
    }
)

# Общий бюджет ремонтов на выпуск (писатель + редактор + судья вместе).
SHARED_REPAIR_BUDGET_PER_RUN = 8

PLAN_FILE = "release_plan.json"
EXECUTION_FILE = "plan_execution_report.json"
FINAL_REPORT_FILE = "final_selection_report.json"
_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)


def plan_path(state_dir: Path) -> Path:
    return state_dir / PLAN_FILE


def execution_path(state_dir: Path) -> Path:
    return state_dir / EXECUTION_FILE


def load_plan(state_dir: Path) -> dict[str, Any]:
    payload = read_json(plan_path(state_dir), {})
    return payload if isinstance(payload, dict) else {}


def plan_slots(plan: dict[str, Any]) -> list[dict[str, Any]]:
    slots = plan.get("slots")
    return [s for s in slots if isinstance(s, dict)] if isinstance(slots, list) else []


def load_execution(state_dir: Path) -> dict[str, Any]:
    payload = read_json(execution_path(state_dir), {})
    if not isinstance(payload, dict) or not payload.get("slots"):
        payload = {
            "schema_version": 2,
            "run_date_london": today_london(),
            "pipeline_run_id": "",
            "slots": {},
            "repair_attempts_used": 0,
            "events": [],
        }
    return payload


def save_execution(state_dir: Path, execution: dict[str, Any]) -> None:
    execution["updated_at_london"] = now_london().isoformat()
    write_json_atomic(execution_path(state_dir), execution)


def init_execution(state_dir: Path, plan: dict[str, Any]) -> dict[str, Any]:
    """Writer calls this once: every slot starts as pending."""
    execution = {
        "schema_version": 2,
        "run_date_london": today_london(),
        "pipeline_run_id": pipeline_run_id_from(plan),
        "slots": {
            str(slot.get("slot_id") or ""): {
                "slot_id": str(slot.get("slot_id") or ""),
                "section": str(slot.get("section") or ""),
                "planned_block": str(slot.get("block") or ""),
                "original_fingerprint": str(slot.get("primary_fingerprint") or ""),
                "status": "pending",
                "final_fingerprint": "",
                "replacement_reason": "",
                "failed_attempts": [],
                "stage": "",
            }
            for slot in plan_slots(plan)
        },
        "repair_attempts_used": 0,
        "events": [],
    }
    lead = plan.get("lead") if isinstance(plan.get("lead"), dict) else {}
    if lead.get("primary_fingerprint"):
        execution["slots"]["lead"] = {
            "slot_id": "lead",
            "section": "Главная история дня",
            "planned_block": "lead_story",
            "original_fingerprint": str(lead.get("primary_fingerprint") or ""),
            "status": "pending",
            "final_fingerprint": "",
            "replacement_reason": "",
            "failed_attempts": [],
            "stage": "",
        }
    save_execution(state_dir, execution)
    return execution


def record_outcome(
    execution: dict[str, Any],
    slot_id: str,
    *,
    status: str,
    final_fingerprint: str = "",
    reason: str = "",
    stage: str = "",
    failed_fingerprint: str = "",
) -> None:
    """status: shown | replaced | removed | unfilled."""
    row = execution.setdefault("slots", {}).setdefault(
        str(slot_id),
        {"slot_id": str(slot_id), "section": "", "status": "pending", "failed_attempts": []},
    )
    if failed_fingerprint:
        attempts = row.setdefault("failed_attempts", [])
        if isinstance(attempts, list):
            attempts.append({"fingerprint": failed_fingerprint, "reason": reason[:200], "stage": stage})
    if status:
        row["status"] = status
        row["final_fingerprint"] = final_fingerprint
        if reason:
            row["replacement_reason"] = reason[:300]
        row["stage"] = stage


def record_repair(
    execution: dict[str, Any],
    slot_id: str,
    *,
    status: str,
    reason: str,
    stage: str = "judge",
) -> None:
    """Record a post-plan prose/fact repair without changing slot composition."""
    row = execution.setdefault("slots", {}).setdefault(
        str(slot_id),
        {"slot_id": str(slot_id), "section": "", "status": "pending", "failed_attempts": []},
    )
    changed = row.get("repair_status") != str(status or "") or row.get("repair_reason") != str(reason or "")[:300]
    row["repair_status"] = str(status or "")
    row["repair_reason"] = str(reason or "")[:300]
    row["stage"] = stage
    events = execution.setdefault("events", [])
    if changed and isinstance(events, list):
        events.append(
            {
                "slot_id": str(slot_id),
                "event": "repair_post_check",
                "status": str(status or ""),
                "reason": str(reason or "")[:300],
                "stage": stage,
            }
        )


def repair_budget_left(execution: dict[str, Any]) -> int:
    used = int(execution.get("repair_attempts_used") or 0)
    return max(0, SHARED_REPAIR_BUDGET_PER_RUN - used)


def consume_repair_attempt(execution: dict[str, Any]) -> bool:
    if repair_budget_left(execution) <= 0:
        return False
    execution["repair_attempts_used"] = int(execution.get("repair_attempts_used") or 0) + 1
    return True


def _backup_still_valid(candidate: dict[str, Any]) -> tuple[bool, str]:
    """Re-check срок/дата/обязательные поля перед вводом запасного.

    Порядок цепочки не пересчитывается — только валидность конкретного
    кандидата на момент замены.
    """
    if not isinstance(candidate, dict):
        return False, "missing_candidate"
    if str(candidate.get("freshness_status") or "") == "stale":
        return False, "stale"
    if candidate.get("synthetic_stale"):
        return False, "stale_synthetic"
    if not candidate.get("source_url") or not candidate.get("source_label"):
        return False, "missing_source_reference"
    try:
        from news_digest.pipeline.writer import (  # noqa: PLC0415
            _is_expired_event_candidate,
            _is_outside_current_weekend_candidate,
        )

        if _is_expired_event_candidate(candidate, str(candidate.get("draft_line") or "")):
            return False, "expired_after_plan"
        if _is_outside_current_weekend_candidate(candidate):
            return False, "outside_section_window"
    except Exception:  # noqa: BLE001 — validity check must never crash a swap
        pass
    return True, ""


def next_backup(
    plan: dict[str, Any],
    execution: dict[str, Any],
    slot_id: str,
    candidates_by_fp: dict[str, dict],
    used_fingerprints: set[str],
) -> tuple[dict[str, Any] | None, str]:
    """Следующий валидный запасной слота, в порядке плана."""
    slot = next((s for s in plan_slots(plan) if str(s.get("slot_id") or "") == str(slot_id)), None)
    if slot is None and str(slot_id) == "lead":
        lead = plan.get("lead") if isinstance(plan.get("lead"), dict) else {}
        backups = [str(fp) for fp in lead.get("understudy_fingerprints") or []]
    else:
        backups = [str(fp) for fp in (slot or {}).get("backup_fingerprints") or []]
    row = (execution.get("slots") or {}).get(str(slot_id)) or {}
    already_failed = {
        str(a.get("fingerprint") or "")
        for a in row.get("failed_attempts") or []
        if isinstance(a, dict)
    }
    for fp in backups:
        if not fp or fp in used_fingerprints or fp in already_failed:
            continue
        candidate = candidates_by_fp.get(fp)
        ok, invalid_reason = _backup_still_valid(candidate or {})
        if not ok:
            record_outcome(
                execution,
                slot_id,
                status="",
                failed_fingerprint=fp,
                reason=f"backup_invalid:{invalid_reason}",
                stage="controller",
            )
            continue
        return candidate, fp
    return None, ""


def normalize_removal_reason(reason: str) -> str:
    reason = str(reason or "").strip()
    return reason if reason in REMOVAL_REASONS else "unrenderable_line"


def _candidate_ref(candidate: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(candidate, dict):
        return {"fingerprint": "", "title": "", "source_label": "", "source_url": ""}
    return {
        "fingerprint": str(candidate.get("fingerprint") or ""),
        "title": str(candidate.get("title") or "")[:180],
        "source_label": str(candidate.get("source_label") or ""),
        "source_url": str(candidate.get("source_url") or ""),
    }


def _html_item_rows(html_text: str) -> list[dict[str, Any]]:
    """Return one row per visible digest card, including the non-bullet lead."""
    rows: list[dict[str, Any]] = []
    current_section = ""
    for raw_index, raw_line in enumerate(html_text.splitlines(), start=1):
        line = raw_line.strip()
        heading = re.fullmatch(r"<b>([^<]+)</b>", line)
        if heading:
            title = html.unescape(heading.group(1)).strip()
            if title.startswith("Greater Manchester Brief"):
                current_section = ""
            else:
                current_section = title
            continue
        is_lead = current_section == "Главная история дня" and line.startswith("<b>")
        if not line.startswith("• ") and not is_lead:
            continue
        urls = [html.unescape(value) for value in _HREF_RE.findall(line)]
        rows.append(
            {
                "html_row": len(rows) + 1,
                "html_line": raw_index,
                "section": current_section,
                "html": line,
                "urls": urls,
                "url_identities": [canonical_url_identity(value) for value in urls if value],
            }
        )
    return rows


def _slot_specs(plan: dict[str, Any]) -> list[dict[str, Any]]:
    specs = [dict(slot) for slot in plan_slots(plan)]
    lead = plan.get("lead") if isinstance(plan.get("lead"), dict) else {}
    if str(lead.get("primary_fingerprint") or ""):
        specs.append(
            {
                "slot_id": "lead",
                "section": "Главная история дня",
                "block": "lead_story",
                "position": 1,
                "primary_fingerprint": str(lead.get("primary_fingerprint") or ""),
                "backup_fingerprints": [str(fp) for fp in lead.get("understudy_fingerprints") or []],
                "required": True,
                "must_show": True,
            }
        )
    order = {str(name): index for index, name in enumerate(plan.get("ordered_sections") or [])}
    specs.sort(key=lambda row: (order.get(str(row.get("section") or ""), 999), int(row.get("position") or 0)))
    return specs


def build_final_execution_report(
    state_dir: Path,
    final_html: str,
    *,
    write: bool = True,
) -> dict[str, Any]:
    """Reconcile final HTML against slots, consuming every HTML row exactly once.

    Candidate inventory is used only as a fingerprint-to-source lookup. It never
    contributes rows or counts to this final report.
    """
    plan = load_plan(state_dir)
    execution = load_execution(state_dir)
    payload = read_json(state_dir / "candidates.json", {"candidates": []})
    by_fp = candidates_by_fingerprint(payload.get("candidates") or [])
    html_rows = _html_item_rows(final_html)
    unused_rows = set(range(len(html_rows)))
    divergences: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    final_rows: list[dict[str, Any]] = []
    removed_slots: list[dict[str, Any]] = []
    execution_slots = execution.get("slots") if isinstance(execution.get("slots"), dict) else {}

    for spec in _slot_specs(plan):
        slot_id = str(spec.get("slot_id") or "")
        exec_row = execution_slots.get(slot_id) if isinstance(execution_slots, dict) else None
        exec_row = exec_row if isinstance(exec_row, dict) else {}
        section = str(spec.get("section") or exec_row.get("section") or "")
        block = str(spec.get("block") or exec_row.get("planned_block") or "")
        original_fp = str(spec.get("primary_fingerprint") or exec_row.get("original_fingerprint") or "")
        status = str(exec_row.get("status") or "pending")
        final_fp = str(exec_row.get("final_fingerprint") or "")
        original_candidate = by_fp.get(original_fp)
        final_candidate = by_fp.get(final_fp)
        final_ident = canonical_url_identity(str((final_candidate or {}).get("source_url") or ""))
        matched_index: int | None = None

        if status in {"shown", "replaced"}:
            same_section = [
                index
                for index in sorted(unused_rows)
                if final_ident
                and final_ident in (html_rows[index].get("url_identities") or [])
                and str(html_rows[index].get("section") or "") == section
            ]
            if same_section:
                matched_index = same_section[0]
                unused_rows.remove(matched_index)
            else:
                wrong_section = [
                    index
                    for index in sorted(unused_rows)
                    if final_ident and final_ident in (html_rows[index].get("url_identities") or [])
                ]
                divergences.append(
                    {
                        "slot_id": slot_id,
                        "kind": "slot_rendered_in_wrong_section" if wrong_section else "planned_line_missing_from_final_html",
                        "planned_section": section,
                        "actual_section": str(html_rows[wrong_section[0]].get("section") or "") if wrong_section else "",
                        "fingerprint": final_fp,
                    }
                )
        elif status == "removed":
            forbidden_fps = {original_fp, final_fp}
            forbidden_fps.update(
                str(item.get("fingerprint") or "")
                for item in exec_row.get("failed_attempts") or []
                if isinstance(item, dict)
            )
            forbidden_idents = {
                canonical_url_identity(str((by_fp.get(fp) or {}).get("source_url") or ""))
                for fp in forbidden_fps
                if fp
            }
            forbidden_idents.discard("")
            visible_removed = [
                row for row in html_rows
                if forbidden_idents.intersection(set(row.get("url_identities") or []))
            ]
            if visible_removed:
                divergences.append(
                    {
                        "slot_id": slot_id,
                        "kind": "removed_line_still_visible",
                        "planned_section": section,
                        "actual_section": visible_removed[0].get("section") or "",
                    }
                )

        matched = html_rows[matched_index] if matched_index is not None else None
        change_reason = str(exec_row.get("repair_reason") or exec_row.get("replacement_reason") or "")
        outcome = {
            "slot_id": slot_id,
            "planned_section": section,
            "planned_block": block,
            "position": int(spec.get("position") or 0),
            "original_candidate": _candidate_ref(original_candidate),
            "final_candidate": _candidate_ref(final_candidate),
            "status": status,
            "change_reason": change_reason,
            "stage": str(exec_row.get("stage") or ""),
            "repair_status": str(exec_row.get("repair_status") or ""),
            "final_html_url": str((matched or {}).get("urls", [""])[-1] if (matched or {}).get("urls") else ""),
            "final_html_section": str((matched or {}).get("section") or ""),
            "final_html_line": int((matched or {}).get("html_line") or 0),
        }
        outcomes.append(outcome)
        if matched is not None:
            final_rows.append(outcome)
        elif status == "removed":
            removed_slots.append(outcome)

        if isinstance(exec_row, dict) and exec_row:
            exec_row["planned_block"] = block
            exec_row["original_fingerprint"] = original_fp
            exec_row["final_html_url"] = outcome["final_html_url"]
            exec_row["final_html_section"] = outcome["final_html_section"]
            exec_row["final_html_line"] = outcome["final_html_line"]
            exec_row["conformance"] = "matched" if matched is not None else "removed" if status == "removed" else "missing"

    for index in sorted(unused_rows):
        row = html_rows[index]
        divergences.append(
            {
                "kind": "line_outside_plan",
                "actual_section": row.get("section") or "",
                "html_line": row.get("html_line") or 0,
                "url": (row.get("urls") or [""])[-1] if row.get("urls") else "",
            }
        )

    occurrences: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in html_rows:
        for ident in set(row.get("url_identities") or []):
            if ident:
                occurrences[ident].append(row)
    for ident, rows in occurrences.items():
        if len(rows) > 1:
            divergences.append(
                {
                    "kind": "html_line_duplicated",
                    "url_identity": ident,
                    "sections": [str(row.get("section") or "") for row in rows],
                }
            )

    planned_by_section = Counter(str(row.get("planned_section") or "") for row in outcomes)
    final_by_section = Counter(str(row.get("final_html_section") or "") for row in final_rows)
    section_rows: dict[str, dict[str, Any]] = {}
    section_names = list(plan.get("ordered_sections") or [])
    for section in planned_by_section:
        if section not in section_names:
            section_names.append(section)
    for section in section_names:
        summary = (plan.get("sections") or {}).get(section) or {}
        minimum = int(summary.get("min") or 0) if isinstance(summary, dict) else 0
        planned = int(planned_by_section.get(section, 0))
        final = int(final_by_section.get(section, 0))
        section_rows[section] = {
            "minimum": minimum,
            "planned": planned,
            "final": final,
            "planned_shortfall": max(0, minimum - planned),
            "execution_loss": max(0, planned - final),
        }

    status_counts = Counter(str(row.get("status") or "") for row in outcomes)
    payload_out = {
        "schema_version": 2,
        "run_date_london": str(plan.get("run_date_london") or today_london()),
        "pipeline_run_id": str(plan.get("pipeline_run_id") or ""),
        "created_at_london": now_london().isoformat(),
        "policy": (
            "Final composition is reconciled slot-by-slot after all repairs. "
            "Each HTML row is consumed at most once; candidates are lookup data, not report rows."
        ),
        "counts": {
            "slots": len(outcomes),
            "shown": int(status_counts.get("shown", 0)),
            "replaced": int(status_counts.get("replaced", 0)),
            "removed": int(status_counts.get("removed", 0)),
            "final_html_rows": len(html_rows),
            "final_report_rows": len(final_rows),
            "lines_outside_plan": len(unused_rows),
        },
        "sections": section_rows,
        "final_rows": sorted(final_rows, key=lambda row: int(row.get("final_html_line") or 0)),
        "removed_slots": removed_slots,
        "slot_outcomes": outcomes,
        "divergences": divergences,
    }
    if len(final_rows) != len(html_rows):
        payload_out["divergences"].append(
            {
                "kind": "final_report_row_count_mismatch",
                "html_rows": len(html_rows),
                "report_rows": len(final_rows),
            }
        )
    if write:
        save_execution(state_dir, execution)
        write_json_atomic(state_dir / FINAL_REPORT_FILE, payload_out)
    return payload_out
