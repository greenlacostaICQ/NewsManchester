"""Этап 3: исполнение слот-плана — единый recovery-контроллер.

Один модуль обслуживает писателя, редактора и предsend-судью:

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

from pathlib import Path
from typing import Any

from news_digest.pipeline.common import (
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
        "unrenderable_line",
    }
)

# Общий бюджет ремонтов на выпуск (писатель + редактор + судья вместе).
SHARED_REPAIR_BUDGET_PER_RUN = 8

PLAN_FILE = "release_plan.json"
EXECUTION_FILE = "plan_execution_report.json"


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
            "schema_version": 1,
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
        "schema_version": 1,
        "run_date_london": today_london(),
        "pipeline_run_id": pipeline_run_id_from(plan),
        "slots": {
            str(slot.get("slot_id") or ""): {
                "slot_id": str(slot.get("slot_id") or ""),
                "section": str(slot.get("section") or ""),
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
        from news_digest.pipeline.writer import _is_expired_event_candidate  # noqa: PLC0415

        if _is_expired_event_candidate(candidate, str(candidate.get("draft_line") or "")):
            return False, "expired_after_plan"
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
