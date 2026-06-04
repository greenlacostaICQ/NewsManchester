from __future__ import annotations

from datetime import date, datetime, timedelta
import re

from news_digest.pipeline.common import now_london


PRACTICAL_BACKFILL_VERSION = 1
WEEKEND_TARGET_MIN = 8
WEEKEND_BACKFILL_LIMIT = 12


def _parse_day(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _event_day(candidate: dict) -> date | None:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    for value in (
        event.get("date_start"),
        event.get("date"),
        candidate.get("published_at"),
    ):
        day = _parse_day(value)
        if day:
            return day
    return None


def _weekend_window(today: date) -> tuple[date, date]:
    days_to_sat = (5 - today.weekday()) % 7
    start = today + timedelta(days=days_to_sat)
    return start, start + timedelta(days=1)


def _is_eventish(candidate: dict) -> bool:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    return category in {"culture_weekly", "venues_tickets", "russian_speaking_events", "diaspora_events"} or block in {
        "next_7_days", "future_announcements", "ticket_radar", "russian_events"
    }


def _count_included(candidates: list[dict], block: str) -> int:
    return sum(1 for c in candidates if isinstance(c, dict) and c.get("include") and str(c.get("primary_block") or "") == block)


def _mark_backfilled(candidate: dict, *, from_block: str, to_block: str, reason: str) -> None:
    candidate["primary_block"] = to_block
    candidate["practical_backfill"] = {
        "version": PRACTICAL_BACKFILL_VERSION,
        "from_block": from_block,
        "to_block": to_block,
        "reason": reason,
    }
    existing = str(candidate.get("reason") or "").strip()
    note = f"Practical backfill: {from_block} → {to_block} ({reason})."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note


def _backfill_weekend(candidates: list[dict], today: date) -> int:
    if today.weekday() < 3 or _count_included(candidates, "weekend_activities") >= WEEKEND_TARGET_MIN:
        return 0
    start, end = _weekend_window(today)
    promoted = 0
    target = max(0, WEEKEND_TARGET_MIN - _count_included(candidates, "weekend_activities"))
    limit = min(WEEKEND_BACKFILL_LIMIT, target)
    for candidate in candidates:
        if promoted >= limit:
            break
        if not isinstance(candidate, dict) or not candidate.get("include") or not _is_eventish(candidate):
            continue
        block = str(candidate.get("primary_block") or "")
        if block == "weekend_activities":
            continue
        day = _event_day(candidate)
        blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text"))
        recurring_weekend = bool(re.search(r"\b(?:every|weekly|saturdays?|sundays?|weekend)\b", blob, re.IGNORECASE))
        if (day and start <= day <= end) or recurring_weekend:
            _mark_backfilled(candidate, from_block=block, to_block="weekend_activities", reason="weekend practical layer was thin")
            promoted += 1
    return promoted


def _backfill_next_7_days(candidates: list[dict], today: date) -> int:
    if _count_included(candidates, "next_7_days") >= 2:
        return 0
    promoted = 0
    horizon = today + timedelta(days=7)
    for candidate in candidates:
        if promoted >= 4:
            break
        if not isinstance(candidate, dict) or not candidate.get("include") or not _is_eventish(candidate):
            continue
        block = str(candidate.get("primary_block") or "")
        if block in {"next_7_days", "weekend_activities"}:
            continue
        day = _event_day(candidate)
        if day and today <= day <= horizon:
            _mark_backfilled(candidate, from_block=block, to_block="next_7_days", reason="7-day practical layer was thin")
            promoted += 1
    return promoted


def _backfill_today_focus(candidates: list[dict]) -> int:
    if _count_included(candidates, "today_focus") >= 2:
        return 0
    promoted = 0
    for candidate in candidates:
        if promoted >= 3:
            break
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        block = str(candidate.get("primary_block") or "")
        if block in {"today_focus", "weather", "transport"}:
            continue
        why_now = str(candidate.get("why_now") or "")
        action = str(candidate.get("reader_action_type") or "")
        if why_now in {"happening_today", "deadline_soon"} or action in {"check_route", "note_deadline", "plan_today"}:
            _mark_backfilled(candidate, from_block=block, to_block="today_focus", reason="today practical layer was thin")
            promoted += 1
    return promoted


def apply_practical_backfill(candidates: list[dict]) -> dict[str, int]:
    today = now_london().date()
    summary = {
        "weekend_activities": _backfill_weekend(candidates, today),
        "next_7_days": _backfill_next_7_days(candidates, today),
        "today_focus": _backfill_today_focus(candidates),
    }
    return {key: value for key, value in summary.items() if value}
