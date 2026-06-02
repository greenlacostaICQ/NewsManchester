from __future__ import annotations

import re


READER_ACTION_VERSION = 1


def _blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text", "primary_block", "category")
    ).lower()


def classify_reader_action(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return "just_know"
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    why_now = str(candidate.get("why_now") or "")
    change_phase = str(candidate.get("change_phase") or "")
    blob = _blob(candidate)

    if block == "transport" or category == "transport":
        return "check_route"
    if re.search(r"\b(?:closure|closed|diversion|avoid|disruption|delay|roadworks?|cordon|strike)\b", blob):
        return "avoid_or_check"
    if change_phase in {"consultation_closing"} or re.search(r"\b(?:deadline|closes?|consultation closes|last chance)\b", blob):
        return "note_deadline"
    if category == "venues_tickets" or block in {"ticket_radar", "outside_gm_tickets"} or change_phase == "tickets_on_sale":
        return "book_or_buy"
    if block == "weekend_activities" or change_phase in {"event_this_week", "starts_today"}:
        return "plan_weekend" if block == "weekend_activities" else "plan_today"
    if block in {"next_7_days", "future_announcements", "russian_events"}:
        return "plan_ahead"
    if why_now in {"happening_today", "deadline_soon"}:
        return "plan_today" if why_now == "happening_today" else "note_deadline"
    if re.search(r"\b(?:police|charged|sentenced|court|murder|stab|fire|crash|missing)\b", blob):
        return "follow_update"
    return "just_know"


def attach_reader_action(candidate: dict) -> dict:
    if isinstance(candidate, dict):
        candidate["reader_action_type"] = classify_reader_action(candidate)
        candidate["reader_action_version"] = READER_ACTION_VERSION
    return candidate
