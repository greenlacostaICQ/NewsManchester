"""Deterministic transport-card fill stage.

Runs after curator-pass and BEFORE llm-rewrite. For every transport
candidate with ``include=True``:

1. Extract a structured ``TransportCard`` (see transport_card.py).
2. Render a Russian Telegram bullet via the deterministic templates.
3. Write the result into ``candidate["draft_line"]``.

The LLM-rewrite stage is then a no-op for transport candidates because
``draft_line`` is already populated. Tier-3 LLM fallback only kicks in
when the extractor failed completely (returned None) — handled inline
by leaving ``draft_line`` empty so the rewrite stage picks it up.

Tram disruptions with a known end_date or duration are persisted to
``data/state/active_tram_disruptions.json``. On subsequent days the
stage:

* Adds new disruptions, updates existing ones.
* Prunes records whose ``end_date`` has passed.
* Injects synthetic "reminder" candidates for every active record that
  is NOT already represented in today's transport candidates.

This keeps long-running Metrolink line closures visible every morning
until the work finishes, so readers don't forget the disruption is
still active. Bus / road / rail disruptions are not persisted — they
are typically short and one-off.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
import re

from news_digest.pipeline.common import (
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.transport_card import (
    TransportCard,
    extract_transport_card,
    render_card,
    render_reminder,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


# ── Helpers ───────────────────────────────────────────────────────────────


_MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


def _ru_date_to_iso(ru_date: str, today: date) -> str | None:
    """Convert '1 июня' → 'YYYY-06-01'. Year resolves to the soonest future
    occurrence (or today's year if it's still ahead).
    """
    if not ru_date:
        return None
    m = re.match(r"\s*(\d{1,2})\s+([а-яё]+)\s*$", ru_date, re.IGNORECASE)
    if not m:
        return None
    day = int(m.group(1))
    month_ru = m.group(2).lower()
    month = _MONTHS_RU.get(month_ru)
    if not month:
        return None
    candidate_year = today.year
    try:
        d = date(candidate_year, month, day)
    except ValueError:
        return None
    if d < today:
        try:
            d = date(candidate_year + 1, month, day)
        except ValueError:
            return None
    return d.isoformat()


def _duration_to_end_iso(duration_phrase: str, start: date) -> str | None:
    """Convert 'две недели' / 'три недели' to a concrete end date."""
    if not duration_phrase:
        return None
    weeks_map = {
        "неделю": 1, "две недели": 2, "три недели": 3, "четыре недели": 4,
        "пять недель": 5, "шесть недель": 6, "семь недель": 7, "восемь недель": 8,
    }
    n = weeks_map.get(duration_phrase.strip().lower())
    if not n:
        m = re.match(r"\s*(\d+)\s+недель?\s*$", duration_phrase, re.IGNORECASE)
        if m:
            n = int(m.group(1))
    if not n:
        return None
    return (start + timedelta(weeks=n)).isoformat()


def _disruption_key(card: TransportCard) -> str:
    """Stable identifier so the same Metrolink line works don't accumulate.

    Built from operator + line + segment so two articles about the same
    Bury-line closure collapse into one persisted record.
    """
    parts = [card.operator.lower()]
    if card.line:
        parts.append(re.sub(r"\s+", "-", card.line.lower()))
    if card.segment:
        parts.append(re.sub(r"\s+", "-", card.segment.lower()))
    if not card.line and not card.segment and card.duration_phrase:
        # Network-wide work — fallback key based on duration so we don't
        # surface duplicate "network works" reminders.
        parts.append("network-" + re.sub(r"\s+", "-", card.duration_phrase.lower()))
    return "|".join(parts)


# ── State file management ─────────────────────────────────────────────────


def _load_active(state_dir: Path) -> dict[str, dict]:
    path = state_dir / "active_tram_disruptions.json"
    if not path.exists():
        return {}
    payload = read_json(path, {"records": []})
    out: dict[str, dict] = {}
    for rec in payload.get("records") or []:
        if isinstance(rec, dict) and rec.get("key"):
            out[rec["key"]] = rec
    return out


def _save_active(state_dir: Path, records: dict[str, dict]) -> Path:
    path = state_dir / "active_tram_disruptions.json"
    write_json(path, {
        "last_updated_london": today_london(),
        "records": sorted(records.values(), key=lambda r: r.get("key", "")),
    })
    return path


def _prune_expired(records: dict[str, dict], today: date) -> int:
    """Drop records whose end_date is in the past."""
    dropped = 0
    for key in list(records.keys()):
        end = records[key].get("end_date")
        if not end:
            continue
        try:
            if date.fromisoformat(end) < today:
                del records[key]
                dropped += 1
        except (TypeError, ValueError):
            continue
    return dropped


def _card_to_record(card: TransportCard, today: date, source_url: str = "") -> dict:
    """Serialize a TransportCard into the persisted record shape, resolving
    Russian date phrases to ISO so pruning works tomorrow."""
    end_iso = _ru_date_to_iso(card.end_date, today)
    start_iso = _ru_date_to_iso(card.start_date, today)
    if not end_iso and card.duration_phrase:
        anchor = date.fromisoformat(start_iso) if start_iso else today
        end_iso = _duration_to_end_iso(card.duration_phrase, anchor)
    return {
        "key": _disruption_key(card),
        "mode": card.mode,
        "operator": card.operator,
        "line": card.line,
        "segment": card.segment,
        "start_date_ru": card.start_date,
        "end_date_ru": card.end_date,
        "duration_phrase": card.duration_phrase,
        "start_date": start_iso or "",
        "end_date": end_iso or "",
        "reason": card.reason,
        "alternative": card.alternative,
        "cost_phrase": card.cost_phrase,
        "first_seen": today.isoformat(),
        "source_url": source_url,
    }


_ISO_MONTH_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
    7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def _iso_to_ru_date(iso: str) -> str:
    if not iso:
        return ""
    try:
        d = date.fromisoformat(iso)
    except (TypeError, ValueError):
        return ""
    return f"{d.day} {_ISO_MONTH_RU[d.month]}"


def _record_to_card(rec: dict) -> TransportCard:
    # Prefer the original Russian phrasing if persisted; otherwise rebuild
    # from the ISO date so reminders always show a concrete "до X" tail.
    end_ru = rec.get("end_date_ru") or _iso_to_ru_date(rec.get("end_date") or "")
    start_ru = rec.get("start_date_ru") or _iso_to_ru_date(rec.get("start_date") or "")
    return TransportCard(
        mode=rec.get("mode") or "tram",
        operator=rec.get("operator") or "Metrolink",
        line=rec.get("line") or "",
        segment=rec.get("segment") or "",
        start_date=start_ru,
        end_date=end_ru,
        duration_phrase=rec.get("duration_phrase") or "",
        reason=rec.get("reason") or "",
        alternative=rec.get("alternative") or "",
        cost_phrase=rec.get("cost_phrase") or "",
    )


# ── Synthetic reminder candidate ──────────────────────────────────────────


def _make_reminder_candidate(rec: dict, today_iso: str) -> dict:
    """Build a synthetic candidate for a Metrolink disruption that has
    no fresh article today. Routed to the transport block as a reminder.
    """
    card = _record_to_card(rec)
    line = render_reminder(card)
    fp = f"transport-reminder|{rec.get('key', '')}|{today_iso}"
    return {
        "fingerprint": fp,
        "title": f"[reminder] {rec.get('operator', 'Metrolink')} {rec.get('line', '')} {rec.get('segment', '')}".strip(),
        "summary": "",
        "lead": "",
        "evidence_text": "",
        "category": "transport",
        "primary_block": "transport",
        "include": True,
        "is_lead": False,
        "source_label": rec.get("operator", "Metrolink"),
        "source_url": rec.get("source_url") or "https://tfgm.com/",
        "published_at": today_iso,
        "published_date_london": today_iso,
        "freshness_status": "reminder",
        "draft_line": line,
        "draft_line_provider": "transport_fill",
        "draft_line_model": "deterministic_reminder",
        "draft_line_written_at": now_london().isoformat(),
        "reason": "Synthetic reminder for ongoing Metrolink disruption.",
        "transport_reminder": True,
    }


# ── Main stage ────────────────────────────────────────────────────────────


def run_transport_fill(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    report_path = state_dir / "transport_fill_report.json"

    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])

    today = date.fromisoformat(today_london())
    today_iso = today.isoformat()

    active = _load_active(state_dir)
    pruned = _prune_expired(active, today)

    filled = 0
    skipped = 0  # extractor returned None — leave for LLM tier 3
    persisted = 0
    seen_keys_today: set[str] = set()
    fill_details: list[dict] = []

    for c in candidates:
        if not isinstance(c, dict):
            continue
        if str(c.get("primary_block") or "") != "transport":
            continue
        if not c.get("include"):
            continue

        # If a deterministic draft_line is already present (e.g. from a
        # previous run during the same day), don't overwrite.
        existing_draft = str(c.get("draft_line") or "").strip()
        if existing_draft and str(c.get("draft_line_provider") or "") == "transport_fill":
            continue

        card = extract_transport_card(c)
        if card is None:
            skipped += 1
            fill_details.append({
                "fingerprint": c.get("fingerprint"),
                "title": c.get("title"),
                "status": "skipped_no_card",
            })
            continue

        rendered = render_card(card)
        c["draft_line"] = rendered
        c["draft_line_provider"] = "transport_fill"
        c["draft_line_model"] = "deterministic_template"
        c["draft_line_written_at"] = now_london().isoformat()
        c["transport_mode"] = card.mode
        c["expected_operator"] = card.operator
        filled += 1
        fill_details.append({
            "fingerprint": c.get("fingerprint"),
            "title": c.get("title"),
            "status": "filled",
            "mode": card.mode,
            "tier": "1" if (
                (card.has_line_or_segment or card.has_street_or_stop)
                and (card.has_dates or card.has_reason or card.has_alternative)
            ) else "2",
        })

        # Persist tram disruptions that have a time horizon. We only
        # persist trams — bus / road / rail are one-off, no reminders.
        if card.mode == "tram" and (card.end_date or card.duration_phrase):
            rec = _card_to_record(card, today, source_url=str(c.get("source_url") or ""))
            key = rec["key"]
            seen_keys_today.add(key)
            if key in active:
                # Update end_date / segment if we now know them better.
                existing = active[key]
                for field_name in ("end_date", "start_date", "line", "segment",
                                    "reason", "alternative", "cost_phrase"):
                    if rec.get(field_name) and not existing.get(field_name):
                        existing[field_name] = rec[field_name]
            else:
                active[key] = rec
                persisted += 1

    # ── Inject reminder candidates for active Metrolink disruptions
    #    that are NOT covered by a fresh article today. ─────────────────
    injected = 0
    for key, rec in active.items():
        if key in seen_keys_today:
            continue
        # Sanity: don't inject reminders for records with no operator.
        if not rec.get("operator"):
            continue
        # If end_date is missing AND first_seen is older than 30 days,
        # treat as stale and drop. Without an end date we can't auto-prune
        # so the cap stops infinite accumulation.
        first_seen = rec.get("first_seen", "")
        try:
            fs_date = date.fromisoformat(first_seen)
            if not rec.get("end_date") and (today - fs_date).days > 30:
                continue
        except (TypeError, ValueError):
            pass

        candidates.append(_make_reminder_candidate(rec, today_iso))
        injected += 1

    candidates_path.write_text(
        __import__("json").dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _save_active(state_dir, active)

    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_iso,
            "stage_status": "complete",
            "filled": filled,
            "skipped_no_card": skipped,
            "persisted_tram_disruptions": persisted,
            "injected_reminders": injected,
            "pruned_expired": pruned,
            "active_tram_count": len(active),
            "details": fill_details,
        },
    )
    logger.info(
        "transport_fill: filled=%d skipped=%d persisted=%d reminders=%d pruned=%d active=%d",
        filled, skipped, persisted, injected, pruned, len(active),
    )
    return StageResult(True, "Transport fill stage completed.", report_path)
