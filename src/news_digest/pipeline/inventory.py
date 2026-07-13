"""Backlog item 8 — inventory as a verifiable layer, not a warehouse.

This module turns the collected pool into checkable inventory: schema-versioned
records with readiness/liveness/expiry, a per-category health verdict, a no-loss
disposition contract, a bounded morning-selection contract, and re-entry of
yesterday's unshown-but-still-relevant items — deduped against the EXISTING
`published_facts` (never a second dedup system).

Design constraints honoured (owner, 2026-07-01):
  - checks stay bounded; stale / last-known-good is NEVER rendered as fresh —
    it is only reserve/diagnostic carrying an honest age;
  - the Russian line is never truth — it stays a cache keyed by
    evidence_hash + prompt_version (handled in llm_rewrite, not here);
  - re-entry reuses `published_facts`, so a re-entered item can't duplicate a
    shown one;
  - show=renderable and prevalidated reserve already exist (0030/0031); this
    layer feeds them, it does not replace them.

The night-collection SCHEDULE (00:30/02:00/03:30/06:15/06:30/07:45) and the
per-category collect command live in the orchestrator + launchd artifacts;
this module is the pure data/contract layer they call into.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from news_digest.pipeline.common import (
    PRIMARY_BLOCKS,
    canonical_url_identity,
    now_london,
    today_london,
    write_json_atomic,
)

INVENTORY_SCHEMA_VERSION = 1


# One inventory policy for every public/legacy block. All night collection,
# card evaluation, morning intake, completeness and source replacement derive
# from this registry; parallel allowlists/caps are intentionally forbidden.
INVENTORY_BLOCK_REGISTRY: dict[str, dict[str, object]] = {
    "weather": {
        "source_report_categories": frozenset({"synthetic"}),
        "candidate_categories": frozenset({"weather"}),
        "mode": "live_only", "serving_ttl_hours": 0.5, "retention_days": 2,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 1, "min_sources": 1, "optional": False, "intake_cap": 0,
    },
    "transport": {
        "source_report_categories": frozenset({"transport"}),
        "candidate_categories": frozenset({"transport"}),
        "mode": "hybrid", "serving_ttl_hours": 1.0, "retention_days": 7,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 0, "min_sources": 1, "optional": True, "intake_cap": 0,
        "required_fields": ("what_happened", "why_now"),
    },
    "today_focus": {
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council"}),
        "mode": "live_only", "serving_ttl_hours": 6.0, "retention_days": 7,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 3, "min_sources": 2, "optional": False, "intake_cap": 0,
    },
    "last_24h": {
        "source_report_categories": frozenset({"media_layer", "gmp"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "council"}),
        "mode": "hybrid", "serving_ttl_hours": 6.0, "retention_days": 14,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 6, "min_sources": 2, "optional": False, "intake_cap": 0,
        "required_fields": ("what_happened", "why_now"),
    },
    "lead_story": {
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council"}),
        "mode": "live_only", "serving_ttl_hours": 6.0, "retention_days": 14,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 0, "min_sources": 1, "optional": True, "intake_cap": 0,
    },
    "city_watch": {
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services", "transport", "tech_business"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council", "transport", "tech_business"}),
        "mode": "hybrid", "serving_ttl_hours": 24.0, "retention_days": 30,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 5, "min_sources": 2, "optional": False, "intake_cap": 0,
        "required_fields": ("what_happened", "why_now"),
    },
    "weekend_activities": {
        "source_report_categories": frozenset({"culture_weekly"}),
        "candidate_categories": frozenset({"culture_weekly"}),
        "mode": "assist", "serving_ttl_hours": 96.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "floor": 6, "min_sources": 2, "optional": False, "intake_cap": 0,
        "required_fields": ("event_name", "specific_event", "venue", "date_start", "action_url", "activity_type", "gm_fit"),
    },
    "next_7_days": {
        "source_report_categories": frozenset({"media_layer", "gmp", "public_services", "transport"}),
        "candidate_categories": frozenset({"media_layer", "gmp", "public_services", "council", "transport"}),
        "mode": "assist", "serving_ttl_hours": 96.0, "retention_days": 30,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "floor": 3, "min_sources": 2, "optional": False, "intake_cap": 12,
        "required_fields": ("event_name", "specific_event", "venue", "date_start", "action_url", "non_leisure"),
    },
    "future_announcements": {
        "source_report_categories": frozenset({"culture_weekly", "venues_tickets"}),
        "candidate_categories": frozenset({"culture_weekly", "venues_tickets"}),
        "mode": "assist", "serving_ttl_hours": 336.0, "retention_days": 400,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "floor": 0, "min_sources": 1, "optional": True, "intake_cap": 20,
        "required_fields": ("event_name", "venue", "action_url"),
    },
    "ticket_radar": {
        "source_report_categories": frozenset({"venues_tickets"}),
        "candidate_categories": frozenset({"venues_tickets"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 400,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "floor": 2, "min_sources": 2, "optional": False, "intake_cap": 20,
        "required_fields": ("event_name", "date_start", "venue", "action_url", "ticket_type", "tier", "venue_scope", "ticket_why_now"),
    },
    "outside_gm_tickets": {
        "source_report_categories": frozenset({"venues_tickets"}),
        "candidate_categories": frozenset({"venues_tickets"}),
        "mode": "assist", "serving_ttl_hours": 336.0, "retention_days": 400,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "floor": 0, "min_sources": 1, "optional": True, "intake_cap": 6,
        "required_fields": ("event_name", "date_start", "venue", "action_url", "ticket_type", "outside_a_tier"),
    },
    "russian_events": {
        "source_report_categories": frozenset({"diaspora_events"}),
        "candidate_categories": frozenset({"russian_speaking_events", "diaspora_events"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 400,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "floor": 1, "min_sources": 1, "optional": False, "intake_cap": 10,
        "required_fields": ("event_name", "specific_event", "date_start", "venue", "russian_evidence", "russian_geography", "action_url"),
    },
    "openings": {
        "source_report_categories": frozenset({"food_openings"}),
        "candidate_categories": frozenset({"food_openings"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 90,
        "text_policy": "morning_writer", "source_replacement_allowed": True,
        "floor": 3, "min_sources": 2, "optional": False, "intake_cap": 10,
        "required_fields": ("event_name", "specific_event", "venue", "specific_venue", "opening_phase_or_date", "food_meaning", "action_url"),
    },
    "tech_business": {
        "source_report_categories": frozenset({"tech_business"}),
        "candidate_categories": frozenset({"tech_business"}),
        "mode": "hybrid", "serving_ttl_hours": 24.0, "retention_days": 30,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 0, "min_sources": 1, "optional": True, "intake_cap": 0,
        "required_fields": ("what_happened", "why_now"),
    },
    "professional_events": {
        "source_report_categories": frozenset({"professional_events"}),
        "candidate_categories": frozenset({"professional_events"}),
        "mode": "assist", "serving_ttl_hours": 168.0, "retention_days": 180,
        "text_policy": "deterministic_or_morning", "source_replacement_allowed": False,
        "floor": 1, "min_sources": 1, "optional": False, "intake_cap": 10,
        "required_fields": ("event_name", "specific_event", "venue", "date_start", "professional_llm_cv", "professional_access", "action_url"),
    },
    "football": {
        "source_report_categories": frozenset({"football"}),
        "candidate_categories": frozenset({"football"}),
        "mode": "hybrid", "serving_ttl_hours": 12.0, "retention_days": 14,
        "text_policy": "morning_live", "source_replacement_allowed": False,
        "floor": 2, "min_sources": 1, "optional": False, "intake_cap": 0,
        "required_fields": ("what_happened", "why_now"),
    },
    "district_radar": {
        "source_report_categories": frozenset(),
        "candidate_categories": frozenset(),
        "mode": "retired", "serving_ttl_hours": 0.0, "retention_days": 0,
        "text_policy": "none", "source_replacement_allowed": False,
        "floor": 0, "min_sources": 0, "optional": True, "intake_cap": 0,
    },
}

if set(INVENTORY_BLOCK_REGISTRY) != set(PRIMARY_BLOCKS):
    raise RuntimeError("INVENTORY_BLOCK_REGISTRY must define every PRIMARY_BLOCK exactly once")


def inventory_category_output_blocks() -> dict[str, frozenset[str]]:
    outputs: dict[str, set[str]] = {}
    for block, policy in INVENTORY_BLOCK_REGISTRY.items():
        if str(policy.get("mode") or "") == "retired":
            continue
        for category in policy.get("source_report_categories") or ():
            outputs.setdefault(str(category), set()).add(block)
    return {category: frozenset(blocks) for category, blocks in outputs.items()}


_registry_category_outputs = inventory_category_output_blocks()
for _block, _policy in INVENTORY_BLOCK_REGISTRY.items():
    _policy["output_blocks"] = frozenset(
        output
        for category in _policy.get("source_report_categories") or ()
        for output in _registry_category_outputs.get(str(category), frozenset())
    )
del _block, _policy, _registry_category_outputs


# ── 8.1 State foundation ──────────────────────────────────────────────────

def inventory_dir(state_dir: Path) -> Path:
    return state_dir / "inventory"


class InventoryLock:
    """Coarse cross-process lock so a night wave, the 06:30 refresh and the
    08:00 build never write inventory state on top of each other. A stale lock
    (holder crashed) is broken after `stale_after_seconds` so a dead job can't
    wedge the pipeline forever — the never-block rule applies to locks too."""

    def __init__(self, state_dir: Path, name: str = "inventory", stale_after_seconds: float = 900.0):
        self.path = inventory_dir(state_dir) / f".{name}.lock"
        self.stale_after_seconds = stale_after_seconds
        self._fd: int | None = None

    def __enter__(self) -> "InventoryLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(3):
            try:
                self._fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, f"{os.getpid()} {now_london().isoformat()}".encode("utf-8"))
                return self
            except FileExistsError:
                try:
                    age = time.time() - self.path.stat().st_mtime
                except OSError:
                    age = 0.0
                if age > self.stale_after_seconds:
                    try:
                        self.path.unlink()
                    except OSError:
                        pass
                    continue
                # Someone healthy holds it — proceed without blocking the run
                # (observability, not mutual exclusion of last resort).
                return self
        return self

    def __exit__(self, *exc: object) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
        try:
            self.path.unlink()
        except OSError:
            pass


def write_inventory(state_dir: Path, category: str, records: list[dict]) -> Path:
    """Persist one category's inventory as schema-versioned JSONL, atomically,
    under the inventory lock. A partial write is never observable (temp+rename),
    so a reader never sees an inventory file as empty mid-write."""
    path = inventory_dir(state_dir) / f"{category}.jsonl"
    body = "\n".join(
        json.dumps({"schema_version": INVENTORY_SCHEMA_VERSION, **record}, ensure_ascii=False)
        for record in records
    )
    with InventoryLock(state_dir, name=f"write-{category}"):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp{os.getpid()}")
        tmp_path.write_text(body + ("\n" if body else ""), encoding="utf-8")
        os.replace(tmp_path, path)
    return path


# 8.4 night waves → category groups. A wave collects ONLY into inventory
# (upsert), never candidates.json — so the 08:00 hot path is untouched and a
# night job can never block or corrupt the morning release.
NIGHT_WAVES: dict[str, frozenset[str]] = {
    "events": frozenset({"culture_weekly"}),
    "tickets": frozenset({"venues_tickets"}),
    "pro_food_russian": frozenset({"professional_events", "food_openings", "diaspora_events"}),
    "live_news": frozenset({"media_layer", "gmp", "public_services", "transport", "football", "tech_business"}),
}
# 07:45 bounded breaking-check: a tiny hard-news subset, headlines only, hard
# time budget — never a second full collect.
BREAKING_CHECK_CATEGORIES: frozenset[str] = frozenset({"media_layer", "gmp", "transport"})


def merge_inventory(state_dir: Path, category: str, new_records: list[dict]) -> int:
    """Upsert new records into a category's inventory by fingerprint (newest
    record wins, refreshing last_seen_at). Returns the resulting record count.
    Used by night waves to accumulate inventory across runs without dropping
    still-valid cards collected earlier."""
    existing = {
        str(r.get("fingerprint") or ""): r
        for r in read_inventory(state_dir, category)
        if isinstance(r, dict) and r.get("fingerprint")
    }
    for record in new_records:
        fingerprint = str(record.get("fingerprint") or "")
        if fingerprint:
            previous = existing.get(fingerprint, {})
            merged = dict(record)
            merged["source_report_category"] = str(
                merged.get("source_report_category") or category
            )
            merged["first_seen_at"] = str(
                previous.get("first_seen_at")
                or previous.get("last_seen_at")
                or merged.get("first_seen_at")
                or merged.get("last_seen_at")
                or ""
            )
            unchanged = bool(
                previous
                and str(previous.get("evidence_hash") or "")
                == str(merged.get("evidence_hash") or "")
            )
            merged["last_changed_at"] = str(
                previous.get("last_changed_at")
                if unchanged and previous.get("last_changed_at")
                else merged.get("last_seen_at") or merged.get("last_changed_at") or ""
            )
            existing[fingerprint] = merged
    write_inventory(state_dir, category, list(existing.values()))
    return len(existing)


def read_inventory(state_dir: Path, category: str) -> list[dict]:
    path = inventory_dir(state_dir) / f"{category}.jsonl"
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            row.setdefault("source_report_category", category)
            rows.append(row)
    return rows


def read_all_inventory(state_dir: Path) -> list[dict]:
    inv_dir = inventory_dir(state_dir)
    if not inv_dir.exists():
        return []
    rows: list[dict] = []
    for path in sorted(inv_dir.glob("*.jsonl")):
        rows.extend(read_inventory(state_dir, path.stem))
    return rows


# ── 8.5 Card rules → readiness ────────────────────────────────────────────
#
# Required fields live in INVENTORY_BLOCK_REGISTRY beside each block's mode,
# freshness and replacement policy.

_LEISURE_CARD_RE = re.compile(
    r"\b(?:market|fair|fayre|festival|concert|gig|live\s+music|recital|show|"
    r"exhibition|screening|workshop|tour|comedy|stand[-\s]?up|car\s*boot)\b",
    re.IGNORECASE,
)
_FOOD_MEANING_RE = re.compile(
    r"\b(?:restaurant|cafe|coffee|bar|pub|bakery|food|drink|market|deli|"
    r"greengrocer|opening|opened|opens|launch|reopen|takeover|pop[-\s]?up)\b",
    re.IGNORECASE,
)


def _card_field_value(candidate: dict, field: str) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if field == "action_url":
        return str(candidate.get("booking_url") or event.get("booking_url") or candidate.get("source_url") or "").strip()
    if field == "specific_event":
        name = str(event.get("event_name") or event.get("name") or candidate.get("title") or "").strip()
        if re.search(
            r"\b(?:what'?s on|things to do|events in manchester|food halls? and markets?|events guide|upcoming events|next page)\b|^\s*\d+\s+(?:new|best|top)\b",
            name,
            re.IGNORECASE,
        ):
            return ""
        return name
    if field == "specific_venue":
        venue = str(event.get("venue") or candidate.get("venue") or "").strip()
        if venue.lower() in {"manchester", "greater manchester", "stockport", "salford", "trafford"}:
            return ""
        return venue
    if field == "opening_phase_or_date":
        phase = str(candidate.get("change_phase") or "").strip()
        if phase:
            return phase
        claim = " ".join(str(candidate.get(name) or "") for name in ("title", "summary", "lead"))
        if re.search(r"\b(?:open(?:s|ed|ing)?|reopen(?:s|ed|ing)?|launch(?:es|ed|ing)?|set to return)\b", claim, re.IGNORECASE):
            return "opening_claim"
        return str(event.get("date_start") or event.get("date") or "").strip()
    if field == "professional_llm_cv":
        match = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
        status = str(candidate.get("professional_match_status") or "")
        return "matched" if status == "llm_cv_matched" and str(match.get("llm_fit") or "") in {"go", "consider"} and match.get("publish") else ""
    if field == "professional_access":
        match = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
        access = str(match.get("access_label") or "").strip().lower()
        return access if access in {"free", "paid", "booking_required"} else ""
    if field == "russian_evidence":
        evidence = candidate.get("russian_evidence") if isinstance(candidate.get("russian_evidence"), dict) else {}
        return "positive" if evidence.get("has_evidence") else ""
    if field == "tier":
        notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
        return str(notability.get("tier") or "").strip()
    if field == "ticket_type":
        return str(candidate.get("ticket_type") or event.get("ticket_type") or "").strip()
    if field == "venue_scope":
        scope = str(candidate.get("venue_scope") or "").strip()
        if str(candidate.get("primary_block") or "") == "ticket_radar":
            return scope if scope in {"GM", "nearby"} else ""
        return scope
    if field == "ticket_why_now":
        ticket_type = str(candidate.get("ticket_type") or event.get("ticket_type") or "").strip()
        notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
        return ticket_type or str(notability.get("signal") or candidate.get("why_now") or "").strip()
    if field == "outside_a_tier":
        notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
        scope = str(candidate.get("venue_scope") or "")
        return "A" if str(notability.get("tier") or "").upper() == "A" and scope in {"outside", "nearby"} else ""
    if field == "activity_type":
        from news_digest.pipeline.weekend_inventory import weekend_activity_type  # noqa: PLC0415

        return weekend_activity_type(candidate)
    if field == "gm_fit":
        scope = str(candidate.get("venue_scope") or "")
        borough = str(event.get("borough") or "").lower()
        return "GM" if scope == "GM" or borough in {
            "bolton", "bury", "manchester", "oldham", "rochdale", "salford",
            "stockport", "tameside", "trafford", "wigan",
        } else ""
    if field == "non_leisure":
        category = str(candidate.get("category") or "")
        source_category = str(candidate.get("source_report_category") or "")
        blob = " ".join(str(candidate.get(name) or "") for name in ("title", "summary", "lead", "evidence_text"))
        if category in {"culture_weekly", "venues_tickets", "russian_speaking_events", "diaspora_events"}:
            return ""
        if source_category in {"culture_weekly", "venues_tickets", "diaspora_events"} or _LEISURE_CARD_RE.search(blob):
            return ""
        return "planning"
    if field == "food_meaning":
        blob = " ".join(
            str(value or "")
            for value in (
                candidate.get("title"), candidate.get("summary"), candidate.get("lead"),
                candidate.get("change_phase"), event.get("event_name"), event.get("venue"),
            )
        )
        return "local_food_change" if _FOOD_MEANING_RE.search(blob) else ""
    if field == "russian_geography":
        return str(candidate.get("venue_city") or event.get("borough") or "").strip()
    return str(candidate.get(field) or event.get(field) or "").strip()


def evaluate_card(candidate: dict) -> tuple[str, bool, list[str]]:
    """(quality_status, render_ready, missing_facts) for one candidate against
    its block's card rule. render_ready requires both the structured fields and
    an existing public line — matching the show=renderable contract (0030)."""
    block = str(candidate.get("primary_block") or "")
    policy = INVENTORY_BLOCK_REGISTRY.get(block, {})
    required = tuple(policy.get("required_fields") or ())
    missing = [field for field in required if not _card_field_value(candidate, field)]
    has_text = bool(str(candidate.get("draft_line") or "").strip())
    if missing:
        return "missing_facts", False, missing
    if not has_text:
        return "needs_text", False, ["draft_line"]
    return "ready", True, []


# ── 8.3 evidence identity (inventory-local; decoupled from the reuse cache) ─

def evidence_cache_extra_fields(candidate: dict) -> dict[str, str]:
    """Structured story facts folded into llm_rewrite's reuse-cache hash (wired
    there). Empty for events/tickets; for hard news a changed fact — casualty
    count, court stage, who's affected — invalidates a cached line even past the
    evidence-text truncation point."""
    return {
        "what_happened": str(candidate.get("what_happened") or "")[:300],
        "who_affected": str(candidate.get("who_affected") or "")[:200],
        "why_now": str(candidate.get("why_now") or "")[:200],
        "event_type": str(candidate.get("story_type") or candidate.get("event_type") or ""),
    }


def compute_evidence_hash(candidate: dict) -> str:
    """Stable identity for an inventory card. Includes structured story facts so
    a materially changed hard-news fact yields a new hash (the same principle as
    the reuse cache, computed standalone so inventory has no llm_rewrite dep)."""
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    ticket_notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    professional_match = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
    professional_llm = candidate.get("professional_llm_match") if isinstance(candidate.get("professional_llm_match"), dict) else {}
    russian_evidence = candidate.get("russian_evidence") if isinstance(candidate.get("russian_evidence"), dict) else {}
    action_url = str(candidate.get("booking_url") or event.get("booking_url") or candidate.get("source_url") or "")
    payload = {
        "primary_block": str(candidate.get("primary_block") or ""),
        "source_report_category": str(candidate.get("source_report_category") or ""),
        "candidate_category": str(candidate.get("category") or ""),
        "title": str(candidate.get("title") or "")[:300],
        "summary": str(candidate.get("summary") or "")[:600],
        "lead": str(candidate.get("lead") or "")[:600],
        "practical_angle": str(candidate.get("practical_angle") or "")[:300],
        "event_name": str(event.get("event_name") or event.get("name") or ""),
        "event_date_start": str(event.get("date_start") or event.get("date") or ""),
        "event_date_end": str(event.get("date_end") or ""),
        "next_occurrence": str(event.get("next_occurrence") or ""),
        "event_status": str(event.get("event_status") or ""),
        "is_recurring": bool(event.get("is_recurring")),
        "venue": str(event.get("venue") or candidate.get("venue") or ""),
        "venue_scope": str(candidate.get("venue_scope") or ""),
        "venue_city": str(candidate.get("venue_city") or ""),
        "action_url": canonical_url_identity(action_url),
        "ticket_type": str(candidate.get("ticket_type") or event.get("ticket_type") or ""),
        "ticket_notability": {
            "tier": str(ticket_notability.get("tier") or ""),
            "kind": str(ticket_notability.get("kind") or ""),
            "signal": str(ticket_notability.get("signal") or ""),
        },
        "change_phase": str(candidate.get("change_phase") or ""),
        "professional_match": {
            "llm_fit": str(professional_match.get("llm_fit") or ""),
            "publish": bool(professional_match.get("publish")),
            "access_label": str(professional_match.get("access_label") or ""),
            "score": str(professional_match.get("fit_score") or ""),
            "status": str(candidate.get("professional_match_status") or ""),
            "provider": str(professional_llm.get("provider") or ""),
        },
        "russian_evidence": {
            "has_evidence": bool(russian_evidence.get("has_evidence")),
            "strong_signals": sorted(str(item) for item in russian_evidence.get("strong_signals") or []),
        },
        "story_facts": evidence_cache_extra_fields(candidate),
        "evidence_text": str(candidate.get("evidence_text") or candidate.get("source_evidence") or "")[:1200],
        "schema_version": INVENTORY_SCHEMA_VERSION,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


# ── 8.2 Canonical item schema ─────────────────────────────────────────────

def _retention_until(candidate: dict, *, now_iso: str) -> str:
    block = str(candidate.get("primary_block") or "")
    policy = INVENTORY_BLOCK_REGISTRY.get(block, {})
    retention_days = max(0, int(policy.get("retention_days") or 0))
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    anchor_raw = str(event.get("date_end") or event.get("date_start") or event.get("date") or "")[:10]
    try:
        anchor = date.fromisoformat(anchor_raw) if anchor_raw else datetime.fromisoformat(now_iso).date()
    except ValueError:
        anchor = now_london().date()
    return (anchor + timedelta(days=retention_days)).isoformat() if retention_days else ""


def build_inventory_record(
    candidate: dict,
    *,
    prompt_version: int,
    now_iso: str | None = None,
    run_id: str = "",
    wave: str = "",
    source_name: str = "",
    source_report_category: str = "",
) -> dict:
    """Canonical inventory card. Stores English raw/evidence for audit, but the
    working unit is the fact card + readiness, never the raw English text."""
    now_iso = now_iso or now_london().isoformat()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    quality_status, render_ready, missing_facts = evaluate_card(candidate)
    fact_card = {
        "event_name": str(event.get("event_name") or event.get("name") or ""),
        "venue": str(event.get("venue") or candidate.get("venue") or ""),
        "date_start": str(event.get("date_start") or event.get("date") or ""),
        "date_end": str(event.get("date_end") or ""),
        "date_text": str(event.get("date_text") or ""),
        "date_confidence": str(event.get("date_confidence") or ""),
        "is_recurring": bool(event.get("is_recurring")),
        "next_occurrence": str(event.get("next_occurrence") or ""),
        "event_status": str(event.get("event_status") or ""),
        "venue_scope": str(candidate.get("venue_scope") or ""),
        "venue_city": str(candidate.get("venue_city") or ""),
        "ticket_type": str(candidate.get("ticket_type") or ""),
        "ticket_notability": candidate.get("ticket_notability")
        if isinstance(candidate.get("ticket_notability"), dict) else {},
        "tier": str((candidate.get("ticket_notability") or {}).get("tier") or "")
        if isinstance(candidate.get("ticket_notability"), dict) else "",
        "what_happened": str(candidate.get("what_happened") or "")[:300],
        "why_now": str(candidate.get("why_now") or "")[:200],
        "story_type": str(candidate.get("story_type") or ""),
        "change_phase": str(candidate.get("change_phase") or ""),
        "professional_event_match": candidate.get("professional_event_match")
        if isinstance(candidate.get("professional_event_match"), dict) else {},
        "professional_llm_match": candidate.get("professional_llm_match")
        if isinstance(candidate.get("professional_llm_match"), dict) else {},
        "professional_match_status": str(candidate.get("professional_match_status") or ""),
        "russian_evidence": candidate.get("russian_evidence")
        if isinstance(candidate.get("russian_evidence"), dict) else {},
    }
    block_policy = INVENTORY_BLOCK_REGISTRY.get(str(candidate.get("primary_block") or ""), {})
    serving_ttl = float(block_policy.get("serving_ttl_hours") or 0.0)
    return {
        "fingerprint": str(candidate.get("fingerprint") or ""),
        "evidence_hash": compute_evidence_hash(candidate),
        "prompt_version": prompt_version,
        "run_id": str(run_id or candidate.get("inventory_run_id") or ""),
        "wave": str(wave or candidate.get("inventory_wave") or ""),
        "source_name": str(source_name or candidate.get("source_label") or ""),
        "source_report_category": str(
            source_report_category or candidate.get("source_report_category") or ""
        ),
        "candidate_category": str(candidate.get("category") or ""),
        "first_seen_at": now_iso,
        "last_seen_at": now_iso,
        "last_changed_at": now_iso,
        "title": str(candidate.get("title") or ""),
        "summary": str(candidate.get("summary") or ""),
        "lead": str(candidate.get("lead") or ""),
        "published_at": str(candidate.get("published_at") or ""),
        "freshness_status": str(candidate.get("freshness_status") or ""),
        "practical_angle": str(candidate.get("practical_angle") or ""),
        "draft_line": str(candidate.get("draft_line") or ""),
        "draft_line_provider": str(candidate.get("draft_line_provider") or ""),
        "draft_line_model": str(candidate.get("draft_line_model") or ""),
        "draft_line_written_at": str(candidate.get("draft_line_written_at") or ""),
        "source_url": str(candidate.get("source_url") or ""),
        "booking_url": str(candidate.get("booking_url") or event.get("booking_url") or ""),
        "source_label": str(candidate.get("source_label") or ""),
        "primary_block": str(candidate.get("primary_block") or ""),
        "category": str(candidate.get("category") or ""),
        "raw_evidence": str(candidate.get("evidence_text") or candidate.get("source_evidence") or "")[:4000],
        "fact_card": fact_card,
        "quality_status": quality_status,
        "render_ready": render_ready,
        "missing_facts": missing_facts,
        "observed_in_wave": True,
        "observed_run_id": str(run_id or candidate.get("inventory_run_id") or ""),
        "action_url_liveness": str(candidate.get("action_url_liveness") or "unknown"),
        "action_url_checked_at": str(candidate.get("action_url_checked_at") or ""),
        "serving_ttl_hours": serving_ttl,
        "serving_expires_at": (
            datetime.fromisoformat(now_iso).astimezone(now_london().tzinfo)
            + timedelta(hours=serving_ttl)
        ).isoformat() if serving_ttl else "",
        "retention_until": _retention_until(candidate, now_iso=now_iso),
    }


# ── 8.7 No-loss disposition contract ──────────────────────────────────────

_EXPIRED_TICKET_TYPES = frozenset({"old_onsale", "old_public_sale"})

# `deferred` and `not_morning_relevant` are valid terminal states produced only
# by the night-inventory path (an item captured at night but not yet morning-
# relevant). The single-run classifier below never needs to invent them, but
# they are accepted so a night-job record is not flagged as unclassified.
TERMINAL_DISPOSITIONS = frozenset(
    {
        "shown",
        "reserve",
        "inventory_only",
        "missing_facts",
        "expired",
        "duplicate",
        "not_render_ready",
        "dropped",
        "deferred",
        "not_morning_relevant",
    }
)


def classify_disposition(candidate: dict, rendered_fingerprints: set[str]) -> str:
    """Exactly one terminal disposition per captured candidate, from fields the
    pipeline already computes. `not_render_ready` is the load-bearing bucket: a
    candidate marked selected/show but absent from the rendered set is the
    'silently lost after selection' failure this contract exists to catch."""
    if not isinstance(candidate, dict):
        return "dropped"
    fingerprint = str(candidate.get("fingerprint") or "")
    if fingerprint and fingerprint in rendered_fingerprints:
        return "shown"
    if candidate.get("recoverable_reserve") or candidate.get("public_reserve"):
        return "reserve"
    if candidate.get("ticket_inventory_held"):
        return "inventory_only"
    if str(candidate.get("dedupe_decision") or candidate.get("change_type") or "") == "drop":
        return "duplicate"
    if str(candidate.get("ticket_type") or "") in _EXPIRED_TICKET_TYPES:
        return "expired"
    status = str(candidate.get("publish_plan_status") or "")
    verdict = str(candidate.get("digest_selection_verdict") or "")
    if status == "needs_enrichment" or verdict == "needs_enrichment":
        return "missing_facts"
    if status in {"must_show", "show"} or verdict == "selected":
        return "not_render_ready"
    return "dropped"


def verify_dispositions(candidates: list[dict], rendered_fingerprints: set[str]) -> dict[str, object]:
    """8.7 criterion: every captured item lands in exactly one disposition and
    sum(dispositions) == captured. A non-empty `violations` means the classifier
    missed a real pipeline state, not that an item is actually lost."""
    totals: dict[str, int] = {}
    violations: list[dict[str, object]] = []
    captured = 0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        captured += 1
        disposition = classify_disposition(candidate, rendered_fingerprints)
        totals[disposition] = totals.get(disposition, 0) + 1
        if disposition not in TERMINAL_DISPOSITIONS:
            violations.append(
                {"fingerprint": candidate.get("fingerprint"), "unclassified": disposition}
            )
    accounted = sum(totals.values())
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "captured": captured,
        "accounted": accounted,
        "conserved": accounted == captured and not violations,
        "totals": totals,
        "silent_loss": int(totals.get("not_render_ready", 0)),
        "violations": violations[:50],
    }


# ── 8.6 Morning selection contract ────────────────────────────────────────

_TICKET_BLOCKS = frozenset({"ticket_radar", "outside_gm_tickets", "future_announcements"})
_TICKET_MORNING_TYPES = frozenset({"on_sale_now", "presale_soon", "newly_listed", "event_this_week", "major_upcoming"})
_RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


def _is_expired(record: dict, today: str) -> bool:
    expires_at = str(record.get("serving_expires_at") or record.get("expires_at") or "")
    return bool(expires_at) and expires_at < today


def _parse_iso_datetime(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def inventory_ttl_hours(record: dict) -> float:
    block = str(record.get("primary_block") or "")
    policy = INVENTORY_BLOCK_REGISTRY.get(block, {})
    return float(record.get("serving_ttl_hours") or policy.get("serving_ttl_hours") or 24.0)


def inventory_age_hours(record: dict, *, now: datetime | None = None) -> float | None:
    seen = _parse_iso_datetime(str(record.get("last_seen_at") or ""))
    if seen is None:
        return None
    now_dt = now or now_london()
    if seen.tzinfo is not None and now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=seen.tzinfo)
    if seen.tzinfo is None and now_dt.tzinfo is not None:
        seen = seen.replace(tzinfo=now_dt.tzinfo)
    return max(0.0, (now_dt - seen).total_seconds() / 3600)


def passes_ttl_contract(record: dict, *, now: datetime | None = None) -> tuple[bool, str]:
    age = inventory_age_hours(record, now=now)
    if age is None:
        return False, "missing_last_seen_at"
    if age > inventory_ttl_hours(record):
        return False, "ttl_expired"
    return True, "ttl_ok"


def _draft_line_future_date_conflicts_with_fact(record: dict, *, today: str) -> bool:
    fact = record.get("fact_card") if isinstance(record.get("fact_card"), dict) else {}
    fact_raw = str(fact.get("date_start") or "").strip()[:10]
    line = str(record.get("draft_line") or "")
    if not fact_raw or not line:
        return False
    try:
        fact_day = date.fromisoformat(fact_raw)
        today_day = date.fromisoformat(today)
    except ValueError:
        return False
    if fact_day >= today_day:
        return False
    for day_raw, month_raw in re.findall(r"\b([0-3]?\d)\s+([а-яё]+)\b", line.lower()):
        month = _RU_MONTHS.get(month_raw)
        if not month:
            continue
        try:
            mentioned = date(today_day.year, month, int(day_raw))
        except ValueError:
            continue
        if mentioned >= today_day and mentioned > fact_day:
            return True
    return False


def inventory_fact_ready(record: dict) -> bool:
    if record.get("render_ready"):
        return True
    status = str(record.get("quality_status") or "")
    missing = [str(item) for item in (record.get("missing_facts") or [])]
    return status == "needs_text" and missing == ["draft_line"]


def ticket_reaches_morning(record: dict) -> bool:
    """Tickets reach the morning issue only on a real reason: new-on-sale,
    near-date, notable tier, or milestone — otherwise inventory_only."""
    fact = record.get("fact_card") if isinstance(record.get("fact_card"), dict) else {}
    if str(fact.get("ticket_type") or "") in _TICKET_MORNING_TYPES:
        return True
    if str(fact.get("tier") or "").upper() in {"A", "B"}:
        return True
    return bool(record.get("milestone"))


def passes_morning_contract(record: dict, *, today: str | None = None) -> tuple[bool, str]:
    """Bounded gate for what the writer may see. Returns (ok, reason). Stale /
    dead-link / expired never pass as fresh; tickets without a morning reason
    fall to inventory_only."""
    today = today or today_london()
    if not inventory_fact_ready(record):
        return False, "missing_facts"
    if str(record.get("action_url_liveness") or record.get("liveness_status") or "") == "dead":
        return False, "dead_link"
    if _is_expired(record, today):
        return False, "expired"
    block = str(record.get("primary_block") or "")
    fact = record.get("fact_card") if isinstance(record.get("fact_card"), dict) else {}
    if block in {
        "next_7_days",
        "weekend_activities",
        "ticket_radar",
        "future_announcements",
        "outside_gm_tickets",
        "professional_events",
        "russian_events",
    }:
        start = str(fact.get("next_occurrence") or fact.get("date_start") or "")[:10]
        end = str(fact.get("date_end") or start)[:10]
        if start and end and end < today:
            return False, "event_expired"
    if block == "weekend_activities":
        try:
            today_day = date.fromisoformat(today)
            if today_day.weekday() < 3:
                return False, "weekend_hidden_by_schedule"
            from news_digest.pipeline.weekend_inventory import current_weekend_window  # noqa: PLC0415

            window_start, window_end = current_weekend_window(today=today_day)
            start_day = date.fromisoformat(str(fact.get("next_occurrence") or fact.get("date_start") or "")[:10])
            end_day = date.fromisoformat(str(fact.get("date_end") or start_day.isoformat())[:10])
            if end_day < window_start or start_day > window_end:
                return False, "outside_current_weekend"
        except ValueError:
            return False, "missing_facts"
    if block == "openings":
        start = str(fact.get("next_occurrence") or fact.get("date_start") or "")[:10]
        end = str(fact.get("date_end") or start)[:10]
        event_name = str(fact.get("event_name") or record.get("title") or "").lower()
        dated_food_event = bool(
            start
            and re.search(r"\b(?:market|fair|festival|car boot|night market|food event|feast|supper|pop-?up|takeover|taking over)\b", event_name)
        )
        if dated_food_event and end and end < today:
            return False, "event_expired"
    ttl_ok, ttl_reason = passes_ttl_contract(record)
    if not ttl_ok:
        return False, ttl_reason
    if _draft_line_future_date_conflicts_with_fact(record, today=today):
        return False, "draft_line_date_conflicts_with_fact"
    if block == "transport" and not record.get("render_ready"):
        return False, "needs_live_refetch"
    if block in _TICKET_BLOCKS and not ticket_reaches_morning(record):
        return False, "inventory_only"
    if str(record.get("quality_status") or "") == "needs_text":
        return True, "morning_relevant_needs_text"
    return True, "morning_relevant"


def inventory_record_to_candidate(record: dict) -> dict:
    """Restore an inventory record to the normal candidate shape without
    bypassing the existing morning validation and writing contracts."""
    fact = record.get("fact_card") if isinstance(record.get("fact_card"), dict) else {}
    event = {
        "event_name": str(fact.get("event_name") or ""),
        "name": str(fact.get("event_name") or ""),
        "venue": str(fact.get("venue") or ""),
        "date_start": str(fact.get("date_start") or ""),
        "date": str(fact.get("date_start") or ""),
        "date_end": str(fact.get("date_end") or ""),
        "date_text": str(fact.get("date_text") or ""),
        "date_confidence": str(fact.get("date_confidence") or ""),
        "is_recurring": bool(fact.get("is_recurring")),
        "next_occurrence": str(fact.get("next_occurrence") or ""),
        "event_status": str(fact.get("event_status") or ""),
        "booking_url": str(record.get("booking_url") or ""),
    }
    candidate = {
        "fingerprint": str(record.get("fingerprint") or ""),
        "title": str(record.get("title") or fact.get("event_name") or ""),
        "summary": str(record.get("summary") or ""),
        "lead": str(record.get("lead") or ""),
        "published_at": str(record.get("published_at") or ""),
        "freshness_status": str(record.get("freshness_status") or ""),
        "practical_angle": str(record.get("practical_angle") or ""),
        "source_url": str(record.get("source_url") or ""),
        "booking_url": str(record.get("booking_url") or ""),
        "source_label": str(record.get("source_label") or ""),
        "primary_block": str(record.get("primary_block") or ""),
        "category": str(record.get("category") or ""),
        "source_report_category": str(record.get("source_report_category") or ""),
        "evidence_text": str(record.get("raw_evidence") or ""),
        "source_evidence": str(record.get("raw_evidence") or ""),
        "draft_line": str(record.get("draft_line") or ""),
        "event": event,
        "venue_scope": str(fact.get("venue_scope") or ""),
        "venue_city": str(fact.get("venue_city") or ""),
        "ticket_type": str(fact.get("ticket_type") or ""),
        "what_happened": str(fact.get("what_happened") or ""),
        "why_now": str(fact.get("why_now") or ""),
        "story_type": str(fact.get("story_type") or ""),
        "change_phase": str(fact.get("change_phase") or ""),
        "professional_event_match": fact.get("professional_event_match")
        if isinstance(fact.get("professional_event_match"), dict) else {},
        "professional_llm_match": fact.get("professional_llm_match")
        if isinstance(fact.get("professional_llm_match"), dict) else {},
        "professional_match_status": str(fact.get("professional_match_status") or ""),
        "russian_evidence": fact.get("russian_evidence")
        if isinstance(fact.get("russian_evidence"), dict) else {},
        "inventory_source": "night_inventory",
        "inventory_run_id": str(record.get("run_id") or ""),
        "inventory_wave": str(record.get("wave") or ""),
        "inventory_source_name": str(record.get("source_name") or ""),
        "inventory_last_seen_at": str(record.get("last_seen_at") or ""),
        "inventory_quality_status": str(record.get("quality_status") or ""),
        "inventory_missing_facts": list(record.get("missing_facts") or []),
        "inventory_needs_text": str(record.get("quality_status") or "") == "needs_text",
        "inventory_requires_refetch": str(record.get("primary_block") or "") == "transport",
        "include": True,
    }
    tier = str(fact.get("tier") or "")
    notability = fact.get("ticket_notability") if isinstance(fact.get("ticket_notability"), dict) else {}
    if tier or notability:
        candidate["ticket_notability"] = {**notability, "tier": tier or str(notability.get("tier") or "")}
    return candidate


def inventory_prewrite_is_current(record: dict, *, prompt_version: int | None = None) -> bool:
    """A cached night line is reusable only for the facts and prompt that wrote it."""
    if not str(record.get("draft_line") or "").strip():
        return False
    if int(record.get("schema_version") or 0) != INVENTORY_SCHEMA_VERSION:
        return False
    if prompt_version is not None and int(record.get("prompt_version") or 0) != int(prompt_version):
        return False
    candidate = inventory_record_to_candidate(record)
    return str(record.get("evidence_hash") or "") == compute_evidence_hash(candidate)


def prewrite_stable_inventory_candidate(candidate: dict) -> bool:
    """Night-only deterministic prewrite for stable blocks.

    Uses the same writer fallback templates the morning writer already trusts.
    Returns True only when a public line was actually written.
    """
    if str(candidate.get("draft_line") or "").strip():
        return False
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    policy = INVENTORY_BLOCK_REGISTRY.get(block, {})
    if str(policy.get("text_policy") or "") != "deterministic_or_morning":
        return False
    try:
        from news_digest.pipeline.writer import (  # noqa: PLC0415
            _build_event_fallback_line,
            _build_professional_event_fallback_line,
            _build_ticket_fallback_line,
        )
    except Exception:
        return False
    line = ""
    if category == "venues_tickets" or block == "ticket_radar":
        line = _build_ticket_fallback_line(candidate)
    elif category == "professional_events" or block == "professional_events":
        line = _build_professional_event_fallback_line(candidate)
    elif category == "culture_weekly":
        line = _build_event_fallback_line(candidate)
    if not line:
        return False
    try:
        from news_digest.pipeline.editor import _strip_empty_editor_ending  # noqa: PLC0415
        from news_digest.pipeline.writer import _draft_line_quality_errors  # noqa: PLC0415

        line, _ = _strip_empty_editor_ending(line, strip_short=True)
        if _draft_line_quality_errors(candidate, line):
            return False
    except Exception:
        return False
    candidate["draft_line"] = line
    candidate["draft_line_provider"] = "night_inventory_prewrite"
    candidate["draft_line_model"] = "deterministic_writer_fallback"
    candidate["draft_line_written_at"] = now_london().isoformat()
    return True


def _inventory_candidate_priority(candidate: dict) -> tuple[int, str]:
    block = str(candidate.get("primary_block") or "")
    if block == "ticket_radar":
        notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
        tier = str(notability.get("tier") or "").upper()
        tier_score = {"A": 500, "PROTECTED": 450, "B": 300, "C": 100}.get(tier, 0)
        ticket_type = str(candidate.get("ticket_type") or "")
        type_score = 200 if ticket_type in _TICKET_MORNING_TYPES else 0
        return (tier_score + type_score, str(candidate.get("title") or ""))
    if block == "weekend_activities":
        return (100 if str(candidate.get("draft_line") or "").strip() else 50, str(candidate.get("title") or ""))
    if block == "openings":
        return (100 if str(candidate.get("draft_line") or "").strip() else 40, str(candidate.get("title") or ""))
    return (0, str(candidate.get("title") or ""))


def _record_with_current_contract(record: dict, *, prompt_version: int | None = None) -> tuple[dict, bool]:
    working = dict(record)
    invalidated = bool(
        str(record.get("draft_line") or "").strip()
        and not inventory_prewrite_is_current(record, prompt_version=prompt_version)
    )
    if invalidated:
        working["draft_line"] = ""
        working["draft_line_provider"] = ""
        working["draft_line_model"] = ""
        working["draft_line_written_at"] = ""
    restored = inventory_record_to_candidate(working)
    quality_status, render_ready, missing_facts = evaluate_card(restored)
    working.update(
        {"quality_status": quality_status, "render_ready": render_ready, "missing_facts": missing_facts}
    )
    return working, invalidated


def build_morning_inventory_intake(
    records: list[dict],
    *,
    existing_fingerprints: set[str] | None = None,
    mode: str = "assist",
    today: str | None = None,
    prompt_version: int | None = None,
) -> tuple[list[dict], dict[str, object]]:
    """Restore every registry block marked assist into the normal morning path."""
    today = today or today_london()
    mode = str(mode or "assist").lower()
    existing = set(existing_fingerprints or set())
    candidates: list[dict] = []
    rejected: dict[str, int] = {}
    by_block: dict[str, dict[str, int]] = {}
    hybrid_signals: dict[str, int] = {}
    supply_candidates: list[dict] = []
    invalidated_prewrite = 0
    funnel = {"records": 0, "card_ready": 0, "morning_eligible": 0, "after_live_dedupe": 0, "inserted_after_cap": 0}
    for record in records:
        if not isinstance(record, dict):
            continue
        working_record, invalidated = _record_with_current_contract(record, prompt_version=prompt_version)
        if invalidated:
            invalidated_prewrite += 1
        block = str(working_record.get("primary_block") or "")
        policy = INVENTORY_BLOCK_REGISTRY.get(block, {})
        block_mode = str(policy.get("mode") or "retired")
        bucket = by_block.setdefault(
            block or "unknown",
            {"records": 0, "card_ready": 0, "eligible": 0, "after_live_dedupe": 0, "inserted": 0, "duplicates": 0},
        )
        bucket["records"] += 1
        funnel["records"] += 1
        if inventory_fact_ready(working_record):
            bucket["card_ready"] += 1
            funnel["card_ready"] += 1
        ok, reason = passes_morning_contract(working_record, today=today)
        supply_ok, _ = _passes_block_supply_contract(working_record, today=today)
        if supply_ok:
            supply_candidates.append(inventory_record_to_candidate(working_record))
        if block_mode == "hybrid":
            hybrid_signals[reason] = hybrid_signals.get(reason, 0) + 1
            continue
        if block_mode != "assist":
            continue
        if not ok:
            rejected[reason] = rejected.get(reason, 0) + 1
            continue
        bucket["eligible"] += 1
        funnel["morning_eligible"] += 1
        candidate = inventory_record_to_candidate(working_record)
        fp = str(candidate.get("fingerprint") or "")
        if fp and fp in existing:
            bucket["duplicates"] += 1
            rejected["duplicate_live_or_inventory"] = rejected.get("duplicate_live_or_inventory", 0) + 1
            continue
        if fp:
            existing.add(fp)
        candidate["inventory_intake_mode"] = mode
        candidates.append(candidate)
        bucket["after_live_dedupe"] += 1
        funnel["after_live_dedupe"] += 1
    candidates.sort(key=_inventory_candidate_priority, reverse=True)
    capped: list[dict] = []
    held_by_cap = 0
    kept_by_block: dict[str, int] = {}
    for candidate in candidates:
        block = str(candidate.get("primary_block") or "")
        policy = INVENTORY_BLOCK_REGISTRY.get(block, {})
        cap = int(policy.get("intake_cap") or 0)
        kept = kept_by_block.get(block, 0)
        notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
        a_tier = str(notability.get("tier") or "").upper() == "A"
        protected = block == "weekend_activities" or a_tier
        if cap and kept >= cap and not protected:
            held_by_cap += 1
            continue
        capped.append(candidate)
        kept_by_block[block] = kept + 1
        by_block[block]["inserted"] += 1
        funnel["inserted_after_cap"] += 1
    if held_by_cap:
        rejected["inventory_block_cap"] = rejected.get("inventory_block_cap", 0) + held_by_cap
    completeness = inventory_block_completeness(supply_candidates)
    cap_report = {
        block: int(policy.get("intake_cap") or 0)
        for block, policy in INVENTORY_BLOCK_REGISTRY.items()
        if int(policy.get("intake_cap") or 0)
    }
    report = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "mode": mode,
        "inserted_candidates": len(capped),
        "candidate_cap": cap_report,
        "held_by_cap": held_by_cap,
        "invalidated_prewrite": invalidated_prewrite,
        "rejected": rejected,
        "hybrid_signals": hybrid_signals,
        "funnel": funnel,
        "by_block": by_block,
        "completeness": completeness,
        "policy": "All block modes, freshness, completeness and caps come from INVENTORY_BLOCK_REGISTRY.",
    }
    return capped, report


def _passes_block_supply_contract(record: dict, *, today: str) -> tuple[bool, str]:
    ok, reason = passes_morning_contract(record, today=today)
    if ok:
        return True, reason
    if str(record.get("primary_block") or "") != "weekend_activities" or reason != "weekend_hidden_by_schedule":
        return False, reason
    fact = record.get("fact_card") if isinstance(record.get("fact_card"), dict) else {}
    try:
        from news_digest.pipeline.weekend_inventory import current_weekend_window  # noqa: PLC0415

        window_start, window_end = current_weekend_window(today=date.fromisoformat(today))
        start = date.fromisoformat(str(fact.get("next_occurrence") or fact.get("date_start") or "")[:10])
        end = date.fromisoformat(str(fact.get("date_end") or start.isoformat())[:10])
    except ValueError:
        return False, "missing_facts"
    if end < window_start or start > window_end:
        return False, "outside_current_weekend"
    ttl_ok, ttl_reason = passes_ttl_contract(record)
    if not ttl_ok:
        return False, ttl_reason
    return True, "weekend_supply_hidden"


def inventory_block_completeness(candidates: list[dict]) -> dict[str, object]:
    by_block: dict[str, dict[str, object]] = {}
    for block, policy in INVENTORY_BLOCK_REGISTRY.items():
        floor = int(policy.get("floor") or 0)
        rows = [c for c in candidates if isinstance(c, dict) and str(c.get("primary_block") or "") == block]
        sources = {str(c.get("source_label") or "") for c in rows if str(c.get("source_label") or "")}
        with_text = sum(1 for c in rows if str(c.get("draft_line") or "").strip())
        min_sources = int(policy.get("min_sources") or 0)
        optional = bool(policy.get("optional"))
        block_mode = str(policy.get("mode") or "")
        applicable = block_mode == "assist"
        sufficient = None if not applicable else (optional or (len(rows) >= floor and len(sources) >= min_sources))
        by_block[block] = {
            "heading": PRIMARY_BLOCKS.get(block, block),
            "mode": block_mode,
            "floor": floor,
            "candidate_count": len(rows),
            "with_text": with_text,
            "source_count": len(sources),
            "min_sources": min_sources,
            "optional": optional,
            "completeness_applicable": applicable,
            "block_sufficient": sufficient,
            "completeness_basis": "post_card_contract_before_visibility_schedule_and_intake_cap",
        }
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "blocks": by_block,
        "sufficient_blocks": [block for block, row in by_block.items() if row.get("block_sufficient") is True],
        "insufficient_blocks": [block for block, row in by_block.items() if row.get("block_sufficient") is False],
        "not_applicable_blocks": [block for block, row in by_block.items() if row.get("block_sufficient") is None],
    }


def operational_night_category_health(
    source_run_log_rows: list[dict], *, current_day: str | None = None
) -> dict[str, dict[str, object]]:
    """One operational verdict per category, based only on its latest run."""
    current_day = current_day or today_london()
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in source_run_log_rows:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or "")
        if not category:
            continue
        run_at = str(row.get("run_at_london") or "")
        run_key = str(row.get("run_id") or f"{run_at[:10]}:{row.get('wave') or ''}")
        grouped.setdefault((category, run_key), []).append(row)
    latest: dict[str, tuple[str, list[dict]]] = {}
    for (category, run_key), rows in grouped.items():
        run_at = max((str(row.get("run_at_london") or "") for row in rows), default="")
        if category not in latest or run_at > latest[category][0]:
            latest[category] = (run_at, rows)
    out: dict[str, dict[str, object]] = {}
    for category, (run_at, rows) in latest.items():
        expected = max((int(row.get("expected_sources") or 0) for row in rows), default=0) or len(rows)
        checked = sum(1 for row in rows if row.get("checked"))
        errors = sum(int(row.get("errors") or 0) for row in rows)
        found = sum(int(row.get("found") or 0) for row in rows)
        run_day = run_at[:10]
        status = "ok" if run_day == current_day and checked == expected and errors == 0 else "degraded"
        if run_day != current_day:
            status = "stale"
        elif checked == 0:
            status = "failed"
        error_reasons = [
            {"source": str(row.get("source") or ""), "reason": str(row.get("error") or "source_error")}
            for row in rows
            if int(row.get("errors") or 0) or not row.get("checked")
        ]
        out[category] = {
            "run_id": str(rows[0].get("run_id") or ""),
            "run_at_london": run_at,
            "current_day": current_day,
            "expected_sources": expected,
            "checked_sources": checked,
            "source_errors": errors,
            "error_reasons": error_reasons,
            "found_this_run": found,
            "status": status,
        }
    return out


def inventory_source_replacement_plan(
    intake_report: dict[str, object],
    category_health: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    """Decide which existing broad scans may be replaced by night inventory."""
    completeness = intake_report.get("completeness") if isinstance(intake_report, dict) else {}
    blocks = completeness.get("blocks") if isinstance(completeness, dict) else {}
    plan: dict[str, dict[str, object]] = {}
    for category, output_blocks in inventory_category_output_blocks().items():
        if category == "synthetic":
            continue
        health = category_health.get(category, {})
        supported = all(
            str(INVENTORY_BLOCK_REGISTRY.get(block, {}).get("mode") or "") == "assist"
            for block in output_blocks
        )
        replacement_allowed = all(
            bool(INVENTORY_BLOCK_REGISTRY.get(block, {}).get("source_replacement_allowed"))
            for block in output_blocks
        )
        block_sufficient = supported and all(
            bool(INVENTORY_BLOCK_REGISTRY.get(block, {}).get("optional"))
            or (isinstance(blocks.get(block), dict) and blocks[block].get("block_sufficient"))
            for block in output_blocks
        )
        scan_complete = str(health.get("status") or "") == "ok"
        safe = bool(supported and replacement_allowed and block_sufficient and scan_complete)
        reason = (
            "safe_post_contract_replacement" if safe
            else "source_replacement_not_enabled" if not replacement_allowed
            else "output_blocks_not_inventory_assisted" if not supported
            else "current_night_scan_incomplete" if not scan_complete
            else "post_contract_block_insufficient"
        )
        plan[category] = {
            "safe_to_skip": safe,
            "reason": reason,
            "output_blocks": sorted(output_blocks),
            "scan_complete": scan_complete,
            "block_sufficient": block_sufficient,
            "source_replacement_allowed": replacement_allowed,
            "night_health": health,
        }
    return plan


def summarise_morning_intake(
    records: list[dict],
    *,
    today: str | None = None,
    now: datetime | None = None,
    prompt_version: int | None = None,
) -> dict[str, object]:
    """Report-only morning inventory intake: what could be considered without
    mutating candidates.json or the public issue."""
    today = today or today_london()
    totals = {
        "records": 0,
        "fact_ready": 0,
        "render_ready": 0,
        "needs_text": 0,
        "eligible": 0,
        "converted_candidates": 0,
    }
    reasons: dict[str, int] = {}
    by_block: dict[str, dict[str, int]] = {
        block: {"records": 0, "fact_ready": 0, "eligible": 0, "needs_text": 0, "render_ready": 0}
        for block in INVENTORY_BLOCK_REGISTRY
    }
    examples: list[dict[str, object]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        record, _ = _record_with_current_contract(record, prompt_version=prompt_version)
        block = str(record.get("primary_block") or "unknown")
        bucket = by_block.setdefault(block, {"records": 0, "fact_ready": 0, "eligible": 0, "needs_text": 0, "render_ready": 0})
        totals["records"] += 1
        bucket["records"] += 1
        if inventory_fact_ready(record):
            totals["fact_ready"] += 1
            bucket["fact_ready"] += 1
        if record.get("render_ready"):
            totals["render_ready"] += 1
            bucket["render_ready"] += 1
        if str(record.get("quality_status") or "") == "needs_text":
            totals["needs_text"] += 1
            bucket["needs_text"] += 1
        ok, reason = passes_morning_contract(record, today=today)
        reasons[reason] = reasons.get(reason, 0) + 1
        if ok:
            totals["eligible"] += 1
            totals["converted_candidates"] += 1
            bucket["eligible"] += 1
            if len(examples) < 20:
                candidate = inventory_record_to_candidate(record)
                examples.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": candidate.get("title"),
                        "primary_block": candidate.get("primary_block"),
                        "quality_status": candidate.get("inventory_quality_status"),
                        "age_hours": inventory_age_hours(record, now=now),
                    }
                )
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "mode": "report_only",
        "morning_consumed": False,
        "totals": totals,
        "reasons": reasons,
        "by_block": by_block,
        "eligible_examples": examples,
    }


# ── 8.7 Re-entry of yesterday's unshown-but-still-relevant items ───────────

def published_fingerprints(published_facts: dict | None) -> set[str]:
    facts = (published_facts or {}).get("facts") if isinstance(published_facts, dict) else None
    return {str(item.get("fingerprint") or "") for item in (facts or []) if isinstance(item, dict)}


def reentry_candidates(prior_records: list[dict], published_facts: dict | None, *, today: str | None = None) -> list[dict]:
    """Yesterday's inventory_only / reserve items that are still render_ready and
    not expired re-enter selection — deduped against `published_facts` so a
    re-entered card can never repeat one already shown. Reuses the existing
    dedup surface; introduces no second dedup system."""
    today = today or today_london()
    already = published_fingerprints(published_facts)
    out: list[dict] = []
    for record in prior_records:
        if not isinstance(record, dict):
            continue
        if str(record.get("fingerprint") or "") in already:
            continue
        if not record.get("render_ready"):
            continue
        if _is_expired(record, today):
            continue
        out.append(record)
    return out


# ── 8.9 Per-category health verdict + fallback ────────────────────────────

def classify_category_health(row: dict[str, object]) -> str:
    """ok / partial / failed / empty_legit / empty_suspicious for one category,
    from aggregated source_run_log counts. A category that fetched nothing is
    `failed`, not indistinguishable from a quiet day; a category that fetched
    fine but errored on every item is `empty_suspicious`, not `empty_legit`."""
    checked = int(row.get("checked_count") or 0)
    fetched = int(row.get("fetched_count") or 0)
    found = int(row.get("found") or 0)
    enriched = int(row.get("enriched") or 0)
    errors = int(row.get("errors") or 0)
    if checked == 0 or fetched == 0:
        return "failed"
    if found == 0:
        return "empty_suspicious" if errors > 0 else "empty_legit"
    if enriched == 0 or errors > 0:
        return "partial"
    return "ok"


def aggregate_category_health(source_run_log_rows: list[dict]) -> dict[str, dict[str, object]]:
    """Roll up per-source source_run_log rows into one verdict per category.
    Reuses the existing log rather than a second collection pass."""
    by_category: dict[str, dict[str, object]] = {}
    for row in source_run_log_rows:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or "unknown")
        agg = by_category.setdefault(
            category,
            {"checked_count": 0, "fetched_count": 0, "found": 0, "enriched": 0, "errors": 0, "source_count": 0},
        )
        agg["checked_count"] = int(agg["checked_count"]) + (1 if row.get("checked") else 0)
        agg["fetched_count"] = int(agg["fetched_count"]) + (1 if row.get("fetched") else 0)
        agg["found"] = int(agg["found"]) + int(row.get("found") or 0)
        agg["enriched"] = int(agg["enriched"]) + int(row.get("enriched") or 0)
        agg["errors"] = int(agg["errors"]) + int(row.get("errors") or 0)
        agg["source_count"] = int(agg["source_count"]) + 1
    return {
        category: {**agg, "verdict": classify_category_health(agg)}
        for category, agg in by_category.items()
    }


def categories_needing_live_fallback(category_health: dict[str, dict[str, object]]) -> list[str]:
    """Categories whose inventory is stale/failed/suspicious — the morning build
    should fall back to a bounded live crawl for these rather than ship a
    silently-empty block as a quiet day."""
    return sorted(
        category
        for category, health in category_health.items()
        if str(health.get("verdict") or "") in {"failed", "stale", "empty_suspicious"}
    )


# ── 8.1 cross-stage integrity (collect → candidates.json) ─────────────────

def verify_collect_conservation(source_run_log_rows: list[dict], candidates_json_count: int) -> dict[str, object]:
    """Cross-stage: did fewer candidates survive into candidates.json than
    collect found? A small POSITIVE delta is expected (synthetic weather/
    transport cards, added outside the per-source count). Only a net shortfall
    (candidates.json has FEWER than collect found) means something disappeared
    between collect and candidates.json."""
    collected_found = sum(int(row.get("found") or 0) for row in source_run_log_rows if isinstance(row, dict))
    delta = candidates_json_count - collected_found
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "collected_found": collected_found,
        "candidates_json_count": candidates_json_count,
        "delta": delta,
        "conserved": delta >= 0,
    }


# ── 8.11 Light morning-relevance fields (ahead of full 0–100 scoring) ─────

def annotate_morning_relevance(candidate: dict, rendered_fingerprints: set[str]) -> None:
    """Attach the light fields backlog 8.11 asks for, ahead of the deferred
    full 0–100 unified scorer. Mutates candidate in place; without these the
    inventory would be opaque."""
    disposition = classify_disposition(candidate, rendered_fingerprints)
    is_must_show = str(candidate.get("publish_plan_status") or "") == "must_show" or bool(candidate.get("is_lead"))
    candidate["selection_bucket"] = (
        "show_candidate" if disposition == "shown"
        else "reserve" if disposition in {"reserve", "inventory_only"}
        else "deferred"
    )
    candidate["morning_relevance_status"] = "relevant" if disposition == "shown" else "not_shown"
    candidate["morning_relevance_reason"] = disposition
    candidate["inventory_priority"] = 100 if is_must_show else 50 if disposition == "shown" else 10
