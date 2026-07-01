"""Backlog item 8 (semantic layer): per-category collection health and a
cross-stage no-loss check — applied to the EXISTING single daily collect/
select run, not a new night-job architecture.

Scope note: this module intentionally does NOT schedule or run new night-time
collection waves (that requires new scraper coverage per source plus a
production cron/launchd change — see IMPROVEMENT_LOG 0033 for what was
deferred and why). Two things it deliberately does NOT reinvent, because they
already exist and cover the same intent:
  - per-candidate disposition + reason: release.py's `_disposition_for_candidate`
    / `final_status` / `_candidate_selection_reason` already give a reason for
    every non-visible item in final_selection_report.json;
  - render-ready gating and reserve pooling: llm_rewrite.py's `_publish_plan_status`
    (0030) and editor.py's `_PrevalidatedReservePool` (0031) already enforce
    show=renderable and prevalidated same-block reserve.

What this module adds instead:
  - a per-category health verdict from the existing source_run_log, so a
    silently-dead source reads as `failed`/`empty_suspicious`, not as
    indistinguishable from a quiet news day;
  - a cross-stage conservation check (collected-at-source vs. survived-into-
    candidates.json), tolerant of the small positive slack from synthetic
    weather/transport cards, that flags only a real net LOSS between collect
    and candidates.json;
  - cache-key fields (`prompt_version`, structured story facts) for
    llm_rewrite's existing reuse-memory, so a changed fact (casualty count,
    court stage) invalidates a cached line even when the change sits past the
    evidence-text truncation point. (Wired into llm_rewrite.py's
    `_candidate_content_hash`.)
"""

from __future__ import annotations

INVENTORY_SCHEMA_VERSION = 1


def classify_category_health(row: dict[str, object]) -> str:
    """ok / partial / failed / empty_legit / empty_suspicious for one
    category, from the aggregated counts in source_run_log.jsonl (item 1).
    A category that fetched nothing at all is `failed`, not indistinguishable
    from a quiet day; a category that fetched fine but errored on every item
    is `empty_suspicious`, not `empty_legit`."""
    checked = int(row.get("checked_count") or 0)
    fetched = int(row.get("fetched_count") or 0)
    found = int(row.get("found") or 0)
    enriched = int(row.get("enriched") or 0)
    errors = int(row.get("errors") or 0)
    if checked == 0 or fetched == 0:
        return "failed"
    if found == 0:
        return "empty_suspicious" if errors > 0 else "empty_legit"
    if enriched == 0:
        return "partial"
    if errors > 0:
        return "partial"
    return "ok"


def aggregate_category_health(source_run_log_rows: list[dict]) -> dict[str, dict[str, object]]:
    """Roll up per-source source_run_log rows (item 1) into one verdict per
    category. Reuses the existing log rather than a second collection pass."""
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


def verify_conservation(source_run_log_rows: list[dict], candidates_json_count: int) -> dict[str, object]:
    """Cross-stage check: did fewer candidates survive into candidates.json
    than collect actually found? A small POSITIVE delta (candidates_json_count
    slightly above collected_found) is expected and healthy — synthetic
    weather/transport status cards are added outside the per-source collect
    count. Only a net shortfall (candidates.json has FEWER than collect
    found) indicates something silently disappeared between collect and
    candidates.json, and that is what `conserved` flags."""
    collected_found = sum(
        int(row.get("found") or 0) for row in source_run_log_rows if isinstance(row, dict)
    )
    delta = candidates_json_count - collected_found
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "collected_found": collected_found,
        "candidates_json_count": candidates_json_count,
        "delta": delta,
        "conserved": delta >= 0,
    }


def evidence_cache_extra_fields(candidate: dict) -> dict[str, str]:
    """Structured story facts folded into the reuse-cache hash alongside the
    existing truncated evidence text. Harmless no-op for events/tickets
    (fields empty there); for hard news it means a materially changed fact —
    casualty count, court stage, who's affected — invalidates the cached line
    even if the change sits past the evidence-text truncation point."""
    return {
        "what_happened": str(candidate.get("what_happened") or "")[:300],
        "who_affected": str(candidate.get("who_affected") or "")[:200],
        "why_now": str(candidate.get("why_now") or "")[:200],
        "event_type": str(candidate.get("story_type") or candidate.get("event_type") or ""),
    }
