from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import shutil

from news_digest.pipeline.common import normalize_title, read_json, today_london, write_json

# Facts older than this are pruned from published_facts.json.
# Must be >= the dedupe look-back window (7 days) with margin.
_PUBLISHED_FACTS_RETENTION_DAYS = 14


def ensure_history_files(state_dir: Path) -> dict[str, Path]:
    state_dir.mkdir(parents=True, exist_ok=True)

    last_sent_path = state_dir / "last_sent_digest.html"
    published_facts_path = state_dir / "published_facts.json"
    dedupe_memory_path = state_dir / "dedupe_memory.json"

    if not last_sent_path.exists():
        last_sent_path.write_text("", encoding="utf-8")
    if not published_facts_path.exists():
        write_json(published_facts_path, {"last_updated_london": None, "facts": []})
    if not dedupe_memory_path.exists():
        write_json(dedupe_memory_path, {"last_updated_london": None, "decisions": []})

    return {
        "last_sent_digest": last_sent_path,
        "published_facts": published_facts_path,
        "dedupe_memory": dedupe_memory_path,
    }


def update_published_facts(project_root: Path, candidates: list[dict]) -> dict[str, Path]:
    """Idempotently merge candidates into published_facts.json.

    Called only after successful Telegram delivery. Gate-pass alone is
    not enough: if sending fails, tomorrow's dedupe must not treat the
    unsent draft as already published.

    Merge is idempotent on candidate fingerprint: re-running on the same
    day refreshes last_published_day_london and preserves
    first_published_day_london.
    """

    state_dir = project_root / "data" / "state"
    paths = ensure_history_files(state_dir)

    payload = read_json(paths["published_facts"], {"last_updated_london": None, "facts": []})
    existing_facts = payload.get("facts", [])
    if not isinstance(existing_facts, list):
        existing_facts = []

    by_fingerprint = {str(item.get("fingerprint")): item for item in existing_facts if isinstance(item, dict)}
    run_day = today_london()
    run_day_date = datetime.strptime(run_day, "%Y-%m-%d").date()

    for candidate in candidates:
        fingerprint = str(candidate.get("fingerprint") or "").strip()
        if not fingerprint:
            continue
        entry = by_fingerprint.get(fingerprint, {})
        entry.update(
            {
                "fingerprint": fingerprint,
                "title": candidate.get("title"),
                "normalized_title": normalize_title(str(candidate.get("title") or "")),
                "category": candidate.get("category"),
                "primary_block": candidate.get("primary_block"),
                "source_label": candidate.get("source_label"),
                "published_at": candidate.get("published_at"),
                # A0 enrichment: borough + change_type so dedupe and "what
                # was previously published" queries can filter by them.
                "borough": _extract_borough_from_blob(candidate) or "",
                "change_type": candidate.get("change_type") or "",
                "first_published_day_london": entry.get("first_published_day_london") or run_day,
                "last_published_day_london": run_day,
            }
        )
        by_fingerprint[fingerprint] = entry

    # Prune facts older than retention window to keep the file bounded.
    cutoff = str(run_day_date - timedelta(days=_PUBLISHED_FACTS_RETENTION_DAYS))
    retained = {
        fp: item for fp, item in by_fingerprint.items()
        if str(item.get("first_published_day_london") or "") >= cutoff
    }

    write_json(
        paths["published_facts"],
        {
            "last_updated_london": run_day,
            "facts": sorted(retained.values(), key=lambda item: str(item.get("fingerprint"))),
        },
    )

    return paths


def record_delivery_artifacts(project_root: Path, source_path: Path, candidates: list[dict]) -> dict[str, Path]:
    """Update last_sent_digest.html and published facts after actual Telegram send."""

    state_dir = project_root / "data" / "state"
    paths = ensure_history_files(state_dir)

    if source_path.exists():
        shutil.copyfile(source_path, paths["last_sent_digest"])

    if candidates:
        update_published_facts(project_root, candidates)
    return paths


# ── A0 — Daily Index Snapshot ────────────────────────────────────────────


_GM_BOROUGH_RE = re.compile(
    r"\b(Bolton|Bury|Manchester|Oldham|Rochdale|Salford|Stockport|"
    r"Tameside|Trafford|Wigan)\b"
)


def _extract_borough_from_blob(candidate: dict) -> str:
    """Pull the first GM borough mentioned in title/summary/lead/evidence.

    Returns an empty string when nothing matches. Used for both
    published_facts enrichment and the daily index snapshot.
    """
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "lead", "summary", "evidence_text")
    )
    m = _GM_BOROUGH_RE.search(blob)
    return m.group(1) if m else ""


def _writer_drop_reason_by_fingerprint(writer_report: dict | None) -> dict[str, str]:
    """Map fingerprint → human-readable writer-stage drop reason."""
    out: dict[str, str] = {}
    if not isinstance(writer_report, dict):
        return out
    for drop in writer_report.get("dropped_candidates") or []:
        if not isinstance(drop, dict):
            continue
        fp = str(drop.get("fingerprint") or "")
        if not fp:
            continue
        reasons = drop.get("reasons") or []
        out[fp] = "; ".join(str(r) for r in reasons) if reasons else "writer drop"
    return out


def write_daily_index_snapshot(project_root: Path) -> Path | None:
    """A0: append-only daily snapshot covering EVERY candidate the
    pipeline saw today — published and rejected alike.

    Format: ``data/state/daily_index/{YYYY-MM-DD}.jsonl`` (one JSON
    object per line). Each record carries the fields the spec calls for:
    title, url, fingerprint, source, category, borough, included,
    reject_reason, change_type. Plus pipeline_run_id and primary_block
    for context.

    Called from the release stage after the gate decision so the snapshot
    is written regardless of delivery outcome. Idempotent within a day:
    re-running the same pipeline overwrites today's snapshot rather than
    appending duplicates.
    """
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    if not candidates_path.exists():
        return None
    payload = read_json(candidates_path, {"candidates": []})
    candidates = payload.get("candidates") or []
    pipeline_run_id = str(payload.get("pipeline_run_id") or "")

    writer_report = read_json(state_dir / "writer_report.json", {})
    drop_reasons = _writer_drop_reason_by_fingerprint(writer_report)

    snapshot_dir = state_dir / "daily_index"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    today = today_london()
    snapshot_path = snapshot_dir / f"{today}.jsonl"

    lines: list[str] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        fp = str(c.get("fingerprint") or "")
        included = bool(c.get("include"))
        reject_reason = ""
        if not included:
            # Prefer curator/dedupe reason; writer drops are for items
            # that passed curator but failed quality gates.
            reject_reason = str(c.get("reason") or "").strip()
        elif fp in drop_reasons:
            # Curator included it, writer dropped it post-quality-check.
            reject_reason = drop_reasons[fp]
            included = False
        record = {
            "ts": today,
            "pipeline_run_id": pipeline_run_id,
            "fingerprint": fp,
            "title": c.get("title") or "",
            "url": c.get("source_url") or "",
            "source_label": c.get("source_label") or "",
            "category": c.get("category") or "",
            "primary_block": c.get("primary_block") or "",
            "borough": _extract_borough_from_blob(c),
            "included": included,
            "change_type": c.get("change_type") or "",
            "reject_reason": reject_reason,
        }
        lines.append(json.dumps(record, ensure_ascii=False))

    # Rewrite today's file (idempotent within a single Run; re-runs are
    # rare but possible during retries).
    snapshot_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    # Prune snapshots older than 60 days so the folder stays bounded.
    cutoff = datetime.strptime(today, "%Y-%m-%d").date() - timedelta(days=60)
    for f in snapshot_dir.glob("*.jsonl"):
        try:
            d = datetime.strptime(f.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < cutoff:
            f.unlink(missing_ok=True)

    return snapshot_path
