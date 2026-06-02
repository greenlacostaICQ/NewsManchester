"""Persistent per-run source health log (Phase 2 #2).

The release report already computes a rich `source_status` table for the
current run, but it is overwritten every day. This module appends one
compact row per run to ``data/state/source_health_history.jsonl`` so a
source's contribution can be tracked over time — the data feeds the
simple anomaly check (Phase 2 #4) that flags a feed quietly going dark.

Append-only, idempotent by ``run_date_london`` (a re-run of the same day
replaces that day's row), pruned to a rolling window. Never raises into
the release path: a logging failure must not block the digest.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

HISTORY_FILENAME = "source_health_history.jsonl"
_RETENTION_DAYS = 120


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def build_row(
    *,
    run_date_london: str,
    pipeline_run_id: str,
    run_at_london: str,
    source_status: dict,
) -> dict:
    """Flatten today's source_status into one compact history row."""
    counts = dict(source_status.get("counts") or {})
    sources = []
    for entry in source_status.get("sources") or []:
        if not isinstance(entry, dict):
            continue
        sources.append(
            {
                "name": str(entry.get("name") or ""),
                "category": str(entry.get("category") or ""),
                "status": str(entry.get("status") or ""),
                "trial": bool(entry.get("trial")),
                "raw": int(entry.get("raw_count") or 0),
                "curated": int(entry.get("curated_count") or entry.get("accepted_count") or 0),
                "rendered": int(entry.get("rendered_count") or 0),
                "failures": int(entry.get("failure_count") or 0),
            }
        )
    return {
        "run_date_london": run_date_london,
        "pipeline_run_id": pipeline_run_id,
        "run_at_london": run_at_london,
        "totals": {
            "ok": int(counts.get("ok") or 0),
            "partial": int(counts.get("partial") or 0),
            "stale": int(counts.get("stale") or 0),
            "empty": int(counts.get("empty") or 0),
            "failed": int(counts.get("failed") or 0),
            "zero_yield": int(counts.get("zero_yield") or 0),
        },
        "sources": sources,
    }


def append_row(state_dir: Path, row: dict) -> Path:
    """Append (idempotent by run_date_london) and prune to the window."""
    path = state_dir / HISTORY_FILENAME
    try:
        rows = [
            obj
            for obj in _read_jsonl(path)
            if obj.get("run_date_london") != row.get("run_date_london")
        ]
        rows.append(row)
        rows = rows[-_RETENTION_DAYS:]
        path.write_text(
            "\n".join(json.dumps(obj, ensure_ascii=False) for obj in rows) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:  # pragma: no cover - disk failure should not block release
        logger.warning("source_health_history: write failed: %s", exc)
    return path


def load_history(state_dir: Path) -> list[dict]:
    return _read_jsonl(state_dir / HISTORY_FILENAME)
