from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
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

    Called from two places:
      1. After build-digest gate passes (the moment the pipeline commits
         to today's content), so tomorrow's dedupe sees today's items
         even if the actual Telegram send fails or is delayed.
      2. After successful Telegram send, as a safety net for older code
         paths that bypass the gate.

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
    """Update last_sent_digest.html on actual Telegram send.

    Calls update_published_facts as a belt-and-braces safety net. The
    primary write path is now release.build_release at gate-pass time.
    """

    state_dir = project_root / "data" / "state"
    paths = ensure_history_files(state_dir)

    if source_path.exists():
        shutil.copyfile(source_path, paths["last_sent_digest"])

    update_published_facts(project_root, candidates)
    return paths
