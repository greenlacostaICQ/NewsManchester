from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from news_digest.pipeline.common import (
    fingerprint_for_candidate,
    normalize_title,
    now_london,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.history import ensure_history_files


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


def initialize_candidates_state(project_root: Path, *, overwrite: bool = False) -> StageResult:
    state_dir = project_root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    ensure_history_files(state_dir)

    path = state_dir / "candidates.json"
    if overwrite or not path.exists():
        write_json(
            path,
            {
                "run_at_london": now_london().isoformat(),
                "run_date_london": today_london(),
                "stage_status": "incomplete",
                "candidates": [
                    {
                        "title": "",
                        "category": "media_layer",
                        "summary": "",
                        "source_url": "",
                        "source_label": "",
                        "primary_block": "last_24h",
                        "include": False,
                        "dedupe_decision": "drop",
                        "carry_over_label": "",
                        "reason": "",
                        "matched_previous_fingerprint": "",
                    }
                ],
            },
        )

    return StageResult(True, f"Candidates state initialized at {path}.", path)


def dedupe_candidates(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    paths = ensure_history_files(state_dir)
    candidates_path = state_dir / "candidates.json"
    report_path = paths["dedupe_memory"]

    payload = read_json(candidates_path, {"candidates": []})
    candidates = payload.get("candidates", [])
    published = read_json(paths["published_facts"], {"facts": []}).get("facts", [])
    published_by_fp = {
        str(item.get("fingerprint")): item for item in published if isinstance(item, dict) and item.get("fingerprint")
    }
    published_titles = [
        item for item in published if isinstance(item, dict) and item.get("normalized_title")
    ]

    errors: list[str] = []
    decisions: list[dict] = []

    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict):
            errors.append(f"Candidate #{index} is not an object.")
            continue

        fingerprint = fingerprint_for_candidate(candidate)
        candidate["fingerprint"] = fingerprint
        previous = published_by_fp.get(fingerprint)
        normalized_title = normalize_title(str(candidate.get("title") or ""))
        similar_previous = _similar_published_titles(normalized_title, published_titles)
        candidate.setdefault("reason", "")
        candidate.setdefault("matched_previous_fingerprint", "")

        decision = str(candidate.get("dedupe_decision") or "").strip()
        category = str(candidate.get("category") or "").strip()
        primary_block = str(candidate.get("primary_block") or "").strip()
        operational_repeat_ok = primary_block in {"weather", "transport", "short_actions", "today_focus"}
        same_day_repeat_ok = (
            previous is not None
            and str(previous.get("last_published_day_london") or "").strip() == today_london()
        )
        if previous is not None and (operational_repeat_ok or same_day_repeat_ok):
            candidate["dedupe_decision"] = "new"
            candidate["include"] = True
            candidate["reason"] = candidate.get("reason") or "Operational block repeat is allowed while it remains relevant."
        elif previous is not None:
            candidate["matched_previous_fingerprint"] = fingerprint
            if decision not in {"carry_over_with_label", "new_phase"}:
                candidate["dedupe_decision"] = "drop"
                candidate["include"] = False
                candidate["reason"] = candidate.get("reason") or "Repeat without new phase."
            elif decision == "carry_over_with_label" and not candidate.get("carry_over_label"):
                candidate["dedupe_decision"] = "drop"
                candidate["include"] = False
                candidate["reason"] = "Carry-over without carry_over_label."
        elif decision not in {"drop", "new", "new_phase", "carry_over_with_label"}:
            candidate["dedupe_decision"] = "drop"
            candidate["include"] = False
            candidate["reason"] = "Invalid dedupe decision."

        if not candidate.get("reason"):
            errors.append(f"Candidate #{index} is missing reason.")

        decisions.append(
            {
                "fingerprint": fingerprint,
                "title": candidate.get("title"),
                "decision": candidate.get("dedupe_decision"),
                "reason": candidate.get("reason"),
                "matched_previous_fingerprint": candidate.get("matched_previous_fingerprint"),
                "carry_over_label": candidate.get("carry_over_label"),
                "similar_previous": similar_previous,
            }
        )

    payload["run_at_london"] = now_london().isoformat()
    payload["run_date_london"] = today_london()
    payload["stage_status"] = "complete" if not errors else "failed"
    write_json(candidates_path, payload)
    write_json(
        report_path,
        {
            "last_updated_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "decisions": decisions,
        },
    )

    return StageResult(not errors, "Dedupe completed." if not errors else "Dedupe completed with errors.", report_path)


def _similar_published_titles(normalized_title: str, published_titles: list[dict]) -> list[dict]:
    title_tokens = set(normalized_title.split())
    if len(title_tokens) < 4:
        return []
    matches: list[dict] = []
    for item in published_titles:
        previous_title = str(item.get("normalized_title") or "")
        previous_tokens = set(previous_title.split())
        if len(previous_tokens) < 4:
            continue
        overlap = len(title_tokens & previous_tokens) / max(len(title_tokens | previous_tokens), 1)
        if overlap >= 0.55:
            matches.append(
                {
                    "fingerprint": item.get("fingerprint"),
                    "title": item.get("title"),
                    "overlap": round(overlap, 2),
                }
            )
    return matches[:3]
