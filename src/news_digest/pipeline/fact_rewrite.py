from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from news_digest.pipeline.common import canonical_url_identity, write_json


REWRITE_OUTPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["rewrites"],
    "properties": {
        "rewrites": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["canonical_url", "draft_line", "needs_manual_review", "review_note"],
                "properties": {
                    "canonical_url": {"type": "string", "minLength": 1},
                    "draft_line": {"type": "string", "minLength": 1},
                    "needs_manual_review": {"type": "boolean"},
                    "review_note": {"type": "string"},
                },
            },
        }
    },
}

PHASE2C_TRUSTED_SOURCE_LABELS = ("BBC Manchester", "GMP", "MEN")
PHASE2C_ACTIVE_FILENAME = "phase2c_active_rewrites.json"


def phase2c_rewrite_contract_payload() -> dict[str, Any]:
    return {
        "input_filter": "publishable=true only",
        "trusted_sources": list(PHASE2C_TRUSTED_SOURCE_LABELS),
        "uses_fields": [
            "fact_type",
            "borough",
            "needs_second_source",
            "primary_entity",
        ],
        "output_schema": REWRITE_OUTPUT_SCHEMA,
        "notes": [
            "Phase 2C rewrites English/source prose into Russian digest bullets.",
            "Phase 2C does not decide publishability; it consumes Phase 2A/2B decisions.",
            "Items with needs_second_source=true should be phrased more cautiously.",
            "Current approved rewrite baseline is BBC Manchester, GMP, and MEN hard-news/public-service items.",
            "For The Mill long-form items with thin or teaser-like summaries, set needs_manual_review=true instead of forcing a confident rewrite.",
        ],
    }


def phase2c_active_rewrites_path(project_root: Path) -> Path:
    return project_root / "data" / "experiments" / PHASE2C_ACTIVE_FILENAME


def _rewrite_input_item(candidate: dict[str, Any]) -> dict[str, Any]:
    normalization = candidate.get("normalization") or {}
    return {
        "canonical_url": candidate.get("canonical_url"),
        "source_label": candidate.get("source_label"),
        "title": candidate.get("title"),
        "summary": candidate.get("summary"),
        "fact_type": candidate.get("fact_type"),
        "borough": normalization.get("normalized_borough"),
        "needs_second_source": candidate.get("needs_second_source"),
        "event_date": candidate.get("event_date"),
        "time_text": candidate.get("time_text"),
        "price_text": candidate.get("price_text"),
        "location_text": candidate.get("location_text"),
        "primary_entity": normalization.get("primary_entity"),
        "normalized_entities": normalization.get("normalized_entities", []),
        "reader_relevance": candidate.get("reader_relevance"),
        "verified_details": candidate.get("verified_details"),
        "drop_reason": candidate.get("drop_reason"),
    }


def build_phase2c_rewrite_prompt(items: list[dict[str, Any]]) -> str:
    schema_json = json.dumps(REWRITE_OUTPUT_SCHEMA, ensure_ascii=False, indent=2)
    items_json = json.dumps(items, ensure_ascii=False, indent=2)
    return (
        "You are the Phase 2C rewrite/editor layer for the Greater Manchester digest.\n"
        "Task: rewrite each input item into one publishable Russian digest bullet.\n\n"
        "Rules:\n"
        "- Output valid JSON only. No markdown, no commentary.\n"
        "- Return an object with one key: `rewrites`, an array with the same length as the input array.\n"
        "- Use Russian for all surrounding prose.\n"
        "- Keep club, venue, company, and official organisation names in English when they are proper names.\n"
        "- Do not invent facts, dates, prices, times, addresses, or causal claims.\n"
        "- Each `draft_line` must be one finished digest bullet starting with `• `.\n"
        "- Each bullet must be self-contained: what happened, where, why it matters, and what to do if relevant.\n"
        "- If `needs_second_source=true`, keep wording cautious and avoid overclaiming missing operational detail.\n"
        "- End each bullet with the source label as plain text, not as a link.\n"
        "- `needs_manual_review=true` only if the item remains too ambiguous or under-specified even after careful rewrite.\n"
        "- `review_note` should be short and empty when no manual review is needed.\n"
        "- Treat BBC Manchester, GMP, and MEN as the current style baseline: concise, self-contained, practical, and reader-facing.\n"
        "- For The Mill items, if the summary is teaser-like or too thin to support a confident self-contained Russian bullet, keep the rewrite cautious and set `needs_manual_review=true`.\n\n"
        "Output schema:\n"
        f"{schema_json}\n\n"
        "Input items:\n"
        f"{items_json}\n"
    )


def build_phase2c_rewrite_pack(input_path: Path, output_prefix: str) -> dict[str, str]:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    candidates = payload.get("fact_candidates", [])
    if not isinstance(candidates, list):
        raise RuntimeError(f"Invalid Phase 2B payload in {input_path}.")

    selected = [candidate for candidate in candidates if isinstance(candidate, dict) and candidate.get("publishable") is True]
    inputs = [_rewrite_input_item(candidate) for candidate in selected]
    prompt = build_phase2c_rewrite_prompt(inputs)

    state_dir = input_path.parent
    inputs_path = state_dir / f"{output_prefix}_inputs.json"
    prompt_path = state_dir / f"{output_prefix}_prompt.txt"
    write_json(inputs_path, {"items": inputs})
    prompt_path.write_text(prompt, encoding="utf-8")

    return {
        "input_path": str(input_path),
        "inputs_path": str(inputs_path),
        "prompt_path": str(prompt_path),
        "items_count": str(len(inputs)),
    }


def _looks_like_russian_rewrite(value: str) -> bool:
    text = str(value or "").strip()
    if not text.startswith("• "):
        return False
    if "<a " in text.lower():
        return False
    if len(text) < 40:
        return False
    return bool(re.search(r"[А-Яа-яЁё]", text))


def activate_phase2c_rewrites(project_root: Path, rewrite_path: Path) -> dict[str, Any]:
    payload = json.loads(rewrite_path.read_text(encoding="utf-8"))
    rewrites = payload.get("rewrites", [])
    if not isinstance(rewrites, list):
        raise RuntimeError(f"Invalid Phase 2C rewrite payload in {rewrite_path}.")

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    rewrite_map: dict[str, dict[str, Any]] = {}

    for index, item in enumerate(rewrites, start=1):
        if not isinstance(item, dict):
            rejected.append({"index": index, "reason": "not-an-object"})
            continue
        canonical_url = canonical_url_identity(str(item.get("canonical_url") or ""))
        draft_line = str(item.get("draft_line") or "").strip()
        needs_manual_review = bool(item.get("needs_manual_review"))
        review_note = str(item.get("review_note") or "").strip()

        if not canonical_url:
            rejected.append({"index": index, "reason": "missing-canonical-url"})
            continue
        if not _looks_like_russian_rewrite(draft_line):
            rejected.append({"index": index, "canonical_url": canonical_url, "reason": "draft-line-not-russian-or-invalid"})
            continue
        if needs_manual_review:
            rejected.append({"index": index, "canonical_url": canonical_url, "reason": "manual-review"})
            continue

        accepted_item = {
            "canonical_url": canonical_url,
            "draft_line": draft_line,
            "needs_manual_review": False,
            "review_note": review_note,
        }
        accepted.append(accepted_item)
        rewrite_map[canonical_url] = accepted_item

    output_path = phase2c_active_rewrites_path(project_root)
    write_json(
        output_path,
        {
            "source_path": str(rewrite_path),
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "trusted_sources": list(PHASE2C_TRUSTED_SOURCE_LABELS),
            "rewrites": accepted,
            "rewrite_map": rewrite_map,
            "rejected": rejected,
        },
    )
    return {
        "output_path": str(output_path),
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
    }
