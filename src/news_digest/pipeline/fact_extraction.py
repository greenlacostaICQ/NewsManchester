from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from news_digest.pipeline.collector.sources import SOURCES
from news_digest.pipeline.common import canonical_url_identity, normalize_title, read_json, write_json


PHASE2A_SOURCE_LABELS = ("BBC Manchester", "GMP", "MEN", "The Mill")

FACT_TYPES = (
    "incident",
    "transport",
    "council",
    "event",
    "ticket",
    "weather",
    "football",
    "food_opening",
    "public_service",
    "other",
)

BOROUGHS = (
    "Manchester",
    "Salford",
    "Trafford",
    "Stockport",
    "Tameside",
    "Oldham",
    "Rochdale",
    "Bury",
    "Bolton",
    "Wigan",
)

ENTITY_TYPES = (
    "club",
    "venue",
    "council",
    "nhs",
    "police",
    "place",
    "organisation",
)

def fact_candidate_schema_payload() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "fact_candidate",
        "type": "object",
        "additionalProperties": False,
        "required": [
            "source_label",
            "source_url",
            "canonical_url",
            "title",
            "summary",
            "fact_type",
            "borough",
            "entities",
            "event_date",
            "time_text",
            "price_text",
            "location_text",
            "verified_details",
            "needs_second_source",
            "reader_relevance",
            "publishable",
            "drop_reason",
        ],
        "properties": {
            "source_label": {"type": "string", "minLength": 1},
            "source_url": {"type": "string", "minLength": 1},
            "canonical_url": {"type": "string", "minLength": 1},
            "title": {"type": "string", "minLength": 1},
            "summary": {"type": "string"},
            "fact_type": {"type": "string", "enum": list(FACT_TYPES)},
            "borough": {"type": ["string", "null"], "enum": [*BOROUGHS, None]},
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["name", "canonical_name", "type"],
                    "properties": {
                        "name": {"type": "string", "minLength": 1},
                        "canonical_name": {"type": "string", "minLength": 1},
                        "type": {"type": "string", "enum": list(ENTITY_TYPES)},
                    },
                },
            },
            "event_date": {"type": ["string", "null"]},
            "time_text": {"type": ["string", "null"]},
            "price_text": {"type": ["string", "null"]},
            "location_text": {"type": ["string", "null"]},
            "verified_details": {
                "type": "object",
                "additionalProperties": False,
                "required": ["date", "time", "price", "address", "official_source"],
                "properties": {
                    "date": {"type": "boolean"},
                    "time": {"type": "boolean"},
                    "price": {"type": "boolean"},
                    "address": {"type": "boolean"},
                    "official_source": {"type": "boolean"},
                },
            },
            "needs_second_source": {"type": "boolean"},
            "reader_relevance": {"type": "string"},
            "publishable": {"type": "boolean"},
            "drop_reason": {"type": "string"},
        },
    }


def fact_candidate_batch_schema_payload() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["fact_candidates"],
        "properties": {
            "fact_candidates": {
                "type": "array",
                "items": fact_candidate_schema_payload(),
            }
        },
    }


def _source_type_lookup() -> dict[str, str]:
    return {source.name: source.source_type for source in SOURCES}


def _fact_type_for_candidate(candidate: dict[str, Any]) -> str:
    category = str(candidate.get("category") or "").strip().lower()
    primary_block = str(candidate.get("primary_block") or "").strip().lower()
    source_label = str(candidate.get("source_label") or "").strip()

    if primary_block == "transport" or category == "transport":
        return "transport"
    if category == "council":
        return "council"
    if category == "gmp":
        return "incident"
    if category == "football" or primary_block == "football":
        return "football"
    if category == "food_openings" or primary_block == "openings":
        return "food_opening"
    if primary_block in {"ticket_radar", "future_announcements"}:
        return "ticket"
    if source_label == "Met Office" or primary_block == "weather":
        return "weather"
    if category == "public_services":
        return "public_service"
    if primary_block in {"next_7_days", "today_focus"}:
        return "event"
    return "other"


def _projection_for_candidate(candidate: dict[str, Any], source_types: dict[str, str]) -> dict[str, Any]:
    source_label = str(candidate.get("source_label") or "").strip()
    source_url = str(candidate.get("source_url") or "").strip()
    validation_errors = candidate.get("validation_errors")
    if not isinstance(validation_errors, list):
        validation_errors = []
    publishable = bool(candidate.get("include")) and not validation_errors

    source_type = source_types.get(source_label, "unknown")
    projection = {
        "source_label": source_label,
        "source_url": source_url,
        "canonical_url": canonical_url_identity(source_url),
        "title": str(candidate.get("title") or "").strip(),
        "summary": str(candidate.get("summary") or "").strip(),
        "fact_type": _fact_type_for_candidate(candidate),
        "borough": None,
        "entities": [],
        "event_date": None,
        "time_text": None,
        "price_text": None,
        "location_text": None,
        "verified_details": {
            "date": bool(candidate.get("published_at")),
            "time": False,
            "price": False,
            "address": False,
            "official_source": True,
        },
        "needs_second_source": False,
        "reader_relevance": str(candidate.get("practical_angle") or "").strip(),
        "publishable": publishable,
        "drop_reason": "; ".join(str(item) for item in validation_errors if str(item).strip()),
        "_deterministic_context": {
            "primary_block": str(candidate.get("primary_block") or "").strip(),
            "category": str(candidate.get("category") or "").strip(),
            "published_at": str(candidate.get("published_at") or "").strip(),
            "lead": str(candidate.get("lead") or "").strip(),
            "source_type": source_type,
            "normalized_title": normalize_title(str(candidate.get("title") or "")),
        },
    }
    return projection


def _select_candidates(
    project_root: Path,
    *,
    state_root: Path | None = None,
    source_labels: tuple[str, ...] | None = None,
    limit_per_source: int = 3,
) -> list[dict[str, Any]]:
    effective_state_root = state_root or (project_root / "data" / "state")
    payload = read_json(effective_state_root / "candidates.json", {"candidates": []})
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list):
        return []

    selected_labels = source_labels or PHASE2A_SOURCE_LABELS
    selected: list[dict[str, Any]] = []
    per_source: dict[str, int] = {label: 0 for label in selected_labels}
    wanted = set(selected_labels)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        label = str(candidate.get("source_label") or "").strip()
        if label not in wanted:
            continue
        if per_source[label] >= limit_per_source:
            continue
        selected.append(candidate)
        per_source[label] += 1
    return selected


def _prompt_input_for_candidate(candidate: dict[str, Any], source_types: dict[str, str]) -> dict[str, Any]:
    source_label = str(candidate.get("source_label") or "").strip()
    source_url = str(candidate.get("source_url") or "").strip()
    return {
        "source_label": source_label,
        "source_type": source_types.get(source_label, "unknown"),
        "source_url": source_url,
        "canonical_url": canonical_url_identity(source_url),
        "published_at": str(candidate.get("published_at") or "").strip() or None,
        "title": str(candidate.get("title") or "").strip(),
        "summary": str(candidate.get("summary") or "").strip(),
        "lead": str(candidate.get("lead") or "").strip(),
        "deterministic_context": {
            "category": str(candidate.get("category") or "").strip(),
            "primary_block": str(candidate.get("primary_block") or "").strip(),
            "practical_angle": str(candidate.get("practical_angle") or "").strip(),
        },
    }


def build_phase2a_rss_pack(
    project_root: Path,
    *,
    state_root: Path | None = None,
    output_state_root: Path | None = None,
    source_labels: tuple[str, ...] | None = None,
    limit_per_source: int = 3,
    output_prefix: str = "phase2a_rss",
) -> dict[str, str]:
    source_types = _source_type_lookup()
    selected = _select_candidates(
        project_root,
        state_root=state_root,
        source_labels=source_labels,
        limit_per_source=limit_per_source,
    )

    inputs = [_prompt_input_for_candidate(candidate, source_types) for candidate in selected]
    baseline = [_projection_for_candidate(candidate, source_types) for candidate in selected]
    prompt_text = build_rss_extraction_prompt(inputs)

    effective_output_root = output_state_root or (project_root / "data" / "state")
    inputs_path = effective_output_root / f"{output_prefix}_inputs.json"
    prompt_path = effective_output_root / f"{output_prefix}_prompt.txt"
    baseline_path = effective_output_root / f"{output_prefix}_deterministic_baseline.json"

    write_json(inputs_path, {"items": inputs})
    prompt_path.write_text(prompt_text, encoding="utf-8")
    write_json(baseline_path, {"fact_candidates": baseline})

    return {
        "inputs_path": str(inputs_path),
        "prompt_path": str(prompt_path),
        "baseline_path": str(baseline_path),
        "items_count": str(len(inputs)),
        "source_labels": ", ".join(source_labels or PHASE2A_SOURCE_LABELS),
    }


def build_rss_extraction_prompt(items: list[dict[str, Any]]) -> str:
    schema_json = json.dumps(fact_candidate_schema_payload(), ensure_ascii=False, indent=2)
    items_json = json.dumps(items, ensure_ascii=False, indent=2)
    return (
        "You are the Phase 2A fact-extraction layer for the Greater Manchester digest.\n"
        "Task: convert each input item into exactly one `fact_candidate` JSON object.\n\n"
        "Rules:\n"
        "- Output valid JSON only. No markdown, no commentary.\n"
        "- Return an object with one key: `fact_candidates`, an array with the same length as the input array.\n"
        "- Use only facts explicitly present in the input item fields.\n"
        "- Do not invent dates, times, prices, addresses, boroughs, or entities.\n"
        "- If the item is not publishable (affiliate, evergreen, not Greater Manchester, too thin), set `publishable=false` and explain why in `drop_reason`.\n"
        "- `borough` must be one of the 10 Greater Manchester boroughs or null.\n"
        "- `entities` should be conservative: only include named organisations/venues/clubs/places that are explicit in the text.\n"
        "- `verified_details` should reflect exactly what is explicit in the text, not what is merely likely.\n"
        "- `reader_relevance` must be one short sentence in English explaining why the item matters today. This is extraction-stage output, not final Russian prose.\n"
        "- `needs_second_source=true` only when the item feels material but the exact operational detail is incomplete or soft.\n\n"
        "Schema:\n"
        f"{schema_json}\n\n"
        "Input items:\n"
        f"{items_json}\n"
    )


def validate_fact_candidate_shape(candidate: dict[str, Any]) -> list[str]:
    schema = fact_candidate_schema_payload()
    required = set(schema["required"])
    errors: list[str] = []
    missing = sorted(required.difference(candidate))
    if missing:
        errors.append(f"Missing required fields: {', '.join(missing)}")
    if candidate.get("fact_type") not in FACT_TYPES:
        errors.append(f"Invalid fact_type: {candidate.get('fact_type')!r}")
    borough = candidate.get("borough")
    if borough is not None and borough not in BOROUGHS:
        errors.append(f"Invalid borough: {borough!r}")
    if not isinstance(candidate.get("entities"), list):
        errors.append("entities must be a list")
    verified_details = candidate.get("verified_details")
    if not isinstance(verified_details, dict):
        errors.append("verified_details must be an object")
    else:
        for key in ("date", "time", "price", "address", "official_source"):
            if not isinstance(verified_details.get(key), bool):
                errors.append(f"verified_details.{key} must be boolean")
    if not isinstance(candidate.get("publishable"), bool):
        errors.append("publishable must be boolean")
    if not isinstance(candidate.get("needs_second_source"), bool):
        errors.append("needs_second_source must be boolean")
    return errors


def _entity_signature(entity: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(entity.get("name") or "").strip().lower(),
        str(entity.get("canonical_name") or "").strip().lower(),
        str(entity.get("type") or "").strip().lower(),
    )


def _entity_signature_set(value: Any) -> set[tuple[str, str, str]]:
    if not isinstance(value, list):
        return set()
    signatures: set[tuple[str, str, str]] = set()
    for item in value:
        if not isinstance(item, dict):
            continue
        signatures.add(_entity_signature(item))
    return signatures


def _bool_dict_match(expected: Any, actual: Any, keys: tuple[str, ...]) -> bool:
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return False
    return all(expected.get(key) is actual.get(key) for key in keys)


def compare_fact_candidates(
    project_root: Path,
    *,
    llm_output_path: Path,
    baseline_path: Path | None = None,
) -> dict[str, Any]:
    effective_baseline_path = baseline_path or (project_root / "data" / "state" / "phase2a_rss_deterministic_baseline.json")
    baseline_payload = read_json(effective_baseline_path, {"fact_candidates": []})
    llm_payload = read_json(llm_output_path, {"fact_candidates": []})
    baseline = baseline_payload.get("fact_candidates", [])
    llm_output = llm_payload.get("fact_candidates", [])

    comparisons: list[dict[str, Any]] = []
    verified_detail_keys = ("date", "time", "price", "address", "official_source")
    for index, (expected, actual) in enumerate(zip(baseline, llm_output), start=1):
        shape_errors = validate_fact_candidate_shape(actual if isinstance(actual, dict) else {})
        expected = expected if isinstance(expected, dict) else {}
        actual = actual if isinstance(actual, dict) else {}
        expected_entities = _entity_signature_set(expected.get("entities"))
        actual_entities = _entity_signature_set(actual.get("entities"))
        comparisons.append(
            {
                "index": index,
                "source_label": expected.get("source_label"),
                "canonical_url_match": expected.get("canonical_url") == actual.get("canonical_url"),
                "title_match": normalize_title(str(expected.get("title") or "")) == normalize_title(str(actual.get("title") or "")),
                "fact_type_expected": expected.get("fact_type"),
                "fact_type_actual": actual.get("fact_type"),
                "fact_type_match": expected.get("fact_type") == actual.get("fact_type"),
                "borough_expected": expected.get("borough"),
                "borough_actual": actual.get("borough"),
                "borough_match": expected.get("borough") == actual.get("borough"),
                "publishable_expected": expected.get("publishable"),
                "publishable_actual": actual.get("publishable"),
                "publishable_match": expected.get("publishable") is actual.get("publishable"),
                "needs_second_source_expected": expected.get("needs_second_source"),
                "needs_second_source_actual": actual.get("needs_second_source"),
                "needs_second_source_match": expected.get("needs_second_source") is actual.get("needs_second_source"),
                "verified_details_expected": expected.get("verified_details"),
                "verified_details_actual": actual.get("verified_details"),
                "verified_details_match": _bool_dict_match(expected.get("verified_details"), actual.get("verified_details"), verified_detail_keys),
                "entity_count_expected": len(expected_entities),
                "entity_count_actual": len(actual_entities),
                "entity_overlap_count": len(expected_entities.intersection(actual_entities)),
                "entity_exact_match": expected_entities == actual_entities,
                "shape_errors": shape_errors,
                "reader_relevance": actual.get("reader_relevance"),
                "drop_reason": actual.get("drop_reason"),
            }
        )

    summary = {
        "baseline_count": len(baseline),
        "llm_count": len(llm_output),
        "matched_pairs": len(comparisons),
        "extra_llm_items": max(0, len(llm_output) - len(baseline)),
        "missing_llm_items": max(0, len(baseline) - len(llm_output)),
        "shape_error_count": sum(1 for row in comparisons if row["shape_errors"]),
        "canonical_url_match_count": sum(1 for row in comparisons if row["canonical_url_match"]),
        "title_match_count": sum(1 for row in comparisons if row["title_match"]),
        "fact_type_match_count": sum(1 for row in comparisons if row["fact_type_match"]),
        "borough_match_count": sum(1 for row in comparisons if row["borough_match"]),
        "publishable_match_count": sum(1 for row in comparisons if row["publishable_match"]),
        "needs_second_source_match_count": sum(1 for row in comparisons if row["needs_second_source_match"]),
        "verified_details_match_count": sum(1 for row in comparisons if row["verified_details_match"]),
        "entity_exact_match_count": sum(1 for row in comparisons if row["entity_exact_match"]),
        "entity_overlap_total": sum(int(row["entity_overlap_count"]) for row in comparisons),
    }

    report = {"summary": summary, "comparisons": comparisons}
    output_path = llm_output_path.with_name(f"{llm_output_path.stem}.comparison.json")
    write_json(output_path, report)
    report["output_path"] = str(output_path)
    return report
