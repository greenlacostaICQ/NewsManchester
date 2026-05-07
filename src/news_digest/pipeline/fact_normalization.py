from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from news_digest.pipeline.fact_extraction import BOROUGHS
from news_digest.pipeline.common import normalize_title, read_json, write_json


TRUSTED_EXTRACTION_FIELDS = {
    "fact_type": {
        "trusted": True,
        "downstream_uses": [
            "section routing",
            "editorial grouping",
            "Phase 2C rewrite tone selection",
        ],
    },
    "borough": {
        "trusted": True,
        "downstream_uses": [
            "district relevance",
            "borough-specific sections",
            "locality-aware filtering",
        ],
    },
    "publishable": {
        "trusted": True,
        "downstream_uses": [
            "pre-writer keep/drop decision",
            "Phase 2C rewrite candidate selection",
        ],
    },
    "needs_second_source": {
        "trusted": True,
        "downstream_uses": [
            "editorial caution flag",
            "verification queue",
            "pre-publication review priority",
        ],
    },
}


BOROUGH_ALIAS_MAP = {
    "bolton": "Bolton",
    "farnworth": "Bolton",
    "horwich": "Bolton",
    "bury": "Bury",
    "middleton": "Rochdale",
    "rochdale": "Rochdale",
    "oldham": "Oldham",
    "derker": "Oldham",
    "manchester": "Manchester",
    "city centre": "Manchester",
    "manchester city centre": "Manchester",
    "didsbury": "Manchester",
    "burton road": "Manchester",
    "salford": "Salford",
    "forest bank": "Salford",
    "forest bank prison": "Salford",
    "trafford": "Trafford",
    "stockport": "Stockport",
    "tameside": "Tameside",
    "wigan": "Wigan",
}


ENTITY_CANONICAL_MAP = {
    "manchester city": {"canonical_name": "Manchester City FC", "type": "club"},
    "man city": {"canonical_name": "Manchester City FC", "type": "club"},
    "man utd": {"canonical_name": "Manchester United FC", "type": "club"},
    "manchester united": {"canonical_name": "Manchester United FC", "type": "club"},
    "salford city": {"canonical_name": "Salford City FC", "type": "club"},
    "rochdale afc": {"canonical_name": "Rochdale AFC", "type": "club"},
    "york city": {"canonical_name": "York City FC", "type": "club"},
    "gmp": {"canonical_name": "Greater Manchester Police", "type": "police"},
    "greater manchester police": {"canonical_name": "Greater Manchester Police", "type": "police"},
    "nhs": {"canonical_name": "National Health Service", "type": "nhs"},
    "national health service": {"canonical_name": "National Health Service", "type": "nhs"},
    "manchester council": {"canonical_name": "Manchester City Council", "type": "council"},
    "manchester city council": {"canonical_name": "Manchester City Council", "type": "council"},
    "salford council": {"canonical_name": "Salford City Council", "type": "council"},
    "salford city council": {"canonical_name": "Salford City Council", "type": "council"},
    "ao arena": {"canonical_name": "AO Arena", "type": "venue"},
    "co-op live": {"canonical_name": "Co-op Live", "type": "venue"},
    "factory international": {"canonical_name": "Factory International", "type": "venue"},
    "home": {"canonical_name": "HOME Manchester", "type": "venue"},
    "hmp forest bank": {"canonical_name": "HMP Forest Bank", "type": "organisation"},
    "forest bank prison": {"canonical_name": "HMP Forest Bank", "type": "organisation"},
    "greater manchester": {"canonical_name": "Greater Manchester", "type": "place"},
    "manchester flower festival": {"canonical_name": "Manchester Flower Festival", "type": "organisation"},
    "operation vulcan": {"canonical_name": "Operation Vulcan", "type": "police"},
}


def phase2b_contract_payload() -> dict[str, Any]:
    return {
        "trusted_extraction_fields": TRUSTED_EXTRACTION_FIELDS,
        "boroughs": list(BOROUGHS),
        "notes": [
            "Phase 2B consumes fact_candidate output from Phase 2A.",
            "Phase 2B does not rewrite prose.",
            "Normalization adds canonical borough/entity hints and trusted-field metadata.",
        ],
    }


def _normalize_borough_value(value: Any) -> tuple[str | None, str]:
    raw = str(value or "").strip()
    if not raw:
        return None, "missing"
    if raw in BOROUGHS:
        return raw, "explicit"
    lowered = raw.lower()
    mapped = BOROUGH_ALIAS_MAP.get(lowered)
    if mapped:
        return mapped, "alias"
    return None, "unresolved"


def _candidate_text_blobs(candidate: dict[str, Any]) -> list[str]:
    blobs = [
        str(candidate.get("title") or ""),
        str(candidate.get("summary") or ""),
        str(candidate.get("location_text") or ""),
    ]
    for entity in candidate.get("entities", []):
        if isinstance(entity, dict):
            blobs.append(str(entity.get("name") or ""))
            blobs.append(str(entity.get("canonical_name") or ""))
    return [blob for blob in blobs if blob.strip()]


def _contains_alias(text: str, alias: str) -> bool:
    normalized_text = normalize_title(text)
    normalized_alias = normalize_title(alias)
    if not normalized_text or not normalized_alias:
        return False
    if normalized_alias == "manchester" and "greater manchester" in normalized_text:
        return False
    return re.search(rf"(^| )({re.escape(normalized_alias)})( |$)", normalized_text) is not None


def _derive_borough_from_content(candidate: dict[str, Any]) -> tuple[str | None, str]:
    aliases = sorted(BOROUGH_ALIAS_MAP.items(), key=lambda item: len(item[0]), reverse=True)
    for blob in _candidate_text_blobs(candidate):
        for alias, borough in aliases:
            if _contains_alias(blob, alias):
                return borough, f"derived:{alias}"
    return None, "unresolved"


def _normalize_entity(entity: dict[str, Any]) -> dict[str, Any]:
    name = str(entity.get("name") or "").strip()
    canonical_name = str(entity.get("canonical_name") or name).strip()
    entity_type = str(entity.get("type") or "").strip()

    candidates = [name, canonical_name]
    for candidate in candidates:
        key = normalize_title(candidate)
        mapped = ENTITY_CANONICAL_MAP.get(key)
        if mapped:
            return {
                "name": name or canonical_name,
                "canonical_name": mapped["canonical_name"],
                "type": mapped["type"],
                "normalization_source": f"dictionary:{key}",
            }

    return {
        "name": name or canonical_name,
        "canonical_name": canonical_name or name,
        "type": entity_type,
        "normalization_source": "passthrough",
    }


def _normalized_entities(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    entities = candidate.get("entities")
    if not isinstance(entities, list):
        return []
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        normalized = _normalize_entity(entity)
        signature = (
            normalize_title(normalized.get("name", "")),
            normalize_title(normalized.get("canonical_name", "")),
            str(normalized.get("type") or "").strip().lower(),
        )
        if signature in seen:
            continue
        seen.add(signature)
        output.append(normalized)
    return output


def _primary_entity(entities: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not entities:
        return None
    priority = {"club": 1, "venue": 2, "council": 3, "nhs": 4, "police": 5, "organisation": 6, "place": 7}
    return sorted(entities, key=lambda item: priority.get(str(item.get("type") or ""), 99))[0]


def normalize_fact_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    output = dict(candidate)
    explicit_borough, explicit_source = _normalize_borough_value(candidate.get("borough"))
    derived_borough, derived_source = _derive_borough_from_content(candidate)
    normalized_borough = explicit_borough or derived_borough
    borough_source = explicit_source if explicit_borough else derived_source

    normalized_entities = _normalized_entities(candidate)
    primary_entity = _primary_entity(normalized_entities)

    output["normalization"] = {
        "normalized_borough": normalized_borough,
        "borough_source": borough_source,
        "normalized_entities": normalized_entities,
        "primary_entity": primary_entity,
        "trusted_extraction_fields": {key: value["trusted"] for key, value in TRUSTED_EXTRACTION_FIELDS.items()},
    }
    return output


def normalize_fact_candidates_file(input_path: Path, output_path: Path) -> dict[str, Any]:
    payload = read_json(input_path, {"fact_candidates": []})
    candidates = payload.get("fact_candidates", [])
    if not isinstance(candidates, list):
        raise RuntimeError(f"Invalid fact_candidates payload in {input_path}.")

    normalized = [normalize_fact_candidate(candidate) for candidate in candidates if isinstance(candidate, dict)]
    write_json(output_path, {"fact_candidates": normalized, "contract": phase2b_contract_payload()})
    return {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "count": len(normalized),
    }
