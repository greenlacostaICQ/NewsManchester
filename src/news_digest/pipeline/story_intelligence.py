from __future__ import annotations

from collections import Counter
from datetime import datetime
import re

from news_digest.pipeline.common import (
    canonical_url_identity,
    fingerprint_for_candidate,
    normalize_title,
)
from news_digest.pipeline.editorial_contracts import attach_editorial_contract, is_specific_topic_key
from news_digest.pipeline.source_selection import pick_winner


EVIDENCE_PACKET_VERSION = 1
STORY_CLUSTER_VERSION = 1


def _blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )


def _unique(values: list[object], *, limit: int = 12) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _compact_text(value: object, *, limit: int = 1200) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def build_evidence_packet(
    candidate: dict,
    *,
    history_matches: list[dict] | None = None,
    story_cluster: dict | None = None,
) -> dict[str, object]:
    """Build the English-first evidence object used by judge/shortlist layers.

    It intentionally stores factual inputs, not Russian prose. Downstream
    rewrite may read this packet, but should not add facts that are absent here.
    """
    if not isinstance(candidate, dict):
        return {}
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    contract = attach_editorial_contract(candidate).get("editorial_contract") or {}
    fp = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
    packet: dict[str, object] = {
        "schema_version": EVIDENCE_PACKET_VERSION,
        "fingerprint": fp,
        "title": str(candidate.get("title") or ""),
        "source_label": str(candidate.get("source_label") or ""),
        "source_url": str(candidate.get("source_url") or ""),
        "published_at": str(candidate.get("published_at") or ""),
        "category": str(candidate.get("category") or ""),
        "primary_block": str(candidate.get("primary_block") or ""),
        "lead": _compact_text(candidate.get("lead"), limit=600),
        "summary": _compact_text(candidate.get("summary"), limit=900),
        "evidence_text": _compact_text(candidate.get("evidence_text"), limit=1800),
        "entities": entities,
        "event": event,
        "editorial_contract": {
            "story_type": contract.get("story_type") or "",
            "event_shape": contract.get("event_shape") or "",
            "anchor_type": contract.get("anchor_type") or "",
            "topic_key": contract.get("topic_key") or "",
            "publish_tier": contract.get("publish_tier") or "",
            "section_policy": contract.get("section_policy") or {},
        },
        "history_matches": history_matches or candidate.get("history_matches") or [],
    }
    cluster = story_cluster if isinstance(story_cluster, dict) else candidate.get("story_cluster")
    if isinstance(cluster, dict) and cluster:
        packet["story_cluster"] = {
            "cluster_key": cluster.get("cluster_key") or "",
            "canonical_fingerprint": cluster.get("canonical_fingerprint") or "",
            "canonical_source_label": cluster.get("canonical_source_label") or "",
            "source_count": cluster.get("source_count") or 0,
            "sources": cluster.get("sources") or [],
            "union_facts": cluster.get("union_facts") or {},
        }
    return packet


def attach_evidence_packet(
    candidate: dict,
    *,
    history_matches: list[dict] | None = None,
    story_cluster: dict | None = None,
) -> dict:
    if not isinstance(candidate, dict):
        return candidate
    candidate["fingerprint"] = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
    if history_matches is not None:
        candidate["history_matches"] = history_matches
    if story_cluster is not None:
        candidate["story_cluster"] = story_cluster
    candidate["evidence_packet"] = build_evidence_packet(
        candidate,
        history_matches=history_matches,
        story_cluster=story_cluster,
    )
    return candidate


def attach_evidence_packets(candidates: list[dict]) -> None:
    for candidate in candidates:
        if isinstance(candidate, dict):
            attach_evidence_packet(candidate)


def _cheap_identity_key(candidate: dict) -> tuple[str, str] | None:
    block = str(candidate.get("primary_block") or "")
    if block in {"weather", "transport"}:
        return None
    url_key = canonical_url_identity(str(candidate.get("source_url") or ""))
    if url_key:
        return ("url", url_key)
    title = normalize_title(str(candidate.get("title") or ""))
    source = normalize_title(str(candidate.get("source_label") or ""))
    category = normalize_title(str(candidate.get("category") or ""))
    if len(title) >= 28 and source:
        return ("source_title", f"{category}|{source}|{title}")
    return None


def apply_cheap_dedup_before_enrich(candidates: list[dict]) -> dict[str, object]:
    """Cheap exact dedup before entity/event enrichment.

    This only catches deterministic duplicates (same canonical URL, or same
    source+title). Cross-source same-story logic stays in story clustering so
    we don't lose new facts from a different outlet.
    """
    groups: dict[tuple[str, str], list[dict]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        key = _cheap_identity_key(candidate)
        if key:
            groups.setdefault(key, []).append(candidate)

    drops: list[dict[str, object]] = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        winner = pick_winner(group) or group[0]
        winner_fp = str(winner.get("fingerprint") or "").strip() or fingerprint_for_candidate(winner)
        winner["fingerprint"] = winner_fp
        for candidate in group:
            if candidate is winner:
                continue
            candidate["fingerprint"] = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
            candidate["include"] = False
            candidate["dedupe_decision"] = "drop"
            candidate["change_type"] = "same_story_rehash"
            candidate["cheap_dedup_drop"] = True
            candidate["reason"] = (
                "Cheap pre-enrich duplicate — same URL/title kept from stronger source."
            )
            drops.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": candidate.get("title") or "",
                    "source_label": candidate.get("source_label") or "",
                    "kept_fingerprint": winner_fp,
                    "kept_title": winner.get("title") or "",
                    "kept_source_label": winner.get("source_label") or "",
                    "key_type": key[0],
                }
            )

    return {
        "version": 1,
        "groups_seen": sum(1 for group in groups.values() if len(group) > 1),
        "drops": drops,
        "dropped_count": len(drops),
    }


def story_cluster_key(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    topic_key = str(contract.get("topic_key") or "")
    if topic_key and is_specific_topic_key(topic_key):
        return topic_key

    category = str(candidate.get("category") or "")
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if category in {"culture_weekly", "venues_tickets", "russian_speaking_events", "diaspora_events"} and event.get("is_event"):
        name = normalize_title(str(event.get("event_name") or candidate.get("title") or ""))
        venue = normalize_title(str(event.get("venue") or ""))
        date = str(event.get("date_start") or event.get("date") or "")
        if name and (venue or date):
            return f"event:{name[:80]}|{venue[:60]}|{date}"

    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    entity_bits: list[str] = []
    for key in ("people", "venues", "councils", "companies", "boroughs", "districts", "stations"):
        values = entities.get(key)
        if isinstance(values, list):
            entity_bits.extend(str(value) for value in values[:2] if str(value).strip())
    title = normalize_title(str(candidate.get("title") or ""))
    if entity_bits and title:
        return "story:" + normalize_title(" ".join(entity_bits) + " " + title)[:160]
    return ""


def _merge_entities(cluster: list[dict]) -> dict[str, list[str]]:
    keys = ("boroughs", "districts", "stations", "councils", "venues", "clubs", "companies", "people")
    merged: dict[str, list[str]] = {}
    for key in keys:
        values: list[object] = []
        for candidate in cluster:
            entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
            raw = entities.get(key)
            if isinstance(raw, list):
                values.extend(raw)
        merged[key] = _unique(values, limit=10)
    return merged


def _best_event(cluster: list[dict]) -> dict:
    events = [
        candidate.get("event") for candidate in cluster
        if isinstance(candidate.get("event"), dict) and candidate.get("event", {}).get("is_event")
    ]
    if not events:
        return {}

    def score(event: dict) -> int:
        return sum(1 for key in ("event_name", "venue", "date_start", "date", "date_text", "borough", "price", "booking_url") if str(event.get(key) or "").strip())

    best = dict(sorted(events, key=score, reverse=True)[0])
    for event in events:
        for key, value in event.items():
            if not str(best.get(key) or "").strip() and str(value or "").strip():
                best[key] = value
    return best


def _cluster_union_facts(cluster: list[dict]) -> dict[str, object]:
    return {
        "titles": _unique([c.get("title") for c in cluster], limit=8),
        "leads": _unique([c.get("lead") for c in cluster], limit=5),
        "summaries": _unique([c.get("summary") for c in cluster], limit=5),
        "evidence_texts": _unique([_compact_text(c.get("evidence_text"), limit=900) for c in cluster], limit=4),
        "entities": _merge_entities(cluster),
        "event": _best_event(cluster),
    }


def attach_story_clusters(candidates: list[dict]) -> dict[str, object]:
    groups: dict[str, list[dict]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate["fingerprint"] = str(candidate.get("fingerprint") or "").strip() or fingerprint_for_candidate(candidate)
        key = story_cluster_key(candidate)
        if key:
            groups.setdefault(key, []).append(candidate)

    clusters: list[dict[str, object]] = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        winner = pick_winner(group) or group[0]
        canonical_fp = str(winner.get("fingerprint") or "").strip() or fingerprint_for_candidate(winner)
        sources = []
        for candidate in group:
            sources.append(
                {
                    "fingerprint": candidate.get("fingerprint") or "",
                    "source_label": candidate.get("source_label") or "",
                    "source_url": candidate.get("source_url") or "",
                    "title": candidate.get("title") or "",
                }
            )
        cluster_payload = {
            "schema_version": STORY_CLUSTER_VERSION,
            "cluster_key": key,
            "canonical_fingerprint": canonical_fp,
            "canonical_source_label": winner.get("source_label") or "",
            "canonical_source_url": winner.get("source_url") or "",
            "source_count": len(_unique([c.get("source_label") for c in group], limit=50)),
            "sources": sources,
            "union_facts": _cluster_union_facts(group),
        }
        for candidate in group:
            candidate["story_cluster_key"] = key
            candidate["story_cluster"] = cluster_payload
            attach_evidence_packet(candidate, story_cluster=cluster_payload)
        clusters.append(
            {
                "cluster_key": key,
                "canonical_fingerprint": canonical_fp,
                "canonical_source_label": winner.get("source_label") or "",
                "member_count": len(group),
                "source_count": cluster_payload["source_count"],
            }
        )

    for candidate in candidates:
        if isinstance(candidate, dict) and not candidate.get("evidence_packet"):
            attach_evidence_packet(candidate)

    counts = Counter(int(item.get("member_count") or 0) for item in clusters)
    return {
        "version": STORY_CLUSTER_VERSION,
        "cluster_count": len(clusters),
        "cluster_size_counts": dict(counts),
        "clusters": clusters[:100],
    }


def history_match_records(matches: list[dict]) -> list[dict]:
    out: list[dict] = []
    for match in matches[:5]:
        if not isinstance(match, dict):
            continue
        out.append(
            {
                "fingerprint": match.get("fingerprint") or "",
                "title": match.get("title") or "",
                "match_type": match.get("match_type") or "",
                "overlap": match.get("overlap"),
                "published_day": (
                    match.get("last_published_day_london")
                    or match.get("first_published_day_london")
                    or match.get("published_day_london")
                    or ""
                ),
            }
        )
    return out
