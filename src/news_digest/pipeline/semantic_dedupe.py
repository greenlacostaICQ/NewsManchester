"""I1 — Semantic deduplication via embeddings.

Wraps OpenAI's text-embedding-3-small (cheap, 1536-dim) behind a tiny
client + on-disk cache and exposes one entry point, ``run_semantic_pass``,
which the deterministic dedupe stage calls AFTER its Jaccard +
shared-entity passes have run.

Why an extra pass:
    Jaccard misses paraphrased same-story coverage when token overlap
    is low ("Police charge Manchester man with murder" vs "Murder
    arrest at Piccadilly: 28-year-old in court"). The LLM curator
    catches some of these but burns tokens on every batch. Embeddings
    are deterministic, cached, and ~10× cheaper than a curator pass.

Failure policy:
    Pure no-op when ``OPENAI_API_KEY`` is missing or the embedding
    call raises. The deterministic heuristic chain still runs and the
    release is never blocked on a network outage — same rule as the
    rest of the pipeline.

Thresholds:
    * ``_HIGH_SIM_THRESHOLD`` (0.86) — drop / classify as rehash.
    * ``_BORDERLINE_SIM_THRESHOLD`` (0.78) — push the pair to the
      existing LLM borderline review pool so the curator can make the
      "£230m requested vs granted" call.

Cache:
    ``data/state/embedding_cache.json`` keyed by fingerprint.
    Vectors are invalidated when the content hash (title + lead +
    evidence_text excerpt) changes. Stale entries older than 14 days
    are pruned to keep the file from growing forever (cross-day dedup
    window is 7 days, so 14d gives a 2× safety margin without bloat).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import logging
import math
import os
import re

from news_digest.pipeline.common import now_london, read_json


logger = logging.getLogger(__name__)


_EMBED_MODEL = "text-embedding-3-small"
_EMBED_DIM = 1536
_EMBED_BATCH_SIZE = 64
_CACHE_MAX_AGE_DAYS = 14
_CACHE_FILENAME = "embedding_cache.json"

# Drop the weaker candidate / treat as cross-day rehash above this cosine.
_HIGH_SIM_THRESHOLD = 0.86
# Hand off to the LLM borderline reviewer between these bounds.
_BORDERLINE_SIM_THRESHOLD = 0.78

# Cap how many characters of evidence_text we include in the embedded
# blob — the title + lead carry the topical signal, evidence_text adds
# proper nouns / amounts. Beyond ~600c we mostly embed shared boilerplate.
_EVIDENCE_EMBED_CAP = 600


# ── Embedding text & cache helpers ────────────────────────────────────────


def _embed_text(candidate: dict) -> str:
    """Build the compact text we actually feed to the embedding model.

    Two design choices:
      1. Use title + lead + evidence excerpt. Skip source_label/URL —
         we want topical similarity, not source overlap.
      2. Cap evidence to avoid embedding paginated boilerplate that
         differs across CMSes but says the same thing.
    """
    title = str(candidate.get("title") or "").strip()
    lead = str(candidate.get("lead") or "").strip()
    summary = str(candidate.get("summary") or "").strip()
    evidence = str(candidate.get("evidence_text") or "").strip()
    parts = [title]
    if lead and lead.lower() != title.lower():
        parts.append(lead)
    elif summary and summary.lower() != title.lower():
        parts.append(summary)
    if evidence:
        parts.append(evidence[:_EVIDENCE_EMBED_CAP])
    return " — ".join(p for p in parts if p)


def _content_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _load_cache(state_dir: Path) -> dict:
    cache = read_json(state_dir / _CACHE_FILENAME, {"model": _EMBED_MODEL, "entries": {}})
    if cache.get("model") != _EMBED_MODEL:
        # Model bumped — invalidate everything. Future-proof against
        # silently mixing 1536-dim vectors with whatever ships next.
        logger.info(
            "embedding cache: model changed (%s → %s); resetting.",
            cache.get("model"), _EMBED_MODEL,
        )
        return {"model": _EMBED_MODEL, "entries": {}}
    return cache


def _save_cache(state_dir: Path, cache: dict) -> None:
    # Embedding vectors dominate this file. Compact JSON keeps the
    # persisted cross-day cache useful without bloating the state commit.
    path = state_dir / _CACHE_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def _prune_cache(cache: dict, *, now_iso: str | None = None) -> int:
    """Drop entries older than _CACHE_MAX_AGE_DAYS. Returns pruned count."""
    entries = cache.get("entries") or {}
    now_dt = datetime.fromisoformat(now_iso) if now_iso else now_london()
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    cutoff = now_dt - timedelta(days=_CACHE_MAX_AGE_DAYS)
    pruned = 0
    for fp in list(entries.keys()):
        saved_at = entries[fp].get("saved_at") or ""
        try:
            ts = datetime.fromisoformat(saved_at)
        except (TypeError, ValueError):
            del entries[fp]
            pruned += 1
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < cutoff:
            del entries[fp]
            pruned += 1
    return pruned


# ── Embedding client ──────────────────────────────────────────────────────


@dataclass
class EmbeddingClient:
    """Thin wrapper around the OpenAI embeddings endpoint.

    ``embed(texts)`` returns ``list[list[float] | None]`` aligned to the
    input order. None entries mean the API call failed for that batch —
    callers must treat None as "no semantic signal available" and fall
    back to deterministic heuristics.
    """

    api_key: str
    model: str = _EMBED_MODEL

    def embed(self, texts: list[str]) -> list[list[float] | None]:
        if not self.api_key or not texts:
            return [None] * len(texts)
        try:
            from openai import OpenAI  # noqa: PLC0415
        except ImportError:  # pragma: no cover
            return [None] * len(texts)
        try:
            client = OpenAI(api_key=self.api_key, timeout=30, max_retries=1)
        except Exception:  # noqa: BLE001
            return [None] * len(texts)

        out: list[list[float] | None] = [None] * len(texts)
        for start in range(0, len(texts), _EMBED_BATCH_SIZE):
            batch = texts[start: start + _EMBED_BATCH_SIZE]
            try:
                resp = client.embeddings.create(model=self.model, input=batch)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "embedding batch %d failed (%d texts): %s",
                    start // _EMBED_BATCH_SIZE, len(batch), exc,
                )
                continue
            # Cost tracking — embeddings carry prompt_tokens only.
            try:
                from news_digest.pipeline.cost_tracker import record_call  # noqa: PLC0415

                usage = getattr(resp, "usage", None)
                pt = int(getattr(usage, "prompt_tokens", 0) or 0) if usage else 0
                record_call(
                    stage="dedupe_embeddings",
                    provider="openai",
                    model=self.model,
                    prompt_name="semantic_dedupe",
                    prompt_tokens=pt,
                    completion_tokens=0,
                )
            except Exception:  # noqa: BLE001 — cost tracking never blocks
                pass
            for i, item in enumerate(resp.data):
                vec = list(item.embedding)
                if len(vec) != _EMBED_DIM:
                    # Defensive: model returned unexpected dim.
                    out[start + i] = None
                else:
                    out[start + i] = vec
        return out


def _l2_norm(v: list[float]) -> float:
    return math.sqrt(sum(x * x for x in v))


def _normalise(v: list[float]) -> list[float] | None:
    n = _l2_norm(v)
    if n <= 0:
        return None
    return [x / n for x in v]


def _cosine(a: list[float] | None, b: list[float] | None) -> float | None:
    """Cosine similarity for two embedding vectors. Returns None if
    either vector is missing or zero-length (so callers can distinguish
    "no signal" from "no similarity")."""
    if not a or not b:
        return None
    na = _l2_norm(a)
    nb = _l2_norm(b)
    if na <= 0 or nb <= 0:
        return None
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (na * nb)


# ── Cache-aware batch embedding ───────────────────────────────────────────


def embed_with_cache(
    client: EmbeddingClient,
    candidates: list[dict],
    cache: dict,
) -> dict[str, list[float] | None]:
    """Return {fingerprint: vector} for every candidate.

    Cache hits are reused; misses go through one batched API call.
    The cache dict is mutated in place with fresh entries — callers
    should ``_save_cache`` afterwards.
    """
    entries = cache.setdefault("entries", {})
    today_iso = now_london().isoformat()
    out: dict[str, list[float] | None] = {}

    to_fetch_fp: list[str] = []
    to_fetch_text: list[str] = []

    for c in candidates:
        if not isinstance(c, dict):
            continue
        fp = str(c.get("fingerprint") or "")
        if not fp:
            continue
        text = _embed_text(c)
        if not text:
            out[fp] = None
            continue
        h = _content_hash(text)
        cached = entries.get(fp)
        if cached and cached.get("hash") == h and cached.get("vector"):
            out[fp] = cached["vector"]
        else:
            to_fetch_fp.append(fp)
            to_fetch_text.append(text)

    if to_fetch_text:
        vectors = client.embed(to_fetch_text)
        for fp, text, vec in zip(to_fetch_fp, to_fetch_text, vectors):
            out[fp] = vec
            if vec is None:
                continue
            entries[fp] = {
                "hash": _content_hash(text),
                "vector": vec,
                "saved_at": today_iso,
            }
    return out


# ── Same-bucket filter (mirrors deterministic dedupe constraints) ─────────


_DEDUP_BLOCK_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"lead_story", "last_24h", "today_focus", "city_watch", "district_radar"}),
    frozenset({"weekend_activities", "next_7_days", "future_announcements",
                "ticket_radar", "outside_gm_tickets", "russian_events"}),
    frozenset({"openings", "tech_business"}),
)
_TRANSPORT_TICKET_BLOCKS = frozenset({"transport", "weather", "ticket_radar", "outside_gm_tickets"})
_MARKET_LISTING_RE = re.compile(r"\b(?:market|car boot|makers market|artisan market|flea market)\b", re.IGNORECASE)


def _block_group(primary_block: str) -> str:
    for i, group in enumerate(_DEDUP_BLOCK_GROUPS):
        if primary_block in group:
            return f"group:{i}"
    return primary_block


def _market_identity_tokens(candidate: dict) -> set[str]:
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("source_label", "title", "summary", "evidence_text")
    ).lower()
    if not _MARKET_LISTING_RE.search(text):
        return set()
    return {
        re.sub(r"\s+", " ", token).strip()
        for token in re.findall(
            r"\b(?:new smithfield|bowlee|barton|burnage|altrincham|northern quarter|"
            r"stockport|urmston|chorlton|levenshulme|wythenshawe|ancoats|cheadle|"
            r"stretford|first street|aerodrome|community park|market house)\b",
            text,
        )
    }


def _distinct_market_pair(a: dict, b: dict) -> bool:
    a_ids = _market_identity_tokens(a)
    b_ids = _market_identity_tokens(b)
    return bool(a_ids and b_ids and a_ids.isdisjoint(b_ids))


def _comparable(a: dict, b: dict) -> bool:
    """Are these two items in the same dedupe bucket?

    Embedding similarity across transport vs city_watch is noisy — a
    weather card and a council story can share enough thematic tokens
    to hit 0.7 cosine. Restrict to the same block group, mirroring the
    existing intra-batch policy.
    """
    block_a = str(a.get("primary_block") or "")
    block_b = str(b.get("primary_block") or "")
    if block_a in _TRANSPORT_TICKET_BLOCKS or block_b in _TRANSPORT_TICKET_BLOCKS:
        return False  # boilerplate-heavy — embeddings give false positives
    if _distinct_market_pair(a, b):
        return False
    return _block_group(block_a) == _block_group(block_b)


_FOLLOW_UP_MARKERS_RU: tuple[str, ...] = (
    "приговор", "осужд", "виновн", "приговорил",
    "следствие продолжа", "расследование продолжа",
    "задержан", "арестован", "обвинен",
    "годовщин", "к годовщине",
    "обновление", "обновлён", "новые подробности", "уточн",
    "вступает в силу", "вступил в силу", "запущен", "открылся",
)
_FOLLOW_UP_MARKERS_EN: tuple[str, ...] = (
    "sentenced", "verdict", "convicted", "guilty",
    "investigation continues", "court update",
    "appeal", "charged", "anniversary",
    "comes into effect", "now in effect", "officially open",
    "follow up", "follow-up",
)


def _has_follow_up_marker(candidate: dict) -> bool:
    blob = " ".join(
        str(candidate.get(f) or "")
        for f in ("title", "lead", "summary", "evidence_text", "practical_angle")
    ).lower()
    if any(m in blob for m in _FOLLOW_UP_MARKERS_RU):
        return True
    return any(m in blob for m in _FOLLOW_UP_MARKERS_EN)


# ── Main entry point ──────────────────────────────────────────────────────


@dataclass
class SemanticPassResult:
    """Per-run summary written into dedupe_memory.json."""

    model: str
    enabled: bool
    candidates_seen: int
    embedded: int  # how many vectors are available (cache + fresh)
    cache_hits: int
    cache_misses: int
    intra_drops: list[dict]  # {fingerprint, kept_fingerprint, sim}
    cross_day_drops: list[dict]  # {fingerprint, prev_fingerprint, sim, change_type}
    borderline_pairs: list[dict]  # {fingerprint, other_fingerprint, sim, kind}

    def to_dict(self) -> dict:
        return {
            "model": self.model,
            "enabled": self.enabled,
            "candidates_seen": self.candidates_seen,
            "embedded": self.embedded,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "intra_drops": self.intra_drops,
            "cross_day_drops": self.cross_day_drops,
            "borderline_pairs": self.borderline_pairs,
            "intra_drop_count": len(self.intra_drops),
            "cross_day_drop_count": len(self.cross_day_drops),
            "borderline_count": len(self.borderline_pairs),
        }


def _source_rank(label: str, category: str = "") -> int:
    """Lower rank = better source. Thin delegate to the single shared
    implementation in ``source_selection`` (imported inline to keep this
    module free of a module-load edge into the dedupe graph)."""
    from news_digest.pipeline.source_selection import source_rank_with_fallback

    return source_rank_with_fallback(label, category)


def _fresh_fact_quality(candidate: dict) -> int:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block not in {"last_24h", "today_focus", "city_watch"} and category not in {"media_layer", "gmp", "public_services", "city_news", "council"}:
        return 0
    frame = candidate.get("story_frame") if isinstance(candidate.get("story_frame"), dict) else {}
    score = 0
    for key, weight in (
        ("what_happened", 4),
        ("where_exact", 2),
        ("when", 2),
        ("who_affected", 2),
        ("why_now", 2),
    ):
        if str(frame.get(key) or "").strip():
            score += weight
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text"))
    if len(blob) >= 250:
        score += 1
    if len(blob) >= 700:
        score += 1
    if re.search(r"\b(?:charged|sentenced|arrested|closed|reopened|approved|rejected|rated|inspected)\b", blob, re.IGNORECASE):
        score += 2
    if re.search(r"\b(?:incident|situation|issue)\b", blob, re.IGNORECASE) and not re.search(
        r"\b(?:stabbing|crash|collision|fire|assault|robbery|cordon|evacuat|charged|sentenced)\b",
        blob,
        re.IGNORECASE,
    ):
        score -= 2
    return score


def _prefer_semantic_candidate(first: dict, second: dict, first_rank: int, second_rank: int) -> tuple[dict, dict, str]:
    if first_rank != second_rank:
        kept, loser = (first, second) if first_rank < second_rank else (second, first)
        return kept, loser, "stronger source"
    first_quality = _fresh_fact_quality(first)
    second_quality = _fresh_fact_quality(second)
    if first_quality or second_quality:
        if first_quality != second_quality:
            kept, loser = (first, second) if first_quality > second_quality else (second, first)
            return kept, loser, "more complete fresh-news facts"
    return first, second, "stronger source"


def run_semantic_pass(
    *,
    candidates: list[dict],
    published_facts: list[dict],
    state_dir: Path,
    client: EmbeddingClient | None = None,
) -> SemanticPassResult:
    """Mutates ``candidates`` in place, adding semantic-dedup decisions.

    Returns a SemanticPassResult summarising what was done. The caller
    (dedupe_candidates) should serialise it into dedupe_memory.json.
    """
    if client is None:
        client = EmbeddingClient(api_key=os.environ.get("OPENAI_API_KEY", ""))
    enabled = bool(client.api_key)

    candidates_seen = sum(
        1 for c in candidates
        if isinstance(c, dict) and c.get("include") and c.get("fingerprint")
    )

    if not enabled or candidates_seen == 0:
        return SemanticPassResult(
            model=_EMBED_MODEL,
            enabled=enabled,
            candidates_seen=candidates_seen,
            embedded=0,
            cache_hits=0,
            cache_misses=0,
            intra_drops=[],
            cross_day_drops=[],
            borderline_pairs=[],
        )

    cache = _load_cache(state_dir)
    pruned = _prune_cache(cache)
    if pruned:
        logger.info("embedding cache: pruned %d stale entr(ies).", pruned)

    # Snapshot cache hit count BEFORE embedding so we can compute misses.
    pre_entries = set((cache.get("entries") or {}).keys())

    included = [
        c for c in candidates
        if isinstance(c, dict) and c.get("include") and c.get("fingerprint")
    ]
    vectors = embed_with_cache(client, included, cache)

    cache_hits = sum(1 for c in included if str(c.get("fingerprint")) in pre_entries)
    cache_misses = len(included) - cache_hits
    embedded = sum(1 for v in vectors.values() if v is not None)

    intra_drops: list[dict] = []
    cross_day_drops: list[dict] = []
    borderline_pairs: list[dict] = []

    # ── Intra-batch: drop weaker source among same-bucket high-sim pairs ──
    dropped_fps: set[str] = set()
    for i, ci in enumerate(included):
        fp_i = str(ci.get("fingerprint") or "")
        if fp_i in dropped_fps:
            continue
        vi = vectors.get(fp_i)
        if vi is None:
            continue
        rank_i = _source_rank(
            str(ci.get("source_label") or ""),
            str(ci.get("category") or ""),
        )
        for cj in included[i + 1:]:
            fp_j = str(cj.get("fingerprint") or "")
            if fp_j in dropped_fps:
                continue
            if not _comparable(ci, cj):
                continue
            vj = vectors.get(fp_j)
            sim = _cosine(vi, vj)
            if sim is None:
                continue
            if sim >= _HIGH_SIM_THRESHOLD:
                rank_j = _source_rank(
                    str(cj.get("source_label") or ""),
                    str(cj.get("category") or ""),
                )
                # Lower rank number = stronger source; if sources tie, keep
                # the version with a fuller fresh-news fact frame.
                kept, loser, kept_reason = _prefer_semantic_candidate(ci, cj, rank_i, rank_j)
                loser_fp = str(loser.get("fingerprint") or "")
                kept_fp = str(kept.get("fingerprint") or "")
                if loser.get("include"):
                    loser["include"] = False
                    loser["dedupe_decision"] = "drop"
                    loser["reason"] = (
                        f"Semantic intra-batch duplicate (cos={sim:.3f}); "
                        f"kept {kept_reason} «{kept.get('source_label', '')}»."
                    )
                    loser["semantic_match_sim"] = round(sim, 4)
                    loser["semantic_match_fingerprint"] = kept_fp
                    loser["semantic_match_kind"] = "intra_batch"
                    dropped_fps.add(loser_fp)
                    intra_drops.append({
                        "fingerprint": loser_fp,
                        "kept_fingerprint": kept_fp,
                        "sim": round(sim, 4),
                        "title": loser.get("title"),
                        "kept_title": kept.get("title"),
                    })
                if loser is ci:
                    break  # ci dropped — stop comparing it further
            elif sim >= _BORDERLINE_SIM_THRESHOLD:
                borderline_pairs.append({
                    "fingerprint": fp_i,
                    "other_fingerprint": fp_j,
                    "sim": round(sim, 4),
                    "kind": "intra_batch",
                })

    # ── Cross-day: compare survivors against published_facts ─────────────
    # Published facts don't ship with vectors today, but the cache key by
    # fingerprint catches re-fetched items. For first contact we embed
    # the published title alone — cheap, ~10 tokens each.
    survivors = [c for c in included if c.get("include")]
    if survivors and published_facts:
        # Build a candidate-shaped projection so embed_with_cache works.
        published_shaped = []
        for fact in published_facts:
            if not isinstance(fact, dict):
                continue
            fp = str(fact.get("fingerprint") or "")
            if not fp or fp in vectors:  # already embedded this run
                continue
            published_shaped.append({
                "fingerprint": fp,
                "title": fact.get("title") or fact.get("normalized_title") or "",
                "lead": "",
                "evidence_text": "",
                "primary_block": fact.get("primary_block") or "",
            })

        # Only embed published facts in the same block groups we actually
        # have survivors for — keeps the API call bounded.
        survivor_groups = {
            _block_group(str(c.get("primary_block") or ""))
            for c in survivors
        }
        published_shaped = [
            p for p in published_shaped
            if _block_group(str(p.get("primary_block") or "")) in survivor_groups
        ]

        published_vectors = embed_with_cache(client, published_shaped, cache)
        fact_by_fp = {str(f.get("fingerprint") or ""): f for f in published_facts if isinstance(f, dict)}

        for c in survivors:
            fp = str(c.get("fingerprint") or "")
            vc = vectors.get(fp)
            if vc is None:
                continue
            best_sim = 0.0
            best_fact_fp = ""
            for pfp, pv in published_vectors.items():
                if pv is None:
                    continue
                pf = fact_by_fp.get(pfp)
                if not pf or not _comparable(c, pf):
                    continue
                sim = _cosine(vc, pv)
                if sim is None:
                    continue
                if sim > best_sim:
                    best_sim = sim
                    best_fact_fp = pfp

            if best_sim >= _HIGH_SIM_THRESHOLD and best_fact_fp:
                prev = fact_by_fp.get(best_fact_fp) or {}
                follow_up = _has_follow_up_marker(c)
                c["semantic_match_sim"] = round(best_sim, 4)
                c["semantic_match_fingerprint"] = best_fact_fp
                c["semantic_match_kind"] = "cross_day"
                c["semantic_dedupe_match"] = "embedding_only"
                if follow_up:
                    c["change_type"] = "follow_up"
                    c["reason"] = (
                        f"{(c.get('reason') or '').strip()} | "
                        f"Semantic match to «{prev.get('title', '')[:80]}» "
                        f"(cos={best_sim:.3f}), kept as follow-up due to marker."
                    ).strip(" |")
                else:
                    c["include"] = False
                    c["dedupe_decision"] = "drop"
                    c["change_type"] = "same_story_rehash"
                    c["reason"] = (
                        f"Semantic cross-day rehash (cos={best_sim:.3f}) of "
                        f"«{prev.get('title', '')[:120]}»."
                    )
                cross_day_drops.append({
                    "fingerprint": fp,
                    "prev_fingerprint": best_fact_fp,
                    "sim": round(best_sim, 4),
                    "title": c.get("title"),
                    "prev_title": prev.get("title"),
                    "change_type": c.get("change_type"),
                    "kept": follow_up,
                })
            elif _BORDERLINE_SIM_THRESHOLD <= best_sim < _HIGH_SIM_THRESHOLD and best_fact_fp:
                borderline_pairs.append({
                    "fingerprint": fp,
                    "other_fingerprint": best_fact_fp,
                    "sim": round(best_sim, 4),
                    "kind": "cross_day",
                })
                # Hint for the existing LLM borderline reviewer.
                c.setdefault("semantic_borderline", []).append({
                    "match_fingerprint": best_fact_fp,
                    "sim": round(best_sim, 4),
                })

    _save_cache(state_dir, cache)

    return SemanticPassResult(
        model=_EMBED_MODEL,
        enabled=True,
        candidates_seen=candidates_seen,
        embedded=embedded,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        intra_drops=intra_drops,
        cross_day_drops=cross_day_drops,
        borderline_pairs=borderline_pairs,
    )


# Compatibility helpers for deterministic cross-day guards and
# published_facts enrichment. They are deliberately local/hash-based:
# run_semantic_pass above may call OpenAI embeddings when available, but
# these helpers must work in tests and in delivery-history writes even
# when the network/API key is absent.
EMBEDDING_VERSION = "semantic-hash-v1"
EMBEDDING_DIMENSIONS = 96

_WORD_RE = re.compile(r"[a-zA-Zа-яёА-ЯЁ0-9£$][a-zA-Zа-яёА-ЯЁ0-9£$'-]*")
_MONEY_RE = re.compile(r"(?:£|\$)\s*\d+(?:[.,]\d+)?\s*(?:m|mn|million|bn|billion)?", re.IGNORECASE)
_DATE_RE = re.compile(
    r"\b(?:20\d{2}|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?|"
    r"\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*|"
    r"\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря))\b",
    re.IGNORECASE,
)
_CAPITALISED_RE = re.compile(r"\b[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?\b")

_LOCAL_STOPWORDS = frozenset({
    "about", "after", "again", "against", "also", "amid", "and", "are", "been",
    "before", "being", "but", "buy", "can", "could", "from", "greater", "have",
    "into", "just", "latest", "local", "man", "manchester", "more", "near",
    "news", "over", "said", "says", "that", "the", "their", "them", "then",
    "there", "this", "today", "update", "updates", "what", "when", "where",
    "which", "while", "woman", "with", "would", "year", "years", "your",
    "боле", "будет", "были", "был", "для", "его", "или", "как", "манчестер",
    "новост", "после", "при", "про", "сегодня", "что", "это",
})
_GENERIC_ANCHORS = frozenset({
    "council", "police", "people", "public", "service", "services", "tickets",
    "event", "events", "family", "first", "great", "live", "major", "music",
    "plans", "road", "school", "story", "street", "ticket", "tickets", "weekend",
})


def semantic_text(item: dict) -> str:
    return " ".join(
        str(item.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text")
    )


def _local_tokens(text: str) -> list[str]:
    out: list[str] = []
    for raw in _WORD_RE.findall(str(text or "").lower()):
        token = raw.strip("'-$£")
        if len(token) < 3 or token in _LOCAL_STOPWORDS:
            continue
        out.append(token)
    return out


def _clean_entity_phrase(value: str) -> str:
    parts = [
        part for part in re.findall(r"[a-zA-Zа-яёА-ЯЁ0-9'-]+", str(value or "").lower())
        if len(part) >= 3 and part not in _LOCAL_STOPWORDS and part not in _GENERIC_ANCHORS
    ]
    return " ".join(parts)


def semantic_embedding(item: dict) -> list[float]:
    vector = [0.0] * EMBEDDING_DIMENSIONS
    toks = _local_tokens(semantic_text(item))
    features = list(toks)
    features.extend(f"{a}_{b}" for a, b in zip(toks, toks[1:]))
    for feature in features:
        digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
        bucket = int.from_bytes(digest[:4], "big") % EMBEDDING_DIMENSIONS
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[bucket] += sign
    norm = math.sqrt(sum(v * v for v in vector))
    if not norm:
        return vector
    return [round(v / norm, 6) for v in vector]


def cosine_similarity(left: list[float] | None, right: list[float] | None) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return sum(a * b for a, b in zip(left, right))


def anchor_tokens(item: dict) -> set[str]:
    text = semantic_text(item)
    anchors: set[str] = set()
    for token in _local_tokens(text):
        if len(token) >= 5 and token not in _GENERIC_ANCHORS:
            anchors.add(token)
    for match in _MONEY_RE.findall(text):
        anchors.add(re.sub(r"\s+", "", match.lower()))
    for match in _DATE_RE.findall(text):
        anchors.add(re.sub(r"\s+", "", match.lower()))
    for match in _CAPITALISED_RE.findall(str(item.get("title") or "")):
        for part in _clean_entity_phrase(match).split():
            if len(part) >= 4 and part not in _LOCAL_STOPWORDS and part not in _GENERIC_ANCHORS:
                anchors.add(part)
    return anchors


def new_fact_tokens(item: dict) -> set[str]:
    text = semantic_text(item)
    tokens: set[str] = set()
    for match in _MONEY_RE.findall(text):
        tokens.add(re.sub(r"\s+", "", match.lower()))
    for match in _DATE_RE.findall(text):
        tokens.add(re.sub(r"\s+", "", match.lower()))
    return tokens


def has_new_fact_signal(candidate: dict, previous: dict | None) -> bool:
    if not previous:
        return False
    previous_facts = new_fact_tokens(previous)
    candidate_facts = new_fact_tokens(candidate)
    if candidate_facts - previous_facts:
        return True
    blob = semantic_text(candidate).lower()
    return any(
        marker in blob
        for marker in (
            "charged", "convicted", "sentenced", "approved", "opened",
            "comes into effect", "now in effect", "new details",
            "обвин", "осужд", "приговор", "одобрен", "открыл", "вступает в силу",
        )
    )
