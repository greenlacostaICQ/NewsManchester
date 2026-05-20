"""Per-run LLM cost & call accounting.

Pipeline stages (dedupe_review, curator, llm_rewrite) call
`record_call(...)` after chat.completions.create responses. The
accumulator is a module-level singleton because the pipeline is
single-process.

At the end of each stage we call `dump_stage(state_dir, stage)` to
write a per-stage cost snapshot. `release.py` aggregates these into
a daily total.

Pricing is per 1M tokens, USD. Update PRICING when adding a model.
Groq free tier → zero cost. If a model is unknown we fall back to
"unknown" pricing and emit a warning at release time (so we notice
when a new model slips in without a price tag). Each record stores both
provider usage and a local estimate so per-run cost remains visible even
when a provider omits usage metadata.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any
import json
import threading


# USD per 1M tokens. Verified against vendor pricing pages 2026-05.
# cached_input applies when the provider reports a cache hit on the
# stable system prefix. DeepSeek and OpenAI both bill cache reads at
# a deep discount (DeepSeek ~26% of input; OpenAI ~50%). When the
# field is missing for a model we fall back to the regular input price.
PRICING_PER_MTOKEN: dict[str, dict[str, float]] = {
    "deepseek-chat":               {"input": 0.27, "cached_input": 0.07,  "output": 1.10},
    "deepseek-reasoner":           {"input": 0.55, "cached_input": 0.14,  "output": 2.19},
    "gpt-4o-mini":                 {"input": 0.15, "cached_input": 0.075, "output": 0.60},
    "gpt-4o":                      {"input": 2.50, "cached_input": 1.25,  "output": 10.00},
    "llama-3.3-70b-versatile":     {"input": 0.0,  "cached_input": 0.0,   "output": 0.0},
    "llama-3.1-70b-versatile":     {"input": 0.0,  "cached_input": 0.0,   "output": 0.0},
    # Embeddings: priced per input token only; completion is always 0
    # for these endpoints so output multiplier is irrelevant.
    "text-embedding-3-small":      {"input": 0.02, "cached_input": 0.02,  "output": 0.0},
}


@dataclass
class CallRecord:
    stage: str
    provider: str
    model: str
    prompt_name: str
    prompt_version: str
    prompt_tokens: int
    completion_tokens: int
    estimated_prompt_tokens: int
    estimated_completion_tokens: int
    cost_usd: float
    estimated_cost_usd: float
    usage_source: str
    # Prompt-caching telemetry. cache_hit_tokens are the portion of
    # prompt_tokens the provider reported as served from cache (DeepSeek
    # ``usage.prompt_cache_hit_tokens``, OpenAI
    # ``usage.prompt_tokens_details.cached_tokens``). cache_miss_tokens
    # is what was billed at full input rate. 0/0 means the provider
    # didn't surface cache info or there was no cache hit.
    cache_hit_tokens: int = 0
    cache_miss_tokens: int = 0


@dataclass
class _Accumulator:
    calls: list[CallRecord] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


_ACC = _Accumulator()


def _cost_for(
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    cache_hit_tokens: int = 0,
) -> float:
    """Cost = miss_input + hit_cached_input + completion_output.

    cache_hit_tokens is the subset of prompt_tokens served from cache.
    Bounded by prompt_tokens — anything above is clamped to avoid
    negative misses if the provider reports inconsistent counts.
    """
    p = PRICING_PER_MTOKEN.get(model)
    if not p:
        return 0.0
    hit = max(0, min(int(cache_hit_tokens or 0), int(prompt_tokens or 0)))
    miss = int(prompt_tokens or 0) - hit
    cached_price = p.get("cached_input", p["input"])
    return (
        miss * p["input"] / 1_000_000
        + hit * cached_price / 1_000_000
        + completion_tokens * p["output"] / 1_000_000
    )


def estimate_tokens_from_text(text: str) -> int:
    """Cheap cross-provider token estimate used before usage is available."""
    if not text:
        return 0
    # English/Cyrillic mixed prompts average around 3-4 chars/token. Use
    # a slightly conservative divisor so budget alerts err high.
    return max(1, int(len(text) / 3.5))


def estimate_prompt_tokens(messages: list[dict[str, str]] | None) -> int:
    if not messages:
        return 0
    total = 0
    for message in messages:
        total += 4
        total += estimate_tokens_from_text(str(message.get("content") or ""))
    return total + 2


def record_call(
    *,
    stage: str,
    provider: str,
    model: str,
    prompt_name: str,
    prompt_tokens: int,
    completion_tokens: int,
    estimated_prompt_tokens: int | None = None,
    estimated_completion_tokens: int | None = None,
    usage_source: str = "actual",
    cache_hit_tokens: int = 0,
    cache_miss_tokens: int = 0,
) -> None:
    """Append one LLM call to the global accumulator."""
    cost = _cost_for(model, prompt_tokens, completion_tokens, cache_hit_tokens)
    est_prompt = int(estimated_prompt_tokens if estimated_prompt_tokens is not None else prompt_tokens)
    est_completion = int(estimated_completion_tokens if estimated_completion_tokens is not None else completion_tokens)
    # Estimates are an upper bound and ignore caching — they're meant to
    # surface "what would this have cost without cache hits", useful for
    # comparing the savings cache produces over time.
    estimated_cost = _cost_for(model, est_prompt, est_completion)
    from news_digest.pipeline.prompts_meta import prompt_tag_for  # noqa: PLC0415

    prompt_version = prompt_tag_for(prompt_name)
    with _ACC.lock:
        _ACC.calls.append(
            CallRecord(
                stage=stage,
                provider=provider,
                model=model,
                prompt_name=prompt_name,
                prompt_version=prompt_version,
                prompt_tokens=int(prompt_tokens or 0),
                completion_tokens=int(completion_tokens or 0),
                estimated_prompt_tokens=est_prompt,
                estimated_completion_tokens=est_completion,
                cost_usd=cost,
                estimated_cost_usd=estimated_cost,
                usage_source=usage_source,
                cache_hit_tokens=int(cache_hit_tokens or 0),
                cache_miss_tokens=int(cache_miss_tokens or 0),
            )
        )


def _extract_cache_tokens(usage: Any, prompt_tokens: int) -> tuple[int, int]:
    """Return (cache_hit_tokens, cache_miss_tokens) from a vendor usage
    object, normalising DeepSeek and OpenAI shapes.

    DeepSeek: ``usage.prompt_cache_hit_tokens`` /
        ``usage.prompt_cache_miss_tokens`` (both populated when caching
        is active).
    OpenAI:   ``usage.prompt_tokens_details.cached_tokens`` only — miss
        is derived as ``prompt_tokens - cached``.

    Returns (0, 0) when the provider didn't report cache info — callers
    treat that as "no cache info available", not "no cache hit".
    """
    hit = int(getattr(usage, "prompt_cache_hit_tokens", 0) or 0)
    miss = int(getattr(usage, "prompt_cache_miss_tokens", 0) or 0)
    if hit or miss:
        return hit, miss
    details = getattr(usage, "prompt_tokens_details", None)
    if details is not None:
        cached = int(getattr(details, "cached_tokens", 0) or 0)
        if cached:
            return cached, max(0, int(prompt_tokens or 0) - cached)
    return 0, 0


def record_call_from_response(
    *,
    response: Any,
    stage: str,
    provider: str,
    model: str,
    prompt_name: str,
    messages: list[dict[str, str]] | None = None,
    max_tokens: int | None = None,
) -> None:
    """Convenience: pull usage from an OpenAI-compatible response object."""
    estimated_prompt = estimate_prompt_tokens(messages)
    estimated_completion = int(max_tokens or 0)
    usage = getattr(response, "usage", None)
    if not usage:
        record_call(
            stage=stage,
            provider=provider,
            model=model,
            prompt_name=prompt_name,
            prompt_tokens=estimated_prompt,
            completion_tokens=0,
            estimated_prompt_tokens=estimated_prompt,
            estimated_completion_tokens=estimated_completion,
            usage_source="estimated",
        )
        return
    pt = int(getattr(usage, "prompt_tokens", 0) or 0)
    ct = int(getattr(usage, "completion_tokens", 0) or 0)
    cache_hit, cache_miss = _extract_cache_tokens(usage, pt)
    record_call(
        stage=stage,
        provider=provider,
        model=model,
        prompt_name=prompt_name,
        prompt_tokens=pt,
        completion_tokens=ct,
        estimated_prompt_tokens=estimated_prompt or pt,
        estimated_completion_tokens=ct,
        usage_source="actual",
        cache_hit_tokens=cache_hit,
        cache_miss_tokens=cache_miss,
    )


def snapshot(stage: str | None = None) -> list[CallRecord]:
    with _ACC.lock:
        if stage is None:
            return list(_ACC.calls)
        return [c for c in _ACC.calls if c.stage == stage]


def reset() -> None:
    """Clear the accumulator. Used by tests / repeated runs in a process."""
    with _ACC.lock:
        _ACC.calls.clear()


def summarise(records: list[CallRecord]) -> dict[str, Any]:
    """Aggregate by stage, by provider, by model. Returns a dict suitable
    for direct JSON serialisation."""
    total_cost = sum(r.cost_usd for r in records)
    total_estimated_cost = sum(r.estimated_cost_usd for r in records)
    total_calls = len(records)
    by_provider: dict[str, dict[str, float | int]] = {}
    by_model: dict[str, dict[str, float | int]] = {}
    by_stage: dict[str, dict[str, float | int]] = {}
    by_prompt: dict[str, dict[str, float | int]] = {}
    def _empty_slot() -> dict[str, float | int]:
        return {
            "calls": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "estimated_prompt_tokens": 0,
            "estimated_completion_tokens": 0,
            "cache_hit_tokens": 0,
            "cache_miss_tokens": 0,
            "cost_usd": 0.0,
            "estimated_cost_usd": 0.0,
        }

    for r in records:
        for bucket, key in ((by_provider, r.provider), (by_model, r.model), (by_stage, r.stage)):
            slot = bucket.setdefault(key, _empty_slot())
            slot["calls"] += 1
            slot["prompt_tokens"] += r.prompt_tokens
            slot["completion_tokens"] += r.completion_tokens
            slot["estimated_prompt_tokens"] += r.estimated_prompt_tokens
            slot["estimated_completion_tokens"] += r.estimated_completion_tokens
            slot["cache_hit_tokens"] += r.cache_hit_tokens
            slot["cache_miss_tokens"] += r.cache_miss_tokens
            slot["cost_usd"] += r.cost_usd
            slot["estimated_cost_usd"] += r.estimated_cost_usd
        prompt_key = r.prompt_version or r.prompt_name or "unknown"
        prompt_slot = by_prompt.setdefault(prompt_key, _empty_slot())
        prompt_slot["calls"] += 1
        prompt_slot["prompt_tokens"] += r.prompt_tokens
        prompt_slot["completion_tokens"] += r.completion_tokens
        prompt_slot["estimated_prompt_tokens"] += r.estimated_prompt_tokens
        prompt_slot["estimated_completion_tokens"] += r.estimated_completion_tokens
        prompt_slot["cache_hit_tokens"] += r.cache_hit_tokens
        prompt_slot["cache_miss_tokens"] += r.cache_miss_tokens
        prompt_slot["cost_usd"] += r.cost_usd
        prompt_slot["estimated_cost_usd"] += r.estimated_cost_usd
    # Surface unknown-priced models so we notice when a new vendor sneaks in.
    unknown_models = sorted({r.model for r in records if r.model not in PRICING_PER_MTOKEN})
    total_cache_hit = sum(r.cache_hit_tokens for r in records)
    total_cache_miss = sum(r.cache_miss_tokens for r in records)
    total_observed = total_cache_hit + total_cache_miss
    cache_hit_ratio = round(total_cache_hit / total_observed, 4) if total_observed else 0.0
    return {
        "total_calls": total_calls,
        "total_cost_usd": round(total_cost, 6),
        "total_estimated_cost_usd": round(total_estimated_cost, 6),
        "total_prompt_tokens": sum(r.prompt_tokens for r in records),
        "total_completion_tokens": sum(r.completion_tokens for r in records),
        "total_estimated_prompt_tokens": sum(r.estimated_prompt_tokens for r in records),
        "total_estimated_completion_tokens": sum(r.estimated_completion_tokens for r in records),
        "total_cache_hit_tokens": total_cache_hit,
        "total_cache_miss_tokens": total_cache_miss,
        "cache_hit_ratio": cache_hit_ratio,
        "by_stage": by_stage,
        "by_provider": by_provider,
        "by_model": by_model,
        "by_prompt": by_prompt,
        "unknown_priced_models": unknown_models,
    }


def dump_stage(state_dir: Path, stage: str) -> Path:
    """Write per-stage cost snapshot to data/state/cost_<stage>.json."""
    records = snapshot(stage=stage)
    payload = {
        "stage": stage,
        "records": [asdict(r) for r in records],
        "summary": summarise(records),
    }
    out = state_dir / f"cost_{stage}.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out
