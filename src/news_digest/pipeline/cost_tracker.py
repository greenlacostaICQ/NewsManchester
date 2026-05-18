"""Per-run LLM cost & call accounting.

Pipeline stages (curator, llm_rewrite) call `record_call(...)` after
every successful chat.completions.create response. The accumulator
is a module-level singleton because the pipeline is single-process.

At the end of each stage we call `dump_stage(state_dir, stage)` to
write a per-stage cost snapshot. `release.py` aggregates these into
a daily total.

Pricing is per 1M tokens, USD. Update PRICING when adding a model.
Groq free tier → zero cost. If a model is unknown we fall back to
"unknown" pricing and emit a warning at release time (so we notice
when a new model slips in without a price tag).
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any
import json
import threading


# USD per 1M tokens. Verified against vendor pricing pages 2026-05.
# cached_input applies when the provider reports a cache hit on the
# stable system prefix (DeepSeek: usage.prompt_cache_hit_tokens;
# OpenAI: usage.prompt_tokens_details.cached_tokens). When absent
# we fall back to the regular input price.
PRICING_PER_MTOKEN: dict[str, dict[str, float]] = {
    "deepseek-chat":               {"input": 0.27, "cached_input": 0.07, "output": 1.10},
    "deepseek-reasoner":           {"input": 0.55, "cached_input": 0.14, "output": 2.19},
    "gpt-4o-mini":                 {"input": 0.15, "cached_input": 0.075, "output": 0.60},
    "gpt-4o":                      {"input": 2.50, "cached_input": 1.25, "output": 10.00},
    "llama-3.3-70b-versatile":     {"input": 0.0,  "cached_input": 0.0, "output": 0.0},
    "llama-3.1-70b-versatile":     {"input": 0.0,  "cached_input": 0.0, "output": 0.0},
}


@dataclass
class CallRecord:
    stage: str
    provider: str
    model: str
    prompt_name: str
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
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
    """Cost = (prompt - cache_hit) * input + cache_hit * cached_input + completion * output."""
    p = PRICING_PER_MTOKEN.get(model)
    if not p:
        return 0.0
    miss = max(0, prompt_tokens - cache_hit_tokens)
    cached_price = p.get("cached_input", p["input"])
    return (
        miss * p["input"] / 1_000_000
        + cache_hit_tokens * cached_price / 1_000_000
        + completion_tokens * p["output"] / 1_000_000
    )


def record_call(
    *,
    stage: str,
    provider: str,
    model: str,
    prompt_name: str,
    prompt_tokens: int,
    completion_tokens: int,
    cache_hit_tokens: int = 0,
    cache_miss_tokens: int = 0,
) -> None:
    """Append one LLM call to the global accumulator."""
    cost = _cost_for(model, prompt_tokens, completion_tokens, cache_hit_tokens)
    with _ACC.lock:
        _ACC.calls.append(
            CallRecord(
                stage=stage,
                provider=provider,
                model=model,
                prompt_name=prompt_name,
                prompt_tokens=int(prompt_tokens or 0),
                completion_tokens=int(completion_tokens or 0),
                cost_usd=cost,
                cache_hit_tokens=int(cache_hit_tokens or 0),
                cache_miss_tokens=int(cache_miss_tokens or 0),
            )
        )


def _extract_cache_tokens(usage: Any, prompt_tokens: int) -> tuple[int, int]:
    """Return (cache_hit_tokens, cache_miss_tokens) from an OpenAI-compatible
    usage object, normalising DeepSeek and OpenAI shapes.

    DeepSeek: usage.prompt_cache_hit_tokens / prompt_cache_miss_tokens
    OpenAI:   usage.prompt_tokens_details.cached_tokens (miss is derived)
    """
    hit = int(getattr(usage, "prompt_cache_hit_tokens", 0) or 0)
    miss = int(getattr(usage, "prompt_cache_miss_tokens", 0) or 0)
    if hit or miss:
        return hit, miss
    details = getattr(usage, "prompt_tokens_details", None)
    if details is not None:
        cached = int(getattr(details, "cached_tokens", 0) or 0)
        if cached:
            return cached, max(0, prompt_tokens - cached)
    return 0, 0


def record_call_from_response(
    *,
    response: Any,
    stage: str,
    provider: str,
    model: str,
    prompt_name: str,
) -> None:
    """Convenience: pull usage from an OpenAI-compatible response object."""
    usage = getattr(response, "usage", None)
    if not usage:
        return
    pt = int(getattr(usage, "prompt_tokens", 0) or 0)
    ct = int(getattr(usage, "completion_tokens", 0) or 0)
    hit, miss = _extract_cache_tokens(usage, pt)
    record_call(
        stage=stage,
        provider=provider,
        model=model,
        prompt_name=prompt_name,
        prompt_tokens=pt,
        completion_tokens=ct,
        cache_hit_tokens=hit,
        cache_miss_tokens=miss,
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
    total_calls = len(records)
    by_provider: dict[str, dict[str, float | int]] = {}
    by_model: dict[str, dict[str, float | int]] = {}
    by_stage: dict[str, dict[str, float | int]] = {}
    for r in records:
        for bucket, key in ((by_provider, r.provider), (by_model, r.model), (by_stage, r.stage)):
            slot = bucket.setdefault(key, {
                "calls": 0, "prompt_tokens": 0, "completion_tokens": 0,
                "cache_hit_tokens": 0, "cache_miss_tokens": 0, "cost_usd": 0.0,
            })
            slot["calls"] += 1
            slot["prompt_tokens"] += r.prompt_tokens
            slot["completion_tokens"] += r.completion_tokens
            slot["cache_hit_tokens"] += r.cache_hit_tokens
            slot["cache_miss_tokens"] += r.cache_miss_tokens
            slot["cost_usd"] += r.cost_usd
    # Surface unknown-priced models so we notice when a new vendor sneaks in.
    unknown_models = sorted({r.model for r in records if r.model not in PRICING_PER_MTOKEN})
    total_hit = sum(r.cache_hit_tokens for r in records)
    total_miss = sum(r.cache_miss_tokens for r in records)
    total_cached_denom = total_hit + total_miss
    cache_hit_ratio = round(total_hit / total_cached_denom, 4) if total_cached_denom else 0.0
    return {
        "total_calls": total_calls,
        "total_cost_usd": round(total_cost, 6),
        "total_prompt_tokens": sum(r.prompt_tokens for r in records),
        "total_completion_tokens": sum(r.completion_tokens for r in records),
        "total_cache_hit_tokens": total_hit,
        "total_cache_miss_tokens": total_miss,
        "cache_hit_ratio": cache_hit_ratio,
        "by_stage": by_stage,
        "by_provider": by_provider,
        "by_model": by_model,
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
