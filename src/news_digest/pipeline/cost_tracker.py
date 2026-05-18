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
PRICING_PER_MTOKEN: dict[str, dict[str, float]] = {
    "deepseek-chat":               {"input": 0.27, "output": 1.10},
    "deepseek-reasoner":           {"input": 0.55, "output": 2.19},
    "gpt-4o-mini":                 {"input": 0.15, "output": 0.60},
    "gpt-4o":                      {"input": 2.50, "output": 10.00},
    "llama-3.3-70b-versatile":     {"input": 0.0,  "output": 0.0},   # Groq free tier
    "llama-3.1-70b-versatile":     {"input": 0.0,  "output": 0.0},
    # Embeddings: priced per input token only; completion is always 0
    # for these endpoints so output multiplier is irrelevant.
    "text-embedding-3-small":      {"input": 0.02, "output": 0.0},
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


@dataclass
class _Accumulator:
    calls: list[CallRecord] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


_ACC = _Accumulator()


def _cost_for(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    p = PRICING_PER_MTOKEN.get(model)
    if not p:
        return 0.0
    return (
        prompt_tokens * p["input"] / 1_000_000
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
) -> None:
    """Append one LLM call to the global accumulator."""
    cost = _cost_for(model, prompt_tokens, completion_tokens)
    est_prompt = int(estimated_prompt_tokens if estimated_prompt_tokens is not None else prompt_tokens)
    est_completion = int(estimated_completion_tokens if estimated_completion_tokens is not None else completion_tokens)
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
            )
        )


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
    for r in records:
        for bucket, key in ((by_provider, r.provider), (by_model, r.model), (by_stage, r.stage)):
            slot = bucket.setdefault(
                key,
                {
                    "calls": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "estimated_prompt_tokens": 0,
                    "estimated_completion_tokens": 0,
                    "cost_usd": 0.0,
                    "estimated_cost_usd": 0.0,
                },
            )
            slot["calls"] += 1
            slot["prompt_tokens"] += r.prompt_tokens
            slot["completion_tokens"] += r.completion_tokens
            slot["estimated_prompt_tokens"] += r.estimated_prompt_tokens
            slot["estimated_completion_tokens"] += r.estimated_completion_tokens
            slot["cost_usd"] += r.cost_usd
            slot["estimated_cost_usd"] += r.estimated_cost_usd
        prompt_key = r.prompt_version or r.prompt_name or "unknown"
        prompt_slot = by_prompt.setdefault(
            prompt_key,
            {
                "calls": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "estimated_prompt_tokens": 0,
                "estimated_completion_tokens": 0,
                "cost_usd": 0.0,
                "estimated_cost_usd": 0.0,
            },
        )
        prompt_slot["calls"] += 1
        prompt_slot["prompt_tokens"] += r.prompt_tokens
        prompt_slot["completion_tokens"] += r.completion_tokens
        prompt_slot["estimated_prompt_tokens"] += r.estimated_prompt_tokens
        prompt_slot["estimated_completion_tokens"] += r.estimated_completion_tokens
        prompt_slot["cost_usd"] += r.cost_usd
        prompt_slot["estimated_cost_usd"] += r.estimated_cost_usd
    # Surface unknown-priced models so we notice when a new vendor sneaks in.
    unknown_models = sorted({r.model for r in records if r.model not in PRICING_PER_MTOKEN})
    return {
        "total_calls": total_calls,
        "total_cost_usd": round(total_cost, 6),
        "total_estimated_cost_usd": round(total_estimated_cost, 6),
        "total_prompt_tokens": sum(r.prompt_tokens for r in records),
        "total_completion_tokens": sum(r.completion_tokens for r in records),
        "total_estimated_prompt_tokens": sum(r.estimated_prompt_tokens for r in records),
        "total_estimated_completion_tokens": sum(r.estimated_completion_tokens for r in records),
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
