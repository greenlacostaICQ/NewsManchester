from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import statistics
import time
from typing import Any

from news_digest.pipeline.common import now_london, read_json, write_json
from news_digest.pipeline.cost_tracker import (
    CallRecord,
    estimate_prompt_tokens,
    record_call_from_response,
    summarise,
)
from news_digest.pipeline.model_routing import (
    DEEPSEEK_BASE_URL,
    DEEPSEEK_MODEL,
    DEEPSEEK_PRO_MODEL,
    OPENAI_BASE_URL,
    OPENAI_SCORING_MODEL,
    ModelRouteStep,
    chat_completion_options_for_route,
    sdk_retries_for_route,
)
from news_digest.pipeline.reader_value import LABELS_PATH, VALID_LABELS, validate_reader_value_labels
from news_digest.pipeline.story_intelligence import (
    MODEL_BAKEOFF_SPEC,
    REASON_CODE_ENUM,
    apply_story_intelligence,
)


MODEL_BAKEOFF_REPORT = Path("data") / "state" / "model_bakeoff_report.json"
DEFAULT_BATCH_SIZE = 12
MAX_TOKENS = 5000
TEMPERATURE = 0.0

JUDGE_PROMPT = f"""You are an English-first editorial judge for a Greater Manchester morning digest.
Evaluate only the English/source evidence in each item. Do not infer from the gold label or from Russian output.

Return a JSON object in this shape: {"items": [...]}. Each item must contain:
- fingerprint: string
- decision: one of publish_candidate, backup_candidate, reject
- false_negative_risk: one of low, medium, high
- reason_codes: array using only this enum: {", ".join(REASON_CODE_ENUM)}
- editorial_score: integer 0-100

Decision rules:
- publish_candidate: local, timely, concrete, useful, or protected public-service/event/ticket/civic item.
- backup_candidate: not strong enough to publish now, but risky to delete silently.
- reject: clear non-GM, duplicate/rehashed, expired, pure PR, evergreen/listicle, weak human-interest, property listing.
- If a useful item might be lost because evidence is thin, prefer backup_candidate over reject.
"""


BAKEOFF_MODELS: tuple[ModelRouteStep, ...] = (
    ModelRouteStep(
        provider="deepseek",
        provider_label="DeepSeek",
        base_url=DEEPSEEK_BASE_URL,
        model=DEEPSEEK_MODEL,
        api_key_env="DEEPSEEK_API_KEY",
        role="cheap_prefilter_or_baseline",
        priority=1,
        batch_size=DEFAULT_BATCH_SIZE,
        timeout_seconds=30,
    ),
    ModelRouteStep(
        provider="deepseek",
        provider_label="DeepSeek",
        base_url=DEEPSEEK_BASE_URL,
        model=DEEPSEEK_PRO_MODEL,
        api_key_env="DEEPSEEK_API_KEY",
        role="english_fact_reader_candidate",
        priority=2,
        batch_size=DEFAULT_BATCH_SIZE,
        timeout_seconds=30,
    ),
    ModelRouteStep(
        provider="openai",
        provider_label="OpenAI",
        base_url=OPENAI_BASE_URL,
        model=OPENAI_SCORING_MODEL,
        api_key_env="OPENAI_API_KEY",
        role="quality_judge_candidate",
        priority=3,
        batch_size=DEFAULT_BATCH_SIZE,
        timeout_seconds=30,
    ),
)


def _candidate_from_label(item: dict) -> dict:
    candidate = {
        "fingerprint": item.get("fingerprint") or "",
        "title": item.get("title") or "",
        "source_label": item.get("source_label") or "",
        "source_url": item.get("source_url") or "",
        "category": item.get("category") or "",
        "primary_block": item.get("primary_block") or "",
        "include": bool(item.get("included")),
        "included": bool(item.get("included")),
        "change_type": item.get("change_type") or "",
        "reason": item.get("reject_reason") or "",
        "reject_reason": item.get("reject_reason") or "",
        # Labels are compact historical samples. Rationale is human audit
        # context, not a model target; it gives the judge enough evidence
        # when the original article body is not in the validation set.
        "summary": item.get("summary") or item.get("rationale") or "",
        "lead": item.get("lead") or "",
        "evidence_text": item.get("evidence_text") or item.get("rationale") or "",
    }
    apply_story_intelligence(candidate)
    return candidate


def _compact_candidate(candidate: dict) -> dict[str, Any]:
    return {
        "fingerprint": candidate.get("fingerprint") or "",
        "title": candidate.get("title") or "",
        "source_label": candidate.get("source_label") or "",
        "category": candidate.get("category") or "",
        "primary_block": candidate.get("primary_block") or "",
        "change_type": candidate.get("change_type") or "",
        "reject_reason": candidate.get("reject_reason") or candidate.get("reason") or "",
        "summary": candidate.get("summary") or "",
        "lead": candidate.get("lead") or "",
        "evidence_text": str(candidate.get("evidence_text") or "")[:1200],
        "news_anchor": candidate.get("news_anchor") or {},
        "protected_lane": candidate.get("protected_lane") or {},
        "rubric_contract": candidate.get("rubric_contract") or {},
    }


def _strip_json_fence(raw: str) -> str:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0]
    return text.strip()


def _normalise_decision(value: object) -> str:
    text = str(value or "").strip()
    if text in {"publish_candidate", "backup_candidate", "reject"}:
        return text
    return "backup_candidate"


def _stub_result(candidate: dict) -> dict[str, object]:
    judge = candidate.get("english_judge") if isinstance(candidate.get("english_judge"), dict) else {}
    score = int(float(judge.get("editorial_score") or candidate.get("section_board_score") or 0))
    return {
        "fingerprint": candidate.get("fingerprint") or "",
        "decision": _normalise_decision(judge.get("decision")),
        "false_negative_risk": judge.get("false_negative_risk") or "low",
        "reason_codes": judge.get("reason_codes") or [],
        "editorial_score": max(0, min(100, score)),
    }


def _metrics_for_results(labels: list[dict], results: dict[str, dict], *, latencies: list[float], cost_summary: dict) -> dict[str, object]:
    labels_by_fp = {str(item.get("fingerprint") or ""): item for item in labels}
    rows: list[dict[str, object]] = []
    valid_json = 0
    for fp, label in labels_by_fp.items():
        actual = str(label.get("label") or "")
        result = results.get(fp) or {}
        decision = _normalise_decision(result.get("decision"))
        valid_json += 1 if result else 0
        predicted_publish = decision == "publish_candidate"
        rows.append(
            {
                "fingerprint": fp,
                "title": label.get("title") or "",
                "label": actual,
                "decision": decision,
                "false_negative": actual == "useful" and not predicted_publish,
                "false_positive": actual == "should_not_include" and predicted_publish,
                "json_valid": bool(result),
                "reason_codes": result.get("reason_codes") or [],
                "editorial_score": result.get("editorial_score"),
            }
        )

    useful = [row for row in rows if row["label"] == "useful"]
    should_not = [row for row in rows if row["label"] == "should_not_include"]
    false_negatives = [row for row in rows if row["false_negative"]]
    false_positives = [row for row in rows if row["false_positive"]]
    p95 = 0.0
    if latencies:
        p95 = max(latencies) if len(latencies) < 20 else statistics.quantiles(latencies, n=20)[-1]
    return {
        "label_count": len(rows),
        "json_validity": round(valid_json / len(rows), 3) if rows else 0.0,
        "false_negative_rate_on_useful": round(len(false_negatives) / len(useful), 3) if useful else 0.0,
        "false_positive_rate_on_should_not_include": round(len(false_positives) / len(should_not), 3) if should_not else 0.0,
        "false_negative_count": len(false_negatives),
        "false_positive_count": len(false_positives),
        "p95_latency_seconds": round(p95, 3),
        "cost_summary": cost_summary,
        "examples": {
            "false_negatives": false_negatives[:10],
            "false_positives": false_positives[:10],
        },
    }


def _call_model_batches(step: ModelRouteStep, candidates: list[dict]) -> tuple[dict[str, dict], list[float], dict[str, object]]:
    api_key = os.environ.get(step.api_key_env, "")
    if not api_key:
        return {}, [], {"status": "skipped_missing_api_key", "api_key_env": step.api_key_env}
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        return {}, [], {"status": "skipped_missing_openai_package"}

    client = OpenAI(
        api_key=api_key,
        base_url=step.base_url,
        timeout=step.timeout_seconds or 30,
        max_retries=sdk_retries_for_route(provider=step.provider, model=step.model, base_url=step.base_url),
    )
    batch_size = int(step.batch_size or DEFAULT_BATCH_SIZE)
    results: dict[str, dict] = {}
    latencies: list[float] = []
    calls: list[CallRecord] = []
    for start in range(0, len(candidates), batch_size):
        batch = candidates[start:start + batch_size]
        messages = [
            {"role": "system", "content": JUDGE_PROMPT},
            {"role": "user", "content": json.dumps([_compact_candidate(c) for c in batch], ensure_ascii=False)},
        ]
        t0 = time.perf_counter()
        response = client.chat.completions.create(
            model=step.model,
            messages=messages,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            **chat_completion_options_for_route(provider=step.provider, model=step.model, base_url=step.base_url),
        )
        latencies.append(time.perf_counter() - t0)
        record_call_from_response(
            response=response,
            stage="model_bakeoff",
            provider=step.provider,
            model=step.model,
            prompt_name="english_judge_bakeoff",
            messages=messages,
            max_tokens=MAX_TOKENS,
        )
        usage = getattr(response, "usage", None)
        prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or estimate_prompt_tokens(messages))
        completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        from news_digest.pipeline.cost_tracker import _cost_for  # noqa: PLC0415

        calls.append(
            CallRecord(
                stage="model_bakeoff",
                provider=step.provider,
                model=step.model,
                prompt_name="english_judge_bakeoff",
                prompt_version="english_judge_bakeoff",
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                estimated_prompt_tokens=estimate_prompt_tokens(messages),
                estimated_completion_tokens=MAX_TOKENS,
                cost_usd=_cost_for(step.model, prompt_tokens, completion_tokens),
                estimated_cost_usd=_cost_for(step.model, estimate_prompt_tokens(messages), MAX_TOKENS),
                usage_source="actual" if usage else "estimated",
            )
        )
        raw = _strip_json_fence(response.choices[0].message.content)
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            for key in ("items", "results", "decisions"):
                if isinstance(parsed.get(key), list):
                    parsed = parsed[key]
                    break
            else:
                parsed = []
        if not isinstance(parsed, list):
            raise ValueError("judge response must be a JSON object with items/results or a JSON array")
        for row in parsed:
            if not isinstance(row, dict):
                continue
            fp = str(row.get("fingerprint") or "")
            if not fp:
                continue
            row["decision"] = _normalise_decision(row.get("decision"))
            results[fp] = row
    return results, latencies, {"status": "ok", "cost_summary": summarise(calls)}


def _promotion_recommendation(model_reports: list[dict]) -> dict[str, object]:
    ok_models = [m for m in model_reports if m.get("status") == "ok"]
    if not ok_models:
        return {"decision": "no_promotion", "reason": "no model completed bake-off"}
    baseline = next((m for m in ok_models if m.get("model") == "deterministic_stub"), None)
    challengers = [m for m in ok_models if m.get("model") != "deterministic_stub"]
    if not challengers:
        return {"decision": "keep_stub", "reason": "only deterministic stub was evaluated"}

    base_metrics = (baseline or {}).get("metrics") or {}
    base_fn = float(base_metrics.get("false_negative_rate_on_useful") or 1.0)
    base_fp = float(base_metrics.get("false_positive_rate_on_should_not_include") or 1.0)
    ranked = sorted(
        challengers,
        key=lambda m: (
            float((m.get("metrics") or {}).get("false_negative_rate_on_useful") or 1.0),
            float((m.get("metrics") or {}).get("false_positive_rate_on_should_not_include") or 1.0),
            float((m.get("metrics") or {}).get("p95_latency_seconds") or 999.0),
        ),
    )
    best = ranked[0]
    metrics = best.get("metrics") or {}
    best_fn = float(metrics.get("false_negative_rate_on_useful") or 1.0)
    best_fp = float(metrics.get("false_positive_rate_on_should_not_include") or 1.0)
    if best_fn <= base_fn and best_fp <= base_fp:
        return {
            "decision": "candidate_for_judge_primary",
            "provider": best.get("provider"),
            "model": best.get("model"),
            "reason": "challenger is no worse on useful false negatives and should_not_include false positives",
        }
    return {
        "decision": "keep_stub",
        "reason": "no challenger beat the deterministic stub under the promotion rule",
    }


def run_model_bakeoff(project_root: Path, *, dry_run: bool = False, limit: int | None = None) -> dict[str, object]:
    labels_path = project_root / LABELS_PATH
    labels_payload = read_json(labels_path, {"labels": []})
    errors = validate_reader_value_labels(labels_payload)
    labels = [item for item in labels_payload.get("labels") or [] if isinstance(item, dict) and str(item.get("label") or "") in VALID_LABELS]
    if limit is not None and limit > 0:
        labels = labels[:limit]
    candidates = [_candidate_from_label(item) for item in labels]

    stub_results = {str(c.get("fingerprint") or ""): _stub_result(c) for c in candidates}
    model_reports: list[dict[str, object]] = [
        {
            "provider": "deterministic",
            "model": "deterministic_stub",
            "role": "current_production_gate",
            "status": "ok",
            "metrics": _metrics_for_results(labels, stub_results, latencies=[], cost_summary=summarise([])),
        }
    ]

    if not dry_run and not errors:
        for step in BAKEOFF_MODELS:
            started = time.perf_counter()
            try:
                results, latencies, status = _call_model_batches(step, candidates)
                elapsed = round(time.perf_counter() - started, 3)
                if status.get("status") == "ok":
                    metrics = _metrics_for_results(labels, results, latencies=latencies, cost_summary=status.get("cost_summary") or {})
                else:
                    metrics = {}
                model_reports.append(
                    {
                        "provider": step.provider,
                        "provider_label": step.provider_label,
                        "model": step.model,
                        "role": step.role,
                        "status": status.get("status"),
                        "elapsed_seconds": elapsed,
                        "metrics": metrics,
                        "diagnostic": {k: v for k, v in status.items() if k != "cost_summary"},
                    }
                )
            except Exception as exc:  # noqa: BLE001 - bake-off is diagnostic, not release path
                model_reports.append(
                    {
                        "provider": step.provider,
                        "provider_label": step.provider_label,
                        "model": step.model,
                        "role": step.role,
                        "status": "failed",
                        "elapsed_seconds": round(time.perf_counter() - started, 3),
                        "metrics": {},
                        "diagnostic": {"error": str(exc)},
                    }
                )
    elif dry_run:
        for step in BAKEOFF_MODELS:
            model_reports.append(
                {
                    "provider": step.provider,
                    "provider_label": step.provider_label,
                    "model": step.model,
                    "role": step.role,
                    "status": "dry_run_not_called",
                    "metrics": {},
                    "diagnostic": {"route": asdict(step)},
                }
            )

    report = {
        **MODEL_BAKEOFF_SPEC,
        "run_at_london": now_london().isoformat(),
        "dry_run": dry_run,
        "validation_errors": errors,
        "validation_set": {
            "path": str(labels_path),
            "label_count": len(labels),
            "label_counts": {label: sum(1 for item in labels if item.get("label") == label) for label in VALID_LABELS},
        },
        "models": model_reports,
        "promotion_recommendation": _promotion_recommendation(model_reports),
    }
    out = project_root / MODEL_BAKEOFF_REPORT
    out.parent.mkdir(parents=True, exist_ok=True)
    write_json(out, report)
    report["report_path"] = str(out.resolve())
    return report
