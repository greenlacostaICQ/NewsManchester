"""Post-publish quality judge (S6).

After the daily digest has been published, an LLM (gpt-4o-mini) scores
the final HTML on 5 axes: factuality, novelty, source_diversity,
signal_density, coherence. Scores are stored append-only in
``data/state/digest_evals.jsonl``. A 7-day rolling mean is compared
to a 30-day baseline; any axis falling >=1 sigma below baseline
becomes a drift signal surfaced in the Telegram admin report.

The judge runs independently of the release gate and never blocks
publication. If the LLM call fails, the day's eval is simply absent
— missing days don't crash drift detection.

Why this is the LAST piece of the S1-S5 plan, not the first:
- S1-S5 changed the rules. If the judge had been wired up before,
  every day's eval would have reflected the broken state and the
  30-day baseline would be poisoned.
- After S1-S5 we get a clean baseline. Drift detected later actually
  means quality is moving, not that we're still fighting old issues.

Cost: 1x gpt-4o-mini call per day, ~5k input tokens + ~300 output.
About $0.001/day at current prices. Pipeline wall-time +5–10s.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, pstdev

from news_digest.pipeline.common import now_london, today_london


logger = logging.getLogger(__name__)


JUDGE_PROMPT_VERSION = "v1"

JUDGE_PROMPT = """Ты редакционный судья для утреннего дайджеста Greater Manchester.

На вход получишь JSON с полями:
  today  — сегодняшняя дата (YYYY-MM-DD);
  digest_html — финальный HTML дайджеста, как он ушёл читателю;
  candidates — краткая выжимка по карточкам (title, source, primary_block).

Оцени выпуск по 5 шкалам, каждая от 0.0 до 5.0 (десятичные допустимы).

1. factuality — насколько утверждения в карточках подкреплены конкретикой.
   5.0 = всё чётко привязано к именам, цифрам, датам. 0.0 = общие фразы,
   возможные галлюцинации без подтверждения.

2. novelty — насколько выпуск даёт новую информацию vs пересказ.
   5.0 = каждая карточка несёт свежий факт, не повторявшийся в последние
   дни. 0.0 = повторы / rehash / события без новых деталей.

3. source_diversity — разнообразие источников.
   5.0 = разные outlets и official-источники в разных секциях.
   0.0 = всё из одного MEN или одного outlet >60%.

4. signal_density — отношение полезной актуальной инфы к общим словам.
   5.0 = каждое предложение несёт факт. 0.0 = вода и эмоциональные
   описания вместо конкретики.

5. coherence — связность структуры выпуска.
   5.0 = нет дублей между секциями, порядок логичен.
   0.0 = одна история в трёх местах, секции перемешаны.

Дай короткий комментарий notes (до 200 символов) на русском —
объясни самый низкий балл одной фразой.

Верни ТОЛЬКО JSON-объект, без markdown-обёртки, ровно такой формы:
{"factuality": X.X, "novelty": X.X, "source_diversity": X.X,
 "signal_density": X.X, "coherence": X.X, "notes": "..."}
"""


AXES: tuple[str, ...] = (
    "factuality", "novelty", "source_diversity", "signal_density", "coherence",
)


@dataclass(frozen=True)
class EvalRow:
    date: str  # YYYY-MM-DD London-local
    factuality: float
    novelty: float
    source_diversity: float
    signal_density: float
    coherence: float
    notes: str = ""
    judge_model: str = "gpt-4o-mini"
    judge_prompt_version: str = JUDGE_PROMPT_VERSION
    pipeline_run_id: str = ""


# ---------------------------------------------------------------------
# Storage — append-only JSONL with date-idempotency
# ---------------------------------------------------------------------


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def append_eval(state_dir: Path, row: EvalRow) -> Path:
    """Append a daily eval row. Idempotent on (date, judge_prompt_version):
    re-running on the same day overwrites today's row, so an in-day
    second pass (e.g. after a re-send) updates rather than duplicates.
    """
    path = state_dir / "digest_evals.jsonl"
    existing = [
        obj for obj in _read_jsonl(path)
        if obj.get("date") != row.date
    ]
    existing.append(asdict(row))
    existing.sort(key=lambda r: str(r.get("date") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in existing) + "\n",
        encoding="utf-8",
    )
    return path


def load_history(state_dir: Path, *, since_days: int = 30) -> list[dict]:
    """Load eval rows from the last ``since_days`` (inclusive), oldest
    first. Uses today_london() as the reference 'today' so a fresh
    eval written this morning is included.
    """
    path = state_dir / "digest_evals.jsonl"
    rows = _read_jsonl(path)
    if not rows:
        return []
    today = datetime.strptime(today_london(), "%Y-%m-%d").date()
    cutoff = today - timedelta(days=since_days - 1)
    keep: list[dict] = []
    for obj in rows:
        try:
            row_date = datetime.strptime(str(obj.get("date") or ""), "%Y-%m-%d").date()
        except ValueError:
            continue
        if row_date >= cutoff:
            keep.append(obj)
    keep.sort(key=lambda r: str(r.get("date") or ""))
    return keep


# ---------------------------------------------------------------------
# Drift detection — compare today vs 30-day baseline
# ---------------------------------------------------------------------


def detect_drift(history: list[dict], *, min_baseline_days: int = 14) -> dict:
    """Compute per-axis today / 7-day mean / 30-day baseline mean /
    baseline stddev / sigmas_below_baseline.

    Flags an axis when:
      - baseline has >= ``min_baseline_days`` data points (otherwise
        stddev is too noisy to trust — fewer rows mean fewer false
        positives early on);
      - baseline stddev > 0 (constant baselines never trigger);
      - today >= 1 sigma below the baseline mean.

    Returns {"status", "baseline_days", "week_days", "axes": {...},
    "signals": [...]} with status in {"no_data", "warming_up", "ok"}.
    """
    if not history:
        return {
            "status": "no_data",
            "baseline_days": 0,
            "week_days": 0,
            "axes": {},
            "signals": [],
        }
    today_row = history[-1]
    week = history[-7:] if len(history) >= 7 else history
    baseline = history[:-1]
    baseline_days = len(baseline)
    has_real_baseline = baseline_days >= min_baseline_days
    status = "ok" if has_real_baseline else "warming_up"

    axes_out: dict[str, dict] = {}
    signals: list[dict] = []
    for axis in AXES:
        today_v = float(today_row.get(axis) or 0.0)
        week_v = mean(float(r.get(axis) or 0.0) for r in week) if week else 0.0
        if baseline:
            baseline_values = [float(r.get(axis) or 0.0) for r in baseline]
            baseline_v = mean(baseline_values)
            sigma = pstdev(baseline_values) if len(baseline_values) > 1 else 0.0
        else:
            baseline_v = week_v
            sigma = 0.0
        if sigma > 0:
            sigmas_below = round((baseline_v - today_v) / sigma, 2)
        else:
            sigmas_below = 0.0
        axis_data = {
            "today": round(today_v, 2),
            "week_mean": round(week_v, 2),
            "baseline_mean": round(baseline_v, 2),
            "baseline_sigma": round(sigma, 2),
            "sigmas_below_baseline": sigmas_below,
        }
        axes_out[axis] = axis_data
        if has_real_baseline and sigma > 0 and sigmas_below >= 1.0:
            signals.append({"axis": axis, **axis_data})

    return {
        "status": status,
        "baseline_days": baseline_days,
        "week_days": len(week),
        "axes": axes_out,
        "signals": signals,
    }


# ---------------------------------------------------------------------
# LLM call wrapper
# ---------------------------------------------------------------------


_SCORE_RANGE = (0.0, 5.0)


def _clamp_score(value: object) -> float | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if score < _SCORE_RANGE[0]:
        return _SCORE_RANGE[0]
    if score > _SCORE_RANGE[1]:
        return _SCORE_RANGE[1]
    return score


def _parse_judge_reply(raw: str) -> dict | None:
    """Tolerant JSON parser: strip code-fence wrapping, accept either
    a bare JSON object or one wrapped in ```json … ```.
    """
    if not raw:
        return None
    text = raw.strip()
    # Strip ```json ... ``` or ``` ... ``` fences.
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        text = m.group(1)
    else:
        # Cut to outermost {...} if there's prose around.
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            text = m.group(0)
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def build_candidate_summary(candidates: list[dict]) -> list[dict]:
    """A compact per-card summary for the judge so it can see the
    source mix without us shipping the full evidence_text.
    """
    summary: list[dict] = []
    for c in candidates:
        if not isinstance(c, dict) or not c.get("include"):
            continue
        summary.append({
            "title": str(c.get("title") or "")[:160],
            "source_label": str(c.get("source_label") or ""),
            "category": str(c.get("category") or ""),
            "primary_block": str(c.get("primary_block") or ""),
            "change_type": str(c.get("change_type") or ""),
        })
    return summary[:60]


def run_judge_llm(
    digest_html: str,
    candidate_summary: list[dict],
    *,
    today_iso: str | None = None,
    api_key: str | None = None,
    model: str = "gpt-4o-mini",
) -> dict | None:
    """Call gpt-4o-mini on the published digest. Returns a parsed JSON
    dict (with the 5 axis scores + notes), or None on any failure.

    The caller is responsible for turning a successful dict into an
    EvalRow and appending it.
    """
    if api_key is None:
        api_key = os.environ.get("OPENAI_API_KEY") or ""
    if not api_key:
        logger.warning("post-publish judge: OPENAI_API_KEY not set; skipping.")
        return None
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.warning("openai package not installed; skipping judge.")
        return None

    payload = {
        "today": today_iso or today_london(),
        "digest_html": digest_html[:18000],
        "candidates": candidate_summary,
    }
    user_content = json.dumps(payload, ensure_ascii=False)
    try:
        client = OpenAI(api_key=api_key, timeout=30, max_retries=1)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": JUDGE_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.0,
            max_tokens=400,
        )
        raw = response.choices[0].message.content
        # Record cost like every other LLM call.
        try:
            from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415
            record_call_from_response(
                response=response,
                stage="post_publish_judge",
                provider="openai",
                model=model,
                prompt_name="post_publish_judge",
                messages=[
                    {"role": "system", "content": JUDGE_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                max_tokens=400,
            )
        except Exception as exc:  # noqa: BLE001 - cost tracking must not break the judge
            logger.warning("post-publish judge: cost tracking failed: %s", exc)
        return _parse_judge_reply(raw or "")
    except Exception as exc:  # noqa: BLE001 - judge must not break the pipeline
        logger.warning("post-publish judge LLM call failed: %s", exc)
        return None


# ---------------------------------------------------------------------
# Main entry point — called from CLI / workflow after the digest ships
# ---------------------------------------------------------------------


def evaluate_today(
    project_root: Path,
    *,
    pipeline_run_id: str = "",
    api_key: str | None = None,
    model: str = "gpt-4o-mini",
) -> dict:
    """End-to-end:
      1. read current_digest.html + candidates.json;
      2. call gpt-4o-mini judge;
      3. append today's eval to digest_evals.jsonl;
      4. compute drift signals;
      5. return {"status", "eval", "drift"} so the caller can write a
         human-readable report.

    Never raises. On any soft failure (no API key, parse error, etc.)
    returns {"status": "skipped", "reason": ..., "drift": ...} so the
    Telegram report can still surface drift from prior days.
    """
    state_dir = project_root / "data" / "state"
    outgoing_dir = project_root / "data" / "outgoing"
    digest_path = outgoing_dir / "current_digest.html"
    candidates_path = state_dir / "candidates.json"

    if not digest_path.exists():
        return {
            "status": "skipped",
            "reason": "current_digest.html missing",
            "drift": detect_drift(load_history(state_dir)),
        }

    try:
        digest_html = digest_path.read_text(encoding="utf-8")
    except OSError as exc:
        return {
            "status": "skipped",
            "reason": f"failed to read digest: {exc}",
            "drift": detect_drift(load_history(state_dir)),
        }

    candidate_summary: list[dict] = []
    if candidates_path.exists():
        try:
            payload = json.loads(candidates_path.read_text(encoding="utf-8"))
            candidate_summary = build_candidate_summary(payload.get("candidates") or [])
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("post-publish judge: candidates load failed: %s", exc)

    today_iso = today_london()
    parsed = run_judge_llm(
        digest_html=digest_html,
        candidate_summary=candidate_summary,
        today_iso=today_iso,
        api_key=api_key,
        model=model,
    )
    if parsed is None:
        return {
            "status": "skipped",
            "reason": "LLM call failed or returned no parseable JSON",
            "drift": detect_drift(load_history(state_dir)),
        }

    scores: dict[str, float] = {}
    for axis in AXES:
        score = _clamp_score(parsed.get(axis))
        if score is None:
            return {
                "status": "skipped",
                "reason": f"judge response missing axis '{axis}'",
                "raw": parsed,
                "drift": detect_drift(load_history(state_dir)),
            }
        scores[axis] = score

    notes = str(parsed.get("notes") or "")[:240]
    row = EvalRow(
        date=today_iso,
        factuality=scores["factuality"],
        novelty=scores["novelty"],
        source_diversity=scores["source_diversity"],
        signal_density=scores["signal_density"],
        coherence=scores["coherence"],
        notes=notes,
        judge_model=model,
        judge_prompt_version=JUDGE_PROMPT_VERSION,
        pipeline_run_id=pipeline_run_id,
    )
    append_eval(state_dir, row)
    history = load_history(state_dir)
    return {
        "status": "ok",
        "eval": asdict(row),
        "drift": detect_drift(history),
    }
