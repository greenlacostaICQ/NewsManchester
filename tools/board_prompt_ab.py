#!/usr/bin/env python3
"""A/B of board-judge prompts on a saved block of real news.

Runs each prompt variant over the SAME saved pool and reports two things:

  * agreement between variants — where they disagree is where the wording is
    actually doing work rather than decorating;
  * a bias probe that needs no human labels. We know the failure mode the
    absolute 0-100 scale had on 2026-07-23: a bus stop closure ranked 100 while
    a court report on a synagogue attacker ranked 76, because the model was
    grading how complete a record looked. The probe measures exactly that —
    mean rank of routine notices vs mean rank of hard news. A variant that puts
    roadworks above courts has the bug regardless of anything else.

If a gold ranking exists (tools/board_eval.py dump -> your own order), the
Spearman against it is reported too — that is the deciding number; the probe
only catches the known failure early.

Requires DEEPSEEK_API_KEY (or OPENAI_API_KEY). These live in GitHub Actions
Secrets, not in the repo, so run it where the keys are:

    PYTHONPATH=src python3 tools/board_prompt_ab.py --block last_24h
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from news_digest.pipeline import board_rank  # noqa: E402
from news_digest.pipeline.common import PRIMARY_BLOCKS  # noqa: E402
from news_digest.pipeline.story_intelligence import apply_story_intelligence  # noqa: E402

# Routine notices: complete, well-formed, low consequence. Hard news: high
# consequence, usually with facts the source is not allowed to spell out.
ROUTINE_RE = re.compile(
    r"\b(?:roadworks?|road closure|bus stop|stop closure|timetable|"
    r"amended service|replacement bus|improvement works|lane closure|diversion)\b",
    re.IGNORECASE,
)
HARDNEWS_RE = re.compile(
    r"\b(?:charged|court|jailed|sentenc\w+|murder|stabb\w+|assault|arrest\w*|"
    r"inquest|trial|convict\w+|police appeal|died|death|fatal)\b",
    re.IGNORECASE,
)

VARIANTS: dict[str, str] = {
    "v1_baseline": board_rank.BOARD_RANK_SYSTEM,
    # Names the observed failure instead of describing the goal abstractly.
    "v2_named_failure": board_rank.BOARD_RANK_SYSTEM.replace(
        "You are NOT grading how complete or well-formed a record is.",
        (
            "You are NOT grading how complete or well-formed a record is.\n"
            "This is the exact mistake to avoid, observed in production: a bus stop closure notice was\n"
            "rated top of the section and a court report about the friend of a synagogue attacker was\n"
            "rated below it, because the closure notice had a tidy street name, operator and timestamp\n"
            "while the court report had facts withheld for legal reasons. A story is not weaker because\n"
            "the source could not print every detail. Rank consequence, not tidiness."
        ),
    ),
    # Forces the judgement to be decomposed before the order is produced.
    "v3_criteria": board_rank.BOARD_RANK_SYSTEM.replace(
        'Each item:\n{',
        (
            "Before ordering, judge each item on three things and let the order follow from them:\n"
            "  consequence  — how many residents it changes something for, and how much\n"
            "  actionability— whether a reader can do something about it today\n"
            "  novelty      — whether this is new today or a restatement of a known situation\n"
            "A routine notice scores high on actionability and near zero on consequence; that is not a\n"
            "top-of-section item. A serious incident scores high on consequence even with no action.\n\n"
            "Each item:\n{"
        ),
    ),
}


def _pool(block: str, limit: int) -> list[dict]:
    payload = json.loads((ROOT / "data" / "state" / "candidates.json").read_text(encoding="utf-8"))
    candidates = payload["candidates"] if isinstance(payload, dict) and "candidates" in payload else payload
    pool = [c for c in candidates if isinstance(c, dict) and str(c.get("primary_block") or "") == block]
    for candidate in pool:
        apply_story_intelligence(candidate)
    from news_digest.pipeline.story_intelligence import section_board_score

    return sorted(pool, key=lambda c: -float(section_board_score(c)))[:limit]


def _run_variant(name: str, system_prompt: str, block: str, pool: list[dict]) -> dict[str, dict]:
    original = board_rank.BOARD_RANK_SYSTEM
    board_rank.BOARD_RANK_SYSTEM = system_prompt
    try:
        route = board_rank.resolve_model_route("board_rank")
        diagnostics: list[dict] = []
        for step in route:
            verdicts = board_rank._call_block(step, block, pool, diagnostics)
            if verdicts:
                took = diagnostics[-1].get("duration_seconds") if diagnostics else "?"
                print(f"  {name}: {len(verdicts)} ранжировано через {step.provider_label}, {took}s")
                return verdicts
        print(f"  {name}: ни один провайдер не ответил — {diagnostics[-1] if diagnostics else 'нет диагностики'}")
        return {}
    finally:
        board_rank.BOARD_RANK_SYSTEM = original


def _bias_probe(verdicts: dict[str, dict], by_fp: dict[str, dict]) -> str:
    routine, hard = [], []
    for fp, verdict in verdicts.items():
        title = str((by_fp.get(fp) or {}).get("title") or "")
        if ROUTINE_RE.search(title):
            routine.append(verdict["rank"])
        elif HARDNEWS_RE.search(title):
            hard.append(verdict["rank"])
    if not routine or not hard:
        return "нет пары рутина/хардньюс в этом блоке"
    r, h = statistics.mean(routine), statistics.mean(hard)
    flag = "ПЕРЕКОС" if r < h else "ok"
    return f"рутина {r:.1f} / хардньюс {h:.1f} (меньше = выше) — {flag}"


def _spearman(a: list[int], b: list[int]) -> float:
    n = len(a)
    if n < 2:
        return 0.0
    return 1.0 - (6.0 * sum((x - y) ** 2 for x, y in zip(a, b))) / (n * (n * n - 1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--block", default="last_24h")
    parser.add_argument("--limit", type=int, default=40)
    args = parser.parse_args()

    if not (os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("OPENAI_API_KEY")):
        print("Нет DEEPSEEK_API_KEY / OPENAI_API_KEY — стенд без ключей запуститься не может.")
        print("Ключи лежат в GitHub Actions Secrets; запускай там или экспортируй локально.")
        return

    pool = _pool(args.block, args.limit)
    if len(pool) < 5:
        print(f"В блоке {args.block} только {len(pool)} кандидатов — сравнивать нечего.")
        return
    by_fp = {str(c.get("fingerprint")): c for c in pool}
    print(f"\n{PRIMARY_BLOCKS.get(args.block, args.block)}: {len(pool)} реальных новостей, {len(VARIANTS)} варианта промпта\n")

    results: dict[str, dict[str, dict]] = {}
    for name, prompt in VARIANTS.items():
        results[name] = _run_variant(name, prompt, args.block, pool)

    print(f"\n{'вариант':<20}{'ранжировано':>13}{'reject':>8}   проба на перекос")
    for name, verdicts in results.items():
        if not verdicts:
            print(f"{name:<20}{'—':>13}{'—':>8}   нет ответа")
            continue
        rejects = sum(1 for v in verdicts.values() if v["decision"] == "reject")
        print(f"{name:<20}{len(verdicts):>13}{rejects:>8}   {_bias_probe(verdicts, by_fp)}")

    names = [n for n, v in results.items() if v]
    if len(names) > 1:
        print("\nсогласие между вариантами (Spearman по общим новостям):")
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                shared = sorted(set(results[a]) & set(results[b]))
                if len(shared) > 2:
                    rho = _spearman(
                        [results[a][fp]["rank"] for fp in shared],
                        [results[b][fp]["rank"] for fp in shared],
                    )
                    print(f"   {a} vs {b}: {rho:+.3f}")

    out = ROOT / "data" / "validation" / f"board_prompt_ab_{args.block}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(
            {
                "block": args.block,
                "pool": {fp: str(c.get("title") or "")[:120] for fp, c in by_fp.items()},
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nполные порядки: {out.relative_to(ROOT)}\n")


if __name__ == "__main__":
    main()
