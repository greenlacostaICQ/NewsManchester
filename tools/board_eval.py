#!/usr/bin/env python3
"""Blind evaluation of the three ways to order a block.

Why not compare against what actually shipped: the issue was ordered by the
formula, so agreement with it measures similarity to the current system, not
quality. The only honest baseline is a human ranking made without seeing any of
the machine scores.

Usage:

    # 1. Dump today's full pre-cut pool for a block, in random order, no scores.
    PYTHONPATH=src python3 tools/board_eval.py dump --block last_24h

    # 2. Rank the printed lines yourself, best first, and save the ids:
    #    data/validation/board_gold/<date>_<block>.json  ->  {"order": ["a3", "b7", ...]}

    # 3. Score every method against your ranking.
    PYTHONPATH=src python3 tools/board_eval.py score --block last_24h

`score` reports Spearman correlation over the whole list plus precision@k for
the slots the section actually has — the top of the list is what readers see,
so a method that gets the head right matters more than one that sorts the tail.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from news_digest.pipeline.board_rank import board_rank_bonus  # noqa: E402
from news_digest.pipeline.common import PRIMARY_BLOCKS, SECTION_MAX_ITEMS, today_london  # noqa: E402
from news_digest.pipeline.story_intelligence import apply_story_intelligence, section_board_score  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = ROOT / "data" / "validation" / "board_gold"


def _pool(block: str) -> list[dict]:
    """Every candidate of the block that was eligible before any cut."""
    payload = json.loads((ROOT / "data" / "state" / "candidates.json").read_text(encoding="utf-8"))
    candidates = payload["candidates"] if isinstance(payload, dict) and "candidates" in payload else payload
    pool = [
        c
        for c in candidates
        if isinstance(c, dict) and str(c.get("primary_block") or "") == block and not c.get("dropped")
    ]
    for candidate in pool:
        apply_story_intelligence(candidate)
    return pool


def _short_id(candidate: dict, index: int) -> str:
    return f"{chr(ord('a') + index // 10)}{index % 10}"


def cmd_dump(block: str) -> None:
    pool = _pool(block)
    if not pool:
        print(f"Пул блока {block} пуст — нечего оценивать.")
        return
    indexed = [(_short_id(c, i), c) for i, c in enumerate(pool)]
    shuffled = list(indexed)
    random.shuffle(shuffled)

    date = today_london()
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    (GOLD_DIR / f"{date}_{block}.pool.json").write_text(
        json.dumps(
            {"block": block, "date": date, "items": {sid: c.get("fingerprint") for sid, c in indexed}},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    section = PRIMARY_BLOCKS.get(block, block)
    print(f"\n{section} — {len(pool)} кандидатов, мест в выпуске: {SECTION_MAX_ITEMS.get(section, '—')}")
    print("Ни баллов, ни вердиктов ниже нет — расставь сам, лучшее первым.\n")
    for sid, candidate in shuffled:
        title = str(candidate.get("title") or "").strip()[:110]
        source = str(candidate.get("source_label") or "")[:24]
        print(f"  {sid}  {title}   [{source}]")
    print(f"\nСохрани порядок в {GOLD_DIR / f'{date}_{block}.json'} как {{\"order\": [\"a0\", ...]}}\n")


def _spearman(a: list[int], b: list[int]) -> float:
    n = len(a)
    if n < 2:
        return 0.0
    d2 = sum((x - y) ** 2 for x, y in zip(a, b))
    return 1.0 - (6.0 * d2) / (n * (n * n - 1))


def cmd_score(block: str, date: str) -> None:
    gold_path = GOLD_DIR / f"{date}_{block}.json"
    pool_path = GOLD_DIR / f"{date}_{block}.pool.json"
    if not gold_path.exists() or not pool_path.exists():
        print(f"Нет {gold_path.name} или {pool_path.name}. Сначала `dump`, потом расставь порядок.")
        return
    gold_order = json.loads(gold_path.read_text(encoding="utf-8"))["order"]
    id_to_fp = json.loads(pool_path.read_text(encoding="utf-8"))["items"]
    by_fp = {str(c.get("fingerprint")): c for c in _pool(block)}

    ranked = [(sid, by_fp.get(id_to_fp.get(sid) or "")) for sid in gold_order]
    ranked = [(sid, c) for sid, c in ranked if c]
    if len(ranked) < 3:
        print("Слишком мало совпадений с текущим пулом — оценивай в тот же день, что и dump.")
        return

    human_rank = {sid: i for i, (sid, _) in enumerate(ranked)}
    methods = {
        "формула": lambda c: section_board_score(c),
        "судья": lambda c: float(c.get("board_rank_score") or 0.0),
        "гибрид": lambda c: section_board_score(c) + board_rank_bonus(c),
    }
    section = PRIMARY_BLOCKS.get(block, block)
    slots = int(SECTION_MAX_ITEMS.get(section, 9) or 9)
    human_top = {sid for sid, _ in ranked[:slots]}

    print(f"\n{section}, {date} — {len(ranked)} новостей, слепой эталон против трёх способов\n")
    print(f"{'способ':<12}{'Spearman':>10}{f'  точность@{slots}':>16}")
    for name, key in methods.items():
        order = sorted(ranked, key=lambda pair: -float(key(pair[1])))
        machine = [human_rank[sid] for sid, _ in order]
        rho = _spearman(sorted(machine), machine)
        hit = len({sid for sid, _ in order[:slots]} & human_top) / max(1, min(slots, len(ranked)))
        print(f"{name:<12}{rho:>10.3f}{hit:>15.0%}")
    unjudged = sum(1 for _, c in ranked if c.get("board_rank_score") is None)
    if unjudged:
        print(f"\nБез вердикта судьи: {unjudged} из {len(ranked)} — их «судья» ставит в конец, это занижает его строку.")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("dump", "score"):
        p = sub.add_parser(name)
        p.add_argument("--block", default="last_24h")
        p.add_argument("--date", default=today_london())
    args = parser.parse_args()
    if args.cmd == "dump":
        cmd_dump(args.block)
    else:
        cmd_score(args.block, args.date)


if __name__ == "__main__":
    main()
