"""Contract of the editorial board judge.

One test class, three behaviours that the rest of the pipeline depends on:
ranks are re-derived so a block is always 1..N, the score is relative to the
block, and a reject only removes an item when the guards allow it.
"""
from __future__ import annotations

import json
import unittest

from news_digest.pipeline.board_rank import (
    JUDGED_BLOCKS,
    _parse_board_rank_results,
    apply_board_rank,
    board_rank_bonus,
    board_reject_verdict,
    judged_block,
    lead_candidate_pool,
)
from news_digest.pipeline.writer import _section_priority_score


class BoardRankContractTests(unittest.TestCase):
    def test_ranks_are_renumbered_and_scored_relative_to_the_block(self) -> None:
        expected = {"fp-a": {"title": "A"}, "fp-b": {"title": "B"}, "fp-c": {"title": "C"}}
        raw = json.dumps(
            {
                "items": [
                    {"fingerprint": "fp-b", "rank": 4, "decision": "publish", "confidence": 0.9},
                    {"fingerprint": "fp-a", "rank": 1, "decision": "publish", "confidence": 0.8},
                    {"fingerprint": "fp-c", "rank": 9, "decision": "backup", "confidence": 0.5},
                ]
            }
        )

        verdicts, diagnostic = _parse_board_rank_results(raw, expected, "last_24h")

        self.assertEqual(diagnostic["accepted"], 3)
        # Model returned 1/4/9 — the block still comes out contiguous 1..3.
        self.assertEqual([verdicts[fp]["rank"] for fp in ("fp-a", "fp-b", "fp-c")], [1, 2, 3])
        self.assertEqual(verdicts["fp-a"]["score"], 100.0)
        self.assertEqual(verdicts["fp-c"]["score"], 0.0)
        # Top of the block pushes up, bottom pushes down, symmetric around zero.
        self.assertEqual(board_rank_bonus({"board_rank_score": 100.0}), 25.0)
        self.assertEqual(board_rank_bonus({"board_rank_score": 0.0}), -25.0)
        # Anything the board never judged is untouched.
        self.assertEqual(board_rank_bonus({}), 0.0)

    def test_reject_is_executed_only_when_every_guard_allows_it(self) -> None:
        confident = {"board_decision": "reject", "board_confidence": 0.9}
        self.assertEqual(board_reject_verdict(confident), (True, "board_reject"))

        unsure = {"board_decision": "reject", "board_confidence": 0.2}
        self.assertFalse(board_reject_verdict(unsure)[0])

        duplicate = {
            "board_decision": "reject",
            "board_confidence": 0.1,
            "board_duplicate_of": "stronger-fire",
        }
        self.assertEqual(board_reject_verdict(duplicate), (True, "board_duplicate"))

    def test_only_judgement_blocks_reach_the_model(self) -> None:
        self.assertEqual(judged_block({"primary_block": "last_24h"}), "last_24h")
        # Tickets rank by Wikidata notability and weekend by coverage — no judge.
        self.assertEqual(judged_block({"primary_block": "ticket_radar"}), "")
        self.assertEqual(judged_block({"primary_block": "weekend_activities"}), "")
        self.assertEqual(judged_block({"primary_block": "transport"}), "")
        self.assertEqual(judged_block({"primary_block": "ticket_radar", "is_lead": True}), "")
        self.assertIn("last_24h", JUDGED_BLOCKS)

    def test_duplicate_is_structured_and_can_never_be_backup(self) -> None:
        expected = {"fire-a": {"title": "Fire A"}, "fire-b": {"title": "Fire B"}}
        raw = json.dumps({
            "items": [
                {"fingerprint": "fire-a", "rank": 1, "decision": "publish", "confidence": 0.9},
                {
                    "fingerprint": "fire-b",
                    "rank": 2,
                    "decision": "backup",
                    "duplicate_of": "fire-a",
                    "confidence": 0.7,
                },
            ]
        })
        verdicts, _ = _parse_board_rank_results(raw, expected, "last_24h")
        self.assertEqual(verdicts["fire-b"]["decision"], "reject")
        self.assertEqual(verdicts["fire-b"]["duplicate_of"], "fire-a")

    def test_lead_board_compares_six_real_news_candidates(self) -> None:
        candidates = [
            {
                "fingerprint": f"fp-{index}",
                "include": True,
                "primary_block": ("today_focus", "last_24h", "city_watch")[index % 3],
                "title": f"Story {index}",
                "summary": "A concrete Greater Manchester news event with public consequence.",
            }
            for index in range(8)
        ]
        candidates.append({
            "fingerprint": "ticket",
            "include": True,
            "primary_block": "ticket_radar",
            "title": "Arena show",
        })
        pool = lead_candidate_pool(candidates)
        self.assertEqual(len(pool), 6)
        self.assertNotIn("ticket", {candidate["fingerprint"] for candidate in pool})

    def test_july_27_blind_pool_contract(self) -> None:
        """One fire, useful GMP failure above filler, weak IT rejected."""
        candidates = [
            {
                "fingerprint": "about-fire",
                "title": "Major fire in Greater Manchester",
                "primary_block": "last_24h",
                "include": True,
                "reader_value_score": 20,
            },
            {
                "fingerprint": "men-fire",
                "title": "Fire crews tackle the same Greater Manchester blaze",
                "primary_block": "last_24h",
                "include": True,
                "reader_value_score": 95,
            },
            {
                "fingerprint": "gmp-it-failure",
                "title": "GMP IT failure disrupts a public service",
                "primary_block": "last_24h",
                "include": True,
                "reader_value_score": 15,
            },
            {
                "fingerprint": "secondary",
                "title": "Secondary local story",
                "primary_block": "last_24h",
                "include": True,
                "reader_value_score": 90,
            },
            {
                "fingerprint": "weak-it",
                "title": "Weak generic IT item",
                "primary_block": "last_24h",
                "include": True,
                "reader_value_score": 100,
            },
        ]
        expected = {candidate["fingerprint"]: candidate for candidate in candidates}
        raw = json.dumps({
            "items": [
                {"fingerprint": "about-fire", "rank": 1, "decision": "publish", "confidence": 0.95},
                {
                    "fingerprint": "men-fire",
                    "rank": 2,
                    "decision": "backup",
                    "duplicate_of": "about-fire",
                    "confidence": 0.8,
                },
                {"fingerprint": "gmp-it-failure", "rank": 2, "decision": "publish", "confidence": 0.9},
                {"fingerprint": "secondary", "rank": 3, "decision": "publish", "confidence": 0.8},
                {"fingerprint": "weak-it", "rank": 4, "decision": "reject", "confidence": 0.9},
            ]
        })
        verdicts, _ = _parse_board_rank_results(raw, expected, "last_24h")
        apply_board_rank(candidates, verdicts)

        self.assertTrue(board_reject_verdict(expected["men-fire"])[0])
        self.assertEqual(expected["men-fire"]["board_decision"], "reject")
        self.assertTrue(board_reject_verdict(expected["weak-it"])[0])
        self.assertGreater(
            _section_priority_score(expected["gmp-it-failure"], "Свежие новости", ""),
            _section_priority_score(expected["secondary"], "Свежие новости", ""),
        )


if __name__ == "__main__":
    unittest.main()
