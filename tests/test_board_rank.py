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
    board_rank_bonus,
    board_reject_verdict,
    judged_block,
)


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

        protected = {
            "board_decision": "reject",
            "board_confidence": 0.99,
            "protected_lane": {"protected": True, "lanes": ["transport"]},
        }
        self.assertEqual(board_reject_verdict(protected), (False, "protected_lane_overrides_board_reject"))

    def test_only_judgement_blocks_reach_the_model(self) -> None:
        self.assertEqual(judged_block({"primary_block": "last_24h"}), "last_24h")
        # Tickets rank by Wikidata notability and weekend by coverage — no judge.
        self.assertEqual(judged_block({"primary_block": "ticket_radar"}), "")
        self.assertEqual(judged_block({"primary_block": "weekend_activities"}), "")
        self.assertEqual(judged_block({"primary_block": "transport"}), "")
        self.assertEqual(judged_block({"primary_block": "ticket_radar", "is_lead": True}), "lead_story")
        self.assertIn("last_24h", JUDGED_BLOCKS)


if __name__ == "__main__":
    unittest.main()
