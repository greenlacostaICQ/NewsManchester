"""Contract of the editorial board judge.

One test class, three behaviours that the rest of the pipeline depends on:
ranks are re-derived so a block is always 1..N, the score is relative to the
block, and a reject only removes an item when the guards allow it.
"""
from __future__ import annotations

import json
import sys
import unittest
from types import SimpleNamespace
from unittest import mock

from news_digest.pipeline import provider_health
from news_digest.pipeline.board_rank import (
    JUDGED_BLOCKS,
    _call_block,
    _parse_board_rank_results,
    apply_board_rank,
    board_rank_bonus,
    board_reject_verdict,
    judged_block,
    lead_candidate_pool,
    rank_boards,
)
from news_digest.pipeline.writer import _section_priority_score


class BoardRankContractTests(unittest.TestCase):
    def setUp(self) -> None:
        provider_health.reset()

    def tearDown(self) -> None:
        provider_health.reset()

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

    def test_partial_listwise_response_is_rejected_atomically(self) -> None:
        expected = {
            "fp-a": {"title": "A"},
            "fp-b": {"title": "B"},
            "fp-c": {"title": "C"},
        }
        raw = json.dumps(
            {
                "items": [
                    {"fingerprint": "fp-a", "rank": 1, "decision": "publish"},
                    {"fingerprint": "fp-b", "rank": 2, "decision": "backup"},
                ]
            }
        )

        verdicts, diagnostic = _parse_board_rank_results(raw, expected, "last_24h")

        self.assertEqual(verdicts, {})
        self.assertEqual(diagnostic["accepted"], 2)
        self.assertEqual(diagnostic["atomic_rejection"], "incomplete_candidate_set")
        self.assertEqual(
            [row["fingerprint"] for row in diagnostic["missing_candidates"]],
            ["fp-c"],
        )

    def test_call_block_reports_http_200_contract_rejection_as_non_transport_failure(self) -> None:
        candidates = [
            {"fingerprint": "fp-a", "title": "A", "summary": "Story A"},
            {"fingerprint": "fp-b", "title": "B", "summary": "Story B"},
        ]
        raw = json.dumps({
            "items": [
                {"fingerprint": "fp-a", "rank": 1, "decision": "publish", "confidence": 0.9},
            ]
        })
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=raw))]
        )
        completions = SimpleNamespace(create=mock.Mock(return_value=response))
        client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
        fake_openai = SimpleNamespace(OpenAI=mock.Mock(return_value=client))
        step = SimpleNamespace(
            provider="deepseek",
            provider_label="DeepSeek",
            model="deepseek-v4-pro",
            api_key="key",
            base_url="https://example.test/v1",
            timeout_seconds=60,
        )
        diagnostics = []

        with mock.patch.dict(sys.modules, {"openai": fake_openai}), mock.patch(
            "news_digest.pipeline.llm_rewrite._API_RATE_LIMITER.acquire",
        ), mock.patch(
            "news_digest.pipeline.llm_rewrite._API_TOKEN_LIMITER.acquire",
        ), mock.patch(
            "news_digest.pipeline.cost_tracker.record_call_from_response",
        ), self.assertLogs("news_digest.pipeline.board_rank", level="WARNING") as captured:
            result = _call_block(step, "last_24h", candidates, diagnostics)

        self.assertEqual(result, {})
        self.assertEqual(diagnostics[0]["atomic_rejection"], "incomplete_candidate_set")
        self.assertIn("reason=incomplete_candidate_set accepted=1/2", "\n".join(captured.output))

    def test_rejected_http_responses_fall_back_without_killing_provider(self) -> None:
        candidates = [
            {
                "fingerprint": "fp-a",
                "include": True,
                "primary_block": "last_24h",
                "title": "A concrete Greater Manchester story",
                "summary": "Public consequence for residents.",
            }
        ]
        route = [
            SimpleNamespace(provider="deepseek", provider_label="DeepSeek", model="deepseek", api_key="key"),
            SimpleNamespace(provider="openai", provider_label="OpenAI", model="mini", api_key="key"),
        ]

        def fake_call(step, block, pool, diagnostics):
            if step.provider == "deepseek":
                diagnostics.append({
                    "block": block,
                    "provider": "DeepSeek",
                    "sent": len(pool),
                    "accepted": 0,
                    "atomic_rejection": "incomplete_candidate_set",
                })
                return {}
            return {
                row["fingerprint"]: {
                    "rank": index,
                    "rank_total": len(pool),
                    "score": 100.0,
                    "decision": "publish",
                    "confidence": 0.9,
                    "duplicate_of": "",
                    "why": "",
                }
                for index, row in enumerate(pool, start=1)
            }

        with mock.patch(
            "news_digest.pipeline.board_rank.resolve_model_route",
            return_value=route,
        ), mock.patch(
            "news_digest.pipeline.board_rank._call_block",
            side_effect=fake_call,
        ):
            verdicts, report = rank_boards(candidates)

        self.assertIn("fp-a", verdicts)
        self.assertTrue(report["enabled"])
        self.assertFalse(provider_health.is_dead("deepseek"))

    def test_two_transport_failures_still_trip_provider_breaker(self) -> None:
        candidates = [
            {
                "fingerprint": "fp-a",
                "include": True,
                "primary_block": "last_24h",
                "title": "A concrete Greater Manchester story",
                "summary": "Public consequence for residents.",
            }
        ]
        route = [
            SimpleNamespace(provider="deepseek", provider_label="DeepSeek", model="deepseek", api_key="key"),
            SimpleNamespace(provider="openai", provider_label="OpenAI", model="mini", api_key="key"),
        ]

        def fake_call(step, _block, pool, _diagnostics):
            if step.provider == "deepseek":
                return None
            return {
                row["fingerprint"]: {
                    "rank": index,
                    "rank_total": len(pool),
                    "score": 100.0,
                    "decision": "publish",
                    "confidence": 0.9,
                    "duplicate_of": "",
                    "why": "",
                }
                for index, row in enumerate(pool, start=1)
            }

        with mock.patch(
            "news_digest.pipeline.board_rank.resolve_model_route",
            return_value=route,
        ), mock.patch(
            "news_digest.pipeline.board_rank._call_block",
            side_effect=fake_call,
        ):
            verdicts, _ = rank_boards(candidates)

        self.assertIn("fp-a", verdicts)
        self.assertTrue(provider_health.is_dead("deepseek"))

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

    def test_structured_board_verdict_controls_duplicates_and_order(self) -> None:
        """Parser/application contract; production evaluates model judgement."""
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
