from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from news_digest.pipeline import cost_tracker
from news_digest.pipeline import llm_rewrite
from news_digest.pipeline.llm_rewrite import _apply_rewrite_shortlist
from news_digest.pipeline import release
from news_digest.pipeline.model_routing import resolve_model_route, route_snapshot
from news_digest.pipeline.prompts_meta import prompt_name_for, snapshot, validate_registry


class PromptVersioningTest(unittest.TestCase):
    def test_prompt_registry_is_valid_and_unique(self) -> None:
        prompts = snapshot()
        names = [p["name"] for p in prompts]

        self.assertTrue(prompts)
        self.assertEqual(validate_registry(), [])
        self.assertEqual(len(names), len(set(names)))
        self.assertTrue(all(p["version"].startswith("v") for p in prompts))
        self.assertTrue(all(len(p["hash"]) == 8 for p in prompts))
        self.assertTrue(
            all(p["tag"] == f"{p['name']}@{p['version']}+{p['hash']}" for p in prompts)
        )

    def test_cost_records_carry_prompt_version_tag(self) -> None:
        cost_tracker.reset()
        cost_tracker.record_call(
            stage="curator",
            provider="OpenAI",
            model="gpt-4o-mini",
            prompt_name="curator",
            prompt_tokens=100,
            completion_tokens=20,
        )

        records = cost_tracker.snapshot()
        self.assertEqual(len(records), 1)
        self.assertTrue(records[0].prompt_version.startswith("curator@"))
        summary = cost_tracker.summarise(records)
        self.assertIn(records[0].prompt_version, summary["by_prompt"])
        self.assertIn("total_estimated_cost_usd", summary)
        self.assertIn("estimated_prompt_tokens", summary["by_provider"]["OpenAI"])

    def test_estimated_cost_records_when_usage_is_missing(self) -> None:
        cost_tracker.reset()
        response = type("Response", (), {"usage": None})()
        cost_tracker.record_call_from_response(
            response=response,
            stage="llm_rewrite",
            provider="OpenAI",
            model="gpt-4o-mini",
            prompt_name="events",
            messages=[
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "candidate payload"},
            ],
            max_tokens=1000,
        )

        [record] = cost_tracker.snapshot()
        self.assertEqual(record.usage_source, "estimated")
        self.assertGreater(record.estimated_prompt_tokens, 0)
        self.assertEqual(record.estimated_completion_tokens, 1000)
        self.assertGreater(record.estimated_cost_usd, 0)

    def test_model_routing_policy_separates_scoring_and_rewrite(self) -> None:
        routes = route_snapshot()

        self.assertEqual(routes["curator"][0]["role"], "curator_mini_primary")
        self.assertEqual(routes["curator"][0]["model"], "gpt-4o-mini")
        self.assertEqual(routes["dedupe_review"][0]["role"], "cheap_scoring")
        self.assertEqual(routes["rewrite"][0]["role"], "mini_rewrite_primary")
        self.assertEqual(routes["rewrite"][0]["provider_label"], "OpenAI")
        self.assertEqual(routes["rewrite"][0]["model"], "gpt-4o-mini")
        self.assertEqual(len(routes["rewrite"]), 1)
        self.assertEqual(routes["english_cards"][0]["role"], "board_ranker_deepseek_pro_primary")
        self.assertEqual(routes["english_cards"][0]["provider_label"], "DeepSeek")
        self.assertEqual(routes["english_cards"][0]["model"], "deepseek-v4-pro")
        self.assertEqual(routes["english_cards"][0]["batch_size"], 6)
        self.assertEqual(routes["english_cards"][1]["role"], "board_judge_mini_reserve")
        self.assertEqual(routes["english_cards"][1]["model"], "gpt-4o-mini")
        self.assertEqual(routes["english_cards"][2]["role"], "lead_only_board_fallback")
        self.assertEqual(routes["english_cards"][2]["model"], "gpt-4o")
        self.assertEqual(routes["final_translate"][0]["role"], "direct_ru_writer_mini_primary")
        self.assertEqual(routes["final_translate"][1]["role"], "direct_ru_writer_independent_fallback")
        self.assertEqual(routes["final_translate"][1]["provider_label"], "DeepSeek")
        self.assertEqual(routes["final_translate"][1]["model"], "deepseek-v4-pro")
        self.assertEqual(routes["final_translate"][2]["role"], "lead_only_direct_ru_fallback")
        self.assertEqual(routes["events_rewrite"][0]["provider_label"], "OpenAI")
        self.assertEqual(routes["events_rewrite"][0]["batch_size"], 5)
        self.assertEqual(len(routes["events_rewrite"]), 1)
        self.assertEqual(routes["repair"][0]["role"], "hard_defect_repair_mini")
        self.assertEqual(routes["repair"][0]["provider_label"], "OpenAI")
        self.assertEqual(routes["repair"][1]["role"], "lead_only_repair_fallback")
        self.assertEqual(routes["pre_send_quality"][0]["role"], "whole_digest_strong_editor")
        self.assertEqual(routes["pre_send_quality"][0]["model"], "gpt-4o")

    def test_model_route_override_uses_manual_single_step(self) -> None:
        route = resolve_model_route(
            "rewrite",
            provider_override="openai",
            base_url_override="https://example.test/v1",
            model_override="gpt-test",
        )

        self.assertEqual(len(route), 1)
        self.assertEqual(route[0].role, "manual_override")
        self.assertEqual(route[0].model, "gpt-test")

    def test_rewrite_shortlist_holds_low_ranked_non_ticket_candidates_in_backup(self) -> None:
        candidates = [
            {
                "fingerprint": f"city-{idx}",
                "title": f"City item {idx}",
                "summary": "A local business update in Greater Manchester.",
                "source_label": "Local Source",
                "category": "tech_business",
                "primary_block": "tech_business",
                "include": True,
                "reader_value_score": 100 - idx,
                "section_board_score": 100 - idx,
            }
            for idx in range(12)
        ]

        selected, report = _apply_rewrite_shortlist(candidates, candidates)

        # tech_business recall cap raised to 10 (2b): the model now sees the
        # realistic competition, only the lowest-ranked tail is held in backup.
        self.assertEqual(report["selected_for_rewrite"], 10)
        self.assertEqual(report["held_for_backup"], 2)
        self.assertEqual(len(selected), 10)
        self.assertTrue(all(c["include"] for c in selected))
        held = [c for c in candidates if c.get("rewrite_shortlist_status") == "backup_before_rewrite"]
        self.assertEqual(len(held), 2)
        self.assertTrue(all(c["backup_candidate"] for c in held))
        self.assertTrue(all(not c["include"] for c in held))

    def test_today_practical_reserve_survives_rewrite_shortlist_cap(self) -> None:
        practical = {
            "fingerprint": "m60-prestwich",
            "title": "M60 Prestwich traffic RECAP: Major motorway shut due to police incident",
            "summary": "The M60 at Prestwich has been shut today, with congestion and delays for drivers.",
            "practical_angle": "Drivers should check routes today and allow extra time.",
            "source_label": "MEN",
            "category": "media_layer",
            "primary_block": "last_24h",
            "include": True,
            "reader_value_score": 1,
            "section_board_score": 1,
        }
        crowded = [
            {
                "fingerprint": f"fresh-{idx}",
                "title": f"Fresh council update {idx}",
                "summary": "A council update in Greater Manchester.",
                "source_label": "Local Source",
                "category": "media_layer",
                "primary_block": "last_24h",
                "include": True,
                "reader_value_score": 200 - idx,
                "section_board_score": 200 - idx,
            }
            for idx in range(20)
        ]

        selected, report = _apply_rewrite_shortlist([practical] + crowded, [practical] + crowded)

        self.assertIn(practical, selected)
        self.assertTrue(practical.get("today_practical_translation_reserve"))
        self.assertEqual(practical["rewrite_shortlist_status"], "selected_uncapped")
        self.assertEqual(report["today_practical_translation_reserve"][0]["fingerprint"], "m60-prestwich")

    def test_prompt_lookup_ignores_runtime_date_header(self) -> None:
        prompt = "TODAY_DATE=2026-05-18\n\n" + llm_rewrite.PROMPT_EVENTS

        self.assertEqual(prompt_name_for(prompt), "events")

    def test_prompt_drift_detects_hash_change_without_version_bump(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            release.write_json(
                state_dir / "cost_history.json",
                [
                    {
                        "run_date_london": "2026-05-17",
                        "prompt_versions": [
                            {"name": "curator", "version": "v3", "hash": "aaaaaaaa"},
                        ],
                    }
                ],
            )
            with patch.object(
                release,
                "_prompts_snapshot",
                return_value=[{"name": "curator", "version": "v3", "hash": "bbbbbbbb"}],
            ):
                self.assertEqual(
                    release._detect_prompt_drift(None, None, state_dir),
                    [
                        {
                            "name": "curator",
                            "version": "v3",
                            "old_hash": "aaaaaaaa",
                            "new_hash": "bbbbbbbb",
                        }
                    ],
                )

    def test_prompt_drift_allows_version_bump(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            release.write_json(
                state_dir / "cost_history.json",
                [
                    {
                        "run_date_london": "2026-05-17",
                        "prompt_versions": [
                            {"name": "curator", "version": "v3", "hash": "aaaaaaaa"},
                        ],
                    }
                ],
            )
            with patch.object(
                release,
                "_prompts_snapshot",
                return_value=[{"name": "curator", "version": "v4", "hash": "bbbbbbbb"}],
            ):
                self.assertEqual(release._detect_prompt_drift(None, None, state_dir), [])

    def test_outgoing_metadata_contains_prompt_versions(self) -> None:
        with TemporaryDirectory() as tmp:
            metadata_path = Path(tmp) / "data" / "outgoing" / "current_digest.meta.json"
            metadata_path.parent.mkdir(parents=True)
            prompt_versions = [{"name": "curator", "version": "v3", "hash": "aaaaaaaa"}]

            release._write_outgoing_metadata(
                metadata_path,
                report_payload={
                    "release_gate_version": 3,
                    "pipeline_run_id": "run-1",
                    "run_at_london": "2026-05-18T08:00:00+01:00",
                    "run_date_london": "2026-05-18",
                    "release_decision": "pass",
                    "output_path": "/tmp/current_digest.html",
                    "model_routing_policy": {"rewrite": [{"role": "quality_rewrite"}]},
                    "prompt_drift": [],
                    "cost_summary": {"total_calls": 1},
                },
                prompt_versions=prompt_versions,
            )

            payload = release.read_json(metadata_path)
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["release_decision"], "pass")
            self.assertEqual(payload["prompt_versions"], prompt_versions)
            self.assertEqual(payload["model_routing_policy"]["rewrite"][0]["role"], "quality_rewrite")

    def test_source_health_reports_o1_counts(self) -> None:
        summary = release._summarise_source_health(
            {
                "categories": {
                    "transport": {
                        "source_health": [
                            {
                                "name": "TfGM",
                                "fetched": True,
                                "candidate_count": 3,
                                "publishable_count": 2,
                                "fresh_last_24h_count": 2,
                                "errors": [],
                                "warnings": [],
                            }
                        ]
                    },
                    "city_news": {
                        "source_health": [
                            {
                                "name": "MEN",
                                "fetched": False,
                                "candidate_count": 0,
                                "publishable_count": 0,
                                "fresh_last_24h_count": 0,
                                "errors": ["timeout"],
                                "warnings": [],
                            }
                        ]
                    },
                }
            },
            {
                "candidates": [
                    {"source_label": "TfGM", "fingerprint": "a", "include": True},
                    {"source_label": "TfGM", "fingerprint": "b", "include": True},
                    {"source_label": "TfGM", "fingerprint": "c", "include": False},
                ]
            },
            {"rendered_candidate_fingerprints": ["a"]},
        )

        [tfgm] = [src for src in summary["sources"] if src["name"] == "TfGM"]
        self.assertEqual(tfgm["raw_count"], 3)
        self.assertEqual(tfgm["accepted_count"], 2)
        self.assertEqual(tfgm["rejected_count"], 1)
        self.assertEqual(tfgm["rendered_count"], 1)
        self.assertEqual(tfgm["failure_count"], 0)

        [men] = [src for src in summary["sources"] if src["name"] == "MEN"]
        self.assertEqual(men["status"], "failed")
        self.assertEqual(men["failure_count"], 1)


if __name__ == "__main__":
    unittest.main()
