from __future__ import annotations

import unittest

from news_digest.pipeline.collector.core import _build_source_health_report
from news_digest.pipeline.llm_rewrite import _build_publish_plan
from news_digest.pipeline.writer import (
    _apply_publish_plan_to_candidates,
    _build_publish_plan_contract_report,
    _is_public_budget_exempt,
)


class SourceHealthAndPublishPlanTests(unittest.TestCase):
    def test_source_health_marks_empty_parser_as_actionable(self) -> None:
        report = {
            "pipeline_run_id": "run-1",
            "run_date_london": "2026-06-24",
            "categories": {
                "culture_weekly": {
                    "source_health": [
                        {
                            "name": "Manchester Theatres Weekend",
                            "url": "https://example.test/weekend",
                            "fetched": True,
                            "candidate_count": 0,
                            "failure_class": "parser_or_filter_empty",
                            "recommended_next_action": "inspect HTML/feed shape",
                            "warnings": ["fetched successfully but no candidate links passed filters"],
                        }
                    ]
                },
                "tech_business": {
                    "source_health": [
                        {
                            "name": "BusinessLive Greater Manchester",
                            "url": "https://example.test/dead",
                            "fetched": False,
                            "candidate_count": 0,
                            "failure_class": "source_url_dead",
                            "recommended_next_action": "replace or disable dead URL",
                            "errors": ["HTTP Error 404: Not Found"],
                        }
                    ]
                },
            },
        }

        health = _build_source_health_report(report)

        self.assertEqual(health["counts"]["empty_parser"], 1)
        self.assertEqual(health["counts"]["failed_fetch"], 1)
        self.assertEqual(health["counts"]["dead_url"], 1)
        self.assertEqual(health["counts"]["needs_action"], 2)
        self.assertEqual(health["dead_parser_today"][0]["name"], "Manchester Theatres Weekend")

    def test_publish_plan_promotes_protected_selected_items_to_must_show(self) -> None:
        candidates = [
            {
                "fingerprint": "lead",
                "title": "Lead story",
                "primary_block": "last_24h",
                "category": "media_layer",
                "source_label": "MEN",
                "digest_selection_verdict": "selected",
                "is_lead": True,
                "include": True,
            },
            {
                "fingerprint": "normal",
                "title": "Normal story",
                "primary_block": "city_watch",
                "category": "media_layer",
                "source_label": "BBC",
                "digest_selection_verdict": "selected",
                "include": True,
            },
            {
                "fingerprint": "thin",
                "title": "Thin event",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_label": "Venue",
                "digest_selection_verdict": "needs_enrichment",
                "include": False,
            },
        ]

        plan = _build_publish_plan(candidates)

        statuses = {item["fingerprint"]: item["status"] for item in plan["items"]}
        self.assertEqual(statuses["lead"], "must_show")
        self.assertEqual(statuses["normal"], "show")
        self.assertEqual(statuses["thin"], "needs_enrichment")

    def test_writer_applies_must_show_contract_and_reports_missing(self) -> None:
        candidates = [
            {
                "fingerprint": "lead",
                "title": "Lead story",
                "primary_block": "last_24h",
                "category": "media_layer",
                "source_label": "MEN",
                "include": True,
                "is_lead": True,
            }
        ]
        plan = {
            "items": [
                {
                    "fingerprint": "lead",
                    "status": "must_show",
                    "reason": "Curator lead.",
                    "budget_bucket": "core_news",
                }
            ]
        }

        application = _apply_publish_plan_to_candidates(candidates, plan)
        self.assertEqual(application["must_show_total"], 1)
        self.assertEqual(candidates[0]["manual_override"], "force_include")
        self.assertTrue(_is_public_budget_exempt("Свежие новости", candidates[0]))

        contract = _build_publish_plan_contract_report(
            candidates=candidates,
            rendered_fp_set=set(),
            dropped_candidates=[
                {
                    "fingerprint": "lead",
                    "reasons": ["draft_line for long-format category needs >=150 chars"],
                }
            ],
            global_budget_dropped=[],
            degraded_shrink_dropped=[],
            publish_plan_application=application,
        )

        self.assertEqual(contract["counts"]["must_show_total"], 1)
        self.assertEqual(contract["counts"]["must_show_missing"], 1)
        self.assertEqual(contract["missing_must_show"][0]["fingerprint"], "lead")


if __name__ == "__main__":
    unittest.main()
