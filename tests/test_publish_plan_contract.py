from __future__ import annotations

import unittest

from news_digest.pipeline.collector.core import _build_source_health_report
from news_digest.pipeline.llm_rewrite import _build_publish_plan
from news_digest.pipeline.release import _repair_public_html_contracts, public_html_contract_errors
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

    def test_publish_plan_only_true_protected_blocks_become_must_show(self) -> None:
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
                "draft_line": "• Lead story has a ready public line.",
            },
            {
                "fingerprint": "normal",
                "title": "Normal story",
                "primary_block": "city_watch",
                "category": "media_layer",
                "source_label": "BBC",
                "digest_selection_verdict": "selected",
                "include": True,
                "draft_line": "• Normal story has a ready public line.",
            },
            {
                "fingerprint": "transport",
                "title": "Tram disruption",
                "primary_block": "transport",
                "category": "transport",
                "source_label": "TfGM",
                "digest_selection_verdict": "selected",
                "include": True,
                "draft_line": "• Tram disruption has a ready public line.",
            },
            {
                "fingerprint": "russian",
                "title": "Russian event",
                "primary_block": "russian_events",
                "category": "russian_speaking_events",
                "source_label": "MTicket",
                "digest_selection_verdict": "selected",
                "include": True,
                "draft_line": "• Russian event has a ready public line.",
            },
            {
                "fingerprint": "professional",
                "title": "Tech event",
                "primary_block": "professional_events",
                "category": "professional_events",
                "source_label": "CompiledMCR",
                "digest_selection_verdict": "selected",
                "include": True,
                # W6: a professional event is must_show only with a CV go/consider.
                "professional_llm_match": {"fit": "go"},
                "draft_line": "• Tech event has a ready public line.",
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
        self.assertEqual(statuses["transport"], "must_show")
        self.assertEqual(statuses["russian"], "must_show")
        self.assertEqual(statuses["professional"], "must_show")
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

    def test_outside_gm_is_not_blanket_budget_exempt(self) -> None:
        outside = {
            "fingerprint": "outside",
            "primary_block": "outside_gm_tickets",
            "publish_plan_status": "show",
        }
        russian = {
            "fingerprint": "russian",
            "primary_block": "russian_events",
            "publish_plan_status": "must_show",
            "publish_plan_must_show": True,
            "publish_plan_protected_budget": True,
        }

        self.assertFalse(_is_public_budget_exempt("Крупные концерты вне GM", outside))
        self.assertTrue(_is_public_budget_exempt("Русскоязычные концерты и стендап UK", russian))

    def test_outside_gm_a_tier_is_budget_exempt(self) -> None:
        outside_a_tier = {
            "fingerprint": "outside-a-tier",
            "primary_block": "outside_gm_tickets",
            "venue_scope": "outside",
            "ticket_notability": {"tier": "A"},
        }

        self.assertTrue(_is_public_budget_exempt("Крупные концерты вне GM", outside_a_tier))

    def test_dated_weekend_event_is_budget_exempt(self) -> None:
        # E4 (2026-06-30): «Выходные в GM» has no cap — a weekend event
        # confirmed by a trustworthy date survives the global budget; undated
        # bookable filler does not (it is dropped upstream by E2 anyway).
        # Use the upcoming Saturday relative to the run date so the fixture never
        # rots into the past (was a hardcoded 2026-07-04 time-bomb).
        from datetime import timedelta

        from news_digest.pipeline.common import now_london

        today = now_london().date()
        saturday = today + timedelta(days=(5 - today.weekday()) % 7)
        dated = {
            "fingerprint": "weekend-dated",
            "primary_block": "weekend_activities",
            "title": "Cheadle Makers Market",
            "summary": "Makers market with food and crafts this Saturday.",
            "source_label": "Pedddle Makers Market",
            "event": {"is_event": True, "date_start": saturday.isoformat(), "date_confidence": "high"},
        }
        undated = {
            "fingerprint": "weekend-undated",
            "primary_block": "weekend_activities",
            "event": {"is_event": True, "date_confidence": "none"},
        }
        self.assertTrue(_is_public_budget_exempt("Выходные в GM", dated))
        self.assertFalse(_is_public_budget_exempt("Выходные в GM", undated))

    def test_public_contract_repair_fixes_generic_cta_before_gate(self) -> None:
        html = (
            "<b>Greater Manchester Brief — 2026-06-25, 08:00</b>\n\n"
            "<b>Билеты / Ticket Radar</b>\n"
            "• Событие сегодня в Manchester Academy: проверьте время на странице события.\n"
        )

        self.assertIn("missing_event_time_cta", " ".join(public_html_contract_errors(html)))
        fixed, report = _repair_public_html_contracts(html)

        self.assertEqual(report["fixed_count"], 1)
        self.assertNotIn("проверьте время", fixed.lower())
        self.assertEqual(public_html_contract_errors(fixed), [])


if __name__ == "__main__":
    unittest.main()
