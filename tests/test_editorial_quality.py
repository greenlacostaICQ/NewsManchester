from __future__ import annotations

import unittest

from news_digest.pipeline.editorial_quality import (
    apply_editorial_quality,
    evaluate_editorial_rubric,
    included_rubric_red_flags,
    reader_value_report,
    rubric_summary,
)


class EditorialQualityTest(unittest.TestCase):
    def test_strong_local_transport_candidate_passes_core_rubric(self) -> None:
        candidate = {
            "title": "Metrolink disruption between Bury and Crumpsall from 17 May",
            "summary": "Replacement buses will run while track works affect services from 17 May.",
            "lead": "",
            "practical_angle": "Проверьте маршрут перед поездкой.",
            "evidence_text": "Metrolink Bury line replacement buses from 17 May between Bury and Crumpsall.",
            "source_url": "https://tfgm.com/travel-updates/bury-line",
            "primary_block": "transport",
            "category": "transport",
        }
        rubric = evaluate_editorial_rubric(candidate)
        self.assertTrue(rubric["new"])
        self.assertTrue(rubric["local"])
        self.assertTrue(rubric["specific"])
        self.assertTrue(rubric["useful"])
        self.assertTrue(rubric["actionable"])

    def test_pr_evergreen_candidate_gets_red_flags(self) -> None:
        candidate = {
            "title": "Award-winning guide to the best places to eat",
            "summary": "A sponsored guide to top 10 places with a promotion deal.",
            "lead": "",
            "practical_angle": "",
            "evidence_text": "A sponsored guide to top 10 places with a promotion deal.",
            "source_url": "https://example.com/guide",
            "primary_block": "city_watch",
            "category": "food_openings",
            "include": True,
        }
        apply_editorial_quality([candidate])
        flags = included_rubric_red_flags([candidate])
        self.assertIn("not_pr", flags[0]["red_flags"])
        self.assertIn("not_evergreen", flags[0]["red_flags"])

    def test_summary_counts_included_red_flags(self) -> None:
        candidates = [
            {
                "include": True,
                "title": "Manchester event on 20 May",
                "summary": "Event at Manchester venue on 20 May.",
                "practical_angle": "Уточните время.",
                "evidence_text": "Manchester event at venue on 20 May.",
                "source_url": "https://example.com/manchester-event",
                "primary_block": "next_7_days",
                "category": "culture_weekly",
            },
            {
                "include": True,
                "title": "Generic advice",
                "summary": "General advice with no local detail.",
                "practical_angle": "",
                "evidence_text": "General advice with no local detail.",
                "source_url": "https://example.com/advice",
                "primary_block": "city_watch",
                "category": "media_layer",
            },
        ]
        apply_editorial_quality(candidates)
        summary = rubric_summary(candidates)
        self.assertEqual(summary["included_candidates"], 2)
        self.assertGreaterEqual(summary["included_with_red_flags"], 1)

    def test_reader_value_score_prefers_specific_actionable_local_item(self) -> None:
        strong = {
            "include": True,
            "fingerprint": "strong-transport",
            "title": "Metrolink disruption between Bury and Crumpsall from 17 May",
            "summary": "Replacement buses will run while track works affect services from 17 May.",
            "practical_angle": "Проверьте маршрут перед поездкой.",
            "evidence_text": "Metrolink disruption between Bury and Crumpsall from 17 May with replacement buses.",
            "source_url": "https://tfgm.com/travel-updates/bury-line",
            "primary_block": "transport",
            "category": "transport",
        }
        weak = {
            "include": True,
            "fingerprint": "weak-generic",
            "title": "Generic travel advice",
            "summary": "General advice with no local detail.",
            "practical_angle": "",
            "evidence_text": "General advice with no local detail.",
            "source_url": "https://example.com/advice",
            "primary_block": "city_watch",
            "category": "media_layer",
        }
        apply_editorial_quality([strong, weak])
        self.assertGreater(strong["reader_value_score"], weak["reader_value_score"])
        self.assertIn("novelty", strong["reader_value_components"])
        report = reader_value_report([strong, weak], limit=1)
        self.assertEqual(report["top"][0]["fingerprint"], strong.get("fingerprint"))


if __name__ == "__main__":
    unittest.main()
