from __future__ import annotations

import unittest

from news_digest.pipeline.candidate_validator import (
    _apply_section_routing_quality,
    _hold_sensitive_thin_or_failed_enrichment,
)
from news_digest.pipeline.glossary_qa import glossary_line_issues, repair_glossary_terms
from news_digest.pipeline.writer import _draft_line_quality_errors, _today_focus_candidate_is_eligible


class GlossaryQATests(unittest.TestCase):
    def test_keep_terms_are_not_flagged(self) -> None:
        line = "• В офисе будет open space для fintech-команд и API-интеграций."
        self.assertEqual(glossary_line_issues(line), [])

    def test_translate_required_term_is_repaired(self) -> None:
        fixed, reasons = repair_glossary_terms("• National Rail disruptions между Stockport и Sheffield.")
        self.assertIn("перебои в движении", fixed)
        self.assertIn("glossary:disruptions", reasons)
        self.assertEqual(glossary_line_issues(fixed), [])

    def test_bare_explain_term_is_flagged_and_repaired(self) -> None:
        line = "• Hazelbrook получил предупреждение CQC после проверки."
        self.assertIn("glossary_explain_required:CQC", glossary_line_issues(line))
        fixed, _reasons = repair_glossary_terms(line)
        self.assertIn("регулятор качества", fixed)
        self.assertEqual(glossary_line_issues(fixed), [])


class EnglishDataQATests(unittest.TestCase):
    def test_transport_item_reroutes_before_translation(self) -> None:
        candidate = {
            "include": True,
            "category": "media_layer",
            "primary_block": "city_watch",
            "source_label": "TfGM",
            "title": "Metrolink disruption on Bury line",
            "summary": "Minor delays are affecting tram services today.",
        }
        reasons = _apply_section_routing_quality(candidate)
        self.assertIn("section_routing:transport", reasons)
        self.assertEqual(candidate["primary_block"], "transport")

    def test_property_item_does_not_stay_in_it_business(self) -> None:
        candidate = {
            "include": True,
            "category": "tech_business",
            "primary_block": "it_business",
            "source_label": "Bdaily Manchester",
            "title": "Developer plans 68 apartments in Trafford",
            "summary": "The housing scheme includes affordable homes and planning approval.",
        }
        reasons = _apply_section_routing_quality(candidate)
        self.assertIn("section_routing:property_not_it", reasons)
        self.assertEqual(candidate["primary_block"], "city_watch")

    def test_sensitive_thin_enrichment_is_held_before_translation(self) -> None:
        candidate = {
            "include": True,
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Man charged after stabbing",
            "summary": "Short teaser.",
            "enrichment_health": {"failed": True, "thin": True},
        }
        self.assertTrue(_hold_sensitive_thin_or_failed_enrichment(candidate))
        self.assertFalse(candidate["include"])
        self.assertIn("sensitive_thin_or_failed_enrichment", candidate["reject_reasons"])


class TodayFocusRegressionTests(unittest.TestCase):
    def test_by_election_is_valid_today_focus(self) -> None:
        candidate = {
            "include": True,
            "category": "media_layer",
            "primary_block": "today_focus",
            "title": "Voters head to the polls for Makerfield by-election",
            "summary": "Voters in Makerfield head to the polls today in a Greater Manchester by-election.",
            "lead": "Polling stations are open today in Makerfield.",
            "practical_angle": "Residents voting today need to know polling stations are open.",
        }
        self.assertTrue(_today_focus_candidate_is_eligible(candidate, "• Makerfield: voters head to the polls today."))

    def test_concise_today_focus_line_is_not_dropped_for_length_only(self) -> None:
        candidate = {
            "include": True,
            "category": "media_layer",
            "primary_block": "today_focus",
            "title": "Voters head to the polls for Makerfield by-election",
            "summary": "Voters in Makerfield head to the polls today in a Greater Manchester by-election.",
            "lead": "Polling stations are open today in Makerfield.",
            "practical_angle": "Residents voting today need to know polling stations are open.",
        }
        line = "• Makerfield: voters head to the polls today in a Greater Manchester by-election, with polling stations open for residents."
        errors = _draft_line_quality_errors(candidate, line)
        self.assertFalse([error for error in errors if "long-format category" in error])


if __name__ == "__main__":
    unittest.main()
