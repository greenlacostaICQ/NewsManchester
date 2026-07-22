from __future__ import annotations

import unittest

from news_digest.pipeline.candidate_validator import (
    _apply_section_routing_quality,
    _exclude_non_gm_transport,
    _exclude_wrong_food_opening_category,
    _hold_sensitive_thin_or_failed_enrichment,
    resolve_venue_scope,
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


class WriterLanguageQATests(unittest.TestCase):
    def test_mixed_latin_cyrillic_word_is_quality_error(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "evidence_text": "Greater Manchester leaders discussed regional transport.",
        }
        line = "• В Норт Уэстern обсудили транспортный план. Жителям стоит проверить сроки."

        errors = _draft_line_quality_errors(candidate, line)

        self.assertTrue(any("mixed Latin/Cyrillic word" in error for error in errors), errors)

    def test_kept_english_brand_words_are_not_mixed_script_errors(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "evidence_text": "The Lowry hosts a dated performance for visitors.",
            "event": {"is_event": True, "date_start": "2026-07-01", "venue": "The Lowry"},
        }
        line = "• The Lowry проведет спектакль 1 июля. Сверьте время перед поездкой."

        errors = _draft_line_quality_errors(candidate, line)

        self.assertFalse(any("mixed Latin/Cyrillic word" in error for error in errors), errors)


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

    def test_generic_service_or_manifesto_does_not_reroute_to_transport(self) -> None:
        cases = [
            {
                "include": True,
                "category": "professional_events",
                "primary_block": "professional_events",
                "source_label": "Manchester Digital Events",
                "title": "A Manifesto for the Northern Tech Economy",
                "summary": (
                    "The manifesto discusses technology policy, public services, "
                    "the Bee Network and transport investment."
                ),
            },
            {
                "include": True,
                "category": "media_layer",
                "primary_block": "last_24h",
                "source_label": "Altrincham Today",
                "title": "Man charged with murder of Stretford Grammar School headteacher appears in court",
                "summary": "The court case has been listed and the defendant appeared before magistrates.",
            },
            {
                "include": True,
                "category": "tech_business",
                "primary_block": "it_business",
                "source_label": "MIDAS Manchester",
                "title": "Invest Manchester",
                "summary": "A refreshed investment identity mentions the city region's transport system.",
            },
            {
                "include": True,
                "category": "media_layer",
                "primary_block": "city_watch",
                "source_label": "About Manchester News",
                "title": "GMP's Northern Quarter fight back - About Manchester",
                "summary": (
                    "Officers hosted targeted days of action in the Northern Quarter, "
                    "including stop searches, patrols and a closure order linked to "
                    "anti-social behaviour."
                ),
            },
        ]
        for candidate in cases:
            with self.subTest(candidate["title"]):
                reasons = _apply_section_routing_quality(candidate)
                self.assertNotIn("section_routing:transport", reasons)
                self.assertNotEqual(candidate["primary_block"], "transport")

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
        self.assertIn("section_routing:source_is_not_it_content", reasons)
        self.assertEqual(candidate["primary_block"], "city_watch")

    def test_irlam_fire_does_not_stay_in_it_business(self) -> None:
        candidate = {
            "include": True,
            "category": "media_layer",
            "primary_block": "tech_business",
            "source_label": "MEN News Sitemap",
            "title": "Fire service issues update after crews tackle huge Irlam blaze for 19 hours",
            "summary": (
                "A fire broke out at an industrial estate. Eight fire engines and an aerial "
                "ladder platform attended; pallets and two vehicles were involved."
            ),
        }

        reasons = _apply_section_routing_quality(candidate)

        self.assertIn("section_routing:incident_is_not_it_content", reasons)
        self.assertEqual(candidate["primary_block"], "city_watch")

    def test_tfgm_alert_wholly_outside_gm_is_rejected(self) -> None:
        candidate = {
            "include": True,
            "category": "transport",
            "primary_block": "transport",
            "source_label": "TfGM",
            "title": "Buses replace trains between Mansfield Woodhouse and Worksop",
            "summary": "$66",
            "evidence_text": "Until 23 July.",
        }

        self.assertTrue(_exclude_non_gm_transport(candidate))
        self.assertFalse(candidate["include"])
        self.assertIn("transport_non_gm", candidate["reject_reasons"])

    def test_transport_alert_with_manchester_anchor_survives_geography_gate(self) -> None:
        candidate = {
            "include": True,
            "category": "transport",
            "primary_block": "transport",
            "source_label": "TfGM",
            "title": "Amended trains between Manchester Piccadilly and Sheffield",
        }

        self.assertFalse(_exclude_non_gm_transport(candidate))
        self.assertTrue(candidate["include"])

    def test_ticketmaster_structured_city_is_authoritative_for_scope(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "title": "Queen of the Night — event 2026-09-03",
            "summary": "Hawth Theatre | Crawley | Rock | event_date=2026-09-03 19:30",
            "event": {
                "venue": "Hawth Theatre",
                "schema_source": "ticketmaster_api",
            },
        }

        self.assertEqual(resolve_venue_scope(candidate), ("outside", "Crawley"))

    def test_restaurant_burglary_is_not_a_food_opening(self) -> None:
        candidate = {
            "include": True,
            "category": "food_openings",
            "primary_block": "openings",
            "title": "Beloved local restaurant responds after hurtful break-in",
            "summary": (
                "The restaurant was burgled and staff wages were stolen. "
                "The venue opened last year in Chorlton."
            ),
        }

        self.assertTrue(_exclude_wrong_food_opening_category(candidate))
        self.assertFalse(candidate["include"])
        self.assertIn("wrong_openings_category", candidate["reject_reasons"])

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
