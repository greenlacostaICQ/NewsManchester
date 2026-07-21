from __future__ import annotations

from datetime import date
import unittest

from news_digest.pipeline.collector.filters import _is_stale_transport
from news_digest.pipeline.event_quality import event_quality_reject_reasons, event_quality_report
from news_digest.pipeline.reader_value import predicted_label, reader_value_score
from news_digest.pipeline.transport_fill import _prune_expired
from news_digest.pipeline.writer import _draft_line_quality_errors


class EditorialRegressionPackTest(unittest.TestCase):
    def test_event_golden_cases(self) -> None:
        cases = [
            (
                "no-date event",
                {
                    "category": "culture_weekly",
                    "primary_block": "next_7_days",
                    "title": "Free family workshop at Manchester Museum",
                    "summary": "Free drop-in workshop at Manchester Museum.",
                    "source_label": "Manchester Museum",
                    "source_url": "https://example.test/event",
                },
                "no_date",
            ),
            (
                "no-place event",
                {
                    "category": "culture_weekly",
                    "primary_block": "next_7_days",
                    "title": "Workshop on 20 May in Manchester",
                    "summary": "Tickets are free but the exact location is not named.",
                    "source_label": "Events Listing",
                    "source_url": "https://example.test/event",
                },
                "source_thin",
            ),
            (
                "no-district event",
                {
                    "category": "culture_weekly",
                    "primary_block": "next_7_days",
                    "title": "Workshop on 20 May at The Gallery",
                    "summary": "Free tickets for a workshop.",
                    "source_label": "Venue",
                    "source_url": "https://example.test/event",
                },
                "source_thin",
            ),
            (
                "no-access event",
                {
                    "category": "culture_weekly",
                    "primary_block": "next_7_days",
                    "title": "Workshop on 20 May at Manchester Museum",
                    "summary": "A public workshop in Manchester.",
                    "source_label": "Venue",
                    "source_url": "https://example.test/event",
                },
                "source_thin",
            ),
            (
                "no-source event",
                {
                    "category": "culture_weekly",
                    "primary_block": "next_7_days",
                    "title": "Free workshop on 20 May at Manchester Museum",
                    "summary": "Free tickets in Manchester.",
                },
                "source_thin",
            ),
        ]

        for name, candidate, expected_reason in cases:
            with self.subTest(name=name):
                self.assertIn(expected_reason, event_quality_reject_reasons(candidate))

        ok_candidate = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "title": "Free workshop on 20 May at Manchester Museum",
            "summary": "Book free tickets for the Manchester event.",
            "source_label": "Manchester Museum",
            "source_url": "https://example.test/event",
        }
        self.assertTrue(event_quality_report(ok_candidate)["ok"])

    def test_draft_line_golden_cases(self) -> None:
        candidate = {
            "category": "public_services",
            "primary_block": "today_focus",
            "title": "Council update",
            "evidence_text": "Council update with enough evidence for a normal line.",
        }
        cases = [
            ("missing bullet", "Совет обновил правила записи, проверьте детали сегодня.", "bullet marker"),
            (
                "source anchor html",
                "• Совет обновил правила записи, <a href=\"https://example.test\">источник</a>, проверьте детали сегодня.",
                "source anchor HTML",
            ),
            (
                "markdown emphasis",
                "• **Совет** обновил правила записи, проверьте детали сегодня.",
                "Markdown emphasis",
            ),
            (
                "english prose",
                "• Manchester council changed appointment rules, check the details today.",
                "normal Russian prose",
            ),
            (
                "empty why-it-matters",
                "• Совет обновил правила записи, это заметный кейс для жителей, проверьте детали сегодня.",
                "bad_editorial_prose",
            ),
        ]

        for name, line, expected in cases:
            with self.subTest(name=name):
                self.assertTrue(
                    any(expected in error for error in _draft_line_quality_errors(candidate, line))
                )

    def test_reader_value_golden_cases(self) -> None:
        cases = [
            (
                "duplicate",
                {"category": "transport", "primary_block": "transport", "change_type": "no_change", "title": "Tram disruption", "reject_reason": "duplicate same story kept from stronger source"},
                "should_not_include",
            ),
            (
                "pr",
                {"category": "public_services", "primary_block": "today_focus", "title": "Award win for local team", "reject_reason": "чистый PR"},
                "should_not_include",
            ),
            (
                "not GM",
                {"category": "venues_tickets", "primary_block": "outside_gm_tickets", "title": "Liverpool arena tickets", "reject_reason": "не относится к Greater Manchester"},
                "should_not_include",
            ),
            (
                "stale",
                {"category": "transport", "primary_block": "transport", "change_type": "no_change", "title": "Old roadworks update", "reject_reason": "устаревшая карточка"},
                "should_not_include",
            ),
            (
                "job advert",
                {"category": "tech_business", "primary_block": "tech_business", "title": "Job: software role in Manchester", "reject_reason": "job advert"},
                "should_not_include",
            ),
            (
                "no concrete date",
                {"category": "culture_weekly", "primary_block": "next_7_days", "title": "Theatre event", "reject_reason": "no concrete upcoming date"},
                "should_not_include",
            ),
            (
                "expired event",
                {"category": "venues_tickets", "primary_block": "ticket_radar", "title": "Concert tickets", "reject_reason": "expired event"},
                "should_not_include",
            ),
            (
                "weather useful",
                {"category": "weather", "primary_block": "weather", "change_type": "new_story", "title": "Weather warning for Greater Manchester", "included": True},
                "useful",
            ),
            (
                "transport useful",
                {"category": "transport", "primary_block": "transport", "change_type": "new_story", "title": "Tram disruption and diversion in Manchester", "included": True},
                "useful",
            ),
            (
                "council useful",
                {"category": "public_services", "primary_block": "today_focus", "change_type": "new_story", "title": "Council confirms housing support", "included": True},
                "useful",
            ),
            (
                "police lead useful",
                {"category": "media_layer", "primary_block": "lead_story", "change_type": "new_story", "title": "Police investigate stabbing in Manchester", "included": True},
                "useful",
            ),
            (
                "opening useful",
                {"category": "food_openings", "primary_block": "openings", "change_type": "new_story", "title": "Market opens in Stockport", "included": True},
                "useful",
            ),
        ]

        for name, item, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(predicted_label(reader_value_score(item)), expected)

    def test_stale_synthetic_transport_golden_cases(self) -> None:
        self.assertTrue(_is_stale_transport(None, "Tram disruption"))
        self.assertTrue(_is_stale_transport("2000-01-01T09:00:00+00:00", "Tram disruption"))
        self.assertFalse(_is_stale_transport("2000-01-01T09:00:00+00:00", "planned works from Monday"))

        records = {
            "old": {"key": "old", "end_date": "2026-05-17"},
            "active": {"key": "active", "end_date": "2026-05-18"},
        }
        self.assertEqual(_prune_expired(records, date(2026, 5, 18)), 1)
        self.assertEqual(sorted(records), ["active"])


if __name__ == "__main__":
    unittest.main()
