from __future__ import annotations

import unittest

from news_digest.pipeline.dedupe import _similar_published_titles
from news_digest.pipeline.transport_fill import _make_reminder_candidate
from news_digest.pipeline.writer import (
    _build_public_service_fallback_line,
    _build_ticket_fallback_line,
    _draft_line_quality_errors,
)


class DedupeTransportTest(unittest.TestCase):
    def test_ticketmaster_metadata_does_not_create_false_rehash(self) -> None:
        matches = _similar_published_titles(
            "sofia and the antoinettes event 2026 05 19 public sale 2026 03 11 10 00",
            "Sofia and the Antoinettes — event 2026-05-19 — public sale 2026-03-11 10:00",
            [
                {
                    "fingerprint": "old-avatar",
                    "title": "Avatar's the Last Airbender - Film with Live Orchestra — event 2026-10-11 — public sale 2025-05-02 15:00",
                    "normalized_title": "avatars the last airbender film with live orchestra event 2026 10 11 public sale 2025 05 02 15 00",
                }
            ],
        )

        self.assertEqual(matches, [])

    def test_ticketmaster_same_artist_still_matches(self) -> None:
        matches = _similar_published_titles(
            "calum scott event 2026 05 27 public sale 2025 04 11 09 00",
            "Calum Scott — event 2026-05-27 — public sale 2025-04-11 09:00",
            [
                {
                    "fingerprint": "old-calum-scott",
                    "title": "Calum Scott — event 2026-05-28 — public sale 2025-04-11 09:00",
                    "normalized_title": "calum scott event 2026 05 28 public sale 2025 04 11 09 00",
                }
            ],
        )

        self.assertEqual(matches[0]["fingerprint"], "old-calum-scott")

    def test_transport_reminder_has_release_safe_dedupe_fields(self) -> None:
        candidate = _make_reminder_candidate(
            {
                "key": "rochdale-line",
                "operator": "Metrolink",
                "line": "Rochdale line",
                "segment": "Victoria – Rochdale Town Centre",
                "end_date": "2026-05-29",
                "source_url": "https://tfgm.com/travel-updates/travel-alerts/rochdale-line-tram-improvement-works",
            },
            "2026-05-18",
        )

        self.assertEqual(candidate["dedupe_decision"], "new")
        self.assertEqual(candidate["change_type"], "same_story_new_facts")
        self.assertTrue(candidate["include"])

    def test_city_watch_keeps_one_sentence_when_other_quality_checks_pass(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "city_watch",
            "title": "Alex's Making a Difference Award win",
            "summary": "University of Manchester award story with named people and enough context.",
            "lead": "University of Manchester award story with named people and enough context.",
            "evidence_text": "Alex Smith received the Making a Difference Award at the University of Manchester for community work with local partners in Greater Manchester.",
        }
        line = (
            "• Manchester: Alex Smith получил Making a Difference Award в University of Manchester "
            "за работу с местными партнёрами и общественными проектами Greater Manchester."
        )

        self.assertEqual(_draft_line_quality_errors(candidate, line), [])

    def test_ticket_fallback_builds_ticketmaster_line_from_summary(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "next_7_days",
            "title": "Sofia and the Antoinettes — event 2026-05-19 — public sale 2026-03-11 10:00",
            "summary": "Manchester The Deaf Institute. | Manchester | Pop | event_date=2026-05-19 19:00 | public_onsale=2026-03-11 10:00 | ticket_signal=upcoming_event",
            "draft_line": "",
            "practical_angle": "Проверьте время, вход и наличие билетов на официальной странице.",
            "source_label": "Ticketmaster Manchester Upcoming",
        }

        line = _build_ticket_fallback_line(candidate)

        self.assertIn("19 мая", line)
        self.assertIn("The Deaf Institute", line)
        self.assertIn("Sofia and the Antoinettes", line)
        self.assertEqual(_draft_line_quality_errors(candidate, line), [])

    def test_ticket_fallback_builds_coop_live_line_from_title(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Madison Beer the locket tour Sun 31 May 2026 Multiple times",
            "summary": "",
            "draft_line": "",
            "practical_angle": "Проверьте время, вход и наличие билетов на официальной странице.",
            "source_label": "Co-op Live",
            # Radar items reach the fallback builder already enriched with a
            # notability tier; without it the score gate (>=50) holds the card.
            "ticket_notability": {"tier": "B", "kind": "artist"},
        }

        line = _build_ticket_fallback_line(candidate)

        self.assertIn("31 мая", line)
        self.assertIn("Co-op Live", line)
        self.assertIn("Madison Beer the locket tour", line)
        self.assertEqual(_draft_line_quality_errors(candidate, line), [])

    def test_public_services_fallback_keeps_nhs_review_when_rewrite_misses(self) -> None:
        candidate = {
            "category": "public_services",
            "primary_block": "today_focus",
            "title": "NHS England's Independent Assurance Review published today | News and Events",
            "summary": (
                "Working with our service users, people and partners we have made significant progress "
                "on our improvement journey and remain committed to improving safe, high-quality patient care."
            ),
            "lead": "",
            "evidence_text": (
                "Working with our service users, people and partners we have made significant progress "
                "on our improvement journey and remain committed to improving safe, high-quality patient care."
            ),
            "source_label": "GMMH",
        }

        line = _build_public_service_fallback_line(candidate)

        self.assertIn("независимый обзор качества", line)
        self.assertEqual(_draft_line_quality_errors(candidate, line), [])


if __name__ == "__main__":
    unittest.main()
