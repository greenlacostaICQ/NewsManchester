from __future__ import annotations

import unittest

from news_digest.pipeline.professional_events import score_professional_event
from news_digest.pipeline.writer import _build_professional_event_fallback_line


class ProfessionalEventsTest(unittest.TestCase):
    def _candidate(self, title: str, summary: str, price: str = "free", venue: str = "Manchester Central") -> dict:
        return {
            "title": title,
            "summary": summary,
            "lead": "",
            "evidence_text": summary,
            "source_label": "Manchester Digital Events",
            "source_url": "https://www.manchesterdigital.com/events/example",
            "category": "professional_events",
            "primary_block": "professional_events",
            "event": {
                "is_event": True,
                "event_name": title,
                "venue": venue,
                "date": "2026-07-02",
                "date_start": "2026-07-02T09:30:00+01:00",
                "price": price,
                "booking_url": "https://www.manchesterdigital.com/events/example",
            },
        }

    def test_major_free_expo_is_published_as_major(self) -> None:
        c = self._candidate(
            "DTX Manchester AI and Digital Transformation Expo",
            "Free delegate pass for business leaders. Conference with AI, data, cloud, product and enterprise technology tracks.",
        )
        match = score_professional_event(c)
        self.assertTrue(match["publish"])
        self.assertEqual(match["event_level"], "major_conference_or_expo")
        self.assertTrue(match["major_conference_or_expo"])
        self.assertEqual(match["recommended_action"], "register")

    def test_basic_free_networking_can_pass_for_english_practice(self) -> None:
        c = self._candidate(
            "Manchester startup networking breakfast",
            "Free local business networking workshop for founders and product people in Manchester.",
            venue="Bonded Warehouse",
        )
        match = score_professional_event(c)
        self.assertTrue(match["publish"])
        self.assertEqual(match["event_level"], "english_practice_networking")
        self.assertTrue(match["english_practice_value"])

    def test_paid_event_without_free_path_is_rejected(self) -> None:
        c = self._candidate(
            "Fintech leadership dinner",
            "Tickets from £95 for a private dinner and vendor demo.",
            price="£95",
        )
        match = score_professional_event(c)
        self.assertFalse(match["publish"])
        self.assertEqual(match["recommended_action"], "skip")

    def test_writer_builds_self_contained_russian_card(self) -> None:
        c = self._candidate(
            "CreaTech Connect: Accelerating University-Industry Partnerships",
            "Free general admission. University-industry partnerships, innovation and business networking at SISTER.",
            venue="Renold Building (SISTER)",
        )
        c["professional_event_match"] = score_professional_event(c)
        line = _build_professional_event_fallback_line(c)
        self.assertIn("CreaTech Connect", line)
        self.assertIn("2 июля", line)
        self.assertIn("Уровень:", line)
        self.assertIn("бесплат", line.lower())
        self.assertIn("Почему тебе:", line)
        self.assertIn("Действие:", line)


if __name__ == "__main__":
    unittest.main()
