from __future__ import annotations

import unittest

from news_digest.pipeline.editorial_contracts import crime_specificity_review
from news_digest.pipeline.writer import (
    _draft_line_quality_errors,
    _is_expired_event_candidate,
)


class Release20260601FixesTest(unittest.TestCase):
    def test_court_reporter_byline_does_not_crime_flag_politics(self) -> None:
        # "Court reporter" is the journalist's beat, not a court case. It must
        # not push a political story into the crime gate (2026-06-01 false hold
        # of "Andy Burnham and Nigel Farage in social media clash").
        candidate = {
            "title": "Andy Burnham and Nigel Farage in social media clash",
            "summary": "The prominent politicians clashed on X.",
            "evidence_text": "Andrew Bardsley, Court reporter. The two clashed on X over policy.",
            "category": "media_layer",
        }
        review = crime_specificity_review(candidate)
        self.assertFalse(review["applies"])

    def test_real_appeal_with_location_is_publishable(self) -> None:
        # The appeal itself is the event; with a location present nothing is
        # missing, so it must not sit at borderline (where the writer silently
        # held the 2026-06-01 missing-girl appeal without a draft_line).
        candidate = {
            "title": "Police make appeal to help find missing teenage girl in Bolton",
            "summary": "Police have launched an appeal to find a missing teenage girl in Bolton.",
            "evidence_text": "Greater Manchester Police appeal for help to find a missing 14-year-old girl last seen in Bolton.",
            "category": "media_layer",
        }
        review = crime_specificity_review(candidate)
        self.assertTrue(review["applies"])
        self.assertEqual(review["severity"], "ok")

    def test_contentless_appeal_stub_still_hard(self) -> None:
        # Guard the other side: a bare "Police appeal for help" with no location
        # and tiny evidence must still be hard (hard_floor=1 for appeals).
        candidate = {
            "title": "Police appeal for help",
            "summary": "Police appeal for help.",
            "evidence_text": "Police appeal for help.",
            "category": "media_layer",
        }
        review = crime_specificity_review(candidate)
        self.assertEqual(review["severity"], "hard")

    def test_recurring_event_with_stale_scrape_date_not_expired(self) -> None:
        # Evergreen recurring market with a 2-year-old scrape date must not be
        # dropped as expired (2026-06-01 Spinningfields Makers Market).
        candidate = {
            "primary_block": "weekend_activities",
            "published_at": "2024-05-26T16:32:38+01:00",
            "title": "THE SPINNINGFIELDS MAKERS MARKET",
            "evidence_text": "Spinningfields Makers Market every third Saturday of the month.",
            "draft_line": "• Spinningfields Makers Market — каждую третью субботу месяца в Spinningfields.",
        }
        self.assertFalse(_is_expired_event_candidate(candidate, candidate["draft_line"]))

    def test_future_russian_date_in_draft_not_expired(self) -> None:
        # The future date lives in the Russian draft_line ("9 октября 2026"); the
        # expired check must read it, not drop the event on a stale published_at.
        candidate = {
            "primary_block": "weekend_activities",
            "published_at": "2026-05-28T12:44:41+01:00",
            "title": "Dom Joly: Trigger Happy TV",
            "evidence_text": "Dom Joly brings his characters to the stage. Location: Romiley Forum.",
            "draft_line": "• Dom Joly — 9 октября 2026 года в 19:30 в Romiley Forum, Stockport. Билеты от £15.",
        }
        self.assertFalse(_is_expired_event_candidate(candidate, candidate["draft_line"]))

    def test_dated_event_card_passes_lower_floor(self) -> None:
        # A complete dated listing (~126 chars, 2 sentences) with an event date
        # must clear the relaxed floor even when event.venue is empty in the
        # struct (2026-06-01 The Misfits dropped at 126 chars).
        candidate = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "evidence_text": "x" * 600,
            "event": {"is_event": True, "date_start": "2026-06-01", "venue": ""},
        }
        line = (
            "• В HOME 1 июня в 17:45 — фильм The Misfits (PG) с Мэрилин Монро "
            "в последнем выступлении. Билеты доступны на сайте."
        )
        errors = _draft_line_quality_errors(candidate, line)
        self.assertEqual([e for e in errors if "chars (got" in e], [])


if __name__ == "__main__":
    unittest.main()
