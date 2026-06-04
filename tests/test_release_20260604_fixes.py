from __future__ import annotations

import unittest

from news_digest.pipeline.candidate_validator import _exclude_non_gm_news, _reroute_tour_announcement
from news_digest.pipeline.change_classifier import classify_change_phase
from news_digest.pipeline.editorial_contracts import scrub_vague_ending
from news_digest.pipeline.place_names import expand_uk_abbreviations
from news_digest.pipeline.ticket_notability import _clean_artist_name
from news_digest.pipeline.writer import _ticket_price


class Release20260604FixesTest(unittest.TestCase):
    # 1. Phase plate must not misfire on negation / landmark words.
    def test_phase_not_charged_when_no_charges(self) -> None:
        c = {
            "title": "'No justice' for woman cyberflashed by GMP custody officer",
            "lead": "He will not face criminal charges after being told there was not enough evidence to charge him.",
            "summary": "A police custody officer has been sacked for sending unwanted images.",
        }
        self.assertEqual(classify_change_phase(c), "")

    def test_phase_not_sentenced_on_prison_landmark(self) -> None:
        c = {
            "title": "Strangeways area could transform under huge housing plans",
            "lead": "The site falls into an area of Strangeways, Manchester, earmarked for regeneration.",
            "summary": "Council backs 189 homes in a 20-storey block on Dutton Street.",
        }
        # "prison" only appeared in the body landmark; it must not set sentenced.
        self.assertNotEqual(classify_change_phase(c), "sentenced")

    def test_phase_still_detects_real_sentencing(self) -> None:
        c = {"title": "Man jailed for 16 years over Manchester drug plot", "lead": "", "summary": "He was sentenced at court."}
        self.assertEqual(classify_change_phase(c), "sentenced")

    # 4. Abbreviation glossary.
    def test_cllr_abbreviation_expanded(self) -> None:
        self.assertEqual(expand_uk_abbreviations("Клр. Марк Робертс"), "советник Марк Робертс")
        self.assertEqual(expand_uk_abbreviations("Cllr Jane Doe"), "советник Jane Doe")

    # 6. Ticket card sanity.
    def test_fee_not_shown_as_price(self) -> None:
        self.assertEqual(_ticket_price({"event": {"price": "£4.75"}}), "")
        self.assertEqual(_ticket_price({"event": {"price": "£15–£40"}}), "£15–£40")

    def test_pipe_garbage_stripped_from_name(self) -> None:
        self.assertEqual(
            _clean_artist_name("Jason Isbell and the 400 Unit | The Bridgewater Hall"),
            "Jason Isbell and the 400 Unit",
        )

    # 7. Weak ending scrubbed.
    def test_empty_subscriber_ending_scrubbed(self) -> None:
        line = "• Salford: дом сильно пострадал. Он собирается держать своих подписчиков в курсе событий."
        cleaned, removed = scrub_vague_ending(line)
        self.assertTrue(removed)
        self.assertNotIn("подписчиков в курсе", cleaned)

    # 3. Geo anchor for crime news.
    def test_blackpool_crime_excluded(self) -> None:
        c = {
            "include": True,
            "category": "media_layer",
            "title": "Blackpool man on trial over death",
            "summary": "The case is being heard after a death in Blackpool.",
            "lead": "",
            "evidence_text": "A man from Blackpool appeared in court over the death.",
        }
        self.assertTrue(_exclude_non_gm_news(c))
        self.assertFalse(c["include"])

    # 2. Tour announcement rerouted out of fresh-news lane.
    def test_tour_announcement_rerouted(self) -> None:
        c = {
            "include": True,
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Amble Announce UK Tour 2026 Including Manchester Apollo",
            "summary": "The Irish folk trio have announced a major UK headline tour.",
            "lead": "",
        }
        self.assertTrue(_reroute_tour_announcement(c))
        self.assertEqual(c["primary_block"], "future_announcements")


if __name__ == "__main__":
    unittest.main()
