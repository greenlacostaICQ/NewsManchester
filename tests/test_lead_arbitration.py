"""Wave 1 / S5: the curator prompt elects one lead PER BATCH, so several
candidates can carry a lead vote. The day's main story must be arbitrated
globally by reader value — not decided by whichever vote appeared first.
"""
import unittest

from news_digest.pipeline.curator import _arbitrate_global_lead


class LeadArbitrationTest(unittest.TestCase):
    def test_strongest_reader_value_wins_across_batches(self):
        votes = [
            {"title": "weak batch-1 lead", "reader_value_score": 40, "section_board_score": 200},
            {"title": "strong batch-2 lead", "reader_value_score": 95, "section_board_score": 10},
            {"title": "mid batch-3 lead", "reader_value_score": 70, "section_board_score": 150},
        ]
        self.assertEqual(_arbitrate_global_lead(votes)["title"], "strong batch-2 lead")

    def test_board_score_breaks_reader_value_ties(self):
        votes = [
            {"title": "tie-low-board", "reader_value_score": 80, "section_board_score": 5},
            {"title": "tie-high-board", "reader_value_score": 80, "section_board_score": 99},
        ]
        self.assertEqual(_arbitrate_global_lead(votes)["title"], "tie-high-board")

    def test_transport_status_does_not_beat_real_city_lead(self):
        votes = [
            {
                "title": "Metrolink delays to Bury after signalling fault",
                "summary": "Passengers face delays this morning.",
                "primary_block": "last_24h",
                "reader_value_score": 99,
                "section_board_score": 99,
            },
            {
                "title": "Council confirms major housing safety intervention",
                "summary": "Manchester Council has ordered urgent action at a housing block.",
                "primary_block": "last_24h",
                "reader_value_score": 70,
                "section_board_score": 20,
            },
        ]
        self.assertEqual(_arbitrate_global_lead(votes)["title"], "Council confirms major housing safety intervention")

    def test_all_weak_leads_still_fall_back_to_score(self):
        votes = [
            {"title": "Weather warning for Greater Manchester", "primary_block": "weather", "reader_value_score": 40},
            {"title": "Metrolink delays on the Bury line", "primary_block": "transport", "reader_value_score": 80},
        ]
        self.assertEqual(_arbitrate_global_lead(votes)["title"], "Metrolink delays on the Bury line")


if __name__ == "__main__":
    unittest.main()
