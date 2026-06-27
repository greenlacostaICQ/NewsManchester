"""Wave 1 / S5: the curator prompt elects one lead PER BATCH, so several
candidates can carry a lead vote. The day's main story must be arbitrated
globally by reader value — not decided by whichever vote appeared first in the
candidate list.
"""
from news_digest.pipeline.curator import _arbitrate_global_lead


def test_strongest_reader_value_wins_across_batches():
    votes = [
        {"title": "weak batch-1 lead", "reader_value_score": 40, "section_board_score": 200},
        {"title": "strong batch-2 lead", "reader_value_score": 95, "section_board_score": 10},
        {"title": "mid batch-3 lead", "reader_value_score": 70, "section_board_score": 150},
    ]
    assert _arbitrate_global_lead(votes)["title"] == "strong batch-2 lead"


def test_board_score_breaks_reader_value_ties():
    votes = [
        {"title": "tie-low-board", "reader_value_score": 80, "section_board_score": 5},
        {"title": "tie-high-board", "reader_value_score": 80, "section_board_score": 99},
    ]
    assert _arbitrate_global_lead(votes)["title"] == "tie-high-board"
