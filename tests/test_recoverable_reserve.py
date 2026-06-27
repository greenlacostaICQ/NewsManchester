"""Wave 1 / S1: unify the two reserve pools so a strong news item cut purely
for board capacity is reachable by the editor backfill.

Before S1 the board overflow was tagged ``public_reserve=False`` +
``backup_pool_only=True``, which the backfill (``_same_section_reserve_line``)
explicitly skipped. That disjoint pool is why every prior backfill fix failed to
refill thin news blocks — the strong cut news sat in a pool it could not read.
"""
import unittest

from news_digest.pipeline import editor
from news_digest.pipeline.common import is_recoverable_reserve, recoverable_reserve_eligible


def _capacity_cut(**over):
    candidate = {
        "validated": True,
        "digest_selection_verdict": "reserve",
        "primary_block": "last_24h",
        "draft_line": (
            '• В Манчестере открыли новый общественный парк для жителей района. '
            '<a href="https://example.test/park">MEN</a>'
        ),
        "backup_candidate": True,
        "backup_pool_only": True,
        "public_reserve": False,
        "recoverable_reserve": True,
    }
    candidate.update(over)
    return candidate


class RecoverableReserveTest(unittest.TestCase):
    def test_capacity_cut_overflow_is_recoverable(self):
        self.assertTrue(recoverable_reserve_eligible(_capacity_cut()))
        self.assertTrue(is_recoverable_reserve(_capacity_cut()))

    def test_excluded_categories_never_recoverable(self):
        for bad in (
            {"validated": False},
            {"synthetic_stale": True},
            {"source_trial": True},
            {"digest_selection_verdict": "drop"},
            {"publish_plan_status": "drop"},
            {"reject_reasons": ["no_date"]},
            {"dedupe_decision": "duplicate"},
            {"held_for_manual_review": True},
        ):
            self.assertFalse(recoverable_reserve_eligible(_capacity_cut(**bad)), bad)

    def test_editor_backfill_now_reaches_capacity_cut_reserve(self):
        line = editor._same_section_reserve_line("Свежие новости", [_capacity_cut()], set(), set())
        self.assertTrue(line.startswith("• "))
        self.assertIn("парк", line)


if __name__ == "__main__":
    unittest.main()
