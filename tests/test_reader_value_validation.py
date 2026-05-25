from __future__ import annotations

from pathlib import Path
import unittest

from news_digest.pipeline.reader_value import (
    evaluate_reader_value_labels,
    load_reader_value_labels,
    predicted_label,
    reader_value_score,
    validate_reader_value_labels,
)


class ReaderValueValidationTest(unittest.TestCase):
    def test_manual_label_set_is_valid_and_balanced(self) -> None:
        payload = load_reader_value_labels(Path.cwd())
        labels = payload["labels"]

        self.assertEqual(validate_reader_value_labels(payload), [])
        self.assertGreaterEqual(len(labels), 30)
        self.assertLessEqual(len(labels), 50)
        self.assertGreaterEqual(sum(1 for item in labels if item["label"] == "useful"), 5)
        self.assertGreaterEqual(sum(1 for item in labels if item["label"] == "neutral"), 5)
        self.assertGreaterEqual(sum(1 for item in labels if item["label"] == "should_not_include"), 5)

    def test_reader_value_benchmark_has_no_dangerous_false_positives(self) -> None:
        report = evaluate_reader_value_labels(Path.cwd())
        summary = report["summary"]

        self.assertEqual(report["errors"], [])
        self.assertEqual(summary["label_count"], 43)
        self.assertGreaterEqual(summary["accuracy"], 0.85)
        self.assertGreaterEqual(summary["useful_recall"], 0.85)
        self.assertEqual(summary["dangerous_false_positive_count"], 0)

    def test_duplicate_transport_is_not_predicted_include(self) -> None:
        item = {
            "title": "Northern: Disruption between Rochdale and Manchester Victoria",
            "source_label": "National Rail",
            "category": "transport",
            "primary_block": "transport",
            "included": False,
            "change_type": "new_story",
            "reject_reason": "Intra-batch topic duplicate — same story kept from stronger source.",
        }

        self.assertEqual(predicted_label(reader_value_score(item)), "should_not_include")


class MorningPracticalBoostTest(unittest.TestCase):
    """Sprint Quality Fix 1 (S5 / 1.13) — items the reader can act on this
    morning outrank evergreen material at parity."""

    def test_today_focus_happening_today_outranks_same_block_evergreen(self) -> None:
        # today_focus has a moderate base so the +8 boost is observable
        # (transport already maxes the scale).
        practical = {
            "category": "public_services",
            "primary_block": "today_focus",
            "change_type": "new_story",
            "title": "Council deadline closes today",
            "include": True,
            "why_now": "happening_today",
        }
        evergreen = {
            "category": "public_services",
            "primary_block": "today_focus",
            "change_type": "new_story",
            "title": "Council long-term plan",
            "include": True,
            "why_now": "unclear",
        }
        self.assertGreater(
            reader_value_score(practical),
            reader_value_score(evergreen),
        )

    def test_ongoing_transport_gets_smaller_bump(self) -> None:
        # Same title in both to isolate the why_now effect from
        # _HIGH_VALUE_TITLE_RE bonuses. We only want to compare the boost.
        ongoing = {
            "category": "public_services",
            "primary_block": "today_focus",
            "change_type": "reminder",
            "title": "Service notice",
            "include": True,
            "why_now": "ongoing",
        }
        no_why_now = dict(ongoing)
        no_why_now["why_now"] = "unclear"
        self.assertGreater(
            reader_value_score(ongoing),
            reader_value_score(no_why_now),
        )

    def test_boost_only_fires_in_practical_blocks(self) -> None:
        # outside_gm_tickets is not a practical morning block; why_now
        # shouldn't bump it.
        with_why = {
            "category": "venues_tickets",
            "primary_block": "outside_gm_tickets",
            "change_type": "new_story",
            "title": "Concert in another city",
            "include": True,
            "why_now": "happening_today",
        }
        without_why = dict(with_why)
        without_why["why_now"] = "unclear"
        self.assertEqual(
            reader_value_score(with_why),
            reader_value_score(without_why),
        )


if __name__ == "__main__":
    unittest.main()
