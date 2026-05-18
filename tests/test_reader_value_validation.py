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


if __name__ == "__main__":
    unittest.main()
