from __future__ import annotations

import unittest

from scripts.run_local_digest import _format_operator_warning_message


class OperatorWarningFormatterTest(unittest.TestCase):
    def test_formats_meaningful_operator_warnings(self) -> None:
        message = _format_operator_warning_message(
            {
                "run_date_london": "2026-05-15",
                "operator_warnings": [
                    {"type": "lost_leads", "count": 1},
                    {"type": "section_underflow", "section": "Что важно сегодня", "visible": 2, "minimum": 3},
                    {"type": "reject_spike", "counts": {"pr": 5, "source_thin": 3}},
                    {"type": "low_reader_value", "count": 4, "threshold": 40},
                ],
            }
        )
        self.assertIn("lost_leads=1", message)
        self.assertIn("underflow=Что важно сегодня (2/3)", message)
        self.assertIn("rejects: pr=5, source_thin=3", message)
        self.assertIn("low_score=4≤40", message)

    def test_empty_when_no_operator_warnings(self) -> None:
        self.assertEqual(_format_operator_warning_message({"operator_warnings": []}), "")


if __name__ == "__main__":
    unittest.main()
