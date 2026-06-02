from __future__ import annotations

import unittest

from news_digest.pipeline.source_anomaly import (
    detect_dead_parsers,
    detect_source_anomalies,
)


def _row(day: str, raw_by_source: dict[str, int]) -> dict:
    return {
        "run_date_london": day,
        "sources": [
            {"name": name, "category": "city_news", "raw": raw}
            for name, raw in raw_by_source.items()
        ],
    }


def _status_row(day: str, status_by_source: dict[str, str]) -> dict:
    return {
        "run_date_london": day,
        "sources": [
            {"name": name, "category": "venues_tickets", "status": status, "raw": 0}
            for name, status in status_by_source.items()
        ],
    }


class SourceAnomalyTest(unittest.TestCase):
    def test_flags_source_that_went_dark_after_steady_week(self):
        history = [_row(f"2026-05-2{d}", {"BBC": 8, "The Mill": 5}) for d in range(0, 7)]
        history.append(_row("2026-05-27", {"BBC": 0, "The Mill": 5}))  # BBC dark today

        anomalies = detect_source_anomalies(history)

        names = {a["name"] for a in anomalies}
        self.assertEqual(names, {"BBC"})  # The Mill steady → not flagged

    def test_warming_up_returns_nothing(self):
        history = [_row("2026-05-20", {"BBC": 8}), _row("2026-05-21", {"BBC": 0})]
        self.assertEqual(detect_source_anomalies(history), [])


class DeadParserTest(unittest.TestCase):
    def test_flags_source_empty_all_week_but_not_failed_or_working(self):
        history = [
            _status_row(
                f"2026-05-2{d}",
                {"AO Arena": "empty", "BBC": "failed", "The Mill": "ok"},
            )
            for d in range(0, 5)
        ]

        dead = detect_dead_parsers(history)

        # AO Arena fetched OK but parsed nothing all week → broken parser.
        # BBC is "failed" (network/WAF — an extractor can't fix it).
        # The Mill is "ok". Neither is flagged.
        self.assertEqual({d["name"] for d in dead}, {"AO Arena"})

    def test_warming_up_returns_nothing(self):
        history = [_status_row("2026-05-20", {"AO Arena": "empty"})]
        self.assertEqual(detect_dead_parsers(history), [])


if __name__ == "__main__":
    unittest.main()
