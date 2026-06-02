from __future__ import annotations

import unittest

from news_digest.pipeline.source_anomaly import detect_source_anomalies


def _row(day: str, raw_by_source: dict[str, int]) -> dict:
    return {
        "run_date_london": day,
        "sources": [
            {"name": name, "category": "city_news", "raw": raw}
            for name, raw in raw_by_source.items()
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


if __name__ == "__main__":
    unittest.main()
