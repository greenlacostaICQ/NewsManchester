from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.city_trends import (
    append_city_intelligence_history,
    build_trend_detection,
    load_city_history,
)
from news_digest.pipeline.common import write_json


def _candidate(
    fingerprint: str,
    title: str,
    *,
    topic_tags: list[str] | None = None,
    boroughs: list[str] | None = None,
    entities: dict | None = None,
    event: dict | None = None,
) -> dict:
    return {
        "fingerprint": fingerprint,
        "title": title,
        "include": True,
        "topic_tags": topic_tags or [],
        "boroughs": boroughs or [],
        "entities": entities or {"schema_version": 1, "all": []},
        "event": event or {},
    }


class TrendDetectionTest(unittest.TestCase):
    def test_growth_is_calculated_for_all_required_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_json(
                state_dir / "city_intelligence_history.json",
                [
                    {
                        "run_date_london": "2026-05-17",
                        "topics": {"housing": 1},
                        "entities": {"council:Manchester City Council": 1},
                    },
                    {
                        "run_date_london": "2026-05-18",
                        "topics": {"housing": 1},
                        "entities": {"council:Manchester City Council": 1},
                    },
                    {
                        "run_date_london": "2026-05-19",
                        "topics": {"housing": 1},
                        "entities": {"council:Manchester City Council": 1},
                    },
                ],
            )
            candidates = [
                _candidate(
                    f"fp-{i}",
                    f"Manchester housing plan {i}",
                    topic_tags=["housing"],
                    entities={
                        "schema_version": 1,
                        "councils": ["Manchester City Council"],
                        "all": [],
                    },
                )
                for i in range(4)
            ]

            result = build_trend_detection(
                state_dir,
                run_date_london="2026-05-20",
                candidates=candidates,
            )

        self.assertEqual(set(result["windows"].keys()), {"1d", "3d", "7d", "30d"})
        housing_3d = next(row for row in result["windows"]["3d"]["topics"] if row["key"] == "housing")
        self.assertEqual(housing_3d["current_count"], 4)
        self.assertEqual(housing_3d["previous_window_total"], 3)
        self.assertEqual(housing_3d["previous_window_avg"], 1.0)
        self.assertEqual(housing_3d["delta_vs_avg"], 3.0)
        entity_1d = result["windows"]["1d"]["entities"][0]
        self.assertEqual(entity_1d["entity_type"], "council")
        self.assertEqual(entity_1d["name"], "Manchester City Council")

    def test_daily_history_replaces_same_date_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            report = {
                "run_date_london": "2026-05-20",
                "release_decision": "pass",
                "warnings": [],
                "source_status": {"counts": {"failed": 0}},
                "digest_health": {"risk_level": "healthy"},
                "city_intelligence": {"borough_coverage": {"skew_flags": []}},
            }
            append_city_intelligence_history(
                state_dir,
                report_payload=report,
                candidates=[_candidate("fp-a", "Stockport park", topic_tags=["environment"], boroughs=["Stockport"])],
            )
            append_city_intelligence_history(
                state_dir,
                report_payload=report,
                candidates=[
                    _candidate("fp-a", "Stockport park", topic_tags=["environment"], boroughs=["Stockport"]),
                    _candidate("fp-b", "Stockport school", topic_tags=["education"], boroughs=["Stockport"]),
                ],
            )

            history = load_city_history(state_dir)

        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["item_count"], 2)
        self.assertEqual(history[0]["boroughs"], {"Stockport": 2})

    def test_daily_history_writer_creates_history_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            report = {
                "run_date_london": "2026-05-20",
                "release_decision": "pass",
                "warnings": [],
                "source_status": {"counts": {"failed": 0}},
                "digest_health": {"risk_level": "healthy"},
                "city_intelligence": {"borough_coverage": {"skew_flags": []}},
            }

            path = append_city_intelligence_history(
                state_dir,
                report_payload=report,
                candidates=[_candidate("fp-a", "Trafford market", topic_tags=["culture"], boroughs=["Trafford"])],
            )

            self.assertTrue(path.exists())
            history = load_city_history(state_dir)
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["run_date_london"], "2026-05-20")


if __name__ == "__main__":
    unittest.main()
