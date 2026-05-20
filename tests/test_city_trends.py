from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.city_trends import (
    append_city_intelligence_history,
    build_trend_detection,
    build_weekly_city_rollup,
    load_city_history,
)
from news_digest.pipeline.common import write_json
from scripts.run_local_digest import _weekly_city_rollup_errors_are_non_blocking


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


class WeeklyRollupTest(unittest.TestCase):
    def test_empty_history_rollup_is_non_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)

            rollup = build_weekly_city_rollup(state_dir, end_date_london="2026-05-20")

        self.assertEqual(rollup["period"]["days"], 0)
        self.assertEqual(rollup["errors"], ["city_intelligence_history.json is empty"])
        self.assertTrue(_weekly_city_rollup_errors_are_non_blocking(rollup))

    def test_weekly_rollup_summarises_topics_boroughs_events_and_risks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_json(
                state_dir / "city_intelligence_history.json",
                [
                    {
                        "run_date_london": "2026-05-18",
                        "basis": "rendered",
                        "item_count": 5,
                        "topics": {"housing": 3, "transport": 2},
                        "entities": {"council:Manchester City Council": 3},
                        "boroughs": {"Manchester": 4, "Stockport": 1},
                        "events": {
                            "event_count": 2,
                            "dated_event_count": 1,
                            "undated_event_count": 1,
                            "by_borough": {"Manchester": 2},
                            "by_venue": {"Co-op Live": 2},
                            "by_date": {"2026-05-22": 1},
                        },
                        "risks": {
                            "release_decision": "pass",
                            "digest_risk_level": "healthy",
                            "warning_count": 1,
                            "source_failed_count": 2,
                            "zero_yield_sources": 3,
                            "lost_leads": 0,
                            "section_underflow": 1,
                            "borough_skew_flags": ["Manchester skew"],
                        },
                        "trend_detection": {
                            "rising_topics": [{"key": "housing", "current_count": 3}],
                            "rising_entities": [{"name": "Manchester City Council", "current_count": 3}],
                        },
                    },
                    {
                        "run_date_london": "2026-05-19",
                        "basis": "rendered",
                        "item_count": 4,
                        "topics": {"housing": 1, "culture": 3},
                        "entities": {"venue:Co-op Live": 3},
                        "boroughs": {"Manchester": 3, "Trafford": 1},
                        "events": {
                            "event_count": 3,
                            "dated_event_count": 3,
                            "undated_event_count": 0,
                            "by_borough": {"Manchester": 3},
                            "by_venue": {"Co-op Live": 3},
                            "by_date": {"2026-05-23": 3},
                        },
                        "risks": {
                            "release_decision": "pass",
                            "digest_risk_level": "at_risk",
                            "warning_count": 2,
                            "source_failed_count": 1,
                            "zero_yield_sources": 0,
                            "lost_leads": 1,
                            "section_underflow": 0,
                            "borough_skew_flags": [],
                        },
                        "trend_detection": {
                            "rising_topics": [{"key": "culture", "current_count": 3}],
                            "rising_entities": [{"name": "Co-op Live", "current_count": 3}],
                        },
                    },
                ],
            )

            rollup = build_weekly_city_rollup(state_dir, end_date_london="2026-05-20")

        self.assertEqual(rollup["period"]["days"], 2)
        self.assertEqual(rollup["totals"]["items"], 9)
        self.assertEqual(rollup["topics"][0], {"name": "housing", "count": 4})
        manchester = next(row for row in rollup["boroughs"] if row["name"] == "Manchester")
        self.assertEqual(manchester["count"], 7)
        self.assertEqual(rollup["events"]["event_count"], 5)
        self.assertEqual(rollup["events"]["by_venue"][0], {"name": "Co-op Live", "count": 5})
        self.assertEqual(rollup["risks"]["counts"]["failed_sources"], 3)
        self.assertEqual(rollup["risks"]["counts"]["lost_leads"], 1)
        self.assertEqual(rollup["trends"]["rising_topics"][0]["key"], "culture")


if __name__ == "__main__":
    unittest.main()
