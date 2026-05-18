"""O2 — Synthetic Freshness Gate.

Covers the refetch×2 policy on weather and the stale-reminder gate on
transport. Past defect: weather block silently shipped 'данные временно
недоступны' with `published_at=now()` so the gate couldn't tell a live
fetch from a fallback placeholder. Past defect: Metrolink reminder
records older than two weeks kept rendering without any way for the
release report to flag them.
"""
from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta
from unittest import mock

from news_digest.pipeline.collector import fallbacks
from news_digest.pipeline.release import _summarise_synthetic_freshness
from news_digest.pipeline.transport_fill import _make_reminder_candidate


# --------------------------------------------------------------------------
# fetch-with-retries helper
# --------------------------------------------------------------------------
class FetchWithRetriesTest(unittest.TestCase):
    def test_first_attempt_success_returns_immediately(self) -> None:
        with mock.patch.object(fallbacks, "_fetch_text", return_value="body-ok") as m:
            body, attempts = fallbacks._fetch_with_retries("http://x", attempts=3)
        self.assertEqual(body, "body-ok")
        self.assertEqual(attempts, 1)
        self.assertEqual(m.call_count, 1)

    def test_two_failures_then_success_counts_three_attempts(self) -> None:
        side = [RuntimeError("net 1"), RuntimeError("net 2"), "body-ok"]
        with (
            mock.patch.object(fallbacks, "_fetch_text", side_effect=side),
            mock.patch.object(fallbacks, "time", new=mock.MagicMock()),  # silence sleep
        ):
            body, attempts = fallbacks._fetch_with_retries("http://x", attempts=3)
        self.assertEqual(body, "body-ok")
        self.assertEqual(attempts, 3)

    def test_all_attempts_fail_raises_last_exception(self) -> None:
        side = [RuntimeError("a"), RuntimeError("b"), RuntimeError("c")]
        with (
            mock.patch.object(fallbacks, "_fetch_text", side_effect=side),
            mock.patch.object(fallbacks, "time", new=mock.MagicMock()),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                fallbacks._fetch_with_retries("http://x", attempts=3)
        self.assertIn("c", str(ctx.exception))


# --------------------------------------------------------------------------
# Weather candidate freshness markers
# --------------------------------------------------------------------------
class WeatherCandidateFreshnessTest(unittest.TestCase):
    """Past defect: weather candidate `published_at` was always `now()`
    so an all-fallback placeholder looked fresh. Now `data_fetched_at`
    is set ONLY on successful live fetch, and `synthetic_stale=True`
    flags the placeholder."""

    _MET_OFFICE_HTML = (
        '<html><body>'
        '<div class="tab-temp-high" data-c=" 18°"></div>'
        '<div class="tab-temp-low" data-c=" 9°"></div>'
        '<tr class="precipitation-chance-row hourly-table">'
        '<td>10%</td><td>20%</td><td>40%</td></tr>'
        '<h4>Headline:</h4><p>Bright morning.</p>'
        '<h4>Today:</h4><p>Sunny intervals.</p>'
        '</body></html>'
    )

    def test_live_met_office_fetch_marks_candidate_fresh(self) -> None:
        with mock.patch.object(
            fallbacks, "_fetch_with_retries", return_value=(self._MET_OFFICE_HTML, 1)
        ):
            candidate = fallbacks._weather_candidate()
        self.assertTrue(candidate["synthetic"])
        self.assertFalse(candidate["synthetic_stale"])
        self.assertIsNotNone(candidate["data_fetched_at"])
        self.assertEqual(candidate["synthetic_fetch_attempts"], 1)
        self.assertEqual(candidate["source_label"], "Met Office")
        # Live data path must include digits in draft_line.
        self.assertRegex(candidate["draft_line"], r"\d+-\d+°C")

    def test_met_office_failure_falls_back_to_open_meteo(self) -> None:
        open_meteo_body = (
            '{"daily":{"temperature_2m_min":[7.4],'
            '"temperature_2m_max":[15.1],"precipitation_probability_max":[55]}}'
        )

        def side(url: str, *, attempts: int = 3):
            if "metoffice" in url:
                raise RuntimeError("Met Office 503")
            return (open_meteo_body, 1)

        with mock.patch.object(fallbacks, "_fetch_with_retries", side_effect=side):
            candidate = fallbacks._weather_candidate()
        self.assertFalse(candidate["synthetic_stale"])
        self.assertEqual(candidate["source_label"], "Open-Meteo")
        self.assertIsNotNone(candidate["data_fetched_at"])
        # 3 attempts for Met Office (all failed) + 1 attempt for Open-Meteo (succeeded).
        self.assertEqual(candidate["synthetic_fetch_attempts"], 4)
        self.assertRegex(candidate["draft_line"], r"\d+-\d+°C")

    def test_both_sources_fail_marks_candidate_synthetic_stale(self) -> None:
        with mock.patch.object(
            fallbacks, "_fetch_with_retries", side_effect=RuntimeError("network down")
        ):
            candidate = fallbacks._weather_candidate()
        self.assertTrue(candidate["synthetic_stale"])
        self.assertIsNone(candidate["data_fetched_at"])
        # 3 attempts to Met Office + 3 attempts to Open-Meteo, all failed.
        self.assertEqual(candidate["synthetic_fetch_attempts"], 6)
        self.assertEqual(candidate["freshness_status"], "stale_synthetic")
        # Placeholder line is still produced so the required block stays
        # non-empty — the gate downgrades to a warning.
        self.assertIn("временно недоступны", candidate["draft_line"])
        self.assertTrue(candidate["include"])
        self.assertEqual(len(candidate["synthetic_warnings"]), 2)


# --------------------------------------------------------------------------
# Transport reminder freshness markers
# --------------------------------------------------------------------------
class TransportReminderFreshnessTest(unittest.TestCase):
    """Past defect: Metrolink reminders rendered for weeks without any
    way for the release report to flag a record that hadn't been
    re-confirmed by a fresh TfGM article."""

    def _record(self, first_seen: str) -> dict:
        return {
            "key": "metrolink|rochdale-line|victoria-rochdale",
            "operator": "Metrolink",
            "line": "Rochdale line",
            "segment": "Victoria – Rochdale Town Centre",
            "end_date": "2026-06-30",
            "first_seen": first_seen,
            "source_url": "https://tfgm.com/travel-updates/rochdale-line",
        }

    def test_fresh_reminder_first_seen_today_not_stale(self) -> None:
        today = date(2026, 5, 18)
        cand = _make_reminder_candidate(self._record("2026-05-18"), today.isoformat())
        self.assertTrue(cand["synthetic"])
        self.assertFalse(cand["synthetic_stale"])
        self.assertEqual(cand["data_fetched_at"], "2026-05-18")
        self.assertEqual(cand["freshness_status"], "reminder")

    def test_reminder_first_seen_two_weeks_ago_flagged_stale(self) -> None:
        today = date(2026, 5, 18)
        old = (today - timedelta(days=15)).isoformat()
        cand = _make_reminder_candidate(self._record(old), today.isoformat())
        self.assertTrue(cand["synthetic_stale"])
        self.assertEqual(cand["freshness_status"], "stale_synthetic")
        # Still included — disruption may genuinely be ongoing; we never
        # silently drop a real Metrolink closure.
        self.assertTrue(cand["include"])
        self.assertEqual(cand["data_fetched_at"], old)

    def test_unparseable_first_seen_treated_as_stale(self) -> None:
        today = date(2026, 5, 18)
        cand = _make_reminder_candidate(self._record(""), today.isoformat())
        self.assertTrue(cand["synthetic_stale"])
        self.assertIsNone(cand["data_fetched_at"])


# --------------------------------------------------------------------------
# Release-level summary
# --------------------------------------------------------------------------
class SyntheticFreshnessSummaryTest(unittest.TestCase):
    def test_summary_counts_only_synthetic_candidates(self) -> None:
        candidates = {
            "candidates": [
                {"synthetic": True, "synthetic_stale": False,
                 "source_label": "Met Office", "primary_block": "weather",
                 "data_fetched_at": "2026-05-18T07:55:00", "synthetic_fetch_attempts": 1},
                {"synthetic": True, "synthetic_stale": True,
                 "source_label": "Metrolink", "primary_block": "transport",
                 "data_fetched_at": "2026-05-01", "synthetic_fetch_attempts": 0},
                # Non-synthetic candidates ignored even if include=True.
                {"source_label": "MEN", "primary_block": "city_watch", "include": True},
            ]
        }
        result = _summarise_synthetic_freshness(candidates)
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["stale_count"], 1)
        self.assertEqual(result["stale_sources"], ["Metrolink"])
        self.assertEqual(len(result["items"]), 2)

    def test_summary_handles_empty_input(self) -> None:
        result = _summarise_synthetic_freshness(None)
        self.assertEqual(result, {"total": 0, "stale_count": 0, "stale_sources": [], "items": []})


if __name__ == "__main__":
    unittest.main()
