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

from news_digest.pipeline.collector import fallbacks, weather
from news_digest.pipeline.collector.weather import _met_office_practical_angle
from news_digest.pipeline.release import _summarise_synthetic_freshness
from news_digest.pipeline.transport_card import TransportCard
from news_digest.pipeline.transport_fill import _make_reminder_candidate, _persistent_tram_record


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

    def test_high_rain_probability_is_not_written_as_heavy_rain_by_itself(self) -> None:
        practical = _met_office_practical_angle(
            "Bright spells.",
            "Sunny intervals, with a few showers possible later.",
            70,
        )

        self.assertIn("дожд", practical.lower())
        self.assertIn("защиту от дождя", practical.lower())
        self.assertNotIn("сильные осадки", practical)
        self.assertNotIn("зонт обязателен", practical)

    def test_heavy_rain_wording_requires_met_office_rain_prose(self) -> None:
        practical = _met_office_practical_angle(
            "Heavy rain possible later.",
            "Cloudy with persistent rain arriving in the evening.",
            70,
        )

        self.assertIn("сильные осадки", practical)
        self.assertNotIn("радар", practical.lower())
        self.assertIn("зонт", practical.lower())

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
        self.assertEqual(candidate["weather_facts"]["status"], "live")
        self.assertFalse(candidate["weather_facts"]["degraded"])
        # Live data path must include concrete morning/day temperatures.
        self.assertIn("днём до 18°", candidate["draft_line"])

    def test_weather_facts_make_28c_plain_not_heat_warning(self) -> None:
        html = """
        <div id="2026-05-22" class="forecast-table-section">
          <table><tbody>
            <tr class="step-time">
              <td><div class="time-step-hours">08:00</div></td>
              <td><div class="time-step-hours">14:00</div></td>
              <td><div class="time-step-hours">18:00</div></td>
            </tr>
            <tr><th class="tooltip-header">Weather symbols</th>
              <td><img alt="Sunny intervals"></td><td><img alt="Sunny day"></td><td><img alt="Clear night"></td>
            </tr>
            <tr><th class="tooltip-header">Chance of precipitation</th>
              <td><div data-value="5">&lt;5%</div></td>
              <td><div data-value="10">10%</div></td>
              <td><div data-value="10">10%</div></td>
            </tr>
            <tr><th class="tooltip-header">Temperature</th>
              <td><div data-unit="temperature" data-c="20°">20°</div></td>
              <td><div data-unit="temperature" data-c="28°">28°</div></td>
              <td><div data-unit="temperature" data-c="24°">24°</div></td>
            </tr>
          </tbody></table>
        </div>
        """

        with mock.patch.object(weather, "today_london", return_value="2026-05-22"):
            facts = weather._extract_met_office_weather_facts(html)
        line = fallbacks._weather_draft_line(20, 28, 10, "", "Met Office", facts)

        self.assertEqual(facts["morning_temp_c"], 20)
        self.assertEqual(facts["max_temp_c"], 28)
        self.assertEqual(facts["rain_probability_max"], 10)
        self.assertEqual(len(facts["hourly"]), 3)
        self.assertEqual(facts["warnings"], [])
        self.assertIn("утром ~20°, днём до 28°", line)
        self.assertNotIn("очень тепло", line)
        self.assertNotIn("жарко", line)
        self.assertNotIn("предупреждение", line.lower())

    def test_weather_heat_wording_requires_impact_threshold(self) -> None:
        base_facts = {
            "morning_temp_c": 20,
            "hourly": [{"hour": 8, "temperature_c": 20, "rain_probability": 0}],
            "warnings": [],
        }

        warm_line = fallbacks._weather_draft_line(20, 29, 0, "", "Met Office", base_facts)
        hot_line = fallbacks._weather_draft_line(20, 30, 0, "", "Met Office", base_facts)

        self.assertNotIn("очень тепло", warm_line)
        self.assertNotIn("жарко", warm_line)
        self.assertIn("очень тепло", hot_line)

    def test_met_office_v2_parser_scopes_precipitation_to_today_section(self) -> None:
        html = """
        <div id="2026-05-22" class="forecast-table-section">
          <table><tbody>
            <tr class="step-time">
              <td><div class="time-step-hours">14:00</div></td>
              <td><div class="time-step-hours">21:00</div></td>
              <td><div class="time-step-hours">23:00</div></td>
            </tr>
            <tr><th class="tooltip-header">Weather symbols</th>
              <td><img alt="Sunny intervals"></td><td><img alt="Sunny day"></td>
            </tr>
            <tr><th class="tooltip-header">Chance of precipitation</th>
              <td><div data-value="0">&lt;5%</div></td>
              <td><div data-value="0">&lt;5%</div></td>
              <td><div data-value="43">40%</div></td>
            </tr>
            <tr><th class="tooltip-header">Temperature</th>
              <td><div data-unit="temperature" data-c="25°">25°</div></td>
              <td><div data-unit="temperature" data-c="18°">18°</div></td>
              <td><div data-unit="temperature" data-c="17°">17°</div></td>
            </tr>
          </tbody></table>
        </div>
        <div id="2026-05-23" class="forecast-table-section">
          <table><tbody>
            <tr><th class="tooltip-header">Chance of precipitation</th>
              <td><div data-value="95">95%</div></td>
            </tr>
            <tr><th class="tooltip-header">Temperature</th>
              <td><div data-unit="temperature" data-c="10°">10°</div></td>
            </tr>
          </tbody></table>
        </div>
        """

        with mock.patch.object(weather, "today_london", return_value="2026-05-22"):
            min_temp, max_temp, rain_probability, practical = weather._extract_met_office_weather(html)

        self.assertEqual((min_temp, max_temp, rain_probability), (17, 25, 0))
        self.assertEqual(practical, "Днём сухо с прояснениями.")

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
        self.assertEqual(candidate["weather_facts"]["source"], "Open-Meteo")
        self.assertIn("днём до 15°", candidate["draft_line"])

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
        self.assertTrue(candidate["synthetic_degraded"])
        self.assertTrue(candidate["weather_facts"]["degraded"])
        self.assertTrue(candidate["weather_facts"]["placeholder"])


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

    def test_rejected_tfgm_news_can_still_persist_bounded_movement_restriction(self) -> None:
        candidate = {
            "include": False,
            "source_label": "TfGM",
            "source_url": "https://tfgm.com/travel-updates/bury-line-closure",
            "title": "Metrolink Bury line closed while engineering work takes place",
            "summary": "No tram service between Victoria and Bury; replacement buses operate.",
        }
        card = TransportCard(
            mode="tram",
            operator="Metrolink",
            line="Bury line",
            segment="Victoria – Bury",
            end_date="31 июля",
        )
        record = _persistent_tram_record(candidate, card, date(2026, 7, 21))
        self.assertIsNotNone(record)
        self.assertEqual(record["end_date"], "2026-07-31")
        self.assertEqual(record["last_confirmed"], "2026-07-21")

    def test_persistence_rejects_unbounded_or_no_movement_cards(self) -> None:
        base = {
            "source_label": "TfGM",
            "source_url": "https://tfgm.com/travel-updates/derker",
            "title": "Metrolink update",
        }
        unbounded = TransportCard(mode="tram", operator="Metrolink", line="Oldham line")
        self.assertIsNone(_persistent_tram_record(
            {**base, "summary": "Trams are delayed on the Oldham line."},
            unbounded,
            date(2026, 7, 21),
        ))
        bounded = TransportCard(
            mode="tram", operator="Metrolink", line="Oldham line", end_date="31 июля"
        )
        self.assertIsNone(_persistent_tram_record(
            {**base, "summary": "Trams are not affected by the lift works."},
            bounded,
            date(2026, 7, 21),
        ))


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
        self.assertEqual(result["degraded_count"], 1)
        self.assertEqual(result["degraded_sources"], ["Metrolink"])
        self.assertEqual(len(result["items"]), 2)

    def test_weather_placeholder_is_degraded_in_release_summary(self) -> None:
        candidates = {
            "candidates": [
                {
                    "synthetic": True,
                    "synthetic_stale": True,
                    "synthetic_degraded": True,
                    "source_label": "Met Office",
                    "primary_block": "weather",
                    "data_fetched_at": None,
                    "synthetic_fetch_attempts": 6,
                    "weather_facts": {
                        "status": "degraded_placeholder",
                        "source": "Met Office",
                        "hourly": [],
                        "max_temp_c": None,
                        "rain_probability_max": None,
                        "warnings": [],
                        "placeholder": True,
                        "degraded": True,
                    },
                },
            ]
        }

        result = _summarise_synthetic_freshness(candidates)

        self.assertEqual(result["degraded_count"], 1)
        self.assertEqual(result["degraded_sources"], ["Met Office"])
        self.assertTrue(result["items"][0]["degraded"])
        self.assertTrue(result["items"][0]["weather_facts"]["placeholder"])
        self.assertEqual(result["items"][0]["weather_facts"]["status"], "degraded_placeholder")

    def test_summary_handles_empty_input(self) -> None:
        result = _summarise_synthetic_freshness(None)
        self.assertEqual(
            result,
            {
                "total": 0,
                "stale_count": 0,
                "stale_sources": [],
                "degraded_count": 0,
                "degraded_sources": [],
                "items": [],
            },
        )


if __name__ == "__main__":
    unittest.main()
