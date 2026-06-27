"""O1 — Source Freshness Health.

Cover the new per-source yield columns (curated / rendered) added to
`_summarise_source_health` so the release report can answer two
questions from one file:

  1. Which sources are alive vs. failed/empty/stale (existing R1).
  2. Which sources actually contributed material that shipped, and
     which silently dropped out late in the pipeline (new O1).
"""
from __future__ import annotations

import unittest

from news_digest.pipeline.release import (
    _count_per_source_yield,
    _summarise_source_health,
)


def _scan_report_with(*health_entries: dict) -> dict:
    return {
        "categories": {
            "media_layer": {
                "source_health": list(health_entries),
            }
        }
    }


def _candidate(
    source_label: str,
    fingerprint: str,
    include: bool,
) -> dict:
    return {
        "source_label": source_label,
        "fingerprint": fingerprint,
        "include": include,
        "title": f"{source_label} story {fingerprint}",
    }


class PerSourceYieldTest(unittest.TestCase):
    def test_curated_counts_only_included_candidates(self) -> None:
        cands = {
            "candidates": [
                _candidate("MEN", "fp1", include=True),
                _candidate("MEN", "fp2", include=True),
                _candidate("MEN", "fp3", include=False),
                _candidate("BBC Manchester", "fp4", include=True),
            ]
        }
        yields = _count_per_source_yield(cands, rendered_fingerprints=set())
        self.assertEqual(yields["MEN"]["curated"], 2)
        self.assertEqual(yields["BBC Manchester"]["curated"], 1)
        self.assertEqual(yields["MEN"]["rendered"], 0)

    def test_rendered_requires_curated_and_fingerprint_match(self) -> None:
        cands = {
            "candidates": [
                _candidate("MEN", "fp1", include=True),
                _candidate("MEN", "fp2", include=True),
                _candidate("MEN", "fp3", include=False),  # not curated, ignored even if rendered
            ]
        }
        yields = _count_per_source_yield(cands, rendered_fingerprints={"fp1", "fp3", "fp99"})
        self.assertEqual(yields["MEN"]["curated"], 2)
        self.assertEqual(yields["MEN"]["rendered"], 1)  # fp1 only; fp3 wasn't curated

    def test_empty_inputs_return_empty_dict(self) -> None:
        self.assertEqual(_count_per_source_yield(None, None), {})
        self.assertEqual(_count_per_source_yield({"candidates": []}, []), {})


class SourceHealthSummaryWithYieldTest(unittest.TestCase):
    """Yield columns must attach to each existing source row without
    breaking the legacy fields, and synthetic sources (Met Office,
    transport_fill) must show up even though they bypass the collector."""

    def test_yield_columns_attached_to_known_source(self) -> None:
        scan = _scan_report_with(
            {
                "name": "MEN",
                "fetched": True,
                "candidate_count": 20,
                "publishable_count": 12,
                "fresh_last_24h_count": 5,
                "errors": [],
                "warnings": [],
            }
        )
        cands = {
            "candidates": [
                _candidate("MEN", "fp1", include=True),
                _candidate("MEN", "fp2", include=True),
                _candidate("MEN", "fp3", include=False),
            ]
        }
        result = _summarise_source_health(scan, candidates_report=cands, rendered_fingerprints={"fp1"})
        rows = result["sources"]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["name"], "MEN")
        self.assertEqual(row["candidate_count"], 20)
        self.assertEqual(row["curated_count"], 2)
        self.assertEqual(row["rendered_count"], 1)

    def test_synthetic_source_appended_when_not_in_collector(self) -> None:
        # Met Office never appears in collector_report.source_health
        # because the weather card is built outside the core collector.
        scan = _scan_report_with(
            {
                "name": "MEN",
                "fetched": True,
                "candidate_count": 5,
                "publishable_count": 5,
                "fresh_last_24h_count": 5,
            }
        )
        cands = {
            "candidates": [
                _candidate("MEN", "fp1", include=True),
                _candidate("Met Office", "weather-1", include=True),
            ]
        }
        result = _summarise_source_health(
            scan,
            candidates_report=cands,
            rendered_fingerprints={"fp1", "weather-1"},
        )
        names = [(row["name"], row["category"]) for row in result["sources"]]
        self.assertIn(("Met Office", "synthetic"), names)
        synth = next(row for row in result["sources"] if row["name"] == "Met Office")
        self.assertEqual(synth["curated_count"], 1)
        self.assertEqual(synth["rendered_count"], 1)
        self.assertEqual(synth["status"], "ok")

    def test_zero_yield_counter_only_includes_collector_sources(self) -> None:
        # Two real sources, both fetched OK. One contributes nothing.
        scan = _scan_report_with(
            {
                "name": "MEN",
                "fetched": True,
                "candidate_count": 10,
                "publishable_count": 5,
                "fresh_last_24h_count": 5,
            },
            {
                "name": "Quiet Source",
                "fetched": True,
                "candidate_count": 3,
                "publishable_count": 3,
                "fresh_last_24h_count": 3,
            },
        )
        cands = {
            "candidates": [
                _candidate("MEN", "fp1", include=True),
            ]
        }
        result = _summarise_source_health(scan, candidates_report=cands, rendered_fingerprints={"fp1"})
        # Quiet Source: candidate_count=3 from collector, rendered=0 ⇒ zero_yield.
        # MEN: candidate_count=10, rendered=1 ⇒ NOT zero_yield.
        self.assertEqual(result["counts"]["zero_yield"], 1)

    def test_zero_yield_rows_carry_loss_stage_and_reason(self) -> None:
        # W10: each source that contributed nothing must name WHERE it dropped
        # out; a source that shipped carries no attribution (no noise).
        scan = _scan_report_with(
            {"name": "MEN", "fetched": True, "candidate_count": 10, "publishable_count": 5, "fresh_last_24h_count": 5},
            {"name": "Quiet Source", "fetched": True, "candidate_count": 3, "publishable_count": 3, "fresh_last_24h_count": 3},
        )
        cands = {"candidates": [_candidate("MEN", "fp1", include=True)]}
        result = _summarise_source_health(scan, candidates_report=cands, rendered_fingerprints={"fp1"})
        rows = {row["name"]: row for row in result["sources"]}
        self.assertNotIn("loss_stage", rows["MEN"])  # shipped → no attribution
        self.assertEqual(rows["Quiet Source"]["loss_stage"], "selected")  # 3 candidates, 0 selected
        self.assertTrue(rows["Quiet Source"]["loss_reason"])
        self.assertEqual(result["counts"]["zero_yield_by_stage"].get("selected"), 1)

    def test_synthetic_with_curated_but_no_rendered_marked_partial(self) -> None:
        cands = {
            "candidates": [
                _candidate("Met Office", "weather-1", include=True),
            ]
        }
        result = _summarise_source_health(
            scan_report=None,
            candidates_report=cands,
            rendered_fingerprints=set(),  # writer killed the weather card
        )
        synth = next(row for row in result["sources"] if row["name"] == "Met Office")
        self.assertEqual(synth["status"], "partial")
        self.assertEqual(synth["curated_count"], 1)
        self.assertEqual(synth["rendered_count"], 0)

    def test_backwards_compat_when_extra_inputs_missing(self) -> None:
        """Calling the function with only the scan_report (legacy call)
        must still work and just default yield columns to 0."""
        scan = _scan_report_with(
            {
                "name": "MEN",
                "fetched": True,
                "candidate_count": 5,
                "publishable_count": 5,
                "fresh_last_24h_count": 5,
            }
        )
        result = _summarise_source_health(scan)
        row = result["sources"][0]
        self.assertEqual(row["curated_count"], 0)
        self.assertEqual(row["rendered_count"], 0)
        # zero_yield counter present even with no candidates context.
        self.assertIn("zero_yield", result["counts"])

    def test_event_calendar_uses_coverage_signal_not_fresh_24h(self) -> None:
        scan = {
            "categories": {
                "culture_weekly": {
                    "source_health": [
                        {
                            "name": "Visit Manchester Weekend",
                            "fetched": True,
                            "candidate_count": 12,
                            "fresh_last_24h_count": 0,
                            "source_contract": "event_calendar",
                            "coverage_signal_count": 12,
                            "coverage_signal_label": "upcoming dated items",
                            "errors": [],
                            "warnings": [],
                        }
                    ]
                }
            }
        }
        result = _summarise_source_health(scan)
        row = result["sources"][0]
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["source_contract"], "event_calendar")
        self.assertEqual(row["coverage_signal_count"], 12)

    def test_hard_news_still_requires_fresh_publication_signal(self) -> None:
        scan = _scan_report_with(
            {
                "name": "Core News",
                "fetched": True,
                "candidate_count": 8,
                "fresh_last_24h_count": 0,
                "source_contract": "hard_news_daily",
                "coverage_signal_count": 0,
                "coverage_signal_label": "fresh published items",
                "errors": [],
                "warnings": [],
            }
        )
        result = _summarise_source_health(scan)
        row = result["sources"][0]
        self.assertEqual(row["status"], "stale")
        self.assertIn("0 fresh", row["detail"])


if __name__ == "__main__":
    unittest.main()
