from __future__ import annotations

import unittest

from news_digest.pipeline.digest_shape import digest_shape_report


def _candidate(index: int, **overrides) -> dict:
    base = {
        "fingerprint": f"fp-{index}",
        "include": True,
        "title": f"Manchester story {index}",
        "summary": "A Greater Manchester local story.",
        "source_label": f"Source {index}",
        "source_url": f"https://example.com/{index}",
        "category": "media_layer",
        "primary_block": "last_24h",
    }
    base.update(overrides)
    return base


class DigestShapeTest(unittest.TestCase):
    def test_reports_topic_and_source_shape_warnings(self) -> None:
        candidates = []
        for index in range(1, 11):
            candidates.append(
                _candidate(
                    index,
                    title=f"City centre concert {index}",
                    summary="Manchester city centre concert at Piccadilly.",
                    source_label="Same Source",
                    category="culture_weekly",
                    primary_block="next_7_days",
                )
            )
        for index in range(11, 15):
            candidates.append(_candidate(index, source_label=f"Source {index}", title=f"Salford council story {index}"))
        report = digest_shape_report(candidates, [candidate["fingerprint"] for candidate in candidates])
        warning_names = {warning["name"] for warning in report["warnings"]}
        self.assertIn("events_share", warning_names)
        self.assertIn("city_centre_share", warning_names)
        self.assertIn("top_source_share", warning_names)
        self.assertEqual(report["visible_count"], 14)

    def test_clean_shape_has_no_warnings_for_small_balanced_issue(self) -> None:
        candidates = [
            _candidate(1, source_label="BBC", title="Salford council approves homes"),
            _candidate(2, source_label="MEN", title="Stockport transport works start"),
            _candidate(3, source_label="TfGM", category="transport", primary_block="transport", title="Bury tram works"),
            _candidate(4, source_label="GMP", category="gmp", title="Bolton police appeal"),
            _candidate(5, source_label="The Mill", title="Oldham school funding update"),
            _candidate(6, source_label="The Manc", category="culture_weekly", primary_block="next_7_days", title="Trafford theatre opens"),
            _candidate(7, source_label="BusinessCloud", category="tech_business", primary_block="tech_business", title="Rochdale firm adds jobs"),
            _candidate(8, source_label="Manchester City", category="football", primary_block="football", title="Manchester City injury update"),
            _candidate(9, source_label="Council", title="Wigan town centre works"),
            _candidate(10, source_label="Altrincham Today", title="Altrincham market opens"),
            _candidate(11, source_label="Venue", category="venues_tickets", primary_block="ticket_radar", title="Manchester tickets on sale"),
            _candidate(12, source_label="Weather", category="weather", primary_block="weather", title="Greater Manchester weather"),
        ]
        report = digest_shape_report(candidates, [candidate["fingerprint"] for candidate in candidates])
        self.assertEqual(report["warnings"], [])


if __name__ == "__main__":
    unittest.main()
