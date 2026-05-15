from __future__ import annotations

import unittest

from news_digest.pipeline.collector.extract import _extract_source_candidates
from news_digest.pipeline.collector.sources import SourceDef
from news_digest.pipeline.event_quality import event_quality_report
from news_digest.pipeline.place_names import preserve_place_names


class MarketEventSourcesTest(unittest.TestCase):
    def test_html_page_event_keeps_direct_market_page(self) -> None:
        source = SourceDef(
            name="First Street Makers Market",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://pedddle.com/market/first-street-makers-market-manchester/",
            primary_block="weekend_activities",
            source_type="html_page_event",
            allowed_hosts=("pedddle.com",),
            max_candidates=1,
        )
        html = """
        <html>
          <head>
            <meta property="og:description" content="Second Saturday of each month at First Street Manchester M15 4FN. Free Entry.">
          </head>
          <body>
            <main>
              <h1>First Street Makers Market</h1>
              <p>Address First Street Manchester M15 4FN.</p>
              <p>Date Second Saturday of each month. Time 11:00am - 5:00pm.</p>
              <p>Facilities All Outdoor Seating Available Dog Friendly Free Entry.</p>
            </main>
          </body>
        </html>
        """

        [candidate] = _extract_source_candidates(source, html)

        self.assertEqual(candidate["title"], "First Street Makers Market")
        self.assertEqual(candidate["source_url"], source.url.rstrip("/"))
        self.assertEqual(candidate["primary_block"], "weekend_activities")
        self.assertIn("Second Saturday", candidate["evidence_text"])
        self.assertIn("Free Entry", candidate["evidence_text"])
        self.assertTrue(event_quality_report(candidate)["ok"])

    def test_recurring_market_schedule_satisfies_event_date_gate(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Northern Quarter Makers Market",
            "summary": "Second Sunday of every month at Oak Street Manchester M4 5JD. Free Entry.",
            "evidence_text": "Date Second Sunday of every month. Time 11:00am - 5:00pm. Facilities Free Entry.",
            "source_url": "https://pedddle.com/market/northern-quarter-makers-market/",
            "source_label": "Northern Quarter Makers Market",
        }

        report = event_quality_report(candidate)

        self.assertTrue(report["checks"]["date"])
        self.assertTrue(report["ok"])

    def test_place_name_preservation_normalizes_russian_transliteration(self) -> None:
        line = "• Рынок: Фёрст Стрит и Нортерн Квортер доступны на выходных."

        self.assertEqual(
            preserve_place_names(line),
            "• Рынок: First Street и Northern Quarter доступны на выходных.",
        )


if __name__ == "__main__":
    unittest.main()
