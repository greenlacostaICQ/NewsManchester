from __future__ import annotations

from datetime import datetime
import unittest
from unittest import mock
from zoneinfo import ZoneInfo

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

    def test_the_manc_weekly_sections_become_food_event_candidates(self) -> None:
        source = SourceDef(
            name="The Manc Weekly Things To Do",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://themanc.com/whats-on-manchester/the-best-things-to-do-in-greater-manchester-this-week-11-17-may-2026/",
            primary_block="weekend_activities",
            source_type="html_the_manc_weekly_events",
            allowed_hosts=("themanc.com",),
            max_candidates=12,
        )
        html = """
        <article>
          <h3>The Flat Baker Pistachio Festival</h3>
          <p>Ancoats Saturday 16 May.</p>
          <p>A festival dedicated to pistachio bakes, desserts and drinks is back in Manchester this weekend.</p>
          <p>Find the bakery event through The Flat Baker source before travelling.</p>
          <h3>Something unrelated</h3>
          <p>A short listing with no practical details.</p>
        </article>
        """

        [candidate] = _extract_source_candidates(source, html)

        self.assertEqual(candidate["title"], "The Flat Baker Pistachio Festival")
        self.assertIn("#the-flat-baker-pistachio-festival", candidate["source_url"])
        self.assertTrue(event_quality_report(candidate)["ok"])

    def test_sectioned_event_guide_extracts_individual_weekend_picks(self) -> None:
        source = SourceDef(
            name="Secret Manchester May Guide",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://secretmanchester.com/things-to-do-in-may/",
            primary_block="weekend_activities",
            source_type="html_sectioned_event_guide",
            allowed_hosts=("secretmanchester.com",),
            max_candidates=12,
        )
        html = """
        <article>
          <h2>Visit Wythenshawe Park &amp; Gardens for a huge food and drink festival</h2>
          <p>Wythenshawe Park, Wythenshawe Road, Manchester, M23 0AB</p>
          <p>16 May 2026 – 17 May 2026</p>
          <p>The festival has live chef demos, live music, artisan markets and family-friendly activities.</p>
          <h2>Subscribe to our newsletter</h2>
          <p>Get offers by email.</p>
        </article>
        """

        [candidate] = _extract_source_candidates(source, html)

        self.assertEqual(candidate["title"], "Visit Wythenshawe Park & Gardens for a huge food and drink festival")
        self.assertEqual(candidate["primary_block"], "weekend_activities")
        self.assertIn("Wythenshawe Park", candidate["evidence_text"])
        self.assertTrue(event_quality_report(candidate)["ok"])

    def test_sectioned_event_guide_keeps_short_gig_titles(self) -> None:
        source = SourceDef(
            name="Secret Manchester Gigs",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://secretmanchester.com/gigs-in-manchester/",
            primary_block="weekend_activities",
            source_type="html_sectioned_event_guide",
            allowed_hosts=("secretmanchester.com",),
            max_candidates=12,
        )
        html = """
        <article>
          <h2>Kraftwerk</h2>
          <p>O2 Apollo Manchester May 22, 23.</p>
          <p>The band brings its Multimedia Tour to Manchester with tickets available.</p>
        </article>
        """

        [candidate] = _extract_source_candidates(source, html)

        self.assertEqual(candidate["title"], "Kraftwerk")
        self.assertIn("O2 Apollo", candidate["evidence_text"])

    def test_rncm_cards_extract_from_aria_event_markup(self) -> None:
        source = SourceDef(
            name="RNCM",
            report_category="venues_tickets",
            candidate_category="venues_tickets",
            url="https://www.rncm.ac.uk/whats-on/",
            primary_block="next_7_days",
            allowed_hosts=("rncm.ac.uk",),
            max_candidates=5,
        )
        html = """
        <div class="event tab-3 dts-3 cf">
          <a href="https://www.rncm.ac.uk/performance/rickie-lee-jones/" aria-label="Rickie Lee Jones">
            <div class="event-picture"></div>
          </a>
          <div class="event-date">May 30<span>th</span></div>
          <div class="event-details">
            <div class="title"><h2>Rickie Lee Jones</h2><span>Senbla Ltd</span></div>
          </div>
        </div>
        """

        # RNCM cards carry a bare "May 30" with no year; the extractor rolls a
        # past date forward to next year. Freeze "today" so the bare month-day
        # resolves deterministically (otherwise the test flakes after May 30).
        frozen = datetime(2026, 5, 15, 12, 0, tzinfo=ZoneInfo("Europe/London"))
        with mock.patch(
            "news_digest.pipeline.collector.extract.now_london",
            return_value=frozen,
        ):
            [candidate] = _extract_source_candidates(source, html)

        self.assertEqual(candidate["title"], "Rickie Lee Jones")
        self.assertEqual(candidate["published_date_london"], "2026-05-30")
        self.assertEqual(candidate["source_url"], "https://rncm.ac.uk/performance/rickie-lee-jones")

    def test_skiddle_cards_extract_event_link_and_date(self) -> None:
        source = SourceDef(
            name="Skiddle Manchester Bank Holiday",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://www.skiddle.com/whats-on/manchester/may-bank-holiday-events/",
            primary_block="weekend_activities",
            allowed_hosts=("skiddle.com",),
            max_candidates=10,
        )
        html = """
        <a href="https://www.skiddle.com/whats-on/Manchester/Bowlers-Exhibition-Centre/Manchester-Forever/42415400/">
          <img alt="Manchester Forever at Bowlers Exhibition Centre">
          <span>Manchester Forever Saturday 1st May 2027 2:00pm - 11:45pm</span>
          <span>Bowlers Exhibition Centre, Manchester</span>
        </a>
        """

        [candidate] = _extract_source_candidates(source, html)

        self.assertEqual(candidate["title"], "Manchester Forever at Bowlers Exhibition Centre")
        self.assertEqual(candidate["published_date_london"], "2027-05-01")

    def test_manchester_academy_slug_supplies_event_date(self) -> None:
        source = SourceDef(
            name="Manchester Academy",
            report_category="venues_tickets",
            candidate_category="venues_tickets",
            url="https://www.manchesteracademy.net/",
            primary_block="ticket_radar",
            allowed_hosts=("manchesteracademy.net",),
            max_candidates=20,
        )
        html = """
        <a href="/order/gateway/13380549/jamie-webster-manchester-academy-2026-09-11-19-00-00">
          Jamie Webster + support Fri 11th Sep, 19:00 On sale Fri 29th May, 10:00
        </a>
        """

        [candidate] = _extract_source_candidates(source, html)

        self.assertIn("Jamie Webster", candidate["title"])
        self.assertEqual(candidate["published_date_london"], "2026-09-11")

    def test_car_boot_without_ticket_language_can_pass_with_source(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Burnage RFC Car Boot Sale",
            "summary": "Every Sunday from 10 May to 30 August at Burnage RFC, Varley Park, Stockport.",
            "evidence_text": "Dates Every Sunday. Location Burnage RFC, Varley Park, Battersea Road, Stockport SK4 3EA.",
            "source_url": "https://www.manchester-rocks.co.uk/things-to-do/burnage-rfc-car-boot-sale",
            "source_label": "Burnage RFC Car Boot Sale",
        }

        report = event_quality_report(candidate)

        self.assertTrue(report["checks"]["date"])
        self.assertTrue(report["checks"]["access"])
        self.assertTrue(report["ok"])

    def test_place_name_preservation_normalizes_russian_transliteration(self) -> None:
        line = "• Рынок: Фёрст Стрит и Нортерн Квортер доступны на выходных."

        self.assertEqual(
            preserve_place_names(line),
            "• Рынок: First Street и Northern Quarter доступны на выходных.",
        )


if __name__ == "__main__":
    unittest.main()
