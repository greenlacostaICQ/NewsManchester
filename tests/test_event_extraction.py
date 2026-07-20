"""Tests for I3 structured event extraction.

Covers:
  - is_event_candidate gate (which categories opt in)
  - date parsing (ISO, English, Russian, ranges, year rollover)
  - price parsing (£N, £N-£M, "от £N", "from £N", "free", "бесплатно")
  - venue resolution (entities vs "at <Name>" fallback)
  - booking URL detection (ticket-native categories vs scan)
  - event_name cleanup (source suffix, age cert)
  - is_event truthiness rule (name AND (date OR venue))
  - end-to-end on candidates that mirror real shapes we've seen in
    data/state/candidates.json
"""
from __future__ import annotations

import unittest
from datetime import date
from unittest import mock

from news_digest.pipeline import event_extraction
from news_digest.pipeline.event_extraction import (
    EVENT_SCHEMA_VERSION,
    enrich_candidate_event,
    enrich_candidates_events,
    event_active_on,
    event_date_is_trustworthy,
    event_is_far_future,
    event_is_multi_day,
    extract_event,
    is_event_candidate,
    _parse_date_details,
    _parse_date_from_blob,
    _extract_price,
)


class IsEventCandidateTest(unittest.TestCase):
    def test_culture_weekly_always_event(self):
        self.assertTrue(is_event_candidate({
            "category": "culture_weekly", "primary_block": "next_7_days",
        }))

    def test_venues_tickets_always_event(self):
        self.assertTrue(is_event_candidate({
            "category": "venues_tickets", "primary_block": "ticket_radar",
        }))

    def test_russian_speaking_events(self):
        self.assertTrue(is_event_candidate({
            "category": "russian_speaking_events", "primary_block": "russian_events",
        }))

    def test_food_openings_in_weekend_block_is_event(self):
        self.assertTrue(is_event_candidate({
            "category": "food_openings", "primary_block": "weekend_activities",
        }))

    def test_food_openings_in_openings_block_is_event(self):
        self.assertTrue(is_event_candidate({
            "category": "food_openings", "primary_block": "openings",
        }))

    def test_gmp_is_not_event(self):
        self.assertFalse(is_event_candidate({
            "category": "gmp", "primary_block": "last_24h",
        }))

    def test_transport_is_not_event(self):
        self.assertFalse(is_event_candidate({
            "category": "transport", "primary_block": "transport",
        }))

    def test_media_layer_is_not_event(self):
        self.assertFalse(is_event_candidate({
            "category": "media_layer", "primary_block": "last_24h",
        }))


class DateParsingTest(unittest.TestCase):
    """All date-only tests pin ``today`` so they are deterministic
    across release cycles."""

    def setUp(self):
        self.today = date(2026, 5, 19)  # mid-May 2026

    def test_iso_date(self):
        iso, txt = _parse_date_from_blob("Concert on 2026-06-15", today=self.today)
        self.assertEqual(iso, "2026-06-15")
        self.assertEqual(txt, "2026-06-15")

    def test_english_day_month_with_year(self):
        iso, txt = _parse_date_from_blob("Show on 15 June 2026", today=self.today)
        self.assertEqual(iso, "2026-06-15")

    def test_english_day_month_no_year_future(self):
        iso, _ = _parse_date_from_blob("Show on 15 June", today=self.today)
        self.assertEqual(iso, "2026-06-15")  # future month → this year

    def test_english_day_month_no_year_rolls_over(self):
        # March is before May → assume next year
        iso, _ = _parse_date_from_blob("Show on 15 March", today=self.today)
        self.assertEqual(iso, "2027-03-15")

    def test_russian_day_month(self):
        iso, txt = _parse_date_from_blob("Концерт 16 мая в Манчестере", today=self.today)
        self.assertEqual(iso, "2026-05-16")
        self.assertIn("16", txt)
        self.assertIn("мая", txt)

    def test_russian_date_with_year(self):
        iso, _ = _parse_date_from_blob("Концерт 16 мая 2027 года", today=self.today)
        self.assertEqual(iso, "2027-05-16")

    def test_english_day_range(self):
        iso, txt = _parse_date_from_blob("16-17 May at the venue", today=self.today)
        self.assertEqual(iso, "2026-05-16")
        self.assertIn("16", txt)

    def test_russian_day_range(self):
        iso, txt = _parse_date_from_blob("Фестиваль 16-17 мая", today=self.today)
        self.assertEqual(iso, "2026-05-16")
        self.assertIn("16", txt)
        self.assertIn("17", txt)
        self.assertIn("мая", txt)

    def test_no_date_returns_empty(self):
        iso, txt = _parse_date_from_blob("This is just text without dates", today=self.today)
        self.assertEqual(iso, "")
        self.assertEqual(txt, "")

    def test_invalid_day_falls_through(self):
        # 32 May is not a date — should be ignored, not crash
        iso, _ = _parse_date_from_blob("32 May 2026", today=self.today)
        # Either no match or falls through to another regex; key: no exception
        # and not 2026-05-32.
        self.assertNotEqual(iso, "2026-05-32")


class PriceParsingTest(unittest.TestCase):
    def test_single_pound_price(self):
        self.assertEqual(_extract_price("Tickets £15 per person"), "£15")

    def test_decimal_price_kept(self):
        self.assertEqual(_extract_price("Tickets £15.50"), "£15.50")

    def test_decimal_price_dot_zero_stripped(self):
        self.assertEqual(_extract_price("Tickets £15.00"), "£15")

    def test_price_range_with_dash(self):
        self.assertEqual(_extract_price("Tickets £15-£75"), "£15–75")

    def test_price_from_english(self):
        self.assertEqual(_extract_price("Tickets from £49.99"), "from £49.99")

    def test_price_from_russian(self):
        self.assertEqual(_extract_price("Билеты от £19.99"), "от £19.99")

    def test_free_english(self):
        self.assertEqual(_extract_price("Free entry for all"), "free")

    def test_free_russian(self):
        self.assertEqual(_extract_price("Вход свободный"), "free")

    def test_no_price(self):
        self.assertEqual(_extract_price("Concert at the venue"), "")

    def test_booking_fee_is_not_event_price(self):
        self.assertEqual(_extract_price("Tickets sold by venue. Booking fee £4.75 applies per order."), "")


class ExtractEventTest(unittest.TestCase):
    def test_manchesters_finest_listing_prefix_supplies_evidenced_venue(self):
        examples = (
            (
                "Green Island Festival 2026 Vol. 2",
                "Sat 25th Jul, 2026 - Warwick Street Green Island Festival 2026 Vol. 2",
                "Warwick Street",
            ),
            (
                "Comic Con Manchester 2026",
                "Sat 25th Jul, 2026 - Sun 26th Jul, 2026 - Bowlers Exhibition Centre Comic Con Manchester 2026",
                "Bowlers Exhibition Centre",
            ),
        )
        for title, evidence, expected_venue in examples:
            with self.subTest(title=title):
                candidate = {
                    "title": title,
                    "summary": "A public event in Manchester on 25 July 2026.",
                    "evidence_text": evidence,
                    "category": "culture_weekly",
                    "primary_block": "weekend_activities",
                    "source_label": "Manchester's Finest Events",
                    "source_url": "https://www.manchestersfinest.com/events/example/",
                }

                result = extract_event(candidate)

                self.assertEqual(result["venue"], expected_venue)
                self.assertEqual(result["date_start"], "2026-07-25")

    def test_returns_empty_for_non_event_category(self):
        self.assertEqual(extract_event({
            "category": "gmp", "primary_block": "last_24h",
            "title": "Police arrest two",
        }), {})

    def test_schema_version_set(self):
        result = extract_event({
            "category": "culture_weekly", "primary_block": "next_7_days",
            "title": "Some show",
        })
        self.assertEqual(result.get("schema_version"), EVENT_SCHEMA_VERSION)

    def test_full_event_with_entities(self):
        c = {
            "title": "Akram Khan Outwitting the Devil",
            "summary": "Dance show 10-14 May at HOME. Tickets from £15.",
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "source_label": "HOME",
            "source_url": "https://homemcr.org/whats-on/akram-khan",
            "entities": {
                "venues": ["HOME"],
                "boroughs": ["Manchester"],
            },
        }
        result = extract_event(c)
        self.assertEqual(result["venue"], "HOME")
        self.assertEqual(result["borough"], "Manchester")
        self.assertTrue(result["date"].startswith("2"))  # ISO date pinned
        self.assertIn("10", result["date_text"])
        self.assertIn("from", result["price"].lower())
        self.assertTrue(result["is_event"])
        self.assertEqual(result["schema_version"], EVENT_SCHEMA_VERSION)

    def test_ticket_native_source_url_is_booking_url(self):
        c = {
            "title": "Concert at Ticketmaster",
            "summary": "Onsale Friday",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "source_url": "https://www.ticketmaster.co.uk/event/12345",
        }
        result = extract_event(c)
        self.assertEqual(result["booking_url"], "https://www.ticketmaster.co.uk/event/12345")

    def test_booking_url_scanned_in_evidence_for_non_ticket_cat(self):
        c = {
            "title": "Festival at HOME",
            "summary": "16 May at HOME.",
            "evidence_text": "Tickets at https://www.eventbrite.co.uk/e/12345",
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "source_url": "https://homemcr.org/news/festival",
            "entities": {"venues": ["HOME"], "boroughs": ["Manchester"]},
        }
        result = extract_event(c)
        self.assertEqual(result["booking_url"], "https://www.eventbrite.co.uk/e/12345")

    def test_is_event_false_without_date_and_venue(self):
        c = {
            "title": "Generic show announcement",
            "summary": "More details to follow.",
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "source_url": "https://example.com/whats-on",
        }
        result = extract_event(c)
        self.assertFalse(result["is_event"])

    def test_is_event_false_with_only_venue(self):
        # A known venue is useful context, but a structured event now requires
        # event_name + date + venue/url so listings do not pass as valid events.
        c = {
            "title": "Some show",
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "entities": {"venues": ["HOME"]},
        }
        result = extract_event(c)
        self.assertFalse(result["is_event"])
        self.assertEqual(result["venue"], "HOME")
        self.assertEqual(result["date"], "")

    def test_russian_concert_real_shape(self):
        c = {
            "title": "Manchester Academy 24 марта — концерт Би-2 от EventCartel",
            "summary": "Двери в 19:00. Билеты £69.75 плюс сбор.",
            "category": "russian_speaking_events",
            "primary_block": "russian_events",
            "source_url": "https://www.ticketline.co.uk/order/tickets/12345",
            "entities": {"venues": ["Manchester Academy"], "boroughs": ["Manchester"]},
        }
        result = extract_event(c)
        self.assertEqual(result["venue"], "Manchester Academy")
        self.assertEqual(result["price"], "£69.75")
        self.assertTrue(result["date"].endswith("-03-24"))
        self.assertEqual(result["booking_url"], "https://www.ticketline.co.uk/order/tickets/12345")
        self.assertTrue(result["is_event"])

    def test_free_market_real_shape(self):
        c = {
            "title": "Makers Market в First Street 16-17 мая",
            "summary": "двухдневный Makers Market, локальные товары, вход свободный",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_url": "https://firststreetmanchester.com/news/makers-market",
            "entities": {"boroughs": ["Manchester"], "districts": ["First Street"]},
        }
        # Freeze "today" so the bare "16-17 мая" (no year) resolves to 2026 and
        # the assertion is not brittle to the calendar moving past May.
        with mock.patch.object(event_extraction, "_today_london", return_value=date(2026, 5, 10)):
            result = extract_event(c)
        self.assertEqual(result["price"], "free")
        self.assertTrue(result["date"].startswith("2026-05-1"))
        self.assertIn("17", result["date_text"])

    def test_stockport_events_uk_slash_date_and_location(self):
        c = {
            "title": "Author talk with Antonia Grave - Stockport Council",
            "summary": (
                "Local author, Antonia Grave will be talking about her crime thriller book, Six Strikes. "
                "Location: Great Moor Library, 12 Gladstone Street, Stockport, SK2 7QF "
                "Fee: Free Event Date and Time: 12/06/2026 18:00 - 19:00"
            ),
            "category": "council",
            "primary_block": "weekend_activities",
            "source_url": "https://www.stockport.gov.uk/events/author-talk-antonia-grave",
        }
        result = extract_event(c)
        self.assertEqual(result["date_start"], "2026-06-12")
        self.assertEqual(result["venue"], "Great Moor Library, 12 Gladstone Street")
        self.assertEqual(result["price"], "free")
        self.assertTrue(result["is_event"])


class EnrichCandidateEventTest(unittest.TestCase):
    def test_enrich_idempotent(self):
        c = {
            "title": "Show 16 May at HOME",
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "entities": {"venues": ["HOME"]},
        }
        enrich_candidate_event(c)
        first = dict(c["event"])
        enrich_candidate_event(c)  # second pass
        self.assertEqual(c["event"], first)

    def test_enrich_overwrites_stale_event(self):
        # If entities change (e.g. dedupe re-enrichment), event must
        # reflect the new entities, not silently stay stale.
        c = {
            "title": "Show 16 May at HOME",
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "entities": {"venues": ["HOME"]},
            "event": {"schema_version": 1, "venue": "OLD_VENUE", "is_event": True},
        }
        enrich_candidate_event(c)
        self.assertEqual(c["event"]["venue"], "HOME")

    def test_enrich_non_event_clears_to_empty(self):
        c = {
            "title": "Police arrest two",
            "category": "gmp",
            "primary_block": "last_24h",
            "event": {"schema_version": 1, "is_event": True},  # stale from prior run
        }
        enrich_candidate_event(c)
        self.assertEqual(c["event"], {})

    def test_enrich_candidates_events_batch(self):
        candidates = [
            {"title": "Concert at HOME 16 May", "category": "culture_weekly",
             "primary_block": "next_7_days", "entities": {"venues": ["HOME"]}},
            {"title": "Police news", "category": "gmp", "primary_block": "last_24h"},
            "not-a-dict",  # collector occasionally yields bad shapes
        ]
        enrich_candidates_events(candidates)
        self.assertTrue(candidates[0]["event"]["is_event"])
        self.assertEqual(candidates[1]["event"], {})
        self.assertEqual(candidates[2], "not-a-dict")


class CanonicalEventFactsTest(unittest.TestCase):
    """W1 / RC3: one module yields date range, free, confidence, and the
    consumer helpers the event blocks + repeat policy read."""

    def setUp(self):
        self.today = date(2026, 6, 27)

    def _culture(self, **extra):
        c = {"category": "culture_weekly", "primary_block": "next_7_days"}
        c.update(extra)
        return c

    def test_multi_day_range_populates_date_end(self):
        parsed = _parse_date_details("Didsbury Arts Festival 27 June – 5 July", today=self.today)
        self.assertEqual(parsed.start, "2026-06-27")
        self.assertEqual(parsed.end, "2026-07-05")
        self.assertEqual(parsed.confidence, "high")

    def test_single_date_has_no_end(self):
        parsed = _parse_date_details("Show on 15 July 2026", today=self.today)
        self.assertEqual(parsed.end, "")

    def test_confidence_low_for_rolled_far_future_bare_month(self):
        # "May 2" seen in late June rolls to NEXT year — the classic stray-
        # mention false positive. Low confidence → not trustworthy.
        parsed = _parse_date_details("A Manifesto, published May 2", today=self.today)
        self.assertEqual(parsed.start, "2027-05-02")
        self.assertEqual(parsed.confidence, "low")

    def test_confidence_medium_for_near_bare_month(self):
        parsed = _parse_date_details("Summit on 3 July", today=self.today)
        self.assertEqual(parsed.confidence, "medium")

    def test_confidence_high_when_year_written(self):
        self.assertEqual(_parse_date_details("3 July 2026", today=self.today).confidence, "high")

    def test_listing_ordinal_comma_date(self):
        # P1-A: Manchester's Finest / Skiddle put the date as "Sun 28th Jun,
        # 2026 - Venue" — ordinal suffix + comma + weekday prefix used to defeat
        # the parser, leaving real events undated and dropped as junk.
        parsed = _parse_date_details("Sun 28th Jun, 2026 - The Beeswing Wine Bar", today=self.today)
        self.assertEqual(parsed.start, "2026-06-28")
        self.assertEqual(parsed.confidence, "high")

    def test_listing_ordinal_range_populates_end(self):
        parsed = _parse_date_details(
            "Thu 2nd Jul, 2026 - Sun 6th Sep, 2026 - Aviva Studios", today=self.today
        )
        self.assertEqual(parsed.start, "2026-07-02")
        self.assertEqual(parsed.end, "2026-09-06")
        self.assertEqual(parsed.confidence, "high")

    def test_free_boolean_set(self):
        ev = extract_event(self._culture(title="Free family day at HOME 5 July",
                                         summary="Free entry"))
        self.assertTrue(ev["free"])
        self.assertEqual(ev["price"], "free")

    def test_paid_event_not_free(self):
        ev = extract_event(self._culture(title="Show 5 July", summary="Tickets £15"))
        self.assertFalse(ev["free"])

    def test_in_lowercase_word_is_not_a_venue(self):
        # "...Technology in Practice" must not yield venue="Practice".
        ev = extract_event(self._culture(
            title="Conversations on Responsible Technology in Practice 2 July"))
        self.assertEqual(ev["venue"], "")

    def test_multiword_venue_still_extracted(self):
        ev = extract_event(self._culture(title="Recital at Bridgewater Hall 2 July"))
        self.assertEqual(ev["venue"], "Bridgewater Hall")

    def test_far_future_helper(self):
        self.assertTrue(event_is_far_future({"date": "2027-05-21"}, today=self.today))
        self.assertFalse(event_is_far_future({"date": "2026-07-03"}, today=self.today))

    def test_multi_day_and_active_on_helpers(self):
        festival = {"date_start": "2026-06-25", "date_end": "2026-07-05"}
        self.assertTrue(event_is_multi_day(festival))
        self.assertTrue(event_active_on(festival, self.today))      # mid-run
        self.assertFalse(event_active_on(festival, date(2026, 7, 6)))  # after end

    def test_trustworthy_rejects_low_confidence(self):
        self.assertFalse(event_date_is_trustworthy(
            {"date": "2027-05-02", "date_confidence": "low"}))
        self.assertTrue(event_date_is_trustworthy(
            {"date": "2026-07-03", "date_confidence": "medium"}))


if __name__ == "__main__":
    unittest.main()
