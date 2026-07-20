from __future__ import annotations

from datetime import date, datetime, timedelta
import unittest

from news_digest.pipeline.llm_rewrite import _apply_rewrite_shortlist
from unittest import mock

from news_digest.pipeline.common import now_london
from news_digest.pipeline.editorial_contracts import calendar_repeat_review
from news_digest.pipeline.event_extraction import enrich_candidate_event
from news_digest.pipeline.inventory import build_inventory_record, prewrite_stable_inventory_candidate
from news_digest.pipeline.weekend_inventory import (
    candidate_recurring_occurrence_date,
    current_weekend_window,
    is_weekend_inventory_candidate,
    weekend_activity_type,
    weekend_occurrence_date,
)
from news_digest.pipeline.writer import (
    _collapse_weekend_duplicate_events,
    _is_expired_event_candidate,
    _is_outside_current_weekend_candidate,
    _line_has_conflicting_event_date,
    _rescue_misrouted_weekend_markets,
)


def _next_saturday() -> date:
    today = now_london().date()
    return today + timedelta(days=(5 - today.weekday()) % 7)


def _weekend_inventory_candidate(idx: int = 0, *, title: str | None = None) -> dict:
    event_day = _next_saturday()
    title = title or f"Manchester Rum Festival {idx}"
    return {
        "include": True,
        "validated": True,
        "fingerprint": f"weekend-{idx}",
        "primary_block": "weekend_activities",
        "category": "culture_weekly",
        "title": title,
        "summary": f"{title} happens this weekend with public visitor access.",
        "source_label": "Manchester's Finest Events",
        "source_url": f"https://example.test/weekend/{idx}",
        "draft_line": f"• {title} пройдет в эти выходные.",
        "event": {
            "is_event": True,
            "event_name": title,
            "date_start": event_day.isoformat(),
            "date": event_day.isoformat(),
            "date_confidence": "high",
            "venue": "New Century",
        },
    }


def _transport_candidate(idx: int) -> dict:
    return {
        "include": True,
        "validated": True,
        "fingerprint": f"transport-{idx}",
        "primary_block": "transport",
        "category": "transport",
        "title": f"Transport disruption {idx}",
        "source_label": "TfGM",
        "source_url": f"https://example.test/transport/{idx}",
        "draft_line": f"• Transport disruption {idx}.",
    }


def _ordinary_selected_uncapped_candidate(idx: int) -> dict:
    return {
        "include": True,
        "validated": True,
        "fingerprint": f"ordinary-{idx}",
        "primary_block": "openings",
        "category": "food_openings",
        "title": f"Ordinary opening {idx:02d}",
        "summary": "A regular food opening item with no protected weekend inventory value.",
        "source_label": "Example Source",
        "source_url": f"https://example.test/opening/{idx}",
        "draft_line": f"• Ordinary opening {idx:02d}.",
        "rewrite_shortlist_status": "selected_uncapped",
    }


class WeekendInventoryContractTests(unittest.TestCase):
    def test_stored_next_occurrence_overrides_old_structured_dates(self) -> None:
        candidate = {
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "title": "Bowlee Car Boot Sale",
            "summary": "Sundays at Bowlee Community Park.",
            "published_at": "2026-07-12T08:00:00+01:00",
            "source_url": "https://example.test/bowlee",
            "event": {
                "event_name": "Bowlee Car Boot Sale",
                "venue": "Bowlee Community Park",
                "date_start": "2026-07-12",
                "date_end": "2026-07-12",
                "next_occurrence": "2026-07-19",
                "is_recurring": True,
            },
        }
        with mock.patch("news_digest.pipeline.weekend_inventory.now_london", return_value=datetime(2026, 7, 19, 8, 0)):
            self.assertEqual(weekend_occurrence_date(candidate), date(2026, 7, 19))
            with mock.patch("news_digest.pipeline.writer.now_london", return_value=datetime(2026, 7, 19, 8, 0)):
                self.assertFalse(_is_outside_current_weekend_candidate(candidate))
                self.assertFalse(_is_expired_event_candidate(candidate))

    def test_asian_night_market_keeps_next_monthly_occurrence(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Stockport Asian Food Night Market",
            "summary": "Returns every second Friday of the month in Churchgate Stockport.",
            "source_label": "SK Lowdown Markets",
            "source_url": "https://sklowdown.co.uk/asian-food-night-market-july",
            "event": {
                "event_name": "Stockport Asian Food Night Market",
                "venue": "Churchgate Stockport",
                "date_start": "2026-07-10",
                "date_end": "2026-07-10",
                "date_confidence": "high",
            },
        }
        today = date(2026, 7, 17)
        self.assertEqual(candidate_recurring_occurrence_date(candidate, today=today), date(2026, 8, 14))
        self.assertFalse(is_weekend_inventory_candidate(candidate, today=today))
        with mock.patch("news_digest.pipeline.weekend_inventory.now_london") as fake_now:
            fake_now.return_value = datetime(2026, 7, 17, 8, 0)
            with mock.patch("news_digest.pipeline.writer.now_london", return_value=datetime(2026, 7, 17, 8, 0)):
                self.assertTrue(prewrite_stable_inventory_candidate(candidate))
                record = build_inventory_record(
                    candidate,
                    prompt_version=1,
                    now_iso="2026-07-17T00:30:00+01:00",
                )
        self.assertEqual(record["fact_card"]["next_occurrence"], "2026-08-14")
        self.assertGreaterEqual(record["retention_until"], "2026-09-13")
        self.assertIn("14 августа", record["draft_line"])
        self.assertNotIn("10 июля", record["draft_line"])
        self.assertNotIn("back in July", record["draft_line"])

    def test_past_event_is_not_revived_by_false_contract_recurrence(self) -> None:
        candidate = _weekend_inventory_candidate(title="South Manchester Food Festival")
        candidate["event"].update(
            date_start="2026-05-16",
            date="2026-05-16",
            date_end="2026-05-17",
            is_recurring=True,
        )
        candidate["summary"] = "A spring festival; last year tickets sold faster than a Sunday roast."
        candidate["editorial_contract"] = {
            "occurrence": {"shape": "recurring", "date": "2026-07-19"}
        }
        self.assertFalse(is_weekend_inventory_candidate(candidate, today=date(2026, 7, 17)))

    def test_incidental_medieval_word_does_not_make_walking_tour_inventory(self) -> None:
        candidate = _weekend_inventory_candidate(title="Manchester City Walking Tour")
        candidate["summary"] = "A guided walk from Central Library through the city centre."
        candidate["evidence_text"] = "The route later passes the Medieval Quarter."
        self.assertEqual(weekend_activity_type(candidate), "")

    def test_every_saturday_page_gets_current_weekend_occurrence(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Campfield Market",
            "summary": "Every Saturday, explore 70+ independent traders and street food pop-ups.",
            "evidence_text": "Campfield Market is a weekly city market in Manchester.",
            "source_label": "Campfield Market",
            "source_url": "https://campfieldmarket.com/",
        }

        with mock.patch("news_digest.pipeline.event_extraction._today_london", return_value=date(2026, 7, 2)):
            event = enrich_candidate_event(candidate)["event"]

        self.assertEqual(event["date_start"], "2026-07-04")
        self.assertTrue(event["is_recurring"])
        self.assertTrue(event["is_event"])

    def test_sundays_bank_holiday_market_uses_current_weekend_occurrence(self) -> None:
        today = now_london().date()
        _, weekend_end = current_weekend_window(today=today)
        old_date = today - timedelta(days=4)
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Bowlee Car Boot Sale",
            "summary": (
                "Bowlee Car Boot Sale and Market. Dates: Sundays and Bank Holiday "
                "Mondays (April - October 2026). Time: from early morning."
            ),
            "lead": "Bowlee Car Boot Sale at Bowlee Community Park.",
            "evidence_text": (
                "Dates: Sundays and Bank Holiday Mondays (April - October 2026). "
                "Location: Bowlee Community Park."
            ),
            "source_label": "Bowlee Car Boot Sale",
            "source_url": "https://example.test/bowlee",
            "event": {
                "is_event": True,
                "event_name": "Bowlee Car Boot Sale",
                "venue": "Bowlee Community Park",
                "date_start": old_date.isoformat(),
                "date": old_date.isoformat(),
            },
        }

        occurrence = weekend_occurrence_date(candidate, today=today)

        self.assertEqual(occurrence, weekend_end)
        self.assertTrue(is_weekend_inventory_candidate(candidate, today=today))
        self.assertFalse(_is_outside_current_weekend_candidate(candidate))
        self.assertFalse(
            _line_has_conflicting_event_date(
                candidate,
                f"• {weekend_end.day} июля — Bowlee Car Boot Sale.",
            )
        )

    def test_from_saturday_market_uses_current_weekend_occurrence(self) -> None:
        today = now_london().date()
        expected_saturday = _next_saturday()
        old_date = today - timedelta(days=5)
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Barton Aerodrome Car Boot Sale",
            "summary": (
                "Barton Aerodrome Car Boot SaleDates: From Saturday 4th April "
                "2026Time: Sellers from 7am | Buyers from 9am."
            ),
            "lead": "Barton Aerodrome Car Boot Sale at Barton Aerodrome.",
            "evidence_text": (
                "Dates: From Saturday 4th April 2026. Location: Barton "
                "Aerodrome, Eccles."
            ),
            "source_label": "Barton Aerodrome Car Boot",
            "source_url": "https://example.test/barton",
            "event": {
                "is_event": True,
                "event_name": "Barton Aerodrome Car Boot Sale",
                "venue": "Barton Aerodrome",
                "date_start": old_date.isoformat(),
                "date": old_date.isoformat(),
            },
        }

        occurrence = weekend_occurrence_date(candidate, today=today)

        self.assertEqual(occurrence, expected_saturday)
        self.assertFalse(_is_outside_current_weekend_candidate(candidate))

    def test_weekend_duplicate_events_collapse_same_date_venue_family(self) -> None:
        date_key = _next_saturday().isoformat()
        sound_fp = "sound"
        hubble_fp = "hubble"
        candidate_by_fp = {
            sound_fp: {
                "fingerprint": sound_fp,
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "title": "Sound Bazaar Festival",
                "summary": "Sound Bazaar Festival at The Yard Manchester.",
                "event": {
                    "date_start": date_key,
                    "event_name": "Sound Bazaar Festival",
                    "venue": "The Yard Manchester",
                },
            },
            hubble_fp: {
                "fingerprint": hubble_fp,
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "title": "Hubble Bubble Sound Bazar Festival",
                "summary": "Hubble Bubble Sound Bazar Festival at The Yard MCR.",
                "event": {
                    "date_start": date_key,
                    "event_name": "Hubble Bubble Sound Bazar Festival",
                    "venue": "The Yard MCR",
                },
            },
        }

        lines, _srcs, fps, _scores, _titles, dropped = _collapse_weekend_duplicate_events(
            [
                "• Hubble Bubble Sound Bazar Festival at The Yard MCR.",
                "• Sound Bazaar Festival at The Yard Manchester.",
            ],
            ["Skiddle", "Manchester's Finest"],
            [hubble_fp, sound_fp],
            [100.0, 90.0],
            ["Hubble Bubble Sound Bazar Festival", "Sound Bazaar Festival"],
            candidate_by_fp,
        )

        self.assertEqual(lines, ["• Hubble Bubble Sound Bazar Festival at The Yard MCR."])
        self.assertEqual(fps, [hubble_fp])
        self.assertEqual(dropped[0]["fingerprint"], sound_fp)

    def test_misrouted_food_opening_market_is_rescued_to_weekend(self) -> None:
        # Pin to a Saturday so the event date lands inside the current-weekend
        # window on any day the suite runs — the rescue only fires when the
        # event day is within current_weekend_window().
        today = date(2026, 7, 11)
        candidate = {
            "include": False,
            "fingerprint": "asian-food-night-market",
            "category": "food_openings",
            "primary_block": "openings",
            "title": "The SK Lowdown",
            "summary": "Stockport's Asian Food Night Market returns every second Friday of the month.",
            "lead": "Asian Food Night Market returns in July.",
            "evidence_text": (
                "Asian Food Night Market - Eat Good West UK. Friday 10 July 2026 "
                "17:00 22:00 Churchgate Stockport. Authentic Asian cuisine, "
                "street food and live music. FREE entry."
            ),
            "source_label": "SK Lowdown Markets",
            "source_url": "https://sklowdown.co.uk/whats-on-stockport/asian-food-night-market-july",
            "reason": "Validator: cross-day rehash — fingerprint already shipped on 2026-07-07.",
            "publish_plan_status": "drop",
            "rubric_contract": {"rubric": "weekend_market"},
            "event": {
                "is_event": True,
                "event_name": "Stockport's Asian Food Night Market is back in July",
                "venue": "Churchgate Stockport",
                "date_start": today.isoformat(),
                "date": today.isoformat(),
            },
        }
        warnings: list[str] = []

        with mock.patch(
            "news_digest.pipeline.weekend_inventory.now_london",
            return_value=datetime(2026, 7, 11, 12, 0),
        ):
            report = _rescue_misrouted_weekend_markets([candidate], warnings)

        self.assertEqual(report["count"], 1)
        self.assertTrue(candidate["include"])
        self.assertEqual(candidate["category"], "culture_weekly")
        self.assertEqual(candidate["primary_block"], "weekend_activities")
        self.assertEqual(candidate["publish_plan_status"], "show")
        self.assertTrue(warnings)

    def test_inventory_scope_excludes_ordinary_afisha_but_keeps_special_weekend_activity(self) -> None:
        event_day = date(2026, 7, 4)
        market = _weekend_inventory_candidate(title="Wythenshawe Makers Market")
        concert = _weekend_inventory_candidate(title="Standalone arena concert")
        concert["summary"] = "Standalone concert listing at AO Arena."
        concert["source_label"] = "Ticketmaster Manchester Upcoming"
        beauty = _weekend_inventory_candidate(title="La Prairie and Chantecaille Beauty Brunch")
        for candidate in (market, concert, beauty):
            candidate["event"]["date_start"] = event_day.isoformat()
            candidate["event"]["date"] = event_day.isoformat()

        self.assertTrue(is_weekend_inventory_candidate(market, today=date(2026, 7, 2)))
        self.assertTrue(is_weekend_inventory_candidate(beauty, today=date(2026, 7, 2)))
        self.assertFalse(is_weekend_inventory_candidate(concert, today=date(2026, 7, 2)))
        self.assertEqual(market["event"]["date_start"], event_day.isoformat())

    def test_rewrite_board_caps_do_not_cut_weekend_inventory(self) -> None:
        weekend = [_weekend_inventory_candidate(idx) for idx in range(12)]
        transport = [_transport_candidate(idx) for idx in range(99)]
        candidates = weekend + transport

        selected, _report = _apply_rewrite_shortlist(candidates, candidates)

        selected_fps = {str(candidate.get("fingerprint") or "") for candidate in selected}
        self.assertTrue({candidate["fingerprint"] for candidate in weekend} <= selected_fps)
        self.assertFalse(any(candidate.get("rewrite_shortlist_status") == "backup_ranking_board_cap" for candidate in weekend))

    def test_current_weekend_inventory_repeat_is_allowed_after_previous_day(self) -> None:
        candidate = _weekend_inventory_candidate(title="Urmston Artisan Market")
        previous = {
            **candidate,
            "last_published_day_london": (now_london().date() - timedelta(days=1)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
        }

        review = calendar_repeat_review(candidate, previous)

        self.assertTrue(review["allow"], review)
        self.assertEqual(review["reason"], "current_weekend_inventory_occurrence")


if __name__ == "__main__":
    unittest.main()
