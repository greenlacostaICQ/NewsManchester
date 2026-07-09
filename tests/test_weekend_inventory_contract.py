from __future__ import annotations

from datetime import date, timedelta
import unittest
from unittest import mock

from news_digest.pipeline.common import now_london
from news_digest.pipeline.editorial_contracts import calendar_repeat_review
from news_digest.pipeline.event_extraction import enrich_candidate_event
from news_digest.pipeline.llm_rewrite import _apply_post_board_translation_cut, _apply_rewrite_shortlist
from news_digest.pipeline.weekend_inventory import (
    current_weekend_window,
    is_weekend_inventory_candidate,
    weekend_occurrence_date,
)
from news_digest.pipeline.writer import (
    _collapse_weekend_duplicate_events,
    _is_outside_current_weekend_candidate,
    _line_has_conflicting_event_date,
    _slice_counting_only_non_exempt,
    _weekend_inventory_loss_trace,
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

    def test_hidden_weekend_inventory_is_not_reported_as_missing(self) -> None:
        candidate = _weekend_inventory_candidate()

        trace = _weekend_inventory_loss_trace(
            [candidate],
            {},
            [],
            show_weekend=False,
        )

        self.assertEqual(trace["counts"]["eligible"], 1)
        self.assertEqual(trace["counts"]["missing"], 0)
        self.assertEqual(trace["counts"]["hidden_by_schedule"], 1)
        self.assertEqual(trace["items"][0]["loss_stage"], "hidden_by_schedule")

    def test_rewrite_board_caps_do_not_cut_weekend_inventory(self) -> None:
        weekend = [_weekend_inventory_candidate(idx) for idx in range(12)]
        transport = [_transport_candidate(idx) for idx in range(99)]
        candidates = weekend + transport

        selected, _report = _apply_rewrite_shortlist(candidates, candidates)

        selected_fps = {str(candidate.get("fingerprint") or "") for candidate in selected}
        self.assertTrue({candidate["fingerprint"] for candidate in weekend} <= selected_fps)
        self.assertFalse(any(candidate.get("rewrite_shortlist_status") == "backup_ranking_board_cap" for candidate in weekend))

    def test_final_translation_cap_does_not_cut_weekend_inventory(self) -> None:
        weekend = [_weekend_inventory_candidate(idx) for idx in range(12)]
        transport = [_transport_candidate(idx) for idx in range(40)]
        board = weekend + transport

        selected, _report = _apply_post_board_translation_cut(board, board)

        selected_fps = {str(candidate.get("fingerprint") or "") for candidate in selected}
        self.assertTrue({candidate["fingerprint"] for candidate in weekend} <= selected_fps)
        self.assertFalse(any(candidate.get("rewrite_shortlist_status") == "backup_after_board_rank" for candidate in weekend))

    def test_selected_uncapped_alone_does_not_bypass_translation_cap(self) -> None:
        board = [_ordinary_selected_uncapped_candidate(idx) for idx in range(60)]

        selected, _report = _apply_post_board_translation_cut(board, board)

        self.assertLess(len(selected), len(board))
        self.assertTrue(any(candidate.get("rewrite_shortlist_status") == "backup_after_board_rank" for candidate in board))

    def test_writer_section_cap_keeps_inventory_but_counts_ordinary_items(self) -> None:
        inventory = [_weekend_inventory_candidate(idx) for idx in range(5)]
        ordinary = []
        for idx in range(4):
            candidate = _weekend_inventory_candidate(100 + idx, title=f"Standalone gig {idx}")
            candidate["summary"] = "Standalone gig listing at an arena."
            candidate["source_label"] = "Ticketmaster Manchester Upcoming"
            ordinary.append(candidate)
        candidates = inventory + ordinary
        fps = [candidate["fingerprint"] for candidate in candidates]
        lines = [candidate["draft_line"] for candidate in candidates]
        srcs = [candidate["source_label"] for candidate in candidates]
        titles = [candidate["title"] for candidate in candidates]
        scores = [0.0 for _ in candidates]

        kept_lines, _srcs, kept_fps, _scores, _titles, dropped_idx, counted = _slice_counting_only_non_exempt(
            lines=lines,
            srcs=srcs,
            fps=fps,
            scores=scores,
            titles=titles,
            candidate_by_fp={candidate["fingerprint"]: candidate for candidate in candidates},
            section_name="Выходные в GM",
            counted_limit=3,
            ignore_section_exemption=True,
        )

        self.assertTrue({candidate["fingerprint"] for candidate in inventory} <= set(kept_fps))
        self.assertEqual(counted, 3)
        self.assertEqual(len(dropped_idx), 1)
        self.assertEqual(len(kept_lines), 8)

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
