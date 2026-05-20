"""Anti-golden test pack — Sprint Quality Fix 1.

These cases come from the failed digest run on 2026-05-20: 74 visible items
including a restaurant from 1 May, a Manchester bakery story already shipped
on 14 May, an undated Chorlton café opening, and a council statement about
an attack on 29 April. After Sprint Fix 1 these stories must not reach the
visible digest — either flagged ``borderline`` (held for manual review),
``demote`` (ranked to the very bottom), or outright ``reject``.
"""
from __future__ import annotations

from datetime import date
import unittest

from news_digest.pipeline.editorial_decision import (
    FRESH_HARD,
    FRESH_SOFT,
    STATUS_BORDERLINE,
    STATUS_DEMOTE,
    STATUS_PUBLISH,
    STATUS_REJECT,
    WHY_HAPPENING_TODAY,
    WHY_NEW_TODAY,
    WHY_ONGOING,
    WHY_STALE,
    WHY_UNCLEAR,
    apply_decision,
    candidate_kind,
    classify_why_now,
    decide,
    decide_and_apply,
    extract_content_date,
)


TODAY = date(2026, 5, 20)


def _trof() -> dict:
    """Restaurant from 1 May, shipped on 20 May with included=True."""
    return {
        "category": "food_openings",
        "primary_block": "openings",
        "change_type": "same_story_new_facts",
        "title": "NQ fave Trof to re-open as The Trof Pub & Dining Room",
        "summary": "",
        "source_label": "Manchester's Finest",
        "source_url": "https://manchestersfinest.com/eating-and-drinking/nq-fave-trof",
        "include": True,
        "event": {
            "is_event": True,
            "event_name": "NQ fave Trof to re-open as The Trof Pub & Dining Room",
            "venue": "",
            "date": "2026-05-01",
            "date_text": "1 May",
            "borough": "Manchester",
        },
        "entities": {"boroughs": ["Manchester"], "districts": ["Northern Quarter"]},
    }


def _gooey_recent() -> dict:
    """Bakery story from 14 May (6 days old by 20 May) — past the demote
    threshold of 3 days for openings."""
    return {
        "category": "food_openings",
        "primary_block": "openings",
        "change_type": "reminder",
        "title": "Gooey is opening its biggest cafe yet at Circle Square",
        "summary": "",
        "source_label": "Manchester's Finest",
        "source_url": "https://manchestersfinest.com/eating-and-drinking/gooey-circle-square",
        "include": True,
        "event": {
            "is_event": True,
            "venue": "Circle Square Cafes",
            "date": "2026-05-14",
            "date_text": "14 May",
            "borough": "Manchester",
        },
        "entities": {"boroughs": ["Manchester"]},
    }


def _brewch_undated() -> dict:
    """Café opening with no date at all in any field."""
    return {
        "category": "food_openings",
        "primary_block": "openings",
        "change_type": "same_story_new_facts",
        "title": "Brewch is getting bigger: Cereal milk stars open new café in Chorlton",
        "summary": "",
        "source_label": "The Manc Eats",
        "source_url": "https://themanc.com/eats/brewch-chorlton",
        "include": True,
        "event": {
            "is_event": True,
            "venue": "Chorlton Anyone",
            "date": "",
            "date_text": "",
            "borough": "Manchester",
        },
        "entities": {"boroughs": ["Manchester"], "districts": ["Chorlton"]},
    }


def _golders_green() -> dict:
    """Council statement about 29 April attack — only carries the date in
    the title. The legacy validator missed this because the event dict was
    empty and the headline date never reached the stale-event check."""
    return {
        "category": "council",
        "primary_block": "city_watch",
        "change_type": "same_story_new_facts",
        "title": "Statement on attack in Golders Green - Wednesday 29 April 2026 - Greater Manchester Combined Authority",
        "summary": "",
        "source_label": "GMCA",
        "source_url": "https://greatermanchester-ca.gov.uk/news/statement-on-attack-in-golders-green-wednesday-29-april-2026",
        "include": True,
        "event": {},
        "entities": {"councils": ["GMCA"]},
    }


def _generic_property_no_place() -> dict:
    """Planning/property item without an identifiable location. Should be
    held for manual review rather than blindly shipped."""
    return {
        "category": "city_news",
        "primary_block": "city_watch",
        "change_type": "new_story",
        "title": "Office block could become flats",
        "summary": "Plans to convert an office building into residential units have been submitted.",
        "source_label": "Some Local Outlet",
        "source_url": "https://example.test/property",
        "include": True,
        "entities": {},
    }


def _useful_transport_today() -> dict:
    return {
        "category": "transport",
        "primary_block": "transport",
        "change_type": "new_story",
        "title": "Metrolink suspended between Cornbrook and Trafford Centre on 20 May",
        "summary": "",
        "source_label": "TfGM",
        "source_url": "https://tfgm.com/metrolink-alert",
        "include": True,
        "entities": {"stations": ["Cornbrook", "Trafford Centre"]},
    }


def _ongoing_transport_disruption() -> dict:
    return {
        "category": "transport",
        "primary_block": "transport",
        "change_type": "reminder",
        "title": "Rochdale line closure continues",
        "summary": "Replacement bus until 30 May. duration=multi-week",
        "source_label": "TfGM",
        "source_url": "https://tfgm.com/rochdale-line",
        "include": True,
        "event": {"date": "2026-05-12"},
        "entities": {"stations": ["Rochdale"]},
    }


def _ao_arena_concert_tomorrow() -> dict:
    return {
        "category": "venues_tickets",
        "primary_block": "ticket_radar",
        "change_type": "new_story",
        "title": "Sabaton at AO Arena on 21 May 2026",
        "summary": "event_date=2026-05-21 ticket_signal=onsale public_onsale=2026-05-19",
        "source_label": "AO Arena",
        "source_url": "https://aoarena.com/sabaton",
        "include": True,
        "event": {"date": "2026-05-21", "venue": "AO Arena", "is_event": True},
        "entities": {"venues": ["AO Arena"]},
    }


# ── Tests ────────────────────────────────────────────────────────────────────


class WhyNowClassifierTest(unittest.TestCase):
    def test_event_today(self) -> None:
        cand = {"category": "venues_tickets", "primary_block": "ticket_radar",
                "event": {"date": "2026-05-20"}, "title": "Workshop today"}
        why, age = classify_why_now(cand, TODAY)
        self.assertEqual(why, WHY_NEW_TODAY)
        self.assertEqual(age, 0)

    def test_event_tomorrow_is_happening_today_window(self) -> None:
        cand = {"category": "venues_tickets", "primary_block": "ticket_radar",
                "event": {"date": "2026-05-21"}, "title": "Workshop tomorrow"}
        why, age = classify_why_now(cand, TODAY)
        # 1 day ahead = happening-today/deadline-soon window.
        self.assertEqual(why, "deadline_soon")
        self.assertEqual(age, -1)

    def test_ongoing_disruption_keeps_ongoing_tag(self) -> None:
        why, _ = classify_why_now(_ongoing_transport_disruption(), TODAY)
        self.assertEqual(why, WHY_ONGOING)

    def test_no_date_anywhere_is_unclear(self) -> None:
        why, age = classify_why_now(_brewch_undated(), TODAY)
        self.assertEqual(why, WHY_UNCLEAR)
        self.assertIsNone(age)

    def test_stale_opening_from_1_may(self) -> None:
        why, age = classify_why_now(_trof(), TODAY)
        self.assertEqual(why, WHY_STALE)
        self.assertEqual(age, 19)

    def test_golders_green_date_extracted_from_title(self) -> None:
        # Title carries "29 April 2026"; event dict is empty. We must still
        # see it as 21 days old, not as "no date".
        content_date = extract_content_date(_golders_green(), TODAY)
        self.assertEqual(content_date, date(2026, 4, 29))


class KindClassifierTest(unittest.TestCase):
    def test_food_openings_is_opening(self) -> None:
        self.assertEqual(candidate_kind(_trof()), "opening")

    def test_council_is_news(self) -> None:
        self.assertEqual(candidate_kind(_golders_green()), "news")

    def test_transport_is_transport(self) -> None:
        self.assertEqual(candidate_kind(_useful_transport_today()), "transport")

    def test_venues_tickets_is_event(self) -> None:
        self.assertEqual(candidate_kind(_ao_arena_concert_tomorrow()), "event")


class AntiGoldenFromTwentyMayTest(unittest.TestCase):
    """The exact stories that polluted 2026-05-20. None of these may publish."""

    def test_trof_from_1_may_is_borderline(self) -> None:
        decision = decide(_trof(), today=TODAY)
        self.assertEqual(decision.status, STATUS_BORDERLINE,
                         msg="Opening 19 days old must be held for manual review")
        self.assertEqual(decision.why_now, WHY_STALE)
        self.assertEqual(decision.freshness_severity, FRESH_HARD)
        self.assertEqual(decision.age_days, 19)
        self.assertTrue(any("stale_opening" in r for r in decision.reasons))

    def test_gooey_from_14_may_is_demoted(self) -> None:
        decision = decide(_gooey_recent(), today=TODAY)
        # 6 days old: past 3d demote, below 7d borderline.
        self.assertEqual(decision.status, STATUS_DEMOTE)
        self.assertEqual(decision.freshness_severity, FRESH_SOFT)
        self.assertEqual(decision.age_days, 6)
        # And it was tagged change_type=reminder — rehash penalty stacks.
        self.assertIn("same_story_rehash", decision.reasons)

    def test_brewch_undated_opening_is_demoted_not_published(self) -> None:
        decision = decide(_brewch_undated(), today=TODAY)
        # No date for an opening = demote with no_anchor_date reason, NOT
        # silently publish like in the old pipeline.
        self.assertNotEqual(decision.status, STATUS_PUBLISH,
                            msg="Undated opening cannot be silently published")
        self.assertEqual(decision.why_now, WHY_UNCLEAR)
        self.assertIn("no_anchor_date", decision.reasons)

    def test_golders_green_29_april_is_borderline(self) -> None:
        decision = decide(_golders_green(), today=TODAY)
        # News older than 14 days = borderline (>14d threshold for news).
        self.assertEqual(decision.status, STATUS_BORDERLINE)
        self.assertEqual(decision.why_now, WHY_STALE)
        self.assertEqual(decision.age_days, 21)

    def test_generic_property_without_place_is_borderline(self) -> None:
        decision = decide(_generic_property_no_place(), today=TODAY)
        # No named place + no entities at all → held for review.
        self.assertEqual(decision.status, STATUS_BORDERLINE)
        self.assertIn("clarity:no_named_place", decision.reasons)


class DoesNotBreakHealthyCandidatesTest(unittest.TestCase):
    """Sprint Fix 1 must not start rejecting healthy candidates."""

    def test_transport_today_publishes(self) -> None:
        decision = decide(_useful_transport_today(), today=TODAY)
        self.assertEqual(decision.status, STATUS_PUBLISH)

    def test_ongoing_disruption_publishes(self) -> None:
        decision = decide(_ongoing_transport_disruption(), today=TODAY)
        self.assertEqual(decision.status, STATUS_PUBLISH)
        self.assertEqual(decision.why_now, WHY_ONGOING)

    def test_concert_tomorrow_publishes(self) -> None:
        decision = decide(_ao_arena_concert_tomorrow(), today=TODAY)
        self.assertEqual(decision.status, STATUS_PUBLISH)


class ApplyDecisionContractTest(unittest.TestCase):
    """Cascade integrates with the existing include/reason contract."""

    def test_reject_clears_include(self) -> None:
        cand = _ao_arena_concert_tomorrow()
        cand["event"]["date"] = "2026-05-01"  # past
        cand["summary"] = "event_date=2026-05-01"
        # Move past so it's a real expired event:
        del cand["entities"]
        decide_and_apply(cand, today=TODAY)
        self.assertFalse(cand["include"])
        self.assertIn("editorial_decision", cand)
        self.assertEqual(cand["editorial_decision"]["status"], STATUS_REJECT)
        self.assertIn("event_date_past", cand["editorial_decision"]["reasons"])

    def test_borderline_clears_include_and_sets_flag(self) -> None:
        cand = _trof()
        decide_and_apply(cand, today=TODAY)
        self.assertFalse(cand["include"])
        self.assertTrue(cand.get("borderline"))
        self.assertEqual(cand["editorial_decision"]["status"], STATUS_BORDERLINE)

    def test_demote_keeps_include_true(self) -> None:
        cand = _gooey_recent()
        decide_and_apply(cand, today=TODAY)
        # Demote keeps the item publishable but flags it.
        self.assertTrue(cand["include"])
        self.assertTrue(cand.get("editorial_demoted"))
        self.assertEqual(cand["editorial_decision"]["status"], STATUS_DEMOTE)

    def test_publish_does_not_touch_include(self) -> None:
        cand = _useful_transport_today()
        decide_and_apply(cand, today=TODAY)
        self.assertTrue(cand["include"])
        self.assertFalse(cand.get("editorial_demoted"))
        self.assertFalse(cand.get("borderline"))

    def test_why_now_is_attached_to_candidate(self) -> None:
        cand = _useful_transport_today()
        decide_and_apply(cand, today=TODAY)
        self.assertIn("why_now", cand)
        # Today's transport with a date in the title → new_today.
        self.assertEqual(cand["why_now"], WHY_NEW_TODAY)


class PublishedReviewTest(unittest.TestCase):
    """Sprint Fix 1 (Q4 / 1.5) — release_report.published_review aggregates
    concerns about what reached the visible digest."""

    def test_counts_stale_demoted_and_unclear_published(self) -> None:
        from news_digest.pipeline.release import _build_published_review

        # Three rendered candidates: one stale-but-published (demoted), one
        # unclear-why_now, one healthy. _build_published_review should
        # group them as concerns/normal correctly.
        stale = _gooey_recent()
        decide_and_apply(stale, today=TODAY)  # → demote (6 days old)
        stale["fingerprint"] = "fp_stale"

        unclear = _brewch_undated()
        decide_and_apply(unclear, today=TODAY)  # → demote (no_anchor_date)
        unclear["fingerprint"] = "fp_unclear"

        healthy = _useful_transport_today()
        decide_and_apply(healthy, today=TODAY)
        healthy["fingerprint"] = "fp_healthy"

        report = {"candidates": [stale, unclear, healthy]}
        review = _build_published_review(report, ["fp_stale", "fp_unclear", "fp_healthy"])

        self.assertEqual(review["counts"]["rendered"], 3)
        self.assertEqual(review["counts"]["rendered_with_decision"], 3)
        self.assertEqual(review["counts"]["rendered_without_decision"], 0)
        self.assertGreaterEqual(review["counts"]["demoted_published"], 2)
        self.assertGreaterEqual(review["counts"]["unclear_why_now_published"], 1)
        self.assertGreaterEqual(len(review["concerns"]), 2)

    def test_no_editorial_decision_is_flagged(self) -> None:
        from news_digest.pipeline.release import _build_published_review

        # Item that somehow reached rendered without an editorial_decision —
        # the cascade either crashed or was skipped. Honest audit must say so.
        cand = {"fingerprint": "fp_legacy", "title": "Legacy item", "include": True}
        review = _build_published_review({"candidates": [cand]}, ["fp_legacy"])
        self.assertEqual(review["counts"]["rendered_without_decision"], 1)
        self.assertTrue(any(c["concern"] == "no_editorial_decision" for c in review["concerns"]))

    def test_healthy_items_dont_flood_concerns(self) -> None:
        from news_digest.pipeline.release import _build_published_review

        cand = _useful_transport_today()
        decide_and_apply(cand, today=TODAY)
        cand["fingerprint"] = "fp_healthy"
        review = _build_published_review({"candidates": [cand]}, ["fp_healthy"])
        self.assertEqual(review["counts"]["demoted_published"], 0)
        self.assertEqual(review["counts"]["stale_published"], 0)
        self.assertEqual(len(review["concerns"]), 0)


class MorningPracticalBoostTest(unittest.TestCase):
    """Sprint Fix 1 (S5 / 1.13) — morning items beat evergreen at parity."""

    def test_today_focus_happening_today_outranks_same_block_evergreen(self) -> None:
        from news_digest.pipeline.reader_value import reader_value_score

        # Pick a block that doesn't max out reader_value at 100. today_focus
        # gives a moderate base + bonus so the morning boost is observable.
        practical = {
            "category": "public_services",
            "primary_block": "today_focus",
            "change_type": "new_story",
            "title": "Council deadline closes today",
            "include": True,
            "why_now": "happening_today",
        }
        evergreen = {
            "category": "public_services",
            "primary_block": "today_focus",
            "change_type": "new_story",
            "title": "Council long-term plan",
            "include": True,
            "why_now": "unclear",
        }
        self.assertGreater(reader_value_score(practical), reader_value_score(evergreen))


class ScoringTraceTest(unittest.TestCase):
    """Sprint Fix 1 (S3 / 1.12) — every score has a structured trace."""

    def test_trace_lists_contributing_signals(self) -> None:
        from news_digest.pipeline.reader_value import reader_value_score_with_trace

        cand = {
            "category": "transport",
            "primary_block": "transport",
            "change_type": "new_story",
            "title": "Tram disruption today",
            "include": True,
            "why_now": "happening_today",
        }
        score, trace = reader_value_score_with_trace(cand)
        self.assertGreater(score, 0)
        self.assertTrue(any("base_category" in t["signal"] for t in trace))
        self.assertTrue(any("morning_practical_boost" in t["signal"] for t in trace))

    def test_attach_reader_value_stamps_trace_on_candidate(self) -> None:
        from news_digest.pipeline.reader_value import attach_reader_value

        cand = {
            "category": "transport",
            "primary_block": "transport",
            "change_type": "new_story",
            "title": "Tram disruption today",
            "include": True,
            "why_now": "new_today",
        }
        attach_reader_value(cand)
        self.assertIn("scoring_trace", cand)
        self.assertIsInstance(cand["scoring_trace"], list)
        self.assertGreater(len(cand["scoring_trace"]), 0)


if __name__ == "__main__":
    unittest.main()
