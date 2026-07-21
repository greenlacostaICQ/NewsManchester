"""Ticket radar: card format (bold artist, structured genre, festival lineup)
and consolidation (festival → one card + lineup, same-venue dates → one run,
drop premium/non-music). Pins the 2026-06-14 fixes.
"""
from __future__ import annotations

import unittest
from unittest import mock

from news_digest.pipeline import writer as w
from news_digest.pipeline.dedupe import _consolidate_tickets, _merge_multinight_ticket_runs


def _tk(title, *, venue="O2", date="2026-06-19", block="outside_gm_tickets",
        subgenre="", segment="Music", attractions=None, tier="A"):
    ev = {"event_name": title, "venue": venue, "date_start": date, "subGenre": subgenre,
          "classifications": {"segment": segment}}
    if attractions is not None:
        ev["attractions"] = [{"name": n} for n in attractions]
    return {
        "title": title, "primary_block": block, "include": True,
        "fingerprint": title + date, "event": ev,
        "ticket_notability": {"tier": tier, "kind": "artist", "artist": title},
    }


class TicketCardFormatTest(unittest.TestCase):
    def test_event_owner_is_not_replaced_by_support_or_top_lineup_act(self) -> None:
        from news_digest.pipeline.ticket_notability import ticket_event_owner

        grace = _tk("Palace Bowl Presents - Grace Jones", venue="Crystal Palace Bowl")
        grace["ticket_notability"] = {"kind": "lineup_or_show", "tier": "A"}
        grace["festival_lineup"] = ["Sophie Ellis-Bextor", "Soul II Soul"]
        self.assertEqual(ticket_event_owner(grace, kind="lineup_or_show"), "Grace Jones")

        gary = _tk("Gary Numan with very special guest Ladytron")
        self.assertEqual(ticket_event_owner(gary, kind="artist"), "Gary Numan")

        pistols = _tk("SEX PISTOLS FT. FRANK CARTER")
        self.assertEqual(ticket_event_owner(pistols, kind="artist"), "SEX PISTOLS")

    def test_festival_keeps_event_name_and_lists_acts_inside_card(self) -> None:
        fest = _tk("Klarna Presents Latitude Festival 2026: General Camping", venue="Henham Park")
        fest["ticket_notability"] = {
            "kind": "lineup_or_show",
            "tier": "A",
            "event_owner": "Latitude Festival",
            "signals": {"a_tier_lineup": ["Lewis Capaldi", "David Byrne"]},
        }
        fest["festival_lineup"] = ["Lewis Capaldi", "David Byrne"]
        with mock.patch.object(w, "_ticket_watch_decision", lambda c: {"decision": "show"}):
            line = w._build_ticket_fallback_line(fest)
        self.assertIn("<b>Latitude Festival</b>", line)
        self.assertIn("Состав:", line)
        self.assertIn("<b>Lewis Capaldi</b>", line)
        self.assertNotIn("Фестивальный состав, не один артист", line)

    def test_a_tier_without_event_date_is_not_must_show(self) -> None:
        from news_digest.pipeline.ticket_notability import a_tier_ticket_policy

        candidate = _tk("Global Star", date="")
        self.assertEqual(a_tier_ticket_policy(candidate), (False, "missing_event_date"))

    def test_a_tier_bypasses_writer_watch_score(self) -> None:
        candidate = _tk("Global Star", venue="Small Hall", date="2099-06-19", block="ticket_radar", tier="A")
        candidate["ticket_type"] = "old_public_sale"
        with mock.patch.object(w, "_ticket_watch_score", return_value=0):
            self.assertEqual(w._ticket_watch_decision(candidate)["decision"], "show")
            self.assertTrue(w._build_ticket_fallback_line(candidate))

    def test_visible_repeat_policy_has_one_explicit_a_tier_override(self) -> None:
        from news_digest.pipeline.repeat_policy import visible_repeat_verdict

        candidate = _tk("Global Star", venue="AO Arena", date="2099-06-19", tier="A")
        previous = {**candidate, "last_published_day_london": "2099-05-01"}
        verdict = visible_repeat_verdict(candidate, previous)
        self.assertTrue(verdict.allow)
        self.assertEqual(verdict.reason, "a_tier_must_show_override")

        duplicate = {**candidate, "reason": "Exact duplicate fragment merged into canonical event."}
        duplicate_verdict = visible_repeat_verdict(duplicate, previous)
        self.assertFalse(duplicate_verdict.allow)
        self.assertNotEqual(duplicate_verdict.reason, "a_tier_must_show_override")

        expired = _tk("Global Star", venue="AO Arena", date="2020-06-19", tier="A")
        expired_previous = {**expired, "last_published_day_london": "2020-06-01"}
        expired_verdict = visible_repeat_verdict(expired, expired_previous)
        self.assertFalse(expired_verdict.allow)
        self.assertNotEqual(expired_verdict.reason, "a_tier_must_show_override")

    def test_bold_artist_and_structured_subgenre(self) -> None:
        with mock.patch.object(w, "_ticket_watch_decision", lambda c: {"decision": "show"}):
            line = w._build_ticket_fallback_line(_tk("Lily Allen", venue="AO Arena", subgenre="Pop"))
        self.assertIn("<b>Lily Allen</b>", line)   # artist bold
        self.assertIn("(Pop)", line)               # structured subGenre, not coarse genre

    def test_festival_card_shows_bold_lineup(self) -> None:
        fest = _tk("Isle of Wight Festival", venue="Isle of Wight")
        fest["ticket_notability"] = {"kind": "lineup_or_show"}
        fest["festival_lineup"] = ["Lewis Capaldi", "Calvin Harris", "On the Waterfront presents", "Calvin Harris"]
        with mock.patch.object(w, "_ticket_watch_decision", lambda c: {"decision": "show"}):
            line = w._build_ticket_fallback_line(fest)
        self.assertIn("Состав:", line)
        self.assertIn("<b>Lewis Capaldi</b>", line)
        self.assertIn("<b>Calvin Harris</b>", line)
        self.assertNotIn("presents", line)          # promoter wrapper filtered
        self.assertEqual(line.count("Calvin Harris"), 1)  # deduped


class TicketOnsaleFromBlobTest(unittest.TestCase):
    def test_onsale_in_listing_text_classified_without_ticketmaster_field(self) -> None:
        # W9: on-sale lived in the listing's own headline and was never parsed
        # for the 600+ non-Ticketmaster tickets. classify now reads it from the
        # blob (W1 date parser), proximity-bounded so the event date elsewhere is
        # not mislabelled and an unparseable "soon" never fabricates a date.
        from datetime import timedelta
        from news_digest.pipeline.common import now_london
        from news_digest.pipeline.editorial_contracts import classify_ticket_type, onsale_datetime_from_blob

        future = (now_london() + timedelta(days=20)).strftime("%d %B %Y")
        future_sale = {"category": "venues_tickets", "title": f"The Offspring at Co-op Live — tickets on sale {future}", "summary": ""}
        self.assertEqual(classify_ticket_type(future_sale), "presale_soon")
        self.assertIsNotNone(onsale_datetime_from_blob(future_sale))

        announced = {"category": "venues_tickets", "title": "Big show — tickets on sale soon", "summary": ""}
        self.assertEqual(classify_ticket_type(announced), "newly_listed")
        self.assertIsNone(onsale_datetime_from_blob(announced))

        self.assertEqual(classify_ticket_type({"title": "Regular gig at small venue", "summary": ""}), "regular_upcoming")

    def test_event_date_after_onsale_phrase_is_not_read_as_sale_date(self) -> None:
        # W9 regression (owner synthetic): the on-sale window must stay inside the
        # phrase's own clause. A following event date — whether the next sentence
        # (period) or the same one (comma/dash) — is the event's, never the
        # sale's, so it must not surface as "в продаже с <event date>".
        from news_digest.pipeline.editorial_contracts import classify_ticket_type, onsale_datetime_from_blob

        period = {"category": "venues_tickets", "title": "Tickets on sale soon. Event date 20 August 2026", "summary": ""}
        self.assertIsNone(onsale_datetime_from_blob(period))
        self.assertEqual(classify_ticket_type(period), "newly_listed")

        comma = {"category": "venues_tickets", "title": "Tickets on sale soon, event date 20 August 2026", "summary": ""}
        self.assertIsNone(onsale_datetime_from_blob(comma))
        self.assertEqual(classify_ticket_type(comma), "newly_listed")


class TicketConsolidationTest(unittest.TestCase):
    def test_consolidation_collapses_and_drops(self) -> None:
        cands = [
            # one artist at two venues/dates → two distinct event cards
            _tk("Dua Lipa", venue="AO Arena", date="2026-06-19"),
            _tk("Dua Lipa", venue="OVO Hydro", date="2026-06-22"),
            # festival fragments → one card + lineup
            _tk("Glasto Festival - Weekend Ticket", venue="Worthy Farm", date="2026-06-26",
                attractions=["Headliner A", "Headliner B"]),
            _tk("Sky presents the Glasto Festival - Saturday", venue="Worthy Farm", date="2026-06-27",
                attractions=["Headliner C"]),
            # premium upsell → dropped
            _tk("Coldplay - Venue Premium Tickets", venue="Wembley", date="2026-06-20"),
            # non-music → dropped
            _tk("Netball Grand Final", venue="Co-op Live", date="2026-06-20", segment="Sports"),
        ]
        _merge_multinight_ticket_runs(cands)
        _consolidate_tickets(cands)
        live = [c for c in cands if c.get("include")]
        titles = [c["title"] for c in live]
        self.assertEqual(sum("Dua Lipa" in t for t in titles), 2)           # distinct events preserved
        self.assertEqual(sum("Glasto Festival" in t for t in titles), 1)    # festival merged
        self.assertFalse(any("Premium" in t for t in titles))              # premium dropped
        self.assertFalse(any("Netball" in t for t in titles))              # non-music dropped
        festival = next(c for c in live if "Glasto" in c["title"])
        self.assertTrue(festival.get("festival_lineup"))                    # lineup carried


if __name__ == "__main__":
    unittest.main()
