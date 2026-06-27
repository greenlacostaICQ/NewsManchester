"""Ticket radar: card format (bold artist, structured genre, festival lineup)
and consolidation (festival → one card + lineup, tour → one per block, drop
premium/non-music). Pins the 2026-06-14 fixes.
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
            # one artist's tour across two cities → one card per block
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
        self.assertEqual(sum("Dua Lipa" in t for t in titles), 1)           # tour collapsed
        self.assertEqual(sum("Glasto Festival" in t for t in titles), 1)    # festival merged
        self.assertFalse(any("Premium" in t for t in titles))              # premium dropped
        self.assertFalse(any("Netball" in t for t in titles))              # non-music dropped
        festival = next(c for c in live if "Glasto" in c["title"])
        self.assertTrue(festival.get("festival_lineup"))                    # lineup carried


class ATierBudgetExemptionTest(unittest.TestCase):
    """W2 / RC4: outside-GM A-tier is capped in the morning digest (excess →
    ticket inventory); only GM/nearby A-tier stays cap-exempt. This supersedes
    the old 'A-tier never trimmed' rule for outside-GM venues — the rule that
    made every cap idle when the outside-GM pool was entirely A-tier
    (owner 2026-06-27 / #0011)."""

    def _kept(self, cand: dict, fps: list[str], section: str, cap: int) -> list[str]:
        from news_digest.pipeline.writer import _slice_counting_only_non_exempt
        lines = ["• x"] * len(fps)
        return _slice_counting_only_non_exempt(
            lines=lines, srcs=lines, fps=fps, scores=[0.0] * len(fps), titles=lines,
            candidate_by_fp=cand, section_name=section,
            counted_limit=cap, ignore_section_exemption=True,
        )[2]

    def test_outside_gm_a_tier_is_capped(self) -> None:
        cand: dict = {}
        fps: list[str] = []
        for i in range(8):  # 8 outside-GM A-tier, cap 6
            fp = f"a{i}"
            fps.append(fp)
            cand[fp] = {"primary_block": "outside_gm_tickets", "venue_scope": "outside",
                        "ticket_notability": {"tier": "A"}}
        kept = self._kept(cand, fps, "Крупные концерты вне GM", 6)
        self.assertEqual(sum(1 for f in kept if f.startswith("a")), 6)  # capped, not 8

    def test_gm_a_tier_stays_exempt(self) -> None:
        cand: dict = {}
        fps: list[str] = []
        for i in range(8):
            fp = f"g{i}"
            fps.append(fp)
            cand[fp] = {"primary_block": "ticket_radar", "venue_scope": "gm",
                        "ticket_notability": {"tier": "A"}}
        kept = self._kept(cand, fps, "Билеты / Ticket Radar", 6)
        self.assertEqual(sum(1 for f in kept if f.startswith("g")), 8)  # all GM A-tier kept

    def test_nearby_a_tier_stays_exempt(self) -> None:
        cand: dict = {}
        fps: list[str] = []
        for i in range(8):
            fp = f"n{i}"
            fps.append(fp)
            cand[fp] = {"primary_block": "outside_gm_tickets", "venue_scope": "nearby",
                        "ticket_notability": {"tier": "A"}}
        kept = self._kept(cand, fps, "Крупные концерты вне GM", 6)
        self.assertEqual(sum(1 for f in kept if f.startswith("n")), 8)  # nearby A-tier kept

    def test_global_cap_dropped_a_tier_goes_to_inventory(self) -> None:
        # P1 / RC4: outside-GM A-tier dropped by the GLOBAL budget cap (not just
        # the section cap) must land in the ticket inventory, not vanish.
        from news_digest.pipeline.writer import _hold_global_capped_a_tier
        outside = {"primary_block": "outside_gm_tickets", "venue_scope": "outside",
                   "ticket_notability": {"tier": "A"}}
        gm = {"primary_block": "ticket_radar", "venue_scope": "gm",
              "ticket_notability": {"tier": "A"}}
        candidate_by_fp = {"o1": outside, "g1": gm}
        dropped = [
            {"fingerprint": "o1", "title": "Outside A", "section": "X"},
            {"fingerprint": "g1", "title": "GM A", "section": "X"},  # exempt — ignored
        ]
        held: list[dict] = []
        _hold_global_capped_a_tier(dropped, candidate_by_fp, held)
        self.assertEqual([h["fingerprint"] for h in held], ["o1"])
        self.assertTrue(outside["ticket_inventory_held"])


if __name__ == "__main__":
    unittest.main()
