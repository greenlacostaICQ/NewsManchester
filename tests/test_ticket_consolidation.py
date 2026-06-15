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


if __name__ == "__main__":
    unittest.main()
