from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from news_digest.pipeline.collector.fallbacks import _weather_draft_line
from news_digest.pipeline.collector.weather import _met_office_practical_angle
from news_digest.pipeline.curator import _is_curator_protected
from news_digest.pipeline.ticket_notability import enrich_ticket_notability
from news_digest.pipeline.writer import (
    _build_recurring_event_fallback_line,
    _build_ticket_fallback_line,
    _draft_line_quality_errors,
    _football_is_sport_news,
    _football_should_route_to_soft,
    _repair_editorial_contract_line,
    _ticket_watch_decision,
)


class PublicOutputContractTests(unittest.TestCase):
    def test_weather_contract_never_mentions_radar(self) -> None:
        practical = _met_office_practical_angle("", "heavy rain", 95)
        line = _weather_draft_line(13, 18, 95, practical, "Met Office")
        self.assertNotIn("радар", line.lower())
        self.assertIn("зонт", line.lower())

    def test_transport_repair_uses_metrolink_not_metro(self) -> None:
        candidate = {"primary_block": "transport"}
        line, reasons = _repair_editorial_contract_line(
            candidate,
            "• В Манчестере закрыты две станции метро — Shudehill и Market Street.",
        )
        self.assertIn("Metrolink", line)
        self.assertNotIn("метро", line.lower())
        self.assertIn("metrolink_not_metro", reasons)

    def test_unknown_artist_does_not_pass_only_for_major_venue(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Unknown Arena Act — event 2026-06-10",
            "summary": "AO Arena | Manchester | Pop | event_date=2026-06-10 19:00 | ticket_type=on_sale_now",
            "event": {"venue": "AO Arena", "date_start": "2026-06-10"},
            "ticket_notability": {"artist": "Unknown Arena Act", "kind": "artist", "tier": "unknown"},
        }
        self.assertEqual(_build_ticket_fallback_line(candidate), "")

    def test_global_artist_passes_outside_gm_without_manual_artist_list(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "outside_gm_tickets",
            "title": "Example Global Artist: World Tour — event 2026-07-10",
            "summary": "Smalltown Bowl | UK | Pop | event_date=2026-07-10 19:00 | ticket_type=major_upcoming",
            "event": {"venue": "Smalltown Bowl", "date_start": "2026-07-10"},
            "ticket_notability": {"artist": "Example Global Artist", "kind": "artist", "tier": "A"},
        }
        line = _build_ticket_fallback_line(candidate)
        self.assertIn("Example Global Artist", line)
        self.assertNotIn("Почему в радаре", line)

    def test_diaspora_ticket_is_protected_from_popularity_filter(self) -> None:
        with patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
            candidate = {
                "category": "russian_speaking_events",
                "primary_block": "russian_events",
                "title": "Goran Bregovic (London)",
                "summary": "London | 20:30 | event_date=2026-06-10 20:30",
            }
            notability = enrich_ticket_notability(candidate)
        self.assertEqual(notability.tier, "protected")
        self.assertEqual(notability.signal, "diaspora_protected")

    def test_curator_does_not_drop_ticket_watchlists_for_gm_only_reasoning(self) -> None:
        self.assertTrue(_is_curator_protected({"primary_block": "outside_gm_tickets"}))
        self.assertTrue(_is_curator_protected({"primary_block": "ticket_radar"}))

    def test_football_soft_item_does_not_count_as_football_minimum(self) -> None:
        candidate = {
            "primary_block": "football",
            "title": "Ruben Dias says he draws line over Maya Jama break-up speculation",
            "summary": "Manchester City defender responds to personal life gossip.",
        }
        self.assertFalse(_football_is_sport_news(candidate))
        self.assertTrue(_football_should_route_to_soft(candidate))

    def test_football_sport_item_counts_toward_football_minimum(self) -> None:
        candidate = {
            "primary_block": "football",
            "title": "Manchester United sign new striker before Premier League fixture",
            "summary": "The transfer is complete and the player could be available for Saturday's match.",
        }
        self.assertTrue(_football_is_sport_news(candidate))
        self.assertFalse(_football_should_route_to_soft(candidate))

    def test_recurring_event_without_concrete_occurrence_is_not_rendered_as_generic_day(self) -> None:
        candidate = {
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "title": "Stockport Makers Market",
            "summary": "A recurring local market with traders and food.",
            "event": {"is_recurring": True},
        }
        self.assertEqual(_build_recurring_event_fallback_line(candidate), "")

    def test_recurring_event_uses_future_event_date_when_weekday_missing(self) -> None:
        candidate = {
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "title": "Stockport Makers Market",
            "summary": "A recurring local market with traders and food.",
            "event": {"is_recurring": True, "date_start": "2026-06-13"},
        }
        with patch("news_digest.pipeline.editorial_contracts.now_london") as fake_now:
            fake_now.return_value.date.return_value = __import__("datetime").date(2026, 6, 2)
            line = _build_recurring_event_fallback_line(candidate)
        self.assertIn("13 июня", line)
        self.assertNotIn("ближайший день расписания", line.lower())

    def test_ticket_decision_explains_show_and_hide(self) -> None:
        show = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Example Global Artist: World Tour — event 2026-07-10",
            "summary": "Smalltown Bowl | UK | Pop | event_date=2026-07-10 19:00 | ticket_type=major_upcoming",
            "event": {"venue": "Smalltown Bowl", "date_start": "2026-07-10"},
            "ticket_notability": {"artist": "Example Global Artist", "kind": "artist", "tier": "A", "signal": "wikidata_sitelinks"},
        }
        hide = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Unknown Arena Act — event 2026-06-10",
            "summary": "AO Arena | Manchester | Pop | event_date=2026-06-10 19:00 | ticket_type=on_sale_now",
            "event": {"venue": "AO Arena", "date_start": "2026-06-10"},
            "ticket_notability": {"artist": "Unknown Arena Act", "kind": "artist", "tier": "unknown", "signal": "not_found"},
        }
        self.assertEqual(_ticket_watch_decision(show)["decision"], "show")
        hidden = _ticket_watch_decision(hide)
        self.assertEqual(hidden["decision"], "hide")
        self.assertIn("threshold", hidden)

    def test_quality_gate_rejects_old_ticket_machine_phrase(self) -> None:
        errors = _draft_line_quality_errors(
            {"category": "venues_tickets", "primary_block": "ticket_radar"},
            "• The Weeknd — 11 июня, Etihad Stadium. Почему в радаре: крупная площадка.",
        )
        self.assertTrue(any("machine explanation" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
