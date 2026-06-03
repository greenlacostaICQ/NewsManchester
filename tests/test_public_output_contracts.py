from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from news_digest.pipeline.collector.fallbacks import _weather_draft_line
from news_digest.pipeline.collector.weather import _met_office_practical_angle
from news_digest.pipeline.curator import _is_curator_protected
from news_digest.pipeline.editorial_contracts import attach_editorial_contract
from news_digest.pipeline.release import _final_loss_check, public_html_contract_errors
from news_digest.pipeline.ticket_notability import (
    enrich_ticket_notability,
    ticket_event_kind,
    ticket_headliner_candidates,
)
from news_digest.pipeline.writer import (
    _build_recurring_event_fallback_line,
    _build_football_fallback_line,
    _build_ticket_fallback_line,
    _cap_minor_bus_stop_lines,
    _draft_line_quality_errors,
    _final_replacement_line,
    _football_is_sport_news,
    _football_should_route_to_soft,
    _append_recovery_step,
    _repair_editorial_contract_line,
    _ticket_watch_decision,
)


class PublicOutputContractTests(unittest.TestCase):
    def _ticket_notability_cache(self, records: dict[str, dict]) -> Path:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "ticket_notability_cache.json"
        checked_at = "2026-06-02T12:00:00+01:00"
        artists = {}
        for key, record in records.items():
            payload = {
                "artist": record.get("artist", key),
                "kind": record.get("kind", "artist"),
                "tier": record.get("tier", "unknown"),
                "confidence": record.get("confidence", 0.0),
                "signal": record.get("signal", "test_cache"),
                "wikidata_id": record.get("wikidata_id", ""),
                "sitelinks": record.get("sitelinks", 0),
                "signals": record.get("signals", {"sitelinks": record.get("sitelinks", 0)}),
                "checked_at": checked_at,
            }
            artists[key] = payload
        path.write_text(json.dumps({"version": 1, "artists": artists}), encoding="utf-8")
        return path

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

    def test_major_artist_anywhere_uses_notability_cache_not_manual_list(self) -> None:
        cache_path = self._ticket_notability_cache(
            {
                "ricky martin": {
                    "artist": "Ricky Martin",
                    "tier": "A",
                    "confidence": 0.95,
                    "signal": "wikidata_sitelinks",
                    "sitelinks": 68,
                }
            }
        )
        candidate = {
            "category": "venues_tickets",
            "primary_block": "outside_gm_tickets",
            "title": "Ricky Martin: UK show — event 2026-09-12",
            "summary": "Small UK town | Pop | event_date=2026-09-12 19:00 | ticket_type=newly_listed",
            "event": {"venue": "Scarborough Open Air Theatre", "date_start": "2026-09-12"},
        }
        with patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
            notability = enrich_ticket_notability(candidate, cache_path)
        candidate["ticket_notability"] = {
            "artist": notability.artist,
            "kind": notability.kind,
            "tier": notability.tier,
            "signal": notability.signal,
            "headliners": list(notability.headliners),
            "signals": notability.signals or {},
        }
        line = _build_ticket_fallback_line(candidate)
        self.assertEqual(notability.tier, "A")
        self.assertIn("Ricky Martin", line)
        self.assertEqual(_ticket_watch_decision(candidate)["decision"], "show")

    def test_live_film_event_without_real_headliner_is_hidden(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Dirty Dancing Live in Concert - with band and singers — event 2026-06-03",
            "summary": "Bridgewater Hall | Manchester | Other | event_date=2026-06-03 18:00 | ticket_type=event_this_week",
            "event": {"venue": "Bridgewater Hall", "date_start": "2026-06-03"},
        }
        with patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
            notability = enrich_ticket_notability(candidate, self._ticket_notability_cache({}))
        candidate["ticket_notability"] = {"artist": notability.artist, "kind": notability.kind, "tier": notability.tier}
        self.assertEqual(ticket_event_kind(candidate), "non_artist_show")
        self.assertEqual(_build_ticket_fallback_line(candidate), "")

    def test_ticketmaster_title_noise_is_removed_before_artist_lookup(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Kings Of Leon Special Guest Snuts Sat 4 Jul 2026 Multiple times — event 2026-07-04",
            "summary": "Co-op Live | Manchester | Rock | event_date=2026-07-04 19:00 | ticket_type=newly_listed",
            "event": {
                "venue": "Co-op Live",
                "date_start": "2026-07-04",
                "attractions": [{"name": "Kings Of Leon", "id": "tm-kol"}],
            },
        }
        self.assertEqual(ticket_headliner_candidates(candidate)[0], "Kings Of Leon")

    def test_festival_lineup_promotes_strong_headliner_inside_card(self) -> None:
        cache_path = self._ticket_notability_cache(
            {
                "metallica": {
                    "artist": "Metallica",
                    "tier": "A",
                    "confidence": 0.95,
                    "signal": "wikidata_sitelinks",
                    "sitelinks": 91,
                },
                "local dj": {"artist": "Local DJ", "tier": "D", "confidence": 0.3, "sitelinks": 1},
            }
        )
        candidate = {
            "category": "venues_tickets",
            "primary_block": "outside_gm_tickets",
            "title": "North Coast Open Air Festival — event 2026-08-01",
            "summary": "Scarborough | Rock | event_date=2026-08-01 16:00 | ticket_type=newly_listed | lineup=Local DJ, Metallica",
            "event": {"venue": "Scarborough Open Air Theatre", "date_start": "2026-08-01"},
        }
        with patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
            notability = enrich_ticket_notability(candidate, cache_path)
        candidate["ticket_notability"] = {
            "artist": notability.artist,
            "kind": notability.kind,
            "tier": notability.tier,
            "signal": notability.signal,
            "headliners": list(notability.headliners),
            "signals": notability.signals or {},
        }
        self.assertIn("Metallica", ticket_headliner_candidates(candidate))
        self.assertEqual(notability.artist, "Metallica")
        self.assertEqual(notability.kind, "lineup_or_show")
        self.assertEqual(_ticket_watch_decision(candidate)["decision"], "show")
        self.assertIn("Metallica", _build_ticket_fallback_line(candidate))

    def test_musicbrainz_ticketmaster_signal_promotes_to_b_tier(self) -> None:
        cache_path = self._ticket_notability_cache(
            {
                "known touring band": {
                    "artist": "Known Touring Band",
                    "tier": "B",
                    "confidence": 0.68,
                    "signal": "musicbrainz_ticketmaster",
                    "signals": {"sitelinks": 0, "musicbrainz_score": 96},
                }
            }
        )
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Known Touring Band — event 2026-07-01",
            "summary": "The Ritz | Manchester | Rock | event_date=2026-07-01 19:00 | ticket_type=newly_listed",
            "event": {"venue": "The Ritz", "date_start": "2026-07-01", "attractions": [{"name": "Known Touring Band", "id": "abc123"}]},
        }
        with patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
            notability = enrich_ticket_notability(candidate, cache_path)
        self.assertEqual(notability.tier, "B")
        self.assertEqual(notability.signal, "musicbrainz_ticketmaster")
        self.assertTrue((notability.signals or {}).get("ticketmaster_attraction"))

    def test_spotify_lastfm_signals_can_promote_without_manual_artist_list(self) -> None:
        cache_path = self._ticket_notability_cache(
            {
                "streaming star": {
                    "artist": "Streaming Star",
                    "confidence": 0.9,
                    "signals": {
                        "sitelinks": 0,
                        "spotify_popularity": 82,
                        "spotify_followers": 2_500_000,
                        "lastfm_listeners": 1_800_000,
                    },
                }
            }
        )
        candidate = {
            "category": "venues_tickets",
            "primary_block": "outside_gm_tickets",
            "title": "Streaming Star — event 2026-10-01",
            "summary": "UK | Pop | event_date=2026-10-01 19:00 | ticket_type=newly_listed",
            "event": {"venue": "Small UK venue", "date_start": "2026-10-01"},
        }
        with patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
            notability = enrich_ticket_notability(candidate, cache_path)
        self.assertEqual(notability.tier, "A")
        self.assertEqual(notability.signal, "streaming_popularity")
        self.assertGreaterEqual(int((notability.signals or {}).get("spotify_popularity") or 0), 80)

    def test_ticket_golden_names_are_a_tier_when_external_signal_exists(self) -> None:
        cache_path = self._ticket_notability_cache(
            {
                "the weeknd": {"artist": "The Weeknd", "tier": "A", "confidence": 0.95, "sitelinks": 104},
                "imagine dragons": {"artist": "Imagine Dragons", "tier": "A", "confidence": 0.95, "sitelinks": 75},
            }
        )
        for title, expected in (
            ("The Weeknd: After Hours Til Dawn Tour — event 2026-06-11", "The Weeknd"),
            ("Imagine Dragons: Loom World Tour — event 2026-07-20", "Imagine Dragons"),
        ):
            candidate = {
                "category": "venues_tickets",
                "primary_block": "outside_gm_tickets",
                "title": title,
                "summary": "UK | Rock | event_date=2026-07-20 19:00 | ticket_type=major_upcoming",
                "event": {"venue": "UK arena", "date_start": "2026-07-20"},
            }
            with patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
                notability = enrich_ticket_notability(candidate, cache_path)
            self.assertEqual(notability.artist, expected)
            self.assertEqual(notability.tier, "A")

    def test_quality_gate_rejects_old_ticket_machine_phrase(self) -> None:
        errors = _draft_line_quality_errors(
            {"category": "venues_tickets", "primary_block": "ticket_radar"},
            "• The Weeknd — 11 июня, Etihad Stadium. Почему в радаре: крупная площадка.",
        )
        self.assertTrue(any("machine explanation" in error for error in errors))

    def test_rendered_html_replay_pack_catches_historical_public_failures(self) -> None:
        bad_html = (
            "<b>Greater Manchester Brief — 2026-06-01, 08:10</b>\n"
            "<b>Погода</b>\n"
            "• Погода: дождь вероятен — проверьте локальный радар.\n"
            "<b>Общественный транспорт сегодня</b>\n"
            "• В Манчестере закрыты две станции метро — Shudehill и Market Street.\n"
            "<b>Билеты / Ticket Radar</b>\n"
            "• The Weeknd — 11 июня, Etihad Stadium. Почему в радаре: крупная площадка.\n"
            "• Manchester Academy — This website makes extensive use of JavaScript.\n"
            "• Artist — 12 июня. Билеты и детали берите на официальной странице.\n"
        )
        errors = public_html_contract_errors(bad_html)
        self.assertTrue(any("weather_local_radar" in error for error in errors))
        self.assertTrue(any("metrolink_written_as_metro" in error for error in errors))
        self.assertTrue(any("ticket_machine_explanation" in error for error in errors))
        self.assertTrue(any("source_chrome_passthrough" in error for error in errors))
        self.assertTrue(any("ticket_generic_cta" in error for error in errors))

    def test_recovery_plan_records_ordered_repair_attempts(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Unknown artist",
            "story_frame": {"missing_facts": ["venue"]},
        }
        _append_recovery_step(candidate, "ticket_structured_recovery", "attempted", missing=["venue"])
        _append_recovery_step(candidate, "ticket_structured_recovery", "held", missing=["venue"])
        plan = candidate["recovery_plan"]
        self.assertEqual(plan["sequence"][0], "best_available_source")
        self.assertIn("ticket_structured_card", plan["sequence"])
        self.assertEqual(plan["outcome"], "held")
        self.assertEqual(plan["missing_facts"], ["venue"])
        self.assertEqual(plan["attempts"][0]["stage"], "structured_repair")

    def test_story_frame_is_attached_with_missing_facts(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Police incident in Manchester",
            "summary": "Police remain at the scene.",
            "source_label": "MEN",
        }
        attach_editorial_contract(candidate)
        frame = candidate["story_frame"]
        self.assertIn("what_happened", frame)
        self.assertIn("missing_facts", frame)
        self.assertIn("why_now", frame["missing_facts"])

    def test_generic_incident_line_fails_without_story_frame_facts(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Police incident in Manchester",
            "summary": "Police remain at the scene.",
            "source_label": "MEN",
        }
        attach_editorial_contract(candidate)
        errors = _draft_line_quality_errors(
            candidate,
            "• Manchester: полиция продолжает работу на месте инцидента. Это важный момент для района.",
        )
        self.assertTrue(any("story_frame" in error for error in errors))

    def test_final_replacement_repairs_bad_official_football_line(self) -> None:
        candidate = {
            "category": "football",
            "primary_block": "football",
            "source_label": "Manchester United",
            "title": "Manchester United sign new striker before Premier League fixture",
            "summary": "The transfer is complete and the player could be available for Saturday's match.",
        }
        line = _build_football_fallback_line(candidate)
        self.assertIn("Manchester United", line)
        self.assertEqual(_final_replacement_line(candidate), line)

    def test_minor_bus_stop_closures_are_capped_at_three(self) -> None:
        lines = [
            f"• Автобусы: закрыта остановка Stop {idx} на Street в Area — ремонтные работы."
            for idx in range(5)
        ]
        capped, *_rest, dropped = _cap_minor_bus_stop_lines(lines, ["TfGM"] * 5, [str(i) for i in range(5)], [0.0] * 5, lines)
        self.assertEqual(len(capped), 3)
        self.assertEqual(len(dropped), 2)

    def test_final_loss_check_explains_missing_facts(self) -> None:
        candidate = {
            "fingerprint": "critical-1",
            "include": True,
            "category": "gmp",
            "primary_block": "today_focus",
            "title": "Police appeal after stabbing in Wigan",
            "source_label": "BBC Manchester",
            "protected_lane": {"protected": True, "lanes": ["public_safety"]},
            "story_frame": {"missing_facts": ["who_affected"], "what_happened": "stabbing"},
            "recovery_trace": [{"step": "hard_news_recovery", "outcome": "held"}],
        }
        report = _final_loss_check(
            candidates_report={"candidates": [candidate]},
            writer_report={"dropped_candidates": [{"fingerprint": "critical-1", "reasons": ["Missing draft_line."]}]},
            rendered_fingerprints=set(),
            dedupe_memory={},
        )
        item = report["items"][0]
        self.assertIn("human_reason", item)
        self.assertTrue(item["missing_facts"])
        self.assertIn("Не хватило:", item["human_reason"])
        self.assertEqual(item["recovery_trace"][0]["step"], "hard_news_recovery")


if __name__ == "__main__":
    unittest.main()
