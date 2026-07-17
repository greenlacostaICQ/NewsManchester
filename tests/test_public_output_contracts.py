from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest

from news_digest.pipeline.release import _final_loss_check, public_html_contract_errors
from unittest.mock import patch

from news_digest.pipeline.common import now_london
from news_digest.pipeline.collector.fallbacks import _weather_draft_line
from news_digest.pipeline.collector.weather import _met_office_practical_angle
from news_digest.pipeline.curator import _is_curator_protected
from news_digest.pipeline.editorial_contracts import attach_editorial_contract
from news_digest.pipeline.transport_language import repair_transport_line_language
from news_digest.pipeline.ticket_notability import (
    _tier_from_signals,
    enrich_ticket_notability,
    ticket_event_kind,
    ticket_headliner_candidates,
)
from news_digest.pipeline.writer import (
    _build_recurring_event_fallback_line,
    _build_event_fallback_line,
    _build_football_fallback_line,
    _build_ticket_fallback_line,
    _cap_minor_bus_stop_lines,
    _draft_line_quality_errors,
    _final_replacement_line,
    _football_is_sport_news,
    _football_should_route_to_soft,
    _is_outside_current_weekend_candidate,
    _weekend_activity_score,
    _today_focus_candidate_is_eligible,
    _transport_line_priority,
    _next_7_event_decision,
    _append_recovery_step,
    _repair_editorial_contract_line,
    _section_priority_score,
    _ticket_watch_decision,
)


class PublicOutputContractTests(unittest.TestCase):
    def _ticket_notability_cache(self, records: dict[str, dict]) -> Path:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "ticket_notability_cache.json"
        # Keep the fixture inside the 30-day recheck window relative to the real
        # clock, otherwise the cached tiers age out to "unknown" and these
        # contract tests become a time-bomb (they were red once the repo clock
        # passed 30 days after a hardcoded 2026-06-02 timestamp).
        checked_at = (now_london() - timedelta(days=1)).isoformat()
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

    def test_transport_groups_minor_bus_stops_and_keeps_metrolink_priority(self) -> None:
        lines = [
            "• Автобусы: остановка на Whitworth Street напротив Palace Theatre закрыта из-за строительных работ; используйте остановки до или после. <a href=\"https://tfgm.com\">TfGM</a>",
            "• Автобусы: остановки у St John's Primary School на Atherton Road закрыты из-за работ; используйте остановки до или после. <a href=\"https://tfgm.com\">TfGM</a>",
            "• Metrolink: до 14 июня нет трамваев между Victoria и Rochdale. <a href=\"https://tfgm.com\">TfGM</a>",
            "• Автобусы: остановка Failsworth Pole на Oldham Road закрыта из-за работ; используйте остановки до или после. <a href=\"https://tfgm.com\">TfGM</a>",
        ]
        srcs = ["TfGM"] * len(lines)
        fps = [f"fp-{idx}" for idx in range(len(lines))]
        scores = [0.0] * len(lines)
        titles = [""] * len(lines)
        out_lines, *_rest, dropped = _cap_minor_bus_stop_lines(lines, srcs, fps, scores, titles)
        self.assertGreater(_transport_line_priority(lines[2]), _transport_line_priority(lines[0]))
        self.assertEqual(len(out_lines), 2)
        self.assertIn("Metrolink", out_lines[0])
        self.assertIn("3 мелких закрытий остановок", " ".join(out_lines))
        self.assertEqual(len(dropped), 2)

    def test_follow_up_line_leads_with_new_phase(self) -> None:
        candidate = {
            "change_type": "follow_up",
            "change_phase": "charged",
            "why_now": "update_today",
            "title": "Kyle Howard charged with murder of Keeley Aspinoll",
            "summary": "A man has been charged after Keeley Aspinoll was found dead in Rochdale.",
        }
        repaired, reasons = _repair_editorial_contract_line(
            candidate,
            "• Rochdale: 25-летний Кайл Ховард обвинён в убийстве Кили Аспинолл; родственники заявили, что она «ушла слишком рано».",
        )
        self.assertIn("follow_up_leads_with_change", reasons)
        # Phase leads as natural prose, no machine "обновление:" marker.
        self.assertIn("Предъявлено обвинение", repaired)
        self.assertNotIn("обновление:", repaired.lower())
        self.assertNotIn("ушла слишком рано", repaired)

    def test_follow_up_phase_must_match_story_type(self) -> None:
        planning = {
            "change_type": "new_phase",
            "change_phase": "sentenced",
            "title": "Council approves 189 homes at Dutton Street",
            "summary": "Manchester Council approved a housing development.",
            "category": "media_layer",
            "primary_block": "today_focus",
        }
        repaired, reasons = _repair_editorial_contract_line(
            planning,
            "• Strangeways: совет одобрил 189 квартир на Dutton Street.",
        )
        self.assertNotIn("follow_up_leads_with_change", reasons)
        self.assertNotIn("вынесен приговор", repaired)

        no_charge = {
            "change_type": "new_phase",
            "change_phase": "charged",
            "title": "GMP staff member sacked but will not face criminal charges",
            "summary": "There was not enough evidence to charge him with a criminal offence.",
            "category": "media_layer",
            "primary_block": "last_24h",
        }
        repaired, reasons = _repair_editorial_contract_line(
            no_charge,
            "• Manchester: сотрудник GMP уволен после жалобы.",
        )
        self.assertNotIn("follow_up_leads_with_change", reasons)
        self.assertNotIn("предъявлено обвинение", repaired)

    def test_incident_and_court_russian_calques_are_repaired(self) -> None:
        candidate = {"category": "gmp", "primary_block": "last_24h"}
        repaired, reasons = _repair_editorial_contract_line(
            candidate,
            "• Manchester: полиция расследует тройное ножевое ранение после отдельных ножевых атак; следствие пришло к открытым выводам.",
        )
        self.assertIn("triple_stabbing_ru", reasons)
        self.assertIn("separate_stabbings_ru", reasons)
        self.assertIn("open_conclusion_ru", reasons)
        self.assertIn("нападение с ножом, в котором пострадали трое", repaired)
        self.assertIn("двух разных нападений с ножом", repaired)
        self.assertIn("точную причину смерти", repaired)

    def test_unclear_entity_is_explained_inside_line(self) -> None:
        candidate = {
            "title": "ANOTR concert cancelled after crowd trouble",
            "summary": "Police said the ANOTR event in Manchester was cancelled.",
        }
        repaired, reasons = _repair_editorial_contract_line(
            candidate,
            "• Manchester City Centre: полиция подтвердила, что организаторы ANOTR отменили концерт после беспорядков.",
        )
        self.assertIn("explained_anotr", reasons)
        self.assertIn("электронного дуэта ANOTR", repaired)

    def test_common_russian_lint_repairs_visible_prose(self) -> None:
        repaired, reasons = _repair_editorial_contract_line(
            {"category": "media_layer", "primary_block": "city_watch"},
            "• В Greater Manchesterе Клр. сообщил о фуд-дестинации и деле о киберфлешинге.",
        )
        self.assertIn("gm_case_ru", reasons)
        self.assertIn("councillor_ru", reasons)
        self.assertIn("food_destination_ru", reasons)
        self.assertIn("cyberflashing_ru", reasons)
        self.assertNotIn("Greater Manchesterе", repaired)
        self.assertNotIn("Клр.", repaired)

    def test_event_fallback_prefers_structured_date_over_summary_noise(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "future_announcements",
            "title": "David Gray at Barton Aerodrome",
            "summary": "Past & Present Tour | Thursday 2nd July | 18:00-22:30",
            "practical_angle": "Билеты доступны на сайте организатора.",
            "event": {
                "is_event": True,
                "event_name": "David Gray",
                "venue": "Barton Aerodrome",
                "date_start": "2026-07-06T18:00:00+01:00",
            },
        }
        line = _build_event_fallback_line(candidate)
        self.assertIn("6 июля", line)
        self.assertNotIn("2 июля", line)

    def test_event_fallback_refuses_source_name_title_and_strips_chrome(self) -> None:
        # «The SK Lowdown» (the site, label «SK Lowdown Markets») carries zero
        # event facts — no card beats «• The SK Lowdown.» (0030 show=renderable).
        sourceish = {
            "category": "food_openings",
            "primary_block": "openings",
            "title": "The SK Lowdown",
            "source_label": "SK Lowdown Markets",
            "event": {"is_event": True, "event_name": "The SK Lowdown"},
        }
        self.assertEqual(_build_event_fallback_line(sourceish), "")
        # A real event title with trailing site chrome sheds the chrome and the
        # venue sheds its «, England, SK1 1YG United Kingdom» address tail.
        real = {
            "category": "food_openings",
            "primary_block": "openings",
            "title": "Stockport's Asian Food Night Market is back — The SK Lowdown",
            "source_label": "SK Lowdown Markets",
            "practical_angle": "",
            "event": {
                "is_event": True,
                "event_name": "Stockport's Asian Food Night Market is back — The SK Lowdown",
                "venue": "Churchgate Stockport, England, SK1 1YG United Kingdom",
                "date_start": "2026-07-10T18:00:00+01:00",
            },
        }
        line = _build_event_fallback_line(real)
        self.assertIn("Asian Food Night Market", line)
        self.assertNotIn("SK Lowdown", line)
        self.assertIn("Churchgate Stockport", line)
        self.assertNotIn("United Kingdom", line)

    def test_event_fallback_repairs_generic_availability_cta(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "title": "The Ballad of Johnny & June",
            "summary": "The Ballad of Johnny & June is a theatre show at The Lowry.",
            "practical_angle": "Проверьте наличие мест перед посещением.",
            "event": {
                "is_event": True,
                "event_name": "The Ballad of Johnny & June",
                "venue": "The Lowry",
                "date_start": "2026-06-11T19:30:00+01:00",
            },
        }
        with patch("news_digest.pipeline.writer.now_london") as fake_now:
            fake_now.return_value = datetime(2026, 6, 11)
            line = _build_event_fallback_line(candidate)
        self.assertIn("11 июня", line)
        self.assertIn("The Lowry", line)
        self.assertNotIn("Проверьте наличие мест", line)

    def test_today_focus_future_only_item_is_not_eligible(self) -> None:
        candidate = {
            "category": "public_services",
            "primary_block": "today_focus",
            "title": "Prestwich library to close on 14 June for carnival",
            "summary": "Prestwich Library will close on 14 June from 09:30 because of the carnival.",
            "editorial_contract": {
                "story_type": "local_service_change",
                "publish_tier": "strong",
                "event_shape": "none",
            },
        }
        with patch("news_digest.pipeline.writer.now_london") as fake_now:
            fake_now.return_value = datetime(2026, 6, 11)
            self.assertFalse(_today_focus_candidate_is_eligible(candidate, "• Prestwich: библиотека закроется 14 июня."))

    def test_next_7_final_date_gate_moves_far_event(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "title": "Robyn at Co-op Live",
            "summary": "Robyn performs at Co-op Live on 27 June.",
            "event": {
                "is_event": True,
                "event_name": "Robyn",
                "venue": "Co-op Live",
                "date_start": "2026-06-27T19:00:00+01:00",
            },
        }
        with patch("news_digest.pipeline.writer.now_london") as fake_now:
            fake_now.return_value = datetime(2026, 6, 11)
            decision, reason = _next_7_event_decision(candidate)
        self.assertEqual(decision, "move_future")
        self.assertIn("16 day", reason)

    def test_city_watch_clear_short_story_does_not_fail_length_only(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "city_watch",
            "title": "Timperley supermarket incident",
            "summary": "Police attended a supermarket in Timperley after a local incident.",
            "story_frame": {"missing_facts": []},
        }
        line = "• Timperley: полиция выезжала к супермаркету после сообщения о нарушении порядка; район проверили, угрозы для жителей не подтвердили."
        errors = _draft_line_quality_errors(candidate, line)
        self.assertFalse([err for err in errors if "long-format category needs" in err])

    def test_weekend_car_boot_fallback_uses_source_facts_not_title_stub(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_label": "Bolton Car Boot Sale",
            "title": "The Carboot Directory",
            "summary": "Huge Sunday car boot at Bolton's Macron Stadium (BL6 6JW), Feb-Dec.",
            "lead": "300+ stalls, £1 per car entry, catering, toilets and dog-friendly.",
            "evidence_text": "The site is open to buyers from 7:00 AM, with entry from £1.",
            "event": {
                "is_event": True,
                "event_name": "Bolton Car Boot Sale",
                "venue": "Bolton Car Boot Sale",
                "date_start": "2026-06-14T00:00:00+01:00",
                "is_recurring": True,
            },
        }
        line = _build_event_fallback_line(candidate)
        self.assertIn("14 июня", line)
        self.assertIn("Macron Stadium", line)
        self.assertIn("для покупателей с 7:00", line)
        self.assertIn("вход £1", line)
        self.assertIn("более 300 продавцов", line)
        self.assertNotIn("Bolton Car Boot Sale — Bolton Car Boot Sale", line)
        self.assertNotIn("Проверьте наличие мест", line)

    @patch("news_digest.pipeline.weekend_inventory.now_london")
    @patch("news_digest.pipeline.writer.now_london")
    def test_weekend_car_boot_fallback_uses_nearest_source_date_and_own_venue(self, mock_now, mock_weekend_now) -> None:
        fixed_now = datetime.fromisoformat("2026-06-12T09:00:00+01:00")
        mock_now.return_value = fixed_now
        # The weekend occurrence date is computed in weekend_inventory, which
        # keeps its own now_london — patch it too or the "next Saturday" falls
        # on the real wall-clock date instead of the mocked one.
        mock_weekend_now.return_value = fixed_now
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_label": "Barton Aerodrome Car Boot",
            "title": "Barton Aerodrome Car Boot Sale",
            "summary": "Barton Aerodrome hosts regular 2026 car boot sales.",
            "lead": "Next dates: Saturday, 13 June 2026; Saturday, 20 June 2026.",
            "evidence_text": (
                "Next dates Saturday, 13 June 2026. Buyers from 9am. "
                "Dogs welcome. Nearby unrelated listing: Altrincham Market."
            ),
            "event": {
                "is_event": True,
                "event_name": "Barton Aerodrome Car Boot Sale",
                "venue": "Barton Aerodrome",
                "date_start": "2026-06-06T00:00:00+01:00",
                "is_recurring": True,
            },
        }

        line = _build_event_fallback_line(candidate)

        self.assertIn("13 июня", line)
        self.assertIn("Barton Aerodrome", line)
        self.assertNotIn("6 июня", line)
        self.assertNotIn("Altrincham Market", line)

    def test_weekend_seller_market_page_is_not_rendered_as_visitor_activity(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_label": "New Smithfield Sunday Market",
            "title": "Casual trading",
            "summary": "You can sell things at New Smithfield Market every Sunday without needing to become a regular trader.",
            "lead": "Apply for a stall before trading at New Smithfield.",
            "event": {
                "is_event": True,
                "event_name": "Markets Sunday trading at New Smithfield Market",
                "venue": "New Smithfield Market You",
                "is_recurring": True,
            },
        }
        self.assertEqual(_build_event_fallback_line(candidate), "")

    def test_weekend_score_puts_clean_market_above_dirty_seller_page(self) -> None:
        clean = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_label": "Bolton Car Boot Sale",
            "title": "Bolton Car Boot Sale",
            "summary": "Huge Sunday car boot at Macron Stadium with 300+ stalls and entry from £1.",
            "event": {"is_event": True, "date_start": "2026-06-14", "event_name": "Bolton Car Boot Sale"},
        }
        dirty = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_label": "New Smithfield Sunday Market",
            "title": "Casual trading",
            "summary": "You can sell things at New Smithfield Market every Sunday without needing to become a regular trader.",
            "event": {"is_event": True, "event_name": "Markets Sunday trading at New Smithfield Market"},
        }
        self.assertGreater(
            _weekend_activity_score(clean, _build_event_fallback_line(clean)),
            _weekend_activity_score(dirty, _build_event_fallback_line(dirty)),
        )

    def test_weekend_window_uses_structured_event_date(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Autisk Family Fun Day",
            "summary": "Family activity with food and crafts.",
            "event": {"is_event": True, "date_start": "2026-08-29", "event_name": "Autisk Family Fun Day"},
        }
        with patch("news_digest.pipeline.writer.now_london") as fake_now:
            fake_now.return_value = datetime(2026, 6, 11)
            self.assertTrue(_is_outside_current_weekend_candidate(candidate))
            self.assertLess(_weekend_activity_score(candidate, _build_event_fallback_line(candidate)), 0)

    def test_weekend_non_gm_market_does_not_win_expanded_block(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_label": "Warrington Car Boot Sale",
            "title": "Warrington Car Boot Sale",
            "summary": "Family car boot with toilets and refreshments.",
            "event": {"is_event": True, "event_name": "Warrington Car Boot Sale", "is_recurring": True},
        }
        self.assertLess(_weekend_activity_score(candidate, _build_event_fallback_line(candidate)), 0)

    def test_weekend_market_fallback_removes_pedddle_seo_tail(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "source_label": "Pedddle Makers Market",
            "title": "Chorlton Makers Market | Markets in Manchester - Pedddle",
            "summary": "Makers market with craft stalls, food and free entry.",
            "event": {"is_event": True, "event_name": "Chorlton Makers Market | Markets in Manchester - Pedddle", "is_recurring": True},
        }
        line = _build_event_fallback_line(candidate)
        self.assertIn("Chorlton Makers Market", line)
        self.assertNotIn("Pedddle", line)
        self.assertNotIn("Markets in Manchester", line)
        self.assertNotIn("Markets in", line)

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

    def test_outside_gm_b_tier_old_sale_is_not_public(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "outside_gm_tickets",
            "title": "Known B Artist — event 2026-06-20 — public sale 2025-11-14 10:00",
            "summary": "M&S Bank Arena Liverpool | Liverpool | Rock | event_date=2026-06-20 19:00 | public_onsale=2025-11-14 10:00",
            "event": {"venue": "M&S Bank Arena Liverpool", "date_start": "2026-06-20T19:00:00+01:00"},
            "ticket_type": "old_public_sale",
            "ticket_notability": {"artist": "Known B Artist", "kind": "artist", "tier": "B"},
        }
        self.assertEqual(_ticket_watch_decision(candidate)["decision"], "hide")
        self.assertEqual(_build_ticket_fallback_line(candidate), "")

    def test_ticket_sorting_keeps_global_this_week_above_b_tier(self) -> None:
        global_item = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "The Weeknd: After Hours Til Dawn Tour — event 2026-06-11",
            "summary": "Etihad Stadium | Manchester | Hip-Hop/Rap | event_date=2026-06-11 17:00",
            "event": {"venue": "Etihad Stadium", "date_start": "2026-06-11T17:00:00+01:00"},
            "ticket_type": "event_this_week",
            "ticket_notability": {"artist": "The Weeknd", "kind": "artist", "tier": "A"},
        }
        b_item = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Keli Holiday — event 2026-06-11",
            "summary": "YES | Manchester | Pop | event_date=2026-06-11 19:00",
            "event": {"venue": "YES", "date_start": "2026-06-11T19:00:00+01:00"},
            "ticket_type": "event_this_week",
            "ticket_notability": {"artist": "Keli Holiday", "kind": "artist", "tier": "B"},
        }
        self.assertGreater(
            _section_priority_score(global_item, "Билеты / Ticket Radar", _build_ticket_fallback_line(global_item)),
            _section_priority_score(b_item, "Билеты / Ticket Radar", _build_ticket_fallback_line(b_item)),
        )

    def test_ticket_reason_is_single_human_occasion_not_repeated_labels(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "The Weeknd: After Hours Til Dawn Tour — event 2026-06-11",
            "summary": "Etihad Stadium | Manchester | Hip-Hop/Rap | event_date=2026-06-11 17:00",
            "event": {"venue": "Etihad Stadium", "date_start": "2026-06-11T17:00:00+01:00"},
            "ticket_type": "event_this_week",
            "ticket_notability": {
                "artist": "The Weeknd",
                "kind": "artist",
                "tier": "A",
                "signal": "streaming_popularity",
                "signals": {"spotify_followers": 41200000},
            },
        }
        line = _build_ticket_fallback_line(candidate)
        self.assertNotIn("Spotify", line)
        self.assertNotIn("сигнал:", line)
        self.assertIn("Etihad Stadium", line)
        self.assertNotIn("крупный артист", line.lower())
        self.assertNotIn("крупная площадка", line.lower())
        self.assertNotIn("A-tier", line)
        self.assertNotIn("A-класс", line)
        self.assertNotIn("Глобальный артист; крупная площадка", line)

    def test_ticket_line_uses_event_date_not_published_at_for_major_artist(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "The Weeknd: After Hours Til Dawn Tour — event 2026-06-11",
            "published_at": "2026-06-10T08:00:00+01:00",
            "summary": "Etihad Stadium | Manchester | Hip-Hop/Rap | event_date=2026-06-11 17:00",
            "event": {"venue": "Etihad Stadium", "date_start": "2026-06-11T17:00:00+01:00"},
            "ticket_type": "event_this_week",
            "ticket_notability": {"artist": "The Weeknd", "kind": "artist", "tier": "A"},
        }

        line = _build_ticket_fallback_line(candidate)

        self.assertIn("11 июня", line)
        self.assertIn("(Hip-Hop/Rap)", line)
        self.assertNotIn("10 июня", line)
        self.assertNotIn("event date in draft_line conflicts", " ".join(_draft_line_quality_errors(candidate, line)))

    def test_multinight_ticket_line_does_not_conflict_with_structured_start_date(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "The Weeknd: After Hours Til Dawn Tour — event 2026-06-11",
            "summary": "Etihad Stadium | Manchester | Hip-Hop/Rap | event_date=2026-06-11 17:00",
            "event": {"venue": "Etihad Stadium", "date_start": "2026-06-11T17:00:00+01:00"},
            "merged_event_dates": ["2026-06-11", "2026-06-12"],
            "ticket_type": "event_this_week",
            "ticket_notability": {"artist": "The Weeknd", "kind": "artist", "tier": "A"},
        }

        line = _build_ticket_fallback_line(candidate)

        self.assertIn("11 и 12 июня", line)
        self.assertNotIn("event date in draft_line conflicts", " ".join(_draft_line_quality_errors(candidate, line)))

    def test_musicbrainz_ticketmaster_identity_does_not_promote_to_public_watch(self) -> None:
        tier, _confidence, signal = _tier_from_signals(
            {
                "sitelinks": 0,
                "musicbrainz_score": 100,
                "ticketmaster_attraction": True,
            }
        )
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Riff Wood — event 2026-06-10",
            "summary": "The Lodge, Manchester | Rock | event_date=2026-06-10 19:00",
            "event": {"venue": "The Lodge, Manchester", "date_start": "2026-06-10T19:00:00+01:00"},
            "ticket_type": "event_this_week",
            "ticket_notability": {"artist": "Riff Wood", "kind": "artist", "tier": tier, "signal": signal},
        }

        self.assertEqual(tier, "C")
        self.assertEqual(_ticket_watch_decision(candidate)["decision"], "hide")
        self.assertEqual(_build_ticket_fallback_line(candidate), "")

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

    def test_football_documentary_item_does_not_count_as_sport_minimum(self) -> None:
        candidate = {
            "primary_block": "football",
            "title": "Pep Guardiola: Former Man City manager's final seasons to air in Amazon documentary",
            "summary": "The behind-the-scenes series will air on Prime Video this summer.",
        }
        self.assertTrue(_football_is_sport_news(candidate))
        self.assertTrue(_football_should_route_to_soft(candidate))

    def test_football_numeric_hallucination_is_replaced_by_official_fallback(self) -> None:
        candidate = {
            "category": "football",
            "primary_block": "football",
            "source_label": "Manchester United",
            "title": "Transfer news: Hojlund joins Napoli",
            "summary": "Transfer news: Hojlund joins Napoli",
            "lead": "Transfer news: Hojlund joins Napoli",
            "evidence_text": "Rasmus Hojlund has joined Napoli on a permanent transfer",
        }
        bad = "• Расмус Хёйлунд перешёл в Napoli. Он провёл за клуб 72 матча и забил 16 голов."
        self.assertTrue(any("number(s) not present" in err for err in _draft_line_quality_errors(candidate, bad)))
        fallback = _build_football_fallback_line(candidate)
        self.assertIn("трансферное обновление", fallback)
        self.assertNotIn("72", fallback)

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

    def test_next_7_event_board_uses_real_week_occurrence_not_filler(self) -> None:
        with patch("news_digest.pipeline.writer.now_london") as fake_now:
            fake_now.return_value = datetime(2026, 6, 10)
            bridgewater = {
                "category": "venues_tickets",
                "primary_block": "next_7_days",
                "source_label": "Bridgewater Hall",
                "source_url": "https://bridgewater-hall.co.uk/whats-on/rob-lamberti-120626",
                "title": "Rob Lamberti presents Perfectly George | The Bridgewater Hall",
                "summary": "Event Timings Auditorium Doors: 7.00pm Concert Start: 7.30pm",
                "event": {
                    "is_event": True,
                    "event_name": "Rob Lamberti presents Perfectly George | The",
                    "venue": "Bridgewater Hall",
                    "date_start": "2026-12-18",
                    "date_text": "18 December",
                },
            }
            self.assertEqual(_next_7_event_decision(bridgewater)[0], "keep")
            line = _build_ticket_fallback_line(bridgewater)
            self.assertIn("12 июня", line)
            self.assertNotIn("00:00", line)
            self.assertNotIn("18 декабря", line)

            exhibition = {
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "source_label": "People's History Museum",
                "title": "100 years of strikes & solidarity",
                "summary": "Headline exhibition on show until 2 November 2026.",
                "event": {
                    "is_event": True,
                    "event_name": "100 years of strikes & solidarity",
                    "venue": "People's History Museum",
                    "date_start": "2026-11-02",
                    "date_text": "2 November 2026",
                },
            }
            self.assertEqual(_next_7_event_decision(exhibition)[0], "hold")

            no_venue = {
                "category": "venues_tickets",
                "primary_block": "next_7_days",
                "source_label": "Band on the Wall",
                "title": "The Bad Plus Farewell Tour - Manchester",
                "summary": "After a quarter of a century, Reid Anderson and Dave King feel like their statement.",
                "event": {
                    "is_event": True,
                    "event_name": "The Bad Plus Farewell Tour - Manchester",
                    "date_start": "2026-07-02",
                },
            }
            self.assertEqual(_next_7_event_decision(no_venue), ("hold", "event has no usable venue"))

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

    def test_support_act_cannot_replace_primary_ticket_headliner(self) -> None:
        cache_path = self._ticket_notability_cache(
            {
                "take that": {
                    "artist": "Take That",
                    "tier": "A",
                    "confidence": 0.95,
                    "signal": "wikidata_sitelinks",
                    "sitelinks": 80,
                },
                "the script": {
                    "artist": "The Script",
                    "tier": "A",
                    "confidence": 0.98,
                    "signal": "wikidata_sitelinks",
                    "sitelinks": 90,
                },
            }
        )
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "TAKE THAT - THE CIRCUS LIVE - Summer 2026 — event 2026-06-19",
            "summary": "Etihad Stadium | Manchester | Pop | event_date=2026-06-19 17:00 | ticket_type=major_upcoming",
            "event": {
                "event_name": "TAKE THAT - THE CIRCUS LIVE - Summer 2026",
                "venue": "Etihad Stadium",
                "date_start": "2026-06-19",
                "attractions": [{"name": "Take That"}, {"name": "The Script"}],
            },
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

        self.assertEqual(notability.artist, "TAKE THAT")
        self.assertEqual((notability.signals or {}).get("headliner_resolution"), "primary_headliner_locked")
        line = _build_ticket_fallback_line(candidate)
        self.assertIn("TAKE THAT", line)
        self.assertNotIn("The Script", line)

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

    def test_musicbrainz_ticketmaster_signal_stays_identity_not_popularity(self) -> None:
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
        self.assertEqual(notability.tier, "C")
        self.assertEqual(notability.signal, "musicbrainz_ticketmaster_identity")
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

    def test_metrolink_name_does_not_trip_metro_contract(self) -> None:
        html = (
            "<b>Greater Manchester Brief — 2026-06-22, 08:10</b>\n"
            "<b>Общественный транспорт сегодня</b>\n"
            "• Metrolink: ведутся работы на остановке Metrolink в Prestwich. "
            "<a href=\"https://tfgm.com/travel-updates\">TfGM</a>\n"
        )
        errors = public_html_contract_errors(html)
        self.assertFalse(any("metrolink_written_as_metro" in error for error in errors))

    def test_public_html_allows_more_than_fifteen_ticket_lines(self) -> None:
        html = "<b>Билеты / Ticket Radar</b>\n" + "\n".join(
            f"• A-tier artist {idx}." for idx in range(16)
        )

        self.assertFalse(any("ticket_radar_over_cap" in error for error in public_html_contract_errors(html)))

    def test_transport_language_repair_fixes_today_metrolink_copy(self) -> None:
        line = (
            "• Metrolink: ведутся работы по улучшению на остановке трамваев Метролинк "
            "в Prestwichе, которые продлятся до 19 августа. "
            "<a href=\"https://tfgm.com/travel-updates/travel-alerts/prestwich-tram-stop-improvement-works\">TfGM</a>"
        )
        fixed, reasons = repair_transport_line_language(line)
        self.assertIn("official_metrolink_name", reasons)
        self.assertIn("metrolink_stop_wording", reasons)
        self.assertIn("latin_place_case", reasons)
        self.assertIn("остановке Metrolink в Prestwich", fixed)
        self.assertNotIn("Метролинк", fixed)
        self.assertNotIn("Prestwichе", fixed)

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

    def test_minor_bus_stop_closures_are_grouped_not_repeated(self) -> None:
        lines = [
            f"• Автобусы: закрыта остановка Stop {idx} на Street в Area — ремонтные работы."
            for idx in range(5)
        ]
        capped, *_rest, dropped = _cap_minor_bus_stop_lines(lines, ["TfGM"] * 5, [str(i) for i in range(5)], [0.0] * 5, lines)
        self.assertEqual(len(capped), 1)
        self.assertIn("5 мелких закрытий остановок", capped[0])
        self.assertEqual(len(dropped), 4)

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
