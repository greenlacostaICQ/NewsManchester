from __future__ import annotations

import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.collector.routing import _adjust_ticket_radar_block
from news_digest.pipeline.collector.fallbacks import _weather_draft_line
from news_digest.pipeline.collector.filters import _is_allowed_source_link
from news_digest.pipeline.collector.sources import SOURCES
from news_digest.pipeline.transport_card import extract_transport_card, render_card
from news_digest.pipeline.common import (
    fingerprint_for_candidate,
    now_london,
    today_london,
)
from news_digest.pipeline.dedupe import (
    _apply_intra_batch_dedup,
    _normalise_person_tokens,
    _people_published_matches,
    dedupe_candidates,
)
from news_digest.pipeline.entity_extraction import extract_entities
from news_digest.pipeline.editorial_contracts import build_editorial_contract, copy_invariant_errors
from news_digest.pipeline.writer import (
    _build_recurring_event_fallback_line,
    _build_ticket_fallback_line,
    _contract_public_drop_reason,
    _line_claims_future_ticket_sale,
    _repair_editorial_contract_line,
    _reserved_later_budget,
    _section_priority_score,
)


class DigestQualityGuardrailsTest(unittest.TestCase):
    def _validate_one(self, candidate: dict) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "run_date_london": "2026-05-21",
                        "candidates": [candidate],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            result = validate_candidates(root)
            self.assertTrue(result.ok)
            payload = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            return payload["candidates"][0]

    def test_drops_non_gm_warrington_story_from_men_chrome(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "warrington-texas",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "Boyfriend speaks out after dad who shot his own daughter dead faces no charges",
                "summary": "News Greater Manchester News Warrington Lucy Harrison was killed in Texas.",
                "lead": "",
                "evidence_text": "Warrington woman Lucy Harrison was killed in Texas before flying home.",
                "source_label": "MEN",
                "source_url": "https://example.test/greater-manchester-news/warrington",
                "published_at": now_london().isoformat(),
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated["include"])
        self.assertIn("not_gm", updated["reject_reasons"])

    def test_drops_loose_tv_local_only_story(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "race-across",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "Katie and Harrison relive epic BBC Race Across the World journey",
                "summary": "Siblings from Manchester entered the BBC show to have an adventure.",
                "lead": "",
                "evidence_text": "The BBC show follows people travelling with a limited budget.",
                "source_label": "BBC Manchester",
                "source_url": "https://example.test/race-across",
                "published_at": now_london().isoformat(),
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated["include"])
        self.assertIn("weak_value_lifestyle", updated["reject_reasons"])

    def test_drops_football_farewell_pr_filler(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "bernardo-farewell",
                "category": "football",
                "primary_block": "football",
                "title": "The connection with our fans was always there - Bernardo",
                "summary": "Bernardo Silva says he leaves Manchester City as a fan for the rest of his life.",
                "lead": "",
                "evidence_text": "Bernardo Silva says he leaves Manchester City as a fan for the rest of his life.",
                "source_label": "Manchester City",
                "source_url": "https://example.test/bernardo-farewell",
                "published_at": now_london().isoformat(),
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated["include"])
        self.assertIn("weak_value_football_pr", updated["reject_reasons"])

    def test_drops_visitor_attraction_from_food_openings(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "corrie-experience",
                "category": "food_openings",
                "primary_block": "openings",
                "title": "The Coronation Street Experience",
                "summary": "A 90-minute visitor attraction at ITV Studios with tours and tickets.",
                "lead": "",
                "evidence_text": "Coronation Street Experience is a TV visitor attraction at ITV Studios.",
                "source_label": "VisitSalford Markets",
                "source_url": "https://example.test/corrie",
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated["include"])
        self.assertIn("wrong_openings_category", updated["reject_reasons"])

    def test_drops_old_undated_election_results_page(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "stockport-election-results",
                "category": "council",
                "primary_block": "city_watch",
                "title": "Stockport local election 2026 results",
                "summary": "The results of voting in the Stockport local elections 7 May 2026 are as follows.",
                "lead": "",
                "evidence_text": "The results of voting in the Stockport local elections 7 May 2026 are as follows.",
                "source_label": "Stockport Council",
                "source_url": "https://example.test/stockport-local-election-2026-results",
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated["include"])
        self.assertIn("stale_undated_news", updated["reject_reasons"])

    def test_old_public_sale_upcoming_event_stays_in_ticket_radar(self) -> None:
        candidate = {
            "include": True,
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Example — event 2026-05-24 — public sale 2025-11-14 10:00",
            "summary": (
                "O2 Victoria Warehouse Manchester | Manchester | Rock | "
                "event_date=2026-05-24 19:00 | public_onsale=2025-11-14 10:00 | "
                "ticket_signal=upcoming_event | ticket_type=regular_upcoming | major_venue=false"
            ),
        }

        _adjust_ticket_radar_block(candidate)

        self.assertEqual(candidate["primary_block"], "ticket_radar")
        self.assertEqual(candidate["ticket_type"], "old_public_sale")

    def test_old_public_sale_fallback_says_already_on_sale_and_keeps_genre(self) -> None:
        line = _build_ticket_fallback_line(
            {
                "category": "venues_tickets",
                "primary_block": "ticket_radar",
                "title": "Example",
                "ticket_type": "old_public_sale",
                "summary": (
                    "O2 Victoria Warehouse Manchester | Manchester | Electronic | "
                    "event_date=2026-05-24 19:00 | public_onsale=2025-11-14 10:00 | "
                    "ticket_signal=upcoming_event | ticket_type=regular_upcoming | major_venue=false"
                ),
                "practical_angle": "Проверьте наличие билетов на официальной странице.",
            }
        )

        self.assertIn("Билеты уже в продаже", line)
        self.assertIn("(Electronic)", line)
        self.assertNotIn("поступят в продажу", line.lower())

    def test_distinct_car_boot_and_market_sources_do_not_collapse(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "bowlee",
                "title": "Casual trading | Casual trading | Rochdale Council",
                "summary": "Every Sunday at Bowlee Community Park.",
                "primary_block": "weekend_activities",
                "source_label": "Bowlee Car Boot Sale",
            },
            {
                "include": True,
                "fingerprint": "new-smithfield",
                "title": "Casual trading | Casual trading | Manchester City Council",
                "summary": "Sunday trading at New Smithfield Market.",
                "primary_block": "weekend_activities",
                "source_label": "New Smithfield Sunday Market",
            },
        ]

        self.assertEqual(_apply_intra_batch_dedup(candidates), [])

    def test_recurring_market_open_on_weekend_passes_date_validator(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "altrincham-market",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "title": "Altrincham Market",
                "summary": "Altrincham Market is open on Saturday and Sunday with food, drink and traders.",
                "lead": "",
                "evidence_text": "Opening hours: open Saturday and Sunday at Market House, Altrincham.",
                "source_label": "Altrincham Market",
                "source_url": "https://visitaltrincham.com/business-directory/altrincham-market/",
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertTrue(updated["include"])

    def test_flower_festival_has_redundant_weekend_sources(self) -> None:
        sources = [
            source
            for source in SOURCES
            if "flower festival" in source.name.lower()
            and source.primary_block == "weekend_activities"
            and source.source_type == "html_page_event"
        ]

        self.assertGreaterEqual(len(sources), 4)
        self.assertTrue(any("visitmanchester.com" in source.url for source in sources))
        self.assertTrue(any("cityco.com" in source.url for source in sources))

    def test_flower_festival_does_not_collapse_into_unrelated_festival(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "flower-festival",
                "title": "The Manchester Flower Festival",
                "summary": "Saturday 23 to Monday 25 May 2026 at St Ann's Square and King Street.",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_label": "Manchester Flower Festival",
            },
            {
                "include": True,
                "fingerprint": "mif-installation",
                "title": "A Possibility | Germaine Kruip | Manchester International Festival 2025",
                "summary": "A Factory International artwork listing.",
                "primary_block": "next_7_days",
                "category": "culture_weekly",
                "source_label": "Factory International",
            },
        ]

        drops = _apply_intra_batch_dedup(candidates)

        self.assertEqual(drops, [])
        self.assertTrue(candidates[0]["include"])

    def test_generic_the_manchester_prefix_is_not_a_dedupe_entity(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "flower-festival",
                "title": "The Manchester Flower Festival",
                "summary": "City-centre floral trail.",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_label": "Manchester Flower Festival",
            },
            {
                "include": True,
                "fingerprint": "museum-event",
                "title": "The Manchester Museum announces weekend dinosaur trail",
                "summary": "Museum family activity this weekend.",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_label": "Manchester Museum",
            },
        ]

        self.assertEqual(_apply_intra_batch_dedup(candidates), [])

    def test_flower_festival_duplicate_sources_keep_clean_event_facts(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "visit-flower",
                "title": "The Manchester Flower Festival",
                "summary": "Celebrate the start of summer at The Manchester Flower Festival.",
                "evidence_text": "Page chrome for Palace Theatre and Opera House. Saturday 23 - Monday 25 May 2026.",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_label": "Manchester Flower Festival",
                "event": {
                    "event_name": "The",
                    "venue": "Palace Theatre",
                    "date_start": "2026-05-25",
                    "date_text": "25 May 2026",
                    "price": "free",
                    "borough": "Manchester",
                },
            },
            {
                "include": True,
                "fingerprint": "cityco-flower-news",
                "title": "The Manchester Flower Festival returns for 2026",
                "summary": (
                    "The Manchester Flower Festival returns from Saturday 23 to Monday 25 May 2026, "
                    "transforming St Ann's Square and King Street with a free city-centre trail."
                ),
                "evidence_text": "Manchester city centre, St Ann's Square and King Street, free festival.",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_label": "Manchester Flower Festival CityCo News",
                "event": {
                    "event_name": "The Manchester Flower Festival returns for 2026",
                    "venue": "",
                    "date_start": "2026-05-23",
                    "date_text": "23-25 May 2026",
                    "price": "free",
                    "borough": "Manchester",
                },
            },
        ]

        drops = _apply_intra_batch_dedup(candidates)

        self.assertFalse(candidates[0]["include"])
        self.assertTrue(candidates[1]["include"])
        self.assertEqual(drops[0]["kept_fingerprint"], "cityco-flower-news")

    def test_weekend_section_has_broad_guide_sources_not_only_direct_pages(self) -> None:
        guide_sources = [
            source.name
            for source in SOURCES
            if source.primary_block == "weekend_activities"
            and source.source_type in {
                "html_visitmanchester_events",
                "html_sectioned_event_guide",
                "html_designmynight",
            }
        ]

        # One old broad guide (Creative Tourist Bank Holiday) is intentionally
        # disabled when its live page is stale; keep the guardrail broad
        # without requiring stale sources to stay enabled.
        self.assertGreaterEqual(len(guide_sources), 6)
        self.assertIn("Visit Manchester Weekend", guide_sources)
        self.assertIn("Secret Manchester May Guide", guide_sources)
        self.assertIn("Secret Manchester Gigs", guide_sources)
        self.assertIn("Manchester Theatres Weekend", [source.name for source in SOURCES])

    # ---------------------------------------------------------------
    # S1 — date-aware guardrails
    # User feedback 2026-05-22: "5 апреля Car Boot какой нахуй / 6 июня
    # Barton нахуя / Lindsey автор книги — причём тут IT".
    # Each test is a direct quote of the bad output → expected fix.
    # ---------------------------------------------------------------

    def test_recurring_market_with_past_start_date_is_kept_not_rejected(self) -> None:
        """User feedback: «5 апреля Car Boot какой нахуй».

        The Bowlee Car Boot Sale opens its season on 5 April but runs
        every Sunday until September. The old validator rejected the
        whole card because the only mentioned date was 5 April (past).
        The new behaviour: detect the recurrence pattern ("каждое
        воскресенье" / "every Sunday") and KEEP the card with
        event.is_recurring = True so the rewriter can produce
        "каждое воскресенье до сентября" instead of the dead start
        date.
        """
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "bowlee-car-boot-recurring",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "title": "Bowlee Community Park Car Boot Sale season opens 5 April",
                "summary": (
                    "The Bowlee Car Boot Sale season opens on 5 April and "
                    "runs every Sunday through to the end of September. "
                    "Entry £2.50 for shoppers, £15 per car."
                ),
                "lead": "Bowlee Community Park, every Sunday until September.",
                "evidence_text": "Sellers arrive from 6am; buyers from 8am every Sunday.",
                "source_label": "Bowlee Car Boot Sale",
                "source_url": "https://example.test/bowlee-car-boot",
                "published_at": now_london().isoformat(),
            }
        )
        # Card should still be included.
        self.assertTrue(
            updated.get("include"),
            f"Recurring market was rejected: {updated.get('reason')}",
        )
        # event.is_recurring should be set by the recurrence-aware stale
        # event check so the rewriter knows to say "every Sunday".
        event = updated.get("event") or {}
        self.assertTrue(
            event.get("is_recurring"),
            f"event.is_recurring not set; event={event}",
        )

    def test_recurring_market_writer_leads_with_next_occurrence(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "bowlee-car-boot-recurring",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Bowlee Community Park Car Boot Sale season opens 5 April",
            "summary": (
                "The Bowlee Car Boot Sale season opens on 5 April and runs every Sunday "
                "through to 11 October. Entry £2.50 for shoppers, £15 per car."
            ),
            "lead": "Bowlee Community Park, every Sunday until October.",
            "evidence_text": "Sellers arrive from 6am; buyers from 7am every Sunday.",
            "source_label": "Bowlee Car Boot Sale",
            "source_url": "https://example.test/bowlee-car-boot",
            "event": {"is_recurring": True},
        }

        line = _build_recurring_event_fallback_line(candidate)

        self.assertIn("воскресенье", line.lower())
        self.assertIn("Bowlee", line)
        self.assertNotRegex(line, r"5\s+апреля|5\s+April")

    def test_one_off_event_with_only_russian_past_date_is_rejected(self) -> None:
        """Past dates in Russian month names must be caught.

        Before: «5 апреля» wasn't recognised by the validator (only
        English months were). The card surfaced 22 May with a 5 April
        date and confused the reader.
        After: Russian month names ловятся точно так же, как английские.
        """
        # Use a Russian month at least 30 days behind today so it can't
        # be misread as the same month current-year — pick a fixed past
        # date 90 days back.
        past_day = (now_london().date() - timedelta(days=90))
        ru_months = [
            "января", "февраля", "марта", "апреля", "мая", "июня",
            "июля", "августа", "сентября", "октября", "ноября", "декабря",
        ]
        past_text = f"{past_day.day} {ru_months[past_day.month - 1]}"
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "ru-past-event",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "title": f"Концерт {past_text} в Manchester",
                "summary": f"Уникальный однодневный концерт {past_text} в The Deaf Institute.",
                "lead": "",
                "evidence_text": f"Билеты на однодневный концерт {past_text} больше не доступны.",
                "source_label": "Example Venue",
                "source_url": "https://example.test/ru-concert",
                "published_at": now_london().isoformat(),
            }
        )
        # One-off (no recurrence pattern) past concert must be rejected.
        self.assertFalse(
            updated.get("include"),
            f"One-off past Russian-dated concert not rejected: {updated.get('reason')}",
        )
        self.assertIn(
            "past",
            str(updated.get("reason") or "").lower(),
        )

    def test_weekend_section_excludes_event_beyond_three_days(self) -> None:
        """User feedback: «Barton Aerodrome 6 июня нахуя? было требование
        showing weekend events».

        A weekend_activities card dated 15 days out should NOT stay in
        the weekend section. It should be demoted to either next_7_days
        (within a week) or future_announcements (within a month).
        """
        far_future = (now_london().date() + timedelta(days=15)).isoformat()
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "barton-far-future",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "title": "Barton Aerodrome Car Boot Sale on 6 June",
                "summary": (
                    f"One-off Barton Aerodrome Car Boot Sale event_date={far_future}. "
                    "Single Saturday market, not part of the regular season."
                ),
                "lead": "",
                "evidence_text": "A one-off Saturday car boot at Barton Aerodrome.",
                "source_label": "Barton Aerodrome",
                "source_url": "https://example.test/barton-6-june",
                "published_at": now_london().isoformat(),
                "event": {
                    "event_name": "Barton Aerodrome Car Boot",
                    "date_iso": far_future,
                    "venue": "Barton Aerodrome",
                    "borough": "Eccles",
                    "is_recurring": False,
                },
            }
        )
        # Card stays included but moves out of weekend_activities.
        self.assertTrue(
            updated.get("include"),
            f"Far-future weekend card was dropped: {updated.get('reason')}",
        )
        self.assertNotEqual(
            updated.get("primary_block"),
            "weekend_activities",
            f"Far-future event still in weekend section: {updated.get('reason')}",
        )
        # Should land in one of the further-out planning blocks.
        self.assertIn(
            updated.get("primary_block"),
            {"next_7_days", "future_announcements"},
        )

    def test_book_author_in_tech_business_is_rejected(self) -> None:
        """User feedback: «IT и бизнес: Линдси Мередит — автор книги,
        причём тут IT?».

        A Bdaily-routed "author hits Amazon bestseller" card has no
        tech/startup angle. Reject from tech_business.
        """
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "lindsey-book-author",
                "category": "tech_business",
                "primary_block": "city_watch",
                "title": "Stockport author hits Amazon bestseller spot",
                "summary": (
                    "Lindsey Meredith, 42, from Stockport, has become a "
                    "bestselling author after her book AUTHORity climbed "
                    "the Amazon charts past Seth Godin and Alex Hormozi."
                ),
                "lead": "Lindsey Meredith is now an Amazon bestseller.",
                "evidence_text": (
                    "Her book AUTHORity is about how to write and promote "
                    "books. The memoir-adjacent guide climbed several Amazon "
                    "categories including career growth and small business."
                ),
                "source_label": "Bdaily Manchester",
                "source_url": "https://example.test/lindsey-book",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertFalse(
            updated.get("include"),
            f"Book author was kept in tech_business: {updated.get('reason')}",
        )
        reject_reasons = updated.get("reject_reasons") or []
        self.assertIn("book_author_misrouted", reject_reasons)

    # ---------------------------------------------------------------
    # S2 — cross-day entity dedup (same-victim / same-suspect repeats)
    # User feedback 2026-05-22:
    #   «Эрика 3 денб получаю эту новость где проверка??»
    #   «Manchester Arena теракт ОПЯТЬ ПРО ТЕРРАКТ»
    # ---------------------------------------------------------------

    def test_people_extraction_finds_russian_victim_name(self) -> None:
        """Stable contract: extract_entities must surface 'Эрика де Соуза
        Корреа' both nominative ('Эрика') and genitive ('Эрики') from a
        typical Russian news blob so the cross-day matcher can align
        them.
        """
        entities = extract_entities(
            {
                "title": "Семья 17-летней Эрики де Соуза Корреа",
                "summary": "17-летняя Эрика де Соуза Корреа погибла во время полицейской погони 5 мая.",
            }
        )
        people = entities.get("people") or []
        self.assertGreaterEqual(len(people), 1)
        joined = " | ".join(people).lower()
        self.assertIn("соуза", joined)
        self.assertIn("корреа", joined)

    def test_people_normalisation_aligns_russian_morphology(self) -> None:
        """Эрика (nom.) and Эрики (gen.) must share >=2 normalised
        tokens so they match across days.
        """
        a = _normalise_person_tokens("Эрика де Соуза Корреа")
        b = _normalise_person_tokens("Эрики де Соуза Корреа")
        # Both should contain stems for Souza and Correa (case-stripped).
        shared = a & b
        self.assertGreaterEqual(
            len(shared), 2,
            f"Russian morphology stripping failed: a={a}, b={b}, shared={shared}",
        )

    def test_different_people_do_not_match_across_day(self) -> None:
        """John Smith published yesterday must NOT block Jane Doe today
        — single-surname overlaps below the 2-token threshold.
        """
        cand = {
            "primary_block": "city_watch",
            "category": "media_layer",
            "entities": {
                "people": ["Jane Doe"],
            },
        }
        fact = {
            "fingerprint": "smith-yesterday",
            "title": "John Smith charged",
            "entities": {"people": ["John Smith"]},
            "primary_block": "city_watch",
        }
        matches = _people_published_matches(cand, [fact])
        self.assertEqual(matches, [])

    def test_same_victim_cross_day_yields_people_match(self) -> None:
        """Direct test of the matcher: today's «Эрика де Соуза Корреа»
        candidate vs yesterday's published fact with the same person —
        must return a non-empty match with shared_tokens >= 2.
        """
        cand = {
            "primary_block": "city_watch",
            "category": "media_layer",
            "entities": {"people": ["Эрика де Соуза Корреа"]},
        }
        fact = {
            "fingerprint": "erica-yesterday",
            "title": "Семья 17-летней Эрики де Соуза Корреа",
            "entities": {"people": ["Эрики де Соуза Корреа"]},
            "primary_block": "city_watch",
        }
        matches = _people_published_matches(cand, [fact])
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["match_type"], "people_entity")
        self.assertGreaterEqual(int(matches[0]["shared_tokens"]), 2)

    def test_cross_day_same_victim_blocks_candidate(self) -> None:
        """User feedback: «Эрика 3 денб получаю эту новость где проверка??».

        Integration through dedupe_candidates: with Эрика published two
        days ago in published_facts.json, a fresh candidate about her
        from a different outlet must be blocked with
        cross_day_entity_repeat=True and dedupe_decision='drop'.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)

            today = today_london()
            # Pre-seed published_facts with a story about Erica from
            # two days ago — different outlet, different URL.
            (state_dir / "published_facts.json").write_text(
                json.dumps(
                    {
                        "last_updated_london": today,
                        "facts": [
                            {
                                "fingerprint": "yesterday-bbc-erica",
                                "title": "Family of 17-year-old Erica de Souza Correa speaks out",
                                "normalized_title": "family of 17 year old erica de souza correa speaks out",
                                "category": "media_layer",
                                "primary_block": "city_watch",
                                "source_label": "BBC Manchester",
                                "entities": {
                                    "schema_version": 2,
                                    "boroughs": ["Bolton"],
                                    "people": ["Erica de Souza Correa"],
                                    "all": [],
                                },
                                "first_published_day_london": today,
                                "last_published_day_london": today,
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            # Important: this candidate is a PURE rehash — same victim
            # but no new fact (no suspect name, no court date, no new
            # figure). If we accidentally include a date or number here,
            # has_new_fact_signal upgrades the verdict to
            # same_story_new_facts and the block doesn't fire. That's
            # actually correct behaviour, but it doesn't exercise the
            # "Эрика 3 дня подряд" path we want to lock down here.
            today_candidate = {
                "fingerprint": "today-themanc-erica",
                "include": True,
                "dedupe_decision": "new",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "Семья Эрики де Соуза Корреа выразила скорбь",
                "summary": (
                    "Близкие Эрики де Соуза Корреа продолжают переживать "
                    "потерю. Семья просит общественность уважать их частную жизнь."
                ),
                "lead": "",
                "evidence_text": (
                    "Семья Эрики де Соуза Корреа выразила скорбь."
                ),
                "source_label": "The Manc",
                "source_url": "https://example.test/themanc/erica-grief",
                "published_at": now_london().isoformat(),
            }
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "t",
                        "run_date_london": today,
                        "candidates": [today_candidate],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            # dedupe_candidates may flag synthetic test data with
            # "missing reason" errors; that's ok — what we care about
            # is the candidate's final state.
            dedupe_candidates(root)
            out = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            updated = out["candidates"][0]
            self.assertFalse(
                updated.get("include"),
                f"Same victim cross-day was not blocked: reason={updated.get('reason')}",
            )
            self.assertTrue(
                updated.get("cross_day_entity_repeat"),
                f"cross_day_entity_repeat flag not set: {updated}",
            )

    def test_cross_day_same_victim_with_new_fact_passes(self) -> None:
        """Защита от слишком жёсткого dedup: если карточка тех же людей
        с новым явным фактом (имя обвиняемого + дата суда + цифра
        приговора) — должна пройти как same_story_new_facts, не block.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            today = today_london()
            (state_dir / "published_facts.json").write_text(
                json.dumps(
                    {
                        "last_updated_london": today,
                        "facts": [
                            {
                                "fingerprint": "yesterday-bbc-erica",
                                "title": "Family of Erica de Souza Correa speaks",
                                "normalized_title": "family of erica de souza correa speaks",
                                "category": "media_layer",
                                "primary_block": "city_watch",
                                "source_label": "BBC Manchester",
                                "entities": {
                                    "schema_version": 2,
                                    "people": ["Erica de Souza Correa"],
                                    "all": [],
                                },
                                "first_published_day_london": today,
                                "last_published_day_london": today,
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            today_candidate = {
                "fingerprint": "today-newfact-erica",
                "include": True,
                "dedupe_decision": "new",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "Bolton officer Mark Davies charged in Erica de Souza Correa pursuit case",
                "summary": (
                    "Mark Davies, the officer at the wheel during the pursuit "
                    "that killed 17-year-old Erica de Souza Correa, has been "
                    "charged with dangerous driving. The trial is set for "
                    "16 September 2026. The Crown said damages of £250,000 "
                    "are being sought."
                ),
                "lead": "",
                "evidence_text": (
                    "Mark Davies has been charged with dangerous driving "
                    "causing death. Trial: 16 September 2026. "
                    "Damages claim £250,000."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/men/erica-officer-charged",
                "published_at": now_london().isoformat(),
            }
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "t",
                        "run_date_london": today,
                        "candidates": [today_candidate],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            # dedupe_candidates may flag synthetic test data with
            # "missing reason" errors; that's ok — what we care about
            # is the candidate's final state.
            dedupe_candidates(root)
            out = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            updated = out["candidates"][0]
            # Either still include=True (new facts upgraded it back), or
            # change_type marked as same_story_new_facts — we accept both
            # outcomes as "didn't silently drop the new development".
            include_or_upgrade_ok = (
                updated.get("include") is True
                or str(updated.get("change_type") or "") == "same_story_new_facts"
            )
            self.assertTrue(
                include_or_upgrade_ok,
                f"News with a new fact was silently blocked: {updated}",
            )

    # ---------------------------------------------------------------
    # S3 — three event templates + post-rewrite completeness review
    # User feedback 2026-05-22:
    #   «Burnage RFC мне не нужно описание»
    #   «Manchester Jazz Festival какое нахуй 15 мая и что значит 24 мая»
    #   «Alcotraz это разве не постоянный бар?»
    #   «Big Manchester Bake что значит с 22 мая каждый день или 1 день»
    # ---------------------------------------------------------------

    def test_date_marker_recognises_russian_month_dates(self) -> None:
        from news_digest.pipeline.release import _DATE_MARKER_RE
        self.assertIsNotNone(_DATE_MARKER_RE.search("в субботу 23 мая в 19:00"))
        self.assertIsNotNone(_DATE_MARKER_RE.search("концерт 24 октября"))

    def test_date_marker_recognises_recurring_phrase(self) -> None:
        """User: «каждое воскресенье до сентября» — это валидный временной
        маркер для повторяющегося события, не «нет даты»."""
        from news_digest.pipeline.release import _DATE_MARKER_RE
        self.assertIsNotNone(_DATE_MARKER_RE.search("каждое воскресенье до конца августа"))
        self.assertIsNotNone(_DATE_MARKER_RE.search("каждую субботу в 10:00"))
        self.assertIsNotNone(_DATE_MARKER_RE.search("еженедельно по воскресеньям"))

    def test_date_marker_recognises_permanent_phrase(self) -> None:
        """User: «Alcotraz это разве не постоянный бар?» — «постоянно
        работает» / «работает по выходным» считаются валидной датой."""
        from news_digest.pipeline.release import _DATE_MARKER_RE
        self.assertIsNotNone(_DATE_MARKER_RE.search("постоянно работает"))
        self.assertIsNotNone(_DATE_MARKER_RE.search("работает по выходным"))
        self.assertIsNotNone(_DATE_MARKER_RE.search("работает круглогодично"))

    def test_date_marker_recognises_range_until_phrase(self) -> None:
        """User: «Manchester Jazz Festival какое нахуй 15 мая и что
        значит 24 мая» — «идёт до 24 мая» / «до конца сентября» теперь
        тоже валидные маркеры (с явной end-date)."""
        from news_digest.pipeline.release import _DATE_MARKER_RE
        self.assertIsNotNone(_DATE_MARKER_RE.search("идёт до 24 мая"))
        self.assertIsNotNone(_DATE_MARKER_RE.search("до конца сентября"))

    def test_event_completeness_flags_missing_date(self) -> None:
        """User feedback: «Burnage RFC мне не нужно описание когда и
        что мне надо получать инфо там то тогда то ярмарка что ты
        мне даешь?»

        If the candidate has an extracted event.date_iso but the
        rewriter produced a draft_line without any time anchor, the
        post-rewrite review must surface it as missing_date.
        """
        from news_digest.pipeline.release import _summarise_event_completeness
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "no-date-event",
                    "primary_block": "weekend_activities",
                    "draft_line": "• Burnage RFC, популярная воскресная распродажа. Сезон проходит на свежем воздухе с большим количеством продавцов.",
                    "title": "Burnage RFC Car Boot",
                    "event": {
                        "venue": "Burnage RFC",
                        "date_iso": "2026-05-25",
                        "date_text": "Sunday 25 May",
                        "is_recurring": False,
                    },
                }
            ],
        }
        rendered = {"no-date-event"}
        result = _summarise_event_completeness(candidates_report, rendered, None)
        self.assertEqual(result["counts"]["missing_date"], 1)
        self.assertTrue(
            any(issue["issue"] == "missing_date" for issue in result["issues"]),
            f"missing_date not surfaced: {result}",
        )

    def test_event_completeness_passes_when_recurring_marker_present(self) -> None:
        """Defensive: a recurring event card that says «каждое
        воскресенье до сентября» must NOT be flagged as missing_date.
        """
        from news_digest.pipeline.release import _summarise_event_completeness
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "recurring-ok",
                    "primary_block": "weekend_activities",
                    "draft_line": "• Burnage RFC car boot — каждое воскресенье до конца августа, 6:00 для продавцов.",
                    "title": "Burnage RFC Car Boot",
                    "event": {
                        "venue": "Burnage RFC",
                        "is_recurring": True,
                    },
                }
            ],
        }
        rendered = {"recurring-ok"}
        result = _summarise_event_completeness(candidates_report, rendered, None)
        self.assertEqual(result["counts"]["missing_date"], 0)
        self.assertEqual(result["counts"]["missing_venue"], 0)

    def test_event_completeness_flags_missing_venue(self) -> None:
        """Carlo missing venue gets surfaced (warning-only)."""
        from news_digest.pipeline.release import _summarise_event_completeness
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "no-venue-event",
                    "primary_block": "weekend_activities",
                    "draft_line": "• 25 мая — концерт без указания места проведения.",
                    "title": "Concert",
                    "event": {
                        "venue": "The Deaf Institute",
                        "date_iso": "2026-05-25",
                    },
                }
            ],
        }
        rendered = {"no-venue-event"}
        result = _summarise_event_completeness(candidates_report, rendered, None)
        self.assertEqual(result["counts"]["missing_venue"], 1)

    def test_events_prompt_is_v4_with_three_templates(self) -> None:
        """The events prompt v4 must mention all three template buckets
        (one-off / festival / recurring) so the LLM picks one explicitly.
        """
        from news_digest.pipeline import llm_rewrite as _lr
        from news_digest.pipeline.prompts_meta import by_name
        events_meta = by_name().get("events")
        self.assertIsNotNone(events_meta)
        self.assertEqual(events_meta.version, "v4", f"events version not bumped to v4: {events_meta}")
        prompt = _lr.PROMPT_EVENTS
        self.assertIn("ТОЧЕЧНОЕ", prompt)
        self.assertIn("ФЕСТИВАЛЬ", prompt)
        self.assertIn("ПОВТОРЯЮЩЕЕСЯ", prompt)

    def test_diaspora_events_prompt_is_v3_with_recurring_template(self) -> None:
        from news_digest.pipeline import llm_rewrite as _lr
        from news_digest.pipeline.prompts_meta import by_name
        meta = by_name().get("diaspora_events")
        self.assertIsNotNone(meta)
        self.assertEqual(meta.version, "v3", f"diaspora events version not bumped: {meta}")
        self.assertIn("каждую субботу", _lr.PROMPT_DIASPORA_EVENTS)

    # ---------------------------------------------------------------
    # S4 — weak items with hard protection against killing real news.
    # User feedback 2026-05-22:
    #   «Ian Brown зашёл в магазин — непонятно»
    #   «второй день получаю такие новости вчера было про дислексию»
    #   «Salford Винни Клей 90-х годов — уже было и зачем мне сейчас»
    # ---------------------------------------------------------------

    def test_celebrity_sighting_without_news_angle_is_rejected(self) -> None:
        """User feedback: «Ian Brown зашёл в магазин — непонятно»."""
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "ian-brown-shop",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "Ian Brown of Stone Roses signs records at Tasty Records",
                "summary": (
                    "Stone Roses frontman Ian Brown popped in to Tasty Records "
                    "in Altrincham yesterday and signed a copy of the debut album. "
                    "The shop joked they would sell it for a good offer."
                ),
                "lead": "Ian Brown stopped by the indie record shop.",
                "evidence_text": "Ian Brown visited the record shop and signed an album.",
                "source_label": "MEN",
                "source_url": "https://example.test/ian-brown-shop",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertFalse(
            updated.get("include"),
            f"Celebrity sighting was kept: {updated.get('reason')}",
        )
        self.assertIn("celebrity_sighting", updated.get("reject_reasons") or [])

    def test_celebrity_with_real_news_anchor_is_kept(self) -> None:
        """Defensive: a celebrity who opens a charity or is charged
        with something must pass — that IS news.
        """
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "ian-brown-charity",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "Ian Brown opens new music charity in Manchester",
                "summary": (
                    "Stone Roses frontman Ian Brown opened a new music charity "
                    "for £250,000 in Bolton today, supporting 30 local teens."
                ),
                "lead": "",
                "evidence_text": "Ian Brown opens charity, £250,000 grant, Bolton.",
                "source_label": "MEN",
                "source_url": "https://example.test/ian-brown-charity",
                "published_at": now_london().isoformat(),
                "entities": {"schema_version": 2, "boroughs": ["Bolton"], "all": []},
            }
        )
        # Charity-opening with location + sum is news, not a sighting.
        self.assertNotIn("celebrity_sighting", updated.get("reject_reasons") or [])

    def test_motivational_human_interest_without_anchor_is_rejected(self) -> None:
        """User feedback: «второй день получаю такие новости вчера было
        про какого кто экзамен завалили и стал успешным, нахера мне это?».
        """
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "cameron-bell-mot",
                "category": "tech_business",
                "primary_block": "city_watch",
                "title": "Cameron Bell, 28, failed his A-levels but is now a CEO",
                "summary": (
                    "Cameron Bell, who has dyslexia, was told he would never "
                    "succeed after failing his A-levels but now runs his own "
                    "company, inspiring other young people."
                ),
                "lead": "Cameron Bell overcame failure to become CEO.",
                "evidence_text": (
                    "Cameron Bell, after failing his A-levels, now inspires "
                    "others and runs his own firm."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/cameron-bell",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertFalse(
            updated.get("include"),
            f"Motivational filler was kept: {updated.get('reason')}",
        )
        self.assertIn(
            "motivational_human_interest", updated.get("reject_reasons") or []
        )

    def test_georgia_style_first_job_profile_is_rejected_by_contract(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "georgia-first-job",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "My first job made me question if I was too stupid to work",
                "summary": (
                    "Georgia Sweeney now helps other young people after her first job "
                    "left her feeling insecure."
                ),
                "lead": "",
                "evidence_text": "She shares her experience to inspire young people.",
                "source_label": "MEN",
                "source_url": "https://example.test/georgia-first-job",
                "published_at": now_london().isoformat(),
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("motivational_human_interest", updated.get("reject_reasons") or [])
        self.assertEqual(
            (updated.get("editorial_contract") or {}).get("story_type"),
            "human_interest",
        )

    def test_men_garage_to_millions_profile_is_rejected(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "emma-thackray-profile",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "'We started in a garage in Altrincham, now our hobby is worth millions'",
                "summary": (
                    "Emma Thackray turned a hobby making non-alcoholic drinks "
                    "in a garage into an international company and launched "
                    "a new soda linked to a Hollywood film."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/emma-thackray",
                "published_at": now_london().isoformat(),
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("motivational_human_interest", updated.get("reject_reasons") or [])

    def test_private_property_listing_is_rejected_from_news(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "salford-house-listing",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "What £500,000 buys you in Salford - a huge seven-bed house with four floors",
                "summary": "A seven-bed semi-detached house is for sale after 23 years.",
                "source_label": "MEN",
                "source_url": "https://example.test/property-listing",
                "published_at": now_london().isoformat(),
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("property_listing", updated.get("reject_reasons") or [])

    def test_day_out_guide_is_rejected_from_news(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "delamere-water-park-guide",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "The water park near Manchester with an inflatable Aqua Park, floating obstacle course and more",
                "summary": "The Cheshire attraction is perfect for a sunny day out near Manchester.",
                "source_label": "MEN",
                "source_url": "https://example.test/water-park-guide",
                "published_at": now_london().isoformat(),
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("day_out_guide", updated.get("reject_reasons") or [])

    def test_real_development_and_cost_news_are_not_filler(self) -> None:
        real_items = [
            {
                "title": "Manchester hotel plan for Charles Street Maldron site submitted",
                "summary": "Developers have submitted plans for a new hotel building on Charles Street in Manchester city centre.",
                "source_url": "https://example.test/manchester-hotel-charles-street-maldron",
            },
            {
                "title": "Manchester CIS Tower plan would turn landmark into skyscraper homes",
                "summary": "A developer has lodged a planning application for the CIS Tower in Manchester.",
                "source_url": "https://example.test/manchester-cis-tower-plan-skyscraper",
            },
            {
                "title": "Parking in Manchester is getting more expensive",
                "summary": "New parking charges in Manchester city centre affect drivers from this week.",
                "source_url": "https://example.test/parking-manchester-getting-expensive",
            },
        ]
        for item in real_items:
            with self.subTest(item=item["title"]):
                candidate = {
                    "include": True,
                    "fingerprint": item["source_url"],
                    "category": "media_layer",
                    "primary_block": "last_24h",
                    "title": item["title"],
                    "summary": item["summary"],
                    "source_label": "MEN",
                    "source_url": item["source_url"],
                    "published_at": now_london().isoformat(),
                    "change_type": "new_story",
                }
                contract = build_editorial_contract(candidate)
                self.assertNotEqual(contract["publish_tier"], "filler")
                self.assertFalse(contract.get("reject_reason"))

    def test_road_only_transport_is_rejected(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "smithy-bridge-road",
                "category": "transport",
                "primary_block": "transport",
                "title": "Smithy Bridge Road, Littleborough - Road Closure",
                "summary": "Road closure due to works.",
                "source_label": "TfGM",
                "source_url": "https://tfgm.com/travel-updates/travel-alerts/smithy-bridge-road-littleborough-road-closure",
                "published_at": now_london().isoformat(),
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("road_only_transport", updated.get("reject_reasons") or [])

    def test_kieran_style_career_pivot_profile_is_rejected(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "kieran-career-pivot",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "He wanted to be the next big name, but nine days changed everything",
                "summary": (
                    "Kieran O'Reilly had dreams of making it in rugby, then an injury "
                    "and the pandemic became a turning point before he decided on a "
                    "proper career in cooking."
                ),
                "lead": "",
                "evidence_text": "He says he had been not knowing what he wanted and now enjoys cooking.",
                "source_label": "MEN",
                "source_url": "https://example.test/kieran-career-pivot",
                "published_at": now_london().isoformat(),
                "dedupe_decision": "new",
                "change_type": "new_story",
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("motivational_human_interest", updated.get("reject_reasons") or [])

    def test_motivational_with_local_event_anchor_is_kept(self) -> None:
        """Defensive: «Cameron Bell открывает офис в Bolton 28 мая» —
        the same motivational subject becomes news when paired with a
        concrete local action.
        """
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "cameron-bell-office",
                "category": "tech_business",
                "primary_block": "city_watch",
                "title": "Cameron Bell, 28, opens new tech office in Bolton on 28 May",
                "summary": (
                    "Cameron Bell, who has dyslexia, opens his new startup office "
                    "in Bolton on 28 May with £500,000 of seed funding."
                ),
                "lead": "",
                "evidence_text": (
                    "Cameron Bell opens Bolton office on 28 May, £500,000 seed, "
                    "10 new jobs."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/cameron-bell-office",
                "published_at": now_london().isoformat(),
                "entities": {"schema_version": 2, "boroughs": ["Bolton"], "all": []},
            }
        )
        self.assertNotIn(
            "motivational_human_interest", updated.get("reject_reasons") or []
        )

    def test_ticket_copy_invariant_catches_past_sale_as_future(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Doja Cat — event 2026-05-23 — public sale 2025-10-03 10:00",
            "summary": (
                "Co-op Live | Manchester | Hip-Hop/Rap | event_date=2026-05-23 19:30 | "
                "public_onsale=2025-10-03 10:00 | ticket_signal=upcoming_event"
            ),
            "source_label": "Ticketmaster Manchester Upcoming",
        }
        line = "• В Co-op Live 23 мая — концерт Doja Cat. Билеты будут доступны на Ticketmaster с 3 октября 2025 года."

        self.assertTrue(_line_claims_future_ticket_sale(candidate, line))
        self.assertIn("past_ticket_sale_written_as_future", copy_invariant_errors(candidate, line))

    def test_weather_copy_invariant_is_repaired_not_published(self) -> None:
        candidate = {
            "category": "weather",
            "primary_block": "weather",
            "title": "Weather",
            "source_label": "Met Office",
        }
        line = "• Погода: 15-21°C, вероятность осадков до 0%. Днём заметно теплее утра."

        repaired, reasons = _repair_editorial_contract_line(candidate, line)

        self.assertIn("weather_wording", reasons)
        self.assertNotIn("до 0%", repaired)
        self.assertIn("без существенных осадков", repaired)
        self.assertNotIn("Днём заметно теплее утра", repaired)

    def test_weather_draft_line_handles_zero_rain_as_weather_not_math(self) -> None:
        line = _weather_draft_line(15, 24, 0, "Днём сухо с прояснениями.", "Met Office")

        self.assertIn("15-24°C", line)
        self.assertIn("дождя не ждём", line)
        self.assertNotIn("до 0%", line)
        self.assertNotIn("почти не ждут", line)
        self.assertNotIn("низкий риск", line)

    def test_weather_draft_line_32c_says_heat_not_dry(self) -> None:
        line = _weather_draft_line(19, 32, 0, "", "Met Office")
        self.assertIn("жарк", line.lower())
        self.assertNotIn("без существенных осадков", line)

    def test_weather_draft_line_low_rain_uses_human_phrase(self) -> None:
        line = _weather_draft_line(16, 24, 10, "Днём сухо с прояснениями.", "Met Office")
        self.assertIn("дождь маловероятен, риск до 10%", line)
        self.assertIn("Для поездок и прогулок погода спокойная", line)
        self.assertNotIn("низкий риск осадков", line)

    def test_local_retail_takeover_is_news_anchor_not_filler(self) -> None:
        candidate = {
            "title": "Asda closing as Waitrose set to take over Greater Manchester supermarket",
            "summary": "The Asda store in Hale Barns Square will close before being replaced by a Waitrose in autumn 2026.",
            "lead": "The immaculate Asda store in Hale Barns Square will close its doors before being replaced by a Waitrose in autumn 2026.",
            "source_label": "MEN",
            "primary_block": "last_24h",
            "category": "media_layer",
        }
        contract = build_editorial_contract(candidate)
        self.assertEqual(contract["anchor_type"], "new_phase")
        self.assertEqual(contract["publish_tier"], "strong")

    def test_metrolink_minor_delay_keeps_line_and_does_not_say_works(self) -> None:
        card = extract_transport_card({
            "title": "Eccles Line - Minor Delay.",
            "summary": "Eccles Line - Minor Delay.",
            "source_label": "TfGM",
            "source_url": "https://tfgm.com/travel-updates/travel-alerts/eccles-line-minor-delay",
        })
        self.assertIsNotNone(card)
        line = render_card(card)
        self.assertIn("небольшие задержки на Eccles line", line)
        self.assertNotIn("работы на Eccles line", line)

    def test_global_budget_reserves_slots_for_events_tickets_and_football(self) -> None:
        sections = {
            "Что произошло за 24 часа": ["x"] * 9,
            "Что важно в ближайшие 7 дней": ["x"] * 5,
            "Билеты / Ticket Radar": ["x"] * 4,
            "Футбол": ["x"] * 3,
        }
        ordered = [
            "Что произошло за 24 часа",
            "Что важно в ближайшие 7 дней",
            "Билеты / Ticket Radar",
            "Футбол",
        ]

        self.assertEqual(_reserved_later_budget(ordered, 0, sections), 7)
        self.assertEqual(_reserved_later_budget(ordered, 1, sections), 4)

    def test_optional_news_cannot_stay_in_top_public_sections(self) -> None:
        candidate = {
            "include": True,
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "General Manchester profile with weak public value",
            "summary": "A general local profile with no decision, date or public action.",
            "source_label": "MEN",
            "source_url": "https://example.test/profile",
            "editorial_contract": {
                "publish_tier": "optional",
                "event_shape": "none",
                "reject_reason": "",
            },
        }

        self.assertEqual(_contract_public_drop_reason(candidate), "optional_news_in_top_section")

    def test_bookable_activity_scores_below_real_weekend_event(self) -> None:
        car_boot = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Bowlee Car Boot Sale every Sunday",
            "summary": "Every Sunday at Bowlee Community Park. Entry £2.50.",
            "source_label": "Bowlee Car Boot Sale",
            "source_url": "https://example.test/bowlee",
            "event": {"is_recurring": True},
        }
        bookable = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Alcotraz Penitentiary immersive cocktail bar",
            "summary": "DesignMyNight bookable experience available from 23 May. Tickets from £40.",
            "source_label": "DesignMyNight Bank Holiday",
            "source_url": "https://example.test/alcotraz",
        }
        real_score = _section_priority_score(car_boot, "Выходные в GM", "• В воскресенье — Bowlee Car Boot Sale.")
        bookable_score = _section_priority_score(bookable, "Выходные в GM", "• На эти выходные можно забронировать Alcotraz.")

        self.assertGreater(real_score, bookable_score + 40)
        self.assertEqual(build_editorial_contract(bookable)["event_shape"], "bookable_activity")
        bookable["editorial_contract"] = build_editorial_contract(bookable)
        self.assertEqual(_contract_public_drop_reason(bookable), "bookable_activity_filler")

    def test_core_news_sources_use_news_surfaces_not_homepage_only(self) -> None:
        by_name = {source.name: source for source in SOURCES}
        self.assertIn("BBC Manchester Web", by_name)
        self.assertIn("MEN Latest News", by_name)
        self.assertIn("MEN News Sitemap", by_name)
        self.assertIn("ITV Granada Greater Manchester", by_name)
        self.assertIn("Place North West", by_name)
        self.assertIn("About Manchester News", by_name)
        self.assertIn("Prolific North Manchester", by_name)
        self.assertEqual(
            by_name["MEN"].url,
            "https://www.manchestereveningnews.co.uk/news/greater-manchester-news/",
        )
        self.assertEqual(by_name["MEN Latest News"].url, "https://www.manchestereveningnews.co.uk/news/")
        self.assertEqual(by_name["MEN News Sitemap"].source_type, "xml_sitemap")
        self.assertEqual(by_name["About Manchester News"].url, "https://aboutmanchester.co.uk/feed/")
        self.assertEqual(by_name["About Manchester News"].source_type, "rss")
        self.assertEqual(
            by_name["Prolific North Manchester"].url,
            "https://www.prolificnorth.co.uk/location/manchester/feed/",
        )
        self.assertEqual(by_name["Prolific North Manchester"].primary_block, "tech_business")

    def test_soft_sources_are_not_in_hard_news_layer(self) -> None:
        by_name = {source.name: source for source in SOURCES}
        for source_name in ("The Manc", "I Love Manchester", "Secret Manchester", "University of Manchester", "University of Salford"):
            self.assertNotIn(source_name, by_name)

    def test_known_empty_sources_have_working_replacements_or_are_disabled(self) -> None:
        by_name = {source.name: source for source in SOURCES}
        self.assertEqual(by_name["GMMH"].url, "https://www.gmmh.nhs.uk/media-centre/")
        self.assertTrue(_is_allowed_source_link(
            by_name["GMMH"],
            "https://www.gmmh.nhs.uk/media-centre/press-releases/greater-manchester-mental-health-nhs-foundation-trust-appoints-new-chief-executive-8025",
            "Greater Manchester Mental Health NHS Foundation Trust appoints new Chief Executive",
            "",
        ))
        self.assertEqual(
            by_name["South Manchester Food Festival"].url,
            "https://www.tickettailor.com/events/foodfestival/1883190",
        )
        self.assertEqual(by_name["Manchester City"].url, "https://www.mancity.com/news?tag=News")
        self.assertIn("Secret Manchester May Guide", by_name)
        self.assertIn("Secret Manchester Gigs", by_name)
        self.assertNotIn("Secret Manchester Weekend Guide", by_name)
        self.assertNotIn("Manchester Flower Festival CityCo News", by_name)
        self.assertNotIn("Manchester United", by_name)
        self.assertNotIn("Prolific North", by_name)
        self.assertNotIn("Sofar Manchester Bank Holiday", by_name)

    def test_men_soft_fluff_is_not_publishable_news(self) -> None:
        examples = [
            "Lazy Sunday Quiz: 20 general knowledge questions to test your family",
            "Wythenshawe dad goes viral after installing 12ft pool in his garden",
            "All the places you can't fly a drone in Greater Manchester this weekend",
        ]
        for title in examples:
            with self.subTest(title=title):
                contract = build_editorial_contract(
                    {
                        "include": True,
                        "category": "media_layer",
                        "primary_block": "last_24h",
                        "title": title,
                        "summary": title,
                        "source_label": "MEN",
                        "source_url": "https://www.manchestereveningnews.co.uk/news/greater-manchester-news/example-34000000",
                    }
                )
                self.assertIn(contract["story_type"], {"soft_news", "day_out_guide"})
                self.assertIn(contract["publish_tier"], {"filler", "reject"})

    def test_public_realm_story_gets_specific_repeat_key(self) -> None:
        bridge = {
            "include": True,
            "category": "council",
            "primary_block": "city_watch",
            "title": "Historic lights on Queen's Park Bridge restored",
            "summary": "Rochdale Council restored the bridge lights after a multi-million-pound restoration.",
            "source_label": "Rochdale Council",
            "source_url": "https://example.test/bridge",
            "entities": {"boroughs": ["Rochdale"], "venues": ["Queen's Park Bridge"]},
        }

        contract = build_editorial_contract(bridge)

        self.assertEqual(contract["story_type"], "planning")
        self.assertTrue(contract["topic_key"].startswith("planning:"))
        self.assertNotIn("rochdale council historic", contract["topic_key"])

    def test_barton_recurring_car_boot_is_not_bookable_or_bowlee(self) -> None:
        candidate = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Barton Aerodrome Car Boot Sale",
            "summary": (
                "A popular Saturday car boot sale in Eccles. Next dates Saturday, "
                "23 May 2026 and Saturday, 30 May 2026."
            ),
            "evidence_text": "Barton Aerodrome hosts regular 2026 car boot sales. No pre-booking is required.",
            "source_label": "Barton Aerodrome Car Boot",
            "source_url": "https://manchester-rocks.co.uk/things-to-do/barton-aerodrome-car-boot-sale",
        }

        contract = build_editorial_contract(candidate)

        self.assertEqual(contract["topic_key"], "event:barton_aerodrome_car_boot")
        self.assertEqual(contract["event_shape"], "recurring")

    def test_old_existing_cafe_profile_is_rejected_from_openings(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "grounded-mcr-profile",
                "category": "food_openings",
                "primary_block": "openings",
                "title": "Grounded MCR - the Levenshulme community cafe crafting coffee",
                "summary": (
                    "Starting off life as a little coffee trike back in 2021, "
                    "Grounded MCR is now based inside a container in Cringle Park."
                ),
                "lead": "",
                "evidence_text": "The cafe serves coffee and food and works with community partners.",
                "source_label": "The Manc Eats",
                "source_url": "https://example.test/grounded-mcr",
                "published_at": now_london().isoformat(),
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("old_existing_food", updated.get("reject_reasons") or [])

    def test_old_april_resident_doctors_strike_is_rejected(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "resident-doctors-april-strike",
                "category": "public_services",
                "primary_block": "city_watch",
                "title": "Strike action taking place in April 2026 | News and Events",
                "summary": (
                    "Resident doctors across England will take part in strike action "
                    "from 7am on Tuesday 07 April until 7am on Monday 13 April."
                ),
                "lead": "",
                "evidence_text": "Patients should attend appointments as planned if not contacted.",
                "source_label": "GMMH",
                "source_url": "https://example.test/april-strike",
                "published_at": now_london().isoformat(),
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("stale_public_service", updated.get("reject_reasons") or [])

    def test_topic_contract_dedupes_makerfield_variants_in_same_issue(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "makerfield-bbc",
                "dedupe_decision": "new",
                "category": "media_layer",
                "primary_block": "today_focus",
                "title": "Makerfield by-election candidates announced after Josh Simons quits",
                "summary": "The by-election in Makerfield will be held on 18 June.",
                "source_label": "BBC Manchester",
                "source_url": "https://example.test/makerfield-bbc",
            },
            {
                "include": True,
                "fingerprint": "makerfield-men",
                "dedupe_decision": "new",
                "category": "media_layer",
                "primary_block": "today_focus",
                "title": "Andy Burnham says he will risk everything over Makerfield election",
                "summary": "Burnham spoke about the Makerfield by-election campaign.",
                "source_label": "MEN",
                "source_url": "https://example.test/makerfield-men",
            },
        ]

        drops = _apply_intra_batch_dedup(candidates)

        self.assertEqual(len(drops), 1)
        self.assertEqual(drops[0]["topic_key"], "politics:makerfield_by_election_2026")
        self.assertEqual(sum(1 for item in candidates if item["include"]), 1)

    def test_historical_archive_without_news_hook_is_rejected(self) -> None:
        """User feedback: «Salford Винни Клей 90-х годов — уже было и
        зачем мне эта новость про город сейчас»."""
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "vinnie-clay-archive",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "Vinnie Clay, one of Salford's most feared gangsters of the 90s",
                "summary": (
                    "Vinnie Clay became famous in the 90s for a samurai sword "
                    "attack. He was one of the most feared figures of the era."
                ),
                "lead": "Vinnie Clay was a notorious Salford gangster.",
                "evidence_text": (
                    "Vinnie Clay was a notorious 1990s Salford figure involved in "
                    "various crimes."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/vinnie-clay-archive",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertFalse(
            updated.get("include"),
            f"Historical archive was kept: {updated.get('reason')}",
        )
        self.assertIn(
            "historical_no_news_angle", updated.get("reject_reasons") or []
        )

    def test_historical_subject_with_fresh_crime_is_kept(self) -> None:
        """Defensive: «гангстер 90-х убил вчера кого-то» — historical
        figure + fresh crime verb + fresh date — MUST pass.
        """
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "vinnie-clay-fresh",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "Vinnie Clay, notorious 90s Salford gangster, charged with murder yesterday",
                "summary": (
                    "Vinnie Clay, one of Salford's most feared gangsters of the 90s, "
                    "was charged yesterday with the murder of a 34-year-old man in "
                    "Salford. The trial is set for 15 September."
                ),
                "lead": "Vinnie Clay was charged yesterday with murder in Salford.",
                "evidence_text": (
                    "Vinnie Clay charged with murder yesterday. Victim 34. Trial "
                    "15 September."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/vinnie-clay-fresh",
                "published_at": now_london().isoformat(),
                "entities": {"schema_version": 2, "boroughs": ["Salford"], "all": []},
            }
        )
        self.assertNotIn(
            "historical_no_news_angle", updated.get("reject_reasons") or []
        )

    # ---------------------------------------------------------------
    # S5 — lead-first news cards + quote/narrative-lead detector.
    # User feedback 2026-05-22:
    #   «Trafford складской проект потом дорога нихера непонятно»
    #   «Sudden junction новость как дерьмо нихрена не понятно»
    #   «Heywood пожар можно было сделать новость 6 часов»
    #   «Univ алкоголь мог бы сказать плохо хорошо или как»
    # ---------------------------------------------------------------

    def test_city_news_prompt_is_v6_with_lead_first_structure(self) -> None:
        """The city-news prompt v6 must explicitly require lead-first
        structure (fact first, details next, what-next last) and ban
        quote/narrative leads.
        """
        from news_digest.pipeline import llm_rewrite as _lr
        from news_digest.pipeline.prompts_meta import by_name
        meta = by_name().get("city_news")
        self.assertIsNotNone(meta)
        self.assertEqual(meta.version, "v6", f"city_news not bumped to v6: {meta}")
        prompt = _lr.PROMPT_CITY_NEWS
        self.assertIn("ОБЯЗАТЕЛЬНАЯ СТРУКТУРА", prompt)
        self.assertIn("ЛИД-ФАКТ", prompt)
        # Required field guidance per type.
        self.assertIn("Пожар", prompt)
        self.assertIn("Планирование", prompt)
        self.assertIn("Наука", prompt)
        # Banned openers.
        self.assertIn("прямой цитаты", prompt)

    def test_quote_lead_is_flagged_in_release_report(self) -> None:
        """User feedback: «Sudden junction новость как дерьмо нихрена
        непонятно» — a draft_line opening with a direct quote in
        quotes must be surfaced as quote_lead.
        """
        from news_digest.pipeline.release import _summarise_news_lead_quality
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "sudden-quote-lead",
                    "primary_block": "city_watch",
                    "category": "media_layer",
                    "draft_line": "• «Я была в ужасе от обилия конусов» — местная жительница Madeeha Sheikh о работах на Sudden junction.",
                    "title": "Sudden junction works",
                }
            ],
        }
        result = _summarise_news_lead_quality(candidates_report, {"sudden-quote-lead"})
        self.assertEqual(result["counts"]["quote_lead"], 1)
        self.assertTrue(
            any(issue["issue"] == "quote_lead" for issue in result["issues"]),
        )

    def test_narrative_lead_about_resident_is_flagged(self) -> None:
        """User: «Trafford складской проект потом дорога нихера не понятно»
        — cards that open with a "местная жительница ..." sentence
        instead of the news fact get surfaced.
        """
        from news_digest.pipeline.release import _summarise_news_lead_quality
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "narr-lead",
                    "primary_block": "city_watch",
                    "category": "media_layer",
                    "draft_line": "• Местная жительница Madeeha Sheikh заявила, что в понедельник утром «была в ужасе» от обилия конусов и барьеров.",
                    "title": "Sudden junction roadworks",
                }
            ],
        }
        result = _summarise_news_lead_quality(candidates_report, {"narr-lead"})
        self.assertEqual(result["counts"]["narrative_lead"], 1)

    def test_fact_lead_is_not_flagged(self) -> None:
        """Defensive: a proper fact-first lead must NOT be flagged."""
        from news_digest.pipeline.release import _summarise_news_lead_quality
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "good-lead",
                    "primary_block": "last_24h",
                    "category": "media_layer",
                    "draft_line": "• Trafford: советники отклонили склад Wain Estates в Carrington — план требовал вырубки 10 000+ деревьев. Решение принято вопреки рекомендации чиновников.",
                    "title": "Trafford rejects Wain Estates",
                }
            ],
        }
        result = _summarise_news_lead_quality(candidates_report, {"good-lead"})
        self.assertEqual(result["counts"]["quote_lead"], 0)
        self.assertEqual(result["counts"]["narrative_lead"], 0)

    def test_event_card_is_not_checked_by_news_lead_detector(self) -> None:
        """Defensive: event cards have their own structure (S3 templates),
        the news-lead detector should NOT touch them.
        """
        from news_digest.pipeline.release import _summarise_news_lead_quality
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "event-quote-allowed",
                    "primary_block": "weekend_activities",
                    "category": "culture_weekly",
                    "draft_line": "• «Это незабываемо» — режиссёр о новой постановке в HOME 22 мая в 20:00.",
                    "title": "HOME play",
                }
            ],
        }
        result = _summarise_news_lead_quality(candidates_report, {"event-quote-allowed"})
        # Event card not checked at all (block is weekend_activities).
        self.assertEqual(result["counts"]["checked"], 0)

    def test_genuine_tech_startup_is_not_blocked_by_book_guard(self) -> None:
        """Defensive: a real tech-author crossover must still pass.

        E.g. "AI startup founder publishes manifesto" mentions both
        book/author markers AND tech markers — guard should NOT fire.
        """
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "ai-founder-book",
                "category": "tech_business",
                "primary_block": "city_watch",
                "title": "Manchester AI startup founder launches manifesto book",
                "summary": (
                    "The founder of a Manchester deeptech startup has "
                    "published a book on building cybersecurity platforms."
                ),
                "lead": "",
                "evidence_text": (
                    "The founder runs a Series A-funded SaaS startup "
                    "focused on AI-driven cybersecurity tooling."
                ),
                "source_label": "Bdaily Manchester",
                "source_url": "https://example.test/ai-founder-book",
                "published_at": now_london().isoformat(),
            }
        )
        # Tech markers present → guard does NOT fire; card not auto-rejected
        # by this predicate. (Other gates may still reject — we only assert
        # that the book guard's reject_reason is absent.)
        reject_reasons = updated.get("reject_reasons") or []
        self.assertNotIn("book_author_misrouted", reject_reasons)


if __name__ == "__main__":
    unittest.main()
