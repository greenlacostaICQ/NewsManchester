from __future__ import annotations

import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.collector.routing import _adjust_ticket_radar_block
from news_digest.pipeline.collector.sources import SOURCES
from news_digest.pipeline.common import now_london
from news_digest.pipeline.dedupe import _apply_intra_batch_dedup
from news_digest.pipeline.writer import _build_ticket_fallback_line


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

        self.assertGreaterEqual(len(guide_sources), 8)
        self.assertIn("Visit Manchester Weekend", guide_sources)
        self.assertIn("Secret Manchester May Guide", guide_sources)
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
