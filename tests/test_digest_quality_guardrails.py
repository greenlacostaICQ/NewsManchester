from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import mock

from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.collector.routing import _adjust_ticket_radar_block
from news_digest.pipeline.collector.extract import (
    _enrich_visit_manchester_items,
    _extract_source_candidates,
)
from news_digest.pipeline.collector.fallbacks import _weather_draft_line
from news_digest.pipeline.collector.filters import _is_allowed_source_link
from news_digest.pipeline.collector.sources import SOURCES, ExtractedItem, SourceDef
from news_digest.pipeline.transport_card import extract_transport_card, render_card
from news_digest.pipeline.common import (
    fingerprint_for_candidate,
    now_london,
    today_london,
)
from news_digest.pipeline.dedupe import (
    _apply_intra_batch_dedup,
    _filter_distinct_market_previous_matches,
    _merge_multinight_ticket_runs,
    _borderline_pairs,
    _normalise_person_tokens,
    _people_published_matches,
    _prefer_dedupe_candidate,
    _topic_published_matches,
    dedupe_candidates,
)
from news_digest.pipeline.entity_extraction import extract_entities
from news_digest.pipeline.editorial_contracts import (
    build_editorial_contract,
    calendar_repeat_review,
    copy_invariant_errors,
    lifecycle_repeat_review,
)
from news_digest.pipeline.llm_rewrite import _apply_rewrite_shortlist
from news_digest.pipeline.writer import (
    _apply_fresh_semantic_duplicate_pass,
    _build_football_fallback_line,
    _build_weekend_event_fallback_line,
    _build_recurring_event_fallback_line,
    _build_ticket_fallback_line,
    _contract_public_drop_reason,
    _draft_line_quality_errors,
    _ensure_source_anchor_for_rendered_line,
    _line_claims_future_ticket_sale,
    _number_tokens,
    _repair_editorial_contract_line,
    _reconcile_rendered_dropped_candidates,
    _section_priority_score,
    _SectionRow,
    _strip_unsupported_number_phrases,
    _today_focus_candidate_is_eligible,
    _today_focus_recovery_line,
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

    def test_football_dedupe_does_not_merge_distinct_city_stories(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "city-maresca",
                "category": "football",
                "primary_block": "football",
                "source_label": "BBC Sport Manchester City",
                "source_url": "https://bbc.example/maresca",
                "title": "Enzo Maresca: Man City still in negotiations to appoint new manager",
                "summary": "Manchester City are continuing negotiations with Chelsea to appoint Enzo Maresca.",
            },
            {
                "include": True,
                "fingerprint": "city-guardiola-doc",
                "category": "football",
                "primary_block": "football",
                "source_label": "BBC Sport Manchester City",
                "source_url": "https://bbc.example/guardiola-doc",
                "title": "Pep Guardiola: Former Man City manager's final seasons to air in Amazon documentary",
                "summary": "Pep Guardiola's final two seasons in charge of Manchester City will air on Amazon.",
            },
        ]

        drops = _apply_intra_batch_dedup(candidates)

        self.assertEqual(drops, [])
        self.assertTrue(all(candidate["include"] for candidate in candidates))

    def test_football_dedupe_still_merges_same_player_same_claim(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "haaland-article",
                "category": "football",
                "primary_block": "football",
                "source_label": "BBC Sport Manchester City",
                "source_url": "https://bbc.example/haaland-article",
                "title": "Erling Haaland: Man City threaten legal action over Real Madrid candidate's transfer claim",
                "summary": "Manchester City dismiss a Real Madrid candidate's transfer claim about Erling Haaland.",
            },
            {
                "include": True,
                "fingerprint": "haaland-video",
                "category": "football",
                "primary_block": "football",
                "source_label": "BBC Sport Manchester City",
                "source_url": "https://bbc.example/haaland-video",
                "title": "Erling Haaland: Man City threaten legal action over Real Madrid candidate's transfer claim",
                "summary": "A video item repeats the same Erling Haaland transfer claim.",
            },
        ]

        drops = _apply_intra_batch_dedup(candidates)

        self.assertEqual(len(drops), 1)
        self.assertEqual(sum(1 for candidate in candidates if candidate["include"]), 1)

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
        # Freeze "today" so the bare day-month dates resolve to 2026 (not a
        # year-rollover) and the staleness window stays deterministic.
        frozen = datetime(2026, 5, 20, 12, 0, tzinfo=now_london().tzinfo)
        with mock.patch("news_digest.pipeline.candidate_validator.now_london", return_value=frozen):
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

    def test_ticket_watchlist_line_keeps_genre_without_generic_cta(self) -> None:
        # Relative future date: a hardcoded event_date rots into the past and
        # the watch decision then hides the line entirely.
        event_day = (now_london().date() + timedelta(days=5)).isoformat()
        line = _build_ticket_fallback_line(
            {
                "category": "venues_tickets",
                "primary_block": "ticket_radar",
                "title": "Lola Young",
                "ticket_type": "old_public_sale",
                "summary": (
                    f"O2 Apollo Manchester | Manchester | R&B | "
                    f"event_date={event_day} 19:00 | public_onsale=2025-11-14 10:00 | "
                    "ticket_signal=upcoming_event | ticket_type=major_upcoming | major_venue=true"
                ),
                "ticket_notability": {"artist": "Lola Young", "kind": "artist", "tier": "B"},
            }
        )

        self.assertIn("Lola Young", line)
        self.assertIn("(R&B)", line)
        self.assertNotIn("Почему в радаре", line)
        self.assertNotIn("Билеты и детали берите", line)
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

    def test_ticket_premium_variant_collapses_into_main_ticket(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "ub40-main",
                "title": "UB40 — event 2026-06-06",
                "primary_block": "ticket_radar",
                "category": "venues_tickets",
                "event": {"event_name": "UB40", "venue": "O2 Apollo Manchester", "date_start": "2026-06-06T18:30:00+01:00"},
            },
            {
                "include": True,
                "fingerprint": "ub40-premium",
                "title": "UB40 - Venue Premium Tickets — event 2026-06-06",
                "primary_block": "ticket_radar",
                "category": "venues_tickets",
                "event": {"event_name": "UB40 - Venue Premium Tickets", "venue": "O2 Apollo Manchester", "date_start": "2026-06-06T18:30:00+01:00"},
            },
        ]

        drops = _merge_multinight_ticket_runs(candidates)

        self.assertEqual(len(drops), 1)
        self.assertTrue(candidates[0]["include"])
        self.assertFalse(candidates[1]["include"])
        self.assertIn("Premium/package", candidates[1]["reason"])

    def test_distinct_markets_do_not_collapse_before_topic_cluster(self) -> None:
        candidates = [
            {
                "include": True,
                "fingerprint": "nq-market",
                "title": "Makers Market",
                "summary": "Every Sunday at Northern Quarter with traders and food.",
                "primary_block": "weekend_activities",
                "source_label": "Northern Quarter Makers Market",
                "story_cluster_key": "generic-makers-market",
            },
            {
                "include": True,
                "fingerprint": "wythenshawe-market",
                "title": "Makers Market",
                "summary": "Every Sunday at Wythenshawe with traders and food.",
                "primary_block": "weekend_activities",
                "source_label": "Wythenshawe Makers Market",
                "story_cluster_key": "generic-makers-market",
            },
        ]

        self.assertEqual(_apply_intra_batch_dedup(candidates), [])

    def test_distinct_market_previous_match_is_ignored(self) -> None:
        candidate = {
            "title": "Prestwich Makers Market",
            "summary": "A makers market in Prestwich this weekend.",
            "evidence_text": "Craft, food and local makers at Prestwich.",
            "source_label": "Prestwich Makers Market",
            "primary_block": "weekend_activities",
        }
        previous = {
            "fingerprint": "northern-quarter-market",
            "title": "Northern Quarter Makers Market",
            "summary": "A makers market at Oak Street in Manchester.",
            "source_label": "Northern Quarter Makers Market",
            "primary_block": "weekend_activities",
        }
        matches = [{"fingerprint": "northern-quarter-market", "title": previous["title"], "overlap": 0.6}]

        filtered = _filter_distinct_market_previous_matches(
            candidate,
            matches,
            {"northern-quarter-market": previous},
        )

        self.assertEqual(filtered, [])

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

    def test_july_weekend_audit_sources_are_direct_weekend_pages(self) -> None:
        required = {
            "Manchester Brick Festival",
            "Foodies Festival Tatton Park",
            "Festwich",
            "Prestwich Makers Market",
        }
        by_name = {source.name: source for source in SOURCES}

        self.assertTrue(required.issubset(by_name))
        for name in required:
            source = by_name[name]
            self.assertEqual(source.primary_block, "weekend_activities")
            self.assertEqual(source.source_type, "html_page_event")
            self.assertEqual(source.max_candidates, 1)

    def test_visit_manchester_rejects_catalog_pages_before_enrichment(self) -> None:
        source = SourceDef(
            name="Visit Manchester Weekend",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://www.visitmanchester.com/whats-on/whats-on-this-weekend/",
            primary_block="weekend_activities",
            source_type="html_visitmanchester_events",
            allowed_hosts=("visitmanchester.com",),
            max_candidates=12,
        )
        html = """
        <a href="/whats-on/whats-on-this-weekend/${Tripbuilder.Path}">template</a>
        <a href="/whats-on/events/whats-on-opera-house-palace-theatre">venue index</a>
        <a href="/whats-on/events/entertainment-events">category index</a>
        <a href="/whats-on/events/events-at-manchester-arena">arena index</a>
        <a href="/whats-on/event/manchester-jazz-festival-2026">real event</a>
        """
        enriched_urls: list[str] = []

        def keep(item_source: SourceDef, item: ExtractedItem) -> ExtractedItem:
            self.assertEqual(item_source, source)
            enriched_urls.append(item.url)
            return item

        with mock.patch("news_digest.pipeline.collector.extract._enrich_item", side_effect=keep):
            candidates = _extract_source_candidates(source, html)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["title"], "Manchester Jazz Festival 2026")
        self.assertEqual(enriched_urls, ["https://www.visitmanchester.com/whats-on/event/manchester-jazz-festival-2026"])

    def test_visit_manchester_child_enrichment_is_bounded_parallel_and_ordered(self) -> None:
        source = SourceDef(
            name="Visit Manchester Weekend",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://www.visitmanchester.com/whats-on/whats-on-this-weekend/",
            primary_block="weekend_activities",
            source_type="html_visitmanchester_events",
        )
        items = [
            ExtractedItem(title=f"Manchester event number {index}", url=f"https://example.test/{index}")
            for index in range(8)
        ]
        lock = threading.Lock()
        active = 0
        peak = 0

        def enrich(_source: SourceDef, item: ExtractedItem) -> ExtractedItem:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.02)
            with lock:
                active -= 1
            return item

        with mock.patch("news_digest.pipeline.collector.extract._enrich_item", side_effect=enrich):
            result = _enrich_visit_manchester_items(source, items)

        self.assertEqual([item.url for item in result], [item.url for item in items])
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 4)

    def test_sk_lowdown_markets_source_routes_to_weekend_inventory(self) -> None:
        source = next(source for source in SOURCES if source.name == "SK Lowdown Markets")

        self.assertEqual(source.report_category, "culture_weekly")
        self.assertEqual(source.candidate_category, "culture_weekly")
        self.assertEqual(source.primary_block, "weekend_activities")

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

    def test_weekend_market_topic_key_does_not_match_planning_story(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "bowlee-car-boot",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Bowlee Car Boot Sale",
            "summary": (
                "Bowlee Car Boot Sale and MarketDates: Sundays and Bank Holiday "
                "Mondays (April - October 2026)."
            ),
            "lead": "Bowlee Car Boot Sale at Bowlee Community Park.",
            "evidence_text": (
                "Dates: Sundays and Bank Holiday Mondays (April - October 2026). "
                "Location: Bowlee Community Park, Middleton."
            ),
            "source_label": "Bowlee Car Boot Sale",
            "source_url": "https://example.test/bowlee-car-boot",
            "repeat_story_key": "event:bowlee_car_boot_sale",
            "event": {
                "is_event": True,
                "event_name": "Bowlee Car Boot Sale",
                "date_start": "2026-07-05",
                "venue": "Bowlee Community Park",
            },
        }
        previous = {
            "fingerprint": "planning-bowlee-homes",
            "title": "Shout of traitors as thousands of homes signed off for countryside",
            "primary_block": "last_24h",
            "category": "media_layer",
            "repeat_story_key": "event:bowlee_car_boot_sale",
            "last_published_day_london": "2026-07-08",
            "editorial_contract": {
                "story_type": "planning",
                "event_shape": "none",
            },
        }

        matches = _topic_published_matches(candidate, {"event:bowlee_car_boot_sale": [previous]})

        self.assertEqual(matches, [])

    def test_weekend_market_does_not_render_collection_time_as_event_time(self) -> None:
        event_day = now_london().date() + timedelta(days=1)
        event_month = event_day.strftime("%B")  # real month of tomorrow — robust across month boundaries
        candidate = {
            "include": True,
            "fingerprint": "didsbury-market-no-published-time",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": f"Didsbury Makers Market on {event_day.day} {event_month}",
            "summary": f"Didsbury Makers Market runs on {event_day.day} {event_month} with makers, food and craft stalls.",
            "lead": "Independent makers and food stalls in Didsbury.",
            "evidence_text": "Makers, food, craft stalls and family-friendly shopping.",
            "source_label": "The Makers Market",
            "source_url": "https://example.test/didsbury-makers-market",
            "published_at": f"{today_london()}T08:14:00+01:00",
            "event": {"is_event": True, "is_recurring": True, "event_name": "Didsbury Makers Market", "venue": "Didsbury"},
        }

        line = _build_weekend_event_fallback_line(candidate)

        self.assertIn(str(event_day.day), line)
        self.assertNotIn("08:14", line)

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

    def test_afisha_classical_event_does_not_occupy_russian_speaking_block(self) -> None:
        event_day = (now_london().date() + timedelta(days=14)).isoformat()
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "afisha-london-eugene-onegin",
                "category": "russian_speaking_events",
                "primary_block": "russian_events",
                "title": "Eugene Onegin at The Grange Festival",
                "summary": "Tchaikovsky opera at The Grange Festival, performed in Hampshire.",
                "lead": "Opera production by The Grange Festival.",
                "evidence_text": "The Grange Festival presents Eugene Onegin. Tickets are available online.",
                "source_label": "Afisha London",
                "source_url": "https://afisha.london/en/event/eugene-onegin-at-the-grange-festival",
                "published_at": now_london().isoformat(),
                "event": {
                    "is_event": True,
                    "event_name": "Eugene Onegin",
                    "venue": "The Grange Festival",
                    "date_start": event_day,
                    "booking_url": "https://afisha.london/en/event/eugene-onegin-at-the-grange-festival",
                },
            }
        )

        self.assertFalse(updated["include"])
        self.assertEqual(updated["russian_event_classifier"]["decision"], "drop_from_russian_block")

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

    def test_pr_only_tech_business_rejected_real_action_kept(self) -> None:
        """User feedback 2026-06-13: «Manchester Digital V25.0 — PR/anniversary».

        tech/business publishes only on a concrete action (jobs, investment,
        opening/closure, contract). Anniversary/campaign PR is rejected; a PR
        wrapper around a real action is kept.
        """
        pr_only = self._validate_one(
            {
                "include": True,
                "fingerprint": "mcr-digital-anniversary",
                "category": "tech_business",
                "primary_block": "city_watch",
                "title": "Manchester Digital celebrates V25.0 anniversary",
                "summary": "Manchester Digital marks 25 years with a celebration campaign.",
                "lead": "",
                "evidence_text": "The community network celebrates its anniversary milestone.",
                "source_label": "Manchester Digital",
                "source_url": "https://example.test/mcr-digital",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertFalse(pr_only.get("include"))
        self.assertIn("tech_business_pr_only", pr_only.get("reject_reasons") or [])

        real_action = self._validate_one(
            {
                "include": True,
                "fingerprint": "fintech-office-jobs",
                "category": "tech_business",
                "primary_block": "city_watch",
                "title": "Fintech firm celebrates 10 years and opens second Manchester office",
                "summary": "The company opens a second office in Manchester, creating 40 new jobs.",
                "lead": "",
                "evidence_text": "The expansion adds 40 roles at a new city-centre office.",
                "source_label": "Bdaily Manchester",
                "source_url": "https://example.test/fintech-office",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertTrue(real_action.get("include"))
        self.assertNotIn("tech_business_pr_only", real_action.get("reject_reasons") or [])

    def test_tech_business_partner_appointment_without_impact_is_rejected(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "sw-tax-partner",
                "category": "tech_business",
                "primary_block": "tech_business",
                "title": "S&W appoints Manchester tax partner",
                "summary": (
                    "Professional services group S&W has appointed Ed Gibson "
                    "as a partner in its Manchester tax team."
                ),
                "lead": "The appointment expands the firm's North West tax team.",
                "evidence_text": (
                    "S&W appoints Manchester tax partner Ed Gibson. The firm said "
                    "the appointment will support clients and the business community."
                ),
                "source_label": "Bdaily Manchester",
                "source_url": "https://example.test/sw-tax-partner",
                "published_at": now_london().isoformat(),
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("tech_business_personnel_pr", updated.get("reject_reasons") or [])

    def test_sold_out_event_is_not_published(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "lowry-babies-sold-out",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "title": "Babies Playtime at The Lowry",
                "summary": "Free baby-and-carer sessions on 18 June. Tickets are sold out.",
                "lead": "",
                "evidence_text": "The Lowry page says this event is sold out with no places available.",
                "source_label": "The Lowry",
                "source_url": "https://example.test/lowry-babies-playtime",
                "published_at": now_london().isoformat(),
                "event": {
                    "is_event": True,
                    "event_name": "Babies Playtime",
                    "venue": "The Lowry",
                    "date_start": (now_london().date() + timedelta(days=3)).isoformat(),
                },
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("event_sold_out", updated.get("reject_reasons") or [])

    def test_next_7_market_is_routed_to_weekend_block(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "bolton-car-boot-next7",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "title": "Bolton Car Boot Sale",
                "summary": "Bolton Car Boot Sale runs every Sunday; buyers from 7am, entry £1.",
                "lead": "",
                "evidence_text": "Recurring car boot market at Macron Stadium every Sunday.",
                "source_label": "Bolton Car Boot Sale",
                "source_url": "https://example.test/bolton-car-boot",
                "published_at": now_london().isoformat(),
                "event": {"is_recurring": True, "venue": "Macron Stadium"},
            }
        )

        self.assertTrue(updated.get("include"))
        self.assertEqual(updated.get("primary_block"), "weekend_activities")

    def test_weekly_market_with_next_weekend_date_stays_weekend(self) -> None:
        event_day = now_london().date() + timedelta(days=5)
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "first-street-makers-market",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "title": "First Street Makers Market",
                "summary": f"event_date={event_day.isoformat()} Monthly First Street Makers Market with craft stalls and food.",
                "lead": "",
                "evidence_text": "Makers market with food and craft stalls for weekend visitors every month.",
                "source_label": "Pedddle Makers Market",
                "source_url": "https://example.test/first-street-makers-market",
                "published_at": now_london().isoformat(),
                "event": {
                    "is_event": True,
                    "event_name": "First Street Makers Market",
                    "venue": "First Street",
                    "date_start": event_day.isoformat(),
                },
            }
        )

        self.assertTrue(updated.get("include"))
        self.assertEqual(updated.get("primary_block"), "weekend_activities")

    def test_annual_food_festival_outside_current_weekend_stays_future(self) -> None:
        # Freeze on Thursday: +5 days is Tuesday and therefore definitely
        # outside the current weekend. Using the real weekday made this test
        # assert the opposite product rule every Monday.
        frozen = datetime(2026, 7, 16, 8, 0, tzinfo=now_london().tzinfo)
        event_day = frozen.date() + timedelta(days=5)
        candidate = {
                "include": True,
                "fingerprint": "bbq-food-festival-next7",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "title": "Manchester BBQ Food Festival",
                "summary": f"event_date={event_day.isoformat()} Annual BBQ and street food festival with live music.",
                "lead": "",
                "evidence_text": "This annual barbecue food festival has street food, artists and live music.",
                "source_label": "Manchester Food & Drink Festival",
                "source_url": "https://example.test/bbq-food-festival",
                "published_at": frozen.isoformat(),
                "event": {
                    "is_event": True,
                    "event_name": "Manchester BBQ Food Festival",
                    "venue": "St Ann's Square",
                    "date_start": event_day.isoformat(),
                },
        }
        with mock.patch("news_digest.pipeline.candidate_validator.now_london", return_value=frozen):
            updated = self._validate_one(candidate)

        self.assertTrue(updated.get("include"))
        self.assertEqual(updated.get("primary_block"), "future_announcements")

    def test_weekend_source_event_outside_current_weekend_stays_future(self) -> None:
        frozen = datetime(2026, 7, 16, 8, 0, tzinfo=now_london().tzinfo)
        event_day = frozen.date() + timedelta(days=5)
        candidate = {
                "include": True,
                "fingerprint": "annual-food-festival-weekend-source",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "title": "South Manchester Food Festival",
                "summary": f"event_date={event_day.isoformat()} Annual food festival with BBQ, live music and family activities.",
                "lead": "",
                "evidence_text": "A once-a-year food festival in Wythenshawe Park with barbecue traders and live music.",
                "source_label": "South Manchester Food Festival",
                "source_url": "https://example.test/south-manchester-food-festival",
                "published_at": frozen.isoformat(),
                "event": {
                    "is_event": True,
                    "event_name": "South Manchester Food Festival",
                    "venue": "Wythenshawe Park",
                    "date_start": event_day.isoformat(),
                },
        }
        with mock.patch("news_digest.pipeline.candidate_validator.now_london", return_value=frozen):
            updated = self._validate_one(candidate)

        self.assertTrue(updated.get("include"))
        self.assertEqual(updated.get("primary_block"), "future_announcements")

    def test_annual_food_festival_in_current_weekend_stays_weekend(self) -> None:
        event_day = now_london().date() + timedelta(days=2)
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "annual-food-festival-current-weekend",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "title": "South Manchester Food Festival",
                "summary": f"event_date={event_day.isoformat()} Annual food festival with BBQ, live music and family activities.",
                "lead": "",
                "evidence_text": "A once-a-year food festival in Wythenshawe Park with barbecue traders and live music.",
                "source_label": "South Manchester Food Festival",
                "source_url": "https://example.test/south-manchester-food-festival-weekend",
                "published_at": now_london().isoformat(),
                "event": {
                    "is_event": True,
                    "event_name": "South Manchester Food Festival",
                    "venue": "Wythenshawe Park",
                    "date_start": event_day.isoformat(),
                },
            }
        )

        self.assertTrue(updated.get("include"))
        self.assertEqual(updated.get("primary_block"), "weekend_activities")

    def test_car_boot_recurring_fallback_says_entry_not_tickets(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "car-boot-entry-wording",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Bolton Car Boot Sale",
            "summary": "Every Sunday at Macron Stadium. Buyers from 7am; entry £1.",
            "lead": "",
            "evidence_text": "Car boot sale with more than 300 sellers. Entry £1 for buyers.",
            "source_label": "Bolton Car Boot Sale",
            "source_url": "https://example.test/bolton-car-boot-entry",
            "event": {"is_recurring": True, "venue": "Macron Stadium"},
        }

        line = _build_recurring_event_fallback_line(candidate)

        self.assertIn("вход £1", line)
        self.assertNotIn("билеты", line.lower())

    def test_court_roundup_listicle_is_rejected(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "locked-up-this-week-roundup",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "The criminals locked up this week in Greater Manchester",
                "summary": "Among those jailed this week are a teacher, a jilted lover and a drugs courier.",
                "lead": "",
                "evidence_text": "A court roundup mixes several unrelated cases.",
                "source_label": "MEN",
                "source_url": "https://example.test/locked-up-this-week",
                "published_at": now_london().isoformat(),
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("court_roundup_listicle", updated.get("reject_reasons") or [])

    def test_council_cabinet_admin_without_reader_impact_is_rejected(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "stockport-cabinet-admin",
                "category": "council",
                "primary_block": "city_watch",
                "title": "Council leader appoints cabinet",
                "summary": "Mark Roberts remains council leader and Gillian Julian remains deputy leader.",
                "lead": "The appointments mark the start of the municipal year.",
                "evidence_text": "The council confirmed cabinet appointments and portfolio names for the year.",
                "source_label": "Stockport Council",
                "source_url": "https://example.test/stockport-cabinet",
                "published_at": now_london().isoformat(),
            }
        )

        self.assertFalse(updated.get("include"))
        self.assertIn("council_admin_no_reader_impact", updated.get("reject_reasons") or [])

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

    def test_far_future_ticket_repeat_waits_for_reader_moment(self) -> None:
        """A ticket/event card should not reappear every morning just because
        the event is still in the future. It may return on useful milestones.
        """
        event_day = (now_london().date() + timedelta(days=21)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Jason Isbell and the 400 Unit — event {event_day} — public sale 2026-03-01 10:00",
            "summary": f"Bridgewater Hall | event_date={event_day} 19:30 | public_onsale=2026-03-01 10:00",
            "event": {
                "is_event": True,
                "event_name": "Jason Isbell and the 400 Unit",
                "venue": "Bridgewater Hall",
                "date_start": event_day,
            },
            "source_label": "Ticketmaster Manchester Upcoming",
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=1)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)
        lifecycle = lifecycle_repeat_review(candidate, previous)

        self.assertFalse(review["allow"], review)
        self.assertTrue(lifecycle["repeat"], lifecycle)
        self.assertEqual(review["reason"], "same_calendar_item_without_new_reader_moment")

    def test_truncated_date_text_is_not_a_material_change(self) -> None:
        """Source-page decay («26 June – 12 July» → «26 June» once the run
        ended) must not re-authorise a daily repeat: Онегин ran 3+ issues on
        exactly this artefact."""
        event_day = (now_london().date() + timedelta(days=350)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "repeat",
            "category": "russian_speaking_events",
            "primary_block": "russian_events",
            "title": "Eugene Onegin at the Grange Festival",
            "summary": "Opera by Tchaikovsky at the Grange Festival.",
            "event": {
                "is_event": True,
                "event_name": "Eugene Onegin",
                "date_start": event_day,
                "date_text": "26 June",
            },
        }
        previous = dict(candidate)
        previous["event"] = {**candidate["event"], "date_text": "26 June – 12 July"}
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=1)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)

        self.assertFalse(review["allow"], review)
        self.assertEqual(review["reason"], "same_calendar_item_without_new_reader_moment")

    def test_multi_day_festival_in_progress_is_not_a_passed_repeat(self) -> None:
        """W1 / RC3 (Didsbury Arts): a multi-day festival that started yesterday
        and ends next week is still running today — not a stale "already passed"
        repeat. The END date, not just the start, decides whether it passed."""
        today = now_london().date()
        start = today - timedelta(days=1)
        end = today + timedelta(days=7)
        candidate = {
            "include": True,
            "dedupe_decision": "repeat",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Didsbury Arts Festival",
            "summary": f"Festival runs {start.day}–{end.day} {end.strftime('%B')}.",
            "event": {
                "is_event": True,
                "event_name": "Didsbury Arts Festival",
                "venue": "Didsbury",
                "date": start.isoformat(),
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
            },
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = start.isoformat()
        previous["first_published_day_london"] = start.isoformat()
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)
        self.assertNotEqual(review.get("reason"), "event_already_passed", review)
        self.assertTrue(review["allow"], review)

    def test_calendar_repeat_rehash_is_not_sent_to_llm_review(self) -> None:
        event_day = (now_london().date() + timedelta(days=170)).isoformat()
        previous = {
            "fingerprint": "eventfirst-skameika",
            "title": 'Спектакль "Скамейка"',
            "summary": "29 ноября в Logan Hall — спектакль с Алексеем Паниным.",
            "category": "russian_speaking_events",
            "primary_block": "russian_events",
            "source_label": "EventFirst Diaspora",
            "event": {
                "is_event": True,
                "event_name": 'Спектакль "Скамейка"',
                "date_start": event_day,
            },
            "last_published_day_london": (now_london().date() - timedelta(days=1)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=6)).isoformat(),
        }
        previous["editorial_contract"] = build_editorial_contract(previous)
        candidate = {
            "fingerprint": "eventfirst-skameika",
            "title": 'Спектакль "Скамейка"',
            "summary": "29 ноября в Logan Hall — спектакль с Алексеем Паниным.",
            "lead": "Театральное событие в Лондоне.",
            "evidence_text": "29 ноября в Logan Hall — спектакль «Скамейка» с Алексеем Паниным. Начало в 19:00.",
            "category": "russian_speaking_events",
            "primary_block": "russian_events",
            "source_label": "EventFirst Diaspora",
            "event": dict(previous["event"]),
            "change_type": "same_story_rehash",
        }
        lifecycle = lifecycle_repeat_review(candidate, previous)
        self.assertTrue(lifecycle["repeat"], lifecycle)
        candidate["topic_lifecycle_repeat"] = lifecycle

        pairs = _borderline_pairs([candidate], {"eventfirst-skameika": previous})

        self.assertEqual(pairs, [])

    def test_ticket_repeat_allows_third_public_show_but_not_fourth(self) -> None:
        event_day = (now_london().date() + timedelta(days=1)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Jason Isbell and the 400 Unit — event {event_day} — public sale 2026-03-01 10:00",
            "summary": f"Bridgewater Hall | event_date={event_day} 19:30 | public_onsale=2026-03-01 10:00",
            "event": {
                "is_event": True,
                "event_name": "Jason Isbell and the 400 Unit",
                "venue": "Bridgewater Hall",
                "date_start": event_day,
            },
            "source_label": "Ticketmaster Manchester Upcoming",
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=2)).isoformat()
        previous["first_published_day_london"] = (now_london().date() - timedelta(days=9)).isoformat()
        previous["published_count"] = 2
        previous["editorial_contract"] = build_editorial_contract(previous)

        third_show = calendar_repeat_review(candidate, previous)
        self.assertTrue(third_show["allow"], third_show)

        previous["published_count"] = 3
        fourth_show = calendar_repeat_review(candidate, previous)
        self.assertFalse(fourth_show["allow"], fourth_show)
        self.assertEqual(fourth_show["reason"], "ticket_repeat_limit_reached")

    def test_far_future_ticket_repeat_dropped_across_sources(self) -> None:
        event_day = (now_london().date() + timedelta(days=21)).isoformat()
        previous = {
            "fingerprint": "bridgewater-jason-old",
            "title": f"Jason Isbell and the 400 Unit — event {event_day} — public sale 2026-03-01 10:00",
            "summary": f"Bridgewater Hall | event_date={event_day} 19:30 | public_onsale=2026-03-01 10:00",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "source_label": "Ticketmaster Manchester Upcoming",
            "event": {
                "is_event": True,
                "event_name": "Jason Isbell and the 400 Unit",
                "venue": "Bridgewater Hall",
                "date_start": event_day,
            },
            "last_published_day_london": (now_london().date() - timedelta(days=1)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=1)).isoformat(),
        }
        previous["editorial_contract"] = build_editorial_contract(previous)
        previous["repeat_story_key"] = previous["editorial_contract"]["topic_key"]
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "reason": "Candidate selected.",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": previous["title"],
            "summary": previous["summary"],
            "event": previous["event"],
            "source_label": "Bridgewater Hall",
            "source_url": "https://example.test/bridgewater/jason-isbell",
            "published_at": now_london().isoformat(),
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "published_facts.json").write_text(
                json.dumps({"last_updated_london": today_london(), "facts": [previous]}, ensure_ascii=False),
                encoding="utf-8",
            )
            (state_dir / "candidates.json").write_text(
                json.dumps({"pipeline_run_id": "t", "run_date_london": today_london(), "candidates": [candidate]}, ensure_ascii=False),
                encoding="utf-8",
            )

            dedupe_candidates(root)
            out = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))

        updated = out["candidates"][0]
        self.assertFalse(updated["include"], updated)
        self.assertEqual(updated["dedupe_decision"], "drop")
        self.assertEqual(updated["change_type"], "same_story_rehash")

    def test_a_tier_major_ticket_repeat_allowed_at_annual_milestone_from_history(self) -> None:
        event_day = (now_london().date() + timedelta(days=365)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Example Global Artist — event {event_day} — public sale 2026-01-01 10:00",
            "summary": f"Co-op Live | event_date={event_day} 19:30 | public_onsale=2026-01-01 10:00",
            "ticket_type": "major_upcoming",
            "event": {
                "is_event": True,
                "event_name": "Example Global Artist",
                "venue": "Co-op Live",
                "date_start": event_day,
            },
            "source_label": "Ticketmaster Manchester Upcoming",
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=10)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["ticket_notability"] = {"artist": "Example Global Artist", "tier": "A"}
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)
        lifecycle = lifecycle_repeat_review(candidate, previous)

        self.assertTrue(review["allow"], review)
        self.assertEqual(review["reason"], "event_milestone_d365")
        self.assertFalse(lifecycle["repeat"], lifecycle)

    def test_b_tier_ticket_repeat_does_not_get_a_tier_long_milestones(self) -> None:
        event_day = (now_london().date() + timedelta(days=90)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Example B Artist — event {event_day} — public sale 2026-01-01 10:00",
            "summary": f"Manchester Academy | event_date={event_day} 19:30 | public_onsale=2026-01-01 10:00",
            "ticket_type": "major_upcoming",
            "event": {
                "is_event": True,
                "event_name": "Example B Artist",
                "venue": "Manchester Academy",
                "date_start": event_day,
            },
            "source_label": "Manchester Academy",
            "ticket_notability": {"artist": "Example B Artist", "tier": "B"},
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=10)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)

        self.assertFalse(review["allow"], review)
        self.assertEqual(review["reason"], "same_calendar_item_without_new_reader_moment")

    def test_unknown_ticket_repeat_does_not_use_generic_thirty_day_event_milestone(self) -> None:
        event_day = (now_london().date() + timedelta(days=30)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Example Small Artist — event {event_day} — public sale 2026-01-01 10:00",
            "summary": f"Small Room | event_date={event_day} 19:30 | public_onsale=2026-01-01 10:00",
            "ticket_type": "regular_upcoming",
            "event": {
                "is_event": True,
                "event_name": "Example Small Artist",
                "venue": "Small Room",
                "date_start": event_day,
            },
            "source_label": "Small Room",
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=10)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)

        self.assertFalse(review["allow"], review)
        self.assertEqual(review["reason"], "same_calendar_item_without_new_reader_moment")

    def test_published_facts_preserve_ticket_notability_for_repeat_rules(self) -> None:
        from news_digest.pipeline.history import update_published_facts

        event_day = (now_london().date() + timedelta(days=365)).isoformat()
        candidate = {
            "include": True,
            "fingerprint": "global-artist-ticket",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Example Global Artist — event {event_day} — public sale 2026-01-01 10:00",
            "summary": f"Co-op Live | event_date={event_day} 19:30 | public_onsale=2026-01-01 10:00",
            "source_label": "Ticketmaster Manchester Upcoming",
            "source_url": "https://example.test/global-artist",
            "ticket_type": "major_upcoming",
            "ticket_notability": {"artist": "Example Global Artist", "tier": "A", "signal": "test"},
            "event": {
                "is_event": True,
                "event_name": "Example Global Artist",
                "venue": "Co-op Live",
                "date_start": event_day,
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            update_published_facts(root, [candidate])
            payload = json.loads((root / "data" / "state" / "published_facts.json").read_text(encoding="utf-8"))

        saved = payload["facts"][0]
        self.assertEqual(saved["ticket_type"], "major_upcoming")
        self.assertEqual(saved["ticket_notability"]["tier"], "A")

    def test_published_facts_increment_ticket_publication_count_across_days(self) -> None:
        from news_digest.pipeline.history import update_published_facts

        event_day = (now_london().date() + timedelta(days=1)).isoformat()
        candidate = {
            "include": True,
            "fingerprint": "jason-ticket",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Jason Isbell and the 400 Unit — event {event_day}",
            "summary": f"Bridgewater Hall | event_date={event_day} 19:30",
            "source_label": "Ticketmaster Manchester Upcoming",
            "source_url": "https://example.test/jason",
            "event": {
                "is_event": True,
                "event_name": "Jason Isbell and the 400 Unit",
                "venue": "Bridgewater Hall",
                "date_start": event_day,
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            yesterday = (now_london().date() - timedelta(days=1)).isoformat()
            (state_dir / "published_facts.json").write_text(
                json.dumps(
                    {
                        "last_updated_london": yesterday,
                        "facts": [
                            {
                                "fingerprint": "jason-ticket",
                                "title": candidate["title"],
                                "category": "venues_tickets",
                                "primary_block": "ticket_radar",
                                "last_published_day_london": yesterday,
                                "first_published_day_london": yesterday,
                                "published_count": 2,
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            update_published_facts(root, [candidate])
            payload = json.loads((state_dir / "published_facts.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["facts"][0]["published_count"], 3)

    def test_tomorrow_ticket_repeat_is_allowed_as_reminder(self) -> None:
        event_day = (now_london().date() + timedelta(days=1)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": f"Jason Isbell and the 400 Unit — event {event_day} — public sale 2026-03-01 10:00",
            "summary": f"Bridgewater Hall | event_date={event_day} 19:30 | public_onsale=2026-03-01 10:00",
            "event": {
                "is_event": True,
                "event_name": "Jason Isbell and the 400 Unit",
                "venue": "Bridgewater Hall",
                "date_start": event_day,
            },
            "source_label": "Ticketmaster Manchester Upcoming",
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=2)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)
        lifecycle = lifecycle_repeat_review(candidate, previous)

        self.assertTrue(review["allow"], review)
        self.assertFalse(lifecycle["repeat"], lifecycle)
        self.assertEqual(review["reason"], "event_milestone_d1")

    def test_next_7_event_can_repeat_when_it_becomes_weekend_plan(self) -> None:
        event_day = (now_london().date() + timedelta(days=2)).isoformat()
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "South Manchester Food Festival",
            "summary": f"event_date={event_day} Annual food festival with BBQ, live music and family activities.",
            "event": {
                "is_event": True,
                "event_name": "South Manchester Food Festival",
                "venue": "Wythenshawe Park",
                "date_start": event_day,
            },
            "source_label": "South Manchester Food Festival",
        }
        previous = dict(candidate)
        previous["primary_block"] = "next_7_days"
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=3)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        review = calendar_repeat_review(candidate, previous)
        lifecycle = lifecycle_repeat_review(candidate, previous)

        self.assertTrue(review["allow"], review)
        self.assertEqual(review["reason"], "planning_item_reached_weekend")
        self.assertFalse(lifecycle["repeat"], lifecycle)

    def test_day_of_ticket_repeat_allowed_even_without_is_event_flag(self) -> None:
        from news_digest.pipeline.dedupe import _calendar_item_should_carry_over
        event_day = now_london().date().isoformat()
        candidate = {
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "title": f"Cammy Barnes — event {event_day} — public sale 2026-04-02 10:00",
            "summary": f"Manchester The Deaf Institute | event_date={event_day} 19:30 | public_onsale=2026-04-02 10:00",
            "event": {
                "event_name": "Cammy Barnes",
                "venue": "Manchester The Deaf Institute",
                "date_start": event_day,
            },
        }
        previous = {
            "title": candidate["title"],
            "summary": candidate["summary"],
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "last_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "event": dict(candidate["event"]),
        }
        previous["editorial_contract"] = build_editorial_contract(previous)

        self.assertTrue(_calendar_item_should_carry_over(candidate, previous))

    def test_food_opening_day_of_date_does_not_get_ticket_repeat_exception(self) -> None:
        from news_digest.pipeline.dedupe import _calendar_item_should_carry_over
        from news_digest.pipeline.repeat_policy import (
            validator_same_fingerprint_allow,
            visible_repeat_verdict,
        )

        event_day = now_london().date().isoformat()
        candidate = {
            "include": True,
            "fingerprint": "food-openings-manchesters-finest-orme",
            "primary_block": "openings",
            "category": "food_openings",
            "title": "Michelin-listed Orme returns to Manchester",
            "summary": f"Orme relaunches in Manchester on {event_day}.",
            "source_label": "Manchester's Finest",
            "source_url": "https://www.manchestersfinest.com/eating-and-drinking/restaurants/orme/",
            "event": {
                "event_name": "Orme restaurant relaunch",
                "venue": "Manchester",
                "date_start": event_day,
            },
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=1)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        self.assertFalse(_calendar_item_should_carry_over(candidate, previous))
        self.assertFalse(validator_same_fingerprint_allow(candidate).allow)
        verdict = visible_repeat_verdict(candidate, previous)
        self.assertFalse(verdict.allow, verdict)
        self.assertEqual(verdict.matched_by, "fingerprint")
        self.assertIn(
            verdict.reason,
            {"topic_lifecycle_rehash:opening:none", "exact_fingerprint_already_published"},
        )

    def test_day_of_ticket_visible_repeat_policy_still_allows_exact_repeat(self) -> None:
        from news_digest.pipeline.repeat_policy import visible_repeat_verdict

        event_day = now_london().date().isoformat()
        candidate = {
            "include": True,
            "fingerprint": "ticket-cammy-barnes",
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "title": f"Cammy Barnes — event {event_day} — public sale 2026-04-02 10:00",
            "summary": f"Manchester The Deaf Institute | event_date={event_day} 19:30 | public_onsale=2026-04-02 10:00",
            "source_label": "Ticketmaster Manchester Upcoming",
            "source_url": "https://example.test/tickets/cammy-barnes",
            "event": {
                "event_name": "Cammy Barnes",
                "venue": "Manchester The Deaf Institute",
                "date_start": event_day,
            },
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=7)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        verdict = visible_repeat_verdict(candidate, previous)

        self.assertTrue(verdict.allow, verdict)
        self.assertEqual(verdict.repeat_class, "calendar")
        self.assertEqual(verdict.reason, "event_milestone_d0")

    def test_undated_ticket_visible_repeat_policy_does_not_bypass_calendar_review(self) -> None:
        from news_digest.pipeline.repeat_policy import visible_repeat_verdict

        candidate = {
            "include": True,
            "fingerprint": "ticket-undated-repeat",
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "title": "Small venue announces another listing",
            "summary": "A ticket listing without a usable event date.",
            "source_label": "Ticketmaster Manchester Upcoming",
            "source_url": "https://example.test/tickets/undated",
            "event": {
                "event_name": "Small venue listing",
                "venue": "Manchester",
            },
        }
        previous = dict(candidate)
        previous["last_published_day_london"] = (now_london().date() - timedelta(days=1)).isoformat()
        previous["first_published_day_london"] = previous["last_published_day_london"]
        previous["editorial_contract"] = build_editorial_contract(previous)

        verdict = visible_repeat_verdict(candidate, previous)

        self.assertFalse(verdict.allow, verdict)
        self.assertEqual(verdict.repeat_class, "calendar")
        self.assertIn(
            verdict.reason,
            {"calendar_review_not_applicable", "same_calendar_item_without_new_reader_moment"},
        )

    def test_day_of_ticket_repeat_allowed_across_sources(self) -> None:
        event_day = now_london().date().isoformat()
        previous = {
            "fingerprint": "cammy-ticketmaster",
            "title": f"Cammy Barnes — event {event_day} — public sale 2026-04-02 10:00",
            "summary": f"Manchester The Deaf Institute | event_date={event_day} 19:30 | public_onsale=2026-04-02 10:00",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "source_label": "Ticketmaster Manchester Upcoming",
            "event": {
                "event_name": "Cammy Barnes",
                "venue": "Manchester The Deaf Institute",
                "date_start": event_day,
            },
            "last_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
        }
        previous["editorial_contract"] = build_editorial_contract(previous)
        previous["repeat_story_key"] = previous["editorial_contract"]["topic_key"]
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "reason": "Candidate selected.",
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": previous["title"],
            "summary": previous["summary"],
            "event": previous["event"],
            "source_label": "Venue Direct",
            "source_url": "https://example.test/venue-direct/cammy-barnes",
            "published_at": now_london().isoformat(),
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "published_facts.json").write_text(
                json.dumps({"last_updated_london": today_london(), "facts": [previous]}, ensure_ascii=False),
                encoding="utf-8",
            )
            (state_dir / "candidates.json").write_text(
                json.dumps({"pipeline_run_id": "t", "run_date_london": today_london(), "candidates": [candidate]}, ensure_ascii=False),
                encoding="utf-8",
            )

            dedupe_candidates(root)
            out = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))

        updated = out["candidates"][0]
        self.assertTrue(updated["include"], updated)
        self.assertNotEqual(updated["dedupe_decision"], "drop")

    def test_recurring_market_repeat_allowed_once_per_week(self) -> None:
        from news_digest.pipeline.dedupe import _calendar_item_should_carry_over
        candidate = {
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "title": "Northern Quarter Makers Market",
            "summary": "A makers market in Manchester every Sunday from 11:00 to 17:00.",
            "event": {
                "event_name": "Northern Quarter Makers Market",
                "venue": "Oak Street, Manchester",
                "is_recurring": True,
            },
        }
        previous = {
            "title": candidate["title"],
            "summary": candidate["summary"],
            "category": candidate["category"],
            "primary_block": candidate["primary_block"],
            "last_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "event": dict(candidate["event"]),
        }
        previous["editorial_contract"] = build_editorial_contract(previous)

        self.assertTrue(_calendar_item_should_carry_over(candidate, previous))

    def test_recurring_market_repeat_not_allowed_again_next_day(self) -> None:
        from news_digest.pipeline.dedupe import _calendar_item_should_carry_over
        candidate = {
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "title": "Northern Quarter Makers Market",
            "summary": "A makers market in Manchester every Sunday from 11:00 to 17:00.",
            "event": {
                "event_name": "Northern Quarter Makers Market",
                "venue": "Oak Street, Manchester",
                "is_recurring": True,
            },
        }
        previous = {
            "title": candidate["title"],
            "summary": candidate["summary"],
            "category": candidate["category"],
            "primary_block": candidate["primary_block"],
            "last_published_day_london": (now_london().date() - timedelta(days=1)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "event": dict(candidate["event"]),
        }
        previous["editorial_contract"] = build_editorial_contract(previous)

        self.assertFalse(_calendar_item_should_carry_over(candidate, previous))

    def test_historical_ira_bomb_rehash_suppressed_unless_new_phase(self) -> None:
        # Owner 2026-06-13: the 1996 IRA bomb retrospective resurfaced as new
        # news. A generic-"news" rehash of a curated historical subject must be
        # suppressed; only a real new phase (inquest opens) re-publishes.
        previous = {
            "title": "Remembering the Manchester IRA bombing",
            "summary": "The 1996 Arndale bombing by the IRA.",
            "primary_block": "last_24h",
            "category": "media_layer",
        }
        previous["editorial_contract"] = build_editorial_contract(previous)
        rehash = {
            "title": "The Mancunian Way: No longer active",
            "summary": "Looking back at the 1996 IRA bomb that devastated the Arndale and Corporation Street.",
            "primary_block": "last_24h",
            "category": "media_layer",
        }
        new_phase = {
            "title": "Inquest opens into 1996 Manchester IRA bomb",
            "summary": "A new inquest has been opened into the 1996 Arndale bombing.",
            "primary_block": "last_24h",
            "category": "media_layer",
        }
        self.assertTrue(lifecycle_repeat_review(rehash, previous)["repeat"])
        self.assertFalse(lifecycle_repeat_review(new_phase, previous)["repeat"])

    def test_new_phase_named_fact_gates_announcement_rehash(self) -> None:
        # W7: the named-fact gate behind the new_phase repeat. An "announced /
        # confirmed" rehash (Örme) carries no concrete development → "" → the
        # lifecycle review suppresses it. A strong development word (opens) or an
        # event fact that changed vs the last publication is a real reader moment.
        from news_digest.pipeline.editorial_contracts import _new_phase_named_fact

        previous = {"title": "Örme bakery announced for Ancoats", "summary": "A new bakery, Örme, has been announced for Ancoats."}
        rehash = {"title": "Örme bakery announced for Ancoats", "summary": "The Örme bakery has been announced and confirmed for Ancoats."}
        opens = {"title": "Örme bakery opens in Ancoats", "summary": "Örme bakery opens its doors in Ancoats this week."}
        date_changed = {
            "title": "Örme bakery announced for Ancoats",
            "summary": "Örme bakery announced.",
            "event": {"date": "2026-07-10"},
        }
        previous_with_date = {"event": {"date": "2026-07-02"}}

        self.assertEqual(_new_phase_named_fact(rehash, previous), "")
        self.assertEqual(_new_phase_named_fact(opens, previous), "strong_phase_development")
        self.assertTrue(_new_phase_named_fact(date_changed, previous_with_date).startswith("event_"))

    def test_undated_event_like_market_repeat_is_not_carried_daily(self) -> None:
        from news_digest.pipeline.dedupe import _calendar_item_should_carry_over
        candidate = {
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "title": "Northern Quarter Makers Market, Manchester - Pedddle",
            "summary": "",
            "event": {
                "event_name": "Northern Quarter Makers Market, Manchester - Pedddle",
                "borough": "Manchester",
                "is_event": False,
            },
        }
        previous = {
            "title": candidate["title"],
            "summary": candidate["summary"],
            "category": candidate["category"],
            "primary_block": candidate["primary_block"],
            "last_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "first_published_day_london": (now_london().date() - timedelta(days=7)).isoformat(),
            "event": dict(candidate["event"]),
        }
        previous["editorial_contract"] = build_editorial_contract(previous)

        self.assertFalse(_calendar_item_should_carry_over(candidate, previous))

    def test_generic_update_marker_does_not_create_follow_up(self) -> None:
        from news_digest.pipeline.dedupe import _classify_change_type
        previous = {
            "fingerprint": "stockport-cabinet-old",
            "title": "Stockport council confirms cabinet appointments",
            "summary": "Mark Roberts remains council leader.",
        }
        candidate = {
            "dedupe_decision": "drop",
            "primary_block": "city_watch",
            "title": "Обновление: появилось обновление по кабинету Stockport Council",
            "summary": "Mark Roberts remains council leader and Gillian Julian remains deputy.",
            "lead": "",
            "evidence_text": "The council confirmed the same cabinet line-up.",
        }

        change_type = _classify_change_type(candidate, None, [previous], previous)

        self.assertEqual(change_type, "same_story_rehash")

    def test_same_title_without_phase_is_not_new_facts(self) -> None:
        from news_digest.pipeline.dedupe import _classify_change_type
        previous = {
            "fingerprint": "abba-old",
            "title": "ABBA themed venue next to Manchester City set to be approved",
            "summary": "The venue is set to be approved near the stadium.",
        }
        candidate = {
            "dedupe_decision": "drop",
            "primary_block": "city_watch",
            "title": previous["title"],
            "summary": "The venue could host 600 guests and is recommended for approval.",
            "lead": "",
            "evidence_text": "The same proposal has more detail but no final approval.",
        }

        change_type = _classify_change_type(candidate, None, [previous], previous)

        self.assertEqual(change_type, "same_story_rehash")

    def test_concrete_sentencing_phase_still_creates_follow_up(self) -> None:
        from news_digest.pipeline.dedupe import _classify_change_type
        previous = {
            "fingerprint": "case-old",
            "title": "Man charged after Little Hulton assault",
            "summary": "A man was charged after a 2003 assault.",
        }
        candidate = {
            "dedupe_decision": "drop",
            "primary_block": "last_24h",
            "title": "Man sentenced to 24 years after Little Hulton assault",
            "summary": "Paul Quinn has been sentenced to 24 years.",
            "lead": "",
            "evidence_text": "Paul Quinn was sentenced at court.",
        }

        change_type = _classify_change_type(candidate, None, [previous], previous)

        self.assertEqual(change_type, "follow_up")

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

    def test_events_prompt_is_v5_with_three_templates(self) -> None:
        """The events prompt v5 must mention all three template buckets
        (one-off / festival / recurring) so the LLM picks one explicitly.
        """
        from news_digest.pipeline import llm_rewrite as _lr
        from news_digest.pipeline.prompts_meta import by_name
        events_meta = by_name().get("events")
        self.assertIsNotNone(events_meta)
        self.assertEqual(events_meta.version, "v5", f"events version not bumped to v5: {events_meta}")
        prompt = _lr.PROMPT_EVENTS
        self.assertIn("ТОЧЕЧНОЕ", prompt)
        self.assertIn("ФЕСТИВАЛЬ", prompt)
        self.assertIn("ПОВТОРЯЮЩЕЕСЯ", prompt)

    def test_diaspora_events_prompt_is_v4_with_recurring_template(self) -> None:
        from news_digest.pipeline import llm_rewrite as _lr
        from news_digest.pipeline.prompts_meta import by_name
        meta = by_name().get("diaspora_events")
        self.assertIsNotNone(meta)
        self.assertEqual(meta.version, "v4", f"diaspora events version not bumped: {meta}")
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

    def test_weather_high_rain_uses_plain_umbrella_wording(self) -> None:
        line = _weather_draft_line(11, 15, 90, "", "Met Office")

        self.assertIn("очень вероятен дождь, риск до 90%", line)
        self.assertIn("зонт или капюшон", line)
        self.assertNotIn("защит", line.lower())

    def test_number_tokens_normalise_time_and_money_formats(self) -> None:
        tokens = _number_tokens("from 9.55am, £50m, 2,200 miles and 07:45")

        self.assertIn("9", tokens)
        self.assertIn("55", tokens)
        self.assertIn("50", tokens)
        self.assertIn("50000000", tokens)
        self.assertIn("2200", tokens)
        self.assertIn("7", tokens)
        self.assertIn("45", tokens)

    def test_fresh_numeric_guard_strips_unsupported_phrase_not_drop(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Woman dies in hospital after medical episode at wheel before Wythenshawe crash",
            "summary": (
                "Wythenshawe: a woman died in hospital after a suspected medical episode "
                "at the wheel before a crash on Southmoor Road. Police continue to investigate."
            ),
            "evidence_text": (
                "A woman suffered a suspected medical episode at the wheel before a crash "
                "on Southmoor Road in Wythenshawe. She later died in hospital. Police continue to investigate."
            ),
            "source_label": "MEN",
        }
        bad_line = (
            "• Wythenshawe: женщина в возрасте 50 лет скончалась в больнице после медицинского "
            "инцидента за рулём, приведшего к аварии на Southmoor Road. Инцидент произошёл "
            "около 9:55 утра, полиция продолжает расследование."
        )

        repaired, reasons = _strip_unsupported_number_phrases(candidate, bad_line)

        self.assertTrue(reasons)
        self.assertNotIn("50", repaired)
        self.assertNotIn("9:55", repaired)
        self.assertFalse(_draft_line_quality_errors(candidate, repaired))

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

    def test_strong_fresh_news_survives_global_rewrite_board_cap(self) -> None:
        fresh = [
            {
                "include": True,
                "fingerprint": f"fresh-{idx}",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": f"Fresh court update {idx}",
                "summary": "Police charged a man after a crash in Manchester.",
                "source_label": "MEN",
                "editorial_contract": {"publish_tier": "strong"},
                "publish_tier": "strong",
                "reader_value_score": 80,
            }
            for idx in range(12)
        ]
        crowded = [
            {
                "include": True,
                "fingerprint": f"city-{idx}",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": f"City watch item {idx}",
                "summary": "Council update in Manchester.",
                "source_label": "MEN",
                "reader_value_score": 50,
            }
            for idx in range(80)
        ]

        selected, report = _apply_rewrite_shortlist(fresh + crowded, fresh + crowded)
        selected_fresh = [c for c in selected if str(c.get("primary_block")) == "last_24h"]

        self.assertEqual(len(selected_fresh), 12)
        self.assertGreater(report["board_overflow"], 0)
        self.assertFalse(any(c.get("rewrite_shortlist_status") == "backup_board_cap" for c in fresh))

    def test_today_focus_eligibility_rejects_soft_opening(self) -> None:
        candidate = {
            "include": True,
            "category": "food_openings",
            "primary_block": "today_focus",
            "title": "Irish deli opens in Altrincham",
            "summary": "A new deli has opened with sandwiches and coffee.",
            "source_label": "Altrincham Today",
            "editorial_contract": {
                "story_type": "opening",
                "event_shape": "none",
                "publish_tier": "optional",
            },
        }

        self.assertFalse(_today_focus_candidate_is_eligible(candidate))

    def test_today_focus_recovery_line_handles_oldham_pub_planning(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "The 'rare' Oldham pub that could be bought by the council - and then demolished",
            "summary": "Oldham £1m plans to buy and then demolish a rare pub; a local MP called for pubs to be protected.",
            "source_label": "MEN",
            "editorial_contract": {
                "story_type": "planning",
                "event_shape": "none",
                "publish_tier": "strong",
                "story_frame": {"why_now": "new_today"},
            },
        }

        line = _today_focus_recovery_line(candidate)

        self.assertIn("Oldham", line)
        self.assertIn("£1m", line)
        self.assertEqual(_draft_line_quality_errors(candidate, line), [])

    def test_today_focus_recovery_line_handles_bury_school_safeguarding(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Woman arrested at Bury school on suspicion of child sex offences",
            "summary": "An investigation continues after police arrest a woman at St Gabriel's RC High School in Bury on suspicion of child sex offences and safeguarding concerns.",
            "source_label": "BBC Manchester",
            "editorial_contract": {
                "story_type": "service_accountability",
                "event_shape": "none",
                "publish_tier": "strong",
                "story_frame": {"why_now": "new_today"},
            },
        }

        line = _today_focus_recovery_line(candidate)

        self.assertIn("Bury", line)
        self.assertIn("St Gabriel", line)
        self.assertEqual(_draft_line_quality_errors(candidate, line), [])

    def test_fresh_final_board_suppresses_cross_source_same_story(self) -> None:
        about_candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "fingerprint": "about-ira",
            "title": "Police investigation into 1996 Manchester bombing is no longer active",
            "summary": "Police say the IRA bombing investigation is no longer active unless new evidence appears.",
            "source_label": "About Manchester News",
            "source_url": "https://aboutmanchester.co.uk/police-investigation-into-1996-manchester-bombing-is-no-longer-active",
        }
        bbc_candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "fingerprint": "bbc-ira",
            "title": "Manchester IRA bomb inquiry no longer active, police say",
            "summary": "The 1996 IRA bomb investigation has exhausted current lines of inquiry.",
            "source_label": "BBC Manchester",
            "source_url": "https://bbc.example/ira-bomb-inquiry",
        }
        rows = [
            _SectionRow(
                section="Свежие новости",
                line="• Манчестер: расследование взрыва бомбы IRA 1996 года больше не активно.",
                source="About Manchester News",
                score=80,
                fingerprint="about-ira",
                title=str(about_candidate["title"]),
                candidate=about_candidate,
            ),
            _SectionRow(
                section="Свежие новости",
                line="• Манчестер: полиция завершила расследование взрыва 1996 года, если не появятся новые доказательства.",
                source="BBC Manchester",
                score=82,
                fingerprint="bbc-ira",
                title=str(bbc_candidate["title"]),
                candidate=bbc_candidate,
            ),
        ]

        kept, suppressed = _apply_fresh_semantic_duplicate_pass(rows)

        self.assertEqual(len(kept), 1)
        self.assertEqual(len(suppressed), 1)
        self.assertEqual(suppressed[0]["reason"], "fresh_semantic_duplicate")
        self.assertEqual(about_candidate.get("writer_suppressed_from_top_news"), "fresh_semantic_duplicate")

    def test_final_rendered_line_gets_missing_source_anchor_from_candidate(self) -> None:
        candidate_by_fp = {
            "bury-stop": {
                "source_url": "https://aboutmanchester.co.uk/police-seize-knife-drugs-and-cash-after-stopping-suspicious-car-in-bury",
                "source_label": "About Manchester News",
            }
        }

        line = _ensure_source_anchor_for_rendered_line(
            "• Bury: полиция арестовала женщину после остановки подозрительного автомобиля.",
            "bury-stop",
            "",
            candidate_by_fp,
        )

        self.assertIn('<a href="https://aboutmanchester.co.uk/police-seize-knife-drugs-and-cash-after-stopping-suspicious-car-in-bury">About Manchester News</a>', line)

    def test_rendered_recovered_candidate_is_not_reported_as_dropped(self) -> None:
        dropped = [
            {"fingerprint": "bury-stop", "reasons": ["draft_line contains number(s) not present in evidence: 10000."]},
            {"fingerprint": "other", "reasons": ["Missing draft_line."]},
        ]
        counts = {
            "dropped_low_quality": 1,
            "dropped_missing_draft_line": 1,
            "dropped_ticket_not_selected": 0,
            "dropped_english_passthrough": 0,
            "held_for_editorial_quality": 0,
        }

        remaining, reconciled = _reconcile_rendered_dropped_candidates(dropped, counts, {"bury-stop"})

        self.assertEqual([item["fingerprint"] for item in remaining], ["other"])
        self.assertEqual([item["fingerprint"] for item in reconciled], ["bury-stop"])
        self.assertEqual(counts["dropped_low_quality"], 0)
        self.assertEqual(counts["dropped_missing_draft_line"], 1)

    def test_fresh_duplicate_prefers_more_complete_fact_frame_on_same_source_rank(self) -> None:
        vague = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Police incident in Wigan",
            "summary": "Police are investigating an incident.",
            "story_frame": {"what_happened": "", "where_exact": "Wigan"},
        }
        complete = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Man stabbed in Wigan",
            "summary": "A man was stabbed on Avon Road at 20:30 and police appealed for witnesses.",
            "story_frame": {
                "what_happened": "A man was stabbed",
                "where_exact": "Avon Road, Wigan",
                "when": "20:30",
                "who_affected": "a man with serious injuries",
                "why_now": "police appealed for witnesses",
            },
        }

        self.assertFalse(_prefer_dedupe_candidate(vague, complete, 50, 50))
        self.assertTrue(_prefer_dedupe_candidate(complete, vague, 50, 50))

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

    def test_dated_bookable_activity_is_kept_by_validator(self) -> None:
        # E2 (2026-06-30): a bookable_activity with a concrete, trustworthy date
        # (Ai Weiwei, Crossroad, a dated makers-market) is a real listing — the
        # validator must keep it, not drop it as filler. Regresses the
        # fall-through that still rejected dated ones despite the E2 guard.
        from news_digest.pipeline.candidate_validator import _exclude_by_editorial_contract
        dated = {
            "include": True,
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "title": "Crossroad - The Price of Choice",
            "summary": "DesignMyNight bookable experience in Manchester. Tickets from £20.",
            "source_label": "DesignMyNight",
            "source_url": "https://example.test/crossroad",
            "event": {"is_event": True, "date_start": "2026-07-03", "date_confidence": "high", "venue": "Aviva Studios"},
        }
        self.assertEqual(build_editorial_contract(dated)["event_shape"], "bookable_activity")
        self.assertFalse(_exclude_by_editorial_contract(dated))
        self.assertTrue(dated["include"])

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
        self.assertEqual(by_name["Manchester City Men"].url, "https://www.mancity.com/news/mens")
        self.assertTrue(_is_allowed_source_link(
            by_name["Manchester City Men"],
            "https://www.mancity.com/news/mens/reijnders-khusanov-cherki-world-cup-warm-ups",
            "Reijnders, Khusanov and Cherki all feature in latest World Cup warm-ups",
            "",
        ))
        self.assertEqual(
            by_name["BBC Sport Manchester United"].url,
            "https://feeds.bbci.co.uk/sport/football/teams/manchester-united/rss.xml",
        )
        self.assertEqual(
            by_name["MEN Manchester United"].url,
            "https://www.manchestereveningnews.co.uk/all-about/manchester-united-fc?service=rss",
        )
        self.assertEqual(
            by_name["MEN Manchester City"].url,
            "https://www.manchestereveningnews.co.uk/all-about/manchester-city-fc?service=rss",
        )
        self.assertEqual(
            by_name["Guardian Manchester United"].url,
            "https://www.theguardian.com/football/manchesterunited/rss",
        )
        self.assertEqual(
            by_name["Guardian Manchester City"].url,
            "https://www.theguardian.com/football/manchestercity/rss",
        )
        self.assertFalse(_is_allowed_source_link(
            by_name["Manchester United"],
            "https://www.manutd.com/en/news/influencers-debate-bruno-fernandes-and-cristiano-ronaldo-in-portugal-world-cup-squad",
            "How Fernandes is ruling the roost alongside Ronaldo",
            "",
        ))
        self.assertFalse(_is_allowed_source_link(
            by_name["Manchester City Men"],
            "https://www.mancity.com/news/mens/city-at-the-2026-world-cup-quiz-63916764",
            "City at the 2026 FIFA World Cup quiz",
            "",
        ))
        self.assertTrue(_is_allowed_source_link(
            by_name["Manchester City Men"],
            "https://www.mancity.com/news/mens/reijnders-khusanov-cherki-world-cup-warm-ups",
            "Reijnders, Khusanov and Cherki all feature in latest World Cup warm-ups",
            "",
        ))
        self.assertTrue(_is_allowed_source_link(
            by_name["MEN Manchester United"],
            "https://www.manchestereveningnews.co.uk/sport/football/transfer-news/manchester-united-transfer-news-live-34125000",
            "Manchester United transfer news live",
            "",
        ))
        self.assertIn("Secret Manchester May Guide", by_name)
        self.assertIn("Secret Manchester Gigs", by_name)
        self.assertNotIn("Secret Manchester Weekend Guide", by_name)
        self.assertNotIn("Manchester Flower Festival CityCo News", by_name)
        self.assertEqual(by_name["Manchester United"].url, "https://www.manutd.com/en/news")
        self.assertNotIn("Prolific North", by_name)
        self.assertNotIn("Sofar Manchester Bank Holiday", by_name)

    def test_manchester_united_article_cards_are_extracted(self) -> None:
        source = SourceDef(
            name="Manchester United",
            report_category="football",
            candidate_category="football",
            url="https://www.manutd.com/en/news",
            primary_block="football",
            allowed_hosts=("manutd.com",),
        )
        html = """
        <article class="articleCard" data-testid="article-card">
          <a data-testid="article-card__floating-link"
             href="/en/news/man-utd-team-news-injury-update-before-world-cup-fixture-2026">
            <span>Man Utd team news and injury update before World Cup fixture</span>
          </a>
          <div data-testid="publish-date"><span>2 days ago</span></div>
          <h5 data-testid="heading">Man Utd team news and injury update before World Cup fixture</h5>
        </article>
        """
        candidates = _extract_source_candidates(source, html)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["title"], "Man Utd team news and injury update before World Cup fixture")
        self.assertEqual(
            candidates[0]["source_url"],
            "https://manutd.com/en/news/man-utd-team-news-injury-update-before-world-cup-fixture-2026",
        )

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

    def test_fresh_service_accountability_is_strong_news(self) -> None:
        contract = build_editorial_contract(
            {
                "include": True,
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "Hazel Grove homecare service rated inadequate by CQC",
                "summary": (
                    "CQC inspectors found problems with medication management "
                    "and staff training at Elite Homecare in Stockport."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/cqc-homecare",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertEqual(contract["story_type"], "service_accountability")
        self.assertEqual(contract["publish_tier"], "strong")

    def test_fresh_public_safety_after_incident_is_strong_news(self) -> None:
        contract = build_editorial_contract(
            {
                "include": True,
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "Fallowfield street evacuated after suspicious item found",
                "summary": (
                    "Police set up a cordon on Abram Close and evacuated 20 homes "
                    "while bomb disposal officers examined a suspicious item."
                ),
                "source_label": "MEN",
                "source_url": "https://example.test/fallowfield-cordon",
                "published_at": now_london().isoformat(),
            }
        )
        self.assertEqual(contract["story_type"], "public_safety_after_incident")
        self.assertEqual(contract["publish_tier"], "strong")

    def test_court_crime_story_does_not_get_planning_repeat_key(self) -> None:
        contract = build_editorial_contract(
            {
                "include": True,
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "She reported him - the police found more than they bargained for",
                "summary": (
                    "Ryan Morgan was caught with hundreds of indecent images of children "
                    "and was sentenced at Minshull Street Crown Court."
                ),
                "source_label": "MEN News Sitemap",
                "source_url": "https://example.test/reported-ex-police-over-photos-34077770",
                "published_at": now_london().isoformat(),
            }
        )

        self.assertEqual(contract["story_type"], "incident")
        self.assertTrue(contract["topic_key"].startswith("incident:"), contract["topic_key"])
        self.assertNotIn("planning:", contract["topic_key"])

    def test_fresh_priority_prefers_public_safety_over_charity_soft_item(self) -> None:
        safety = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Timperley knife attack sees two injured and cordon put in place outside Iceland",
            "summary": "Two people were injured after a knife attack and police set up a cordon.",
            "source_label": "MEN",
            "editorial_contract": {
                "story_type": "public_safety_after_incident",
                "publish_tier": "strong",
            },
        }
        charity = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Kevin Sinfield reveals final MND ultramarathon challenge",
            "summary": "Kevin Sinfield announced a final charity ultramarathon fundraising challenge.",
            "source_label": "BBC Manchester",
            "editorial_contract": {
                "story_type": "incident",
                "publish_tier": "strong",
            },
        }

        self.assertGreater(
            _section_priority_score(safety, "Свежие новости", "• test."),
            _section_priority_score(charity, "Свежие новости", "• test."),
        )

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

    def test_today_focus_requires_reader_action_not_just_serious_topic(self) -> None:
        anniversary = {
            "include": True,
            "fingerprint": "ira-anniversary",
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "The 99 minutes that changed Manchester forever",
            "summary": "A new article remembers the 1996 IRA bomb anniversary and how it changed the city.",
            "lead": "The anniversary article looks back at the warning before the blast.",
            "evidence_text": "The story is a retrospective and tribute.",
            "source_label": "MEN",
        }
        poll = {
            "include": True,
            "fingerprint": "national-social-poll",
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "POLL: Is a UK social media ban for under-16s a good idea? Have your say",
            "summary": "The Prime Minister is considering a national social media ban for children.",
            "lead": "Readers are asked to vote in a poll.",
            "evidence_text": "The story is a national poll about TikTok and Instagram.",
            "source_label": "MEN",
        }
        cqc = {
            "include": True,
            "fingerprint": "cqc-warning",
            "category": "media_layer",
            "primary_block": "today_focus",
            "title": "Greater Manchester's latest CQC reports including warning notice on nursing home",
            "summary": "A care home requires improvement after CQC inspectors found fire safety and safeguarding problems.",
            "lead": "CQC inspectors issued a warning notice after a care-home inspection.",
            "evidence_text": "Residents and families should know about the inspection and warning notice.",
            "source_label": "MEN",
        }
        empty_homes = {
            "include": True,
            "fingerprint": "empty-homes-report",
            "category": "council",
            "primary_block": "city_watch",
            "title": "The Council wants to bring hundreds of empty homes back into use",
            "summary": "Manchester residents can report long-term empty homes online or by email.",
            "lead": "Manchester City Council is asking residents to report empty homes.",
            "evidence_text": "The strategy asks residents to spot and report empty homes.",
            "source_label": "Manchester Council",
        }
        old_conviction = {
            "include": True,
            "fingerprint": "old-conviction",
            "category": "media_layer",
            "primary_block": "today_focus",
            "title": "Murderer serving life after city centre attack speaks from prison",
            "summary": "The article revisits an old conviction and sentence.",
            "lead": "The offender is serving life for a past murder.",
            "evidence_text": "No current hearing, deadline or service change is attached.",
            "source_label": "MEN",
        }

        self.assertFalse(_today_focus_candidate_is_eligible(anniversary))
        self.assertFalse(_today_focus_candidate_is_eligible(poll))
        self.assertFalse(_today_focus_candidate_is_eligible(old_conviction))
        self.assertTrue(_today_focus_candidate_is_eligible(cqc))
        self.assertTrue(_today_focus_candidate_is_eligible(empty_homes))

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

    def test_city_news_prompt_is_v7_with_lead_first_structure(self) -> None:
        """The city-news prompt v7 must explicitly require lead-first
        structure (fact first, details next, what-next last) and ban
        quote/narrative leads.
        """
        from news_digest.pipeline import llm_rewrite as _lr
        from news_digest.pipeline.prompts_meta import by_name
        meta = by_name().get("city_news")
        self.assertIsNotNone(meta)
        self.assertEqual(meta.version, "v7", f"city_news not bumped to v7: {meta}")
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
