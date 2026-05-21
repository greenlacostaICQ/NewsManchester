from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.collector.routing import _adjust_ticket_radar_block
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


if __name__ == "__main__":
    unittest.main()
