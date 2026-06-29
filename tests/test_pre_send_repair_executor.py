import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from news_digest.pipeline.collector.extract import _enrich_item
from news_digest.pipeline.collector.sources import ExtractedItem, SourceDef
from news_digest.pipeline.editor import _editor_item_fact_lock_errors
from news_digest.pipeline.pre_send_quality_judge import _apply_repair_executor
from news_digest.pipeline.release import _write_final_selection_report


class PreSendRepairExecutorTest(unittest.TestCase):
    def test_editor_fact_lock_allows_source_fact_but_rejects_new_date(self) -> None:
        item = {
            "line": "• HOME показывает выставку на 28 June. <a href=\"https://home.test/event\">HOME</a>",
            "evidence": {
                "title": "Exhibition at HOME",
                "summary": "The exhibition runs on 28 June at HOME.",
                "source_label": "HOME",
            },
        }

        self.assertFalse(
            _editor_item_fact_lock_errors(
                item,
                "• HOME показывает выставку на 28 June. <a href=\"https://home.test/event\">HOME</a>",
            )
        )
        self.assertIn(
            "1 july",
            _editor_item_fact_lock_errors(
                item,
                "• HOME показывает выставку на 1 July. <a href=\"https://home.test/event\">HOME</a>",
            ),
        )

    def test_pre_send_repair_rejects_hallucinated_patch_and_uses_clean_reserve(self) -> None:
        digest_html = (
            "<b>Greater Manchester Brief — 2026-06-29, 08:00</b>\n\n"
            "<b>Свежие новости</b>\n"
            "• Битая строка без фактов. <a href=\"https://example.test/bad\">MEN</a>\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "fingerprint": "reserve-1",
                                "primary_block": "last_24h",
                                "public_reserve": True,
                                "backup_pool_only": False,
                                "draft_line": "• Чистая резервная новость про город.",
                                "source_url": "https://example.test/reserve",
                                "source_label": "BBC Manchester",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            repaired, report = _apply_repair_executor(
                project_root=root,
                digest_html=digest_html,
                actions=[
                    {
                        "line_index": 1,
                        "section": "Свежие новости",
                        "action": "patch",
                        "replacement_text": "• AO Arena подтверждает новую дату 1 July.",
                    }
                ],
                critical_errors=[],
                deterministic_post_check={"errors": []},
                dry_run=False,
            )

        self.assertIn("Чистая резервная новость", repaired)
        self.assertNotIn("AO Arena", repaired)
        self.assertEqual(report["fact_lock_rejected"], 1)
        self.assertEqual(report["reserve_replacement_used"], 1)

    @mock.patch("news_digest.pipeline.collector.extract._fetch_text")
    def test_deep_event_enrichment_fetches_child_page_facts_for_home(self, fetch_text: mock.Mock) -> None:
        source = SourceDef(
            name="HOME",
            report_category="culture_weekly",
            candidate_category="culture_weekly",
            url="https://homemcr.org/whats-on/",
            primary_block="weekend_activities",
            source_type="html",
            allowed_hosts=("homemcr.org",),
        )
        item = ExtractedItem(
            title="Film night",
            url="https://homemcr.org/event/film-night/",
            summary="Film night listing",
            enrichment_status="ok_page_event",
        )
        fetch_text.return_value = """
        <html><head>
          <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"Event","name":"Film night",
           "startDate":"2026-07-03T19:30:00+01:00",
           "location":{"@type":"Place","name":"HOME"},
           "organizer":{"@type":"Organization","name":"HOME Events"},
           "offers":{"price":"12","priceCurrency":"GBP","url":"https://homemcr.org/book/film-night"}}
          </script>
          <meta name="description" content="Film night at HOME with booking details.">
        </head><body><main><p>Film night at HOME with tickets from £12.</p></main></body></html>
        """

        enriched = _enrich_item(source, item)

        self.assertEqual(enriched.title, "Film night")
        self.assertEqual(enriched.structured_event_hint["venue"], "HOME")
        self.assertEqual(enriched.structured_event_hint["organizer"], "HOME Events")
        self.assertEqual(enriched.structured_event_hint["date_start"][:10], "2026-07-03")
        self.assertEqual(enriched.structured_event_hint["price"], "£12")
        self.assertIn("book", enriched.structured_event_hint["booking_url"])

    def test_final_selection_report_shows_top_visible_and_lost_by_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            candidates = [
                {
                    "fingerprint": "visible",
                    "title": "Visible high score",
                    "source_url": "https://example.test/visible",
                    "source_label": "BBC",
                    "primary_block": "last_24h",
                    "include": True,
                    "section_board_score": 95,
                },
                {
                    "fingerprint": "lost",
                    "title": "Lost lower score",
                    "source_url": "https://example.test/lost",
                    "source_label": "MEN",
                    "primary_block": "last_24h",
                    "include": True,
                    "section_board_score": 40,
                },
            ]
            summary = _write_final_selection_report(
                state_dir=state_dir,
                current_day_london="2026-06-29",
                candidates_report={"candidates": candidates},
                writer_report={"dropped_candidates": [{"fingerprint": "lost", "reasons": ["no clean draft_line"]}]},
                rendered_fingerprints=set(),
                dedupe_memory={},
                final_html='<b>Свежие новости</b>\n• Visible <a href="https://example.test/visible">BBC</a>',
            )
            payload = json.loads((state_dir / "final_selection_report.json").read_text(encoding="utf-8"))

        section = payload["sections"]["Свежие новости"]
        self.assertEqual(summary["section_count"], 1)
        self.assertGreaterEqual(section["top"][0]["score"], section["top"][1]["score"])
        self.assertEqual(section["visible"][0]["final_status"], "visible_after_repair")
        self.assertEqual(section["lost_or_rejected"][0]["final_status"], "writer_dropped")


if __name__ == "__main__":
    unittest.main()
