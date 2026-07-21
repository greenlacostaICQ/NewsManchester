import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from news_digest.pipeline.collector.extract import _enrich_item
from news_digest.pipeline.collector.sources import ExtractedItem, SourceDef
from news_digest.pipeline.editor import _editor_item_fact_lock_errors
from news_digest.pipeline.pre_send_quality_judge import (
    _apply_repair_executor,
)
from news_digest.pipeline.plan_execution import build_final_execution_report


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

    def test_pre_send_repair_rejects_hallucinated_patch_and_keeps_line_without_plan_backup(self) -> None:
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
                                "validated": True,
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

        # Этап 3: замены только из цепочки планового слота. Кандидат без
        # plan_slot_id заменён быть не может — строка честно сохраняется.
        self.assertIn("Битая строка без фактов", repaired)
        self.assertNotIn("AO Arena", repaired)
        self.assertNotIn("Чистая резервная новость", repaired)
        self.assertEqual(report["fact_lock_rejected"], 1)
        self.assertEqual(report["reserve_replacement_used"], 0)
        self.assertEqual(report["unresolved"], 1)
        self.assertEqual(report["operations"][0]["outcome"], "unresolved")

    def test_strip_does_not_keep_bad_line_for_section_floor(self) -> None:
        # A requested strip is never silently kept merely to protect a floor.
        # Without execution evidence the operation remains honestly unresolved,
        # but both bad HTML lines are removed.
        bullets = "".join(
            f'• Новость номер {i} про Большой Манчестер сегодня. <a href="https://example.test/{i}">MEN</a>\n'
            for i in range(1, 7)
        )
        digest_html = (
            "<b>Greater Manchester Brief — 2026-07-09, 08:00</b>\n\n"
            "<b>Свежие новости</b>\n" + bullets
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(json.dumps({"candidates": []}), encoding="utf-8")
            repaired, report = _apply_repair_executor(
                project_root=root,
                digest_html=digest_html,
                actions=[],
                critical_errors=[
                    {"line_index": 2, "section": "Свежие новости", "risk": "duplicate",
                     "problem": "Дублирование информации о дорожных ограничениях M62 и M6.",
                     "suggested_action": "strip"},
                    {"line_index": 3, "section": "Свежие новости", "risk": "date",
                     "problem": "Дата 9 августа не подтверждена источником.",
                     "suggested_action": "strip"},
                ],
                deterministic_post_check={"errors": []},
                dry_run=False,
            )
        self.assertNotIn("Новость номер 2", repaired)
        self.assertNotIn("Новость номер 3", repaired)
        self.assertEqual(report["stripped"], 2)
        self.assertNotIn("kept_below_floor", report)

    def test_related_duplicate_lines_are_one_unresolved_operation_while_both_remain(self) -> None:
        digest_html = (
            "<b>Greater Manchester Brief — 2026-07-21, 08:00</b>\n\n"
            "<b>Билеты / Ticket Radar</b>\n"
            '• <b>Steel Panther</b> — 21 июля, Manchester Academy. <a href="https://tickets.test/steel-1">A</a>\n'
            '• <b>Steel Panther</b> — 21 июля, Manchester Academy. <a href="https://tickets.test/steel-2">B</a>\n'
        )
        candidates = []
        slots = {}
        for idx in (1, 2):
            fp = f"steel-{idx}"
            candidates.append(
                {
                    "fingerprint": fp,
                    "plan_slot_id": f"ticket_radar-0{idx}",
                    "source_url": f"https://tickets.test/steel-{idx}",
                    "source_label": "Tickets",
                    "ticket_notability": {"artist": "Steel Panther"},
                    "event": {"event_name": "Steel Panther", "date_start": "2026-07-21", "venue": "Manchester Academy"},
                }
            )
            slots[f"ticket_radar-0{idx}"] = {
                "slot_id": f"ticket_radar-0{idx}",
                "section": "Билеты / Ticket Radar",
                "status": "shown",
                "final_fingerprint": fp,
                "replacement_reason": "",
                "failed_attempts": [],
            }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(json.dumps({"candidates": candidates}), encoding="utf-8")
            (state_dir / "plan_execution_report.json").write_text(json.dumps({"slots": slots}), encoding="utf-8")
            _, report = _apply_repair_executor(
                project_root=root,
                digest_html=digest_html,
                actions=[
                    {"line_index": 1, "section": "Билеты / Ticket Radar", "action": "keep", "reason": "Дублирование Steel Panther в строках 1 и 2. Оставить одну."},
                    {"line_index": 2, "section": "Билеты / Ticket Radar", "action": "keep", "reason": "Дублирование Steel Panther в строках 1 и 2. Удалить дубль."},
                ],
                critical_errors=[],
                deterministic_post_check={"errors": []},
                dry_run=False,
            )

        self.assertEqual(len(report["operations"]), 1)
        self.assertEqual(report["operations"][0]["outcome"], "unresolved")
        self.assertEqual(report["unresolved"], 1)
        self.assertEqual(report["blocking_unresolved"], 0)

    def test_wrong_artist_is_unresolved_and_blocking_until_expected_headliner_is_visible(self) -> None:
        digest_html = (
            "<b>Greater Manchester Brief — 2026-07-21, 08:00</b>\n\n"
            "<b>Крупные концерты вне GM</b>\n"
            '• <b>Ladytron</b> — 6 августа, Crystal Palace Bowl. <a href="https://tickets.test/gary-numan">Tickets</a>\n'
        )
        candidate = {
            "fingerprint": "gary-numan",
            "plan_slot_id": "outside_gm_tickets-01",
            "source_url": "https://tickets.test/gary-numan",
            "source_label": "Tickets",
            "event": {"event_name": "Palace Bowl Presents - Gary Numan", "date_start": "2026-08-06", "venue": "Crystal Palace Bowl"},
            "ticket_notability": {"artist": "Ladytron"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(json.dumps({"candidates": [candidate]}), encoding="utf-8")
            (state_dir / "plan_execution_report.json").write_text(
                json.dumps(
                    {
                        "slots": {
                            "outside_gm_tickets-01": {
                                "slot_id": "outside_gm_tickets-01",
                                "section": "Крупные концерты вне GM",
                                "status": "shown",
                                "final_fingerprint": "gary-numan",
                                "replacement_reason": "",
                                "failed_attempts": [],
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            _, report = _apply_repair_executor(
                project_root=root,
                digest_html=digest_html,
                actions=[
                    {
                        "line_index": 1,
                        "section": "Крупные концерты вне GM",
                        "action": "keep",
                        "reason": "Неправильное указание основного артиста.",
                        "risk": "fact_integrity",
                    }
                ],
                critical_errors=[],
                deterministic_post_check={"errors": []},
                dry_run=False,
            )

        self.assertEqual(report["operations"][0]["outcome"], "unresolved")
        self.assertEqual(report["blocking_unresolved"], 1)

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

    def test_final_selection_report_is_slot_based_and_counts_each_html_row_once(self) -> None:
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
                {
                    "fingerprint": "same-url-inventory-copy",
                    "title": "Inventory copy must not create a report row",
                    "source_url": "https://example.test/visible",
                    "source_label": "Duplicate feed",
                    "primary_block": "city_watch",
                    "include": False,
                },
            ]
            (state_dir / "candidates.json").write_text(json.dumps({"candidates": candidates}), encoding="utf-8")
            (state_dir / "release_plan.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "report-test",
                        "run_date_london": "2026-06-29",
                        "ordered_sections": ["Свежие новости"],
                        "sections": {"Свежие новости": {"min": 2, "planned": 2}},
                        "slots": [
                            {"slot_id": "last_24h-01", "section": "Свежие новости", "block": "last_24h", "position": 1, "primary_fingerprint": "visible", "backup_fingerprints": []},
                            {"slot_id": "last_24h-02", "section": "Свежие новости", "block": "last_24h", "position": 2, "primary_fingerprint": "lost", "backup_fingerprints": []},
                        ],
                        "lead": {},
                    }
                ),
                encoding="utf-8",
            )
            (state_dir / "plan_execution_report.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "report-test",
                        "run_date_london": "2026-06-29",
                        "slots": {
                            "last_24h-01": {"slot_id": "last_24h-01", "section": "Свежие новости", "status": "shown", "final_fingerprint": "visible", "replacement_reason": "", "failed_attempts": []},
                            "last_24h-02": {"slot_id": "last_24h-02", "section": "Свежие новости", "status": "removed", "final_fingerprint": "", "replacement_reason": "unrenderable_line", "failed_attempts": []},
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = build_final_execution_report(
                state_dir,
                '<b>Свежие новости</b>\n• Visible <a href="https://example.test/visible">BBC</a>',
            )

        self.assertEqual(payload["counts"]["slots"], 2)
        self.assertEqual(payload["counts"]["final_html_rows"], 1)
        self.assertEqual(payload["counts"]["final_report_rows"], 1)
        self.assertEqual(len(payload["final_rows"]), 1)
        self.assertEqual(len(payload["removed_slots"]), 1)
        self.assertEqual(payload["sections"]["Свежие новости"]["execution_loss"], 1)


if __name__ == "__main__":
    unittest.main()
