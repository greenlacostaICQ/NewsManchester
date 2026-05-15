from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from news_digest.pipeline.auto_editor import auto_edit_digest, repair_rendered_line
from news_digest.pipeline.event_quality import event_quality_report
from news_digest.pipeline.writer import _draft_line_quality_errors


def _run_auto_editor(candidates: list[dict]) -> list[dict]:
    root = Path(tempfile.mkdtemp())
    state_dir = root / "data" / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "candidates.json").write_text(
        json.dumps({"pipeline_run_id": "test-run", "candidates": candidates}),
        encoding="utf-8",
    )
    auto_edit_digest(root)
    return json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))["candidates"]


class AutoEditorRulesTest(unittest.TestCase):
    def test_weather_duplicate_prefix_is_normalized(self) -> None:
        repaired, notes = repair_rendered_line("Погода", "• Погода: Погода: 6-13°C. Зонт нужен.")
        self.assertEqual(repaired, "• Погода: 6-13°C. Зонт нужен.")
        self.assertEqual(notes, [])

    def test_metrolink_named_line_is_preserved(self) -> None:
        [candidate] = _run_auto_editor(
            [
                {
                    "fingerprint": "metrolink-bury",
                    "title": "No trams on Bury line between Bury Interchange and Crumpsall from 17 May to 1 June",
                    "summary": "Track replacement works; replacement bus services will run between Bury Interchange and Crumpsall.",
                    "lead": "",
                    "evidence_text": "Bury line between Bury Interchange and Crumpsall from 17 May to 1 June replacement bus track replacement",
                    "source_url": "https://example.com/metrolink",
                    "source_label": "TfGM",
                    "primary_block": "last_24h",
                    "category": "media_layer",
                    "include": True,
                    "dedupe_decision": "new",
                    "reason": "test",
                    "draft_line": "• Metrolink: трамваи не ходят на одной из основных линий.",
                }
            ]
        )
        self.assertTrue(candidate["include"])
        self.assertEqual(candidate["primary_block"], "transport")
        self.assertIn("Bury line", candidate["draft_line"])
        self.assertIn("Bury Interchange", candidate["draft_line"])
        self.assertIn("Crumpsall", candidate["draft_line"])

    def test_past_event_is_replaced_from_neighbor_pool(self) -> None:
        old_event, replacement = _run_auto_editor(
            [
                {
                    "fingerprint": "old-event",
                    "title": "Digital Infrastructure North 2026 - 13 May",
                    "summary": "event_date=2026-05-13 09:00",
                    "lead": "",
                    "evidence_text": "conference 13 May",
                    "source_url": "https://example.com/event",
                    "source_label": "Example",
                    "primary_block": "next_7_days",
                    "category": "tech_business",
                    "include": True,
                    "dedupe_decision": "new",
                    "reason": "test",
                    "draft_line": "• IT: Digital Infrastructure North 2026 прошла 13 мая.",
                },
                {
                    "fingerprint": "replacement",
                    "title": "Manchester Games Network launched today",
                    "summary": "A new city games network launched today for local studios and founders",
                    "lead": "",
                    "evidence_text": "A new city games network launched today for local studios and founders",
                    "source_url": "https://example.com/repl",
                    "source_label": "Example",
                    "primary_block": "tech_business",
                    "category": "tech_business",
                    "include": False,
                    "dedupe_decision": "new",
                    "reason": "test",
                    "draft_line": "",
                },
            ]
        )
        self.assertFalse(old_event["include"])
        self.assertIn("expired", old_event["reject_reasons"])
        self.assertTrue(replacement["include"])
        self.assertIn("IT и бизнес", replacement["draft_line"])

    def test_writer_accepts_equivalent_pound_amounts(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Opening of new £26.5m hospice",
            "summary": "The new hospice cost £26.5m.",
            "lead": "",
            "evidence_text": "The new hospice cost £26.5m.",
        }
        errors = _draft_line_quality_errors(
            candidate,
            "• Хоспис: новый центр стоимостью £26,5млн открылся в Greater Manchester. Следите за обновлениями.",
        )
        self.assertNotIn("Pound amount '£26,5млн' not present in evidence_text.", errors)

    def test_writer_rejects_under_specified_culture_item(self) -> None:
        candidate = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "title": "Angel's Bone",
            "summary": "Angel's Bone at a Manchester venue",
            "lead": "",
            "evidence_text": "Angel's Bone at a Manchester venue",
        }
        errors = _draft_line_quality_errors(
            candidate,
            "• Культура: Angel's Bone появится в Манчестере. Уточните детали перед поездкой.",
        )
        self.assertTrue(any("under-specified" in error for error in errors))

    def test_event_quality_gate_requires_date_place_location_and_access(self) -> None:
        complete = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "title": "Concert at AO Arena Manchester on 20 May",
            "summary": "AO Arena, Manchester | 20 May 2026 19:30 | tickets from £35",
            "source_url": "https://example.com/tickets",
            "source_label": "Venue",
        }
        thin = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "title": "Concert coming soon",
            "summary": "A show at a venue.",
            "source_url": "https://example.com/event",
            "source_label": "Venue",
        }
        self.assertTrue(event_quality_report(complete)["ok"])
        report = event_quality_report(thin)
        self.assertFalse(report["ok"])
        self.assertIn("date", report["missing"])
        self.assertIn("district", report["missing"])
        self.assertIn("price_or_free_or_booking", report["missing"])

    def test_auto_editor_drops_event_without_access_signal(self) -> None:
        [candidate] = _run_auto_editor(
            [
                {
                    "fingerprint": "thin-event",
                    "title": "Concert at AO Arena Manchester on 20 May",
                    "summary": "AO Arena, Manchester | 20 May 2026 19:30",
                    "lead": "",
                    "evidence_text": "Concert at AO Arena Manchester on 20 May 2026.",
                    "source_url": "https://example.com/event",
                    "source_label": "Venue",
                    "primary_block": "next_7_days",
                    "category": "culture_weekly",
                    "include": True,
                    "dedupe_decision": "new",
                    "reason": "test",
                    "draft_line": "• Культура: концерт пройдет 20 мая в AO Arena в Manchester. Уточните детали перед поездкой.",
                }
            ]
        )
        self.assertFalse(candidate["include"])
        self.assertIn("source_thin", candidate["reject_reasons"])


if __name__ == "__main__":
    unittest.main()
