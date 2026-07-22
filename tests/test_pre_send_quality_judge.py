from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from news_digest.pipeline.pre_send_quality_judge import (
    REPORT_NAME,
    _chunk_digest_slots,
    _combine_map_reduce_results,
    _product_completeness_context,
    _rendered_candidates,
    digest_hash,
    digest_lines_from_html,
    evaluate_pre_send_quality,
    quality_gate_error_for_digest,
)
from news_digest.pipeline.common import today_london


class PreSendQualityJudgeTests(unittest.TestCase):
    def _project(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name)
        (root / "data" / "outgoing").mkdir(parents=True)
        (root / "data" / "state").mkdir(parents=True)
        return tmp, root

    def test_digest_lines_extract_sections_and_items(self) -> None:
        html = """<b>Greater Manchester Brief — 2026-06-17, 08:10</b>

<b>Свежие новости</b>
• <b>Manchester:</b> line one. <a href="https://example.com">MEN</a>
<b>Футбол</b>
• City line.
"""
        lines = digest_lines_from_html(html)
        self.assertEqual([line["section"] for line in lines], ["Свежие новости", "Футбол"])
        self.assertEqual(lines[0]["line_index"], 1)
        self.assertIn("Manchester:", lines[0]["text"])

    def test_quality_gate_never_blocks_current_digest_for_quality_verdict(self) -> None:
        tmp, root = self._project()
        with tmp:
            today = today_london()
            html = f"<b>Greater Manchester Brief — {today}, 08:10</b>\n\n<b>Свежие новости</b>\n• Safe line."
            digest_path = root / "data" / "outgoing" / "current_digest.html"
            digest_path.write_text(html, encoding="utf-8")
            report_path = root / "data" / "state" / REPORT_NAME
            report_path.write_text(
                json.dumps(
                    {
                        "decision": "pass",
                        "can_send": True,
                        "run_date_london": today,
                        "digest_sha256": digest_hash(html),
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(quality_gate_error_for_digest(root, digest_path), "")
            report_path.write_text(
                json.dumps(
                    {
                        "decision": "repair_required",
                        "can_send": False,
                        "run_date_london": today,
                        "digest_sha256": digest_hash(html),
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(quality_gate_error_for_digest(root, digest_path), "")

    def test_dry_run_writes_non_blocking_report_without_api(self) -> None:
        tmp, root = self._project()
        with tmp:
            today = today_london()
            html = f"<b>Greater Manchester Brief — {today}, 08:10</b>\n\n<b>Свежие новости</b>\n• Safe line."
            (root / "data" / "outgoing" / "current_digest.html").write_text(html, encoding="utf-8")
            result = evaluate_pre_send_quality(root, dry_run=True)
            self.assertEqual(result["status"], "dry_run")
            self.assertTrue(result["can_send"])
            report = json.loads((root / "data" / "state" / REPORT_NAME).read_text(encoding="utf-8"))
            self.assertEqual(report["digest_sha256"], digest_hash(html))

    @mock.patch("news_digest.pipeline.pre_send_quality_judge.resolve_model_route", return_value=[])
    def test_missing_quality_model_is_warning_not_delivery_block(self, _route: mock.Mock) -> None:
        tmp, root = self._project()
        with tmp:
            today = today_london()
            html = f"<b>Greater Manchester Brief — {today}, 08:10</b>\n\n<b>Свежие новости</b>\n• Safe line."
            (root / "data" / "outgoing" / "current_digest.html").write_text(html, encoding="utf-8")

            result = evaluate_pre_send_quality(root)

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["decision"], "warn")
        self.assertTrue(result["can_send"])

    def test_dry_run_records_product_completeness_alerts(self) -> None:
        tmp, root = self._project()
        with tmp:
            today = today_london()
            html = (
                f"<b>Greater Manchester Brief — {today}, 08:10</b>\n\n"
                "<b>Свежие новости</b>\n"
                "• One news line.\n\n"
                "<b>Билеты / Ticket Radar</b>\n"
                + "\n".join(f"• Ticket {idx}." for idx in range(8))
            )
            (root / "data" / "outgoing" / "current_digest.html").write_text(html, encoding="utf-8")
            (root / "data" / "state" / "writer_report.json").write_text(
                json.dumps(
                    {
                        "section_counts": {
                            "Свежие новости": 1,
                            "Футбол": 0,
                            "Билеты / Ticket Radar": 8,
                        },
                        "quality_counts": {"included_candidates": 20, "rendered_candidates": 9},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = evaluate_pre_send_quality(root, dry_run=True)

            alerts = result["product_completeness"]["alerts"]
            self.assertTrue(any("Свежие новости" in alert for alert in alerts))
            self.assertTrue(any("ticket dominance" in alert for alert in alerts))

    def test_product_completeness_does_not_alert_hidden_weekend_monday_to_wednesday(self) -> None:
        tmp, root = self._project()
        with tmp, mock.patch("news_digest.pipeline.pre_send_quality_judge.now_london", return_value=datetime(2026, 7, 7, 8, 0)):
            (root / "data" / "state" / "draft_digest.html").write_text(
                "<b>Greater Manchester Brief — 2026-07-07, 08:00</b>\n\n"
                "<b>Свежие новости</b>\n"
                "• One news line.\n",
                encoding="utf-8",
            )
            product = _product_completeness_context(root, [])

        self.assertNotIn("Выходные в GM", product["core_counts"])
        self.assertFalse(any("Выходные в GM" in alert for alert in product["alerts"]))

    def test_rendered_candidates_uses_compact_judge_payload(self) -> None:
        tmp, root = self._project()
        with tmp:
            state_dir = root / "data" / "state"
            (state_dir / "writer_report.json").write_text(
                json.dumps({"rendered_candidate_fingerprints": ["fp-1"]}),
                encoding="utf-8",
            )
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "fingerprint": "fp-1",
                                "title": "Council confirms a new city centre consultation",
                                "source_label": "Manchester City Council",
                                "source_url": "https://example.test/story?utm=1",
                                "primary_block": "city",
                                "category": "council",
                                "summary": "summary that should not be copied",
                                "lead": "lead that should not be copied",
                                "draft_line": "• Draft line that should not be copied.",
                                "evidence_text": "Evidence " + ("x" * 1500),
                                "practical_angle": "Check the consultation deadline before responding.",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            rows = _rendered_candidates(root)

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertNotIn("summary", row)
            self.assertNotIn("lead", row)
            self.assertNotIn("draft_line", row)
            self.assertNotIn("evidence_text", row)
            self.assertNotIn("practical_angle", row)
            self.assertLessEqual(len(row["compact_facts"]), 520)

    def test_judge_chunks_split_large_section_without_cross_section_blending(self) -> None:
        slots = [
            {"line_index": idx, "section": "Билеты / Ticket Radar", "text": f"Ticket {idx}", "html": f"• Ticket {idx}."}
            for idx in range(1, 14)
        ]

        chunks = _chunk_digest_slots(slots, {}, max_lines=12)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["line_range"], [1, 12])
        self.assertEqual(chunks[1]["line_range"], [13, 13])
        self.assertEqual(chunks[0]["sections"], ["Билеты / Ticket Radar"])

    def test_map_reduce_partial_escalates_clean_pass_to_warn(self) -> None:
        status, combined, raw = _combine_map_reduce_results(
            [
                {
                    "status": "ok",
                    "chunk_id": "chunk-01",
                    "parsed": {
                        "decision": "pass",
                        "confidence": 0.9,
                        "critical_errors": [],
                        "actions": [],
                        "warnings": [],
                    },
                },
                {"status": "failed", "chunk_id": "chunk-02", "error": "429 rate limit"},
            ],
            {
                "status": "ok",
                "chunk_id": "reduce",
                "parsed": {
                    "decision": "pass",
                    "confidence": 0.8,
                    "critical_errors": [],
                    "actions": [],
                    "warnings": [],
                },
            },
        )

        self.assertEqual(status, "partial")
        self.assertEqual(combined["decision"], "warn")
        self.assertEqual(raw["failed_chunk_count"], 1)
        self.assertTrue(any("429" in warning for warning in combined["warnings"]))


if __name__ == "__main__":
    unittest.main()
