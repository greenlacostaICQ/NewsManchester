from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.pre_send_quality_judge import (
    REPORT_NAME,
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

    def test_quality_gate_requires_fresh_pass_for_current_digest(self) -> None:
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
            self.assertIn("blocked", quality_gate_error_for_digest(root, digest_path))

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


if __name__ == "__main__":
    unittest.main()
