from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from news_digest.pipeline.writer import write_digest


class WriterReaderValueSortTest(unittest.TestCase):
    def test_reader_value_score_sorts_candidates_inside_section(self) -> None:
        root = Path(tempfile.mkdtemp())
        state_dir = root / "data" / "state"
        state_dir.mkdir(parents=True)
        candidates = [
            {
                "fingerprint": "weak-transport",
                "title": "Generic travel advice",
                "summary": "General travel advice with no local route detail.",
                "lead": "",
                "practical_angle": "",
                "evidence_text": "General travel advice with no local route detail.",
                "source_url": "https://example.com/advice",
                "source_label": "Example",
                "primary_block": "transport",
                "category": "transport",
                "include": True,
                "dedupe_decision": "new",
                "reason": "test",
                "draft_line": "• Автобус: общий совет для поездок без конкретного маршрута. Проверьте обновления перед выходом.",
            },
            {
                "fingerprint": "strong-transport",
                "title": "Metrolink disruption between Bury and Crumpsall from 17 May",
                "summary": "Replacement buses will run between Bury and Crumpsall from 17 May.",
                "lead": "",
                "practical_angle": "Проверьте маршрут перед поездкой.",
                "evidence_text": "Metrolink disruption between Bury and Crumpsall from 17 May. Replacement buses will run.",
                "source_url": "https://tfgm.com/travel-updates/bury-line",
                "source_label": "TfGM",
                "primary_block": "transport",
                "category": "transport",
                "include": True,
                "dedupe_decision": "new",
                "reason": "test",
                "draft_line": "• Metrolink: сбой между Bury и Crumpsall с 17 мая. Проверьте маршрут перед поездкой.",
            },
        ]
        (state_dir / "candidates.json").write_text(
            json.dumps({"pipeline_run_id": "test-run", "candidates": candidates}),
            encoding="utf-8",
        )

        result = write_digest(root)

        self.assertTrue(result.ok)
        text = result.draft_path.read_text(encoding="utf-8")
        self.assertLess(text.index("Metrolink: сбой"), text.index("Автобус: общий совет"))
        report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))
        self.assertGreater(
            report["reader_value_report"]["top"][0]["reader_value_score"],
            report["reader_value_report"]["bottom"][0]["reader_value_score"],
        )


if __name__ == "__main__":
    unittest.main()
