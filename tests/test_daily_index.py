from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from news_digest.pipeline.common import today_london
from news_digest.pipeline.daily_index import (
    append_daily_index,
    apply_daily_index_comparison,
    compare_candidate_to_daily_index,
    daily_index_record,
)


class DailyIndexTest(unittest.TestCase):
    def test_snapshot_record_contains_memory_fields(self) -> None:
        candidate = {
            "title": "Metrolink closure in Bury",
            "source_url": "https://example.com/story?utm=1",
            "source_label": "TfGM",
            "fingerprint": "fp-1",
            "category": "transport",
            "primary_block": "transport",
            "include": False,
            "reject_reasons": ["no_change"],
            "change_type": "no_change",
        }
        record = daily_index_record(candidate, pipeline_run_id="run-1")
        self.assertEqual(record["title"], candidate["title"])
        self.assertEqual(record["url"], candidate["source_url"])
        self.assertEqual(record["fingerprint"], "fp-1")
        self.assertEqual(record["source"], "TfGM")
        self.assertEqual(record["category"], "transport")
        self.assertEqual(record["borough"], "bury")
        self.assertFalse(record["included"])
        self.assertEqual(record["reject_reason"], "no_change")
        self.assertEqual(record["change_type"], "no_change")

    def test_yesterday_comparison_marks_same_story_lite(self) -> None:
        candidate = {
            "title": "Metrolink closure in Bury",
            "source_url": "https://example.com/story",
            "source_label": "TfGM",
            "fingerprint": "fp-new",
            "category": "transport",
            "primary_block": "transport",
            "include": True,
        }
        previous = {
            "run_date_london": "2026-05-14",
            "title": "Metrolink closure in Bury",
            "canonical_url": "example.com/story",
            "fingerprint": "fp-old",
            "source": "TfGM",
            "source_family": "tfgm",
            "borough": "bury",
            "entities": ["bury", "metrolink"],
            "normalized_title": "metrolink closure in bury",
            "included": True,
            "change_type": "new_story",
        }
        matches = compare_candidate_to_daily_index(candidate, [previous])
        self.assertEqual(matches[0]["match_type"], "canonical_url")
        report = apply_daily_index_comparison([candidate], [previous])
        self.assertEqual(candidate["change_type"], "same_story_rehash")
        self.assertEqual(candidate["matched_daily_index_fingerprint"], "fp-old")
        self.assertEqual(report["matched_candidates"], 1)

    def test_append_daily_index_writes_jsonl(self) -> None:
        root = Path(tempfile.mkdtemp())
        state_dir = root / "data" / "state"
        summary = append_daily_index(
            state_dir,
            [
                {
                    "title": "Salford market opens",
                    "source_url": "https://example.com/market",
                    "source_label": "Example",
                    "fingerprint": "fp-market",
                    "category": "culture_weekly",
                    "primary_block": "weekend_activities",
                    "include": True,
                    "change_type": "new_story",
                }
            ],
            pipeline_run_id="run-1",
        )
        self.assertEqual(summary["appended_records"], 1)
        path = state_dir / "daily_index.jsonl"
        [line] = path.read_text(encoding="utf-8").splitlines()
        record = json.loads(line)
        self.assertEqual(record["run_date_london"], today_london())
        self.assertEqual(record["fingerprint"], "fp-market")


if __name__ == "__main__":
    unittest.main()
