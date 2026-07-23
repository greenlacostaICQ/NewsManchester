from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.common import REQUIRED_SCAN_CATEGORIES, today_london
from news_digest.pipeline.dedupe import dedupe_candidates
from news_digest.pipeline.release import (
    _validate_candidates,
    _validate_curator_report,
    _validate_scan_report,
)


class DeliveryContentDegradesTest(unittest.TestCase):
    def test_release_treats_content_gaps_as_warnings(self) -> None:
        errors: list[str] = []
        warnings: list[str] = []
        scan = {
            "run_date_london": today_london(),
            "categories": {
                key: {"checked": False, "usable_for_release": False}
                for key in REQUIRED_SCAN_CATEGORIES
            },
        }
        _validate_scan_report(scan, today_london(), errors, warnings)
        context = _validate_candidates(
            {
                "run_date_london": today_london(),
                "candidates": [{}, "not-an-object"],
            },
            today_london(),
            errors,
            warnings,
        )
        _validate_curator_report(
            {
                "run_date_london": today_london(),
                "status": "skipped",
                "reason": "provider outage",
            },
            today_london(),
            errors,
            warnings,
        )

        self.assertEqual(errors, [])
        self.assertEqual(context["included_candidates"], [])
        self.assertTrue(warnings)

    def test_malformed_rows_do_not_make_dedupe_or_validator_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "data" / "state"
            state.mkdir(parents=True)
            (state / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "content-degrades-test",
                        "run_date_london": today_london(),
                        "candidates": ["not-an-object"],
                    }
                ),
                encoding="utf-8",
            )

            dedupe_result = dedupe_candidates(root)
            validator_result = validate_candidates(root)
            dedupe_report = json.loads((state / "dedupe_memory.json").read_text(encoding="utf-8"))
            validator_report = json.loads(
                (state / "candidate_validation_report.json").read_text(encoding="utf-8")
            )

        self.assertTrue(dedupe_result.ok)
        self.assertTrue(validator_result.ok)
        self.assertEqual(dedupe_report["stage_status"], "complete")
        self.assertEqual(validator_report["stage_status"], "complete")
        self.assertTrue(dedupe_report["warnings"])
        self.assertTrue(validator_report["warnings"])


if __name__ == "__main__":
    unittest.main()
