from __future__ import annotations

import unittest

from news_digest.pipeline.reject_reasons import (
    add_reject_reason,
    classify_reject_reason_text,
    ensure_reject_reason,
    reject_reason_counts,
    reject_reasons,
)


class RejectReasonsTest(unittest.TestCase):
    def test_classifies_existing_human_reasons(self) -> None:
        self.assertEqual(classify_reject_reason_text("Curator drop: Evergreen-листинг без даты"), "evergreen")
        self.assertEqual(classify_reject_reason_text("Intra-batch topic duplicate"), "duplicate")
        self.assertEqual(classify_reject_reason_text("Validator: evidence is too thin"), "source_thin")
        self.assertEqual(classify_reject_reason_text("Auto-editor: draft_line is not Russian prose."), "english_prose")

    def test_backfills_include_false_candidate(self) -> None:
        candidate = {"include": False, "reason": "Repeat without new phase."}
        ensure_reject_reason(candidate)
        self.assertEqual(candidate["reject_reason"], "no_change")
        self.assertEqual(candidate["reject_reasons"], ["no_change"])

    def test_counts_rejected_candidates_only(self) -> None:
        rejected = {"include": False, "reason": "test"}
        add_reject_reason(rejected, "pr")
        add_reject_reason(rejected, "evergreen")
        included = {"include": True, "reject_reasons": ["weak_value"]}
        self.assertEqual(reject_reasons(rejected), ["pr", "evergreen"])
        self.assertEqual(reject_reason_counts([rejected, included]), {"evergreen": 1, "pr": 1})


if __name__ == "__main__":
    unittest.main()
