import json
import unittest

from news_digest.pipeline.llm_rewrite import (
    _needs_quality_repair,
    _parse_provider_results,
    _skip_llm_for_manual_review,
)
from news_digest.pipeline.curator import _skip_curator_for_manual_review


def _candidate(fingerprint: str, title: str = "Test story") -> dict:
    return {
        "fingerprint": fingerprint,
        "title": title,
        "category": "council",
        "primary_block": "last_24h",
        "evidence_text": "Manchester council published a detailed update with dates, names and local impact.",
    }


class LlmRewriteDiagnosticsTests(unittest.TestCase):
    def test_parse_provider_results_reports_rejection_reasons(self) -> None:
        batch = [_candidate("fp-1", "Good"), _candidate("fp-2", "Empty"), _candidate("fp-3", "No bullet")]
        raw = json.dumps(
            [
                {"fingerprint": "fp-1", "draft_line": "• Манчестер: совет утвердил новый план. Детали опубликованы сегодня."},
                {"fingerprint": "fp-2", "draft_line": ""},
                {"fingerprint": "fp-3", "draft_line": "Манчестер: текст без bullet."},
                {"fingerprint": "fp-missing", "draft_line": "• Чужой fingerprint не должен попасть в mapping."},
            ],
            ensure_ascii=False,
        )

        mapping, diagnostic = _parse_provider_results(
            raw,
            batch,
            provider_name="DeepSeek",
            model="deepseek-chat",
            prompt_name="city_news@v-test",
            batch_idx=1,
            total_batches=1,
        )

        self.assertEqual(set(mapping), {"fp-1"})
        self.assertEqual(diagnostic["sent"], 3)
        self.assertEqual(diagnostic["returned_items"], 4)
        self.assertEqual(diagnostic["accepted"], 1)
        self.assertEqual(diagnostic["rejected_counts"]["empty_draft_line"], 1)
        self.assertEqual(diagnostic["rejected_counts"]["missing_bullet"], 1)
        self.assertEqual(diagnostic["rejected_counts"]["unknown_fingerprint"], 1)
        self.assertEqual(diagnostic["missing_candidates"][0]["fingerprint"], "fp-2")
        self.assertTrue(diagnostic["raw_excerpt"])

    def test_parse_provider_results_accepts_common_object_wrapper(self) -> None:
        batch = [_candidate("fp-1")]
        raw = json.dumps(
            {
                "results": [
                    {"fingerprint": "fp-1", "draft_line": "• Манчестер: совет обновил правила. Жителям стоит проверить сроки."}
                ]
            },
            ensure_ascii=False,
        )

        mapping, diagnostic = _parse_provider_results(
            raw,
            batch,
            provider_name="OpenAI",
            model="gpt-4o-mini",
            prompt_name="city_news@v-test",
            batch_idx=1,
            total_batches=1,
        )

        self.assertEqual(set(mapping), {"fp-1"})
        self.assertEqual(diagnostic["coerced_from_object_key"], "results")
        self.assertEqual(diagnostic["accepted"], 1)

    def test_quality_repair_uses_writer_gate_for_long_format_cards(self) -> None:
        candidate = _candidate("fp-1")
        candidate["draft_line"] = "• Манчестер: совет опубликовал короткое обновление."

        self.assertTrue(_needs_quality_repair(candidate))

    def test_borderline_items_are_not_sent_to_llm_without_manual_override(self) -> None:
        candidate = _candidate("fp-1")
        candidate["include"] = True
        candidate["editorial_status"] = "borderline"

        self.assertTrue(_skip_llm_for_manual_review(candidate))

        candidate["manual_override"] = "force_include"
        self.assertFalse(_skip_llm_for_manual_review(candidate))
        self.assertFalse(_skip_curator_for_manual_review(candidate))

    def test_curator_also_skips_borderline_without_manual_override(self) -> None:
        candidate = _candidate("fp-1")
        candidate["include"] = True
        candidate["editorial_status"] = "borderline"

        self.assertTrue(_skip_curator_for_manual_review(candidate))


if __name__ == "__main__":
    unittest.main()
