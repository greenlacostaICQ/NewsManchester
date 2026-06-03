import json
import unittest

from news_digest.pipeline.llm_rewrite import (
    _is_protected_rewrite_candidate,
    _needs_quality_repair,
    _parse_provider_results,
    _rewrite_batch_items,
    _skip_llm_for_manual_review,
)
from news_digest.pipeline.curator import _skip_curator_for_manual_review
from news_digest.pipeline.curator import _is_curator_protected


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

    def test_parse_provider_results_accepts_structured_reason_for_empty_line(self) -> None:
        batch = [_candidate("fp-1")]
        raw = json.dumps(
            [
                {
                    "fingerprint": "fp-1",
                    "decision": "needs_enrichment",
                    "draft_line": "",
                    "missing_facts": ["what_happened"],
                }
            ],
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

        self.assertEqual(mapping, {})
        self.assertEqual(diagnostic["rejected_counts"]["empty_draft_line"], 0)
        self.assertEqual(diagnostic["rejected_counts"]["empty_draft_line_with_reason"], 1)
        self.assertEqual(diagnostic["missing_candidates"][0]["fingerprint"], "fp-1")

    def test_hard_news_and_transport_get_extra_rewrite_recovery(self) -> None:
        self.assertTrue(_is_protected_rewrite_candidate({"primary_block": "last_24h"}))
        self.assertTrue(_is_protected_rewrite_candidate({"primary_block": "transport"}))
        self.assertTrue(_is_protected_rewrite_candidate({"category": "gmp"}))
        self.assertFalse(_is_protected_rewrite_candidate({"category": "venues_tickets", "primary_block": "ticket_radar"}))

    def test_diaspora_events_skip_gm_only_curator(self) -> None:
        self.assertTrue(_is_curator_protected({"category": "russian_speaking_events", "primary_block": "russian_events"}))
        self.assertTrue(_is_curator_protected({"category": "diaspora_events"}))

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

    def test_rewrite_packet_is_support_not_replacement_for_evidence(self) -> None:
        candidate = _candidate("fp-1", "James Milner reaches Premier League appearance record")
        candidate.update(
            {
                "summary": "James Milner made his 654th Premier League appearance.",
                "evidence_text": "BBC Sport says James Milner made his 654th Premier League appearance.",
                "practical_angle": "Use the record number if writing the football card.",
                "change_phase": "new_record",
                "reader_action_type": "just_know",
            }
        )

        item = _rewrite_batch_items([candidate])[0]

        self.assertIn("rewrite_packet", item)
        self.assertEqual(item["rewrite_packet"]["what"], candidate["title"])
        self.assertIn("654th Premier League appearance", item["evidence_text"])
        self.assertIn("Use the record number", item["practical_angle"])


if __name__ == "__main__":
    unittest.main()
