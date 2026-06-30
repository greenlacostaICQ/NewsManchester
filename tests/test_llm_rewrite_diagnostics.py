import json
import unittest
from datetime import date, timedelta
from unittest import mock

from news_digest.pipeline.llm_rewrite import (
    _apply_cost_after_quality_guard,
    _cap_repair_targets,
    _call_with_fallback,
    _cost_after_quality_skip_reason,
    _force_write_evidence_floor,
    _is_protected_rewrite_candidate,
    _is_actionable_weekend_candidate,
    _needs_quality_repair,
    _parse_english_card_results,
    _parse_provider_results,
    _rewrite_batch_items,
    _translation_batch_items,
    _skip_llm_for_manual_review,
)
from news_digest.pipeline.model_routing import ResolvedModelRouteStep
from news_digest.pipeline import provider_health
from news_digest.pipeline.common import today_london
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

    def test_parse_english_card_results_accepts_rubric_without_reader_action(self) -> None:
        batch = [_candidate("fp-1", "Prince gives Manchester speech")]
        raw = json.dumps(
            [
                {
                    "fingerprint": "fp-1",
                    "rubric": "civic",
                    "fact_card": {
                        "what_happened": "Prince William reflected on public service in a Manchester speech.",
                        "where": "Manchester",
                        "when": "",
                        "who_affected": "",
                        "why_now": "The speech happened at a Manchester event.",
                        "reader_value": "Local civic context.",
                        "reader_action": "",
                        "missing_facts": ["exact venue"],
                    },
                    "reader_card": "Manchester: Prince William used a city speech to reflect on public service and the Queen's death, making it a civic local moment rather than a practical alert.",
                    "editorial_score": 72,
                    "selection_hint": "publish",
                    "missing_facts": ["exact venue"],
                }
            ],
            ensure_ascii=False,
        )

        mapping, diagnostic = _parse_english_card_results(
            raw,
            batch,
            provider_name="DeepSeek",
            model="deepseek-v4-pro",
            batch_idx=1,
            total_batches=1,
        )

        self.assertEqual(set(mapping), {"fp-1"})
        card, provider, model = mapping["fp-1"]
        self.assertEqual(provider, "DeepSeek")
        self.assertEqual(model, "deepseek-v4-pro")
        self.assertEqual(card["rubric"], "civic")
        self.assertEqual(card["editorial_score"], 72)
        self.assertEqual(diagnostic["accepted"], 1)

    def test_translation_payload_uses_english_card_not_raw_evidence(self) -> None:
        candidate = _candidate("fp-1")
        candidate["evidence_text"] = "x" * 5000
        candidate["english_rubric"] = "transport"
        candidate["english_reader_card"] = "Metrolink: the Eccles line has minor delays today; TfGM has not named a specific affected section."
        candidate["english_fact_card"] = {"what_happened": "minor delays", "missing_facts": ["affected section"]}

        item = _translation_batch_items([candidate])[0]

        self.assertIn("english_reader_card", item)
        self.assertIn("english_fact_card", item)
        self.assertNotIn("evidence_text", item)

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

    def test_quality_repair_is_hard_defects_only(self) -> None:
        candidate = _candidate("fp-1")
        candidate["draft_line"] = "• Манчестер: совет опубликовал короткое обновление."

        self.assertFalse(_needs_quality_repair(candidate))

        candidate["draft_line"] = "• Manchester council published an update and residents should check the details."
        self.assertTrue(_needs_quality_repair(candidate))

    def test_cost_after_quality_holds_headline_only_candidate_before_model(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "fp-empty",
            "title": "Teaser headline",
            "summary": "",
            "lead": "",
            "evidence_text": "",
            "primary_block": "last_24h",
            "category": "media_layer",
        }

        selected, report = _apply_cost_after_quality_guard([candidate])

        self.assertEqual(selected, [])
        self.assertFalse(candidate["include"])
        self.assertEqual(candidate["digest_selection_verdict"], "needs_enrichment")
        self.assertTrue(candidate["backup_pool_only"])
        self.assertEqual(report["held_before_model"], 1)

    def test_cost_after_quality_holds_no_date_event_before_model(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "fp-no-date-event",
            "title": "Community workshop at HOME",
            "summary": "A workshop for local residents.",
            "lead": "",
            "evidence_text": "A workshop for local residents at HOME.",
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "event": {"is_event": True, "event_name": "Community workshop", "venue": "HOME"},
        }

        self.assertIn("no actionable date", _cost_after_quality_skip_reason(candidate))

    def test_cost_after_quality_keeps_dated_event_for_model(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "fp-dated-event",
            "title": "Community workshop at HOME",
            "summary": "A workshop for local residents.",
            "lead": "",
            "evidence_text": "A workshop for local residents at HOME.",
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "event": {
                "is_event": True,
                "event_name": "Community workshop",
                "venue": "HOME",
                "date_start": today_london(),
            },
        }

        self.assertEqual(_cost_after_quality_skip_reason(candidate), "")

    def test_cost_after_quality_keeps_text_dated_event_when_struct_missing(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "fp-text-date-event",
            "title": "Community workshop at HOME on 1 July 2026",
            "summary": "A workshop for local residents.",
            "lead": "",
            "evidence_text": "A workshop for local residents at HOME on 1 July 2026.",
            "primary_block": "next_7_days",
            "category": "culture_weekly",
            "event": {"is_event": True, "event_name": "Community workshop", "venue": "HOME"},
        }

        self.assertEqual(_cost_after_quality_skip_reason(candidate), "")

    def test_repair_targets_are_capped_before_second_model_spend(self) -> None:
        candidates = [
            {**_candidate(f"fp-{idx}"), "draft_line": "• Manchester council published an update and residents should check the details."}
            for idx in range(3)
        ]

        selected, report = _cap_repair_targets(candidates, max_items=1)

        self.assertEqual(len(selected), 1)
        self.assertEqual(report["held_after_cap"], 2)
        self.assertEqual(
            sum(1 for candidate in candidates if candidate.get("llm_repair_skipped_reason")),
            2,
        )

    def test_included_borderline_items_are_sent_to_llm(self) -> None:
        # ADR 0025: borderline items are no longer dropped by the writer, so an
        # included borderline item must still be rewritten into good Russian
        # copy. Only a non-included borderline item skips the model spend.
        included = _candidate("fp-1")
        included["include"] = True
        included["editorial_status"] = "borderline"
        self.assertFalse(_skip_llm_for_manual_review(included))

        held = _candidate("fp-2")
        held["editorial_status"] = "borderline"
        self.assertTrue(_skip_llm_for_manual_review(held))

        held["manual_override"] = "force_include"
        self.assertFalse(_skip_llm_for_manual_review(held))
        self.assertFalse(_skip_curator_for_manual_review(held))

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

    def test_official_football_thin_evidence_still_gets_force_write_floor(self) -> None:
        self.assertEqual(
            _force_write_evidence_floor(
                {"category": "football", "source_label": "Manchester United"}
            ),
            40,
        )
        self.assertEqual(_force_write_evidence_floor({"category": "media_layer"}), 400)

    def test_rewrite_misses_retry_openai_before_deepseek(self) -> None:
        provider_health.reset()
        candidates = [_candidate("fp-1"), _candidate("fp-2"), _candidate("fp-3")]
        route = [
            ResolvedModelRouteStep(
                provider="openai",
                provider_label="OpenAI",
                base_url="https://openai.test/v1",
                model="gpt-4o-mini",
                api_key="openai-key",
                api_key_env="OPENAI_API_KEY",
                role="quality_rewrite_primary",
                priority=1,
                batch_size=3,
                timeout_seconds=60,
            ),
            ResolvedModelRouteStep(
                provider="deepseek",
                provider_label="DeepSeek",
                base_url="https://deepseek.test/v1",
                model="deepseek-chat",
                api_key="deepseek-key",
                api_key_env="DEEPSEEK_API_KEY",
                role="rewrite_last_resort",
                priority=2,
                batch_size=3,
                timeout_seconds=60,
            ),
        ]
        calls: list[tuple[str, list[str], int | None]] = []

        def fake_provider(*args, **kwargs):
            batch = args[3]
            provider_name = args[4]
            calls.append((provider_name, [item["fingerprint"] for item in batch], kwargs.get("batch_size")))
            if provider_name == "OpenAI":
                return {"fp-1": ("• OpenAI wrote first.", "OpenAI", "gpt-4o-mini")}
            if provider_name == "OpenAI-retry":
                return {"fp-2": ("• OpenAI retry wrote second.", "OpenAI-retry", "gpt-4o-mini")}
            if provider_name == "DeepSeek":
                return {"fp-3": ("• DeepSeek wrote third.", "DeepSeek", "deepseek-chat")}
            return {}

        with (
            mock.patch("news_digest.pipeline.llm_rewrite.resolve_model_route", return_value=route),
            mock.patch("news_digest.pipeline.llm_rewrite._call_provider_batch", side_effect=fake_provider),
        ):
            mapping = _call_with_fallback(
                candidates,
                "prompt",
                provider_override="",
                base_url_override="",
                model_override="",
                prompt_name="city_news@v-test",
                route_name="rewrite",
            )

        self.assertEqual(set(mapping), {"fp-1", "fp-2"})
        self.assertEqual(
            calls,
            [
                ("OpenAI", ["fp-1", "fp-2", "fp-3"], 3),
                ("OpenAI-retry", ["fp-2", "fp-3"], 1),
            ],
        )

    def test_weekend_candidate_uses_london_date_object(self) -> None:
        today = date.fromisoformat(today_london())
        days_to_sat = (5 - today.weekday()) % 7
        saturday = today + timedelta(days=days_to_sat)

        self.assertTrue(
            _is_actionable_weekend_candidate(
                {"event": {"date": saturday.isoformat()}, "title": "Saturday family event"}
            )
        )


if __name__ == "__main__":
    unittest.main()
