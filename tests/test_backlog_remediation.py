from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import timedelta
from io import StringIO
from pathlib import Path

from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.common import REQUIRED_SCAN_CATEGORIES, now_london
from news_digest.pipeline.editorial_contracts import build_editorial_contract, scrub_vague_ending
from news_digest.pipeline.collector.routing import _TICKET_HORIZON_DAYS
from news_digest.pipeline.dedupe import _apply_semantic_drop_guard
from news_digest.pipeline.history import write_daily_index_snapshot
from news_digest.pipeline.model_bakeoff import run_model_bakeoff
from news_digest.pipeline.story_intelligence import (
    apply_cheap_dedup_before_enrich,
    apply_story_intelligence,
    attach_story_clusters,
    backup_pool_record,
    mark_reject_second_opinion,
    new_facts_diff,
    section_board_score,
)
from news_digest.pipeline.release import (
    build_release,
    _classify_published_candidates,
    _classify_rendered_html_quality,
    _event_miss_review,
    _final_loss_check,
    _quality_scorecard,
    _borderline_queue,
    _summarise_diaspora_diagnostics,
    _summarise_source_health,
    _summarise_transport_coverage,
    _update_feedback_items,
)
from news_digest.pipeline.writer import write_digest
from scripts.run_local_digest import (
    _borderline_verdict,
    _diaspora_verdict_human,
    _explain_source_failure,
    _humanize_quality_warning,
    _humanize_borough_flag,
    _humanize_source_reason,
    _section_name_human,
    _section_shape_rows,
    _source_counts_phrase,
    _build_product_support_text,
    _support_top_issues,
    _ticket_type_human,
    _ticketmaster_rows,
    _translate_health_signal,
    cmd_pipeline_config,
)


class WriterRenderedFingerprintTest(unittest.TestCase):
    def test_media_article_rerouted_to_transport_uses_transport_length_rules(self) -> None:
        candidate = {
            "category": "media_layer",
            "primary_block": "transport",
            "title": "No trams to run on Rochdale line",
            "summary": "Metrolink works affect the Rochdale line.",
            "evidence_text": "Metrolink works affect the Rochdale line between Victoria and Rochdale.",
        }
        line = "• На Rochdale line не будет трамваев до 29 мая — проверьте маршрут."

        from news_digest.pipeline.writer import _draft_line_quality_errors

        self.assertEqual(_draft_line_quality_errors(candidate, line), [])

    def test_rendered_fingerprints_follow_final_section_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            candidates = []
            for idx in range(15):
                candidates.append(
                    {
                        "include": True,
                        "fingerprint": f"fp-{idx}",
                        "category": "media_layer",
                        "primary_block": "city_watch",
                        "title": f"Manchester council update {idx}",
                        "summary": "Manchester council confirmed a practical local update for residents.",
                        "lead": "",
                        "evidence_text": (
                            "Manchester council confirmed a practical local update for residents "
                            "with specific travel and service details for this week."
                        ),
                        "source_label": f"Source {idx}",
                        "source_url": f"https://example.test/{idx}",
                        "draft_line": (
                            "• Manchester Council подтвердил локальное обновление для жителей "
                            "с конкретными деталями по городским сервисам на этой неделе. "
                            "Перед поездками и записями сегодня проверьте источник и уточните "
                            "актуальные сроки."
                        ),
                    }
                )
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "run_date_london": "2026-05-20",
                        "candidates": candidates,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = write_digest(root)

            self.assertTrue(result.ok)
            report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["section_counts"]["Что важно сегодня"], 2)
            self.assertEqual(report["section_counts"]["Городской радар"], 12)
            self.assertEqual(report["quality_counts"]["rendered_candidates"], 14)
            self.assertEqual(len(report["rendered_candidate_fingerprints"]), 14)
            self.assertNotIn("fp-14", report["rendered_candidate_fingerprints"])

    def test_degraded_llm_shrink_holds_lower_priority_soft_section_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            candidates = []
            for idx in range(8):
                candidates.append(
                    {
                        "fingerprint": f"city-{idx}",
                        "include": True,
                        "category": "media_layer",
                        "primary_block": "city_watch",
                        "title": f"City item {idx}",
                        "lead": f"Manchester council confirmed useful local change {idx}.",
                        "summary": f"Residents get a concrete update with dates and affected services {idx}.",
                        "evidence_text": (
                            f"Manchester council confirmed useful local change {idx}. Residents get a concrete "
                            "update with dates, affected services, local impact, borough context, and a clear "
                            "reason to check whether the change affects their routine this week."
                        ),
                        "practical_angle": "Проверьте, касается ли это вашего района.",
                        "source_label": f"Source {idx}",
                        "source_url": f"https://example.test/{idx}",
                        "draft_line": (
                            f"• Manchester: совет подтвердил практическое изменение {idx}, которое касается жителей "
                            "района и ближайших сервисов. В тексте есть конкретный повод, дата и понятное действие "
                            "для читателя, поэтому пункт можно оценить без догадок."
                        ),
                        "reader_value_score": 100 - idx,
                    }
                )
            (state_dir / "candidates.json").write_text(json.dumps({"candidates": candidates}), encoding="utf-8")
            (state_dir / "llm_rewrite_report.json").write_text(
                json.dumps({"stage_status": "degraded", "warnings": ["LLM rewrite yield low"]}),
                encoding="utf-8",
            )

            result = write_digest(root)
            self.assertTrue(result.ok)
            report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))

            self.assertEqual(report["section_counts"]["Городской радар"], 5)
            self.assertEqual(report["degraded_shrink"]["dropped_count"], 3)
            self.assertEqual(len(report["rendered_candidate_fingerprints"]), 5)
            self.assertNotIn("city-7", report["rendered_candidate_fingerprints"])

    def test_borderline_candidate_is_held_not_rendered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "run_date_london": "2026-05-20",
                        "candidates": [
                            {
                                "include": True,
                                "fingerprint": "borderline-1",
                                "category": "media_layer",
                                "primary_block": "city_watch",
                                "editorial_status": "borderline",
                                "title": "Police appeal for help",
                                "summary": "Police appeal for help.",
                                "source_label": "Source",
                                "source_url": "https://example.test/borderline",
                                "draft_line": "• Полиция просит помочь. Проверьте детали.",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = write_digest(root)

            self.assertTrue(result.ok)
            report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["quality_counts"]["held_for_editorial_quality"], 1)
            self.assertNotIn("borderline-1", report["rendered_candidate_fingerprints"])

    def test_capped_sections_keep_higher_reader_value_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            candidates = []
            for idx in range(11):
                candidates.append(
                    {
                        "include": True,
                        "fingerprint": f"low-{idx}",
                        "category": "media_layer",
                        "primary_block": "last_24h",
                        "title": f"Award win for local team {idx}",
                        "summary": "A local organisation shared a general awards update.",
                        "lead": "",
                        "published_at": "2026-05-20T08:00:00+01:00",
                        "evidence_text": (
                            "A local organisation shared a general awards update for Manchester readers. "
                            "The item names Manchester, the organisation, the award, the local audience, "
                            "the background context, and enough detail to support a normal digest card. "
                            "The update does not announce a service change, a deadline, a disruption, "
                            "a council decision, a public safety issue, or a practical action for readers. "
                            "It is useful mainly as background context and should sit below more urgent news."
                        ),
                        "source_label": f"Low Source {idx}",
                        "source_url": f"https://example.test/low-{idx}",
                        "draft_line": (
                            "• Локальная организация сообщила об отраслевой награде и "
                            "обновила справочную информацию для жителей Манчестера, но без "
                            "нового решения, срока или практического изменения для города. "
                            "Если тема вам важна для контекста района, уточните детали в источнике."
                        ),
                    }
                )
            candidates.append(
                {
                    "include": True,
                    "fingerprint": "high-police",
                    "category": "media_layer",
                    "primary_block": "last_24h",
                    "title": "Police investigate stabbing in Manchester city centre",
                    "summary": "Police confirmed an investigation after a stabbing in Manchester city centre.",
                    "lead": "",
                    "published_at": "2026-05-20T08:00:00+01:00",
                    "evidence_text": (
                        "Police confirmed an investigation after a stabbing in Manchester city centre. "
                        "The source says officers are handling the incident and readers may need to "
                        "watch for local updates before travelling through nearby streets. The item "
                        "has a concrete public-safety subject, a specific city-centre location, and "
                        "a practical reason to monitor official updates during the day."
                    ),
                    "source_label": "High Source",
                    "source_url": "https://example.test/high",
                    "draft_line": (
                        "• Полиция расследует нападение с ножом в центре Манчестера; "
                        "это может повлиять на движение и доступ к улицам рядом с местом "
                        "инцидента в течение дня. Если вы рядом с этим районом сегодня, "
                        "следите за обновлениями служб и проверьте маршрут."
                    ),
                }
            )
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "run_date_london": "2026-05-20",
                        "candidates": candidates,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = write_digest(root)

            self.assertTrue(result.ok)
            report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))
            rendered = set(report["rendered_candidate_fingerprints"])
            self.assertIn("high-police", rendered)
            self.assertNotIn("low-10", rendered)


class EventQualityPipelineTest(unittest.TestCase):
    def _validate_one(self, candidate: dict) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "run_date_london": "2026-05-20",
                        "candidates": [candidate],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            result = validate_candidates(root)
            self.assertTrue(result.ok)
            payload = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            return payload["candidates"][0]

    def test_validator_hard_drops_event_without_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            candidate = {
                "include": True,
                "fingerprint": "event-1",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "title": "Workshop at The Gallery",
                "summary": "Free tickets for a workshop.",
                "lead": "",
                "evidence_text": "Free tickets for a workshop at The Gallery.",
                "source_label": "Venue",
                "source_url": "https://example.test/event",
                "dedupe_decision": "new",
                "reason": "New candidate.",
            }
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "run_date_london": "2026-05-20",
                        "candidates": [candidate],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = validate_candidates(root)

            self.assertTrue(result.ok)
            payload = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            [updated] = payload["candidates"]
            self.assertFalse(updated["include"])
            self.assertIn("no_date", updated["reject_reasons"])
            self.assertIn("no concrete upcoming date", updated["reason"])
            self.assertEqual(updated["event_quality"]["severity"], "hard")

    def test_deep_date_extraction_uses_evidence_before_event_gate(self) -> None:
        event_day = now_london().date() + timedelta(days=1)
        event_text = f"{event_day.day} {event_day.strftime('%B')} {event_day.year}"
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "deep-date",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "title": "The Coronation Street Experience",
                "summary": "Tickets for a visitor experience.",
                "lead": "",
                "evidence_text": f"The visitor experience runs at Visit Salford on {event_text} with timed tickets.",
                "source_label": "Visit Salford",
                "source_url": "https://example.test/corrie",
                "dedupe_decision": "new",
                "reason": "New candidate.",
            }
        )

        self.assertTrue(updated["include"])
        self.assertEqual(updated["event"]["date_start"], event_day.isoformat())
        self.assertTrue(updated["event_schema_completeness"]["applies"])

    def test_validator_soft_warns_under_specified_dated_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            event_day = now_london().date()
            event_text = f"{event_day.day} {event_day.strftime('%B')}"
            candidate = {
                "include": True,
                "fingerprint": "event-2",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "title": f"Workshop on {event_text} at The Gallery",
                "summary": "Free tickets for a workshop.",
                "lead": "",
                "evidence_text": f"Free tickets for a workshop on {event_text} at The Gallery.",
                "source_label": "Venue",
                "source_url": "https://example.test/event",
                "dedupe_decision": "new",
                "reason": "New candidate.",
            }
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "run_date_london": "2026-05-20",
                        "candidates": [candidate],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = validate_candidates(root)

            self.assertTrue(result.ok)
            payload = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            [updated] = payload["candidates"]
            self.assertTrue(updated["include"])
            self.assertEqual(updated["event_quality"]["severity"], "soft")
            self.assertIn("source_thin", updated["event_quality_warnings"])

    def test_validator_drops_stale_news_without_new_phase(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "old-news",
                "category": "council",
                "primary_block": "city_watch",
                "title": "Mayor comments on community concern",
                "summary": "A general statement was issued after an incident last month.",
                "lead": "",
                "evidence_text": "A general statement was issued after an incident last month.",
                "published_at": "2026-04-29T09:00:00+01:00",
                "source_label": "GMCA",
                "source_url": "https://example.test/old",
                "dedupe_decision": "new",
            }
        )

        self.assertFalse(updated["include"])
        self.assertIn("stale_no_new_phase", updated["reject_reasons"])

    def test_validator_blocks_cross_day_rehash_of_food_opening(self) -> None:
        """2026-05-25 complaint: GRUB Stretford shipped 4 days in a row.

        Same fingerprint already in yesterday's daily_index must be
        blocked today as cross_day_rehash — regardless of LLM-assigned
        change_type ('reminder' / 'same_story_new_facts').
        """
        from datetime import date, timedelta
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            (state_dir / "daily_index").mkdir(parents=True)
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            (state_dir / "daily_index" / f"{yesterday}.jsonl").write_text(
                json.dumps({
                    "fingerprint": "grub-stretford",
                    "included": True,
                    "title": "GRUB takes over Stretford car park",
                    "ts": yesterday,
                }) + "\n",
                encoding="utf-8",
            )
            (state_dir / "candidates.json").write_text(
                json.dumps({
                    "pipeline_run_id": "test",
                    "candidates": [{
                        "include": True,
                        "fingerprint": "grub-stretford",
                        "category": "food_openings",
                        "primary_block": "openings",
                        "title": "GRUB opens new street food market at Stretford car park",
                        "summary": "Reminder about the foodhall opening",
                        "evidence_text": "Bakery and street food market opens at Stretford multi-storey.",
                        "published_at": (date.today() - timedelta(days=4)).isoformat() + "T09:00:00+01:00",
                        "source_label": "Manchester's Finest",
                        "source_url": "https://example.test/grub-stretford-opens-food-market",
                        "dedupe_decision": "new",
                    }],
                }, ensure_ascii=False),
                encoding="utf-8",
            )
            validate_candidates(root)
            payload = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            cand = payload["candidates"][0]
            self.assertFalse(
                cand["include"],
                msg=f"Cross-day rehash should be blocked but include={cand['include']}; reason={cand.get('reason')}",
            )
            self.assertIn("cross_day_rehash", cand.get("reject_reasons") or [])

    def test_validator_does_not_block_cross_day_for_transport_disruption(self) -> None:
        """Ongoing transport disruption legitimately re-appears each day —
        operational blocks are exempt from cross_day_rehash."""
        from datetime import date, timedelta
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            (state_dir / "daily_index").mkdir(parents=True)
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            (state_dir / "daily_index" / f"{yesterday}.jsonl").write_text(
                json.dumps({
                    "fingerprint": "rochdale-line-works",
                    "included": True,
                    "title": "Rochdale Line - Tram Improvement Works",
                    "ts": yesterday,
                }) + "\n",
                encoding="utf-8",
            )
            (state_dir / "candidates.json").write_text(
                json.dumps({
                    "pipeline_run_id": "test",
                    "candidates": [{
                        "include": True,
                        "fingerprint": "rochdale-line-works",
                        "category": "transport",
                        "primary_block": "transport",
                        "title": "Rochdale Line - Tram Improvement Works",
                        "summary": "Works continue until 29 May",
                        "evidence_text": "Replacement bus until 29 May between Victoria and Rochdale Town Centre.",
                        "published_at": date.today().isoformat() + "T09:00:00+01:00",
                        "source_label": "TfGM",
                        "source_url": "https://tfgm.com/travel-updates/rochdale-line-works",
                        "dedupe_decision": "new",
                    }],
                }, ensure_ascii=False),
                encoding="utf-8",
            )
            validate_candidates(root)
            payload = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))
            cand = payload["candidates"][0]
            self.assertTrue(
                cand["include"],
                msg="Transport disruption must not be blocked by cross_day_rehash",
            )
            self.assertNotIn("cross_day_rehash", cand.get("reject_reasons") or [])

    def test_validator_holds_unclear_why_now_for_manual_review(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "unclear-why-now",
                "category": "council",
                "primary_block": "last_24h",
                "title": "Council shares general community statement",
                "summary": "The council shared a general community statement with no new decision or date.",
                "lead": "",
                "evidence_text": (
                    "The council shared a general community statement with broad comments "
                    "about local priorities and partnership work."
                ),
                "published_at": (now_london() - timedelta(days=2)).replace(hour=9, minute=0, second=0, microsecond=0).isoformat(),
                "source_label": "Council Source",
                "source_url": "https://example.test/unclear",
                "dedupe_decision": "new",
            }
        )

        self.assertTrue(updated["include"])
        self.assertEqual(updated["why_now"], "unclear")
        self.assertEqual(updated["editorial_status"], "borderline")
        self.assertIn("why_now_unclear:unclear", updated["quality_warnings"])
        self.assertEqual(updated["primary_block"], "city_watch")

    def test_validator_drops_stale_and_far_future_food_openings(self) -> None:
        stale = self._validate_one(
            {
                "include": True,
                "fingerprint": "trof",
                "category": "food_openings",
                "primary_block": "openings",
                "title": "Trof reopens in Northern Quarter on 1 May",
                "summary": "The pub reopens on 1 May in Manchester.",
                "lead": "",
                "evidence_text": "The pub reopens on 1 May in Manchester.",
                "published_at": "2026-05-01T09:00:00+01:00",
                "source_label": "Food Source",
                "source_url": "https://example.test/trof",
                "dedupe_decision": "new",
            }
        )
        future = self._validate_one(
            {
                "include": True,
                "fingerprint": "counter",
                "category": "food_openings",
                "primary_block": "openings",
                "title": "The Counter opens on September 10 on John Dalton Street",
                "summary": "A restaurant is due to open on September 10 in Manchester.",
                "lead": "",
                "evidence_text": "A restaurant is due to open on September 10 in Manchester.",
                "published_at": "2026-05-20T09:00:00+01:00",
                "source_label": "Food Source",
                "source_url": "https://example.test/counter",
                "dedupe_decision": "new",
            }
        )

        self.assertIn("stale_opening", stale["reject_reasons"])
        self.assertIn("future_opening_too_early", future["reject_reasons"])

    def test_validator_keeps_today_food_opening(self) -> None:
        today = now_london().date()
        date_text = f"{today.day} {today.strftime('%B')}"
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "bunsik",
                "category": "food_openings",
                "primary_block": "openings",
                "title": f"Bunsik opens at Trafford Centre on {date_text}",
                "summary": f"The Trafford Centre opening runs {date_text} with launch offers.",
                "lead": "",
                "evidence_text": f"The Trafford Centre opening runs {date_text} with launch offers.",
                "published_at": f"{today.isoformat()}T09:00:00+01:00",
                "source_label": "Food Source",
                "source_url": "https://example.test/bunsik",
                "dedupe_decision": "new",
            }
        )

        self.assertTrue(updated["include"])

    def test_crime_specificity_demotes_borderline_after_enrichment(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "crime-appeal",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "Police appeal after Moss Side stabbing",
                "summary": "Police are appealing for information after a stabbing in Moss Side.",
                "lead": "",
                "evidence_text": (
                    "Police are appealing for information after a stabbing in Moss Side. "
                    "Officers asked witnesses to share mobile phone footage."
                ),
                "enrichment_status": "article_html",
                "source_label": "The Manc",
                "source_url": "https://example.test/crime",
                "published_at": "2026-05-20T08:00:00+01:00",
                "dedupe_decision": "new",
            }
        )

        self.assertTrue(updated["include"])
        self.assertEqual(updated["primary_block"], "city_watch")
        self.assertEqual(updated["editorial_status"], "borderline")
        self.assertIn("crime", updated["specificity_review"])

    def test_crime_specificity_rejects_unreadable_police_stub(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "crime-stub",
                "category": "media_layer",
                "primary_block": "last_24h",
                "title": "Police appeal for help",
                "summary": "Police appeal for help.",
                "lead": "",
                "evidence_text": "Police appeal for help.",
                "source_label": "Source",
                "source_url": "https://example.test/stub",
                "published_at": "2026-05-20T08:00:00+01:00",
                "dedupe_decision": "new",
            }
        )

        self.assertFalse(updated["include"])
        self.assertTrue(any("crime_too_unclear" in r for r in updated["reject_reasons"]))

    def test_property_specificity_borderline_not_blind_reject(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "property-office",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "Office building could become 34 flats",
                "summary": "A developer submitted plans to convert an office building into apartments.",
                "lead": "",
                "evidence_text": (
                    "A developer submitted plans to convert an office building into apartments. "
                    "The application is being considered by the council."
                ),
                "enrichment_status": "article_html",
                "source_label": "Local Source",
                "source_url": "https://example.test/property",
                # Relative recent date: a hardcoded 2026-05-20 turned this
                # into a "stale, 8 days old" reject once the clock moved
                # past 2026-05-27, masking the property-borderline path.
                "published_at": (now_london() - timedelta(days=1)).isoformat(),
                "dedupe_decision": "new",
            }
        )

        self.assertTrue(updated["include"])
        self.assertEqual(updated["editorial_status"], "borderline")
        self.assertIn("property", updated["specificity_review"])

    def test_property_specificity_allows_known_place_without_address(self) -> None:
        updated = self._validate_one(
            {
                "include": True,
                "fingerprint": "arndale-sale",
                "category": "media_layer",
                "primary_block": "city_watch",
                "title": "Manchester Arndale shopping centre owner confirms sale plan",
                "summary": "The Manchester Arndale owner confirmed a property sale plan.",
                "lead": "",
                "evidence_text": "Manchester Arndale shopping centre owner confirmed a property sale plan.",
                "source_label": "Local Source",
                "source_url": "https://example.test/arndale",
                "published_at": (now_london() - timedelta(days=1)).isoformat(),
                "dedupe_decision": "new",
            }
        )

        self.assertTrue(updated["include"])
        self.assertNotEqual(updated.get("editorial_status"), "borderline")


class TicketRadarPolicyTest(unittest.TestCase):
    def test_ticket_radar_uses_annual_discovery_horizon(self) -> None:
        self.assertGreaterEqual(_TICKET_HORIZON_DAYS, 365)

    def test_vague_ending_is_scrubbed_without_dropping_item(self) -> None:
        line, removed = scrub_vague_ending(
            "• Manchester Council подтвердил новое решение по жилью. Это важный сигнал."
        )

        self.assertEqual(line, "• Manchester Council подтвердил новое решение по жилью.")
        self.assertIn("это важный сигнал", removed)


class SourceFunnelDiagnosticsTest(unittest.TestCase):
    def test_source_status_carries_reject_reasons_for_any_source(self) -> None:
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "a",
                    "source_label": "Ticketmaster Manchester Upcoming",
                    "include": False,
                    "reject_reasons": ["regular_upcoming_non_major"],
                }
            ]
        }
        scan_report = {
            "categories": {
                "venues_tickets": {
                    "source_health": [
                        {
                            "name": "Ticketmaster Manchester Upcoming",
                            "candidate_count": 1,
                            "errors": [],
                        }
                    ]
                }
            }
        }

        status = _summarise_source_health(scan_report, candidates_report, set(), {})
        [row] = status["sources"]
        self.assertEqual(row["raw_count"], 1)
        self.assertEqual(row["accepted_count"], 0)
        self.assertEqual(row["rendered_count"], 0)
        self.assertEqual(row["reject_reasons"]["regular_upcoming_non_major"], 1)

    def test_not_modified_source_is_no_new_material_not_empty(self) -> None:
        scan_report = {
            "categories": {
                "culture_weekly": {
                    "source_health": [
                        {
                            "name": "Spinningfields Makers Market",
                            "fetched": True,
                            "not_modified": True,
                            "candidate_count": 0,
                            "fresh_last_24h_count": 0,
                            "errors": [],
                            "warnings": ["304 Not Modified — no new content since last fetch"],
                        }
                    ]
                }
            }
        }

        status = _summarise_source_health(scan_report, {"candidates": []}, set(), {})
        [row] = status["sources"]

        self.assertEqual(row["status"], "stale")
        self.assertEqual(status["counts"]["empty"], 0)
        self.assertEqual(status["counts"]["stale"], 1)

    def test_transport_coverage_distinguishes_checked_empty_from_not_checked(self) -> None:
        checked = _summarise_transport_coverage(
            {"categories": {"transport": {"checked": True, "source_health": [{"name": "TfGM", "candidate_count": 0}]}}},
            {"candidates": []},
            set(),
        )
        missing = _summarise_transport_coverage({"categories": {"transport": {"checked": False}}}, {"candidates": []}, set())

        self.assertEqual(checked["verdict"], "checked_no_disruptions")
        self.assertEqual(missing["verdict"], "not_checked")

    def test_transport_coverage_counts_tfgm_tram_alerts_as_metrolink_checked(self) -> None:
        coverage = _summarise_transport_coverage(
            {
                "categories": {
                    "transport": {
                        "checked": True,
                        "source_health": [{"name": "TfGM", "candidate_count": 2}],
                    }
                }
            },
            {
                "candidates": [
                    {
                        "include": True,
                        "fingerprint": "rochdale-line",
                        "primary_block": "transport",
                        "source_label": "TfGM",
                        "title": "Rochdale Line - Tram Improvement Works",
                    }
                ]
            },
            {"rochdale-line"},
        )

        self.assertTrue(coverage["tfgm_checked"])
        self.assertTrue(coverage["metrolink_checked"])
        self.assertEqual(coverage["verdict"], "disruptions_rendered")

    def test_diaspora_diagnostics_explains_empty_block(self) -> None:
        source_status = {
            "sources": [
                {
                    "name": "EventFirst Diaspora",
                    "category": "diaspora_events",
                    "raw_count": 2,
                    "accepted_count": 0,
                    "rendered_count": 0,
                    "reject_reasons": {"geo_filter": 2},
                }
            ]
        }
        diag = _summarise_diaspora_diagnostics(
            {"categories": {"diaspora_events": {"checked": True}}},
            source_status,
        )

        self.assertEqual(diag["verdict"], "fetched_but_filtered")
        self.assertEqual(diag["raw_count"], 2)

    def test_rendered_html_review_reads_actual_html_not_candidates_only(self) -> None:
        html = '<b>Городской радар</b>\n• Непонятный пункт. <a href="https://example.test/a">Source</a>\n'
        review = _classify_rendered_html_quality(
            html,
            {
                "candidates": [
                    {
                        "fingerprint": "a",
                        "source_url": "https://example.test/a",
                        "title": "Unclear item",
                        "source_label": "Source",
                        "editorial_status": "borderline",
                    }
                ]
            },
        )

        self.assertEqual(review["counts"]["visible_lines"], 1)
        self.assertEqual(review["counts"]["bad_visible_items"], 1)

    def test_borderline_queue_includes_manual_include_hint(self) -> None:
        queue = _borderline_queue(
            {
                "candidates": [
                    {
                        "fingerprint": "b1",
                        "title": "Borderline property item",
                        "source_label": "Source",
                        "editorial_status": "borderline",
                        "quality_warnings": ["property_borderline:specific_location"],
                    }
                ]
            },
            {"dropped_candidates": []},
        )

        self.assertEqual(queue["counts"]["borderline"], 1)
        self.assertIn("force_include", queue["items"][0]["manual_include_hint"])

    def test_quality_scorecard_and_feedback_snapshot_are_rendered_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            candidates_report = {
                "candidates": [
                    {
                        "fingerprint": "shown",
                        "title": "Shown item",
                        "source_label": "BBC Manchester",
                        "category": "venues_tickets",
                        "primary_block": "ticket_radar",
                        "ticket_type": "major_upcoming",
                        "scoring_trace": {"reader_value_score": 80},
                    },
                    {
                        "fingerprint": "hidden",
                        "title": "Hidden item",
                        "source_label": "MEN",
                        "category": "media_layer",
                        "primary_block": "city_watch",
                    },
                ]
            }
            scorecard = _quality_scorecard(
                state_dir=state_dir,
                current_day_london="2026-05-20",
                candidates_report=candidates_report,
                writer_report={},
                rendered_fingerprints={"shown"},
                source_status={"counts": {"zero_yield": 0}},
                published_review={"counts": {"suspiciously_published": 0}},
                transport_coverage={"verdict": "checked_no_disruptions"},
            )
            feedback = _update_feedback_items(state_dir, "2026-05-20", candidates_report, {"shown"})

            self.assertEqual(scorecard["today"]["visible_count"], 1)
            self.assertEqual(scorecard["today"]["full_count"], 2)
            self.assertEqual(scorecard["today"]["ticket_types"]["major_upcoming"]["published"], 1)
            self.assertEqual(feedback["rendered_items_recorded_today"], 1)
            payload = json.loads((state_dir / "personalization_feedback.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["items"][0]["fingerprint"], "shown")


class AdminAlertCopyTest(unittest.TestCase):
    def test_quality_signal_copy_uses_visible_items_and_target_range(self) -> None:
        text = _translate_health_signal(
            {
                "name": "too_many_items",
                "detail": "74 item(s) rendered — above the 22-item editorial cap target.",
            }
        )

        self.assertIn("74 опубликованных пунктов", text)
        self.assertIn("14–45", text)

    def test_pipeline_config_includes_transport_fill_between_curator_and_rewrite(self) -> None:
        buf = StringIO()
        with redirect_stdout(buf):
            code = cmd_pipeline_config()

        self.assertEqual(code, 0)
        pipeline = json.loads(buf.getvalue())["pipeline"]
        self.assertLess(pipeline.index("curator-pass"), pipeline.index("transport-fill"))
        self.assertLess(pipeline.index("transport-fill"), pipeline.index("llm-rewrite"))

    def test_admin_support_copy_uses_product_language_not_pipeline_jargon(self) -> None:
        self.assertIn("не ясно, что именно произошло", _humanize_quality_warning("property_borderline:decision_or_action"))
        self.assertIn("не Greater Manchester", _humanize_source_reason("Curator drop: Событие в Лондоне, не относится к GM."))
        self.assertEqual(_diaspora_verdict_human("fetched_but_filtered"), "события нашлись, но все отсеялись фильтрами")
        self.assertEqual(
            _source_counts_phrase({"raw_count": 2, "accepted_count": 1, "rendered_count": 0}),
            "нашли 2, прошло отбор 1, опубликовано 0",
        )
        self.assertIn("5xx", _explain_source_failure("HTTP 500 server error"))

    def test_admin_support_surfaces_missing_operational_sections(self) -> None:
        rows = _section_shape_rows({"section_counts": {"Билеты / Ticket Radar": 6, "Что важно сегодня": 2}})
        by_name = {row["section"]: row for row in rows}
        # 2026-05-29: an uncapped ticket rail buried football and hard news.
        self.assertEqual(by_name["Билеты / Ticket Radar"]["max"], 6)
        self.assertEqual(by_name["Что важно сегодня"]["status"], "ниже минимума")
        self.assertEqual(_section_name_human("Билеты / Ticket Radar"), "Билеты и концерты")
        self.assertIn(
            "нет опубликованных пунктов",
            _humanize_borough_flag("В 1 GM borough(s) ноль видимых пунктов: Tameside."),
        )
        self.assertEqual(_ticket_type_human("presale_soon"), "скоро пресейл/старт продаж")
        ticket_rows = _ticketmaster_rows(
            {"sources": [{"name": "Ticketmaster Manchester Upcoming"}, {"name": "BBC Manchester"}]}
        )
        self.assertEqual(len(ticket_rows), 1)
        self.assertIn(
            "спорно",
            _borderline_verdict({"quality_warnings": ["crime_borderline:what_happened"]}),
        )
        self.assertIn(
            "ошибку извлечения",
            _borderline_verdict({"quality_warnings": ["event_schema_missing:no_date"]}),
        )

        issues = _support_top_issues(
            rendered=47,
            health_level="at_risk",
            health_signals=[],
            writer_report={},
            transport_coverage={"metrolink_checked": False},
            quality_scorecard={"today": {"ticket_types": {"unknown": {"fetched": 47, "published": 6}}}},
            source_status={"counts": {"failed": 1}},
            synthetic_freshness={"stale_count": 0},
            prompt_drift=[],
            cost_summary={"unknown_priced_models": []},
            warnings=["LLM rewrite was degraded; writer/release quality gates handled the remaining candidates."],
            suspicious_rejects=[],
            suspicious_published=[],
            borderline_queue={"counts": {"borderline": 48}},
        )
        joined = " ".join(title for title, _ in issues)
        self.assertIn("Выпуск раздут", joined)
        self.assertIn("Генерация текста", joined)
        self.assertIn("Metrolink", joined)
        forbidden = ("LLM rewrite", "provider/model", "Ticket Radar", "synthetic-карточки", "Prompt drift")
        for phrase in forbidden:
            self.assertNotIn(phrase, joined)

        event_issues = _support_top_issues(
            rendered=18,
            health_level="healthy",
            health_signals=[],
            writer_report={},
            transport_coverage={"metrolink_checked": True},
            quality_scorecard={"today": {"ticket_types": {}}},
            source_status={"counts": {"failed": 0}},
            synthetic_freshness={"stale_count": 0},
            prompt_drift=[],
            cost_summary={"unknown_priced_models": []},
            warnings=[],
            suspicious_rejects=[],
            suspicious_published=[],
            borderline_queue={"counts": {"borderline": 0}},
            event_miss_review={"counts": {"critical_misses": 2}},
        )
        self.assertIn("Событийный pipeline", event_issues[0][0])

    def test_product_support_report_is_human_sized_and_hides_technical_dump(self) -> None:
        report = {
            "run_date_london": "2026-05-23",
            "release_decision": "pass",
            "warnings": ["LLM rewrite was degraded; writer/release quality gates handled the remaining candidates."],
            "digest_health": {"risk_level": "at_risk", "signals": []},
            "quality_scorecard": {
                "today": {
                    "ticket_types": {
                        "old_public_sale": {"fetched": 28, "published": 5},
                        "unknown": {"fetched": 18, "published": 1},
                    }
                }
            },
            "event_miss_review": {
                "counts": {"critical_misses": 2},
                "critical_misses": [
                    {
                        "title": "Sasha & John Digweed Manchester 2026",
                        "verdict": "dedupe_lost_event",
                        "kept_title": "Bulletin of the John Rylands Library",
                        "days_out": 1,
                        "score": 9,
                    },
                    {
                        "title": "Neddy Goes To Glasto",
                        "verdict": "writer_dropped_event",
                        "days_out": 0,
                        "score": 8,
                    },
                ],
            },
            "borderline_queue": {
                "counts": {"borderline": 52},
                "items": [
                    {"title": "Man shot in Prestwich", "quality_warnings": ["crime_borderline:what_happened"]},
                    {"title": "Office building plan", "quality_warnings": ["property_borderline:decision_or_action"]},
                ],
            },
            "transport_coverage": {
                "verdict": "disruptions_rendered",
                "tfgm_checked": True,
                "metrolink_checked": False,
                "national_rail_checked": True,
                "disruptions_found": 3,
                "disruptions_rendered": 3,
            },
            "source_status": {
                "counts": {"ok": 72, "failed": 0, "empty": 19, "stale": 16, "zero_yield": 61},
                "sources": [],
            },
        }
        writer = {
            "quality_counts": {
                "included_candidates": 124,
                "rendered_candidates": 47,
                "dropped_missing_draft_line": 3,
                "dropped_low_quality": 9,
            },
            "section_counts": {
                "Городской радар": 9,
                "Выходные в GM": 8,
                "Билеты / Ticket Radar": 6,
                "Погода": 1,
            },
            "degraded_shrink": {"enabled": True, "dropped_count": 7},
        }
        text = _build_product_support_text(report, writer)

        self.assertIn("опубликовано 47 пунктов", text)
        self.assertIn("Возможные пропуски", text)
        self.assertIn("ложный дубль", text)
        self.assertIn("Metrolink: не проверен отдельно", text)
        self.assertIn("Осторожный режим: удержано 7", text)
        self.assertIn("полный список скрыт из Telegram", text)
        for forbidden in ("fingerprint", "rendered", "accepted", "fetched=", "zero-yield"):
            self.assertNotIn(forbidden, text)


class PublishedReviewTest(unittest.TestCase):
    def test_event_miss_review_flags_false_duplicate_weekend_event(self) -> None:
        candidates = [
            {
                "fingerprint": "flower",
                "title": "The Manchester Flower Festival",
                "source_label": "Manchester Flower Festival CityCo News",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "include": False,
                "reason": "Intra-batch topic duplicate — same story kept from stronger source.",
                "summary": "Free festival at St Ann's Square and King Street.",
                "event": {
                    "date_start": "2026-05-23",
                    "date_text": "23-25 May 2026",
                    "price": "free",
                    "borough": "Manchester",
                },
            },
            {
                "fingerprint": "germaine",
                "title": "A Possibility | Germaine Kruip | Manchester International Festival 2025",
                "source_label": "Factory International",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "include": True,
                "event": {"date_start": "2026-05-24"},
            },
        ]

        review = _event_miss_review(
            {"candidates": candidates},
            {},
            rendered_fingerprints={"germaine"},
            current_day_london="2026-05-22",
            dedupe_memory={
                "intra_batch_dedup_drops": [
                    {
                        "fingerprint": "flower",
                        "kept_fingerprint": "germaine",
                        "kept_title": "A Possibility | Germaine Kruip | Manchester International Festival 2025",
                        "kept_source_label": "Factory International",
                    }
                ]
            },
        )

        self.assertEqual(review["counts"]["critical_misses"], 1)
        self.assertEqual(review["critical_misses"][0]["verdict"], "dedupe_lost_event")

    def test_event_miss_review_ignores_duplicate_covered_by_rendered_item(self) -> None:
        candidates = [
            {
                "fingerprint": "flower-duplicate",
                "title": "The Manchester Flower Festival",
                "source_label": "Manchester Flower Festival",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "include": False,
                "summary": "Free festival at St Ann's Square and King Street.",
                "event": {"date_start": "2026-05-23", "date_text": "23-25 May 2026", "price": "free", "borough": "Manchester"},
            },
            {
                "fingerprint": "flower-winner",
                "title": "The Manchester Flower Festival returns for 2026",
                "source_label": "Manchester Flower Festival CityCo News",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "include": True,
                "event": {"date_start": "2026-05-23"},
            },
        ]

        review = _event_miss_review(
            {"candidates": candidates},
            {},
            rendered_fingerprints={"flower-winner"},
            current_day_london="2026-05-22",
            dedupe_memory={
                "intra_batch_dedup_drops": [
                    {"fingerprint": "flower-duplicate", "kept_fingerprint": "flower-winner"}
                ]
            },
        )

        self.assertEqual(review["counts"]["critical_misses"], 0)
        self.assertEqual(review["items"][0]["verdict"], "covered_by_rendered_duplicate")

    def test_event_miss_warning_does_not_block_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            run_date = now_london().strftime("%Y-%m-%d")
            run_id = "event-miss-warning-test"
            categories = {
                key: {
                    "checked": True,
                    "usable_for_release": True,
                    "candidate_count": 1,
                    "publishable_count": 1,
                    "sources": [],
                    "source_health": [],
                }
                for key in REQUIRED_SCAN_CATEGORIES
            }
            categories["public_services"]["active_disruption_today"] = False
            (state_dir / "collector_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "categories": categories,
            }), encoding="utf-8")
            candidates = [
                {
                    "fingerprint": "city-1",
                    "title": "Manchester council confirms service change",
                    "source_label": "Manchester Council",
                    "source_url": "https://example.test/city-1",
                    "category": "council",
                    "primary_block": "today_focus",
                    "include": True,
                    "dedupe_decision": "new",
                    "summary": "Manchester council confirmed a service change today.",
                },
                {
                    "fingerprint": "city-2",
                    "title": "Stockport transport works confirmed",
                    "source_label": "BBC Manchester",
                    "source_url": "https://example.test/city-2",
                    "category": "media_layer",
                    "primary_block": "last_24h",
                    "include": True,
                    "dedupe_decision": "new",
                    "freshness_status": "fresh_24h",
                    "summary": "A local transport update was confirmed today.",
                },
                {
                    "fingerprint": "missed-event",
                    "title": "The Manchester Flower Festival",
                    "source_label": "Manchester Flower Festival CityCo News",
                    "source_url": "https://example.test/flower",
                    "category": "culture_weekly",
                    "primary_block": "weekend_activities",
                    "include": False,
                    "dedupe_decision": "drop",
                    "reason": "Intra-batch topic duplicate — same story kept from stronger source.",
                    "summary": "Free festival at St Ann's Square and King Street.",
                    "event": {
                        "date_start": run_date,
                        "date_text": "today",
                        "price": "free",
                        "borough": "Manchester",
                    },
                },
            ]
            (state_dir / "candidates.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "candidates": candidates,
            }), encoding="utf-8")
            (state_dir / "curator_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "status": "complete",
                "reviewed": 2,
            }), encoding="utf-8")
            (state_dir / "llm_rewrite_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "stage_status": "complete",
            }), encoding="utf-8")
            (state_dir / "writer_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "stage_status": "complete",
                "rendered_candidate_fingerprints": ["city-1", "city-2"],
                "quality_counts": {"included_candidates": 2, "rendered_candidates": 2},
                "section_counts": {"Погода": 1, "Что важно сегодня": 1, "Что произошло за 24 часа": 1},
                "dropped_candidates": [],
            }), encoding="utf-8")
            (state_dir / "editor_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "stage_status": "complete",
            }), encoding="utf-8")
            (state_dir / "dedupe_memory.json").write_text(json.dumps({
                "intra_batch_dedup_drops": [
                    {
                        "fingerprint": "missed-event",
                        "kept_fingerprint": "unrendered-other-event",
                        "kept_title": "Other unrelated festival",
                    }
                ]
            }), encoding="utf-8")
            (state_dir / "draft_digest.html").write_text(
                f"<b>Greater Manchester Brief — {run_date}, 08:00</b>\n\n"
                "<b>Погода</b>\n"
                "• Погода: 12-16°C. <a href=\"https://example.test/weather\">Met Office</a>\n\n"
                "<b>Что важно сегодня</b>\n"
                "• Manchester council confirms service change. <a href=\"https://example.test/city-1\">Manchester Council</a>\n\n"
                "<b>Что произошло за 24 часа</b>\n"
                "• Stockport transport works confirmed. <a href=\"https://example.test/city-2\">BBC Manchester</a>\n",
                encoding="utf-8",
            )

            result = build_release(root)
            report = json.loads((state_dir / "release_report.json").read_text(encoding="utf-8"))

            self.assertTrue(result.ok)
            self.assertEqual(report["release_decision"], "pass")
            self.assertEqual(report["errors"], [])
            self.assertEqual(report["event_miss_review"]["counts"]["critical_misses"], 1)
            self.assertTrue(any("Event miss review" in warning for warning in report["warnings"]))

    def test_public_services_source_failure_does_not_block_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            run_date = now_london().strftime("%Y-%m-%d")
            run_id = "public-services-warning-test"
            categories = {
                key: {
                    "checked": True,
                    "usable_for_release": True,
                    "candidate_count": 1,
                    "publishable_count": 1,
                    "sources": [],
                    "source_health": [],
                }
                for key in REQUIRED_SCAN_CATEGORIES
            }
            categories["public_services"].update({
                "usable_for_release": False,
                "candidate_count": 0,
                "publishable_count": 0,
                "active_disruption_today": False,
                "errors": ["GMMH: Remote end closed connection without response"],
                "source_health": [
                    {
                        "name": "GMMH",
                        "status": "failed",
                        "candidate_count": 0,
                        "publishable_count": 0,
                    }
                ],
            })
            (state_dir / "collector_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "categories": categories,
            }), encoding="utf-8")
            candidates = [
                {
                    "fingerprint": "city-1",
                    "title": "Manchester council confirms service change",
                    "source_label": "Manchester Council",
                    "source_url": "https://example.test/city-1",
                    "category": "council",
                    "primary_block": "today_focus",
                    "include": True,
                    "dedupe_decision": "new",
                    "summary": "Manchester council confirmed a service change today.",
                },
                {
                    "fingerprint": "city-2",
                    "title": "Stockport transport works confirmed",
                    "source_label": "BBC Manchester",
                    "source_url": "https://example.test/city-2",
                    "category": "media_layer",
                    "primary_block": "last_24h",
                    "include": True,
                    "dedupe_decision": "new",
                    "freshness_status": "fresh_24h",
                    "summary": "A local transport update was confirmed today.",
                },
            ]
            (state_dir / "candidates.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "candidates": candidates,
            }), encoding="utf-8")
            (state_dir / "curator_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "status": "complete",
                "reviewed": 2,
            }), encoding="utf-8")
            (state_dir / "llm_rewrite_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "stage_status": "complete",
            }), encoding="utf-8")
            (state_dir / "writer_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "stage_status": "complete",
                "rendered_candidate_fingerprints": ["city-1", "city-2"],
                "quality_counts": {"included_candidates": 2, "rendered_candidates": 2},
                "section_counts": {"Погода": 1, "Что важно сегодня": 1, "Что произошло за 24 часа": 1},
                "dropped_candidates": [],
            }), encoding="utf-8")
            (state_dir / "editor_report.json").write_text(json.dumps({
                "pipeline_run_id": run_id,
                "run_date_london": run_date,
                "stage_status": "complete",
            }), encoding="utf-8")
            (state_dir / "draft_digest.html").write_text(
                f"<b>Greater Manchester Brief — {run_date}, 08:00</b>\n\n"
                "<b>Погода</b>\n"
                "• Погода: 12-16°C. <a href=\"https://example.test/weather\">Met Office</a>\n\n"
                "<b>Что важно сегодня</b>\n"
                "• Manchester council confirms service change. <a href=\"https://example.test/city-1\">Manchester Council</a>\n\n"
                "<b>Что произошло за 24 часа</b>\n"
                "• Stockport transport works confirmed. <a href=\"https://example.test/city-2\">BBC Manchester</a>\n",
                encoding="utf-8",
            )

            result = build_release(root)
            report = json.loads((state_dir / "release_report.json").read_text(encoding="utf-8"))

            self.assertTrue(result.ok)
            self.assertEqual(report["release_decision"], "pass")
            self.assertFalse(any("public services" in error for error in report["errors"]))

    def test_release_review_flags_bad_visible_items(self) -> None:
        today = now_london().date()
        old_news_day = today - timedelta(days=25)
        stale_food_day = today - timedelta(days=10)
        candidates = [
            {
                "fingerprint": "gmca-old",
                "title": "Mayor comments on community concern",
                "source_label": "GMCA",
                "category": "council",
                "primary_block": "city_watch",
                "published_at": f"{old_news_day.isoformat()}T09:00:00+01:00",
                "summary": "A general statement was issued after an old incident.",
            },
            {
                "fingerprint": "food-stale",
                "title": "Trof reopens in Northern Quarter",
                "source_label": "Food Source",
                "category": "food_openings",
                "primary_block": "openings",
                "published_at": f"{stale_food_day.isoformat()}T09:00:00+01:00",
                "event": {"date_start": stale_food_day.isoformat()},
            },
            {
                "fingerprint": "food-current",
                "title": "Bunsik opens at Trafford Centre",
                "source_label": "Food Source",
                "category": "food_openings",
                "primary_block": "openings",
                "published_at": f"{today.isoformat()}T09:00:00+01:00",
                "event": {"date_start": today.isoformat()},
            },
        ]

        review = _classify_published_candidates(
            {"candidates": candidates},
            {"gmca-old", "food-stale", "food-current"},
        )

        titles = {item["fingerprint"] for item in review["suspiciously_published"]}
        self.assertEqual(review["counts"]["suspiciously_published"], 2)
        self.assertEqual(review["counts"]["warning_visible_items"], 2)
        self.assertIn("gmca-old", titles)
        self.assertIn("food-stale", titles)
        self.assertNotIn("food-current", titles)

    def test_digest_health_cap_is_warning_only(self) -> None:
        health = _translate_health_signal(
            {
                "name": "too_many_items",
                "detail": "74 item(s) rendered — above the 22-item editorial cap target.",
            }
        )

        self.assertIn("Выпуск раздут", health)
        self.assertIn("74", health)

    def test_anti_golden_2026_05_20_visible_garbage_is_warning_only(self) -> None:
        today = now_london().date()
        old_news_day = today - timedelta(days=25)
        stale_food_day = today - timedelta(days=10)
        candidates = [
            {
                "fingerprint": "old-gmca",
                "title": "Mayor statement after Golders Green incident",
                "source_label": "GMCA",
                "category": "council",
                "primary_block": "city_watch",
                "published_at": f"{old_news_day.isoformat()}T09:00:00+01:00",
                "summary": "Mayor comments on community concern after an old incident.",
            },
            {
                "fingerprint": "trof-stale",
                "title": "Trof reopens in Northern Quarter",
                "source_label": "Manchester's Finest",
                "category": "food_openings",
                "primary_block": "openings",
                "published_at": f"{stale_food_day.isoformat()}T09:00:00+01:00",
                "event": {"date_start": stale_food_day.isoformat()},
            },
            {
                "fingerprint": "bunsik-today",
                "title": "Bunsik opens at Trafford Centre",
                "source_label": "About Manchester",
                "category": "food_openings",
                "primary_block": "openings",
                "published_at": f"{today.isoformat()}T09:00:00+01:00",
                "event": {"date_start": today.isoformat()},
            },
        ]

        review = _classify_published_candidates(
            {"candidates": candidates},
            {"old-gmca", "trof-stale", "bunsik-today"},
        )

        self.assertEqual(review["counts"]["warning_visible_items"], 2)
        self.assertEqual(
            {item["fingerprint"] for item in review["suspiciously_published"]},
            {"old-gmca", "trof-stale"},
        )


class DailyIndexSnapshotTest(unittest.TestCase):
    def test_snapshot_includes_reader_value_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "test-run",
                        "candidates": [
                            {
                                "include": True,
                                "fingerprint": "transport-1",
                                "category": "transport",
                                "primary_block": "transport",
                                "title": "Metrolink disruption in Manchester",
                                "summary": "Passengers should check routes.",
                                "source_label": "TfGM",
                                "source_url": "https://example.test/tfgm",
                                "change_type": "new_story",
                                "why_now": "ongoing_disruption",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (state_dir / "writer_report.json").write_text("{}", encoding="utf-8")

            path = write_daily_index_snapshot(root)

            self.assertIsNotNone(path)
            [record] = [
                json.loads(line)
                for line in Path(path).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertIn("reader_value_score", record)
            self.assertIn("reader_value_label", record)
            self.assertEqual(record["why_now"], "ongoing_disruption")
            self.assertEqual(record["reader_value_label"], "useful")


class StoryIntelligenceTest(unittest.TestCase):
    def test_story_intelligence_builds_evidence_cluster_and_cheap_dedup(self) -> None:
        duplicate_candidates = [
            {
                "title": "Council approves Manchester tower plan",
                "source_label": "BBC Manchester",
                "source_url": "https://example.test/story",
                "category": "media_layer",
                "primary_block": "last_24h",
                "include": True,
            },
            {
                "title": "Council approves Manchester tower plan",
                "source_label": "MEN",
                "source_url": "https://example.test/story?utm=1",
                "category": "media_layer",
                "primary_block": "last_24h",
                "include": True,
            },
        ]

        cheap = apply_cheap_dedup_before_enrich(duplicate_candidates)

        self.assertEqual(cheap["dropped_count"], 1)
        self.assertTrue(duplicate_candidates[1]["cheap_dedup_drop"])
        self.assertTrue(duplicate_candidates[0]["include"])

        cluster_candidates = [
            {
                "title": "Manchester Flower Festival",
                "source_label": "Manchester Flower Festival CityCo News",
                "source_url": "https://example.test/flower-official",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "include": True,
                "summary": "Free floral trail across St Ann's Square and King Street.",
                "evidence_text": "Manchester Flower Festival runs this weekend with installations and workshops.",
                "event": {
                    "is_event": True,
                    "event_name": "Manchester Flower Festival",
                    "venue": "St Ann's Square",
                    "date_start": "2026-05-23",
                    "date": "2026-05-23",
                    "borough": "Manchester",
                    "price": "free",
                },
            },
            {
                "title": "Floral trail returns to Manchester city centre",
                "source_label": "I Love Manchester Flower Festival",
                "source_url": "https://example.test/flower-guide",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "include": True,
                "summary": "Guide mentions St Ann's Square, Exchange Street and family activities.",
                "event": {
                    "is_event": True,
                    "event_name": "Manchester Flower Festival",
                    "venue": "St Ann's Square",
                    "date_start": "2026-05-23",
                    "date": "2026-05-23",
                    "borough": "Manchester",
                },
            },
        ]

        clusters = attach_story_clusters(cluster_candidates)

        self.assertEqual(clusters["cluster_count"], 1)
        packet = cluster_candidates[0]["evidence_packet"]
        self.assertEqual(packet["story_cluster"]["source_count"], 2)
        self.assertEqual(packet["story_cluster"]["canonical_source_label"], "Manchester Flower Festival CityCo News")
        union = packet["story_cluster"]["union_facts"]
        self.assertIn("Guide mentions St Ann's Square", " ".join(union["summaries"]))

    def test_rubric_history_windows_are_not_global(self) -> None:
        recurring = {
            "title": "Bowlee Car Boot Sale every Sunday",
            "summary": "Every Sunday at Bowlee Community Park.",
            "source_label": "Bowlee Car Boot Sale",
            "category": "culture_weekly",
            "primary_block": "weekend_activities",
            "event": {"is_event": True, "is_recurring": True, "event_name": "Bowlee Car Boot Sale"},
        }
        incident = {
            "title": "Man charged after Manchester crash",
            "summary": "Police said a man has been charged after a crash in Manchester.",
            "source_label": "BBC Manchester",
            "category": "media_layer",
            "primary_block": "last_24h",
        }

        recurring_policy = build_editorial_contract(recurring)["section_policy"]
        incident_policy = build_editorial_contract(incident)["section_policy"]

        self.assertEqual(recurring_policy["history_window_days"], 2)
        self.assertEqual(incident_policy["history_window_days"], 14)

    def test_story_intelligence_adds_anchor_protection_judge_and_section_score(self) -> None:
        candidate = {
            "title": "Manchester Council approves CIS Tower hotel plan",
            "summary": "Manchester Council approved the CIS Tower hotel plan today after a planning vote.",
            "evidence_text": "The council approved the plan today. The scheme affects the CIS Tower in Manchester.",
            "source_label": "Manchester Council",
            "source_url": "https://example.test/cis",
            "category": "council",
            "primary_block": "last_24h",
            "include": True,
            "published_at": now_london().isoformat(),
            "entities": {
                "boroughs": ["Manchester"],
                "councils": ["Manchester City Council"],
                "venues": ["CIS Tower"],
            },
        }

        apply_story_intelligence(candidate)

        self.assertTrue(candidate["news_anchor"]["has_news_anchor"])
        self.assertTrue(candidate["protected_lane"]["protected"])
        self.assertIn("planning_civic", candidate["protected_lane"]["lanes"])
        self.assertEqual(candidate["english_judge"]["decision"], "publish_candidate")
        self.assertGreater(candidate["section_board_score"], 100)

    def test_new_facts_diff_detects_stage_and_entity_changes(self) -> None:
        previous = {
            "title": "Police arrest man after Manchester crash",
            "summary": "Police arrested a man after a crash on Oxford Road.",
            "entities": {"people": [], "districts": ["Oxford Road"], "boroughs": ["Manchester"]},
        }
        candidate = {
            "title": "John Smith charged after Manchester crash",
            "summary": "John Smith was charged after the Oxford Road crash and will appear in court on 30 May.",
            "entities": {"people": ["John Smith"], "districts": ["Oxford Road"], "boroughs": ["Manchester"]},
        }

        diff = new_facts_diff(candidate, previous)

        self.assertTrue(diff["has_new_facts"])
        self.assertIn("stages", diff["new_fact_types"])
        self.assertIn("entities", diff["new_fact_types"])

    def test_protected_reject_requires_second_opinion_and_backup(self) -> None:
        candidate = {
            "title": "Russian stand-up show in Manchester",
            "summary": "Russian-language stand-up show in Manchester on 30 May.",
            "source_label": "Kontramarka UK",
            "category": "russian_speaking_events",
            "primary_block": "russian_events",
            "include": False,
            "entities": {"boroughs": ["Manchester"]},
            "event": {
                "is_event": True,
                "event_name": "Russian stand-up show",
                "date_start": "2026-05-30",
                "venue": "Manchester Academy",
                "borough": "Manchester",
            },
        }

        mark_reject_second_opinion(candidate, "missing_venue")

        self.assertTrue(candidate["backup_candidate"])
        self.assertTrue(candidate["second_opinion_required"])
        self.assertIn("russian_event", candidate["second_opinion_reason"]["protected_lanes"])

    def test_section_score_prefers_protected_specific_item_over_filler(self) -> None:
        protected = {
            "title": "Metrolink disruption on Airport Line today",
            "summary": "TfGM says Metrolink disruption affects the Airport Line today.",
            "source_label": "TfGM",
            "category": "transport",
            "primary_block": "transport",
            "include": True,
            "evidence_text": "Metrolink disruption affects the Airport Line today.",
        }
        filler = {
            "title": "Local woman shares inspiring career journey",
            "summary": "A local woman shares how she overcame school struggles.",
            "source_label": "MEN",
            "category": "media_layer",
            "primary_block": "last_24h",
            "include": True,
            "evidence_text": "A profile about a career journey.",
        }

        self.assertGreater(section_board_score(protected), section_board_score(filler))

    def test_enrich_failure_on_protected_item_goes_to_backup_not_silent_reject(self) -> None:
        candidate = {
            "title": "TfGM confirms Metrolink disruption on Airport Line today",
            "summary": "TfGM confirms disruption on the Airport Line today.",
            "source_label": "TfGM",
            "source_url": "https://example.test/tfgm",
            "category": "transport",
            "primary_block": "transport",
            "include": True,
            "enrichment_status": "failed: 403 Forbidden",
            "evidence_text": "",
        }

        apply_story_intelligence(candidate)
        backup = backup_pool_record(candidate, reason="enrichment failed", current_day_london="2026-05-26")

        self.assertTrue(candidate["enrichment_health"]["warning"])
        self.assertTrue(candidate["backup_candidate"])
        self.assertTrue(candidate["second_opinion_required"])
        self.assertEqual(backup["expires_on_london"], "2026-05-27")
        self.assertEqual(backup["ttl_reason"], "transport_weather_short_ttl")

    def test_final_loss_check_flags_unrendered_protected_candidate(self) -> None:
        candidate = {
            "fingerprint": "fp-ticket",
            "title": "Tickets announced for Russian stand-up show in Manchester on 30 May",
            "summary": "Russian-language stand-up show in Manchester on 30 May.",
            "source_label": "Kontramarka UK",
            "category": "russian_speaking_events",
            "primary_block": "russian_events",
            "include": True,
            "entities": {"boroughs": ["Manchester"]},
            "event": {
                "is_event": True,
                "event_name": "Russian stand-up show",
                "date_start": "2026-05-30",
                "venue": "Manchester Academy",
                "borough": "Manchester",
            },
        }
        apply_story_intelligence(candidate)

        review = _final_loss_check(
            candidates_report={"candidates": [candidate]},
            writer_report={"dropped_candidates": [{"fingerprint": "fp-ticket", "reasons": ["cap"]}]},
            rendered_fingerprints=set(),
            dedupe_memory={},
        )

        self.assertEqual(review["counts"]["critical_losses"], 1)
        self.assertEqual(review["critical_losses"][0]["disposition"], "writer_dropped")
        self.assertIn("russian_event", review["critical_losses"][0]["protected_lanes"])

    def test_model_bakeoff_dry_run_uses_validation_set_without_model_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            validation_dir = root / "data" / "validation"
            validation_dir.mkdir(parents=True)
            source = Path("data/validation/reader_value_labels.json")
            (validation_dir / "reader_value_labels.json").write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

            report = run_model_bakeoff(root, dry_run=True, limit=6)

            self.assertEqual(report["validation_set"]["label_count"], 6)
            self.assertEqual(report["models"][0]["model"], "deterministic_stub")
            self.assertEqual(report["models"][1]["status"], "dry_run_not_called")
            self.assertTrue((root / "data" / "state" / "model_bakeoff_report.json").exists())


class TelegramBacklog20260527Test(unittest.TestCase):
    """Behaviour pinned for the 2026-05-27 release-report findings: public
    digest cap was 22 (cut 113.75-score business items), today-of-event
    Ticketmaster concerts were tagged `old_onsale`, Manchester concerts
    were dumped in outside_gm_tickets purely by source label, and the
    borderline_queue stored every hold as `no_reason`. One assertion per
    behaviour."""

    def test_public_digest_max_visible_items_is_25(self) -> None:
        from news_digest.pipeline.writer import PUBLIC_DIGEST_MAX_VISIBLE_ITEMS
        self.assertEqual(PUBLIC_DIGEST_MAX_VISIBLE_ITEMS, 25)

    def test_ticket_within_7_days_kept_in_ticket_radar_despite_old_onsale(self) -> None:
        from news_digest.pipeline.candidate_validator import _exclude_stale_ticket_onsale
        # Concert tonight; tickets went on sale a year ago. Should NOT be
        # demoted to future_announcements — it's a hot day-of ticket.
        today = now_london().date()
        last_year = (now_london() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M")
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "summary": (
                f"ticket_signal=onsale public_onsale={last_year} "
                f"event_date={today.strftime('%Y-%m-%d')} 19:00"
            ),
        }
        _exclude_stale_ticket_onsale(candidate)
        self.assertEqual(candidate["ticket_type"], "event_this_week")

    def test_outside_gm_ticket_reclassified_when_local_venue_present(self) -> None:
        from news_digest.pipeline.candidate_validator import _reclassify_outside_gm_when_local_venue
        candidate = {
            "category": "venues_tickets",
            "primary_block": "outside_gm_tickets",
            "title": "Calum Scott — event 2026-05-27 — public sale 2025-04-11 09:00",
            "summary": "event=2026-05-27 19:00",
            "event": {"venue": "Manchester Apollo"},
            "source_label": "Ticketmaster UK Major Onsale",
        }
        _reclassify_outside_gm_when_local_venue(candidate)
        self.assertEqual(candidate["primary_block"], "ticket_radar")

    def test_ticket_topic_key_includes_event_date_to_separate_tour_legs(self) -> None:
        # Calum Scott on 2026-05-27 vs Calum Scott on 2026-05-28 are two
        # concerts, not one cluster. Without the date suffix the second
        # night was lost as dedupe_lost_event.
        c1 = {
            "category": "venues_tickets",
            "title": "Calum Scott — event 2026-05-27 — public sale 2025-04-11 09:00",
            "summary": "AO Arena | Manchester",
            "event": {"venue": "AO Arena", "date_start": "2026-05-27", "is_event": True},
        }
        c2 = dict(c1, title="Calum Scott — event 2026-05-28 — public sale 2025-04-11 09:00",
                  event={"venue": "AO Arena", "date_start": "2026-05-28", "is_event": True})
        k1 = build_editorial_contract(c1)["topic_key"]
        k2 = build_editorial_contract(c2)["topic_key"]
        self.assertNotEqual(k1, k2)

    def test_ticket_topic_key_strips_venue_premium_prefix_to_share_cluster(self) -> None:
        # "Calum Scott" and "Venue Premium Tickets - Calum Scott" are the
        # same concert; the premium variant must share the cluster key
        # so we keep one row, not reject two as duplicates.
        base = {
            "category": "venues_tickets",
            "title": "Calum Scott — event 2026-05-27 — public sale 2025-04-11 09:00",
            "summary": "AO Arena | Manchester",
            "event": {"venue": "AO Arena", "date_start": "2026-05-27", "is_event": True},
        }
        premium = dict(base, title="Venue Premium Tickets - Calum Scott — event 2026-05-27 — public sale 2025-08-13 11:00")
        self.assertEqual(
            build_editorial_contract(base)["topic_key"],
            build_editorial_contract(premium)["topic_key"],
        )

    def test_transport_intra_batch_dedup_keeps_distinct_stops(self) -> None:
        # Piccadilly tram escalator and Prestwich tram improvement works
        # are two different incidents — they must both stay published.
        from news_digest.pipeline.dedupe import _apply_intra_batch_dedup
        items = [
            {
                "include": True, "fingerprint": "tfgm-piccadilly-escalator",
                "title": "Piccadilly Tram Stop - Escalator out of service",
                "primary_block": "transport", "source_label": "TfGM",
                "category": "transport",
            },
            {
                "include": True, "fingerprint": "tfgm-prestwich-improvement",
                "title": "Prestwich Tram Stop - Improvement Works",
                "primary_block": "transport", "source_label": "TfGM",
                "category": "transport",
            },
        ]
        _apply_intra_batch_dedup(items)
        self.assertTrue(items[0]["include"] and items[1]["include"])

    def test_protected_hard_news_with_anchor_is_not_held_for_specificity(self) -> None:
        # "Man arrested over Manchester synagogue attack" carried
        # protected_lane=public_safety and has_news_anchor=True yet
        # still landed in the borderline pool on 2026-05-27. After the
        # fix: protected + anchor short-circuits the demotion.
        from news_digest.pipeline.candidate_validator import _apply_specificity_review
        candidate = {
            "include": True,
            "title": "Man arrested over Manchester synagogue attack",
            "summary": "Police arrested a man following the attack on a Manchester synagogue.",
            "lead": "Police arrested a man following the attack on a Manchester synagogue.",
            "evidence_text": "Greater Manchester Police said a man was arrested in connection with the synagogue attack.",
            "category": "media_layer",
            "primary_block": "today_focus",
            "protected_lane": {"protected": True, "lanes": ["public_safety"]},
            "news_anchor": {"has_news_anchor": True, "missing": []},
        }
        _apply_specificity_review(candidate)
        self.assertNotEqual(candidate.get("editorial_status"), "borderline")

    def test_impact_verb_regex_covers_regulatory_and_judicial_verbs(self) -> None:
        # 2026-05-27 Haaland Instagram ad came back with has_news_anchor=
        # False because "banned" was not in the impact_verb regex.
        from news_digest.pipeline.story_intelligence import formal_news_anchor
        candidate = {
            "title": "'Inappropriate' Instagram advert featuring Erling Haaland banned today",
            "summary": "ASA banned the advert after a ruling on 2026-05-27.",
            "entities": {"people": ["Erling Haaland"], "venues": [], "companies": ["ASA"]},
        }
        self.assertTrue(formal_news_anchor(candidate)["has_news_anchor"])

    def test_property_specificity_does_not_apply_to_crime_stories(self) -> None:
        # Synagogue attack carried property_borderline:decision_or_action
        # because _PROPERTY_MARKERS matched "attack". After the fix the
        # property review backs off when crime markers are present.
        from news_digest.pipeline.editorial_contracts import property_specificity_review
        review = property_specificity_review({
            "title": "Man arrested over Manchester synagogue attack",
            "summary": "Police charged the suspect following the attack.",
            "lead": "Police charged a man over the attack.",
            "evidence_text": "Greater Manchester Police said the man was arrested in connection with the synagogue attack.",
        })
        self.assertFalse(review["applies"])

    def test_bookable_listing_with_upcoming_event_not_blocked_as_rehash(self) -> None:
        # Concert / market on Saturday must not be rejected on Tuesday
        # just because Monday's digest already mentioned it. Closes the
        # 2026-05-27 Lowry Boys / Cherryholt loss.
        from news_digest.pipeline.candidate_validator import _exclude_cross_day_rehash
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            daily_dir = state_dir / "daily_index"
            daily_dir.mkdir(parents=True)
            yesterday = (now_london().date() - timedelta(days=1)).isoformat()
            (daily_dir / f"{yesterday}.jsonl").write_text(
                json.dumps({"fingerprint": "fp-concert-future", "included": True}) + "\n",
                encoding="utf-8",
            )
            event_day = (now_london().date() + timedelta(days=5)).isoformat()
            candidate = {
                "include": True,
                "fingerprint": "fp-concert-future",
                "title": "Lowry Boys",
                "editorial_contract": {
                    "anchor_type": "bookable_listing",
                    "section_policy": {"repeat_ttl_days": 1},
                },
                "event": {"date_start": event_day, "is_event": True},
            }
            _exclude_cross_day_rehash(candidate, state_dir)
            self.assertTrue(candidate["include"])

    def test_section_min_floor_pulls_back_unrendered_included_candidates(self) -> None:
        # «Главная история дня» was 1 item on 2026-05-27 while score-10
        # candidates sat with include=True and no draft_line. The pull-
        # back must promote them up to SECTION_MIN_ITEMS using the
        # event fallback builder when LLM did not write a draft_line.
        from news_digest.pipeline.writer import _apply_section_min_floor_pull_back
        candidates = [
            {
                "include": True,
                "fingerprint": "fp-event-a",
                "title": "Makers Market double header this May!",
                "category": "culture_weekly",
                "primary_block": "weekend_activities",
                "reader_value_score": 130.0,
                "source_label": "First Street",
                "source_url": "https://firststreetmanchester.com/news/makers-market",
                "event": {
                    "is_event": True,
                    "event_name": "Makers Market double header",
                    "venue": "First Street",
                    "date_start": (now_london().date() + timedelta(days=2)).isoformat(),
                },
            },
        ]
        lines, fps, scores, titles, srcs = _apply_section_min_floor_pull_back(
            "Выходные в GM", [], [], [], [], [],
            candidates, set(), 1, [],
        )
        # The market with a deterministic event fallback must surface.
        self.assertEqual(len(lines), 1)

    def test_ticket_type_default_set_for_venues_tickets_without_signal(self) -> None:
        # 110 venues_tickets items shipped with ticket_type=NONE on
        # 2026-05-27, which made them appear in the 'unknown' bucket
        # of the ticket funnel. After C1: validator pass ensures a
        # default ticket_type, and events within 14 days become
        # event_this_week (the protected bucket).
        from news_digest.pipeline.candidate_validator import _ensure_default_ticket_type
        event_day = (now_london().date() + timedelta(days=3)).strftime("%Y-%m-%d")
        candidate = {
            "category": "venues_tickets",
            "summary": f"AO Arena | Manchester | event_date={event_day} 19:00",
        }
        _ensure_default_ticket_type(candidate)
        self.assertEqual(candidate["ticket_type"], "event_this_week")

    def test_validator_stage_rejects_appear_in_reject_review(self) -> None:
        # 91 rejects were missing from reject_review on 2026-05-27
        # because the classifier only walked writer and curator stages.
        # After C4: candidates with include=False and a reject_reason
        # are classified too.
        from news_digest.pipeline.release import _classify_rejected_candidates
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "fp-validator-drop",
                    "title": "Old listing rejected by validator",
                    "include": False,
                    "reject_reasons": ["property_listing"],
                    "reason": "Validator: property listing without civic angle.",
                    "reader_value_score": 30,
                },
            ]
        }
        report = _classify_rejected_candidates({"dropped_candidates": []}, {"decisions": []}, candidates_report)
        self.assertGreaterEqual(report["counts"]["correctly_rejected"], 1)

    def test_telegram_thresholds_no_longer_claim_old_14_22_norm(self) -> None:
        # The support report should not contradict the writer's global
        # digest budget with the old 14–22 hard wording.
        text = Path("scripts/run_local_digest.py").read_text(encoding="utf-8")
        self.assertNotIn("14–22", text)

    def test_evidence_chrome_stripped_but_facts_preserved(self) -> None:
        # Police cards came out terrible on 2026-05-28 because the
        # enriched evidence was raw page chrome (breadcrumbs + byline)
        # not clean article text. The cleaner must remove the chrome and
        # KEEP the facts.
        from news_digest.pipeline.collector.extract import _strip_evidence_chrome
        out = _strip_evidence_chrome(
            "Share Save Add as preferred on Google Jonny Humphries North West PA Media "
            "Eight people have been arrested in connection with the attack. Police continue to investigate."
        )
        self.assertIn("Eight people have been arrested", out)
        self.assertNotIn("PA Media", out)
        self.assertNotIn("Add as preferred", out)
        # 2026-05-29: Manchester Academy ticket pages prepend a JS-disabled
        # warning that was scraped as the concert description. Strip it, keep
        # the real event facts (date/venue/billing).
        ticket = _strip_evidence_chrome(
            "This website makes extensive use of JavaScript in places to provide a "
            "better experience for our users To view the site as intended, please enable "
            "JavaScript in your browser settings Live Nation Presents 6LACK on Saturday "
            "27 September 2026 at Manchester Academy. Doors 7pm."
        )
        self.assertNotIn("JavaScript", ticket)
        self.assertIn("6LACK", ticket)
        self.assertIn("Manchester Academy", ticket)

    def test_tram_card_keeps_stop_location_and_distinguishes_stops(self) -> None:
        # 2026-05-28: 'Piccadilly Gardens - Tram Improvement Works' and
        # 'Prestwich Tram Stop - Improvement Works' both rendered as the
        # locationless, identical 'предупреждение TfGM по трамваям —
        # ремонтные работы'. The stop must survive and the two must differ.
        from news_digest.pipeline.transport_card import extract_transport_card, render_card
        def render(title):
            return render_card(extract_transport_card({
                "title": title, "summary": "", "evidence_text": "",
                "source_url": "https://tfgm.com", "source_label": "TfGM",
            }))
        a = render("Piccadilly Gardens - Tram Improvement Works")
        b = render("Prestwich Tram Stop - Improvement Works")
        c = render("Manchester Airport Tram Stop - Escalator out of service")
        self.assertIn("Piccadilly Gardens", a)
        self.assertNotEqual(a, b)
        self.assertIn("эскалатор не работает", c)  # not mistranslated to 'ремонтные работы'

    def test_transport_reminder_states_location_from_url_slug(self) -> None:
        # 2026-05-29: an active Metrolink record with empty line/segment
        # rendered the contentless tier-4 stub "Metrolink: [reminder]
        # Metrolink — подробности в источнике", breaking the hard "always
        # say WHERE" rule. The location lives in the TfGM alert URL slug.
        from news_digest.pipeline.transport_fill import _record_to_card
        from news_digest.pipeline.transport_card import render_reminder
        rec = {
            "mode": "tram", "operator": "Metrolink", "line": "", "segment": "",
            "end_date_ru": "29 мая", "reason": "ремонтные работы",
            "source_url": "https://tfgm.com/travel-updates/travel-alerts/"
            "piccadilly-gardens-tram-improvement-works",
        }
        line = render_reminder(_record_to_card(rec))
        self.assertIn("Piccadilly Gardens", line)
        self.assertNotIn("подробности в источнике", line)

    def test_tram_card_without_locator_is_held_not_published_as_stub(self) -> None:
        from news_digest.pipeline.transport_card import TransportCard, render_card
        line = render_card(TransportCard(mode="tram", operator="Metrolink", reason="ремонтные работы"))
        self.assertEqual(line, "")

    def test_ticket_fallback_rejects_js_chrome_summary(self) -> None:
        from news_digest.pipeline.writer import _build_ticket_fallback_line
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "source_label": "Manchester Academy",
            "title": "6LACK - 27th September 2026",
            "summary": (
                "This website makes extensive use of JavaScript in places to provide a better "
                "experience for our users To view the site as intended, please enable JavaScript "
                "in your browser settings All ages welcome."
            ),
            "event": {"is_event": True, "event_name": "6LACK", "venue": "Manchester Academy", "date_start": "2026-09-27"},
        }
        self.assertEqual(_build_ticket_fallback_line(candidate), "")

    def test_hard_news_missing_draft_line_gets_recovery_line(self) -> None:
        from news_digest.pipeline.writer import _hard_news_recovery_line
        line = _hard_news_recovery_line(
            {
                "category": "media_layer",
                "primary_block": "last_24h",
                "borough": "Whitefield",
                "title": "Two men charged after man shot in Whitefield police incident",
            }
        )
        self.assertIn("Whitefield", line)
        self.assertIn("предъявлены обвинения", line)

    def test_breadcrumb_page_title_collapsed(self) -> None:
        # 2026-05-29: New Smithfield rendered as "Casual trading | Casual
        # trading | Manchester City Council" — a duplicated breadcrumb title.
        from news_digest.pipeline.collector.summary import _clean_title_text
        self.assertEqual(
            _clean_title_text("Casual trading | Casual trading | Manchester City Council"),
            "Casual trading",
        )

    def test_same_venue_different_events_not_merged_by_token_overlap(self) -> None:
        # Strike Den! and The Fabric of Protest are two different shows at
        # People's History Museum — shared venue + date tokens must not
        # collapse them in intra-batch dedup.
        from news_digest.pipeline.dedupe import _apply_intra_batch_dedup
        items = [
            {
                "include": True, "fingerprint": "phm-strike-den",
                "title": "Strike Den!, Wed 27 - Sat 31 May 2026, 10.00am - 4.00pm - People's History Museum",
                "primary_block": "next_7_days", "category": "culture_weekly", "source_label": "PHM",
                "event": {"is_event": True, "event_name": "Strike Den", "venue": "People's History Museum"},
            },
            {
                "include": True, "fingerprint": "phm-fabric-protest",
                "title": "The Fabric of Protest, Sat 30 May 2026, 1.00pm - 3.00pm - People's History Museum",
                "primary_block": "next_7_days", "category": "culture_weekly", "source_label": "PHM",
                "event": {"is_event": True, "event_name": "The Fabric of Protest", "venue": "People's History Museum"},
            },
        ]
        _apply_intra_batch_dedup(items)
        self.assertTrue(items[0]["include"] and items[1]["include"])

    def test_no_source_text_item_separated_from_generation_failures(self) -> None:
        # Headline-only / paywalled items (evidence_text empty) must be
        # classified as an enrichment gap, not counted against LLM yield.
        from news_digest.pipeline.llm_rewrite import _has_nothing_to_write_from
        paywalled = {
            "title": "'Independent and clearly very troublesome'",
            "evidence_text": "", "summary": "", "lead": "",
        }
        self.assertTrue(_has_nothing_to_write_from(paywalled))

    def test_upcoming_dated_event_carries_over_despite_no_food_signal_terms(self) -> None:
        # Cherryholt / Skipinnish / Calum Scott are concerts with a real
        # future date but no 'festival/market/fair' wording, so the
        # calendar-carry signal-term gate rejected them as 'no new facts'
        # on 2026-05-28 even though the gig is today/this week.
        from news_digest.pipeline.dedupe import _calendar_item_should_carry_over
        event_day = (now_london().date() + timedelta(days=1)).strftime("%Y-%m-%d")
        candidate = {
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "title": f"Cherryholt — event {event_day}",
            "event": {"is_event": True, "date_start": event_day, "venue": "Manchester Academy"},
        }
        previous = {"first_published_day_london": (now_london().date() - timedelta(days=3)).isoformat(),
                    "last_published_day_london": (now_london().date() - timedelta(days=3)).isoformat()}
        self.assertTrue(_calendar_item_should_carry_over(candidate, previous))

    def test_structured_event_card_allowed_to_be_concise(self) -> None:
        # Bluey's Big Play (The Lowry) was dropped twice for a 120-char
        # draft_line under the ≥150-char long-format gate. A complete
        # dated+venue event card must be allowed to be short.
        from news_digest.pipeline.writer import _draft_line_quality_errors
        candidate = {
            "category": "culture_weekly",
            "primary_block": "next_7_days",
            "evidence_text": "x" * 2000,  # rich evidence — would normally force the 150-char gate
            "event": {"is_event": True, "date_start": "2026-05-29", "venue": "The Lowry"},
        }
        line = "• 29 мая в The Lowry — Bluey's Big Play, шоу для детей. Билеты на сайте."
        errors = _draft_line_quality_errors(candidate, line)
        self.assertFalse(any("long-format category needs ≥150" in e for e in errors))

    def test_event_this_week_ticket_is_strong_tier_not_optional(self) -> None:
        # The event_this_week ticket_type I introduced must be a strong
        # tier and a ticket_opportunity why_now, otherwise day-of
        # concerts sink and the funnel shows them as 'unknown'.
        from news_digest.pipeline.editorial_contracts import build_editorial_contract, infer_why_now
        candidate = {
            "category": "venues_tickets",
            "title": "Dead Pony — event 2026-05-29",
            "summary": "Manchester Academy | event_date=2026-05-29 19:00",
            "ticket_type": "event_this_week",
            "event": {"is_event": True, "event_name": "Dead Pony", "venue": "Manchester Academy",
                      "date_start": "2026-05-29"},
        }
        self.assertEqual(infer_why_now(candidate), "ticket_opportunity")

    def test_writer_does_not_degrade_on_soft_quality_warnings_only(self) -> None:
        # 94% yield + weak/repair messages must NOT trigger degraded_shrink.
        # Previously any warning with the word "degraded" flipped the
        # writer and chopped reader_value 800+ Manchester Academy cards.
        from news_digest.pipeline.writer import _llm_rewrite_is_degraded
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            (state_dir / "llm_rewrite_report.json").write_text(
                json.dumps({
                    "stage_status": "complete",
                    "warnings": [],
                    "soft_warnings": [
                        "37 draft_line(s) still look weak after repair.",
                        "Repair pass rejected 19 replacement(s) that still failed writer quality gate.",
                    ],
                }),
                encoding="utf-8",
            )
            degraded, _ = _llm_rewrite_is_degraded(state_dir)
            self.assertFalse(degraded)

    def test_borderline_queue_records_reason_code_per_item(self) -> None:
        candidates_report = {
            "candidates": [
                {
                    "fingerprint": "fp-1",
                    "title": "Held item",
                    "editorial_status": "borderline",
                    "quality_warnings": ["property_borderline:no_clear_action"],
                    "english_judge": {"reason_codes": ["property_listing"]},
                }
            ]
        }
        queue = _borderline_queue(candidates_report, {})
        # The hold MUST carry a reason_code so we never see another
        # 30-of-77 dump labelled "no_reason" in production reports.
        self.assertEqual(queue["items"][0]["reason_code"], "property_borderline")


class SemanticGuardTest(unittest.TestCase):
    def test_embedding_only_guard_restores_excessive_drops_with_review_payload(self) -> None:
        candidates = []
        for idx in range(40):
            drop = idx < 12
            candidates.append(
                {
                    "include": not drop,
                    "fingerprint": f"fp-{idx}",
                    "title": f"Story {idx}",
                    "source_label": "Source",
                    "primary_block": "city_watch",
                    "dedupe_decision": "drop" if drop else "new",
                    "change_type": "same_story_rehash" if drop else "new_story",
                    "semantic_dedupe_match": "embedding_only" if drop else "",
                    "semantic_match_sim": 0.91,
                    "semantic_match_fingerprint": f"old-{idx}",
                    "reason": "Semantic cross-day rehash.",
                }
            )

        guard = _apply_semantic_drop_guard(candidates)

        self.assertTrue(guard["triggered"])
        self.assertEqual(guard["restored"], 12)
        self.assertEqual(len(guard["restored_candidates"]), 12)
        self.assertTrue(all(c["include"] for c in candidates[:12]))
        self.assertIn("previous_reason", guard["restored_candidates"][0])


if __name__ == "__main__":
    unittest.main()
