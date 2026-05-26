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
from news_digest.pipeline.editorial_contracts import scrub_vague_ending
from news_digest.pipeline.collector.routing import _TICKET_HORIZON_DAYS
from news_digest.pipeline.dedupe import _apply_semantic_drop_guard
from news_digest.pipeline.history import write_daily_index_snapshot
from news_digest.pipeline.release import (
    build_release,
    _classify_published_candidates,
    _classify_rendered_html_quality,
    _event_miss_review,
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
                "published_at": "2026-05-18T09:00:00+01:00",
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
                "published_at": "2026-05-20T08:00:00+01:00",
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
                "published_at": "2026-05-20T08:00:00+01:00",
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
        self.assertIn("14–22", text)

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
