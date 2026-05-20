from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.dedupe import _apply_semantic_drop_guard
from news_digest.pipeline.history import write_daily_index_snapshot
from news_digest.pipeline.writer import write_digest


class WriterRenderedFingerprintTest(unittest.TestCase):
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

    def test_validator_soft_warns_under_specified_dated_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            candidate = {
                "include": True,
                "fingerprint": "event-2",
                "category": "culture_weekly",
                "primary_block": "next_7_days",
                "title": "Workshop on 20 May at The Gallery",
                "summary": "Free tickets for a workshop.",
                "lead": "",
                "evidence_text": "Free tickets for a workshop on 20 May at The Gallery.",
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
