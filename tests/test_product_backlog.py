from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.common import now_london
from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.change_classifier import classify_change_phase
from news_digest.pipeline.collector.extract import _extract_source_candidates
from news_digest.pipeline.collector.sources import SourceDef
from news_digest.pipeline.dead_parser_repair import build_dead_parser_repair_report
from news_digest.pipeline.practical_backfill import apply_practical_backfill
from news_digest.pipeline.reader_actions import classify_reader_action
from news_digest.pipeline.release import _source_funnel_human
from news_digest.pipeline.source_anomaly import detect_dead_parsers, detect_source_anomalies
from news_digest.pipeline.source_discovery import discover_sources


class ProductBacklogTest(unittest.TestCase):
    def test_jsonld_event_hint_populates_candidate_event(self) -> None:
        source = SourceDef(
            "Test Venue",
            "culture_weekly",
            "culture_weekly",
            "https://venue.test/event",
            "next_7_days",
            source_type="html_page_event",
        )
        body = """
        <html><head><script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "Event",
          "name": "Manchester Music Night at Test Hall",
          "startDate": "2026-06-06T19:30:00+01:00",
          "endDate": "2026-06-06T22:00:00+01:00",
          "location": {"@type": "Place", "name": "Test Hall"},
          "offers": {"@type": "Offer", "price": "12.50", "priceCurrency": "GBP", "url": "https://venue.test/tickets"}
        }
        </script><title>Manchester Music Night at Test Hall</title></head>
        <body><main><h1>Manchester Music Night at Test Hall</h1><p>A practical event in Manchester with tickets.</p></main></body></html>
        """
        candidate = _extract_source_candidates(source, body)[0]
        self.assertEqual(candidate["structured_event_hint"]["venue"], "Test Hall")
        self.assertEqual(candidate["structured_event_hint"]["price"], "£12.50")

    def test_trial_candidate_validates_but_is_not_publishable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "data" / "state"
            state.mkdir(parents=True)
            (state / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "trial-test",
                        "candidates": [
                            {
                                "include": True,
                                "source_trial": True,
                                "fingerprint": "trial-fp",
                                "title": "Manchester consultation opens",
                                "summary": "Manchester Council opened a consultation today.",
                                "source_url": "https://example.test/consultation",
                                "source_label": "Trial Council",
                                "category": "council",
                                "primary_block": "city_watch",
                                "dedupe_decision": "new",
                                "change_type": "new_story",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = validate_candidates(root)
            self.assertTrue(result.ok)
            payload = json.loads((state / "candidates.json").read_text(encoding="utf-8"))
            candidate = payload["candidates"][0]
            self.assertFalse(candidate["include"])
            self.assertEqual(candidate["trial_status"], "validated_not_publishable")

    def test_change_phase_and_reader_action_are_product_signals(self) -> None:
        closing = {
            "title": "Manchester consultation closes tomorrow",
            "summary": "Residents have until tomorrow to comment.",
            "primary_block": "city_watch",
        }
        self.assertEqual(classify_change_phase(closing), "consultation_closing")
        closing["change_phase"] = "consultation_closing"
        self.assertEqual(classify_reader_action(closing), "note_deadline")

    def test_practical_backfill_promotes_next_week_event_when_7_day_layer_empty(self) -> None:
        event_day = (now_london().date()).isoformat()
        candidates = [
            {
                "include": True,
                "title": "Useful event",
                "category": "culture_weekly",
                "primary_block": "future_announcements",
                "event": {"is_event": True, "date_start": event_day, "venue": "Test Hall"},
            }
        ]
        summary = apply_practical_backfill(candidates)
        self.assertEqual(summary.get("next_7_days"), 1)
        self.assertEqual(candidates[0]["primary_block"], "next_7_days")

    def test_source_discovery_turns_seed_links_into_trial_recommendations(self) -> None:
        def fake_fetcher(url: str) -> str:
            return """
            <html><head><link rel="alternate" type="application/rss+xml" href="/news/rss.xml"></head>
            <body><a href="/consultations">Consultations</a><a href="/events">Events</a></body></html>
            """

        found = discover_sources(["https://example.test/"], fetcher=fake_fetcher)
        self.assertTrue(any(item["kind"] == "rss" for item in found))
        self.assertTrue(any(item["primary_block_guess"] == "city_watch" for item in found))
        self.assertTrue(all(item["trial"] for item in found))
        first = found[0]
        self.assertIn("recommended_source_def", first)
        self.assertIn("how_to_check", first)
        self.assertTrue(first["recommended_source_def"]["trial"])
        self.assertIn("можно включать", " ".join(first["trial_verdict_rules"]))

    def test_source_loss_funnel_has_human_template_and_action(self) -> None:
        human = _source_funnel_human(
            "MEN News Sitemap",
            {
                "raw_count": 40,
                "curated_count": 4,
                "rendered_count": 0,
                "reject_reasons": {"duplicate_same_story": 22},
                "loss_funnel": {
                    "rejected_before_writer": 8,
                    "backup_before_rewrite": 6,
                    "included_missing_draft_line": 1,
                    "writer_dropped": 0,
                },
            },
        )
        text = "\n".join(human["template"])
        self.assertIn("Собрали: 40", text)
        self.assertIn("Повторы / уже было: 22", text)
        self.assertIn("Ушло в резерв до перевода: 6", text)
        self.assertIn("Попало в выпуск: 0", text)
        self.assertIn("дубли", human["conclusion"])
        self.assertIn("Что делать", f"Что делать: {human['action']}")

    def test_dead_parser_repair_suggests_html_extractor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "data" / "state"
            state.mkdir(parents=True)
            (state / "release_report.json").write_text(
                json.dumps({"dead_parsers": [{"name": "Manchester Council"}]}),
                encoding="utf-8",
            )

            def fake_fetcher(url: str) -> str:
                return '<html><body><a href="/news/2026/test-story">Test news story</a></body></html>'

            report = build_dead_parser_repair_report(root, fetcher=fake_fetcher)
            self.assertEqual(report["repairs"][0]["status"], "probed")
            self.assertIn("HTML link extractor", report["repairs"][0]["suggestion"])

    def test_trial_sources_do_not_trigger_source_anomaly_or_dead_parser(self) -> None:
        history = [
            {
                "run_date_london": f"2026-06-0{idx}",
                "sources": [{"name": "Trial", "category": "council", "trial": True, "raw": 9, "status": "empty"}],
            }
            for idx in range(1, 6)
        ]
        self.assertEqual(detect_source_anomalies(history), [])
        self.assertEqual(detect_dead_parsers(history), [])


if __name__ == "__main__":
    unittest.main()
