import json
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.llm_rewrite import _publish_plan_status
from news_digest.pipeline.editor import _PrevalidatedReservePool, _same_section_reserve_line
from news_digest.pipeline.writer import _SectionRow, _fresh_rows_are_same_story, write_digest


def _row(candidate: dict) -> _SectionRow:
    return _SectionRow(
        section="Свежие новости",
        line=str(candidate.get("draft_line") or ""),
        source=str(candidate.get("source_label") or "MEN"),
        score=0.0,
        fingerprint=str(candidate.get("fingerprint") or candidate.get("title") or ""),
        title=str(candidate.get("title") or ""),
        candidate=candidate,
    )


class PublicFreshDedupeTest(unittest.TestCase):
    def test_incident_rows_do_not_merge_on_overlap_without_concrete_anchor(self) -> None:
        murder_trial = {
            "fingerprint": "murder-trial",
            "title": "Murder trial begins after Manchester court hears police evidence",
            "summary": "The court heard police evidence in a murder trial today.",
            "topic_key": "crime",
            "story_frame": {"event_type": "court", "where_exact": "Manchester Crown Court", "when": "2026-06-30"},
            "entities": {"people": ["John Doe"], "districts": ["Manchester"]},
            "draft_line": "• Manchester: суд начал слушать дело об убийстве после новых показаний полиции.",
        }
        fallowfield_crash = {
            "fingerprint": "fallowfield-car",
            "title": "Fallowfield car crash sees police close Manchester road",
            "summary": "Police closed a road after a car crash in Fallowfield today.",
            "topic_key": "crime",
            "story_frame": {"event_type": "incident", "where_exact": "Fallowfield", "when": "2026-06-30"},
            "entities": {"districts": ["Fallowfield"]},
            "draft_line": "• Fallowfield: полиция закрыла дорогу после ДТП.",
        }

        self.assertFalse(_fresh_rows_are_same_story(_row(murder_trial), _row(fallowfield_crash)))

    def test_incident_rows_merge_on_shared_location_and_incident_marker(self) -> None:
        guns = {
            "fingerprint": "guns-woodland",
            "title": "Guns found in woodland near Moston after police search",
            "summary": "Police found guns in woodland near Moston.",
            "topic_key": "crime",
            "story_frame": {"event_type": "incident", "where_exact": "Moston", "when": "2026-06-30"},
            "entities": {"districts": ["Moston"]},
            "draft_line": "• Moston: полиция нашла оружие после поиска в лесополосе.",
        }
        firearms = {
            "fingerprint": "moston-firearms",
            "title": "Moston firearms discovered by police in woodland",
            "summary": "Firearms were discovered in Moston woodland.",
            "topic_key": "crime",
            "story_frame": {"event_type": "incident", "where_exact": "Moston", "when": "2026-06-30"},
            "entities": {"districts": ["Moston"]},
            "draft_line": "• Moston: обнаружено огнестрельное оружие после полицейского поиска.",
        }

        self.assertTrue(_fresh_rows_are_same_story(_row(guns), _row(firearms)))
        self.assertEqual(guns["dedupe_merge_reason"], "fresh_incident_evidence_match")
        self.assertIn("firearms", guns["dedupe_merge_evidence"]["type"])

    def test_broad_manchester_location_is_not_enough_for_incident_merge(self) -> None:
        first = {
            "fingerprint": "murder-a",
            "title": "Police investigate murder in Manchester",
            "summary": "Police opened a murder investigation in Manchester.",
            "topic_key": "crime",
            "story_frame": {"event_type": "incident", "where_exact": "Manchester", "when": "2026-06-30"},
            "entities": {"boroughs": ["Manchester"]},
            "draft_line": "• Manchester: полиция расследует убийство.",
        }
        second = {
            "fingerprint": "murder-b",
            "title": "Separate murder trial hears evidence in Manchester",
            "summary": "A separate murder trial heard evidence in Manchester.",
            "topic_key": "crime",
            "story_frame": {"event_type": "court", "where_exact": "Manchester", "when": "2026-06-30"},
            "entities": {"boroughs": ["Manchester"]},
            "draft_line": "• Manchester: в суде слушают другое дело об убийстве.",
        }

        self.assertFalse(_fresh_rows_are_same_story(_row(first), _row(second)))

    def test_non_incident_overlap_still_merges(self) -> None:
        left = {
            "fingerprint": "council-a",
            "title": "Manchester Council approves new city centre housing plan after committee vote",
            "summary": "Manchester Council approved the city centre housing plan after a committee vote.",
            "topic_key": "planning",
            "story_frame": {"event_type": "planning"},
            "draft_line": "• Manchester Council approved a new city centre housing plan after committee vote.",
        }
        right = {
            "fingerprint": "council-b",
            "title": "City centre housing plan approved by Manchester Council after committee vote",
            "summary": "The committee vote approved the city centre housing plan at Manchester Council.",
            "topic_key": "planning",
            "story_frame": {"event_type": "planning"},
            "draft_line": "• City centre housing plan approved by Manchester Council after committee vote.",
        }

        self.assertTrue(_fresh_rows_are_same_story(_row(left), _row(right)))


class ShowRenderableContractTest(unittest.TestCase):
    def test_publish_plan_status_requires_text_or_explicit_deterministic_ready_fields(self) -> None:
        self.assertEqual(_publish_plan_status({"digest_selection_verdict": "selected"}), "needs_enrichment")
        self.assertIn(
            _publish_plan_status({"digest_selection_verdict": "selected", "draft_line": "• Готовая строка."}),
            {"show", "must_show"},
        )
        self.assertEqual(
            _publish_plan_status(
                {
                    "digest_selection_verdict": "selected",
                    "category": "venues_tickets",
                    "title": "Example Artist",
                    "primary_block": "ticket_radar",
                }
            ),
            "needs_enrichment",
        )
        self.assertIn(
            _publish_plan_status(
                {
                    "digest_selection_verdict": "selected",
                    "category": "venues_tickets",
                    "title": "Example Artist",
                    "primary_block": "ticket_radar",
                    "event": {"date_start": "2026-07-20", "venue": "AO Arena"},
                }
            ),
            {"show", "must_show"},
        )

    def test_writer_drops_event_without_headline_only_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            candidate = {
                "include": True,
                "validated": True,
                "fingerprint": "thin-event",
                "category": "culture_weekly",
                "primary_block": "openings",
                "title": "Thin event listing",
                "summary": "",
                "lead": "",
                "source_label": "Venue",
                "source_url": "https://example.test/thin-event",
            }
            (state_dir / "candidates.json").write_text(
                json.dumps({"candidates": [candidate]}, ensure_ascii=False),
                encoding="utf-8",
            )

            result = write_digest(root)
            report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))
            html = (state_dir / "draft_digest.html").read_text(encoding="utf-8")

        self.assertTrue(result.ok)
        self.assertNotIn("Thin event listing", html)
        self.assertIn(report["dropped_candidates"][0]["reasons"][0], {"Missing draft_line.", "Headline-only fallback forbidden."})
        self.assertFalse(report["dropped_candidates"][0].get("recoverable_reserve", False))


class PrevalidatedReserveTest(unittest.TestCase):
    def test_prevalidated_pool_uses_only_existing_render_ready_lines(self) -> None:
        candidates = [
            {
                "validated": True,
                "public_reserve": True,
                "backup_pool_only": False,
                "primary_block": "ticket_radar",
                "category": "venues_tickets",
                "title": "Textless Artist — event 2026-07-20",
                "summary": "AO Arena | event_date=2026-07-20 19:00",
                "source_url": "https://example.test/textless",
                "source_label": "Ticketmaster",
                "event": {"date_start": "2026-07-20T19:00:00+01:00", "venue": "AO Arena"},
                "ticket_notability": {"artist": "Textless Artist", "tier": "A", "kind": "artist"},
                "reader_value_score": 999,
            },
            {
                "validated": True,
                "public_reserve": True,
                "backup_pool_only": False,
                "primary_block": "ticket_radar",
                "category": "venues_tickets",
                "title": "Clean Artist",
                "draft_line": "• <b>Clean Artist</b> — 20 июля, AO Arena. Проверьте билеты.",
                "source_url": "https://example.test/clean",
                "source_label": "Ticketmaster",
                "event": {"date_start": "2026-07-20T19:00:00+01:00", "venue": "AO Arena"},
                "reader_value_score": 10,
            },
        ]
        rendered_urls: set[str] = set()
        rendered_story_keys: set[str] = set()
        reserve_pool = _PrevalidatedReservePool.build(candidates, rendered_urls, rendered_story_keys)
        stats: dict[str, object] = {}

        line = _same_section_reserve_line(
            "Билеты / Ticket Radar",
            candidates,
            rendered_urls,
            rendered_story_keys,
            stats,
            reserve_pool,
        )

        self.assertIn("Clean Artist", line)
        self.assertNotIn("Textless Artist", line)
        self.assertEqual(stats["prevalidated_pop_used"], 1)
        self.assertEqual(stats.get("enriched_rewrite_attempts", 0), 0)


if __name__ == "__main__":
    unittest.main()
