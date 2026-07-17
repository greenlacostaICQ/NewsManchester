import json
import tempfile
import unittest
from pathlib import Path

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
    def test_russian_event_must_show_respects_repeat_policy(self) -> None:
        # Этап 3: правило must_show для русских событий живёт в планёрке.
        from news_digest.pipeline.plan_digest import _must_show

        candidate = {"primary_block": "russian_events", "fingerprint": "ru-repeat-1"}
        self.assertFalse(_must_show(candidate, repeat_allowed=False))
        self.assertTrue(_must_show(candidate, repeat_allowed=True))
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
        self.assertIn(report["dropped_candidates"][0]["reasons"][0], {"Missing draft_line.", "Headline-only fallback forbidden.", "missing_required_facts"})
        self.assertFalse(report["dropped_candidates"][0].get("recoverable_reserve", False))


if __name__ == "__main__":
    unittest.main()
