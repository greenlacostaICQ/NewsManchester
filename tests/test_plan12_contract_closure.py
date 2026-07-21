from __future__ import annotations

from datetime import date, timedelta
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import scripts.run_local_digest as run_local_digest
from news_digest.pipeline.candidate_validator import _exclude_cross_day_rehash, validate_candidates
from news_digest.pipeline.common import now_london, today_london
from news_digest.pipeline.repeat_policy import RepeatVerdict
from news_digest.pipeline.weekend_inventory import effective_occurrence_window


class Plan12ContractClosureTest(unittest.TestCase):
    def test_effective_occurrence_window_is_the_candidate_and_fact_entrypoint(self) -> None:
        candidate = {
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "title": "Manchester makers market every Saturday",
            "summary": "The market runs every Saturday in Manchester.",
            "event": {"date_start": "2026-01-03", "date_end": "2026-01-03"},
        }
        start, end = effective_occurrence_window(candidate, today=date(2026, 7, 21))
        self.assertEqual(start, date(2026, 7, 25))
        self.assertEqual(end, date(2026, 7, 25))
        self.assertEqual(candidate["event"]["next_occurrence"], "2026-07-25")
        self.assertEqual(
            effective_occurrence_window({"date_start": "2026-07-24", "date_end": "2026-07-26"}),
            (date(2026, 7, 24), date(2026, 7, 26)),
        )

    def test_validator_cross_day_gate_uses_visible_repeat_verdict(self) -> None:
        candidate = {
            "include": True,
            "fingerprint": "repeat-fp",
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "title": "Manchester market every Saturday",
            "summary": "A recurring Manchester market.",
            "editorial_contract": {"section_policy": {"repeat_ttl_days": 1}},
        }
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            daily = state_dir / "daily_index"
            daily.mkdir()
            yesterday = (now_london().date() - timedelta(days=1)).isoformat()
            (daily / f"{yesterday}.jsonl").write_text(
                json.dumps({"fingerprint": "repeat-fp", "included": True, "title": candidate["title"]}) + "\n",
                encoding="utf-8",
            )
            verdict = RepeatVerdict(True, "calendar", "current_occurrence_window")
            with patch("news_digest.pipeline.candidate_validator.visible_repeat_verdict", return_value=verdict) as mocked:
                excluded = _exclude_cross_day_rehash(candidate, state_dir)
        self.assertFalse(excluded)
        mocked.assert_called_once()
        self.assertEqual(candidate["visible_repeat_verdict"]["reason"], "current_occurrence_window")

    def test_invalid_event_page_is_not_protected_after_validation(self) -> None:
        candidate = {
            "include": True,
            "dedupe_decision": "new",
            "fingerprint": "invalid-protected-ticket",
            "source_url": "https://example.test/show",
            "source_label": "Ticket source",
            "event_page_type": "homepage",
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "title": "Global Star Manchester show",
            "summary": "Tickets for Global Star at AO Arena on 1 January 2099.",
            "published_at": now_london().isoformat(),
            "event": {
                "is_event": True,
                "event_name": "Global Star",
                "date_start": "2099-01-01",
                "date_confidence": "high",
                "venue": "AO Arena",
            },
            "ticket_notability": {"artist": "Global Star", "tier": "A", "kind": "artist"},
            "venue_scope": "GM",
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(json.dumps({"candidates": [candidate]}), encoding="utf-8")
            validate_candidates(root)
            final = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))["candidates"][0]
        self.assertFalse(final["validated"])
        self.assertIn("Event candidate must use an official event page.", final["validation_errors"])
        self.assertFalse(final["protected_lane"]["protected"])
        self.assertIn("validation_failed", final["protected_lane"]["ineligible_reason_codes"])

    def test_manual_send_gate_runs_final_plan_verification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            outgoing = root / "data" / "outgoing"
            state_dir.mkdir(parents=True)
            outgoing.mkdir(parents=True)
            digest = outgoing / "current_digest.html"
            digest.write_text(
                f'<b>Greater Manchester Brief — {today_london()}, 08:00</b>\n'
                '<b>Погода</b>\n• Тест <a href="https://example.test/item">Source</a>\n',
                encoding="utf-8",
            )
            (state_dir / "release_report.json").write_text(
                json.dumps(
                    {
                        "release_decision": "pass",
                        "release_gate_version": run_local_digest.REQUIRED_RELEASE_GATE_VERSION,
                        "run_date_london": today_london(),
                        "output_path": str(digest),
                    }
                ),
                encoding="utf-8",
            )
            original_root = run_local_digest.PROJECT_ROOT
            try:
                run_local_digest.PROJECT_ROOT = root
                error = run_local_digest._release_gate_error_for_file(digest)
            finally:
                run_local_digest.PROJECT_ROOT = original_root
        self.assertIsNotNone(error)
        self.assertIn("final plan verification failed", str(error))


if __name__ == "__main__":
    unittest.main()
