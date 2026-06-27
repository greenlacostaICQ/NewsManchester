"""E1/E2: editor pacing (reuse the token bucket, run concurrent) + tiered
evidence (full only for faithfulness-critical lines)."""
import unittest
from unittest import mock

from news_digest.pipeline import editor


class EditorPacingTest(unittest.TestCase):
    def test_tiered_evidence_full_for_sensitive_short_for_routine(self):
        crime = {"draft_line": "• Мужчина осуждён за нападение с ножом в Манчестере."}
        routine = {"draft_line": "• Levenshulme Artisan Market пройдёт в субботу с 10:00."}
        self.assertTrue(editor._evidence_is_sensitive(crime))
        self.assertFalse(editor._evidence_is_sensitive(routine))
        # English-only source still counts (title/evidence not yet translated)
        self.assertTrue(editor._evidence_is_sensitive({"title": "Man charged with murder"}))

    def test_token_limiter_builds_and_paces_without_cycle(self):
        with mock.patch.object(editor.time, "sleep", lambda _s: None):
            limiter = editor._editor_token_limiter()
            limiter.acquire(1000)  # reuses the rewrite-stage bucket; no import cycle
        self.assertGreaterEqual(editor.PRE_SEND_EDITOR_MAX_WORKERS, 2)


if __name__ == "__main__":
    unittest.main()
