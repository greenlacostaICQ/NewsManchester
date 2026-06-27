"""E1/E2: editor pacing (reuse the token bucket, run concurrent) + tiered
evidence (full only for faithfulness-critical lines)."""
from news_digest.pipeline import editor


def test_tiered_evidence_full_for_sensitive_short_for_routine():
    crime = {"draft_line": "• Мужчина осуждён за нападение с ножом в Манчестере."}
    routine = {"draft_line": "• Levenshulme Artisan Market пройдёт в субботу с 10:00."}
    assert editor._evidence_is_sensitive(crime) is True
    assert editor._evidence_is_sensitive(routine) is False
    # English-only source still counts (title/evidence not yet translated)
    assert editor._evidence_is_sensitive({"title": "Man charged with murder"}) is True


def test_token_limiter_builds_and_paces_without_cycle(monkeypatch):
    monkeypatch.setattr(editor.time, "sleep", lambda _s: None)
    limiter = editor._editor_token_limiter()
    limiter.acquire(1000)  # reuses the rewrite-stage bucket; no import cycle, returns
    assert editor.PRE_SEND_EDITOR_MAX_WORKERS == 3  # concurrency restored (paced, not sequential)
