"""Delivery hardening regression tests.

2026-05-29: a single raw "&" inside an event name ("Currents & Erra",
"Brighton & Hove Albion") made Telegram reject the chunk with HTTP 400, which
aborted the send loop mid-digest — the football card (rendered, later in the
message) never reached the reader and the whole run failed. The fix sanitizes
HTML before sending and, as a last resort, re-sends a rejected chunk as plain
text instead of crashing.
"""
from __future__ import annotations

import unittest

from news_digest.delivery.telegram import (
    TelegramClient,
    TelegramTransportError,
    html_to_plain_text,
    sanitize_telegram_html,
)


class TelegramDeliveryHardeningTest(unittest.TestCase):
    def test_sanitize_escapes_raw_ampersand_but_keeps_tags(self):
        out = sanitize_telegram_html('<b>Currents & Erra</b> <a href="https://x">t</a>')
        self.assertEqual(out, '<b>Currents &amp; Erra</b> <a href="https://x">t</a>')
        # bare angle brackets in source text get escaped too
        self.assertEqual(sanitize_telegram_html("a < b > c"), "a &lt; b &gt; c")
        # already-valid entities survive untouched
        self.assertEqual(
            sanitize_telegram_html("keep &amp; and &lt; intact"),
            "keep &amp; and &lt; intact",
        )

    def test_send_message_falls_back_to_plain_text_on_html_400(self):
        calls: list[dict] = []

        class _Client(TelegramClient):
            def _post(self, method, payload):  # type: ignore[override]
                calls.append(payload)
                if payload.get("parse_mode") == "HTML":
                    raise TelegramTransportError("Bad Request", status_code=400)
                return {"ok": True}

        client = _Client(bot_token="x")
        result = client.send_message("chat", "<b>Brighton & Hove</b>", parse_mode="HTML")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(calls), 2)  # HTML attempt rejected, then plain-text retry
        self.assertNotIn("parse_mode", calls[1])
        self.assertEqual(calls[1]["text"], html_to_plain_text("<b>Brighton &amp; Hove</b>"))

    def test_post_retries_transient_5xx_then_succeeds(self):
        from unittest import mock
        from urllib import error

        client = TelegramClient(bot_token="x", retry_backoff_seconds=0.0)
        attempts = {"n": 0}

        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return b'{"ok": true, "result": {"message_id": 7}}'

        def _fake_urlopen(req, timeout=0):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise error.HTTPError(req.full_url, 503, "Service Unavailable", {}, None)
            return _Resp()

        with mock.patch("news_digest.delivery.telegram.request.urlopen", _fake_urlopen):
            result = client._post("sendMessage", {"chat_id": "c", "text": "hi"})

        self.assertEqual(attempts["n"], 3)  # 503, 503, then 200
        self.assertEqual(result["result"]["message_id"], 7)


if __name__ == "__main__":
    unittest.main()
