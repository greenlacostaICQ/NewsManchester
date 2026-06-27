"""Wave 1 / S2 + E1: the editor must survive the gpt-4o 30k TPM tier instead of
recording a rate-limited batch as a clean pass, and (E1) run paced-concurrent
rather than sequential.
"""
import unittest
from unittest import mock

import news_digest.pipeline.editor as editor


class _Rate429(Exception):
    status_code = 429

    def __str__(self) -> str:
        return "Rate limit reached for gpt-4o ... Please try again in 0.2s. ..."


class _FakeCompletions:
    def __init__(self, fail_times: int):
        self.calls = 0
        self._fail_times = fail_times

    def create(self, **_kwargs):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise _Rate429()
        return "OK"


class _FakeClient:
    def __init__(self, completions: _FakeCompletions):
        self.chat = type("_Chat", (), {"completions": completions})()


class EditorBackoffTest(unittest.TestCase):
    def test_retryable_classification(self):
        self.assertTrue(editor._is_retryable_api_error(_Rate429()))
        self.assertFalse(editor._is_retryable_api_error(ValueError("invalid response_format schema")))

    def test_retry_seconds_honours_api_hint_and_caps(self):
        self.assertTrue(0.6 <= editor._editor_retry_seconds(_Rate429(), 0) <= 0.8)
        self.assertLessEqual(editor._editor_retry_seconds(ValueError("x"), 9), editor.PRE_SEND_EDITOR_RETRY_CAP_SECONDS)

    def test_create_with_backoff_retries_then_succeeds(self):
        with mock.patch.object(editor.time, "sleep", lambda _s: None):
            completions = _FakeCompletions(fail_times=1)
            out = editor._editor_create_with_backoff(_FakeClient(completions), model="gpt-4o", messages=[])
        self.assertEqual(out, "OK")
        self.assertEqual(completions.calls, 2)  # failed once (429), retried, succeeded

    def test_create_with_backoff_gives_up_on_non_retryable(self):
        class _Bad(_FakeCompletions):
            def create(self, **_kwargs):
                self.calls += 1
                raise ValueError("invalid schema")

        bad = _Bad(fail_times=0)
        with mock.patch.object(editor.time, "sleep", lambda _s: None):
            with self.assertRaises(ValueError):
                editor._editor_create_with_backoff(_FakeClient(bad), model="gpt-4o", messages=[])
        self.assertEqual(bad.calls, 1)  # no pointless retries on a bad request

    def test_editor_dispatch_is_paced_concurrent(self):
        # E1 superseded S2's sequential stop-gap: concurrency restored but gated
        # by a token bucket sized to the TPM ceiling — fast AND never 429s.
        self.assertGreaterEqual(editor.PRE_SEND_EDITOR_MAX_WORKERS, 2)
        self.assertGreater(editor.PRE_SEND_EDITOR_MAX_TPM, 0)


if __name__ == "__main__":
    unittest.main()
