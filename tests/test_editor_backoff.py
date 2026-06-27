"""Wave 1 / S2: the editor must survive the gpt-4o 30k TPM tier instead of
recording a rate-limited batch as a clean pass. Backoff + sequential dispatch
turn round-2-total-failure (3/3 batches 429) into completed coverage.
"""
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


def test_retryable_classification():
    assert editor._is_retryable_api_error(_Rate429()) is True
    # a genuine bad-request must not be retried
    assert editor._is_retryable_api_error(ValueError("invalid response_format schema")) is False


def test_retry_seconds_honours_api_hint_and_caps():
    # "try again in 0.2s" → 0.2 + 0.5 grace
    assert 0.6 <= editor._editor_retry_seconds(_Rate429(), 0) <= 0.8
    assert editor._editor_retry_seconds(ValueError("x"), 9) <= editor.PRE_SEND_EDITOR_RETRY_CAP_SECONDS


def test_create_with_backoff_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(editor.time, "sleep", lambda _s: None)
    completions = _FakeCompletions(fail_times=1)
    out = editor._editor_create_with_backoff(_FakeClient(completions), model="gpt-4o", messages=[])
    assert out == "OK"
    assert completions.calls == 2  # failed once (429), retried, succeeded — not dropped


def test_create_with_backoff_gives_up_on_non_retryable(monkeypatch):
    monkeypatch.setattr(editor.time, "sleep", lambda _s: None)

    class _Bad(_FakeCompletions):
        def create(self, **_kwargs):
            self.calls += 1
            raise ValueError("invalid schema")

    bad = _Bad(fail_times=0)
    try:
        editor._editor_create_with_backoff(_FakeClient(bad), model="gpt-4o", messages=[])
        raised = False
    except ValueError:
        raised = True
    assert raised
    assert bad.calls == 1  # no pointless retries on a bad request


def test_editor_dispatch_is_sequential_by_default():
    # 3 concurrent 24k-token batches were what breached the 30k TPM limit.
    assert editor.PRE_SEND_EDITOR_MAX_WORKERS == 1
