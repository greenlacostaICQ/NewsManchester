"""Token-per-minute (TPM) pacing for the LLM rewrite — the real OpenAI ceiling
that caused the 2026-06-11..13 429 storms and the blocked 2026-06-13 digest.
"""
from __future__ import annotations

import time
import unittest

from news_digest.pipeline.llm_rewrite import _TokenRateLimiter, _estimate_request_tokens


class TokenRateLimiterTest(unittest.TestCase):
    def test_estimate_counts_input_chars_and_reserved_output(self) -> None:
        messages = [
            {"role": "system", "content": "a" * 400},
            {"role": "user", "content": "b" * 400},
        ]
        # 800 input chars / 4 + 1000 reserved output.
        self.assertEqual(_estimate_request_tokens(messages, 1000), 200 + 1000)

    def test_within_capacity_is_instant(self) -> None:
        limiter = _TokenRateLimiter(60000)
        start = time.monotonic()
        limiter.acquire(1000)
        limiter.acquire(2000)
        self.assertLess(time.monotonic() - start, 0.2)

    def test_paces_once_budget_is_spent(self) -> None:
        limiter = _TokenRateLimiter(6000)  # 100 tokens/sec
        limiter.acquire(6000)  # drain the bucket
        start = time.monotonic()
        limiter.acquire(200)  # needs ~2s to refill
        waited = time.monotonic() - start
        self.assertGreater(waited, 1.0)
        self.assertLess(waited, 4.0)

    def test_request_larger_than_budget_still_proceeds(self) -> None:
        # A single request bigger than the whole per-minute budget must not
        # wait forever — it is clamped to the capacity.
        limiter = _TokenRateLimiter(6000)
        start = time.monotonic()
        limiter.acquire(99999)
        self.assertLess(time.monotonic() - start, 1.0)


if __name__ == "__main__":
    unittest.main()
