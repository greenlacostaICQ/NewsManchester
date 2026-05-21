"""Tests for the in-run provider circuit breaker."""
from __future__ import annotations

import unittest

from news_digest.pipeline import provider_health


class ProviderHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        provider_health.reset()

    def tearDown(self) -> None:
        provider_health.reset()

    def test_provider_is_alive_initially(self) -> None:
        self.assertFalse(provider_health.is_dead("deepseek"))
        self.assertEqual(provider_health.dead_providers(), [])

    def test_first_failure_does_not_trip_breaker(self) -> None:
        transitioned = provider_health.record_failure("deepseek")
        self.assertFalse(transitioned)
        self.assertFalse(provider_health.is_dead("deepseek"))

    def test_two_consecutive_failures_trip_breaker(self) -> None:
        provider_health.record_failure("deepseek")
        transitioned = provider_health.record_failure("deepseek")
        self.assertTrue(transitioned)
        self.assertTrue(provider_health.is_dead("deepseek"))
        self.assertEqual(provider_health.dead_providers(), ["deepseek"])

    def test_success_resets_failure_counter(self) -> None:
        provider_health.record_failure("deepseek")
        provider_health.record_success("deepseek")
        # After success, the next failure is a fresh start (count=1, not trip).
        transitioned = provider_health.record_failure("deepseek")
        self.assertFalse(transitioned)
        self.assertFalse(provider_health.is_dead("deepseek"))

    def test_providers_tracked_independently(self) -> None:
        provider_health.record_failure("deepseek")
        provider_health.record_failure("deepseek")
        self.assertTrue(provider_health.is_dead("deepseek"))
        # Openai untouched.
        self.assertFalse(provider_health.is_dead("openai"))
        provider_health.record_failure("openai")
        self.assertFalse(provider_health.is_dead("openai"))

    def test_record_failure_on_dead_provider_is_idempotent(self) -> None:
        provider_health.record_failure("deepseek")
        provider_health.record_failure("deepseek")
        self.assertTrue(provider_health.is_dead("deepseek"))
        transitioned = provider_health.record_failure("deepseek")
        self.assertFalse(transitioned)
        # Still dead, dead_providers stays the same.
        self.assertEqual(provider_health.dead_providers(), ["deepseek"])

    def test_reset_clears_state(self) -> None:
        provider_health.record_failure("deepseek")
        provider_health.record_failure("deepseek")
        provider_health.record_failure("openai")
        provider_health.reset()
        self.assertFalse(provider_health.is_dead("deepseek"))
        self.assertFalse(provider_health.is_dead("openai"))
        self.assertEqual(provider_health.dead_providers(), [])


if __name__ == "__main__":
    unittest.main()
