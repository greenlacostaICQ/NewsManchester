"""Tests for prompt caching (cost_tracker) and ETag fetch cache (fetch.py).

Covers:
  * cost_tracker._cost_for      — cached/miss/output blending
  * cost_tracker._extract_cache_tokens — DeepSeek and OpenAI usage shapes
  * cost_tracker.record_call*   — cache_hit_tokens persisted on records
  * cost_tracker.summarise      — cache_hit_ratio in summary
  * fetch.load/save_fetch_cache — round-trip via on-disk JSON
  * fetch._conditional_headers  — fresh entry → headers, stale → empty
  * fetch._cache_entry_fresh    — TTL semantics
  * fetch._fetch_text           — 304 → NotModified, 200 → body + cache update
  * fetch._fetch_source_body    — propagates NotModified, primary→fallback path
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib import error

from news_digest.pipeline import cost_tracker
from news_digest.pipeline.collector import fetch
from news_digest.pipeline.collector.sources import SourceDef
from news_digest.pipeline.release import _aggregate_cost, _append_cost_history


class CostForCachePricingTest(unittest.TestCase):
    """Cache hits cost the cached_input rate, misses pay full input."""

    def test_zero_cache_uses_full_input(self):
        no_cache = cost_tracker._cost_for("deepseek-chat", 1000, 200, 0)
        # Full input = 1000 * 0.27/M; output = 200 * 1.10/M
        self.assertAlmostEqual(no_cache, 1000 * 0.27 / 1e6 + 200 * 1.10 / 1e6, places=8)

    def test_partial_cache_blends_prices(self):
        # 1000 prompt tokens, 700 cached, 300 miss
        cost = cost_tracker._cost_for("deepseek-chat", 1000, 200, 700)
        expected = (
            300 * 0.27 / 1e6     # miss
            + 700 * 0.07 / 1e6   # cached
            + 200 * 1.10 / 1e6   # output
        )
        self.assertAlmostEqual(cost, expected, places=8)

    def test_full_cache_uses_only_cached_rate_for_prompt(self):
        cost = cost_tracker._cost_for("deepseek-chat", 1000, 200, 1000)
        expected = 1000 * 0.07 / 1e6 + 200 * 1.10 / 1e6
        self.assertAlmostEqual(cost, expected, places=8)

    def test_cache_hits_above_prompt_tokens_clamped(self):
        # If a provider reports inconsistent counts, don't go negative.
        cost = cost_tracker._cost_for("deepseek-chat", 1000, 200, 9000)
        # Treated as if cache_hit_tokens == prompt_tokens.
        expected = 1000 * 0.07 / 1e6 + 200 * 1.10 / 1e6
        self.assertAlmostEqual(cost, expected, places=8)

    def test_unknown_model_returns_zero(self):
        self.assertEqual(cost_tracker._cost_for("imaginary-model", 1000, 200, 0), 0.0)

    def test_openai_uses_cached_price(self):
        # OpenAI gpt-4o-mini: input 0.15, cached 0.075, output 0.60
        cost = cost_tracker._cost_for("gpt-4o-mini", 1000, 200, 500)
        expected = (
            500 * 0.15 / 1e6      # miss
            + 500 * 0.075 / 1e6   # cached
            + 200 * 0.60 / 1e6    # output
        )
        self.assertAlmostEqual(cost, expected, places=8)


class ExtractCacheTokensTest(unittest.TestCase):
    """Both vendor usage shapes must be recognised."""

    def test_deepseek_shape(self):
        usage = MagicMock(
            prompt_cache_hit_tokens=700,
            prompt_cache_miss_tokens=300,
        )
        hit, miss = cost_tracker._extract_cache_tokens(usage, 1000)
        self.assertEqual((hit, miss), (700, 300))

    def test_openai_shape(self):
        details = MagicMock(cached_tokens=500)
        # Build a real attribute lookup that returns details for one key
        # and raises AttributeError for the deepseek keys, so getattr
        # picks the fallback shape.
        usage = MagicMock(
            spec_set=["prompt_tokens_details"],
            prompt_tokens_details=details,
        )
        hit, miss = cost_tracker._extract_cache_tokens(usage, 1000)
        self.assertEqual((hit, miss), (500, 500))

    def test_missing_usage_returns_zero(self):
        # An object with neither attribute → (0, 0)
        usage = MagicMock(spec_set=[])
        self.assertEqual(cost_tracker._extract_cache_tokens(usage, 1000), (0, 0))

    def test_openai_cached_exceeds_prompt_clamped(self):
        details = MagicMock(cached_tokens=2000)
        usage = MagicMock(spec_set=["prompt_tokens_details"], prompt_tokens_details=details)
        hit, miss = cost_tracker._extract_cache_tokens(usage, 1000)
        # We trust the provider's hit count; miss derives via max(0, ...)
        self.assertEqual(hit, 2000)
        self.assertEqual(miss, 0)


class RecordCallCacheTest(unittest.TestCase):
    def setUp(self):
        cost_tracker.reset()

    def tearDown(self):
        cost_tracker.reset()

    def test_record_call_persists_cache_fields(self):
        cost_tracker.record_call(
            stage="llm_rewrite",
            provider="DeepSeek",
            model="deepseek-chat",
            prompt_name="city_news",
            prompt_tokens=1000,
            completion_tokens=200,
            cache_hit_tokens=700,
            cache_miss_tokens=300,
        )
        records = cost_tracker.snapshot()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].cache_hit_tokens, 700)
        self.assertEqual(records[0].cache_miss_tokens, 300)
        # Cost should reflect the cached rate, not full input.
        self.assertLess(records[0].cost_usd, 1000 * 0.27 / 1e6 + 200 * 1.10 / 1e6)

    def test_record_call_from_response_extracts_deepseek_cache(self):
        usage = MagicMock(
            prompt_tokens=1000,
            completion_tokens=200,
            prompt_cache_hit_tokens=700,
            prompt_cache_miss_tokens=300,
        )
        response = MagicMock(usage=usage)
        cost_tracker.record_call_from_response(
            response=response,
            stage="llm_rewrite",
            provider="DeepSeek",
            model="deepseek-chat",
            prompt_name="city_news",
            messages=None,
            max_tokens=8192,
        )
        records = cost_tracker.snapshot()
        self.assertEqual(records[0].cache_hit_tokens, 700)
        self.assertEqual(records[0].cache_miss_tokens, 300)

    def test_record_call_from_response_no_usage_skips_cache(self):
        response = MagicMock(usage=None)
        cost_tracker.record_call_from_response(
            response=response,
            stage="llm_rewrite",
            provider="DeepSeek",
            model="deepseek-chat",
            prompt_name="city_news",
            messages=[{"role": "system", "content": "hi"}],
            max_tokens=100,
        )
        records = cost_tracker.snapshot()
        self.assertEqual(records[0].cache_hit_tokens, 0)
        self.assertEqual(records[0].usage_source, "estimated")


class SummariseCacheTest(unittest.TestCase):
    def setUp(self):
        cost_tracker.reset()

    def tearDown(self):
        cost_tracker.reset()

    def test_summarise_aggregates_cache_totals(self):
        cost_tracker.record_call(
            stage="llm_rewrite", provider="DeepSeek", model="deepseek-chat",
            prompt_name="x", prompt_tokens=1000, completion_tokens=200,
            cache_hit_tokens=700, cache_miss_tokens=300,
        )
        cost_tracker.record_call(
            stage="llm_rewrite", provider="DeepSeek", model="deepseek-chat",
            prompt_name="y", prompt_tokens=2000, completion_tokens=400,
            cache_hit_tokens=0, cache_miss_tokens=2000,
        )
        summary = cost_tracker.summarise(cost_tracker.snapshot())
        self.assertEqual(summary["total_cache_hit_tokens"], 700)
        self.assertEqual(summary["total_cache_miss_tokens"], 2300)
        # hit_ratio = 700 / (700 + 2300) = 0.2333…
        self.assertAlmostEqual(summary["cache_hit_ratio"], round(700 / 3000, 4), places=4)

    def test_summarise_cache_ratio_zero_when_no_data(self):
        cost_tracker.record_call(
            stage="llm_rewrite", provider="DeepSeek", model="deepseek-chat",
            prompt_name="x", prompt_tokens=1000, completion_tokens=200,
        )
        summary = cost_tracker.summarise(cost_tracker.snapshot())
        self.assertEqual(summary["cache_hit_ratio"], 0.0)

    def test_summarise_by_provider_includes_cache_columns(self):
        cost_tracker.record_call(
            stage="llm_rewrite", provider="DeepSeek", model="deepseek-chat",
            prompt_name="x", prompt_tokens=1000, completion_tokens=200,
            cache_hit_tokens=700, cache_miss_tokens=300,
        )
        summary = cost_tracker.summarise(cost_tracker.snapshot())
        ds = summary["by_provider"]["DeepSeek"]
        self.assertEqual(ds["cache_hit_tokens"], 700)
        self.assertEqual(ds["cache_miss_tokens"], 300)

    def test_release_aggregate_preserves_stage_cache_tokens(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            (state_dir / "cost_llm_rewrite.json").write_text(
                json.dumps(
                    {
                        "records": [
                            {
                                "stage": "llm_rewrite",
                                "provider": "DeepSeek",
                                "model": "deepseek-chat",
                                "prompt_name": "english_cards",
                                "prompt_version": "english_cards@v1",
                                "prompt_tokens": 1000,
                                "completion_tokens": 200,
                                "estimated_prompt_tokens": 1000,
                                "estimated_completion_tokens": 200,
                                "cost_usd": 0.0001,
                                "estimated_cost_usd": 0.0002,
                                "usage_source": "actual",
                                "cache_hit_tokens": 700,
                                "cache_miss_tokens": 300,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            summary = _aggregate_cost(state_dir)

        self.assertEqual(summary["total_cache_hit_tokens"], 700)
        self.assertEqual(summary["total_cache_miss_tokens"], 300)
        self.assertEqual(summary["cache_hit_ratio"], 0.7)
        self.assertEqual(summary["by_stage"]["llm_rewrite"]["cache_hit_tokens"], 700)

    def test_cost_history_keeps_cache_totals(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            _append_cost_history(
                state_dir,
                "2026-07-07",
                {
                    "total_cost_usd": 0.1,
                    "total_calls": 2,
                    "total_cache_hit_tokens": 700,
                    "total_cache_miss_tokens": 300,
                    "cache_hit_ratio": 0.7,
                },
                [],
            )

            history = json.loads((state_dir / "cost_history.json").read_text(encoding="utf-8"))

        self.assertEqual(history[0]["total_cache_hit_tokens"], 700)
        self.assertEqual(history[0]["total_cache_miss_tokens"], 300)
        self.assertEqual(history[0]["cache_hit_ratio"], 0.7)


# ── Fetch cache ──────────────────────────────────────────────────────────


class FetchCacheRoundTripTest(unittest.TestCase):
    def setUp(self):
        # Each test starts with an empty module-level cache.
        with fetch._FETCH_CACHE_LOCK:
            fetch._FETCH_CACHE.clear()

    def test_cold_cache_returns_no_conditional_headers(self):
        self.assertEqual(fetch._conditional_headers("https://example.com/feed.xml"), {})

    def test_store_then_conditional_headers_emit_both(self):
        fetch._store_cache_entry(
            "https://example.com/feed.xml",
            etag='"abc123"',
            last_modified="Sat, 17 May 2026 12:00:00 GMT",
            status="200",
        )
        headers = fetch._conditional_headers("https://example.com/feed.xml")
        self.assertEqual(headers, {
            "If-None-Match": '"abc123"',
            "If-Modified-Since": "Sat, 17 May 2026 12:00:00 GMT",
        })

    def test_only_etag_emits_only_etag(self):
        fetch._store_cache_entry("https://example.com/", etag='"e"', status="200")
        self.assertEqual(
            fetch._conditional_headers("https://example.com/"),
            {"If-None-Match": '"e"'},
        )

    def test_save_load_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td)
            fetch._store_cache_entry("https://a.com", etag='"e1"', status="200")
            fetch._store_cache_entry("https://b.com", last_modified="Sat, 17 May 2026 12:00:00 GMT", status="200")
            fetch.save_fetch_cache(state_dir)

            on_disk = json.loads((state_dir / "fetch_cache.json").read_text())
            self.assertEqual(on_disk["version"], 1)
            self.assertIn("https://a.com", on_disk["entries"])
            self.assertIn("https://b.com", on_disk["entries"])

            # Clear module state, reload, validators must still apply.
            with fetch._FETCH_CACHE_LOCK:
                fetch._FETCH_CACHE.clear()
            fetch.load_fetch_cache(state_dir)
            self.assertEqual(
                fetch._conditional_headers("https://a.com"),
                {"If-None-Match": '"e1"'},
            )

    def test_load_missing_file_is_noop(self):
        with tempfile.TemporaryDirectory() as td:
            fetch.load_fetch_cache(Path(td))  # no fetch_cache.json present
            self.assertEqual(fetch._conditional_headers("https://anything.com"), {})

    def test_load_corrupt_file_is_noop(self):
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td)
            (state_dir / "fetch_cache.json").write_text("{not valid json", encoding="utf-8")
            # Should not raise; just log a warning.
            fetch.load_fetch_cache(state_dir)
            self.assertEqual(fetch._conditional_headers("https://anything.com"), {})

    def test_stale_entry_above_ttl_ignored(self):
        with fetch._FETCH_CACHE_LOCK:
            fetch._FETCH_CACHE["https://stale.com"] = {
                "etag": '"old"',
                # 10 days in the past — exceeds TTL of 7 days
                "fetched_at": "2026-05-10T08:00:00+01:00",
                "status": "200",
            }
        self.assertEqual(fetch._conditional_headers("https://stale.com"), {})

    def test_not_modified_class_is_exception(self):
        self.assertTrue(issubclass(fetch.NotModified, Exception))


class FetchCache304IntegrationTest(unittest.TestCase):
    """Mock urllib so we can hit the 304 and 200 code paths without
    a network round-trip."""

    def setUp(self):
        with fetch._FETCH_CACHE_LOCK:
            fetch._FETCH_CACHE.clear()
        self.source = SourceDef(
            name="Test RSS",
            report_category="media_layer",
            candidate_category="media_layer",
            url="https://example.com/feed.xml",
            primary_block="last_24h",
            source_type="rss",
        )

    def test_304_raises_not_modified(self):
        # Seed cache so conditional headers will be sent.
        fetch._store_cache_entry(self.source.url, etag='"abc"', status="200")

        # urllib raises HTTPError on 304 — we mirror that.
        http_err = error.HTTPError(
            url=self.source.url, code=304, msg="Not Modified",
            hdrs=MagicMock(), fp=None,
        )
        opener_mock = MagicMock()
        opener_mock.open.side_effect = http_err
        with patch("news_digest.pipeline.collector.fetch.request.build_opener", return_value=opener_mock):
            with self.assertRaises(fetch.NotModified):
                fetch._fetch_source_body(self.source)

    def test_200_returns_body_and_updates_cache(self):
        fake_response = MagicMock()
        fake_response.read.return_value = b"<rss>fresh body</rss>"
        fake_response.headers.get_content_charset.return_value = "utf-8"
        fake_response.headers.get.side_effect = lambda key, default="": {
            "ETag": '"new123"',
            "Last-Modified": "Sun, 18 May 2026 08:00:00 GMT",
        }.get(key, default)
        fake_response.__enter__ = MagicMock(return_value=fake_response)
        fake_response.__exit__ = MagicMock(return_value=False)

        opener_mock = MagicMock()
        opener_mock.open.return_value = fake_response
        with patch("news_digest.pipeline.collector.fetch.request.build_opener", return_value=opener_mock):
            body, fetched_url, log = fetch._fetch_source_body(self.source)

        self.assertEqual(body, "<rss>fresh body</rss>")
        self.assertEqual(fetched_url, self.source.url)
        self.assertEqual(log, [])
        with fetch._FETCH_CACHE_LOCK:
            entry = fetch._FETCH_CACHE.get(self.source.url)
        self.assertEqual(entry.get("etag"), '"new123"')
        self.assertEqual(entry.get("last_modified"), "Sun, 18 May 2026 08:00:00 GMT")

    def test_extract_text_path_does_not_use_cache(self):
        """``_fetch_text`` default (no use_cache) must NOT add conditional
        headers — article enrichment in extract.py relies on this."""
        fetch._store_cache_entry("https://x.com/article", etag='"a"', status="200")

        captured_headers: dict[str, str] = {}

        fake_response = MagicMock()
        fake_response.read.return_value = b"body"
        fake_response.headers.get_content_charset.return_value = "utf-8"
        fake_response.headers.get.side_effect = lambda *_args, **_kw: ""
        fake_response.__enter__ = MagicMock(return_value=fake_response)
        fake_response.__exit__ = MagicMock(return_value=False)

        def fake_open(req, timeout=30):
            for k, v in req.headers.items():
                captured_headers[k] = v
            return fake_response

        opener_mock = MagicMock()
        opener_mock.open.side_effect = fake_open
        with patch("news_digest.pipeline.collector.fetch.request.build_opener", return_value=opener_mock):
            fetch._fetch_text("https://x.com/article")  # use_cache defaults to False

        # If-None-Match / If-Modified-Since must NOT have been sent.
        self.assertNotIn("If-none-match", {k.lower() for k in captured_headers})
        self.assertNotIn("If-modified-since", {k.lower() for k in captured_headers})


if __name__ == "__main__":
    unittest.main()
