"""Notability enrichment: parallel prefetch, short-circuit, error taxonomy,
read-only writer path. These pin the 2026-06-11 fix where ~100 serial artist
lookups pushed the writer stage to ~6 minutes.

All lookups are mocked — nothing touches the network. The throttle is patched
to a no-op so tests don't sleep.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

from news_digest.pipeline import ticket_notability as tn
from news_digest.pipeline.common import now_london


def _ticket(title: str, *, days_out: int = 3) -> dict:
    day = (now_london() + timedelta(days=days_out)).date().isoformat()
    return {
        "primary_block": "ticket_radar",
        "category": "venues_tickets",
        "title": title,
        "source_label": "Ticketmaster Manchester",
        "event": {"date": day},
    }


class ArtistNotabilityTest(unittest.TestCase):
    def setUp(self) -> None:
        # No sleeping in tests, and a clean env each time.
        self._throttle = mock.patch.object(tn._THROTTLE, "wait", lambda host: None)
        self._throttle.start()
        self.addCleanup(self._throttle.stop)
        self._env = mock.patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "1"})
        self._env.start()
        self.addCleanup(self._env.stop)

    def test_short_circuit_skips_other_apis_when_wikidata_notable(self) -> None:
        calls = {"wd": 0, "yt": 0, "lf": 0, "mb": 0}

        def wd(_a):
            calls["wd"] += 1
            return {"sitelinks": 90, "wikidata_id": "Q123", "description": "band"}

        def yt(_a, _known_id=""):
            calls["yt"] += 1
            return {}

        def lf(_a):
            calls["lf"] += 1
            return {}

        def mb(_a):
            calls["mb"] += 1
            return {}

        with mock.patch.multiple(
            tn,
            _lookup_wikidata=wd,
            _lookup_youtube=yt,
            _lookup_lastfm=lf,
            _lookup_musicbrainz=mb,
        ):
            result = tn._artist_notability("Coldplay", "artist", _ticket("Coldplay"), {}, now_london(), allow_network=True)

        self.assertEqual(result.tier, "A")
        self.assertEqual(calls["wd"], 1)
        # The whole point: a clearly-notable Wikidata hit means we never spend
        # YouTube/Last.fm/MusicBrainz (the rate-limited one).
        self.assertEqual((calls["yt"], calls["lf"], calls["mb"]), (0, 0, 0))

    def test_musicbrainz_only_runs_when_still_unknown(self) -> None:
        calls = {"mb": 0}

        def thin(_a):
            return {}

        def mb(_a):
            calls["mb"] += 1
            return {}

        with mock.patch.multiple(
            tn,
            _lookup_wikidata=thin,
            _lookup_youtube=lambda artist, known_id="": thin(artist),
            _lookup_lastfm=thin,
            _lookup_musicbrainz=mb,
        ):
            tn._artist_notability("Obscure Act", "artist", _ticket("Obscure Act"), {}, now_london(), allow_network=True)

        # Wikidata + YouTube + Last.fm all blank → still unknown → MusicBrainz runs.
        self.assertEqual(calls["mb"], 1)

    def test_read_only_does_no_network(self) -> None:
        calls = {"n": 0}

        def boom(_a):
            calls["n"] += 1
            return {}

        with mock.patch.multiple(
            tn,
            _lookup_wikidata=boom,
            _lookup_youtube=lambda artist, known_id="": boom(artist),
            _lookup_lastfm=boom,
            _lookup_musicbrainz=boom,
        ):
            # Default allow_network=False (the writer render-loop path).
            result = tn._artist_notability("Anyone", "artist", _ticket("Anyone"), {}, now_london())

        self.assertEqual(calls["n"], 0)
        self.assertEqual(result.signal, "lookup_disabled")

    def test_error_taxonomy_recheck_windows(self) -> None:
        def raise_(_a):
            raise OSError("network down")

        def empty(_a):
            return {}

        def notable(_a):
            return {"sitelinks": 90, "wikidata_id": "Q9"}

        # api_failed → retry next run (1 day).
        cache: dict = {}
        with mock.patch.multiple(
            tn,
            _lookup_wikidata=raise_,
            _lookup_youtube=lambda artist, known_id="": empty(artist),
            _lookup_lastfm=empty,
            _lookup_musicbrainz=empty,
        ):
            tn._artist_notability("Fails", "artist", _ticket("Fails"), cache, now_london(), allow_network=True)
        self.assertEqual(cache[tn._cache_key("Fails")]["recheck_days"], 1)

        # clean not_found → 7 days.
        cache = {}
        with mock.patch.multiple(
            tn,
            _lookup_wikidata=empty,
            _lookup_youtube=lambda artist, known_id="": empty(artist),
            _lookup_lastfm=empty,
            _lookup_musicbrainz=empty,
        ):
            tn._artist_notability("Nobody", "artist", _ticket("Nobody"), cache, now_london(), allow_network=True)
        self.assertEqual(cache[tn._cache_key("Nobody")]["recheck_days"], 7)

        # found → 30 days.
        cache = {}
        with mock.patch.multiple(
            tn,
            _lookup_wikidata=notable,
            _lookup_youtube=lambda artist, known_id="": empty(artist),
            _lookup_lastfm=empty,
            _lookup_musicbrainz=empty,
        ):
            tn._artist_notability("Famous", "artist", _ticket("Famous"), cache, now_london(), allow_network=True)
        self.assertEqual(cache[tn._cache_key("Famous")]["recheck_days"], 30)

    def test_failed_recheck_preserves_last_complete_tier(self) -> None:
        now = now_london()
        cache = {
            tn._cache_key("Stable Artist"): {
                "artist": "Stable Artist",
                "kind": "artist",
                "tier": "A",
                "confidence": 0.97,
                "signal": "youtube+lastfm",
                "signals": {
                    "youtube_subscribers": 3_000_000,
                    "lastfm_listeners": 1_500_000,
                },
                "checked_at": (now - timedelta(days=31)).isoformat(),
                "recheck_days": 30,
            }
        }
        with mock.patch.multiple(
            tn,
            _lookup_wikidata=mock.Mock(side_effect=OSError("down")),
            _lookup_youtube=mock.Mock(side_effect=OSError("down")),
            _lookup_lastfm=mock.Mock(side_effect=OSError("down")),
            _lookup_musicbrainz=mock.Mock(side_effect=OSError("down")),
        ):
            result = tn._artist_notability(
                "Stable Artist",
                "artist",
                _ticket("Stable Artist"),
                cache,
                now,
                allow_network=True,
            )
        self.assertEqual(result.tier, "A")
        self.assertEqual(cache[tn._cache_key("Stable Artist")]["tier"], "A")
        self.assertTrue(cache[tn._cache_key("Stable Artist")]["a_tier_recheck_pending"])

    def test_canonical_stamp_overwrites_conflicting_tiers_on_every_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "ticket_notability_cache.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "version": tn._CACHE_VERSION,
                        "artists": {
                            tn._cache_key("Anastacia"): {
                                "artist": "Anastacia",
                                "kind": "artist",
                                "tier": "A",
                                "confidence": 0.98,
                                "signal": "wikidata_sitelinks",
                                "signals": {"sitelinks": 100},
                                "sitelinks": 100,
                                "checked_at": now_london().isoformat(),
                                "recheck_days": 30,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            rows = [
                {
                    **_ticket("Anastacia"),
                    "include": True,
                    "ticket_notability": {"artist": "Anastacia", "tier": "A"},
                },
                {
                    **_ticket("Anastacia"),
                    "include": False,
                    "source_label": "Second feed",
                    "ticket_notability": {"artist": "Anastacia", "tier": "B"},
                },
            ]
            report = tn.stamp_canonical_ticket_notability(rows, cache_path)
        self.assertEqual({row["ticket_notability"]["tier"] for row in rows}, {"A"})
        self.assertEqual(report["preexisting_tier_conflicts"]["anastacia"], ["A", "B"])

    def test_canonical_stamp_does_not_erase_complete_tier_when_cache_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            rows = [
                {
                    **_ticket("Vanessa Carlton"),
                    "include": True,
                    "ticket_notability": {
                        "artist": "Vanessa Carlton",
                        "kind": "artist",
                        "tier": "A",
                        "confidence": 0.95,
                    },
                },
                {
                    **_ticket("Vanessa Carlton"),
                    "include": False,
                    "ticket_notability": {
                        "artist": "Vanessa Carlton",
                        "kind": "artist",
                        "tier": "B",
                        "confidence": 0.8,
                    },
                },
            ]
            tn.stamp_canonical_ticket_notability(
                rows,
                Path(tmp) / "missing-cache.json",
            )
        self.assertEqual({row["ticket_notability"]["tier"] for row in rows}, {"A"})

    def test_lastfm_alone_cannot_award_a_and_provider_status_is_reported(self) -> None:
        cache: dict = {}
        with mock.patch.multiple(
            tn,
            _lookup_wikidata=lambda _a: {},
            _lookup_youtube=lambda _a, _known_id="": {"_provider_status": "no_credentials"},
            _lookup_lastfm=lambda _a: {"lastfm_listeners": 2_000_000},
            _lookup_musicbrainz=lambda _a: {},
        ):
            result = tn._artist_notability(
                "Legacy-heavy artist",
                "artist",
                _ticket("Legacy-heavy artist"),
                cache,
                now_london(),
                allow_network=True,
            )
        self.assertNotEqual(result.tier, "A")
        self.assertEqual((result.signals or {})["provider_status"]["youtube"], "no_credentials")
        self.assertEqual(cache[tn._cache_key("Legacy-heavy artist")]["recheck_days"], 1)

    def test_youtube_plus_lastfm_can_award_a(self) -> None:
        cache: dict = {}
        with mock.patch.multiple(
            tn,
            _lookup_wikidata=lambda _a: {},
            _lookup_youtube=lambda _a, _known_id="": {
                "youtube_channel_id": "UC123",
                "youtube_subscribers": 3_000_000,
                "youtube_views": 900_000_000,
            },
            _lookup_lastfm=lambda _a: {"lastfm_listeners": 1_800_000},
            _lookup_musicbrainz=lambda _a: {},
        ):
            result = tn._artist_notability(
                "Two-signal artist",
                "artist",
                _ticket("Two-signal artist"),
                cache,
                now_london(),
                allow_network=True,
            )
        self.assertEqual(result.tier, "A")
        self.assertEqual((result.signals or {})["provider_status"]["youtube"], "ok")

    def test_wikidata_channel_id_skips_youtube_search(self) -> None:
        day = now_london().date().isoformat()
        tn._YOUTUBE_SEARCH_BUDGET.bind({"date": day, "search_calls": 0}, day)
        channel_payload = {
            "items": [{
                "id": "UCknown",
                "snippet": {"title": "Known Artist"},
                "statistics": {"subscriberCount": "1200000", "viewCount": "400000000"},
            }]
        }
        with mock.patch.dict(os.environ, {"YOUTUBE_API_KEY": "test-key"}), mock.patch.object(
            tn, "_youtube_json", return_value=channel_payload
        ) as youtube_json:
            result = tn._lookup_youtube("Known Artist", "UCknown")
        self.assertEqual(result["youtube_channel_id"], "UCknown")
        self.assertEqual(result["youtube_subscribers"], 1_200_000)
        self.assertEqual(tn._YOUTUBE_SEARCH_BUDGET.snapshot()["search_calls"], 0)
        self.assertEqual(youtube_json.call_args.args[0], "channels")

    def test_wikidata_extracts_official_youtube_channel_claim(self) -> None:
        search_payload = {
            "search": [{"id": "Q123", "label": "Known Artist", "description": "English singer"}]
        }
        details_payload = {
            "entities": {
                "Q123": {
                    "descriptions": {"en": {"value": "English singer and songwriter"}},
                    "sitelinks": {"enwiki": {}, "dewiki": {}},
                    "claims": {
                        "P2397": [{
                            "mainsnak": {"datavalue": {"value": "UCknown"}}
                        }]
                    },
                }
            }
        }
        with mock.patch.object(
            tn,
            "_wikidata_json",
            side_effect=[search_payload, details_payload],
        ):
            result = tn._lookup_wikidata("Known Artist")
        self.assertEqual(result["wikidata_id"], "Q123")
        self.assertEqual(result["youtube_channel_id"], "UCknown")

    def test_hidden_youtube_subscribers_are_recorded_without_fake_count(self) -> None:
        payload = {
            "items": [{
                "id": "UChidden",
                "snippet": {"title": "Hidden Artist"},
                "statistics": {
                    "hiddenSubscriberCount": True,
                    "subscriberCount": "9999999",
                    "viewCount": "800000000",
                },
            }]
        }
        with mock.patch.dict(os.environ, {"YOUTUBE_API_KEY": "test-key"}), mock.patch.object(
            tn, "_youtube_json", return_value=payload
        ):
            result = tn._lookup_youtube("Hidden Artist", "UChidden")
        self.assertTrue(result["youtube_subscribers_hidden"])
        self.assertEqual(result["youtube_subscribers"], 0)
        self.assertEqual(result["youtube_views"], 800_000_000)

    def test_youtube_search_rejects_fan_channel_and_keeps_official(self) -> None:
        day = now_london().date().isoformat()
        tn._YOUTUBE_SEARCH_BUDGET.bind({"date": day, "search_calls": 0}, day)
        search_payload = {
            "items": [
                {"id": {"channelId": "UCfan"}},
                {"id": {"channelId": "UCofficial"}},
            ]
        }
        channel_payload = {
            "items": [
                {
                    "id": "UCfan",
                    "snippet": {"title": "Test Artist", "description": "Unofficial fan page"},
                    "statistics": {"subscriberCount": "9000000", "viewCount": "1"},
                },
                {
                    "id": "UCofficial",
                    "snippet": {"title": "Test Artist Official", "description": "Official artist channel"},
                    "statistics": {"subscriberCount": "1500000", "viewCount": "500000000"},
                    "topicDetails": {"topicCategories": ["https://en.wikipedia.org/wiki/Music"]},
                },
            ]
        }
        with mock.patch.dict(os.environ, {"YOUTUBE_API_KEY": "test-key"}), mock.patch.object(
            tn, "_youtube_json", side_effect=[search_payload, channel_payload]
        ):
            result = tn._lookup_youtube("Test Artist")
        self.assertEqual(result["youtube_channel_id"], "UCofficial")
        self.assertEqual(result["youtube_identity_source"], "youtube_search")
        self.assertEqual(tn._YOUTUBE_SEARCH_BUDGET.snapshot()["search_calls"], 1)

    def test_youtube_daily_search_limit_defers_without_network(self) -> None:
        day = now_london().date().isoformat()
        tn._YOUTUBE_SEARCH_BUDGET.bind({"date": day, "search_calls": 1}, day)
        with mock.patch.dict(
            os.environ,
            {"YOUTUBE_API_KEY": "test-key", "YOUTUBE_SEARCH_DAILY_LIMIT": "1"},
        ), mock.patch.object(tn, "_youtube_json") as youtube_json:
            result = tn._lookup_youtube("Deferred Artist")
        self.assertEqual(result["_provider_status"], "quota_deferred")
        youtube_json.assert_not_called()


class PrefetchTest(unittest.TestCase):
    def setUp(self) -> None:
        mock.patch.object(tn._THROTTLE, "wait", lambda host: None).start()
        self.addCleanup(mock.patch.stopall)
        mock.patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "1"}).start()
        # Each candidate resolves to one deterministic artist name.
        mock.patch.object(tn, "ticket_headliner_candidates", side_effect=lambda c: [c["title"]]).start()
        for name in ("_lookup_wikidata", "_lookup_lastfm", "_lookup_musicbrainz"):
            mock.patch.object(tn, name, lambda _a: {}).start()
        mock.patch.object(tn, "_lookup_youtube", lambda _a, _known_id="": {}).start()

    def _cache_path(self) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        return Path(temp_dir.name) / "ticket_notability_cache.json"

    def test_prefetch_looks_up_new_and_skips_fresh(self) -> None:
        path = self._cache_path()
        tn._CACHE_MEM.clear()
        candidates = [
            _ticket("Artist A"),
            _ticket("Artist B"),
            {"primary_block": "last_24h", "title": "Not a ticket"},
        ]
        report = tn.prefetch_notability(candidates, path, budget_seconds=30, max_workers=4)
        self.assertTrue(report["enabled"])
        self.assertEqual(report["queued"], 2)  # the non-ticket candidate is ignored
        self.assertEqual(report["looked_up"], 2)

        # Second run: both now fresh in cache → skipped, nothing looked up.
        tn._CACHE_MEM.clear()
        report2 = tn.prefetch_notability(candidates, path, budget_seconds=30, max_workers=4)
        self.assertEqual(report2["looked_up"], 0)
        self.assertEqual(report2["skipped_fresh"], 2)

    def test_cache_v1_a_tier_is_forced_through_new_provider_contract(self) -> None:
        path = self._cache_path()
        tn.write_json(path, {
            "version": 1,
            "artists": {
                "legacy artist": {
                    "artist": "Legacy Artist",
                    "tier": "A",
                    "checked_at": now_london().isoformat(),
                    "recheck_days": 30,
                    "signals": {"lastfm_listeners": 2_000_000},
                }
            },
        })
        tn._CACHE_MEM.clear()

        report = tn.prefetch_notability([], path, budget_seconds=30, max_workers=1)

        self.assertEqual(report["queued"], 1)
        self.assertEqual(report["looked_up"], 1)
        self.assertEqual(tn.read_json(path, {})["version"], 3)

    def test_persisted_youtube_search_budget_survives_cache_reload(self) -> None:
        path = self._cache_path()
        day = now_london().date().isoformat()
        tn.write_json(path, {
            "version": 3,
            "artists": {},
            "youtube_search_quota": {"date": day, "search_calls": 94, "daily_limit": 95},
        })
        tn._CACHE_MEM.clear()
        tn._load_cache(path)
        with mock.patch.dict(os.environ, {"YOUTUBE_SEARCH_DAILY_LIMIT": "95"}):
            self.assertTrue(tn._YOUTUBE_SEARCH_BUDGET.acquire())
            self.assertFalse(tn._YOUTUBE_SEARCH_BUDGET.acquire())
        self.assertEqual(tn._YOUTUBE_SEARCH_BUDGET.snapshot()["search_calls"], 95)

    def test_prefetch_budget_defers_without_dropping(self) -> None:
        path = self._cache_path()
        tn._CACHE_MEM.clear()
        candidates = [_ticket(f"Artist {i}") for i in range(5)]
        # Make the wall-clock budget appear already exceeded: the first
        # monotonic() call sets the deadline, every later call is far past it.
        state = {"first": True}

        def fake_monotonic() -> float:
            if state["first"]:
                state["first"] = False
                return 0.0
            return 1000.0

        with mock.patch.object(tn.time, "monotonic", fake_monotonic):
            report = tn.prefetch_notability(candidates, path, budget_seconds=10, max_workers=2)
        # Nothing looked up this run, but all stay queued (deferred) — no
        # coverage is dropped, they just wait for the next run.
        self.assertEqual(report["looked_up"], 0)
        self.assertEqual(report["deferred_budget"], 5)
        self.assertEqual(report["queued"], 5)

    def test_prefetch_noop_when_lookup_disabled(self) -> None:
        with mock.patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "0"}):
            report = tn.prefetch_notability([_ticket("X")], self._cache_path())
        self.assertFalse(report["enabled"])
        self.assertEqual(report["looked_up"], 0)


if __name__ == "__main__":
    unittest.main()
