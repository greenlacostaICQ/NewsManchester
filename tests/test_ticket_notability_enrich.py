"""Notability enrichment: parallel prefetch, short-circuit, error taxonomy,
read-only writer path. These pin the 2026-06-11 fix where ~100 serial artist
lookups pushed the writer stage to ~6 minutes.

All lookups are mocked — nothing touches the network. The throttle is patched
to a no-op so tests don't sleep.
"""
from __future__ import annotations

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
        calls = {"wd": 0, "sp": 0, "lf": 0, "mb": 0}

        def wd(_a):
            calls["wd"] += 1
            return {"sitelinks": 50, "wikidata_id": "Q123", "description": "band"}

        def sp(_a):
            calls["sp"] += 1
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
            _lookup_spotify=sp,
            _lookup_lastfm=lf,
            _lookup_musicbrainz=mb,
        ):
            result = tn._artist_notability("Coldplay", "artist", _ticket("Coldplay"), {}, now_london(), allow_network=True)

        self.assertEqual(result.tier, "A")
        self.assertEqual(calls["wd"], 1)
        # The whole point: a clearly-notable Wikidata hit means we never spend
        # Spotify/Last.fm/MusicBrainz (the rate-limited one).
        self.assertEqual((calls["sp"], calls["lf"], calls["mb"]), (0, 0, 0))

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
            _lookup_spotify=thin,
            _lookup_lastfm=thin,
            _lookup_musicbrainz=mb,
        ):
            tn._artist_notability("Obscure Act", "artist", _ticket("Obscure Act"), {}, now_london(), allow_network=True)

        # Wikidata + Spotify + Last.fm all blank → still unknown → MusicBrainz runs.
        self.assertEqual(calls["mb"], 1)

    def test_read_only_does_no_network(self) -> None:
        calls = {"n": 0}

        def boom(_a):
            calls["n"] += 1
            return {}

        with mock.patch.multiple(
            tn, _lookup_wikidata=boom, _lookup_spotify=boom, _lookup_lastfm=boom, _lookup_musicbrainz=boom
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
            return {"sitelinks": 50, "wikidata_id": "Q9"}

        # api_failed → retry next run (1 day).
        cache: dict = {}
        with mock.patch.multiple(
            tn, _lookup_wikidata=raise_, _lookup_spotify=empty, _lookup_lastfm=empty, _lookup_musicbrainz=empty
        ):
            tn._artist_notability("Fails", "artist", _ticket("Fails"), cache, now_london(), allow_network=True)
        self.assertEqual(cache[tn._cache_key("Fails")]["recheck_days"], 1)

        # clean not_found → 7 days.
        cache = {}
        with mock.patch.multiple(
            tn, _lookup_wikidata=empty, _lookup_spotify=empty, _lookup_lastfm=empty, _lookup_musicbrainz=empty
        ):
            tn._artist_notability("Nobody", "artist", _ticket("Nobody"), cache, now_london(), allow_network=True)
        self.assertEqual(cache[tn._cache_key("Nobody")]["recheck_days"], 7)

        # found → 30 days.
        cache = {}
        with mock.patch.multiple(
            tn, _lookup_wikidata=notable, _lookup_spotify=empty, _lookup_lastfm=empty, _lookup_musicbrainz=empty
        ):
            tn._artist_notability("Famous", "artist", _ticket("Famous"), cache, now_london(), allow_network=True)
        self.assertEqual(cache[tn._cache_key("Famous")]["recheck_days"], 30)


class PrefetchTest(unittest.TestCase):
    def setUp(self) -> None:
        mock.patch.object(tn._THROTTLE, "wait", lambda host: None).start()
        self.addCleanup(mock.patch.stopall)
        mock.patch.dict(os.environ, {"NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP": "1"}).start()
        # Each candidate resolves to one deterministic artist name.
        mock.patch.object(tn, "ticket_headliner_candidates", side_effect=lambda c: [c["title"]]).start()
        for name in ("_lookup_wikidata", "_lookup_spotify", "_lookup_lastfm", "_lookup_musicbrainz"):
            mock.patch.object(tn, name, lambda _a: {}).start()

    def _cache_path(self) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        return Path(temp_dir.name) / "ticket_notability_cache.json"

    def test_prefetch_looks_up_new_and_skips_fresh(self) -> None:
        path = self._cache_path()
        tn._CACHE_MEM.clear()
        candidates = [_ticket("Artist A"), _ticket("Artist B"), {"primary_block": "last_24h", "title": "Not a ticket"}]
        report = tn.prefetch_notability(candidates, path, budget_seconds=30, max_workers=4)
        self.assertTrue(report["enabled"])
        self.assertEqual(report["queued"], 2)  # the non-ticket candidate is ignored
        self.assertEqual(report["looked_up"], 2)

        # Second run: both now fresh in cache → skipped, nothing looked up.
        tn._CACHE_MEM.clear()
        report2 = tn.prefetch_notability(candidates, path, budget_seconds=30, max_workers=4)
        self.assertEqual(report2["looked_up"], 0)
        self.assertEqual(report2["skipped_fresh"], 2)

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
