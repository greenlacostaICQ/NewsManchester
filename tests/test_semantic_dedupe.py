"""I1 — Semantic deduplication via embeddings.

Tests exercise the full ``run_semantic_pass`` with a fake
``EmbeddingClient`` so nothing touches the network. The fake returns
deterministic vectors keyed by topic so we can construct exactly the
similarities we want to assert against (drop, preserve as follow_up,
push to borderline pool).
"""
from __future__ import annotations

import math
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path

from news_digest.pipeline import semantic_dedupe as sd


# --------------------------------------------------------------------------
# Fake embedding client
# --------------------------------------------------------------------------
@dataclass
class FakeClient:
    """Returns vectors based on topic tags injected via text prefix.

    A text starting with ``"TOPIC:<id> "`` gets the canonical unit
    vector for that topic (with a tiny perturbation per call so two
    identical topics still produce cosine ~1.0). Anything else gets
    an arbitrary unique vector so unrelated items don't collide.
    """

    api_key: str = "test-key"
    model: str = sd._EMBED_MODEL
    embed_calls: list[list[str]] = field(default_factory=list)

    def embed(self, texts: list[str]) -> list[list[float] | None]:
        self.embed_calls.append(list(texts))
        out: list[list[float] | None] = []
        for i, text in enumerate(texts):
            topic = "noise-" + str(i + len(self.embed_calls) * 1000)
            if text.startswith("TOPIC:"):
                topic = text.split(" ", 1)[0][len("TOPIC:"):]
            # Canonical unit vector for the topic. Deterministic per topic.
            seed = sum(ord(c) for c in topic)
            vec = [math.sin((seed + j) * 0.13) for j in range(sd._EMBED_DIM)]
            n = math.sqrt(sum(x * x for x in vec))
            out.append([x / n for x in vec] if n > 0 else None)
        return out


def _candidate(
    fp: str,
    title: str,
    *,
    topic: str | None = None,
    primary_block: str = "city_watch",
    source_label: str = "MEN",
    include: bool = True,
    lead: str = "",
    evidence: str = "",
) -> dict:
    # The fake client looks at the FIRST prefix in the embedded blob,
    # which our _embed_text helper puts at the start (title).
    embed_title = f"TOPIC:{topic} {title}" if topic else title
    return {
        "fingerprint": fp,
        "title": embed_title,
        "lead": lead,
        "summary": "",
        "evidence_text": evidence,
        "primary_block": primary_block,
        "category": "media_layer",
        "source_label": source_label,
        "include": include,
    }


def _fact(fp: str, title: str, *, topic: str | None, primary_block: str = "city_watch") -> dict:
    return {
        "fingerprint": fp,
        "title": f"TOPIC:{topic} {title}" if topic else title,
        "normalized_title": title.lower(),
        "primary_block": primary_block,
        "first_published_day_london": "2026-05-17",
    }


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
class CosineTest(unittest.TestCase):
    def test_identical_vectors_score_one(self) -> None:
        v = [1.0, 2.0, -3.0]
        self.assertAlmostEqual(sd._cosine(v, v), 1.0, places=6)

    def test_orthogonal_vectors_score_zero(self) -> None:
        self.assertAlmostEqual(sd._cosine([1.0, 0.0], [0.0, 1.0]), 0.0, places=6)

    def test_missing_vector_returns_none(self) -> None:
        self.assertIsNone(sd._cosine(None, [1.0, 2.0]))
        self.assertIsNone(sd._cosine([0.0, 0.0], [1.0, 2.0]))

    def test_dot_unit_matches_cosine(self) -> None:
        # The hot loops pre-normalise vectors once and use _dot_unit instead
        # of _cosine for speed. Lock that the two stay numerically identical,
        # so the optimisation never silently changes a dedup decision.
        import random
        random.seed(0)
        for _ in range(200):
            a = [random.gauss(0, 1) for _ in range(64)]
            b = [random.gauss(0, 1) for _ in range(64)]
            self.assertAlmostEqual(
                sd._dot_unit(sd._normalise(a), sd._normalise(b)),
                sd._cosine(a, b),
                places=9,
            )
        # None / zero vectors degrade to None just like _cosine.
        self.assertIsNone(sd._dot_unit(sd._normalise([0.0, 0.0]), sd._normalise([1.0, 2.0])))


# --------------------------------------------------------------------------
# Cache behaviour
# --------------------------------------------------------------------------
class CacheBehaviourTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_cache_miss_then_hit_avoids_second_api_call(self) -> None:
        client = FakeClient()
        cand = _candidate("fp1", "Story A", topic="a")
        # First call → 1 API hit
        cache = sd._load_cache(self.state_dir)
        vectors = sd.embed_with_cache(client, [cand], cache)
        sd._save_cache(self.state_dir, cache)
        self.assertEqual(len(client.embed_calls), 1)
        self.assertIsNotNone(vectors["fp1"])

        # Second call with same content → 0 API hits
        cache2 = sd._load_cache(self.state_dir)
        vectors2 = sd.embed_with_cache(client, [cand], cache2)
        self.assertEqual(len(client.embed_calls), 1)  # unchanged
        self.assertEqual(vectors2["fp1"], vectors["fp1"])

    def test_content_change_invalidates_cache_entry(self) -> None:
        client = FakeClient()
        cand_v1 = _candidate("fp1", "Story A", topic="a", evidence="first")
        cache = sd._load_cache(self.state_dir)
        sd.embed_with_cache(client, [cand_v1], cache)
        sd._save_cache(self.state_dir, cache)
        self.assertEqual(len(client.embed_calls), 1)

        cand_v2 = _candidate("fp1", "Story A", topic="a", evidence="second")  # title same fp, text differs
        cache = sd._load_cache(self.state_dir)
        sd.embed_with_cache(client, [cand_v2], cache)
        self.assertEqual(len(client.embed_calls), 2)

    def test_model_change_resets_cache(self) -> None:
        # Pre-populate with a different model
        sd._save_cache(self.state_dir, {"model": "old-model", "entries": {"fp1": {"hash": "x", "vector": [0.1] * sd._EMBED_DIM, "saved_at": "2026-01-01T00:00:00+00:00"}}})
        cache = sd._load_cache(self.state_dir)
        self.assertEqual(cache["entries"], {})


# --------------------------------------------------------------------------
# Disabled path (no API key)
# --------------------------------------------------------------------------
class DisabledClientTest(unittest.TestCase):
    def test_run_semantic_pass_noops_without_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = sd.run_semantic_pass(
                candidates=[_candidate("fp1", "Story A", topic="a")],
                published_facts=[],
                state_dir=Path(tmp),
                client=sd.EmbeddingClient(api_key=""),
            )
        self.assertFalse(result.enabled)
        self.assertEqual(result.embedded, 0)
        self.assertEqual(result.intra_drops, [])


# --------------------------------------------------------------------------
# Intra-batch semantic drop
# --------------------------------------------------------------------------
class IntraBatchDropTest(unittest.TestCase):
    def test_two_paraphrased_items_drop_weaker_source(self) -> None:
        c_bbc = _candidate("fp-bbc", "Murder arrest in Manchester city centre", topic="murder-mcr", source_label="BBC Manchester")
        c_men = _candidate("fp-men", "Man, 28, charged after Piccadilly attack", topic="murder-mcr", source_label="MEN")
        with tempfile.TemporaryDirectory() as tmp:
            result = sd.run_semantic_pass(
                candidates=[c_bbc, c_men],
                published_facts=[],
                state_dir=Path(tmp),
                client=FakeClient(),
            )
        self.assertEqual(len(result.intra_drops), 1)
        drop = result.intra_drops[0]
        # BBC ranks higher than MEN → MEN should be dropped.
        self.assertEqual(drop["fingerprint"], "fp-men")
        self.assertEqual(drop["kept_fingerprint"], "fp-bbc")
        self.assertGreaterEqual(drop["sim"], sd._HIGH_SIM_THRESHOLD)
        self.assertFalse(c_men["include"])
        self.assertTrue(c_bbc["include"])

    def test_different_topics_are_not_intra_dropped(self) -> None:
        c1 = _candidate("fp1", "Council confirms budget", topic="budget")
        c2 = _candidate("fp2", "Trams disrupted on Bury line", topic="trams")
        with tempfile.TemporaryDirectory() as tmp:
            result = sd.run_semantic_pass(
                candidates=[c1, c2],
                published_facts=[],
                state_dir=Path(tmp),
                client=FakeClient(),
            )
        self.assertEqual(result.intra_drops, [])
        self.assertTrue(c1["include"] and c2["include"])

    def test_transport_block_skipped_in_semantic_pass(self) -> None:
        # Two transport items with the SAME topic must NOT trigger
        # semantic dedup — the boilerplate matrix would create false
        # positives. Deterministic transport dedup owns this lane.
        c1 = _candidate("fp-tfgm-a", "Bus 86 diverted", topic="bus-86", primary_block="transport", source_label="TfGM")
        c2 = _candidate("fp-tfgm-b", "Service change 86", topic="bus-86", primary_block="transport", source_label="TfGM")
        with tempfile.TemporaryDirectory() as tmp:
            result = sd.run_semantic_pass(
                candidates=[c1, c2],
                published_facts=[],
                state_dir=Path(tmp),
                client=FakeClient(),
            )
        self.assertEqual(result.intra_drops, [])


# --------------------------------------------------------------------------
# Cross-day semantic dedup
# --------------------------------------------------------------------------
class CrossDayDropTest(unittest.TestCase):
    def test_cross_day_rehash_dropped_without_follow_up_marker(self) -> None:
        cand = _candidate("fp-new", "Council confirms scheme details", topic="scheme-2026")
        fact = _fact("fp-old", "Council reveals new scheme", topic="scheme-2026")
        with tempfile.TemporaryDirectory() as tmp:
            result = sd.run_semantic_pass(
                candidates=[cand],
                published_facts=[fact],
                state_dir=Path(tmp),
                client=FakeClient(),
            )
        self.assertEqual(len(result.cross_day_drops), 1)
        self.assertFalse(cand["include"])
        self.assertEqual(cand["change_type"], "same_story_rehash")
        self.assertEqual(cand["semantic_match_fingerprint"], "fp-old")

    def test_cross_day_with_follow_up_marker_kept(self) -> None:
        cand = _candidate(
            "fp-new",
            "Sentenced after Manchester murder verdict",
            topic="murder-2026",
            evidence="Man sentenced today after jury reached guilty verdict.",
        )
        fact = _fact("fp-old", "Murder arrest in Manchester", topic="murder-2026")
        with tempfile.TemporaryDirectory() as tmp:
            result = sd.run_semantic_pass(
                candidates=[cand],
                published_facts=[fact],
                state_dir=Path(tmp),
                client=FakeClient(),
            )
        self.assertEqual(len(result.cross_day_drops), 1)
        drop = result.cross_day_drops[0]
        self.assertTrue(drop["kept"])
        self.assertTrue(cand["include"])
        self.assertEqual(cand["change_type"], "follow_up")

    def test_borderline_pair_added_for_llm_review(self) -> None:
        # Fake similarity ~0.81 (close-but-not-identical) — craft a
        # client that hands out v1 then v2 across calls, so candidate
        # gets v1 and published fact gets v2.
        @dataclass
        class BorderlineClient:
            api_key: str = "k"
            model: str = sd._EMBED_MODEL
            _counter: list[int] = field(default_factory=lambda: [0])

            def embed(self, texts):
                v1 = [1.0] + [0.0] * (sd._EMBED_DIM - 1)
                v2 = [0.81] + [math.sqrt(1 - 0.81 ** 2)] + [0.0] * (sd._EMBED_DIM - 2)
                pool = [v1, v2]
                out = []
                for _ in texts:
                    out.append(pool[self._counter[0] % 2])
                    self._counter[0] += 1
                return out

        cand = _candidate("fp-new", "Story title", primary_block="city_watch")
        fact = _fact("fp-old", "Different angle on similar story", topic=None)
        with tempfile.TemporaryDirectory() as tmp:
            result = sd.run_semantic_pass(
                candidates=[cand],
                published_facts=[fact],
                state_dir=Path(tmp),
                client=BorderlineClient(),
            )
        # Should not be dropped — sim is below high threshold.
        self.assertTrue(cand["include"])
        self.assertEqual(len(result.borderline_pairs), 1)
        bp = result.borderline_pairs[0]
        self.assertEqual(bp["kind"], "cross_day")
        self.assertGreaterEqual(bp["sim"], sd._BORDERLINE_SIM_THRESHOLD)
        self.assertLess(bp["sim"], sd._HIGH_SIM_THRESHOLD)


# --------------------------------------------------------------------------
# Block-group filter
# --------------------------------------------------------------------------
class ComparableBucketTest(unittest.TestCase):
    def test_same_block_group_is_comparable(self) -> None:
        a = {"primary_block": "city_watch"}
        b = {"primary_block": "last_24h"}  # same group as city_watch
        self.assertTrue(sd._comparable(a, b))

    def test_different_block_groups_not_comparable(self) -> None:
        a = {"primary_block": "city_watch"}
        b = {"primary_block": "weekend_activities"}
        self.assertFalse(sd._comparable(a, b))


# --------------------------------------------------------------------------
# Follow-up marker detection
# --------------------------------------------------------------------------
class FollowUpMarkerTest(unittest.TestCase):
    def test_russian_marker_detected(self) -> None:
        self.assertTrue(sd._has_follow_up_marker({"title": "Вынесен приговор по делу о краже"}))

    def test_english_marker_detected(self) -> None:
        self.assertTrue(sd._has_follow_up_marker({"evidence_text": "Sentenced to 5 years."}))

    def test_no_marker(self) -> None:
        self.assertFalse(sd._has_follow_up_marker({"title": "Council to discuss proposal next month"}))


if __name__ == "__main__":
    unittest.main()
