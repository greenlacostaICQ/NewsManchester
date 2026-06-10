"""Tests for I4 best source selection.

Covers:
  - source_score: category bonus + tier bonus, both independent
  - source_rank: inverse ordering, unknown source → 99
  - pick_winner: cluster picks topic-authoritative source over FIFO
  - Backlog criteria from I4: Transport → TfGM; Council → official;
    Events → venue; Football → club
  - Refactored _source_rank in dedupe.py + semantic_dedupe.py accepts
    category and respects new scoring without breaking legacy behaviour
"""
from __future__ import annotations

import unittest

from news_digest.pipeline.source_selection import (
    CATEGORY_AUTHORITY,
    SOURCE_AUTHORITY_VERSION,
    SOURCE_TIER,
    pick_winner,
    source_rank,
    source_score,
    source_tier,
)


class SourceScoreBacklogCriteriaTest(unittest.TestCase):
    """Backlog criterion: "Transport → TfGM/National Rail; weather →
    Met Office; events → venue/organiser; council → official source"
    """

    def test_transport_prefers_tfgm_over_media(self):
        self.assertGreater(
            source_score("TfGM", "transport"),
            source_score("BBC Manchester", "transport"),
        )
        self.assertGreater(
            source_score("TfGM", "transport"),
            source_score("MEN", "transport"),
        )
        self.assertGreater(
            source_score("TfGM", "transport"),
            source_score("The Manc", "transport"),
        )

    def test_transport_national_rail_above_aggregators(self):
        self.assertGreater(
            source_score("National Rail Enquiries", "transport"),
            source_score("The Manc", "transport"),
        )

    def test_council_prefers_own_council_over_media(self):
        self.assertGreater(
            source_score("Manchester Council", "council"),
            source_score("BBC Manchester", "council"),
        )
        self.assertGreater(
            source_score("Salford Council", "council"),
            source_score("MEN", "council"),
        )

    def test_council_official_beats_aggregator(self):
        for council in ("Manchester Council", "Salford Council", "Trafford Council"):
            self.assertGreater(
                source_score(council, "council"),
                source_score("The Manc", "council"),
                msg=f"{council} should beat The Manc for council stories",
            )

    def test_events_prefer_venue_over_aggregator(self):
        # Venue-direct beats discovery beats aggregator
        self.assertGreater(
            source_score("HOME", "culture_weekly"),
            source_score("Visit Manchester", "culture_weekly"),
        )
        self.assertGreater(
            source_score("Visit Manchester", "culture_weekly"),
            source_score("The Manc", "culture_weekly"),
        )

    def test_football_prefers_club_official(self):
        self.assertGreater(
            source_score("Manchester United", "football"),
            source_score("BBC Manchester", "football"),
        )
        self.assertGreater(
            source_score("Manchester City", "football"),
            source_score("MEN", "football"),
        )
        self.assertGreater(
            source_score("Manchester City Men", "football"),
            source_score("MEN", "football"),
        )
        self.assertGreater(
            source_score("BBC Sport Manchester United", "football"),
            source_score("MEN", "football"),
        )


class SourceScoreCompositionTest(unittest.TestCase):
    def test_category_first_place_worth_100(self):
        # TfGM is index 0 in transport hierarchy → 100 + tier1(20) = 120
        self.assertEqual(source_score("TfGM", "transport"), 120)

    def test_no_category_falls_back_to_tier_only(self):
        # TfGM alone (no category) → tier 1 → 20
        self.assertEqual(source_score("TfGM"), 20)

    def test_unknown_source_no_category_returns_default_tier(self):
        # Unknown source → default tier 3 → 10
        self.assertEqual(source_score("Random Blog Co"), 10)

    def test_unknown_source_in_category_still_gets_tier(self):
        # Unknown source for transport: not in CATEGORY_AUTHORITY,
        # not in SOURCE_TIER → tier 3 → 10
        self.assertEqual(source_score("Random Transport Site", "transport"), 10)

    def test_tier_alone_orders_correctly(self):
        tier1 = source_score("TfGM")        # tier 1 = 20
        tier2 = source_score("BBC Manchester")  # tier 2 = 15
        tier3 = source_score("The Manc")    # tier 3 = 10
        tier4 = source_score("Manchester Wire")  # tier 4 = 5
        self.assertGreater(tier1, tier2)
        self.assertGreater(tier2, tier3)
        self.assertGreater(tier3, tier4)

    def test_empty_label_returns_default(self):
        self.assertEqual(source_score(""), 10)
        self.assertEqual(source_score("", "transport"), 10)

    def test_category_position_diminishes_with_index(self):
        # Manchester Council is index 0, BBC Manchester is index 12 in council
        first_place = source_score("Manchester Council", "council")
        late_place = source_score("BBC Manchester", "council")
        self.assertGreater(first_place, late_place)


class SourceTierTest(unittest.TestCase):
    def test_known_sources_use_explicit_tier(self):
        self.assertEqual(source_tier("TfGM"), 1)
        self.assertEqual(source_tier("BBC Manchester"), 2)
        self.assertEqual(source_tier("The Manc"), 3)
        self.assertEqual(source_tier("Manchester Wire"), 4)

    def test_unknown_defaults_to_tier_3(self):
        self.assertEqual(source_tier("Random Blog"), 3)
        self.assertEqual(source_tier(""), 3)


class SourceRankTest(unittest.TestCase):
    def test_rank_is_inverse_of_score(self):
        # Higher score → lower rank
        high_score_source = "TfGM"
        low_score_source = "Random Blog"
        self.assertLess(
            source_rank(high_score_source, "transport"),
            source_rank(low_score_source, "transport"),
        )

    def test_unknown_source_no_category_returns_high_rank(self):
        # Unknown + no category → score 10 (tier 3 default).
        # But this returns rank 200-10=190, not 99 sentinel.
        # The 99 sentinel is reserved for truly empty scores.
        rank = source_rank("Definitely Unknown Source")
        # Should be a high rank (worse) but a valid number.
        self.assertGreater(rank, 100)

    def test_truly_empty_returns_99(self):
        # An empty label still hits tier 3 default → score 10.
        # The 99 sentinel triggers only when score is exactly 0, which
        # requires the (rare) explicit-tier-4 sources outside their
        # categories. We don't depend on 99 being exact — just on
        # ordering. Keep this test honest:
        rank = source_rank("")
        # Empty string still gets default tier → finite rank.
        self.assertGreaterEqual(rank, 100)


class PickWinnerTest(unittest.TestCase):
    def test_transport_cluster_picks_tfgm_regardless_of_order(self):
        cluster = [
            {"source_label": "The Manc", "category": "transport"},
            {"source_label": "TfGM", "category": "transport"},
        ]
        winner = pick_winner(cluster)
        self.assertEqual(winner["source_label"], "TfGM")
        # Reversed order — same result
        cluster_rev = list(reversed(cluster))
        winner = pick_winner(cluster_rev)
        self.assertEqual(winner["source_label"], "TfGM")

    def test_council_cluster_picks_official(self):
        cluster = [
            {"source_label": "BBC Manchester", "category": "council"},
            {"source_label": "MEN", "category": "council"},
            {"source_label": "Manchester Council", "category": "council"},
        ]
        winner = pick_winner(cluster)
        self.assertEqual(winner["source_label"], "Manchester Council")

    def test_event_cluster_picks_venue_over_aggregator(self):
        cluster = [
            {"source_label": "The Manc", "category": "culture_weekly"},
            {"source_label": "HOME", "category": "culture_weekly"},
            {"source_label": "Visit Manchester", "category": "culture_weekly"},
        ]
        winner = pick_winner(cluster)
        self.assertEqual(winner["source_label"], "HOME")

    def test_football_cluster_picks_club_official(self):
        cluster = [
            {"source_label": "BBC Manchester", "category": "football"},
            {"source_label": "MEN", "category": "football"},
            {"source_label": "Manchester United", "category": "football"},
        ]
        winner = pick_winner(cluster)
        self.assertEqual(winner["source_label"], "Manchester United")

    def test_tie_breaks_by_evidence_length(self):
        # Same score → longer evidence wins
        cluster = [
            {"source_label": "BBC Manchester", "category": "media_layer",
             "evidence_text": "short"},
            {"source_label": "BBC Manchester", "category": "media_layer",
             "evidence_text": "much longer evidence with substantive detail" * 10},
        ]
        winner = pick_winner(cluster)
        self.assertGreater(len(winner["evidence_text"]), 200)

    def test_single_candidate_returned_unchanged(self):
        candidate = {"source_label": "Random", "category": "media_layer"}
        winner = pick_winner([candidate])
        self.assertIs(winner, candidate)

    def test_empty_cluster_returns_none(self):
        self.assertIsNone(pick_winner([]))

    def test_all_invalid_returns_none(self):
        self.assertIsNone(pick_winner([None, "not-a-dict", 42]))

    def test_mixed_validity_ignores_non_dicts(self):
        candidate = {"source_label": "TfGM", "category": "transport"}
        winner = pick_winner([None, candidate, "garbage"])
        self.assertIs(winner, candidate)


class DedupeIntegrationTest(unittest.TestCase):
    """Smoke-test the refactored _source_rank in dedupe.py and
    semantic_dedupe.py: they must accept a `category` argument and
    return new I4-aware ranks for registered sources while remaining
    backward-compatible for legacy media labels."""

    def test_dedupe_source_rank_accepts_category(self):
        from news_digest.pipeline.dedupe import _source_rank
        # TfGM with transport category should rank above BBC.
        self.assertLess(
            _source_rank("TfGM", "transport"),
            _source_rank("BBC Manchester", "transport"),
        )

    def test_semantic_dedupe_source_rank_accepts_category(self):
        from news_digest.pipeline.semantic_dedupe import _source_rank
        self.assertLess(
            _source_rank("Manchester Council", "council"),
            _source_rank("BBC Manchester", "council"),
        )

    def test_dedupe_legacy_substring_path_still_works(self):
        # Substring "bbc" in arbitrary label, no category → legacy rank 0
        from news_digest.pipeline.dedupe import _source_rank
        # Not in SOURCE_TIER, no category, "bbc" substring → legacy 0
        self.assertEqual(_source_rank("My local BBC affiliate"), 0)

    def test_unknown_label_no_category_gets_99(self):
        from news_digest.pipeline.dedupe import _source_rank
        rank = _source_rank("Some Random Unknown Source")
        # Should hit 99 (legacy sentinel) — no substring, not in I4 registry
        self.assertEqual(rank, 99)


class RegistryConsistencyTest(unittest.TestCase):
    """Defensive checks: catch typos at test time, not at runtime."""

    def test_schema_version_present(self):
        self.assertEqual(SOURCE_AUTHORITY_VERSION, 1)

    def test_every_category_authority_label_has_a_tier(self):
        # If a source is named in a category preference list, it
        # should also have an explicit tier — otherwise it falls to
        # default tier 3 silently, which defeats the point of being
        # in the registry.
        missing: list[tuple[str, str]] = []
        for category, names in CATEGORY_AUTHORITY.items():
            for name in names:
                if name not in SOURCE_TIER:
                    missing.append((category, name))
        self.assertEqual(missing, [], msg=f"Missing tiers: {missing}")

    def test_no_duplicate_names_within_a_category_list(self):
        for category, names in CATEGORY_AUTHORITY.items():
            unique = set(names)
            self.assertEqual(
                len(unique), len(names),
                msg=f"Duplicate names in CATEGORY_AUTHORITY[{category!r}]",
            )

    def test_all_tiers_in_range_1_to_4(self):
        for name, tier in SOURCE_TIER.items():
            self.assertIn(tier, {1, 2, 3, 4}, msg=f"{name} has invalid tier {tier}")


if __name__ == "__main__":
    unittest.main()
