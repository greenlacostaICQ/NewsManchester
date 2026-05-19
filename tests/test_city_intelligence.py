from __future__ import annotations

import unittest

from news_digest.pipeline.city_intelligence import (
    annotate_city_intelligence,
    build_borough_coverage,
    build_topic_clusters,
    candidate_boroughs,
    candidate_topic_tags,
)


def _candidate(
    fingerprint: str,
    title: str,
    *,
    summary: str = "",
    source_label: str = "MEN",
    primary_block: str = "city_watch",
    include: bool = True,
) -> dict:
    return {
        "fingerprint": fingerprint,
        "title": title,
        "summary": summary,
        "source_label": source_label,
        "primary_block": primary_block,
        "category": "media_layer",
        "include": include,
    }


class TopicClusteringTest(unittest.TestCase):
    def test_groups_related_housing_candidates_into_topic(self) -> None:
        candidates = [
            _candidate(
                "fp-a",
                "Manchester Council approves 120 affordable homes in Ancoats",
                summary="The housing development moves forward after a planning decision.",
                source_label="Manchester Council",
            ),
            _candidate(
                "fp-b",
                "Ancoats affordable housing scheme moves forward",
                summary="Manchester City Council says the homes are part of a regeneration plan.",
                source_label="The Mill",
            ),
            _candidate(
                "fp-c",
                "Police appeal after robbery in Stockport",
                summary="Officers ask witnesses to contact Greater Manchester Police.",
            ),
        ]

        result = build_topic_clusters(candidates)

        self.assertEqual(result["cluster_count"], 1)
        [cluster] = result["clusters"]
        self.assertEqual(cluster["primary_topic"], "housing")
        self.assertEqual(set(cluster["fingerprints"]), {"fp-a", "fp-b"})
        self.assertIn("Manchester", cluster["boroughs"])

    def test_does_not_merge_unrelated_event_listings_at_same_venue(self) -> None:
        candidates = [
            _candidate(
                "gig-a",
                "Indie night tickets released at Co-op Live",
                summary="The concert goes on sale this week.",
                source_label="Co-op Live",
                primary_block="ticket_radar",
            ),
            _candidate(
                "gig-b",
                "Comedy show adds extra date at Co-op Live",
                summary="The event listing confirms a new show.",
                source_label="Co-op Live",
                primary_block="ticket_radar",
            ),
        ]

        result = build_topic_clusters(candidates)

        self.assertEqual(result["cluster_count"], 0)

    def test_annotation_writes_tags_boroughs_and_cluster_ids(self) -> None:
        candidates = [
            _candidate(
                "fp-a",
                "Manchester Council approves affordable homes in Ancoats",
                summary="A housing and planning update.",
                source_label="Manchester Council",
            ),
            _candidate(
                "fp-b",
                "Ancoats affordable housing plan gets council backing",
                summary="The homes are part of a Manchester regeneration scheme.",
            ),
        ]

        summary = annotate_city_intelligence(candidates)

        self.assertEqual(summary["topic_clusters"]["cluster_count"], 1)
        self.assertEqual(candidates[0]["topic_cluster_id"], "topic-001")
        self.assertEqual(candidates[1]["topic_cluster_id"], "topic-001")
        self.assertIn("housing", candidates[0]["topic_tags"])
        self.assertEqual(candidates[0]["boroughs"], ["Manchester"])


class BoroughCoverageTest(unittest.TestCase):
    def test_infers_boroughs_from_council_district_station_and_venue(self) -> None:
        self.assertEqual(
            candidate_boroughs(
                _candidate(
                    "fp-council",
                    "Trafford Council confirms plan",
                    source_label="Trafford Council",
                )
            ),
            ["Trafford"],
        )
        self.assertEqual(
            candidate_boroughs(_candidate("fp-district", "New opening in Prestwich")),
            ["Bury"],
        )
        self.assertEqual(
            candidate_boroughs(_candidate("fp-station", "Disruption near Stockport station")),
            ["Stockport"],
        )
        self.assertEqual(
            candidate_boroughs(_candidate("fp-venue", "The Lowry announces exhibition")),
            ["Salford"],
        )

    def test_flags_rendered_borough_skew(self) -> None:
        candidates = [
            _candidate("m1", "Manchester Council housing update", source_label="Manchester Council"),
            _candidate("m2", "Ancoats planning decision", summary="Manchester housing scheme."),
            _candidate("m3", "Didsbury school plan approved", summary="Manchester Council decision."),
            _candidate("m4", "Wythenshawe health hub opens", summary="NHS and Manchester Council update."),
            _candidate("m5", "City Centre roadworks begin", summary="Manchester transport update."),
            _candidate("s1", "Stockport Council confirms park plan", source_label="Stockport Council"),
        ]

        coverage = build_borough_coverage(
            candidates,
            rendered_fingerprints={"m1", "m2", "m3", "m4", "m5", "s1"},
        )

        self.assertEqual(coverage["counts"]["covered_boroughs_rendered"], 2)
        self.assertEqual(coverage["dominant_borough"]["borough"], "Manchester")
        self.assertTrue(any("Manchester" in flag for flag in coverage["skew_flags"]))

    def test_topic_tags_have_deterministic_fallback(self) -> None:
        tags = candidate_topic_tags(
            _candidate(
                "fp-transport",
                "Morning service update",
                primary_block="transport",
            )
        )

        self.assertEqual(tags, ["transport"])


if __name__ == "__main__":
    unittest.main()
