from __future__ import annotations

import unittest

from news_digest.pipeline.inventory import (
    aggregate_category_health,
    classify_category_health,
    verify_conservation,
)
from news_digest.pipeline.llm_rewrite import _candidate_content_hash


class CategoryHealthTest(unittest.TestCase):
    def test_failed_when_nothing_fetched(self) -> None:
        row = {"checked_count": 3, "fetched_count": 0, "found": 0, "enriched": 0, "errors": 0}
        self.assertEqual(classify_category_health(row), "failed")

    def test_empty_suspicious_vs_empty_legit(self) -> None:
        fetched_clean = {"checked_count": 3, "fetched_count": 3, "found": 0, "enriched": 0, "errors": 0}
        fetched_with_errors = {"checked_count": 3, "fetched_count": 3, "found": 0, "enriched": 0, "errors": 2}
        self.assertEqual(classify_category_health(fetched_clean), "empty_legit")
        self.assertEqual(classify_category_health(fetched_with_errors), "empty_suspicious")

    def test_partial_when_found_but_not_enriched(self) -> None:
        row = {"checked_count": 2, "fetched_count": 2, "found": 10, "enriched": 0, "errors": 0}
        self.assertEqual(classify_category_health(row), "partial")

    def test_ok_when_clean(self) -> None:
        row = {"checked_count": 2, "fetched_count": 2, "found": 10, "enriched": 10, "errors": 0}
        self.assertEqual(classify_category_health(row), "ok")

    def test_aggregate_rolls_up_by_category(self) -> None:
        rows = [
            {"category": "media_layer", "checked": True, "fetched": True, "found": 5, "enriched": 5, "errors": 0},
            {"category": "media_layer", "checked": True, "fetched": False, "found": 0, "enriched": 0, "errors": 1},
        ]
        result = aggregate_category_health(rows)
        self.assertEqual(result["media_layer"]["found"], 5)
        self.assertEqual(result["media_layer"]["source_count"], 2)


class ConservationTest(unittest.TestCase):
    def test_flags_real_net_loss(self) -> None:
        # A category claims 100 candidates were found at collect, but only 60
        # survived into candidates.json — that is a real disappearance
        # between collect and candidates.json, not synthetic-card slack.
        rows = [{"found": 100}]
        result = verify_conservation(rows, candidates_json_count=60)
        self.assertFalse(result["conserved"])
        self.assertEqual(result["delta"], -40)

    def test_small_positive_slack_is_healthy(self) -> None:
        # candidates.json has one MORE than collect found (synthetic
        # weather/transport card) — expected, not a loss.
        rows = [{"found": 100}]
        result = verify_conservation(rows, candidates_json_count=101)
        self.assertTrue(result["conserved"])


class EvidenceCacheStructuredFactsTest(unittest.TestCase):
    def test_changed_hard_news_fact_invalidates_cache_even_with_same_evidence_text(self) -> None:
        # Backlog 8.3: a materially changed fact (here: who was affected) must
        # change the reuse hash even though evidence_text is identical and
        # short (well under the old 3200-char truncation point) — proving the
        # structured-fields extension does real work, not just adds inert
        # fields to the payload.
        base = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Fire on Oxford Road",
            "evidence_text": "A fire broke out on Oxford Road this morning.",
            "who_affected": "two shops",
        }
        updated = dict(base, who_affected="two shops and a nearby flat")
        self.assertNotEqual(_candidate_content_hash(base), _candidate_content_hash(updated))

    def test_identical_candidate_is_stable(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Some Artist at Co-op Live",
            "evidence_text": "Tickets on sale now.",
        }
        self.assertEqual(_candidate_content_hash(dict(candidate)), _candidate_content_hash(dict(candidate)))


if __name__ == "__main__":
    unittest.main()
