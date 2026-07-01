from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.inventory import (
    InventoryLock,
    aggregate_category_health,
    annotate_morning_relevance,
    build_inventory_record,
    categories_needing_live_fallback,
    classify_category_health,
    classify_disposition,
    evaluate_card,
    passes_morning_contract,
    read_inventory,
    reentry_candidates,
    ticket_reaches_morning,
    verify_collect_conservation,
    verify_dispositions,
    write_inventory,
)
from news_digest.pipeline.llm_rewrite import _candidate_content_hash


class CategoryHealthTest(unittest.TestCase):
    def test_failed_when_nothing_fetched(self) -> None:
        self.assertEqual(
            classify_category_health({"checked_count": 3, "fetched_count": 0, "found": 0, "enriched": 0, "errors": 0}),
            "failed",
        )

    def test_empty_suspicious_vs_empty_legit(self) -> None:
        self.assertEqual(
            classify_category_health({"checked_count": 3, "fetched_count": 3, "found": 0, "enriched": 0, "errors": 0}),
            "empty_legit",
        )
        self.assertEqual(
            classify_category_health({"checked_count": 3, "fetched_count": 3, "found": 0, "enriched": 0, "errors": 2}),
            "empty_suspicious",
        )

    def test_fallback_targets_only_broken_categories(self) -> None:
        health = {
            "media_layer": {"verdict": "ok"},
            "venues_tickets": {"verdict": "failed"},
            "food_openings": {"verdict": "empty_suspicious"},
            "football": {"verdict": "partial"},
        }
        self.assertEqual(categories_needing_live_fallback(health), ["food_openings", "venues_tickets"])


class CollectConservationTest(unittest.TestCase):
    def test_flags_real_net_loss(self) -> None:
        result = verify_collect_conservation([{"found": 100}], candidates_json_count=60)
        self.assertFalse(result["conserved"])
        self.assertEqual(result["delta"], -40)

    def test_small_positive_slack_is_healthy(self) -> None:
        self.assertTrue(verify_collect_conservation([{"found": 100}], candidates_json_count=101)["conserved"])


class DispositionTest(unittest.TestCase):
    def test_selected_but_not_rendered_is_flagged_silent_loss(self) -> None:
        # The load-bearing bucket: chosen but absent from the rendered set.
        cand = {"fingerprint": "x1", "publish_plan_status": "show"}
        self.assertEqual(classify_disposition(cand, rendered_fingerprints=set()), "not_render_ready")

    def test_every_captured_item_has_exactly_one_disposition(self) -> None:
        candidates = [
            {"fingerprint": "shown1", "publish_plan_status": "show"},
            {"fingerprint": "res1", "recoverable_reserve": True},
            {"fingerprint": "held1", "ticket_inventory_held": True},
            {"fingerprint": "dup1", "dedupe_decision": "drop"},
            {"fingerprint": "lost1", "digest_selection_verdict": "selected"},
            {"fingerprint": "drop1"},
        ]
        result = verify_dispositions(candidates, rendered_fingerprints={"shown1"})
        self.assertTrue(result["conserved"])
        self.assertEqual(result["accounted"], result["captured"])
        self.assertEqual(result["captured"], 6)
        self.assertEqual(result["totals"]["shown"], 1)
        self.assertEqual(result["silent_loss"], 1)  # lost1
        self.assertEqual(result["violations"], [])


class CardRulesTest(unittest.TestCase):
    def test_event_missing_date_is_not_render_ready(self) -> None:
        cand = {"primary_block": "next_7_days", "event": {"event_name": "X", "venue": "HOME"}, "draft_line": "• text"}
        status, ready, missing = evaluate_card(cand)
        self.assertFalse(ready)
        self.assertEqual(status, "missing_facts")
        self.assertIn("date_start", missing)

    def test_complete_event_with_text_is_ready(self) -> None:
        cand = {
            "primary_block": "next_7_days",
            "event": {"event_name": "X", "venue": "HOME", "date_start": "2026-07-05"},
            "draft_line": "• Концерт X в HOME 5 июля.",
        }
        status, ready, missing = evaluate_card(cand)
        self.assertTrue(ready)
        self.assertEqual((status, missing), ("ready", []))

    def test_fields_present_but_no_text_is_needs_text(self) -> None:
        cand = {
            "primary_block": "next_7_days",
            "event": {"event_name": "X", "venue": "HOME", "date_start": "2026-07-05"},
        }
        status, ready, missing = evaluate_card(cand)
        self.assertEqual((status, ready, missing), ("needs_text", False, ["draft_line"]))


class MorningContractTest(unittest.TestCase):
    def test_stale_ticket_without_reason_is_inventory_only(self) -> None:
        record = {"primary_block": "ticket_radar", "render_ready": True, "fact_card": {"ticket_type": "regular_upcoming", "tier": "C"}}
        ok, reason = passes_morning_contract(record, today="2026-07-01")
        self.assertFalse(ok)
        self.assertEqual(reason, "inventory_only")

    def test_a_tier_ticket_reaches_morning(self) -> None:
        record = {"primary_block": "ticket_radar", "render_ready": True, "fact_card": {"tier": "A", "ticket_type": "regular_upcoming"}}
        self.assertTrue(ticket_reaches_morning(record))
        ok, reason = passes_morning_contract(record, today="2026-07-01")
        self.assertTrue(ok)

    def test_expired_never_passes_as_fresh(self) -> None:
        record = {"primary_block": "last_24h", "render_ready": True, "expires_at": "2026-06-01"}
        ok, reason = passes_morning_contract(record, today="2026-07-01")
        self.assertFalse(ok)
        self.assertEqual(reason, "expired")


class ReentryTest(unittest.TestCase):
    def test_reentry_dedupes_against_published_facts(self) -> None:
        prior = [
            {"fingerprint": "keep", "render_ready": True},
            {"fingerprint": "already_shown", "render_ready": True},
            {"fingerprint": "not_ready", "render_ready": False},
        ]
        published = {"facts": [{"fingerprint": "already_shown"}]}
        result = reentry_candidates(prior, published, today="2026-07-01")
        self.assertEqual([r["fingerprint"] for r in result], ["keep"])


class StateFoundationTest(unittest.TestCase):
    def test_write_read_roundtrip_stamps_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_inventory(state_dir, "events", [{"fingerprint": "e1", "render_ready": True}])
            rows = read_inventory(state_dir, "events")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["schema_version"], 1)
            self.assertEqual(rows[0]["fingerprint"], "e1")

    def test_lock_is_acquirable_and_released(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            with InventoryLock(state_dir, name="t"):
                pass
            # second acquisition after release must succeed
            with InventoryLock(state_dir, name="t"):
                pass


class BuildRecordTest(unittest.TestCase):
    def test_record_has_canonical_schema(self) -> None:
        cand = {
            "fingerprint": "fp1",
            "primary_block": "ticket_radar",
            "source_url": "https://x/t",
            "venue_scope": "gm",
            "ticket_type": "on_sale_now",
            "event": {"event_name": "Artist", "venue": "Co-op Live", "date_start": "2026-08-01"},
            "draft_line": "• Artist в Co-op Live 1 августа.",
        }
        rec = build_inventory_record(cand, prompt_version=1)
        for key in ("fingerprint", "evidence_hash", "prompt_version", "fact_card", "render_ready", "missing_facts", "expires_at"):
            self.assertIn(key, rec)
        self.assertTrue(rec["render_ready"])
        self.assertEqual(rec["fact_card"]["ticket_type"], "on_sale_now")


class NightWaveTest(unittest.TestCase):
    def test_wave_writes_inventory_only_never_candidates(self) -> None:
        import types
        from unittest import mock

        import scripts.run_local_digest as runner
        from news_digest.pipeline.collector import core as collector_core

        fake_sources = [
            types.SimpleNamespace(name="BBC Manchester", report_category="media_layer"),
            types.SimpleNamespace(name="GMP", report_category="gmp"),
            types.SimpleNamespace(name="Ticketmaster", report_category="venues_tickets"),  # not in live_news wave
        ]

        def fake_collect(source):
            health = {"checked": True, "fetched": True, "errors": []}
            cand = {
                "fingerprint": f"{source.name}-1",
                "primary_block": "last_24h",
                "category": source.report_category,
                "what_happened": "something happened",
                "why_now": "today",
                "draft_line": "• Новость.",
            }
            return health, [cand]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data" / "state").mkdir(parents=True)
            with mock.patch.object(runner, "PROJECT_ROOT", root), \
                    mock.patch.object(collector_core, "SOURCES", fake_sources), \
                    mock.patch.object(collector_core, "_collect_single_source", side_effect=fake_collect), \
                    mock.patch("sys.stdout"):
                rc = runner.cmd_collect_inventory("live_news")

            self.assertEqual(rc, 0)
            # candidates.json must NOT be created by a night wave
            self.assertFalse((root / "data" / "state" / "candidates.json").exists())
            # inventory written for the wave's categories only
            self.assertEqual([r["fingerprint"] for r in read_inventory(root / "data" / "state", "media_layer")], ["BBC Manchester-1"])
            self.assertEqual([r["fingerprint"] for r in read_inventory(root / "data" / "state", "gmp")], ["GMP-1"])
            # the ticket source is outside the live_news wave — not collected
            self.assertEqual(read_inventory(root / "data" / "state", "venues_tickets"), [])


class EvidenceCacheStructuredFactsTest(unittest.TestCase):
    def test_changed_hard_news_fact_invalidates_cache(self) -> None:
        base = {
            "category": "media_layer",
            "primary_block": "last_24h",
            "title": "Fire on Oxford Road",
            "evidence_text": "A fire broke out on Oxford Road this morning.",
            "who_affected": "two shops",
        }
        updated = dict(base, who_affected="two shops and a nearby flat")
        self.assertNotEqual(_candidate_content_hash(base), _candidate_content_hash(updated))


class ScoringFieldsTest(unittest.TestCase):
    def test_light_fields_attached(self) -> None:
        cand = {"fingerprint": "s1", "publish_plan_status": "show"}
        annotate_morning_relevance(cand, rendered_fingerprints={"s1"})
        self.assertEqual(cand["selection_bucket"], "show_candidate")
        self.assertEqual(cand["morning_relevance_status"], "relevant")
        self.assertEqual(cand["inventory_priority"], 50)


if __name__ == "__main__":
    unittest.main()
