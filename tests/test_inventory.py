from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.inventory import (
    InventoryLock,
    aggregate_category_health,
    annotate_morning_relevance,
    build_inventory_record,
    build_morning_inventory_intake,
    categories_needing_live_fallback,
    classify_category_health,
    classify_disposition,
    evaluate_card,
    inventory_stable_block_completeness,
    inventory_record_to_candidate,
    passes_morning_contract,
    prewrite_stable_inventory_candidate,
    read_inventory,
    reentry_candidates,
    summarise_morning_intake,
    ticket_reaches_morning,
    verify_collect_conservation,
    verify_dispositions,
    write_inventory,
)
from news_digest.pipeline.llm_rewrite import _candidate_content_hash
from news_digest.pipeline.release import _summarise_inventory_morning_effect


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
        record = {
            "primary_block": "ticket_radar",
            "render_ready": True,
            "last_seen_at": "2026-07-09T08:00:00+01:00",
            "fact_card": {"ticket_type": "regular_upcoming", "tier": "C"},
        }
        ok, reason = passes_morning_contract(record, today="2026-07-01")
        self.assertFalse(ok)
        self.assertEqual(reason, "inventory_only")

    def test_a_tier_ticket_reaches_morning(self) -> None:
        record = {
            "primary_block": "ticket_radar",
            "render_ready": True,
            "last_seen_at": "2026-07-09T08:00:00+01:00",
            "fact_card": {"tier": "A", "ticket_type": "regular_upcoming"},
        }
        self.assertTrue(ticket_reaches_morning(record))
        ok, reason = passes_morning_contract(record, today="2026-07-01")
        self.assertTrue(ok)

    def test_expired_never_passes_as_fresh(self) -> None:
        record = {"primary_block": "last_24h", "render_ready": True, "last_seen_at": "2026-07-09T08:00:00+01:00", "expires_at": "2026-06-01"}
        ok, reason = passes_morning_contract(record, today="2026-07-01")
        self.assertFalse(ok)
        self.assertEqual(reason, "expired")

    def test_fact_ready_without_text_reaches_morning_as_needs_text(self) -> None:
        record = {
            "primary_block": "weekend_activities",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "render_ready": False,
            "last_seen_at": "2026-07-09T08:00:00+01:00",
            "fact_card": {"event_name": "Market", "venue": "Stockport", "date_start": "2026-07-11"},
        }
        ok, reason = passes_morning_contract(record, today="2026-07-09")
        self.assertTrue(ok)
        self.assertEqual(reason, "morning_relevant_needs_text")

    def test_ttl_expired_record_is_not_morning_relevant(self) -> None:
        record = {
            "primary_block": "last_24h",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "last_seen_at": "2026-07-01T08:00:00+01:00",
            "fact_card": {"what_happened": "x", "why_now": "today"},
        }
        ok, reason = passes_morning_contract(record, today="2026-07-09")
        self.assertFalse(ok)
        self.assertEqual(reason, "ttl_expired")

    def test_transport_needs_live_refetch_when_only_fact_ready(self) -> None:
        record = {
            "primary_block": "transport",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "last_seen_at": "2099-07-09T08:00:00+01:00",
        }
        ok, reason = passes_morning_contract(record, today="2026-07-09")
        self.assertFalse(ok)
        self.assertEqual(reason, "needs_live_refetch")


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
        self.assertEqual(rec["title"], "")

    def test_inventory_record_restores_normal_candidate_shape(self) -> None:
        record = {
            "fingerprint": "fp1",
            "title": "Stockport Makers Market",
            "summary": "Market this weekend.",
            "source_url": "https://example.test/market",
            "source_label": "Stockport Events",
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "raw_evidence": "Stockport Makers Market runs this Saturday.",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "last_seen_at": "2026-07-09T01:00:00+01:00",
            "fact_card": {"event_name": "Stockport Makers Market", "venue": "Stockport", "date_start": "2026-07-11"},
        }
        candidate = inventory_record_to_candidate(record)
        self.assertEqual(candidate["inventory_source"], "night_inventory")
        self.assertTrue(candidate["include"])
        self.assertTrue(candidate["inventory_needs_text"])
        self.assertEqual(candidate["event"]["venue"], "Stockport")

    def test_report_only_intake_counts_fact_ready_candidates(self) -> None:
        records = [
            {
                "fingerprint": "fp1",
                "title": "Market",
                "primary_block": "weekend_activities",
                "quality_status": "needs_text",
                "missing_facts": ["draft_line"],
                "last_seen_at": "2026-07-09T01:00:00+01:00",
                "fact_card": {"event_name": "Market", "venue": "Stockport", "date_start": "2026-07-11"},
            },
            {
                "fingerprint": "fp2",
                "primary_block": "weekend_activities",
                "quality_status": "missing_facts",
                "missing_facts": ["venue"],
                "last_seen_at": "2026-07-09T01:00:00+01:00",
            },
        ]
        report = summarise_morning_intake(records, today="2026-07-09")
        self.assertEqual(report["mode"], "report_only")
        self.assertEqual(report["totals"]["records"], 2)
        self.assertEqual(report["totals"]["fact_ready"], 1)
        self.assertEqual(report["totals"]["eligible"], 1)
        self.assertEqual(report["reasons"]["missing_facts"], 1)

    def test_morning_inventory_intake_only_inserts_stable_blocks(self) -> None:
        records = [
            {
                "fingerprint": "weekend-1",
                "title": "Stockport Makers Market",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "quality_status": "needs_text",
                "missing_facts": ["draft_line"],
                "last_seen_at": "2026-07-09T01:00:00+01:00",
                "fact_card": {"event_name": "Stockport Makers Market", "venue": "Stockport", "date_start": "2026-07-11"},
            },
            {
                "fingerprint": "fresh-1",
                "title": "Breaking story",
                "primary_block": "last_24h",
                "category": "media_layer",
                "quality_status": "needs_text",
                "missing_facts": ["draft_line"],
                "last_seen_at": "2026-07-09T07:30:00+01:00",
                "fact_card": {"what_happened": "x", "why_now": "today"},
            },
        ]
        candidates, report = build_morning_inventory_intake(records, mode="assist", today="2026-07-09")
        self.assertEqual([candidate["fingerprint"] for candidate in candidates], ["weekend-1"])
        self.assertEqual(report["inserted_candidates"], 1)
        self.assertIn("morning_relevant_needs_text", report["hybrid_signals"])

    def test_stable_block_completeness_has_floors(self) -> None:
        candidates = [
            {"primary_block": "weekend_activities", "source_label": f"src{i}", "draft_line": "• line"}
            for i in range(6)
        ] + [
            {"primary_block": "openings", "source_label": "food"}
        ]
        report = inventory_stable_block_completeness(candidates)
        self.assertIn("weekend_activities", report["complete_blocks"])
        self.assertIn("openings", report["incomplete_blocks"])

    def test_ticket_inventory_intake_is_capped(self) -> None:
        records = [
            {
                "fingerprint": f"ticket-{idx}",
                "title": f"A-tier show {idx}",
                "primary_block": "ticket_radar",
                "category": "venues_tickets",
                "quality_status": "needs_text",
                "missing_facts": ["draft_line"],
                "last_seen_at": "2026-07-09T01:00:00+01:00",
                "fact_card": {
                    "event_name": f"A-tier show {idx}",
                    "venue": "Co-op Live",
                    "date_start": "2026-08-01",
                    "ticket_type": "major_upcoming",
                    "tier": "A",
                },
            }
            for idx in range(25)
        ]
        candidates, report = build_morning_inventory_intake(records, mode="assist", today="2026-07-09")
        self.assertEqual(len(candidates), 20)
        self.assertEqual(report["held_by_cap"], 5)

    def test_prewrite_uses_ticket_deterministic_writer(self) -> None:
        candidate = {
            "category": "venues_tickets",
            "primary_block": "ticket_radar",
            "title": "Artist at Co-op Live",
            "source_url": "https://example.test/ticket",
            "ticket_type": "major_upcoming",
            "ticket_notability": {"artist": "Artist", "tier": "A", "kind": "artist", "confidence": 0.9},
            "event": {"event_name": "Artist", "venue": "Co-op Live", "date_start": "2026-08-01"},
        }
        self.assertTrue(prewrite_stable_inventory_candidate(candidate))
        self.assertTrue(str(candidate.get("draft_line") or "").startswith("• "))
        self.assertEqual(candidate["draft_line_provider"], "night_inventory_prewrite")


class NightWaveTest(unittest.TestCase):
    def test_wave_writes_inventory_only_never_candidates(self) -> None:
        import types
        from unittest import mock

        import scripts.run_local_digest as runner
        from news_digest.pipeline.collector import core as collector_core
        from news_digest.pipeline import entity_extraction, event_extraction

        fake_sources = [
            types.SimpleNamespace(name="BBC Manchester", report_category="media_layer"),
            types.SimpleNamespace(name="GMP", report_category="gmp"),
            types.SimpleNamespace(name="Ticketmaster", report_category="venues_tickets"),  # not in live_news wave
        ]

        def fake_collect(source):
            health = {"checked": True, "fetched": True, "errors": []}
            cand = {
                "fingerprint": f"{source.name}-1",
                "primary_block": "next_7_days",
                "category": source.report_category,
                "title": "Market at HOME",
            }
            return health, [cand]

        def fake_entities(candidates):
            for candidate in candidates:
                candidate["entities"] = {"venues": ["HOME"]}

        def fake_events(candidates):
            for candidate in candidates:
                candidate["event"] = {"event_name": "Market", "venue": "HOME", "date_start": "2026-07-11"}

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data" / "state").mkdir(parents=True)
            with mock.patch.object(runner, "PROJECT_ROOT", root), \
                    mock.patch.object(collector_core, "SOURCES", fake_sources), \
                    mock.patch.object(collector_core, "_collect_single_source", side_effect=fake_collect), \
                    mock.patch.object(entity_extraction, "enrich_candidates_entities", side_effect=fake_entities), \
                    mock.patch.object(event_extraction, "enrich_candidates_events", side_effect=fake_events), \
                    mock.patch("sys.stdout"):
                rc = runner.cmd_collect_inventory("live_news")

            self.assertEqual(rc, 0)
            # candidates.json must NOT be created by a night wave
            self.assertFalse((root / "data" / "state" / "candidates.json").exists())
            # inventory written for the wave's categories only
            self.assertEqual([r["fingerprint"] for r in read_inventory(root / "data" / "state", "media_layer")], ["BBC Manchester-1"])
            self.assertEqual([r["fingerprint"] for r in read_inventory(root / "data" / "state", "gmp")], ["GMP-1"])
            self.assertEqual(read_inventory(root / "data" / "state", "media_layer")[0]["fact_card"]["venue"], "HOME")
            self.assertEqual(read_inventory(root / "data" / "state", "media_layer")[0]["quality_status"], "needs_text")
            # the ticket source is outside the live_news wave — not collected
            self.assertEqual(read_inventory(root / "data" / "state", "venues_tickets"), [])

    def test_release_reports_inventory_as_not_yet_morning_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "data" / "state"
            state_dir.mkdir(parents=True)
            write_inventory(
                state_dir,
                "media_layer",
                [
                    {
                        "fingerprint": "night-1",
                        "render_ready": True,
                        "last_seen_at": "2026-07-07T07:34:00+01:00",
                    },
                    {
                        "fingerprint": "night-2",
                        "render_ready": False,
                        "last_seen_at": "2026-07-07T07:35:00+01:00",
                    },
                ],
            )
            (state_dir / "inventory_run_log.jsonl").write_text(
                '{"wave":"breaking","run_at_london":"2026-07-07T07:35:00+01:00"}\n',
                encoding="utf-8",
            )

            summary = _summarise_inventory_morning_effect(state_dir)

        self.assertFalse(summary["morning_consumed"])
        self.assertEqual(summary["inventory_files"], 1)
        self.assertEqual(summary["total_records"], 2)
        self.assertEqual(summary["render_ready_records"], 1)
        self.assertEqual(summary["last_wave"], "breaking")


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
