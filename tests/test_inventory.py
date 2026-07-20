from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest

from news_digest.pipeline.common import PRIMARY_BLOCKS
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from news_digest.pipeline.event_extraction import enrich_candidate_event
from news_digest.pipeline.inventory import (
    InventoryLock,
    INVENTORY_BLOCK_REGISTRY,
    aggregate_category_health,
    action_url_probe_result,
    annotate_morning_relevance,
    build_inventory_record,
    build_morning_inventory_intake,
    categories_needing_live_fallback,
    classify_category_health,
    classify_disposition,
    evaluate_card,
    inventory_block_completeness,
    inventory_category_output_blocks,
    inventory_record_to_candidate,
    inventory_source_replacement_plan,
    merge_inventory,
    operational_night_category_health,
    passes_morning_contract,
    prewrite_stable_inventory_candidate,
    prune_inventory,
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
    def test_selected_but_not_rendered_is_reported_explicitly(self) -> None:
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
        self.assertEqual(result["selected_not_rendered"], 1)  # lost1
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
            "category": "public_services",
            "source_report_category": "public_services",
            "summary": "A council service closure starts next week.",
            "source_url": "https://example.test/event",
            "event": {"event_name": "Council service closure", "venue": "HOME", "date_start": "2026-07-15"},
            "draft_line": "• С 15 июля HOME закрывается на ремонт.",
        }
        status, ready, missing = evaluate_card(cand)
        self.assertTrue(ready)
        self.assertEqual((status, missing), ("ready", []))

    def test_fields_present_but_no_text_is_needs_text(self) -> None:
        cand = {
            "primary_block": "next_7_days",
            "category": "public_services",
            "source_report_category": "public_services",
            "summary": "A council service closure starts next week.",
            "source_url": "https://example.test/event",
            "event": {"event_name": "Council service closure", "venue": "HOME", "date_start": "2026-07-15"},
        }
        status, ready, missing = evaluate_card(cand)
        self.assertEqual((status, ready, missing), ("needs_text", False, ["draft_line"]))

    def test_opening_needs_specific_subject_and_phase_or_date(self) -> None:
        candidate = {
            "primary_block": "openings",
            "title": "New cafe",
            "source_url": "https://example.test/cafe",
            "event": {"event_name": "New cafe"},
            "draft_line": "• В Стокпорте открылось новое кафе.",
        }
        status, _, missing = evaluate_card(candidate)
        self.assertEqual(status, "missing_facts")
        self.assertIn("opening_phase_or_date", missing)

    def test_weekend_requires_public_activity_and_gm_fit(self) -> None:
        candidate = {
            "primary_block": "weekend_activities", "category": "culture_weekly",
            "title": "Generic concert", "source_url": "https://example.test/show",
            "venue_scope": "outside",
            "event": {"event_name": "Generic concert", "venue": "London Arena", "date_start": "2026-07-18"},
            "draft_line": "• Generic concert.",
        }
        status, ready, missing = evaluate_card(candidate)
        self.assertFalse(ready)
        self.assertEqual(status, "missing_facts")
        self.assertIn("activity_type", missing)
        self.assertIn("gm_fit", missing)

    def test_professional_keyword_score_cannot_replace_llm_cv_match(self) -> None:
        candidate = {
            "primary_block": "professional_events", "category": "professional_events",
            "title": "AI founders breakfast", "source_url": "https://example.test/pro",
            "event": {"event_name": "AI founders breakfast", "venue": "Manchester Central", "date_start": "2026-07-20"},
            "professional_event_match": {"publish": True, "access_label": "free"},
            "draft_line": "• AI founders breakfast.",
        }
        self.assertIn("professional_llm_cv", evaluate_card(candidate)[2])
        candidate["professional_event_match"].update({"llm_fit": "go", "publish": True})
        candidate["professional_match_status"] = "llm_cv_matched"
        self.assertEqual(evaluate_card(candidate), ("ready", True, []))

    def test_outside_gm_inventory_accepts_only_a_tier(self) -> None:
        candidate = {
            "primary_block": "outside_gm_tickets", "category": "venues_tickets",
            "title": "Artist in London", "source_url": "https://example.test/ticket",
            "venue_scope": "outside", "ticket_type": "major_upcoming",
            "ticket_notability": {"tier": "B"},
            "event": {"event_name": "Artist", "venue": "The O2", "date_start": "2026-09-01"},
            "draft_line": "• Artist в Лондоне.",
        }
        self.assertIn("outside_a_tier", evaluate_card(candidate)[2])
        candidate["ticket_notability"]["tier"] = "A"
        self.assertEqual(evaluate_card(candidate), ("ready", True, []))


class MorningContractTest(unittest.TestCase):
    def test_stale_ticket_without_reason_is_inventory_only(self) -> None:
        record = {
            "primary_block": "ticket_radar",
            "render_ready": True,
            "last_seen_at": "2026-07-09T08:00:00+01:00",
            "fact_card": {"ticket_type": "regular_upcoming", "tier": "C"},
        }
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-09T09:00:00+01:00"),
        ):
            ok, reason = passes_morning_contract(record, today="2026-07-09")
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
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-09T09:00:00+01:00"),
        ):
            ok, reason = passes_morning_contract(record, today="2026-07-09")
        self.assertTrue(ok)

    def test_expired_never_passes_as_fresh(self) -> None:
        record = {"primary_block": "last_24h", "render_ready": True, "last_seen_at": "2026-07-09T08:00:00+01:00", "expires_at": "2026-06-01"}
        ok, reason = passes_morning_contract(record, today="2026-07-01")
        self.assertFalse(ok)
        self.assertEqual(reason, "expired")

    def test_recurring_weekend_uses_next_occurrence_not_old_date_end(self) -> None:
        record = {
            "primary_block": "weekend_activities",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "render_ready": False,
            "last_seen_at": "2026-07-19T01:00:00+01:00",
            "source_url": "https://example.test/bowlee",
            "title": "Bowlee Car Boot Sale",
            "fact_card": {
                "event_name": "Bowlee Car Boot Sale",
                "venue": "Bowlee Community Park",
                "venue_scope": "GM",
                "date_start": "2026-07-12",
                "date_end": "2026-07-12",
                "next_occurrence": "2026-07-19",
                "is_recurring": True,
            },
        }
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-19T08:00:00+01:00"),
        ):
            ok, reason = passes_morning_contract(record, today="2026-07-19")
        self.assertTrue(ok, reason)
        self.assertEqual(reason, "morning_relevant_needs_text")

    def test_food_inventory_recovers_named_venue_and_has_valid_dedupe_seed(self) -> None:
        record = {
            "fingerprint": "osma",
            "title": "OSMA set to return with new restaurant and grocery in the city centre",
            "source_url": "https://example.test/osma",
            "source_label": "Manchester's Finest",
            "primary_block": "openings",
            "category": "food_openings",
            "raw_evidence": "OSMA will reopen at One Port Street in the Northern Quarter alongside a grocery.",
            "fact_card": {
                "event_name": "OSMA set to return with new restaurant and grocery in the city centre",
                "venue": "",
                "date_start": "",
                "venue_scope": "GM",
            },
        }
        candidate = inventory_record_to_candidate(record)
        self.assertEqual(candidate["event"]["venue"], "OSMA")
        self.assertEqual(candidate["dedupe_decision"], "new")
        self.assertEqual(candidate["reason"], "pending dedupe")
        self.assertEqual(evaluate_card(candidate), ("needs_text", False, ["draft_line"]))

    def test_food_recurring_date_requires_explicit_recurrence_evidence(self) -> None:
        base = {
            "primary_block": "openings",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "last_seen_at": "2026-07-20T03:37:00+01:00",
        }
        one_off = {
            **base,
            "title": "Joe & The Juice arrives this Friday",
            "raw_evidence": "The store will open on Friday 10 July 2026.",
            "fact_card": {
                "event_name": "Joe & The Juice",
                "venue": "Sunlight House",
                "date_start": "2026-07-10",
                "is_recurring": True,
                "next_occurrence": "2026-07-24",
            },
        }
        monthly = {
            **base,
            "title": "Asian Food Night Market",
            "raw_evidence": "The market returns every second Friday of the month.",
            "fact_card": {
                "event_name": "Asian Food Night Market",
                "venue": "Churchgate Stockport",
                "date_start": "2026-07-10",
                "is_recurring": True,
                "next_occurrence": "2026-08-14",
            },
        }

        self.assertEqual(passes_morning_contract(one_off, today="2026-07-20"), (False, "event_expired"))
        self.assertTrue(passes_morning_contract(monthly, today="2026-07-20")[0])

    def test_fact_ready_without_text_reaches_morning_as_needs_text(self) -> None:
        record = {
            "primary_block": "next_7_days",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "render_ready": False,
            "last_seen_at": "2026-07-09T08:00:00+01:00",
            "fact_card": {"event_name": "Market", "venue": "Stockport", "date_start": "2026-07-11"},
        }
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-09T08:00:00+01:00"),
        ):
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

    def test_ongoing_event_uses_date_end_but_past_one_day_event_expires(self) -> None:
        base = {
            "primary_block": "next_7_days",
            "quality_status": "ready",
            "render_ready": True,
            "last_seen_at": "2099-07-13T01:00:00+01:00",
        }
        ongoing = {**base, "fact_card": {"date_start": "2026-07-10", "date_end": "2026-07-20"}}
        expired = {**base, "fact_card": {"date_start": "2026-07-10", "date_end": "2026-07-10"}}
        self.assertTrue(passes_morning_contract(ongoing, today="2026-07-13")[0])
        self.assertEqual(passes_morning_contract(expired, today="2026-07-13"), (False, "event_expired"))


class ReplacementPlanTest(unittest.TestCase):
    def test_registry_covers_all_blocks_and_derives_mixed_outputs(self) -> None:
        self.assertEqual(set(INVENTORY_BLOCK_REGISTRY), set(PRIMARY_BLOCKS))
        self.assertEqual(
            inventory_category_output_blocks()["venues_tickets"],
            frozenset({"ticket_radar", "future_announcements", "outside_gm_tickets"}),
        )

    def test_mixed_ticket_category_never_skips_without_explicit_permission(self) -> None:
        report = {"completeness": {"blocks": {
            "ticket_radar": {"block_sufficient": True, "liveness_sufficient_for_replacement": True},
            "future_announcements": {"block_sufficient": True, "liveness_sufficient_for_replacement": True},
            "outside_gm_tickets": {"block_sufficient": True, "liveness_sufficient_for_replacement": True},
            "openings": {"block_sufficient": True, "liveness_sufficient_for_replacement": True},
        }}}
        health = {
            "venues_tickets": {"status": "ok"},
            "food_openings": {"status": "ok"},
        }
        plan = inventory_source_replacement_plan(report, health)
        self.assertFalse(plan["venues_tickets"]["safe_to_skip"])
        self.assertEqual(plan["venues_tickets"]["reason"], "source_replacement_not_enabled")
        self.assertTrue(plan["food_openings"]["safe_to_skip"])

    def test_latest_night_health_exposes_source_errors(self) -> None:
        rows = [
            {"run_id": "r1", "run_at_london": "2026-07-13T03:30:00+01:00", "category": "food_openings", "checked": True, "found": 2, "errors": 0, "expected_sources": 2},
            {"run_id": "r1", "run_at_london": "2026-07-13T03:30:01+01:00", "category": "food_openings", "checked": True, "found": 0, "errors": 1, "expected_sources": 2},
        ]
        health = operational_night_category_health(rows, current_day="2026-07-13")["food_openings"]
        self.assertEqual(health["status"], "degraded")
        self.assertEqual(health["source_errors"], 1)

    def test_old_healthy_wave_is_stale_not_operationally_green(self) -> None:
        rows = [{
            "run_id": "old", "run_at_london": "2026-07-12T03:30:00+01:00",
            "category": "food_openings", "source": "Food source", "checked": True,
            "found": 3, "errors": 0, "expected_sources": 1,
        }]
        health = operational_night_category_health(rows, current_day="2026-07-13")["food_openings"]
        self.assertEqual(health["status"], "stale")

    def test_degraded_wave_does_not_poison_healthy_sibling_category(self) -> None:
        rows = [{
            "run_id": "partial-wave",
            "run_at_london": "2026-07-15T03:30:00+01:00",
            "category": "food_openings",
            "source": "Food source",
            "checked": True,
            "found": 3,
            "errors": 0,
            "expected_sources": 1,
            "wave_status": "degraded",
        }]
        health = operational_night_category_health(rows, current_day="2026-07-15")["food_openings"]
        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["category_status"], "success")
        self.assertEqual(health["wave_status"], "degraded")


class InventoryFactPreservationTest(unittest.TestCase):
    def test_empty_morning_extraction_does_not_erase_night_range(self) -> None:
        candidate = {
            "inventory_source": "night_inventory",
            "event": {
                "event_name": "Exhibition",
                "venue": "HOME",
                "date_start": "2026-07-10",
                "date_end": "2026-07-20",
                "is_recurring": True,
            }
        }
        with patch("news_digest.pipeline.event_extraction.extract_event", return_value={"event_name": "", "venue": "", "date_start": ""}):
            enrich_candidate_event(candidate)
        self.assertEqual(candidate["event"]["venue"], "HOME")
        self.assertEqual(candidate["event"]["date_end"], "2026-07-20")

    def test_matching_live_candidate_keeps_live_values_and_gains_night_facts(self) -> None:
        live = {
            "fingerprint": "same",
            "title": "Fresh live title",
            "summary": "Fresh live summary",
            "source_url": "https://example.test/events/market?utm_source=morning",
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "event": {"event_name": "", "venue": "", "date_start": ""},
        }
        record = {
            "fingerprint": "same",
            "title": "Night title",
            "summary": "Night summary",
            "source_url": "https://example.test/events/market",
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "last_seen_at": "2026-07-15T01:00:00+01:00",
            "run_id": "night-1",
            "wave": "events",
            "fact_card": {
                "event_name": "Stockport Night Market",
                "venue": "Churchgate",
                "date_start": "2026-07-18",
                "venue_scope": "GM",
            },
        }
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-16T08:00:00+01:00"),
        ):
            inserted, report = build_morning_inventory_intake(
                [record], existing_candidates=[live], mode="assist", today="2026-07-16"
            )
        self.assertEqual(inserted, [])
        self.assertEqual(live["summary"], "Fresh live summary")
        self.assertEqual(live["event"]["venue"], "Churchgate")
        self.assertEqual(live["event"]["date_start"], "2026-07-18")
        self.assertTrue(live["inventory_merged_into_live"])
        self.assertEqual(report["funnel"]["merged_into_live"], 1)

    def test_secondary_url_match_rejects_index_page(self) -> None:
        live = {
            "fingerprint": "live-index",
            "source_url": "https://example.test/events",
            "event": {"venue": ""},
        }
        record = {
            "fingerprint": "night-index",
            "title": "Stockport Night Market",
            "source_url": "https://example.test/events",
            "primary_block": "weekend_activities",
            "category": "culture_weekly",
            "quality_status": "needs_text",
            "missing_facts": ["draft_line"],
            "last_seen_at": "2026-07-16T01:00:00+01:00",
            "fact_card": {
                "event_name": "Stockport Night Market",
                "venue": "Churchgate",
                "venue_scope": "GM",
                "date_start": "2026-07-18",
            },
        }
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-16T08:00:00+01:00"),
        ):
            inserted, _ = build_morning_inventory_intake(
                [record], existing_candidates=[live], mode="assist", today="2026-07-16"
            )
        self.assertEqual(live["event"]["venue"], "")
        self.assertEqual([candidate["fingerprint"] for candidate in inserted], ["night-index"])


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

    def test_merge_preserves_first_seen_and_changes_last_changed_only_on_new_facts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            first = {
                "fingerprint": "e1", "evidence_hash": "h1",
                "first_seen_at": "2026-07-12T01:00:00+01:00",
                "last_seen_at": "2026-07-12T01:00:00+01:00",
                "last_changed_at": "2026-07-12T01:00:00+01:00",
            }
            merge_inventory(state_dir, "food_openings", [first])
            same = {**first, "last_seen_at": "2026-07-13T01:00:00+01:00"}
            merge_inventory(state_dir, "food_openings", [same])
            row = read_inventory(state_dir, "food_openings")[0]
            self.assertEqual(row["first_seen_at"], "2026-07-12T01:00:00+01:00")
            self.assertEqual(row["last_changed_at"], "2026-07-12T01:00:00+01:00")
            changed = {**same, "evidence_hash": "h2", "last_seen_at": "2026-07-14T01:00:00+01:00"}
            merge_inventory(state_dir, "food_openings", [changed])
            row = read_inventory(state_dir, "food_openings")[0]
            self.assertEqual(row["last_changed_at"], "2026-07-14T01:00:00+01:00")
            self.assertEqual(row["source_report_category"], "food_openings")
            # second acquisition after release must succeed
            with InventoryLock(state_dir, name="t"):
                pass

    def test_link_is_dead_only_after_two_not_found_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            base = {
                "fingerprint": "gone",
                "primary_block": "ticket_radar",
                "run_id": "run-1",
                "last_seen_at": "2026-07-15T01:00:00+01:00",
                "action_url_probe_result": "not_found",
            }
            merge_inventory(state_dir, "venues_tickets", [base])
            first = read_inventory(state_dir, "venues_tickets")[0]
            self.assertEqual(first["action_url_liveness"], "unknown")
            merge_inventory(
                state_dir,
                "venues_tickets",
                [{**base, "run_id": "run-2", "last_seen_at": "2026-07-16T01:00:00+01:00"}],
            )
            second = read_inventory(state_dir, "venues_tickets")[0]
            self.assertEqual(second["action_url_liveness"], "dead")
            self.assertEqual(second["action_url_failure_run_ids"], ["run-1", "run-2"])

    def test_unprobed_refresh_preserves_previous_alive_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            first = {
                "fingerprint": "alive",
                "last_seen_at": "2026-07-15T01:00:00+01:00",
                "action_url_probe_result": "alive",
                "action_url_checked_at": "2026-07-15T01:01:00+01:00",
            }
            merge_inventory(state_dir, "food_openings", [first])
            merge_inventory(
                state_dir,
                "food_openings",
                [{"fingerprint": "alive", "last_seen_at": "2026-07-16T01:00:00+01:00"}],
            )
            row = read_inventory(state_dir, "food_openings")[0]
            self.assertEqual(row["action_url_liveness"], "alive")
            self.assertEqual(row["action_url_checked_at"], "2026-07-15T01:01:00+01:00")

    def test_retention_cleanup_keeps_future_ticket_and_removes_old_transport(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_inventory(
                state_dir,
                "mixed",
                [
                    {
                        "fingerprint": "future-ticket",
                        "primary_block": "ticket_radar",
                        "last_seen_at": "2026-07-01T01:00:00+01:00",
                        "serving_expires_at": "2026-07-02T01:00:00+01:00",
                        "fact_card": {"date_start": "2026-12-01"},
                    },
                    {
                        "fingerprint": "old-transport",
                        "primary_block": "transport",
                        "last_seen_at": "2026-06-01T01:00:00+01:00",
                        "fact_card": {},
                    },
                ],
            )
            report = prune_inventory(state_dir, today="2026-07-15")
            rows = read_inventory(state_dir, "mixed")
            self.assertEqual([row["fingerprint"] for row in rows], ["future-ticket"])
            self.assertEqual(rows[0]["retention_until"], "2026-12-31")
            self.assertEqual(report["removed_expired"], 1)

    def test_http_status_contract_keeps_blocking_responses_unknown(self) -> None:
        self.assertEqual(action_url_probe_result(200), "alive")
        self.assertEqual(action_url_probe_result(302), "alive")
        self.assertEqual(action_url_probe_result(404), "not_found")
        self.assertEqual(action_url_probe_result(410), "not_found")
        self.assertEqual(action_url_probe_result(403), "unknown")
        self.assertEqual(action_url_probe_result(429), "unknown")


class BuildRecordTest(unittest.TestCase):
    def test_record_has_canonical_schema(self) -> None:
        cand = {
            "fingerprint": "fp1",
            "primary_block": "ticket_radar",
            "source_url": "https://x/t",
            "venue_scope": "GM",
            "ticket_type": "on_sale_now",
            "ticket_notability": {"tier": "A"},
            "event": {"event_name": "Artist", "venue": "Co-op Live", "date_start": "2026-08-01"},
            "draft_line": "• Artist в Co-op Live 1 августа.",
        }
        rec = build_inventory_record(cand, prompt_version=1)
        for key in (
            "fingerprint", "evidence_hash", "prompt_version", "fact_card", "render_ready",
            "missing_facts", "first_seen_at", "last_seen_at", "last_changed_at",
            "observed_in_wave", "action_url_liveness", "serving_expires_at", "retention_until",
        ):
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
                "source_url": "https://example.test/market",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_report_category": "culture_weekly",
                "quality_status": "needs_text",
                "missing_facts": ["draft_line"],
                "last_seen_at": "2026-07-09T01:00:00+01:00",
                "fact_card": {
                    "event_name": "Market", "venue": "Stockport", "venue_scope": "GM",
                    "date_start": "2026-07-11",
                },
            },
            {
                "fingerprint": "fp2",
                "primary_block": "weekend_activities",
                "quality_status": "missing_facts",
                "missing_facts": ["venue"],
                "last_seen_at": "2026-07-09T01:00:00+01:00",
            },
        ]
        # Pin the clock: eligibility runs the TTL contract against now_london(),
        # so a record seen at 01:00 on the test's "today" must be measured from
        # that same day, not the real wall-clock date.
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-09T08:00:00+01:00"),
        ):
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
                "source_url": "https://example.test/weekend",
                "primary_block": "weekend_activities",
                "category": "culture_weekly",
                "source_report_category": "culture_weekly",
                "quality_status": "needs_text",
                "missing_facts": ["draft_line"],
                "last_seen_at": "2026-07-09T01:00:00+01:00",
                "fact_card": {
                    "event_name": "Stockport Makers Market", "venue": "Stockport",
                    "venue_scope": "GM", "date_start": "2026-07-11",
                },
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
        # The TTL contract measures record age against now_london(); pin it to
        # the test's "today" so a record seen at 01:00 that day stays within TTL
        # regardless of the real wall-clock date the suite runs on.
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-09T08:00:00+01:00"),
        ):
            candidates, report = build_morning_inventory_intake(records, mode="assist", today="2026-07-09")
        self.assertEqual([candidate["fingerprint"] for candidate in candidates], ["weekend-1"])
        self.assertNotIn("recoverable_reserve", candidates[0])
        self.assertNotIn("public_reserve", candidates[0])
        self.assertEqual(report["inserted_candidates"], 1)
        self.assertIn("morning_relevant_needs_text", report["hybrid_signals"])

    def test_block_completeness_separates_required_and_optional_blocks(self) -> None:
        candidates = [
            {"primary_block": "weekend_activities", "source_label": f"src{i}", "draft_line": "• line"}
            for i in range(6)
        ] + [
            {"primary_block": "openings", "source_label": "food"}
        ]
        report = inventory_block_completeness(candidates)
        self.assertIn("weekend_activities", report["sufficient_blocks"])
        self.assertIn("openings", report["insufficient_blocks"])
        self.assertTrue(report["blocks"]["future_announcements"]["block_sufficient"])
        self.assertEqual(report["blocks"]["future_announcements"]["candidate_count"], 0)

    def test_a_tier_ticket_inventory_bypasses_intake_cap(self) -> None:
        records = [
            {
                "fingerprint": f"ticket-{idx}",
                "title": f"A-tier show {idx}",
                "source_url": f"https://example.test/ticket-{idx}",
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
                    "venue_scope": "GM",
                },
            }
            for idx in range(25)
        ]
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-09T08:00:00+01:00"),
        ):
            candidates, report = build_morning_inventory_intake(records, mode="assist", today="2026-07-09")
        self.assertEqual(len(candidates), 25)
        self.assertEqual(report["held_by_cap"], 0)

    def test_changed_facts_invalidate_cached_night_line(self) -> None:
        candidate = {
            "fingerprint": "event-1",
            "title": "Artist",
            "source_url": "https://example.test/artist?utm_source=x",
            "source_label": "Official venue",
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "source_report_category": "venues_tickets",
            "venue_scope": "GM",
            "ticket_type": "major_upcoming",
            "ticket_notability": {"tier": "A", "signal": "major artist"},
            "event": {"event_name": "Artist", "venue": "Co-op Live", "date_start": "2026-08-01"},
            "draft_line": "• 1 августа в Co-op Live выступит Artist.",
        }
        record = {"schema_version": 1, **build_inventory_record(candidate, prompt_version=7)}
        record["title"] = "Market date changed"
        with patch(
            "news_digest.pipeline.inventory.now_london",
            return_value=datetime.fromisoformat("2026-07-09T08:00:00+01:00"),
        ):
            candidates, report = build_morning_inventory_intake(
                [record], today="2026-07-09", prompt_version=7
            )
        self.assertEqual(report["invalidated_prewrite"], 1)
        self.assertEqual(candidates[0]["draft_line"], "")
        self.assertTrue(candidates[0]["inventory_needs_text"])

    def test_canonical_tracking_url_does_not_invalidate_but_status_does(self) -> None:
        candidate = {
            "primary_block": "ticket_radar", "category": "venues_tickets",
            "source_report_category": "venues_tickets", "title": "Artist",
            "source_url": "https://example.test/artist?utm_source=one", "venue_scope": "GM",
            "ticket_type": "major_upcoming", "ticket_notability": {"tier": "A", "signal": "major"},
            "event": {"event_name": "Artist", "venue": "Co-op Live", "date_start": "2026-08-01", "event_status": "scheduled"},
        }
        first = build_inventory_record(candidate, prompt_version=7)["evidence_hash"]
        candidate["source_url"] = "https://example.test/artist?utm_source=two"
        self.assertEqual(first, build_inventory_record(candidate, prompt_version=7)["evidence_hash"])
        candidate["event"]["event_status"] = "cancelled"
        self.assertNotEqual(first, build_inventory_record(candidate, prompt_version=7)["evidence_hash"])

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
        self.assertEqual(candidate["draft_line_provider"], "night_inventory_deterministic")

    def test_food_and_russian_do_not_use_generic_deterministic_prewrite(self) -> None:
        for category, block in (("food_openings", "openings"), ("diaspora_events", "russian_events")):
            candidate = {
                "category": category,
                "primary_block": block,
                "title": "Named event",
                "source_url": "https://example.test/event",
                "event": {"event_name": "Named event", "venue": "Named Venue", "date_start": "2026-08-01"},
            }
            self.assertFalse(prewrite_stable_inventory_candidate(candidate))
            self.assertNotIn("draft_line", candidate)


class NightWaveTest(unittest.TestCase):
    def test_complete_wave_requires_every_expected_source_row(self) -> None:
        from scripts.run_local_digest import _complete_inventory_wave_for_day

        complete_rows = [
            {
                "run_id": "events-1", "wave": "events",
                "run_at_london": "2026-07-20T00:31:01+01:00",
                "expected_sources": 2, "checked": True, "errors": 0,
            },
            {
                "run_id": "events-1", "wave": "events",
                "run_at_london": "2026-07-20T00:31:02+01:00",
                "expected_sources": 2, "checked": True, "errors": 1,
            },
        ]
        result = _complete_inventory_wave_for_day(
            complete_rows, wave="events", day_london="2026-07-20"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["errors"], 1)
        self.assertIsNone(_complete_inventory_wave_for_day(
            complete_rows[:1], wave="events", day_london="2026-07-20"
        ))

    def test_night_source_collection_is_bounded_parallel_and_ordered(self) -> None:
        from scripts.run_local_digest import _collect_inventory_sources

        lock = threading.Lock()
        active = 0
        peak = 0

        def collect(source):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.03)
            with lock:
                active -= 1
            return {"checked": True}, [{"title": source}]

        results = _collect_inventory_sources(["a", "b", "c", "d"], collect, max_workers=2)
        self.assertEqual([row[0] for row in results], ["a", "b", "c", "d"])
        self.assertGreater(peak, 1)
        self.assertLessEqual(peak, 2)

    def test_daily_state_commit_reuses_bounded_retry_loop(self) -> None:
        workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "daily-digest.yml").read_text(encoding="utf-8")
        self.assertIn("for attempt in 1 2 3 4 5", workflow)
        self.assertIn("Pushed daily state on attempt", workflow)

    def test_night_state_commit_pushes_back_to_dispatched_branch(self) -> None:
        workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "night-inventory.yml").read_text(encoding="utf-8")
        self.assertIn('target_branch="${GITHUB_REF_NAME:-main}"', workflow)
        self.assertIn('git pull --rebase --autostash origin "$target_branch"', workflow)
        self.assertIn('git push origin "HEAD:$target_branch"', workflow)

    def test_events_has_dst_safe_idempotent_schedule_fallback(self) -> None:
        workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "night-inventory.yml").read_text(encoding="utf-8")
        self.assertIn("cron: '41 23 * * *'", workflow)
        self.assertIn("cron: '41 0 * * *'", workflow)
        self.assertIn("inventory-wave-complete --wave", workflow)
        self.assertIn("steps.wave.outputs.skip != 'true'", workflow)

    def test_main_code_pushes_run_ci_but_state_only_commits_do_not(self) -> None:
        workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
        self.assertNotIn("branches-ignore", workflow)
        self.assertIn("'data/state/**'", workflow)
        self.assertIn("'data/outgoing/**'", workflow)

    def test_wave_status_distinguishes_success_degraded_and_failed(self) -> None:
        from scripts.run_local_digest import _inventory_wave_status

        self.assertEqual(
            _inventory_wave_status([{"checked": True, "errors": 0}], 1),
            "success",
        )
        self.assertEqual(
            _inventory_wave_status(
                [{"checked": True, "errors": 0}, {"checked": True, "errors": 1}],
                2,
            ),
            "degraded",
        )
        self.assertEqual(
            _inventory_wave_status([{"checked": False, "errors": 1}], 1),
            "failed",
        )
        mixed_wave = [
            {"category": "media_layer", "checked": True, "errors": 1},
            {"category": "transport", "checked": True, "errors": 0},
        ]
        self.assertEqual(_inventory_wave_status(mixed_wave, 2), "degraded")
        self.assertEqual(
            _inventory_wave_status(
                [row for row in mixed_wave if row["category"] == "transport"],
                1,
            ),
            "success",
        )

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
            self.assertEqual(read_inventory(root / "data" / "state", "media_layer")[0]["quality_status"], "missing_facts")
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

    def test_release_funnel_tracks_night_lineage_merged_into_live(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "data" / "state"
            outgoing_dir = Path(tmp) / "data" / "outgoing"
            state_dir.mkdir(parents=True)
            outgoing_dir.mkdir(parents=True)
            (state_dir / "morning_inventory_intake_report.json").write_text(
                json.dumps({
                    "mode": "assist",
                    "actual_intake": {
                        "inserted_candidates": 0,
                        "funnel": {"records": 1, "merged_into_live": 1},
                        "lineages": [{
                            "lineage_id": "0:night-1",
                            "inventory_fingerprint": "night-1-with-evidence",
                            "candidate_fingerprint": "night-1-with-evidence",
                            "live_fingerprint": "night-1-with-evidence",
                            "source_url": "https://example.test/story",
                            "primary_block": "weekend_activities",
                            "intake_status": "merged_into_live",
                            "operational_provenance": "current",
                        }],
                    },
                }),
                encoding="utf-8",
            )
            (state_dir / "candidates.json").write_text(
                json.dumps({"candidates": [{
                    "fingerprint": "live-1",
                    "title": "Live story",
                    "source_url": "https://example.test/story",
                    "primary_block": "weekend_activities",
                    "validated": True,
                    "include": True,
                    "inventory_lineages": [{"fingerprint": "night-1-with-evidence"}],
                }]}),
                encoding="utf-8",
            )
            (state_dir / "candidate_validation_report.json").write_text(
                json.dumps({"items": [{"fingerprint": "live-1", "validated": True}]}),
                encoding="utf-8",
            )
            (state_dir / "writer_report.json").write_text(
                json.dumps({"rendered_candidate_fingerprints": ["live-1"]}),
                encoding="utf-8",
            )
            (outgoing_dir / "current_digest.html").write_text(
                '<a href="https://example.test/yesterday">old source</a>',
                encoding="utf-8",
            )

            summary = _summarise_inventory_morning_effect(
                state_dir,
                final_html='<a href="https://example.test/story">current source</a>',
            )

        self.assertTrue(summary["morning_consumed"])
        self.assertEqual(summary["final_funnel"]["merged_into_live"], 1)
        self.assertEqual(summary["final_funnel"]["operational_lineages"], 1)
        self.assertEqual(summary["final_funnel"]["active_morning_lineages"], 1)
        self.assertEqual(summary["final_funnel"]["active_current_lineages"], 1)
        self.assertEqual(summary["final_funnel"]["visible_in_final_html"], 1)
        self.assertEqual(summary["final_funnel"]["lineages"][0]["final_status"], "visible_html")
        self.assertEqual(summary["final_funnel"]["lineages"][0]["candidate_fingerprint"], "live-1")


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
