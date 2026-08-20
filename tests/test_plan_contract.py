"""Этап 3: контракт слот-плана.

1. Детерминизм: одинаковый вход → идентичный план.
2. Дублёры lead не занимают публичные слоты (из-под границы отбора).
3. Писатель не меняет состав: каждая видимая строка ∈ плану.
4. Редактор не имеет API блоковых действий и не меняет состав.
5. Финальная сверка ловит пропажу плановой строки; технический брак блокирует.
6. Негодный (протухший) запасной отклоняется контроллером, берётся следующий.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from news_digest.pipeline.block_policy import BLOCK_POLICY_VERSION
from news_digest.pipeline.common import (
    candidates_by_fingerprint,
    canonical_url_identity,
    fingerprint_for_candidate,
    now_london,
)
from news_digest.pipeline.dedupe import dedupe_candidates
from news_digest.pipeline.editor import edit_digest
from news_digest.pipeline.plan_digest import _apply_routing, run_plan_digest
from news_digest.pipeline.plan_execution import (
    build_final_execution_report,
    load_execution,
    load_plan,
    next_backup,
    save_execution,
)
from news_digest.pipeline.repeat_policy import RepeatVerdict
from news_digest.pipeline.release import _planner_repeat_decision
from news_digest.pipeline.verify_digest_plan import run_verify_digest_plan
from news_digest.pipeline.writer import write_digest


def _candidate(idx: int, block: str = "last_24h", **over: object) -> dict:
    base = {
        "include": True,
        "validated": True,
        "fingerprint": f"fp-{block}-{idx}",
        "category": "media_layer",
        "primary_block": block,
        "title": f"Manchester service update {idx}",
        "summary": "Manchester council confirmed a practical service update for residents.",
        "lead": "",
        "published_at": now_london().isoformat(),
        "evidence_text": (
            "Manchester council confirmed a practical service update for residents "
            "with specific travel and service details for this week."
        ),
        "source_label": f"Source {idx}",
        "source_url": f"https://example.test/{block}/{idx}",
        "draft_line": (
            "• Manchester Council подтвердил практичное обновление городского сервиса "
            "с деталями по поездкам и записям на эту неделю. Перед выходом сегодня "
            "проверьте официальную страницу и уточните актуальные сроки."
        ),
    }
    base.update(over)
    return base


def _seed(root: Path, candidates: list[dict]) -> Path:
    state_dir = root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "candidates.json").write_text(
        json.dumps(
            {
                "pipeline_run_id": "plan-contract-test",
                "run_date_london": now_london().strftime("%Y-%m-%d"),
                "candidates": candidates,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return state_dir


def _strip_volatile(plan: dict) -> dict:
    plan = dict(plan)
    plan.pop("created_at_london", None)
    return plan


class PlanContractTest(unittest.TestCase):
    def test_outside_gm_ticket_keeps_geo_lane_when_timing_moves_future(self) -> None:
        event_day = (now_london().date() + timedelta(days=20)).isoformat()
        candidate = _candidate(
            1900,
            block="outside_gm_tickets",
            category="venues_tickets",
            title=f"Creamfields — event {event_day}",
            event={
                "is_event": True,
                "event_name": "Creamfields",
                "date_start": event_day,
                "venue": "Daresbury Estate",
                "borough": "Daresbury",
            },
            venue_scope="outside_gm",
        )

        reason = _apply_routing(candidate, [])

        self.assertEqual(reason, "")
        self.assertEqual(candidate["primary_block"], "outside_gm_tickets")

    def test_next7_is_optional_but_plans_a_real_candidate_when_present(self) -> None:
        candidate = _candidate(
            90,
            block="next_7_days",
            category="public_services",
            title="Council service hours change next Thursday",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [candidate])
            with patch("news_digest.pipeline.plan_digest._apply_routing", return_value=""), patch(
                "news_digest.pipeline.plan_digest._admission_verdict", return_value=("ok", "")
            ):
                run_plan_digest(root)
            plan = load_plan(state_dir)

        section = plan["sections"]["Что важно в ближайшие 7 дней"]
        self.assertEqual(section["planned"], 1)
        self.assertIsNone(section["expected_shortfall"])

    def test_canonical_lookup_keeps_selected_enriched_twin(self) -> None:
        selected = _candidate(
            1,
            fingerprint="same-fp",
            include=True,
            dedupe_decision="new",
            digest_selection_verdict="selected",
            evidence_text="Detailed evidence " * 80,
        )
        dropped_twin = _candidate(
            2,
            fingerprint="same-fp",
            include=False,
            dedupe_decision="drop",
            digest_selection_verdict="drop",
            evidence_text="Thin duplicate.",
        )

        resolved = candidates_by_fingerprint([selected, dropped_twin])

        self.assertIs(resolved["same-fp"], selected)

    def test_same_run_cheap_duplicate_cannot_be_resurrected_by_repeat_policy(self) -> None:
        candidate = _candidate(
            11,
            block="weekend_activities",
            category="culture_weekly",
            title="Prestwich Makers Market",
            source_url="https://pedddle.com/market/prestwich-makers-market",
            event={
                "is_event": True,
                "event_name": "Prestwich Makers Market",
                "venue": "Prestwich M25 1BR",
                "date_start": now_london().strftime("%Y-%m-%d"),
            },
            include=False,
            dedupe_decision="drop",
            cheap_dedup_drop=True,
            cheap_dedup_of="kept-jsonld-row",
            reason="Cheap pre-enrich duplicate — same URL/title kept from stronger source.",
        )
        fingerprint = fingerprint_for_candidate(candidate)
        candidate["fingerprint"] = fingerprint
        previous = {
            **candidate,
            "include": True,
            "fingerprint": fingerprint,
            "normalized_title": "prestwich makers market",
            "last_published_day_london": now_london().strftime("%Y-%m-%d"),
            "first_published_day_london": now_london().strftime("%Y-%m-%d"),
        }
        allow_calendar_repeat = RepeatVerdict(
            True,
            "calendar",
            "current_weekend_inventory_occurrence",
            previous_fingerprint=fingerprint,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [candidate])
            (state_dir / "published_facts.json").write_text(
                json.dumps(
                    {
                        "last_updated_london": now_london().isoformat(),
                        "facts": [previous],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with patch(
                "news_digest.pipeline.dedupe.visible_repeat_verdict",
                return_value=allow_calendar_repeat,
            ), patch(
                "news_digest.pipeline.dedupe._review_borderline_with_llm",
                return_value={},
            ), patch(
                "news_digest.pipeline.dedupe._review_semantic_borderline_with_llm",
                return_value={},
            ), patch.dict(
                os.environ,
                {"OPENAI_API_KEY": "", "DEEPSEEK_API_KEY": "", "GROQ_API_KEY": ""},
            ):
                dedupe_candidates(root)
            updated = json.loads(
                (state_dir / "candidates.json").read_text(encoding="utf-8")
            )["candidates"][0]
            report = json.loads(
                (state_dir / "dedupe_memory.json").read_text(encoding="utf-8")
            )

        self.assertFalse(updated["include"])
        self.assertEqual(updated["dedupe_decision"], "drop")
        self.assertTrue(report["cheap_dedup_invariant_restored"])

    def test_current_weekend_inventory_is_primary_even_if_rank_marks_reserve(self) -> None:
        candidate = _candidate(
            18,
            block="weekend_activities",
            category="culture_weekly",
            title="UK B-Boy Championships World Finals",
            source_url="https://example.test/bboy-finals",
            digest_selection_verdict="reserve",
            event={
                "is_event": True,
                "event_name": "UK B-Boy Championships World Finals",
                "venue": "Aviva Studios",
                "date_start": "2026-08-16",
            },
        )
        with patch.dict(os.environ, {"NEWS_DIGEST_FAKE_NOW": "2026-08-16T08:00:00+01:00"}):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                state_dir = _seed(root, [candidate])
                with patch("news_digest.pipeline.plan_digest._apply_routing", return_value=""), patch(
                    "news_digest.pipeline.plan_digest._admission_verdict", return_value=("ok", "")
                ):
                    run_plan_digest(root)
                plan = load_plan(state_dir)

        primaries = {slot["primary_fingerprint"] for slot in plan["slots"]}
        self.assertIn(candidate["fingerprint"], primaries)

    def test_planner_allows_one_primary_or_backup_per_canonical_url(self) -> None:
        shared_url = "https://pedddle.com/market/prestwich-makers-market"
        rows = [
            _candidate(
                21 + idx,
                block="weekend_activities",
                category="culture_weekly",
                fingerprint=f"prestwich-variant-{idx}",
                title="Prestwich Makers Market",
                source_url=shared_url,
                event={
                    "is_event": True,
                    "event_name": "Prestwich Makers Market",
                    "venue": venue,
                    "date_start": "2026-08-09",
                },
            )
            for idx, venue in enumerate(("Prestwich M25 1BR", "Outside Longfield Centre"))
        ]
        with patch.dict(os.environ, {"NEWS_DIGEST_FAKE_NOW": "2026-08-09T08:00:00+01:00"}):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                state_dir = _seed(root, rows)
                with patch(
                    "news_digest.pipeline.plan_digest._apply_routing",
                    return_value="",
                ), patch(
                    "news_digest.pipeline.plan_digest._admission_verdict",
                    return_value=("ok", ""),
                ):
                    run_plan_digest(root)
                plan = load_plan(state_dir)
                stored = json.loads(
                    (state_dir / "candidates.json").read_text(encoding="utf-8")
                )["candidates"]

        by_fp = {row["fingerprint"]: row for row in stored}
        refs = [slot["primary_fingerprint"] for slot in plan["slots"]]
        refs.extend(
            fp
            for slot in plan["slots"]
            for fp in slot.get("backup_fingerprints") or []
        )
        matching = [
            fp
            for fp in refs
            if canonical_url_identity(by_fp[fp]["source_url"])
            == canonical_url_identity(shared_url)
        ]
        self.assertEqual(len(matching), 1)
        self.assertEqual(plan["totals"]["canonical_url_duplicates_demoted"], 1)

    def test_cap_demoted_fresh_candidates_become_slot_backups(self) -> None:
        candidates = [_candidate(i) for i in range(16)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            result = run_plan_digest(root)
            plan = load_plan(state_dir)
            report = json.loads((state_dir / "plan_digest_report.json").read_text(encoding="utf-8"))

        self.assertTrue(result.ok)
        fresh_slots = [slot for slot in plan["slots"] if slot["block"] == "last_24h"]
        self.assertTrue(any(slot.get("backup_fingerprints") for slot in fresh_slots))
        self.assertGreater(report["totals"]["backups_assigned"], 0)

    def test_rank_recommendation_does_not_mutate_include_but_planner_executes_it(self) -> None:
        candidate = _candidate(
            90,
            rewrite_shortlist_status="board_rejected",
            digest_selection_verdict="reserve",
        )
        self.assertTrue(candidate["include"])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [candidate])
            run_plan_digest(root)
            plan = load_plan(state_dir)
        self.assertTrue(candidate["include"])
        self.assertTrue(
            any(
                row["fingerprint"] == candidate["fingerprint"]
                and row["reason"] == "rank_recommendation:board_rejected"
                for row in plan["out_sample"]
            )
        )

    def test_rank_cost_guard_does_not_mutate_include_but_planner_executes_it(self) -> None:
        candidate = _candidate(
            91,
            rewrite_shortlist_status="held_cost_after_quality",
            digest_selection_verdict="needs_enrichment",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [candidate])
            run_plan_digest(root)
            plan = load_plan(state_dir)
        self.assertTrue(candidate["include"])
        self.assertTrue(
            any(
                row["fingerprint"] == candidate["fingerprint"]
                and row["reason"] == "rank_recommendation:held_cost_after_quality"
                for row in plan["out_sample"]
            )
        )

    def test_planner_is_final_owner_of_generic_cross_day_repeat(self) -> None:
        candidate = _candidate(
            92,
            change_type="same_story_rehash",
            dedupe_decision="repeat_pending_planner",
            repeat_policy_previous={
                "fingerprint": "previous-source-version",
                "title": "Manchester service update 92",
                "category": "media_layer",
                "primary_block": "last_24h",
                "last_published_day_london": "2026-07-27",
                "published_count": 4,
            },
        )
        with patch.dict(os.environ, {"NEWS_DIGEST_FAKE_NOW": "2026-07-28T08:00:00"}):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                state_dir = _seed(root, [candidate])
                run_plan_digest(root)
                plan = load_plan(state_dir)
                stored = json.loads(
                    (state_dir / "candidates.json").read_text(encoding="utf-8")
                )["candidates"][0]
        self.assertFalse(any(
            slot["primary_fingerprint"] == candidate["fingerprint"]
            for slot in plan["slots"]
        ))
        self.assertFalse(stored["governing_repeat_decision"]["allow"])
        self.assertEqual(stored["governing_repeat_decision"]["owner"], "plan_digest")
        self.assertTrue(any(
            row["fingerprint"] == candidate["fingerprint"]
            and row["reason"].startswith("repeat_blocked:")
            for row in plan["out_sample"]
        ))

    def test_release_reads_planner_repeat_decision_and_never_recomputes_it(self) -> None:
        previous = {"fingerprint": "prior", "published_count": 99}
        missing = _planner_repeat_decision({"fingerprint": "current"}, previous)
        self.assertFalse(missing["allow"])
        self.assertEqual(missing["reason"], "missing_governing_repeat_decision")
        self.assertEqual(missing["owner"], "missing")

        governing = {
            "allow": True,
            "repeat_class": "calendar",
            "reason": "event_milestone_d7",
            "owner": "plan_digest",
        }
        self.assertIs(
            _planner_repeat_decision(
                {"governing_repeat_decision": governing},
                previous,
            ),
            governing,
        )

    def test_1_plan_is_deterministic_for_same_input(self) -> None:
        candidates = [_candidate(i) for i in range(8)]
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = _seed(Path(tmp), candidates)
            run_plan_digest(Path(tmp))
            first = _strip_volatile(load_plan(state_dir))
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = _seed(Path(tmp), [json.loads(json.dumps(c)) for c in candidates])
            run_plan_digest(Path(tmp))
            second = _strip_volatile(load_plan(state_dir))
        self.assertEqual(first["slots"], second["slots"])
        self.assertEqual(first["lead"], second["lead"])
        self.assertEqual(first["sections"], second["sections"])

    def test_2_lead_understudies_are_disjoint_from_public_slots(self) -> None:
        candidates = [_candidate(i) for i in range(12)]
        # два сильных резерва под дублёров (не include → ниже границы отбора)
        for i in (100, 101):
            candidates.append(
                _candidate(i, include=False, digest_selection_verdict="reserve")
            )
        rejected_reserve = _candidate(
            102,
            include=False,
            digest_selection_verdict="reserve",
            board_decision="reject",
            board_confidence=0.99,
        )
        candidates.append(rejected_reserve)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            plan = load_plan(state_dir)
        slot_fps = {s["primary_fingerprint"] for s in plan["slots"]}
        understudies = set(plan["lead"]["understudy_fingerprints"])
        self.assertTrue(understudies, "lead must have understudies when reserves exist")
        self.assertFalse(understudies & slot_fps, "дублёры lead не могут занимать публичные слоты")
        self.assertNotIn(
            rejected_reserve["fingerprint"],
            understudies,
            "board-reject не может стать дублёром главной",
        )
        self.assertNotIn(plan["lead"]["primary_fingerprint"], slot_fps)
        self.assertEqual(plan["sections"]["Главная история дня"]["planned"], 1)
        self.assertEqual(plan["sections"]["Главная история дня"]["slots"], ["lead"])
        self.assertIsNone(
            plan["sections"]["Главная история дня"]["expected_shortfall"]
        )
        self.assertEqual(plan["totals"]["lead_slots"], 1)
        self.assertEqual(
            plan["totals"]["public_slots"],
            len(plan["slots"]) + 1,
        )
        self.assertEqual(
            plan["totals"]["backups_assigned"],
            sum(len(slot["backup_fingerprints"]) for slot in plan["slots"])
            + len(plan["lead"]["understudy_fingerprints"]),
        )

    def test_3_writer_renders_only_plan_composition(self) -> None:
        candidates = [_candidate(i) for i in range(9)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            plan = load_plan(state_dir)
            result = write_digest(root)
            self.assertTrue(result.ok)
            report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))
        allowed = {s["primary_fingerprint"] for s in plan["slots"]}
        for slot in plan["slots"]:
            allowed.update(slot.get("backup_fingerprints") or [])
        allowed.add(plan["lead"]["primary_fingerprint"])
        allowed.update(plan["lead"]["understudy_fingerprints"])
        rendered = set(report["rendered_candidate_fingerprints"])
        self.assertTrue(rendered, "writer must render the plan")
        self.assertLessEqual(rendered, allowed, "видимая строка вне плана запрещена")

    def test_writer_replaces_late_same_url_collision_from_slot_backup(self) -> None:
        candidates = [_candidate(i) for i in range(18)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            plan = load_plan(state_dir)
            section_slots: dict[str, list[dict]] = {}
            for slot in plan["slots"]:
                section_slots.setdefault(slot["section"], []).append(slot)
            earlier = target = None
            for slots in section_slots.values():
                slots.sort(key=lambda row: int(row.get("position") or 0))
                for index, slot in enumerate(slots[1:], start=1):
                    if slot.get("backup_fingerprints"):
                        earlier, target = slots[index - 1], slot
                        break
                if target is not None:
                    break
            self.assertIsNotNone(earlier)
            self.assertIsNotNone(target)
            by_fp = {row["fingerprint"]: row for row in candidates}
            shared_url = by_fp[earlier["primary_fingerprint"]]["source_url"]
            by_fp[target["primary_fingerprint"]]["source_url"] = shared_url
            (state_dir / "candidates.json").write_text(
                json.dumps(
                    {
                        "pipeline_run_id": "plan-contract-test",
                        "run_date_london": now_london().strftime("%Y-%m-%d"),
                        "candidates": candidates,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            result = write_digest(root)
            execution = load_execution(state_dir)
            draft = (state_dir / "draft_digest.html").read_text(encoding="utf-8")

        self.assertTrue(result.ok)
        outcome = execution["slots"][target["slot_id"]]
        self.assertEqual(outcome["status"], "replaced")
        self.assertTrue(
            any(
                attempt.get("reason") == "duplicate_after_plan"
                for attempt in outcome.get("failed_attempts") or []
            )
        )
        self.assertEqual(draft.count(shared_url), 1)

    def test_4b_editor_does_not_delete_identical_planned_rows(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            write_digest(root)
            draft_path = state_dir / "draft_digest.html"
            lines = draft_path.read_text(encoding="utf-8").splitlines()
            duplicate = next(line for line in lines if line.startswith("• "))
            insert_at = lines.index(duplicate) + 1
            lines.insert(insert_at, duplicate)
            draft_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            edit_digest(root)
            edited = draft_path.read_text(encoding="utf-8")
        self.assertEqual(edited.count(duplicate), 2)

    def test_5_verify_blocks_missing_planned_line_and_stale_artifact(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            html = (state_dir / "draft_digest.html").read_text(encoding="utf-8")
            # вырезаем одну плановую строку из финального HTML
            lines = [ln for ln in html.splitlines()]
            victim = next(i for i, ln in enumerate(lines) if ln.startswith("• "))
            removed_line = lines.pop(victim)
            (outgoing / "current_digest.html").write_text("\n".join(lines) + "\n", encoding="utf-8")
            result = run_verify_digest_plan(root)
            report = json.loads((state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8"))
            self.assertFalse(result.ok, "HTML, не совпадающий со слотами, не должен отправляться")
            self.assertFalse(report["ship_degraded"])
            kinds = {d["kind"] for d in report["divergences"]}
            self.assertIn("planned_line_missing_from_final_html", kinds)
            self.assertIn("execution_loss", kinds)
            final_selection = json.loads((state_dir / "final_selection_report.json").read_text(encoding="utf-8"))
            self.assertEqual(final_selection["counts"]["final_html_rows"], 6)
            self.assertEqual(final_selection["counts"]["final_report_rows"], 6)
            self.assertEqual(final_selection["sections"]["Свежие новости"]["execution_loss"], 1)
            self.assertEqual(final_selection["sections"]["Что важно в ближайшие 7 дней"]["planned_shortfall"], 0)
            # технический брак: вчерашняя шапка — блокирует
            stale = "\n".join(lines).replace(
                now_london().strftime("%Y-%m-%d"), "2020-01-01", 1
            )
            (outgoing / "current_digest.html").write_text(stale + "\n", encoding="utf-8")
            result2 = run_verify_digest_plan(root)
            self.assertFalse(result2.ok, "устаревший артефакт должен блокировать отправку")
            self.assertIn("removed_line_marker", [removed_line[:1] and "removed_line_marker"])

    def test_5b_verify_is_single_truth_for_full_funnel_events_and_shortfalls(self) -> None:
        event_day = now_london().date() + timedelta(days=3)
        event = _candidate(
            50,
            block="ticket_radar",
            category="venues_tickets",
            source_label="Event Source",
            source_url="https://event.test/show",
            title="Global Star — public sale",
            draft_line=f"• Global Star выступит {event_day.strftime('%d.%m')}; площадка — HOME. Дату уточните.",
            event={
                "is_event": True,
                "event_name": "Global Star",
                "venue": "HOME",
                "date_start": event_day.isoformat(),
            },
            ticket_notability={
                "artist": "Global Star",
                "tier": "A",
                "kind": "artist",
                "confidence": 0.99,
                "signals": {},
            },
            ticket_type="on_sale_now",
            venue_scope="gm",
            curated_for_rank=True,
            digest_selection_verdict="selected",
        )
        planner_lost = _candidate(
            99,
            block="unknown_block",
            source_label="Planner Lost",
            source_url="https://lost.test/story",
            curated_for_rank=True,
            digest_selection_verdict="selected",
        )
        candidates = [_candidate(i, curated_for_rank=True, digest_selection_verdict="selected") for i in range(7)]
        candidates.extend([event, planner_lost])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            (state_dir / "collector_report.json").write_text(
                json.dumps(
                    {
                        "categories": {
                            "media_layer": {
                                "source_health": [
                                    {"name": "Planner Lost", "candidate_count": 1},
                                    {"name": "Event Source", "candidate_count": 1},
                                ]
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            run_plan_digest(root)
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            (outgoing / "current_digest.html").write_text(
                (state_dir / "draft_digest.html").read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            result = run_verify_digest_plan(root)
            report = json.loads((state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8"))

        self.assertTrue(result.ok)
        by_source = {row["name"]: row for row in report["source_funnel"]["sources"]}
        self.assertEqual(
            {key: by_source["Planner Lost"][key] for key in ("raw", "curated", "ranked", "planned", "written", "final")},
            {"raw": 1, "curated": 1, "ranked": 1, "planned": 0, "written": 0, "final": 0},
        )
        self.assertEqual(by_source["Planner Lost"]["loss_stage"], "planned")
        self.assertGreater(by_source["Event Source"]["final"], 0)
        self.assertEqual(
            report["event_completeness"]["counts"],
            {"checked": 1, "missing_date": 0, "missing_venue": 0},
        )
        self.assertEqual(
            report["shortfalls"]["Что важно в ближайшие 7 дней"]["planned_shortfall"],
            0,
        )

    def test_6_controller_skips_invalid_backup_and_uses_next(self) -> None:
        primary = _candidate(0)
        stale_backup = _candidate(
            1, include=False, digest_selection_verdict="reserve", freshness_status="stale"
        )
        rejected_backup = _candidate(
            3,
            include=False,
            digest_selection_verdict="reserve",
            board_decision="reject",
            board_confidence=0.99,
        )
        good_backup = _candidate(2, include=False, digest_selection_verdict="reserve")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [primary, stale_backup, rejected_backup, good_backup])
            run_plan_digest(root)
            plan = load_plan(state_dir)
            execution = load_execution(state_dir)
            by_fp = {
                c["fingerprint"]: c
                for c in [primary, stale_backup, rejected_backup, good_backup]
            }
            slot = plan["slots"][0] if plan["slots"] else None
            # негодный запасной вставляем в цепочку насильно — контроллер
            # обязан отклонить его при вводе и взять следующего
            target_slot = slot["slot_id"] if slot else "lead"
            chain = [
                stale_backup["fingerprint"],
                rejected_backup["fingerprint"],
                good_backup["fingerprint"],
            ]
            if slot:
                slot["backup_fingerprints"] = chain
            else:
                plan["lead"]["understudy_fingerprints"] = chain
            backup, fp = next_backup(plan, execution, target_slot, by_fp, set())
            self.assertEqual(fp, good_backup["fingerprint"], "stale запасной должен быть пропущен")
            failed = (execution["slots"].get(target_slot) or {}).get("failed_attempts") or []
            self.assertTrue(any("backup_invalid:stale" in str(a.get("reason")) for a in failed))
            self.assertTrue(
                any("backup_invalid:board_reject" in str(a.get("reason")) for a in failed)
            )

    def test_verify_enforces_exact_section_removed_absence_and_no_foreign_lines(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            original = (state_dir / "draft_digest.html").read_text(encoding="utf-8")
            lines = original.splitlines()
            victim_index = next(i for i, line in enumerate(lines) if line.startswith("• "))
            victim = lines.pop(victim_index)
            moved = "\n".join(lines + ["", "<b>Футбол</b>", victim]) + "\n"
            (outgoing / "current_digest.html").write_text(moved, encoding="utf-8")
            run_verify_digest_plan(root)
            report = json.loads((state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8"))
            self.assertIn("slot_rendered_in_wrong_section", {row["kind"] for row in report["divergences"]})

            foreign = original + '• Вне плана. <a href="https://foreign.test/item">X</a>\n'
            (outgoing / "current_digest.html").write_text(foreign, encoding="utf-8")
            run_verify_digest_plan(root)
            report = json.loads((state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8"))
            self.assertIn("line_outside_plan", {row["kind"] for row in report["divergences"]})

            execution = json.loads((state_dir / "plan_execution_report.json").read_text(encoding="utf-8"))
            visible_slot = next(row for row in execution["slots"].values() if row.get("status") == "shown")
            visible_slot["status"] = "removed"
            visible_slot["replacement_reason"] = "unrenderable_line"
            visible_slot["final_fingerprint"] = ""
            (state_dir / "plan_execution_report.json").write_text(json.dumps(execution), encoding="utf-8")
            (outgoing / "current_digest.html").write_text(original, encoding="utf-8")
            run_verify_digest_plan(root)
            report = json.loads((state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8"))
            self.assertIn("removed_line_still_visible", {row["kind"] for row in report["divergences"]})

    def test_7_a_tier_ticket_exempt_from_section_cap(self) -> None:
        # Каждый canonical A-tier event виден сверх любых лимитов.
        tickets = []
        for i in range(16):
            tickets.append(_candidate(
                i, block="ticket_radar", category="venues_tickets",
                title=f"Ordinary Artist {i} — event 2099-01-10 — public sale",
                draft_line=f"• Ordinary Artist {i} — 10 января, AO Arena.",
                event={"date_start": "2099-01-10T19:00:00+00:00", "venue": "AO Arena", "is_event": True},
                ticket_notability={"artist": f"Ordinary Artist {i}", "tier": "B", "kind": "artist", "confidence": 0.9, "signals": {}},
                ticket_type="on_sale_now",
            ))
        a_tiers = [
            _candidate(
                99 + n, block="ticket_radar", category="venues_tickets",
                title=f"Global Star {n} — event 2099-01-12 — public sale",
                draft_line=f"• Global Star {n} — 12 января, Co-op Live.",
                event={"date_start": "2099-01-12T20:00:00+00:00", "venue": "Co-op Live", "is_event": True},
                ticket_notability={"artist": f"Global Star {n}", "tier": "A", "kind": "artist", "confidence": 0.99, "signals": {}},
                ticket_type="on_sale_now",
                venue_scope=scope,
            )
            for n, scope in enumerate(("gm", "nearby", "outside_gm"))
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, tickets + a_tiers)
            run_plan_digest(root)
            plan = load_plan(state_dir)
        slot_fps = {s_["primary_fingerprint"] for s_ in plan["slots"] if s_["section"] == "Билеты / Ticket Radar"}
        for a_tier in a_tiers:  # правило 0094: любой scope — gm/nearby/outside
            self.assertIn(a_tier["fingerprint"], slot_fps, "A-tier обязан быть в слотах сверх капа")

    def test_7b_a_tier_repeat_obeys_calendar_policy(self) -> None:
        ticket = _candidate(
            700,
            block="ticket_radar",
            category="venues_tickets",
            include=False,
            validated=True,
            digest_selection_verdict="reserve",
            dedupe_decision="drop",
            reason="Без новых фактов: уже был 2026-07-14.",
            title="Global Star — event 2099-01-12 — public sale",
            event={"date_start": "2099-01-12", "venue": "Co-op Live", "is_event": True},
            ticket_notability={"artist": "Global Star", "tier": "A", "kind": "artist"},
            ticket_type="regular_upcoming",
            venue_scope="GM",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [ticket])
            (state_dir / "published_facts.json").write_text(
                json.dumps({"facts": [{"fingerprint": ticket["fingerprint"], "last_published_day_london": "2026-07-14"}]}),
                encoding="utf-8",
            )
            run_plan_digest(root)
            plan = load_plan(state_dir)
            planned_ticket = next(
                candidate
                for candidate in json.loads(
                    (state_dir / "candidates.json").read_text(encoding="utf-8")
                )["candidates"]
                if candidate["fingerprint"] == ticket["fingerprint"]
            )
        self.assertFalse(
            any(s["primary_fingerprint"] == ticket["fingerprint"] for s in plan["slots"])
        )
        self.assertTrue(
            any(
                row["fingerprint"] == ticket["fingerprint"]
                and "repeat_blocked" in row["reason"]
                for row in plan["out_sample"]
            )
        )
        self.assertEqual(planned_ticket["a_tier_policy_status"], "calendar_blocked")
        [outcome] = plan["a_tier_conservation"]["physical_event_outcomes"]
        self.assertEqual(outcome["status"], "calendar_blocked")
        self.assertEqual(plan["a_tier_conservation"]["eligible"], 0)

    def test_7ba_final_a_tier_repeat_overrides_stale_preliminary_b_tier_decision(self) -> None:
        ticket = _candidate(
            704,
            block="ticket_radar",
            category="venues_tickets",
            title="Global Star — event 2026-07-31 — public sale",
            event={
                "date_start": "2026-07-31",
                "event_name": "Global Star",
                "venue": "Co-op Live",
                "booking_url": "https://example.test/tickets/global-star",
                "is_event": True,
            },
            ticket_notability={"artist": "Global Star", "tier": "A", "kind": "artist"},
            ticket_type="regular_upcoming",
            venue_scope="GM",
            visible_repeat_verdict={
                "allow": False,
                "repeat_class": "blocked",
                "reason": "no_eligible_new_phase",
            },
            repeat_policy_previous={
                "fingerprint": "old-source-labelled-fingerprint",
                "last_published_day_london": "2026-07-27",
                "published_count": 99,
                "primary_block": "ticket_radar",
                "event": {
                    "date_start": "2026-07-31",
                    "event_name": "Global Star",
                    "venue": "Co-op Live",
                    "is_event": True,
                },
                "ticket_notability": {"artist": "Global Star", "tier": "B"},
                "ticket_type": "regular_upcoming",
            },
        )
        with patch.dict(os.environ, {"NEWS_DIGEST_FAKE_NOW": "2026-07-28T08:00:00"}):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                state_dir = _seed(root, [ticket])
                run_plan_digest(root)
                plan = load_plan(state_dir)
                stored = json.loads(
                    (state_dir / "candidates.json").read_text(encoding="utf-8")
                )["candidates"][0]
        self.assertTrue(
            any(slot["primary_fingerprint"] == ticket["fingerprint"] for slot in plan["slots"])
        )
        self.assertTrue(stored["governing_repeat_decision"]["allow"])
        self.assertEqual(stored["governing_repeat_decision"]["reason"], "event_milestone_d3")
        self.assertEqual(stored["governing_repeat_decision"]["owner"], "plan_digest")

    def test_7bb_verify_uses_planner_a_tier_ledger_not_stale_candidate_flag(self) -> None:
        today = now_london().date()
        eligible = _candidate(
            701,
            block="ticket_radar",
            category="venues_tickets",
            title="Visible Star — public sale",
            event={
                "is_event": True,
                "event_name": "Visible Star",
                "date_start": (today + timedelta(days=3)).isoformat(),
                "venue": "Co-op Live",
            },
            ticket_notability={"artist": "Visible Star", "tier": "A", "kind": "artist"},
            ticket_type="major_upcoming",
            venue_scope="GM",
        )
        blocked = _candidate(
            702,
            block="ticket_radar",
            category="venues_tickets",
            title="Blocked Star — public sale",
            event={
                "is_event": True,
                "event_name": "Blocked Star",
                "date_start": (today + timedelta(days=10)).isoformat(),
                "venue": "AO Arena",
            },
            ticket_notability={"artist": "Blocked Star", "tier": "A", "kind": "artist"},
            ticket_type="major_upcoming",
            venue_scope="GM",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [eligible, blocked])
            (state_dir / "published_facts.json").write_text(
                json.dumps(
                    {
                        "facts": [
                            {
                                "fingerprint": blocked["fingerprint"],
                                "last_published_day_london": (
                                    today - timedelta(days=1)
                                ).isoformat(),
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            run_plan_digest(root)
            candidates_payload = json.loads(
                (state_dir / "candidates.json").read_text(encoding="utf-8")
            )
            blocked_state = next(
                row
                for row in candidates_payload["candidates"]
                if row["fingerprint"] == blocked["fingerprint"]
            )
            # The old verify used this stale per-candidate flag instead of the
            # planner's physical-event ledger.
            blocked_state["a_tier_policy_status"] = "must_show"
            (state_dir / "candidates.json").write_text(
                json.dumps(candidates_payload),
                encoding="utf-8",
            )
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            (outgoing / "current_digest.html").write_text(
                (state_dir / "draft_digest.html").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            result = run_verify_digest_plan(root)
            report = json.loads(
                (state_dir / "verify_digest_plan_report.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertTrue(result.ok)
        self.assertEqual(report["a_tier_conservation"]["eligible"], 1)
        self.assertEqual(report["a_tier_conservation"]["visible"], 1)
        self.assertEqual(report["a_tier_conservation"]["calendar_blocked"], 1)
        self.assertEqual(report["a_tier_conservation"]["missing"], [])
        self.assertEqual(report["block_policy_version"], BLOCK_POLICY_VERSION)
        self.assertFalse(
            any(
                row["kind"].startswith("a_tier_missing")
                for row in report["divergences"]
            )
        )

    def test_7c_a_tier_identity_preserves_distinct_physical_events(self) -> None:
        rows = [
            _candidate(
                710 + idx,
                block="ticket_radar" if venue == "AO Arena" else "outside_gm_tickets",
                category="venues_tickets",
                title=f"Global Star — event {event_day}",
                event={"date_start": event_day, "venue": venue, "is_event": True},
                ticket_notability={"artist": "Global Star", "tier": "A", "kind": "artist"},
                ticket_type="regular_upcoming",
                venue_scope="GM" if venue == "AO Arena" else "outside",
            )
            for idx, (venue, event_day) in enumerate(
                (("AO Arena", "2099-01-12"), ("AO Arena", "2099-01-13"), ("Usher Hall", "2099-01-14"))
            )
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, rows)
            run_plan_digest(root)
            plan = load_plan(state_dir)
            planned_candidates = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))["candidates"]
        planned = [s for s in plan["slots"] if s["primary_fingerprint"] in {row["fingerprint"] for row in rows}]
        self.assertEqual(len(planned), 3)
        self.assertEqual(plan["a_tier_conservation"]["recognised"], 3)
        self.assertEqual(plan["a_tier_conservation"]["identity"]["collapsed_rows"], 0)
        self.assertFalse(any(row.get("a_tier_collapsed_into") for row in planned_candidates))

    def test_7ca_merged_a_tier_row_conserves_every_physical_date(self) -> None:
        row = _candidate(
            719,
            block="outside_gm_tickets",
            category="venues_tickets",
            title="Global Star — event 2099-01-12",
            event={
                "date_start": "2099-01-12",
                "venue": "Wembley Stadium",
                "is_event": True,
            },
            merged_event_dates=["2099-01-12", "2099-01-13"],
            ticket_notability={"artist": "Global Star", "tier": "A", "kind": "artist"},
            ticket_type="regular_upcoming",
            venue_scope="outside",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [row])
            run_plan_digest(root)
            plan = load_plan(state_dir)

        conservation = plan["a_tier_conservation"]
        self.assertEqual(conservation["recognised"], 2)
        self.assertEqual(conservation["eligible"], 2)
        self.assertEqual(conservation["planned"], 2)
        self.assertEqual(conservation["missing_from_plan"], [])

    def test_0166_outside_gm_tour_becomes_one_artist_card_listing_its_dates(self) -> None:
        # 29 A-tier artists produced 54 physical dates and 46 Outside-GM lines.
        # The dates stay in the pool; the section shows one card per tour.
        rows = [
            _candidate(
                760 + idx,
                block="outside_gm_tickets",
                category="venues_tickets",
                title=f"Touring Star — event {event_day}",
                event={"date_start": event_day, "venue": venue, "is_event": True},
                ticket_notability={"artist": "Touring Star", "tier": "A", "kind": "artist"},
                ticket_type="regular_upcoming",
                venue_scope="outside",
            )
            for idx, (venue, event_day) in enumerate(
                (("Wembley Stadium", "2099-01-12"), ("Wembley Stadium", "2099-01-13"), ("SEC Armadillo", "2099-01-20"))
            )
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, rows)
            run_plan_digest(root)
            plan = load_plan(state_dir)
            planned_candidates = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))["candidates"]
        fingerprints = {row["fingerprint"] for row in rows}
        planned = [s for s in plan["slots"] if s["primary_fingerprint"] in fingerprints]
        self.assertEqual(len(planned), 1)
        survivor = next(row for row in planned_candidates if row["fingerprint"] == planned[0]["primary_fingerprint"])
        self.assertEqual(
            [stop["venue"] for stop in survivor["a_tier_tour_stops"]],
            ["Wembley Stadium", "Wembley Stadium", "SEC Armadillo"],
        )
        self.assertIn("12 и 13 января, Wembley Stadium; 20 января, SEC Armadillo", survivor["draft_line"])
        # every physical date is still a record, only its own slot is gone
        collapsed = [row for row in planned_candidates if row["fingerprint"] in fingerprints]
        self.assertEqual(len(collapsed), 3)

    def test_7d_a_tier_identity_collapses_only_same_owner_venue_and_date(self) -> None:
        rows = [
            _candidate(
                740 + idx,
                block="ticket_radar",
                category="venues_tickets",
                title="Global Star — AO Arena — 12 January",
                source_label=f"Ticket Source {idx}",
                event={"date_start": "2099-01-12", "venue": "AO Arena", "is_event": True},
                ticket_notability={"artist": "Global Star", "tier": "A", "kind": "artist"},
                ticket_type="regular_upcoming",
                venue_scope="GM",
            )
            for idx in range(2)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, rows)
            run_plan_digest(root)
            plan = load_plan(state_dir)
        planned = [s for s in plan["slots"] if s["primary_fingerprint"] in {row["fingerprint"] for row in rows}]
        self.assertEqual(len(planned), 1)
        self.assertEqual(plan["a_tier_conservation"]["identity"]["collapsed_rows"], 1)

    def test_8_verify_is_fail_closed_on_missing_or_broken_execution(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            html = (state_dir / "draft_digest.html").read_text(encoding="utf-8")
            (outgoing / "current_digest.html").write_text(html, encoding="utf-8")
            exec_path = state_dir / "plan_execution_report.json"
            exec_payload = exec_path.read_text(encoding="utf-8")
            # нет отчёта исполнения -> блок
            exec_path.unlink()
            self.assertFalse(run_verify_digest_plan(root).ok, "без execution report сверка обязана блокировать")
            exec_path.write_text(exec_payload, encoding="utf-8")
            # незавершённый статус слота -> блок
            broken = json.loads(exec_payload)
            first_key = next(iter(broken["slots"]))
            broken["slots"][first_key]["status"] = "pending"
            exec_path.write_text(json.dumps(broken, ensure_ascii=False), encoding="utf-8")
            self.assertFalse(run_verify_digest_plan(root).ok, "pending-статус = конвейер не дошёл до конца")
            exec_path.write_text(exec_payload, encoding="utf-8")
            # битый Telegram-HTML (потерян закрывающий тег ссылки) -> блок
            (outgoing / "current_digest.html").write_text(html.replace("</a>", "", 1), encoding="utf-8")
            self.assertFalse(run_verify_digest_plan(root).ok, "битые теги = технический брак артефакта")

    def test_9_plan_promotes_backups_when_pool_below_minimum(self) -> None:
        # «Свежие новости»: min=6; lead и два дублёра честно съедают три
        # истории — оставшиеся слоты планёрка добирает из резервов.
        candidates = [_candidate(i) for i in range(4)]
        for i in range(200, 206):
            candidates.append(_candidate(i, include=False, digest_selection_verdict="reserve"))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            plan = load_plan(state_dir)
        fresh = plan["sections"]["Свежие новости"]
        self.assertGreaterEqual(fresh["planned"], fresh["min"], "недобор при живом резерве недопустим")
        self.assertIsNone(fresh["expected_shortfall"])

    def test_10_transport_fallback_is_planned_only_after_complete_fresh_tfgm_scan(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            (state_dir / "collector_report.json").write_text(
                json.dumps(
                    {
                        "run_date_london": now_london().strftime("%Y-%m-%d"),
                        "run_at_london": now_london().isoformat(),
                        "categories": {
                            "transport": {
                                "checked": True,
                                "usable_for_release": True,
                                "source_health": [
                                    {"name": "TfGM", "fetched": True, "not_modified": False, "errors": []},
                                    {"name": "National Rail Enquiries", "fetched": True, "not_modified": False, "errors": []},
                                ],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            run_plan_digest(root)
            plan = load_plan(state_dir)
            planned_candidates = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))["candidates"]
        transport_slots = [row for row in plan["slots"] if row["section"] == "Общественный транспорт сегодня"]
        self.assertEqual(len(transport_slots), 1)
        fallback = next(row for row in planned_candidates if row.get("transport_status_fallback"))
        self.assertEqual(transport_slots[0]["primary_fingerprint"], fallback["fingerprint"])

    def test_11_transport_fallback_never_masks_incomplete_scan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [_candidate(i) for i in range(7)])
            (state_dir / "collector_report.json").write_text(
                json.dumps(
                    {
                        "run_date_london": now_london().strftime("%Y-%m-%d"),
                        "categories": {
                            "transport": {
                                "checked": True,
                                "usable_for_release": False,
                                "source_health": [{"name": "TfGM", "fetched": False, "errors": ["timeout"]}],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            run_plan_digest(root)
            plan = load_plan(state_dir)
            planned_candidates = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))["candidates"]
        self.assertFalse(any(row["section"] == "Общественный транспорт сегодня" for row in plan["slots"]))
        self.assertFalse(any(row.get("transport_status_fallback") for row in planned_candidates))

    def test_12_transport_fallback_does_not_replace_concrete_restriction(self) -> None:
        concrete = _candidate(
            90,
            block="transport",
            category="transport",
            title="TfGM confirms Airport line delays today",
            summary="TfGM confirms delays on the Airport line affecting passengers today.",
            source_label="TfGM",
            source_url="https://tfgm.com/travel-updates/airport-line-delays",
            draft_line="• Metrolink: на линии Airport сегодня задержки; перед поездкой проверьте маршрут.",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [_candidate(i) for i in range(7)] + [concrete])
            (state_dir / "collector_report.json").write_text(
                json.dumps(
                    {
                        "run_date_london": now_london().strftime("%Y-%m-%d"),
                        "categories": {"transport": {"checked": True, "usable_for_release": True, "source_health": [{"name": "TfGM", "fetched": True, "errors": []}]}},
                    }
                ),
                encoding="utf-8",
            )
            run_plan_digest(root)
            plan = load_plan(state_dir)
            planned_candidates = json.loads((state_dir / "candidates.json").read_text(encoding="utf-8"))["candidates"]
        transport_slots = [row for row in plan["slots"] if row["section"] == "Общественный транспорт сегодня"]
        self.assertEqual([row["primary_fingerprint"] for row in transport_slots], [concrete["fingerprint"]])
        self.assertFalse(any(row.get("transport_status_fallback") for row in planned_candidates))

    def test_13_food_uses_two_live_sources_when_second_source_is_eligible(self) -> None:
        candidates = [
            _candidate(
                i,
                block="openings",
                category="food_openings",
                source_label="Food Source A" if i < 4 else "Food Source B",
                title=f"Manchester restaurant opening {i}",
                summary=f"A real Manchester restaurant opening {i} with confirmed venue details.",
            )
            for i in range(5)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            plan = load_plan(state_dir)
        food_slots = [slot for slot in plan["slots"] if slot["block"] == "openings"]
        self.assertEqual(len(food_slots), 3)
        self.assertEqual(
            {slot["source_label"] for slot in food_slots},
            {"Food Source A", "Food Source B"},
        )

    def test_14_food_keeps_three_slots_when_earlier_sections_fill_issue_budget(self) -> None:
        rows: list[dict] = []
        index = 2000
        for block, count, category in (
            ("last_24h", 9, "media_layer"),
            ("today_focus", 5, "public_services"),
            ("football", 3, "football"),
            ("weekend_activities", 10, "culture_weekly"),
            ("city_watch", 12, "city_news"),
            ("next_7_days", 6, "culture_weekly"),
            ("openings", 3, "food_openings"),
        ):
            for offset in range(count):
                rows.append(
                    _candidate(
                        index,
                        block=block,
                        category=category,
                        source_label=f"{block}-source-{offset % 3}",
                        title=f"Unique {block} story {offset}",
                    )
                )
                index += 1
        rows[0]["is_lead"] = True
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, rows)
            with patch("news_digest.pipeline.plan_digest._apply_routing", return_value=""), patch(
                "news_digest.pipeline.plan_digest._admission_verdict", return_value=("ok", "")
            ):
                run_plan_digest(root)
            plan = load_plan(state_dir)
        food = plan["sections"]["Еда, открытия и рынки"]
        self.assertEqual(food["planned"], 3)
        self.assertIsNone(food["expected_shortfall"])

    def test_15_verify_rejects_non_coded_removal_reason(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            draft = (state_dir / "draft_digest.html").read_text(encoding="utf-8")
            execution = load_execution(state_dir)
            slot = next(row for row in execution["slots"].values() if row.get("status") == "shown")
            candidate = next(row for row in candidates if row["fingerprint"] == slot["final_fingerprint"])
            (outgoing / "current_digest.html").write_text(
                "\n".join(line for line in draft.splitlines() if candidate["source_url"] not in line) + "\n",
                encoding="utf-8",
            )
            slot["status"] = "removed"
            slot["replacement_reason"] = "arbitrary_free_text_reason"
            slot["final_fingerprint"] = ""
            save_execution(state_dir, execution)
            result = run_verify_digest_plan(root)
            report = json.loads((state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8"))
        self.assertFalse(result.ok)
        self.assertTrue(any("invalid coded reason" in error for error in report["technical_errors"]))

    def test_16_verify_reports_unresolved_quality_but_never_blocks_delivery(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            (outgoing / "current_digest.html").write_text(
                (state_dir / "draft_digest.html").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (state_dir / "pre_send_quality_report.json").write_text(
                json.dumps(
                    {
                        "repair_executor": {
                            "unresolved": 1,
                            "blocking_unresolved": 1,
                            "operations": [{"outcome": "unresolved", "known_factual_error": True}],
                        }
                    }
                ),
                encoding="utf-8",
            )
            result = run_verify_digest_plan(root)
            report = json.loads((state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8"))

        self.assertTrue(result.ok)
        self.assertTrue(report["ship_degraded"])
        self.assertEqual(report["technical_errors"], [])
        self.assertIn("unresolved_known_factual", {row["kind"] for row in report["divergences"]})

    def test_17_verify_reports_planned_url_duplicate_but_never_blocks_delivery(self) -> None:
        candidates = [_candidate(i) for i in range(7)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            write_digest(root)
            outgoing = root / "data" / "outgoing"
            outgoing.mkdir(parents=True, exist_ok=True)
            html = (state_dir / "draft_digest.html").read_text(encoding="utf-8")
            (outgoing / "current_digest.html").write_text(html, encoding="utf-8")
            final_selection = build_final_execution_report(state_dir, html, write=False)
            final_selection["divergences"] = [
                {
                    "kind": "html_line_duplicated",
                    "url_identity": "pedddle.com/market/prestwich-makers-market",
                    "sections": ["Выходные в GM", "Выходные в GM"],
                }
            ]
            with patch(
                "news_digest.pipeline.verify_digest_plan.build_final_execution_report",
                return_value=final_selection,
            ):
                result = run_verify_digest_plan(root)
            report = json.loads(
                (state_dir / "verify_digest_plan_report.json").read_text(encoding="utf-8")
            )

        self.assertTrue(result.ok)
        self.assertTrue(report["ship_degraded"])
        self.assertEqual(report["technical_errors"], [])
        self.assertIn(
            "html_line_duplicated",
            {row["kind"] for row in report["divergences"]},
        )


if __name__ == "__main__":
    unittest.main()
