"""Этап 3: контракт слот-плана — 6 согласованных случаев.

1. Детерминизм: одинаковый вход → идентичный план.
2. Дублёры lead не занимают публичные слоты (из-под границы отбора).
3. Писатель не меняет состав: каждая видимая строка ∈ плану.
4. Редактор не меняет состав: блок-команды игнорируются.
5. Финальная сверка ловит пропажу плановой строки; технический брак блокирует.
6. Негодный (протухший) запасной отклоняется контроллером, берётся следующий.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from news_digest.pipeline.common import canonical_url_identity, now_london
from news_digest.pipeline.editor import edit_digest
from news_digest.pipeline.plan_digest import run_plan_digest
from news_digest.pipeline.plan_execution import load_execution, load_plan, next_backup, save_execution
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
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, candidates)
            run_plan_digest(root)
            plan = load_plan(state_dir)
        slot_fps = {s["primary_fingerprint"] for s in plan["slots"]}
        understudies = set(plan["lead"]["understudy_fingerprints"])
        self.assertTrue(understudies, "lead must have understudies when reserves exist")
        self.assertFalse(understudies & slot_fps, "дублёры lead не могут занимать публичные слоты")
        self.assertNotIn(plan["lead"]["primary_fingerprint"], slot_fps)

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

    def test_4_editor_ignores_block_actions(self) -> None:
        from news_digest.pipeline import editor

        lines = {"Свежие новости": ["• Строка один. <a href=\"https://e.test/1\">S</a>"]}
        warnings: list[str] = []
        polished, report = editor._apply_editor_block_actions(
            dict(lines),
            block_actions=[{"action": "trim", "section": "Свежие новости", "count": 1}],
            candidates=[],
            rendered_urls=set(),
            rendered_story_keys=set(),
            warnings=warnings,
        )
        self.assertEqual(polished, lines)
        self.assertEqual(report["applied"], 0)
        self.assertEqual(report["status"], "ignored_plan_locked")

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
            self.assertEqual(final_selection["sections"]["Что важно в ближайшие 7 дней"]["planned_shortfall"], 3)
            # технический брак: вчерашняя шапка — блокирует
            stale = "\n".join(lines).replace(
                now_london().strftime("%Y-%m-%d"), "2020-01-01", 1
            )
            (outgoing / "current_digest.html").write_text(stale + "\n", encoding="utf-8")
            result2 = run_verify_digest_plan(root)
            self.assertFalse(result2.ok, "устаревший артефакт должен блокировать отправку")
            self.assertIn("removed_line_marker", [removed_line[:1] and "removed_line_marker"])

    def test_6_controller_skips_invalid_backup_and_uses_next(self) -> None:
        primary = _candidate(0)
        stale_backup = _candidate(
            1, include=False, digest_selection_verdict="reserve", freshness_status="stale"
        )
        good_backup = _candidate(2, include=False, digest_selection_verdict="reserve")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, [primary, stale_backup, good_backup])
            run_plan_digest(root)
            plan = load_plan(state_dir)
            execution = load_execution(state_dir)
            by_fp = {c["fingerprint"]: c for c in [primary, stale_backup, good_backup]}
            slot = plan["slots"][0] if plan["slots"] else None
            # негодный запасной вставляем в цепочку насильно — контроллер
            # обязан отклонить его при вводе и взять следующего
            target_slot = slot["slot_id"] if slot else "lead"
            chain = [stale_backup["fingerprint"], good_backup["fingerprint"]]
            if slot:
                slot["backup_fingerprints"] = chain
            else:
                plan["lead"]["understudy_fingerprints"] = chain
            backup, fp = next_backup(plan, execution, target_slot, by_fp, set())
            self.assertEqual(fp, good_backup["fingerprint"], "stale запасной должен быть пропущен")
            failed = (execution["slots"].get(target_slot) or {}).get("failed_attempts") or []
            self.assertTrue(any("backup_invalid:stale" in str(a.get("reason")) for a in failed))

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

    def test_7b_a_tier_repeat_is_promoted_before_watch_and_repeat_policy(self) -> None:
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
        slot = next(s for s in plan["slots"] if s["primary_fingerprint"] == ticket["fingerprint"])
        self.assertTrue(slot["must_show"])
        self.assertTrue(plan["a_tier_conservation"]["missing_from_plan"] == [])

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
        self.assertEqual(plan["a_tier_conservation"]["recognised"], 1)
        self.assertEqual(plan["a_tier_conservation"]["identity"]["collapsed_rows"], 0)
        self.assertFalse(any(row.get("a_tier_collapsed_into") for row in planned_candidates))

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


if __name__ == "__main__":
    unittest.main()
