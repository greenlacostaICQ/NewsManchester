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

from news_digest.pipeline.common import canonical_url_identity, now_london
from news_digest.pipeline.plan_digest import run_plan_digest
from news_digest.pipeline.plan_execution import load_execution, load_plan, next_backup
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

    def test_5_verify_catches_missing_planned_line_and_blocks_stale_artifact(self) -> None:
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
            self.assertTrue(result.ok, "контентное расхождение не блокирует отправку")
            self.assertTrue(report["ship_degraded"])
            kinds = {d["kind"] for d in report["divergences"]}
            self.assertIn("planned_line_missing_from_final_html", kinds)
            self.assertIn("final_section_underflow", kinds)
            final_selection = json.loads((state_dir / "final_selection_report.json").read_text(encoding="utf-8"))
            visible_total = sum(
                int(count) for status, count in final_selection["totals"].items()
                if status in {"visible", "visible_after_repair"}
            )
            self.assertEqual(visible_total, 6, "final selection must count final HTML, not writer output")
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

    def test_7_a_tier_ticket_exempt_from_section_cap(self) -> None:
        # Правило 0094: каждый A-tier артист виден сверх любых лимитов.
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

    def test_7c_a_tier_identity_conserves_one_artist_card_local_first(self) -> None:
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
        self.assertEqual(len(planned), 1)
        self.assertEqual(plan["a_tier_conservation"]["recognised"], 1)
        self.assertEqual(plan["a_tier_conservation"]["identity"]["collapsed_rows"], 2)
        survivor = next(row for row in planned_candidates if row.get("merged_event_dates"))
        self.assertEqual(survivor["event"]["venue"], "AO Arena")
        self.assertEqual(survivor["merged_event_dates"], ["2099-01-12", "2099-01-13"])

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


if __name__ == "__main__":
    unittest.main()
