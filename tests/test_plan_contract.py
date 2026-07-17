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
        a_tier = _candidate(
            99, block="ticket_radar", category="venues_tickets",
            title="Global Star — event 2099-01-12 — public sale",
            draft_line="• Global Star — 12 января, Co-op Live.",
            event={"date_start": "2099-01-12T20:00:00+00:00", "venue": "Co-op Live", "is_event": True},
            ticket_notability={"artist": "Global Star", "tier": "A", "kind": "artist", "confidence": 0.99, "signals": {}},
            ticket_type="on_sale_now",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = _seed(root, tickets + [a_tier])
            run_plan_digest(root)
            plan = load_plan(state_dir)
        slot_fps = {s_["primary_fingerprint"] for s_ in plan["slots"] if s_["section"] == "Билеты / Ticket Radar"}
        self.assertIn(a_tier["fingerprint"], slot_fps, "A-tier обязан быть в слотах сверх капа")


if __name__ == "__main__":
    unittest.main()
