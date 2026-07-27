from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
import tempfile
import unittest

from news_digest.pipeline.block_policy import BLOCK_POLICY_VERSION, block_policy
from news_digest.pipeline.collector.routing import (
    _promote_to_today_focus,
    _today_focus_native_fit,
    route_future_practical_change,
)
from news_digest.pipeline.common import SECTION_MIN_ITEMS, now_london
from news_digest.pipeline.dedupe import close_pending_dedupe_reasons
from news_digest.pipeline.editorial_contracts import calendar_repeat_review
from news_digest.pipeline.event_quality import event_quality_report
from news_digest.pipeline.fact_completeness import translation_completeness_review
from news_digest.pipeline.llm_rewrite import _apply_rewrite_shortlist
from news_digest.pipeline.plan_digest import _backup_render_path
from news_digest.pipeline.plan_execution import (
    JUDGE_REPAIR_BUDGET_RESERVE,
    SHARED_REPAIR_BUDGET_PER_RUN,
    consume_repair_attempt,
)
from news_digest.pipeline.pre_send_quality_judge import _drop_out_of_contract_geo_rows
from news_digest.pipeline.transport_fill import _collapse_transport_segment_duplicates, _expire_finished_transport


class Release20260727FixesTest(unittest.TestCase):
    # 0157 — география судьи по контракту раздела.
    def test_judge_geo_action_rejected_for_russian_uk_section(self) -> None:
        # Разметка снята с реального выпуска: заголовок раздела — <b> на
        # отдельной строке, карточки — строки с «• » и ссылкой источника.
        digest_html = "\n".join(
            [
                "<b>Русскоязычные концерты и стендап UK</b>",
                '• Лондон: русскоязычный стендап 3 августа. <a href="https://example.test/ru">Афиша</a>',
                '• Лондон: русскоязычный концерт 4 августа. <a href="https://example.test/ru2">Афиша</a>',
                "<b>Дальние анонсы</b>",
                '• Liverpool: фестиваль 12 сентября. <a href="https://example.test/lp">Site</a>',
            ]
        )
        rows = [
            {"line_index": 1, "action": "strip", "risk": "geo", "reason": "не Greater Manchester"},
            {"line_index": 2, "action": "strip", "risk": "geo", "reason": "не Greater Manchester"},
            {"line_index": 3, "action": "strip", "risk": "geo", "reason": "не Greater Manchester"},
        ]

        kept, rejected = _drop_out_of_contract_geo_rows(rows, digest_html)

        self.assertEqual([row["line_index"] for row in kept], [3])
        self.assertEqual([row["line_index"] for row in rejected], [1, 2])

    def test_one_registry_controls_rewrite_minimum_and_russian_geography(self) -> None:
        self.assertEqual(block_policy("tech_business")["min"], 0)
        self.assertEqual(block_policy("russian_events")["geo_scope"], "uk")
        self.assertNotIn("IT и бизнес", SECTION_MIN_ITEMS)
        self.assertTrue(BLOCK_POLICY_VERSION)

        rejected = [
            {
                "fingerprint": f"it-{idx}",
                "primary_block": "tech_business",
                "category": "tech_business",
                "include": True,
                "board_decision": "reject",
                "board_confidence": 0.99,
                "board_rank": idx,
            }
            for idx in range(2)
        ]
        selected, report = _apply_rewrite_shortlist(rejected, rejected)
        self.assertEqual(selected, [])
        self.assertEqual(report["board_rejects_executed"], 2)

    def test_completeness_uses_story_facts_not_titles_or_artist_names(self) -> None:
        fixtures = [
            ("Council faces a death sentence over funding", {"what_happened": "For me, it is a death sentence if the facility closes"}, "• Объект может закрыться."),
            ("Band Thy Art Is Murder announces Manchester show", {"what_happened": "The band announced a concert"}, "• Группа анонсировала концерт."),
        ]
        for title, story_facts, line in fixtures:
            candidate = {"title": title, "story_facts": story_facts}
            from news_digest.pipeline.pre_send_quality_judge import _completeness_source_blob

            review = translation_completeness_review(_completeness_source_blob(candidate), line)
            self.assertFalse(review["applies"], title)

        killed = translation_completeness_review(
            "A man was killed in Manchester",
            "• Полиция расследует убийство мужчины в Манчестере.",
        )
        self.assertEqual(killed["missing_critical"], [])

    def test_a_tier_has_no_cap_but_only_calendar_moments_repeat(self) -> None:
        today = now_london().date()
        base = {
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "ticket_notability": {"tier": "A", "artist": "Example Artist"},
            "ticket_type": "major_upcoming",
            "event": {
                "is_event": True,
                "event_name": "Example Artist",
                "venue": "Co-op Live",
                "date_start": (today + timedelta(days=10)).isoformat(),
            },
        }
        previous = {
            **base,
            "published_count": 99,
            "last_published_day_london": (today - timedelta(days=1)).isoformat(),
        }
        self.assertFalse(calendar_repeat_review(base, previous)["allow"])

        d3 = {**base, "event": {**base["event"], "date_start": (today + timedelta(days=3)).isoformat()}}
        previous_d3 = {**previous, "event": dict(d3["event"])}
        review = calendar_repeat_review(d3, previous_d3)
        self.assertTrue(review["allow"], review)
        self.assertEqual(review["reason"], "event_milestone_d3")

    def test_writer_budget_reserves_two_attempts_for_judge(self) -> None:
        execution = {"repair_attempts_used": 0, "repair_attempts_by_stage": {}}
        writer_limit = SHARED_REPAIR_BUDGET_PER_RUN - JUDGE_REPAIR_BUDGET_RESERVE
        self.assertTrue(all(consume_repair_attempt(execution, stage="writer") for _ in range(writer_limit)))
        self.assertFalse(consume_repair_attempt(execution, stage="writer"))
        self.assertTrue(consume_repair_attempt(execution, stage="judge"))

    # 0158 — «pending dedupe» не переживает стадию дедупа.
    def test_pending_dedupe_reason_is_closed(self) -> None:
        candidate = {
            "fingerprint": "food-1",
            "title": "Rudy's to open in Monton Salford",
            "primary_block": "openings",
            "include": False,
            "dedupe_decision": "drop",
            "reason": "pending dedupe",
        }

        closed = close_pending_dedupe_reasons([candidate])

        self.assertEqual(len(closed), 1)
        self.assertNotIn("pending dedupe", candidate["reason"].lower())

    # 0159 — Today наполняется по смыслу сегодняшнего действия.
    def test_today_native_fit_requires_action_place_and_people(self) -> None:
        eligible = {
            "title": "M60 traffic stopped as lanes closed in Salford",
            "summary": "Drivers are facing delays after two lanes were closed near Eccles.",
        }
        no_action = {
            "title": "Manchester museum unveils new Peterloo exhibit",
            "summary": "Residents can see the jug from next month.",
        }

        self.assertEqual(_today_focus_native_fit(eligible), (True, "restriction"))
        self.assertEqual(_today_focus_native_fit(no_action)[0], False)

    def test_today_refill_counts_only_post_validation_survivors(self) -> None:
        rejected = {
            "fingerprint": "rejected",
            "include": False,
            "primary_block": "today_focus",
            "category": "gmp",
            "title": "M60 lanes closed in Salford",
            "summary": "Drivers face delays near Eccles.",
        }
        m60 = {
            "fingerprint": "m60",
            "include": True,
            "validated": True,
            "primary_block": "last_24h",
            "category": "media_layer",
            "freshness_status": "fresh_24h",
            "title": "M60 lanes closed in Salford",
            "summary": "Drivers face delays near Eccles today.",
        }

        _promote_to_today_focus([rejected, m60])

        self.assertFalse(rejected["include"])
        self.assertEqual(m60["primary_block"], "today_focus")

    def test_next7_producer_accepts_only_dated_practical_change(self) -> None:
        change_day = (now_london().date() + timedelta(days=4)).isoformat()
        practical = {
            "include": True,
            "category": "public_services",
            "primary_block": "city_watch",
            "title": "Salford library service closes for works",
            "summary": "Residents must use another branch while works start.",
            "event": {"date_start": change_day},
        }
        leisure = {
            **practical,
            "title": "Salford music festival opens",
            "summary": "Residents can attend the festival.",
        }

        self.assertTrue(route_future_practical_change(practical))
        self.assertEqual(practical["primary_block"], "next_7_days")
        self.assertFalse(route_future_practical_change(leisure))

    # 0160 — недосуговая карточка Next7 не обязана нести цену/бронирование.
    def test_non_leisure_next7_needs_no_price_or_booking(self) -> None:
        candidate = {
            "primary_block": "next_7_days",
            "category": "council",
            "title": "Salford council consultation closes on 31 July",
            "summary": "Residents in Eccles must respond by 31 July; the council office on Chorley Road takes replies.",
            "source_url": "https://example.test/consultation",
            "source_label": "Salford Council",
        }

        report = event_quality_report(candidate)

        self.assertNotIn("price_or_free_or_booking", report["missing"])

    # 0161 — слот плана только с доказуемым путём до строки.
    def test_plan_render_path_absent_without_facts(self) -> None:
        factless = {"title": "Thin listing", "category": "culture_weekly", "primary_block": "openings"}
        writable = dict(factless, summary="Thin listing at a Salford venue.")

        self.assertEqual(_backup_render_path(factless), "")
        self.assertEqual(_backup_render_path(writable), "model_write")

    # 0162 — закончившееся окно и смысловой дубль участка.
    def test_finished_transport_and_media_duplicate_are_removed(self) -> None:
        now = now_london().replace(hour=8, minute=31, second=0, microsecond=0)
        finished = {
            "fingerprint": "tpe-1",
            "primary_block": "transport",
            "category": "transport",
            "include": True,
            "title": "TransPennine Express: Disruption between Stalybridge and Manchester Piccadilly",
            "summary": "Disruption expected until 08:00",
        }
        official = {
            "fingerprint": "metrolink-1",
            "primary_block": "transport",
            "category": "transport",
            "include": True,
            "title": "Metrolink Eccles line",
            "draft_line": "• Metrolink (до 2 августа): нет трамваев на Eccles line — ремонтные работы; замещающий автобус.",
        }
        media = {
            "fingerprint": "bbc-1",
            "primary_block": "transport",
            "category": "media_layer",
            "include": True,
            "title": "Metrolink track disruption will be 'worth the inconvenience'",
            "summary": "Engineering work on the Eccles line means no trams; replacement buses run.",
        }
        candidates = [finished, official, media]

        expired = _expire_finished_transport(candidates)
        duplicates = _collapse_transport_segment_duplicates(candidates)

        self.assertEqual([row["fingerprint"] for row in expired], ["tpe-1"])
        self.assertEqual([row["fingerprint"] for row in duplicates], ["bbc-1"])
        self.assertTrue(official["include"])
        del now

    # 0163 — пустой план не останавливает выпуск.
    def test_empty_plan_ships_degraded_instead_of_failing(self) -> None:
        from news_digest.pipeline.writer import write_digest

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_dir = root / "data" / "state"
            state_dir.mkdir(parents=True)
            (state_dir / "candidates.json").write_text(
                json.dumps({"candidates": [], "pipeline_run_id": "run-1"}), encoding="utf-8"
            )
            (state_dir / "release_plan.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "pipeline_run_id": "run-1",
                        "slots": [],
                        "lead": {"primary_fingerprint": ""},
                        "ordered_sections": [],
                        "sections": {},
                    }
                ),
                encoding="utf-8",
            )

            result = write_digest(root)
            report = json.loads((state_dir / "writer_report.json").read_text(encoding="utf-8"))
            draft_exists = (state_dir / "draft_digest.html").exists()

        self.assertTrue(result.ok)
        self.assertTrue(draft_exists)
        self.assertEqual(report["stage_status"], "complete_degraded")
        self.assertEqual(report["errors"], [])

    # 0170 — нативная Today-карточка проходит тот же шлюз.
    def test_native_today_card_without_action_is_demoted_before_counting(self) -> None:
        from news_digest.pipeline.collector.routing import (
            _demote_unfit_native_today,
            _today_focus_substantive,
        )

        native = {
            "fingerprint": "gmmh-1",
            "primary_block": "today_focus",
            "category": "public_services",
            "include": True,
            "source_label": "GMMH",
            "title": "GMMH staff celebrate award for community mental health work",
            "summary": "Trust staff in Manchester received a national award for their service.",
        }

        self.assertEqual(len(_today_focus_substantive([native])), 1)

        demoted = _demote_unfit_native_today([native])

        self.assertEqual([c["fingerprint"] for c in demoted], ["gmmh-1"])
        self.assertEqual(native["primary_block"], "city_watch")
        self.assertEqual(_today_focus_substantive([native]), [])


if __name__ == "__main__":
    unittest.main()
