from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest

from news_digest.pipeline.block_policy import (
    BLOCK_POLICY_VERSION,
    block_policy,
    required_block_headings,
)
from news_digest.pipeline.collector.routing import (
    _promote_to_today_focus,
    _today_focus_native_fit,
    route_future_practical_change,
)
from news_digest.pipeline.common import now_london
from news_digest.pipeline.dedupe import close_pending_dedupe_reasons
from news_digest.pipeline.editorial_contracts import calendar_repeat_review
from news_digest.pipeline.event_quality import event_quality_report
from news_digest.pipeline.fact_completeness import translation_completeness_review
from news_digest.pipeline.llm_rewrite import _apply_rewrite_shortlist
from news_digest.pipeline.plan_digest import _backup_eligible, _backup_render_path
from news_digest.pipeline.plan_execution import (
    JUDGE_REPAIR_BUDGET_RESERVE,
    SHARED_REPAIR_BUDGET_PER_RUN,
    consume_repair_attempt,
)
from news_digest.pipeline.pre_send_quality_judge import _drop_out_of_contract_geo_rows
from news_digest.pipeline.transport_card import transport_end_datetime
from news_digest.pipeline.transport_fill import _collapse_transport_segment_duplicates, _expire_finished_transport


class Release20260727FixesTest(unittest.TestCase):
    def test_required_blocks_follow_registry_schedule(self) -> None:
        self.assertNotIn("Выходные в GM", required_block_headings(2))
        self.assertIn("Выходные в GM", required_block_headings(3))

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

    def test_judge_cannot_strip_wigan_transport_as_non_gm(self) -> None:
        digest_html = "\n".join(
            [
                "<b>Общественный транспорт сегодня</b>",
                '• Поезда Liverpool–Wigan отменены до вечера. <a href="https://example.test/rail">National Rail</a>',
                '• В Liverpool закрыта городская улица. <a href="https://example.test/road">Council</a>',
            ]
        )
        rows = [
            {"line_index": 1, "action": "strip", "risk": "geo", "reason": "Wigan is not in Greater Manchester"},
            {"line_index": 2, "action": "strip", "risk": "geo", "reason": "Liverpool is not in Greater Manchester"},
        ]

        kept, rejected = _drop_out_of_contract_geo_rows(rows, digest_html)

        self.assertEqual([row["line_index"] for row in kept], [2])
        self.assertEqual([row["line_index"] for row in rejected], [1])
        self.assertTrue(rejected[0]["gm_anchor_present"])

    def test_judge_cannot_strip_gm_club_away_match(self) -> None:
        digest_html = "\n".join(
            [
                "<b>Футбол</b>",
                '• Manchester City сыграл матч в Сеуле. <a href="https://example.test/city">Club</a>',
            ]
        )
        rows = [
            {"line_index": 1, "action": "replace", "risk": "geo", "reason": "The match took place outside Greater Manchester"},
        ]

        kept, rejected = _drop_out_of_contract_geo_rows(rows, digest_html)

        self.assertEqual(kept, [])
        self.assertEqual([row["line_index"] for row in rejected], [1])
        self.assertEqual(block_policy("football")["geo_scope"], "gm_subject")

    def test_relative_transport_deadline_stays_on_publication_day(self) -> None:
        london_tz = now_london().tzinfo
        now = datetime(2026, 8, 10, 8, 0, tzinfo=london_tz)
        candidate = {
            "published_at": "2026-08-08T16:25:00+01:00",
            "summary": "Disruption between Altrincham and Stockport until 18:00 tonight.",
        }

        end = transport_end_datetime(candidate, now=now)

        self.assertIsNotNone(end)
        self.assertEqual(end.isoformat(), "2026-08-08T18:00:00+01:00")
        self.assertLess(end, now)

    def test_one_registry_controls_rewrite_minimum_and_russian_geography(self) -> None:
        self.assertEqual(block_policy("tech_business")["min"], 0)
        self.assertEqual(block_policy("russian_events")["geo_scope"], "uk")
        self.assertEqual(block_policy("tech_business")["min"], 0)
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

    def test_ticket_sale_repeat_is_an_announcement_and_start_milestone_not_a_window(self) -> None:
        today = now_london().date()
        event_day = today + timedelta(days=20)
        sale_day = today + timedelta(days=3)
        base = {
            "primary_block": "ticket_radar",
            "category": "venues_tickets",
            "title": f"Example Artist — public sale {sale_day.isoformat()}",
            "ticket_notability": {"tier": "A", "artist": "Example Artist"},
            "event": {
                "is_event": True,
                "event_name": "Example Artist",
                "venue": "Co-op Live",
                "date_start": event_day.isoformat(),
            },
        }
        previous_without_sale = {
            **base,
            "title": "Example Artist — tickets announced",
            "last_published_day_london": (today - timedelta(days=1)).isoformat(),
        }
        announced = calendar_repeat_review(base, previous_without_sale)
        self.assertTrue(announced["allow"], announced)
        self.assertEqual(announced["reason"], "ticket_sale_date_announced_or_changed")

        previous_same_sale = {
            **base,
            "last_published_day_london": (today - timedelta(days=1)).isoformat(),
        }
        before_sale = calendar_repeat_review(base, previous_same_sale)
        self.assertFalse(before_sale["allow"], before_sale)

        starts_today = {
            **base,
            "title": f"Example Artist — public sale {today.isoformat()}",
        }
        previous_starts_today = {
            **previous_same_sale,
            "title": starts_today["title"],
        }
        started = calendar_repeat_review(starts_today, previous_starts_today)
        self.assertTrue(started["allow"], started)
        self.assertEqual(started["reason"], "ticket_sale_started_today")

        started_yesterday = {
            **base,
            "title": (
                "Example Artist — public sale "
                f"{(today - timedelta(days=1)).isoformat()}"
            ),
        }
        previous_started_yesterday = {
            **previous_same_sale,
            "title": started_yesterday["title"],
        }
        after_sale = calendar_repeat_review(
            started_yesterday,
            previous_started_yesterday,
        )
        self.assertFalse(after_sale["allow"], after_sale)

        newly_enriched_old_sale = {
            **previous_started_yesterday,
            "event": {
                **previous_started_yesterday["event"],
                "sale_start": (today - timedelta(days=30)).isoformat(),
            },
        }
        previous_without_sale = {
            **newly_enriched_old_sale,
            "event": {
                key: value
                for key, value in newly_enriched_old_sale["event"].items()
                if key != "sale_start"
            },
        }
        old_sale_backfill = calendar_repeat_review(
            newly_enriched_old_sale,
            previous_without_sale,
        )
        self.assertFalse(old_sale_backfill["allow"], old_sale_backfill)

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

    def test_today_native_fit_uses_article_evidence_only_for_affected_people(self) -> None:
        truncated_motorway_story = {
            "title": "Greater Manchester motorway closures this week on the M60 and M62",
            "summary": "Several overnight closures are planned from Monday to Thursday.",
            "evidence_text": "National Highways says drivers should be aware of the disruptions.",
        }
        foreign_sidebar_without_story_action = {
            "title": "Manchester museum unveils a new Peterloo exhibit",
            "summary": "The exhibition opens next month.",
            "evidence_text": "External links: M60 closed in Salford; drivers face delays.",
        }
        closed_historically_without_current_audience = {
            "title": "Manchester Town Hall restoration is months from completion",
            "summary": "The Town Hall has been closed since 2018. Local Democracy Reporter.",
        }

        self.assertEqual(
            _today_focus_native_fit(truncated_motorway_story),
            (True, "restriction"),
        )
        self.assertEqual(_today_focus_native_fit(foreign_sidebar_without_story_action)[0], False)
        self.assertEqual(_today_focus_native_fit(closed_historically_without_current_audience)[0], False)

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

    def test_next7_producer_resolves_future_weekday_from_publication_day(self) -> None:
        today = now_london().date()
        change_day = today + timedelta(days=4)
        weekday = (
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        )[change_day.weekday()]
        practical = {
            "include": True,
            "category": "public_services",
            "primary_block": "city_watch",
            "published_at": f"{today.isoformat()}T07:00:00+01:00",
            "title": f"Salford library closes from {weekday}",
            "summary": "Residents must use another branch while works start.",
        }

        self.assertTrue(route_future_practical_change(practical))
        self.assertEqual(
            practical["next_7_effective_date"],
            change_day.isoformat(),
        )

    def test_calendar_repeat_overrides_only_provisional_why_now_reject(self) -> None:
        candidate = {
            "validated": True,
            "digest_selection_verdict": "reserve",
            "primary_block": "professional_events",
            "category": "professional_events",
            "title": "Rochdale Business Growth Hub Drop-In",
            "summary": "Free business advice at Fire Up Co-Working.",
            "source_url": "https://example.test/rochdale",
            "reject_reasons": ["why_now_stale"],
            "governing_repeat_decision": {
                "allow": True,
                "repeat_class": "calendar",
                "reason": "event_milestone_d1",
            },
            "professional_llm_match": {"fit": "consider"},
        }

        allowed, _ = _backup_eligible(candidate)
        self.assertTrue(allowed)

        candidate["reject_reasons"].append("event_missing_date")
        allowed, reason = _backup_eligible(candidate)
        self.assertFalse(allowed)
        self.assertEqual(reason, "rejected")

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

    def test_wigan_liverpool_media_rewrite_collapses_into_official_status(self) -> None:
        official = {
            "fingerprint": "national-rail-wigan",
            "primary_block": "transport",
            "category": "transport",
            "include": True,
            "title": "Northern: Major disruption between Liverpool Lime Street and Wigan North Western expected until 12:30",
            "summary": "The line is blocked after an emergency incident and trains are disrupted.",
        }
        media = {
            "fingerprint": "men-wigan",
            "primary_block": "transport",
            "category": "media_layer",
            "include": True,
            "title": "Major disruption as Greater Manchester train line blocked",
            "summary": "The line between Wigan and Liverpool is blocked, causing serious disruption until 12:30.",
        }
        different_wigan_route = {
            "fingerprint": "wigan-bolton",
            "primary_block": "transport",
            "category": "media_layer",
            "include": True,
            "title": "Delays between Wigan and Bolton",
            "summary": "A separate signalling fault is delaying trains.",
        }

        dropped = _collapse_transport_segment_duplicates(
            [official, media, different_wigan_route]
        )

        self.assertEqual([row["fingerprint"] for row in dropped], ["men-wigan"])
        self.assertFalse(media["include"])
        self.assertTrue(different_wigan_route["include"])

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

    # 0191 — затронутый объект называет аудиторию, но только у длящегося действия.
    def test_named_place_counts_as_audience_only_while_action_lasts(self) -> None:
        cordon = {
            "title": "Cheetham Hill stabbing sees air ambulance called to street and cordon put in place",
            "summary": "Residential streets have been taped off.",
        }
        historic = {
            "title": "Manchester Town Hall's £500m restoration is months away from completion",
            "summary": "The Town Hall has been closed since 2018.",
        }

        self.assertEqual(_today_focus_native_fit(cordon), (True, "restriction"))
        self.assertEqual(_today_focus_native_fit(historic)[0], False)

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
