from __future__ import annotations

import unittest

from news_digest.pipeline.collector.routing import _today_focus_native_fit
from news_digest.pipeline.common import now_london
from news_digest.pipeline.dedupe import close_pending_dedupe_reasons
from news_digest.pipeline.event_quality import event_quality_report
from news_digest.pipeline.plan_digest import _backup_render_path
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
                "<b>Дальние анонсы</b>",
                '• Liverpool: фестиваль 12 сентября. <a href="https://example.test/lp">Site</a>',
            ]
        )
        rows = [
            {"line_index": 1, "action": "strip", "risk": "geo", "reason": "не Greater Manchester"},
            {"line_index": 2, "action": "strip", "risk": "geo", "reason": "не Greater Manchester"},
        ]

        kept, rejected = _drop_out_of_contract_geo_rows(rows, digest_html)

        self.assertEqual([row["line_index"] for row in kept], [2])
        self.assertEqual([row["line_index"] for row in rejected], [1])

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


if __name__ == "__main__":
    unittest.main()
