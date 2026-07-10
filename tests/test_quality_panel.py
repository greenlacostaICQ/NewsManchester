"""Quality panel — 5 indicators per sent issue.

Fixtures are verbatim fragments of REAL sent digests from git history:
the 2026-06-25 issue shipped the same Leigh sinkhole story twice (one URL,
two rewrites), the 2026-06-28 issue shipped a literal «следите за
обновлениями», and the 2026-07-07 issue shipped an empty «Главная история
дня». The panel must catch all three; the clean fragment mirrors 2026-07-10.
"""

import json
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline.quality_panel import (
    append_panel_row,
    build_panel_row,
    weekly_panel_summary,
)

HEADER = "<b>Greater Manchester Brief — 2026-07-10, 08:22</b>\n\n"

CLEAN_ISSUE = HEADER + """<b>Главная история дня</b>
<b>В Фейлсворте начато расследование убийства.</b> Четыре человека освобождены под залог. <a href="https://bbc.com/news/articles/c5yzl23pmn6o">BBC Manchester</a>

<b>Свежие новости</b>
• Manchester council confirms service change. <a href="https://example.test/city-1">Manchester Council</a>
• Stockport transport works confirmed. <a href="https://example.test/city-2">BBC Manchester</a>

<b>Билеты / Ticket Radar</b>
• <b>Sex Pistols</b> — 12 июля, Castlefield Bowl (Рок). <a href="https://ticketmaster.co.uk/sex">Ticketmaster</a>
"""

BAD_ISSUE = HEADER + """<b>Главная история дня</b>

<b>Свежие новости</b>
• В Ли, графство Уиган, закрыли главную дорогу у школы после появления провала в асфальте. <a href="https://manchestereveningnews.co.uk/news/greater-manchester-news/major-leigh-route-near-high-34185313">MEN</a>
• В Ли, на улице Уэстли, закрыто движение после появления провала, связанного с оборудованием United Utilities. <a href="https://manchestereveningnews.co.uk/news/greater-manchester-news/major-leigh-route-near-high-34185313">MEN</a>
• Мужчина скончался после задержания полицией; следите за обновлениями. <a href="https://example.test/jet2">MEN</a>
•

<b>Билеты / Ticket Radar</b>
• <b>Sex Pistols</b> — 12 июля. <a href="https://ticketmaster.co.uk/sex">Ticketmaster</a>
• <b>Breaking Benjamin</b> — 16 июля. <a href="https://ticketmaster.co.uk/bb">Ticketmaster</a>

<b>Крупные концерты вне GM</b>
• <b>Pitbull</b> — 25 июля, Лондон. <a href="https://ticketmaster.co.uk/pit">Ticketmaster</a>
• <b>Kesha</b> — 26 июля, Лондон. <a href="https://ticketmaster.co.uk/kesha">Ticketmaster</a>
"""


class QualityPanelTest(unittest.TestCase):
    def test_clean_issue_is_ok(self) -> None:
        row = build_panel_row(CLEAN_ISSUE, "2026-07-10")
        self.assertTrue(row["ok"])
        self.assertTrue(row["lead_present"])
        self.assertEqual(row["repeat_lines"], 0)
        self.assertEqual(row["placeholder_lines"], 0)
        self.assertEqual(row["empty_sections"], [])

    def test_bad_issue_trips_all_five_indicators(self) -> None:
        row = build_panel_row(BAD_ISSUE, "2026-06-25")
        self.assertFalse(row["ok"])
        self.assertFalse(row["lead_present"])          # пустая «Главная история дня» (07-07)
        self.assertIn("Главная история дня", row["empty_sections"])
        self.assertEqual(row["empty_lines"], 1)          # голый «•»
        self.assertEqual(row["repeat_lines"], 1)         # провал в Ли дважды (06-25)
        self.assertEqual(row["placeholder_lines"], 1)    # «следите за обновлениями» (06-28)
        self.assertTrue(row["tickets_dominate"])         # 4 билетных против 3 новостных

    def test_history_appends_one_row_per_day_and_weekly_summary_renders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            append_panel_row(state_dir, build_panel_row(BAD_ISSUE, "2026-07-09"))
            append_panel_row(state_dir, build_panel_row(CLEAN_ISSUE, "2026-07-10"))
            append_panel_row(state_dir, build_panel_row(CLEAN_ISSUE, "2026-07-10"))  # same day → replace
            days = json.loads((state_dir / "quality_panel_history.json").read_text(encoding="utf-8"))["days"]
            self.assertEqual([d["date"] for d in days], ["2026-07-09", "2026-07-10"])
            summary = weekly_panel_summary(state_dir)
            self.assertIn("1 из 2 дней без замечаний", summary)
            self.assertIn("2026-07-10", summary)


if __name__ == "__main__":
    unittest.main()
