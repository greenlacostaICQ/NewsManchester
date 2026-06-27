"""Wave 1 / S4: the shipped HTML is the source of truth. The reconciler measures
the draft (not writer_report), flags the writer-vs-HTML divergence, recovers
thin sections from the unified recoverable reserve, and (P0-2) enforces the
must_show contract — all before promotion.
"""
import tempfile
import unittest
from pathlib import Path

from news_digest.pipeline import release_reconcile as rr


def _draft(fresh_bullets: int, *, lead: bool = False) -> str:
    lines = ["<b>Greater Manchester Brief — 2026-06-27</b>"]
    if lead:
        lines += ["<b>Главная история дня</b>", '<b>Главная новость дня.</b> Подробности. <a href="https://lead">MEN</a>']
    lines.append("<b>Свежие новости</b>")
    for i in range(fresh_bullets):
        lines.append(f'• Существующая новость {i}. <a href="https://men/exist{i}">MEN</a>')
    return "\n".join(lines) + "\n"


def _reserve(idx: int) -> dict:
    return {
        "validated": True,
        "recoverable_reserve": True,
        "primary_block": "last_24h",
        "source_url": f"https://reserve.test/news{idx}",
        "source_label": "MEN",
        "draft_line": f'• Городской совет принял новое решение номер {idx} сегодня. <a href="https://reserve.test/news{idx}">MEN</a>',
    }


class VisibleContractTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.draft = Path(self._tmp.name) / "draft_digest.html"

    def tearDown(self):
        self._tmp.cleanup()

    def test_insert_after_last_bullet_and_noop_when_section_absent(self):
        html = "<b>Свежие новости</b>\n• Один. <a href=\"x\">MEN</a>\n<b>Футбол</b>\n• Гол.\n"
        out, added = rr.insert_bullets_after_section(html, "Свежие новости", ["• Два. <a href=\"y\">MEN</a>"])
        self.assertEqual(added, 1)
        self.assertLess(out.index("Два."), out.index("<b>Футбол</b>"))
        self.assertLess(out.index("Один."), out.index("Два."))
        out2, added2 = rr.insert_bullets_after_section(html, "Погода", ["• X"])
        self.assertEqual(added2, 0)
        self.assertEqual(out2, html)

    def test_control_assertion_flags_writer_vs_html_divergence(self):
        self.draft.write_text(_draft(1), encoding="utf-8")
        report = rr.reconcile_visible_html(
            self.draft, candidates=[], writer_section_counts={"Свежие новости": 9, "Главная история дня": 1}
        )
        self.assertFalse(report["control_assertion"]["ok"])
        diverged = {d["section"]: (d["writer"], d["html"]) for d in report["control_assertion"]["writer_vs_html_divergent_sections"]}
        self.assertEqual(diverged["Свежие новости"], (9, 1))
        self.assertFalse(report["lead_visible"])

    def test_lead_visible_for_bold_no_bullet_line(self):
        # P0-1: the lead renders bold without a bullet; it must still count as 1.
        self.draft.write_text(_draft(1, lead=True), encoding="utf-8")
        report = rr.reconcile_visible_html(self.draft, candidates=[], writer_section_counts={})
        self.assertTrue(report["lead_visible"])

    def test_bounded_recovery_fills_thin_section_from_reserve(self):
        self.draft.write_text(_draft(1), encoding="utf-8")  # Свежие новости = 1, min = 6
        report = rr.reconcile_visible_html(
            self.draft, candidates=[_reserve(i) for i in range(5)], writer_section_counts={"Свежие новости": 1}
        )
        self.assertEqual(report["html_section_counts"]["Свежие новости"], 6)
        self.assertEqual(report["inserted_total"], 5)
        self.assertEqual(self.draft.read_text(encoding="utf-8").count("новое решение номер"), 5)
        self.assertNotIn("Свежие новости", {s["section"] for s in report["still_under_minimum"]})

    def test_must_show_missing_item_is_recovered(self):
        # P0-2: a must_show item absent from the HTML must be recovered or reported.
        self.draft.write_text(_draft(6), encoding="utf-8")  # Fresh already at floor
        must = {
            "validated": True,
            "publish_plan_must_show": True,
            "primary_block": "last_24h",
            "source_url": "https://must.show/item",
            "source_label": "MEN",
            "draft_line": '• Обязательная к показу новость дня. <a href="https://must.show/item">MEN</a>',
        }
        report = rr.reconcile_visible_html(self.draft, candidates=[must], writer_section_counts={})
        self.assertEqual(report["must_show_recovered"], 1)
        self.assertIn("Обязательная к показу", self.draft.read_text(encoding="utf-8"))

    def test_recovery_is_bounded_by_cap(self):
        self.draft.write_text(_draft(0), encoding="utf-8")
        report = rr.reconcile_visible_html(
            self.draft, candidates=[_reserve(i) for i in range(50)], writer_section_counts={}, insert_cap=3
        )
        self.assertEqual(report["inserted_total"], 3)


if __name__ == "__main__":
    unittest.main()
