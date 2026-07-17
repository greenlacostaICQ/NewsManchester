"""The shipped HTML is the source of truth and release never mutates it."""
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

    def test_thin_section_is_reported_without_late_recovery(self):
        self.draft.write_text(_draft(1), encoding="utf-8")  # Свежие новости = 1, min = 6
        before = self.draft.read_text(encoding="utf-8")
        report = rr.reconcile_visible_html(
            self.draft, candidates=[_reserve(i) for i in range(5)], writer_section_counts={"Свежие новости": 1}
        )
        self.assertEqual(report["html_section_counts"]["Свежие новости"], 1)
        self.assertEqual(report["inserted_total"], 0)
        self.assertEqual(self.draft.read_text(encoding="utf-8"), before)
        self.assertIn("Свежие новости", {s["section"] for s in report["still_under_minimum"]})

    def test_must_show_missing_item_is_reported_not_recovered(self):
        self.draft.write_text(_draft(6), encoding="utf-8")  # Fresh already at floor
        must = {
            "validated": True,
            "publish_plan_must_show": True,
            "primary_block": "last_24h",
            "source_url": "https://must.show/item",
            "source_label": "MEN",
            "draft_line": '• Обязательная к показу новость дня. <a href="https://must.show/item">MEN</a>',
        }
        before = self.draft.read_text(encoding="utf-8")
        report = rr.reconcile_visible_html(self.draft, candidates=[must], writer_section_counts={})
        self.assertEqual(report["must_show_recovered"], 0)
        self.assertEqual(len(report["must_show_missing"]), 1)
        self.assertEqual(self.draft.read_text(encoding="utf-8"), before)

    def test_existing_lead_guard_is_preserved_until_0114(self):
        self.draft.write_text(_draft(6), encoding="utf-8")
        lead = {
            "fingerprint": "lead-fp",
            "is_lead": True,
            "publish_plan_must_show": True,
            "primary_block": "last_24h",
            "source_url": "https://lead/item",
            "source_label": "MEN",
            "draft_line": "• Главная проверенная история дня. Подробности подтверждены.",
        }
        report = rr.reconcile_visible_html(self.draft, candidates=[lead], writer_section_counts={})

        self.assertEqual(report["lead_guard_recovered"], 1)
        self.assertTrue(report["lead_visible"])
        self.assertIn("Главная проверенная история", self.draft.read_text(encoding="utf-8"))

    def test_report_invariants_recomputed_on_final_html(self):
        # Missing must_show is reported, but final counts stay tied to the HTML.
        self.draft.write_text(_draft(6, lead=True), encoding="utf-8")
        must = {
            "validated": True, "publish_plan_must_show": True, "primary_block": "last_24h",
            "source_url": "https://must/x", "source_label": "MEN",
            "draft_line": '• Обязательная новость дня. <a href="https://must/x">MEN</a>',
        }
        report = rr.reconcile_visible_html(self.draft, candidates=[must], writer_section_counts={"Свежие новости": 6})
        self.assertEqual(report["html_section_counts"]["Свежие новости"], 6)
        diverged = {d["section"]: d["html"] for d in report["control_assertion"]["writer_vs_html_divergent_sections"]}
        self.assertNotIn("Свежие новости", diverged)
        self.assertTrue(report["lead_visible"])

    def test_resolved_quarantine_is_not_reported_as_silent_must_show_loss(self):
        self.draft.write_text(_draft(6, lead=True), encoding="utf-8")
        must = {
            "fingerprint": "repeat-fp",
            "publish_plan_must_show": True,
            "primary_block": "professional_events",
            "source_url": "https://must/repeat",
            "title": "Already published event",
        }
        report = rr.reconcile_visible_html(
            self.draft,
            candidates=[must],
            writer_section_counts={},
            resolved_dispositions={"repeat-fp": "repeat_quarantine"},
        )

        self.assertEqual(report["must_show_missing"], [])
        self.assertEqual(
            report["must_show_resolved_before_contract"][0]["disposition"],
            "repeat_quarantine",
        )


if __name__ == "__main__":
    unittest.main()
