"""Wave 1 / S4: the shipped HTML is the source of truth. The reconciler measures
the draft (not writer_report), flags the writer-vs-HTML divergence that hid the
vanished lead, and runs bounded recovery from the unified recoverable reserve
before promotion.
"""
from news_digest.pipeline import release_reconcile as rr


def _draft(fresh_bullets: int) -> str:
    lines = ["<b>Greater Manchester Brief — 2026-06-27</b>", "<b>Свежие новости</b>"]
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


def test_insert_after_last_bullet_and_noop_when_section_absent():
    html = "<b>Свежие новости</b>\n• Один. <a href=\"x\">MEN</a>\n<b>Футбол</b>\n• Гол.\n"
    out, added = rr.insert_bullets_after_section(html, "Свежие новости", ["• Два. <a href=\"y\">MEN</a>"])
    assert added == 1
    # inserted directly after the existing Fresh bullet, before the next heading
    assert out.index("Два.") < out.index("<b>Футбол</b>")
    assert out.index("Один.") < out.index("Два.")
    # unknown section: nothing fabricated
    out2, added2 = rr.insert_bullets_after_section(html, "Погода", ["• X"])
    assert added2 == 0 and out2 == html


def test_control_assertion_flags_writer_vs_html_divergence(tmp_path):
    draft = tmp_path / "draft_digest.html"
    draft.write_text(_draft(1), encoding="utf-8")
    # writer claimed 9 fresh + a visible lead; the HTML has 1 fresh and no lead.
    report = rr.reconcile_visible_html(
        draft, candidates=[], writer_section_counts={"Свежие новости": 9, "Главная история дня": 1}
    )
    assert report["control_assertion"]["ok"] is False
    diverged = {d["section"]: (d["writer"], d["html"]) for d in report["control_assertion"]["writer_vs_html_divergent_sections"]}
    assert diverged["Свежие новости"] == (9, 1)
    assert report["lead_visible"] is False


def test_bounded_recovery_fills_thin_section_from_reserve(tmp_path):
    draft = tmp_path / "draft_digest.html"
    draft.write_text(_draft(1), encoding="utf-8")  # Свежие новости = 1, min = 6
    report = rr.reconcile_visible_html(
        draft, candidates=[_reserve(i) for i in range(5)], writer_section_counts={"Свежие новости": 1}
    )
    # 1 existing + 5 recovered = 6 == minimum, and the draft on disk was rewritten.
    assert report["html_section_counts"]["Свежие новости"] == 6
    assert report["inserted_total"] == 5
    assert draft.read_text(encoding="utf-8").count("новое решение номер") == 5
    short_sections = {s["section"] for s in report["still_under_minimum"]}
    assert "Свежие новости" not in short_sections  # reached its floor


def test_recovery_is_bounded_by_cap(tmp_path):
    draft = tmp_path / "draft_digest.html"
    draft.write_text(_draft(0), encoding="utf-8")
    report = rr.reconcile_visible_html(
        draft, candidates=[_reserve(i) for i in range(50)], writer_section_counts={}, insert_cap=3
    )
    assert report["inserted_total"] == 3  # never balloons the issue
