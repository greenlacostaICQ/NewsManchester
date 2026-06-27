"""Wave 1 / S3: a deterministic backstop must catch half-translated titles and
glued bilingual tokens even when the LLM editor is down, so "On The линия"-class
defects never ship as a clean line.
"""
from news_digest.pipeline.editor import _line_needs_russian_editor


def test_half_translated_title_is_flagged():
    line = "• Выставка 'On The линия: 100 лет забастовок и солидарности' открыта. <a href=\"https://x\">PHM</a>"
    assert _line_needs_russian_editor(line) is True


def test_glued_bilingual_tokens_flagged_both_directions():
    assert _line_needs_russian_editor("• Stockportа закрыт для проезда.") is True   # latin→cyrillic (existing)
    assert _line_needs_russian_editor("• улицаStreet закрыта на ремонт.") is True   # cyrillic→latin (S3)


def test_kept_english_brands_are_not_false_flagged():
    # Brands stay Latin next to Russian — the S3 article-glue detector must not
    # fire on "The Mill", "The Lowry", "Co-op Live".
    for clean in (
        "• Авторская журналистика The Mill рассказала о решении совета.",
        "• The Lowry проводит выставку сегодня в 19:00.",
        "• Концерт в Co-op Live состоится завтра вечером.",
    ):
        assert _line_needs_russian_editor(clean) is False, clean
