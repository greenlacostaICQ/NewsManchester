"""Wave 1 / S3: a deterministic backstop must catch half-translated titles and
glued bilingual tokens even when the LLM editor is down, so "On The линия"-class
defects never ship as a clean line.
"""
import unittest

from news_digest.pipeline.editor import _line_needs_russian_editor


class TranslationDefectCheckerTest(unittest.TestCase):
    def test_half_translated_title_is_flagged(self):
        line = "• Выставка 'On The линия: 100 лет забастовок и солидарности' открыта. <a href=\"https://x\">PHM</a>"
        self.assertTrue(_line_needs_russian_editor(line))

    def test_glued_bilingual_tokens_flagged_both_directions(self):
        self.assertTrue(_line_needs_russian_editor("• Stockportа закрыт для проезда."))   # latin→cyrillic (existing)
        self.assertTrue(_line_needs_russian_editor("• улицаStreet закрыта на ремонт."))   # cyrillic→latin (S3)
        self.assertTrue(_line_needs_russian_editor("• В Норт Уэстern открыли новый маршрут."))

    def test_kept_english_brands_are_not_false_flagged(self):
        for clean in (
            "• Авторская журналистика The Mill рассказала о решении совета.",
            "• The Lowry проводит выставку сегодня в 19:00.",
            "• Концерт в Co-op Live состоится завтра вечером.",
        ):
            self.assertFalse(_line_needs_russian_editor(clean), clean)


if __name__ == "__main__":
    unittest.main()
