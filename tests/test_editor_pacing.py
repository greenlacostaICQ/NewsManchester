"""E1/E2: editor pacing (reuse the token bucket, run concurrent) + tiered
evidence (full only for faithfulness-critical lines)."""
import unittest
from unittest import mock

from news_digest.pipeline import editor


class EditorPacingTest(unittest.TestCase):
    def test_tiered_evidence_full_for_sensitive_short_for_routine(self):
        crime = {"draft_line": "• Мужчина осуждён за нападение с ножом в Манчестере."}
        routine = {"draft_line": "• Levenshulme Artisan Market пройдёт в субботу с 10:00."}
        self.assertTrue(editor._evidence_is_sensitive(crime))
        self.assertFalse(editor._evidence_is_sensitive(routine))
        # English-only source still counts (title/evidence not yet translated)
        self.assertTrue(editor._evidence_is_sensitive({"title": "Man charged with murder"}))

    def test_token_limiter_builds_and_paces_without_cycle(self):
        with mock.patch.object(editor.time, "sleep", lambda _s: None):
            limiter = editor._editor_token_limiter()
            limiter.acquire(1000)  # reuses the rewrite-stage bucket; no import cycle
        self.assertGreaterEqual(editor.PRE_SEND_EDITOR_MAX_WORKERS, 2)

    def test_empty_ending_post_check_strips_generic_filler_and_keeps_link(self):
        line = (
            '• HOME: билеты на серию показов поступят в продажу 5 июля, '
            'а участники программы уже названы. Следите за обновлениями площадки. '
            '<a href="https://example.test/home">HOME</a>'
        )

        fixed, reason = editor._strip_empty_editor_ending(line)

        self.assertEqual(reason, "empty_generic_ending_stripped")
        self.assertIn("5 июля", fixed)
        self.assertNotIn("Следите за обновлениями", fixed)
        self.assertTrue(fixed.endswith('<a href="https://example.test/home">HOME</a>'))

    def test_empty_ending_post_check_keeps_concrete_status_action(self):
        line = (
            '• Metrolink: задержки на линии Bury идут из-за проверки путей; '
            'перед поездкой проверьте страницу статуса TfGM. '
            '<a href="https://tfgm.com/status">TfGM</a>'
        )

        fixed, reason = editor._strip_empty_editor_ending(line)

        self.assertEqual(reason, "")
        self.assertEqual(fixed, line)

    def test_empty_ending_post_check_reports_removed_rows(self):
        warnings: list[str] = []
        polished, report = editor._apply_empty_ending_post_check(
            {
                "Свежие новости": [
                    '• Manchester: совет открыл консультацию по центру города до 12 июля. '
                    'Проверьте сроки и детали. <a href="https://example.test/council">Council</a>'
                ]
            },
            warnings,
        )

        self.assertEqual(report["removed"], 1)
        self.assertEqual(report["remaining"], 0)
        self.assertIn("Final editor post-check stripped 1", warnings[0])
        self.assertNotIn("Проверьте сроки и детали", polished["Свежие новости"][0])

    def test_ship_time_pass_strips_broadened_and_short_boilerplate_endings(self):
        # Weekend/transport lines bypass the editor post-check, so the ship-time
        # pass over the shipped HTML must catch broadened patterns ("Сверьте
        # часы") and short transport stubs ("уточняйте на странице перевозчика").
        from news_digest.pipeline.pre_send_quality_judge import _strip_empty_endings_in_html

        html = (
            "<b>Выходные в GM</b>\n"
            "• 11 июля — Sound Bazaar Festival: фестиваль: еда, живая музыка. "
            'Сверьте часы и условия перед поездкой. <a href="https://example.test/sb">MF</a>\n'
            "<b>Общественный транспорт сегодня</b>\n"
            "• TfGM: ремонтные работы — Brooklands. Сроки и объёмы работ уточняйте "
            'на странице перевозчика. <a href="https://tfgm.com/x">TfGM</a>\n'
        )

        cleaned, n = _strip_empty_endings_in_html(html)

        self.assertEqual(n, 2)
        self.assertNotIn("Сверьте часы", cleaned)
        self.assertIn("Sound Bazaar Festival", cleaned)
        self.assertIn('href="https://example.test/sb"', cleaned)
        self.assertNotIn("уточняйте на странице перевозчика", cleaned)
        self.assertIn("ремонтные работы — Brooklands", cleaned)


if __name__ == "__main__":
    unittest.main()
