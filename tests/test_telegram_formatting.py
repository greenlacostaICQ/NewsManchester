from __future__ import annotations

import unittest

from news_digest.pipeline.telegram_formatting import validate_telegram_formatting


class TelegramFormattingTest(unittest.TestCase):
    def test_valid_digest_format_passes(self) -> None:
        html = (
            "<b>Greater Manchester Brief — 2026-05-15, 08:00</b>\n\n"
            "<b>Погода</b>\n"
            '• Погода: 10–14°C, дождь возможен. Проверьте прогноз перед выходом. <a href="https://example.com/weather">Met Office</a>\n\n'
            "<b>Главная история дня</b>\n"
            '<b>Совет утвердил новый план.</b> Жителям района стоит сверить сроки работ. <a href="https://example.com/story">BBC</a>\n\n'
            "<b>Что важно сегодня</b>\n"
            '• Транспорт: Metrolink предупреждает о задержках. Проверьте маршрут. <a href="https://example.com/tfgm">TfGM</a>\n'
        )
        report = validate_telegram_formatting(html)
        self.assertTrue(report["ok"])
        self.assertEqual(report["errors"], [])

    def test_rejects_unsupported_html_and_bad_bullets(self) -> None:
        html = (
            "<b>Greater Manchester Brief — 2026-05-15, 08:00</b>\n\n"
            "<b>Что важно сегодня</b>\n"
            '<span>No bullet English prose with the and for with from after words.</span> <a href="/bad">Source</a>\n'
        )
        report = validate_telegram_formatting(html)
        errors = "\n".join(report["errors"])
        self.assertIn("Unsupported HTML tag", errors)
        self.assertIn("Anchor tag must have an http(s) href", errors)
        self.assertIn("must start with a bullet", errors)
        self.assertIn("English prose", errors)

    def test_rejects_banned_words_and_empty_formulations(self) -> None:
        html = (
            "<b>Greater Manchester Brief — 2026-05-15, 08:00</b>\n\n"
            "<b>Что важно сегодня</b>\n"
            '• Город: жители в шоке после обновления. Подробности уточняйте. <a href="https://example.com">MEN</a>\n'
        )
        report = validate_telegram_formatting(html)
        errors = "\n".join(report["errors"])
        self.assertIn("Banned word/phrase", errors)
        self.assertIn("Empty formulation", errors)


if __name__ == "__main__":
    unittest.main()
