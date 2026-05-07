from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from news_digest.models.digest import DigestIssue, DigestSection


LONDON_TZ = ZoneInfo("Europe/London")


def build_demo_digest() -> DigestIssue:
    now = datetime.now(LONDON_TZ)
    date_line = now.strftime("%d.%m.%Y, %H:%M Europe/London")

    sections = [
        DigestSection(
            title="Главное за утро",
            lines=[
                "Локальный MVP запущен: Telegram-бот подключён, локальная сборка готова к тестовым отправкам.",
                "Следующий шаг — добавить первые реальные источники и начать собирать не демо-выпуск, а настоящий brief.",
                "Токен бота хранится вне кода, через .env.local или переменные окружения.",
            ],
        ),
        DigestSection(
            title="Что уже умеет проект",
            lines=[
                "Локальная конфигурация и state-папки.",
                "Сборка тестового дайджеста в текстовом формате.",
                "Отправка сообщения в Telegram по целевому chat id или username канала.",
                "Получение getUpdates, чтобы найти chat id после старта бота.",
            ],
        ),
        DigestSection(
            title="Что добавим следующим",
            lines=[
                "Первые 3-5 стабильных источников.",
                "Базовый extraction и нормализацию фактов.",
                "Сохранение уже отправленных выпусков и дедупликацию между днями.",
            ],
        ),
    ]

    return DigestIssue(
        title="Greater Manchester Brief — локальный тестовый выпуск",
        subtitle=f"Собрано: {date_line}",
        sections=sections,
    )

