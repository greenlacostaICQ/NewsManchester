from __future__ import annotations

from news_digest.assembly.demo_digest import build_demo_digest
from news_digest.config.settings import Settings
from news_digest.delivery.telegram import TelegramClient
from news_digest.state.store import StateStore


def send_demo_digest(settings: Settings) -> dict[str, str]:
    if not settings.telegram_target:
        raise RuntimeError(
            "TELEGRAM_TARGET is not set. Use a numeric chat id or a channel username like @channel_name."
        )

    issue = build_demo_digest()
    rendered = issue.render_text()

    telegram = TelegramClient(settings.telegram_bot_token)
    telegram.send_message(settings.telegram_target, rendered)

    store = StateStore(settings.state_dir, settings.archive_dir)
    archive_path = store.mark_demo_run(rendered)

    return {
        "target": settings.telegram_target,
        "archive_path": str(archive_path),
    }

