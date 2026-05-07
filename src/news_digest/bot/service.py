from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from news_digest.delivery.telegram import TelegramClient
from news_digest.state.store import StateStore


WELCOME_TEXT = """Привет. Это бот ежедневного дайджеста по Greater Manchester.

Что можно сделать:
/latest — показать последний готовый дайджест
/subscribe — подписать этот чат на ежедневную отправку
/unsubscribe — отписать этот чат от ежедневной отправки
/help — показать команды
"""


@dataclass(slots=True)
class BotProcessResult:
    processed_updates: int = 0
    handled_messages: int = 0
    replies_sent: int = 0


class DigestBotService:
    def __init__(
        self,
        client: TelegramClient,
        store: StateStore,
        latest_digest_path: Path,
    ) -> None:
        self.client = client
        self.store = store
        self.latest_digest_path = latest_digest_path

    def process_updates(self, updates: dict[str, Any]) -> BotProcessResult:
        result = BotProcessResult()
        max_update_id: int | None = None

        for update in updates.get("result", []):
            update_id = int(update.get("update_id", 0))
            max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)
            result.processed_updates += 1
            replies = self._process_single_update(update)
            if replies is None:
                continue
            result.handled_messages += 1
            result.replies_sent += replies

        if max_update_id is not None:
            self.store.set_last_update_id(max_update_id)

        return result

    def _process_single_update(self, update: dict[str, Any]) -> int | None:
        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            return None

        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()
        if chat_id is None or not text:
            return None

        command = self._extract_command(text)
        if not command:
            self.client.send_message(str(chat_id), self._unknown_command_text())
            return 1

        if command == "/start":
            replies = 1
            self.client.send_message(str(chat_id), self._welcome_text(str(chat_id), include_latest_hint=False))
            replies += self._send_latest_digest(str(chat_id), intro_text="Ниже отправляю последний готовый выпуск.")
            return replies
        if command == "/help":
            self.client.send_message(str(chat_id), self._welcome_text(str(chat_id), include_latest_hint=True))
            return 1
        if command == "/latest":
            return self._send_latest_digest(str(chat_id))
        if command == "/subscribe":
            return self._subscribe(str(chat_id))
        if command == "/unsubscribe":
            return self._unsubscribe(str(chat_id))

        self.client.send_message(str(chat_id), self._unknown_command_text())
        return 1

    def _extract_command(self, text: str) -> str | None:
        if not text.startswith("/"):
            return None
        command = text.split(maxsplit=1)[0].strip().lower()
        if "@" in command:
            command = command.split("@", 1)[0]
        return command

    def _welcome_text(self, chat_id: str, include_latest_hint: bool) -> str:
        is_subscribed = chat_id in self.store.list_subscribers()
        status_line = "Статус: этот чат уже подписан на ежедневный дайджест." if is_subscribed else (
            "Статус: этот чат пока не подписан. Нажмите /subscribe, если хотите получать дайджест автоматически."
        )
        latest_hint = "\nСразу открыть последний выпуск: /latest" if include_latest_hint else ""
        return f"{WELCOME_TEXT}\n{status_line}{latest_hint}"

    def _unknown_command_text(self) -> str:
        return "Не понял команду. Доступно: /latest, /subscribe, /unsubscribe, /help."

    def _send_latest_digest(self, chat_id: str, intro_text: str | None = None) -> int:
        replies = 0
        if intro_text:
            self.client.send_message(chat_id, intro_text)
            replies += 1
        if not self.latest_digest_path.exists():
            self.client.send_message(
                chat_id,
                "Последний дайджест пока не найден. Сначала нужно собрать и сохранить выпуск.",
            )
            return replies + 1

        text = self.latest_digest_path.read_text(encoding="utf-8")
        responses = self.client.send_text_in_chunks(chat_id, text, parse_mode="HTML")
        return replies + len(responses)

    def _subscribe(self, chat_id: str) -> int:
        added = self.store.add_subscriber(chat_id)
        if added:
            self.client.send_message(
                chat_id,
                "Готово. Этот чат подписан на ежедневный дайджест.",
            )
            return 1

        self.client.send_message(
            chat_id,
            "Этот чат уже подписан на ежедневный дайджест.",
        )
        return 1

    def _unsubscribe(self, chat_id: str) -> int:
        removed = self.store.remove_subscriber(chat_id)
        if removed:
            self.client.send_message(
                chat_id,
                "Готово. Этот чат больше не получает ежедневный дайджест.",
            )
            return 1

        self.client.send_message(
            chat_id,
            "Этот чат и так не был подписан.",
        )
        return 1
