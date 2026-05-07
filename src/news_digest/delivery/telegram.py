from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any
from urllib import error, parse, request


class TelegramTransportError(RuntimeError):
    """Telegram API transport failed before we got an HTTP response."""


def _normalize_html_parse_mode_text(text: str) -> str:
    normalized = str(text or "")
    normalized = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", normalized, flags=re.DOTALL)
    normalized = re.sub(r"(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)", r"<i>\1</i>", normalized, flags=re.DOTALL)
    return normalized


@dataclass(slots=True)
class TelegramClient:
    bot_token: str
    message_limit: int = 3800

    @property
    def api_base(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}"

    def _get(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = ""
        if params:
            query = "?" + parse.urlencode(params)
        try:
            with request.urlopen(f"{self.api_base}/{method}{query}", timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.URLError as exc:
            raise TelegramTransportError(f"Telegram GET {method} failed: {exc.reason}") from exc

    def _post(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{self.api_base}/{method}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.URLError as exc:
            raise TelegramTransportError(f"Telegram POST {method} failed: {exc.reason}") from exc

    def send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str | None = None,
        reply_to_message_id: int | None = None,
    ) -> dict[str, Any]:
        payload_text = text
        if parse_mode and parse_mode.upper() == "HTML":
            payload_text = _normalize_html_parse_mode_text(text)
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": payload_text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
        return self._post("sendMessage", payload)

    def send_text_in_chunks(
        self,
        chat_id: str,
        text: str,
        parse_mode: str | None = None,
    ) -> list[dict[str, Any]]:
        chunks = self._split_text(text)
        results: list[dict[str, Any]] = []
        for chunk in chunks:
            results.append(self.send_message(chat_id, chunk, parse_mode=parse_mode))
        return results

    def _split_text(self, text: str) -> list[str]:
        if len(text) <= self.message_limit:
            return [text]

        paragraphs = text.split("\n\n")
        chunks: list[str] = []
        current = ""

        for paragraph in paragraphs:
            candidate = paragraph if not current else f"{current}\n\n{paragraph}"
            if len(candidate) <= self.message_limit:
                current = candidate
                continue

            if current:
                chunks.append(current)
                current = ""

            if len(paragraph) <= self.message_limit:
                current = paragraph
                continue

            lines = paragraph.splitlines()
            line_buffer = ""
            for line in lines:
                line_candidate = line if not line_buffer else f"{line_buffer}\n{line}"
                if len(line_candidate) <= self.message_limit:
                    line_buffer = line_candidate
                    continue
                if line_buffer:
                    chunks.append(line_buffer)
                line_buffer = line
            if line_buffer:
                current = line_buffer

        if current:
            chunks.append(current)
        return chunks

    def get_me(self) -> dict[str, Any]:
        return self._get("getMe")

    def get_updates(self, offset: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"timeout": 1}
        if offset is not None:
            params["offset"] = offset
        return self._get("getUpdates", params)
