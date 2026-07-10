from __future__ import annotations

from dataclasses import dataclass
import json
import re
import time
from typing import Any
from urllib import error, parse, request


# HTTP statuses worth a retry: Telegram rate-limit and transient server-side
# faults. A 400 (bad payload/HTML) is NOT here — retrying it just repeats the
# same rejection; the HTML→plain fallback in send_message handles that case.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class TelegramTransportError(RuntimeError):
    """Telegram API transport failed before we got an HTTP response."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _normalize_html_parse_mode_text(text: str) -> str:
    normalized = str(text or "")
    normalized = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", normalized, flags=re.DOTALL)
    normalized = re.sub(r"(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)", r"<i>\1</i>", normalized, flags=re.DOTALL)
    return normalized


# Tags Telegram accepts in parse_mode=HTML. Everything else (and every stray
# "&", "<", ">") MUST be escaped or the API rejects the whole message with
# HTTP 400 and the send loop aborts mid-digest. On 2026-05-29 a single raw "&"
# inside "Currents & Erra" / "Brighton & Hove Albion" 400'd the send and the
# football card (rendered, but later in the message) never reached the reader.
_TELEGRAM_TAG_RE = re.compile(
    r"</?(?:b|strong|i|em|u|ins|s|strike|del|code|pre|tg-spoiler|blockquote)>"
    r"|<a\s+href=\"[^\"]*\"\s*>"
    r"|</a>",
    re.IGNORECASE,
)
_ENTITY_OK_RE = re.compile(r"&(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);")


def _escape_telegram_text(segment: str) -> str:
    """Escape &, <, > in a plain-text segment, leaving valid entities intact."""
    parked: list[str] = []

    def _park(match: "re.Match[str]") -> str:
        parked.append(match.group(0))
        return f"\x00{len(parked) - 1}\x01"

    segment = _ENTITY_OK_RE.sub(_park, segment)  # protect already-valid entities
    segment = segment.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return re.sub(r"\x00(\d+)\x01", lambda m: parked[int(m.group(1))], segment)


def sanitize_telegram_html(text: str) -> str:
    """Make text safe for parse_mode=HTML: keep valid tags, escape the rest.

    Splits on the small whitelist of Telegram-supported tags and escapes every
    in-between text segment, so a stray ``&`` / ``<`` in source-derived event
    names can never 400 the send again.
    """
    raw = str(text or "")
    out: list[str] = []
    last = 0
    for match in _TELEGRAM_TAG_RE.finditer(raw):
        out.append(_escape_telegram_text(raw[last:match.start()]))
        out.append(match.group(0))
        last = match.end()
    out.append(_escape_telegram_text(raw[last:]))
    return "".join(out)


def html_to_plain_text(text: str) -> str:
    """Strip Telegram tags and unescape entities for a plain-text fallback send."""
    stripped = _TELEGRAM_TAG_RE.sub("", str(text or ""))
    stripped = re.sub(r"</?[a-zA-Z][^>]*>", "", stripped)  # drop any other stray tags
    for entity, char in (("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'")):
        stripped = stripped.replace(entity, char)
    return stripped


@dataclass(slots=True)
class TelegramClient:
    bot_token: str
    message_limit: int = 3800
    # Transient-failure handling for the send path. The morning digest is the
    # last metre where a one-off network blip silently loses the whole outgoing
    # message, so retry rate-limit/5xx with linear backoff before giving up.
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0

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
        attempts = max(1, self.max_retries)
        last_exc: TelegramTransportError | None = None
        for attempt in range(1, attempts + 1):
            req = request.Request(
                f"{self.api_base}/{method}",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with request.urlopen(req, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8"))
            except error.HTTPError as exc:
                last_exc = TelegramTransportError(
                    f"Telegram POST {method} failed: {exc.reason}", status_code=exc.code
                )
                if exc.code not in _RETRYABLE_STATUS:
                    raise last_exc from exc
            except error.URLError as exc:
                # Network-level failure (timeout, DNS, connection refused) — transient.
                last_exc = TelegramTransportError(f"Telegram POST {method} failed: {exc.reason}")
            if attempt < attempts:
                time.sleep(self.retry_backoff_seconds * attempt)
        assert last_exc is not None
        raise last_exc

    def send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str | None = None,
        reply_to_message_id: int | None = None,
    ) -> dict[str, Any]:
        is_html = bool(parse_mode and parse_mode.upper() == "HTML")
        payload_text = text
        if is_html:
            payload_text = sanitize_telegram_html(_normalize_html_parse_mode_text(text))

        def _build_payload(body: str, mode: str | None) -> dict[str, Any]:
            data: dict[str, Any] = {
                "chat_id": chat_id,
                "text": body,
                "disable_web_page_preview": True,
            }
            if mode:
                data["parse_mode"] = mode
            if reply_to_message_id is not None:
                data["reply_to_message_id"] = reply_to_message_id
            return data

        try:
            return self._post("sendMessage", _build_payload(payload_text, parse_mode))
        except TelegramTransportError as exc:
            # NEVER let a malformed-HTML 400 silently drop part of the digest.
            # Re-send the same content as plain text so the reader still gets it.
            if is_html and exc.status_code == 400:
                return self._post("sendMessage", _build_payload(html_to_plain_text(payload_text), None))
            raise

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
