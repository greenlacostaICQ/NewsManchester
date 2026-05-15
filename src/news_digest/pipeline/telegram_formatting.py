from __future__ import annotations

from html.parser import HTMLParser
import re

from news_digest.pipeline.common import extract_sections


ALLOWED_TAGS = {"a", "b", "strong", "i", "em", "u", "s", "code", "pre"}
MAX_LINE_CHARS = 650
MAX_HARD_LINE_CHARS = 900
ENGLISH_PROSE_PATTERN = re.compile(
    r"\b(?:the|and|for|with|from|after|following|across|response|operators|said|says|their|"
    r"will|have|has|this|that|into|over|under|between)\b",
    re.IGNORECASE,
)
BANNED_WORDS = (
    "ticket office",
    "booking fee",
    "under-30s",
    "claimants",
    "takeaway",
    "forecast",
    "attractions",
    "highlights",
    "matchday",
    "check before",
    "live alert",
    "live disruption",
    "слот входа",
    "висит карточка",
    "слот подтверждён",
    "жители в шоке",
    "эмоциональное прощание",
    "отличный повод",
    "важный сигнал",
    "заметный кейс",
    "центр притяжения",
    "новая достопримечательность",
    "обещает стать",
    "подробности ниже",
    "читайте подробнее",
    "убедитесь сами",
    "перепроверьте",
)
EMPTY_FORMULATIONS = (
    "подробности уточняйте",
    "детали уточняйте",
    "билеты и даты уточняйте",
    "дату и время уточняйте",
    "время и дату уточняйте",
    "следите за обновлениями",
    "стоит знать",
    "это важно для жителей",
    "это важный сигнал",
    "это заметный кейс",
)


class _TelegramHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.errors: list[str] = []
        self._stack: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered not in ALLOWED_TAGS:
            self.errors.append(f"Unsupported HTML tag: <{tag}>.")
            return
        if lowered == "a":
            href = ""
            for key, value in attrs:
                if key.lower() == "href":
                    href = str(value or "").strip()
                    break
            if not href.startswith(("http://", "https://")):
                self.errors.append("Anchor tag must have an http(s) href.")
        self._stack.append(lowered)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered not in ALLOWED_TAGS:
            self.errors.append(f"Unsupported HTML closing tag: </{tag}>.")
            return
        if lowered not in self._stack:
            self.errors.append(f"Unmatched HTML closing tag: </{tag}>.")
            return
        while self._stack:
            current = self._stack.pop()
            if current == lowered:
                return
            self.errors.append(f"Unclosed HTML tag before </{tag}>: <{current}>.")

    def close(self) -> None:
        super().close()
        for tag in reversed(self._stack):
            self.errors.append(f"Unclosed HTML tag: <{tag}>.")
        self._stack.clear()


def _visible_text(value: str) -> str:
    text = re.sub(r"<a\b[^>]*>(.*?)</a>", lambda match: match.group(1), value, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _line_report(line: str, section_name: str) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    stripped = line.strip()
    visible = _visible_text(stripped)
    visible_lower = visible.lower()

    if not visible or visible == "•":
        errors.append(f"Empty rendered line in section {section_name}.")
        return errors, warnings
    if len(visible) > MAX_HARD_LINE_CHARS:
        errors.append(f"Line in section {section_name} is too long for Telegram ({len(visible)} chars).")
    elif len(visible) > MAX_LINE_CHARS:
        warnings.append(f"Line in section {section_name} is long for Telegram ({len(visible)} chars).")

    if section_name != "Главная история дня" and not stripped.startswith("• "):
        errors.append(f"Line in section {section_name} must start with a bullet marker.")
    if stripped.startswith("•") and not stripped.startswith("• "):
        errors.append(f"Bullet line in section {section_name} must use '• ' spacing.")
    if "**" in stripped:
        errors.append(f"Markdown emphasis marker found in section {section_name}.")
    for marker in BANNED_WORDS:
        if marker in visible_lower:
            errors.append(f"Banned word/phrase in section {section_name}: {marker}.")
            break
    for marker in EMPTY_FORMULATIONS:
        if marker in visible_lower:
            errors.append(f"Empty formulation in section {section_name}: {marker}.")
            break
    if not re.search(r"[а-яё]", visible, flags=re.IGNORECASE):
        english_hits = len(ENGLISH_PROSE_PATTERN.findall(visible))
        latin_words = re.findall(r"[A-Za-z][A-Za-z'’-]+", visible)
        if english_hits >= 2 and len(latin_words) >= 8:
            errors.append(f"English prose in section {section_name}.")
    return errors, warnings


def validate_telegram_formatting(html_text: str) -> dict[str, object]:
    errors: list[str] = []
    warnings: list[str] = []
    parser = _TelegramHTMLParser()
    parser.feed(str(html_text or ""))
    parser.close()
    errors.extend(parser.errors)

    sections = extract_sections(str(html_text or ""))
    if not sections:
        errors.append("Digest has no parseable sections.")
    line_count = 0
    current_section: str | None = None
    for raw_line in str(html_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        heading_match = re.fullmatch(r"<b>([^<]+)</b>", line)
        if heading_match:
            heading = heading_match.group(1).strip()
            current_section = None if heading.startswith("Greater Manchester Brief") else heading
            continue
        if current_section and not line.startswith("• "):
            line_errors, line_warnings = _line_report(line, current_section)
            errors.extend(line_errors)
            warnings.extend(line_warnings)

    for section_name, lines in sections.items():
        for line in lines:
            if not str(line).strip():
                continue
            line_count += 1
            line_errors, line_warnings = _line_report(str(line), section_name)
            errors.extend(line_errors)
            warnings.extend(line_warnings)

    return {
        "ok": not errors,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "line_count": line_count,
        "max_line_chars": MAX_LINE_CHARS,
        "max_hard_line_chars": MAX_HARD_LINE_CHARS,
        "errors": errors,
        "warnings": warnings,
    }
