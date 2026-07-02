"""Deterministic fact-lock helpers for final editorial rewrites.

The model may improve Russian wording, but it must not introduce fresh scalar
facts or new named entities that are absent from the candidate evidence. This
module intentionally stays small and conservative: it catches the visible
classes that broke the digest (dates, times, money, numbers, and Latin proper
nouns) without trying to be a full fact checker.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any


FACT_LOCK_VERSION = "v1"

_MONTH_WORDS = (
    "jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|"
    "aug|august|sep|sept|september|oct|october|nov|november|dec|december|"
    "январ[ья]|феврал[ья]|март[ае]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|"
    "август[ае]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья]"
)

_SCALAR_FACT_RE = re.compile(
    rf"""
    (?:
        £\s?\d+(?:[.,]\d+)?(?:\s?(?:m|mn|k|million|тыс\.?|млн))?
      | \d+(?:[.,]\d+)?\s?(?:%|per\s?cent)
      | \b\d{{1,2}}:\d{{2}}\b
      | \b20\d{{2}}-\d{{2}}-\d{{2}}(?:[T\s]\d{{2}}:\d{{2}})?\b
      | \b\d{{1,2}}[/-]\d{{1,2}}(?:[/-]20\d{{2}})?\b
      | \b\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{_MONTH_WORDS})(?:\s+20\d{{2}})?\b
      | \b(?:{_MONTH_WORDS})\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,\s*20\d{{2}})?\b
      | \b\d{{2,}}(?:[.,]\d+)?\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_LATIN_PROPER_RE = re.compile(
    r"\b(?:[A-Z][A-Za-z0-9&.'-]{1,}|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9&.'-]{1,}|[A-Z]{2,}|of|the|and|&))*"
)

_PROPER_SKIP = {
    "brief",
    "greater manchester brief",
    "source",
    "ticket radar",
}


def iter_fact_texts(value: Any) -> Iterable[str]:
    """Yield all string-like leaves from nested candidate/evidence objects."""
    if value is None:
        return
    if isinstance(value, str):
        if value.strip():
            yield value
        return
    if isinstance(value, (int, float)):
        yield str(value)
        return
    if isinstance(value, dict):
        for child in value.values():
            yield from iter_fact_texts(child)
        return
    if isinstance(value, (list, tuple, set)):
        for child in value:
            yield from iter_fact_texts(child)


def _normalise_token(token: str) -> str:
    token = re.sub(r"<[^>]+>", " ", str(token or ""))
    token = token.replace("\u00a0", " ")
    token = re.sub(r"\s+", " ", token).strip(" .,;:()[]{}")
    token = token.lower()
    token = token.replace("£ ", "£")
    token = re.sub(r"\s+(st|nd|rd|th)\b", r"\1", token)
    return token


def fact_tokens_from_text(text: str) -> set[str]:
    tokens: set[str] = set()
    raw = str(text or "")
    for match in _SCALAR_FACT_RE.finditer(raw):
        token = _normalise_token(match.group(0))
        if token:
            tokens.add(token)
    for match in _LATIN_PROPER_RE.finditer(raw):
        token = _normalise_token(match.group(0))
        if token and token not in _PROPER_SKIP and len(token) > 2:
            tokens.add(token)
    return tokens


def scalar_fact_tokens(text: str) -> set[str]:
    """Return only the scalar facts (money, %, time, date, number) in ``text``.

    Unlike :func:`fact_tokens_from_text` this excludes Latin proper nouns, so it
    is safe for cross-language coverage checks where names get transliterated.
    """
    tokens: set[str] = set()
    for match in _SCALAR_FACT_RE.finditer(str(text or "")):
        token = _normalise_token(match.group(0))
        if token:
            tokens.add(token)
    return tokens


def allowed_fact_tokens(values: Iterable[Any]) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        for text in iter_fact_texts(value):
            tokens.update(fact_tokens_from_text(text))
    return tokens


def unsupported_fact_tokens(candidate_text: str, allowed_values: Iterable[Any]) -> list[str]:
    """Return visible fact tokens in ``candidate_text`` that evidence lacks."""
    visible = fact_tokens_from_text(candidate_text)
    if not visible:
        return []
    allowed = allowed_fact_tokens(allowed_values)
    return sorted(token for token in visible if token not in allowed)
