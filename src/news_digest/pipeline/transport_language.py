"""Shared public-language rules for GM transport copy."""

from __future__ import annotations

import re


_METRO_WORD_RE = re.compile(r"(?<![A-Za-zА-Яа-яЁё])метро(?![A-Za-zА-Яа-яЁё])", re.IGNORECASE)
_METROLINK_CYRILLIC_RE = re.compile(r"\bметролинк\b", re.IGNORECASE)
_LATIN_WORD_WITH_RU_CASE_RE = re.compile(
    r"\b([A-Za-z][A-Za-z0-9'’.-]{1,48})(?:е|а|у|ом|ем|ой|ах|ами)\b"
)
_TRANSPORT_CONTEXT_RE = re.compile(
    r"\b(?:metrolink|tfgm|tram|trams?|bee network|national rail|northern|"
    r"transpennine|transport for wales|avanti)\b|"
    r"\b(?:трамва\w*|поезд\w*|железнодорожн\w*|остановк\w*)\b|"
    r"метролинк|метро",
    re.IGNORECASE,
)
_BAD_TRANSPORT_RU_RE = re.compile(
    r"(?:остановк[аеуыи]\s+трамва(?:ев|я)\s+метролинк|"
    r"трамвайн\w+\s+остановк[аеуыи]\s+метролинк|"
    r"\b[A-Za-z][A-Za-z0-9'’.-]{1,48}(?:е|а|у|ом|ем|ой|ах|ами)\b)",
    re.IGNORECASE,
)


def transport_public_contract_errors(line: str) -> list[str]:
    """Hard public-output transport errors.

    Match the standalone Russian word "метро"; do not flag the official
    service name Metrolink or the transliterated word "Метролинк".
    """
    text = str(line or "")
    if _METRO_WORD_RE.search(text):
        return ["metrolink_written_as_metro"]
    return []


def transport_language_issues(line: str) -> list[str]:
    """Repairable transport-language issues for writer/editor passes."""
    text = str(line or "")
    if not text or not _TRANSPORT_CONTEXT_RE.search(text):
        return []
    issues = transport_public_contract_errors(text)
    if _BAD_TRANSPORT_RU_RE.search(text):
        issues.append("bad_transport_russian")
    return issues


def repair_transport_line_language(line: str) -> tuple[str, list[str]]:
    """Deterministically fix common Russian transport mistranslations.

    The repair is deliberately conservative: it only rewrites wording when the
    line clearly talks about transport / Metrolink, and it preserves links.
    """
    fixed = str(line or "")
    if not fixed or not _TRANSPORT_CONTEXT_RE.search(fixed):
        return fixed, []

    reasons: list[str] = []
    before = fixed

    # "станции метро" was a real historical error for Metrolink stops.
    replacements = (
        (r"\bстанции\s+метро\b", "остановки Metrolink"),
        (r"\bстанция\s+метро\b", "остановка Metrolink"),
        (r"\bстанцию\s+метро\b", "остановку Metrolink"),
        (r"\bстанцией\s+метро\b", "остановкой Metrolink"),
        (r"\bстанциях\s+метро\b", "остановках Metrolink"),
    )
    for pattern, replacement in replacements:
        fixed = re.sub(pattern, replacement, fixed, flags=re.IGNORECASE)
    fixed = _METRO_WORD_RE.sub("Metrolink", fixed)
    if fixed != before:
        reasons.append("metrolink_not_metro")
        before = fixed

    fixed = _METROLINK_CYRILLIC_RE.sub("Metrolink", fixed)
    if fixed != before:
        reasons.append("official_metrolink_name")
        before = fixed

    fixed = re.sub(
        r"\b(остановк[аеуыи])\s+трамва(?:ев|я)\s+Metrolink\b",
        r"\1 Metrolink",
        fixed,
        flags=re.IGNORECASE,
    )
    fixed = re.sub(
        r"\bтрамвайн\w+\s+(остановк[аеуыи])\s+Metrolink\b",
        r"\1 Metrolink",
        fixed,
        flags=re.IGNORECASE,
    )
    if fixed != before:
        reasons.append("metrolink_stop_wording")
        before = fixed

    fixed = re.sub(r"\bФирсвуд[аеуы]?\b", "Firswood", fixed, flags=re.IGNORECASE)
    fixed = re.sub(r"\bПрест(?:в|у)ич[аеуы]?\b", "Prestwich", fixed, flags=re.IGNORECASE)
    fixed = _LATIN_WORD_WITH_RU_CASE_RE.sub(r"\1", fixed)
    if fixed != before:
        reasons.append("latin_place_case")
        before = fixed

    fixed = re.sub(r"\s{2,}", " ", fixed)
    fixed = re.sub(r"\s+([,.;:])", r"\1", fixed).strip()
    return fixed, reasons
