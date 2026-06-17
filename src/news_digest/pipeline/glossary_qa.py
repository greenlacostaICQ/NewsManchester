from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import re


def _default_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _glossary_path(project_root: Path | None = None) -> Path:
    return (project_root or _default_project_root()) / "data" / "translation_glossary.json"


@lru_cache(maxsize=8)
def _load_terms_cached(path: str) -> tuple[dict[str, str], ...]:
    glossary = Path(path)
    if not glossary.exists():
        return ()
    try:
        payload = json.loads(glossary.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ()
    terms = payload.get("terms") if isinstance(payload, dict) else []
    if not isinstance(terms, list):
        return ()
    cleaned: list[dict[str, str]] = []
    for term in terms:
        if not isinstance(term, dict):
            continue
        match = str(term.get("match") or "").strip()
        ru = str(term.get("ru") or "").strip()
        if not match or not ru:
            continue
        cleaned.append({"match": match, "ru": ru, "note": str(term.get("note") or "").strip()})
    return tuple(cleaned)


def load_translation_glossary(project_root: Path | None = None) -> list[dict[str, str]]:
    return [dict(term) for term in _load_terms_cached(str(_glossary_path(project_root)))]


def _term_policy(term: dict[str, str]) -> str:
    match = str(term.get("match") or "").strip()
    ru = str(term.get("ru") or "").strip()
    note = str(term.get("note") or "").lower()
    if "explain" in note or "expand" in note:
        return "explain"
    if ru.casefold() == match.casefold() or "keep" in note or "do not translate" in note:
        return "keep"
    return "translate"


def _body_and_suffix(line: str) -> tuple[str, str]:
    text = str(line or "")
    match = re.search(r"\s*<a\s+[^>]+>.*?</a>\s*$", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return text, ""
    return text[: match.start()], text[match.start() :]


def _contains_term(text: str, term: str) -> bool:
    escaped = re.escape(term)
    if re.search(r"[A-Za-z0-9]$", term):
        return bool(re.search(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", text, flags=re.IGNORECASE))
    return bool(re.search(escaped, text, flags=re.IGNORECASE))


_EXPLANATION_HINTS: dict[str, re.Pattern[str]] = {
    "CQC": re.compile(r"\b(?:регулятор|качест\w+\s+(?:медицин|социальн))", re.IGNORECASE),
    "MDC": re.compile(r"\b(?:корпорац\w+\s+мэрск|mayoral\s+development\s+corporation|мэрск\w+\s+развит)", re.IGNORECASE),
    "PBSA": re.compile(r"\b(?:студенческ\w+\s+жиль|purpose-built\s+student)", re.IGNORECASE),
    "AGM": re.compile(r"\b(?:годов\w+\s+общ\w+\s+собран|annual\s+general\s+meeting)", re.IGNORECASE),
}


def glossary_line_issues(line: str, project_root: Path | None = None) -> list[str]:
    """Return only glossary-contract issues, not every English word.

    The product rule is "no unexplained / forbidden English term", not
    "zero Latin tokens": names, venues, brands and accepted global terms stay.
    """
    body, _suffix = _body_and_suffix(str(line or ""))
    issues: list[str] = []
    for term in load_translation_glossary(project_root):
        match = str(term.get("match") or "")
        ru = str(term.get("ru") or "")
        if not match or not _contains_term(body, match):
            continue
        policy = _term_policy(term)
        if policy == "translate" and ru and not _contains_term(body, ru):
            issues.append(f"glossary_translate_required:{match}->{ru}")
        elif policy == "explain":
            hint = _EXPLANATION_HINTS.get(match)
            if hint is not None and not hint.search(body):
                issues.append(f"glossary_explain_required:{match}")
            elif hint is None and ru and ru.casefold() != match.casefold() and not _contains_term(body, ru):
                issues.append(f"glossary_explain_required:{match}->{ru}")
    return issues


def repair_glossary_terms(line: str, project_root: Path | None = None) -> tuple[str, list[str]]:
    body, suffix = _body_and_suffix(str(line or ""))
    fixed = body
    reasons: list[str] = []
    for term in load_translation_glossary(project_root):
        match = str(term.get("match") or "")
        ru = str(term.get("ru") or "")
        if not match or not ru or not _contains_term(fixed, match):
            continue
        policy = _term_policy(term)
        if policy == "keep":
            continue
        replacement = ru
        if policy == "explain" and match == "CQC":
            replacement = "регулятор качества медико-социальных услуг (CQC)"
        elif policy == "explain" and match == "MDC":
            replacement = "корпорация мэрского развития (MDC)"
        escaped = re.escape(match)
        if re.search(r"[A-Za-z0-9]$", match):
            pattern = re.compile(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", re.IGNORECASE)
        else:
            pattern = re.compile(escaped, re.IGNORECASE)
        updated = pattern.sub(replacement, fixed)
        if updated != fixed:
            fixed = updated
            reasons.append(f"glossary:{match}")
    fixed = re.sub(r"\s{2,}", " ", fixed).strip()
    return f"{fixed}{suffix}", reasons
