from __future__ import annotations

import re


CHANGE_PHASE_VERSION = 1


_PHASE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("consultation_closing", re.compile(r"\bconsultation\b.{0,120}\b(?:closes?|closing|deadline)\b|\b(?:closes?|deadline)\b.{0,120}\bconsultation\b", re.IGNORECASE)),
    ("consultation_opened", re.compile(r"\bconsultation\b.{0,120}\b(?:opens?|opened|launch(?:es|ed)?)\b|\b(?:opens?|opened|launch(?:es|ed)?)\b.{0,120}\bconsultation\b", re.IGNORECASE)),
    ("tickets_on_sale", re.compile(r"\b(?:tickets?|public sale|presale|on sale|onsale)\b", re.IGNORECASE)),
    ("starts_today", re.compile(r"\b(?:starts?|begins?|opens?|launch(?:es|ed)?)\s+(?:today|tonight)\b|\b(?:today|tonight)\b.{0,80}\b(?:starts?|begins?|opens?)\b", re.IGNORECASE)),
    ("event_this_week", re.compile(r"\b(?:this week|this weekend|weekend|tomorrow|tonight)\b", re.IGNORECASE)),
    ("approved", re.compile(r"\b(?:approved|given approval|green light|backed|signed off|одобр)\b", re.IGNORECASE)),
    ("rejected", re.compile(r"\b(?:rejected|refused|turned down|blocked|отклони)\b", re.IGNORECASE)),
    ("delayed", re.compile(r"\b(?:delayed|postponed|pushed back|отлож)\b", re.IGNORECASE)),
    ("cancelled", re.compile(r"\b(?:cancelled|canceled|scrapped|called off|отмен)\b", re.IGNORECASE)),
    ("reopened", re.compile(r"\b(?:reopened|re-opens?|back open|reopen)\b", re.IGNORECASE)),
    ("charged", re.compile(r"\b(?:charged|обвинён|обвинен|предъявлен\w* обвинени\w*)\b", re.IGNORECASE)),
    ("sentenced", re.compile(r"\b(?:sentenced|jailed|приговор|осужд)\b", re.IGNORECASE)),
    ("appeal_updated", re.compile(r"\b(?:appeal|renewed appeal|witness appeal|разыск|обращени)\b", re.IGNORECASE)),
    ("announced", re.compile(r"\b(?:announced|revealed|confirmed|unveiled|объяв|подтверд)\b", re.IGNORECASE)),
)


def _blob(candidate: dict) -> str:
    # Classify the phase from the story's own claim (title/lead/summary), NOT
    # from raw evidence_text. On 2026-06-04 a housing story matched "sentenced"
    # because the article body mentioned "close to Strangeways prison" (a
    # landmark, not a sentence), and a "no charges" story matched "charged" on
    # boilerplate buried in the body. The headline carries the actual development.
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "lead", "summary")
    )


# A phase keyword that sits inside a negation describes the OPPOSITE of what
# the plate would claim ("will not face charges", "not enough evidence to
# charge", "не предъявлено обвинение"). Such a match must not set the phase.
_NEGATION_RE = re.compile(
    r"\b(?:not|no|without|never|denied|cleared|won'?t|cannot|can'?t|"
    r"fail(?:s|ed)?|unable|dropped|acquitted|не|без|недостаточно|нет)\b",
    re.IGNORECASE,
)
_SPECULATIVE_APPROVAL_RE = re.compile(
    r"\b(?:set|expected|recommended|due|poised|likely)\s+to\s+be\s+approved\b",
    re.IGNORECASE,
)


def _has_unnegated_match(blob: str, pattern: re.Pattern[str]) -> bool:
    for match in pattern.finditer(blob):
        window = blob[max(0, match.start() - 45): match.start()]
        if not _NEGATION_RE.search(window):
            return True
    return False


def classify_change_phase(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    blob = _blob(candidate)
    for phase, pattern in _PHASE_PATTERNS:
        if phase == "approved" and _SPECULATIVE_APPROVAL_RE.search(blob):
            continue
        if _has_unnegated_match(blob, pattern):
            return phase
    return ""


def attach_change_phase(candidate: dict) -> dict:
    if isinstance(candidate, dict):
        phase = classify_change_phase(candidate)
        candidate["change_phase"] = phase
        candidate["change_phase_version"] = CHANGE_PHASE_VERSION
    return candidate
