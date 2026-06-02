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
    ("charged", re.compile(r"\b(?:charged|charge|обвин)\b", re.IGNORECASE)),
    ("sentenced", re.compile(r"\b(?:sentenced|jailed|prison|приговор|осужд)\b", re.IGNORECASE)),
    ("appeal_updated", re.compile(r"\b(?:appeal|renewed appeal|witness appeal|разыск|обращени)\b", re.IGNORECASE)),
    ("announced", re.compile(r"\b(?:announced|revealed|confirmed|unveiled|объяв|подтверд)\b", re.IGNORECASE)),
)


def _blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "evidence_text")
    )


def classify_change_phase(candidate: dict) -> str:
    if not isinstance(candidate, dict):
        return ""
    blob = _blob(candidate)
    for phase, pattern in _PHASE_PATTERNS:
        if pattern.search(blob):
            return phase
    return ""


def attach_change_phase(candidate: dict) -> dict:
    if isinstance(candidate, dict):
        phase = classify_change_phase(candidate)
        candidate["change_phase"] = phase
        candidate["change_phase_version"] = CHANGE_PHASE_VERSION
    return candidate
