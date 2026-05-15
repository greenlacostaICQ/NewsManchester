from __future__ import annotations

from collections import Counter
import re
from typing import Iterable


VALID_REJECT_REASONS = frozenset(
    {
        "duplicate",
        "not_gm",
        "no_date",
        "pr",
        "evergreen",
        "weak_value",
        "source_thin",
        "no_change",
        "english_prose",
        "expired",
        "invalid_url",
    }
)


def reject_reasons(candidate: dict) -> list[str]:
    reasons = candidate.get("reject_reasons")
    if isinstance(reasons, list):
        return [str(reason) for reason in reasons if str(reason) in VALID_REJECT_REASONS]
    reason = candidate.get("reject_reason")
    if str(reason) in VALID_REJECT_REASONS:
        return [str(reason)]
    return []


def add_reject_reason(candidate: dict, code: str) -> None:
    if code not in VALID_REJECT_REASONS:
        code = "weak_value"
    reasons = reject_reasons(candidate)
    if code not in reasons:
        reasons.append(code)
    candidate["reject_reasons"] = reasons
    candidate["reject_reason"] = reasons[0] if reasons else ""


def classify_reject_reason_text(text: object) -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return "weak_value"
    if any(token in lowered for token in ("duplicate", "dedup", "same story", "near-duplicate", "дубл")):
        return "duplicate"
    if any(token in lowered for token in ("not gm", "non-gm", "not in gm", "not greater manchester", "не в gm", "без привязки к gm", "cheshire")):
        return "not_gm"
    if re.search(r"\bpr\b", lowered) or any(token in lowered for token in ("promo", "promotion", "affiliate", "deal", "без новостного повода", "чистый пиар")):
        return "pr"
    if any(token in lowered for token in ("evergreen", "listicle", "листинг", "подборка", "общеобразовательная")):
        return "evergreen"
    if any(token in lowered for token in ("no date", "no concrete", "no usable date", "no date signal", "undated", "без даты", "даты нет")):
        return "no_date"
    if any(token in lowered for token in ("repeat", "carry-over", "new phase", "no_change", "already published", "повтор")):
        return "no_change"
    if any(
        token in lowered
        for token in (
            "thin",
            "too fact-thin",
            "too thin",
            "source material",
            "evidence is too thin",
            "under-specified event",
            "missing venue",
            "missing district",
            "missing price",
            "missing booking",
            "мало фактов",
        )
    ):
        return "source_thin"
    if any(token in lowered for token in ("english", "untranslated", "not russian", "russian prose", "англ")):
        return "english_prose"
    if any(token in lowered for token in ("expired", "stale", "already in the past", "date is in the past", "дата прошла", "устаревш")):
        return "expired"
    if any(token in lowered for token in ("invalid url", "topic/index", "search url", "amp url", "homepage", "aggregator")):
        return "invalid_url"
    if any(token in lowered for token in ("weak", "vague", "placeholder", "quality", "without city value", "без конкрет", "важный сигнал")):
        return "weak_value"
    if re.search(r"\bno\s+(?:clear\s+)?(?:subject|action|impact|value)\b", lowered):
        return "weak_value"
    return "weak_value"


def ensure_reject_reason(candidate: dict, *, default: str = "weak_value") -> None:
    if candidate.get("include"):
        return
    if reject_reasons(candidate):
        return
    text = candidate.get("reason") or candidate.get("dedupe_decision") or ""
    code = classify_reject_reason_text(text)
    add_reject_reason(candidate, code if code else default)


def ensure_reject_reasons(candidates: Iterable[dict]) -> None:
    for candidate in candidates:
        if isinstance(candidate, dict):
            ensure_reject_reason(candidate)


def reject_reason_counts(candidates: Iterable[dict]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for candidate in candidates:
        if not isinstance(candidate, dict) or candidate.get("include"):
            continue
        ensure_reject_reason(candidate)
        counter.update(reject_reasons(candidate))
    return dict(sorted(counter.items()))
