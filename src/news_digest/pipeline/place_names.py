from __future__ import annotations

import re


_PLACE_NAME_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = tuple(
    (re.compile(pattern, re.IGNORECASE), replacement)
    for pattern, replacement in (
        (r"\bПиплс\s+Хистори\s+Мьюзеум\b", "People's History Museum"),
        (r"\bPeople'?s\s+History\s+Museum\b", "People's History Museum"),
        (r"\bФ[её]рст\s+Стрит\b", "First Street"),
        (r"\bЛевенс?хулм(?:е)?\b", "Levenshulme"),
        (r"\bСпиннингфилдс\b", "Spinningfields"),
        (r"\bГрейт\s+Нортерн\b", "Great Northern"),
        (r"\bНортерн\s+Кв?о?ртер\b", "Northern Quarter"),
        (r"\bСтивенсон\s+Скв[еэ]р\b", "Stevenson Square"),
        (r"\bОлтринчам\b", "Altrincham"),
        (r"\bСтокпорт\s+Маркет\s+Холл\b", "Stockport Market Hall"),
        (r"\bУрмстон\b", "Urmston"),
        (r"\bУитеншоу\b", "Wythenshawe"),
    )
)


def preserve_place_names(text: str) -> str:
    result = str(text or "")
    for pattern, replacement in _PLACE_NAME_REPLACEMENTS:
        result = pattern.sub(replacement, result)
    return result
