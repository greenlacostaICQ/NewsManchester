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


# UK civic / legal / health abbreviations that the writer leaves untranslated
# or transliterates ("Cllr" → "Клр."). One glossary, applied as a single pass;
# extend by adding a row, not by writing new code. Order longest-first so
# multi-word forms win before their abbreviations.
_UK_ABBREVIATIONS: tuple[tuple[re.Pattern[str], str], ...] = tuple(
    (re.compile(pattern), replacement)
    for pattern, replacement in (
        (r"\bCllrs\b", "советники"),
        (r"\bCllr\.?\b|\bКлр\.?", "советник"),
        (r"\bCouncillors\b", "советники"),
        (r"\bCouncillor\b", "советник"),
        (r"\bA&E\b|\bA\s*&\s*E\b", "приёмное отделение"),
        (r"\bPCSOs\b", "общественные помощники полиции"),
        (r"\bPCSO\b", "общественный помощник полиции"),
        (r"\bCPS\b", "Королевская прокуратура"),
        (r"\bMEPs\b", "депутаты Европарламента"),
        (r"\bMEP\b", "депутат Европарламента"),
        (r"\bKC\b|\bQC\b", "королевский адвокат"),
        (r"\bASBO\b", "судебный запрет на антиобщественное поведение"),
    )
)


def expand_uk_abbreviations(text: str) -> str:
    result = str(text or "")
    for pattern, replacement in _UK_ABBREVIATIONS:
        result = pattern.sub(replacement, result)
    return re.sub(r"\s{2,}", " ", result).strip()


def preserve_place_names(text: str) -> str:
    result = str(text or "")
    for pattern, replacement in _PLACE_NAME_REPLACEMENTS:
        result = pattern.sub(replacement, result)
    return expand_uk_abbreviations(result)
