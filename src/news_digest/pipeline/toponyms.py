"""Restore English spellings of Greater Manchester toponyms.

LLM rewriters routinely Russify town and district names that should stay
in Latin script (e.g. "Олтрингем" instead of "Altrincham"). The repair
prompt mentions a generic rule but it does not stick — so this module
runs a final deterministic pass over rendered draft_line text.

Only canonical GM districts, boroughs and a handful of well-known
neighbourhoods are listed. The Russian capital "Манчестер" is intentionally
left untouched — it is well-established and reads natively in Russian text.
"""
from __future__ import annotations

import re


# Russian-spelling → English-spelling. Keep keys in their natural cases
# (capitalised); the replacement preserves the leading capital because the
# LLM only produces capitalised toponyms.
_TOPONYM_REPLACEMENTS: dict[str, str] = {
    # Boroughs
    "Олтрингем": "Altrincham",
    "Олтринхем": "Altrincham",
    "Олтрингхем": "Altrincham",
    "Бери": "Bury",
    "Уиган": "Wigan",
    "Стокпорт": "Stockport",
    "Теймсайд": "Tameside",
    "Тэймсайд": "Tameside",
    "Траффорд": "Trafford",
    "Трэффорд": "Trafford",
    "Трафорд": "Trafford",
    "Болтон": "Bolton",
    "Рочдейл": "Rochdale",
    "Олдхэм": "Oldham",
    "Олдем": "Oldham",
    "Солфорд": "Salford",
    "Салфорд": "Salford",
    # Districts / neighbourhoods commonly Russified
    "Оффертон": "Offerton",
    "Прествич": "Prestwich",
    "Ферст-стрит": "First Street",
    "Ферст Стрит": "First Street",
    "Дидсбери": "Didsbury",
    "Чорлтон": "Chorlton",
    "Левеншульм": "Levenshulme",
    "Левенсхульм": "Levenshulme",
    "Энкоутс": "Ancoats",
    "Анкоутс": "Ancoats",
    "Хьюм": "Hulme",
    "Хальме": "Hulme",
    "Гэтли": "Gatley",
    "Гатли": "Gatley",
    "Урмстон": "Urmston",
    "Уайтеншо": "Wythenshawe",
    "Уайтеншоу": "Wythenshawe",
    "Уитеншоу": "Wythenshawe",
    "Эштон-андер-Лайн": "Ashton-under-Lyne",
    "Эштон-ин-Мейкерфилд": "Ashton-in-Makerfield",
    "Мейкерфилд": "Makerfield",
    "Бридгуотер": "Bridgewater",  # RHS Garden Bridgewater
    "Экклс": "Eccles",
    "Стретфорд": "Stretford",
    "Олд-Траффорд": "Old Trafford",
    "Олд Траффорд": "Old Trafford",
    "Грейтер Манчестер": "Greater Manchester",
    # "Большой Манчестер" — intentionally NOT replaced. User accepts it.
    "Нозерн Куортер": "Northern Quarter",
    "Нортерн Куортер": "Northern Quarter",
    "Спиннингфилдс": "Spinningfields",
    "Чорлтон-кам-Харди": "Chorlton-cum-Hardy",
    "Мосс-Сайд": "Moss Side",
    "Мосс Сайд": "Moss Side",
    "Дин-Лейн": "Dean Lane",
    "Дин Лейн": "Dean Lane",
    "Дин-стрит": "Deansgate",
    "Динсгейт": "Deansgate",
}

# Compile a single alternation; longest forms first so "Олд-Траффорд" wins over "Траффорд".
_RU_TO_EN_PATTERN = re.compile(
    "|".join(
        re.escape(key)
        for key in sorted(_TOPONYM_REPLACEMENTS, key=len, reverse=True)
    )
)


def restore_english_toponyms(text: str) -> str:
    """Replace Russified GM place names with their English originals.

    Idempotent: applying twice produces the same output. Safe to call on
    text that already contains English toponyms — only Russian-script
    substrings match.
    """
    if not text:
        return text
    return _RU_TO_EN_PATTERN.sub(lambda m: _TOPONYM_REPLACEMENTS[m.group(0)], text)
