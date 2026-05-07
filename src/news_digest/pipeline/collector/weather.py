"""Met Office HTML parsing for the synthetic weather candidate.

`_extract_met_office_weather` returns (min, max, precip%, practical) from
the Met Office Manchester forecast page. `_met_office_practical_angle`
converts headline/today text + precipitation into a Russian one-liner
appropriate for the digest's weather block.
"""

from __future__ import annotations

import re

from .summary import _clean_snippet


def _extract_met_office_weather(html_text: str) -> tuple[int, int, int, str]:
    max_match = re.search(
        r'class="tab-temp-high".*?data-c="\s*(\d+)°"',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    min_match = re.search(
        r'class="tab-temp-low".*?data-c="\s*(\d+)°"',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    precip_row = re.search(
        r'<tr class="precipitation-chance-row hourly-table">(.*?)</tr>',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not max_match or not min_match or not precip_row:
        raise RuntimeError("Met Office weather markers not found")
    precip_values = [int(value) for value in re.findall(r'(?:&lt;)?(\d+)%', precip_row.group(1))]
    if not precip_values:
        raise RuntimeError("Met Office precipitation markers not found")
    headline_match = re.search(r"<h4>Headline:</h4>\s*<p>(.*?)</p>", html_text, flags=re.IGNORECASE | re.DOTALL)
    today_match = re.search(r"<h4>Today:</h4>\s*<p>(.*?)</p>", html_text, flags=re.IGNORECASE | re.DOTALL)
    headline = _clean_snippet(headline_match.group(1) if headline_match else "")
    today_text = _clean_snippet(today_match.group(1) if today_match else "")
    practical = _met_office_practical_angle(headline, today_text, max(precip_values))
    return int(min_match.group(1)), int(max_match.group(1)), max(precip_values), practical


def _met_office_practical_angle(headline: str, today_text: str, precip_max: int) -> str:
    blob = f"{headline} {today_text}".lower()
    if any(token in blob for token in ("amber warning", "yellow warning", "red warning", "flood")):
        return "Проверить, действует ли предупреждение Met Office в вашем районе."
    if precip_max >= 60:
        return "После обеда возможны сильные осадки, лучше взять зонт и перепроверить forecast перед выходом."
    if precip_max >= 30:
        return "Во второй половине дня возможны локальные осадки, лучше проверить forecast перед выходом."
    if "showers" in blob:
        return "Днём могут появиться локальные showers, так что прогноз лучше перепроверить перед выходом."
    if "sunny" in blob or "bright" in blob:
        return "Днём заметно комфортнее утра, но утреннюю прохладу всё равно стоит учитывать."
    return "Проверить обновлённый прогноз перед выходом."
