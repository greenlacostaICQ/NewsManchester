"""Met Office HTML parsing for the synthetic weather candidate.

`_extract_met_office_weather` returns (min, max, precip%, practical) from
the Met Office Manchester forecast page. `_met_office_practical_angle`
converts headline/today text + precipitation into a Russian one-liner
appropriate for the digest's weather block.
"""

from __future__ import annotations

from html import unescape
import re

from ..common import today_london
from .summary import _clean_snippet


def _extract_met_office_weather(html_text: str) -> tuple[int, int, int, str]:
    try:
        return _extract_met_office_weather_v2(html_text)
    except RuntimeError:
        pass

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


def _extract_met_office_weather_v2(html_text: str) -> tuple[int, int, int, str]:
    """Parse the current Met Office forecast-table markup for today's section."""
    day = today_london()
    section_match = re.search(
        rf'<div id="{re.escape(day)}" class="forecast-table-section">(.*?)(?=<div id="20\d{{2}}-\d{{2}}-\d{{2}}" class="forecast-table-section">|</div>\s*</div>\s*<script)',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not section_match:
        raise RuntimeError("Met Office today forecast section not found")
    section = section_match.group(1)

    precip_match = re.search(
        r"<th[^>]*>Chance of precipitation.*?</th>(.*?)</tr>",
        section,
        flags=re.IGNORECASE | re.DOTALL,
    )
    temp_match = re.search(
        r"<th[^>]*>Temperature.*?</th>(.*?)</tr>",
        section,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not precip_match or not temp_match:
        raise RuntimeError("Met Office v2 weather markers not found")

    time_values = [
        int(value)
        for value in re.findall(r'<div class="time-step-hours">(\d{2}):\d{2}</div>', section, flags=re.IGNORECASE)
    ]
    precip_values = []
    for value in re.findall(r"<div[^>]*>(.*?)</div>", precip_match.group(1), flags=re.IGNORECASE | re.DOTALL):
        text = _clean_snippet(unescape(re.sub(r"<[^>]+>", " ", value)))
        if "<5%" in text:
            precip_values.append(0)
            continue
        match = re.search(r"(\d+)%", text)
        if match:
            precip_values.append(int(match.group(1)))
    temp_values = [
        int(value)
        for value in re.findall(r'data-c="\s*(\d+)°"', temp_match.group(1), flags=re.IGNORECASE)
    ]
    if not precip_values or not temp_values:
        raise RuntimeError("Met Office v2 precipitation/temperature values not found")
    if len(time_values) == len(precip_values):
        daytime_precip = [
            value for hour, value in zip(time_values, precip_values)
            if 6 <= hour <= 21
        ]
        if daytime_precip:
            precip_values = daytime_precip

    symbols = " ".join(
        _clean_snippet(unescape(value))
        for value in re.findall(r'<img[^>]+alt="([^"]+)"', section, flags=re.IGNORECASE)
    )
    practical = _met_office_practical_angle("", symbols, max(precip_values))
    return min(temp_values), max(temp_values), max(precip_values), practical


def _met_office_practical_angle(headline: str, today_text: str, precip_max: int) -> str:
    blob = f"{headline} {today_text}".lower()
    if any(token in blob for token in ("amber warning", "yellow warning", "red warning", "flood")):
        return "Проверить, действует ли предупреждение Met Office в вашем районе."
    # Precipitation probability is not rain intensity. A 60-70% hourly
    # chance can still mean scattered showers or a mostly dry day at the
    # reader's exact location, so avoid categorical "heavy rain" wording
    # unless the Met Office prose itself says heavy/persistent rain.
    if precip_max >= 60 and any(token in blob for token in ("heavy rain", "persistent rain", "prolonged rain")):
        return "Возможны продолжительные или сильные осадки; поездки держите с запасом, зонт или непромокаемая куртка пригодятся."
    if precip_max >= 60:
        return "Для прогулок и пересадок держите защиту от дождя под рукой."
    if precip_max >= 30:
        return "Во второй половине дня возможны локальные осадки."
    if "showers" in blob:
        return "Днём возможны кратковременные дожди."
    if "sunny" in blob or "bright" in blob:
        return "Днём сухо с прояснениями."
    return "Без резких перемен погоды."
