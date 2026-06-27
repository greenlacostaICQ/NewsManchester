"""Met Office HTML parsing for the synthetic weather candidate.

`_extract_met_office_weather` returns (min, max, precip%, practical) from
the Met Office Manchester forecast page. `_extract_met_office_weather_facts`
keeps the structured hourly/max/rain/warning facts used by the public weather
contract. `_met_office_practical_angle` converts headline/today text +
precipitation into a Russian one-liner appropriate for the digest's weather
block.
"""

from __future__ import annotations

from html import unescape
import re

from ..common import today_london
from .summary import _clean_snippet


def _weather_warning_labels(*texts: str) -> list[str]:
    blob = " ".join(str(text or "") for text in texts).lower()
    warnings: list[str] = []
    if "red warning" in blob:
        warnings.append("red_warning")
    if "amber warning" in blob:
        warnings.append("amber_warning")
    if "yellow warning" in blob:
        warnings.append("yellow_warning")
    if "flood warning" in blob:
        warnings.append("flood_warning")
    elif "flood" in blob:
        warnings.append("flood_risk")
    return list(dict.fromkeys(warnings))


def _morning_temperature(hourly: list[dict[str, int | str]]) -> int | None:
    if not hourly:
        return None
    for preferred_hour in (8, 9, 7, 10, 6, 11):
        for row in hourly:
            if row.get("hour") == preferred_hour and isinstance(row.get("temperature_c"), int):
                return int(row["temperature_c"])
    morning_values = [
        int(row["temperature_c"])
        for row in hourly
        if isinstance(row.get("hour"), int)
        and 6 <= int(row["hour"]) <= 11
        and isinstance(row.get("temperature_c"), int)
    ]
    if morning_values:
        return round(sum(morning_values) / len(morning_values))
    temperatures = [int(row["temperature_c"]) for row in hourly if isinstance(row.get("temperature_c"), int)]
    return min(temperatures) if temperatures else None


def _weather_facts_from_values(
    *,
    source: str,
    min_temp: int,
    max_temp: int,
    rain_probability: int,
    practical: str,
    hourly: list[dict[str, int | str]] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, object]:
    hourly_rows = list(hourly or [])
    return {
        "schema_version": 1,
        "status": "live",
        "source": source,
        "hourly": hourly_rows,
        "morning_temp_c": _morning_temperature(hourly_rows) if hourly_rows else min_temp,
        "min_temp_c": min_temp,
        "max_temp_c": max_temp,
        "rain_probability_max": rain_probability,
        "warnings": list(warnings or []),
        "practical_angle": practical,
        "placeholder": False,
        "degraded": False,
    }


def _extract_met_office_weather_facts(html_text: str) -> dict[str, object]:
    try:
        return _extract_met_office_weather_facts_v2(html_text)
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
    rain_probability = max(precip_values)
    practical = _met_office_practical_angle(headline, today_text, rain_probability)
    return _weather_facts_from_values(
        source="Met Office",
        min_temp=int(min_match.group(1)),
        max_temp=int(max_match.group(1)),
        rain_probability=rain_probability,
        practical=practical,
        warnings=_weather_warning_labels(headline, today_text),
    )


def _extract_met_office_weather(html_text: str) -> tuple[int, int, int, str]:
    facts = _extract_met_office_weather_facts(html_text)
    return (
        int(facts.get("min_temp_c") or 0),
        int(facts.get("max_temp_c") or 0),
        int(facts.get("rain_probability_max") or 0),
        str(facts.get("practical_angle") or ""),
    )


def _extract_met_office_weather_facts_v2(html_text: str) -> dict[str, object]:
    """Parse the current Met Office forecast-table markup for today's section."""
    day = today_london()
    section_match = re.search(
        rf'<div id="{re.escape(day)}" class="forecast-table-section">(.*?)(?=<div id="20\d{{2}}-\d{{2}}-\d{{2}}" class="forecast-table-section">|</div>\s*</div>\s*<script|$)',
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
    hourly: list[dict[str, int | str]] = []
    if len(time_values) == len(precip_values) == len(temp_values):
        hourly = [
            {
                "hour": hour,
                "temperature_c": temp,
                "rain_probability": rain,
            }
            for hour, temp, rain in zip(time_values, temp_values, precip_values, strict=True)
        ]
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
    return _weather_facts_from_values(
        source="Met Office",
        min_temp=min(temp_values),
        max_temp=max(temp_values),
        rain_probability=max(precip_values),
        practical=practical,
        hourly=hourly,
        warnings=_weather_warning_labels(symbols),
    )


def _extract_met_office_weather_v2(html_text: str) -> tuple[int, int, int, str]:
    facts = _extract_met_office_weather_facts_v2(html_text)
    return (
        int(facts.get("min_temp_c") or 0),
        int(facts.get("max_temp_c") or 0),
        int(facts.get("rain_probability_max") or 0),
        str(facts.get("practical_angle") or ""),
    )


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
        return "Возьмите защиту от дождя, особенно если планируете прогулки или пересадки днём."
    if precip_max >= 30:
        return "Во второй половине дня возможны локальные осадки."
    if "showers" in blob:
        return "Днём возможны кратковременные дожди."
    if "sunny" in blob or "bright" in blob:
        return "Днём сухо с прояснениями."
    return "Без резких перемен погоды."
