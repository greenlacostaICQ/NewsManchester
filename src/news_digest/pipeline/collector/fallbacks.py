"""Synthetic candidates that keep required blocks non-empty.

When live scan finds nothing for a required block (weather, or last_24h
with no fresh dated city item), we generate a labelled synthetic candidate so the gate can
distinguish "we checked and there is nothing material" from "we never
checked".
"""

from __future__ import annotations

import json

from news_digest.pipeline.common import fingerprint_for_candidate, now_london, today_london

from .fetch import _fetch_text
from .weather import _extract_met_office_weather


def _weather_candidate() -> dict:
    current = now_london()
    weather_url = "https://weather.metoffice.gov.uk/forecast/gcw2hzs1u"
    draft_line = "• Погода: данные Met Office временно недоступны. Met Office"
    source_url = "https://weather.metoffice.gov.uk/forecast/gcw2hzs1u"
    source_label = "Met Office"
    try:
        body = _fetch_text(weather_url)
        min_temp, max_temp, rain_probability, practical = _extract_met_office_weather(body)
        draft_line = (
            f"• Погода: {min_temp}-{max_temp}°C, вероятность осадков до {rain_probability}%. "
            f"{practical} Met Office"
        )
    except Exception:
        # Keep a live weather line even if the official parser breaks.
        fallback_url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=53.4808&longitude=-2.2426"
            "&daily=temperature_2m_min,temperature_2m_max,precipitation_probability_max"
            "&timezone=Europe%2FLondon&forecast_days=1"
        )
        try:
            body = _fetch_text(fallback_url)
            payload = json.loads(body)
            daily = payload.get("daily", {})
            min_temp = round(float(daily.get("temperature_2m_min", [0])[0]))
            max_temp = round(float(daily.get("temperature_2m_max", [0])[0]))
            rain_probability = round(float(daily.get("precipitation_probability_max", [0])[0]))
            draft_line = (
                f"• Погода: {min_temp}-{max_temp}°C, вероятность осадков до {rain_probability}%. "
                "Open-Meteo"
            )
            source_url = fallback_url
            source_label = "Open-Meteo"
        except Exception:
            pass

    candidate = {
        "title": f"Weather placeholder for {today_london()}",
        "category": "weather",
        "summary": draft_line.removeprefix("• "),
        "source_url": source_url,
        "source_label": source_label,
        "primary_block": "weather",
        "include": True,
        "dedupe_decision": "new",
        "carry_over_label": "",
        "reason": "Pipeline weather stage placeholder; should be replaced by live weather collector.",
        "matched_previous_fingerprint": "",
        "practical_angle": "Актуальный прогноз — на сайте Met Office.",
        "lead": "",
        "event_page_type": "unknown",
        "published_at": current.isoformat(),
        "published_date_london": today_london(),
        "freshness_status": "fresh_24h",
        "source_health": "dated",
        "draft_line": draft_line,
    }
    candidate["fingerprint"] = fingerprint_for_candidate(candidate)
    return candidate


def _transport_fallback_candidates(report: dict) -> list[dict]:
    # Transport block is optional (not in REQUIRED_BLOCKS). A quiet day with
    # no disruptions produces no transport candidates — that is correct behaviour.
    return []


def _last_24h_fallback_candidates(candidates: list[dict]) -> list[dict]:
    fresh_city = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("primary_block") == "last_24h"
        and candidate.get("category") in {"media_layer", "gmp", "public_services", "council"}
    ]
    if fresh_city:
        return []

    # include=False: this candidate is a gate signal only, not rendered in the digest.
    # Release gate checks fresh_last_24h_candidates count from included candidates,
    # so this fallback should NOT be included — it would create a fake "city news" count.
    candidate = {
        "title": "No fresh dated city/public-affairs item confirmed in last 24 hours",
        "category": "city_news",
        "summary": "На утренней проверке не нашлось подтверждённого city/public-affairs item с верифицированной датой публикации за последние 24 часа.",
        "source_url": "https://www.bbc.com/news/england/manchester",
        "source_label": "BBC Manchester",
        "primary_block": "last_24h",
        "include": False,
        "dedupe_decision": "new",
        "carry_over_label": "",
        "reason": "Last 24h fallback: gate signal only. No fresh dated city/public-affairs candidates survived.",
        "matched_previous_fingerprint": "",
        "practical_angle": "",
        "lead": "",
        "event_page_type": "unknown",
        "published_at": now_london().isoformat(),
        "published_date_london": today_london(),
        "freshness_status": "not_applicable",
        "source_health": "dated",
        "draft_line": "",
    }
    candidate["fingerprint"] = fingerprint_for_candidate(candidate)
    return [candidate]
