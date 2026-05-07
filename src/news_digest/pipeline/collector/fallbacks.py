"""Synthetic candidates that keep required blocks non-empty.

When live scan finds nothing for a required block (transport with no
live disruption, last_24h with no fresh dated city item, weather, short
actions), we generate a labelled synthetic candidate so the gate can
distinguish "we checked and there is nothing material" from "we never
checked".
"""

from __future__ import annotations

import html
import json

from news_digest.pipeline.common import fingerprint_for_candidate, now_london, today_london

from .fetch import _fetch_text
from .summary import _clean_snippet
from .weather import _extract_met_office_weather


def _weather_candidate() -> dict:
    current = now_london()
    weather_url = "https://weather.metoffice.gov.uk/forecast/gcw2hzs1u"
    draft_line = "• Погода: live-прогноз не получен; перед выходом проверьте обновление Met Office. Met Office"
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
                "Проверить обновлённый прогноз перед выходом. Open-Meteo"
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
        "practical_angle": "Проверить обновлённый прогноз перед выходом.",
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
    transport = report.get("categories", {}).get("transport", {})
    if int(transport.get("publishable_count") or 0) > 0:
        return []
    if not transport.get("checked"):
        return []

    candidate = {
        "title": "No major transport disruption detected on morning scan",
        "category": "transport",
        "summary": "На утренней проверке крупных подтверждённых сетевых сбоев по Greater Manchester не видно.",
        "source_url": "https://tfgm.com/travel-updates/travel-alerts",
        "source_label": "TfGM",
        "primary_block": "transport",
        "include": True,
        "dedupe_decision": "new",
        "carry_over_label": "",
        "reason": "Transport fallback created because live scan found no publishable transport candidate.",
        "matched_previous_fingerprint": "",
        "practical_angle": "Если едете по точному маршруту, всё равно перепроверьте live alerts перед выходом.",
        "lead": "На утренней проверке крупных подтверждённых сетевых сбоев по Greater Manchester не видно.",
        "event_page_type": "unknown",
        "published_at": now_london().isoformat(),
        "published_date_london": today_london(),
        "freshness_status": "not_applicable",
        "source_health": "dated",
        "draft_line": (
            "• На утренней проверке крупных подтверждённых сетевых сбоев по Greater Manchester не видно. "
            "Если едете по точному маршруту, всё равно перепроверьте live alerts перед выходом. TfGM"
        ),
    }
    candidate["fingerprint"] = fingerprint_for_candidate(candidate)
    return [candidate]


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


def _short_actions_fallback_candidates(candidates: list[dict]) -> list[dict]:
    if any(candidate.get("primary_block") == "short_actions" for candidate in candidates if isinstance(candidate, dict)):
        return []

    buckets = (
        ("today_focus", "public_services"),
        ("ticket_radar", "venues_tickets"),
        ("transport", "transport"),
        ("last_24h", "city_news"),
    )
    preferred: list[dict] = []
    seen_fingerprints: set[str] = set()
    for block_name, category in buckets:
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if candidate.get("primary_block") != block_name:
                continue
            if category and candidate.get("category") != category:
                continue
            fingerprint = str(candidate.get("fingerprint") or "").strip()
            if fingerprint and fingerprint in seen_fingerprints:
                continue
            if fingerprint:
                seen_fingerprints.add(fingerprint)
            preferred.append(candidate)
            break
    if not preferred:
        return []

    selected = preferred[:2]
    fallback: list[dict] = []
    for index, source in enumerate(selected, start=1):
        title = str(source.get("title") or "").strip()
        source_label = str(source.get("source_label") or "").strip()
        category = str(source.get("category") or "").strip()
        lead = _clean_snippet(str(source.get("lead") or "").strip()) or title
        summary = _clean_snippet(str(source.get("summary") or "").strip())
        practical = _clean_snippet(str(source.get("practical_angle") or "").strip())
        if category == "transport":
            lead = "Утренний транспортный scan не показывает крупных сетевых сбоев"
        elif category == "public_services" and lead:
            lead = f"Сегодня в фокусе: {lead}"
        candidate = {
            "title": title,
            "category": "short_actions",
            "summary": summary or lead or title,
            "source_url": source.get("source_url"),
            "source_label": source_label,
            "primary_block": "short_actions",
            "include": True,
            "dedupe_decision": "new",
            "carry_over_label": "",
            "reason": "Short actions fallback derived from actionable candidate.",
            "matched_previous_fingerprint": "",
            "practical_angle": practical,
            "lead": lead,
            "event_page_type": "unknown",
            "published_at": source.get("published_at"),
            "published_date_london": source.get("published_date_london"),
            "freshness_status": source.get("freshness_status"),
            "source_health": source.get("source_health"),
        }
        if lead:
            parts = [html.escape(lead.rstrip(".")) + "."]
            if practical and practical.lower() != lead.lower():
                parts.append(html.escape(practical.rstrip(".")) + ".")
            if source_label:
                parts.append(f'<a href="{html.escape(str(source.get("source_url") or ""), quote=True)}">{html.escape(source_label)}</a>')
            candidate["draft_line"] = "• " + " ".join(parts)
        candidate["fingerprint"] = fingerprint_for_candidate(candidate)
        fallback.append(candidate)
    return fallback
