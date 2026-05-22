"""Synthetic candidates that keep required blocks non-empty.

When live scan finds nothing for a required block (weather, or last_24h
with no fresh dated city item), we generate a labelled synthetic candidate so the gate can
distinguish "we checked and there is nothing material" from "we never
checked".

O2 — Synthetic Freshness Gate
─────────────────────────────
Every synthetic candidate carries:
  - ``data_fetched_at`` — ISO timestamp of the *successful* underlying
    fetch (None if no live data was obtained).
  - ``synthetic_stale`` — bool. True ⇒ the upstream source was
    unreachable after 1 initial attempt + 2 retries on BOTH the primary
    URL and the fallback URL. The candidate still ships (so the
    required "Погода" block doesn't break the release gate) but the
    release report flags it as stale.
  - ``synthetic_fetch_attempts`` — total HTTP attempts made.

Policy (agreed): refetch with two retries, then reject. "Reject" here
means: mark the candidate as ``synthetic_stale=True`` so the release
gate downgrades the weather-block-missing-digits *error* to a *warning*
— never block the daily digest send.
"""

from __future__ import annotations

import json
import logging
import time

from news_digest.pipeline.common import fingerprint_for_candidate, now_london, today_london

from .fetch import _fetch_text
from .weather import _extract_met_office_weather


logger = logging.getLogger(__name__)


# 1 initial attempt + 2 retries per URL. Matches the agreed O2 policy:
# "refetch with two retries, then reject".
_SYNTHETIC_FETCH_ATTEMPTS = 3
_SYNTHETIC_RETRY_BACKOFF_SECONDS = 1.5


def _fetch_with_retries(url: str, *, attempts: int = _SYNTHETIC_FETCH_ATTEMPTS) -> tuple[str, int]:
    """Wrap ``_fetch_text`` with O2's refetch policy.

    Returns ``(body, total_attempts)`` on success.
    Raises the last exception after ``attempts`` failed tries.

    ``_fetch_text`` already retries once on transient URLError. We add
    an outer retry loop so the synthetic-source policy is explicit and
    independent of the inner network-layer retry — the inner one is
    counted as part of the same outer attempt.
    """
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return _fetch_text(url), attempt
        except Exception as exc:  # noqa: BLE001 — policy is to retry on any failure.
            last_exc = exc
            logger.info(
                "synthetic fetch attempt %d/%d failed for %s: %s",
                attempt, attempts, url, exc,
            )
            if attempt < attempts:
                time.sleep(_SYNTHETIC_RETRY_BACKOFF_SECONDS)
    assert last_exc is not None  # for type checker
    raise last_exc


def _weather_candidate() -> dict:
    current = now_london()
    weather_url = "https://weather.metoffice.gov.uk/forecast/gcw2hzs1u?new-design=false"
    fallback_url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=53.4808&longitude=-2.2426"
        "&daily=temperature_2m_min,temperature_2m_max,precipitation_probability_max"
        "&timezone=Europe%2FLondon&forecast_days=1"
    )
    draft_line = "• Погода: данные Met Office временно недоступны. Met Office"
    source_url = weather_url
    source_label = "Met Office"

    data_fetched_at: str | None = None
    synthetic_stale = False
    synthetic_warnings: list[str] = []
    total_attempts = 0

    # ── Primary: Met Office (3 attempts) ──────────────────────────────────
    try:
        body, attempts = _fetch_with_retries(weather_url)
        total_attempts += attempts
        min_temp, max_temp, rain_probability, practical = _extract_met_office_weather(body)
        draft_line = (
            f"• Погода: {min_temp}-{max_temp}°C, вероятность осадков до {rain_probability}%. "
            f"{practical} Met Office"
        )
        data_fetched_at = now_london().isoformat()
    except Exception as exc:  # noqa: BLE001 — fall through to fallback.
        total_attempts += _SYNTHETIC_FETCH_ATTEMPTS
        synthetic_warnings.append(
            f"Met Office unreachable after {_SYNTHETIC_FETCH_ATTEMPTS} attempts: {exc}"
        )
        # ── Fallback: Open-Meteo (3 attempts) ──────────────────────────────
        try:
            body, attempts = _fetch_with_retries(fallback_url)
            total_attempts += attempts
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
            data_fetched_at = now_london().isoformat()
        except Exception as fallback_exc:  # noqa: BLE001
            total_attempts += _SYNTHETIC_FETCH_ATTEMPTS
            synthetic_warnings.append(
                f"Open-Meteo unreachable after {_SYNTHETIC_FETCH_ATTEMPTS} attempts: {fallback_exc}"
            )
            synthetic_stale = True
            logger.warning(
                "Weather synthetic: all sources failed after %d total attempts; shipping placeholder.",
                total_attempts,
            )

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
        "reason": (
            "Synthetic weather card; live fetch failed after retries — placeholder shipping."
            if synthetic_stale
            else "Synthetic weather card backed by live fetch."
        ),
        "matched_previous_fingerprint": "",
        "practical_angle": "Актуальный прогноз — на сайте Met Office.",
        "lead": "",
        "event_page_type": "unknown",
        "published_at": current.isoformat(),
        "published_date_london": today_london(),
        "freshness_status": "fresh_24h" if not synthetic_stale else "stale_synthetic",
        "source_health": "dated",
        "draft_line": draft_line,
        # ── O2 freshness markers ─────────────────────────────────────────
        "synthetic": True,
        "data_fetched_at": data_fetched_at,
        "synthetic_stale": synthetic_stale,
        "synthetic_fetch_attempts": total_attempts,
        "synthetic_warnings": synthetic_warnings,
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
