"""Block-routing logic: freshness, today_focus promotion, ticket horizon.

`_freshness_status` and `_resolve_primary_block` decide where a fresh
candidate lands. `_promote_to_today_focus` (and helpers) is the
"pull-up" pass that ensures Что важно сегодня has substantive material.
`_adjust_ticket_radar_block` demotes far-future ticket items.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import re

from news_digest.pipeline.common import now_london

from .dates import _parse_datetime_value
from .filters import _has_gm_token
from .sources import SourceDef


def _freshness_status(source: SourceDef, published_at: str | None) -> str:
    """Classify a candidate's publication time relative to today's window.

    Semantically, 'fresh_24h' means 'happened yesterday or today (London)'
    rather than the literal 'within the last 24 hours' — at any scan time
    items published since yesterday midnight London count as fresh, so an
    item from yesterday afternoon is fresh whether the digest runs at
    08:00 or 18:00. The label is kept for backward compatibility with
    downstream report fields (`fresh_last_24h_count` etc.).
    """

    if source.primary_block != "last_24h":
        return "not_applicable"
    if not published_at:
        return "unknown"
    published_dt = _parse_datetime_value(published_at)
    if published_dt is None:
        return "unknown"
    now = now_london()
    if published_dt > now + timedelta(minutes=5):
        return "future"
    yesterday_midnight = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if published_dt >= yesterday_midnight:
        return "fresh_24h"
    return "stale"


def _resolve_primary_block(source: SourceDef, published_at: str | None) -> str:
    if source.primary_block != "last_24h":
        return source.primary_block
    return "last_24h" if _freshness_status(source, published_at) == "fresh_24h" else "today_focus"


_TODAY_FOCUS_KEYWORDS: tuple[str, ...] = (
    "strike",
    "industrial action",
    "walkout",
    "closure",
    "closed",
    "cancel",
    "fire",
    "blaze",
    "smoke",
    "warning",
    "evacuat",
    "police",
    "gmp",
    "stab",
    "charged",
    "arrest",
    "court",
    "election",
    "council",
    "mayor",
    "metrolink",
    "airport",
    "piccadilly",
    "victoria",
    "weather warning",
)


def _today_facing_practical_angle(candidate: dict) -> str:
    """Return a today-facing practical angle for a promoted city candidate.

    Avoids the "Включать только…" placeholder that writer.py drops as
    insufficiently actionable, and tailors the message to the topic.
    """

    blob = (
        f"{str(candidate.get('title') or '')} {str(candidate.get('summary') or '')}"
    ).lower()
    if any(token in blob for token in ("strike", "industrial action", "walkout", "cancel", "closure", "closed")):
        return "Если это касается вашего сервиса или маршрута сегодня, уточнить статус заранее."
    if any(token in blob for token in ("fire", "blaze", "smoke", "warning", "evacuat", "windows closed")):
        return "Проверить, остаётся ли предупреждение активным сегодня и касается ли оно района."
    if any(token in blob for token in ("police", "gmp", "stab", "charged", "arrest", "court")):
        return "Сверить с официальным policing update; учесть, как это влияет на район сегодня."
    if any(token in blob for token in ("election", "council", "mayor")):
        return "Учитывать, что сегодня это влияет на повестку городской политики."
    if any(token in blob for token in ("train", "metrolink", "airport", "bus", "rail")):
        return "Проверить, влияет ли это на поездки сегодня перед выходом."
    return "Сверить, остаётся ли история актуальной для сегодняшнего дня перед публикацией."


def _today_focus_score(candidate: dict) -> int:
    """Score a candidate's fitness for today_focus promotion.

    Higher = more important. Items must score at least 15 to be promoted,
    so we don't escalate generic stories.
    """

    score = 0
    if candidate.get("freshness_status") == "fresh_24h":
        score += 100
    elif candidate.get("freshness_status") == "stale":
        score += 5  # tolerate stale only if topical
    if candidate.get("category") == "gmp":
        score += 25
    elif candidate.get("category") == "public_services":
        score += 15
    blob = (
        f"{str(candidate.get('title') or '')} {str(candidate.get('summary') or '')}"
    ).lower()
    for keyword in _TODAY_FOCUS_KEYWORDS:
        if keyword in blob:
            score += 10
            break
    if _has_gm_token(blob):
        score += 5
    return score


def _promote_to_today_focus(candidates: list[dict]) -> None:
    """Ensure 'Что важно сегодня' has at least 2 substantive items.

    Substantive = practical_angle does NOT start with the writer's
    auto-skip prefix 'Включать только…'. If the block is thinner than
    that, mutate the strongest qualifying media_layer/gmp candidate's
    primary_block to today_focus and rewrite its practical_angle to a
    today-facing variant. Conservative: nothing happens unless the
    candidate scores >= 15 and is fresh_24h or topical.
    """

    substantive = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("primary_block") == "today_focus"
        and not str(candidate.get("practical_angle") or "").startswith("Включать только")
    ]
    if len(substantive) >= 2:
        return

    needed = 2 - len(substantive)
    promotion_pool = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("category") in {"media_layer", "gmp", "council"}
        and candidate.get("primary_block") in {"last_24h", "city_watch"}
        and not candidate.get("promoted_to_today_focus")
    ]
    promotion_pool.sort(key=_today_focus_score, reverse=True)

    promoted_fingerprints = {
        str(candidate.get("fingerprint") or "") for candidate in substantive
    }

    for candidate in promotion_pool[:needed]:
        if _today_focus_score(candidate) < 15:
            return  # nothing strong enough — fail closed via existing fallback layer
        fingerprint = str(candidate.get("fingerprint") or "")
        if fingerprint and fingerprint in promoted_fingerprints:
            continue
        candidate["primary_block"] = "today_focus"
        candidate["promoted_to_today_focus"] = True
        candidate["practical_angle"] = _today_facing_practical_angle(candidate)
        existing_reason = str(candidate.get("reason") or "").strip()
        promotion_note = "Promoted to today_focus by today_focus_policy."
        candidate["reason"] = (
            f"{existing_reason} | {promotion_note}".strip(" |") if existing_reason else promotion_note
        )
        if fingerprint:
            promoted_fingerprints.add(fingerprint)


_TICKET_DATE_PATTERN = re.compile(
    r"\b(?:mon|tue|wed|thu|fri|sat|sun)?\s*"
    r"(\d{1,2})\s+"
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"
    r"\s+(20\d{2})",
    re.IGNORECASE,
)
_TICKET_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_TICKET_HORIZON_DAYS = 60


def _ticket_event_max_date(title: str) -> datetime | None:
    """Return the latest event date mentioned in a venues/tickets title.

    Used to demote items whose first show is months away, so the
    Билеты / Ticket Radar block focuses on near-term on-sale moments.
    """

    latest: datetime | None = None
    for match in _TICKET_DATE_PATTERN.finditer(str(title or "")):
        day_str, month_str, year_str = match.group(1), match.group(2).lower()[:3], match.group(3)
        month = _TICKET_MONTHS.get(month_str)
        if month is None:
            continue
        try:
            candidate = datetime(int(year_str), month, int(day_str), 12, 0, tzinfo=now_london().tzinfo)
        except ValueError:
            continue
        if latest is None or candidate > latest:
            latest = candidate
    return latest


def _adjust_ticket_radar_block(candidate: dict) -> None:
    """Demote ticket items whose earliest date is past the radar horizon.

    Items > 60 days out drop to 'future_announcements' (Дальние анонсы).
    Items entirely in the past are excluded (include=False).
    """

    if candidate.get("primary_block") != "ticket_radar":
        return
    title = str(candidate.get("title") or "")
    latest = _ticket_event_max_date(title)
    if latest is None:
        return
    today_dt = now_london()
    if latest < today_dt - timedelta(days=1):
        candidate["include"] = False
        candidate["reason"] = (
            "Ticket radar candidate excluded because all dated occurrences are in the past."
        )
        return
    days_out = (latest - today_dt).days
    if days_out > _TICKET_HORIZON_DAYS:
        candidate["primary_block"] = "future_announcements"
        existing_reason = str(candidate.get("reason") or "").strip()
        note = f"Demoted from ticket_radar: earliest date is ~{days_out} day(s) away."
        candidate["reason"] = f"{existing_reason} | {note}".strip(" |") if existing_reason else note
