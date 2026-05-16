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
    """Decide which block a candidate lands in based on source config + freshness.

    Previously stale items from last_24h-sources defaulted to ``today_focus``,
    which is the WRONG direction — "Что важно сегодня" should be today's
    fresh news, not yesterday's leftovers. Stale items now go to
    ``city_watch`` (the catch-all radar block) so today_focus stays
    reserved for promotion-pass output.
    """
    if source.primary_block != "last_24h":
        return source.primary_block
    if _freshness_status(source, published_at) == "fresh_24h":
        return "last_24h"
    return "city_watch"


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


_AWARENESS_TOKENS = re.compile(
    r"\b(awareness week|awareness month|mental health awareness|deaf awareness|"
    r"cancer awareness|heart awareness|diabetes awareness|stroke awareness|"
    r"week \d{4}|month \d{4})\b",
    re.IGNORECASE,
)


def _is_awareness_item(candidate: dict) -> bool:
    blob = f"{candidate.get('title', '')} {candidate.get('summary', '')}".lower()
    return bool(_AWARENESS_TOKENS.search(blob))


_TODAY_FOCUS_TARGET = 3  # editorial minimum, was 2
_TODAY_FOCUS_NORMAL_SCORE = 15
_TODAY_FOCUS_FAILSAFE_SCORE = 5  # accept weaker candidates rather than ship empty


def _promote_to_today_focus(candidates: list[dict]) -> None:
    """Ensure 'Что важно сегодня' has at least _TODAY_FOCUS_TARGET substantive items.

    Substantive = not an awareness press release and not the auto-skip
    "Включать только…" placeholder. Routine GMMH/NHS press releases
    that just happened to be tagged today_focus by their source aren't
    counted as enough — we still pull in real news on top.

    Two-pass promotion:

    1. NORMAL pass: pull candidates scoring ≥ 15 (fresh_24h news with
       GM/topical signals). Fills the bulk of the block on a normal day.

    2. FAIL-SAFE pass: if today_focus would still ship empty or with
       only 1 item after the normal pass, lower the bar to score ≥ 5
       and promote the best available media_layer/gmp/council item.
       Better a slightly off-target news in "Что важно сегодня" than
       an empty block that breaks the required-block invariant.

    Net effect: today_focus is never empty when last_24h has anything.
    """

    substantive = _today_focus_substantive(candidates)
    if len(substantive) >= _TODAY_FOCUS_TARGET:
        return

    promoted_fingerprints = {
        str(c.get("fingerprint") or "") for c in substantive
    }

    def _do_promote(threshold: int, slots: int) -> int:
        pool = [
            c for c in candidates
            if isinstance(c, dict)
            and c.get("include")  # only promote items that will actually publish
            and c.get("category") in {"media_layer", "gmp", "council"}
            and c.get("primary_block") in {"last_24h", "city_watch"}
            and not c.get("promoted_to_today_focus")
            and str(c.get("fingerprint") or "") not in promoted_fingerprints
        ]
        pool.sort(key=_today_focus_score, reverse=True)
        promoted_count = 0
        for c in pool:
            if promoted_count >= slots:
                break
            if _today_focus_score(c) < threshold:
                break  # pool is sorted; nothing below either
            fp = str(c.get("fingerprint") or "")
            c["primary_block"] = "today_focus"
            c["promoted_to_today_focus"] = True
            c["practical_angle"] = _today_facing_practical_angle(c)
            existing = str(c.get("reason") or "").strip()
            note = f"Promoted to today_focus (threshold={threshold})."
            c["reason"] = f"{existing} | {note}".strip(" |") if existing else note
            if fp:
                promoted_fingerprints.add(fp)
            promoted_count += 1
        return promoted_count

    # Pass 1 — normal threshold.
    needed = _TODAY_FOCUS_TARGET - len(substantive)
    _do_promote(_TODAY_FOCUS_NORMAL_SCORE, needed)

    # Pass 2 — fail-safe. Recount substantive (promotion may have added some).
    substantive = _today_focus_substantive(candidates)
    if len(substantive) >= 1:
        return  # at least one real news item is fine — don't dilute further
    needed = max(1, _TODAY_FOCUS_TARGET - len(substantive))
    _do_promote(_TODAY_FOCUS_FAILSAFE_SCORE, needed)


def _today_focus_substantive(candidates: list[dict]) -> list[dict]:
    """Today_focus items that are real news (not awareness/PR boilerplate)."""
    return [
        c for c in candidates
        if isinstance(c, dict)
        and c.get("primary_block") == "today_focus"
        and not str(c.get("practical_angle") or "").startswith("Включать только")
        and not _is_awareness_item(c)
    ]


_TRANSIT_DISRUPTION_RE = re.compile(
    r'\b(no\s+trams?|trams?\s+(not|won\'t)\s+(run|operate)|line\s+closure|'
    r'metrolink\s+(suspended|closed|disruption|closure|replacement|works)|'
    r'replacement\s+bus\s+service|track\s+replacement|'
    r'two\s+weeks?|several\s+weeks?)\b',
    re.IGNORECASE,
)
_TRANSIT_SUBJECT_RE = re.compile(
    r'\b(metrolink|trams?|bee\s+network|northern|transpennine)\b',
    re.IGNORECASE,
)
_TRANSIT_ROUTE_SPECIFICITY_RE = re.compile(
    r"\b(?:bury|rochdale|oldham|eccles|ashton|airport|trafford\s+park|"
    r"east\s+didsbury|altrincham)\s+line\b|"
    r"\bbetween\s+[A-Z][A-Za-z' -]{2,}\s+and\s+[A-Z][A-Za-z' -]{2,}\b|"
    r"\b(?:victoria|piccadilly|crumpsall|rochdale\s+town\s+centre|bury\s+interchange)\b",
    re.IGNORECASE,
)


def _reroute_media_transit_to_transport(candidates: list[dict]) -> None:
    """Move media_layer/city_news articles about Metrolink/transit closures to transport block.

    The TfGM live-alerts feed only covers currently-active alerts. Planned multi-day
    closures (e.g. "no trams on Bury line for two weeks") often surface first via
    media sources (The Manc, BBC Manchester) in the media_layer category. This pass
    detects them and moves them to the transport block so they sit alongside live alerts.
    """
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        if candidate.get("primary_block") not in {"last_24h", "city_watch"}:
            continue
        if candidate.get("category") not in {"media_layer", "city_news"}:
            continue
        blob = (
            f"{str(candidate.get('title') or '')} "
            f"{str(candidate.get('summary') or '')} "
            f"{str(candidate.get('lead') or '')} "
            f"{str(candidate.get('evidence_text') or '')}"
        )
        if (
            _TRANSIT_DISRUPTION_RE.search(blob)
            and _TRANSIT_SUBJECT_RE.search(blob)
            and _TRANSIT_ROUTE_SPECIFICITY_RE.search(blob)
        ):
            candidate["primary_block"] = "transport"
            existing_reason = str(candidate.get("reason") or "").strip()
            note = "Rerouted media_layer transit disruption to transport block."
            candidate["reason"] = (
                f"{existing_reason} | {note}".strip(" |") if existing_reason else note
            )


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

# Matches "event_date=YYYY-MM-DD" or "public_onsale=YYYY-MM-DD" in Ticketmaster summary fields.
_SUMMARY_ISODATE_PATTERN = re.compile(
    r'\b(event_date|public_onsale)=(\d{4}-\d{2}-\d{2})'
)


def _parse_summary_field_date(summary: str, field: str) -> datetime | None:
    """Extract a date value from the structured summary field (e.g. 'event_date=2026-10-05')."""
    for m in _SUMMARY_ISODATE_PATTERN.finditer(summary):
        if m.group(1) == field:
            try:
                d = datetime.strptime(m.group(2), "%Y-%m-%d")
                return d.replace(tzinfo=now_london().tzinfo)
            except ValueError:
                return None
    return None


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

    For onsale items: if the public_onsale date has already passed, the item
    is no longer a "ticket radar" (start-of-sale) candidate. We check the
    event_date and either demote to future_announcements (event still ahead)
    or drop (event also past).
    """

    if candidate.get("primary_block") != "ticket_radar":
        return

    summary = str(candidate.get("summary") or "")
    today_dt = now_london()

    if "ticket_signal=onsale" in summary.lower():
        onsale_dt = _parse_summary_field_date(summary, "public_onsale")
        if onsale_dt is not None and onsale_dt < today_dt:
            # The on-sale window already opened — this is no longer a radar alert.
            event_dt = _parse_summary_field_date(summary, "event_date")
            if event_dt is None or event_dt < today_dt - timedelta(days=1):
                candidate["include"] = False
                candidate["reason"] = (
                    "Onsale date is in the past and event date has passed or is missing."
                )
            else:
                days_out = (event_dt - today_dt).days
                candidate["primary_block"] = "future_announcements"
                existing_reason = str(candidate.get("reason") or "").strip()
                note = f"Onsale already open; event ~{days_out} day(s) away, moved to future_announcements."
                candidate["reason"] = f"{existing_reason} | {note}".strip(" |") if existing_reason else note
        return

    title = str(candidate.get("title") or "")
    latest = _ticket_event_max_date(title)
    if latest is None:
        return
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
