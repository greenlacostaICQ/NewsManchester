from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import threading
import time
from urllib import error, parse, request

from news_digest.pipeline.common import now_london, read_json, write_json


MUSIC_ENTITY_RE = re.compile(
    r"\b(?:singer|songwriter|musician|rapper|band|group|duo|dj|producer|"
    r"composer|orchestra|comedian|actor|actress|performer|artist|vocalist)\b",
    re.IGNORECASE,
)

NON_ARTIST_EVENT_RE = re.compile(
    r"\b(?:venue premium tickets|premium tickets|tribute|film with live orchestra|"
    r"games in concert|with band and singers|stunt show|bottomless|club night|after party|day party)\b",
    re.IGNORECASE,
)

_CACHE_MEM: dict[str, dict] = {}
_CACHE_VERSION = 3
_YOUTUBE_SEARCH_DAILY_LIMIT = 95

# Minimum seconds between calls to each external API, enforced across all
# worker threads. MusicBrainz documents ~1 request/second per IP — the others
# are lenient. This is a technical anti-ban/throttle gate, NOT a coverage cap:
# no artist is dropped, the queue just drains at a sustainable rate.
_API_MIN_INTERVAL = {
    "musicbrainz": 1.1,
    "wikidata": 0.15,
    "youtube": 0.1,
    "lastfm": 0.1,
}


class _ApiThrottle:
    """Per-host minimum-interval gate, thread-safe."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next: dict[str, float] = {}

    def wait(self, host: str) -> None:
        interval = _API_MIN_INTERVAL.get(host, 0.0)
        if interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            earliest = self._next.get(host, 0.0)
            sleep_for = earliest - now
            self._next[host] = max(now, earliest) + interval
        if sleep_for > 0:
            time.sleep(sleep_for)


_THROTTLE = _ApiThrottle()


class _YoutubeSearchBudget:
    """Persisted, thread-safe guard for YouTube's daily search quota.

    Known channel IDs use channels.list and never spend this budget. Only the
    expensive discovery fallback (search.list) takes a token. The state object
    is part of ticket_notability_cache.json, so workflow reruns on the same day
    cannot accidentally start from zero again.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[str, object] = {}

    def bind(self, state: dict, day: str) -> None:
        with self._lock:
            if str(state.get("date") or "") != day:
                state.clear()
                state.update({"date": day, "search_calls": 0})
            state.setdefault("search_calls", 0)
            self._state = state

    def acquire(self) -> bool:
        raw_limit = os.environ.get("YOUTUBE_SEARCH_DAILY_LIMIT", "").strip()
        try:
            limit = int(raw_limit) if raw_limit else _YOUTUBE_SEARCH_DAILY_LIMIT
        except ValueError:
            limit = _YOUTUBE_SEARCH_DAILY_LIMIT
        limit = max(0, min(100, limit))
        with self._lock:
            used = int(self._state.get("search_calls") or 0)
            if used >= limit:
                return False
            self._state["search_calls"] = used + 1
            self._state["daily_limit"] = limit
            return True

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return dict(self._state)


_YOUTUBE_SEARCH_BUDGET = _YoutubeSearchBudget()

LINEUP_EVENT_RE = re.compile(
    r"\b(?:festival|open air|open-air|line[- ]?up|weekender)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class TicketNotability:
    artist: str
    kind: str
    tier: str
    confidence: float
    signal: str
    wikidata_id: str = ""
    sitelinks: int = 0
    headliners: tuple[str, ...] = ()
    signals: dict[str, object] | None = None
    event_owner: str = ""


_A_TIER_PUBLIC_BLOCKS = frozenset({
    "ticket_radar",
    "outside_gm_tickets",
    "future_announcements",
    "next_7_days",
})
_A_TIER_NON_EVENT_DROP_MARKERS = (
    "duplicate",
    "merged into",
    "fragment merged",
    "premium/hospitality",
    "premium/package",
    "non-music ticket",
)


def is_a_tier_ticket(candidate: dict | None) -> bool:
    """Recognise A-tier before timing/watch/cap/repeat decisions."""
    if not isinstance(candidate, dict):
        return False
    block = str(candidate.get("primary_block") or "")
    if block not in _A_TIER_PUBLIC_BLOCKS and str(candidate.get("category") or "") != "venues_tickets":
        return False
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    return str(notability.get("tier") or "").strip().upper() == "A"


def a_tier_ticket_policy(candidate: dict | None) -> tuple[bool, str]:
    """Say whether a recognised A-tier row must enter public planning.

    Exact/variant duplicates and non-event upsells stay out. Cross-day repeat,
    watch score, section timing and capacity do not downgrade an active A-tier
    event; hard validity checks still run in the normal contract path.
    """
    if not is_a_tier_ticket(candidate):
        return False, "not_a_tier"
    assert isinstance(candidate, dict)
    if candidate.get("a_tier_collapsed_into"):
        return False, "duplicate_physical_a_tier_event"
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    status = str(event.get("event_status") or candidate.get("event_status") or "").strip().lower()
    if any(marker in status for marker in ("cancelled", "canceled", "postponed", "rescheduled")):
        return False, f"event_status:{status}"
    if not str(event.get("date_start") or event.get("date") or "").strip():
        return False, "missing_event_date"
    from news_digest.pipeline.weekend_inventory import effective_occurrence_window  # noqa: PLC0415

    _, occurrence_end = effective_occurrence_window(candidate)
    if occurrence_end is not None and occurrence_end < now_london().date():
        return False, "event_expired"
    reason_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("reason", "dedupe_reason", "duplicate_reason")
    ).lower()
    if any(marker in reason_blob for marker in _A_TIER_NON_EVENT_DROP_MARKERS):
        return False, "exact_or_variant_duplicate"
    return True, "must_show_before_timing_watch_cap_repeat"


def _clean_artist_name(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(title or "")).strip()
    # Pipe-delimited source titles ("Jason Isbell and the 400 Unit | The
    # Bridgewater Hall") leave a dangling "| The —" in the card; keep only the
    # part before the first pipe.
    cleaned = re.split(r"\s*\|\s*", cleaned, maxsplit=1)[0].strip()
    cleaned = re.split(r"\s+[—-]\s+event\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    cleaned = re.sub(r"\s+[—-]\s+public\s+sale\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:venue premium tickets|premium tickets)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*buy\s+tickets?\s+(?:for\s+)?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"^(?:vip\s+package|resale\s+tickets|official\s+platinum|platinum\s+tickets|"
        r"hospitality\s+packages?)\s*[-–—:]\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s*[-–]\s*(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*\s+)?\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+20\d{2}\s*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    # Promoter / presenter prefix: "On the Waterfront presents Snow Patrol",
    # "Sounds of the City Present The K's" → keep the act after "present(s)".
    presenter = re.search(r"\bpresents?\b\s+(.+)$", cleaned, flags=re.IGNORECASE)
    if presenter and len(presenter.group(1).strip()) >= 3:
        cleaned = presenter.group(1).strip()
    # Support / guest act: "Kings Of Leon Special Guest Snuts Sat 4 Jul 2026
    # Multiple times" → drop from the support act on (it also drags the date
    # noise with it, which is why external identity lookups returned
    # not_found for real headliners on 2026-06-03).
    cleaned = re.split(
        r"\s+(?:with\s+|plus\s+|\+\s*)?(?:very\s+)?special\s+guests?\b",
        cleaned,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    cleaned = re.split(r"\s+(?:ft\.?|feat\.?|featuring)\s+", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    cleaned = re.split(r"\s+(?:\+|plus|with)\s+support\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    # Date / time noise anywhere in the string: "Sat 4 Jul 2026", "4 Jul 2026",
    # "Multiple times" (Co-op Live / Ticketmaster titles carry these inline).
    cleaned = re.sub(
        r"\b(?:mon|tue|wed|thu|fri|sat|sun)\w*\s+\d{1,2}(?:st|nd|rd|th)?\s+[a-z]{3,9}\s+20\d{2}\b",
        " ", cleaned, flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\b\d{1,2}(?:st|nd|rd|th)?\s+[a-z]{3,9}\s+20\d{2}\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bmultiple\s+times\b", " ", cleaned, flags=re.IGNORECASE)
    # "The Weeknd: After Hours ..." is an artist plus tour name; keep artist.
    if ":" in cleaned and not re.search(r"\b(?:festival|live in concert|experience)\b", cleaned, flags=re.IGNORECASE):
        cleaned = cleaned.split(":", 1)[0]
    # "ARTIST - Tour / Subtitle / Date" → keep the artist. Ticket titles append
    # tour names ("- 50th Anniversary Tour"), subtitles and dates after a
    # spaced dash; these are not part of the artist identity used for lookup.
    if re.search(r"\s[-–]\s", cleaned):
        head = re.split(r"\s[-–]\s", cleaned, maxsplit=1)[0].strip()
        if len(head) >= 3 and not re.search(r"\b(?:festival|live in concert|experience)\b", head, flags=re.IGNORECASE):
            cleaned = head
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned.strip(" .,-–—")[:90]


_LINEUP_FIELD_RE = re.compile(
    r"\b(?:line[- ]?up|headliners?|featuring|feat\.?|with special guests?|with guests?)\s*[:=]\s*([^|.;]+)",
    re.IGNORECASE,
)
_LINEUP_SPLIT_RE = re.compile(r"\s*(?:,|;|\+|/|\band\b|\bwith\b|&)\s*", re.IGNORECASE)
_LINEUP_STOP_RE = re.compile(
    r"\b(?:live|tour|festival|open air|open-air|tickets?|premium|venue|doors|show|"
    r"all ages|under 16|orchestra|film|concert|experience|party|band|singers?|cast)\b",
    re.IGNORECASE,
)


def _split_lineup(value: str) -> list[str]:
    names: list[str] = []
    for part in _LINEUP_SPLIT_RE.split(str(value or "")):
        name = _clean_artist_name(part)
        if len(name) < 3:
            continue
        if _LINEUP_STOP_RE.fullmatch(name) or _LINEUP_STOP_RE.search(name) and len(name.split()) <= 2:
            continue
        names.append(name)
    return list(dict.fromkeys(names))


def ticket_headliner_candidates(candidate: dict) -> list[str]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    hint = candidate.get("structured_event_hint") if isinstance(candidate.get("structured_event_hint"), dict) else {}
    names: list[str] = []
    for key in ("headliner", "artist", "performer"):
        text = str(event.get(key) or hint.get(key) or "").strip()
        if text:
            names.extend(_split_lineup(text))
    for key in ("headliners", "artists", "lineup", "performers"):
        values = event.get(key) or hint.get(key)
        if isinstance(values, list):
            names.extend(_split_lineup(", ".join(str(value) for value in values)))
        elif isinstance(values, str):
            names.extend(_split_lineup(values))
    for key in ("attraction", "attractions"):
        values = event.get(key) or hint.get(key) or candidate.get(key)
        if isinstance(values, list):
            for value in values:
                if isinstance(value, dict):
                    names.extend(_split_lineup(str(value.get("name") or value.get("artist") or "")))
                else:
                    names.extend(_split_lineup(str(value)))
        elif isinstance(values, dict):
            names.extend(_split_lineup(str(values.get("name") or values.get("artist") or "")))
        elif isinstance(values, str):
            names.extend(_split_lineup(values))
    blob = " | ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    )
    for match in _LINEUP_FIELD_RE.finditer(blob):
        names.extend(_split_lineup(match.group(1)))
    primary = ticket_artist_name(candidate)
    if primary:
        names.insert(0, primary)
    return list(dict.fromkeys(names))[:8]


def ticket_artist_name(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("event_name") or candidate.get("title") or "").strip()
    return _clean_artist_name(raw)


def ticket_event_owner(candidate: dict, *, kind: str = "") -> str:
    """Return the physical event's owner, never the strongest lineup act.

    A normal show is owned by its primary headliner. A festival/open-air card
    is owned by the festival itself; A-tier acts remain lineup facts inside the
    card and must not rename or multiply the physical event.
    """
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    explicit = str(event.get("event_owner") or "").strip()
    if explicit:
        return _clean_artist_name(explicit)
    resolved_kind = kind or ticket_event_kind(candidate)
    raw = str(event.get("event_name") or candidate.get("title") or "").strip()
    if resolved_kind == "lineup_or_show":
        festival = re.search(r"(.*?\b(?:festival|weekender|open[- ]air))\b", raw, re.IGNORECASE)
        owner = festival.group(1) if festival else raw
        owner = re.sub(r"^.*?\bpresents\b\s*", "", owner, flags=re.IGNORECASE)
        owner = re.split(r"\s+[—–-]\s+(?:event|public\s+sale)\b", owner, maxsplit=1, flags=re.IGNORECASE)[0]
        owner = re.sub(r"\s+\b(?:weekend|day)\s+tickets?\b.*$", "", owner, flags=re.IGNORECASE)
        owner = re.sub(r"\s+", " ", owner).strip(" .,-–—")
        if owner:
            return owner[:90]
    return ticket_artist_name(candidate)


def ticket_event_kind(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    hint = candidate.get("structured_event_hint") if isinstance(candidate.get("structured_event_hint"), dict) else {}
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    if NON_ARTIST_EVENT_RE.search(blob) and len(ticket_headliner_candidates(candidate)) <= 1:
        return "non_artist_show"
    identity_blob = " ".join(
        str(value or "")
        for value in (candidate.get("title"), event.get("event_name"), hint.get("event_name"))
    )
    explicit_lineup = event.get("lineup") or hint.get("lineup")
    if LINEUP_EVENT_RE.search(identity_blob) or (
        isinstance(explicit_lineup, list) and len(explicit_lineup) > 1
    ):
        return "lineup_or_show"
    return "artist"


def _is_lineup_mode(candidate: dict, kind: str) -> bool:
    if kind == "lineup_or_show":
        return True
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    if re.search(r"\bline[- ]?up\s*=", blob, re.IGNORECASE):
        return True
    if str(candidate.get("source_label") or "") == "Heritage Live":
        return True
    return False


def _cache_key(artist: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", artist.lower()).strip()


def _load_cache(path: Path) -> dict:
    cache_id = str(path.resolve())
    if cache_id in _CACHE_MEM:
        return _CACHE_MEM[cache_id]
    payload = read_json(path, {})
    if not isinstance(payload, dict):
        payload = {"version": _CACHE_VERSION, "artists": {}}
    previous_version = int(payload.get("version") or 1)
    payload.setdefault("artists", {})
    if previous_version < 2:
        # One-time blind recheck of every cached A result. Old records allowed
        # Last.fm alone to award A and did not record partial provider failure.
        for record in payload["artists"].values():
            if not isinstance(record, dict) or str(record.get("tier") or "").upper() != "A":
                continue
            record["a_tier_recheck_pending"] = True
    if previous_version < 3:
        # Spotify Development Mode now requires a paid Premium owner account.
        # Remove its stale signals and route any pending/A contract recheck to
        # the free, quota-bounded YouTube reach signal instead.
        for record in payload["artists"].values():
            if not isinstance(record, dict):
                continue
            signals = record.get("signals") if isinstance(record.get("signals"), dict) else {}
            signals.pop("spotify_id", None)
            signals.pop("spotify_name", None)
            signals.pop("spotify_popularity", None)
            signals.pop("spotify_followers", None)
            statuses = signals.get("provider_status")
            if isinstance(statuses, dict):
                statuses.pop("spotify", None)
            record["signals"] = signals
            if str(record.get("tier") or "").upper() == "A" or record.get("a_tier_recheck_pending"):
                record["a_tier_recheck_pending"] = True
        payload["version"] = _CACHE_VERSION
    payload.setdefault("youtube_search_quota", {})
    _YOUTUBE_SEARCH_BUDGET.bind(payload["youtube_search_quota"], now_london().date().isoformat())
    _CACHE_MEM[cache_id] = payload
    return payload


def _wikidata_json(url: str) -> dict:
    req = request.Request(
        url,
        headers={
            "User-Agent": "NewsManchester/1.0 (personal city intelligence; ticket notability)",
            "Accept": "application/json",
        },
    )
    with request.urlopen(req, timeout=4) as response:  # noqa: S310 - public Wikidata API.
        return json.loads(response.read().decode("utf-8"))


def _lookup_wikidata(artist: str) -> dict:
    query = parse.urlencode(
        {
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "limit": "3",
            "search": artist,
        }
    )
    search = _wikidata_json(f"https://www.wikidata.org/w/api.php?{query}")
    for result in search.get("search") or []:
        label = str(result.get("label") or "")
        description = str(result.get("description") or "")
        if not label:
            continue
        # Prefer exact-ish label matches; allow a high-signal music/performer
        # description for names with punctuation variants.
        exactish = _cache_key(label) == _cache_key(artist)
        performerish = bool(MUSIC_ENTITY_RE.search(description))
        if not exactish and not performerish:
            continue
        entity_id = str(result.get("id") or "")
        if not entity_id:
            continue
        details_query = parse.urlencode(
            {
                "action": "wbgetentities",
                "format": "json",
                "ids": entity_id,
                "props": "sitelinks|descriptions|labels|claims",
                "languages": "en",
            }
        )
        details = _wikidata_json(f"https://www.wikidata.org/w/api.php?{details_query}")
        entity = (details.get("entities") or {}).get(entity_id) or {}
        sitelinks = entity.get("sitelinks") or {}
        desc = (
            ((entity.get("descriptions") or {}).get("en") or {}).get("value")
            or description
        )
        if not MUSIC_ENTITY_RE.search(desc) and not performerish:
            continue
        youtube_channel_id = ""
        for claim in ((entity.get("claims") or {}).get("P2397") or []):
            value = (((claim.get("mainsnak") or {}).get("datavalue") or {}).get("value"))
            if isinstance(value, str) and value.strip():
                youtube_channel_id = value.strip()
                break
        return {
            "wikidata_id": entity_id,
            "label": label,
            "description": desc,
            "sitelinks": len(sitelinks),
            "youtube_channel_id": youtube_channel_id,
        }
    return {}


def _musicbrainz_json(url: str) -> dict:
    req = request.Request(
        url,
        headers={
            "User-Agent": "NewsManchester/1.0 (personal city intelligence; ticket notability)",
            "Accept": "application/json",
        },
    )
    with request.urlopen(req, timeout=4) as response:  # noqa: S310 - public MusicBrainz API.
        return json.loads(response.read().decode("utf-8"))


def _lookup_musicbrainz(artist: str) -> dict:
    query = parse.urlencode({"query": f'artist:"{artist}"', "fmt": "json", "limit": "3"})
    payload = _musicbrainz_json(f"https://musicbrainz.org/ws/2/artist/?{query}")
    best: dict = {}
    best_score = 0
    for item in payload.get("artists") or []:
        name = str(item.get("name") or "")
        score = int(item.get("score") or 0)
        if not name or score < best_score:
            continue
        exactish = _cache_key(name) == _cache_key(artist)
        if not exactish and score < 92:
            continue
        best = {
            "musicbrainz_id": str(item.get("id") or ""),
            "musicbrainz_name": name,
            "musicbrainz_score": score,
            "musicbrainz_type": str(item.get("type") or ""),
        }
        best_score = score
    return best


def _youtube_json(resource: str, params: dict[str, object], api_key: str) -> dict:
    query = parse.urlencode({**params, "key": api_key})
    req = request.Request(
        f"https://www.googleapis.com/youtube/v3/{resource}?{query}",
        headers={
            "User-Agent": "NewsManchester/1.0 (personal city intelligence; ticket notability)",
            "Accept": "application/json",
        },
    )
    try:
        with request.urlopen(req, timeout=4) as response:  # noqa: S310 - public YouTube API.
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace").lower()
        if exc.code == 403 and "quota" in body:
            return {"_provider_status": "quota_deferred"}
        raise


def _youtube_identity_key(value: str) -> str:
    cleaned = re.sub(
        r"(?:\s*[-–—]\s*)?(?:official(?:\s+artist)?(?:\s+channel)?|music|topic)$",
        "",
        value.strip(),
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"vevo$", "", cleaned, flags=re.IGNORECASE)
    return _cache_key(cleaned)


def _youtube_channel_match_score(artist: str, channel: dict) -> int:
    snippet = channel.get("snippet") if isinstance(channel.get("snippet"), dict) else {}
    topic_details = channel.get("topicDetails") if isinstance(channel.get("topicDetails"), dict) else {}
    title = str(snippet.get("title") or "").strip()
    description = str(snippet.get("description") or "").lower()
    if not title:
        return -1
    identity_blob = f"{title} {description}".lower()
    if re.search(r"\b(?:fan|fanpage|tribute|covers?|reaction|unofficial)\b", identity_blob):
        return -1
    artist_key = _cache_key(artist)
    raw_exact = _cache_key(title) == artist_key
    normalized_exact = _youtube_identity_key(title) == artist_key
    if not raw_exact and not normalized_exact:
        return -1
    score = 5 if raw_exact else 4
    if re.search(r"\b(?:official|vevo)\b", title, re.IGNORECASE) or "official" in description:
        score += 2
    topics = " ".join(str(x) for x in (topic_details.get("topicCategories") or []))
    if "music" in topics.lower():
        score += 1
    return score


def _youtube_channel_rows(channel_ids: list[str], api_key: str) -> list[dict]:
    unique_ids = list(dict.fromkeys(x for x in channel_ids if x))
    if not unique_ids:
        return []
    payload = _youtube_json(
        "channels",
        {
            "part": "snippet,statistics,topicDetails",
            "id": ",".join(unique_ids[:50]),
            "maxResults": min(50, len(unique_ids)),
        },
        api_key,
    )
    if payload.get("_provider_status"):
        return [payload]
    return [x for x in (payload.get("items") or []) if isinstance(x, dict)]


def _youtube_result(channel: dict, *, identity_source: str, confidence: float) -> dict:
    snippet = channel.get("snippet") if isinstance(channel.get("snippet"), dict) else {}
    statistics = channel.get("statistics") if isinstance(channel.get("statistics"), dict) else {}
    hidden = bool(statistics.get("hiddenSubscriberCount"))
    return {
        "youtube_channel_id": str(channel.get("id") or ""),
        "youtube_channel_name": str(snippet.get("title") or ""),
        "youtube_subscribers": 0 if hidden else int(statistics.get("subscriberCount") or 0),
        "youtube_views": int(statistics.get("viewCount") or 0),
        "youtube_subscribers_hidden": hidden,
        "youtube_identity_source": identity_source,
        "youtube_identity_confidence": confidence,
    }


def _lookup_youtube(artist: str, known_channel_id: str = "") -> dict:
    api_key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        return {"_provider_status": "no_credentials"}
    if known_channel_id:
        rows = _youtube_channel_rows([known_channel_id], api_key)
        if rows and rows[0].get("_provider_status"):
            return rows[0]
        if not rows:
            return {"_provider_status": "no_match"}
        return _youtube_result(rows[0], identity_source="wikidata", confidence=1.0)

    if not _YOUTUBE_SEARCH_BUDGET.acquire():
        return {"_provider_status": "quota_deferred"}
    search = _youtube_json(
        "search",
        {"part": "snippet", "type": "channel", "q": artist, "maxResults": 5},
        api_key,
    )
    if search.get("_provider_status"):
        return search
    ids = [
        str(((item.get("id") or {}).get("channelId")) or "")
        for item in (search.get("items") or [])
        if isinstance(item, dict)
    ]
    rows = _youtube_channel_rows(ids, api_key)
    if rows and rows[0].get("_provider_status"):
        return rows[0]
    ranked = sorted(
        (
            (
                _youtube_channel_match_score(artist, row),
                int(((row.get("statistics") or {}).get("subscriberCount")) or 0),
                row,
            )
            for row in rows
        ),
        key=lambda item: (item[0], item[1]),
        reverse=True,
    )
    if not ranked or ranked[0][0] < 4:
        return {"_provider_status": "no_match"}
    score, _subscribers, best = ranked[0]
    confidence = min(0.98, 0.72 + (score * 0.04))
    return _youtube_result(best, identity_source="youtube_search", confidence=confidence)


def _lookup_lastfm(artist: str) -> dict:
    api_key = os.environ.get("LASTFM_API", "").strip() or os.environ.get("LASTFM_API_KEY", "").strip()
    if not api_key:
        return {"_provider_status": "no_credentials"}
    query = parse.urlencode(
        {
            "method": "artist.getinfo",
            "artist": artist,
            "api_key": api_key,
            "format": "json",
        }
    )
    req = request.Request(
        f"https://ws.audioscrobbler.com/2.0/?{query}",
        headers={
            "User-Agent": "NewsManchester/1.0 (personal city intelligence; ticket notability)",
            "Accept": "application/json",
        },
    )
    with request.urlopen(req, timeout=4) as response:  # noqa: S310 - public Last.fm API.
        payload = json.loads(response.read().decode("utf-8"))
    artist_payload = payload.get("artist") if isinstance(payload.get("artist"), dict) else {}
    stats = artist_payload.get("stats") if isinstance(artist_payload.get("stats"), dict) else {}
    name = str(artist_payload.get("name") or "")
    if name and _cache_key(name) != _cache_key(artist):
        return {}
    return {
        "lastfm_name": name,
        "lastfm_listeners": int(stats.get("listeners") or 0),
        "lastfm_playcount": int(stats.get("playcount") or 0),
    }


def _ticketmaster_signal(candidate: dict, artist: str) -> dict:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    hint = candidate.get("structured_event_hint") if isinstance(candidate.get("structured_event_hint"), dict) else {}
    attractions = event.get("attractions") or hint.get("attractions") or event.get("attraction") or hint.get("attraction") or candidate.get("attractions") or candidate.get("attraction")
    attraction_blob = ""
    if isinstance(attractions, list):
        attraction_blob = " ".join(str(item) for item in attractions)
    elif isinstance(attractions, (str, dict)):
        attraction_blob = str(attractions)
    has_attraction_data = bool(attractions)
    blob = " ".join(
        str(value or "")
        for value in (
            event.get("attraction_id"),
            event.get("attractionId"),
            event.get("attraction_url"),
            event.get("ticketmaster_attraction_id"),
            hint.get("ticketmaster_attraction_id"),
            candidate.get("ticketmaster_attraction_id"),
            candidate.get("ticketmaster_attraction"),
            attraction_blob,
            candidate.get("summary"),
        )
    )
    if artist and (
        has_attraction_data
        or re.search(r"\battraction(?:_?id)?\b\s*[=:]|/attraction/|ticketmaster_attraction", blob, re.IGNORECASE)
    ):
        return {"ticketmaster_attraction": True}
    return {"ticketmaster_attraction": False}


def _tier_from_sitelinks(sitelinks: int) -> tuple[str, float]:
    # A single source may award A only when Wikidata presence is exceptionally
    # high. The old 45-link threshold made a large historical Last.fm/Wiki
    # footprint sufficient without a second contemporary signal.
    if sitelinks >= 80:
        return "A", 0.95
    if sitelinks >= 45:
        return "B", 0.9
    if sitelinks >= 16:
        return "B", 0.85
    if sitelinks >= 5:
        return "C", 0.65
    if sitelinks > 0:
        return "D", 0.45
    return "unknown", 0.0


def _tier_from_signals(signals: dict) -> tuple[str, float, str]:
    tier, confidence = _tier_from_sitelinks(int(signals.get("sitelinks") or 0))
    source = "wikidata_sitelinks" if tier != "unknown" else ""
    mb_score = int(signals.get("musicbrainz_score") or 0)
    tm = bool(signals.get("ticketmaster_attraction"))
    youtube_subscribers = int(signals.get("youtube_subscribers") or 0)
    lastfm_listeners = int(signals.get("lastfm_listeners") or 0)
    youtube_a = youtube_subscribers >= 1_000_000
    lastfm_a = lastfm_listeners >= 1_500_000
    independent_support = lastfm_listeners >= 250_000 or int(signals.get("sitelinks") or 0) >= 16
    if youtube_a and independent_support:
        if tier in {"unknown", "D", "C"}:
            return "A", 0.9, "youtube_plus_independent_signal"
        if tier == "B":
            return "A", 0.93, "youtube_plus_wikidata"
    if youtube_subscribers >= 250_000 or lastfm_listeners >= 250_000 or lastfm_a:
        if tier in {"unknown", "D"}:
            return "B", 0.78, "audience_popularity"
    if youtube_subscribers >= 50_000 or lastfm_listeners >= 50_000:
        if tier == "unknown":
            return "C", 0.62, "audience_popularity"
    if tier == "unknown":
        # MusicBrainz + Ticketmaster proves identity/live-market presence; it
        # is not enough to call the act notable for a personal UK-wide watch.
        # Last.fm/Wiki/YouTube-scale signals must do that promotion.
        if mb_score >= 95 and tm:
            return "C", 0.62, "musicbrainz_ticketmaster_identity"
        if mb_score >= 95:
            return "D", 0.5, "musicbrainz_artist"
        if tm:
            return "C", 0.62, "ticketmaster_attraction"
    elif tier == "D" and mb_score >= 95 and tm:
        return "C", 0.62, "musicbrainz_ticketmaster_identity"
    elif mb_score >= 90 or tm or youtube_subscribers or lastfm_listeners:
        source = f"{source}+multi_source"
        confidence = min(0.99, confidence + 0.04)
    return tier, confidence, source or "not_found"


def _rank_tuple(notability: TicketNotability) -> tuple[int, float, int]:
    tier_rank = {"A": 5, "B": 4, "C": 3, "D": 2, "protected": 6, "unknown": 0}
    return (
        tier_rank.get(notability.tier, tier_rank.get(notability.tier.upper(), 0)),
        notability.confidence,
        notability.sitelinks,
    )


def _artist_notability(
    artist: str,
    kind: str,
    candidate: dict,
    artists_cache: dict,
    now: datetime,
    *,
    allow_network: bool = False,
) -> TicketNotability:
    key = _cache_key(artist)
    cached = artists_cache.get(key)
    tm_signal = _ticketmaster_signal(candidate, artist)
    if isinstance(cached, dict):
        checked_at = str(cached.get("checked_at") or "")
        try:
            checked = datetime.fromisoformat(checked_at)
        except ValueError:
            checked = None
        # Outcome-based recheck window: a confirmed result holds for 30d, a
        # clean "not found" is retried in a week, a transient API failure next
        # run — so a blip never poisons the cache for a month.
        recheck_days = int(cached.get("recheck_days") or 30)
        if (
            checked
            and now - checked <= timedelta(days=recheck_days)
            and not (allow_network and cached.get("a_tier_recheck_pending"))
        ):
            signals = dict(cached.get("signals") or {})
            signals.setdefault("sitelinks", int(cached.get("sitelinks") or 0))
            signals.setdefault("wikidata_id", str(cached.get("wikidata_id") or ""))
            signals.update(tm_signal)
            tier, confidence, signal = _tier_from_signals(signals)
            return TicketNotability(
                artist=artist,
                kind=kind,
                tier=tier,
                confidence=confidence,
                signal=signal,
                wikidata_id=str(cached.get("wikidata_id") or ""),
                sitelinks=int(signals.get("sitelinks") or 0),
                signals=signals,
            )

    # Read-only callers (the writer's render loop) never touch the network: the
    # cache is pre-populated by prefetch_notability before the writer runs, so
    # an un-warmed artist just ships without a notability label this issue and
    # is looked up for the next one. Network happens only via prefetch.
    if not allow_network or os.environ.get("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "").strip() != "1":
        return TicketNotability(artist, kind, "unknown", 0.0, "lookup_disabled", signals=tm_signal)

    provider_status: dict[str, str] = {}

    def _lookup(host: str, fn) -> dict:
        _THROTTLE.wait(host)
        try:
            result = fn(artist)
            marker = str(result.get("_provider_status") or "") if isinstance(result, dict) else ""
            if marker in {"no_credentials", "auth_error", "no_match", "timeout", "quota_deferred"}:
                provider_status[host] = marker
                return {}
            provider_status[host] = "ok" if isinstance(result, dict) and result else "no_match"
            return result if isinstance(result, dict) else {}
        except error.HTTPError as exc:  # pragma: no cover - real provider response.
            provider_status[host] = "auth_error" if exc.code in {401, 403} else "timeout"
            return {}
        except Exception:  # pragma: no cover - network failure is fail-open.
            provider_status[host] = "timeout"
            return {}

    # Short-circuit ladder. Only exceptionally high Wikidata is independently
    # sufficient for A. Every other possible A spends YouTube plus Last.fm so a
    # large historical footprint cannot award A by itself. MusicBrainz —
    # the strict ~1 req/sec service — runs LAST and only if still unknown, so it
    # only ever sees the residual tail, not the whole pool.
    wd = _lookup("wikidata", _lookup_wikidata)
    signals: dict = {
        "sitelinks": int(wd.get("sitelinks") or 0),
        "wikidata_id": str(wd.get("wikidata_id") or ""),
        **tm_signal,
    }
    tier, _conf, _sig = _tier_from_signals(signals)
    if tier != "A":
        yt = _lookup(
            "youtube",
            lambda name: _lookup_youtube(name, str(wd.get("youtube_channel_id") or "")),
        )
        lf = _lookup("lastfm", _lookup_lastfm)
        signals.update(
            {
                "youtube_channel_id": str(yt.get("youtube_channel_id") or ""),
                "youtube_channel_name": str(yt.get("youtube_channel_name") or ""),
                "youtube_subscribers": int(yt.get("youtube_subscribers") or 0),
                "youtube_views": int(yt.get("youtube_views") or 0),
                "youtube_subscribers_hidden": bool(yt.get("youtube_subscribers_hidden")),
                "youtube_identity_source": str(yt.get("youtube_identity_source") or ""),
                "youtube_identity_confidence": float(yt.get("youtube_identity_confidence") or 0.0),
                "lastfm_listeners": int(lf.get("lastfm_listeners") or 0),
                "lastfm_playcount": int(lf.get("lastfm_playcount") or 0),
            }
        )
        tier, _conf, _sig = _tier_from_signals(signals)
        if tier == "unknown":
            mb = _lookup("musicbrainz", _lookup_musicbrainz)
            signals.update(
                {
                    "musicbrainz_id": str(mb.get("musicbrainz_id") or ""),
                    "musicbrainz_score": int(mb.get("musicbrainz_score") or 0),
                    "musicbrainz_type": str(mb.get("musicbrainz_type") or ""),
                }
            )
    signals["provider_status"] = dict(provider_status)

    tier, confidence, signal = _tier_from_signals(signals)
    # Error taxonomy → recheck window. found=30d; clean not_found=7d; transient
    # api_failed=1d (retry next run, don't cache a failure for a month).
    provider_failed = any(
        status in {"no_credentials", "auth_error", "timeout", "quota_deferred"}
        for status in provider_status.values()
    )
    if provider_failed:
        recheck_days = 1
    elif tier != "unknown":
        recheck_days = 30
    else:
        recheck_days = 7
    record = {
        "artist": artist,
        "kind": kind,
        "tier": tier,
        "confidence": confidence,
        "signal": signal,
        "wikidata_id": signals.get("wikidata_id", ""),
        "sitelinks": signals.get("sitelinks", 0),
        "description": str(wd.get("description") or ""),
        "signals": signals,
        "checked_at": now.isoformat(),
        "recheck_days": recheck_days,
    }
    if provider_failed:
        record["a_tier_recheck_pending"] = bool(cached and cached.get("a_tier_recheck_pending"))
    artists_cache[key] = record
    return TicketNotability(
        artist=artist,
        kind=kind,
        tier=tier,
        confidence=confidence,
        signal=signal,
        wikidata_id=signals.get("wikidata_id", ""),
        sitelinks=int(signals.get("sitelinks") or 0),
        signals=signals,
    )


def prefetch_notability(
    candidates: list,
    cache_path: Path | None = None,
    *,
    budget_seconds: float = 75.0,
    max_workers: int = 8,
) -> dict:
    """Populate the notability cache for ticket artists in parallel, BEFORE the
    writer runs, so the render loop only ever reads a warm cache.

    Reuses the same per-artist logic and 30-day cache as the writer, but:
      • runs artists concurrently (thread pool) with per-API rate limits,
      • orders near-term events first, far-future to the tail,
      • skips artists already fresh in cache,
      • stops starting new lookups past a wall-clock budget — un-done artists
        simply stay queued for the next run (no coverage cap, nothing lost).
    Returns a small report for the writer report / logs.
    """
    if os.environ.get("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "").strip() != "1":
        return {"enabled": False, "looked_up": 0, "skipped_fresh": 0, "queued": 0, "deferred_budget": 0}

    cache_path = cache_path or Path("data/state/ticket_notability_cache.json")
    cache = _load_cache(cache_path)
    artists = cache.setdefault("artists", {})
    now = now_london()

    def _days_out(c: dict) -> int:
        event = c.get("event") if isinstance(c.get("event"), dict) else {}
        raw = str(event.get("date") or event.get("date_start") or "")
        try:
            return (datetime.fromisoformat(raw).date() - now.date()).days
        except (ValueError, TypeError):
            return 9999  # undated → tail of the queue

    def _is_fresh(name: str) -> bool:
        rec = artists.get(_cache_key(name))
        if not isinstance(rec, dict):
            return False
        if rec.get("a_tier_recheck_pending"):
            return False
        try:
            checked = datetime.fromisoformat(str(rec.get("checked_at") or ""))
        except ValueError:
            return False
        return now - checked <= timedelta(days=int(rec.get("recheck_days") or 30))

    work: list[tuple[int, str, dict]] = []
    seen: set[str] = set()
    for c in candidates:
        if not isinstance(c, dict):
            continue
        if str(c.get("category") or "") != "venues_tickets" and str(c.get("primary_block") or "") not in {
            "ticket_radar",
            "outside_gm_tickets",
        }:
            continue
        names = ticket_headliner_candidates(c) or [ticket_artist_name(c)]
        proximity = _days_out(c)
        for name in names:
            key = _cache_key(name)
            if not name or key in seen:
                continue
            seen.add(key)
            if _is_fresh(name):
                continue
            work.append((proximity, name, c))

    # Cache migrations route every historical A row through the current
    # YouTube + independent-signal contract. These cache-only rows need no
    # ticket context and stay pending until YouTube is genuinely available.
    for record in artists.values():
        if not isinstance(record, dict) or not record.get("a_tier_recheck_pending"):
            continue
        name = str(record.get("artist") or "").strip()
        key = _cache_key(name)
        if not name or key in seen:
            continue
        seen.add(key)
        work.append((
            9998,
            name,
            {"title": name, "category": "venues_tickets", "primary_block": "ticket_radar", "event": {}},
        ))

    work.sort(key=lambda item: item[0])  # near-term first, far-future last
    skipped_fresh = len(seen) - len(work)
    deadline = time.monotonic() + max(1.0, budget_seconds)
    looked = 0
    deferred = 0
    counter_lock = threading.Lock()
    provider_status_counts: dict[str, dict[str, int]] = {}

    def _task(name: str, c: dict) -> None:
        nonlocal looked, deferred
        if time.monotonic() >= deadline:
            with counter_lock:
                deferred += 1
            return
        result = _artist_notability(name, ticket_event_kind(c), c, artists, now, allow_network=True)
        with counter_lock:
            looked += 1
            statuses = (result.signals or {}).get("provider_status")
            if isinstance(statuses, dict):
                for provider, status in statuses.items():
                    bucket = provider_status_counts.setdefault(str(provider), {})
                    bucket[str(status)] = bucket.get(str(status), 0) + 1

    if work:
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = [executor.submit(_task, name, c) for _, name, c in work]
            for future in futures:
                future.result()
        write_json(cache_path, cache)

    return {
        "enabled": True,
        "looked_up": looked,
        "skipped_fresh": skipped_fresh,
        "queued": len(work),
        "deferred_budget": deferred,
        "provider_status": provider_status_counts,
        "youtube_search_quota": _YOUTUBE_SEARCH_BUDGET.snapshot(),
    }


def enrich_ticket_notability(candidate: dict, cache_path: Path | None = None) -> TicketNotability:
    kind = ticket_event_kind(candidate)
    event_owner = ticket_event_owner(candidate, kind=kind)
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if event_owner:
        event["event_owner"] = event_owner
        if kind == "lineup_or_show":
            event["event_name"] = event_owner
        candidate["event"] = event
    headliners = ticket_headliner_candidates(candidate)
    artist = headliners[0] if headliners else ticket_artist_name(candidate)
    if not artist:
        return TicketNotability("", kind, "unknown", 0.0, "no_artist", event_owner=event_owner)

    if str(candidate.get("primary_block") or "") == "russian_events" or str(candidate.get("category") or "") in {
        "russian_speaking_events",
        "diaspora_events",
    }:
        return TicketNotability(
            artist, kind, "protected", 1.0, "diaspora_protected",
            headliners=tuple(headliners), event_owner=event_owner,
        )

    if kind == "non_artist_show" and len(headliners) <= 1:
        return TicketNotability(
            artist, kind, "D", 0.7, "non_artist_show",
            headliners=tuple(headliners), event_owner=event_owner,
        )

    cache_path = cache_path or Path("data/state/ticket_notability_cache.json")
    cache = _load_cache(cache_path)
    artists = cache.setdefault("artists", {})
    now = now_london()
    # For a normal named-artist show, the first source/title headliner is the
    # truth. Do not let a support act with a stronger cache record become the
    # public artist ("Take That" title rendered as "The Script"). For festival
    # / open-air / explicit lineup cards, ranking across the lineup is correct:
    # it lets a major supporting artist surface the event.
    candidate_names = headliners or [artist]
    lineup_mode = _is_lineup_mode(candidate, kind)
    if lineup_mode:
        individual_names = [
            name for name in candidate_names
            if "+" not in name and not re.search(r"\s+\bwith\b\s+", name, re.IGNORECASE)
        ]
        if individual_names:
            candidate_names = individual_names
    elif artist:
        candidate_names = [artist]
    ranked = [_artist_notability(name, kind, candidate, artists, now) for name in candidate_names]
    best = max(ranked, key=_rank_tuple)
    if not lineup_mode and best.artist != artist:
        best = ranked[0]
    # Read-only path: the cache is owned and written by prefetch_notability,
    # which runs before the writer. The render loop never writes it, and
    # _artist_notability above is called read-only (allow_network defaults off).
    signals = dict(best.signals or {})
    signals["headliner_resolution"] = "lineup_ranked" if lineup_mode else "primary_headliner_locked"
    if lineup_mode:
        signals["a_tier_lineup"] = list(dict.fromkeys(
            item.artist for item in ranked if str(item.tier or "").upper() == "A" and item.artist
        ))
    if len(headliners) > 1 and not lineup_mode:
        signals["ignored_support_candidates"] = [name for name in headliners[1:] if name]
    return TicketNotability(
        artist=best.artist,
        kind=kind if kind != "non_artist_show" or len(headliners) <= 1 else "lineup_or_show",
        tier=best.tier,
        confidence=best.confidence,
        signal=best.signal,
        wikidata_id=best.wikidata_id,
        sitelinks=best.sitelinks,
        headliners=tuple(candidate_names),
        signals=signals,
        event_owner=event_owner,
    )
