from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
from urllib import parse, request

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

LINEUP_EVENT_RE = re.compile(
    r"\b(?:festival|open air|open-air|presents|with special guest|with guests|"
    r"line[- ]?up|weekender|live in concert)\b",
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
    # noise with it, which is why the Wikidata/Spotify lookup returned
    # not_found for real headliners on 2026-06-03).
    cleaned = re.split(r"\s+(?:with\s+|plus\s+|\+\s*)?special\s+guests?\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
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


def ticket_event_kind(candidate: dict) -> str:
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    if NON_ARTIST_EVENT_RE.search(blob) and len(ticket_headliner_candidates(candidate)) <= 1:
        return "non_artist_show"
    if LINEUP_EVENT_RE.search(blob):
        return "lineup_or_show"
    return "artist"


def _cache_key(artist: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", artist.lower()).strip()


def _load_cache(path: Path) -> dict:
    cache_id = str(path.resolve())
    if cache_id in _CACHE_MEM:
        return _CACHE_MEM[cache_id]
    payload = read_json(path, {})
    if not isinstance(payload, dict):
        payload = {"version": 1, "artists": {}}
    payload.setdefault("version", 1)
    payload.setdefault("artists", {})
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
                "props": "sitelinks|descriptions|labels",
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
        return {
            "wikidata_id": entity_id,
            "label": label,
            "description": desc,
            "sitelinks": len(sitelinks),
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


def _spotify_json(url: str, token: str) -> dict:
    req = request.Request(
        url,
        headers={
            "User-Agent": "NewsManchester/1.0 (personal city intelligence; ticket notability)",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with request.urlopen(req, timeout=4) as response:  # noqa: S310 - public Spotify API.
        return json.loads(response.read().decode("utf-8"))


def _spotify_access_token() -> str:
    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        return ""
    body = parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    req = request.Request(
        "https://accounts.spotify.com/api/token",
        data=body,
        headers={
            "User-Agent": "NewsManchester/1.0 (personal city intelligence; ticket notability)",
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic}",
        },
    )
    with request.urlopen(req, timeout=4) as response:  # noqa: S310 - public Spotify API.
        payload = json.loads(response.read().decode("utf-8"))
    return str(payload.get("access_token") or "")


def _lookup_spotify(artist: str) -> dict:
    token = _spotify_access_token()
    if not token:
        return {}
    query = parse.urlencode({"q": artist, "type": "artist", "limit": "3"})
    payload = _spotify_json(f"https://api.spotify.com/v1/search?{query}", token)
    best: dict = {}
    best_rank = (-1, -1)
    for item in (((payload.get("artists") or {}).get("items")) or []):
        name = str(item.get("name") or "")
        if not name:
            continue
        exactish = _cache_key(name) == _cache_key(artist)
        popularity = int(item.get("popularity") or 0)
        followers = int(((item.get("followers") or {}).get("total")) or 0)
        if not exactish and popularity < 55:
            continue
        rank = (popularity, followers)
        if rank <= best_rank:
            continue
        best = {
            "spotify_id": str(item.get("id") or ""),
            "spotify_name": name,
            "spotify_popularity": popularity,
            "spotify_followers": followers,
        }
        best_rank = rank
    return best


def _lookup_lastfm(artist: str) -> dict:
    api_key = os.environ.get("LASTFM_API", "").strip() or os.environ.get("LASTFM_API_KEY", "").strip()
    if not api_key:
        return {}
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
    if sitelinks >= 45:
        return "A", 0.95
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
    spotify_popularity = int(signals.get("spotify_popularity") or 0)
    spotify_followers = int(signals.get("spotify_followers") or 0)
    lastfm_listeners = int(signals.get("lastfm_listeners") or 0)
    if spotify_popularity >= 78 or spotify_followers >= 2_000_000 or lastfm_listeners >= 1_500_000:
        if tier in {"unknown", "D", "C"}:
            return "A", 0.9, "streaming_popularity"
    if spotify_popularity >= 58 or spotify_followers >= 250_000 or lastfm_listeners >= 250_000:
        if tier in {"unknown", "D"}:
            return "B", 0.78, "streaming_popularity"
    if spotify_popularity >= 42 or spotify_followers >= 50_000 or lastfm_listeners >= 50_000:
        if tier == "unknown":
            return "C", 0.62, "streaming_popularity"
    if tier == "unknown":
        if mb_score >= 95 and tm:
            return "B", 0.78, "musicbrainz_ticketmaster"
        if mb_score >= 95:
            return "D", 0.5, "musicbrainz_artist"
        if tm:
            return "C", 0.62, "ticketmaster_attraction"
    elif tier == "D" and mb_score >= 95 and tm:
        return "B", 0.8, "musicbrainz_ticketmaster"
    elif mb_score >= 90 or tm or spotify_popularity or lastfm_listeners:
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
        if checked and now - checked <= timedelta(days=30):
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

    if os.environ.get("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "").strip() != "1":
        return TicketNotability(artist, kind, "unknown", 0.0, "lookup_disabled", signals=tm_signal)

    try:
        wd = _lookup_wikidata(artist)
    except Exception as exc:  # pragma: no cover - network failure is fail-open.
        wd = {"error": type(exc).__name__}
    try:
        mb = _lookup_musicbrainz(artist)
    except Exception as exc:  # pragma: no cover - network failure is fail-open.
        mb = {"musicbrainz_error": type(exc).__name__}
    try:
        sp = _lookup_spotify(artist)
    except Exception as exc:  # pragma: no cover - network failure is fail-open.
        sp = {"spotify_error": type(exc).__name__}
    try:
        lf = _lookup_lastfm(artist)
    except Exception as exc:  # pragma: no cover - network failure is fail-open.
        lf = {"lastfm_error": type(exc).__name__}

    signals = {
        "sitelinks": int(wd.get("sitelinks") or 0),
        "wikidata_id": str(wd.get("wikidata_id") or ""),
        "musicbrainz_id": str(mb.get("musicbrainz_id") or ""),
        "musicbrainz_score": int(mb.get("musicbrainz_score") or 0),
        "musicbrainz_type": str(mb.get("musicbrainz_type") or ""),
        "spotify_id": str(sp.get("spotify_id") or ""),
        "spotify_popularity": int(sp.get("spotify_popularity") or 0),
        "spotify_followers": int(sp.get("spotify_followers") or 0),
        "lastfm_listeners": int(lf.get("lastfm_listeners") or 0),
        "lastfm_playcount": int(lf.get("lastfm_playcount") or 0),
        **tm_signal,
    }
    tier, confidence, signal = _tier_from_signals(signals)
    record = {
        "artist": artist,
        "kind": kind,
        "tier": tier,
        "confidence": confidence,
        "signal": signal,
        "wikidata_id": signals["wikidata_id"],
        "sitelinks": signals["sitelinks"],
        "description": str(wd.get("description") or ""),
        "signals": signals,
        "checked_at": now.isoformat(),
    }
    artists_cache[key] = record
    return TicketNotability(
        artist=artist,
        kind=kind,
        tier=tier,
        confidence=confidence,
        signal=signal,
        wikidata_id=signals["wikidata_id"],
        sitelinks=signals["sitelinks"],
        signals=signals,
    )


def enrich_ticket_notability(candidate: dict, cache_path: Path | None = None) -> TicketNotability:
    kind = ticket_event_kind(candidate)
    headliners = ticket_headliner_candidates(candidate)
    artist = headliners[0] if headliners else ticket_artist_name(candidate)
    if not artist:
        return TicketNotability("", kind, "unknown", 0.0, "no_artist")

    if str(candidate.get("primary_block") or "") == "russian_events" or str(candidate.get("category") or "") in {
        "russian_speaking_events",
        "diaspora_events",
    }:
        return TicketNotability(artist, kind, "protected", 1.0, "diaspora_protected", headliners=tuple(headliners))

    if kind == "non_artist_show" and len(headliners) <= 1:
        return TicketNotability(artist, kind, "D", 0.7, "non_artist_show", headliners=tuple(headliners))

    cache_path = cache_path or Path("data/state/ticket_notability_cache.json")
    cache = _load_cache(cache_path)
    artists = cache.setdefault("artists", {})
    now = now_london()
    candidate_names = headliners or [artist]
    ranked = [_artist_notability(name, kind, candidate, artists, now) for name in candidate_names]
    best = max(ranked, key=_rank_tuple)
    if os.environ.get("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "").strip() == "1":
        write_json(cache_path, cache)
    return TicketNotability(
        artist=best.artist,
        kind=kind if kind != "non_artist_show" or len(headliners) <= 1 else "lineup_or_show",
        tier=best.tier,
        confidence=best.confidence,
        signal=best.signal,
        wikidata_id=best.wikidata_id,
        sitelinks=best.sitelinks,
        headliners=tuple(candidate_names),
        signals=best.signals,
    )
