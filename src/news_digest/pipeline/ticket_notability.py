from __future__ import annotations

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
    r"games in concert|stunt show|bottomless|club night|after party|day party)\b",
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


def _clean_artist_name(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(title or "")).strip()
    cleaned = re.split(r"\s+[—-]\s+event\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    cleaned = re.sub(r"\s+[—-]\s+public\s+sale\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:venue premium tickets|premium tickets)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*buy\s+tickets?\s+(?:for\s+)?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\s*[-–]\s*(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*\s+)?\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+20\d{2}\s*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    # "The Weeknd: After Hours ..." is an artist plus tour name; keep artist.
    if ":" in cleaned and not re.search(r"\b(?:festival|live in concert|experience)\b", cleaned, flags=re.IGNORECASE):
        cleaned = cleaned.split(":", 1)[0]
    # Tour suffixes are not part of artist identity.
    cleaned = re.sub(
        r"\s+[-–]\s+(?:the\s+)?(?:tour|world\s+tour|uk\s+tour|arena\s+tour|"
        r"anniversary\s+tour|live\s+tour|multimedia\s+tour)\b.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned.strip(" .,-–—")[:90]


def ticket_artist_name(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("event_name") or candidate.get("title") or "").strip()
    return _clean_artist_name(raw)


def ticket_event_kind(candidate: dict) -> str:
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    if NON_ARTIST_EVENT_RE.search(blob):
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


def enrich_ticket_notability(candidate: dict, cache_path: Path | None = None) -> TicketNotability:
    artist = ticket_artist_name(candidate)
    kind = ticket_event_kind(candidate)
    if not artist:
        return TicketNotability("", kind, "unknown", 0.0, "no_artist")

    if str(candidate.get("primary_block") or "") == "russian_events" or str(candidate.get("category") or "") in {
        "russian_speaking_events",
        "diaspora_events",
    }:
        return TicketNotability(artist, kind, "protected", 1.0, "diaspora_protected")

    if kind == "non_artist_show":
        return TicketNotability(artist, kind, "D", 0.7, "non_artist_show")

    cache_path = cache_path or Path("data/state/ticket_notability_cache.json")
    cache = _load_cache(cache_path)
    artists = cache.setdefault("artists", {})
    key = _cache_key(artist)
    cached = artists.get(key)
    now = now_london()
    if isinstance(cached, dict):
        checked_at = str(cached.get("checked_at") or "")
        try:
            checked = datetime.fromisoformat(checked_at)
        except ValueError:
            checked = None
        if checked and now - checked <= timedelta(days=30):
            return TicketNotability(
                artist=artist,
                kind=kind,
                tier=str(cached.get("tier") or "unknown"),
                confidence=float(cached.get("confidence") or 0.0),
                signal=str(cached.get("signal") or "cache"),
                wikidata_id=str(cached.get("wikidata_id") or ""),
                sitelinks=int(cached.get("sitelinks") or 0),
            )

    if os.environ.get("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "").strip() != "1":
        return TicketNotability(artist, kind, "unknown", 0.0, "lookup_disabled")

    try:
        wd = _lookup_wikidata(artist)
    except Exception as exc:  # pragma: no cover - network failure is fail-open.
        wd = {"error": type(exc).__name__}

    sitelinks = int(wd.get("sitelinks") or 0)
    tier, confidence = _tier_from_sitelinks(sitelinks)
    signal = "wikidata_sitelinks" if wd and "error" not in wd else str(wd.get("error") or "not_found")
    record = {
        "artist": artist,
        "kind": kind,
        "tier": tier,
        "confidence": confidence,
        "signal": signal,
        "wikidata_id": str(wd.get("wikidata_id") or ""),
        "sitelinks": sitelinks,
        "description": str(wd.get("description") or ""),
        "checked_at": now.isoformat(),
    }
    artists[key] = record
    write_json(cache_path, cache)
    return TicketNotability(
        artist=artist,
        kind=kind,
        tier=tier,
        confidence=confidence,
        signal=signal,
        wikidata_id=record["wikidata_id"],
        sitelinks=sitelinks,
    )
