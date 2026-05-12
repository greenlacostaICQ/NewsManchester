"""Date parsing utilities used by the collector.

All datetimes are coerced to Europe/London via `now_london().tzinfo`.
Functions here are pure (no I/O) and free of cross-module deps inside
the collector package.
"""

from __future__ import annotations

from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib import parse
import re
import xml.etree.ElementTree as ET

from news_digest.pipeline.common import now_london


def _parse_datetime_value(raw_value: str) -> datetime | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        parsed = None
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=now_london().tzinfo)
    return parsed.astimezone(now_london().tzinfo)


def _parse_datetime_value_flexible(raw_value: str) -> str | None:
    parsed = _parse_datetime_value(raw_value)
    if parsed is not None:
        return parsed.isoformat()
    value = str(raw_value or "").strip()
    if not value:
        return None
    for fmt in ("%A %d %B %Y", "%d %B %Y", "%d %b %Y", "%A, %d %B %Y"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(hour=12, minute=0, tzinfo=now_london().tzinfo).isoformat()
        except ValueError:
            continue
    return None


def _local_noon(year: int, month: int, day: int) -> str | None:
    try:
        return datetime(year, month, day, 12, 0, tzinfo=now_london().tzinfo).isoformat()
    except ValueError:
        return None


def _date_hint_from_text(text: str) -> str | None:
    """Extract a 'DD/MM/YY(YY)' style date from arbitrary anchor text.

    Used by LinkExtractor before titles are cleaned, so that listings
    like 'News | 24/04/26 Greater Manchester residents urged…' don't
    lose their publication date when the prefix is stripped.
    """

    text = str(text or "").strip()
    if not text:
        return None
    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", text)
    if not match:
        return None
    day, month, year = match.groups()
    full_year = int(year) + 2000 if len(year) == 2 else int(year)
    try:
        return _local_noon(full_year, int(month), int(day))
    except ValueError:
        return None


def _published_at_from_title_or_url(title: str, url: str) -> str | None:
    title_match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", title)
    if title_match:
        day, month, year = title_match.groups()
        full_year = int(year) + 2000 if len(year) == 2 else int(year)
        return _local_noon(full_year, int(month), int(day))

    path = parse.urlsplit(url).path.lower()
    numeric = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)", path)
    if numeric:
        year, month, day = numeric.groups()
        return _local_noon(int(year), int(month), int(day))

    month_names = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    named = re.search(r"/(20\d{2})/([a-z]+)/(\d{1,2})(?:/|$)", path)
    if named and named.group(2) in month_names:
        return _local_noon(int(named.group(1)), month_names[named.group(2)], int(named.group(3)))
    return None


def _feed_item_published_at(node: ET.Element) -> str | None:
    candidates = [
        (node.findtext("pubDate") or "").strip(),
        (node.findtext("published") or "").strip(),
        (node.findtext("updated") or "").strip(),
        (node.findtext("{http://purl.org/dc/elements/1.1/}date") or "").strip(),
        (node.findtext("{http://www.w3.org/2005/Atom}published") or "").strip(),
        (node.findtext("{http://www.w3.org/2005/Atom}updated") or "").strip(),
    ]
    for raw_value in candidates:
        parsed = _parse_datetime_value(raw_value)
        if parsed is not None:
            return parsed.isoformat()
    return None
