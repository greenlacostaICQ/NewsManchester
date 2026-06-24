"""Per-source extraction and enrichment.

`_extract_source_candidates` is the per-source orchestrator: it picks
the right parser (Funnelback JSON / RSS-Atom / HTML anchors), de-dups
URLs, calls `_enrich_item` to thicken thin RSS summaries with article
HTML, and routes each item to the right primary_block.

`_extract_meta_description` and `_extract_article_published_at` are the
HTML enrichment helpers used after re-fetching an article.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from html import unescape
from html.parser import HTMLParser
from urllib import parse
import hashlib
import json
import re
import xml.etree.ElementTree as ET

from news_digest.pipeline.common import clean_url, fingerprint_for_candidate, now_london
from news_digest.pipeline.editorial_contracts import is_major_ticket_venue

from .dates import (
    _date_hint_from_text,
    _feed_item_published_at,
    _parse_datetime_value,
    _parse_datetime_value_flexible,
    _published_at_from_title_or_url,
)
from .fetch import _fetch_text
from .filters import (
    _is_allowed_source_link,
    _is_football_fluff,
    _is_listicle_opening,
    _is_stale_public_service,
    _is_stale_transport,
    _looks_like_candidate_title,
    _looks_like_city_watch_topical,
    _looks_like_diaspora_event_signal,
)
from .routing import (
    _adjust_ticket_radar_block,
    _freshness_status,
    _resolve_primary_block,
)
from .sources import ExtractedItem, SourceDef
from .summary import (
    _clean_snippet,
    _clean_title_text,
    _default_lead,
    _default_practical_angle,
    _default_summary,
    _derive_lead,
    _is_thin_summary,
    _looks_like_active_disruption,
    _source_specific_summary,
)


class LinkExtractor(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self._href: str | None = None
        self._text: list[str] = []
        self.links: list[ExtractedItem] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "img" and self._href:
            attrs_dict = dict(attrs)
            alt = attrs_dict.get("alt")
            if alt:
                self._text.append(alt)
            return
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if href:
            self._href = parse.urljoin(self.base_url, href)
            label = attrs_dict.get("aria-label") or attrs_dict.get("title") or ""
            self._text = [label] if label else []

    def handle_data(self, data: str) -> None:
        if self._href:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._href:
            return
        raw_title = re.sub(r"\s+", " ", " ".join(self._text)).strip()
        title = _clean_title_text(raw_title)
        if _looks_like_candidate_title(title):
            # Capture date hints from the *raw* anchor text before
            # _clean_title_text strips date-like prefixes ('News | 24/04/26
            # …'). Without this, the only chance to learn the date is to
            # re-fetch the article HTML for <meta article:published_time>,
            # which is wasteful when the listing already carries the date.
            published_hint = _date_hint_from_text(raw_title)
            self.links.append(
                ExtractedItem(title=title, url=self._href, published_at=published_hint)
            )
        self._href = None
        self._text = []


def _title_from_slug(slug: str) -> str:
    text = slug.strip("/").split("/")[-1].replace("-", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text.title()


def _extract_slug_link_items(
    base_url: str,
    body: str,
    path_pattern: str,
) -> list[ExtractedItem]:
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for href in re.findall(r'href=["\']([^"\']+)["\']', body, flags=re.IGNORECASE):
        absolute = parse.urljoin(base_url, href)
        parsed = parse.urlsplit(absolute)
        lowered_path = parsed.path.lower().rstrip("/")
        if not re.search(path_pattern, lowered_path):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        title = _clean_title_text(_title_from_slug(parsed.path))
        if _looks_like_candidate_title(title):
            items.append(ExtractedItem(title=title, url=absolute))
    return items


def _extract_meta_description(html_text: str) -> str:
    """Pick the richest summary tag from article HTML.

    Tries og:description (preferred — usually editorial), twitter:description,
    article-specific tags, and standard `<meta name="description">` last. For
    each shape we accept either attribute order (property/name first vs
    content first) and tolerate whitespace variation.
    """

    candidates = (
        r'og:description',
        r'twitter:description',
        r'description',
    )
    for tag_value in candidates:
        # Tolerate (a) attribute order, (b) whitespace, (c) value quoting.
        patterns = (
            rf'<meta\b[^>]*?(?:property|name)\s*=\s*["\']{tag_value}["\'][^>]*?content\s*=\s*["\']([^"\']+)["\']',
            rf'<meta\b[^>]*?content\s*=\s*["\']([^"\']+)["\'][^>]*?(?:property|name)\s*=\s*["\']{tag_value}["\']',
        )
        for pattern in patterns:
            match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
            if match:
                snippet = _clean_snippet(match.group(1))
                if snippet:
                    return snippet
    # Final fallback: first <p> in <article>/<main>/<body>.
    article_match = re.search(
        r"<(?:article|main)[^>]*>(.*?)</(?:article|main)>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    candidate_html = article_match.group(1) if article_match else html_text
    paragraph_match = re.search(
        r"<p[^>]*>(.*?)</p>",
        candidate_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if paragraph_match:
        snippet = _clean_snippet(paragraph_match.group(1))
        if len(snippet) >= 40:
            return snippet
    return ""


def _jsonld_nodes(value: object) -> list[dict]:
    nodes: list[dict] = []
    if isinstance(value, dict):
        nodes.append(value)
        graph = value.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                nodes.extend(_jsonld_nodes(item))
    elif isinstance(value, list):
        for item in value:
            nodes.extend(_jsonld_nodes(item))
    return nodes


def _extract_jsonld_nodes(html_text: str) -> list[dict]:
    nodes: list[dict] = []
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw = re.sub(r"^\s*<!--|-->\s*$", "", match.group(1).strip())
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        nodes.extend(_jsonld_nodes(payload))
    return nodes


def _extract_jsonld_description(html_text: str) -> str:
    for node in _extract_jsonld_nodes(html_text):
        for key in ("description", "articleBody"):
            snippet = _clean_snippet(str(node.get(key) or ""))
            if len(snippet) >= 40:
                return snippet
    return ""


def _extract_jsonld_title(html_text: str) -> str:
    for node in _extract_jsonld_nodes(html_text):
        for key in ("name", "headline"):
            title = _strip_page_title_suffix(_clean_title_text(str(node.get(key) or "")))
            if len(title) >= 8:
                return title
    return ""


def _extract_jsonld_start_date(html_text: str) -> str | None:
    for node in _extract_jsonld_nodes(html_text):
        for key in ("startDate", "datePublished", "dateModified", "uploadDate"):
            parsed = _parse_datetime_value(str(node.get(key) or ""))
            if parsed is not None:
                return parsed.isoformat()
    return None


def _jsonld_type_matches(node: dict, expected: str) -> bool:
    raw = node.get("@type")
    values = raw if isinstance(raw, list) else [raw]
    return any(str(value or "").lower() == expected.lower() for value in values)


def _jsonld_text(value: object) -> str:
    if isinstance(value, dict):
        for key in ("name", "text", "streetAddress", "addressLocality"):
            text = _clean_long_text(str(value.get(key) or ""))
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_jsonld_text(item) for item in value]
        return _clean_long_text(", ".join(part for part in parts if part))
    return _clean_long_text(str(value or ""))


def _jsonld_location_name(value: object) -> str:
    if isinstance(value, dict):
        name = _jsonld_text(value.get("name"))
        if name:
            return name
        address = value.get("address")
        if isinstance(address, dict):
            parts = [
                _jsonld_text(address.get("streetAddress")),
                _jsonld_text(address.get("addressLocality")),
            ]
            return _clean_long_text(", ".join(part for part in parts if part))
        return _jsonld_text(address)
    if isinstance(value, list):
        for item in value:
            name = _jsonld_location_name(item)
            if name:
                return name
    return _jsonld_text(value)


def _jsonld_offer_fields(value: object) -> tuple[str, str]:
    offers = value if isinstance(value, list) else [value]
    prices: list[str] = []
    booking_url = ""
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        raw_price = _clean_long_text(str(offer.get("price") or ""))
        currency = _clean_long_text(str(offer.get("priceCurrency") or ""))
        if raw_price:
            if currency.upper() == "GBP" and not raw_price.startswith("£"):
                prices.append(f"£{raw_price}")
            else:
                prices.append(raw_price)
        if not booking_url:
            booking_url = _clean_long_text(str(offer.get("url") or ""))
    return ("–".join(prices[:2]), booking_url)


def _extract_jsonld_event_hint(html_text: str) -> dict:
    """Pull schema.org/Event fields from JSON-LD when a venue exposes them.

    This is deliberately small and deterministic: the normal event_extraction
    module still owns final normalisation, but JSON-LD gives it clean dates,
    venue and booking facts instead of forcing regex recovery from prose.
    """
    for node in _extract_jsonld_nodes(html_text):
        if not isinstance(node, dict) or not _jsonld_type_matches(node, "Event"):
            continue
        name = _jsonld_text(node.get("name"))
        start = _parse_datetime_value(str(node.get("startDate") or ""))
        end = _parse_datetime_value(str(node.get("endDate") or ""))
        venue = _jsonld_location_name(node.get("location"))
        price, booking_url = _jsonld_offer_fields(node.get("offers"))
        status = _jsonld_text(node.get("eventStatus"))
        out = {
            "schema_source": "jsonld_event",
            "event_name": name,
            "venue": venue,
            "date": start.isoformat() if start else "",
            "date_start": start.isoformat() if start else "",
            "date_end": end.isoformat() if end else "",
            "date_text": start.date().isoformat() if start else "",
            "price": price,
            "booking_url": booking_url,
            "event_status": status,
        }
        return {key: value for key, value in out.items() if str(value or "").strip()}
    return {}


def _jsonld_event_instance_id(name: str, start: str, venue: str) -> str:
    raw = f"{name}|{start[:10]}|{venue}".strip().lower()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12] if raw else ""


def _jsonld_event_node_to_item(source: SourceDef, node: dict, index: int) -> ExtractedItem | None:
    name = _clean_title_text(_jsonld_text(node.get("name")))
    start = _parse_datetime_value(str(node.get("startDate") or ""))
    end = _parse_datetime_value(str(node.get("endDate") or ""))
    venue = _jsonld_location_name(node.get("location"))
    price, booking_url = _jsonld_offer_fields(node.get("offers"))
    event_url = _jsonld_text(node.get("url")) or booking_url or source.url
    event_url = parse.urljoin(source.url, event_url)
    if not name or not start or not (venue or event_url):
        return None
    description = _clean_long_text(_jsonld_text(node.get("description")))
    status = _jsonld_text(node.get("eventStatus"))
    instance_id = _jsonld_event_instance_id(name, start.isoformat(), venue) or f"event-{index}"
    hint = {
        "schema_source": "jsonld_event",
        "event_name": name,
        "venue": venue,
        "date": start.isoformat(),
        "date_start": start.isoformat(),
        "date_end": end.isoformat() if end else "",
        "date_text": start.date().isoformat(),
        "price": price,
        "booking_url": booking_url,
        "event_status": status,
        "event_instance_id": instance_id,
    }
    hint = {key: value for key, value in hint.items() if str(value or "").strip()}
    summary_bits = [
        description,
        f"Event date: {start.date().isoformat()}",
        f"Venue: {venue}" if venue else "",
        f"Price: {price}" if price else "",
        f"Booking: {booking_url}" if booking_url else "",
        f"Status: {status}" if status else "",
    ]
    evidence = _clean_long_text(" ".join(bit for bit in summary_bits if bit))
    event_url_with_instance = event_url
    if clean_url(event_url) == clean_url(source.url):
        event_url_with_instance = f"{event_url}#jsonld-event-{instance_id}"
    return ExtractedItem(
        title=name,
        url=event_url_with_instance,
        published_at=start.isoformat(),
        summary=_summary_from_evidence(evidence) or f"{name} at {venue} on {start.date().isoformat()}",
        lead=f"{name} at {venue}" if venue else name,
        evidence_text=evidence[:6000],
        enrichment_status="ok_jsonld_event",
        structured_event_hint=hint,
    )


def _source_is_event_listing(source: SourceDef) -> bool:
    return (
        source.source_contract in {"event_calendar", "venue_calendar", "ticket_api"}
        or source.report_category in {"culture_weekly", "venues_tickets", "diaspora_events", "professional_events"}
        or source.primary_block in {"weekend_activities", "next_7_days", "ticket_radar", "russian_events"}
    )


def _extract_jsonld_event_items(source: SourceDef, html_text: str) -> list[ExtractedItem]:
    if not _source_is_event_listing(source):
        return []
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for index, node in enumerate(_extract_jsonld_nodes(html_text), start=1):
        if not isinstance(node, dict) or not _jsonld_type_matches(node, "Event"):
            continue
        item = _jsonld_event_node_to_item(source, node, index)
        if item is None:
            continue
        key = str((item.structured_event_hint or {}).get("event_instance_id") or item.url)
        if key in seen:
            continue
        seen.add(key)
        items.append(item)
    return items


def _strip_page_title_suffix(title: str) -> str:
    cleaned = str(title or "").strip()
    cleaned = re.sub(r"\s*[|–-]\s*Albert Hall Manchester\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s*[|–-]\s*(?:Home|Spinningfields|The Makers Market)\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    return cleaned


def _extract_page_title(html_text: str) -> str:
    patterns = (
        r'<meta\b[^>]*?(?:property|name)\s*=\s*["\']og:title["\'][^>]*?content\s*=\s*["\']([^"\']+)["\']',
        r'<meta\b[^>]*?content\s*=\s*["\']([^"\']+)["\'][^>]*?(?:property|name)\s*=\s*["\']og:title["\']',
        r"<title[^>]*>(.*?)</title>",
    )
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        title = _strip_page_title_suffix(_clean_title_text(unescape(match.group(1))))
        if len(title) >= 8:
            return title
    return ""


# Page-chrome that pollutes scraped article bodies and then poisons the
# Russian rewrite. Breadcrumb runs ("News Greater Manchester News Salford"),
# share/byline boilerplate ("Share Save Add as preferred on Google",
# "Jonny Humphries North West PA Media") and engagement prompts gave us
# garbled cards on 2026-05-28 (e.g. Whitefield "застрелили … перелом локтя").
_EVIDENCE_CHROME_TOKENS = (
    "subscribe", "newsletter", "advertisement", "cookies", "privacy policy",
    "sign up to", "sign up for", "add as preferred on google", "share save",
    "follow us", "read more", "most read", "most recent", "trending",
    "pa media", "getty images", "image source", "image caption",
    "skip to content", "back to top", "more on this story",
    "we use cookies", "accept all", "manage consent",
)
# Leading navigation/engagement chrome (case-insensitive), peeled
# repeatedly from the front: "Share Save Add as preferred on Google",
# "News Greater Manchester News Salford", "Comments", etc.
# Geo words (manchester / greater manchester / uk) are only treated as nav
# chrome when paired with "news" — the breadcrumb runs always are ("News
# Greater Manchester News Salford"). A bare leading city word that starts a
# real title ("Manchester Forever at Bowlers…", "UK Garage Night") must NOT
# be peeled, otherwise the event card loses its first word.
_LEAD_CHROME_RE = re.compile(
    r"^(?:\s*(?:share|save|comments?|add as preferred on google|"
    r"home|news|sport|sports|in your area|breaking news|local news|"
    r"greater manchester\s+news|manchester\s+news|uk\s+news|"
    r"what'?s on)\b[\s\W]*)+",
    re.IGNORECASE,
)
# Leading byline: 1-4 capitalised name words followed by an agency/desk
# marker — "Jonny Humphries North West PA Media", "… BBC News".
_BYLINE_PREFIX_RE = re.compile(
    r"^(?:[A-Z][a-z]+\s+){1,4}(?:North West|BBC News|PA Media|"
    r"Local Democracy Reporting Service|Reporter|Correspondent)\b[\s\W]*",
)
# Inline chrome phrases removed anywhere in the body.
_INLINE_CHROME_RE = re.compile(
    r"\b(?:add as preferred on google|pa media|getty images|image source|"
    r"image caption|sign up to[^.]*newsletter|follow us[^.]*|"
    r"read more[^.]*|most read|skip to content|back to top|"
    r"we use cookies[^.]*|accept all cookies)\b",
    re.IGNORECASE,
)
# "JavaScript-disabled" warning that venue ticket pages (Manchester Academy
# and the same CMS family) put at the top of the body. On 2026-05-29 it was
# scraped as the event summary/lead and shown as the concert description
# ("This website makes extensive use of JavaScript…"). Spans without sentence
# punctuation, so the sentence filter can't catch it — strip the whole phrase.
_JS_DISABLED_RE = re.compile(
    r"this website makes extensive use of javascript.*?browser settings\.?",
    re.IGNORECASE | re.DOTALL,
)


def _strip_evidence_chrome(text: str) -> str:
    """Remove navigation breadcrumbs, share/byline boilerplate and
    engagement prompts from scraped article text so the rewrite works
    from clean facts. Site-agnostic; applied to every enriched body."""
    cleaned = str(text or "")
    # 0) Drop the JavaScript-disabled warning ticket pages prepend.
    cleaned = _JS_DISABLED_RE.sub(" ", cleaned).strip()
    # 1) Peel leading nav/engagement chrome, then a byline, repeatedly.
    for _ in range(4):
        before = cleaned
        cleaned = _LEAD_CHROME_RE.sub("", cleaned).strip()
        cleaned = _BYLINE_PREFIX_RE.sub("", cleaned).strip()
        if cleaned == before:
            break
    # 2) Remove inline chrome phrases (keeps the surrounding sentence).
    cleaned = _INLINE_CHROME_RE.sub(" ", cleaned)
    # 3) Drop only SHORT sentences that are pure chrome; never drop a
    #    long sentence just because a chrome token survived inside it.
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    kept = [
        s for s in sentences
        if s.strip()
        and (len(s) > 120 or not any(tok in s.lower() for tok in _EVIDENCE_CHROME_TOKENS))
    ]
    result = re.sub(r"\s+", " ", " ".join(kept)).strip()
    return result or re.sub(r"\s+", " ", cleaned).strip()


def _extract_paragraph_evidence(html_text: str, title: str = "") -> str:
    article_match = re.search(
        r"<(?:article|main)[^>]*>(.*?)</(?:article|main)>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    candidate_html = article_match.group(1) if article_match else html_text
    title_key = re.sub(r"[^a-z0-9а-яё]+", " ", str(title or "").lower()).strip()
    paragraphs: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"<p[^>]*>(.*?)</p>", candidate_html, flags=re.IGNORECASE | re.DOTALL):
        snippet = _clean_snippet(match.group(1))
        if len(snippet) < 45:
            continue
        lowered = snippet.lower()
        if any(token in lowered for token in _EVIDENCE_CHROME_TOKENS):
            continue
        key = re.sub(r"[^a-z0-9а-яё]+", " ", lowered).strip()
        if not key or key in seen or (title_key and key == title_key):
            continue
        seen.add(key)
        paragraphs.append(snippet)
        if len(paragraphs) >= 6:
            break
    return _strip_evidence_chrome(" ".join(paragraphs))


def _clean_long_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.strip(" |-—·•:.,")


def _html_to_visible_text(html_text: str) -> str:
    text = re.sub(r"<(?:script|style|noscript|svg)\b.*?</(?:script|style|noscript|svg)>", " ", html_text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|li|h[1-6]|tr|section|article|main)>", ". ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return _clean_long_text(text)


def _extract_h1_title(html_text: str) -> str:
    match = re.search(r"<h1\b[^>]*>(.*?)</h1>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return _strip_page_title_suffix(_clean_title_text(_clean_snippet(match.group(1))))


def _extract_text_date_hint(text: str) -> str | None:
    patterns = (
        r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+),?\s+(20\d{2})\b",
        r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+),?\s+(20\d{2})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        day, month, year = match.groups()
        parsed = _parse_datetime_value_flexible(f"{int(day)} {month} {year}")
        if parsed:
            return parsed
    return None


def _extract_html_page_event(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Treat a source URL as a single event/market page.

    Several market sites either have JS-rendered listings or no useful listing
    page at all, while their canonical event page has the exact details we need.
    This parser turns that page into one candidate and keeps visible page text as
    evidence so later gates can see date/place/free/booking details.
    """

    title = ""
    for candidate_title in (_extract_jsonld_title(body), _extract_page_title(body), _extract_h1_title(body)):
        candidate_title = _strip_page_title_suffix(_clean_title_text(candidate_title))
        if candidate_title and _looks_like_candidate_title(candidate_title):
            title = candidate_title
            break
    if not title or not _looks_like_candidate_title(title):
        return []
    visible_text = _html_to_visible_text(body)
    paragraph_evidence = _extract_paragraph_evidence(body, title)
    enriched_summary = _extract_jsonld_description(body) or _extract_meta_description(body)
    structured_event_hint = _extract_jsonld_event_hint(body)
    evidence = _clean_long_text(" ".join(part for part in (enriched_summary, paragraph_evidence, visible_text) if part))
    summary = _summary_from_evidence(evidence) or enriched_summary or _default_summary(source, title)
    published_at = (
        _extract_jsonld_start_date(body)
        or _extract_article_published_at(body)
        or _extract_text_date_hint(evidence)
        or _published_at_from_title_or_url(title, source.url)
    )
    return [
        ExtractedItem(
            title=title,
            url=source.url,
            published_at=published_at,
            summary=summary,
            lead=_derive_lead(source, title, summary),
            evidence_text=evidence[:6000],
            enrichment_status="ok_page_event",
            structured_event_hint=structured_event_hint,
        )
    ]


def _summary_from_evidence(evidence: str) -> str:
    cleaned = _clean_snippet(evidence)
    if not cleaned:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    summary = " ".join(sentence for sentence in sentences[:2] if sentence).strip()
    return summary[:700].rstrip()


def _extract_article_published_at(html_text: str) -> str | None:
    patterns = (
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        r'name=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        r'<time[^>]+datetime=["\']([^"\']+)["\']',
    )
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE)
        if not match:
            continue
        parsed = _parse_datetime_value(match.group(1))
        if parsed is not None:
            return parsed.isoformat()
    return None


def _should_enrich_source(source: SourceDef) -> bool:
    if source.source_type in {"html_page_event", "html_the_manc_weekly_events"}:
        return False
    if source.report_category == "transport":
        return False
    if source.source_type == "json_ticketmaster":
        return False
    if source.name in {"Co-op Live", "AO Arena"}:
        return False
    return (
        source.report_category in {
            "media_layer",
            "gmp",
            "public_services",
            "culture_weekly",
            "venues_tickets",
            "diaspora_events",
            "food_openings",
            "football",
            "tech_business",
            "professional_events",
        }
        or source.candidate_category == "council"
    )


_TRUSTED_CARD_ENRICHMENT = {
    "ok_dmn_card", "ok_skiddle_card", "ok_page_event",
    "ok_weekly_section", "ok_sectioned_guide", "ok_gmmh_press_release",
    "ok_heritage_card", "ok_manchester_theatres_card",
}


# #1 A summary can clear the 60-char "thin" bar yet still be too short to
# enrich from (BBC Sport's "Milner has broken the appearance record" = 110
# chars, no match count). Below this floor we fetch the article body so the
# rewrite has real facts instead of just the RSS one-liner.
_MIN_BODY_EVIDENCE_CHARS = 240


def _enrich_item(source: SourceDef, item: ExtractedItem) -> ExtractedItem:
    # Dedicated card extractors (RNCM, Skiddle, DesignMyNight, …) already
    # produce a trustworthy clean title from the listing card. Re-enriching
    # would fetch the event page and overwrite it with the page <title>,
    # which for venues carries a venue suffix — e.g. "Rickie Lee Jones"
    # became "Rickie Lee Jones - Royal Northern College of Music". Keep
    # the card title.
    if str(item.enrichment_status or "") in _TRUSTED_CARD_ENRICHMENT:
        return item
    if not _should_enrich_source(source):
        return item
    summary_thin = _is_thin_summary(item.summary, item.title)
    # #1 Treat a short-but-not-"thin" summary as needing the article body too,
    # so sources like BBC Sport stop shipping a one-line RSS teaser as evidence.
    evidence_too_short = len(str(item.summary or "").strip()) < _MIN_BODY_EVIDENCE_CHARS
    force_fetch = (
        source.report_category in {"media_layer", "gmp", "food_openings"}
        or source.candidate_category == "council"
        or source.name == "Albert Hall Manchester"
    )
    if item.published_at and not summary_thin and not force_fetch and not evidence_too_short:
        summary = _strip_evidence_chrome(_source_specific_summary(source, item.title, item.summary))
        return ExtractedItem(
            title=_clean_title_text(item.title),
            url=item.url,
            published_at=item.published_at,
            summary=summary,
            lead=_strip_evidence_chrome(item.lead or _derive_lead(source, item.title, summary)),
            evidence_text=_strip_evidence_chrome(item.summary),
            enrichment_status="skipped_existing_summary",
            structured_event_hint=dict(item.structured_event_hint or {}),
        )
    try:
        article_html = _fetch_text(item.url)
    except Exception as exc:  # noqa: BLE001 - enrichment is best-effort.
        summary = _strip_evidence_chrome(_source_specific_summary(source, item.title, item.summary))
        return ExtractedItem(
            title=_clean_title_text(item.title),
            url=item.url,
            published_at=item.published_at,
            summary=summary,
            lead=_strip_evidence_chrome(item.lead or _derive_lead(source, item.title, summary)),
            evidence_text=_strip_evidence_chrome(item.summary),
            enrichment_status=f"failed: {exc}",
            structured_event_hint=dict(item.structured_event_hint or {}),
        )

    paragraph_evidence = _extract_paragraph_evidence(article_html, item.title)
    enriched_summary = _extract_jsonld_description(article_html) or _extract_meta_description(article_html)
    enriched_title = _extract_jsonld_title(article_html) or _extract_page_title(article_html)
    structured_event_hint = _extract_jsonld_event_hint(article_html) or dict(item.structured_event_hint or {})
    evidence_text = _strip_evidence_chrome(paragraph_evidence or enriched_summary or item.summary)
    if summary_thin and paragraph_evidence:
        enriched_summary = _summary_from_evidence(paragraph_evidence)
    summary = item.summary
    if summary_thin and enriched_summary and not _is_thin_summary(enriched_summary, item.title):
        summary = enriched_summary
    elif not summary:
        summary = enriched_summary
    summary = _strip_evidence_chrome(_source_specific_summary(source, item.title, summary))
    lead = _strip_evidence_chrome(item.lead or _derive_lead(source, item.title, summary))
    published_at = (
        item.published_at
        or _extract_jsonld_start_date(article_html)
        or _extract_article_published_at(article_html)
        or _published_at_from_title_or_url(item.title, item.url)
    )
    preserve_listing_title = source.name in {
        "RNCM",
        "Manchester Theatres Weekend",
        "Manchester Theatres Next Weekend",
    }
    return ExtractedItem(
        title=item.title if preserve_listing_title else _clean_title_text(unescape(enriched_title or item.title)),
        url=item.url,
        published_at=published_at,
        summary=summary,
        lead=lead,
        # Evidence_text feeds the LLM rewriter, which needs enough body
        # text to write a 250-450 char Russian card with concrete details
        # (date/venue/price for events; £ amounts/names for news). The
        # default _clean_snippet cap is 280 — too tight; pass 2500 here.
        evidence_text=_clean_snippet(evidence_text, max_chars=2500),
        enrichment_status="ok" if evidence_text else "ok_no_evidence",
        structured_event_hint=structured_event_hint,
    )


def _norm_feed_title(title: str) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip()).lower()


def _headline_from_summary(summary: str, max_chars: int = 150) -> str:
    """Derive a headline from a feed item's <description>/<summary>.

    Used when the item <title> is generic — e.g. BBC Sport team feeds give
    every item the title "Manchester United" and put the actual story in the
    description. Takes the first sentence, or the fuller snippet when that
    sentence is too short to stand alone as a title.
    """
    cleaned = _clean_snippet(summary, max_chars=max_chars * 2)
    if not cleaned:
        return ""
    first = re.split(r"(?<=[.!?])\s+", cleaned, maxsplit=1)[0].strip()
    headline = first if len(first) >= 40 else cleaned
    if len(headline) > max_chars:
        headline = headline[:max_chars].rsplit(" ", 1)[0].strip()
    return headline


def _extract_feed_items(base_url: str, body: str) -> list[ExtractedItem]:
    """Parse RSS/Atom items, recovering feeds with generic titles.

    Some live feeds (notably BBC Sport team feeds) repeat the same generic
    <title> — the section/team name — on every item while the real story
    lives in <description>. Reading <title> alone makes every item identical,
    so dedup collapses the whole feed to nothing. A title is treated as
    generic when it is empty, echoes the channel/feed title, or repeats across
    two or more items; the headline is then derived from the description.
    """
    items: list[ExtractedItem] = []
    root = ET.fromstring(body)
    atom_ns = "{http://www.w3.org/2005/Atom}"
    channel_title = _norm_feed_title(
        root.findtext(".//channel/title") or root.findtext(f"{atom_ns}title") or ""
    )

    def _resolve_title(raw_title: str, description: str, counts: Counter) -> tuple[str, str]:
        summary = _clean_snippet(description)
        norm = _norm_feed_title(raw_title)
        is_generic = (
            not norm
            or (bool(channel_title) and norm == channel_title)
            or counts.get(norm, 0) >= 2
        )
        if is_generic:
            headline = _headline_from_summary(description)
            if headline:
                return headline, summary
        return raw_title, summary

    rss_items = root.findall(".//item")
    rss_counts = Counter(_norm_feed_title(i.findtext("title") or "") for i in rss_items)
    for item in rss_items:
        raw_title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        published_at = _feed_item_published_at(item)
        title, summary = _resolve_title(raw_title, item.findtext("description") or "", rss_counts)
        if title and link and _looks_like_candidate_title(title):
            items.append(
                ExtractedItem(
                    title=_clean_title_text(title),
                    url=parse.urljoin(base_url, link),
                    published_at=published_at,
                    summary=summary,
                )
            )
    atom_entries = root.findall(f".//{atom_ns}entry")
    atom_counts = Counter(_norm_feed_title(e.findtext(f"{atom_ns}title") or "") for e in atom_entries)
    for entry in atom_entries:
        raw_title = (entry.findtext(f"{atom_ns}title") or "").strip()
        link_el = entry.find(f"{atom_ns}link")
        href = link_el.attrib.get("href", "") if link_el is not None else ""
        published_at = _feed_item_published_at(entry)
        description = (
            entry.findtext(f"{atom_ns}summary") or entry.findtext(f"{atom_ns}content") or ""
        )
        title, summary = _resolve_title(raw_title, description, atom_counts)
        if title and href and _looks_like_candidate_title(title):
            items.append(
                ExtractedItem(
                    title=_clean_title_text(title),
                    url=parse.urljoin(base_url, href),
                    published_at=published_at,
                    summary=summary,
                )
            )
    return items


_TFGM_ALERT_PATTERN = re.compile(
    r'\\"title\\":\\"([^"\\]{5,200})\\"[^}]{0,800}?\\"description\\":\\"([^"\\]{10,500})\\"'
)

# TfGM travel-alerts page surfaces both public-transport disruptions AND general
# road/motorway/active-travel works. We only want items relevant to public
# transport users. An item is kept only if its title or description mentions a
# recognisable public-transport keyword.
# Trams (Metrolink) are shown EVERY day even for long works — a commuter whose
# stop is closed for weeks needs the daily reminder. Buses/road works use the
# anti-flood rule (only near start/end). Detected by 'tram'/'metrolink'.
_TFGM_TRAM_RE = re.compile(r"\b(metrolink|trams?)\b", re.IGNORECASE)
_TFGM_PUBLIC_TRANSPORT_RE = re.compile(
    r'\b(metrolink|trams?|bus\b|buses|coach|bee\s+network|rail|train|northern|transpennine|'
    r'piccadilly|victoria|altrincham\s+line|bury\s+line|eccles\s+line|ashton\s+line|'
    r'rochdale\s+line|didsbury\s+line|airport\s+line|stop\s+closure)\b',
    re.IGNORECASE,
)


_TFGM_MONTHS_EN = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _tfgm_alert_objects(body: str) -> list[dict]:
    """Parse FULL TfGM alert objects from the page's embedded JSON.

    The page ships each alert as escaped JSON with title/description/effect/
    advice/validityPeriods/impactedServices. We pull all of it (not just the
    title) so the transport card can state dates, the alternative and the
    affected routes. Returns [] if the shape is unrecognised — the caller then
    falls back to the legacy title+description regex so a TfGM redesign never
    blanks the whole transport section.
    """
    out: list[dict] = []
    starts = [m.start() for m in re.finditer(r'\\"title\\":\\"', body)]
    for i, pos in enumerate(starts):
        chunk = body[pos: starts[i + 1] if i + 1 < len(starts) else pos + 1600]

        def f(name: str) -> str:
            m = re.search(r'\\"' + name + r'\\":\\"((?:[^\\]|\\.)*?)\\"', chunk)
            return m.group(1).replace('\\u0026', '&') if m else ""

        title = f("title")
        if not title:
            continue
        dd = re.findall(
            r'\\"start\\":\\"\$D([0-9T:.\-]+Z)\\",\\"end\\":\\"\$D([0-9T:.\-]+Z)\\"',
            chunk,
        )
        seg = re.search(r'\\"impactedServices\\":\[(.*?)\]', chunk)
        svc = re.findall(r'\\"name\\":\\"([^\\"]+)\\"', seg.group(1)) if seg else []
        out.append({
            "title": title.strip(),
            "desc": f("description").strip(),
            "effect": f("effect").strip(),
            "advice": f("advice").strip(),
            "dates": dd,
            "services": [s for s in svc if s and s.lower() != "$undefined"],
        })
    return out


def _tfgm_validity_text(dates: list) -> str:
    """End-date as English 'Until 10 June' so the card date parser catches it."""
    if not dates:
        return ""
    try:
        import datetime as _dt  # noqa: PLC0415
        end = _dt.datetime.fromisoformat(dates[0][1].replace("Z", "+00:00")) + _dt.timedelta(hours=1)
        return f"Until {end.day} {_TFGM_MONTHS_EN[end.month - 1]}."
    except Exception:  # noqa: BLE001
        return ""


def _tfgm_real_advice(advice: str) -> str:
    """Drop 'advice' that is just 'check our page' — that's not an alternative."""
    low = advice.lower().strip()
    if not low:
        return ""
    if "tfgm.com" in low or "more information" in low or low.startswith(("see ", "please check", "check ")):
        return ""
    return advice


def _extract_tfgm_alerts(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract TfGM travel alerts from the page's embedded JSON.

    Primary path parses the full alert object (dates + advice + routes into
    evidence_text). If that yields nothing (TfGM changed the shape) we fall
    back to the legacy title+description regex, so the section degrades to the
    old bare line instead of vanishing.
    """
    fetched_at = now_london().isoformat()
    items: list[ExtractedItem] = []
    seen: set[str] = set()

    objects = _tfgm_alert_objects(body)
    for obj in objects:
        title = obj["title"]
        desc = obj["desc"]
        if not title or title in seen:
            continue
        if not _TFGM_PUBLIC_TRANSPORT_RE.search(f"{title} {desc}"):
            continue
        # Trams: keep daily (commuter reminder). Buses/road: anti-flood — a long
        # multi-day planned closure only shows near its start or end.
        is_tram = bool(_TFGM_TRAM_RE.search(f"{title} {desc}"))
        if not is_tram and obj["dates"]:
            from news_digest.pipeline.nre_incidents import relevant_today  # noqa: PLC0415
            s, e = obj["dates"][0]
            if not relevant_today(s, e, True, now_london().date()):
                continue
        seen.add(title)
        validity = _tfgm_validity_text(obj["dates"])
        advice = _tfgm_real_advice(obj["advice"])
        routes = ", ".join(obj["services"][:6])
        # Rich evidence_text: description + dates + alternative + routes, so the
        # transport card states WHAT, WHEN, the OBJEZD and affected services —
        # instead of "подробности в источнике".
        detail = " ".join(p for p in (
            desc,
            validity,
            advice,
            (f"Affected services: {routes}." if routes else ""),
        ) if p)
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        items.append(
            ExtractedItem(
                title=_clean_title_text(title),
                url=f"{source.url}/{slug}",
                published_at=fetched_at,
                summary=_clean_snippet(desc or title)[:500],
                evidence_text=_clean_snippet(detail, max_chars=900),
            )
        )
    if items:
        return items

    # Fallback: legacy title+description regex (TfGM shape changed).
    for match in _TFGM_ALERT_PATTERN.finditer(body):
        title = match.group(1).strip().replace('\\u0026', '&')
        description = match.group(2).strip().replace('\\u0026', '&')
        if not title or title in seen:
            continue
        if not _TFGM_PUBLIC_TRANSPORT_RE.search(title + " " + description):
            continue
        seen.add(title)
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        items.append(
            ExtractedItem(
                title=_clean_title_text(title),
                url=f"{source.url}/{slug}",
                published_at=fetched_at,
                summary=_clean_snippet(description)[:500],
            )
        )
    return items


_NATIONAL_RAIL_BUILD_ID_PATTERN = re.compile(r'"buildId":"([^"]+)"')
# Operator codes are sometimes empty in the JSON payload — fall back to
# operator name substrings for the GM-relevant set. Long-distance operators
# (Avanti, CrossCountry) cover non-GM segments too — exclude them here and
# rely on the National Rail filter's GM-station term list to surface
# Manchester-stop disruptions when those operators publish them.
_NATIONAL_RAIL_GM_OPERATOR_EXACT = {"northern", "transpennine express"}


def _is_national_rail_gm_operator(name: str) -> bool:
    lowered = re.sub(r"\s+", " ", str(name or "").strip().lower())
    if lowered in _NATIONAL_RAIL_GM_OPERATOR_EXACT:
        return True
    return bool(re.search(r"\btranspennine\b", lowered))


def _extract_national_rail(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Two-step extractor: HTML → buildId → JSON disruption feed.

    The /status-and-disruptions/ HTML page is just a Next.js shell. The
    structured data lives at /_next/data/{buildId}/status-and-disruptions.json
    where buildId is embedded in the shell's script tag. We pull operator-
    level status indicators and unplanned incidents, filter to GM operators
    (Northern, TransPennine, Avanti West Coast), and skip 'Good service'
    rows that have nothing to report.
    """

    match = _NATIONAL_RAIL_BUILD_ID_PATTERN.search(body)
    if not match:
        return []
    build_id = match.group(1)
    json_url = f"https://www.nationalrail.co.uk/_next/data/{build_id}/status-and-disruptions.json"
    try:
        json_body = _fetch_text(json_url)
    except Exception:
        return []
    try:
        payload = json.loads(json_body)
    except json.JSONDecodeError:
        return []

    data = payload.get("pageProps", {}).get("data", {})
    indicators = data.get("serviceIndicatorsData", {}).get("serviceIndicators", []) or []
    unplanned = data.get("serviceIndicatorsData", {}).get("unplannedIncidents", []) or []
    disruptions = data.get("disruptionsData", {}).get("disruptions", []) or []

    items: list[ExtractedItem] = []
    seen: set[str] = set()
    fetched_at = now_london().isoformat()

    def _is_gm_operator(name: str, operators_collection: list[dict] | None = None) -> bool:
        if _is_national_rail_gm_operator(name):
            return True
        for op in operators_collection or []:
            op_name = str(op.get("name") or "")
            if _is_national_rail_gm_operator(op_name):
                return True
        return False

    for indicator in indicators:
        name = str(indicator.get("name") or "").strip()
        status = str(indicator.get("status") or "").strip()
        description = (
            str(indicator.get("customStatusDescription") or "").strip()
            or str(indicator.get("additionalInfoMessage") or "").strip()
        )
        if not _is_gm_operator(name):
            continue
        if status.lower() == "good service" and not description:
            continue
        title = f"{name}: {description or status}".strip(": ")
        if title in seen:
            continue
        seen.add(title)
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        items.append(
            ExtractedItem(
                title=_clean_title_text(title)[:200],
                url=f"{source.url}{slug}",
                published_at=fetched_at,
                summary=_clean_snippet(description or status)[:400],
            )
        )

    for incident in (*unplanned, *disruptions):
        ops = incident.get("operatorsAffectedCollection") or []
        if not _is_gm_operator("", ops):
            continue
        name = str(incident.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        op_label = ", ".join(str(o.get("name") or "").strip() for o in ops if o.get("name"))
        title = f"{op_label}: {name}" if op_label else name
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        items.append(
            ExtractedItem(
                title=_clean_title_text(title)[:200],
                url=f"{source.url}{slug}",
                published_at=fetched_at,
                summary=_clean_snippet(name)[:400],
            )
        )

    return items


_EVENTBRITE_EVENT_LINK_PATTERN = re.compile(
    r'href="(https://www\.eventbrite\.[a-z.]+/e/([a-z0-9\-]+)-tickets[^"]+)"'
)
# Eventbrite's "markets" category page mixes in unrelated events
# (webinars, polo days, food tours from other cities). Require an actual
# market keyword in the slug AND a GM city/borough so we don't import
# Halifax / Sheffield / London markets.
_EVENTBRITE_MARKET_KEYWORDS = (
    "market",
    "fair",
    "flea",
    "maker",
    "vintage",
    "bazaar",
    "car-boot",
    "boot-sale",
)
_EVENTBRITE_GM_LOCATION_TOKENS = (
    "manchester",
    "salford",
    "stockport",
    "trafford",
    "tameside",
    "rochdale",
    "oldham",
    "wigan",
    "bolton",
    "bury",
    "altrincham",
    "prestwich",
    "didsbury",
    "chorlton",
    "ancoats",
    "northern-quarter",
    "cutting-room",
)


def _extract_eventbrite_markets(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract market events from an Eventbrite category listing page.

    Eventbrite event cards expose `/e/{slug}-tickets-{id}` URLs. The slug
    contains a human-readable event name. We require both a market-type
    keyword and a GM location token in the slug — Eventbrite's category
    page surfaces unrelated events (webinars, polo days) and markets from
    neighbouring cities (Halifax, Sheffield) that should not enter a GM
    digest.
    """

    items: list[ExtractedItem] = []
    seen: set[str] = set()
    marker = '"upcomingEvents":'
    marker_index = body.find(marker)
    if marker_index != -1:
        array_start = body.find("[", marker_index)
        if array_start != -1:
            try:
                events, _ = json.JSONDecoder().raw_decode(body[array_start:])
            except json.JSONDecodeError:
                events = []
            if isinstance(events, list):
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    url = str(event.get("url") or "").strip()
                    title = _clean_title_text(str(event.get("name") or ""))
                    if not url or url in seen or not title:
                        continue
                    slug = parse.urlsplit(url).path.lower()
                    venue = event.get("primary_venue") or {}
                    venue_name = str(venue.get("name") or "").strip() if isinstance(venue, dict) else ""
                    address = venue.get("address") or {} if isinstance(venue, dict) else {}
                    city = str(address.get("city") or "").strip() if isinstance(address, dict) else ""
                    haystack = f"{title} {slug} {venue_name} {city}".lower()
                    if not any(kw in haystack for kw in _EVENTBRITE_MARKET_KEYWORDS):
                        continue
                    if not any(loc in haystack for loc in _EVENTBRITE_GM_LOCATION_TOKENS):
                        continue
                    seen.add(url)
                    start_date = str(event.get("start_date") or "").strip()
                    start_time = str(event.get("start_time") or "").strip()
                    published_at = _parse_datetime_value_flexible(start_date) if start_date else None
                    summary = _clean_event_card_field(" | ".join(part for part in (city, venue_name, start_date, start_time, "tickets") if part))
                    if _looks_like_candidate_title(title):
                        items.append(ExtractedItem(title=title, url=url, published_at=published_at, summary=summary))
                if items:
                    return items
    for match in _EVENTBRITE_EVENT_LINK_PATTERN.finditer(body):
        url = match.group(1)
        slug = match.group(2)
        if url in seen:
            continue
        seen.add(url)
        if not any(kw in slug for kw in _EVENTBRITE_MARKET_KEYWORDS):
            continue
        if not any(loc in slug for loc in _EVENTBRITE_GM_LOCATION_TOKENS):
            continue
        title = slug.replace("-", " ").strip()
        if len(title) < 8:
            continue
        # Title-case the leading words but keep numbers and short tokens raw.
        title = " ".join(
            word if (word.isdigit() or len(word) <= 2) else word.capitalize()
            for word in title.split()
        )
        items.append(
            ExtractedItem(
                title=_clean_title_text(title)[:200],
                url=url,
                published_at=None,
                summary="",
            )
        )
    return items


def _extract_visit_manchester_events(source: SourceDef, body: str) -> list[ExtractedItem]:
    items = _extract_slug_link_items(
        source.url,
        body,
        r"^/whats-on/[^/]+",
    )
    return [
        item
        for item in items
        if parse.urlsplit(item.url).path.rstrip("/").lower() != "/whats-on"
    ]


def _extract_phm_events(source: SourceDef, body: str) -> list[ExtractedItem]:
    return _extract_slug_link_items(
        source.url,
        body,
        r"^/(?:whats-on|events_new)/[^/]+",
    )


def _extract_the_manc_weekly_events(source: SourceDef, body: str) -> list[ExtractedItem]:
    article_match = (
        re.search(r"<main[^>]*>(.*?)</main>", body, flags=re.IGNORECASE | re.DOTALL)
        or re.search(r"<article[^>]*>(.*?)</article>", body, flags=re.IGNORECASE | re.DOTALL)
    )
    html_text = article_match.group(1) if article_match else body
    starts = list(re.finditer(r"<h3\b[^>]*>(.*?)</h3>", html_text, flags=re.IGNORECASE | re.DOTALL))
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    title_terms = (
        "market",
        "makers",
        "festival",
        "food",
        "flat baker",
        "pistachio",
        "car boot",
    )
    evidence_terms = (
        "food festival",
        "street food",
        "makers market",
        "car boot",
        "flat baker",
        "pistachio",
    )
    for index, match in enumerate(starts):
        title = _clean_title_text(_clean_long_text(match.group(1)))
        if not title or title.lower().startswith(("advertisement", "did you know")):
            continue
        block_start = match.end()
        block_end = starts[index + 1].start() if index + 1 < len(starts) else len(html_text)
        block = html_text[block_start:block_end]
        evidence = _clean_long_text(block)
        if len(evidence) < 80:
            continue
        title_lower = title.lower()
        evidence_lower = evidence.lower()
        if not any(term in title_lower for term in title_terms) and not any(
            term in evidence_lower for term in evidence_terms
        ):
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        if not slug or slug in seen:
            continue
        seen.add(slug)
        published_at = _extract_text_date_hint(f"{title} {evidence}")
        items.append(
            ExtractedItem(
                title=title,
                url=f"{source.url}#{slug}",
                published_at=published_at,
                summary=_summary_from_evidence(evidence),
                lead=_derive_lead(source, title, evidence),
                evidence_text=evidence[:6000],
                enrichment_status="ok_weekly_section",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


_SECTIONED_GUIDE_TERMS = (
    "market",
    "makers",
    "festival",
    "fair",
    "car boot",
    "flea",
    "food",
    "drink",
    "family",
    "kids",
    "free",
    "exhibition",
    "immersive",
    "theatre",
    "gig",
    "concert",
    "music",
    "show",
    "arena",
    "tour",
    "comedy",
    "club",
    "planetarium",
    "bank holiday",
    "workshop",
    "trail",
    "outdoor",
    "pop-up",
    "co-op live",
    "ao arena",
    "o2 apollo",
    "academy",
    "cathedral",
)


def _looks_like_sectioned_event_title(title: str) -> bool:
    lowered = str(title or "").lower().strip()
    if not (4 <= len(lowered) <= 160):
        return False
    blocked = (
        "advertisement", "subscribe", "newsletter", "privacy", "cookie",
        "discover our cities", "about secret manchester",
    )
    return not any(term in lowered for term in blocked)


_MANCHESTER_THEATRES_CHROME_RE = re.compile(
    r"\b(?:you may also like|what'?s on|book tickets|buy tickets|more info|"
    r"manchester theatres|london theatres|restaurants|bars|hotels|"
    r"follow us|advertisement|privacy|cookie|search)\b",
    re.IGNORECASE,
)
_MANCHESTER_THEATRES_DAY_RE = re.compile(
    r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+"
    r"\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+\s+20\d{2}\b",
    re.IGNORECASE,
)


def _looks_like_manchester_theatres_chrome(title: str) -> bool:
    clean = _clean_long_text(title)
    lowered = clean.lower()
    if not clean:
        return True
    if _MANCHESTER_THEATRES_CHROME_RE.search(clean):
        return True
    if _MANCHESTER_THEATRES_DAY_RE.fullmatch(lowered):
        return True
    return False


def _extract_manchester_theatres_link_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Fallback for Manchester Theatres pages whose cards are not grouped by day.

    The current pages still expose real event detail links under ``/event/``.
    Keep those event titles and let enrichment fetch the detail page for dates,
    venue and richer evidence.
    """

    parser = LinkExtractor(source.url)
    parser.feed(body)
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for link in parser.links:
        parsed = parse.urlsplit(link.url)
        if "/event/" not in parsed.path.lower():
            continue
        title = _clean_title_text(link.title)
        if (
            not _looks_like_candidate_title(title)
            or _looks_like_manchester_theatres_chrome(title)
            or len(title) < 4
        ):
            continue
        normalized_url = clean_url(link.url)
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        items.append(
            ExtractedItem(
                title=title,
                url=link.url,
                published_at=link.published_at,
                summary=title,
                lead=_derive_lead(source, title, title),
                enrichment_status="needs_manchester_theatres_detail",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _extract_manchester_theatres_events(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract event cards from Manchester Theatres weekend pages.

    These pages are grouped by day. The generic section parser treated day
    headings ("Saturday 13 June 2026") and chrome ("You may also like") as
    events, then attached a whole day of evidence to one candidate. This parser
    keeps the day as context and emits the actual linked event cards.
    """

    article_match = (
        re.search(r"<main[^>]*>(.*?)</main>", body, flags=re.IGNORECASE | re.DOTALL)
        or re.search(r"<article[^>]*>(.*?)</article>", body, flags=re.IGNORECASE | re.DOTALL)
    )
    html_text = article_match.group(1) if article_match else body
    headings = list(
        re.finditer(r"<h[1-4]\b[^>]*>(.*?)</h[1-4]>", html_text, flags=re.IGNORECASE | re.DOTALL)
    )
    day_sections: list[tuple[str, int, int]] = []
    for index, heading in enumerate(headings):
        heading_text = _clean_long_text(heading.group(1))
        if not _MANCHESTER_THEATRES_DAY_RE.search(heading_text):
            continue
        section_end = headings[index + 1].start() if index + 1 < len(headings) else len(html_text)
        day_sections.append((heading_text, heading.end(), section_end))
    if not day_sections:
        day_sections = [("", 0, len(html_text))]

    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for day_heading, section_start, section_end in day_sections:
        section_html = html_text[section_start:section_end]
        date_hint = _extract_text_date_hint(day_heading) if day_heading else None
        for link in re.finditer(
            r'<a\b[^>]*\bhref=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
            section_html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            raw_href, raw_title = link.groups()
            title = _clean_title_text(_clean_long_text(raw_title))
            if (
                not _looks_like_candidate_title(title)
                or _looks_like_manchester_theatres_chrome(title)
                or len(title) < 4
            ):
                continue
            local_start = max(0, link.start() - 800)
            local_end = min(len(section_html), link.end() + 1400)
            local_html = section_html[local_start:local_end]
            evidence = _clean_long_text(local_html)
            if _MANCHESTER_THEATRES_CHROME_RE.fullmatch(evidence.strip()):
                continue
            slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
            booking_url = parse.urljoin(source.url, unescape(raw_href.strip()))
            normalized_url = f"{clean_url(source.url).rstrip('/')}/card/{slug}"
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            venue = ""
            venue_match = re.search(
                r"\b(?:at|venue)\s+([A-Z][A-Za-z0-9'& .-]{2,80})\b|"
                r"\b(The Lowry|Lowry|Bridgewater Hall|Palace Theatre|Opera House|HOME|Aviva Studios)\b",
                evidence,
                flags=re.IGNORECASE,
            )
            if venue_match:
                venue = _clean_event_card_field(next(g for g in venue_match.groups() if g) or "")
                if venue.lower() == "lowry":
                    venue = "The Lowry"
            price = ""
            price_match = re.search(r"£\s*\d+(?:\.\d{1,2})?", evidence)
            if price_match:
                price = price_match.group(0).replace(" ", "")
            summary_bits = [day_heading, venue, price, _summary_from_evidence(evidence)]
            summary = _clean_snippet(" | ".join(part for part in summary_bits if part), max_chars=700)
            items.append(
                ExtractedItem(
                    title=title,
                    url=normalized_url,
                    published_at=date_hint or _extract_text_date_hint(evidence),
                    summary=summary or title,
                    lead=_derive_lead(source, title, summary or evidence),
                    evidence_text=_clean_long_text(" ".join(part for part in (day_heading, evidence) if part))[:3000],
                    enrichment_status="ok_manchester_theatres_card",
                    structured_event_hint={
                        "is_event": True,
                        "event_name": title,
                        "date_start": date_hint or "",
                        "date_text": day_heading,
                        "venue": venue,
                        "price": price,
                        "booking_url": booking_url,
                        "schema_source": "manchester_theatres_card",
                    },
                )
            )
            if len(items) >= source.max_candidates:
                return items
    return items or _extract_manchester_theatres_link_items(source, body)


def _extract_sectioned_event_guide(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract individual events from editorial guide pages.

    Secret Manchester / Creative Tourist / similar pages often expose the best
    weekend picks as H2/H3 sections rather than listing cards. A generic link
    scraper sees only "Read more"; this keeps the heading plus the facts beneath
    it as one candidate.
    """

    article_match = re.search(
        r"<(?:article|main)[^>]*>(.*?)</(?:article|main)>",
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    html_text = article_match.group(1) if article_match else body
    starts = list(re.finditer(r"<h[23]\b[^>]*>(.*?)</h[23]>", html_text, flags=re.IGNORECASE | re.DOTALL))
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for index, match in enumerate(starts):
        heading_html = match.group(1)
        link_match = re.search(r'<a\b[^>]*\bhref="([^"]+)"[^>]*>(.*?)</a>', heading_html, flags=re.IGNORECASE | re.DOTALL)
        if link_match:
            url = parse.urljoin(source.url, unescape(link_match.group(1).strip()))
            raw_title = link_match.group(2)
        else:
            raw_title = heading_html
            url = ""
        title = _clean_title_text(_clean_long_text(raw_title))
        if not title or title.lower().startswith(("advertisement", "read more", "share")):
            continue
        block_start = match.end()
        block_end = starts[index + 1].start() if index + 1 < len(starts) else len(html_text)
        block_html = html_text[block_start:block_end]
        evidence = _clean_long_text(block_html)
        if len(evidence) < 60:
            continue
        combined = f"{title} {evidence}"
        lowered = combined.lower()
        if not any(term in lowered for term in _SECTIONED_GUIDE_TERMS):
            continue
        if not re.search(r"\b(?:20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|today|tomorrow|weekend|saturday|sunday|monday)\b", lowered):
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        if not url:
            url = f"{source.url}#{slug}"
        base_url = clean_url(url)
        normalized_url = f"{base_url}#{slug}" if clean_url(source.url) == base_url else base_url
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        if _looks_like_candidate_title(title) or _looks_like_sectioned_event_title(title):
            items.append(
                ExtractedItem(
                    title=title,
                    url=normalized_url,
                    published_at=_extract_text_date_hint(combined),
                    summary=_summary_from_evidence(evidence) or title,
                    lead=_derive_lead(source, title, evidence),
                    evidence_text=evidence[:6000],
                    enrichment_status="ok_sectioned_guide",
                )
            )
        if len(items) >= source.max_candidates:
            break
    return items


def _extract_gmmh_press_releases(source: SourceDef, body: str) -> list[ExtractedItem]:
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    link_iter = re.finditer(
        r'<a\b[^>]*href="([^"]*press-releases[^"]+)"[^>]*>\s*(.*?)\s*</a>',
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for link_match in link_iter:
        url = parse.urljoin(source.url, unescape(link_match.group(1).strip()))
        title = _clean_title_text(_clean_long_text(link_match.group(2)))
        if not title or len(title) < 25 or title.lower().endswith((".jpg", ".png", ".webp")):
            continue
        if url in seen:
            continue
        seen.add(url)
        block = body[link_match.end(): link_match.end() + 1200]
        summary_match = re.search(r'<p\b[^>]*class="[^"]*blog-post-summary[^"]*"[^>]*>(.*?)</p>', block, flags=re.IGNORECASE | re.DOTALL)
        date_match = re.search(r'<time\b[^>]*datetime="([^"]+)"', block, flags=re.IGNORECASE)
        summary = _clean_long_text(summary_match.group(1)) if summary_match else ""
        items.append(
            ExtractedItem(
                title=title,
                url=url,
                published_at=date_match.group(1) if date_match else None,
                summary=summary or title,
                lead=_derive_lead(source, title, summary or title),
                evidence_text=_clean_long_text(block)[:3000],
                enrichment_status="ok_gmmh_press_release",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _published_at_from_relative_text(text: str) -> str | None:
    lowered = _clean_long_text(text).lower()
    now = now_london()
    if re.search(r"\b(?:just now|today|minutes? ago|hours? ago)\b", lowered):
        return now.isoformat()
    match = re.search(r"\b(\d+)\s+days?\s+ago\b", lowered)
    if match:
        return (now - timedelta(days=int(match.group(1)))).isoformat()
    return None


def _extract_manutd_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Manchester United renders article cards server-side inside a Next page.

    The generic HTMLParser often sees only nav/sponsor anchors on this page
    because the card tree is embedded in a large React payload. Anchor+span
    extraction from article-card blocks keeps the official club source usable
    without crawling the whole site.
    """
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    article_blocks = re.findall(
        r'<article\b[^>]*data-testid="article-card"[^>]*>(.*?)</article>',
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for block in article_blocks:
        href_match = re.search(
            r'<a\b[^>]*data-testid="article-card__floating-link"[^>]*href="([^"]+)"[^>]*>\s*<span>(.*?)</span>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not href_match:
            continue
        url = parse.urljoin(source.url, unescape(href_match.group(1).strip()))
        title = _clean_title_text(_clean_long_text(href_match.group(2)))
        if not title or url in seen:
            continue
        seen.add(url)
        publish_match = re.search(
            r'data-testid="publish-date"[^>]*>.*?<span[^>]*>(.*?)</span>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        published_text = _clean_long_text(publish_match.group(1)) if publish_match else ""
        evidence = _clean_long_text(block)
        items.append(
            ExtractedItem(
                title=title,
                url=url,
                published_at=_published_at_from_relative_text(published_text),
                summary=title,
                lead=_derive_lead(source, title, title),
                evidence_text=evidence[:3000],
                enrichment_status="ok_manutd_article_card",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _collapse_repeated_card_title(title: str) -> str:
    clean = _clean_title_text(title)
    if not clean:
        return ""
    # SPA cards often expose the same text through image alt + link text.
    words = clean.split()
    for size in range(3, min(len(words) // 2, 14) + 1):
        if words[:size] == words[size:size * 2]:
            return _clean_title_text(" ".join(words[:size]))
    for end in range(24, min(len(clean) // 2, 120)):
        prefix = clean[:end].strip()
        if len(prefix) >= 20 and clean[end:].lstrip().startswith(prefix):
            return _clean_title_text(prefix)
    return clean


def _extract_mancity_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Manchester City pages expose useful article links server-side.

    The generic link parser sees them, but some card titles are repeated because
    image alt text and anchor text are concatenated. This keeps only men's-team
    news links and collapses repeated card labels before filtering.
    """

    parser = LinkExtractor(source.url)
    parser.feed(body)
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for link in parser.links:
        parsed = parse.urlsplit(link.url)
        lowered_path = parsed.path.lower()
        if "/news/mens/" not in lowered_path:
            continue
        if not re.search(r"\d{5,}", lowered_path):
            continue
        title = _collapse_repeated_card_title(link.title)
        if not title or not _looks_like_candidate_title(title) or _is_football_fluff(title, link.url):
            continue
        normalized_url = clean_url(link.url)
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        items.append(
            ExtractedItem(
                title=title,
                url=link.url,
                published_at=link.published_at or _date_hint_from_text(link.title),
                summary=title,
                lead=_derive_lead(source, title, title),
                enrichment_status="ok_mancity_link",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _clean_bbc_sport_team_title(title: str) -> str:
    clean = _clean_title_text(title)
    clean = re.sub(
        r"^\d{1,2}:\d{2}\s+(?:BST|GMT)\s+\d{1,2}\s+[A-Z][a-z]+\.?\s+",
        "",
        clean,
    )
    clean = re.sub(r"\s*,?\s*published at\b.*$", "", clean, flags=re.IGNORECASE)
    return _clean_title_text(clean)


def _bbc_sport_team_markers(source_name: str) -> tuple[str, ...]:
    if "Manchester United" in source_name:
        return ("man utd", "man united", "manchester united")
    if "Manchester City" in source_name:
        return ("man city", "manchester city")
    return ()


def _bbc_sport_team_title_score(source: SourceDef, title: str, raw_title: str) -> int:
    lowered = title.lower()
    if not any(marker in lowered for marker in _bbc_sport_team_markers(source.name)):
        return -1000
    generic = (
        "gossip column",
        "score updates",
        "live match updates",
        "live lock screen",
        "how to follow",
        "take a dive",
        "bbc sport journalists",
        "key names being discussed",
    )
    if any(token in lowered for token in generic):
        return -1000
    score = min(len(title), 160)
    if re.search(r"\d{1,2}:\d{2}\s+(?:BST|GMT)", raw_title):
        score += 20
    return score


def _extract_bbc_sport_team_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """BBC team RSS currently surfaces Sounds/podcast cards, not articles.

    The HTML team pages contain real article links under
    ``/sport/football/articles/``. Extract those directly and let enrichment
    fetch the article body when the listing text is thin.
    """

    parser = LinkExtractor(source.url)
    parser.feed(body)
    by_url: dict[str, tuple[int, ExtractedItem]] = {}
    order: list[str] = []
    for link in parser.links:
        parsed = parse.urlsplit(link.url)
        if "/sport/football/articles/" not in parsed.path.lower():
            continue
        title = _clean_bbc_sport_team_title(link.title)
        if not title or not _looks_like_candidate_title(title) or _is_football_fluff(title, link.url):
            continue
        normalized_url = clean_url(link.url)
        score = _bbc_sport_team_title_score(source, title, link.title)
        if score < 0:
            continue
        item = (
            ExtractedItem(
                title=title,
                url=link.url,
                published_at=link.published_at or _date_hint_from_text(link.title),
                summary=title,
                lead=_derive_lead(source, title, title),
                enrichment_status="ok_bbc_sport_team_link",
            )
        )
        if normalized_url not in by_url:
            order.append(normalized_url)
        if normalized_url not in by_url or score > by_url[normalized_url][0]:
            by_url[normalized_url] = (score, item)
        if len(order) >= source.max_candidates:
            break
    return [by_url[url][1] for url in order if url in by_url]


def _extract_rncm_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """RNCM event cards use image/aria labels, not useful anchor text."""
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    event_blocks = re.findall(
        r'<div class="event\s[^"]*"[^>]*>(.*?)(?=<div class="event\s|</div>\s*</div>\s*</div>)',
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for block in event_blocks:
        href_match = re.search(
            r'<a\b[^>]*\bhref="(https://www\.rncm\.ac\.uk/performance/[^"]+)"[^>]*',
            block,
            flags=re.IGNORECASE,
        )
        title_match = re.search(r"<h2\b[^>]*>(.*?)</h2>", block, flags=re.IGNORECASE | re.DOTALL)
        if not href_match or not title_match:
            continue
        url = clean_url(href_match.group(1))
        if url in seen:
            continue
        seen.add(url)
        title = _clean_title_text(_clean_event_card_field(title_match.group(1)))
        if not title or len(title) < 4:
            continue
        date_text = ""
        date_match = re.search(
            r'<div class="event-date">\s*([A-Za-z]{3})\s*(\d{1,2})',
            block,
            flags=re.IGNORECASE,
        )
        published_at = None
        if date_match:
            month = month_map.get(date_match.group(1).lower())
            day = int(date_match.group(2))
            if month:
                try:
                    event_dt = now_london().replace(
                        month=month, day=day, hour=12, minute=0, second=0, microsecond=0
                    )
                    if event_dt.date() < now_london().date():
                        event_dt = event_dt.replace(year=event_dt.year + 1)
                    published_at = event_dt.isoformat()
                    date_text = event_dt.strftime("%Y-%m-%d")
                except ValueError:
                    published_at = None
        promoter_match = re.search(
            r'<div class="title">\s*<h2\b[^>]*>.*?</h2>\s*<span>\s*(.*?)\s*</span>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        promoter = _clean_event_card_field(promoter_match.group(1)) if promoter_match else ""
        summary = _clean_snippet(" | ".join(part for part in ("RNCM", promoter, date_text, "tickets") if part))
        items.append(
            ExtractedItem(
                title=title,
                url=url,
                published_at=published_at,
                summary=summary,
                evidence_text=summary,
                enrichment_status="ok_rncm_card",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _extract_designmynight_cards(source: SourceDef, body: str) -> list[ExtractedItem]:
    """DesignMyNight «things to do» — venue/event cards.

    Page structure (verified live 2026-05-16):
      <article id="card-X" class="card">
        <h3 class="card__title">
          <a href="..." title="Cherry Jam">Cherry Jam</a>
        </h3>
        <p>... description ...</p>
      </article>

    The generic HTML extractor produces a single "DesignMyNight" item
    with the page title because <a> links have no useful text on the
    listing page. This parser walks every card, extracts venue/event
    title + URL + surrounding description, and emits one ExtractedItem
    per real card.

    Date handling: the page URL pattern is
    ``…/things-to-do-this-weekend-in-manchester`` so every event is
    implicitly for the current weekend. We don't fabricate a published_at
    here — the implicit-weekend bypass in candidate_validator handles
    that based on source_url.
    """
    items: list[ExtractedItem] = []
    seen: set[str] = set()

    # Match each <article class="card"> block until its closing tag or
    # the start of the next article. id="card-X" is the stable marker.
    card_pattern = re.compile(
        r'<article\b[^>]*\bid="card-([a-z0-9-]+)"[^>]*\bclass="[^"]*\bcard\b[^"]*"[^>]*>(.*?)(?=<article\b[^>]*\bid="card-|</main>|</body>)',
        re.IGNORECASE | re.DOTALL,
    )
    for match in card_pattern.finditer(body):
        card_id = match.group(1)
        card_html = match.group(2)

        # Inner anchor inside the title h3.
        title_match = re.search(
            r'<h[23]\b[^>]*\bclass="[^"]*card__title[^"]*"[^>]*>\s*<a\b[^>]*\bhref="([^"]+)"[^>]*>(.*?)</a>',
            card_html,
            re.IGNORECASE | re.DOTALL,
        )
        if not title_match:
            continue
        url = parse.urljoin(source.url, unescape(title_match.group(1).strip()))
        title = _clean_title_text(re.sub(r"<[^>]+>", "", title_match.group(2)).strip())
        if not title or len(title) < 3:
            continue
        if url in seen:
            continue
        seen.add(url)

        # Description: every <p> inside the card, joined.
        description_chunks = re.findall(
            r'<p\b[^>]*>(.*?)</p>',
            card_html,
            re.IGNORECASE | re.DOTALL,
        )
        evidence_raw = " ".join(
            re.sub(r"<[^>]+>", " ", chunk) for chunk in description_chunks
        )
        evidence = _clean_long_text(evidence_raw)
        # Many DMN cards have a near-empty description — add the title
        # so summary/lead derivation has something to work with.
        if len(evidence) < 30:
            evidence = title

        items.append(
            ExtractedItem(
                title=title,
                url=url,
                published_at="",  # implicit-weekend; validator handles it
                summary=_summary_from_evidence(evidence) or title,
                lead=_derive_lead(source, title, evidence),
                evidence_text=evidence[:2500],
                enrichment_status="ok_dmn_card",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _extract_skiddle_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract real Skiddle event cards instead of navigation links."""
    month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "september": 9, "oct": 10, "october": 10,
        "nov": 11, "november": 11, "dec": 12, "december": 12,
    }
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    card_pattern = re.compile(
        r'<a\b[^>]*\bhref="(https://www\.skiddle\.com/whats-on/Manchester/[^"]+/)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in card_pattern.finditer(body):
        url = clean_url(match.group(1))
        if url in seen or "/may-bank-holiday-events" in url.lower():
            continue
        seen.add(url)
        card_html = match.group(2)
        alt_match = re.search(r'<img\b[^>]*\balt="([^"]+)"', card_html, flags=re.IGNORECASE)
        raw_title = _clean_event_card_field(alt_match.group(1)) if alt_match else _title_from_slug(url)
        title = _clean_title_text(raw_title)
        if not title or len(title) < 4:
            continue
        evidence = _clean_long_text(card_html)
        published_at = _parse_datetime_value_flexible(evidence)
        if not published_at:
            date_match = re.search(
                r"\b(?:mon|tue|wed|thu|fri|sat|sun)?[a-z]*\s*"
                r"(\d{1,2})(?:st|nd|rd|th)?\s+"
                r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
                r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
                r"\s+(20\d{2})(?:\s+(\d{1,2}):(\d{2})(am|pm)?)?",
                evidence,
                flags=re.IGNORECASE,
            )
            if date_match:
                day, month_name, year, hour, minute, ampm = date_match.groups()
                month = month_map.get(month_name.lower())
                if month:
                    hour_int = int(hour or 12)
                    if ampm:
                        marker = ampm.lower()
                        if marker == "pm" and hour_int < 12:
                            hour_int += 12
                        elif marker == "am" and hour_int == 12:
                            hour_int = 0
                    try:
                        published_at = now_london().replace(
                            year=int(year),
                            month=month,
                            day=int(day),
                            hour=hour_int,
                            minute=int(minute or 0),
                            second=0,
                            microsecond=0,
                        ).isoformat()
                    except ValueError:
                        published_at = None
        summary = _summary_from_evidence(evidence) or title
        items.append(
            ExtractedItem(
                title=title,
                url=url,
                published_at=published_at,
                summary=summary,
                lead=_derive_lead(source, title, evidence),
                evidence_text=evidence[:4000],
                enrichment_status="ok_skiddle_card",
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _extract_skiddle_api_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Parse the Skiddle Events API (``json_skiddle``) search response.

    Skiddle's public search endpoint returns
    ``{"error": 0, "results": [{event}], "totalcount": N}``. Each event
    carries a clean name, a venue object (name/town/lat/lon) and a start
    date — exactly the (name + date + venue) identity the event dedupe key
    needs — so we publish a structured_event_hint instead of leaving the
    event to be re-derived from a listing page. This replaces the fragile
    HTML scrape of /whats-on/Manchester/ that broke on Skiddle redesigns.
    """
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(payload, dict) or int(payload.get("error") or 0) != 0:
        return []
    results = payload.get("results")
    if not isinstance(results, list):
        return []
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for event in results:
        if not isinstance(event, dict):
            continue
        title = _clean_title_text(str(event.get("eventname") or ""))
        url = str(event.get("link") or "").strip()
        if not title or not url or url in seen:
            continue
        seen.add(url)
        venue_obj = event.get("venue") if isinstance(event.get("venue"), dict) else {}
        venue_name = str(venue_obj.get("name") or "").strip()
        town = str(venue_obj.get("town") or "").strip()
        start_raw = str(event.get("startdate") or event.get("date") or "").strip()
        start_iso = _parse_datetime_value_flexible(start_raw) or None
        description = str(event.get("description") or "").strip()
        genres = event.get("genres") if isinstance(event.get("genres"), list) else []
        genre_names = ", ".join(
            str(g.get("name") or "").strip()
            for g in genres
            if isinstance(g, dict) and g.get("name")
        )
        evidence_parts = [
            title,
            f"Venue: {venue_name}" if venue_name else "",
            f"Town: {town}" if town else "",
            f"Date: {start_raw[:16].replace('T', ' ')}" if start_raw else "",
            f"Type: {event.get('EventCode') or ''}" if event.get("EventCode") else "",
            f"Genres: {genre_names}" if genre_names else "",
            description,
        ]
        evidence = ". ".join(part for part in evidence_parts if part)
        hint: dict = {}
        if venue_name or start_iso:
            instance = _jsonld_event_instance_id(title, start_iso or "", venue_name)
            hint = {
                "is_event": True,
                "event_name": title,
                "venue": venue_name,
                "town": town,
                "start_date": start_iso or start_raw,
                "event_instance_id": instance or f"skiddle-{event.get('id') or url}",
            }
        items.append(
            ExtractedItem(
                title=title,
                url=url,
                published_at=start_iso,
                summary=_summary_from_evidence(evidence) or description[:300] or title,
                lead=_derive_lead(source, title, evidence),
                evidence_text=evidence[:4000],
                enrichment_status="ok_skiddle_card",
                structured_event_hint=hint,
            )
        )
        if len(items) >= source.max_candidates:
            break
    return items


def _extract_eventbrite_events(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract Eventbrite event links from an organiser/search page.

    Diaspora relevance is decided after page enrichment: the useful signal is
    often organiser/body text, not the Eventbrite listing slug.
    """

    items: list[ExtractedItem] = []
    seen: set[str] = set()
    marker = '"upcomingEvents":'
    marker_index = body.find(marker)
    if marker_index != -1:
        array_start = body.find("[", marker_index)
        if array_start != -1:
            try:
                events, _ = json.JSONDecoder().raw_decode(body[array_start:])
            except json.JSONDecodeError:
                events = []
            if isinstance(events, list):
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    url = str(event.get("url") or "").strip()
                    title = _clean_title_text(str(event.get("name") or ""))
                    if not url or url in seen or not title:
                        continue
                    seen.add(url)
                    start_date = str(event.get("start_date") or "").strip()
                    start_time = str(event.get("start_time") or "").strip()
                    venue = event.get("primary_venue") or {}
                    venue_name = str(venue.get("name") or "").strip() if isinstance(venue, dict) else ""
                    address = venue.get("address") or {} if isinstance(venue, dict) else {}
                    city = str(address.get("city") or "").strip() if isinstance(address, dict) else ""
                    published_at = _parse_datetime_value_flexible(start_date) if start_date else None
                    summary = _clean_event_card_field(" | ".join(part for part in (city, venue_name, start_date, start_time, "tickets") if part))
                    if _looks_like_candidate_title(title):
                        items.append(ExtractedItem(title=title, url=url, published_at=published_at, summary=summary))
                if items:
                    return items
    for match in _EVENTBRITE_EVENT_LINK_PATTERN.finditer(body):
        url = match.group(1)
        slug = match.group(2)
        if url in seen:
            continue
        seen.add(url)
        title = slug.replace("-", " ").strip()
        if len(title) < 8:
            continue
        title = " ".join(
            word if (word.isdigit() or len(word) <= 2) else word.capitalize()
            for word in title.split()
        )
        items.append(ExtractedItem(title=_clean_title_text(title)[:200], url=url))
    return items


def _extract_kontramarka_items(body: str) -> list[ExtractedItem]:
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    year = now_london().year
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    for card in re.findall(r'<div class="card">(.*?)</div>\s*</div>\s*</div>', body, flags=re.IGNORECASE | re.DOTALL):
        title_match = re.search(r'class="fw-bolder card-title">\s*(.*?)\s*</span>', card, flags=re.IGNORECASE | re.DOTALL)
        href_match = re.search(r'href="(https://widget\.kontramarka\.uk/[^"]+/event/\d+)"', card, flags=re.IGNORECASE)
        if not title_match or not href_match:
            continue
        url = href_match.group(1)
        if url in seen:
            continue
        seen.add(url)
        title = _clean_title_text(title_match.group(1))
        day_match = re.search(r'class="ms-2 fs-3 fw-bolder">\s*(\d{1,2})\s*</span>\s*<span class="fs-6 fw-light">\s*([A-Za-z]+)\s*</span>', card, flags=re.IGNORECASE)
        time_city_price = [_clean_snippet(part) for part in re.findall(r'<div class="mt-2 ms-1(?: fw-bolder)?">\s*(.*?)\s*</div>', card, flags=re.IGNORECASE | re.DOTALL)]
        time_text = time_city_price[0] if len(time_city_price) > 0 else ""
        city = time_city_price[1] if len(time_city_price) > 1 else ""
        price = time_city_price[2] if len(time_city_price) > 2 else ""
        published_at = None
        if day_match:
            day = int(day_match.group(1))
            month = month_map.get(day_match.group(2).lower())
            if month:
                try:
                    candidate_dt = now_london().replace(year=year, month=month, day=day, hour=12, minute=0, second=0, microsecond=0)
                    if candidate_dt.date() < now_london().date():
                        candidate_dt = candidate_dt.replace(year=year + 1)
                    published_at = candidate_dt.isoformat()
                except ValueError:
                    published_at = None
        summary = _clean_snippet(" | ".join(part for part in (city, time_text, price, "tickets") if part))
        if title and _looks_like_candidate_title(title):
            items.append(ExtractedItem(title=title, url=url, published_at=published_at, summary=summary))
    return items


def _clean_event_card_field(value: str) -> str:
    return _strip_evidence_chrome(re.sub(r"<[^>]+>", " ", unescape(str(value or ""))))


def _extract_eventfirst_items(body: str) -> list[ExtractedItem]:
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    starts = [match.start() for match in re.finditer(r'<div class="upcoming-events__item\s', body, flags=re.IGNORECASE)]
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if idx + 1 < len(starts) else len(body)
        block = body[start:end]
        href_match = re.search(r'href="(https://eventfirst\.co\.uk/event/[^"]+)"', block, flags=re.IGNORECASE)
        title_match = re.search(r'<h3 class="upcoming-events__item-title">\s*(.*?)\s*</h3>', block, flags=re.IGNORECASE | re.DOTALL)
        if not href_match or not title_match:
            continue
        url = href_match.group(1)
        if url in seen:
            continue
        seen.add(url)
        title = _clean_title_text(unescape(title_match.group(1)))
        date_text = ""
        time_text = ""
        venue = ""
        city = ""
        date_match = re.search(r'class="upcoming-events__item-info--date">\s*(.*?)\s*</div>', block, flags=re.IGNORECASE | re.DOTALL)
        time_match = re.search(r'class="upcoming-events__item-info--time">\s*(.*?)\s*</div>', block, flags=re.IGNORECASE | re.DOTALL)
        place_match = re.search(r'class="upcoming-events__item-info--place">\s*<div>\s*(.*?)\s*</div>\s*<div class="upcoming-events__item-info--city">\s*(.*?)\s*</div>', block, flags=re.IGNORECASE | re.DOTALL)
        if date_match:
            date_text = _clean_event_card_field(date_match.group(1))
        if time_match:
            time_text = _clean_event_card_field(time_match.group(1))
        if place_match:
            venue = _clean_event_card_field(place_match.group(1))
            city = _clean_event_card_field(place_match.group(2))
        published_at = _parse_datetime_value_flexible(date_text) if date_text else None
        summary = _clean_snippet(" | ".join(part for part in (city, venue, date_text, time_text, "tickets") if part))
        if title and _looks_like_candidate_title(title):
            items.append(ExtractedItem(title=title, url=url, published_at=published_at, summary=summary))

    return items


def _extract_funnelback_items(body: str) -> list[ExtractedItem]:
    payload = json.loads(body)
    results = (
        payload.get("response", {})
        .get("resultPacket", {})
        .get("results", [])
    )
    items: list[ExtractedItem] = []
    for result in results:
        metadata = result.get("listMetadata") or {}
        title = str((metadata.get("t") or [result.get("title") or ""])[0] or "").strip()
        url = str(result.get("liveUrl") or result.get("indexUrl") or result.get("displayUrl") or "").strip()
        summary = str((metadata.get("intro") or [result.get("summary") or ""])[0] or "").strip()
        published_at = _parse_datetime_value_flexible(
            str((metadata.get("d") or [metadata.get("timestamp") or ""])[0] or "").strip()
        )
        if title and url and _looks_like_candidate_title(title):
            items.append(
                ExtractedItem(
                    title=_clean_title_text(title),
                    url=url,
                    published_at=published_at,
                    summary=summary,
                )
            )
    return items


_TICKETMASTER_MAJOR_LONDON_VENUES = (
    "alexandra palace",
    "american express stadium",
    "eventim apollo",
    "hyde park",
    "knebworth",
    "liverpool anfield",
    "london stadium",
    "lytham festival",
    "murrayfield",
    "o2 academy brixton",
    "ovo arena",
    "principality stadium",
    "royal albert hall",
    "st james' park",
    "st. james' park",
    "the o2",
    "tottenham hotspur stadium",
    "utilita arena",
    "wembley arena",
    "wembley stadium",
)


def _format_ticketmaster_date(value: str | None) -> str:
    if not value:
        return ""
    parsed = _parse_datetime_value_flexible(value)
    if not parsed:
        return str(value)
    return parsed[:16].replace("T", " ")


def _is_major_london_venue(venue: str) -> bool:
    lowered = venue.lower()
    return any(token in lowered for token in _TICKETMASTER_MAJOR_LONDON_VENUES)


def _extract_ticketmaster_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    payload = json.loads(body)
    events = (payload.get("_embedded") or {}).get("events") or []
    items: list[ExtractedItem] = []
    major_london_only = "london major" in source.name.lower()
    onsale_scan = "onsale" in source.name.lower()
    for event in events:
        title = str(event.get("name") or "").strip()
        url = str(event.get("url") or "").strip()
        dates = (event.get("dates") or {}).get("start") or {}
        event_start_raw = dates.get("dateTime") or dates.get("localDate") or ""
        event_start = _parse_datetime_value_flexible(event_start_raw)
        public_sale = ((event.get("sales") or {}).get("public") or {})
        onsale_start_raw = public_sale.get("startDateTime") or ""
        onsale_start = _parse_datetime_value_flexible(onsale_start_raw)
        venue = ""
        city = ""
        venues = ((event.get("_embedded") or {}).get("venues") or [])
        if venues:
            venue = str(venues[0].get("name") or "").strip()
            city = str((venues[0].get("city") or {}).get("name") or "").strip()
        if major_london_only and not _is_major_london_venue(venue):
            continue
        major_venue = is_major_ticket_venue(venue)
        if "uk major" in source.name.lower() and _is_major_london_venue(venue):
            major_venue = True
        classifications = event.get("classifications") or []
        genre = ""
        subgenre = ""
        classification_payload: dict[str, str] = {}
        if classifications:
            first_classification = classifications[0]
            genre = str((first_classification.get("genre") or {}).get("name") or "").strip()
            subgenre = str((first_classification.get("subGenre") or {}).get("name") or "").strip()
            classification_payload = {
                "segment": str((first_classification.get("segment") or {}).get("name") or "").strip(),
                "genre": genre,
                "subGenre": subgenre,
                "type": str((first_classification.get("type") or {}).get("name") or "").strip(),
                "subType": str((first_classification.get("subType") or {}).get("name") or "").strip(),
            }
        attractions_payload: list[dict[str, str]] = []
        for attraction in ((event.get("_embedded") or {}).get("attractions") or []):
            if not isinstance(attraction, dict):
                continue
            attraction_name = str(attraction.get("name") or "").strip()
            attraction_id = str(attraction.get("id") or "").strip()
            attraction_url = str(attraction.get("url") or "").strip()
            attraction_classifications = attraction.get("classifications") or []
            attraction_genre = ""
            attraction_subgenre = ""
            if attraction_classifications and isinstance(attraction_classifications[0], dict):
                first_attraction_class = attraction_classifications[0]
                attraction_genre = str((first_attraction_class.get("genre") or {}).get("name") or "").strip()
                attraction_subgenre = str((first_attraction_class.get("subGenre") or {}).get("name") or "").strip()
            if attraction_name:
                attractions_payload.append(
                    {
                        "name": attraction_name,
                        "id": attraction_id,
                        "url": attraction_url,
                        "genre": attraction_genre,
                        "subGenre": attraction_subgenre,
                    }
                )
        promoter = event.get("promoter") if isinstance(event.get("promoter"), dict) else {}
        title_parts = [title]
        if event_start:
            title_parts.append(f"event {event_start[:10]}")
        if onsale_start:
            title_parts.append(f"public sale {onsale_start[:16].replace('T', ' ')}")
        display_title = " — ".join(title_parts)
        ticket_type = "major_upcoming" if major_venue else "regular_upcoming"
        if onsale_scan:
            if onsale_start:
                try:
                    onsale_dt = datetime.fromisoformat(onsale_start)
                    if onsale_dt <= now_london():
                        ticket_type = "on_sale_now"
                    else:
                        ticket_type = "presale_soon"
                except ValueError:
                    ticket_type = "newly_listed"
            else:
                ticket_type = "newly_listed"
        summary_parts = [
            venue,
            city,
            genre,
            f"event_date={_format_ticketmaster_date(event_start_raw)}" if event_start_raw else "",
            f"public_onsale={_format_ticketmaster_date(onsale_start_raw)}" if onsale_start_raw else "",
            "ticket_signal=onsale" if onsale_scan else "ticket_signal=upcoming_event",
            f"ticket_type={ticket_type}",
            f"major_venue={'true' if major_venue else 'false'}",
        ]
        summary = " | ".join(filter(None, summary_parts))
        if title and url and _looks_like_candidate_title(display_title):
            items.append(
                ExtractedItem(
                    title=_clean_title_text(display_title),
                    url=url,
                    published_at=onsale_start if onsale_scan and onsale_start else event_start,
                    summary=summary,
                    structured_event_hint={
                        "schema_source": "ticketmaster_api",
                        "event_name": title,
                        "venue": venue,
                        "date_start": _format_ticketmaster_date(event_start_raw)[:10] if event_start_raw else "",
                        "date_text": _format_ticketmaster_date(event_start_raw) if event_start_raw else "",
                        "booking_url": url,
                        "genre": genre,
                        "subGenre": subgenre,
                        "classifications": classification_payload,
                        "attractions": attractions_payload,
                        "ticketmaster_attraction_id": attractions_payload[0]["id"] if attractions_payload else "",
                        "promoter": str(promoter.get("name") or "").strip(),
                        "ticket_type": ticket_type,
                    },
                )
            )
    return items


_HERITAGE_DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\s+(20\d{2})\b",
    re.IGNORECASE,
)
_HERITAGE_LINEUP_RE = re.compile(r"\s+\+\s+")
_HERITAGE_SKIP_RE = re.compile(
    r"\b(?:what'?s on|artists? & events|more info|buy tickets|mailing list|"
    r"glamping|coach travel|past events|good times|privacy|terms|venues|"
    r"there were no results|discounted multiday|book glamping|book coach)\b",
    re.IGNORECASE,
)


def _html_text_lines(body: str) -> list[str]:
    scrubbed = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", body)
    scrubbed = re.sub(r"(?i)<br\s*/?>", "\n", scrubbed)
    scrubbed = re.sub(r"(?i)</(?:p|div|li|h[1-6]|a|section|article|span)>", "\n", scrubbed)
    scrubbed = re.sub(r"(?s)<[^>]+>", " ", scrubbed)
    scrubbed = re.sub(r"\^\{(?:st|nd|rd|th)\}", "", scrubbed, flags=re.IGNORECASE)
    scrubbed = unescape(scrubbed)
    lines: list[str] = []
    for line in scrubbed.splitlines():
        text = re.sub(r"\s+", " ", line).strip(" -–—\t")
        if text:
            lines.append(text)
    return lines


def _heritage_date_iso(text: str) -> str:
    match = _HERITAGE_DATE_RE.search(text)
    if not match:
        return ""
    day_raw, month_raw, year_raw = match.groups()
    month = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }.get(month_raw.lower())
    if not month:
        return ""
    try:
        return datetime(int(year_raw), month, int(day_raw), 18, 0, tzinfo=now_london().tzinfo).isoformat()
    except ValueError:
        return ""


def _heritage_slug(*parts: str) -> str:
    slug = "-".join(parts)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", slug).strip("-").lower()
    return slug[:96] or "event"


def _heritage_lineup_names(lineup: str) -> list[str]:
    names: list[str] = []
    for part in _HERITAGE_LINEUP_RE.split(lineup):
        name = re.sub(r"\s+", " ", part).strip(" +.,-–—")
        if len(name) >= 3:
            names.append(name)
    return list(dict.fromkeys(names))


def _looks_like_heritage_lineup(line: str) -> bool:
    if _HERITAGE_SKIP_RE.search(line):
        return False
    if _HERITAGE_DATE_RE.search(line):
        return False
    letters = re.sub(r"[^A-Za-z&+.'’ ]+", "", line).strip()
    if len(letters) < 4 or len(letters) > 140:
        return False
    has_artist_case = bool(re.search(r"[A-Z]{2,}", line))
    return has_artist_case or "+" in line


def _extract_heritage_live_items(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract Heritage Live cards from heading/date/venue text.

    The homepage renders useful event data as card text while its links often
    read only "More Info". Generic anchor extraction therefore reports raw=0
    even when the page clearly lists major UK shows and lineups.
    """

    lines: list[str] = []
    for line in _html_text_lines(body):
        if line.lstrip().startswith("+") and lines:
            lines[-1] = f"{lines[-1]} {line.strip()}"
        else:
            lines.append(line)
    items: list[ExtractedItem] = []
    seen: set[tuple[str, str, str]] = set()
    for idx, line in enumerate(lines):
        if not _looks_like_heritage_lineup(line):
            continue
        date_line = ""
        venue = ""
        for lookahead in lines[idx + 1 : idx + 5]:
            if not date_line and _HERITAGE_DATE_RE.search(lookahead):
                date_line = lookahead
                after_date = _HERITAGE_DATE_RE.sub("", lookahead, count=1).strip(" ,;-")
                if after_date:
                    venue = after_date
                continue
            if date_line and not venue and not _HERITAGE_SKIP_RE.search(lookahead):
                venue = lookahead
                break
        date_iso = _heritage_date_iso(date_line)
        if not date_iso or not venue:
            continue
        names = _heritage_lineup_names(line)
        if not names:
            continue
        key = (line.lower(), date_iso[:10], venue.lower())
        if key in seen:
            continue
        seen.add(key)
        url = f"{source.url.rstrip('/')}#{_heritage_slug(line, date_iso[:10], venue)}"
        summary_parts = [
            venue,
            "Music",
            f"event_date={date_iso[:16].replace('T', ' ')}",
            "ticket_signal=upcoming_event",
            "ticket_type=major_upcoming",
            "major_venue=true",
            f"lineup={', '.join(names)}",
        ]
        title = f"{line} — event {date_iso[:10]}"
        items.append(
            ExtractedItem(
                title=_clean_title_text(title),
                url=url,
                published_at=date_iso,
                summary=" | ".join(summary_parts),
                enrichment_status="ok_heritage_card",
                structured_event_hint={
                    "schema_source": "heritage_live",
                    "event_name": line,
                    "venue": venue,
                    "date_start": date_iso[:10],
                    "date_text": date_iso[:16].replace("T", " "),
                    "booking_url": source.url,
                    "genre": "Music",
                    "lineup": names,
                    "headliners": names,
                    "ticket_type": "major_upcoming",
                },
            )
        )
    return items


def _is_outside_gm_ticket_source(source: SourceDef) -> bool:
    lowered = source.name.lower()
    return (
        "ticketmaster liverpool" in lowered
        or "ticketmaster london major" in lowered
        or "ticketmaster uk major" in lowered
        or source.name == "Heritage Live"
    )


def _extract_wp_rest_items(body: str) -> list[ExtractedItem]:
    payload = json.loads(body)
    if not isinstance(payload, list):
        return []
    items: list[ExtractedItem] = []
    for entry in payload:
        title = _clean_title_text(str(((entry.get("title") or {}).get("rendered")) or "").strip())
        url = str(entry.get("link") or "").strip()
        summary = _clean_snippet(str(((entry.get("excerpt") or {}).get("rendered")) or ""))
        published_at = _parse_datetime_value_flexible(str(entry.get("date") or "").strip())
        if title and url and _looks_like_candidate_title(title):
            items.append(
                ExtractedItem(
                    title=title,
                    url=url,
                    published_at=published_at,
                    summary=summary,
                )
            )
    return items


def _extract_markdown_link_items(body: str) -> list[ExtractedItem]:
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for anchor_text, absolute in re.findall(r"\[(.*?)\]\((https?://[^)]+)\)", body, flags=re.DOTALL):
        if absolute in seen:
            continue
        seen.add(absolute)
        title_match = re.search(r"###\s*(.*?)\s*Category:", anchor_text, flags=re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""
        if not title:
            continue
        title = _clean_title_text(re.sub(r"\s+", " ", title))
        summary = anchor_text
        if title_match:
            summary = anchor_text[title_match.end():]
        summary = re.sub(r"!\[[^\]]*\]\([^)]+\)", " ", summary)
        summary = _clean_snippet(re.sub(r"\s+", " ", summary))
        if _looks_like_candidate_title(title):
            items.append(ExtractedItem(title=title, url=absolute, summary=summary))
    return items


def _extract_sitemap_items(base_url: str, body: str) -> list[ExtractedItem]:
    items: list[ExtractedItem] = []
    seen: set[str] = set()
    for match in re.finditer(r"<loc>([^<]+)</loc>", body):
        url = match.group(1).strip()
        if url in seen:
            continue
        seen.add(url)
        slug = url.rstrip("/").split("/")[-1].replace("-", " ")
        title = _clean_title_text(slug.title())
        if title and _looks_like_candidate_title(title):
            items.append(ExtractedItem(title=title, url=url))
    return items


# #2 Soft-reroute: an evaluative question headline ("How good a mayor is Andy
# Burnham?"), a profile/retrospective or an explainer is analysis, not a fresh
# event — it belongs in Городской радар, not «Свежие новости». We reroute it
# out of last_24h UNLESS it carries a real hard-news signal (then a question
# headline is just framing on real news and stays in Fresh).
_ANALYSIS_OPINION_RE = re.compile(
    r"\b(?:how good|the rise of|looks? back|everything we know|what we know|"
    r"explained|opinion|analysis|profile of|a profile|review of|"
    r"completes? (?:his|her|their|a)?\s*(?:nine|eight|seven|six|five|four|\d+)[- ]year|"
    r"end of an era|legacy of|nine years as)\b",
    re.IGNORECASE,
)
_ANALYSIS_QUESTION_RE = re.compile(
    r"^\s*(?:how|is|are|should|why|what|who|will|can|does|did|has|was|were)\b.*\?\s*$",
    re.IGNORECASE,
)
_FRESH_HARD_NEWS_RE = re.compile(
    r"\b(?:police|arrest|charged?|sentenced?|jailed|court|murder|stab|knife|"
    r"crash|collision|fire|death|died|killed|evacuat|cordon|closed|closure|"
    r"cqc|ofsted|inquest|raid|appeal|missing|attack|assault|disruption|strike|"
    r"rescue|injured|hospital)\b",
    re.IGNORECASE,
)


def _looks_like_analysis_opinion(title: str, summary: str) -> bool:
    blob = f"{title or ''} {summary or ''}"
    if _FRESH_HARD_NEWS_RE.search(blob):
        return False
    return bool(
        _ANALYSIS_OPINION_RE.search(blob)
        or _ANALYSIS_QUESTION_RE.search(str(title or "").strip())
    )


def _extract_nre_incidents(source: SourceDef, body: str) -> list[ExtractedItem]:
    """GM rail disruptions from the National Rail Enquiries Incidents API.

    Ignores ``body`` (the standard pre-fetch of the status page, kept only so
    the collector marks the source reachable) — the real data comes from the
    token-authenticated NRE feed via the nre_incidents adapter. Best-effort:
    returns [] on any failure so the transport stage never blocks.
    """
    from news_digest.pipeline import nre_incidents as _nre  # noqa: PLC0415

    fetched_at = now_london().isoformat()
    items: list[ExtractedItem] = []
    try:
        incidents = _nre.gm_incidents()
    except Exception as exc:  # noqa: BLE001
        logger.warning("NRE incidents extractor failed: %s", exc)
        return _extract_national_rail(source, body)
    for inc in incidents:
        summ = (inc.get("summary") or "").strip()
        if not summ:
            continue
        # Prefix the operator so the card/LLM can lead with it ("Transport for
        # Wales: …") — the operator name is often absent from the summary text.
        ops = inc.get("operators") or []
        op = str(ops[0]).strip() if ops else ""
        if op and op.lower() not in summ.lower():
            summ = f"{op}: {summ}"
        routes = (inc.get("routes") or "").strip()
        end = inc.get("end") or ""
        until = ""
        if end:
            try:
                d = datetime.fromisoformat(end).date()
                until = f"Until {d.day} {_TFGM_MONTHS_EN[d.month - 1]}."
            except ValueError:
                until = ""
        detail = " ".join(p for p in (summ, routes, until) if p)
        slug = re.sub(r"[^a-z0-9]+", "-", summ.lower()).strip("-")[:80]
        items.append(
            ExtractedItem(
                title=_clean_title_text(summ),
                url=f"{source.url.rstrip('/')}/{slug}",
                published_at=fetched_at,
                summary=_clean_snippet(summ)[:500],
                evidence_text=_clean_snippet(detail, max_chars=600),
            )
        )
    return items or _extract_national_rail(source, body)


def _extract_source_candidates(source: SourceDef, body: str) -> list[dict]:
    if source.source_type == "json_funnelback":
        links = _extract_funnelback_items(body)
    elif source.source_type == "xml_sitemap":
        links = _extract_sitemap_items(source.url, body)
    elif source.source_type == "json_ticketmaster":
        links = _extract_ticketmaster_items(source, body)
    elif source.source_type == "json_skiddle":
        links = _extract_skiddle_api_items(source, body)
    elif source.source_type == "json_wp_rest":
        links = _extract_wp_rest_items(body)
    elif source.source_type == "markdown_links":
        links = _extract_markdown_link_items(body)
    elif source.source_type == "html_tfgm_alerts":
        links = _extract_tfgm_alerts(source, body)
    elif source.source_type == "json_nre_incidents":
        links = _extract_nre_incidents(source, body)
    elif source.source_type == "json_national_rail":
        links = _extract_national_rail(source, body)
    elif source.source_type == "html_eventbrite":
        links = _extract_eventbrite_markets(source, body)
    elif source.source_type == "html_eventbrite_events":
        links = _extract_eventbrite_events(source, body)
    elif source.source_type == "html_kontramarka":
        links = _extract_kontramarka_items(body)
    elif source.source_type == "html_eventfirst":
        links = _extract_eventfirst_items(body)
    elif source.source_type == "html_page_event":
        links = _extract_html_page_event(source, body)
    elif source.source_type == "html_visitmanchester_events":
        links = _extract_visit_manchester_events(source, body)
    elif source.source_type == "html_phm_events":
        links = _extract_phm_events(source, body)
    elif source.source_type == "html_the_manc_weekly_events":
        links = _extract_the_manc_weekly_events(source, body)
    elif source.name.startswith("Manchester Theatres"):
        links = _extract_manchester_theatres_events(source, body)
    elif source.source_type == "html_sectioned_event_guide":
        links = _extract_sectioned_event_guide(source, body)
    elif source.source_type == "html_designmynight":
        links = _extract_designmynight_cards(source, body)
    elif source.source_type == "html_heritage_live":
        links = _extract_heritage_live_items(source, body)
    elif source.name in {"Skiddle Manchester", "Skiddle Manchester Bank Holiday"}:
        links = _extract_skiddle_items(source, body)
    elif source.name == "GMMH":
        links = _extract_gmmh_press_releases(source, body)
    elif source.name == "Manchester United":
        links = _extract_manutd_items(source, body)
    elif source.name in {"Manchester City", "Manchester City Men"}:
        links = _extract_mancity_items(source, body)
    elif source.name in {"BBC Sport Manchester United", "BBC Sport Manchester City"}:
        links = _extract_bbc_sport_team_items(source, body)
    elif source.name == "RNCM":
        links = _extract_rncm_items(source, body)
    elif "<rss" in body[:500].lower() or "<feed" in body[:500].lower():
        links = _extract_feed_items(source.url, body)
    else:
        parser = LinkExtractor(source.url)
        parser.feed(body)
        links = parser.links
        if source.name == "Trafford Council":
            links = _extract_slug_link_items(
                source.url,
                body,
                r"^/news/20\d{2}/",
            )

    jsonld_event_links = _extract_jsonld_event_items(source, body)
    if jsonld_event_links:
        links = jsonld_event_links + links

    seen: set[str] = set()
    candidates: list[dict] = []
    for item in links:
        base_url = clean_url(item.url)
        fragment = parse.urlsplit(item.url).fragment
        jsonld_event_item = str(item.enrichment_status or "") == "ok_jsonld_event"
        normalized_url = (
            f"{base_url}#{fragment}"
            if (
                source.source_type in {"html_the_manc_weekly_events", "html_sectioned_event_guide", "html_heritage_live"}
                or jsonld_event_item
            ) and fragment
            else base_url
        )
        same_source_page = source.source_type in {"html_page_event", "html_the_manc_weekly_events", "html_sectioned_event_guide", "html_heritage_live"} and base_url == clean_url(source.url)
        if not jsonld_event_item and not same_source_page and not _is_allowed_source_link(source, base_url, item.title, item.summary):
            continue
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        item = _enrich_item(source, item)
        if source.report_category == "diaspora_events" and not _looks_like_diaspora_event_signal(
            source.name,
            item.title,
            item.summary,
            item.lead,
            item.evidence_text,
        ):
            continue
        published_at = item.published_at or _published_at_from_title_or_url(item.title, normalized_url)
        freshness_status = _freshness_status(source, published_at)
        primary_block = _resolve_primary_block(source, published_at)
        if _is_outside_gm_ticket_source(source):
            primary_block = "outside_gm_tickets"
        if source.report_category in {"media_layer", "gmp"} and freshness_status in {"stale", "unknown"}:
            # Stale/undated city items only deserve a Городской радар slot
            # when the title or summary actually carries a topical keyword
            # (police/fire/election/transport/etc). Otherwise it's just
            # 'soft city background' that crowds the digest — drop it.
            text_blob = f"{item.title or ''} {item.summary or ''}"
            if not _looks_like_city_watch_topical(text_blob):
                continue
            primary_block = "city_watch"
        if source.report_category == "transport" and _is_stale_transport(published_at, item.title):
            continue
        if source.report_category == "public_services" and _is_stale_public_service(published_at, item.title):
            # GMMH and similar public-services sources publish soft PR
            # news (awards, surveys, new term launches) alongside real
            # disruptions (strikes, closures). Keep stale items visible to
            # validation/source-health so the source is not misreported as
            # empty, but demote them out of today_focus.
            primary_block = "city_watch"
        if primary_block == "last_24h" and _looks_like_analysis_opinion(item.title, item.summary):
            # #2 analysis/opinion/profile → Городской радар, not «Свежие новости».
            primary_block = "city_watch"
        candidate = {
            "title": item.title,
            "category": source.candidate_category,
            "summary": item.summary or _default_summary(source, item.title),
            "source_url": normalized_url,
            "source_label": source.name,
            "primary_block": primary_block,
            "include": True,
            "dedupe_decision": "new",
            "carry_over_label": "",
            "reason": "Collected from live source; pending dedupe review.",
            "matched_previous_fingerprint": "",
            "practical_angle": _default_practical_angle(source, item.title, item.summary),
            "lead": item.lead or _default_lead(source, item.title, item.summary),
            "event_page_type": "official" if source.report_category in {"venues_tickets", "culture_weekly", "diaspora_events"} else "unknown",
            "published_at": published_at,
            "published_date_london": published_at[:10] if published_at else "",
            "freshness_status": freshness_status,
            "source_health": "dated" if published_at else "undated",
            "evidence_text": item.evidence_text,
            "enrichment_status": item.enrichment_status,
            "source_trial": bool(getattr(source, "trial", False)),
            "structured_event_hint": dict(item.structured_event_hint or {}),
        }
        if source.primary_block == "last_24h" and primary_block not in {"last_24h", "city_watch"}:
            candidate["reason"] = (
                "Collected from live source; kept out of last_24h until publication time is recent and confirmed."
            )
        # Block-policy filters: ticket horizon, listicle openings, football fluff.
        _adjust_ticket_radar_block(candidate)
        if source.report_category == "food_openings" and _is_listicle_opening(item.title):
            continue
        # Drop food_openings entries with a publication date older than 21 days.
        # Last week's digest carried Popeyes from late February and Sticks'n'Sushi
        # from March 30 — those slots should rotate, not freeze. We accept
        # undated items (some sources never expose a date) and items in the
        # future (announced openings).
        if source.report_category == "food_openings" and published_at:
            try:
                pub_day = published_at[:10]
                if pub_day and pub_day < (now_london().date().isoformat()):
                    from datetime import date  # noqa: PLC0415
                    delta = (now_london().date() - date.fromisoformat(pub_day)).days
                    if delta > 21:
                        continue
            except (ValueError, TypeError):
                pass
        if source.report_category == "football" and _is_football_fluff(item.title, normalized_url):
            continue
        candidate["fingerprint"] = fingerprint_for_candidate(candidate)
        event_instance_id = str((item.structured_event_hint or {}).get("event_instance_id") or "").strip()
        if event_instance_id:
            candidate["event_instance_id"] = event_instance_id
            candidate["fingerprint"] = f"{candidate['fingerprint']}-{event_instance_id}"[:180]
        candidates.append(candidate)
        if len(candidates) >= source.max_candidates:
            break
    return candidates
