"""Per-source extraction and enrichment.

`_extract_source_candidates` is the per-source orchestrator: it picks
the right parser (Funnelback JSON / RSS-Atom / HTML anchors), de-dups
URLs, calls `_enrich_item` to thicken thin RSS summaries with article
HTML, and routes each item to the right primary_block.

`_extract_meta_description` and `_extract_article_published_at` are the
HTML enrichment helpers used after re-fetching an article.
"""

from __future__ import annotations

from html.parser import HTMLParser
from urllib import parse
import json
import re
import xml.etree.ElementTree as ET

from news_digest.pipeline.common import clean_url, fingerprint_for_candidate

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
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if href:
            self._href = parse.urljoin(self.base_url, href)
            self._text = []

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


def _enrich_item(source: SourceDef, item: ExtractedItem) -> ExtractedItem:
    if source.report_category not in {"media_layer", "gmp", "public_services", "culture_weekly"} and source.candidate_category != "council":
        return item
    summary_thin = _is_thin_summary(item.summary, item.title)
    if item.published_at and not summary_thin:
        return item
    try:
        article_html = _fetch_text(item.url)
    except Exception:
        return item

    enriched_summary = _extract_meta_description(article_html)
    summary = item.summary
    if summary_thin and enriched_summary and not _is_thin_summary(enriched_summary, item.title):
        summary = enriched_summary
    elif not summary:
        summary = enriched_summary
    summary = _source_specific_summary(source, item.title, summary)
    lead = item.lead or _derive_lead(source, item.title, summary)
    published_at = item.published_at or _extract_article_published_at(article_html) or _published_at_from_title_or_url(item.title, item.url)
    return ExtractedItem(
        title=_clean_title_text(item.title),
        url=item.url,
        published_at=published_at,
        summary=summary,
        lead=lead,
    )


def _extract_feed_items(base_url: str, body: str) -> list[ExtractedItem]:
    items: list[ExtractedItem] = []
    root = ET.fromstring(body)
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        published_at = _feed_item_published_at(item)
        if title and link and _looks_like_candidate_title(title):
            items.append(
                ExtractedItem(
                    title=_clean_title_text(title),
                    url=parse.urljoin(base_url, link),
                    published_at=published_at,
                    summary=_clean_snippet(item.findtext("description") or ""),
                )
            )
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = (entry.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
        link_el = entry.find("{http://www.w3.org/2005/Atom}link")
        href = link_el.attrib.get("href", "") if link_el is not None else ""
        published_at = _feed_item_published_at(entry)
        if title and href and _looks_like_candidate_title(title):
            items.append(
                ExtractedItem(
                    title=_clean_title_text(title),
                    url=parse.urljoin(base_url, href),
                    published_at=published_at,
                    summary=_clean_snippet(entry.findtext("{http://www.w3.org/2005/Atom}summary") or ""),
                )
            )
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
    "eventim apollo",
    "hyde park",
    "london stadium",
    "ovo arena",
    "royal albert hall",
    "the o2",
    "tottenham hotspur stadium",
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
        classifications = event.get("classifications") or []
        genre = ""
        if classifications:
            genre = str((classifications[0].get("genre") or {}).get("name") or "").strip()
        title_parts = [title]
        if event_start:
            title_parts.append(f"event {event_start[:10]}")
        if onsale_start:
            title_parts.append(f"public sale {onsale_start[:16].replace('T', ' ')}")
        display_title = " — ".join(title_parts)
        summary_parts = [
            venue,
            city,
            genre,
            f"event_date={_format_ticketmaster_date(event_start_raw)}" if event_start_raw else "",
            f"public_onsale={_format_ticketmaster_date(onsale_start_raw)}" if onsale_start_raw else "",
            "ticket_signal=onsale" if onsale_scan else "ticket_signal=upcoming_event",
        ]
        summary = " | ".join(filter(None, summary_parts))
        if title and url and _looks_like_candidate_title(display_title):
            items.append(
                ExtractedItem(
                    title=_clean_title_text(display_title),
                    url=url,
                    published_at=onsale_start if onsale_scan and onsale_start else event_start,
                    summary=summary,
                )
            )
    return items


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


def _extract_source_candidates(source: SourceDef, body: str) -> list[dict]:
    if source.source_type == "json_funnelback":
        links = _extract_funnelback_items(body)
    elif source.source_type == "json_ticketmaster":
        links = _extract_ticketmaster_items(source, body)
    elif source.source_type == "json_wp_rest":
        links = _extract_wp_rest_items(body)
    elif source.source_type == "markdown_links":
        links = _extract_markdown_link_items(body)
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

    seen: set[str] = set()
    candidates: list[dict] = []
    for item in links:
        normalized_url = clean_url(item.url)
        if not _is_allowed_source_link(source, normalized_url, item.title, item.summary):
            continue
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        item = _enrich_item(source, item)
        published_at = item.published_at or _published_at_from_title_or_url(item.title, normalized_url)
        freshness_status = _freshness_status(source, published_at)
        primary_block = _resolve_primary_block(source, published_at)
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
            # disruptions (strikes, closures). Items older than 7 days
            # that look like active disruptions are demoted to city_watch;
            # everything else older than 7 days is dropped entirely so
            # today_focus is not flooded with stale NHS press releases.
            if _looks_like_active_disruption(item.title):
                primary_block = "city_watch"
            else:
                continue
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
            "event_page_type": "official" if source.report_category in {"venues_tickets", "culture_weekly"} else "unknown",
            "published_at": published_at,
            "published_date_london": published_at[:10] if published_at else "",
            "freshness_status": freshness_status,
            "source_health": "dated" if published_at else "undated",
        }
        if source.primary_block == "last_24h" and primary_block not in {"last_24h", "city_watch"}:
            candidate["reason"] = (
                "Collected from live source; kept out of last_24h until publication time is recent and confirmed."
            )
        # Block-policy filters: ticket horizon, listicle openings, football fluff.
        _adjust_ticket_radar_block(candidate)
        if source.report_category == "food_openings" and _is_listicle_opening(item.title):
            continue
        if source.report_category == "football" and _is_football_fluff(item.title, normalized_url):
            continue
        candidate["fingerprint"] = fingerprint_for_candidate(candidate)
        candidates.append(candidate)
        if len(candidates) >= 5:
            break
    return candidates
