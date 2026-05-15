"""Per-source extraction and enrichment.

`_extract_source_candidates` is the per-source orchestrator: it picks
the right parser (Funnelback JSON / RSS-Atom / HTML anchors), de-dups
URLs, calls `_enrich_item` to thicken thin RSS summaries with article
HTML, and routes each item to the right primary_block.

`_extract_meta_description` and `_extract_article_published_at` are the
HTML enrichment helpers used after re-fetching an article.
"""

from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
from urllib import parse
import json
import re
import xml.etree.ElementTree as ET

from news_digest.pipeline.common import clean_url, fingerprint_for_candidate, now_london

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
        if any(token in lowered for token in ("subscribe", "newsletter", "advertisement", "cookies", "privacy policy")):
            continue
        key = re.sub(r"[^a-z0-9а-яё]+", " ", lowered).strip()
        if not key or key in seen or (title_key and key == title_key):
            continue
        seen.add(key)
        paragraphs.append(snippet)
        if len(paragraphs) >= 6:
            break
    return " ".join(paragraphs)


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
        }
        or source.candidate_category == "council"
    )


def _enrich_item(source: SourceDef, item: ExtractedItem) -> ExtractedItem:
    if not _should_enrich_source(source):
        return item
    summary_thin = _is_thin_summary(item.summary, item.title)
    force_fetch = (
        source.report_category in {"media_layer", "gmp", "food_openings"}
        or source.candidate_category == "council"
        or source.name == "Albert Hall Manchester"
    )
    if item.published_at and not summary_thin and not force_fetch:
        return ExtractedItem(
            title=_clean_title_text(item.title),
            url=item.url,
            published_at=item.published_at,
            summary=_source_specific_summary(source, item.title, item.summary),
            lead=item.lead or _derive_lead(source, item.title, item.summary),
            evidence_text=item.summary,
            enrichment_status="skipped_existing_summary",
        )
    try:
        article_html = _fetch_text(item.url)
    except Exception as exc:  # noqa: BLE001 - enrichment is best-effort.
        return ExtractedItem(
            title=_clean_title_text(item.title),
            url=item.url,
            published_at=item.published_at,
            summary=item.summary,
            lead=item.lead,
            evidence_text=item.summary,
            enrichment_status=f"failed: {exc}",
        )

    paragraph_evidence = _extract_paragraph_evidence(article_html, item.title)
    enriched_summary = _extract_jsonld_description(article_html) or _extract_meta_description(article_html)
    enriched_title = _extract_jsonld_title(article_html) or _extract_page_title(article_html)
    evidence_text = paragraph_evidence or enriched_summary or item.summary
    if summary_thin and paragraph_evidence:
        enriched_summary = _summary_from_evidence(paragraph_evidence)
    summary = item.summary
    if summary_thin and enriched_summary and not _is_thin_summary(enriched_summary, item.title):
        summary = enriched_summary
    elif not summary:
        summary = enriched_summary
    summary = _source_specific_summary(source, item.title, summary)
    lead = item.lead or _derive_lead(source, item.title, summary)
    published_at = (
        item.published_at
        or _extract_jsonld_start_date(article_html)
        or _extract_article_published_at(article_html)
        or _published_at_from_title_or_url(item.title, item.url)
    )
    return ExtractedItem(
        title=_clean_title_text(unescape(enriched_title or item.title)),
        url=item.url,
        published_at=published_at,
        summary=summary,
        lead=lead,
        evidence_text=_clean_snippet(evidence_text)[:2500],
        enrichment_status="ok" if evidence_text else "ok_no_evidence",
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


_TFGM_ALERT_PATTERN = re.compile(
    r'\\"title\\":\\"([^"\\]{5,200})\\"[^}]{0,800}?\\"description\\":\\"([^"\\]{10,500})\\"'
)

# TfGM travel-alerts page surfaces both public-transport disruptions AND general
# road/motorway/active-travel works. We only want items relevant to public
# transport users. An item is kept only if its title or description mentions a
# recognisable public-transport keyword.
_TFGM_PUBLIC_TRANSPORT_RE = re.compile(
    r'\b(metrolink|trams?|bus\b|buses|coach|bee\s+network|rail|train|northern|transpennine|'
    r'piccadilly|victoria|altrincham\s+line|bury\s+line|eccles\s+line|ashton\s+line|'
    r'rochdale\s+line|didsbury\s+line|airport\s+line|stop\s+closure)\b',
    re.IGNORECASE,
)


def _extract_tfgm_alerts(source: SourceDef, body: str) -> list[ExtractedItem]:
    """Extract TfGM travel alerts from Next.js inline JSON.

    The /travel-updates/travel-alerts page server-renders alert data inside
    `self.__next_f.push([1, "...escaped JSON..."])` strings. Each alert has
    a title (location + cause) and description. Bee Network bus disruptions
    are surfaced on the same page.

    Alerts don't have per-item permalinks — all items point at the source
    listing URL. Curator + LLM rewrite turn them into single-line entries.
    """

    items: list[ExtractedItem] = []
    seen: set[str] = set()
    # Alerts are live — stamp them with current time so the transport
    # staleness check (which drops items without a published_at) keeps them.
    fetched_at = now_london().isoformat()
    for match in _TFGM_ALERT_PATTERN.finditer(body):
        title = match.group(1).strip().replace('\\u0026', '&')
        description = match.group(2).strip().replace('\\u0026', '&')
        if not title or title in seen:
            continue
        # Only include alerts relevant to public transport users. Road/motorway/
        # active-travel works without a transit component are dropped here — they
        # belong in a separate "driving" section, not the transit brief.
        if not _TFGM_PUBLIC_TRANSPORT_RE.search(title + " " + description):
            continue
        seen.add(title)
        # Alerts have no per-item permalink. Synthesize a unique path so
        # fingerprint_for_candidate sees distinct items (clean_url drops
        # query/fragment, so we put the slug in the path itself).
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
_NATIONAL_RAIL_GM_OPERATOR_NAMES = (
    "northern",
    "transpennine",
)


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
        lowered = (name or "").lower()
        if any(op in lowered for op in _NATIONAL_RAIL_GM_OPERATOR_NAMES):
            return True
        for op in operators_collection or []:
            op_name = str(op.get("name") or "").lower()
            if any(o in op_name for o in _NATIONAL_RAIL_GM_OPERATOR_NAMES):
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
    article_match = re.search(
        r"<(?:article|main)[^>]*>(.*?)</(?:article|main)>",
        body,
        flags=re.IGNORECASE | re.DOTALL,
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
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(str(value or "")))).strip()


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


def _is_outside_gm_ticket_source(source: SourceDef) -> bool:
    lowered = source.name.lower()
    return "ticketmaster liverpool" in lowered or "ticketmaster london major" in lowered


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


def _extract_source_candidates(source: SourceDef, body: str) -> list[dict]:
    if source.source_type == "json_funnelback":
        links = _extract_funnelback_items(body)
    elif source.source_type == "xml_sitemap":
        links = _extract_sitemap_items(source.url, body)
    elif source.source_type == "json_ticketmaster":
        links = _extract_ticketmaster_items(source, body)
    elif source.source_type == "json_wp_rest":
        links = _extract_wp_rest_items(body)
    elif source.source_type == "markdown_links":
        links = _extract_markdown_link_items(body)
    elif source.source_type == "html_tfgm_alerts":
        links = _extract_tfgm_alerts(source, body)
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
        base_url = clean_url(item.url)
        fragment = parse.urlsplit(item.url).fragment
        normalized_url = f"{base_url}#{fragment}" if source.source_type == "html_the_manc_weekly_events" and fragment else base_url
        same_source_page = source.source_type in {"html_page_event", "html_the_manc_weekly_events"} and base_url == clean_url(source.url)
        if not same_source_page and not _is_allowed_source_link(source, base_url, item.title, item.summary):
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
            "event_page_type": "official" if source.report_category in {"venues_tickets", "culture_weekly", "diaspora_events"} else "unknown",
            "published_at": published_at,
            "published_date_london": published_at[:10] if published_at else "",
            "freshness_status": freshness_status,
            "source_health": "dated" if published_at else "undated",
            "evidence_text": item.evidence_text,
            "enrichment_status": item.enrichment_status,
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
        candidates.append(candidate)
        if len(candidates) >= source.max_candidates:
            break
    return candidates
