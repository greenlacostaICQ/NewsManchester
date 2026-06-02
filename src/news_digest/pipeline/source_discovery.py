from __future__ import annotations

from dataclasses import dataclass, asdict
from html.parser import HTMLParser
from pathlib import Path
from urllib import parse
import re

from news_digest.pipeline.collector.fetch import _fetch_text
from news_digest.pipeline.collector.sources import SOURCES


SOURCE_DISCOVERY_VERSION = 1

_DEFAULT_SEEDS = (
    "https://www.manchester.gov.uk/",
    "https://www.salford.gov.uk/",
    "https://www.trafford.gov.uk/",
    "https://www.stockport.gov.uk/",
    "https://www.oldham.gov.uk/",
    "https://www.rochdale.gov.uk/",
    "https://www.bolton.gov.uk/",
    "https://www.bury.gov.uk/",
    "https://www.wigan.gov.uk/",
    "https://www.tameside.gov.uk/",
    "https://tfgm.com/",
    "https://www.visitmanchester.com/",
)

_SOURCE_HINT_RE = re.compile(
    r"\b(news|updates?|events?|what'?s\s+on|consultations?|planning|roadworks?|closures?|alerts?|travel|tickets?)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DiscoveredSource:
    seed_url: str
    url: str
    kind: str
    source_type_guess: str
    reason: str
    recommended_name: str
    report_category_guess: str
    primary_block_guess: str
    trial: bool = True


class _DiscoveryParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.feeds: list[str] = []
        self.links: list[tuple[str, str]] = []
        self._href = ""
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        if tag == "link":
            rel = str(attrs_dict.get("rel") or "").lower()
            typ = str(attrs_dict.get("type") or "").lower()
            href = attrs_dict.get("href") or ""
            if "alternate" in rel and ("rss" in typ or "atom" in typ or "xml" in typ) and href:
                self.feeds.append(parse.urljoin(self.base_url, href))
            return
        if tag != "a":
            return
        href = attrs_dict.get("href") or ""
        if href:
            self._href = parse.urljoin(self.base_url, href)
            self._text = [attrs_dict.get("aria-label") or attrs_dict.get("title") or ""]

    def handle_data(self, data: str) -> None:
        if self._href:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._href:
            text = re.sub(r"\s+", " ", " ".join(self._text)).strip()
            self.links.append((self._href, text))
            self._href = ""
            self._text = []


def _known_hosts() -> set[str]:
    hosts: set[str] = set()
    for source in SOURCES:
        for value in [source.url, *source.fallback_urls, *source.allowed_hosts]:
            host = parse.urlparse(value if "://" in value else f"https://{value}").hostname or ""
            if host:
                hosts.add(host.lower().removeprefix("www."))
    return hosts


def _guess(url: str, text: str) -> tuple[str, str, str]:
    blob = f"{url} {text}".lower()
    if "consult" in blob:
        return "council", "city_watch", "html"
    if "planning" in blob:
        return "council", "city_watch", "html"
    if any(token in blob for token in ("event", "what", "ticket", "calendar")):
        return "culture_weekly", "next_7_days", "html"
    if any(token in blob for token in ("roadwork", "closure", "travel", "alert")):
        return "transport", "transport", "html"
    return "media_layer", "city_watch", "html"


def _name_from_url(url: str, text: str) -> str:
    label = re.sub(r"\s+", " ", text).strip(" -–—|")
    if label and len(label) <= 60:
        return label
    host = parse.urlparse(url).hostname or url
    path = parse.urlparse(url).path.strip("/").split("/")[:2]
    suffix = " ".join(part.replace("-", " ").title() for part in path if part)
    return f"{host.removeprefix('www.')} {suffix}".strip()


def discover_sources(
    seeds: list[str] | None = None,
    *,
    fetcher=_fetch_text,
    limit_per_seed: int = 12,
) -> list[dict]:
    known = _known_hosts()
    out: list[DiscoveredSource] = []
    seen: set[str] = set()
    for seed in seeds or list(_DEFAULT_SEEDS):
        try:
            body = fetcher(seed)
        except Exception:
            continue
        parser = _DiscoveryParser(seed)
        parser.feed(body)
        candidates: list[tuple[str, str, str]] = []
        for feed_url in parser.feeds:
            candidates.append((feed_url, "RSS/Atom autodiscovery", "rss"))
        sitemap = parse.urljoin(seed, "/sitemap.xml")
        candidates.append((sitemap, "standard sitemap location", "xml_sitemap"))
        for href, text in parser.links:
            if _SOURCE_HINT_RE.search(f"{href} {text}"):
                candidates.append((href, f"navigation link: {text[:80]}", "html"))

        for url, reason, kind in candidates[:limit_per_seed]:
            cleaned = parse.urldefrag(url)[0]
            host = (parse.urlparse(cleaned).hostname or "").lower().removeprefix("www.")
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            if host in known and kind != "xml_sitemap":
                continue
            report_category, primary_block, source_type = _guess(cleaned, reason)
            out.append(
                DiscoveredSource(
                    seed_url=seed,
                    url=cleaned,
                    kind=kind,
                    source_type_guess=source_type if kind != "rss" else "rss",
                    reason=reason,
                    recommended_name=_name_from_url(cleaned, reason),
                    report_category_guess=report_category,
                    primary_block_guess=primary_block,
                )
            )
    return [asdict(item) for item in out]


def write_discovery_report(project_root: Path, *, seeds: list[str] | None = None) -> Path:
    from news_digest.pipeline.common import now_london, today_london, write_json

    state_dir = project_root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SOURCE_DISCOVERY_VERSION,
        "run_at_london": now_london().isoformat(),
        "run_date_london": today_london(),
        "recommendations": discover_sources(seeds),
    }
    path = state_dir / "source_discovery_report.json"
    write_json(path, payload)
    return path
