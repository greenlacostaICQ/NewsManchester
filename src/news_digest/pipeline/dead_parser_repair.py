from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
from urllib import parse
import re

from news_digest.pipeline.collector.fetch import _fetch_text
from news_digest.pipeline.collector.sources import SOURCES, SourceDef
from news_digest.pipeline.common import read_json, write_json, now_london, today_london


DEAD_PARSER_REPAIR_VERSION = 1


class _LinkProbe(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[dict[str, str]] = []
        self._href = ""
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_dict = dict(attrs)
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
            if text:
                self.links.append({"url": parse.urldefrag(self._href)[0], "text": text[:140]})
            self._href = ""
            self._text = []


def _source_by_name() -> dict[str, SourceDef]:
    return {source.name: source for source in SOURCES}


def _looks_candidate_link(row: dict[str, str]) -> bool:
    blob = f"{row.get('url', '')} {row.get('text', '')}"
    return bool(re.search(r"\b(news|events?|what'?s-on|consult|planning|updates?|alerts?|tickets?|20\d{2})\b", blob, re.IGNORECASE))


def _suggest_repair(source: SourceDef, links: list[dict[str, str]], body: str) -> str:
    if "<rss" in body[:600].lower() or "<feed" in body[:600].lower():
        return "switch source_type to rss or Atom parser"
    if re.search(r"<loc>https?://[^<]+</loc>", body, re.IGNORECASE):
        return "switch source_type to xml_sitemap"
    candidate_links = [link for link in links if _looks_candidate_link(link)]
    if candidate_links:
        return "write a per-source HTML link extractor using visible article/event links"
    if re.search(r"__NEXT_DATA__|application/json|window\.__", body):
        return "site likely renders data in embedded JSON; add JSON extractor"
    return "no obvious static links; check WAF/JS-rendered page or use a better fallback URL"


def build_dead_parser_repair_report(project_root: Path, *, fetcher=_fetch_text) -> dict:
    release = read_json(project_root / "data" / "state" / "release_report.json", {})
    dead = [item for item in (release.get("dead_parsers") or []) if isinstance(item, dict)]
    sources = _source_by_name()
    repairs: list[dict] = []
    for item in dead:
        name = str(item.get("name") or "")
        source = sources.get(name)
        if source is None:
            repairs.append({"name": name, "status": "unknown_source", "suggestion": "source is not in current SOURCES registry"})
            continue
        try:
            body = fetcher(source.url)
        except Exception as exc:  # noqa: BLE001
            repairs.append({"name": name, "url": source.url, "status": "fetch_failed", "error": str(exc)})
            continue
        parser = _LinkProbe(source.url)
        parser.feed(body)
        links = parser.links[:50]
        candidate_links = [link for link in links if _looks_candidate_link(link)][:12]
        repairs.append(
            {
                "name": name,
                "url": source.url,
                "category": source.report_category,
                "source_type": source.source_type,
                "status": "probed",
                "suggestion": _suggest_repair(source, links, body),
                "candidate_links": candidate_links,
            }
        )
    return {
        "schema_version": DEAD_PARSER_REPAIR_VERSION,
        "run_at_london": now_london().isoformat(),
        "run_date_london": today_london(),
        "repairs": repairs,
    }


def write_dead_parser_repair_report(project_root: Path) -> Path:
    state_dir = project_root / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "dead_parser_repair_report.json"
    write_json(path, build_dead_parser_repair_report(project_root))
    return path
