"""Source registry and data shapes for the collector.

`SourceDef` describes one external source (URL, category, fallback list,
allowed hosts). `ExtractedItem` is the canonical shape produced by
parsers (RSS, JSON, HTML). `SOURCES` is the live registry; everything
else in the collector iterates over it.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True, slots=True)
class SourceDef:
    name: str
    report_category: str
    candidate_category: str
    url: str
    primary_block: str
    source_type: str = "html"
    # If the primary URL fails (HTTP 403, 5xx, timeout), try these in
    # order. Use this for sources that block bot UA on the HTML landing
    # page but expose the same content via RSS/Atom or a sister path.
    fallback_urls: tuple[str, ...] = ()
    # If non-empty, accept item links from any of these host suffixes
    # instead of inferring the allowed host from `url`. Use this when
    # the primary URL is a feed (e.g. feeds.bbci.co.uk) and items point
    # at the canonical content domain (e.g. bbc.com/news/articles/...).
    allowed_hosts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ExtractedItem:
    title: str
    url: str
    published_at: str | None = None
    summary: str = ""
    lead: str = ""


_SOURCES_TOML = Path(__file__).parents[4] / "data" / "sources.toml"


def _load_sources() -> tuple[SourceDef, ...]:
    with open(_SOURCES_TOML, "rb") as _f:
        _data = tomllib.load(_f)
    return tuple(
        SourceDef(
            s["name"],
            s["report_category"],
            s["candidate_category"],
            s["url"],
            s["primary_block"],
            source_type=s.get("source_type", "html"),
            fallback_urls=tuple(s.get("fallback_urls", [])),
            allowed_hosts=tuple(s.get("allowed_hosts", [])),
        )
        for s in _data["sources"]
        if s.get("enabled", True)
    )


SOURCES: tuple[SourceDef, ...] = _load_sources()
