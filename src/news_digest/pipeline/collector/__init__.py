"""News collector package.

Public API (kept stable for backwards compatibility — both
`scripts/run_local_digest.py` and any future callers should import from
this top-level only):

- `collect_digest(project_root)` — run the live broad-scan stage.
- `initialize_collector_state(project_root, *, overwrite=False)` —
  create a stub `collector_report.json` for fresh checkouts.
- `StageResult` — dataclass returned by the entry points.

Internal modules (read directly only when changing collector internals):

- `sources` — `SourceDef`, `ExtractedItem`, `SOURCES` registry
- `fetch` — HTTP layer with browser-shaped headers + fallback URLs
- `dates` — RFC822/ISO/URL-path date parsing
- `summary` — text cleaners, lead/summary/practical_angle defaults
- `filters` — per-source URL/title gates, GM tokens, listicle/football/city_watch policies
- `routing` — freshness, primary_block routing, today_focus promotion, ticket horizon
- `extract` — `LinkExtractor`, RSS/Atom/Funnelback parsers, og:description enrichment
- `weather` — Met Office forecast HTML parser
- `fallbacks` — synthetic candidates (weather, transport, last_24h, short_actions)
- `core` — `collect_digest` entry point and the `_default_report` shape
"""

from __future__ import annotations

from .core import StageResult, collect_digest, initialize_collector_state

__all__ = ["StageResult", "collect_digest", "initialize_collector_state"]
