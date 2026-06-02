"""Simple per-source anomaly detection (Phase 2 #4).

Plain statistics over ``source_health_history.jsonl`` — no ML, no ADTK.
The goal is one warning: "this feed produced material every day for a
week and today it went dark", which usually means a parser broke or the
source changed its markup. Warning-only; it never blocks the release.

Signal is the raw candidate count per source (how much the feed yielded
*before* editorial filtering), because curated/rendered legitimately hit
zero on quiet days — a raw drop to zero is the real "source is broken".
"""

from __future__ import annotations

from statistics import median

# A source must have produced at least this much on a typical day for a
# drop to be worth flagging — keeps low-volume feeds out of the noise.
_MIN_MEDIAN = 3.0
# Need at least this many baseline days, else we are still warming up.
_MIN_BASELINE_DAYS = 4
# Trailing window (days, excluding today) used for the median.
_WINDOW_DAYS = 7
# Today counts as a drop if it is at or below this fraction of the median.
_DROP_FRACTION = 0.34


def detect_source_anomalies(history: list[dict]) -> list[dict]:
    """Return one row per source whose raw yield dropped sharply today.

    ``history`` is the full jsonl (any order). The most recent
    ``run_date_london`` is "today"; the preceding window is the baseline.
    """
    rows = [r for r in history if isinstance(r, dict) and r.get("run_date_london")]
    if len(rows) < _MIN_BASELINE_DAYS + 1:
        return []
    rows.sort(key=lambda r: str(r.get("run_date_london")))
    today = rows[-1]
    baseline = rows[-(_WINDOW_DAYS + 1):-1]

    # Per-source raw history across the baseline window.
    baseline_raw: dict[str, list[int]] = {}
    category_of: dict[str, str] = {}
    for row in baseline:
        for src in row.get("sources") or []:
            if not isinstance(src, dict):
                continue
            if src.get("trial"):
                continue
            name = str(src.get("name") or "")
            if not name:
                continue
            baseline_raw.setdefault(name, []).append(int(src.get("raw") or 0))

    today_raw: dict[str, int] = {}
    for src in today.get("sources") or []:
        if isinstance(src, dict) and src.get("name"):
            if src.get("trial"):
                continue
            name = str(src["name"])
            today_raw[name] = int(src.get("raw") or 0)
            category_of[name] = str(src.get("category") or "")

    anomalies: list[dict] = []
    for name, samples in baseline_raw.items():
        if len(samples) < _MIN_BASELINE_DAYS:
            continue
        med = median(samples)
        if med < _MIN_MEDIAN:
            continue
        current = today_raw.get(name, 0)
        if current <= med * _DROP_FRACTION:
            anomalies.append(
                {
                    "name": name,
                    "category": category_of.get(name, ""),
                    "today_raw": current,
                    "median_raw": round(med, 1),
                    "baseline_days": len(samples),
                    "reason": (
                        f"raw yield {current} today vs 7-day median {round(med, 1)} "
                        f"over {len(samples)} day(s)"
                    ),
                }
            )
    anomalies.sort(key=lambda a: a["median_raw"], reverse=True)
    return anomalies


# A source that fetched OK but parsed zero candidate links on at least this
# many recent days is a likely-broken parser (the site changed its markup,
# or never had a working per-source extractor) — distinct from a feed going
# dark after being healthy (that is ``detect_source_anomalies`` above) and
# from a fetch failure (``status == "failed"`` = Cloudflare/WAF/network,
# which a new extractor cannot fix).
_DEAD_PARSER_MIN_DAYS = 4


def detect_dead_parsers(history: list[dict]) -> list[dict]:
    """Return sources that fetched OK but parsed nothing for the whole window.

    The signal is ``status == "empty"`` ("fetched but no candidate links
    parsed"). A source stuck there across every observed day in the trailing
    window has a parser that needs a dedicated per-source extractor — this
    names exactly which sources to write one for.
    """
    rows = [r for r in history if isinstance(r, dict) and r.get("run_date_london")]
    if len(rows) < _DEAD_PARSER_MIN_DAYS:
        return []
    rows.sort(key=lambda r: str(r.get("run_date_london")))
    window = rows[-_WINDOW_DAYS:]

    seen: dict[str, list[str]] = {}
    category_of: dict[str, str] = {}
    for row in window:
        for src in row.get("sources") or []:
            if not isinstance(src, dict):
                continue
            if src.get("trial"):
                continue
            name = str(src.get("name") or "")
            if not name:
                continue
            seen.setdefault(name, []).append(str(src.get("status") or ""))
            category_of[name] = str(src.get("category") or "")

    dead: list[dict] = []
    for name, statuses in seen.items():
        if len(statuses) < _DEAD_PARSER_MIN_DAYS:
            continue
        if all(s == "empty" for s in statuses):
            dead.append(
                {
                    "name": name,
                    "category": category_of.get(name, ""),
                    "days": len(statuses),
                    "reason": (
                        f"fetched OK but parsed 0 items on {len(statuses)} day(s) — "
                        "parser likely needs a per-source extractor"
                    ),
                }
            )
    dead.sort(key=lambda d: d["days"], reverse=True)
    return dead
