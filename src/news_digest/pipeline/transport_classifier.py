"""Classify transport alerts into mode + Russian operator label.

TfGM raw titles look like "Church Street, Eccles - Bus Stop Closure" or
"Hibbert Lane, Marple - Road Closure" — neither starts with the mode, so
the LLM keeps picking the source label ("TfGM") as the operator. This
pre-pass reads title + summary + URL and writes two fields to the
candidate dict:

* `transport_mode`  ∈ {"bus", "tram", "rail", "coach", "road"}
* `expected_operator`  — Russian-facing prefix the LLM must use
  ("Автобус:", "Metrolink:", "Northern:" …)

The rewrite prompt then reads `expected_operator` and prepends it verbatim,
so we never depend on the model to infer mode from a roadworks bulletin.
"""
from __future__ import annotations

import re


_TITLE_BUS_RE = re.compile(r"\bbus\s+stop\b|\bbus\s+lane\b|\bbus\s+route\b|\bbus\s+services?\b", re.IGNORECASE)
# Metrolink line names — TfGM titles like "Ashton/Eccles Lines - Minor Delay"
# and "Bury Line - Stop Closure" omit the word Metrolink, so also match the
# distinctive "<area>/<area> Line(s)" pattern.
_METROLINK_LINE_NAMES = (
    "ashton", "altrincham", "bury", "eccles", "rochdale", "oldham",
    "trafford park", "trafford center", "manchester airport",
    "east didsbury", "didsbury", "media city", "deansgate-castlefield",
    "piccadilly", "victoria",
)
_TITLE_TRAM_RE = re.compile(
    r"\bmetrolink\b|\btrams?\b|\bstop\s+(?:open|closure)\b.*\bline\b|"
    r"\b(?:" + "|".join(re.escape(n) for n in _METROLINK_LINE_NAMES) + r")(?:\s*/\s*\w+)*\s+lines?\b",
    re.IGNORECASE,
)
_TITLE_RAIL_RE = re.compile(
    r"\b(?:northern|transpennine|avanti|tpe|east\s+midlands|emr|chiltern|"
    r"crosscountry|cross\s+country|grand\s+central|hull\s+trains|lumo|"
    r"national\s+rail|network\s+rail)\b",
    re.IGNORECASE,
)
_TITLE_ROAD_RE = re.compile(r"\b(?:road\s+closure|roadworks?|road\s+works?|diversion)\b", re.IGNORECASE)
_SUMMARY_BUS_RE = re.compile(r"\bbus\s+services?\s+(?:are\s+)?(?:on\s+)?diver(?:t|sion)|bus\s+stops?\b", re.IGNORECASE)


# TfGM road-closure alerts almost always mean "buses diverted" because TfGM
# tracks the transport-relevant impact. We surface those as bus alerts.
_OPERATOR_LABEL: dict[str, str] = {
    "bus": "Автобус",
    "tram": "Metrolink",
    "rail": "Поезд",  # default; rail-specific operator name handled below
    "coach": "Coach",
    "road": "Дорога",
}


def _classify_rail_operator(title: str, summary: str) -> str | None:
    """Pick the canonical English operator name when title names one explicitly."""
    text = f"{title} {summary}"
    # Order matters: "TransPennine Express" must beat "Express" etc.
    for canonical in (
        "TransPennine Express",
        "Avanti West Coast",
        "East Midlands Railway",
        "CrossCountry",
        "Northern",
        "Grand Central",
        "Hull Trains",
        "Lumo",
        "Chiltern",
        "Network Rail",
        "National Rail",
    ):
        if re.search(rf"\b{re.escape(canonical)}\b", text, re.IGNORECASE):
            return canonical
    return None


def classify_transport_candidate(candidate: dict) -> None:
    """Enrich candidate with transport_mode + expected_operator (in-place).

    Safe to call on non-transport candidates: it inspects primary_block /
    category and exits if the candidate isn't a transport alert.
    """
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block != "transport" and category != "transport":
        return

    title = str(candidate.get("title") or "")
    summary = str(candidate.get("summary") or "")
    url = str(candidate.get("source_url") or "")
    path = url.split("?", 1)[0].lower()

    mode: str | None = None

    if _TITLE_TRAM_RE.search(title) or "metrolink" in path:
        mode = "tram"
    elif _TITLE_RAIL_RE.search(title):
        mode = "rail"
    elif _TITLE_BUS_RE.search(title) or _SUMMARY_BUS_RE.search(summary):
        mode = "bus"
    elif _TITLE_ROAD_RE.search(title) and _SUMMARY_BUS_RE.search(summary):
        # "Road Closure" titles where summary mentions diverted buses.
        mode = "bus"
    elif _TITLE_ROAD_RE.search(title):
        mode = "road"

    if not mode:
        return

    candidate["transport_mode"] = mode
    if mode == "rail":
        operator = _classify_rail_operator(title, summary)
        candidate["expected_operator"] = operator or _OPERATOR_LABEL[mode]
    else:
        candidate["expected_operator"] = _OPERATOR_LABEL[mode]


def annotate_transport_candidates(candidates: list[dict]) -> int:
    """Annotate every transport candidate. Returns the count enriched."""
    n = 0
    for c in candidates:
        if not isinstance(c, dict):
            continue
        before = c.get("expected_operator")
        classify_transport_candidate(c)
        if c.get("expected_operator") and c.get("expected_operator") != before:
            n += 1
    return n
