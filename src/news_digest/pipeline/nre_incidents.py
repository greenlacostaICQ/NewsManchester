"""National Rail Enquiries (NRE) Knowledgebase Incidents feed — GM rail disruptions.

Token auth: POST /authenticate with NRE_USERNAME / NRE_PASSWORD → token →
GET /api/staticfeeds/5.0/incidents with X-Auth-Token. Parsed to the GM-relevant
rail disruptions used by the transport section, replacing the old operator-dump
scrape of the public status page.

Best-effort by design: any failure (no creds, auth error, fetch error, parse
error) returns [] so the digest never blocks on rail data. Attribution: data
must be credited to National Rail Enquiries (terms of the open-data licence).

The NRDP portal is migrating to RDM in 2026 (6 months notice before shutdown),
so the source endpoint/auth live behind this one module — migration is a local
change here, not a pipeline-wide rewrite.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

ATTRIBUTION = "National Rail Enquiries"
_AUTH_URL = "https://opendata.nationalrail.co.uk/authenticate"
_INCIDENTS_URL = "https://opendata.nationalrail.co.uk/api/staticfeeds/5.0/incidents"

# An incident is GM only if its affected ROUTE or SUMMARY names one of these.
# Bare "Victoria"/"Piccadilly"/"Oxford Road" are EXCLUDED — they collide with
# London Victoria etc. (a Maidstone notice leaked through "London Victoria").
# The GM versions are always written in full ("Manchester Piccadilly"), so the
# "Manchester" stem covers them; the rest are unambiguous GM towns/stations.
_GM_TERMS = re.compile(
    r"\b(Manchester|Salford|Bolton|Stockport|Wigan|Rochdale|Oldham|Deansgate|"
    r"Bury(?!\s+St)|"  # 'Bury' the GM town, NOT 'Bury St Edmunds' (Suffolk)
    r"Altrincham|Ashton-under-Lyne|Eccles|Hazel Grove|Gatley|Marple|Hindley|"
    r"Patricroft|Newton-le-Willows|Hattersley|Levenshulme|Heaton Chapel|Reddish|"
    r"Bredbury|Romiley|Swinton|Walkden|Irlam|Denton|Guide Bridge|Mills Hill|"
    r"Moston|Littleborough|Smithy Bridge|Castlefield)\b",
    re.IGNORECASE,
)
# A morning brief cares about real disruptions + daytime works, not overnight
# tweaks. This flags purely late-night/early-hours planned amendments to drop.
_OVERNIGHT_RE = re.compile(r"\b(late night|overnight|2[0-3]:\d\d|0[0-5]:\d\d)\b", re.IGNORECASE)
_MAX_RAIL_ITEMS = 8


def _token() -> str:
    user = os.environ.get("NRE_USERNAME", "").strip()
    pw = os.environ.get("NRE_PASSWORD", "").strip()
    if not user or not pw:
        logger.info("NRE creds not set — skipping rail incidents feed.")
        return ""
    data = urllib.parse.urlencode({"username": user, "password": pw}).encode()
    req = urllib.request.Request(
        _AUTH_URL, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return str(json.loads(resp.read().decode()).get("token", "") or "")
    except Exception as exc:  # noqa: BLE001 - never block the digest on rail auth
        logger.warning("NRE auth failed: %s", exc)
        return ""


def _fetch_incidents_xml(token: str) -> str:
    req = urllib.request.Request(_INCIDENTS_URL, headers={"X-Auth-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=40) as resp:
            return resp.read().decode("utf-8", "replace")
    except Exception as exc:  # noqa: BLE001
        logger.warning("NRE incidents fetch failed: %s", exc)
        return ""


def _cdata(block: str, tag: str) -> str:
    m = re.search(r"<" + tag + r">\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</" + tag + r">", block, re.S)
    return re.sub(r"<[^>]+>", " ", re.sub(r"\s+", " ", m.group(1))).strip() if m else ""


def _tag(block: str, tag: str) -> str:
    m = re.search(r"<(?:com:)?" + tag + r">(.*?)</(?:com:)?" + tag + r">", block, re.S)
    return m.group(1).strip() if m else ""


def parse_incidents(xml: str) -> list[dict]:
    """Parse PtIncident blocks → list of dicts. Tolerant regex (CDATA-heavy XML)."""
    out: list[dict] = []
    for block in xml.split("<PtIncident>")[1:]:
        out.append({
            "summary": _cdata(block, "Summary"),
            "routes": _cdata(block, "RoutesAffected"),
            "operators": re.findall(r"<OperatorName>(.*?)</OperatorName>", block),
            "start": _tag(block, "StartTime")[:10],
            "end": _tag(block, "EndTime")[:10],
            "planned": _tag(block, "Planned").lower() == "true",
        })
    return out


def relevant_today(start: str, end: str, planned: bool, today: datetime.date) -> bool:
    """Anti-flood: a multi-day PLANNED work is news only NEAR its start or end,
    not on every single day of a weeks-long window (that's what buried the
    reader in repeated 'buses replace trains until 28 June' / 'Prestwich until
    19 Aug' lines). Unplanned disruptions always show.
    """
    if not planned:
        return True
    if not start and not end:
        return True  # no dates — can't tell, show it

    def _near(value: str) -> bool:
        try:
            return abs((datetime.date.fromisoformat(value[:10]) - today).days) <= 1
        except (ValueError, TypeError):
            return False

    return bool((start and _near(start)) or (end and _near(end)))


def gm_incidents(today: datetime.date | None = None) -> list[dict]:
    """Return GM-relevant, currently-active rail incidents (best-effort, [] on any failure)."""
    token = _token()
    if not token:
        return []
    xml = _fetch_incidents_xml(token)
    if not xml:
        return []
    today = today or datetime.date.today()
    result: list[dict] = []
    try:
        incidents = parse_incidents(xml)
    except Exception as exc:  # noqa: BLE001
        logger.warning("NRE incidents parse failed: %s", exc)
        return []
    for inc in incidents:
        # GM match on the SUMMARY (the headline names the actual affected
        # service). Matching the route list over-includes national incidents
        # that list a Manchester leg among many regions.
        if not _GM_TERMS.search(inc["summary"]):
            continue
        end = inc.get("end") or ""
        try:
            if end and datetime.date.fromisoformat(end) < today:
                continue  # already over
        except ValueError:
            pass
        # Editorial cut: drop purely overnight/late-night PLANNED amendments
        # (23:28, 22:14, "late night") unless they also hit the early-morning
        # peak. Unplanned disruptions are always kept.
        if inc["planned"]:
            s = inc["summary"]
            if _OVERNIGHT_RE.search(s) and "early morning" not in s.lower():
                continue
        # Anti-flood: long multi-day planned works only near start/end.
        if not relevant_today(inc.get("start", ""), inc.get("end", ""), inc["planned"], today):
            continue
        result.append(inc)
    # Unplanned (real disruptions) first, then by soonest end date; capped so a
    # quiet day shows a handful, not a wall of engineering notices.
    result.sort(key=lambda x: (x.get("planned", True), x.get("end") or "9999"))
    return result[:_MAX_RAIL_ITEMS]
