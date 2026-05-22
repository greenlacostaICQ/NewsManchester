from __future__ import annotations

from collections import OrderedDict
import re

ENTITY_SCHEMA_VERSION = 2  # added "people" bucket for cross-day same-victim/same-suspect dedup


def _rx(value: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-zА-Яа-яЁё0-9]){re.escape(value)}(?![A-Za-zА-Яа-яЁё0-9])", re.IGNORECASE)


_BOROUGHS: dict[str, tuple[str, ...]] = {
    "Bolton": ("Bolton",),
    "Bury": ("Bury",),
    "Manchester": ("Manchester", "City of Manchester"),
    "Oldham": ("Oldham",),
    "Rochdale": ("Rochdale",),
    "Salford": ("Salford",),
    "Stockport": ("Stockport",),
    "Tameside": ("Tameside",),
    "Trafford": ("Trafford",),
    "Wigan": ("Wigan",),
}

_DISTRICTS: dict[str, tuple[str, ...]] = {
    "Altrincham": ("Altrincham",),
    "Ancoats": ("Ancoats",),
    "Ashton-under-Lyne": ("Ashton-under-Lyne", "Ashton under Lyne"),
    "Chorlton": ("Chorlton", "Chorlton-cum-Hardy"),
    "City Centre": ("City Centre", "Manchester city centre"),
    "Deansgate": ("Deansgate",),
    "Didsbury": ("Didsbury",),
    "Eccles": ("Eccles",),
    "First Street": ("First Street",),
    "Hulme": ("Hulme",),
    "Levenshulme": ("Levenshulme",),
    "Makerfield": ("Makerfield",),
    "Moss Side": ("Moss Side",),
    "Northern Quarter": ("Northern Quarter", "NQ"),
    "Old Trafford": ("Old Trafford",),
    "Prestwich": ("Prestwich",),
    "Spinningfields": ("Spinningfields",),
    "Stretford": ("Stretford",),
    "Urmston": ("Urmston",),
    "Wythenshawe": ("Wythenshawe",),
}

_STATIONS: dict[str, tuple[str, ...]] = {
    "Manchester Piccadilly": ("Manchester Piccadilly", "Piccadilly station"),
    "Manchester Victoria": ("Manchester Victoria", "Victoria station"),
    "Manchester Oxford Road": ("Manchester Oxford Road", "Oxford Road station"),
    "Deansgate": ("Deansgate station",),
    "Stockport": ("Stockport station",),
    "Bolton": ("Bolton station",),
    "Bury": ("Bury Interchange",),
    "Altrincham": ("Altrincham Interchange", "Altrincham station"),
    "Rochdale Town Centre": ("Rochdale Town Centre",),
    "Oldham Mumps": ("Oldham Mumps",),
    "MediaCityUK": ("MediaCityUK", "Media City UK"),
    "Trafford Centre": ("Trafford Centre tram stop", "The Trafford Centre tram stop"),
}

_COUNCILS: dict[str, tuple[str, ...]] = {
    "Bolton Council": ("Bolton Council", "Bolton Council's"),
    "Bury Council": ("Bury Council",),
    "Manchester City Council": ("Manchester City Council", "Manchester Council"),
    "Oldham Council": ("Oldham Council",),
    "Rochdale Borough Council": ("Rochdale Borough Council", "Rochdale Council"),
    "Salford City Council": ("Salford City Council", "Salford Council"),
    "Stockport Council": ("Stockport Council",),
    "Tameside Council": ("Tameside Council",),
    "Trafford Council": ("Trafford Council",),
    "Wigan Council": ("Wigan Council",),
    "GMCA": ("GMCA", "Greater Manchester Combined Authority"),
}

_VENUES: dict[str, tuple[str, ...]] = {
    "AO Arena": ("AO Arena", "Manchester AO Arena"),
    "Aviva Studios": ("Aviva Studios", "Factory International"),
    "Bridgewater Hall": ("Bridgewater Hall", "The Bridgewater Hall"),
    "Co-op Live": ("Co-op Live", "Co op Live"),
    "Depot Mayfield": ("Depot Mayfield", "Mayfield Depot"),
    "Manchester Academy": ("Manchester Academy",),
    "Manchester Apollo": ("Manchester Apollo", "O2 Apollo Manchester"),
    "Manchester Central": ("Manchester Central",),
    "Manchester Museum": ("Manchester Museum",),
    "Old Trafford": ("Old Trafford",),
    "Palace Theatre": ("Palace Theatre",),
    "People's History Museum": ("People's History Museum", "PHM"),
    "The Deaf Institute": ("The Deaf Institute",),
    "The Lowry": ("The Lowry",),
    "The Ritz": ("The Ritz", "O2 Ritz Manchester"),
    "The Whitworth": ("The Whitworth", "Whitworth Art Gallery"),
    "Trafford Centre": ("Trafford Centre", "The Trafford Centre"),
    "Victoria Warehouse": ("Victoria Warehouse",),
}

_CLUBS: dict[str, tuple[str, ...]] = {
    "Manchester City": ("Manchester City", "Man City", "MCFC"),
    "Manchester United": ("Manchester United", "Man United", "MUFC"),
    "Bolton Wanderers": ("Bolton Wanderers",),
    "Bury FC": ("Bury FC",),
    "FC United of Manchester": ("FC United of Manchester", "FC United"),
    "Oldham Athletic": ("Oldham Athletic",),
    "Rochdale AFC": ("Rochdale AFC",),
    "Salford City": ("Salford City",),
    "Stockport County": ("Stockport County",),
    "Wigan Athletic": ("Wigan Athletic",),
}

_KNOWN_COMPANIES: dict[str, tuple[str, ...]] = {
    "BBC": ("BBC", "BBC Manchester"),
    "Co-op": ("Co-op", "Co-op Group", "Co-operative Group"),
    "Greater Manchester Mental Health NHS Foundation Trust": (
        "Greater Manchester Mental Health NHS Foundation Trust",
        "GMMH",
    ),
    "ITV": ("ITV", "ITV Granada"),
    "Metrolink": ("Metrolink",),
    "Northern": ("Northern", "Northern Rail"),
    "Openreach": ("Openreach",),
    "Prolific North": ("Prolific North",),
    "Stagecoach": ("Stagecoach",),
    "TfGM": ("TfGM", "Transport for Greater Manchester"),
    "The Mill": ("The Mill",),
    "Ticketmaster": ("Ticketmaster",),
    "TransPennine Express": ("TransPennine Express", "TPE"),
}

_COMPANY_SUFFIX_RE = re.compile(
    r"\b([A-Z][A-Za-z0-9&.'-]*(?:\s+[A-Z][A-Za-z0-9&.'-]*){0,4}\s+"
    r"(?:Ltd|Limited|Group|Trust|Foundation|University|College|Airports?|Developments?|Partners|Studios))\b"
)

_TEXT_FIELDS = ("title", "summary", "lead", "practical_angle", "evidence_text", "source_label")


# --- People extraction (cross-day same-victim/same-suspect dedup) -----
#
# We don't ship a real NER. Instead we look for capitalised name-shaped
# tokens that sit next to phrases that tag someone as a real person in
# the news sense (victim, accused, organiser, family of, the 17-year-old).
# This catches Erica de Souza Correa, Mohanad Goobe, Ian Brown, etc.,
# while rejecting "Manchester Arena", "Greater Manchester Police",
# "The Lowry" — those are venues/orgs.
#
# False positives are filtered against the entity stopword list below and
# against a length / capitalisation check inside _candidate_people.

# Phrases that indicate the next/previous capitalised name is a person.
# Cyrillic + Latin variants. We compile WITHOUT IGNORECASE so that the
# uppercase character classes in the name capture group ([A-Z], [А-ЯЁ])
# stay strict — otherwise lowercase verbs after the name leak into the
# captured group ("Mohanad Goobe in Moss"). The anchor phrases at the
# edges are simple lowercase words so case-sensitivity costs us nothing
# on the anchor side.
_EN_NAME = r"[A-Z][A-Za-z'’-]{1,}(?:\s+(?:de\s+|van\s+|von\s+)?[A-Z][A-Za-z'’-]{1,}){1,3}"
_RU_NAME = r"[А-ЯЁ][А-Яа-яёЁ'’-]{1,}(?:\s+(?:де\s+|фон\s+|ван\s+)?[А-ЯЁ][А-Яа-яёЁ'’-]{1,}){1,4}"

_PEOPLE_ANCHOR_PATTERNS = (
    # English: anchor word (lower-case) → capitalised name follows.
    rf"(?:victim|deceased|accused|suspect|defendant|killer|attacker|"
    rf"witness|complainant|driver|cyclist|pedestrian|"
    rf"the\s+(?:teenager|man|woman|boy|girl|teen|youth))\s+({_EN_NAME})",
    # English: name → trailing verb (lower-case).
    rf"({_EN_NAME})\s*,?\s*"
    rf"(?:has\s+been\s+(?:charged|jailed|sentenced|named|killed|attacked|murdered)|"
    rf"was\s+(?:charged|jailed|sentenced|killed|attacked|murdered|named|hit)|"
    rf"died|pleaded\s+(?:guilty|not\s+guilty)|appeared\s+in\s+court|"
    rf"is\s+accused|has\s+been\s+remanded)",
    # English age-prefixed: "15-year-old Mohanad Goobe".
    rf"\d{{1,3}}-year-old\s+({_EN_NAME})",
    # Russian: anchor word → name follows. Russian morphology means the
    # anchor word has many forms (жертва / жертвой / жертвы …) so we
    # accept any single lower-case Cyrillic token of plausible shape
    # right after the role keyword, then the capitalised name.
    rf"(?:жертв[а-яё]*|погибш[а-яё]+|подозреваем[а-яё]+|обвиняем[а-яё]+|"
    rf"осуждённ[а-яё]+|свидетель|пострадавш[а-яё]+|водитель|велосипедист|"
    rf"пешеход|семь[а-яё]+|родственник[а-яё]*|\d{{1,3}}-летн[а-яё]+)\s+"
    rf"({_RU_NAME})",
    # Russian: name → trailing verb.
    rf"({_RU_NAME})\s*,?\s*"
    rf"(?:погиб(?:ла)?|убил(?:а)?|арестован(?:а)?|осуждён(?:а)?|приговорён(?:а)?|"
    rf"обвиняется|обвинён(?:а)?|разыскивается|задержан(?:а)?|"
    rf"скончал(?:ся|ась)|подозревается)",
)
# UNICODE flag matters for the Russian patterns; no IGNORECASE.
_PEOPLE_ANCHOR_RE = tuple(re.compile(pat, re.UNICODE) for pat in _PEOPLE_ANCHOR_PATTERNS)

# Tokens that look name-shaped but are place/org/role and must NEVER be
# treated as a person. Lower-cased compare.
_PEOPLE_STOPWORDS = frozenset({
    "manchester", "greater manchester", "greater manchester police",
    "manchester arena", "manchester city", "manchester united",
    "salford", "salford city", "bolton", "bury", "oldham", "rochdale",
    "stockport", "tameside", "trafford", "wigan",
    "manchester piccadilly", "manchester victoria", "piccadilly",
    "manchester airport", "the lowry", "the deaf institute",
    "gmp", "tfgm", "nhs", "metrolink", "national rail",
    "manchester city council", "bbc manchester", "the manc", "men",
    "premier league", "city centre", "town centre", "bank holiday",
    "manchester arena bombing",
    # Anniversary/commemoration is not a person.
    "manchester arena attack", "ariana grande concert",
})


def _looks_like_person_name(name: str) -> bool:
    """Filter for plausible person names: at least one space (first+last),
    each word starts with a capital, total length 6-60, not in stopwords.
    Rejects URLs, dates, all-caps acronyms, single tokens.
    """
    cleaned = re.sub(r"\s+", " ", name).strip(" ,.;:|-'\"")
    if not cleaned or len(cleaned) < 6 or len(cleaned) > 60:
        return False
    if cleaned.lower() in _PEOPLE_STOPWORDS:
        return False
    # Must have at least two capitalised words, neither all-uppercase.
    words = cleaned.split()
    if len(words) < 2:
        return False
    # Particles/prepositions inside names: lowercase EN + RU.
    particles = {
        "de", "van", "von", "der", "of", "the", "la", "le", "del", "di",
        "де", "фон", "ван", "ди", "ла", "ле",
    }
    for word in words:
        if word.lower() in particles:
            continue
        if not word[:1].isupper():
            return False
        if word.isupper() and len(word) > 3:
            # All-caps acronyms like NHS, GMP, BBC.
            return False
    return True


def _normalize_person_name(name: str) -> str:
    """Canonical form for cross-day matching: lowercase, single-spaced."""
    return re.sub(r"\s+", " ", name).strip(" ,.;:|-'\"").lower()


def _candidate_people(blob: str) -> list[str]:
    """Find person-name candidates by anchored regex, dedupe by canonical
    form, return original-cased names in document order.
    """
    seen: dict[str, str] = {}
    for pattern in _PEOPLE_ANCHOR_RE:
        for match in pattern.finditer(blob):
            name = match.group(1)
            if not _looks_like_person_name(name):
                continue
            canon = _normalize_person_name(name)
            if canon not in seen:
                cleaned = re.sub(r"\s+", " ", name).strip(" ,.;:|-'\"")
                seen[canon] = cleaned
    return list(seen.values())


def _candidate_blob(candidate: dict) -> str:
    return " ".join(str(candidate.get(field) or "") for field in _TEXT_FIELDS)


def _add_entity(
    out: "OrderedDict[tuple[str, str], dict]",
    *,
    entity_type: str,
    name: str,
    matched: str,
    confidence: float,
) -> None:
    key = (entity_type, name.lower())
    existing = out.get(key)
    payload = {
        "type": entity_type,
        "name": name,
        "matched": matched,
        "confidence": confidence,
    }
    if existing is None or confidence > float(existing.get("confidence") or 0.0):
        out[key] = payload


def _scan_aliases(
    out: "OrderedDict[tuple[str, str], dict]",
    text: str,
    entity_type: str,
    aliases_by_name: dict[str, tuple[str, ...]],
    confidence: float = 0.95,
) -> None:
    for name, aliases in aliases_by_name.items():
        for alias in aliases:
            match = _rx(alias).search(text)
            if match and _alias_match_allowed(text, match, entity_type, name, alias):
                _add_entity(out, entity_type=entity_type, name=name, matched=alias, confidence=confidence)
                break


def _alias_match_allowed(
    text: str,
    match: re.Match[str],
    entity_type: str,
    name: str,
    alias: str,
) -> bool:
    before = text[max(0, match.start() - 16):match.start()].lower()
    after = text[match.end():match.end() + 24].lower()
    alias_l = alias.lower()
    if entity_type == "borough" and name == "Manchester" and before.endswith("greater "):
        return False
    if entity_type == "club" and name == "Manchester City" and alias_l == "manchester city":
        if re.match(r"\s+(centre|center|council|region|news)\b", after):
            return False
    if entity_type == "club" and name == "Manchester United" and alias_l == "manchester united":
        if re.match(r"\s+(kingdom|utilities|authority)\b", after):
            return False
    return True


def extract_entities(candidate: dict) -> dict:
    """Return structured entities for a digest candidate.

    Shape is stable and intentionally simple: typed lists for common
    queries plus a flat ``all`` list when downstream needs confidence or
    the matched alias.
    """
    text = _candidate_blob(candidate)
    out: "OrderedDict[tuple[str, str], dict]" = OrderedDict()

    _scan_aliases(out, text, "borough", _BOROUGHS)
    _scan_aliases(out, text, "district", _DISTRICTS)
    _scan_aliases(out, text, "station", _STATIONS)
    _scan_aliases(out, text, "council", _COUNCILS)
    _scan_aliases(out, text, "venue", _VENUES)
    _scan_aliases(out, text, "club", _CLUBS)
    _scan_aliases(out, text, "company", _KNOWN_COMPANIES)

    for match in _COMPANY_SUFFIX_RE.finditer(text):
        name = re.sub(r"\s+", " ", match.group(1)).strip(" ,.;:|-")
        if len(name) >= 5:
            _add_entity(out, entity_type="company", name=name, matched=name, confidence=0.72)

    entities = list(out.values())
    by_type: dict[str, list[str]] = {
        "boroughs": [],
        "districts": [],
        "stations": [],
        "councils": [],
        "venues": [],
        "clubs": [],
        "companies": [],
    }
    plural = {
        "borough": "boroughs",
        "district": "districts",
        "station": "stations",
        "council": "councils",
        "venue": "venues",
        "club": "clubs",
        "company": "companies",
    }
    for entity in entities:
        bucket = plural.get(str(entity.get("type") or ""))
        if bucket:
            by_type[bucket].append(str(entity["name"]))

    people = _candidate_people(text)

    return {
        "schema_version": ENTITY_SCHEMA_VERSION,
        **by_type,
        "people": people,
        "all": entities,
    }


def enrich_candidate_entities(candidate: dict) -> dict:
    candidate["entities"] = extract_entities(candidate)
    boroughs = candidate["entities"].get("boroughs") or []
    if boroughs and not candidate.get("borough"):
        candidate["borough"] = boroughs[0]
    return candidate


def enrich_candidates_entities(candidates: list[dict]) -> list[dict]:
    for candidate in candidates:
        if isinstance(candidate, dict):
            enrich_candidate_entities(candidate)
    return candidates
