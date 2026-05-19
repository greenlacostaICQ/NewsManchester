from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import re

from news_digest.pipeline.common import fingerprint_for_candidate, normalize_title
from news_digest.pipeline.entity_extraction import extract_entities


CITY_INTELLIGENCE_SCHEMA_VERSION = 1

GM_BOROUGHS: tuple[str, ...] = (
    "Bolton",
    "Bury",
    "Manchester",
    "Oldham",
    "Rochdale",
    "Salford",
    "Stockport",
    "Tameside",
    "Trafford",
    "Wigan",
)

_BOROUGH_ORDER = {name: idx for idx, name in enumerate(GM_BOROUGHS)}

_DISTRICT_TO_BOROUGH: dict[str, str] = {
    "Altrincham": "Trafford",
    "Ancoats": "Manchester",
    "Ashton-under-Lyne": "Tameside",
    "Chorlton": "Manchester",
    "City Centre": "Manchester",
    "Deansgate": "Manchester",
    "Didsbury": "Manchester",
    "Eccles": "Salford",
    "First Street": "Manchester",
    "Hulme": "Manchester",
    "Levenshulme": "Manchester",
    "Makerfield": "Wigan",
    "Moss Side": "Manchester",
    "Northern Quarter": "Manchester",
    "Old Trafford": "Trafford",
    "Prestwich": "Bury",
    "Spinningfields": "Manchester",
    "Stretford": "Trafford",
    "Urmston": "Trafford",
    "Wythenshawe": "Manchester",
}

_STATION_TO_BOROUGH: dict[str, str] = {
    "Manchester Piccadilly": "Manchester",
    "Manchester Victoria": "Manchester",
    "Manchester Oxford Road": "Manchester",
    "Deansgate": "Manchester",
    "Stockport": "Stockport",
    "Bolton": "Bolton",
    "Bury": "Bury",
    "Altrincham": "Trafford",
    "Rochdale Town Centre": "Rochdale",
    "Oldham Mumps": "Oldham",
    "MediaCityUK": "Salford",
    "Trafford Centre": "Trafford",
}

_COUNCIL_TO_BOROUGH: dict[str, str] = {
    "Bolton Council": "Bolton",
    "Bury Council": "Bury",
    "Manchester City Council": "Manchester",
    "Oldham Council": "Oldham",
    "Rochdale Borough Council": "Rochdale",
    "Salford City Council": "Salford",
    "Stockport Council": "Stockport",
    "Tameside Council": "Tameside",
    "Trafford Council": "Trafford",
    "Wigan Council": "Wigan",
}

_SOURCE_TO_BOROUGH: dict[str, str] = {
    "Bolton Council": "Bolton",
    "Bury Council": "Bury",
    "Manchester Council": "Manchester",
    "Manchester City Council": "Manchester",
    "Oldham Council": "Oldham",
    "Rochdale Council": "Rochdale",
    "Rochdale Borough Council": "Rochdale",
    "Salford Council": "Salford",
    "Salford City Council": "Salford",
    "Stockport Council": "Stockport",
    "Tameside Council": "Tameside",
    "Trafford Council": "Trafford",
    "Wigan Council": "Wigan",
}

_VENUE_TO_BOROUGH: dict[str, str] = {
    "AO Arena": "Manchester",
    "Aviva Studios": "Manchester",
    "Bridgewater Hall": "Manchester",
    "Co-op Live": "Manchester",
    "Depot Mayfield": "Manchester",
    "Manchester Academy": "Manchester",
    "Manchester Apollo": "Manchester",
    "Manchester Central": "Manchester",
    "Manchester Museum": "Manchester",
    "Old Trafford": "Trafford",
    "Palace Theatre": "Manchester",
    "People's History Museum": "Manchester",
    "The Deaf Institute": "Manchester",
    "The Lowry": "Salford",
    "The Ritz": "Manchester",
    "The Whitworth": "Manchester",
    "Trafford Centre": "Trafford",
    "Victoria Warehouse": "Trafford",
}

_CLUB_TO_BOROUGH: dict[str, str] = {
    "Manchester City": "Manchester",
    "Manchester United": "Trafford",
    "Bolton Wanderers": "Bolton",
    "Bury FC": "Bury",
    "FC United of Manchester": "Manchester",
    "Oldham Athletic": "Oldham",
    "Rochdale AFC": "Rochdale",
    "Salford City": "Salford",
    "Stockport County": "Stockport",
    "Wigan Athletic": "Wigan",
}

_TOPIC_PATTERNS: dict[str, tuple[str, ...]] = {
    "housing": (
        r"\bhousing\b",
        r"\bhomes?\b",
        r"\bflats?\b",
        r"\bapartments?\b",
        r"\brent\b",
        r"\baffordable\b",
        r"\bdevelopment\b",
        r"\bregeneration\b",
        r"\bplanning\b",
        r"\bproperty\b",
        r"\bжиль[её]\b",
        r"\bквартир",
    ),
    "transport": (
        r"\btram(?:s)?\b",
        r"\bmetrolink\b",
        r"\bbus(?:es)?\b",
        r"\brail\b",
        r"\btrain(?:s)?\b",
        r"\bstation\b",
        r"\broadworks?\b",
        r"\bM60\b",
        r"\bclosure\b",
        r"\bdelay(?:s|ed)?\b",
        r"\bdiversion\b",
        r"\bтрамва",
        r"\bавтобус",
        r"\bпоезд",
    ),
    "policing": (
        r"\bpolice\b",
        r"\barrest(?:ed|s)?\b",
        r"\bcharged\b",
        r"\bcourt\b",
        r"\bmurder\b",
        r"\battack\b",
        r"\brobbery\b",
        r"\bfire\b",
        r"\bdrugs?\b",
        r"\bassault\b",
        r"\bполици",
        r"\bсуд\b",
        r"\bарест",
    ),
    "health": (
        r"\bNHS\b",
        r"\bhospital\b",
        r"\bhealth\b",
        r"\bmental health\b",
        r"\bcare\b",
        r"\bGMMH\b",
        r"\bclinic\b",
        r"\bбольниц",
        r"\bздоров",
    ),
    "council": (
        r"\bcouncil\b",
        r"\bcouncillors?\b",
        r"\bmayor\b",
        r"\belection\b",
        r"\bbudget\b",
        r"\bconsultation\b",
        r"\bGMCA\b",
        r"\bдепутат",
        r"\bсовет",
        r"\bмэр",
        r"\bбюджет",
    ),
    "weather": (
        r"\bweather\b",
        r"\brain\b",
        r"\bwind\b",
        r"\bMet Office\b",
        r"\bflood\b",
        r"\bforecast\b",
        r"\bпогод",
        r"\bдожд",
        r"\bветер",
    ),
    "business": (
        r"\bbusiness\b",
        r"\bcompany\b",
        r"\bjobs?\b",
        r"\bfunding\b",
        r"\binvest(?:ment|s)?\b",
        r"\bstartup\b",
        r"\btech\b",
        r"\bretail\b",
        r"\beconomy\b",
        r"\bбизнес",
        r"\bкомпани",
    ),
    "culture": (
        r"\bfestival\b",
        r"\bconcert\b",
        r"\btheatre\b",
        r"\bexhibition\b",
        r"\bmuseum\b",
        r"\bgig\b",
        r"\bshow\b",
        r"\barts?\b",
        r"\bmusic\b",
        r"\bконцерт",
        r"\bтеатр",
        r"\bвыстав",
    ),
    "events": (
        r"\bevent\b",
        r"\bticket(?:s)?\b",
        r"\bonsale\b",
        r"\bline-?up\b",
        r"\bweekend\b",
        r"\bwhat'?s on\b",
        r"\bафиш",
        r"\bбилет",
    ),
    "food": (
        r"\brestaurant\b",
        r"\bcafe\b",
        r"\bpub\b",
        r"\bbar\b",
        r"\bopening\b",
        r"\bmarket\b",
        r"\bfood\b",
        r"\bdining\b",
        r"\bменю\b",
        r"\bресторан",
        r"\bкафе",
    ),
    "football": (
        r"\bfootball\b",
        r"\bManchester United\b",
        r"\bManchester City\b",
        r"\btransfer\b",
        r"\bfixture\b",
        r"\bmatch\b",
        r"\bPremier League\b",
        r"\bфутбол",
    ),
    "education": (
        r"\bschool\b",
        r"\buniversity\b",
        r"\bcollege\b",
        r"\bstudents?\b",
        r"\beducation\b",
        r"\bшкол",
        r"\bуниверситет",
    ),
    "environment": (
        r"\bpark\b",
        r"\bgreen\b",
        r"\bclimate\b",
        r"\bwaste\b",
        r"\brecycling\b",
        r"\bpollution\b",
        r"\benvironment\b",
        r"\bпарк\b",
        r"\bклимат",
    ),
}

_TOPIC_ORDER: tuple[str, ...] = tuple(_TOPIC_PATTERNS) + ("general",)
_TOPIC_LABELS: dict[str, str] = {
    "housing": "Housing",
    "transport": "Transport",
    "policing": "Police / courts",
    "health": "Health",
    "council": "Council",
    "weather": "Weather",
    "business": "Business",
    "culture": "Culture",
    "events": "Events",
    "food": "Food",
    "football": "Football",
    "education": "Education",
    "environment": "Environment",
    "general": "General",
}

_BASIS_LABEL_RU = {
    "rendered": "видимых",
    "included": "отобранных",
    "candidate": "собранных",
}

_EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
}

_STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "also",
    "amid",
    "and",
    "are",
    "around",
    "been",
    "before",
    "being",
    "city",
    "could",
    "day",
    "first",
    "for",
    "from",
    "greater",
    "has",
    "have",
    "into",
    "its",
    "latest",
    "live",
    "local",
    "manchester",
    "new",
    "news",
    "not",
    "now",
    "official",
    "over",
    "plans",
    "say",
    "says",
    "set",
    "the",
    "this",
    "today",
    "update",
    "updates",
    "will",
    "with",
    "you",
    "для",
    "что",
    "это",
    "как",
    "или",
    "при",
    "уже",
}

_GENERIC_ENTITY_NAMES = {
    "bbc",
    "bbc manchester",
    "itv",
    "itv granada",
    "men",
    "metrolink",
    "northern",
    "northern rail",
    "openreach",
    "prolific north",
    "stagecoach",
    "tfgm",
    "the mill",
    "ticketmaster",
    "transport for greater manchester",
}


@dataclass(slots=True)
class _PreparedCandidate:
    index: int
    candidate: dict
    fingerprint: str
    title: str
    include: bool
    rendered: bool
    source_label: str
    primary_block: str
    block_family: str
    boroughs: tuple[str, ...]
    topic_tags: tuple[str, ...]
    signal_tags: frozenset[str]
    entities: frozenset[str]
    venue_entities: frozenset[str]
    tokens: frozenset[str]


def _candidate_text(candidate: dict) -> str:
    fields = ("title", "summary", "lead", "evidence_text", "practical_angle", "source_label")
    return " ".join(str(candidate.get(field) or "") for field in fields)


def _candidate_fingerprint(candidate: dict) -> str:
    existing = str(candidate.get("fingerprint") or "").strip()
    return existing or fingerprint_for_candidate(candidate)


def _normalise_borough(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    for borough in GM_BOROUGHS:
        if lowered == borough.lower():
            return borough
    if lowered == "city of manchester":
        return "Manchester"
    return ""


def _append_unique(out: list[str], value: str) -> None:
    if value and value not in out:
        out.append(value)


def _as_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]


def _candidate_entities(candidate: dict) -> dict:
    existing = candidate.get("entities")
    if isinstance(existing, dict) and existing.get("schema_version"):
        return existing
    return extract_entities(candidate)


def _ordered_boroughs(values: list[str]) -> list[str]:
    return sorted(
        {value for value in values if value in _BOROUGH_ORDER},
        key=lambda name: _BOROUGH_ORDER[name],
    )


def candidate_boroughs(candidate: dict) -> list[str]:
    """Infer concrete GM boroughs for a candidate without calling a model."""
    out: list[str] = []

    top_level = _normalise_borough(candidate.get("borough"))
    _append_unique(out, top_level)
    for value in _as_string_list(candidate.get("boroughs")):
        _append_unique(out, _normalise_borough(value))

    entities = _candidate_entities(candidate)
    for value in _as_string_list(entities.get("boroughs")):
        _append_unique(out, _normalise_borough(value))
    for value in _as_string_list(entities.get("councils")):
        _append_unique(out, _COUNCIL_TO_BOROUGH.get(value, ""))
    for value in _as_string_list(entities.get("districts")):
        _append_unique(out, _DISTRICT_TO_BOROUGH.get(value, ""))
    for value in _as_string_list(entities.get("stations")):
        _append_unique(out, _STATION_TO_BOROUGH.get(value, ""))
    for value in _as_string_list(entities.get("venues")):
        _append_unique(out, _VENUE_TO_BOROUGH.get(value, ""))
    for value in _as_string_list(entities.get("clubs")):
        _append_unique(out, _CLUB_TO_BOROUGH.get(value, ""))

    source_label = str(candidate.get("source_label") or "").strip().lower()
    for source_name, borough in _SOURCE_TO_BOROUGH.items():
        if source_label == source_name.lower():
            _append_unique(out, borough)

    return _ordered_boroughs(out)


def _block_family(primary_block: str) -> str:
    block = str(primary_block or "").strip()
    if block == "transport":
        return "transport"
    if block == "weather":
        return "weather"
    if block == "football":
        return "football"
    if block == "tech_business":
        return "business"
    if block == "openings":
        return "food"
    if block in _EVENT_BLOCKS:
        return "events"
    return "news"


def candidate_topic_tags(candidate: dict) -> list[str]:
    text = _candidate_text(candidate)
    found: list[str] = []
    for topic, patterns in _TOPIC_PATTERNS.items():
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
            found.append(topic)

    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    family = _block_family(block)
    fallback = {
        "transport": "transport",
        "weather": "weather",
        "football": "football",
        "business": "business",
        "food": "food",
        "events": "events",
    }.get(family)
    if fallback and fallback not in found:
        found.append(fallback)
    if "venues" in category and "events" not in found:
        found.append("events")
    if "food" in category and "food" not in found:
        found.append("food")
    if not found:
        found.append("general")
    return sorted(found, key=lambda tag: _TOPIC_ORDER.index(tag) if tag in _TOPIC_ORDER else 999)[:4]


def _tokenise(candidate: dict) -> frozenset[str]:
    raw = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle")
    )
    normalised = normalize_title(raw)
    tokens = {
        token
        for token in normalised.split()
        if len(token) >= 4 and token not in _STOPWORDS and not token.isdigit()
    }
    return frozenset(tokens)


def _salient_entities(candidate: dict) -> tuple[frozenset[str], frozenset[str]]:
    entities = _candidate_entities(candidate)
    names: set[str] = set()
    venues: set[str] = set()
    for bucket in ("councils", "stations", "clubs", "companies"):
        for value in _as_string_list(entities.get(bucket)):
            lowered = value.lower()
            if lowered not in _GENERIC_ENTITY_NAMES:
                names.add(value)
    for value in _as_string_list(entities.get("venues")):
        venues.add(value)
        if value.lower() not in _GENERIC_ENTITY_NAMES:
            names.add(value)
    return frozenset(names), frozenset(venues)


def _prepare_candidate(
    index: int,
    candidate: dict,
    rendered_set: set[str],
) -> _PreparedCandidate | None:
    fingerprint = _candidate_fingerprint(candidate)
    title = str(candidate.get("title") or "").strip()
    if not fingerprint and not title:
        return None
    topic_tags = tuple(candidate_topic_tags(candidate))
    entities, venue_entities = _salient_entities(candidate)
    return _PreparedCandidate(
        index=index,
        candidate=candidate,
        fingerprint=fingerprint,
        title=title,
        include=bool(candidate.get("include")),
        rendered=bool(fingerprint and fingerprint in rendered_set),
        source_label=str(candidate.get("source_label") or "").strip(),
        primary_block=str(candidate.get("primary_block") or "").strip(),
        block_family=_block_family(str(candidate.get("primary_block") or "")),
        boroughs=tuple(candidate_boroughs(candidate)),
        topic_tags=topic_tags,
        signal_tags=frozenset(tag for tag in topic_tags if tag not in {"general", "events"}),
        entities=entities,
        venue_entities=venue_entities,
        tokens=_tokenise(candidate),
    )


def _rendered_set(rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None) -> set[str]:
    if isinstance(rendered_fingerprints, dict):
        return {str(fp) for fp in (rendered_fingerprints.get("rendered_candidate_fingerprints") or [])}
    return {str(fp) for fp in (rendered_fingerprints or [])}


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _families_compatible(a: _PreparedCandidate, b: _PreparedCandidate) -> bool:
    if a.block_family == b.block_family:
        return True
    if "news" in {a.block_family, b.block_family}:
        return True
    return bool(a.signal_tags & b.signal_tags)


def _should_link(a: _PreparedCandidate, b: _PreparedCandidate) -> bool:
    if a.fingerprint and a.fingerprint == b.fingerprint:
        return True

    title_sim = _jaccard(a.tokens, b.tokens)
    shared_signal_tags = a.signal_tags & b.signal_tags
    shared_entities = a.entities & b.entities
    shared_boroughs = set(a.boroughs) & set(b.boroughs)

    if a.block_family == "events" and b.block_family == "events":
        # Venue calendars share too much boilerplate; require strong text
        # overlap so two unrelated gigs at the same room do not merge.
        return title_sim >= 0.34 and bool(shared_signal_tags or shared_entities or shared_boroughs)

    if title_sim >= 0.45 and _families_compatible(a, b):
        return True
    if not _families_compatible(a, b):
        return False
    if shared_signal_tags and shared_entities and title_sim >= 0.14:
        return True
    if shared_signal_tags and shared_boroughs and title_sim >= 0.26:
        return True
    if shared_entities and title_sim >= 0.24:
        return True
    return len(shared_signal_tags) >= 2 and title_sim >= 0.20


def _component_roots(size: int, links: list[tuple[int, int]]) -> dict[int, list[int]]:
    parent = list(range(size))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    for a, b in links:
        union(a, b)

    components: dict[int, list[int]] = defaultdict(list)
    for idx in range(size):
        components[find(idx)].append(idx)
    return components


def _ordered_counter_values(counter: Counter[str]) -> list[dict[str, object]]:
    return [
        {"name": name, "count": count}
        for name, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _cluster_label(members: list[_PreparedCandidate], topic_tags: list[str], boroughs: list[str]) -> str:
    primary_topic = topic_tags[0] if topic_tags else "general"
    topic_label = _TOPIC_LABELS.get(primary_topic, primary_topic.title())
    entity_counts: Counter[str] = Counter()
    for member in members:
        entity_counts.update(member.entities)
    if entity_counts:
        return f"{topic_label} / {entity_counts.most_common(1)[0][0]}"
    if len(boroughs) == 1:
        return f"{topic_label} / {boroughs[0]}"
    if len(boroughs) > 1:
        return f"{topic_label} / {len(boroughs)} boroughs"
    return topic_label


def _cluster_payload(cluster_id: str, members: list[_PreparedCandidate]) -> dict[str, object]:
    topic_counter: Counter[str] = Counter()
    borough_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    block_counter: Counter[str] = Counter()
    entity_counter: Counter[str] = Counter()
    for member in members:
        topic_counter.update(member.topic_tags)
        borough_counter.update(member.boroughs)
        if member.source_label:
            source_counter.update([member.source_label])
        if member.primary_block:
            block_counter.update([member.primary_block])
        entity_counter.update(member.entities)

    topic_tags = [
        tag
        for tag, _count in sorted(
            topic_counter.items(),
            key=lambda item: (-item[1], _TOPIC_ORDER.index(item[0]) if item[0] in _TOPIC_ORDER else 999),
        )
    ][:4]
    boroughs = [
        name
        for name, _count in sorted(
            borough_counter.items(),
            key=lambda item: (_BOROUGH_ORDER.get(item[0], 999), -item[1]),
        )
    ]
    fingerprints = [member.fingerprint for member in members if member.fingerprint]
    return {
        "id": cluster_id,
        "label": _cluster_label(members, topic_tags, boroughs),
        "primary_topic": topic_tags[0] if topic_tags else "general",
        "topic_tags": topic_tags,
        "boroughs": boroughs,
        "primary_entity": entity_counter.most_common(1)[0][0] if entity_counter else "",
        "candidate_count": len(members),
        "included_count": sum(1 for member in members if member.include),
        "rendered_count": sum(1 for member in members if member.rendered),
        "sources": _ordered_counter_values(source_counter),
        "primary_blocks": _ordered_counter_values(block_counter),
        "fingerprints": fingerprints,
        "titles": [member.title for member in members if member.title][:8],
    }


def build_topic_clusters(
    candidates: list[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None = None,
    *,
    include_all: bool = False,
) -> dict[str, object]:
    rendered_set = _rendered_set(rendered_fingerprints)
    prepared_all = [
        prepared
        for idx, candidate in enumerate(candidates)
        if isinstance(candidate, dict)
        for prepared in [_prepare_candidate(idx, candidate, rendered_set)]
        if prepared is not None
    ]
    prepared = [
        item
        for item in prepared_all
        if include_all or item.include or item.rendered or str(item.candidate.get("draft_line") or "").strip()
    ]

    links: list[tuple[int, int]] = []
    for i, left in enumerate(prepared):
        for j in range(i + 1, len(prepared)):
            if _should_link(left, prepared[j]):
                links.append((i, j))

    clusters: list[list[_PreparedCandidate]] = [
        [prepared[idx] for idx in indices]
        for indices in _component_roots(len(prepared), links).values()
        if len(indices) >= 2
    ]
    clusters.sort(
        key=lambda members: (
            -sum(1 for member in members if member.rendered),
            -sum(1 for member in members if member.include),
            -len(members),
            min(member.title for member in members if member.title) if any(member.title for member in members) else "",
        )
    )
    payloads = [
        _cluster_payload(f"topic-{idx:03d}", members)
        for idx, members in enumerate(clusters, start=1)
    ]
    clustered_fingerprints = {
        fp
        for cluster in payloads
        for fp in cluster.get("fingerprints", [])
        if isinstance(fp, str) and fp
    }
    topic_counts: Counter[str] = Counter()
    for item in prepared:
        topic_counts.update(item.topic_tags)
    return {
        "schema_version": CITY_INTELLIGENCE_SCHEMA_VERSION,
        "eligible_candidate_count": len(prepared),
        "cluster_count": len(payloads),
        "clustered_candidate_count": len(clustered_fingerprints),
        "unclustered_candidate_count": max(0, len(prepared) - len(clustered_fingerprints)),
        "topic_counts": _ordered_counter_values(topic_counts),
        "clusters": payloads,
    }


def build_borough_coverage(
    candidates: list[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None = None,
) -> dict[str, object]:
    rendered_set = _rendered_set(rendered_fingerprints)
    rows: dict[str, dict[str, object]] = {
        borough: {
            "borough": borough,
            "candidate_count": 0,
            "included_count": 0,
            "rendered_count": 0,
            "source_count": 0,
            "_sources": set(),
        }
        for borough in GM_BOROUGHS
    }
    unassigned = {"candidate_count": 0, "included_count": 0, "rendered_count": 0}
    totals = {"candidate_count": 0, "included_count": 0, "rendered_count": 0}
    multi_borough_candidates = 0

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        fingerprint = _candidate_fingerprint(candidate)
        include = bool(candidate.get("include"))
        rendered = bool(fingerprint and fingerprint in rendered_set)
        source_label = str(candidate.get("source_label") or "").strip()
        boroughs = candidate_boroughs(candidate)

        totals["candidate_count"] += 1
        if include:
            totals["included_count"] += 1
        if rendered:
            totals["rendered_count"] += 1
        if len(boroughs) > 1:
            multi_borough_candidates += 1
        if not boroughs:
            unassigned["candidate_count"] += 1
            if include:
                unassigned["included_count"] += 1
            if rendered:
                unassigned["rendered_count"] += 1
            continue

        for borough in boroughs:
            row = rows[borough]
            row["candidate_count"] = int(row["candidate_count"]) + 1
            if include:
                row["included_count"] = int(row["included_count"]) + 1
            if rendered:
                row["rendered_count"] = int(row["rendered_count"]) + 1
            if source_label:
                sources = row["_sources"]
                if isinstance(sources, set):
                    sources.add(source_label)

    borough_rows: list[dict[str, object]] = []
    for borough in GM_BOROUGHS:
        row = rows[borough]
        sources = row.pop("_sources")
        row["source_count"] = len(sources) if isinstance(sources, set) else 0
        borough_rows.append(row)

    basis = "rendered"
    basis_total = sum(int(row["rendered_count"]) for row in borough_rows)
    if basis_total == 0:
        basis = "included"
        basis_total = sum(int(row["included_count"]) for row in borough_rows)
    if basis_total == 0:
        basis = "candidate"
        basis_total = sum(int(row["candidate_count"]) for row in borough_rows)

    count_field = f"{basis}_count"
    covered_boroughs = [row for row in borough_rows if int(row[count_field]) > 0]
    dominant_borough: dict[str, object] | None = None
    skew_flags: list[str] = []
    if basis_total:
        top = max(borough_rows, key=lambda row: int(row[count_field]))
        top_count = int(top[count_field])
        share = top_count / basis_total if basis_total else 0.0
        if basis_total >= 5 and share >= 0.60:
            dominant_borough = {
                "borough": top["borough"],
                "basis": basis,
                "count": top_count,
                "total": basis_total,
                "share": round(share, 3),
            }
            skew_flags.append(
                f"{top['borough']} забирает {round(share * 100)}% {_BASIS_LABEL_RU.get(basis, basis)} "
                "borough-specific пунктов "
                f"({top_count}/{basis_total})."
            )
        if basis_total >= 8 and len(covered_boroughs) < 3:
            skew_flags.append(
                f"Только {len(covered_boroughs)} GM borough(s) есть среди "
                f"{_BASIS_LABEL_RU.get(basis, basis)} borough-specific пунктов."
            )

    undercovered = [
        str(row["borough"])
        for row in borough_rows
        if int(row[count_field]) == 0
    ]
    if basis_total >= 8 and undercovered:
        preview = ", ".join(undercovered[:6])
        suffix = f" и ещё {len(undercovered) - 6}" if len(undercovered) > 6 else ""
        skew_flags.append(
            f"В {len(undercovered)} GM borough(s) ноль {_BASIS_LABEL_RU.get(basis, basis)} "
            f"пунктов: {preview}{suffix}."
        )

    return {
        "schema_version": CITY_INTELLIGENCE_SCHEMA_VERSION,
        "basis": basis,
        "counts": {
            **totals,
            "borough_specific_candidate_count": sum(int(row["candidate_count"]) for row in borough_rows),
            "borough_specific_included_count": sum(int(row["included_count"]) for row in borough_rows),
            "borough_specific_rendered_count": sum(int(row["rendered_count"]) for row in borough_rows),
            "covered_boroughs_candidate": sum(1 for row in borough_rows if int(row["candidate_count"]) > 0),
            "covered_boroughs_included": sum(1 for row in borough_rows if int(row["included_count"]) > 0),
            "covered_boroughs_rendered": sum(1 for row in borough_rows if int(row["rendered_count"]) > 0),
            "unassigned_candidate_count": unassigned["candidate_count"],
            "unassigned_included_count": unassigned["included_count"],
            "unassigned_rendered_count": unassigned["rendered_count"],
            "multi_borough_candidate_count": multi_borough_candidates,
        },
        "boroughs": borough_rows,
        "dominant_borough": dominant_borough,
        "undercovered_boroughs": undercovered,
        "skew_flags": skew_flags,
    }


def summarise_city_intelligence(
    candidates: list[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None = None,
) -> dict[str, object]:
    if not isinstance(candidates, list):
        candidates = []
    return {
        "schema_version": CITY_INTELLIGENCE_SCHEMA_VERSION,
        "topic_clusters": build_topic_clusters(candidates, rendered_fingerprints),
        "borough_coverage": build_borough_coverage(candidates, rendered_fingerprints),
    }


def annotate_city_intelligence(
    candidates: list[dict],
    rendered_fingerprints: set[str] | list[str] | tuple[str, ...] | dict | None = None,
) -> dict[str, object]:
    if not isinstance(candidates, list):
        candidates = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate["boroughs"] = candidate_boroughs(candidate)
        candidate["topic_tags"] = candidate_topic_tags(candidate)
        candidate["topic_cluster_id"] = ""
        candidate["topic_cluster_label"] = ""

    summary = summarise_city_intelligence(candidates, rendered_fingerprints)
    cluster_by_fingerprint: dict[str, tuple[str, str]] = {}
    for cluster in (summary.get("topic_clusters") or {}).get("clusters") or []:
        if not isinstance(cluster, dict):
            continue
        cluster_id = str(cluster.get("id") or "")
        label = str(cluster.get("label") or "")
        for fingerprint in cluster.get("fingerprints") or []:
            if isinstance(fingerprint, str) and fingerprint:
                cluster_by_fingerprint[fingerprint] = (cluster_id, label)

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        cluster_id, label = cluster_by_fingerprint.get(_candidate_fingerprint(candidate), ("", ""))
        candidate["topic_cluster_id"] = cluster_id
        candidate["topic_cluster_label"] = label
    return summary
