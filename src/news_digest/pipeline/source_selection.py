"""I4 — Best source selection.

When the deduplication passes find a cluster of candidates covering
the same story, pick the source most authoritative for that topic
rather than the one that happened to be listed first.

Authority is per-category:
  * transport       → TfGM / National Rail before any media
  * council         → the actual council before BBC / MEN
  * football        → the club's official site before media
  * culture / venues / tickets → venue site before aggregator
  * general media   → BBC > MEN > The Mill > regional aggregator

Used by:
  * ``dedupe.py``           — deterministic Jaccard / shared-entity pass
  * ``semantic_dedupe.py``  — embeddings high-similarity pairs

Both call ``source_score(label, category)`` so the ranking lives in
one place. ``source_rank(label, category)`` is the backward-compatible
inverse used in older call sites where lower = better.

Design rules (Q1 reviewer notes):
  * Lists are hand-curated against ``data/sources.toml`` ``name`` values.
  * No regex / fuzzy matching — labels must match exactly so a future
    rename forces a deliberate update here.
  * Tier is universal (cross-category) and capped at 4. Don't introduce
    finer-grained tiers without a tested reason.
  * Unknown sources fall back to tier 3 (aggregator) — never to tier 1.
    A new source is "unknown" until it's added to this map explicitly.
"""
from __future__ import annotations


SOURCE_AUTHORITY_VERSION = 1


# Per-category preference lists — first = best.
# When a candidate's category matches one of these keys AND its source
# label appears in the list, it gets a position-based bonus. Tier still
# applies on top, so an unknown source in the right category still
# beats a wrong-category source from a high tier.
CATEGORY_AUTHORITY: dict[str, tuple[str, ...]] = {
    "transport": (
        "TfGM",
        "National Rail",
    ),
    "gmp": (
        # GMP RSS is disabled (Cloudflare blocks the runner — see
        # project_runner_waf memory). BBC Manchester fallback is the
        # active source for public-safety stories.
        "BBC Manchester public safety fallback",
        "BBC Manchester",
        "BBC Manchester Web",
        "ITV Granada Greater Manchester",
        "MEN",
        "MEN Latest News",
    ),
    "council": (
        # Borough-specific stories should prefer the borough's own
        # council site over media. Within the list, the GMCA appears
        # after Manchester Council because city-region authority
        # decisions usually come labelled "Manchester Council"
        # before they surface on GMCA. Order matters.
        "Manchester Council",
        "Salford Council",
        "Trafford Council",
        "Stockport Council",
        "Oldham Council",
        "Rochdale Council",
        "Bolton Council",
        "Bury Council",
        "Wigan Council",
        "Tameside Council",
        "GMCA",
        "BBC Manchester",
        "BBC Manchester Web",
        "The Mill",
        "MEN",
        "MEN Latest News",
        "ITV Granada Greater Manchester",
        "Place North West",
    ),
    "football": (
        "Manchester United",
        "Manchester City",
        "BBC Manchester",
        "BBC Manchester Web",
        "MEN",
        "MEN Latest News",
    ),
    "culture_weekly": (
        # Venue-direct sources first — they own the listing.
        "HOME",
        "The Lowry",
        "Factory International",
        "Bridgewater Hall",
        "Albert Hall Manchester",
        "Band on the Wall",
        "RNCM",
        "Whitworth",
        "People's History Museum",
        "Contact Theatre",
        "John Rylands Library",
        "Palace Theatre",
        "Manchester Flower Festival",
        "Manchester Flower Festival CityCo",
        "Manchester Flower Festival CityCo News",
        # Discovery layer (multi-venue, broad reach).
        "Visit Manchester",
        "Visit Manchester This Week",
        "Visit Manchester Weekend",
        "Visit Manchester Bank Holiday Guide",
        # Aggregators last.
        "I Love Manchester Flower Festival",
        "Manchester's Finest Flower Festival",
        "The Manc Weekly Things To Do",
        "The Manc",
        "Secret Manchester May Guide",
        "Secret Manchester Weekend Guide",
        "Secret Manchester Gigs",
        "Manchester Theatres Weekend",
        "Manchester Theatres Next Weekend",
        "Manchester Wire",
        "Creative Tourist Manchester",
        "Creative Tourist Bank Holiday",
        "DesignMyNight Manchester",
        "DesignMyNight Bank Holiday",
        "Manchester's Finest Events",
        "Time Out Manchester",
        "Eventbrite Manchester",
        "Skiddle Manchester",
        "Skiddle Manchester Bank Holiday",
        "Sofar Manchester Bank Holiday",
        "Eventbrite Manchester Markets",
        "Pedddle Markets",
        "Pedddle Makers Market",
    ),
    "venues_tickets": (
        "Co-op Live",
        "AO Arena",
        "Bridgewater Hall",
        "Albert Hall Manchester",
        "Band on the Wall",
        "RNCM",
        # API-based ticket sources (structured, cross-venue).
        "Ticketmaster Manchester Onsale",
        "Ticketmaster Manchester Upcoming",
        "Ticketmaster Liverpool Onsale",
        "Ticketmaster Liverpool Upcoming",
        "Ticketmaster London Major Onsale",
        "Ticketmaster London Major Upcoming",
        "Ticketmaster UK Major Onsale",
        "Ticketmaster UK Major Upcoming",
    ),
    "russian_speaking_events": (
        "Manchester Academy Diaspora",
        "Kontramarka UK",
        "EventFirst Diaspora",
        "UK Stand-Up Club",
        "UK Stand-Up Club Eventbrite",
    ),
    "food_openings": (
        "Manchester's Finest",
        "About Manchester Food & Drink",
        "The Manc Eats",
    ),
    "public_services": (
        "GMMH",
        "GMCA",
        "BBC Manchester",
        "BBC Manchester Web",
        "MEN",
        "MEN Latest News",
    ),
    "tech_business": (
        "MIDAS Manchester",
        "Prolific North",
        "BusinessCloud",
        "Manchester Digital",
        "Bdaily Manchester",
        "University of Manchester",
        "University of Salford",
        "Manchester Metropolitan",
    ),
    "media_layer": (
        # General-news ladder (no category-specific preference).
        "BBC Manchester",
        "BBC Manchester Web",
        "MEN",
        "MEN Latest News",
        "The Mill",
        "ITV Granada Greater Manchester",
        "Place North West",
        "About Manchester News",
        "Altrincham Today",
        "The Manc",
        "I Love Manchester",
        "Secret Manchester",
    ),
}


# Universal tier (cross-category). Lower = more authoritative.
# Default for unknown labels is tier 3 (aggregator) — never tier 1.
SOURCE_TIER: dict[str, int] = {
    # ── Tier 1: official primary source (owns its topic) ────────────────
    "TfGM": 1,
    "National Rail": 1,
    "Manchester United": 1,
    "Manchester City": 1,
    "Manchester Council": 1,
    "GMCA": 1,
    "Salford Council": 1,
    "Trafford Council": 1,
    "Stockport Council": 1,
    "Oldham Council": 1,
    "Rochdale Council": 1,
    "Bolton Council": 1,
    "Bury Council": 1,
    "Wigan Council": 1,
    "Tameside Council": 1,
    "HOME": 1,
    "The Lowry": 1,
    "Factory International": 1,
    "Bridgewater Hall": 1,
    "Albert Hall Manchester": 1,
    "Band on the Wall": 1,
    "RNCM": 1,
    "Whitworth": 1,
    "People's History Museum": 1,
    "Contact Theatre": 1,
    "John Rylands Library": 1,
    "Palace Theatre": 1,
    "Co-op Live": 1,
    "AO Arena": 1,
    "University of Manchester": 1,
    "University of Salford": 1,
    "Manchester Metropolitan": 1,
    "GMMH": 1,
    "Manchester Flower Festival": 1,
    "Manchester Flower Festival CityCo": 1,
    "Manchester Flower Festival CityCo News": 1,

    # ── Tier 2: regional / national authoritative media ─────────────────
    "BBC Manchester": 2,
    "BBC Manchester Web": 2,
    "BBC Manchester public safety fallback": 2,
    "ITV Granada Greater Manchester": 2,
    "MEN": 2,
    "MEN Latest News": 2,
    "The Mill": 2,
    "Place North West": 2,
    "About Manchester News": 2,
    "MIDAS Manchester": 2,
    "Prolific North": 2,
    "BusinessCloud": 2,
    "Bdaily Manchester": 2,
    "Manchester Digital": 2,
    # Ticket APIs — high signal but secondary to venue listings.
    "Ticketmaster Manchester Onsale": 2,
    "Ticketmaster Manchester Upcoming": 2,
    "Ticketmaster Liverpool Onsale": 2,
    "Ticketmaster Liverpool Upcoming": 2,
    "Ticketmaster London Major Onsale": 2,
    "Ticketmaster London Major Upcoming": 2,
    "Ticketmaster UK Major Onsale": 2,
    "Ticketmaster UK Major Upcoming": 2,
    "Visit Manchester": 2,
    "Visit Manchester This Week": 2,
    "Visit Manchester Weekend": 2,
    "Visit Manchester Bank Holiday Guide": 2,

    # ── Tier 3: aggregator / lifestyle / round-up ───────────────────────
    "The Manc": 3,
    "The Manc Weekly Things To Do": 3,
    "Manchester's Finest": 3,
    "Manchester's Finest Events": 3,
    "Manchester's Finest Flower Festival": 3,
    "I Love Manchester": 3,
    "I Love Manchester Flower Festival": 3,
    "Secret Manchester May Guide": 3,
    "Secret Manchester Weekend Guide": 3,
    "Secret Manchester Gigs": 3,
    "Manchester Theatres Weekend": 3,
    "Manchester Theatres Next Weekend": 3,
    "Creative Tourist Bank Holiday": 3,
    "Secret Manchester": 3,
    "Altrincham Today": 3,
    "About Manchester Food & Drink": 3,
    "The Manc Eats": 3,
    "Confidentials": 3,

    # ── Tier 4: weak / discovery-only / signal ──────────────────────────
    "DesignMyNight Manchester": 4,
    "DesignMyNight Bank Holiday": 4,
    "Manchester Wire": 4,
    "Creative Tourist Manchester": 4,
    "Time Out Manchester": 4,
    "Eventbrite Manchester": 4,
    "Skiddle Manchester": 4,
    "Skiddle Manchester Bank Holiday": 4,
    "Sofar Manchester Bank Holiday": 4,
    "Eventbrite Manchester Markets": 4,
    "Pedddle Markets": 4,
    "Pedddle Makers Market": 4,
    # Diaspora promoters fall in tier 4 only because their schedules
    # aren't a "primary" registry of an own venue — they're event-
    # specific landing pages. Still preferred for the right category.
    "Manchester Academy Diaspora": 4,
    "Kontramarka UK": 4,
    "EventFirst Diaspora": 4,
    "UK Stand-Up Club": 4,
    "UK Stand-Up Club Eventbrite": 4,
}


_DEFAULT_TIER = 3


def source_tier(source_label: str) -> int:
    """Universal tier for a source label. Unknown → tier 3."""
    return SOURCE_TIER.get(str(source_label or ""), _DEFAULT_TIER)


def source_score(source_label: str, category: str = "") -> int:
    """Combined authority score. Higher = better.

    Composition:
        category_bonus = max(0, 100 - position * 10) if source is in
            CATEGORY_AUTHORITY[category], else 0. So first place in
            the category list is worth 100, second 90, ..., tenth 10,
            eleventh+ → 0 (still listed for documentation; the tier
            bonus still kicks in).
        tier_bonus = max(0, (5 - tier) * 5). Tier 1 → 20, tier 2 → 10,
            tier 3 → 10, tier 4 → 5, default → 10.

    Examples:
        TfGM, category="transport"        → 100 + 20 = 120
        BBC Manchester, category="transport" → 0 + 10 = 10
        TfGM, category="culture_weekly"   → 0 + 20 = 20
        The Manc, category="culture_weekly" → still bonus from list

    A higher score means we should keep this candidate when collapsing
    a same-story cluster.
    """
    label = str(source_label or "")
    cat = str(category or "")

    cat_bonus = 0
    if cat:
        hierarchy = CATEGORY_AUTHORITY.get(cat, ())
        for index, name in enumerate(hierarchy):
            if name == label:
                cat_bonus = max(0, 100 - index * 10)
                break

    tier = source_tier(label)
    tier_bonus = max(0, (5 - tier) * 5)
    return cat_bonus + tier_bonus


def source_rank(source_label: str, category: str = "") -> int:
    """Inverse of ``source_score`` — lower = better.

    Preserved for call sites that already compare with ``rank <= rank``
    (semantic_dedupe._source_rank, dedupe._source_rank). Unknowns stay
    at the historical sentinel value of 99 so legacy comparisons keep
    treating them as the worst option.
    """
    score = source_score(source_label, category)
    if score == 0:
        return 99
    # Linear inverse so the relative ordering matches what call sites
    # expect (lower number wins). The exact constant doesn't matter as
    # long as it's monotonic.
    return max(0, 200 - score)


def pick_winner(candidates: list[dict]) -> dict | None:
    """Return the best candidate of a cluster covering the same story.

    Selection:
      1. Highest ``source_score(source_label, category)``.
      2. Tie-break: longest ``evidence_text`` (more substance is more
         useful when the writer / LLM rewrite reads only the winner).
      3. Stable: earlier-listed wins remaining ties.

    Returns None for an empty / all-invalid cluster.
    """
    valid = [c for c in candidates if isinstance(c, dict)]
    if not valid:
        return None
    if len(valid) == 1:
        return valid[0]

    def sort_key(c: dict) -> tuple[int, int]:
        score = source_score(
            str(c.get("source_label") or ""),
            str(c.get("category") or ""),
        )
        evidence_len = len(str(c.get("evidence_text") or ""))
        # Negative for descending order via standard sort.
        return (-score, -evidence_len)

    return sorted(valid, key=sort_key)[0]
