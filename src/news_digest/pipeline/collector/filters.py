"""Filtering predicates that decide whether a candidate link survives.

`_is_allowed_source_link` is the per-source URL/title gate. `_has_gm_token`
checks Greater Manchester locality. Per-block policies (listicle, football
fluff, city_watch topical) live here too because they all share the
filter style: input == candidate text, output == bool.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib import parse
import re

from news_digest.pipeline.common import now_london

from .dates import _parse_datetime_value
from .sources import SourceDef


GM_TOKENS: tuple[str, ...] = (
    "manchester",
    "salford",
    "stockport",
    "trafford",
    "tameside",
    "rochdale",
    "oldham",
    "wigan",
    "bolton",
    "bury",
    "altrincham",
    "ashton-under-lyne",
    "ashton under lyne",
    "prestwich",
    "eccles",
    "swinton",
    "didsbury",
    "chorlton",
    "rusholme",
    "fallowfield",
    "moss side",
    "harpurhey",
    "wythenshawe",
    "old trafford",
    "etihad",
    "metrolink",
    "bee network",
    "tfgm",
    "gmp",
    "greater manchester",
    "mancunian",
)


def _has_gm_token(*haystacks: str) -> bool:
    """Return True if any haystack contains a Greater Manchester token.

    Uses word-boundary matching so 'bury' won't match 'westbury' and short
    tokens won't fire on unrelated substrings.
    """

    blob = " ".join(haystacks).lower()
    for token in GM_TOKENS:
        if re.search(rf"\b{re.escape(token.strip())}\b", blob):
            return True
    return False


# Anchor texts that look like sidebar/navigation chrome rather than article
# headlines. Hits inside titles for clubs/news sites that surface their own
# section labels as link text.
_NAV_CHROME_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(rf"\b{re.escape(token)}\b", re.IGNORECASE)
    for token in (
        "Women's Team",
        "Men's Team",
        "EDS & Academy",
        "Academy",
        "Club Shop",
        "Membership",
        "Fixtures",
    )
)


def _is_navigation_chrome(title: str) -> bool:
    """Catch anchor text that is essentially a navigation label.

    A title is treated as chrome when stripping all chrome leaves under
    25 characters of actual content.
    """

    if not title:
        return True
    stripped = title
    for pattern in _NAV_CHROME_PATTERNS:
        stripped = pattern.sub(" ", stripped)
    stripped_compact = re.sub(r"\s+", " ", stripped).strip()
    return len(stripped_compact) < 25


def _looks_like_candidate_title(title: str) -> bool:
    if len(title) < 18 or len(title) > 160:
        return False
    lowered = title.lower()
    blocked = {
        "privacy",
        "cookie",
        "terms",
        "subscribe",
        "newsletter",
        "accessibility",
        "facebook",
        "instagram",
        "twitter",
        "linkedin",
        "sign in",
        "skip to",
        "menu",
        "search",
        "family & education",
        "manchester city centre",
        "rape, sexual assault",
        "view all",
        "read more",
        "what's on",
    }
    return not any(word in lowered for word in blocked)


_LISTICLE_OPENINGS_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bbest\s+\w+\b",
        r"\btop\s+\d+",
        r"\bnew\s+restaurants?\s+and\s+bars?\s+opening\b",
        r"\bplaces?\s+to\s+(eat|drink)",
        r"\broundup\b",
        r"\bsunday\s+roasts?\b",
        r"\bbreakfast\s+(?:and|&)\s+brunch\b",
        r"\blunch\s+deals\b",
    )
)


_FOOD_OPENING_TERMS: tuple[str, ...] = (
    "open",
    "opening",
    "opens",
    "opened",
    "launch",
    "launches",
    "launched",
    "arrives",
    "lands",
    "coming to",
    "coming soon",
    "first look",
    "look inside",
    "now open",
    "officially open",
    "debut",
    "debuts",
    "unveil",
    "unveils",
    "reopen",
    "reopens",
    "closed",
    "closes",
    "quietly closes",
    "shuts",
    "new restaurant",
    "new bar",
    "new cafe",
    "new café",
    "new pub",
    "new venue",
    "new menu",
    "new spot",
    "new dining",
    "new food",
)


_FOOD_LOCAL_PLACE_TERMS: tuple[str, ...] = (
    "bar",
    "boozer",
    "brewery",
    "cafe",
    "café",
    "cocktail",
    "dining",
    "food hall",
    "pub",
    "restaurant",
    "taproom",
)


_FOOD_EVERGREEN_TOKENS: tuple[str, ...] = (
    "advertising",
    "all the bars",
    "competition terms",
    "competitions on",
    "deals & offers",
    "discount dining",
    "list your business",
    "make a contribution",
    "restaurant deals",
    "shipping and refunds",
    "where to eat for less",
    "where to get",
)


_FOOD_NON_GM_TOKENS: tuple[str, ...] = (
    "cheshire",
    "glossop",
    "liverpool",
    "london",
    "sheffield",
    "wilmslow",
)


def _is_listicle_opening(title: str) -> bool:
    lowered = str(title or "").lower()
    return any(pattern.search(lowered) for pattern in _LISTICLE_OPENINGS_PATTERNS)


def _has_food_opening_signal(title: str) -> bool:
    lowered = str(title or "").lower()
    return any(term in lowered for term in _FOOD_OPENING_TERMS)


def _has_food_place_signal(title: str) -> bool:
    lowered = str(title or "").lower()
    return any(re.search(rf"\b{re.escape(term)}\b", lowered) for term in _FOOD_LOCAL_PLACE_TERMS)


def _is_food_evergreen_or_admin(title: str, path: str = "") -> bool:
    lowered = f"{title} {path}".lower()
    return _is_listicle_opening(title) or any(token in lowered for token in _FOOD_EVERGREEN_TOKENS)


def _is_obviously_non_gm_food_item(title: str, path: str, summary: str = "") -> bool:
    text = f"{title} {path} {summary}".lower()
    if not any(re.search(rf"\b{re.escape(token)}\b", text) for token in _FOOD_NON_GM_TOKENS):
        return False
    # Keep inbound stories like "Edinburgh Street Food expands to
    # Manchester", but reject outbound/fringe items such as "opens second
    # site in Wilmslow" even if the brand originated in a GM district.
    return not re.search(
        r"\b(?:to|in|into|on|at)\s+(?:greater\s+manchester|manchester|"
        r"altrincham|bolton|bury|chorlton|deansgate|oldham|rochdale|"
        r"salford|spinningfields|stockport|tameside|trafford|wigan)\b",
        text,
    )


_WEEKEND_EVERGREEN_TOKENS: tuple[str, ...] = (
    "best bars",
    "best restaurants",
    "best things to do",
    "birthday ideas",
    "city guide",
    "destination guide",
    "guide to",
    "places to",
    "pretty bars",
    "things to do in manchester",
    "where to",
    # additional evergreen patterns
    "explore the",
    "discover the",
    "must-visit",
    "must-see",
    "what to see",
    "what to do",
    "cultural attractions",
    "tourist attraction",
    "free things",
    "how to spend",
    "weekend guide",
    "visitor guide",
)


_WEEKEND_NON_GM_TOKENS: tuple[str, ...] = (
    "cumbria",
    "edinburgh",
    "liverpool",
    "london",
    "windermere",
    "yorkshire",
)


def _is_evergreen_weekend_title(title: str) -> bool:
    lowered = str(title or "").lower()
    return _is_listicle_opening(lowered) or any(token in lowered for token in _WEEKEND_EVERGREEN_TOKENS)


def _is_obviously_non_gm_weekend_item(title: str, path: str, summary: str = "") -> bool:
    text = f"{title} {path} {summary}".lower()
    return any(re.search(rf"\b{re.escape(token)}\b", text) for token in _WEEKEND_NON_GM_TOKENS)


def _has_weekend_date_signal(title: str, path: str) -> bool:
    text = f"{title} {path}".lower()
    if re.search(r"\b(?:today|tonight|tomorrow|weekend|this week|next week)\b", text):
        return True
    if re.search(r"\b(?:mon|tue|wed|thu|fri|sat|sun),?\s+\d{1,2}\s+[a-z]{3,9}\b", text):
        return True
    if re.search(r"\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b", text):
        return True
    if re.search(r"\b20\d{2}[/-]\d{1,2}[/-]\d{1,2}\b", text):
        return True
    return False


_FOOTBALL_FLUFF_TOKENS: tuple[str, ...] = (
    "tv listings",
    "tv information",
    "academy",
    "u18",
    "u21",
    "pl2",
    "ticket information",
    "ticketing information",
    "club shop",
    "membership",
    "highlights",
    "merchandise",
    # Press conferences, podcasts, behind-the-scenes — not news
    "press conference",
    "pre-match press",
    "post-match press",
    "podcast",
    "matchday programme",
    "programme cover",
    "programme promo",
    "training session",
    "in pictures",
    "photo gallery",
    "behind the scenes",
    "afta studios",
    "wembley final",  # women's WSL — separate competition
    "wsl",
    "women's super league",
    "citc",  # City in the Community — charity, not first team
    # PR / commercial announcements
    "donat",  # donation, donate, donates
    "award",
    "shortlist",
    "partnership",
    "sponsor",
    "kit launch",
    "shirt launch",
    "community matchday",
    "community day",
    "in frame for",
    "under-18",
    "under 18",
    "u-18",
)


def _is_football_fluff(title: str, url: str = "") -> bool:
    # Normalize URL slugs (hyphens → spaces) so tokens like "press conference"
    # also match "press-conference" in article URL paths.
    normalized = f"{title} {url.replace('-', ' ')}".lower()
    if " on tv" in normalized or "tv listings" in normalized:
        return True
    return any(token in normalized for token in _FOOTBALL_FLUFF_TOKENS)


def _is_football_publishable(title: str, url: str = "") -> bool:
    normalized = f"{title} {url.replace('-', ' ')}".lower()
    if re.search(r"\b\d+\s*[-–]\s*\d+\b", normalized):
        return True
    transfer_terms = (
        "sign", "signed", "signs", "joins", "joined", "leaves", "left",
        "transfer", "fee", "deal agreed", "contract", "loan",
    )
    if any(term in normalized for term in transfer_terms) and (
        "£" in normalized or "from " in normalized or "to " in normalized
    ):
        return True
    preview_terms = ("match preview", "preview", "team news", "confirmed line up", "confirmed lineup")
    if any(term in normalized for term in preview_terms):
        return True
    fitness_terms = (
        "doubtful", "injury", "injured", "fit for", "ruled out", "returns from",
        "suspended", "suspension", "squad named", "starting lineup", "starting eleven",
        "fitness doubt", "fitness test", "available for", "misses out", "set to miss",
    )
    if any(term in normalized for term in fitness_terms):
        return True
    return False


_CITY_WATCH_TOPICAL_KEYWORDS: tuple[str, ...] = (
    # public order / safety
    "police", "gmp", "stab", "knife", "charge", "arrest", "court", "sentence",
    "murder", "assault", "robbery", "drug", "raid", "gang",
    # disruption / safety alert
    "fire", "blaze", "smoke", "warning", "evacuat", "cordon", "lockdown",
    "flood", "weather warning",
    # public affairs
    "election", "polls", "ballot", "council", "mayor", "campaign", "manifesto",
    "by-election", "vote",
    # transport / infra
    "metrolink", "bee network", "tram", "bus service", "rail", "train",
    "airport", "tfgm", "roadworks", "closure",
    # public services
    "nhs", "strike", "industrial action", "walkout", "hospital", "ambulance",
    "school", "ofsted",
    # housing / civic
    "housing", "evict", "homeless", "rent rise",
)


def _looks_like_city_watch_topical(text: str) -> bool:
    """Return True if a stale/undated city item carries a topical keyword.

    Used to keep Городской радар from filling up with soft entertainment
    or feel-good background ('Dance partners reunite', 'Marathon runner
    aged 88') when more topical material is available.
    """

    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in _CITY_WATCH_TOPICAL_KEYWORDS)


_PUBLIC_SERVICE_MAX_AGE_DAYS = 7
_TRANSPORT_MAX_AGE_DAYS = 1  # transport disruptions older than 1 day are likely resolved


def _is_stale_transport(published_at: str | None, title: str) -> bool:
    """Return True if a transport disruption item is past its relevance window.

    Transport items without a publication date are dropped (stale-safe).
    Items published more than 1 day ago are treated as stale — a disruption
    that was published 2+ days ago has either ended or is well-known already.

    Exception: items that contain future-date signals (e.g. "from Monday",
    "starting", "planned") are kept regardless of publication age.
    """
    lowered = str(title or "").lower()
    future_signals = ("from monday", "from tuesday", "from wednesday", "from thursday",
                      "from friday", "starting", "planned works", "engineering works",
                      "advance notice", "next week")
    if any(sig in lowered for sig in future_signals):
        return False
    if not published_at:
        return True
    parsed = _parse_datetime_value(published_at)
    if parsed is None:
        return True
    age = now_london() - parsed
    return age.days > _TRANSPORT_MAX_AGE_DAYS


def _is_stale_public_service(published_at: str | None, title: str) -> bool:
    """Return True if a public-services item is older than the max age.

    GMMH and similar sources publish soft PR stories (awards, surveys,
    new-term launches) that are not time-sensitive.  Items older than 7
    days clutter today_focus and should be filtered or demoted.  Items
    without a publication date are treated as stale to be safe — undated
    NHS press releases should not auto-land in today_focus.
    """

    if not published_at:
        return True
    parsed = _parse_datetime_value(published_at)
    if parsed is None:
        return True
    age = now_london() - parsed
    return age.days > _PUBLIC_SERVICE_MAX_AGE_DAYS


@dataclass(frozen=True, slots=True)
class _SourcePolicy:
    path_must_contain: tuple[str, ...] = ()      # path must contain ANY of these
    path_all_must_contain: tuple[str, ...] = ()  # path must contain ALL of these
    path_must_start: str = ""
    path_banned_segments: tuple[str, ...] = ()   # ANY present in path → drop
    path_banned_starts: tuple[str, ...] = ()     # path starts with ANY → drop
    min_path_depth: int = 0
    exact_path_depth: int = 0                    # 0 = not enforced
    min_title_len: int = 0
    blocked_title_tokens: tuple[str, ...] = ()   # ANY in lowered title → drop
    require_gm_token: bool = False
    require_food_opening_or_place: bool = False
    require_date_signal: bool = False
    require_gm_or_date: bool = False             # GM token OR date signal
    require_event_path_or_date: bool = False     # /events path OR date signal
    block_evergreen: bool = False
    block_food_evergreen: bool = False
    block_non_gm_food: bool = False
    block_non_gm_weekend: bool = False
    block_listicle: bool = False


_COUNCIL_NAV_SLUGS: tuple[str, ...] = (
    "translate-this-page", "emergency-contacts", "social-media-and-email-updates",
    "freedom-of-information", "modern-slavery-statement", "births-marriages-and-deaths",
    "business-and-licensing", "environmental-health", "accessibility-statement",
    "cookie-policy", "privacy-notice", "site-map", "search",
)

_SOURCE_POLICIES: dict[str, _SourcePolicy] = {
    # ── GMP ──────────────────────────────────────────────────────────────────
    "GMP": _SourcePolicy(path_must_contain=("/news/greater-manchester/news/news/",)),
    # ── Media layer ───────────────────────────────────────────────────────────
    "MEN": _SourcePolicy(
        path_must_contain=("/news/", "/whats-on/"),
        path_banned_segments=("/all-about/", "/topic/", "/author/", "/newsletter/"),
    ),
    "The Mill": _SourcePolicy(
        path_banned_segments=("/tag/", "/author/", "/page/"),
        min_title_len=18,
    ),
    "The Manc": _SourcePolicy(
        path_must_contain=("/news", "/2026/", "/whats-on", "/events"),
        require_gm_token=True,
        blocked_title_tokens=("best things to do",),
    ),
    "I Love Manchester": _SourcePolicy(
        path_banned_segments=("/category/", "/event-category/", "/tag/", "/author/", "/page/"),
        exact_path_depth=1,
        require_gm_token=True,
    ),
    "Secret Manchester": _SourcePolicy(
        path_banned_segments=("/category/", "/tag/", "/author/", "/page/"),
        exact_path_depth=1,
        require_gm_token=True,
    ),
    # ── Councils ──────────────────────────────────────────────────────────────
    "Manchester Council": _SourcePolicy(path_must_start="/news-stories/20", min_path_depth=4),
    "Salford Council": _SourcePolicy(path_must_start="/news/", min_path_depth=3),
    "Trafford Council": _SourcePolicy(path_must_start="/news/20", min_path_depth=4),
    "Stockport Council": _SourcePolicy(
        path_must_start="/news/",
        path_banned_segments=("/newsroom",),
        min_path_depth=2,
    ),
    "Oldham Council": _SourcePolicy(path_must_start="/news/article/", min_path_depth=4),
    "Rochdale Council": _SourcePolicy(path_must_start="/news/article/", min_path_depth=4),
    "Bolton Council": _SourcePolicy(path_must_start="/news/article/", min_path_depth=4),
    "Tameside Council": _SourcePolicy(path_must_start="/newsroom/articles/", min_path_depth=4),
    "Bury Council": _SourcePolicy(path_all_must_contain=("/pressreleases/", "/bury-council/")),
    "Wigan Council": _SourcePolicy(path_must_start="/news/articles/20", min_path_depth=5),
    "GMMH": _SourcePolicy(path_must_contain=("/news", "/2026/", "/whats-on", "/events")),
    # ── Transport ─────────────────────────────────────────────────────────────
    "TfGM": _SourcePolicy(path_must_contain=("/travel-updates", "/planned-works")),
    # National Rail: override below (GM token OR named station)
    # ── Culture ───────────────────────────────────────────────────────────────
    "HOME": _SourcePolicy(path_must_contain=("/whats-on", "/events")),
    "Whitworth": _SourcePolicy(path_must_contain=("/whats-on", "/events")),
    "The Lowry": _SourcePolicy(path_must_contain=("/whats-on", "/events")),
    "Factory International": _SourcePolicy(path_must_contain=("/whats-on/",), min_path_depth=3),
    "Palace Theatre": _SourcePolicy(
        path_must_start="/shows/",
        path_all_must_contain=("/palace-theatre-manchester",),
    ),
    # ── Venues ────────────────────────────────────────────────────────────────
    "Co-op Live": _SourcePolicy(path_must_contain=("/events/",), min_path_depth=3),
    "AO Arena": _SourcePolicy(path_must_contain=("/events",), min_path_depth=3),
    # ── Football ──────────────────────────────────────────────────────────────
    "Manchester United": _SourcePolicy(
        path_must_contain=("/en/news/",),
        blocked_title_tokens=("women", "academy", "ticket information", "ticketing", "community"),
    ),
    "Manchester City": _SourcePolicy(
        path_must_contain=("/news/",),
        blocked_title_tokens=("eds", "academy", "women", "ticket information", "ticketing", "pl2", "u18"),
    ),
    # ── Food / Openings ───────────────────────────────────────────────────────
    # Manchester's Finest: override below (food_opening alone is sufficient)
    "Confidentials": _SourcePolicy(
        path_must_start="/manchester/",
        path_banned_segments=("/p2", "/p3", "/p4", "/p5", "/p6", "/p7", "/p8", "/p9", "/p10", "/page/"),
        block_food_evergreen=True,
        block_non_gm_food=True,
        require_gm_token=True,
    ),
    "About Manchester Food & Drink": _SourcePolicy(
        block_food_evergreen=True,
        block_non_gm_food=True,
        require_gm_token=True,
        require_food_opening_or_place=True,
    ),
    "The Manc Eats": _SourcePolicy(
        path_must_contain=("/eats/", "/food-and-drink/", "/manchester/"),
        block_food_evergreen=True,
        block_non_gm_food=True,
        require_gm_token=True,
        require_food_opening_or_place=True,
    ),
    # ── Tech / Business ───────────────────────────────────────────────────────
    # Manchester Digital: override below (GM token OR tech terms)
    "Prolific North": _SourcePolicy(path_must_contain=("/news/",), require_gm_token=True),
    "BusinessCloud": _SourcePolicy(
        path_banned_segments=("/tag/", "/category/", "/author/", "/page/"),
        min_path_depth=2,
        min_title_len=25,
        require_gm_token=True,
    ),
    "Bdaily Manchester": _SourcePolicy(
        path_must_contain=("/articles/",),
        path_banned_segments=("/tag/", "/category/", "/author/", "/page/", "/region/"),
        min_title_len=25,
        require_gm_token=True,
    ),
    "MIDAS Manchester": _SourcePolicy(
        path_must_contain=("/news/",),
        path_banned_segments=(
            "get-started", "sectors", "why-manchester", "about", "contact",
            "advanced-materials", "digital-cyber", "financial-professional",
            "life-sciences", "energy", "sport",
        ),
        min_path_depth=3,
    ),
    # ── Weekend ───────────────────────────────────────────────────────────────
    "Manchester Wire": _SourcePolicy(
        exact_path_depth=1,
        path_banned_segments=("/guide/", "/what/", "/where/", "/tag/", "/author/", "/page/"),
        block_evergreen=True,
        require_gm_or_date=True,
    ),
    "Creative Tourist Manchester": _SourcePolicy(
        min_path_depth=2,
        path_banned_starts=("/venue/", "/place/", "/articles/", "/manchester/", "/locations/"),
        path_banned_segments=("/food-and-drink-guides", "/neighbourhoods/", "/day-trips/"),
        block_evergreen=True,
        require_gm_or_date=True,
    ),
    "DesignMyNight Manchester": _SourcePolicy(
        min_path_depth=4,
        path_must_start="/manchester/whats-on/",
        path_banned_segments=("/things-to-do", "/best-", "/guide-", "/clubs", "/bars", "/restaurants"),
        block_evergreen=True,
    ),
    "Fairfield Social Club": _SourcePolicy(
        min_path_depth=2,
        path_banned_segments=("/info", "/accessibility", "/book-a-table", "/private", "/weddings", "/corporate", "/sign-up"),
        require_event_path_or_date=True,
    ),
    "Skiddle Manchester": _SourcePolicy(path_must_contain=("/whats-on/",), min_path_depth=4),
    "Eventbrite Manchester": _SourcePolicy(
        path_all_must_contain=("/e/", "tickets-"),
        block_non_gm_weekend=True,
    ),
    "Manchester Markets": _SourcePolicy(
        min_path_depth=2,
        path_must_contain=("/market", "/event", "/whats-on"),
    ),
    "Time Out Manchester": _SourcePolicy(
        min_path_depth=3,
        path_must_contain=("/manchester/things-to-do", "/manchester/food-drink", "/manchester/art-culture"),
        path_banned_segments=("/travel/",),
        block_non_gm_weekend=True,
        block_listicle=True,
        block_evergreen=True,
        require_date_signal=True,
    ),
    "Manchester Food & Drink Festival": _SourcePolicy(
        min_path_depth=2,
        path_must_contain=("/event", "/festival", "/market"),
        path_banned_segments=("/news/",),
    ),
    "Visit Manchester Markets": _SourcePolicy(
        path_must_contain=("/whats-on/",),
        path_banned_starts=("/things-to-see-and-do/",),
        block_evergreen=True,
    ),
    "Manchester City Events": _SourcePolicy(
        min_path_depth=3,
        path_must_contain=("/event", "/whats-on"),
        path_banned_segments=_COUNCIL_NAV_SLUGS,
    ),
    "Salford Events": _SourcePolicy(
        min_path_depth=3,
        path_must_contain=("/event", "/whats-on"),
        path_banned_segments=_COUNCIL_NAV_SLUGS,
    ),
    "Stockport Events": _SourcePolicy(path_must_contain=("/events/",), min_path_depth=2),
    "Bolton Events": _SourcePolicy(path_must_contain=("/events/",), min_path_depth=2),
    "Visit Manchester": _SourcePolicy(
        block_evergreen=True,
        block_non_gm_weekend=True,
        require_date_signal=True,
    ),
}


def _evaluate_policy(
    policy: _SourcePolicy,
    path: str,
    lowered_path: str,
    lowered_title: str,
    lowered_summary: str,
) -> bool:
    depth = len([p for p in path.split("/") if p])
    if policy.path_must_contain and not any(t in lowered_path for t in policy.path_must_contain):
        return False
    if policy.path_all_must_contain and not all(t in lowered_path for t in policy.path_all_must_contain):
        return False
    if policy.path_must_start and not lowered_path.startswith(policy.path_must_start):
        return False
    if policy.path_banned_segments and any(t in lowered_path for t in policy.path_banned_segments):
        return False
    if policy.path_banned_starts and any(lowered_path.startswith(t) for t in policy.path_banned_starts):
        return False
    if policy.min_path_depth and depth < policy.min_path_depth:
        return False
    if policy.exact_path_depth and depth != policy.exact_path_depth:
        return False
    if policy.min_title_len and len(lowered_title) < policy.min_title_len:
        return False
    if policy.blocked_title_tokens and any(t in lowered_title for t in policy.blocked_title_tokens):
        return False
    if policy.block_evergreen and _is_evergreen_weekend_title(lowered_title):
        return False
    if policy.block_food_evergreen and _is_food_evergreen_or_admin(lowered_title, lowered_path):
        return False
    if policy.block_non_gm_food and _is_obviously_non_gm_food_item(lowered_title, lowered_path, lowered_summary):
        return False
    if policy.block_non_gm_weekend and _is_obviously_non_gm_weekend_item(lowered_title, lowered_path, lowered_summary):
        return False
    if policy.block_listicle and _is_listicle_opening(lowered_title):
        return False
    has_gm = _has_gm_token(lowered_title, lowered_path, lowered_summary)
    if policy.require_gm_token and not has_gm:
        return False
    if policy.require_food_opening_or_place:
        if not (_has_food_opening_signal(lowered_title) or _has_food_place_signal(lowered_title)):
            return False
    if policy.require_date_signal and not _has_weekend_date_signal(lowered_title, lowered_path):
        return False
    if policy.require_gm_or_date:
        if not (has_gm or _has_weekend_date_signal(lowered_title, lowered_path)):
            return False
    if policy.require_event_path_or_date:
        event_in_path = any(t in lowered_path for t in ("/events", "/whats-on", "/event"))
        if not (event_in_path or _has_weekend_date_signal(lowered_title, lowered_path)):
            return False
    return True


def _source_override(
    source: SourceDef,
    lowered_path: str,
    lowered_title: str,
    lowered_summary: str,
) -> bool | None:
    """Return True/False for sources whose logic can't be expressed as _SourcePolicy.
    Return None to fall through to policy/default evaluation.
    """
    if source.name == "BBC Manchester":
        if not ("/news/articles/" in lowered_path or "/news/uk-england-" in lowered_path):
            return False
        return _has_gm_token(lowered_title, lowered_path, lowered_summary)

    if source.name == "BBC Manchester public safety fallback":
        if not ("/news/articles/" in lowered_path or "/news/uk-england-" in lowered_path):
            return False
        public_safety_terms = (
            "police", "gmp", "arrest", "charged", "court", "stab",
            "murder", "assault", "fire", "crash", "drug",
        )
        return _has_gm_token(lowered_title, lowered_path) and any(
            term in lowered_title for term in public_safety_terms
        )

    if source.name == "National Rail":
        if not ("/status-and-disruptions" in lowered_path or "/engineering-works/" in lowered_path):
            return False
        if _has_gm_token(lowered_title):
            return True
        rail_terms = (
            "manchester airport", "manchester piccadilly", "manchester victoria",
            "oxford road", "deansgate",
        )
        return any(re.search(rf"\b{re.escape(t)}\b", lowered_title) for t in rail_terms)

    if source.name == "Manchester United":
        if "/en/news/" not in lowered_path:
            return False
        if _is_football_fluff(lowered_title, lowered_path):
            return False
        return _is_football_publishable(lowered_title, lowered_path)

    if source.name == "Manchester City":
        if "/news/" not in lowered_path:
            return False
        if _is_football_fluff(lowered_title, lowered_path):
            return False
        return _is_football_publishable(lowered_title, lowered_path)

    if source.name == "Manchester's Finest":
        if not any(t in lowered_path for t in ("/eating-and-drinking/", "/food-and-drink/", "/news/")):
            return False
        if _is_food_evergreen_or_admin(lowered_title, lowered_path):
            return False
        if _is_obviously_non_gm_food_item(lowered_title, lowered_path, lowered_summary):
            return False
        return _has_food_opening_signal(lowered_title) or (
            _has_gm_token(lowered_title, lowered_path, lowered_summary) and _has_food_place_signal(lowered_title)
        )

    if source.name == "Manchester Digital":
        if "/post/manchester-digital/" not in lowered_path:
            return False
        tech_terms = ("digital", "tech", "ecommerce", "infrastructure", "agency", "software", "ai")
        return _has_gm_token(lowered_title, lowered_path, lowered_summary) or any(
            term in lowered_title for term in tech_terms
        )

    return None


def _is_allowed_source_link(source: SourceDef, url: str, title: str, summary: str = "") -> bool:
    parsed = parse.urlsplit(url)
    path = parsed.path.rstrip("/")
    host = parsed.netloc.replace("www.", "")

    if source.allowed_hosts:
        if host and not any(
            host == allowed or host.endswith("." + allowed)
            for allowed in source.allowed_hosts
        ):
            return False
    else:
        base_host = parse.urlsplit(source.url).netloc.replace("www.", "")
        if host and base_host and not host.endswith(base_host):
            return False
    if not path or path == parse.urlsplit(source.url).path.rstrip("/"):
        return False

    lowered_title = title.lower()
    lowered_path = path.lower()
    lowered_summary = str(summary or "").lower()

    if source.report_category == "football" and _is_navigation_chrome(title):
        return False

    override = _source_override(source, lowered_path, lowered_title, lowered_summary)
    if override is not None:
        return override

    policy = _SOURCE_POLICIES.get(source.name)
    if policy is None:
        return len(lowered_title) >= 18

    return _evaluate_policy(policy, path, lowered_path, lowered_title, lowered_summary)
