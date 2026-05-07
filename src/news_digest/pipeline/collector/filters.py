"""Filtering predicates that decide whether a candidate link survives.

`_is_allowed_source_link` is the per-source URL/title gate. `_has_gm_token`
checks Greater Manchester locality. Per-block policies (listicle, football
fluff, city_watch topical) live here too because they all share the
filter style: input == candidate text, output == bool.
"""

from __future__ import annotations

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


def _is_listicle_opening(title: str) -> bool:
    lowered = str(title or "").lower()
    return any(pattern.search(lowered) for pattern in _LISTICLE_OPENINGS_PATTERNS)


_FOOTBALL_FLUFF_TOKENS: tuple[str, ...] = (
    "tv listings",
    "tv information",
    "academy",
    "u18",
    "u21",
    "pl2",
    "ticket information",
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
    "training session",
    "in pictures",
    "photo gallery",
    "behind the scenes",
    "afta studios",
    "wembley final",  # women's WSL — separate competition
    "wsl",
    "women's super league",
    "citc",  # City in the Community — charity, not first team
)


def _is_football_fluff(title: str, url: str = "") -> bool:
    lowered = f"{title} {url}".lower()
    if " on tv" in lowered or "tv-listings" in lowered:
        return True
    return any(token in lowered for token in _FOOTBALL_FLUFF_TOKENS)


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


def _is_allowed_source_link(source: SourceDef, url: str, title: str, summary: str = "") -> bool:
    parsed = parse.urlsplit(url)
    path = parsed.path.rstrip("/")
    host = parsed.netloc.replace("www.", "")
    if source.allowed_hosts:
        # Explicit allow-list — used when the source URL is a feed on a
        # different subdomain than the article links it carries.
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

    # Football club pages tend to surface section labels ('Women's Team',
    # 'EDS & Academy') as anchor text. Reject those before category-specific
    # rules so they don't slip into the football block.
    if source.report_category == "football" and _is_navigation_chrome(title):
        return False

    if source.name == "BBC Manchester":
        # BBC Manchester RSS surfaces regional North-West pieces where the
        # Greater Manchester geography only appears in the description, not
        # the headline.  Check title, URL path *and* RSS summary so that
        # items like 'Farmer refuses to budge for 2,000 new houses' (about
        # Tameside) are not silently dropped.
        if not ("/news/articles/" in lowered_path or "/news/uk-england-" in lowered_path):
            return False
        return _has_gm_token(lowered_title, lowered_path, str(summary or "").lower())
    if source.name == "BBC Manchester public safety fallback":
        if not ("/news/articles/" in lowered_path or "/news/uk-england-" in lowered_path):
            return False
        public_safety_terms = (
            "police",
            "gmp",
            "arrest",
            "charged",
            "court",
            "stab",
            "murder",
            "assault",
            "fire",
            "crash",
            "drug",
        )
        return _has_gm_token(lowered_title, lowered_path) and any(
            term in lowered_title for term in public_safety_terms
        )
    if source.name == "GMP":
        return "/news/greater-manchester/news/news/" in lowered_path
    if source.name == "MEN":
        if any(token in lowered_path for token in ("/all-about/", "/topic/", "/author/", "/newsletter/")):
            return False
        return "/news/" in lowered_path or "/whats-on/" in lowered_path
    if source.name == "ManchesterWorld":
        # ManchesterWorld is itself a Greater Manchester regional outlet.
        # The previous GM-token gate rejected every item because their
        # headlines often use district names ('Oldham', 'Wigan') that
        # already lived in GM_TOKENS but never the literal word
        # "Manchester". The check resulted in publishable_count = 0 even
        # though the feed worked. Trust the source by default; rely on
        # path filtering (must be under /news/), tag-page exclusion, and
        # downstream city_watch topical filter to drop noise.
        if "/news/" not in lowered_path:
            return False
        if any(token in lowered_path for token in ("/tag/", "/author/", "/page/", "/topic/")):
            return False
        return len(lowered_title) >= 18
    if source.name == "The Mill":
        if any(token in lowered_path for token in ("/tag/", "/author/", "/page/")):
            return False
        return len(lowered_title) >= 18
    if source.name == "Manchester Council":
        return lowered_path.startswith("/news-stories/20") and len(path.split("/")) >= 4
    if source.name == "Salford Council":
        return lowered_path.startswith("/news/") and len(path.split("/")) >= 3
    if source.name == "Trafford Council":
        return lowered_path.startswith("/news/20") and len(path.split("/")) >= 4
    if source.name == "Stockport Council":
        return lowered_path.startswith("/news/") and "/newsroom" not in lowered_path and len(path.split("/")) >= 3
    if source.name in {"Oldham Council", "Rochdale Council", "Bolton Council"}:
        return lowered_path.startswith("/news/article/") and len(path.split("/")) >= 4
    if source.name == "Tameside Council":
        return lowered_path.startswith("/newsroom/articles/") and len(path.split("/")) >= 4
    if source.name == "Bury Council":
        return "/pressreleases/" in lowered_path and "/bury-council/" in lowered_path
    if source.name == "Wigan Council":
        return lowered_path.startswith("/news/articles/20") and len(path.split("/")) >= 5
    if source.name == "Factory International":
        return "/whats-on/" in lowered_path and len(path.split("/")) >= 3
    if source.name == "Palace Theatre":
        return lowered_path.startswith("/shows/") and "/palace-theatre-manchester" in lowered_path
    if source.name == "TfGM":
        return "/travel-updates" in lowered_path or "/planned-works" in lowered_path
    if source.name == "National Rail":
        rail_terms = (
            "manchester airport",
            "manchester piccadilly",
            "manchester victoria",
            "oxford road",
            "deansgate",
        )
        if not (
            "/status-and-disruptions" in lowered_path or "/engineering-works/" in lowered_path
        ):
            return False
        if _has_gm_token(lowered_title):
            return True
        return any(re.search(rf"\b{re.escape(term)}\b", lowered_title) for term in rail_terms)
    if source.name == "Co-op Live":
        return "/events/" in lowered_path and len(path.split("/")) >= 3
    if source.name == "AO Arena":
        return "/events" in lowered_path and len(path.split("/")) >= 3
    if source.name == "Manchester United":
        if "/en/news/" not in lowered_path:
            return False
        return not any(token in lowered_title for token in ("women", "academy", "ticket information", "community"))
    if source.name == "Manchester City":
        if "/news/" not in lowered_path:
            return False
        return not any(token in lowered_title for token in ("eds", "academy", "women", "ticket information", "pl2", "u18"))
    if source.name == "Salford City":
        if "/news/" not in lowered_path:
            return False
        return not any(token in lowered_title for token in ("ticket information", "community", "highlights"))
    if source.name in {"HOME", "Whitworth", "The Lowry"}:
        return "/whats-on" in lowered_path or "/events" in lowered_path
    if source.name == "Manchester's Finest":
        if not any(token in lowered_path for token in ["/eating-and-drinking/", "/food-and-drink/", "/news/"]):
            return False
        # Accept (a) explicit opening signals or (b) named-establishment
        # signals — Manchester's Finest writes mostly about specific new
        # venues and rarely uses "opens" verbatim. The previous strict
        # filter dropped everything; this one keeps single-venue items
        # while the listicle filter (`_is_listicle_opening`) still drops
        # "best X in Manchester" roundups.
        opening_terms = (
            "opening", "opens", "launch", "launches", "launching",
            "coming soon", "first look", "now open", "officially open",
            "debut", "debuts", "unveil", "unveils", "reopen", "reopens",
        )
        named_venue_terms = (
            "new restaurant", "new bar", "new cafe", "new café",
            "new pub", "new venue", "new menu", "new spot",
            "rooftop", "tasting menu", "chef ", "by chef",
            "michelin", "head chef",
        )
        if any(term in lowered_title for term in opening_terms):
            return True
        if any(term in lowered_title for term in named_venue_terms):
            return True
        return False
    if source.name == "Manchester Digital":
        if "/post/manchester-digital/" not in lowered_path:
            return False
        return _has_gm_token(lowered_title, lowered_path, str(summary or "").lower()) or any(
            token in lowered_title
            for token in ("digital", "tech", "ecommerce", "infrastructure", "agency", "software", "ai")
        )
    if source.name == "Prolific North":
        if "/news/" not in lowered_path:
            return False
        return _has_gm_token(lowered_title, lowered_path, str(summary or "").lower()) or any(
            token in lowered_title
            for token in ("manchester", "northern", "agency", "digital", "media", "tech")
        )
    if source.name == "ITV Granada":
        # Granada region covers the wider North-West but the listing also
        # shows Lancashire/Cheshire-only stories. Keep only items that
        # mention Greater Manchester.
        if not any(token in lowered_path for token in ["/news", "/2026/"]):
            return False
        return _has_gm_token(lowered_title, lowered_path)
    if source.name == "The Manc":
        if not any(token in lowered_path for token in ["/news", "/2026/", "/whats-on", "/events"]):
            return False
        # The Manc occasionally publishes UK-wide listicles. Require an
        # explicit GM token in title or URL.
        if not _has_gm_token(lowered_title, lowered_path):
            return False
        return "best things to do" not in lowered_title
    if source.name == "I Love Manchester":
        if any(token in lowered_path for token in ("/category/", "/event-category/", "/tag/", "/author/", "/page/")):
            return False
        if len([part for part in path.split("/") if part]) != 1:
            return False
        return _has_gm_token(lowered_title, lowered_path, str(summary or "").lower())
    if source.name == "Secret Manchester":
        if any(token in lowered_path for token in ("/category/", "/tag/", "/author/", "/page/")):
            return False
        if len([part for part in path.split("/") if part]) != 1:
            return False
        return _has_gm_token(lowered_title, lowered_path, str(summary or "").lower())
    if source.name == "Confidentials":
        if not lowered_path.startswith("/manchester/"):
            return False
        if any(token in lowered_path for token in ("/p2", "/p3", "/p4", "/p5", "/p6", "/p7", "/p8", "/p9", "/p10", "/page/")):
            return False
        opening_terms = (
            "open", "opening", "opens", "launched", "launch", "coming to",
            "new bar", "new restaurant", "new cafe", "new café", "first look",
            "look inside",
        )
        return _has_gm_token(lowered_title, lowered_path, str(summary or "").lower()) or any(
            term in lowered_title for term in opening_terms
        )
    if source.name == "GMMH":
        return any(token in lowered_path for token in ["/news", "/2026/", "/whats-on", "/events"])

    # ── Weekend event sources ─────────────────────────────────────────────
    if source.name == "Skiddle Manchester":
        # Accept individual event pages: /events/REGION/CITY/VENUE/EVENT-NAME/
        return "/events/" in lowered_path and len([p for p in path.split("/") if p]) >= 4

    if source.name == "Eventbrite Manchester":
        # Accept individual event ticket pages: /e/event-name-tickets-12345/
        return "/e/" in lowered_path and "tickets-" in lowered_path

    if source.name == "Manchester Markets":
        # Accept market-specific pages, not homepage
        return len([p for p in path.split("/") if p]) >= 2 and (
            any(token in lowered_path for token in ("/market", "/event", "/whats-on"))
        )

    if source.name == "Time Out Manchester":
        # Accept specific what's-on pages (3+ path segments), reject evergreen listicles
        segments = [p for p in path.split("/") if p]
        if len(segments) < 3:
            return False
        if not lowered_path.startswith("/manchester/"):
            return False
        # Reject "best X", "top N" evergreen articles
        if _is_listicle_opening(lowered_title):
            return False
        # Reject year-in-review and trend articles
        if re.search(r"\b(2025|trends|guide to)\b", lowered_title):
            return False
        return True

    if source.name == "Manchester Food & Drink Festival":
        return len([p for p in path.split("/") if p]) >= 2 and (
            any(token in lowered_path for token in ("/event", "/festival", "/market", "/news"))
        )

    if source.name in {"Manchester City Events", "Salford Events"}:
        # Council event pages: require event-specific deep URL (3+ segments)
        segments = [p for p in path.split("/") if p]
        if len(segments) < 3:
            return False
        # Block known navigation slugs
        nav_slugs = {
            "translate-this-page", "emergency-contacts", "social-media-and-email-updates",
            "freedom-of-information", "modern-slavery-statement", "births-marriages-and-deaths",
            "business-and-licensing", "environmental-health", "accessibility-statement",
            "cookie-policy", "privacy-notice", "site-map", "search",
        }
        if any(slug in lowered_path for slug in nav_slugs):
            return False
        return "/event" in lowered_path or "/whats-on" in lowered_path

    if source.name in {"Stockport Events", "Bolton Events"}:
        segments = [p for p in path.split("/") if p]
        if len(segments) < 3:
            return False
        nav_slugs = {
            "translate", "emergency", "freedom-of-information", "modern-slavery",
            "accessibility", "cookie", "privacy", "sitemap", "search",
            "births", "business-and-licensing", "environmental-health",
        }
        if any(slug in lowered_path for slug in nav_slugs):
            return False
        return True

    # ── IT и бизнес — новые источники ────────────────────────────────────
    if source.name == "BusinessCloud":
        # RSS feed — accept article paths, reject tag/category/author pages
        if any(token in lowered_path for token in ("/tag/", "/category/", "/author/", "/page/")):
            return False
        return len([p for p in path.split("/") if p]) >= 2 and len(lowered_title) >= 25

    if source.name == "Bdaily Manchester":
        # RSS — accept article paths only
        if any(token in lowered_path for token in ("/tag/", "/category/", "/author/", "/page/", "/region/")):
            return False
        return "/articles/" in lowered_path and len(lowered_title) >= 25

    if source.name == "MIDAS Manchester":
        # Reject sector/navigation pages — only accept dated news articles
        nav_slugs = {
            "get-started", "sectors", "why-manchester", "about", "contact",
            "advanced-materials", "digital-cyber", "financial-professional",
            "life-sciences", "energy", "sport",
        }
        if any(slug in lowered_path for slug in nav_slugs):
            return False
        return "/news/" in lowered_path and len([p for p in path.split("/") if p]) >= 3

    return len(lowered_title) >= 18
