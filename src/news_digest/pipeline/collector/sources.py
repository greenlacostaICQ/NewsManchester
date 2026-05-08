"""Source registry and data shapes for the collector.

`SourceDef` describes one external source (URL, category, fallback list,
allowed hosts). `ExtractedItem` is the canonical shape produced by
parsers (RSS, JSON, HTML). `SOURCES` is the live registry; everything
else in the collector iterates over it.
"""

from __future__ import annotations

from dataclasses import dataclass


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


SOURCES: tuple[SourceDef, ...] = (
    SourceDef(
        "BBC Manchester",
        "media_layer",
        "media_layer",
        # RSS feed exposes pubDate per item, no need to re-fetch each
        # article HTML for <meta article:published_time>. Anchor URLs
        # land on the canonical bbc.com / bbc.co.uk article pages.
        "https://feeds.bbci.co.uk/news/england/manchester/rss.xml",
        "last_24h",
        source_type="rss",
        fallback_urls=("https://www.bbc.com/news/england/manchester",),
        allowed_hosts=("bbc.com", "bbc.co.uk", "bbci.co.uk"),
    ),
    # ITV Granada: PHASE 1 WAIVER — server enforces Cloudflare bot-challenge
    # on every request from urllib (timeout/403). No public RSS. Cannot be
    # scraped without a headless browser. Excluded from Phase 1 completion
    # criteria. Re-evaluate in Phase 2 with Playwright or a third-party feed.
    # SourceDef("ITV Granada", "media_layer", "media_layer", "https://www.itv.com/news/granada", "last_24h"),
    SourceDef(
        "MEN",
        "media_layer",
        "media_layer",
        "https://www.manchestereveningnews.co.uk/",
        "last_24h",
        allowed_hosts=("manchestereveningnews.co.uk",),
    ),
    # ManchesterWorld: PHASE 1 WAIVER — /rss and all sub-path RSS feeds
    # (/news/rss, /news/local-news/rss) contain only affiliate/recommended
    # consumer-commerce noise (category=Recommended), not local news.
    # Homepage (/news) returns 404 or blocks urllib. No usable feed found.
    # Re-evaluate in Phase 2 when they expose a proper local-news RSS or
    # a scrapeable news index. Fingerprint: manchesterworld.uk/rss returns
    # 7 items, all aff, 0 pass _is_allowed_source_link filter.
    # SourceDef(
    #     "ManchesterWorld",
    #     "media_layer",
    #     "media_layer",
    #     "https://www.manchesterworld.uk/rss",
    #     "last_24h",
    #     allowed_hosts=("manchesterworld.uk",),
    # ),
    SourceDef(
        "The Mill",
        "media_layer",
        "media_layer",
        "https://manchestermill.co.uk/rss/",
        "last_24h",
        source_type="rss",
        allowed_hosts=("manchestermill.co.uk",),
    ),
    SourceDef("The Manc", "media_layer", "media_layer", "https://themanc.com/", "last_24h"),
    SourceDef(
        "GMP",
        "gmp",
        "gmp",
        "https://www.gmp.police.uk/news/greater-manchester/news/GetNewsRss/",
        "last_24h",
        source_type="rss",
        fallback_urls=(
            # GMP's public listing page exposes real article links and a
            # working RSS endpoint. Prefer RSS for stable pubDate data and
            # fall back to the listing page if needed.
            "https://www.gmp.police.uk/news/greater-manchester/news/",
            "https://www.gmp.police.uk/news/greater-manchester/news/?newsCategory=News",
        ),
    ),
    SourceDef("BBC Manchester public safety fallback", "gmp", "gmp", "https://www.bbc.com/news/england/manchester", "last_24h"),
    SourceDef("TfGM", "transport", "transport", "https://tfgm.com/travel-updates/travel-alerts", "transport"),
    SourceDef("National Rail", "transport", "transport", "https://www.nationalrail.co.uk/status-and-disruptions/", "transport"),
    SourceDef("GMMH", "public_services", "public_services", "https://www.gmmh.nhs.uk/news/", "today_focus"),
    SourceDef(
        "Manchester Council",
        "media_layer",
        "council",
        (
            "https://manchester2-search.funnelback.squiz.cloud/s/search.json"
            "?collection=manchester~sp-search"
            "&profile=news-archive_preview"
            "&query="
            "&sort=dmetatimestamp"
            "&num_ranks=20"
            "&gscope1=news"
        ),
        "last_24h",
        source_type="json_funnelback",
        fallback_urls=("https://www.manchester.gov.uk/news-stories",),
        allowed_hosts=("manchester.gov.uk",),
    ),
    SourceDef("Salford Council", "media_layer", "council", "https://news.salford.gov.uk/news/", "last_24h"),
    SourceDef(
        "Trafford Council",
        "media_layer",
        "council",
        "https://www.trafford.gov.uk/news/",
        "last_24h",
        allowed_hosts=("trafford.gov.uk",),
    ),
    SourceDef(
        "Stockport Council",
        "media_layer",
        "council",
        "https://www.stockport.gov.uk/newsroom?page=1&pageSize=60&type=rss&view=Standard",
        "last_24h",
        allowed_hosts=("stockport.gov.uk",),
    ),
    SourceDef(
        "Oldham Council",
        "media_layer",
        "council",
        "https://www.oldham.gov.uk/rss/news",
        "last_24h",
        source_type="rss",
        allowed_hosts=("oldham.gov.uk",),
    ),
    SourceDef(
        "Rochdale Council",
        "media_layer",
        "council",
        "https://www.rochdale.gov.uk/rss/news",
        "last_24h",
        source_type="rss",
        allowed_hosts=("rochdale.gov.uk",),
    ),
    SourceDef(
        "Bolton Council",
        "media_layer",
        "council",
        "https://www.bolton.gov.uk/rss/news",
        "last_24h",
        source_type="rss",
        allowed_hosts=("bolton.gov.uk",),
    ),
    SourceDef(
        "Tameside Council",
        "media_layer",
        "council",
        "https://www.tameside.gov.uk/newsroom",
        "last_24h",
        allowed_hosts=("tameside.gov.uk",),
    ),
    SourceDef(
        "Bury Council",
        "media_layer",
        "council",
        "https://www.mynewsdesk.com/uk/rss/current_news/49585",
        "last_24h",
        source_type="rss",
        allowed_hosts=("mynewsdesk.com",),
    ),
    SourceDef(
        "Wigan Council",
        "media_layer",
        "council",
        "https://www.wigan.gov.uk/News/News.aspx",
        "last_24h",
        allowed_hosts=("wigan.gov.uk",),
    ),
    SourceDef("HOME", "culture_weekly", "culture_weekly", "https://homemcr.org/whats-on/", "next_7_days"),
    SourceDef("Whitworth", "culture_weekly", "culture_weekly", "https://www.whitworth.manchester.ac.uk/whats-on/events/", "next_7_days"),
    SourceDef("Factory International", "culture_weekly", "culture_weekly", "https://factoryinternational.org/whats-on/", "next_7_days"),
    SourceDef("The Lowry", "culture_weekly", "culture_weekly", "https://thelowry.com/whats-on", "next_7_days"),
    SourceDef(
        "Palace Theatre",
        "culture_weekly",
        "culture_weekly",
        "https://www.atgtickets.com/venues/palace-theatre-manchester/whats-on/",
        "next_7_days",
        allowed_hosts=("atgtickets.com",),
    ),
    SourceDef("Co-op Live", "venues_tickets", "venues_tickets", "https://www.cooplive.com/events", "ticket_radar"),
    SourceDef("AO Arena", "venues_tickets", "venues_tickets", "https://www.ao-arena.com/events", "next_7_days"),
    SourceDef("Manchester's Finest", "food_openings", "food_openings", "https://www.manchestersfinest.com/", "openings"),
    SourceDef(
        "Manchester Digital",
        "tech_business",
        "tech_business",
        "https://www.manchesterdigital.com/",
        "tech_business",
        allowed_hosts=("manchesterdigital.com",),
    ),
    SourceDef(
        "Prolific North",
        "tech_business",
        "tech_business",
        "https://www.prolificnorth.co.uk/news/?feed=rss2",
        "tech_business",
        source_type="rss",
        allowed_hosts=("prolificnorth.co.uk",),
    ),
    SourceDef(
        "University of Salford",
        "tech_business",
        "tech_business",
        "https://www.salford.ac.uk/news",
        "tech_business",
        allowed_hosts=("salford.ac.uk",),
    ),
    SourceDef(
        "University of Manchester",
        "tech_business",
        "tech_business",
        "https://www.manchester.ac.uk/about/news/",
        "tech_business",
        allowed_hosts=("manchester.ac.uk",),
    ),
    SourceDef(
        "Manchester Metropolitan",
        "tech_business",
        "tech_business",
        "https://www.mmu.ac.uk/news-and-events",
        "tech_business",
        allowed_hosts=("mmu.ac.uk",),
    ),
    SourceDef(
        "GMCA",
        "public_services",
        "public_services",
        "https://www.greatermanchester-ca.gov.uk/who-we-are/the-mayor/news/",
        "last_24h",
        allowed_hosts=("greatermanchester-ca.gov.uk",),
    ),
    SourceDef(
        "Visit Manchester",
        "culture_weekly",
        "culture_weekly",
        "https://www.visitmanchester.com/whats-on",
        "next_7_days",
        allowed_hosts=("visitmanchester.com",),
    ),
    SourceDef(
        "Contact Theatre",
        "culture_weekly",
        "culture_weekly",
        "https://contactmcr.com/whats-on/",
        "next_7_days",
        allowed_hosts=("contactmcr.com",),
    ),
    SourceDef(
        "People's History Museum",
        "culture_weekly",
        "culture_weekly",
        "https://phm.org.uk/whats-on/",
        "next_7_days",
        allowed_hosts=("phm.org.uk",),
    ),
    SourceDef(
        "John Rylands Library",
        "culture_weekly",
        "culture_weekly",
        "https://www.library.manchester.ac.uk/rylands/visit/events/",
        "next_7_days",
        allowed_hosts=("library.manchester.ac.uk",),
    ),
    SourceDef(
        "Altrincham Today",
        "media_layer",
        "media_layer",
        "https://altrincham.todaynews.co.uk/",
        "last_24h",
        allowed_hosts=("altrincham.todaynews.co.uk",),
    ),
    SourceDef(
        "Confidentials",
        "food_openings",
        "food_openings",
        "https://confidentials.com/manchester/food-drink/drink/new-openings",
        "openings",
        allowed_hosts=("confidentials.com",),
    ),
    SourceDef(
        "I Love Manchester",
        "media_layer",
        "media_layer",
        "https://ilovemanchester.com/feed",
        "city_watch",
        source_type="rss",
        allowed_hosts=("ilovemanchester.com",),
    ),
    SourceDef(
        "Secret Manchester",
        "media_layer",
        "media_layer",
        "https://secretmanchester.com/feed",
        "city_watch",
        source_type="rss",
        allowed_hosts=("secretmanchester.com",),
    ),
    # ── Выходные события / Weekend activities ──────────────────────────────
    SourceDef(
        "Manchester Wire",
        "culture_weekly",
        "culture_weekly",
        "https://manchesterwire.co.uk/",
        "weekend_activities",
        allowed_hosts=("manchesterwire.co.uk",),
    ),
    SourceDef(
        "Creative Tourist Manchester",
        "culture_weekly",
        "culture_weekly",
        "https://www.creativetourist.com/whats-on/",
        "weekend_activities",
        allowed_hosts=("creativetourist.com",),
    ),
    SourceDef(
        "DesignMyNight Manchester",
        "culture_weekly",
        "culture_weekly",
        "https://www.designmynight.com/manchester/whats-on/things-to-do-this-weekend-in-manchester",
        "weekend_activities",
        allowed_hosts=("designmynight.com",),
    ),
    # Resident Advisor Manchester is intentionally not active in the urllib
    # collector: ra.co returns Cloudflare HTTP 403 to direct HTTP fetches.
    # Re-enable only with a browser/API fetch path, otherwise it looks wired
    # but contributes zero events.
    # SourceDef(
    #     "Resident Advisor Manchester",
    #     "culture_weekly",
    #     "culture_weekly",
    #     "https://ra.co/events/uk/manchester",
    #     "weekend_activities",
    #     allowed_hosts=("ra.co",),
    # ),
    SourceDef(
        "Fairfield Social Club",
        "culture_weekly",
        "culture_weekly",
        "https://fscmcr.co.uk/",
        "weekend_activities",
        allowed_hosts=("fscmcr.co.uk",),
    ),
    SourceDef(
        "Eventbrite Manchester",
        "culture_weekly",
        "culture_weekly",
        "https://www.eventbrite.co.uk/d/united-kingdom--manchester/",
        "weekend_activities",
        allowed_hosts=("eventbrite.co.uk", "eventbrite.com"),
    ),
    SourceDef(
        "Manchester Markets",
        "culture_weekly",
        "culture_weekly",
        "https://www.manchestermarkets.com/",
        "weekend_activities",
        allowed_hosts=("manchestermarkets.com",),
    ),
    SourceDef(
        "Skiddle Manchester",
        "culture_weekly",
        "culture_weekly",
        "https://www.skiddle.com/whats-on/Manchester/",
        "weekend_activities",
        allowed_hosts=("skiddle.com",),
    ),
    SourceDef(
        "Visit Manchester Markets",
        "culture_weekly",
        "culture_weekly",
        "https://www.visitmanchester.com/whats-on/markets",
        "weekend_activities",
        allowed_hosts=("visitmanchester.com",),
    ),
    SourceDef(
        "Time Out Manchester",
        "culture_weekly",
        "culture_weekly",
        "https://www.timeout.com/manchester/things-to-do",
        "weekend_activities",
        allowed_hosts=("timeout.com",),
    ),
    SourceDef(
        "Manchester Food & Drink Festival",
        "culture_weekly",
        "culture_weekly",
        "https://www.foodanddrinkfestival.com/",
        "weekend_activities",
        allowed_hosts=("foodanddrinkfestival.com",),
    ),
    # ── IT и бизнес — расширенное покрытие ─────────────────────────────────
    SourceDef(
        "BusinessCloud",
        "tech_business",
        "tech_business",
        "https://businesscloud.co.uk/feed/",
        "tech_business",
        source_type="rss",
        allowed_hosts=("businesscloud.co.uk",),
    ),
    SourceDef(
        "Bdaily Manchester",
        "tech_business",
        "tech_business",
        "https://bdaily.co.uk/feed/region/manchester",
        "tech_business",
        source_type="rss",
        allowed_hosts=("bdaily.co.uk",),
    ),
    SourceDef(
        "MIDAS Manchester",
        "tech_business",
        "tech_business",
        "https://www.investinmanchester.com/news/",
        "tech_business",
        allowed_hosts=("investinmanchester.com",),
    ),
    # ── Районные события — советы боро (с фильтрами в filters.py) ──────────
    SourceDef(
        "Manchester City Events",
        "culture_weekly",
        "culture_weekly",
        "https://www.manchester.gov.uk/events",
        "weekend_activities",
        allowed_hosts=("manchester.gov.uk",),
    ),
    SourceDef(
        "Salford Events",
        "culture_weekly",
        "culture_weekly",
        "https://www.salford.gov.uk/leisure-and-culture/events/",
        "weekend_activities",
        allowed_hosts=("salford.gov.uk",),
    ),
    SourceDef(
        "Stockport Events",
        "culture_weekly",
        "culture_weekly",
        "https://www.stockport.gov.uk/events",
        "weekend_activities",
        allowed_hosts=("stockport.gov.uk",),
    ),
    SourceDef(
        "Bolton Events",
        "culture_weekly",
        "culture_weekly",
        "https://www.bolton.gov.uk/events",
        "weekend_activities",
        allowed_hosts=("bolton.gov.uk",),
    ),
    # ── Футбол — только Man Utd и Man City (трансферы + матчи) ────────────
    SourceDef("Manchester United", "football", "football", "https://www.manutd.com/en/news", "football"),
    SourceDef("Manchester City", "football", "football", "https://www.mancity.com/news", "football"),
    # Salford City убран — не входит в фокус дайджеста
)
