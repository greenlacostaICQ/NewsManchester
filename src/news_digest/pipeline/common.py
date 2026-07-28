from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
from urllib import parse
from zoneinfo import ZoneInfo

from news_digest.pipeline.block_policy import (
    BLOCK_POLICY_REGISTRY,
    PRIMARY_BLOCKS,
    block_active_on_weekday,
)

LONDON_TZ = ZoneInfo("Europe/London")

REQUIRED_SCAN_CATEGORIES = {
    "media_layer": "media-layer",
    "transport": "transport",
    "gmp": "GMP",
    "public_services": "public services",
    "culture_weekly": "culture weekly",
    "venues_tickets": "venues / tickets",
    "food_openings": "food / openings",
    "football": "football",
    "tech_business": "tech / business",
    "professional_events": "professional events",
    "diaspora_events": "Russian-speaking / diaspora events",
}

REQUIRED_BLOCKS = [
    str(policy["heading"])
    for policy in BLOCK_POLICY_REGISTRY.values()
    if int(policy.get("min") or 0)
    and not bool(policy.get("optional"))
    and str(policy.get("schedule") or "") != "retired"
]


def required_blocks_for_weekday(weekday: int) -> list[str]:
    return [
        str(policy["heading"])
        for block, policy in BLOCK_POLICY_REGISTRY.items()
        if int(policy.get("min") or 0)
        and not bool(policy.get("optional"))
        and block_active_on_weekday(block, weekday)
    ]


LOW_SIGNAL_BLOCKS = [
    str(policy["heading"])
    for policy in BLOCK_POLICY_REGISTRY.values()
    if bool(policy.get("optional"))
    and str(policy.get("schedule") or "") != "retired"
]

SECTION_MAX_ITEMS = {
    str(policy["heading"]): int(policy["max"])
    for policy in BLOCK_POLICY_REGISTRY.values()
    if int(policy.get("max") or 0)
}

# Soft minimums: release gate emits a warning (does not block) when a
# section ends up below this count after caps and quality drops. Used to
# catch days when curator only nominated 2 items for "Что важно сегодня"
# or similar — so the underflow is visible in release_report instead of
# silently shipping a thin section.
SECTION_MIN_ITEMS = {
    str(policy["heading"]): int(policy["min"])
    for policy in BLOCK_POLICY_REGISTRY.values()
    if int(policy.get("min") or 0)
}

# Max items per single source per section. Universities pump out 5+ press
# releases a day each and dominated city_watch on 2026-05-12 — keep them
# capped so they don't crowd out actual city news.
SECTION_MAX_PER_SOURCE = {
    str(policy["heading"]): int(policy["max_per_source"])
    for policy in BLOCK_POLICY_REGISTRY.values()
    if int(policy.get("max_per_source") or 0)
}


VAGUE_PRACTICAL_ANGLES = {
    "Оценить городскую значимость перед выпуском.",
    "Проверить матчевый контекст перед включением в футбольный блок.",
}

def now_london() -> datetime:
    # NEWS_DIGEST_FAKE_NOW (ISO datetime) freezes pipeline time for offline
    # replays of past days (scripts/replay_day.py). Never set in production.
    fake = os.environ.get("NEWS_DIGEST_FAKE_NOW", "").strip()
    if fake:
        parsed = datetime.fromisoformat(fake)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=LONDON_TZ)
        return parsed.astimezone(LONDON_TZ)
    return datetime.now(LONDON_TZ)


def today_london() -> str:
    return now_london().strftime("%Y-%m-%d")


def new_pipeline_run_id() -> str:
    return now_london().strftime("%Y%m%dT%H%M%S%z")


def pipeline_run_id_from(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("pipeline_run_id") or payload.get("run_id") or "").strip()


def read_json(path: Path, default: dict | None = None) -> dict:
    if not path.exists():
        return {} if default is None else default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_json_atomic(path: Path, payload: dict) -> None:
    """Write via temp file + rename so a concurrent reader never sees a
    partially-written file (os.replace is atomic on the same filesystem)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp{os.getpid()}")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)


def clean_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = parse.urlsplit(raw)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = re.sub(r"/+$", "", parsed.path or "") or "/"
    return parse.urlunsplit((scheme, netloc, path, "", ""))


# A published source_url must be a clean absolute http(s) link. JSON-LD `url`
# fields and some feeds leak raw HTML/description text into the URL slot — e.g.
# a CONEXEN event whose `url` was "<p><strong>Registration needed: https:/..."
# which urljoin then glued into a broken href. Reject anything carrying markup,
# whitespace or a non-http scheme so the caller can fall back to a real link.
# Note: bare "&" (valid query separator) is intentionally NOT rejected; only the
# HTML-entity forms "&lt;"/"&gt;" and literal angle brackets/whitespace are.
_URL_MARKUP_RE = re.compile(r"[<>\s]|&lt;|&gt;")


def valid_http_url(url: str) -> bool:
    raw = str(url or "").strip()
    if not raw or _URL_MARKUP_RE.search(raw):
        return False
    parsed = parse.urlsplit(raw)
    if parsed.scheme.lower() not in {"http", "https"}:
        return False
    # A real host has a dot; "https:/host/path" (single slash) lands the host in
    # the path and leaves netloc empty — reject it.
    return bool(parsed.netloc) and "." in parsed.netloc


def first_valid_http_url(*candidates: str) -> str:
    for candidate in candidates:
        cleaned = str(candidate or "").strip()
        if valid_http_url(cleaned):
            return cleaned
    return ""


# BBC publishes the same article under both bbc.com and bbc.co.uk. Our two
# BBC Manchester feeds (RSS → bbc.com links, web backup → bbc.co.uk links) thus
# produced two different URL identities for one story, so the twin slipped past
# exact-URL dedup into the noisier topic dedup. Fold the domains together.
_BBC_HOST_ALIASES = ("bbc.co.uk", "bbc.com")


def canonical_url_identity(url: str) -> str:
    cleaned = clean_url(url)
    if not cleaned:
        return ""
    parsed = parse.urlsplit(cleaned)
    netloc = parsed.netloc
    if any(netloc == h or netloc.endswith("." + h) for h in _BBC_HOST_ALIASES):
        netloc = "bbc.com"
    return f"{netloc}{parsed.path}"


def normalize_title(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9а-яё]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _fingerprint_slug(prefix: str, identity: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(identity or "").lower()).strip("-")
    digest = hashlib.sha1(str(identity or "").encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{normalized[:150]}-{digest}".strip("-")[:180]


def _event_identity_name(value: object) -> str:
    """Normalise collector metadata without erasing a real event title."""
    text = str(value or "").strip()
    text = re.split(
        r"\s+[—–-]\s+event\s+20\d{2}-\d{2}-\d{2}\b",
        text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    text = re.split(
        r"\s+[—–-]\s+(?:public\s+sale|tickets?\s+on\s+sale)\b",
        text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    return normalize_title(text)


def fingerprint_for_candidate(candidate: dict) -> str:
    """Return the source-independent identity of one article or occurrence.

    Source labels describe provenance, not identity.  The same Ticketmaster
    event can arrive through Manchester, London and UK feeds; including the
    label created parallel publication counters for one physical event.
    Structured occurrences therefore use, in order, the provider event id,
    name/date/venue identity, canonical URL, and only then a title fallback.
    """
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    hint = (
        candidate.get("structured_event_hint")
        if isinstance(candidate.get("structured_event_hint"), dict)
        else {}
    )
    instance_id = str(
        candidate.get("event_instance_id")
        or event.get("event_instance_id")
        or event.get("ticketmaster_event_id")
        or hint.get("event_instance_id")
        or hint.get("ticketmaster_event_id")
        or ""
    ).strip()
    source_url = canonical_url_identity(str(candidate.get("source_url") or ""))
    provider = str(event.get("schema_source") or hint.get("schema_source") or "").strip()
    if not provider and source_url:
        provider = source_url.split("/", 1)[0]
    if instance_id:
        return _fingerprint_slug("event-id", f"{provider}|{instance_id}")

    is_event = bool(event.get("is_event") or hint.get("event_name"))
    if is_event:
        explicit_identity = str(
            candidate.get("event_identity_key")
            or event.get("event_identity_key")
            or hint.get("event_identity_key")
            or ""
        ).strip()
        if explicit_identity:
            return _fingerprint_slug("event", explicit_identity)
        event_name = _event_identity_name(
            str(event.get("event_name") or hint.get("event_name") or candidate.get("title") or "")
        )
        event_date = str(
            event.get("date_start")
            or event.get("date")
            or hint.get("date_start")
            or hint.get("date")
            or ""
        ).strip()[:10]
        venue = normalize_title(str(event.get("venue") or hint.get("venue") or ""))
        if event_name and event_date and venue:
            return _fingerprint_slug("event", f"{event_name}|{event_date}|{venue}")

    raw_source_url = str(candidate.get("source_url") or "")
    parsed_source_url = parse.urlsplit(raw_source_url)
    event_page_type = str(candidate.get("event_page_type") or "").strip().lower()
    path_segments = [value for value in parsed_source_url.path.rstrip("/").lower().split("/") if value]
    generic_event_tail = bool(
        path_segments
        and path_segments[-1] in {"events", "calendar", "programme", "whats-on", "what-s-on"}
    )
    if is_event and parsed_source_url.fragment and source_url:
        return _fingerprint_slug("event-url", f"{source_url}#{parsed_source_url.fragment}")
    if source_url and not (
        is_event
        and (event_page_type in {"homepage", "aggregator"} or generic_event_tail)
    ):
        return _fingerprint_slug("url", source_url)

    title = str(candidate.get("title") or "").strip().lower()
    category = str(candidate.get("category") or "").strip().lower()
    return _fingerprint_slug("title", f"{category}|{normalize_title(title)}")


def candidates_by_fingerprint(candidates: list[dict]) -> dict[str, dict]:
    """Return the strongest canonical row for each fingerprint.

    The collected pool intentionally retains dropped twin-source rows for
    provenance. Downstream slot execution must not use ordinary last-write-wins:
    a dropped web duplicate can otherwise overwrite the selected, enriched row
    carrying the same canonical URL.
    """

    def _priority(candidate: dict) -> tuple[int, ...]:
        plan_status = str(candidate.get("publish_plan_status") or "")
        selection = str(candidate.get("digest_selection_verdict") or "")
        dedupe = str(candidate.get("dedupe_decision") or "")
        return (
            {"must_show": 3, "show": 2, "reserve": 1}.get(plan_status, 0),
            int(bool(candidate.get("include"))),
            int(dedupe != "drop"),
            {"selected": 3, "reserve": 2, "needs_enrichment": 1}.get(selection, 0),
            int(bool(candidate.get("validated"))),
            int(bool(str(candidate.get("draft_line") or "").strip())),
            int(bool(candidate.get("prewrite_enrichment") or candidate.get("enriched_from_source"))),
            len(str(candidate.get("evidence_text") or "")),
            len(str(candidate.get("summary") or candidate.get("lead") or "")),
        )

    result: dict[str, dict] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        fingerprint = str(candidate.get("fingerprint") or "").strip()
        if not fingerprint:
            continue
        current = result.get(fingerprint)
        if current is None or _priority(candidate) > _priority(current):
            result[fingerprint] = candidate
    return result


def is_placeholder_practical_angle(value: str) -> bool:
    text = str(value or "").strip()
    return text in VAGUE_PRACTICAL_ANGLES or text.startswith("Включать только")


def extract_sections(html_text: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current_section: str | None = None

    for raw_line in html_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = re.fullmatch(r"<b>([^<]+)</b>", line)
        if match:
            heading = match.group(1).strip()
            if heading.startswith("Greater Manchester Brief"):
                current_section = None
                continue
            current_section = heading
            sections.setdefault(current_section, [])
            continue
        if current_section is None:
            continue
        if line.startswith("• "):
            sections[current_section].append(line)
        elif line.startswith("<b>"):
            # The lead story renders as a bold sentence with NO bullet (see
            # writer.py "Lead story: no bullet, bold first sentence"). A pure
            # <b>heading</b> was already consumed above, so a <b>-prefixed line
            # here is real content (the lead). Capture it, otherwise the lead
            # block parses as empty and the editor rebuild / HTML-truth count /
            # lead-visible check all silently lose the day's main story.
            sections[current_section].append(line)

    return sections
