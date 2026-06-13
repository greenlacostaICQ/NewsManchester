from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import html
import logging
import os
from pathlib import Path
import re

logger = logging.getLogger(__name__)

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    SECTION_MAX_ITEMS,
    SECTION_MAX_PER_SOURCE,
    SECTION_MIN_ITEMS,
    is_placeholder_practical_angle,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.editorial_contracts import (
    attach_editorial_contract,
    classify_ticket_type,
    copy_invariant_errors,
    scrub_vague_ending,
    why_now_is_publishable,
)
from news_digest.pipeline.reader_value import reader_value_score
from news_digest.pipeline.reader_actions import classify_reader_action
from news_digest.pipeline.source_selection import source_score
from news_digest.pipeline.story_intelligence import section_board_score
from news_digest.pipeline.ticket_notability import enrich_ticket_notability, prefetch_notability, ticket_artist_name
from news_digest.pipeline.toponyms import restore_english_toponyms
from news_digest.pipeline.place_names import preserve_place_names


MODEL_WRITTEN_CATEGORIES = {"media_layer", "gmp", "council", "public_services", "food_openings"}
REQUIRE_DRAFT_LINE_CATEGORIES = MODEL_WRITTEN_CATEGORIES | {
    "transport",
    "venues_tickets",
    "russian_speaking_events",
    "culture_weekly",
    "football",
    "tech_business",
    "city_news",
}
# Categories that should render as 350–450 char multi-sentence cards rather
# than single-line headlines. Transport / weather / billet are explicitly
# excluded — they're shorter by design.
LONG_FORMAT_CATEGORIES = {
    "media_layer",
    "gmp",
    "council",
    "public_services",
    "city_news",
    "food_openings",
    "tech_business",
    "culture_weekly",
    "venues_tickets",
    "russian_speaking_events",
    "football",
}
LONG_FORMAT_MIN_CHARS = 150
# A fully dated event card (real event + concrete date) that the source
# kept short does not need 150 chars of prose. The extractor often fails to
# populate event.venue even when the venue is right there in the draft ("В
# HOME 1 июня…"), so requiring date+venue in the struct dropped complete
# listings like The Misfits (PG) at 126 chars on 2026-06-01. A dated card
# only needs to clear this lower floor + the 2-sentence rule.
DATED_EVENT_MIN_CHARS = 110
LONG_FORMAT_MIN_SENTENCES = 2
SHORT_TICKET_BLOCKS = {"ticket_radar", "outside_gm_tickets"}
# Tickets are intentionally short. Weekend cards are planning cards, not
# ticket rows: they need enough room for "what / where / when / useful
# detail" so the reader can choose between markets, fairs and activities.
SHORT_EVENT_BLOCKS = SHORT_TICKET_BLOCKS
# Sequential fallback: event blocks where we PREFER 150+ char cards
# (more detail = better) but ACCEPT shorter ones when the source RSS
# only gave us a thin evidence_text. Logic in _draft_line_quality_errors
# checks evidence size before applying the LONG_FORMAT_MIN_CHARS gate.
# We only relax when evidence was genuinely tiny (< 500 chars).
EVENT_BLOCKS_RELAXABLE = {"weekend_activities", "next_7_days", "future_announcements", "russian_events"}
EVENT_RELAX_EVIDENCE_THRESHOLD = 500
TODAY_FOCUS_SECTION = "Что важно сегодня"
# Order matters: backfill takes the first non-empty section. We previously
# pulled from transport FIRST, which dumped bus-stop closures into "Что
# важно сегодня" (those are not "important news of the day" — they're
# already shown in the transport block above). Now media news leads;
# transport is the last-resort fallback only when there's literally nothing
# else to put up top.
TODAY_FOCUS_BACKFILL_SECTIONS = (
    "Свежие новости",
    "Городской радар",
)
TODAY_FOCUS_BACKFILL_TARGET = 2
TODAY_FOCUS_BACKFILL_MIN_SCORE = 67.5
TODAY_FOCUS_MIN_SOURCE_REMAINING = {
    # Don't gut source blocks just to fill today_focus.
    "Свежие новости": 3,
    "Городской радар": 4,
}
FRESH_NEWS_TARGET_ITEMS = 7
TODAY_FOCUS_TARGET_ITEMS = 4

# When the LLM rewrite stage is degraded, keep soft rails compact without
# suppressing hard-news that did get rewritten.
DEGRADED_LLM_SECTION_MAX_ITEMS = {
    "Свежие новости": 6,
    "Городской радар": 5,
    "Что важно в ближайшие 7 дней": 4,
    "Выходные в GM": 6,
    "Билеты / Ticket Radar": 3,
    "Еда, открытия и рынки": 2,
    "IT и бизнес": 2,
    "Футбол": 2,
    "Русскоязычные концерты и стендап UK": 3,
}

PUBLIC_DIGEST_MAX_VISIBLE_ITEMS = 35
PUBLIC_SECTION_RESERVED_MIN = {
    # Fresh/Today are the product spine of the morning issue. They must not be
    # squeezed by later ticket/event rails when strong written news already
    # exists.
    "Свежие новости": FRESH_NEWS_TARGET_ITEMS,
    TODAY_FOCUS_SECTION: SECTION_MIN_ITEMS.get(TODAY_FOCUS_SECTION, 3),
    # These sections answer "what can I do / see / book now"; they must not
    # disappear just because early news sections are noisy on a given morning.
    "Выходные в GM": 8,
    "Что важно в ближайшие 7 дней": 3,
    "Билеты / Ticket Radar": 2,
    "Футбол": 2,
}
_BAD_EDITORIAL_PROSE_MARKERS = (
    "ticket office",
    "слот входа",
    "госпитальн",
    "кадровый и дисциплинарный кейс",
    "заметный кейс",
    "новая фаза истории",
    "сетка влияния",
    "this website makes extensive use of javascript",
    "browser settings",
    "проверьте время",
    "проверьте дату",
    "билеты и детали берите",
    "undefined",
    "следить компаниям",
    "business-impact",
    "лучше взять зонт",
    "лучше прихватить зонт",
    "не забудьте зонт",
    "прихватите зонт",
    "live alert",
    "live disruption",
    "forecast",
    "attractions",
    "highlights",
    "matchday",
    "check before",
    "опубликовал важное обновление",
    "появилось новое обновление",
    "судебное обновление",
    "новое судебное",
    "футбольное обновление",
    "перепроверьте",
    "убедитесь сами",
    "читайте подробнее",
    "подробности ниже",
    # PR filler endings from LLM padding
    "обогатит",
    "обещает стать",
    "центр притяжения",
    "новая достопримечательность",
    "другие детали не сообщаются",
    "подробности не раскрываются",
    "остаётся нерешённой",
    "привлечёт внимание",
    "вступило в силу.",
    "билеты и даты уточняйте",
    "время и дату уточняйте",
    "дату и время уточняйте",
    "уточните даты",
    "проверьте детали",
    "свяжитесь с организатор",
    "проверьте сами",
)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path
    draft_path: Path


@dataclass(slots=True)
class _SectionRow:
    section: str
    line: str
    source: str
    score: float
    fingerprint: str
    title: str
    candidate: dict | None


def _title_line() -> str:
    now = now_london()
    return f"<b>Greater Manchester Brief — {now.strftime('%Y-%m-%d, %H:%M')}</b>"


def _normalize_text_key(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9а-яё]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _summary_is_useful(summary: str, headline: str) -> bool:
    cleaned = str(summary or "").strip()
    if not cleaned:
        return False
    if _normalize_text_key(cleaned) == _normalize_text_key(headline):
        return False
    if len(cleaned) < 28:
        return False
    return True


_TODAY_FOCUS_BOARD_SOURCE_SECTIONS = (
    "Свежие новости",
    TODAY_FOCUS_SECTION,
    "Городской радар",
)
_TODAY_FOCUS_ALLOWED_STORY_TYPES = {
    "public_safety_after_incident",
    "service_accountability",
    "planning",
    "civic",
    "local_cost",
    "local_service_change",
    "incident",
}
_TODAY_FOCUS_BLOCKED_STORY_TYPES = {
    "event",
    "ticket",
    "human_interest",
    "soft_news",
    "research",
    "memorial",
    "opening",
    "old_existing_food",
    "property_listing",
    "day_out_guide",
}
_TODAY_FOCUS_BLOCKED_CATEGORIES = {
    "venues_tickets",
    "culture_weekly",
    "russian_speaking_events",
    "diaspora_events",
    "football",
    "food_openings",
    "tech_business",
}
_TODAY_FOCUS_SOFT_RE = re.compile(
    r"\b(?:charity|fundrais|tribute|award|celebrat|anniversary|ultramarathon|"
    r"personal story|speaks out|silently screaming|dream|proud|inspiring)\b",
    re.IGNORECASE,
)
_TODAY_FOCUS_ROAD_RE = re.compile(
    r"\b(?:traffic|roadworks?|diversion|m6|m60|m62|m56|a580|east\s+lancs|"
    r"lane|closed|closure|crash|collision|queues?|delays?)\b",
    re.IGNORECASE,
)
_FRESH_COMMERCIAL_PR_RE = re.compile(
    r"\b(?:fulfilment|fulfillment|warehouse|retailer|online\s+retailer|"
    r"workforce|orders?|sq\s*ft|square\s+feet|centre\s+opens?|site\s+opens?|"
    r"jobs?|growth|fastest-growing|investment)\b",
    re.IGNORECASE,
)
_FRESH_SIDEBAR_RE = re.compile(
    r"\b(?:mum|mother|family|parent|reacts?|horrified|calls?\s+for|"
    r"speaks?\s+out|tribute)\b",
    re.IGNORECASE,
)
_HARD_SERVICE_ACCOUNTABILITY_RE = re.compile(
    r"\b(?:cqc|ofsted|inspection|inadequate|requires\s+improvement|safety|"
    r"safeguarding|council|licen[cs]e|closure|closed|funding|waiting\s+list|"
    r"patients?|children'?s\s+safety|police|court)\b",
    re.IGNORECASE,
)


def _candidate_contract(candidate: dict | None) -> dict:
    if not isinstance(candidate, dict):
        return {}
    attach_editorial_contract(candidate)
    return candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}


def _candidate_story_type(candidate: dict | None) -> str:
    contract = _candidate_contract(candidate)
    return str(contract.get("story_type") or "")


def _candidate_publish_tier(candidate: dict | None) -> str:
    contract = _candidate_contract(candidate)
    return str(contract.get("publish_tier") or "")


def _row_blob(row: _SectionRow) -> str:
    c = row.candidate or {}
    return " ".join(
        str(value or "")
        for value in (
            row.title,
            row.line,
            c.get("title"),
            c.get("summary"),
            c.get("lead"),
            c.get("evidence_text"),
            c.get("source_label"),
        )
    )


def _today_focus_bucket(row: _SectionRow) -> str:
    story_type = _candidate_story_type(row.candidate)
    blob = _row_blob(row)
    if story_type in {"service_accountability", "local_service_change"}:
        return "service"
    if story_type in {"planning", "civic", "local_cost"}:
        return "civic"
    if (
        story_type == "public_safety_after_incident"
        or _TODAY_FOCUS_ROAD_RE.search(blob)
        or re.search(r"\b(?:warning|warned|parents?|abandoned|unsafe|danger|safety)\b", blob, re.IGNORECASE)
    ):
        return "safety"
    if story_type == "incident":
        return "incident"
    return "other"


def _candidate_future_only_dates(candidate: dict, line: str = "") -> list[date]:
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle", "source_url")
    )
    if line:
        text = f"{text} {line}"
    dates = _date_signals(text)
    if not dates:
        return []
    today = now_london().date()
    if any(day <= today for day in dates):
        return []
    if re.search(r"\b(?:today|сегодня|this morning|tonight|now|ongoing|continues|active|сейчас|продолжа)\b", text, re.IGNORECASE):
        return []
    return sorted(day for day in dates if day > today)


def _today_focus_candidate_is_eligible(candidate: dict | None, line: str = "") -> bool:
    if not isinstance(candidate, dict):
        return False
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    if category in _TODAY_FOCUS_BLOCKED_CATEGORIES:
        return False
    if block in {"weather", "transport", "football", "ticket_radar", "outside_gm_tickets"}:
        return False
    contract = _candidate_contract(candidate)
    story_type = str(contract.get("story_type") or "")
    event_shape = str(contract.get("event_shape") or "")
    tier = str(contract.get("publish_tier") or "")
    if event_shape not in {"", "none"}:
        return False
    if story_type in _TODAY_FOCUS_BLOCKED_STORY_TYPES:
        return False
    if story_type not in _TODAY_FOCUS_ALLOWED_STORY_TYPES:
        return False
    if _candidate_future_only_dates(candidate, line):
        return False
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle")
    )
    if line:
        text = f"{text} {line}"
    if tier == "reject":
        return False
    if tier == "filler" and not (
        story_type == "local_cost"
        and re.search(r"\b(?:flood|water|electric|power|damage|closed|closure|reopen|cost|thousands?)\b", text, re.IGNORECASE)
    ):
        return False
    if story_type in {"incident", "public_safety_after_incident"} and not re.search(
        r"\b(?:warning|warned|parents?|abandoned|licen[cs]e|council|flood|water|electric|"
        r"power|road|m6|m60|m62|m56|a580|closed|closure|delays?|diversion|appeal|cctv|"
        r"tram\s+stop|school\s+closed|unsafe|danger)\b",
        text,
        re.IGNORECASE,
    ):
        return False
    if story_type == "service_accountability" and _TODAY_FOCUS_SOFT_RE.search(text) and not _HARD_SERVICE_ACCOUNTABILITY_RE.search(text):
        return False
    if _FRESH_COMMERCIAL_PR_RE.search(text) and not re.search(
        r"\b(?:council|licen[cs]e|safety|warning|flood|water|electric|power|closed|closure|ofsted|cqc)\b",
        text,
        re.IGNORECASE,
    ):
        return False
    # Personal awareness pieces can live in City Radar, but they should not
    # fill the morning practical block unless a service/accountability anchor
    # is explicit.
    if story_type not in {"service_accountability", "local_service_change"} and _TODAY_FOCUS_SOFT_RE.search(text):
        return False
    return True


def _today_focus_candidate_score(row: _SectionRow) -> float:
    c = row.candidate or {}
    story_type = _candidate_story_type(c)
    tier = _candidate_publish_tier(c)
    blob = _row_blob(row)
    score = _section_priority_score(c, TODAY_FOCUS_SECTION, row.line) if c else row.score
    if tier == "must_include":
        score += 25
    elif tier == "strong":
        score += 14
    score += {
        "public_safety_after_incident": 40,
        "service_accountability": 34,
        "local_service_change": 28,
        "planning": 24,
        "civic": 20,
        "local_cost": 24,
        "incident": 10,
    }.get(story_type, 0)
    if _TODAY_FOCUS_ROAD_RE.search(blob):
        score += 16
    if re.search(r"\b(?:flood|water|electric|power|damage|thousands?|compensation|reopen)\b", blob, re.IGNORECASE):
        score += 55
    if re.search(r"\b(?:licen[cs]e|council|ofsted|cqc|safety|warning|flood|power|water|closed|closure)\b", blob, re.IGNORECASE):
        score += 14
    if _TODAY_FOCUS_SOFT_RE.search(blob):
        score -= 45
    if _FRESH_COMMERCIAL_PR_RE.search(blob):
        score -= 28
    return score


def _fresh_related_story_key(row: _SectionRow) -> str:
    blob = _row_blob(row).lower()
    if "co-op academy" in blob and re.search(r"\b(?:stab|knife|attack)\b", blob):
        return "incident:co_op_academy_stabbing"
    if "market street" in blob and "droylsden" in blob and "licen" in blob:
        return "civic:droylsden_market_street_licence"
    return ""


_FRESH_DUPLICATE_STOPWORDS = {
    "about", "after", "again", "also", "amid", "been", "before", "being",
    "city", "could", "from", "greater", "have", "into", "latest", "local",
    "manchester", "news", "over", "said", "says", "source", "that", "their",
    "there", "this", "through", "with", "would",
    # Generic crime/news words are not enough to prove two stories are the
    # same; names, places, dates, offences and outcomes must carry the match.
    "arrest", "arrested", "charge", "charged", "court", "gmp", "police",
    "sentenced", "statement", "update",
    "большого", "большой", "манчестер", "манчестера", "новости", "полиция",
    "сегодня", "суд", "суда",
}


def _fresh_duplicate_tokens(row: _SectionRow) -> set[str]:
    c = row.candidate or {}
    parts = [
        row.title,
        row.line,
        str(c.get("title") or ""),
        str(c.get("summary") or ""),
        str(c.get("lead") or ""),
        str(c.get("source_url") or ""),
    ]
    story_frame = c.get("story_frame") if isinstance(c.get("story_frame"), dict) else {}
    parts.extend(str(story_frame.get(k) or "") for k in ("what_happened", "where_exact", "when", "who_affected", "why_now"))
    text = html.unescape(" ".join(parts)).lower()
    text = re.sub(r"<[^>]+>", " ", text)
    replacements = {
        "bombing": "bomb",
        "bombed": "bomb",
        "investigation": "investigate",
        "investigating": "investigate",
        "investigated": "investigate",
        "licence": "license",
        "licensed": "license",
        "licensing": "license",
        "расследование": "investigate",
        "расследования": "investigate",
        "взрыва": "bomb",
        "взрыв": "bomb",
        "ира": "ira",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    raw = re.findall(r"[a-zа-яё0-9][a-zа-яё0-9'-]*", text, flags=re.IGNORECASE)
    tokens: set[str] = set()
    for token in raw:
        token = token.strip("-'")
        if not token or token in _FRESH_DUPLICATE_STOPWORDS:
            continue
        if token.isdigit() or token in {"ira", "m6", "m56", "m60", "m62"} or len(token) >= 4:
            tokens.add(token)
    return tokens


def _fresh_story_cluster_key(row: _SectionRow) -> str:
    c = row.candidate or {}
    cluster = c.get("story_cluster") if isinstance(c.get("story_cluster"), dict) else {}
    for field in ("cluster_key", "semantic_key", "story_key"):
        value = _normalize_text_key(str(cluster.get(field) or ""))
        if len(value) >= 12 and value not in {"none", "unknown"}:
            return value
    contract = c.get("editorial_contract") if isinstance(c.get("editorial_contract"), dict) else {}
    topic = _normalize_text_key(str(contract.get("topic_key") or c.get("topic_key") or ""))
    if len(topic) >= 18 and not topic.startswith(("fresh ", "news ")):
        return topic
    return ""


def _fresh_rows_are_same_story(left: _SectionRow, right: _SectionRow) -> bool:
    if left.fingerprint and right.fingerprint and left.fingerprint == right.fingerprint:
        return True
    left_related = _fresh_related_story_key(left)
    if left_related and left_related == _fresh_related_story_key(right):
        return True
    left_cluster = _fresh_story_cluster_key(left)
    if left_cluster and left_cluster == _fresh_story_cluster_key(right):
        return True

    left_tokens = _fresh_duplicate_tokens(left)
    right_tokens = _fresh_duplicate_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    common = left_tokens & right_tokens
    if not common:
        return False
    if {"1996", "ira", "bomb"} <= common or {"1996", "ira", "investigate"} <= common:
        return True

    strong_common = {
        token for token in common
        if token.isdigit() or len(token) >= 6 or token in {"ira", "m6", "m56", "m60", "m62"}
    }
    if len(strong_common) < 2:
        return False
    union = left_tokens | right_tokens
    jaccard = len(common) / max(len(union), 1)
    overlap = len(common) / max(min(len(left_tokens), len(right_tokens)), 1)
    return (len(common) >= 5 and overlap >= 0.62) or (len(common) >= 4 and jaccard >= 0.46)


def _fresh_duplicate_preference_score(row: _SectionRow) -> float:
    c = row.candidate or {}
    evidence_size = sum(
        len(str(c.get(field) or ""))
        for field in ("summary", "lead", "evidence_text", "draft_line")
    )
    category = str(c.get("category") or "")
    return (
        _fresh_news_score(row)
        + source_score(row.source, category) * 4
        + min(evidence_size, 2500) / 250.0
    )


def _apply_fresh_semantic_duplicate_pass(rows: list[_SectionRow]) -> tuple[list[_SectionRow], list[dict[str, str]]]:
    """Final same-story pass for top news.

    URL dedupe runs earlier, but the public issue can still receive the same
    story from BBC/MEN/About as separate links after enrichment. This pass uses
    the already-written row plus the candidate's facts to keep the best public
    card and leave room for another Fresh item.
    """
    kept: list[_SectionRow] = []
    suppressed: list[dict[str, str]] = []
    for row in rows:
        duplicate_idx: int | None = None
        for idx, current in enumerate(kept):
            if _fresh_rows_are_same_story(row, current):
                duplicate_idx = idx
                break
        if duplicate_idx is None:
            kept.append(row)
            continue

        current = kept[duplicate_idx]
        if _fresh_duplicate_preference_score(row) > _fresh_duplicate_preference_score(current):
            kept[duplicate_idx] = row
            loser = current
            winner = row
        else:
            loser = row
            winner = current
        if loser.candidate is not None:
            loser.candidate["writer_suppressed_from_top_news"] = "fresh_semantic_duplicate"
        suppressed.append(
            {
                "title": loser.title,
                "kept_title": winner.title,
                "source_label": loser.source,
                "kept_source_label": winner.source,
                "reason": "fresh_semantic_duplicate",
            }
        )
    return kept, suppressed


def _fresh_news_score(row: _SectionRow) -> float:
    c = row.candidate or {}
    story_type = _candidate_story_type(c)
    tier = _candidate_publish_tier(c)
    blob = _row_blob(row)
    score = _section_priority_score(c, "Свежие новости", row.line) if c else row.score
    if tier == "must_include":
        score += 16
    elif tier == "strong":
        score += 9
    score += {
        "public_safety_after_incident": 46,
        "service_accountability": 32,
        "incident": 28,
        "local_service_change": 18,
        "planning": 14,
        "civic": 12,
        "local_cost": 10,
    }.get(story_type, 0)
    if re.search(r"\b(?:stab|knife|killed|death|died|serious|child|school|court|sentenced|charged|arrested|collision|crash|fire|robbery|assault|gmp|police)\b", blob, re.IGNORECASE):
        score += 18
    if _FRESH_SIDEBAR_RE.search(blob):
        score -= 24
    if story_type in {"human_interest", "soft_news", "research", "opening"}:
        score -= 55
    if _FRESH_COMMERCIAL_PR_RE.search(blob):
        score -= 42
    if _TODAY_FOCUS_SOFT_RE.search(blob):
        score -= 14
    return score


def _related_story_preference_score(row: _SectionRow) -> float:
    score = _fresh_news_score(row)
    if row.section == "Главная история дня":
        score += 1000
    if _FRESH_SIDEBAR_RE.search(_row_blob(row)):
        score -= 100
    return score


def _fresh_hard_news_can_bypass_source_cap(candidate: dict | None, line: str) -> bool:
    if not isinstance(candidate, dict):
        return False
    story_type = _candidate_story_type(candidate)
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    )
    blob = f"{blob} {line}"
    if _FRESH_COMMERCIAL_PR_RE.search(blob):
        return False
    if story_type in {"incident", "public_safety_after_incident", "service_accountability", "local_service_change"}:
        return True
    return bool(re.search(r"\b(?:stab|knife|killed|death|died|court|sentenced|charged|arrested|collision|crash|robbery|assault|gmp|police)\b", blob, re.IGNORECASE))


def _section_rows(
    section_name: str,
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
    candidate_by_fp: dict[str, dict],
) -> list[_SectionRow]:
    rows: list[_SectionRow] = []
    lines = sections.get(section_name) or []
    srcs = section_sources.get(section_name) or []
    scores = section_scores.get(section_name) or []
    fps = section_fingerprints.get(section_name) or []
    titles = section_titles.get(section_name) or []
    for idx, line in enumerate(lines):
        fp = fps[idx] if idx < len(fps) else ""
        rows.append(
            _SectionRow(
                section=section_name,
                line=line,
                source=srcs[idx] if idx < len(srcs) else "",
                score=float(scores[idx] if idx < len(scores) else 0.0),
                fingerprint=fp,
                title=titles[idx] if idx < len(titles) else "",
                candidate=candidate_by_fp.get(str(fp or "")),
            )
        )
    return rows


def _set_section_rows(
    section_name: str,
    rows: list[_SectionRow],
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
) -> None:
    sections[section_name] = [row.line for row in rows]
    section_sources[section_name] = [row.source for row in rows]
    section_scores[section_name] = [row.score for row in rows]
    section_fingerprints[section_name] = [row.fingerprint for row in rows]
    section_titles[section_name] = [row.title for row in rows]


def _reroute_today_focus_row(row: _SectionRow) -> str:
    c = row.candidate or {}
    category = str(c.get("category") or "")
    if category == "football":
        return "Футбол"
    if category in {"venues_tickets", "culture_weekly", "russian_speaking_events", "diaspora_events"}:
        return "Что важно в ближайшие 7 дней"
    return "Городской радар"


def _append_section_row(
    section_name: str,
    row: _SectionRow,
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
) -> None:
    sections.setdefault(section_name, []).append(row.line)
    section_sources.setdefault(section_name, []).append(row.source)
    section_scores.setdefault(section_name, []).append(row.score)
    section_fingerprints.setdefault(section_name, []).append(row.fingerprint)
    section_titles.setdefault(section_name, []).append(row.title)


def _allocate_fresh_and_today_focus(
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
    candidate_by_fp: dict[str, dict],
) -> dict[str, object]:
    """Editor board for the two top news blocks.

    Fresh answers "what new happened". Today Focus answers "what should a
    resident account for today". This runs after lines are written but before
    caps/budget, so it can move a good already-written story instead of
    asking a model to rewrite anything.
    """
    fresh_rows = _section_rows("Свежие новости", sections, section_sources, section_scores, section_fingerprints, section_titles, candidate_by_fp)
    today_rows = _section_rows(TODAY_FOCUS_SECTION, sections, section_sources, section_scores, section_fingerprints, section_titles, candidate_by_fp)
    city_rows = _section_rows("Городской радар", sections, section_sources, section_scores, section_fingerprints, section_titles, candidate_by_fp)
    lead_rows = _section_rows("Главная история дня", sections, section_sources, section_scores, section_fingerprints, section_titles, candidate_by_fp)

    all_rows = fresh_rows + today_rows + city_rows
    seen_fps = {row.fingerprint for row in all_rows if row.fingerprint}
    # Some useful rows can exist as included+draft_line but only be pulled by a
    # later floor. Put them on the board now, while section assignment is still
    # editable.
    for candidate in candidate_by_fp.values():
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        fp = str(candidate.get("fingerprint") or "")
        if not fp or fp in seen_fps:
            continue
        block = str(candidate.get("primary_block") or "")
        section_name = PRIMARY_BLOCKS.get(block)
        if section_name not in _TODAY_FOCUS_BOARD_SOURCE_SECTIONS:
            continue
        line = str(candidate.get("draft_line") or "").strip()
        if not line:
            continue
        if not line.startswith("• "):
            line = f"• {line}"
        row = _SectionRow(
            section=section_name,
            line=line,
            source=str(candidate.get("source_label") or ""),
            score=_section_priority_score(candidate, section_name, line),
            fingerprint=fp,
            title=str(candidate.get("title") or ""),
            candidate=candidate,
        )
        all_rows.append(row)
        seen_fps.add(fp)

    for row in all_rows + lead_rows:
        if row.candidate:
            attach_editorial_contract(row.candidate)

    suppressed_sidebars: list[dict[str, str]] = []
    related_best: dict[str, _SectionRow] = {}
    related_members: dict[str, list[_SectionRow]] = {}
    for row in all_rows + lead_rows:
        key = _fresh_related_story_key(row)
        if not key:
            continue
        related_members.setdefault(key, []).append(row)
        current = related_best.get(key)
        if current is None or _related_story_preference_score(row) > _related_story_preference_score(current):
            related_best[key] = row
    suppressed_fps: set[str] = set()
    for key, members in related_members.items():
        keeper = related_best.get(key)
        for row in members:
            if keeper and row.fingerprint != keeper.fingerprint:
                if row in all_rows:
                    suppressed_fps.add(row.fingerprint)
                    if row.candidate is not None:
                        row.candidate["writer_suppressed_from_top_news"] = "related_story_sidebar"
                    suppressed_sidebars.append(
                        {
                            "title": row.title,
                            "kept_title": keeper.title,
                            "reason": "related_story_sidebar",
                        }
                    )
    if suppressed_fps:
        all_rows = [row for row in all_rows if row.fingerprint not in suppressed_fps]

    original_section_by_fp = {
        row.fingerprint: row.section
        for row in all_rows
        if row.fingerprint
    }

    today_candidates = [
        row for row in all_rows
        if _today_focus_candidate_is_eligible(row.candidate, row.line)
    ]
    for row in today_candidates:
        row.score = _today_focus_candidate_score(row)
    today_candidates.sort(key=lambda row: row.score, reverse=True)

    selected_today: list[_SectionRow] = []
    selected_fps: set[str] = set()
    bucket_counts: dict[str, int] = {}

    def take_today(row: _SectionRow) -> bool:
        if row.fingerprint in selected_fps:
            return False
        bucket = _today_focus_bucket(row)
        if bucket == "incident" and bucket_counts.get(bucket, 0) >= 1:
            return False
        if bucket == "safety" and _TODAY_FOCUS_ROAD_RE.search(_row_blob(row)) and bucket_counts.get("road", 0) >= 1:
            return False
        selected_today.append(row)
        selected_fps.add(row.fingerprint)
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        if bucket == "safety" and _TODAY_FOCUS_ROAD_RE.search(_row_blob(row)):
            bucket_counts["road"] = bucket_counts.get("road", 0) + 1
        return True

    # Protect at least one service/civic and one safety/disruption item when
    # available. Then fill by score.
    for wanted in ({"service", "civic"}, {"safety"}):
        if len(selected_today) >= TODAY_FOCUS_TARGET_ITEMS:
            break
        for row in today_candidates:
            if _today_focus_bucket(row) in wanted and take_today(row):
                break
    for row in today_candidates:
        if len(selected_today) >= TODAY_FOCUS_TARGET_ITEMS:
            break
        take_today(row)

    today_fps = {row.fingerprint for row in selected_today if row.fingerprint}
    remaining_rows = [row for row in all_rows if row.fingerprint not in today_fps]

    # If multiple Fresh rows are the same underlying incident, keep the direct
    # fact over reaction/sidebar coverage.
    best_by_key: dict[str, _SectionRow] = {}
    non_fresh_board: list[_SectionRow] = []
    for row in remaining_rows:
        if row.section != "Свежие новости":
            non_fresh_board.append(row)
            continue
        row.score = _fresh_news_score(row)
        key = _fresh_related_story_key(row)
        if not key:
            best_by_key[row.fingerprint or f"row:{len(best_by_key)}"] = row
            continue
        current = best_by_key.get(key)
        if current is None or _fresh_news_score(row) > _fresh_news_score(current):
            best_by_key[key] = row
    fresh_board_rows = sorted(best_by_key.values(), key=lambda row: row.score, reverse=True)
    suppressed_fresh_commercial: list[dict[str, str]] = []
    fresh_hard_floor = SECTION_MIN_ITEMS.get("Свежие новости", 6)
    noncommercial_fresh = [
        row for row in fresh_board_rows
        if not _FRESH_COMMERCIAL_PR_RE.search(_row_blob(row))
    ]
    if len(noncommercial_fresh) >= fresh_hard_floor:
        suppressed_fresh_commercial = [
            {
                "title": row.title,
                "source_label": row.source,
                "reason": "commercial_pr_below_fresh_hard_floor",
            }
            for row in fresh_board_rows
            if row not in noncommercial_fresh
        ]
        for row in fresh_board_rows:
            if row not in noncommercial_fresh and row.candidate is not None:
                row.candidate["writer_suppressed_from_top_news"] = "commercial_pr_below_fresh_hard_floor"
        fresh_board_rows = noncommercial_fresh

    fresh_board_rows, suppressed_fresh_duplicates = _apply_fresh_semantic_duplicate_pass(fresh_board_rows)

    city_out: list[_SectionRow] = []
    rerouted_from_today: list[dict[str, str]] = []
    for row in non_fresh_board:
        if row.section == TODAY_FOCUS_SECTION:
            dest = _reroute_today_focus_row(row)
            if dest != row.section:
                rerouted_from_today.append({"title": row.title, "to_section": dest})
            row.section = dest
        if row.section == "Городской радар":
            city_out.append(row)
        elif row.section not in {"Свежие новости", TODAY_FOCUS_SECTION}:
            _append_section_row(row.section, row, sections, section_sources, section_scores, section_fingerprints, section_titles)

    for row in selected_today:
        row.section = TODAY_FOCUS_SECTION
        row.score = _today_focus_candidate_score(row)
    _set_section_rows("Свежие новости", fresh_board_rows, sections, section_sources, section_scores, section_fingerprints, section_titles)
    _set_section_rows(TODAY_FOCUS_SECTION, selected_today, sections, section_sources, section_scores, section_fingerprints, section_titles)
    # Rank the radar by news value (like «Свежие новости»), not arrival order,
    # so the strongest local story leads instead of whatever scored high on the
    # generic board (a charity ultramarathon led the radar on 2026-06-10).
    city_out.sort(
        key=lambda row: _section_priority_score(row.candidate or {}, "Городской радар", row.line),
        reverse=True,
    )
    _set_section_rows("Городской радар", city_out, sections, section_sources, section_scores, section_fingerprints, section_titles)

    return {
        "target_items": TODAY_FOCUS_TARGET_ITEMS,
        "hard_floor": SECTION_MIN_ITEMS.get(TODAY_FOCUS_SECTION, 3),
        "input_candidates": len(all_rows),
        "eligible_candidates": len(today_candidates),
        "rendered_candidates": len(selected_today),
        "moved_from_fresh": sum(1 for row in selected_today if row.section == TODAY_FOCUS_SECTION and row.fingerprint in {r.fingerprint for r in fresh_rows}),
        "moved_from_city_watch": sum(1 for row in selected_today if row.fingerprint in {r.fingerprint for r in city_rows}),
        "kept_existing_today_focus": sum(1 for row in selected_today if row.fingerprint in {r.fingerprint for r in today_rows}),
        "rerouted_from_today_focus": rerouted_from_today,
        "suppressed_related_sidebars": suppressed_sidebars,
        "suppressed_fresh_commercial": suppressed_fresh_commercial,
        "suppressed_fresh_duplicates": suppressed_fresh_duplicates,
        "underflow_reason": "" if len(selected_today) >= SECTION_MIN_ITEMS.get(TODAY_FOCUS_SECTION, 3) else "not_enough_eligible_practical_items",
        "selected": [
            {
                "title": row.title,
                "from_section": original_section_by_fp.get(row.fingerprint, row.section),
                "source_label": row.source,
                "score": row.score,
                "story_type": _candidate_story_type(row.candidate),
                "bucket": _today_focus_bucket(row),
            }
            for row in selected_today
        ],
        "fresh_selected_preview": [
            {
                "title": row.title,
                "source_label": row.source,
                "score": row.score,
                "story_type": _candidate_story_type(row.candidate),
            }
            for row in fresh_board_rows[:SECTION_MAX_ITEMS.get("Свежие новости", 9)]
        ],
    }


def _backfill_today_focus(
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
    source_sections: tuple[str, ...] = TODAY_FOCUS_BACKFILL_SECTIONS,
) -> int:
    if sections.get(TODAY_FOCUS_SECTION):
        return 0

    moved = 0
    sections.setdefault(TODAY_FOCUS_SECTION, [])
    section_sources.setdefault(TODAY_FOCUS_SECTION, [])
    section_scores.setdefault(TODAY_FOCUS_SECTION, [])
    section_fingerprints.setdefault(TODAY_FOCUS_SECTION, [])
    section_titles.setdefault(TODAY_FOCUS_SECTION, [])

    for source_section in source_sections:
        lines = sections.get(source_section) or []
        sources = section_sources.get(source_section) or []
        scores = section_scores.get(source_section) or []
        fingerprints = section_fingerprints.get(source_section) or []
        titles = section_titles.get(source_section) or []
        if scores:
            ranked = sorted(
                zip(
                    lines,
                    sources + [""] * (len(lines) - len(sources)),
                    scores + [0.0] * (len(lines) - len(scores)),
                    fingerprints + [""] * (len(lines) - len(fingerprints)),
                    titles + [""] * (len(lines) - len(titles)),
                ),
                key=lambda item: item[2],
                reverse=True,
            )
            lines = [item[0] for item in ranked]
            sources = [item[1] for item in ranked]
            scores = [item[2] for item in ranked]
            fingerprints = [item[3] for item in ranked]
            titles = [item[4] for item in ranked]
        min_remaining = TODAY_FOCUS_MIN_SOURCE_REMAINING.get(source_section, 0)
        while lines and moved < TODAY_FOCUS_BACKFILL_TARGET and len(lines) > min_remaining:
            if scores and scores[0] < TODAY_FOCUS_BACKFILL_MIN_SCORE:
                break
            sections[TODAY_FOCUS_SECTION].append(lines.pop(0))
            section_sources[TODAY_FOCUS_SECTION].append(sources.pop(0) if sources else "")
            section_scores[TODAY_FOCUS_SECTION].append(scores.pop(0) if scores else 0.0)
            section_fingerprints[TODAY_FOCUS_SECTION].append(fingerprints.pop(0) if fingerprints else "")
            section_titles[TODAY_FOCUS_SECTION].append(titles.pop(0) if titles else "")
            moved += 1
        sections[source_section] = lines
        section_sources[source_section] = sources
        section_scores[source_section] = scores
        section_fingerprints[source_section] = fingerprints
        section_titles[source_section] = titles
        if moved >= TODAY_FOCUS_BACKFILL_TARGET:
            break

    if not sections.get(TODAY_FOCUS_SECTION):
        sections.pop(TODAY_FOCUS_SECTION, None)
        section_sources.pop(TODAY_FOCUS_SECTION, None)
        section_scores.pop(TODAY_FOCUS_SECTION, None)
        section_fingerprints.pop(TODAY_FOCUS_SECTION, None)
        section_titles.pop(TODAY_FOCUS_SECTION, None)
    return moved


def _contract_public_drop_reason(candidate: dict) -> str:
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    tier = str(contract.get("publish_tier") or candidate.get("publish_tier") or "")
    event_shape = str(contract.get("event_shape") or candidate.get("event_shape") or "")
    reject_reason = str(contract.get("reject_reason") or "")
    if reject_reason:
        return f"editorial_contract:{reject_reason}"
    if block == "transport" and str(candidate.get("transport_mode") or "") == "road":
        return "road_only_transport"
    if (
        tier == "filler"
        and block in {"last_24h", "today_focus", "city_watch"}
        and category in {"media_layer", "council", "public_services", "gmp", "city_news"}
    ):
        return "editorial_filler"
    if (
        tier == "optional"
        and block in {"last_24h", "today_focus"}
        and category in {"media_layer", "gmp", "city_news"}
    ):
        return "optional_news_in_top_section"
    if event_shape == "bookable_activity" and (
        block == "weekend_activities"
        or (
            block == "next_7_days"
            and "designmynight" in str(candidate.get("source_label") or "").lower()
        )
    ):
        return "bookable_activity_filler"
    return ""


def _classify_drop_bucket(item: dict) -> str:
    """Sort a dropped candidate into failure / quarantine / reserve.

    The release report previously lumped every non-rendered candidate into a
    single "dropped N" number that read as panic. The three buckets carry very
    different meaning: a *failure* is a production fault we must fix; a
    *quarantine* is a deliberate editorial hold; a *reserve* item is good and
    simply over budget / out of window, and rotates in on a later day.
    """
    reasons = " ".join(str(r).lower() for r in (item.get("reasons") or []))
    category = str(item.get("category") or "")
    if "weekend window" in reasons or "expired event" in reasons:
        return "reserve"
    if "missing draft_line" in reasons or "untranslated" in reasons or "passthrough" in reasons:
        # Structured categories hold an incomplete card for review; news
        # categories losing their draft_line is a genuine writer failure.
        return "quarantine" if category in {"venues_tickets", "transport"} else "failure"
    # Everything else (borderline holds, editorial-contract drops) is an
    # intentional editorial quarantine, not a fault.
    return "quarantine"


def _quality_count_key_for_drop(item: dict) -> str:
    reasons = " ".join(str(r).lower() for r in (item.get("reasons") or []))
    if "missing draft_line" in reasons:
        return "dropped_missing_draft_line"
    if "ticket not selected" in reasons:
        return "dropped_ticket_not_selected"
    if "untranslated" in reasons or "passthrough" in reasons:
        return "dropped_english_passthrough"
    if "held for manual review" in reasons or "borderline" in reasons:
        return "held_for_editorial_quality"
    return "dropped_low_quality"


def _reconcile_rendered_dropped_candidates(
    dropped_candidates: list[dict[str, object]],
    quality_counts: dict[str, int],
    rendered_fingerprints: set[str],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Remove contradictions after late recovery/top-up.

    A candidate can fail an early public-line check, then be recovered by the
    section top-up or replacement layer. The support report must describe the
    final public issue, so a rendered fingerprint cannot remain in
    dropped_candidates.
    """
    remaining: list[dict[str, object]] = []
    reconciled: list[dict[str, object]] = []
    for item in dropped_candidates:
        fp = str(item.get("fingerprint") or "")
        if fp and fp in rendered_fingerprints:
            reconciled.append(item)
            key = _quality_count_key_for_drop(item)
            if key in quality_counts:
                quality_counts[key] = max(0, int(quality_counts.get(key) or 0) - 1)
            continue
        remaining.append(item)
    return remaining, reconciled


# The main Ticket Radar now counts toward the 45-item issue budget so a quiet
# news day can't bloat the issue (2026-05-31 shipped 69 because tickets were
# exempt). It is ordered last and has a reserved minimum (2), so it is trimmed
# last by reader-value rather than dropped entirely. The small diaspora rails
# stay exempt — they serve a distinct audience and are short by nature.
_PUBLIC_BUDGET_EXEMPT_SECTIONS = {
    "Крупные концерты вне GM",
    "Русскоязычные концерты и стендап UK",
}

_MARKET_EVENT_RE = re.compile(
    r"\b(?:car\s*boot|market|markets|makers\s+market|farmer'?s\s+market|"
    r"farmers\s+market|flea\s+market|vintage\s+market|food\s+market|flower\s+festival)\b",
    re.IGNORECASE,
)


def _is_market_or_recurring_event(candidate: dict) -> bool:
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "source_label")
    )
    if str(protected.get("lane") or "") in {"weekend_market", "recurring_market"}:
        return True
    if str(contract.get("event_shape") or candidate.get("event_shape") or "") == "recurring" and _MARKET_EVENT_RE.search(text):
        return True
    return bool(event.get("is_recurring") and _MARKET_EVENT_RE.search(text))


def _is_public_budget_exempt(section_name: str, candidate: dict | None) -> bool:
    if section_name in _PUBLIC_BUDGET_EXEMPT_SECTIONS:
        return True
    if not isinstance(candidate, dict):
        return False
    # venues_tickets no longer gets a blanket budget pass: the main Ticket
    # Radar must count toward the 45-item issue budget. Evergreen markets /
    # recurring drop-ins stay exempt (they answer "what can I do this weekend"
    # and should survive a noisy news morning).
    return _is_market_or_recurring_event(candidate)


def _slice_counting_only_non_exempt(
    *,
    lines: list[str],
    srcs: list[str],
    fps: list[str],
    scores: list[float],
    titles: list[str],
    candidate_by_fp: dict[str, dict],
    section_name: str,
    counted_limit: int,
    ignore_section_exemption: bool = False,
) -> tuple[list[str], list[str], list[str], list[float], list[str], list[int], int]:
    kept_idx: list[int] = []
    dropped_idx: list[int] = []
    counted_kept = 0
    for idx, fp in enumerate(fps):
        candidate = candidate_by_fp.get(str(fp or ""))
        # The blanket section exemption (e.g. "Крупные концерты вне GM") means
        # "do not eat the global 45-item budget" — it must NOT also disable the
        # section's own SECTION_MAX_ITEMS cap, or the section grows without
        # bound (25 out-of-GM concerts on 2026-06-04). For the per-section cap
        # only the per-candidate market/recurring pass applies.
        if ignore_section_exemption:
            # Weekend markets/fairs should rank first, but still count toward
            # the weekend section cap. Otherwise a market-heavy Saturday can
            # grow without bound and crowd out the rest of the issue.
            if section_name == "Выходные в GM":
                exempt = False
            else:
                exempt = _is_market_or_recurring_event(candidate) if isinstance(candidate, dict) else False
        else:
            exempt = _is_public_budget_exempt(section_name, candidate)
        if exempt or counted_kept < counted_limit:
            kept_idx.append(idx)
            if not exempt:
                counted_kept += 1
        else:
            dropped_idx.append(idx)
    return (
        [lines[i] for i in kept_idx],
        [srcs[i] if i < len(srcs) else "" for i in kept_idx],
        [fps[i] if i < len(fps) else "" for i in kept_idx],
        [scores[i] if i < len(scores) else 0.0 for i in kept_idx],
        [titles[i] if i < len(titles) else "" for i in kept_idx],
        dropped_idx,
        counted_kept,
    )


def _reserved_later_budget(
    ordered_sections: list[str],
    current_index: int,
    sections: dict[str, list[str]],
) -> int:
    reserved = 0
    for later_section in ordered_sections[current_index + 1:]:
        minimum = PUBLIC_SECTION_RESERVED_MIN.get(later_section, 0)
        if minimum <= 0:
            continue
        available = len(sections.get(later_section) or [])
        if available:
            reserved += min(minimum, available)
    return reserved


def _contains_cyrillic(value: str) -> bool:
    return bool(re.search(r"[а-яё]", str(value or ""), flags=re.IGNORECASE))


def _looks_like_untranslated_english(value: str) -> bool:
    text = str(value or "").strip()
    if not text or _contains_cyrillic(text):
        return False
    latin_words = re.findall(r"[A-Za-z][A-Za-z'’-]+", text)
    if len(latin_words) < 8:
        return False
    stopwords = {
        "the", "and", "for", "with", "from", "after", "following", "into", "across",
        "will", "have", "has", "had", "that", "this", "they", "their", "about", "said",
        "says", "into", "over", "under", "following", "response", "operators",
    }
    stopword_hits = sum(1 for word in latin_words if word.lower() in stopwords)
    return stopword_hits >= 2


def _llm_rewrite_is_degraded(state_dir: Path) -> tuple[bool, dict]:
    report = read_json(state_dir / "llm_rewrite_report.json", {})
    if not isinstance(report, dict):
        return False, {}
    # Trust llm_rewrite stage_status: it is set to "degraded" only when
    # yield falls below 90%. Editorial soft warnings (weak draft_line,
    # repair-pass rejections) are reported via soft_warnings and MUST
    # NOT trigger degraded_shrink — that was the 2026-05-27 dropper for
    # Manchester Academy ticket cards at reader_value 800+.
    status = str(report.get("stage_status") or "").strip().lower()
    degraded = status == "degraded"
    return degraded, report


def _source_anchor(source_url: str, source_label: str) -> str:
    return f'<a href="{html.escape(source_url, quote=True)}">{html.escape(source_label)}</a>'


def _attach_source_anchor(line: str, source_url: str, source_label: str) -> str:
    text = str(line or "").strip()
    if "<a " in text.lower():
        return text
    label = _public_source_label(source_label)
    label_lower = label.lower()
    # Normalise by stripping trailing punctuation before checking — handles both
    # "...Met Office" and "...Met Office." (period added by LLM or practical angle).
    if label and text.lower().rstrip(" .").endswith(label_lower):
        base = text.rstrip(" .")
        # Only strip trailing spaces (not periods) so the sentence period before
        # the label is preserved: "...зонт обязателен. Met Office" → "...зонт обязателен."
        text = base[: len(base) - len(label)].rstrip(" ")
    return f"{text} {_source_anchor(source_url, label)}".strip()


def _ensure_source_anchor_for_rendered_line(line: str, fingerprint: str, source_label: str, candidate_by_fp: dict[str, dict]) -> str:
    text = str(line or "").strip()
    if "<a " in text.lower():
        return text
    candidate = candidate_by_fp.get(str(fingerprint or "")) or {}
    source_url = str(candidate.get("source_url") or "")
    label = str(candidate.get("source_label") or source_label or "")
    if not source_url or not label:
        return text
    return _attach_source_anchor(text, source_url, label)


def _public_source_label(source_label: str) -> str:
    label = re.sub(r"\s+", " ", str(source_label or "")).strip()
    label = re.sub(r"\s+public\s+safety\s+fallback\b", "", label, flags=re.IGNORECASE)
    label = re.sub(r"\s+fallback\b", "", label, flags=re.IGNORECASE)
    return label.strip() or str(source_label or "").strip()


_SUMMER_MONTHS = frozenset({6, 7, 8})
_HEAVY_SNOW_PATTERN = re.compile(
    r"\b(?:heavy\s+snow|blizzard|snowstorm|snowfall|снегопад|метель|снежная\s+буря)\b",
    re.IGNORECASE,
)
_EXTREME_TEMP_PATTERN = re.compile(r"\b([1-9]\d)\s*°[Cc]\b")
_EVENT_BLOCKS = {"weekend_activities", "next_7_days", "ticket_radar", "outside_gm_tickets", "russian_events", "future_announcements"}
_WEEKEND_BLOCK = "weekend_activities"
_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}
_RU_MONTHS_GENITIVE = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


def _sanity_flags(candidate: dict, line: str) -> list[str]:
    flags: list[str] = []
    month = now_london().month
    if month in _SUMMER_MONTHS and _HEAVY_SNOW_PATTERN.search(line):
        flags.append("Seasonal impossibility: heavy snow in summer month.")
    for m in _EXTREME_TEMP_PATTERN.finditer(line):
        temp = int(m.group(1))
        if temp > 38 or temp < 0 and month in _SUMMER_MONTHS:
            flags.append(f"Implausible Manchester temperature: {m.group()}.")
    return flags


def _parse_day(value: object) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(now_london().tzinfo).date()
    except ValueError:
        return None


def _date_signals(text: str) -> list[date]:
    today = now_london().date()
    lowered = str(text or "").lower()
    dates: list[date] = []
    for match in re.finditer(r"\b(20\d{2})[/-](\d{1,2})[/-](\d{1,2})\b", lowered):
        year, month, day = (int(part) for part in match.groups())
        try:
            dates.append(date(year, month, day))
        except ValueError:
            continue
    for match in re.finditer(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)", lowered):
        year, month, day = (int(part) for part in match.groups())
        try:
            dates.append(date(year, month, day))
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-zа-яё]{3,9})(?:\s+(20\d{2}))?\b", lowered):
        day_raw, month_raw, year_raw = match.groups()
        month = _MONTHS.get(month_raw)
        if not month:
            continue
        year = int(year_raw) if year_raw else today.year
        try:
            dates.append(date(year, month, int(day_raw)))
        except ValueError:
            continue
    for match in re.finditer(r"\b([a-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s*,?\s*(20\d{2}))?\b", lowered):
        month_raw, day_raw, year_raw = match.groups()
        month = _MONTHS.get(month_raw)
        if not month:
            continue
        year = int(year_raw) if year_raw else today.year
        try:
            dates.append(date(year, month, int(day_raw)))
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{1,2})\s*[–-]\s*(\d{1,2})\s+([a-zа-яё]{3,9})(?:\s+(20\d{2}))?\b", lowered):
        _start_day_raw, end_day_raw, month_raw, year_raw = match.groups()
        month = _MONTHS.get(month_raw)
        if not month:
            continue
        year = int(year_raw) if year_raw else today.year
        try:
            dates.append(date(year, month, int(end_day_raw)))
        except ValueError:
            continue
    return dates


def _future_date_signal(text: str) -> bool:
    dates = _date_signals(text)
    return bool(dates and max(dates) >= now_london().date())


def _format_ru_day_month(value: datetime | None) -> str:
    if value is None:
        return ""
    return f"{value.day} {_RU_MONTHS_GENITIVE.get(value.month, '')}".strip()


def _parse_ticket_datetime(candidate: dict) -> datetime | None:
    summary = str(candidate.get("summary") or "")
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    structured = _event_structured_datetime(candidate)
    if structured:
        return structured
    # Ticket cards must use the occurrence date, not the article/collection
    # timestamp. Using published_at here made major upcoming shows render as
    # "today", then fail the structured-date QA and disappear.
    for raw in (
        event.get("date_start"),
        event.get("date"),
        candidate.get("event_date"),
        event.get("date_end"),
        candidate.get("event_end_date"),
    ):
        parsed = str(raw or "").strip()
        if not parsed:
            continue
        try:
            return datetime.fromisoformat(parsed.replace("Z", "+00:00")).astimezone(now_london().tzinfo)
        except ValueError:
            continue
    match = re.search(r"\bevent_date=(20\d{2}-\d{2}-\d{2})(?:\s+(\d{2}:\d{2}))?", summary)
    if match:
        raw = match.group(1)
        if match.group(2):
            raw = f"{raw}T{match.group(2)}:00+01:00"
        else:
            raw = f"{raw}T12:00:00+01:00"
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None
    title = str(candidate.get("title") or "")
    # Accept both "Wed 28 November 2026" and Manchester Academy's
    # "28th November 2026" (ordinal day, no weekday).
    title_match = re.search(
        r"\b(?:(?:mon|tue|wed|thu|fri|sat|sun)\w*\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\s+(20\d{2})\b",
        title,
        re.IGNORECASE,
    )
    if title_match:
        day_raw, month_raw, year_raw = title_match.groups()
        month = _MONTHS.get(month_raw.lower())
        if month:
            try:
                return datetime(int(year_raw), month, int(day_raw), 12, 0, 0)
            except ValueError:
                return None
    return None


def _ticket_headliner(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(title or "")).strip()
    cleaned = re.split(r"\s+[—-]\s+event\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    # Strip a trailing date the venue appended: "… - 28th November 2026" or
    # "… - Wed 28 November 2026" — the parsed date is rendered separately.
    cleaned = re.sub(
        r"\s*[-–]\s*(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*\s+)?\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+20\d{2}\s*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip(" -–,")
    cleaned = re.sub(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" -–,")
    return cleaned or "событие"


def _ticket_venue(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    event_venue = re.sub(r"\s+", " ", str(event.get("venue") or "")).strip()
    if event_venue and event_venue.lower() not in {"greater manchester", "manchester"}:
        return event_venue
    summary = str(candidate.get("summary") or "")
    source_label = str(candidate.get("source_label") or "").strip()
    if source_label in {"Manchester Academy", "RNCM"}:
        return source_label
    first_chunk = summary.split("|", 1)[0].strip(" .")
    first_chunk = re.sub(r"^(Manchester|Liverpool|London)\s+", "", first_chunk, flags=re.IGNORECASE).strip(" .")
    if first_chunk and len(first_chunk) >= 4 and not _looks_like_source_chrome(first_chunk):
        return first_chunk
    return source_label


def _ticket_genre(candidate: dict) -> str:
    summary = str(candidate.get("summary") or "")
    chunks = [chunk.strip(" .") for chunk in summary.split("|")]
    ignored = {
        "manchester",
        "liverpool",
        "london",
        "greater manchester",
        "united kingdom",
        "uk",
    }
    for chunk in chunks[1:4]:
        lowered = chunk.lower()
        if not chunk or lowered in ignored:
            continue
        if "=" in chunk or lowered.startswith("ticket_") or lowered == "undefined":
            continue
        if _looks_like_source_chrome(chunk):
            continue
        if re.search(r"\b(?:arena|hall|warehouse|academy|institute|studios|club|depot|apollo|ritz|theatre|stadium)\b", lowered):
            continue
        return chunk
    return ""


_TICKET_MAJOR_VENUE_RE = re.compile(
    r"\b(?:ao arena|co-?op live|etihad stadium|old trafford|wembley|the o2|o2 arena|"
    r"ovo arena wembley|royal albert hall|manchester apollo|o2 apollo|bridgewater hall|"
    r"aviva studios|factory international|castlefield bowl|albert hall|new century hall|"
    r"palace theatre|the lowry|rncm|royal northern college|manchester academy|"
    r"victoria warehouse|o2 victoria warehouse)\b",
    re.IGNORECASE,
)
_TICKET_PREFERRED_GENRE_RE = re.compile(
    r"\b(?:jazz|blues|soul|r&b|rnb|reggae|funk|folk|world|classical|hip-hop|rap)\b",
    re.IGNORECASE,
)
_TICKET_NEGATIVE_RE = re.compile(
    r"\b(?:venue premium tickets|tribute act|tribute show|stunt show|games in concert|"
    r"film with live orchestra|bottomless|party|unknown|undefined)\b",
    re.IGNORECASE,
)


def _ticket_price(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    price = re.sub(r"\s+", " ", str(event.get("price") or "")).strip()
    if not price:
        blob = " ".join(str(candidate.get(field) or "") for field in ("summary", "lead", "evidence_text"))
        prices = re.findall(r"£\s?\d+(?:\.\d{1,2})?(?:\s?[–-]\s?£?\d+(?:\.\d{1,2})?)?", blob)
        price = prices[0].replace(" ", "") if prices else ""
    if not price:
        return ""
    # Fee-not-price guard: a lone amount under ~£8 (e.g. "£4.75") is almost
    # always a per-ticket booking/transaction fee, not the ticket price. Showing
    # "цена £4.75" is worse than no price (Jason Isbell on 2026-06-04), so drop
    # it. A range ("£15–£40") keeps its top value and is left alone.
    amounts = [float(x) for x in re.findall(r"\d+(?:\.\d{1,2})?", price)]
    if amounts and max(amounts) < 8:
        return ""
    return price


def _is_diaspora_ticket(candidate: dict) -> bool:
    return (
        str(candidate.get("category") or "") in {"russian_speaking_events", "diaspora_events"}
        or str(candidate.get("primary_block") or "") == "russian_events"
        or str(candidate.get("source_label") or "") in {"Kontramarka UK", "EventFirst Diaspora", "UK Stand-Up Club", "UK Stand-Up Club Eventbrite"}
    )


def _ticket_days_to_event(candidate: dict) -> int | None:
    event_dt = _parse_ticket_datetime(candidate)
    if event_dt is None:
        return None
    return (event_dt.date() - now_london().date()).days


def _ticket_has_active_public_reason(candidate: dict) -> bool:
    ticket_type = str(candidate.get("ticket_type") or "").strip() or classify_ticket_type(candidate)
    if ticket_type in {"on_sale_now", "presale_soon", "newly_listed", "major_upcoming"}:
        return True
    days = _ticket_days_to_event(candidate)
    return days is not None and 0 <= days <= 7


def _ticket_public_mode(candidate: dict) -> str:
    ticket_type = str(candidate.get("ticket_type") or "").strip() or classify_ticket_type(candidate)
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    kind = str(notability.get("kind") or "")
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    if ticket_type in {"on_sale_now", "presale_soon", "newly_listed"}:
        return "sale_radar"
    if kind == "lineup_or_show" or re.search(r"\bline[- ]?up\s*=", blob, re.IGNORECASE):
        return "lineup_radar"
    days = _ticket_days_to_event(candidate)
    if days is not None and 0 <= days <= 14:
        return "upcoming_major_show"
    if ticket_type == "major_upcoming":
        return "upcoming_major_show"
    return "ticket_watch"


def _ticket_public_priority_score(candidate: dict) -> float:
    """Product ordering for ticket sections: fame first, ticket occasion second."""
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    tier = str(notability.get("tier") or "").upper()
    ticket_type = str(candidate.get("ticket_type") or "").strip() or classify_ticket_type(candidate)
    days = _ticket_days_to_event(candidate)
    tier_score = {"PROTECTED": 900, "A": 820, "B": 260, "C": 50, "D": -200, "UNKNOWN": -200}.get(tier, -200)
    reason_score = 0
    if ticket_type in {"on_sale_now", "presale_soon", "newly_listed"}:
        reason_score = 260
    elif ticket_type == "major_upcoming":
        reason_score = 220
    elif days is not None and 0 <= days <= 7:
        reason_score = 120
    elif days is not None and 8 <= days <= 14:
        reason_score = 80
    elif ticket_type in {"old_onsale", "old_public_sale"}:
        reason_score = -80
    freshness = 0 if days is None else max(0, 21 - max(days, 0))
    return float(reason_score + tier_score + freshness)


def _ticket_watch_score(candidate: dict) -> float:
    title = _ticket_headliner(str(candidate.get("title") or ""))
    venue = _ticket_venue(candidate)
    genre = _ticket_genre(candidate)
    source = str(candidate.get("source_label") or "")
    summary = str(candidate.get("summary") or "")
    ticket_type = str(candidate.get("ticket_type") or "").strip() or classify_ticket_type(candidate)
    blob = " ".join([title, venue, genre, source, summary]).lower()
    score = 0.0
    if _is_diaspora_ticket(candidate):
        score += 100
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    tier = str(notability.get("tier") or "").upper()
    kind = str(notability.get("kind") or "")
    if tier == "A":
        score += 115
    elif tier == "B":
        score += 82
    elif tier == "C":
        score += 18
    elif tier == "PROTECTED":
        score += 100
    # Venue and genre are supporting signals only. They must not promote an
    # unknown artist into the public radar by themselves.
    if _TICKET_MAJOR_VENUE_RE.search(venue) or _TICKET_MAJOR_VENUE_RE.search(summary):
        score += 10
    if ticket_type in {"on_sale_now", "presale_soon", "newly_listed"}:
        score += 16
    elif ticket_type in {"major_upcoming", "event_this_week"}:
        score += 8
    elif ticket_type in {"old_onsale", "old_public_sale"}:
        score -= 10
    event_dt = _parse_ticket_datetime(candidate)
    if event_dt is not None:
        days = (event_dt.date() - now_london().date()).days
        if 0 <= days <= 14:
            score += 8
        elif days > 180 and not _is_diaspora_ticket(candidate):
            score -= 12
    if _TICKET_NEGATIVE_RE.search(blob):
        score -= 35
    if kind == "non_artist_show":
        score -= 60
    if kind == "lineup_or_show" and tier not in {"A", "B", "PROTECTED"}:
        score -= 20
    if not genre:
        score -= 4
    if not venue:
        score -= 12
    return score


_TICKET_PUBLIC_THRESHOLD = 50


def _ticket_watch_decision(candidate: dict) -> dict[str, object]:
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    score = round(_ticket_watch_score(candidate), 2)
    ticket_type = str(candidate.get("ticket_type") or "").strip() or classify_ticket_type(candidate)
    tier = str(notability.get("tier") or "unknown")
    kind = str(notability.get("kind") or "unknown")
    decision = "show" if score >= _TICKET_PUBLIC_THRESHOLD else "hide"
    if _is_diaspora_ticket(candidate):
        decision = "show"
    block = str(candidate.get("primary_block") or "")
    tier_upper = tier.upper()
    active_reason = _ticket_has_active_public_reason(candidate)
    if block == "outside_gm_tickets":
        if tier_upper not in {"A", "PROTECTED"}:
            if not (tier_upper == "B" and ticket_type in {"on_sale_now", "presale_soon", "newly_listed"}):
                decision = "hide"
        if not active_reason:
            decision = "hide"
    elif block == "ticket_radar":
        if not active_reason and ticket_type in {"old_onsale", "old_public_sale"} and tier_upper not in {"A", "PROTECTED"}:
            decision = "hide"
    reasons = [part.strip() for part in _ticket_watch_reason(candidate).split(";") if part.strip()]
    if not reasons and decision == "hide":
        reasons = ["недостаточный notability-сигнал"]
    return {
        "decision": decision,
        "score": score,
        "threshold": _TICKET_PUBLIC_THRESHOLD,
        "tier": tier,
        "kind": kind,
        "signal": notability.get("signal") or "",
        "artist": notability.get("artist") or ticket_artist_name(candidate),
        "headliners": notability.get("headliners") or [],
        "signals": notability.get("signals") or {},
        "ticket_type": ticket_type,
        "ticket_mode": _ticket_public_mode(candidate),
        "source_label": candidate.get("source_label") or "",
        "primary_block": candidate.get("primary_block") or "",
        "reasons": reasons,
    }


def _ticket_watch_reason(candidate: dict) -> str:
    title = _ticket_headliner(str(candidate.get("title") or ""))
    venue = _ticket_venue(candidate)
    genre = _ticket_genre(candidate)
    summary = str(candidate.get("summary") or "")
    blob = " ".join([title, venue, genre, str(candidate.get("source_label") or ""), summary])
    reasons: list[str] = []
    if _is_diaspora_ticket(candidate):
        return "русскоязычное событие"
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    tier = str(notability.get("tier") or "").upper()
    ticket_type = str(candidate.get("ticket_type") or "").strip() or classify_ticket_type(candidate)
    days = _ticket_days_to_event(candidate)
    lineup = re.search(r"\bline[- ]?up\s*=", blob, re.IGNORECASE) or str(notability.get("kind") or "") == "lineup_or_show"
    estate_show = re.search(r"\b(?:estate|open air|open-air|castle|palace|park)\b", blob, re.IGNORECASE)
    arena_show = _TICKET_MAJOR_VENUE_RE.search(venue) or _TICKET_MAJOR_VENUE_RE.search(summary)
    if ticket_type == "presale_soon":
        return "presale скоро"
    if ticket_type in {"on_sale_now", "newly_listed"}:
        return "продажа открылась сейчас"
    if days is not None and 0 <= days <= 7:
        return "концерт на ближайшей неделе"
    if days is not None and 8 <= days <= 14:
        return "ближайшая дата крупного тура"
    if lineup:
        return "сильный фестивальный lineup"
    if estate_show:
        return "open-air концерт на estate-площадке"
    if arena_show:
        return "крупная arena/stadium дата"
    if tier == "A":
        return "крупный артист с UK-датой"
    if ticket_type == "event_this_week":
        return "концерт на этой неделе"
    if ticket_type == "major_upcoming":
        return "заметная UK-дата"
    return "билетный повод"


def _build_ticket_fallback_line(candidate: dict) -> str:
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    title = str(notability.get("artist") or "").strip() or ticket_artist_name(candidate) or _ticket_headliner(str(candidate.get("title") or ""))
    venue = _ticket_venue(candidate)
    genre = _ticket_genre(candidate)
    # Build the card from the CLEAN structured fields (event name + venue +
    # date + genre). We do NOT gate on summary/lead here: those often hold
    # page boilerplate, but the structured fields are clean, so holding the
    # whole card over a dirty summary just loses a real show. Only bail if
    # the structured parts we actually render are themselves chrome.
    if not title or _looks_like_source_chrome(" ".join([title, venue, genre])):
        return ""
    if str(candidate.get("primary_block") or "") in {"ticket_radar", "outside_gm_tickets"} and _ticket_watch_decision(candidate)["decision"] != "show":
        return ""
    reason = _ticket_watch_reason(candidate)
    price = _ticket_price(candidate)
    price_part = f"; цена {price}" if price else ""
    event_dt = _parse_ticket_datetime(candidate)
    if str(candidate.get("primary_block") or "") == "next_7_days" and event_dt:
        days_out = (event_dt.date() - now_london().date()).days
        if 0 <= days_out <= 7:
            reason = "событие на этой неделе"
    day_month = _format_ru_day_month(event_dt)
    time_part = ""
    if event_dt and event_dt.strftime("%H:%M") not in {"00:00", "12:00"}:
        time_part = f" в {event_dt.strftime('%H:%M')}"
    # #7 Same artist+venue on several nights was merged in dedupe — render the
    # whole run on one line ("10 и 11 июня") instead of repeating the card.
    merged_dates = candidate.get("merged_event_dates")
    if isinstance(merged_dates, list) and len(merged_dates) >= 2:
        parts: list[str] = []
        for iso in merged_dates:
            try:
                formatted = _format_ru_day_month(datetime.fromisoformat(str(iso)))
            except ValueError:
                formatted = ""
            if formatted and formatted not in parts:
                parts.append(formatted)
        if len(parts) >= 2:
            months = {p.split()[-1] for p in parts}
            if len(months) == 1:
                days = [p.split()[0] for p in parts]
                day_month = f"{', '.join(days[:-1])} и {days[-1]} {next(iter(months))}"
            else:
                day_month = f"{', '.join(parts[:-1])} и {parts[-1]}"
            time_part = ""  # multiple nights — a single start time would mislead
    genre_part = f" ({genre})" if genre else ""
    reason_part = f" {reason[:1].upper()}{reason[1:]}." if reason else ""
    if day_month and venue:
        return f"• {title} — {day_month}{time_part}, {venue}{genre_part}{price_part}.{reason_part}"
    if day_month:
        return f"• {title} — {day_month}{time_part}{genre_part}{price_part}.{reason_part}"
    if venue:
        return f"• {title} — {venue}{genre_part}{price_part}.{reason_part}"
    return f"• {title}{genre_part}{price_part}.{reason_part}"


def _looks_like_source_chrome(value: str) -> bool:
    text = str(value or "").lower()
    return any(
        marker in text
        for marker in (
            "this website makes extensive use of javascript",
            "browser settings",
            "once selected, tickets will be reserved",
            "enable javascript",
        )
    )


def _build_transport_fallback_line(candidate: dict) -> str:
    """Recover a location-bearing transport line when the rewrite produced
    nothing. We do NOT hold the alert silently: the stop/area is almost
    always recoverable from the TfGM alert URL slug (…/piccadilly-gardens-…)
    or the title head, and the reason from the title text. Telling the
    reader WHERE is the hard editorial rule — a held card breaks it."""
    from news_digest.pipeline.transport_fill import _location_from_tfgm_slug  # noqa: PLC0415
    from news_digest.pipeline.transport_card import _translate_reason  # noqa: PLC0415

    title = re.sub(r"\s+", " ", str(candidate.get("title") or "")).strip()
    url = str(candidate.get("source_url") or "")
    location = _location_from_tfgm_slug(url)
    if not location:
        head = re.split(r"\s+[-–|]\s+", title, maxsplit=1)[0].strip()
        if head and len(head) <= 60 and not _looks_like_source_chrome(head):
            location = head
    if not location:
        return ""
    reason = _translate_reason(title) or "ограничения движения"
    operator = str(candidate.get("source_label") or "").strip() or "TfGM"
    lowered = title.lower()
    if "lift out of service" in lowered:
        return (
            f"• {operator}: лифт не работает на остановке {location}. "
            "Если вам нужен безбарьерный доступ, проверьте альтернативную остановку или маршрут перед выходом."
        )
    if "improvement works" in lowered or "tram stop" in lowered:
        return (
            f"• {operator}: на остановке {location} идут работы. "
            "Если едете через неё сегодня, проверьте страницу TfGM перед выходом."
        )
    return (
        f"• {operator}: {reason} — {location}. "
        "Сроки и объёмы работ уточняйте на странице перевозчика."
    )


def _build_football_fallback_line(candidate: dict) -> str:
    source = str(candidate.get("source_label") or "")
    if source not in {
        "Manchester United",
        "Manchester City",
        "Manchester City Men",
        "BBC Sport Manchester United",
        "BBC Sport Manchester City",
    }:
        return ""
    if not _football_is_sport_news(candidate):
        return ""
    title = re.sub(r"\s+", " ", str(candidate.get("title") or "")).strip()
    summary = re.sub(r"\s+", " ", str(candidate.get("summary") or candidate.get("lead") or "")).strip()
    if not title or _looks_like_source_chrome(title):
        return ""
    blob = f"{source} {title} {summary}".lower()
    club = "Manchester United" if "united" in blob or "man utd" in blob else "Manchester City"
    if re.search(r"\bfixture|fixtures|calendar|schedule\b", blob):
        return (
            f"• {club}: скоро объявят календарь Премьер-лиги на новый сезон. "
            "Это задаст первые матчи, даты и ранние выезды, за которыми стоит следить болельщикам."
        )
    if re.search(r"\binjur|fitness|ruled out|available\b", blob):
        subject = re.split(r"\b(?:picks up|injur|fitness|ruled out|available)\b", title, maxsplit=1, flags=re.IGNORECASE)[0]
        subject = re.sub(r"[:\-–]\s*$", "", subject).strip() or "игроку"
        return (
            f"• {club}: обновление по травме или готовности — {subject}. "
            "Это важно для состава на ближайшие матчи."
        )
    if re.search(r"\btransfer|sign(?:s|ed|ing)?|loan|bid|fee|deal\b", blob):
        subject = re.split(r"[:\-–]", title, maxsplit=1)[0].strip() or "игроку"
        return (
            f"• {club}: трансферное обновление по {subject}. "
            "Ситуация влияет на состав и планы клуба на сезон."
        )
    if re.search(r"\bappoint|appointment|manager|coach|negotiat", blob):
        subject = re.split(r"[:\-–]", title, maxsplit=1)[0].strip() or "кандидату"
        return (
            f"• {club}: обновление по тренерскому вопросу — {subject}. "
            "Это влияет на подготовку команды к сезону."
        )
    if summary and len(summary) >= 40 and not _looks_like_source_chrome(summary):
        return f"• {club}: {title}. {summary.rstrip('.')[:220]}."
    return f"• {club}: {title}."


_EVENT_DATE_BLOCKS = {
    "next_7_days",
    "weekend_activities",
    "future_announcements",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
}

_PHASE_LABELS_RU = {
    "charged": "предъявлено обвинение",
    "sentenced": "вынесен приговор",
    "approved": "решение одобрено",
    "reopened": "объект снова открыт",
    "cancelled": "мероприятие отменено",
    "delayed": "сроки перенесены",
    "appeal_updated": "появилось обновление по обращению",
    "tickets_on_sale": "появился билетный повод",
    "consultation_opened": "открыта консультация",
    "consultation_closing": "подходит срок консультации",
}

_EXPLAINABLE_TERMS = {
    "ANOTR": "электронного дуэта ANOTR",
    "DJI": "бренда дронов и камер DJI",
    "PBSA": "студенческого жилья PBSA",
    "AGM": "годового собрания совета AGM",
}


def _blob_for_repair(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )


def _strip_empty_emotive_quote(line: str) -> str:
    # These family/tribute phrases can be useful in a full article, but in a
    # daily intelligence bullet they often replace the actual update.
    return re.sub(
        r"\s*;?\s*(?:родственники|семья|близкие)\s+заявил[аи]?,?\s+что\s+[^.]{0,120}«(?:ушла слишком рано|уш[её]л слишком рано)»\.?",
        "",
        line,
        flags=re.IGNORECASE,
    ).strip()


def _repair_incident_and_legal_russian(line: str) -> tuple[str, list[str]]:
    repaired = str(line or "")
    reasons: list[str] = []
    replacements = (
        (r"отдельн\w*\s+ножев\w*\s+атак\w*", "двух разных нападений с ножом", "separate_stabbings_ru"),
        (r"тройн\w*\s+ножев\w*\s+ранени\w*", "нападение с ножом, в котором пострадали трое", "triple_stabbing_ru"),
        (r"следствие\s+пришло\s+к\s+открыт\w*\s+вывод\w*", "коронер не смог установить точную причину смерти", "open_conclusion_ru"),
        (r"открыт\w*\s+вывод\w*", "открытое заключение коронера: точную причину не установили", "open_conclusion_ru"),
        (r"\bmanslaughter\b", "неумышленное убийство", "manslaughter_ru"),
        (r"\bPBSA\b", "студенческое жильё PBSA", "pbsa_ru"),
    )
    for pattern, repl, reason in replacements:
        updated = re.sub(pattern, repl, repaired, flags=re.IGNORECASE)
        if updated != repaired:
            repaired = updated
            reasons.append(reason)
    repaired = re.sub(r"\s+", " ", repaired).strip()
    return repaired, reasons


def _repair_follow_up_line(candidate: dict, line: str) -> tuple[str, list[str]]:
    change_type = str(candidate.get("change_type") or "")
    why_now = str(candidate.get("why_now") or "")
    phase = str(candidate.get("change_phase") or "")
    if change_type not in {"follow_up", "same_story_new_facts", "new_phase"} and why_now != "update_today":
        return line, []
    # Ticket / event lines already carry their own occasion ("на этой неделе",
    # "появился билетный повод") and a clock time like "18:30". The follow-up
    # lead is a NEWS device; on a ticket line it is redundant AND its place-
    # prefix regex matched the colon inside the time, injecting the label
    # mid-time ("18:обновление: появился билетный повод; 00") on 2026-06-04.
    if (
        phase == "tickets_on_sale"
        or str(candidate.get("category") or "") == "venues_tickets"
        or str(candidate.get("primary_block") or "") in {"ticket_radar", "outside_gm_tickets", "next_7_days", "future_announcements"}
    ):
        return line, []
    if not phase or re.search(r"^\s*•\s*(?:обновление|update)\b", line, re.IGNORECASE):
        return line, []

    blob = _blob_for_repair(candidate).lower()
    event_type = str((candidate.get("story_frame") or {}).get("event_type") or "")
    planningish = event_type in {"planning", "civic"} or re.search(r"\b(?:planning|housing|development|council|consultation|approved|homes?)\b", blob)
    courtish = event_type in {"incident", "crime", "court"} or re.search(r"\b(?:court|charged|sentenced|murder|police|trial|jury|коронер|суд)\b", blob)
    if phase in {"charged", "sentenced"} and not courtish:
        return line, []
    if phase == "charged" and re.search(r"\b(?:no criminal charges|not enough evidence to charge|will not face criminal charges)\b", blob):
        return line, []
    if phase in {"approved", "consultation_opened", "consultation_closing"} and courtish and not planningish:
        return line, []

    label = _PHASE_LABELS_RU.get(phase)
    if not label:
        # Unknown phase → the generic "появилось обновление" only produced the
        # tautological "обновление: появилось обновление;" lead (Stockport /
        # Oldham on 2026-06-04). Skip the lead unless the phase says something.
        return line, []
    repaired = _strip_empty_emotive_quote(line)
    # Keep the original place prefix if it exists: "• Rochdale: ...". The colon
    # must be a place-label colon, never a clock-time colon ("18:00"), so forbid
    # a digit immediately before it.
    match = re.match(r"^(•\s*[^:]{2,45}(?<!\d):\s*)(.+)$", repaired)
    if match:
        return f"{match.group(1)}обновление: {label}; {match.group(2)[:1].lower()}{match.group(2)[1:]}", ["follow_up_leads_with_change"]
    return f"• Обновление: {label}; {repaired.removeprefix('• ').strip()}", ["follow_up_leads_with_change"]


def _repair_explainable_terms(candidate: dict, line: str) -> tuple[str, list[str]]:
    repaired = str(line or "")
    reasons: list[str] = []
    blob = _blob_for_repair(candidate)
    for term, explanation in _EXPLAINABLE_TERMS.items():
        if term not in repaired:
            continue
        if explanation in repaired:
            continue
        # Prefer explaining terms that appear in the source material. This
        # avoids inventing meaning for random acronyms while still fixing the
        # common local-product failures the owner flagged.
        if term not in blob:
            continue
        repaired = re.sub(rf"\b{re.escape(term)}\b", explanation, repaired, count=1)
        reasons.append(f"explained_{term.lower()}")
    return repaired, reasons


def _repair_common_russian_line(line: str) -> tuple[str, list[str]]:
    repaired = str(line or "")
    reasons: list[str] = []
    replacements = (
        (r"\bКлр\.\s*", "депутат совета ", "councillor_ru"),
        (r"Greater Manchesterе\b", "Greater Manchester", "gm_case_ru"),
        (r"\bфуд-дестинаци[яюи]\b", "место с барами и едой", "food_destination_ru"),
        (r"\bкиберфлешинг[аеуом]*\b", "отправка непрошеных интимных изображений", "cyberflashing_ru"),
        (r"\bс связями\b", "со связями", "ru_preposition"),
    )
    for pattern, repl, reason in replacements:
        updated = re.sub(pattern, repl, repaired, flags=re.IGNORECASE)
        if updated != repaired:
            repaired = updated
            reasons.append(reason)
    repaired = re.sub(r"\s+", " ", repaired).strip()
    return repaired, reasons


def _event_structured_datetime(candidate: dict) -> datetime | None:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    url_dt = _bridgewater_slug_datetime(candidate)
    raw = str(event.get("date_start") or event.get("date") or "").strip()
    if not raw:
        return url_dt
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return url_dt
    if url_dt:
        today = now_london().date()
        parsed_days = (parsed.date() - today).days
        url_days = (url_dt.date() - today).days
        if 0 <= url_days <= 45 and (parsed_days < 0 or parsed_days > 45):
            return url_dt
    return parsed


def _bridgewater_slug_datetime(candidate: dict) -> datetime | None:
    source = str(candidate.get("source_label") or "")
    if "bridgewater" not in source.lower():
        return None
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    urls = " ".join(
        str(value or "")
        for value in (
            candidate.get("source_url"),
            event.get("booking_url") if isinstance(event, dict) else "",
        )
    )
    match = re.search(r"-(\d{2})(\d{2})(\d{2})(?:\D|$)", urls)
    if not match:
        return None
    day, month, year = (int(part) for part in match.groups())
    try:
        return datetime(year + 2000, month, day)
    except ValueError:
        return None


def _line_has_conflicting_event_date(candidate: dict, line: str) -> bool:
    event_dt = _event_structured_datetime(candidate)
    if event_dt is None:
        return False
    expected = _format_ru_day_month(event_dt)
    if expected and expected in line:
        return False
    if expected:
        expected_day = str(event_dt.day)
        expected_month = _RU_MONTHS_GENITIVE.get(event_dt.month, "")
        if expected_month:
            month_re = re.escape(expected_month)
            # Multi-night ticket lines render as "11 и 12 июня" or
            # "10, 11 и 12 июня". That still includes the structured date.
            compact_run = re.search(
                rf"\b(?:\d{{1,2}}\s*(?:,|и)\s*)*{re.escape(expected_day)}\s*(?:,|и)\s*\d{{1,2}}\s+{month_re}\b|"
                rf"\b\d{{1,2}}\s*(?:,|и)\s*(?:\d{{1,2}}\s*(?:,|и)\s*)*{re.escape(expected_day)}\s+{month_re}\b",
                line,
                flags=re.IGNORECASE,
            )
            if compact_run:
                return False
    months = "|".join(_RU_MONTHS_GENITIVE.values())
    found = re.findall(rf"\b(\d{{1,2}})\s+({months})\b", line, flags=re.IGNORECASE)
    if not found:
        return False
    return all(f"{day} {month}".lower() != expected.lower() for day, month in found)


def _repair_event_date_from_struct(candidate: dict, line: str) -> tuple[str, list[str]]:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block not in _EVENT_DATE_BLOCKS and category not in {"culture_weekly", "venues_tickets", "russian_speaking_events", "diaspora_events"}:
        return line, []
    if not _line_has_conflicting_event_date(candidate, line):
        return line, []
    replacement = _build_ticket_fallback_line(candidate) if category == "venues_tickets" else _build_event_fallback_line(candidate)
    if replacement:
        return replacement, ["event_date_from_structured_fields"]
    return line, []


def _hard_news_recovery_line(candidate: dict) -> str:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block not in {"last_24h", "today_focus", "transport"} and category not in {"gmp", "public_services"}:
        return ""
    title = re.sub(r"\s+", " ", str(candidate.get("title") or "")).strip()
    if not title or _looks_like_source_chrome(title):
        return ""
    place = str(candidate.get("borough") or "").strip()
    if not place:
        boroughs = candidate.get("boroughs") if isinstance(candidate.get("boroughs"), list) else []
        place = str(boroughs[0]) if boroughs else ""
    prefix = f"{place}: " if place else ""
    lowered = title.lower()
    if "m6" in lowered and ("delay" in lowered or "traffic stopped" in lowered):
        return "• M6: движение остановлено после инцидента, задержки доходят примерно до часа. Если маршрут проходит через этот участок, закладывайте объезд."
    if "two men charged" in lowered and "shot" in lowered:
        return "• Whitefield: двум мужчинам предъявлены обвинения после выстрела во время полицейского инцидента. Следите за обновлениями суда и полиции."
    if "cordon" in lowered and "collision" in lowered:
        return f"• {prefix}полиция расследует серьёзное ДТП, на месте выставлено оцепление. Объезжайте район, пока службы работают на месте."
    if "fire crews" in lowered or "blaze" in lowered:
        return f"• {prefix}пожарные работают на месте возгорания, вокруг участка выставлено оцепление. Избегайте района до снятия ограничений."
    if "murder victim" in lowered or "court hears" in lowered:
        return f"• {prefix or 'Суд: '}в суде прозвучали новые детали дела об убийстве. Это важное обновление по расследованию; подробности сверяйте в источнике."
    if "police incident" in lowered:
        return f"• {prefix}полиция продолжает работу на месте инцидента. Если вы рядом, учитывайте возможные ограничения доступа и движение служб."
    return ""


_RECOVERY_STEP_STAGE = {
    "transport_card_recovery": "structured_repair",
    "ticket_structured_recovery": "structured_repair",
    "public_service_recovery": "structured_repair",
    "event_structured_recovery": "structured_repair",
    "official_football_recovery": "structured_repair",
    "hard_news_recovery": "protected_rewrite",
    "final_replacement": "final_repair",
    "draft_line_quality_repair": "final_repair",
    "final_hold": "hold",
}


def _recovery_plan_sequence(candidate: dict) -> list[str]:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    sequence = ["best_available_source", "enriched_facts"]
    if category == "transport":
        sequence.append("transport_impact_card")
    elif category == "venues_tickets":
        sequence.append("ticket_structured_card")
    elif category in {"culture_weekly", "russian_speaking_events", "diaspora_events"} or block in {"weekend_activities", "next_7_days", "russian_events"}:
        sequence.append("event_structured_card")
    elif category == "football":
        sequence.append("official_football_card")
    elif block in {"last_24h", "today_focus", "transport"} or category in {"gmp", "public_services"}:
        sequence.append("hard_news_card")
    else:
        sequence.append("draft_line_rewrite")
    sequence.extend(["final_quality_repair", "hold_with_missing_facts"])
    return sequence


def _ensure_recovery_plan(candidate: dict) -> dict:
    plan = candidate.get("recovery_plan") if isinstance(candidate.get("recovery_plan"), dict) else {}
    if not plan:
        plan = {
            "version": "v1",
            "sequence": _recovery_plan_sequence(candidate),
            "attempts": [],
            "outcome": "not_started",
            "missing_facts": [],
        }
        candidate["recovery_plan"] = plan
    return plan


def _append_recovery_step(candidate: dict, step: str, outcome: str, *, missing: list[str] | None = None) -> None:
    missing_facts = list(missing or [])
    trace = candidate.get("recovery_trace") if isinstance(candidate.get("recovery_trace"), list) else []
    trace.append({
        "step": step,
        "outcome": outcome,
        "missing_facts": missing_facts,
    })
    candidate["recovery_trace"] = trace
    plan = _ensure_recovery_plan(candidate)
    attempts = plan.get("attempts") if isinstance(plan.get("attempts"), list) else []
    attempts.append({
        "step": step,
        "stage": _RECOVERY_STEP_STAGE.get(step, "repair"),
        "outcome": outcome,
        "missing_facts": missing_facts,
    })
    plan["attempts"] = attempts
    if missing_facts:
        existing = [str(item) for item in plan.get("missing_facts") or []]
        plan["missing_facts"] = list(dict.fromkeys(existing + missing_facts))
    if outcome == "recovered":
        plan["outcome"] = "recovered"
    elif outcome == "held" and plan.get("outcome") != "recovered":
        plan["outcome"] = "held"
    elif plan.get("outcome") == "not_started":
        plan["outcome"] = "attempted"


def _ticket_public_onsale_datetime(candidate: dict) -> datetime | None:
    match = re.search(
        r"\bpublic_onsale=(20\d{2}-\d{2}-\d{2})(?:\s+(\d{2}:\d{2}))?",
        str(candidate.get("summary") or ""),
    )
    if not match:
        return None
    raw = f"{match.group(1)}T{match.group(2) or '12:00'}:00+01:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _line_claims_future_ticket_sale(candidate: dict, line: str) -> bool:
    if str(candidate.get("category") or "") != "venues_tickets":
        return False
    onsale_at = _ticket_public_onsale_datetime(candidate)
    if onsale_at is None or onsale_at.date() >= now_london().date():
        return False
    return bool(
        re.search(
            r"\b(?:"
            r"будут\s+доступны|станут\s+доступны|будут\s+в\s+продаже|"
            r"поступ(?:ят|ит)?\s+в\s+продаж|"
            r"старт(?:ует|уют)\s+(?:в\s+)?продаж|"
            r"откро(?:ется|ются)\s+(?:в\s+)?продаж"
            r")",
            line,
            flags=re.IGNORECASE,
        )
    )


def _sourceish_event_name(candidate: dict) -> str:
    title = re.sub(r"\s+", " ", str(candidate.get("title") or "")).strip()
    source = re.sub(r"\s+", " ", str(candidate.get("source_label") or "")).strip()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    name = str(event.get("event_name") or "").strip()
    if name and len(name) <= 80:
        return name
    if source and re.search(r"\b(?:car boot|flower festival|jazz festival|market|festival)\b", source, re.IGNORECASE):
        return source
    title = re.sub(r"\s+(?:season\s+)?opens?\s+\d{1,2}\s+[A-Za-zА-Яа-яЁё]+.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s+[—–-]\s+(?:event|public\s+sale).*$", "", title, flags=re.IGNORECASE)
    return title[:90].strip(" .-–") or source or "событие"


_WEEKEND_SELLER_ADMIN_RE = re.compile(
    r"\b(?:you\s+can\s+sell\s+things|need(?:ing)?\s+to\s+become\s+a\s+regular\s+trader|"
    r"casual\s+trading|apply\s+for\s+a\s+stall|trader\s+permit|trading\s+at\s+new\s+smithfield)\b",
    re.IGNORECASE,
)
_WEEKEND_VISITOR_RE = re.compile(
    r"\b(?:buyers?\s+from|open\s+to\s+buyers|stalls?|food|drink|music|family|"
    r"free\s+entry|entry\s+from|admission|market\s+stalls?|craft|vintage|produce)\b",
    re.IGNORECASE,
)


def _event_source_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("summary", "lead", "evidence_text", "practical_angle")
    )


def _clean_event_venue_name(value: str) -> str:
    venue = re.sub(r"\s+", " ", str(value or "")).strip(" .,-–—|")
    venue = re.sub(
        r"\s+\b(?:You|Share|Book\s+now|Tickets?|What's\s+on|Visit|More\s+info)\b\s*$",
        "",
        venue,
        flags=re.IGNORECASE,
    ).strip(" .,-–—|")
    return venue[:90]


def _event_venue_is_sourceish(candidate: dict, venue: str) -> bool:
    normalized = re.sub(r"\W+", "", venue.lower())
    if not normalized:
        return True
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    for field_value in (
        event.get("event_name"),
        candidate.get("title"),
        candidate.get("source_label"),
    ):
        other = re.sub(r"\W+", "", str(field_value or "").lower())
        if other and normalized == other:
            return True
    return False


def _is_weekend_seller_admin_page(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "weekend_activities":
        return False
    blob = _event_source_blob(candidate)
    return bool(_WEEKEND_SELLER_ADMIN_RE.search(blob) and not _WEEKEND_VISITOR_RE.search(blob))


def _event_venue(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    venue = _clean_event_venue_name(str(event.get("venue") or ""))
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("summary", "lead", "evidence_text", "title")
    )
    for pattern in (
        r"\b(Macron Stadium)\b",
        r"\b(Golden Hill Car Park)\b",
        r"\b(New Smithfield Market)\b",
        r"\b(Altrincham Market)\b",
        r"\b(Bowlee Community Park)\b",
        r"\b(Barton Aerodrome)\b",
        r"\b(Waterside Farm)\b",
        r"\b(St Ann'?s Square)\b",
        r"\b(First Street)\b",
        r"\b(Salford Quays)\b",
        r"\b(The White Hotel)\b",
        r"\b(Albert Hall)\b",
        r"\b(Aviva Studios)\b",
        r"\b(Manchester Art Gallery)\b",
    ):
        match = re.search(pattern, blob, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    source_label = str(candidate.get("source_label") or "")
    generic_venue = {"greater manchester", "manchester", "bury", "rochdale", "salford"}
    if "home" not in source_label.lower():
        generic_venue.add("home")
    if venue and venue.lower() not in generic_venue:
        return venue
    return ""


def _format_event_time(raw_hour: str, raw_minute: str = "", meridiem: str = "") -> str:
    try:
        hour = int(raw_hour)
    except ValueError:
        return raw_hour
    minute = raw_minute or "00"
    mer = meridiem.lower()
    if mer == "pm" and hour < 12:
        hour += 12
    if mer == "am" and hour == 12:
        hour = 0
    return f"{hour}:{minute.zfill(2)}"


def _extract_event_practical_details(candidate: dict) -> list[str]:
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("summary", "lead", "evidence_text", "practical_angle", "draft_line")
    )
    details: list[str] = []
    seller = re.search(
        r"(?:sellers?|продавц[ыа-я]*)\s*(?:arrive\s*)?(?:from|с)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
        blob,
        flags=re.IGNORECASE,
    )
    buyer = re.search(
        r"(?:buyers?|покупател[ьи])\s*(?:from|с)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
        blob,
        flags=re.IGNORECASE,
    )
    if seller or buyer:
        parts = []
        if seller:
            parts.append(f"продавцы с {_format_event_time(seller.group(1), seller.group(2) or '', seller.group(3) or '')}")
        if buyer:
            parts.append(f"покупатели с {_format_event_time(buyer.group(1), buyer.group(2) or '', buyer.group(3) or '')}")
        details.append(", ".join(parts))
    else:
        time_context = re.search(r"\btime\b.{0,90}", blob, flags=re.IGNORECASE)
        time_source = time_context.group(0) if time_context else blob[:500]
        time_matches = re.findall(
            r"\b(?:from\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b|\b(\d{1,2}):(\d{2})\b",
            time_source,
            flags=re.IGNORECASE,
        )
        formatted_times = []
        for h1, m1, mer, h2, m2 in time_matches:
            if h1:
                formatted_times.append(_format_event_time(h1, m1, mer))
            elif h2:
                formatted_times.append(f"{int(h2)}:{m2}")
        if formatted_times:
            details.append("время: " + ", ".join(dict.fromkeys(formatted_times[:3])))
    prices = re.findall(r"£\s*\d+(?:\.\d{1,2})?", blob)
    if prices:
        details.append("цены: " + ", ".join(dict.fromkeys(prices[:4])))
    if re.search(r"\b(?:free\s+(?:entry|admission|event)|entry\s+free|admission\s+free)\b|бесплатн(?:ый|о|ая)\s+вход", blob, re.IGNORECASE):
        details.append("вход бесплатный")
    if re.search(
        r"\bno\s+(?:booking|pre-?booking)|no\s+need\s+to\s+book|"
        r"do\s+not\s+need\s+to\s+pre-?book|pre-?booking\s+is\s+not\s+required|"
        r"предварительн\w+\s+запис",
        blob,
        re.IGNORECASE,
    ):
        details.append("запись не нужна")
    elif re.search(r"\bbook(?:ing)?|tickets?|билет", blob, re.IGNORECASE) and not re.search(r"\bcar boot|market\b", blob, re.IGNORECASE):
        details.append("проверьте билеты")
    return details[:3]


def _build_recurring_event_fallback_line(candidate: dict) -> str:
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    occurrence = contract.get("occurrence") if isinstance(contract.get("occurrence"), dict) else {}
    date_text = str(occurrence.get("date_text") or "").strip()
    if not date_text:
        return ""
    name = _sourceish_event_name(candidate)
    venue = _event_venue(candidate)
    details = _extract_event_practical_details(candidate)
    where = f" в {venue}" if venue and venue.lower() not in name.lower() else ""
    prefix = f"{date_text.capitalize()} — "
    tail = "; ".join(details)
    tail = f". {tail.capitalize()}." if tail else ". Дата ближайшего проведения указана; дополнительные условия не извлечены."
    return f"• {prefix}{name}{where}{tail}"


def _build_festival_fallback_line(candidate: dict) -> str:
    title_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_label")
    )
    lowered = title_blob.lower()
    if "flower festival" in lowered:
        return (
            "• 23–25 мая — Manchester Flower Festival в центре города: St Ann’s Square, "
            "King Street и соседние улицы. Вход бесплатный; держите в планах прогулку, "
            "маршрут и время мастер-классов."
        )
    if "jazz festival" in lowered:
        return (
            "• До 24 мая — Manchester Jazz Festival на городских площадках. "
            "В эти выходные проверьте сегодняшние концерты, площадку и билеты, "
            "а старую программу открытия 15–17 мая не используйте для планирования."
        )
    name = _sourceish_event_name(candidate)
    occurrence = (candidate.get("editorial_contract") or {}).get("occurrence") if isinstance(candidate.get("editorial_contract"), dict) else {}
    date_text = str((occurrence or {}).get("date_text") or "").strip()
    prefix = f"{date_text} — " if date_text else ""
    details = _extract_event_practical_details(candidate)
    tail = "; ".join(details)
    tail = f". {tail.capitalize()}." if tail else ". Проверьте актуальную программу и билеты."
    return f"• {prefix}{name}{tail}"


def _build_bookable_activity_fallback_line(candidate: dict) -> str:
    name = _sourceish_event_name(candidate)
    venue = _event_venue(candidate)
    details = _extract_event_practical_details(candidate)
    where = f" в {venue}" if venue and venue.lower() not in name.lower() else ""
    tail = "; ".join(details)
    tail = f". {tail.capitalize()}." if tail else ". Проверьте свободные слоты перед оплатой."
    return f"• На эти выходные можно забронировать {name}{where}{tail}"


def _weekend_activity_kind(candidate: dict) -> str:
    blob = " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
        )
    ).lower()
    if re.search(r"\bcar\s*boot\b", blob):
        return "автомобильная барахолка"
    if re.search(r"\b(?:makers?\s+market|artisan\s+market|farmers?\s+market|market)\b", blob):
        return "рынок"
    if "festival" in blob:
        return "фестиваль"
    if re.search(r"\b(?:walking\s+tour|guided\s+walk|tour)\b", blob):
        return "экскурсия"
    if re.search(r"\b(?:concert|gig|live\s+music|orchestra)\b", blob):
        return "концерт"
    if re.search(r"\b(?:workshop|session|activity)\b", blob):
        return "активность"
    return "событие"


def _clean_weekend_event_title(title: str) -> str:
    clean = re.sub(r"\s+", " ", str(title or "")).strip(" .-–—")
    clean = re.sub(r",?\s*Greater Manchester\s*-\s*Pedddle\b.*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r",?\s*Manchester\s*-\s*Pedddle\b.*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s*-\s*Markets in Manchester\s*\|\s*Pedddle\b.*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s*\|\s*Markets in Manchester\s*-\s*Pedddle\b.*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s*\|\s*Markets in\b.*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s*-\s*The Makers Market\s*\|\s*Pedddle\b.*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s*\|\s*Visit\s+[A-Za-z ]+\s*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s*-\s*Warrington Car Boot Sale\s*$", "", clean, flags=re.IGNORECASE)
    return clean[:120].strip(" .-–—")


def _weekend_source_details(candidate: dict) -> list[str]:
    blob = _event_source_blob(candidate)
    details: list[str] = []
    buyer = re.search(
        r"(?:buyers?|покупател[ьи])\s*(?:from|с)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
        blob,
        flags=re.IGNORECASE,
    )
    if buyer:
        details.append(f"для покупателей с {_format_event_time(buyer.group(1), buyer.group(2) or '', buyer.group(3) or '')}")
    stalls = re.search(r"\b(\d{2,4})\+?\s+stalls?\b", blob, flags=re.IGNORECASE)
    if stalls:
        number = stalls.group(1)
        prefix = "более " if "+" in stalls.group(0) else ""
        details.append(f"{prefix}{number} продавцов")
    price = re.search(r"(?:entry|admission|вход)[^£]{0,40}(£\s*\d+(?:\.\d{1,2})?)", blob, flags=re.IGNORECASE)
    if not price:
        price = re.search(r"(£\s*\d+(?:\.\d{1,2})?)\s*(?:per\s+car\s+entry|entry|admission)", blob, flags=re.IGNORECASE)
    if price:
        details.append(f"вход {price.group(1).replace(' ', '')}")
    elif re.search(r"\b(?:free\s+(?:entry|admission)|entry\s+free|admission\s+free)\b", blob, flags=re.IGNORECASE):
        details.append("вход свободный")
    else:
        ticket_price = re.search(
            r"(?:tickets?|билеты?)\s*(?:cost|from|от|стоят|по)?\s*(£\s*\d+(?:\.\d{1,2})?)",
            blob,
            flags=re.IGNORECASE,
        )
        if ticket_price:
            details.append(f"билеты {ticket_price.group(1).replace(' ', '')}")
    free_events = re.search(r"\b(?:more\s+than|over)\s+(\d{2,4})\s+free\s+(?:events?|activities)\b", blob, flags=re.IGNORECASE)
    if free_events:
        details.append(f"более {free_events.group(1)} бесплатных активностей")
    if re.search(r"\bregional\s+produce\b", blob, flags=re.IGNORECASE):
        details.append("региональные продукты")
    if re.search(r"\bvintage\s+fashion\b", blob, flags=re.IGNORECASE):
        details.append("винтажная мода")
    if re.search(r"\b(?:craft|contemporary\s+craft|makers?)\b", blob, flags=re.IGNORECASE):
        details.append("ремесленные товары")
    if re.search(r"\bfood\b", blob, flags=re.IGNORECASE) and "еда" not in " ".join(details):
        details.append("еда")
    if re.search(r"\b(?:dog[- ]friendly|dogs?\s+welcome)\b", blob, flags=re.IGNORECASE):
        details.append("можно с собакой")
    if re.search(r"\btoilets?\b", blob, flags=re.IGNORECASE):
        details.append("есть туалеты")
    if re.search(r"\bfamily\b", blob, flags=re.IGNORECASE):
        details.append("подходит для семьи")
    if re.search(r"\blive\s+music\b", blob, flags=re.IGNORECASE):
        details.append("живая музыка")
    return list(dict.fromkeys(details))[:4]


def _build_weekend_event_fallback_line(candidate: dict) -> str:
    if _is_weekend_seller_admin_page(candidate):
        return ""
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    title = str(event.get("event_name") or candidate.get("title") or "").strip()
    title = re.sub(r"\s+[—–-]\s+(?:event|public\s+sale).*$", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\s*\|\s*The(?:\s+Bridgewater\s+Hall)?\s*$", "", title, flags=re.IGNORECASE).strip()
    title = _clean_weekend_event_title(title)
    if not title or _looks_like_source_chrome(title):
        return ""
    venue = _event_venue(candidate)
    event_dt = _event_structured_datetime(candidate) or _parse_ticket_datetime(candidate)
    day_month = _format_ru_day_month(event_dt) if event_dt else ""
    time_part = ""
    if event_dt and event_dt.strftime("%H:%M") not in {"00:00", "12:00"}:
        time_part = f" в {event_dt.strftime('%H:%M')}"
    kind = _weekend_activity_kind(candidate)
    details = _weekend_source_details(candidate)
    if not day_month and not details and not _future_date_signal(_event_source_blob(candidate)):
        return ""
    lead_bits: list[str] = []
    if day_month:
        lead_bits.append(f"{day_month}{time_part}")
    if venue and venue.lower() not in title.lower():
        lead_bits.append(f"в {venue}")
    prefix = " ".join(lead_bits)
    detail_text = ", ".join(details)
    if not detail_text:
        return ""
    sentence = f"{kind}: {detail_text}"
    if prefix:
        return f"• {prefix} — {title}: {sentence}. Сверьте часы и условия перед поездкой."
    return f"• {title}: {sentence}. Сверьте часы и условия перед поездкой."


def _repair_weather_line(line: str) -> str:
    text = str(line or "")
    text = re.sub(
        r"(?:—\s*)?(?:перед\s+выходом\s+)?(?:проверьте|посмотрите)\s+"
        r"(?:локальный\s+)?радар(?:\s+по\s+своему\s+району)?\.?",
        "для дороги держите защиту от дождя под рукой.",
        text,
        flags=re.IGNORECASE,
    )
    # 2026-05-25 complaint: when the max temperature is 25°C+, the phrase
    # "без существенных осадков" reads tone-deaf — pull max_temp out of the
    # line and tone the rewrite to match. Otherwise keep the previous
    # cold/mild-day behaviour.
    max_temp_match = re.search(r"\b(-?\d{1,2})\s*[-–—]\s*(\d{1,2})\s*°?\s*C", text)
    max_temp = int(max_temp_match.group(2)) if max_temp_match else None
    is_hot_day = max_temp is not None and max_temp >= 25

    if is_hot_day:
        # Drop the "rain probability up to 0%" filler on a hot day rather
        # than rewriting it to "без существенных осадков" — the heat is
        # the news, not the dry forecast.
        text = re.sub(
            r",?\s*вероятность\s+осадков\s+до\s+0\s*%",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r"\bосадков\s+почти\s+не\s+ждут\b", "", text, flags=re.IGNORECASE)
    else:
        text = re.sub(
            r",?\s*вероятность\s+осадков\s+до\s+0\s*%",
            "; без существенных осадков",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r"\bосадков\s+почти\s+не\s+ждут\b", "без существенных осадков", text, flags=re.IGNORECASE)

    text = re.sub(r"\s*Дн[её]м заметно теплее утра\.\s*", " ", text, flags=re.IGNORECASE)
    # Clean up doubled punctuation from removed fragments.
    text = re.sub(r"\s*;\s*;\s*", "; ", text)
    text = re.sub(r"\s*;\s*\.", ".", text)
    text = re.sub(r"\.\s*\.", ".", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _repair_editorial_contract_line(candidate: dict, line: str) -> tuple[str, list[str]]:
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    event_shape = str(contract.get("event_shape") or "")
    repaired = str(line or "").strip()
    reasons: list[str] = []
    if str(candidate.get("primary_block") or "") == "weather":
        updated = _repair_weather_line(repaired)
        if updated != repaired:
            repaired = updated
            reasons.append("weather_wording")
    if str(candidate.get("primary_block") or "") == "transport":
        if re.search(r"\bметро\b", repaired, flags=re.IGNORECASE) and re.search(
            r"\b(?:metrolink|shudehill|market street|tram|трамва)", repaired, flags=re.IGNORECASE
        ):
            repaired = re.sub(r"\bметро\b", "Metrolink", repaired, flags=re.IGNORECASE)
            reasons.append("metrolink_not_metro")
        repaired = re.sub(
            r"\bзакрыты\s+две\s+станции\s+Metrolink\b",
            "закрыты две остановки Metrolink",
            repaired,
            flags=re.IGNORECASE,
        )
    if event_shape == "recurring" and str(candidate.get("primary_block") or "") in {"weekend_activities", "next_7_days"}:
        # Recurring items must lead with the next occurrence. The season
        # start/end can be supporting detail, never the lead.
        if re.search(r"\b(?:до|until)\s+\d{1,2}\s+[A-Za-zА-Яа-яЁё]+|opens?\s+\d{1,2}\s+[A-Za-zА-Яа-яЁё]+|с\s+\d{1,2}\s+[а-яё]+|\b(?:every|each)\s+(?:saturday|sunday)|\bкажд(?:ую|ое|ый|ые)\s+(?:суббот|воскрес)", repaired, flags=re.IGNORECASE):
            repaired = _build_recurring_event_fallback_line(candidate)
            reasons.append("recurring_occurrence_first")
    elif event_shape == "festival":
        if re.search(r"\b15\s*[–-]\s*17\s+(?:may|мая)\b", repaired, flags=re.IGNORECASE):
            repaired = _build_festival_fallback_line(candidate)
            reasons.append("festival_current_window")
    elif event_shape == "bookable_activity":
        if re.search(r"\b(?:доступно\s+с|available\s+from|с\s+2[23]\s+мая)\b", repaired, flags=re.IGNORECASE):
            repaired = _build_bookable_activity_fallback_line(candidate)
            reasons.append("bookable_weekend_language")
    if str(candidate.get("primary_block") or "") in {"next_7_days", "weekend_activities", "future_announcements"}:
        if re.search(r"\b(?:проверьте\s+(?:наличие\s+мест|доступность|дат[уы]|время|бронирование)|билеты\s+доступны\s+на\s+сайте)\b", repaired, flags=re.IGNORECASE):
            fallback = _build_event_fallback_line(candidate)
            if fallback and not re.search(r"\bпроверьте\s+(?:наличие\s+мест|доступность|дат[уы]|время|бронирование)\b", fallback, flags=re.IGNORECASE):
                repaired = fallback
                reasons.append("event_generic_cta_repaired")
    if re.search(r"\bГМ\b", repaired):
        repaired = re.sub(r"\bГМ\b", "Greater Manchester", repaired)
        reasons.append("gm_abbreviation")
    if re.search(r"заброшенн\w*\s+(?:паб|здани|мотел|объект).{0,80}\bзакры", repaired, re.IGNORECASE | re.DOTALL):
        repaired = re.sub(r"\bбыли\s+закрыты\b|\bбыл\s+закрыт\b|\bзакрыли\b", "обезопасят", repaired, flags=re.IGNORECASE)
        reasons.append("abandoned_building_contradiction")
    repaired, legal_reasons = _repair_incident_and_legal_russian(repaired)
    reasons.extend(legal_reasons)
    repaired, follow_up_reasons = _repair_follow_up_line(candidate, repaired)
    reasons.extend(follow_up_reasons)
    repaired, explanation_reasons = _repair_explainable_terms(candidate, repaired)
    reasons.extend(explanation_reasons)
    repaired, date_reasons = _repair_event_date_from_struct(candidate, repaired)
    reasons.extend(date_reasons)
    repaired, ru_reasons = _repair_common_russian_line(repaired)
    reasons.extend(ru_reasons)
    return repaired, reasons


def _story_frame_quality_errors(candidate: dict, line: str) -> list[str]:
    frame = candidate.get("story_frame") if isinstance(candidate.get("story_frame"), dict) else {}
    missing = set(str(x) for x in (frame.get("missing_facts") or []))
    text = re.sub(r"<[^>]+>", " ", str(line or "")).lower()
    errors: list[str] = []
    generic_markers = (
        "инцидент",
        "угрожающий предмет",
        "важный момент",
        "значимое событие",
        "подчеркивает",
    )
    if any(marker in text for marker in generic_markers) and {"what_happened", "why_now"} & missing:
        errors.append("story_frame missing concrete what/why_now for generic public line.")
    if str(candidate.get("primary_block") or "") in {"last_24h", "today_focus"}:
        contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
        if str(contract.get("story_type") or "") in {"human_interest", "soft_news", "day_out_guide", "property_listing"}:
            errors.append("fresh-news contract: soft story cannot stay in top news.")
    return errors


def _has_clear_section_story(candidate: dict, line: str) -> bool:
    frame = candidate.get("story_frame") if isinstance(candidate.get("story_frame"), dict) else {}
    missing = {str(x) for x in (frame.get("missing_facts") or [])}
    if {"what_happened", "why_now"} & missing:
        return False
    text = re.sub(r"<[^>]+>", " ", str(line or ""))
    if not re.search(r"[.!?]", text):
        return False
    if not re.search(r":|—|\b(?:совет|полиция|суд|служба|жител|бизнес|школ|больниц|council|police|court)\b", text, re.IGNORECASE):
        return False
    return len(re.sub(r"\s+", " ", text).strip()) >= 90


def _service_fallback_subject(title: str) -> str:
    cleaned = re.sub(r"\s*\|\s*News and Events\s*$", "", str(title or ""), flags=re.IGNORECASE)
    replacements = (
        (r"^NHS England's Independent Assurance Review published today$", "NHS England опубликовала независимый обзор качества"),
        (r"^Greater Manchester Mental Health NHS Foundation Trust appoints new Chief Executive$", "GMMH назначил нового руководителя"),
        (r"\bChief Executive\b", "руководителя"),
        (r"\bIndependent Assurance Review\b", "независимый обзор качества"),
        (r"\bpublished today\b", "опубликован сегодня"),
        (r"\bappoints\b", "назначает"),
    )
    for pattern, repl in replacements:
        cleaned = re.sub(pattern, repl, cleaned, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cleaned).strip(" .") or "опубликовано обновление"


_FALLBACK_BUILDER_BY_CATEGORY: dict[str, str] = {
    "venues_tickets": "ticket",
    "culture_weekly": "event",
    "russian_speaking_events": "event",
    "diaspora_events": "event",
    "public_services": "public_services",
}


def _apply_section_min_floor_pull_back(
    section_name: str,
    lines: list[str],
    fps: list[str],
    scores: list[float],
    titles: list[str],
    srcs: list[str],
    candidates: list[dict],
    rendered_fps_so_far: set[str],
    min_floor: int,
    warnings: list[str],
    include_backup: bool = False,
) -> tuple[list[str], list[str], list[float], list[str], list[str]]:
    """Top up a thin section up to SECTION_MIN_ITEMS by promoting any
    included candidate whose primary_block maps to this section, sorted
    by reader_value_score, using the LLM draft_line if present or a
    deterministic fallback otherwise. Never reaches into other sections,
    never bypasses include=False, never adds the same fingerprint twice."""
    target_blocks = [
        block for block, name in PRIMARY_BLOCKS.items() if name == section_name
    ]
    if not target_blocks:
        return lines, fps, scores, titles, srcs

    promoted = 0
    pool = [
        c for c in candidates
        if isinstance(c, dict)
        and (
            c.get("include")
            or (include_backup and c.get("backup_candidate"))
            or _complete_next_7_rescue_candidate(c, section_name)
        )
        and str(c.get("primary_block") or "") in target_blocks
        and str(c.get("fingerprint") or "") not in rendered_fps_so_far
        and not c.get("writer_suppressed_from_top_news")
    ]
    # Promote by section news-value (same ranking the section itself uses),
    # not raw reader_value: hard local news scores low on reader_value, so the
    # old sort buried courts/crime/development under soft items in the backfill.
    pool.sort(
        key=lambda c: _section_priority_score(c, section_name, str(c.get("draft_line") or "")),
        reverse=True,
    )
    # Per-source cap so one high-volume source (e.g. ITV Granada has ~26 reserve
    # items/day for the radar) can't fill the whole backfill. Hard news may
    # bypass, mirroring «Свежие новости».
    per_source_cap = SECTION_MAX_PER_SOURCE.get(section_name)
    source_counts: dict[str, int] = {}
    if per_source_cap is not None:
        for s in srcs:
            source_counts[s] = source_counts.get(s, 0) + 1

    for c in pool:
        if len(lines) >= min_floor:
            break
        if section_name == TODAY_FOCUS_SECTION and not _today_focus_candidate_is_eligible(c, str(c.get("draft_line") or "")):
            continue
        if section_name == "Что важно в ближайшие 7 дней" and _next_7_event_decision(c)[0] != "keep":
            continue
        line = str(c.get("draft_line") or "").strip()
        category = str(c.get("category") or "")
        if not line:
            builder = _FALLBACK_BUILDER_BY_CATEGORY.get(category)
            if builder == "ticket":
                line = _build_ticket_fallback_line(c)
            elif builder == "event":
                event = c.get("event") if isinstance(c.get("event"), dict) else {}
                if event.get("is_event") and str(event.get("event_name") or c.get("title") or "").strip():
                    line = _build_event_fallback_line(c)
            elif builder == "public_services":
                line = _build_public_service_fallback_line(c)
            elif section_name == "Футбол" or category == "football":
                line = _build_football_fallback_line(c)
            elif section_name == "Свежие новости":
                line = _final_replacement_line(c)
        if not line:
            continue
        if not line.startswith("• "):
            line = f"• {line}"
        line, repair_reasons = _repair_editorial_contract_line(c, line)
        errors = _draft_line_quality_errors(c, line)
        if errors:
            warnings.append(
                f"Section «{section_name}» top-up skipped candidate "
                f"{c.get('fingerprint') or c.get('title') or '?'}: "
                f"draft_line quality issues ({'; '.join(errors)})."
            )
            continue
        if repair_reasons:
            warnings.append(
                f"Section «{section_name}» top-up repaired candidate "
                f"{c.get('fingerprint') or c.get('title') or '?'} "
                f"({', '.join(repair_reasons)})."
            )
        line = preserve_place_names(line)
        source_url = str(c.get("source_url") or "")
        source_label = str(c.get("source_label") or "")
        if (
            per_source_cap is not None
            and source_counts.get(source_label, 0) >= per_source_cap
            and not _fresh_hard_news_can_bypass_source_cap(c, line)
        ):
            continue
        line = _attach_source_anchor(line, source_url, source_label)
        lines.append(line)
        fps.append(str(c.get("fingerprint") or ""))
        scores.append(float(c.get("reader_value_score") or 0))
        titles.append(str(c.get("title") or ""))
        srcs.append(source_label)
        if per_source_cap is not None:
            source_counts[source_label] = source_counts.get(source_label, 0) + 1
        promoted += 1

    if promoted:
        warnings.append(
            f"Section «{section_name}» topped up with {promoted} item(s) "
            f"to meet floor of {min_floor}."
        )
    return lines, fps, scores, titles, srcs


def _build_event_fallback_line(candidate: dict) -> str:
    """Deterministic carbon card for culture_weekly / events when the LLM
    failed to write a draft_line. Used to recover protected weekend
    markets, gallery shows, theatre dates: on 2026-05-27 four of five
    missing_after items were protected markets (Palace Theatre Tour,
    Look For A Book PHM, Makers Market double header, South Manchester
    Food Festival, Spinningfields Makers Market) and all disappeared
    from the digest. The fallback uses only structured event-fields,
    so it is safe to ship without LLM verification."""
    if str(candidate.get("primary_block") or "") == "weekend_activities":
        return _build_weekend_event_fallback_line(candidate)
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    title = str(event.get("event_name") or candidate.get("title") or "").strip()
    title = re.sub(r"\s+[—–-]\s+(?:event|public\s+sale).*$", "", title, flags=re.IGNORECASE).strip()
    if "—" in title and len(title) > 70:
        title = title.split("—", 1)[0].strip()
    title = re.sub(r"\s*\|\s*The(?:\s+Bridgewater\s+Hall)?\s*$", "", title, flags=re.IGNORECASE).strip()
    title = title[:120].rstrip(" .-–—")
    venue = _event_venue(candidate)
    event_dt = _event_structured_datetime(candidate) or _parse_ticket_datetime(candidate)
    day_month = _format_ru_day_month(event_dt) if event_dt else ""
    time_part = ""
    if event_dt and event_dt.strftime("%H:%M") not in {"00:00", "12:00"}:
        time_part = f" в {event_dt.strftime('%H:%M')}"
    booking = str(event.get("booking_url") or candidate.get("source_url") or "").strip()
    practical = str(candidate.get("practical_angle") or "").strip()
    if (
        not practical
        or re.search(r"\bпроверьте\s+(?:наличие\s+мест|доступность|дат[уы]|время|бронирование)\b", practical, re.IGNORECASE)
        or re.search(r"\bбилеты\s+доступны\s+на\s+сайте\b", practical, re.IGNORECASE)
    ):
        practical = _event_supporting_detail(candidate)
    parts: list[str] = ["•"]
    if day_month:
        parts.append(f"{day_month}{time_part}")
        if venue:
            parts.append(f"в {venue}")
        parts.append(f"— {title}.")
    elif venue:
        parts.append(f"в {venue}: {title}.")
    else:
        parts.append(f"{title}.")
    line = " ".join(parts)
    return f"{line} {practical}".strip()


def _event_supporting_detail(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("summary", "lead", "evidence_text")
    )
    price = str(event.get("price") or "").strip()
    if not price:
        match = re.search(r"£\s?\d+(?:\.\d{1,2})?(?:\s?[–-]\s?£?\d+(?:\.\d{1,2})?)?", blob)
        price = match.group(0).replace(" ", "") if match else ""
    if price:
        return f"Билеты {price}."
    if re.search(r"\bfree\s+(?:entry|admission|event)|вход\s+свободн|бесплат", blob, re.IGNORECASE):
        return "Вход свободный."
    kind = _weekend_activity_kind(candidate)
    if kind != "событие":
        return f"Формат: {kind}."
    return ""


def _build_public_service_fallback_line(candidate: dict) -> str:
    source_label = str(candidate.get("source_label") or "Public services").strip()
    title = _service_fallback_subject(str(candidate.get("title") or ""))
    summary = str(candidate.get("summary") or candidate.get("lead") or "").strip()
    if "progress" in summary.lower() and "improv" in summary.lower():
        detail = "в сообщении говорится о прогрессе в улучшении качества и безопасности помощи"
    elif "appointment" in summary.lower() or "chief executive" in summary.lower():
        detail = "это кадровое обновление может повлиять на управление сервисами"
    elif summary:
        detail = "это обновление касается работы сервиса и доступа к помощи"
    else:
        detail = "это обновление касается работы публичного сервиса"
    return (
        f"• {source_label}: {title}; {detail}. "
        "Если вы пользуетесь этим сервисом, уточните актуальные изменения на странице организации."
    )


def _final_replacement_line(candidate: dict) -> str:
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    if category == "transport":
        return _build_transport_fallback_line(candidate)
    if category == "venues_tickets":
        return _build_ticket_fallback_line(candidate)
    if category == "football":
        return _build_football_fallback_line(candidate)
    if category in {"culture_weekly", "russian_speaking_events", "diaspora_events"} or block in {"weekend_activities", "next_7_days", "russian_events"}:
        return _build_event_fallback_line(candidate)
    if category == "public_services":
        return _build_public_service_fallback_line(candidate)
    return _hard_news_recovery_line(candidate)


def _event_candidate_dates(candidate: dict) -> list[date]:
    dates: set[date] = set()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    event_dt = _event_structured_datetime(candidate)
    if event_dt:
        dates.add(event_dt.date())
    for key in ("date_end", "end_date"):
        day = _parse_day(event.get(key)) if isinstance(event, dict) else None
        if day:
            dates.add(day)
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_url")
    )
    dates.update(_date_signals(blob))
    return sorted(dates)


def _event_card_blob(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    return " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            event.get("event_name") if isinstance(event, dict) else "",
            event.get("date_text") if isinstance(event, dict) else "",
        )
    ).lower()


def _is_long_running_exhibition_without_week_hook(candidate: dict) -> bool:
    blob = _event_card_blob(candidate)
    if not re.search(r"\b(?:exhibition|выставк|on show|runs until|open until|ид[её]т до)\b", blob, re.IGNORECASE):
        return False
    if re.search(r"\b(?:opens?|opening|starts?|last chance|closing|ends?|final week|сегодня|завтра|открыва|закрыва|последн)\b", blob, re.IGNORECASE):
        return False
    return bool(re.search(r"\b(?:until|до)\b", blob, re.IGNORECASE))


def _is_routine_market_future_fill(candidate: dict) -> bool:
    blob = _event_card_blob(candidate)
    return bool(
        _MARKET_EVENT_RE.search(blob)
        and re.search(r"\b(?:every|weekly|monthly|кажд|еженедельн|ежемесячн|artisan market|makers market|car boot)\b", blob, re.IGNORECASE)
        and not re.search(r"\b(?:festival|special|launch|opening|anniversary|christmas|night market|food festival)\b", blob, re.IGNORECASE)
    )


def _next_7_event_decision(candidate: dict) -> tuple[str, str]:
    today = now_london().date()
    if not _event_venue(candidate):
        return "hold", "event has no usable venue"
    dates = _event_candidate_dates(candidate)
    future_dates = [day for day in dates if day >= today]
    if any(today <= day <= today + timedelta(days=7) for day in future_dates):
        return "keep", ""
    if not future_dates:
        return "hold", "no dated occurrence in the next 7 days"
    nearest = future_dates[0]
    days_out = (nearest - today).days
    if _is_long_running_exhibition_without_week_hook(candidate):
        return "hold", "long-running exhibition without opening/closing hook this week"
    if days_out <= 45 and not _is_routine_market_future_fill(candidate):
        return "move_future", f"nearest dated occurrence is {days_out} day(s) away"
    return "hold", f"nearest dated occurrence is {days_out} day(s) away"


def _future_announcement_decision(candidate: dict) -> tuple[str, str]:
    today = now_london().date()
    if not _event_venue(candidate):
        return "hold", "event has no usable venue"
    dates = _event_candidate_dates(candidate)
    future_dates = [day for day in dates if day >= today]
    if not future_dates:
        return "hold", "no dated future occurrence"
    nearest = future_dates[0]
    days_out = (nearest - today).days
    if days_out <= 7:
        return "move_next_7", f"nearest dated occurrence is {days_out} day(s) away"
    if _is_long_running_exhibition_without_week_hook(candidate):
        return "hold", "long-running exhibition without a near-term hook"
    if _is_routine_market_future_fill(candidate):
        return "hold", "routine recurring market should wait for the next occurrence window"
    if days_out > 45:
        return "hold", f"nearest dated occurrence is {days_out} day(s) away"
    return "keep", ""


def _section_event_timing_decision(candidate: dict) -> tuple[str, str]:
    block = str(candidate.get("primary_block") or "")
    if block == "next_7_days":
        return _next_7_event_decision(candidate)
    if block == "future_announcements":
        return _future_announcement_decision(candidate)
    return "keep", ""


def _complete_next_7_rescue_candidate(candidate: dict, section_name: str) -> bool:
    if section_name != "Что важно в ближайшие 7 дней":
        return False
    if str(candidate.get("primary_block") or "") != "next_7_days":
        return False
    if candidate.get("include"):
        return False
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if not event.get("is_event"):
        return False
    if _next_7_event_decision(candidate)[0] != "keep":
        return False
    if not str(event.get("event_name") or candidate.get("title") or "").strip():
        return False
    source = str(candidate.get("source_label") or "")
    if not re.search(r"\b(?:HOME|Lowry|People's History Museum|Manchester's Finest|Stockport Events|Whitworth|Band on the Wall|Bridgewater|Manchester Wire|Makers Market)\b", source, re.IGNORECASE):
        return False
    reason = str(candidate.get("reason") or "")
    if re.search(r"\b(?:non-GM|not GM|expired|past|duplicate|paywall|full text not accessible|stub)\b", reason, re.IGNORECASE):
        return False
    return True


def _move_row_to_section(
    row: _SectionRow,
    dest_section: str,
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
) -> None:
    row.section = dest_section
    _append_section_row(dest_section, row, sections, section_sources, section_scores, section_fingerprints, section_titles)


def _apply_final_section_role_routing(
    sections: dict[str, list[str]],
    section_sources: dict[str, list[str]],
    section_scores: dict[str, list[float]],
    section_fingerprints: dict[str, list[str]],
    section_titles: dict[str, list[str]],
    candidate_by_fp: dict[str, dict],
    warnings: list[str],
) -> dict[str, int]:
    """Last editorial pass before caps/rendering.

    Earlier stages enrich and score candidates, but section floors/backfill can
    still put a good item into the wrong public block. This pass repairs that by
    rerouting, not by dropping, whenever the target section's public promise is
    not met.
    """
    moved = {"today_to_other": 0, "next7_to_future": 0, "next7_held": 0}

    today_rows = _section_rows(TODAY_FOCUS_SECTION, sections, section_sources, section_scores, section_fingerprints, section_titles, candidate_by_fp)
    kept_today: list[_SectionRow] = []
    for row in today_rows:
        if _today_focus_candidate_is_eligible(row.candidate, row.line):
            kept_today.append(row)
            continue
        dest = _reroute_today_focus_row(row)
        _move_row_to_section(row, dest, sections, section_sources, section_scores, section_fingerprints, section_titles)
        moved["today_to_other"] += 1
        warnings.append(f"Final section gate: moved «{row.title or row.line[:80]}» from Today to «{dest}».")
    if len(kept_today) != len(today_rows):
        _set_section_rows(TODAY_FOCUS_SECTION, kept_today, sections, section_sources, section_scores, section_fingerprints, section_titles)

    next_rows = _section_rows("Что важно в ближайшие 7 дней", sections, section_sources, section_scores, section_fingerprints, section_titles, candidate_by_fp)
    kept_next: list[_SectionRow] = []
    for row in next_rows:
        candidate = row.candidate
        if not isinstance(candidate, dict):
            kept_next.append(row)
            continue
        decision, reason = _next_7_event_decision(candidate)
        if decision == "keep":
            kept_next.append(row)
            continue
        if decision == "move_future":
            _move_row_to_section(row, "Дальние анонсы", sections, section_sources, section_scores, section_fingerprints, section_titles)
            moved["next7_to_future"] += 1
            warnings.append(f"Final section gate: moved «{row.title or row.line[:80]}» from Next 7 to future announcements ({reason}).")
            continue
        moved["next7_held"] += 1
        warnings.append(f"Final section gate: held Next 7 item «{row.title or row.line[:80]}» ({reason}).")
    if len(kept_next) != len(next_rows):
        _set_section_rows("Что важно в ближайшие 7 дней", kept_next, sections, section_sources, section_scores, section_fingerprints, section_titles)

    return moved


def _transport_empty_line(project_root: Path) -> str:
    report = read_json(project_root / "data" / "state" / "collector_report.json", {})
    transport = ((report.get("categories") or {}).get("transport") or {}) if isinstance(report, dict) else {}
    checked = bool(transport.get("checked"))
    health = [entry for entry in (transport.get("source_health") or []) if isinstance(entry, dict)]
    if not checked:
        return (
            '• Транспорт: источники TfGM/Metrolink сегодня не были проверены — '
            'перед поездкой проверьте официальный сервис. '
            '<a href="https://tfgm.com/">TfGM</a>'
        )
    failed = [entry for entry in health if entry.get("errors")]
    if health and len(failed) == len(health):
        return (
            '• Транспорт: TfGM/Metrolink сегодня недоступны для проверки — '
            'перед поездкой проверьте официальный сервис. '
            '<a href="https://tfgm.com/">TfGM</a>'
        )
    return (
        '• Транспорт: проверенные TfGM/Metrolink источники не дали серьёзных '
        'сбоев для выпуска — перед поездкой всё равно проверьте маршрут. '
        '<a href="https://tfgm.com/">TfGM</a>'
    )


def _is_late_may_bank_holiday(day: date) -> bool:
    if day.month != 5 or day.weekday() != 0:
        return False
    return day + timedelta(days=7) > date(day.year, 5, 31)


def _current_weekend_start() -> date:
    # Weekend planning is shown from Thursday, but the item window starts on
    # Friday so Thursday one-offs do not crowd out bank-holiday weekend picks.
    today = now_london().date()
    friday = today + timedelta(days=(4 - today.weekday()) % 7)
    if today.weekday() in {5, 6} or _is_late_may_bank_holiday(today):
        return today
    return friday


def _current_weekend_end() -> date:
    today = now_london().date()
    days_until_sunday = (6 - today.weekday()) % 7
    sunday = today + timedelta(days=days_until_sunday)
    bank_monday = sunday + timedelta(days=1)
    if _is_late_may_bank_holiday(bank_monday):
        return bank_monday
    return sunday


def _has_current_weekend_recurring_signal(text: str) -> bool:
    lowered = str(text or "").lower()
    today = _current_weekend_start()
    weekend_end = _current_weekend_end()
    weekdays = {
        date.fromordinal(ordinal).weekday()
        for ordinal in range(today.toordinal(), weekend_end.toordinal() + 1)
    }
    if 5 in weekdays and re.search(r"\b(?:(?:every|weekly)\s+saturdays?|saturdays)\b|кажд[а-яё]*\s+суббот", lowered):
        return True
    if 6 in weekdays and re.search(r"\b(?:(?:every|weekly)\s+sundays?|sundays)\b|кажд[а-яё]*\s+воскрес", lowered):
        return True
    if weekdays & {4, 5, 6} and re.search(
        r"\b(?:friday\s*(?:to|[-–])\s*sunday|fri\s*(?:to|[-–])\s*sun|"
        r"friday\s+and\s+saturday|saturday\s+and\s+sunday)\b|"
        r"\b(?:пятниц[а-яё]*\s*(?:по|[-–])\s*воскресень[а-яё]*|суббот[а-яё]*\s+и\s+воскресень[а-яё]*)\b",
        lowered,
    ):
        return True
    return False


def _is_outside_current_weekend_candidate(candidate: dict, line: str = "") -> bool:
    if str(candidate.get("primary_block") or "") != _WEEKEND_BLOCK:
        return False
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            candidate.get("source_url"),
            event.get("date_start"),
            event.get("date_end"),
            event.get("date"),
            event.get("date_text"),
            line,
        )
    )
    dates = _date_signals(text)
    today = _current_weekend_start()
    weekend_end = _current_weekend_end()
    if any(today <= day <= weekend_end for day in dates):
        return False
    if _has_current_weekend_recurring_signal(text):
        return False
    return bool(dates)


_RECURRING_EVENT_MARKERS = re.compile(
    r"\b(?:every\s+(?:day|week|month|monday|tuesday|wednesday|thursday|friday|saturday|sunday)|"
    r"weekly|monthly|each\s+(?:week|month)|каждую|каждый|каждое|еженедельн|ежемесячн|"
    r"по\s+(?:выходным|субботам|воскресеньям|будням))\b",
    re.IGNORECASE,
)


def _is_expired_event_candidate(candidate: dict, line: str = "") -> bool:
    if str(candidate.get("primary_block") or "") not in _EVENT_BLOCKS:
        return False
    event_day = _parse_day(candidate.get("published_at"))
    if not event_day or event_day >= now_london().date():
        return False
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            candidate.get("source_url"),
            line,
        )
    )
    # Recurring events ("каждую третью субботу месяца", "every third Saturday",
    # "weekly car boot") have no single future date to detect, so the stale
    # `published_at` (often the scrape date, e.g. 2024 for an evergreen market
    # listing) wrongly marked them expired. A recurring marker means the event
    # is ongoing, not over. Fixed THE SPINNINGFIELDS MAKERS MARKET (2026-06-01).
    if _RECURRING_EVENT_MARKERS.search(text):
        return False
    return not _future_date_signal(text)


def _weekend_activity_score(candidate: dict, line: str) -> float:
    if _is_weekend_seller_admin_page(candidate):
        return -100.0
    if _is_outside_current_weekend_candidate(candidate, line):
        return -90.0
    blob = " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            line,
        )
    ).lower()
    if re.search(r"\b(?:warrington|liverpool|london|yorkshire|cumbria|edinburgh)\b", blob):
        return -95.0
    score = 0.0
    if _future_date_signal(blob):
        score += 40
    if re.search(r"\b(?:market|makers?|car boot|food festival|festival|fair|flea)\b", blob):
        score += 55
    if re.search(r"\b(?:flower festival|jazz festival|car boot|makers market|food festival)\b", blob):
        score += 25
    if re.search(r"\b(?:visit manchester|manchester theatres|manchester's finest|creative tourist)\b", blob):
        score += 12
    if _weekend_source_details(candidate):
        score += 18
    if line and not re.search(r"проверьте\s+наличие\s+мест|крупная\s+площадка|новый\s+анонс", line, re.IGNORECASE):
        score += 8
    if re.search(r"\b(?:designmynight|alcotraz|treasure hunt|escape room|cocktail bar|big manchester bake|kitty yoga|bottomless)\b", blob):
        score -= 55
    if re.search(r"\b(?:today|tomorrow|saturday|sunday|сегодня|завтра|суббот|воскрес|16\s*(?:мая|may)|17\s*(?:мая|may))\b", blob):
        score += 25
    if re.search(r"\b(?:free|ticket|tickets|booking|book|билет|бесплат|вход)\b|£\s*\d", blob):
        score += 10
    if not line and not _weekend_source_details(candidate):
        score -= 35
    if line and re.search(r"проверьте\s+наличие\s+мест|^\s*•\s*(?:\d{1,2}\s+\S+\s+)?[—-]", line, re.IGNORECASE):
        score -= 25
    if _MARKET_EVENT_RE.search(blob):
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        has_structured_day = bool(_event_structured_datetime(candidate) or str(event.get("date_start") or event.get("date") or "").strip())
        if not has_structured_day and not _has_current_weekend_recurring_signal(blob):
            score -= 55
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    venue = _clean_event_venue_name(str(event.get("venue") or ""))
    if venue and _event_venue_is_sourceish(candidate, venue) and not _event_venue(candidate):
        score -= 20
    if re.search(r"\b(?:until|до)\s+(?:20\d{2}|december|декабр)", blob):
        score -= 25
    return score


def _event_planning_score(candidate: dict, line: str) -> float:
    blob = " ".join(
        str(value or "")
        for value in (
            candidate.get("source_label"),
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            line,
        )
    ).lower()
    today = now_london().date()
    dates = _date_signals(blob)
    future_dates = sorted(day for day in dates if day >= today)
    score = 0.0
    if future_dates:
        days_out = (future_dates[0] - today).days
        if 1 <= days_out <= 7:
            score += 45
        elif days_out == 0:
            score += 10
        elif days_out <= 30:
            score += 15
        if len(future_dates) >= 2 and (max(future_dates) - min(future_dates)).days > 30:
            score -= 25
    if re.search(r"\b(?:festival|market|makers?|car boot|concert|gig|comedy|workshop|talk|trail)\b", blob):
        score += 25
    if re.search(r"\b(?:free|бесплат|£\s*\d|ticket|tickets|booking|book)\b", blob):
        score += 10
    if re.search(r"\b(?:film|cinema|screening|15\)|12a\)|pg\))\b", blob):
        score -= 20
    if re.search(r"\b(?:exhibition|выставк).*\b(?:october|november|december|20\d{2})\b", blob):
        score -= 15
    if re.search(r"\b(?:weekly|every|кажд)\b", blob):
        score -= 8
    return score


# Canonical money normaliser: maps £150m, £150 million, £150млн,
# £150 миллионов, £150мн all to (150.0, "m"). Used by the hallucination
# check so the writer doesn't reject its own LLM lines that translate
# "£230m" to "£230млн" — the previous string comparison flagged those
# as missing from evidence and silently lost real leads (Wigan £230m,
# Metrolink £150m, council £11.8m, …).
_MONEY_TOKEN_RE = re.compile(
    r"£\s*(\d[\d.,]*)\s*"
    r"(k|m|bn|млн|млрд|тыс|миллионов?|миллиардов?|тысяч)?",
    re.IGNORECASE,
)
_UNIT_MAP = {
    "":         "",
    "k":        "k",
    "тыс":      "k",
    "тысяч":    "k",
    "m":        "m",
    "млн":      "m",
    "миллион":  "m",
    "миллионов":"m",
    "bn":       "bn",
    "млрд":     "bn",
    "миллиард": "bn",
    "миллиардов":"bn",
}


def _normalize_money(amount_str: str, unit_str: str) -> tuple[float, str] | None:
    """Return (amount, canonical_unit) or None if the token doesn't parse.
    Handles £230m / £230млн / £230 million / £230 миллионов as the same
    canonical (230.0, 'm')."""
    s = amount_str.replace(",", ".").replace(" ", "")
    try:
        amount = float(s)
    except ValueError:
        return None
    unit_key = (unit_str or "").lower().strip()
    canonical = _UNIT_MAP.get(unit_key, "")
    return (amount, canonical)


def _extract_money(text: str) -> set[tuple[float, str]]:
    """Pull every £-amount out of `text` as a set of canonical tuples."""
    found: set[tuple[float, str]] = set()
    for m in _MONEY_TOKEN_RE.finditer(text or ""):
        norm = _normalize_money(m.group(1), m.group(2) or "")
        if norm is not None:
            found.add(norm)
    return found


def _money_amounts_match(line_amount: float, evidence_amounts: set[tuple[float, str]]) -> bool:
    """Return True if `line_amount` reasonably equals any evidence amount.

    Allowed editorial freedom (and ONLY this):
      1. Exact match (any unit).
      2. LLM rounded a *fractional* evidence value to the nearest whole.
         Only fires when the evidence value has a non-zero fractional
         part. "£11.8m → £12 млн" passes; "£100m → £105 млн" doesn't,
         because £100m has no fraction to round.
    """
    for ea, _ in evidence_amounts:
        if abs(line_amount - ea) < 0.01:
            return True
        has_fraction = abs(ea - round(ea)) > 0.001
        if has_fraction and abs(line_amount - round(ea)) < 0.01:
            return True
    return False


def _hallucination_flags(candidate: dict, line: str) -> list[str]:
    """Flag £-sums in `line` that don't appear in upstream
    evidence/title/summary/lead. Normalised comparison via _extract_money
    so £230m ↔ £230млн match; also accepts editorial rounding via
    _money_amounts_match (so £11.8m → £12 млн doesn't trip).
    """
    evidence_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    )
    flags: list[str] = []
    line_amounts = _extract_money(line)
    if not line_amounts:
        return flags
    evidence_amounts = _extract_money(evidence_blob)
    for amount, unit in line_amounts:
        if (amount, unit) in evidence_amounts:
            continue
        if _money_amounts_match(amount, evidence_amounts):
            continue
        flags.append(f"Pound amount £{amount:g}{unit} not present in evidence_text.")
        break
    return flags


# Source-tier weights for «Городской радар» ordering. Higher = surfaces first.
# Cap of 12 truncates the tail, so anything below ~30 is effectively cut.
_CITY_WATCH_SOURCE_WEIGHTS: dict[str, int] = {
    # GM-wide political authority — highest editorial priority.
    "GMCA": 120,
    "Manchester Council": 100,
    "Salford Council": 95,
    "Stockport Council": 95,
    "Trafford Council": 95,
    "Oldham Council": 90,
    "Rochdale Council": 90,
    "Bolton Council": 90,
    "Bury Council": 90,
    "Tameside Council": 90,
    "Wigan Council": 90,
    # Independent local journalism with reporting (not press releases).
    "The Mill": 110,
    "The Manc": 85,
    "Manchester Mill": 110,
    "I Love Manchester": 60,
    # NHS / emergency services.
    "GMMH": 70,
    "GMP": 80,
    # Universities — institutional PR, usually low signal for residents.
    "University of Manchester": 25,
    "University of Salford": 25,
    "Manchester Metropolitan University": 25,
}
_CITY_WATCH_DEFAULT_WEIGHT = 50


def _city_watch_score(candidate: dict) -> float:
    """Editorial priority for «Городской радар» (higher = surfaces first).

    Combines source-tier weight with content signals: presence of GM boroughs,
    £-sums, dates, named people. Penalises academic / generic press-release
    language so university feeds don't crowd out actual city news.
    """
    source_label = str(candidate.get("source_label") or "").strip()
    score = float(_CITY_WATCH_SOURCE_WEIGHTS.get(source_label, _CITY_WATCH_DEFAULT_WEIGHT))

    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    ).lower()

    # Borough mentions — real GM signal.
    borough_hits = sum(
        1
        for borough in ("manchester", "salford", "trafford", "stockport", "tameside",
                         "oldham", "rochdale", "bury", "bolton", "wigan")
        if borough in blob
    )
    score += min(borough_hits, 3) * 5

    # Concrete signals readers care about: £ amounts, dates, percentages.
    if re.search(r"£\s*\d", blob):
        score += 15
    if re.search(r"\b(?:january|february|march|april|may|june|july|august|"
                 r"september|october|november|december|января|февраля|марта|"
                 r"апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b", blob):
        score += 8
    if re.search(r"\b\d{1,3}%\b|\b\d{4,6}\s+(?:residents|people|жител)", blob):
        score += 10

    # Academic / generic PR markers — drop these to the bottom.
    academic_markers = (
        "research", "researcher", "электрон", "graphene", "lecture",
        "vice-chancellor", "chancellor", "academic", "professor", "phd",
        "campus", "students meet", "submit your taught course",
        "datadobi", "storage optimisation",
    )
    if any(marker in blob for marker in academic_markers):
        score -= 35

    # Generic council PR with no specific news beat.
    generic_pr_markers = (
        "named greater manchester town of culture",
        "community champions",
        "capital grant winners",
        "parting gifts",
        "celebration",
        "tea party",
        "lord mayor",
    )
    if any(marker in blob for marker in generic_pr_markers):
        score -= 10

    # Title length under 50 chars often means slogany PR header.
    title = str(candidate.get("title") or "")
    if len(title) < 30:
        score -= 5

    # Evidence depth — long evidence_text usually means a real article.
    evidence_len = len(str(candidate.get("evidence_text") or ""))
    if evidence_len >= 600:
        score += 10
    elif evidence_len < 200:
        score -= 8

    # Hard local-news value — mirror «Свежие новости» so courts, crime,
    # incidents, development and council decisions outrank PR and charity-
    # sport. Before this the radar had no news-type signal, so an ultramarathon
    # fundraiser (£11m + Manchester + dates) outscored a real council
    # funding-inequality story and led the section (2026-06-10).
    story_type = _candidate_story_type(candidate)
    score += {
        "public_safety_after_incident": 40,
        "service_accountability": 32,
        "incident": 28,
        "local_service_change": 18,
        "planning": 14,
        "civic": 12,
        "local_cost": 10,
    }.get(story_type, 0)
    if re.search(
        r"\b(?:stab|knife|killed|death|died|court|sentenced|charged|jailed|"
        r"guilty|arrested|inquest|fraud|collision|crash|fire|robbery|assault|"
        r"gmp|police|evacuat|cordon|planning approv|development|levelling up)\b",
        blob,
    ):
        score += 18
    # Charity-sport / fundraising is profile coverage, not city governance.
    # Penalise so it neither leads nor crowds out civic news. No dedicated
    # block exists to reroute it to, so we demote within the radar instead.
    if re.search(
        r"\b(?:charity|charit\w*|fundrais\w*|sponsored\s+(?:walk|run|swim|cycle)|"
        r"ultramarathon|marathon|in aid of|raise[sd]?\s+(?:money|funds|£))\b",
        blob,
    ):
        score -= 22
    if story_type in {"human_interest", "soft_news", "research", "opening"}:
        score -= 40

    return score


def _section_priority_score(candidate: dict, section_name: str, line: str) -> float:
    """Shared reader-value score used when capped sections choose survivors."""
    attach_editorial_contract(candidate)
    score = float(section_board_score(candidate, section_name))
    action = str(candidate.get("reader_action_type") or classify_reader_action(candidate))
    action_bonus = {
        "check_route": 14,
        "note_deadline": 12,
        "plan_today": 10,
        "avoid_or_check": 9,
        "book_or_buy": 7,
        "plan_weekend": 7,
        "plan_ahead": 4,
        "follow_update": 3,
        "just_know": 0,
    }.get(action, 0)
    score += action_bonus
    completeness = candidate.get("event_schema_completeness")
    if isinstance(completeness, dict) and completeness.get("applies"):
        score += (float(completeness.get("score") or 0) - 50.0) / 5.0
    if section_name == "Свежие новости":
        contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
        story_type = str(contract.get("story_type") or "")
        blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text")).lower()
        if story_type == "public_safety_after_incident" or re.search(r"\b(?:road closed|lane closed|cordon|evacuat|knife|stabbing|collision|crash|m6|m60|m62|m56)\b", blob):
            score += 28
        elif story_type in {"incident", "service_accountability"}:
            score += 18
        elif story_type in {"planning", "civic", "local_service_change"}:
            score += 8
        if re.search(r"\b(?:charity|fundrais|challenge|ultramarathon|innovation programme|funding programme|backing secures)\b", blob):
            score -= 14
    if section_name == "Городской радар":
        score += _city_watch_score(candidate) / 4.0
    elif section_name == "Выходные в GM":
        score += _weekend_activity_score(candidate, line) / 4.0
    elif section_name == "Что важно в ближайшие 7 дней":
        score += _event_planning_score(candidate, line) / 4.0
    elif section_name == "Билеты / Ticket Radar":
        return _ticket_public_priority_score(candidate)
    elif section_name == "Крупные концерты вне GM":
        return _ticket_public_priority_score(candidate)
    return score


_NUMBER_TOKEN_RE = re.compile(r"\b\d{1,4}(?:[,.]\d{3})*(?:\.\d+)?\b")
_TIME_TOKEN_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\s*(?:am|pm|a\.m\.|p\.m\.)?\b", re.IGNORECASE)
_MONEY_MAGNITUDE_RE = re.compile(r"\b£?\s*(\d+(?:\.\d+)?)\s*(m|million|bn|billion)\b", re.IGNORECASE)


def _number_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    text = str(value or "")
    for hour, minute in _TIME_TOKEN_RE.findall(text):
        tokens.add(str(int(hour)))
        tokens.add(str(int(minute)))
        tokens.add(minute)
    for amount, magnitude in _MONEY_MAGNITUDE_RE.findall(text):
        whole = amount.split(".", 1)[0]
        if whole:
            tokens.add(whole)
        try:
            multiplier = 1_000_000_000 if magnitude.lower().startswith("b") else 1_000_000
            expanded = int(float(amount) * multiplier)
            tokens.add(str(expanded))
        except ValueError:
            pass
    for match in _NUMBER_TOKEN_RE.finditer(text):
        normalised = match.group(0).replace(",", "")
        if normalised in {"0", "00"}:
            continue
        tokens.add(normalised)
        if "." in normalised:
            head, tail = normalised.split(".", 1)
            if head:
                tokens.add(head)
            if tail:
                tokens.add(tail)
        if normalised.startswith("0"):
            stripped = normalised.lstrip("0")
            if stripped:
                tokens.add(stripped)
    return tokens


def _number_evidence_tokens(candidate: dict) -> set[str]:
    fields = [
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle")
    ]
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    fields.extend(str(event.get(key) or "") for key in ("date", "date_start", "date_end", "date_text", "price"))
    rewrite_packet = candidate.get("rewrite_packet") if isinstance(candidate.get("rewrite_packet"), dict) else {}
    fields.extend(str(value) for value in (rewrite_packet.get("allowed_numbers") or []) if str(value).strip())
    fields.extend(str(value) for value in (candidate.get("evidence_numbers") or []) if str(value).strip())
    return _number_tokens(" ".join(fields))


def _numeric_missing_tokens(candidate: dict, line: str) -> list[str]:
    line_tokens = _number_tokens(line)
    if not line_tokens:
        return []
    evidence_tokens = _number_evidence_tokens(candidate)
    return sorted(token for token in line_tokens if token not in evidence_tokens)


def _numeric_evidence_errors(candidate: dict, line: str) -> list[str]:
    category = str(candidate.get("category") or "")
    if category not in {"media_layer", "gmp", "council", "public_services", "city_news", "football", "tech_business", "food_openings"}:
        return []
    missing = _numeric_missing_tokens(candidate, line)
    if not missing:
        return []
    return [f"draft_line contains number(s) not present in candidate evidence: {', '.join(missing[:5])}."]


def _strip_unsupported_number_phrases(candidate: dict, line: str) -> tuple[str, list[str]]:
    """Remove unsupported numeric claims without dropping protected Fresh.

    This is deliberately deterministic: once the QA guard finds a number that
    is not in the saved evidence, do not ask a model to invent a "repair".
    Remove the smallest useful phrase around the number, then re-run normal
    quality checks. If the line remains readable, it ships.
    """
    missing = _numeric_missing_tokens(candidate, line)
    if not missing:
        return line, []
    repaired = str(line or "")
    reasons: list[str] = []
    for token in missing:
        escaped = re.escape(token)
        before = repaired
        # Age phrases: "50-летняя", "в возрасте 50 лет".
        repaired = re.sub(rf"\b{escaped}\s*[-‑–—]?\s*летн\w*\s*", "", repaired, flags=re.IGNORECASE)
        repaired = re.sub(rf"\s*в\s+возрасте\s+{escaped}\s+лет\b", "", repaired, flags=re.IGNORECASE)
        # Time windows and exact times: remove the unsupported time phrase,
        # not the whole news sentence.
        repaired = re.sub(
            rf"\s*(?:около|примерно|с|со|до|после|перед|к)\s+{escaped}(?::\d{{2}})?\s*(?:утра|вечера|дня|ночи|am|pm|a\.m\.|p\.m\.)?",
            "",
            repaired,
            flags=re.IGNORECASE,
        )
        repaired = re.sub(
            rf"\s*{escaped}(?::\d{{2}})?\s*(?:утра|вечера|дня|ночи|am|pm|a\.m\.|p\.m\.)",
            "",
            repaired,
            flags=re.IGNORECASE,
        )
        # If the unsupported token still survives, drop only the sentence
        # containing it. This is the final stop-loss before replacement.
        if re.search(rf"(?<!\d){escaped}(?!\d)", repaired):
            sentences = re.split(r"(?<=[.!?])\s+", repaired)
            kept = [s for s in sentences if not re.search(rf"(?<!\d){escaped}(?!\d)", s)]
            if kept:
                repaired = " ".join(kept)
        if repaired != before:
            reasons.append(f"removed_unsupported_number:{token}")
    repaired = re.sub(r"\s+", " ", repaired).strip()
    repaired = re.sub(r"\s+([,.!?])", r"\1", repaired)
    return repaired, reasons


def _draft_line_quality_errors(candidate: dict, line: str) -> list[str]:
    text = str(line or "").strip()
    errors: list[str] = []
    if not text:
        return ["Missing draft_line."]
    if not text.startswith("• "):
        errors.append("draft_line must start with bullet marker.")
    if "<a " in text.lower():
        errors.append("draft_line must not include source anchor HTML.")
    if "Почему в радаре" in text:
        errors.append("ticket radar line must not use machine explanation label.")
    if re.search(r"\*\*.+?\*\*", text) or re.search(r"(?<!\*)\*(?!\s).+?(?<!\s)\*(?!\*)", text):
        errors.append("draft_line must not use Markdown emphasis markers.")
    if not _contains_cyrillic(text):
        errors.append("draft_line must contain normal Russian prose.")
    normalized = re.sub(r"\s+", " ", text)
    if len(normalized) < 45:
        errors.append("draft_line is too short to be a self-contained item.")
    category = str(candidate.get("category") or "").strip()
    sentence_count = len(re.findall(r"[.!?]", text))
    if str(candidate.get("draft_line_provider") or "") == "writer_hard_news_recovery":
        return errors
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and sentence_count < 1:
        errors.append("draft_line must contain at least one complete sentence.")
    block_key = str(candidate.get("primary_block") or "").strip()
    # Sequential gate:
    #  1. Always-short blocks (tickets, weekend) — no min_chars at all.
    #  2. Relaxable event blocks (next_7_days, future_announcements,
    #     russian_events) — apply min ONLY when evidence_text was rich
    #     enough to write a full card. If source only gave us a thin
    #     280-char teaser, accept whatever LLM produced rather than
    #     dropping a real event for being a sentence too short.
    is_transport_block = block_key == "transport"
    if block_key == "weather" and re.search(r"\b(?:локальн\w+\s+)?радар\b", text, re.IGNORECASE):
        errors.append("weather line must not tell the reader to check a radar.")
    if is_transport_block and re.search(r"\bметро\b", text, re.IGNORECASE):
        errors.append("Metrolink/tram transport must not be called metro.")
    if is_transport_block and re.search(r"ремонтные работы на остановке [^.]{2,60}\.$", text, re.IGNORECASE):
        errors.append("transport stop works line must explain reader impact/action.")
    if _line_has_conflicting_event_date(candidate, text):
        errors.append("event date in draft_line conflicts with structured event date.")
    if re.search(r"\b(?:тройн\w*\s+ножев\w*\s+ранени|отдельн\w*\s+ножев\w*\s+атак|открыт\w*\s+вывод)", text, re.IGNORECASE):
        errors.append("incident/legal line contains literal translated legal/crime phrasing.")
    for term in _EXPLAINABLE_TERMS:
        if term in text and _EXPLAINABLE_TERMS[term] not in text:
            errors.append(f"unexplained local/entity term: {term}.")
    if category in LONG_FORMAT_CATEGORIES and block_key not in SHORT_EVENT_BLOCKS and not is_transport_block:
        evidence_len = len(str(candidate.get("evidence_text") or "").strip())
        evidence_rich = evidence_len >= EVENT_RELAX_EVIDENCE_THRESHOLD
        skip_min = (block_key in EVENT_BLOCKS_RELAXABLE) and not evidence_rich
        # A fully structured event card (real event + date + venue) is
        # allowed to be concise even when the source page evidence is
        # rich. 'Bluey's Big Play' (The Lowry) was dropped on both
        # 2026-05-27 and 2026-05-28 for a 120-char draft_line because
        # the long-format gate demanded ≥150 chars of prose for a kids'
        # show listing. A complete dated card does not need padding.
        _ev = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        _has_event_date = bool(_ev.get("is_event") and str(_ev.get("date_start") or _ev.get("date") or "").strip())
        if _has_event_date and str(_ev.get("venue") or "").strip():
            skip_min = True
        if block_key == "city_watch" and _has_clear_section_story(candidate, text):
            skip_min = True
        # Dated event with no struct venue (extractor gap) still gets a lower
        # floor instead of the full 150 — a complete short listing is not weak.
        min_chars = DATED_EVENT_MIN_CHARS if _has_event_date else LONG_FORMAT_MIN_CHARS
        if not skip_min:
            if len(normalized) < min_chars:
                errors.append(
                    f"draft_line for long-format category needs ≥{min_chars} chars (got {len(normalized)})."
                )
        if sentence_count < LONG_FORMAT_MIN_SENTENCES and block_key != "city_watch" and not (_has_event_date and _event_venue(candidate)):
            errors.append(
                f"draft_line for long-format category needs ≥{LONG_FORMAT_MIN_SENTENCES} sentences (got {sentence_count})."
            )
    lowered = text.lower()
    for marker in _BAD_EDITORIAL_PROSE_MARKERS:
        if marker in lowered:
            errors.append(f"draft_line contains bad editorial prose marker: {marker}.")
            break
    errors.extend(_sanity_flags(candidate, text))
    errors.extend(_story_frame_quality_errors(candidate, text))
    for invariant in copy_invariant_errors(candidate, text):
        errors.append(f"copy invariant failed: {invariant}.")
    errors.extend(_hallucination_flags(candidate, text))
    errors.extend(_numeric_evidence_errors(candidate, text))
    if category == "football":
        blob = _blob_for_repair(candidate)
        if (
            re.search(r"\brecord\b|рекорд", blob, re.IGNORECASE)
            and re.search(r"\b\d{2,4}\b", blob)
            and not re.search(r"\b\d{2,4}\b", text)
        ):
            errors.append("football record item needs the key number when source carries one.")
    if category in {"public_services", "council"} or str(candidate.get("source_label") or "") in {"GMMH", "Manchester Council"}:
        published_raw = str(candidate.get("published_date_london") or "")[:10]
        try:
            published_day = date.fromisoformat(published_raw)
            if (now_london().date() - published_day).days > 7 and str(candidate.get("why_now") or "") != "new_today":
                phase = str(candidate.get("change_phase") or "")
                if phase not in {"approved", "reopened", "consultation_opened", "consultation_closing", "sentenced", "charged"}:
                    errors.append("old official/public-service item needs a concrete new public reason.")
        except ValueError:
            pass
    if re.search(r"\b(?:lease|retail mix|experiential retail|10-year lease|аренд)", _blob_for_repair(candidate), re.IGNORECASE):
        if not re.search(r"\b(?:откро|opening|opens?|доступн|store|магазин|дата|from\s+\d|с\s+\d)", _blob_for_repair(candidate), re.IGNORECASE):
            errors.append("commercial/retail item needs opening/access/useful local impact.")
    # Thin-evidence + long-draft = LLM padded a teaser into a vague card.
    # We only check long-format categories (city news / events / business etc.) —
    # transport / weather are intentionally short. Football already has its own
    # "return draft_line=\"\"" rule in the prompt.
    if category in LONG_FORMAT_CATEGORIES and category != "football" and not is_transport_block:
        evidence = str(candidate.get("evidence_text") or candidate.get("summary") or candidate.get("lead") or "")
        evidence_meaningful = len(re.sub(r"\s+", " ", evidence).strip())
        draft_len = len(normalized)
        # Concrete signals: numbers, £-amount, date, capitalised proper noun pair.
        has_concrete = bool(
            re.search(r"\b\d{2,}", text)
            or re.search(r"£\s*\d", text)
            or re.search(r"\b(?:января|февраля|марта|апреля|мая|июня|июля|"
                         r"августа|сентября|октября|ноября|декабря)\b", text, re.IGNORECASE)
            or re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", text)
        )
        if evidence_meaningful < 150 and draft_len > 220 and not has_concrete:
            errors.append(
                f"draft_line padded from thin evidence "
                f"(evidence={evidence_meaningful}c, draft={draft_len}c, no concrete signal)."
            )
    return errors


_FOOTBALL_SPORT_RE = re.compile(
    r"\b(?:match|fixture|result|score|goal|injur|fitness|transfer|sign(?:s|ed|ing)?|"
    r"contract|loan|squad|line[- ]?up|team news|manager|coach|tournament|cup|"
    r"league|champions league|world cup|europa|premier league|wsl|fa cup|"
    r"call[- ]?up|debut|suspension|ban|training return|ruled out|available)\b",
    re.IGNORECASE,
)
_FOOTBALL_SOFT_RE = re.compile(
    r"\b(?:birthday|break[- ]?up|girlfriend|boyfriend|maya jama|personal life|"
    r"fan reaction|fans react|social media|instagram|party|gossip|rumour|"
    r"speculation|shirt launch|kit launch|award|charity|community|documentary|"
    r"amazon|prime video|behind[- ]the[- ]scenes|poll|vote|supporters?|fans?)\b",
    re.IGNORECASE,
)
_FOOTBALL_HARD_NEWS_RE = re.compile(
    r"\b(?:match|fixture|result|score|goal|injur|fitness|transfer|sign(?:s|ed|ing)?|"
    r"contract|loan|squad|line[- ]?up|team news|appoint(?:s|ed|ment)?|"
    r"negotiat(?:e|es|ed|ions?)|bid|rejected|accepted|available|ruled out|"
    r"debut|call[- ]?up|suspension|ban|training return)\b",
    re.IGNORECASE,
)


def _football_is_sport_news(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "football":
        return False
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_url")
    )
    return bool(_FOOTBALL_SPORT_RE.search(blob))


def _football_should_route_to_soft(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "football":
        return False
    if not _football_is_sport_news(candidate):
        return True
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "source_url")
    )
    return bool(_FOOTBALL_SOFT_RE.search(blob) and not _FOOTBALL_HARD_NEWS_RE.search(blob))


_NON_GM_REGIONAL_RE = re.compile(
    r"\b(?:southport|liverpool|lancashire|cheshire|yorkshire|cumbria|london|devon|north-west|north west)\b",
    re.IGNORECASE,
)
_GM_TEXT_RE = re.compile(
    r"\b(?:greater manchester|manchester|salford|trafford|stockport|tameside|oldham|"
    r"rochdale|bury|bolton|wigan|denton|burnage|radcliffe|fallowfield|prestwich|"
    r"altrincham|stretford|withington|levenshulme|rochdale|middleton|old trafford)\b",
    re.IGNORECASE,
)
_SOFT_TOP_NEWS_RE = re.compile(
    r"\b(?:guinness world record|marathon costume|charity challenge|laughing stock|"
    r"most-viewed home|rightmove|best places|pretty villages|mum earning|benefits.*struggling)\b",
    re.IGNORECASE,
)


def _top_news_route_or_drop(candidate: dict) -> str:
    block = str(candidate.get("primary_block") or "")
    if block not in {"last_24h", "today_focus"}:
        return ""
    category = str(candidate.get("category") or "")
    if category not in {"media_layer", "gmp", "public_services", "city_news", "council"}:
        return ""
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    story_type = str(contract.get("story_type") or "")
    frame = contract.get("story_frame") if isinstance(contract.get("story_frame"), dict) else {}
    why_now = str(frame.get("why_now") or candidate.get("why_now") or "")
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text")
    )
    if not why_now_is_publishable(why_now):
        return "city_watch"
    if story_type in {"human_interest", "soft_news", "day_out_guide", "property_listing"} or _SOFT_TOP_NEWS_RE.search(text):
        return "city_watch"
    if _NON_GM_REGIONAL_RE.search(text) and not _GM_TEXT_RE.search(text):
        return "drop_non_gm_regional"
    return ""


def _should_defer_next_7_market(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "next_7_days":
        return False
    if now_london().weekday() >= 3:
        return False
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    if str(contract.get("event_shape") or "") != "recurring":
        return False
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "source_label"))
    return bool(_MARKET_EVENT_RE.search(blob))


_MINOR_BUS_STOP_RE = re.compile(
    r"\bАвтобусы:\s+(?:закрыт[аы]\s+остановк[аи]|остановк[аи][^.]{0,80}\s+закрыт[аы])\b",
    re.IGNORECASE,
)


def _is_minor_bus_stop_line(line: str) -> bool:
    text = re.sub(r"<[^>]+>", " ", str(line or ""))
    if not _MINOR_BUS_STOP_RE.search(text):
        return False
    return not re.search(r"\b(?:объезд|закрыты дороги|нет автобусов|маршрут[ыа]?|замещающ)\b", text, re.IGNORECASE)


def _transport_line_priority(line: str, score: float = 0.0) -> float:
    text = re.sub(r"<[^>]+>", " ", str(line or "")).lower()
    priority = float(score)
    if re.search(r"\b(?:metrolink|трамва|tram|shudehill|market street|bury line|rochdale line|ashton line|eccles line)\b", text):
        priority += 1000
    elif re.search(r"\b(?:national rail|northern|transpennine|transport for wales|поезд|piccadilly|victoria|salford crescent|airport)\b", text):
        priority += 700
    elif re.search(r"\b(?:m6|m60|m62|m56|road closed|закрыты дороги|объезд|diversion|route)\b", text):
        priority += 420
    elif _is_minor_bus_stop_line(line):
        priority -= 100
    elif "автобус" in text:
        priority += 120
    return priority


def _extract_bus_stop_label(line: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(line or ""))
    text = re.sub(r"\s+", " ", text).strip(" .")
    patterns = (
        r"остановка\s+на\s+([^.;—]+?)(?:\s+закрыта|\s+закрыт|\s+из-за|;|\.|$)",
        r"остановки\s+у\s+([^.;—]+?)(?:\s+закрыты|\s+из-за|;|\.|$)",
        r"закрыта\s+остановка\s+на\s+([^.;—]+?)(?:\s+из-за|;|\.|$)",
        r"закрыты\s+остановки\s+у\s+([^.;—]+?)(?:\s+из-за|;|\.|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            label = match.group(1).strip(" ,")
            label = re.sub(r"\s*\(пересечение с [^)]+\)", "", label, flags=re.IGNORECASE)
            return label[:50]
    return text.replace("• Автобусы:", "").strip()[:50]


def _cap_minor_bus_stop_lines(lines: list[str], srcs: list[str], fps: list[str], scores: list[float], titles: list[str]) -> tuple[list[str], list[str], list[str], list[float], list[str], list[int]]:
    ranked_idx = sorted(
        range(len(lines)),
        key=lambda idx: _transport_line_priority(lines[idx], scores[idx] if idx < len(scores) else 0.0),
        reverse=True,
    )
    lines = [lines[i] for i in ranked_idx]
    srcs = [srcs[i] if i < len(srcs) else "" for i in ranked_idx]
    fps = [fps[i] if i < len(fps) else "" for i in ranked_idx]
    scores = [scores[i] if i < len(scores) else 0.0 for i in ranked_idx]
    titles = [titles[i] if i < len(titles) else "" for i in ranked_idx]
    minor_indices = [idx for idx, line in enumerate(lines) if _is_minor_bus_stop_line(line)]
    if len(minor_indices) < 3:
        return lines, srcs, fps, scores, titles, []
    first_minor = minor_indices[0]
    labels = [_extract_bus_stop_label(lines[idx]) for idx in minor_indices]
    labels = [label for label in dict.fromkeys(labels) if label]
    anchor = ""
    anchor_match = re.search(r'\s*(<a\s+href="[^"]+">[^<]+</a>)\s*$', lines[first_minor])
    if anchor_match:
        anchor = f" {anchor_match.group(1)}"
    label_text = ", ".join(labels[:5])
    if len(labels) > 5:
        label_text += f" и ещё {len(labels) - 5}"
    grouped_line = (
        f"• Автобусы: {len(minor_indices)} мелких закрытий остановок из-за работ: "
        f"{label_text}. Используйте соседние остановки.{anchor}"
    )
    kept: list[int] = [idx for idx in range(len(lines)) if idx not in minor_indices]
    kept.insert(first_minor, first_minor)
    dropped = [idx for idx in minor_indices if idx != first_minor]
    lines[first_minor] = grouped_line
    scores[first_minor] = max([scores[idx] if idx < len(scores) else 0.0 for idx in minor_indices] + [0.0]) - 25
    titles[first_minor] = f"{len(minor_indices)} bus stop closures"
    return (
        [lines[i] for i in kept],
        [srcs[i] if i < len(srcs) else "" for i in kept],
        [fps[i] if i < len(fps) else "" for i in kept],
        [scores[i] if i < len(scores) else 0.0 for i in kept],
        [titles[i] if i < len(titles) else "" for i in kept],
        dropped,
    )


def write_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "writer_report.json"

    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])
    candidate_by_fp = {
        str(candidate.get("fingerprint") or ""): candidate
        for candidate in candidates
        if isinstance(candidate, dict)
    }
    ticket_notability_report: list[dict[str, object]] = []
    ticket_notability_cache = state_dir / "ticket_notability_cache.json"
    # Warm the notability cache in parallel BEFORE the render loop so the loop
    # only reads it (no per-ticket network on the critical render path). This is
    # what kept the writer at ~6min on 2026-06-11: ~100 serial artist lookups.
    notability_prefetch = prefetch_notability(candidates, ticket_notability_cache)
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        if str(candidate.get("category") or "") != "venues_tickets" and str(candidate.get("primary_block") or "") not in {
            "ticket_radar",
            "outside_gm_tickets",
            "russian_events",
        }:
            continue
        notability = enrich_ticket_notability(candidate, ticket_notability_cache)
        candidate["ticket_notability"] = {
            "artist": notability.artist,
            "kind": notability.kind,
            "tier": notability.tier,
            "confidence": notability.confidence,
            "signal": notability.signal,
            "wikidata_id": notability.wikidata_id,
            "sitelinks": notability.sitelinks,
            "headliners": list(notability.headliners),
            "signals": notability.signals or {},
        }
        _append_recovery_step(candidate, "ticket_notability", "scored")
        decision = _ticket_watch_decision(candidate)
        ticket_notability_report.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "source_label": candidate.get("source_label"),
                "primary_block": candidate.get("primary_block"),
                "artist": notability.artist,
                "kind": notability.kind,
                "tier": notability.tier,
                "confidence": notability.confidence,
                "signal": notability.signal,
                "headliners": list(notability.headliners),
                "signals": notability.signals or {},
                "score": decision["score"],
                "decision": decision["decision"],
                "threshold": decision["threshold"],
                "ticket_type": decision["ticket_type"],
                "reasons": decision["reasons"],
            }
        )
    llm_degraded, llm_rewrite_report = _llm_rewrite_is_degraded(state_dir)
    sections = {heading: [] for heading in PRIMARY_BLOCKS.values()}
    # Parallel list of source_labels per section (same indices as sections[*]).
    # Used to apply SECTION_MAX_PER_SOURCE caps at render time.
    section_sources: dict[str, list[str]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    # Parallel list of candidate fingerprints per section. This is written
    # after all caps/filtering so published_facts only records items that
    # actually reached the Telegram HTML.
    section_fingerprints: dict[str, list[str]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    # Editorial priority score per line — populated only for «Городской радар»
    # where we re-sort candidates before truncation so the cap drops the
    # weakest items (PR releases) rather than whatever happened to come last.
    section_scores: dict[str, list[float]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    section_titles: dict[str, list[str]] = {h: [] for h in PRIMARY_BLOCKS.values()}
    errors: list[str] = []
    warnings: list[str] = []
    quality_counts = {
        "included_candidates": 0,
        "rendered_candidates": 0,
        "blocked_for_quality": 0,
        "held_for_editorial_quality": 0,
        "dropped_missing_draft_line": 0,
        "dropped_ticket_not_selected": 0,
        "dropped_english_passthrough": 0,
        "dropped_low_quality": 0,
    }
    dropped_candidates: list[dict[str, object]] = []
    degraded_shrink_dropped: list[dict[str, object]] = []
    global_budget_dropped: list[dict[str, object]] = []

    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        attach_editorial_contract(candidate)
        _append_recovery_step(
            candidate,
            "story_frame",
            "attached",
            missing=(candidate.get("story_frame") or {}).get("missing_facts") or [],
        )
        quality_counts["included_candidates"] += 1
        contract_drop_reason = _contract_public_drop_reason(candidate)
        if contract_drop_reason and candidate.get("manual_override") != "force_include":
            warnings.append(f"Candidate #{index} dropped by editorial contract: {contract_drop_reason}.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": [contract_drop_reason],
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                }
            )
            continue
        if (
            candidate.get("editorial_status") == "borderline"
            and candidate.get("manual_override") != "force_include"
        ):
            warnings.append(f"Candidate #{index} held for manual review: borderline editorial status.")
            quality_counts["held_for_editorial_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["Held for manual review: borderline editorial status."],
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                }
            )
            continue
        if candidate.get("validation_errors"):
            errors.append(f"Candidate #{index} is include=true but still has validation_errors.")
            quality_counts["blocked_for_quality"] += 1
            continue
        if not candidate.get("source_url") or not candidate.get("source_label"):
            errors.append(f"Candidate #{index} is include=true but missing source reference.")
            quality_counts["blocked_for_quality"] += 1
            continue
        # practical_angle is no longer a hard gate: the new long-format prompts
        # derive the "so what" sentence directly from evidence_text, so an
        # empty / placeholder practical_angle should not block rendering.
        practical_angle = str(candidate.get("practical_angle") or "").strip()
        if not practical_angle:
            warnings.append(f"Candidate #{index}: empty practical_angle (kept).")
        elif is_placeholder_practical_angle(practical_angle):
            warnings.append(f"Candidate #{index}: placeholder practical_angle (kept).")
        if str(candidate.get("primary_block") or "") == "last_24h" and not str(candidate.get("published_at") or "").strip():
            errors.append(f"Candidate #{index} is in last_24h without published_at.")
            quality_counts["blocked_for_quality"] += 1
            continue
        if _is_outside_current_weekend_candidate(candidate):
            warnings.append(f"Candidate #{index} dropped: outside current weekend window.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["Outside current weekend window."],
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                }
            )
            continue
        if _is_expired_event_candidate(candidate, str(candidate.get("draft_line") or "")):
            warnings.append(f"Candidate #{index} dropped: expired event date.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["Expired event date."],
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                }
            )
            continue

        if _football_should_route_to_soft(candidate):
            candidate["primary_block"] = "city_watch"
            candidate["football_soft_routed"] = True
            warnings.append(
                f"Candidate #{index}: football soft item routed to «Городской радар»; it does not count toward football minimum."
            )
        top_news_route = _top_news_route_or_drop(candidate)
        if top_news_route == "city_watch":
            candidate["primary_block"] = "city_watch"
            warnings.append(
                f"Candidate #{index}: soft/top-news item routed to «Городской радар» instead of top news."
            )
        if _should_defer_next_7_market(candidate):
            candidate["primary_block"] = "future_announcements"
            warnings.append(
                f"Candidate #{index}: recurring market deferred from «Что важно в ближайшие 7 дней» early in the week."
            )
        elif top_news_route == "drop_non_gm_regional" and candidate.get("manual_override") != "force_include":
            warnings.append(f"Candidate #{index} dropped: regional story is outside Greater Manchester.")
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": str(candidate.get("title") or ""),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": ["regional story outside Greater Manchester."],
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                }
            )
            continue

        block_key = str(candidate.get("primary_block") or "").strip()
        section_name = PRIMARY_BLOCKS.get(block_key)
        if not section_name:
            errors.append(f"Candidate #{index} has unknown primary_block: {block_key!r}.")
            quality_counts["blocked_for_quality"] += 1
            continue

        line = str(candidate.get("draft_line") or "").strip()
        title = str(candidate.get("title") or "").strip()
        lead = str(candidate.get("lead") or "").strip()
        summary = str(candidate.get("summary") or "").strip()
        source_label = str(candidate.get("source_label") or "").strip()
        source_url = str(candidate.get("source_url") or "").strip()
        category = str(candidate.get("category") or "").strip()

        timing_decision, timing_reason = _section_event_timing_decision(candidate)
        if timing_decision == "move_future":
            candidate["primary_block"] = "future_announcements"
            block_key = "future_announcements"
            section_name = PRIMARY_BLOCKS.get(block_key) or section_name
            warnings.append(
                f"Candidate #{index}: moved from «Что важно в ближайшие 7 дней» "
                f"to «Дальние анонсы» ({timing_reason})."
            )
        elif timing_decision == "move_next_7":
            candidate["primary_block"] = "next_7_days"
            block_key = "next_7_days"
            section_name = PRIMARY_BLOCKS.get(block_key) or section_name
            warnings.append(
                f"Candidate #{index}: moved from «Дальние анонсы» "
                f"to «Что важно в ближайшие 7 дней» ({timing_reason})."
            )
        elif timing_decision == "hold":
            warnings.append(
                f"Candidate #{index} held: event timing is not suitable for «{section_name}» ({timing_reason})."
            )
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": title,
                    "category": category,
                    "primary_block": block_key,
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": [f"Event timing unsuitable for section: {timing_reason}."],
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                    "recovery_plan": candidate.get("recovery_plan") or {},
                }
            )
            continue

        if _normalize_text_key(lead) and _normalize_text_key(lead) == _normalize_text_key(summary):
            summary = ""

        english_detected = False
        if category in {"media_layer", "gmp", "public_services", "city_news", "council", "transport", "venues_tickets", "russian_speaking_events", "culture_weekly", "football", "tech_business", "food_openings"}:
            english_fields = [field for field in (lead, summary, title) if _looks_like_untranslated_english(field)]
            if english_fields:
                english_detected = True

        if not line and category == "transport":
            _append_recovery_step(candidate, "transport_card_recovery", "attempted", missing=(candidate.get("story_frame") or {}).get("missing_facts") or [])
            line = _build_transport_fallback_line(candidate)
            if line:
                _append_recovery_step(candidate, "transport_card_recovery", "recovered")
                warnings.append(f"Candidate #{index}: transport location recovered from URL/title (no LLM draft_line).")
                logger.info("TRANSPORT location recovery | %s | %s", block_key, title[:80])
            else:
                _append_recovery_step(candidate, "transport_card_recovery", "held", missing=["transport_impact"])
                warnings.append(f"Candidate #{index}: transport item held — no location recoverable from URL/title.")
                logger.info("HOLD transport_no_usable_card | %s | %s", block_key, title[:80])

        if not line and category == "venues_tickets":
            _append_recovery_step(candidate, "ticket_structured_recovery", "attempted", missing=(candidate.get("story_frame") or {}).get("missing_facts") or [])
            line = _build_ticket_fallback_line(candidate)
            if line:
                _append_recovery_step(candidate, "ticket_structured_recovery", "recovered")
                warnings.append(f"Candidate #{index}: ticket structured fallback used (no LLM draft_line).")
                logger.info("TICKET structured fallback | %s | %s", block_key, title[:80])
            else:
                _append_recovery_step(candidate, "ticket_structured_recovery", "held", missing=["artist_or_date_or_venue_or_notability"])
                warnings.append(f"Candidate #{index}: ticket held because structured fields were incomplete or dirty.")
                logger.info("HOLD ticket_dirty_or_incomplete | %s | %s", block_key, title[:80])

        if not line and category == "public_services":
            _append_recovery_step(candidate, "public_service_recovery", "attempted")
            line = _build_public_service_fallback_line(candidate)
            _append_recovery_step(candidate, "public_service_recovery", "recovered")
            warnings.append(f"Candidate #{index}: public-services fallback stub used (no LLM draft_line).")
            logger.info("TIER4 public_services stub | %s | %s", block_key, title[:80])

        # Protected weekend events / culture_weekly fallback: when the
        # LLM did not write a draft_line and the item is in a protected
        # lane (weekend_market / russian_event) with structured event
        # fields, do not drop — assemble a deterministic card.
        if not line and category in {"culture_weekly", "russian_speaking_events", "diaspora_events"}:
            protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
            event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
            if (
                (protected.get("protected") or block_key in {"weekend_activities", "next_7_days", "russian_events"})
                and event.get("is_event")
                and str(event.get("event_name") or candidate.get("title") or "").strip()
            ):
                _append_recovery_step(candidate, "event_structured_recovery", "attempted", missing=(candidate.get("story_frame") or {}).get("missing_facts") or [])
                line = _build_event_fallback_line(candidate)
                if line:
                    _append_recovery_step(candidate, "event_structured_recovery", "recovered")
                    warnings.append(f"Candidate #{index}: event fallback stub used (no LLM draft_line).")
                    logger.info("TIER4 event stub | %s | %s", block_key, title[:80])
                else:
                    _append_recovery_step(candidate, "event_structured_recovery", "held", missing=(candidate.get("story_frame") or {}).get("missing_facts") or [])

        if not line:
            _append_recovery_step(candidate, "official_football_recovery", "attempted")
            line = _build_football_fallback_line(candidate)
            if line:
                _append_recovery_step(candidate, "official_football_recovery", "recovered")
                warnings.append(f"Candidate #{index}: official football fallback used after missing model draft_line.")

        if not line:
            _append_recovery_step(candidate, "hard_news_recovery", "attempted")
            recovery_line = _hard_news_recovery_line(candidate)
            if recovery_line:
                line = recovery_line
                candidate["draft_line_provider"] = "writer_hard_news_recovery"
                candidate["draft_line_model"] = "deterministic_hard_news_recovery"
                _append_recovery_step(candidate, "hard_news_recovery", "recovered")
                warnings.append(f"Candidate #{index}: hard-news recovery line used after missing model draft_line.")
                logger.info("RECOVER hard_news | %s | %s", block_key, title[:80])

        if not line:
            if category == "venues_tickets":
                decision = _ticket_watch_decision(candidate)
                reasons = [str(reason) for reason in decision.get("reasons") or [] if str(reason).strip()]
                if not reasons:
                    reasons = ["ticket did not meet public radar criteria"]
                _append_recovery_step(candidate, "ticket_public_selection", "held", missing=reasons)
                warnings.append(
                    f"Candidate #{index}: ticket not selected for public radar ({'; '.join(reasons)})."
                )
                logger.info("HOLD ticket_not_selected | %s | %s | %s", block_key, title[:80], "; ".join(reasons))
                quality_counts["dropped_ticket_not_selected"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": [f"Ticket not selected: {reason}" for reason in reasons],
                        "ticket_watch": decision,
                        "story_frame": candidate.get("story_frame") or {},
                        "recovery_trace": candidate.get("recovery_trace") or [],
                        "recovery_plan": candidate.get("recovery_plan") or {},
                    }
                )
                continue
            if category in REQUIRE_DRAFT_LINE_CATEGORIES:
                _append_recovery_step(candidate, "final_hold", "held", missing=(candidate.get("story_frame") or {}).get("missing_facts") or ["draft_line"])
                warnings.append(f"Candidate #{index} dropped: no model draft_line for {category!r}.")
                logger.info("DROP no_draft_line | %s | %s | %s", category, block_key, title[:80])
                quality_counts["dropped_missing_draft_line"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": ["Missing draft_line."],
                        "story_frame": candidate.get("story_frame") or {},
                        "recovery_trace": candidate.get("recovery_trace") or [],
                        "recovery_plan": candidate.get("recovery_plan") or {},
                    }
                )
                continue
            if english_detected:
                warnings.append(f"Candidate #{index} dropped: English passthrough without translation.")
                logger.info("DROP english_passthrough | %s | %s | %s", category, block_key, title[:80])
                quality_counts["dropped_english_passthrough"] += 1
                dropped_candidates.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": title,
                        "category": category,
                        "primary_block": block_key,
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": ["Untranslated English."],
                        "story_frame": candidate.get("story_frame") or {},
                        "recovery_trace": candidate.get("recovery_trace") or [],
                        "recovery_plan": candidate.get("recovery_plan") or {},
                    }
                )
                continue
            headline = lead or title or summary
            rendered_parts: list[str] = []
            if headline:
                rendered_parts.append(html.escape(headline.rstrip(".")) + ".")
            if _summary_is_useful(summary, headline):
                rendered_parts.append(html.escape(summary.rstrip(".")) + ".")
            line = "• " + " ".join(rendered_parts).strip()

        scrubbed_line, removed_vague_endings = scrub_vague_ending(line)
        if removed_vague_endings:
            warnings.append(
                f"Candidate #{index}: removed vague ending(s): {', '.join(removed_vague_endings)}."
            )
            line = scrubbed_line
        if _line_claims_future_ticket_sale(candidate, line):
            line = _build_ticket_fallback_line(candidate)
            warnings.append(
                f"Candidate #{index}: replaced stale ticket-sale wording with deterministic ticket line."
            )
        line, repair_reasons = _repair_editorial_contract_line(candidate, line)
        if repair_reasons:
            warnings.append(
                f"Candidate #{index}: editorial contract repaired line ({', '.join(repair_reasons)})."
            )

        draft_line_errors = _draft_line_quality_errors(candidate, line)
        if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
            numeric_errors = [err for err in draft_line_errors if err.startswith("draft_line contains number(s) not present")]
            if numeric_errors:
                stripped_line, strip_repairs = _strip_unsupported_number_phrases(candidate, line)
                if strip_repairs and stripped_line != line:
                    stripped_errors = _draft_line_quality_errors(candidate, stripped_line)
                    if not stripped_errors:
                        line = stripped_line
                        draft_line_errors = []
                        _append_recovery_step(candidate, "draft_line_quality_repair", "recovered", missing=strip_repairs)
                        warnings.append(
                            f"Candidate #{index}: removed unsupported numeric claim(s) instead of dropping ({', '.join(strip_repairs)})."
                        )
                    else:
                        _append_recovery_step(candidate, "draft_line_quality_repair", "attempted", missing=strip_repairs + stripped_errors)
                        draft_line_errors = stripped_errors
        if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
            _append_recovery_step(candidate, "final_replacement", "attempted", missing=(candidate.get("story_frame") or {}).get("missing_facts") or draft_line_errors)
            replacement = _final_replacement_line(candidate)
            if replacement and replacement != line:
                replacement, replacement_repairs = _repair_editorial_contract_line(candidate, replacement)
                replacement_errors = _draft_line_quality_errors(candidate, replacement)
                if not replacement_errors:
                    line = replacement
                    draft_line_errors = []
                    _append_recovery_step(candidate, "final_replacement", "recovered")
                    warnings.append(
                        f"Candidate #{index}: final quality check replaced bad public line ({', '.join(replacement_repairs) or 'deterministic fallback'})."
                    )
                else:
                    _append_recovery_step(candidate, "final_replacement", "held", missing=replacement_errors)
        if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
            _append_recovery_step(candidate, "draft_line_quality_repair", "held", missing=(candidate.get("story_frame") or {}).get("missing_facts") or draft_line_errors)
            warnings.append(
                f"Candidate #{index} dropped: draft_line quality issues ({'; '.join(draft_line_errors)})."
            )
            logger.info("DROP low_quality | %s | %s | %s | %s", category, block_key, title[:80], "; ".join(draft_line_errors))
            quality_counts["dropped_low_quality"] += 1
            dropped_candidates.append(
                {
                    "fingerprint": candidate.get("fingerprint"),
                    "title": title,
                    "category": category,
                    "primary_block": block_key,
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": draft_line_errors,
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                    "recovery_plan": candidate.get("recovery_plan") or {},
                }
            )
            continue

        # Scrub LLM placeholder genres like "Madison Beer, жанр не указан" /
        # "Avatar, другой жанр" — the rewrite prompt says "не выдумывай жанр"
        # but gpt-4o-mini still tacks these phrases on. Strip them post-hoc.
        line = re.sub(r",\s*(?:жанр\s+не\s+указан|другой\s+жанр|жанр\s+не\s+определ[её]н|жанр\s+неизвестен)\s*(?=[.!?]|$)", "", line, flags=re.IGNORECASE)
        # Restore English spellings for GM toponyms (Altrincham, Bury, Wigan, ...).
        line = restore_english_toponyms(line)
        if candidate.get("is_lead"):
            # Lead story: no bullet, bold first sentence, placed in main_story block
            line = line.lstrip("• ").strip()
            sentences = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)
            if len(sentences) == 2:
                line = f"<b>{sentences[0]}</b> {sentences[1]}"
            else:
                line = f"<b>{line}</b>"
            line = preserve_place_names(line)
            line = _attach_source_anchor(line, source_url, source_label)
            sections.setdefault("Главная история дня", []).insert(0, line)
            section_sources.setdefault("Главная история дня", []).insert(0, source_label)
            section_scores.setdefault("Главная история дня", []).insert(0, 0.0)
            section_fingerprints.setdefault("Главная история дня", []).insert(0, str(candidate.get("fingerprint") or "").strip())
            section_titles.setdefault("Главная история дня", []).insert(0, title)
        else:
            if not line.startswith("• "):
                line = f"• {line}"
            line = preserve_place_names(line)
            line = _attach_source_anchor(line, source_url, source_label)
            sections[section_name].append(line)
            section_sources[section_name].append(source_label)
            section_fingerprints[section_name].append(str(candidate.get("fingerprint") or "").strip())
            section_scores[section_name].append(_section_priority_score(candidate, section_name, line))
            section_titles[section_name].append(title)

    missing_draft_count = quality_counts["dropped_missing_draft_line"]
    if missing_draft_count:
        warnings.append(
            f"Writer dropped {missing_draft_count} included candidate(s) with missing draft_line — digest continues."
        )

    today_focus_board = _allocate_fresh_and_today_focus(
        sections,
        section_sources,
        section_scores,
        section_fingerprints,
        section_titles,
        candidate_by_fp,
    )
    backfilled_today_focus = 0
    if int(today_focus_board.get("moved_from_fresh") or 0) or int(today_focus_board.get("moved_from_city_watch") or 0):
        warnings.append(
            f"Writer board filled «{TODAY_FOCUS_SECTION}» with "
            f"{today_focus_board.get('rendered_candidates')} practical item(s) "
            f"(moved from Fresh: {today_focus_board.get('moved_from_fresh')}, "
            f"from City Radar: {today_focus_board.get('moved_from_city_watch')})."
        )
    if not sections.get("Общественный транспорт сегодня"):
        sections["Общественный транспорт сегодня"] = [_transport_empty_line(project_root)]
        section_sources["Общественный транспорт сегодня"] = ["TfGM"]
        section_scores["Общественный транспорт сегодня"] = [0.0]
        section_fingerprints["Общественный транспорт сегодня"] = [""]
        section_titles["Общественный транспорт сегодня"] = ["Транспорт проверен"]
        warnings.append("Writer added honest empty-transport coverage line.")
    final_section_routing = _apply_final_section_role_routing(
        sections,
        section_sources,
        section_scores,
        section_fingerprints,
        section_titles,
        candidate_by_fp,
        warnings,
    )

    rendered: list[str] = [_title_line(), ""]

    # "Выходные в GM" показываем только с четверга (weekday >= 3)
    london_weekday = now_london().weekday()  # 0=Пн … 6=Вс
    show_weekend = london_weekday >= 3

    ordered_sections = [
        "Погода",
        "Главная история дня",
        "Свежие новости",
        "Общественный транспорт сегодня",
        "Что важно сегодня",
        "Футбол",
        *(["Выходные в GM"] if show_weekend else []),
        "Городской радар",
        "Что важно в ближайшие 7 дней",
        "Дальние анонсы",
        "Билеты / Ticket Radar",
        "Крупные концерты вне GM",
        "Русскоязычные концерты и стендап UK",
        "Еда, открытия и рынки",
        "IT и бизнес",
        "Радар по районам",
    ]
    section_counts: dict[str, int] = {}
    rendered_candidate_fingerprints: list[str] = []
    visible_item_count = 0
    for section_index, section_name in enumerate(ordered_sections):
        lines = sections.get(section_name, [])
        if not lines:
            continue
        srcs = section_sources.get(section_name, [])
        scores = section_scores.get(section_name, [])
        fps = section_fingerprints.get(section_name, [])
        titles = section_titles.get(section_name, [])
        # Re-rank capped sections so the cap keeps practical local value,
        # rather than whichever source happened to run first.
        if (section_name in SECTION_MAX_ITEMS or section_name == "Выходные в GM") and scores:
            triples = sorted(
                zip(lines, srcs + [""] * (len(lines) - len(srcs)),
                    scores + [0.0] * (len(lines) - len(scores)),
                    fps + [""] * (len(lines) - len(fps)),
                    titles + [""] * (len(lines) - len(titles))),
                key=lambda triple: triple[2],
                reverse=True,
            )
            lines = [t[0] for t in triples]
            srcs = [t[1] for t in triples]
            fps = [t[3] for t in triples]
            scores = [t[2] for t in triples]
            titles = [t[4] for t in triples]
        per_source_cap = SECTION_MAX_PER_SOURCE.get(section_name)
        if per_source_cap:
            src_counts: dict[str, int] = {}
            filtered: list[str] = []
            filtered_srcs: list[str] = []
            filtered_fps: list[str] = []
            filtered_scores: list[float] = []
            filtered_titles: list[str] = []
            for idx, ln in enumerate(lines):
                src = srcs[idx] if idx < len(srcs) else ""
                fp = fps[idx] if idx < len(fps) else ""
                if (
                    src_counts.get(src, 0) >= per_source_cap
                    and not _is_public_budget_exempt(section_name, candidate_by_fp.get(str(fp or "")))
                    and not (
                        section_name == "Свежие новости"
                        and _fresh_hard_news_can_bypass_source_cap(candidate_by_fp.get(str(fp or "")), ln)
                    )
                ):
                    continue
                src_counts[src] = src_counts.get(src, 0) + 1
                filtered.append(ln)
                filtered_srcs.append(src)
                filtered_fps.append(fps[idx] if idx < len(fps) else "")
                filtered_scores.append(scores[idx] if idx < len(scores) else 0.0)
                filtered_titles.append(titles[idx] if idx < len(titles) else "")
            min_items = SECTION_MIN_ITEMS.get(section_name, 0)
            if not min_items or len(filtered) >= min_items or len(lines) < min_items:
                lines = filtered
                srcs = filtered_srcs
                fps = filtered_fps
                scores = filtered_scores
                titles = filtered_titles
        if section_name == "Общественный транспорт сегодня":
            lines, srcs, fps, scores, titles, minor_bus_dropped = _cap_minor_bus_stop_lines(lines, srcs, fps, scores, titles)
            if minor_bus_dropped:
                warnings.append(
                    f"Transport impact contract: held {len(minor_bus_dropped)} minor bus-stop closure(s) after top 3."
                )
        normal_cap = SECTION_MAX_ITEMS.get(section_name)
        degraded_cap = DEGRADED_LLM_SECTION_MAX_ITEMS.get(section_name) if llm_degraded else None
        cap = normal_cap
        if degraded_cap is not None:
            cap = min(cap, degraded_cap) if cap else degraded_cap
        if cap:
            if llm_degraded and degraded_cap is not None and len(lines) > cap:
                normal_cutoff = normal_cap if normal_cap is not None else len(lines)
                for idx in range(cap, min(len(lines), normal_cutoff)):
                    if _is_public_budget_exempt(section_name, candidate_by_fp.get(str(fps[idx] if idx < len(fps) else ""))):
                        continue
                    degraded_shrink_dropped.append(
                        {
                            "section": section_name,
                            "fingerprint": fps[idx] if idx < len(fps) else "",
                            "title": titles[idx] if idx < len(titles) else re.sub(r"<[^>]+>", "", lines[idx])[:120],
                            "source_label": srcs[idx] if idx < len(srcs) else "",
                            "reader_value_score": scores[idx] if idx < len(scores) else 0.0,
                            "reason": "LLM rewrite was degraded; held lower-priority item for review.",
                        }
                    )
            lines, srcs, fps, scores, titles, _cap_dropped_idx, _ = _slice_counting_only_non_exempt(
                lines=lines,
                srcs=srcs,
                fps=fps,
                scores=scores,
                titles=titles,
                candidate_by_fp=candidate_by_fp,
                section_name=section_name,
                counted_limit=cap,
                ignore_section_exemption=True,
            )
        # Section min-floor pull-back. On 2026-05-27 «Главная история
        # дня»=1 and «Что важно сегодня»=2 while score-10 candidates
        # sat with include=True but never made it through the writer
        # (no draft_line, dropped by section cap elsewhere, etc.).
        # Closes A2 + C3: if we're still below the editorial floor for
        # this section, top up from not-yet-rendered included
        # candidates that targeted this block, sorted by reader_value.
        min_floor = SECTION_MIN_ITEMS.get(section_name, 0)
        target_floor = FRESH_NEWS_TARGET_ITEMS if section_name == "Свежие новости" else min_floor
        if target_floor and len(lines) < target_floor:
            rendered_fps_so_far = (
                {fp for slist in section_fingerprints.values() for fp in slist if fp}
                | {fp for fp in fps if fp}
            )
            lines, fps, scores, titles, srcs = _apply_section_min_floor_pull_back(
                section_name, lines, fps, scores, titles, srcs,
                candidates, rendered_fps_so_far, target_floor, warnings,
                include_backup=section_name in {"Свежие новости", "Футбол", "Городской радар"},
            )
        reserved_later_budget = _reserved_later_budget(ordered_sections, section_index, sections)
        remaining_budget = PUBLIC_DIGEST_MAX_VISIBLE_ITEMS - visible_item_count - reserved_later_budget
        if section_name in PUBLIC_SECTION_RESERVED_MIN:
            remaining_budget += min(
                PUBLIC_SECTION_RESERVED_MIN[section_name],
                len(lines),
            )
        if remaining_budget <= 0:
            next_lines, next_srcs, next_fps, next_scores, next_titles, dropped_idx, counted_kept = _slice_counting_only_non_exempt(
                lines=lines,
                srcs=srcs,
                fps=fps,
                scores=scores,
                titles=titles,
                candidate_by_fp=candidate_by_fp,
                section_name=section_name,
                counted_limit=0,
            )
            for idx in dropped_idx:
                ln = lines[idx]
                global_budget_dropped.append(
                    {
                        "section": section_name,
                        "fingerprint": fps[idx] if idx < len(fps) else "",
                        "title": titles[idx] if idx < len(titles) else re.sub(r"<[^>]+>", "", ln)[:120],
                        "source_label": srcs[idx] if idx < len(srcs) else "",
                        "reader_value_score": scores[idx] if idx < len(scores) else 0.0,
                            "reason": (
                                f"Public digest budget cap {PUBLIC_DIGEST_MAX_VISIBLE_ITEMS} reached "
                                f"(reserved later event/ticket budget: {reserved_later_budget})."
                            ),
                    }
                )
            lines, srcs, fps, scores, titles = next_lines, next_srcs, next_fps, next_scores, next_titles
            if not lines:
                section_counts[section_name] = 0
                continue
        if remaining_budget > 0:
            next_lines, next_srcs, next_fps, next_scores, next_titles, dropped_idx, counted_kept = _slice_counting_only_non_exempt(
                lines=lines,
                srcs=srcs,
                fps=fps,
                scores=scores,
                titles=titles,
                candidate_by_fp=candidate_by_fp,
                section_name=section_name,
                counted_limit=remaining_budget,
            )
            for idx in dropped_idx:
                global_budget_dropped.append(
                    {
                        "section": section_name,
                        "fingerprint": fps[idx] if idx < len(fps) else "",
                        "title": titles[idx] if idx < len(titles) else re.sub(r"<[^>]+>", "", lines[idx])[:120],
                        "source_label": srcs[idx] if idx < len(srcs) else "",
                        "reader_value_score": scores[idx] if idx < len(scores) else 0.0,
                        "reason": (
                            f"Public digest budget cap {PUBLIC_DIGEST_MAX_VISIBLE_ITEMS} reached "
                            f"(reserved later event/ticket budget: {reserved_later_budget})."
                        ),
                    }
                )
            lines, srcs, fps, scores, titles = next_lines, next_srcs, next_fps, next_scores, next_titles
        # Per-source / per-section caps can filter every remaining line —
        # don't emit a bare section header in that case, the release gate
        # rejects empty low-signal blocks.
        if not lines:
            section_counts[section_name] = 0
            continue
        lines = [
            _ensure_source_anchor_for_rendered_line(
                ln,
                fps[idx] if idx < len(fps) else "",
                srcs[idx] if idx < len(srcs) else "",
                candidate_by_fp,
            )
            for idx, ln in enumerate(lines)
        ]
        section_counts[section_name] = len(lines)
        visible_item_count += sum(
            0 if _is_public_budget_exempt(section_name, candidate_by_fp.get(str(fp or ""))) else 1
            for fp in fps
        )
        rendered_candidate_fingerprints.extend(fp for fp in fps if fp)
        rendered.append(f"<b>{section_name}</b>")
        rendered.extend(lines)
        rendered.append("")

    quality_counts["rendered_candidates"] = len(rendered_candidate_fingerprints)
    rendered_fp_set = set(rendered_candidate_fingerprints)
    dropped_candidates, rendered_after_drop_reconciled = _reconcile_rendered_dropped_candidates(
        dropped_candidates,
        quality_counts,
        rendered_fp_set,
    )
    fresh_candidates = [
        c for c in candidates
        if isinstance(c, dict) and str(c.get("primary_block") or "") == "last_24h"
    ]
    fresh_report = {
        "target_items": FRESH_NEWS_TARGET_ITEMS,
        "hard_floor": SECTION_MIN_ITEMS.get("Свежие новости", 0),
        "input_candidates": len(fresh_candidates),
        "included_candidates": sum(1 for c in fresh_candidates if c.get("include")),
        "backup_candidates": sum(1 for c in fresh_candidates if c.get("backup_candidate")),
        "rendered_candidates": sum(1 for c in fresh_candidates if str(c.get("fingerprint") or "") in rendered_fp_set),
        "dropped_in_writer": sum(1 for c in dropped_candidates if str(c.get("primary_block") or "") == "last_24h"),
    }
    if degraded_shrink_dropped:
        warnings.append(
            "LLM degraded shrink held "
            f"{len(degraded_shrink_dropped)} lower-priority item(s) out of the digest."
        )
    if global_budget_dropped:
        warnings.append(
            "Public issue budget held "
            f"{len(global_budget_dropped)} lower-priority item(s) out of the digest."
        )

    drop_breakdown = {"failure": 0, "quarantine": 0, "reserve": 0}
    for _item in dropped_candidates:
        drop_breakdown[_classify_drop_bucket(_item)] += 1
    # Degrade-shrink and global-budget trims are good items held back for
    # capacity, not faults — they belong in the reserve pool.
    drop_breakdown["reserve"] += len(degraded_shrink_dropped) + len(global_budget_dropped)

    draft_path.write_text("\n".join(rendered).strip() + "\n", encoding="utf-8")
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "warnings": warnings,
            "quality_counts": quality_counts,
            "section_counts": section_counts,
            "visible_item_count": visible_item_count,
            "public_digest_budget": {
                "max_visible_items": PUBLIC_DIGEST_MAX_VISIBLE_ITEMS,
                "dropped_count": len(global_budget_dropped),
                "dropped_items": global_budget_dropped[:80],
            },
            "backfilled_today_focus": backfilled_today_focus,
            "today_focus_board": today_focus_board,
            "final_section_routing": final_section_routing,
            "degraded_shrink": {
                "enabled": bool(llm_degraded),
                "llm_stage_status": str(llm_rewrite_report.get("stage_status") or "") if llm_rewrite_report else "",
                "caps": DEGRADED_LLM_SECTION_MAX_ITEMS if llm_degraded else {},
                "dropped_count": len(degraded_shrink_dropped),
                "dropped_items": degraded_shrink_dropped[:50],
            },
            "ticket_notability": {
                "lookup_enabled": bool(os.environ.get("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "").strip() == "1"),
                "prefetch": notability_prefetch,
                "items": ticket_notability_report[:120],
            },
            "fresh_news_board": fresh_report,
            "drop_breakdown": drop_breakdown,
            "rendered_after_drop_reconciled": rendered_after_drop_reconciled[:50],
            "rendered_candidate_fingerprints": rendered_candidate_fingerprints,
            "dropped_candidates": dropped_candidates,
            "draft_path": str(draft_path.resolve()),
        },
    )
    return StageResult(
        not errors,
        "Writer stage completed." if not errors else "Writer stage found blocking issues.",
        report_path,
        draft_path,
    )
