from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import html
import json
import logging
import os
from pathlib import Path
import re
import time

logger = logging.getLogger(__name__)

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    SECTION_MAX_ITEMS,
    SECTION_MAX_PER_SOURCE,
    SECTION_MIN_ITEMS,
    is_recoverable_reserve,
    is_placeholder_practical_angle,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.transport_language import (
    repair_transport_line_language,
    transport_public_contract_errors,
)
from news_digest.pipeline.editorial_contracts import (
    attach_editorial_contract,
    classify_ticket_type,
    copy_invariant_errors,
    onsale_datetime_from_blob,
    scrub_vague_ending,
    why_now_is_publishable,
)
from news_digest.pipeline.glossary_qa import glossary_line_issues, repair_glossary_terms
from news_digest.pipeline.reader_value import reader_value_score
from news_digest.pipeline.reader_actions import classify_reader_action
from news_digest.pipeline.source_selection import source_score
from news_digest.pipeline.story_intelligence import section_board_score
from news_digest.pipeline.ticket_notability import enrich_ticket_notability, prefetch_notability, ticket_artist_name
from news_digest.pipeline.toponyms import restore_english_toponyms
from news_digest.pipeline.place_names import preserve_place_names
from news_digest.pipeline.professional_events import score_professional_event
from news_digest.pipeline.weekend_inventory import (
    current_weekend_window,
    is_weekend_inventory_candidate,
    weekend_occurrence_date,
)


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
FRESH_NEWS_TARGET_ITEMS = 7
TODAY_FOCUS_TARGET_ITEMS = 4
CORE_EMERGENCY_FLOORS = {
    "Свежие новости": 3,
    "Футбол": 1,
    "Выходные в GM": 3,
}
CORE_UNDERFLOW_TICKET_CAPS = {
    "Билеты / Ticket Radar": 4,
    "Крупные концерты вне GM": 2,
}

# When the LLM rewrite stage is degraded, keep soft rails compact without
# suppressing hard-news that did get rewritten.
DEGRADED_LLM_SECTION_MAX_ITEMS = {
    "Свежие новости": 6,
    TODAY_FOCUS_SECTION: 3,
    "Городской радар": 5,
    "Что важно в ближайшие 7 дней": 4,
    "Выходные в GM": 6,
    "Business/tech события для тебя": 2,
    "Билеты / Ticket Radar": 3,
    "Еда, открытия и рынки": 2,
    "IT и бизнес": 2,
    "Футбол": 2,
    "Русскоязычные концерты и стендап UK": 3,
}

PUBLIC_DIGEST_MAX_VISIBLE_ITEMS = 40  # counted public budget; reserved sections can borrow within the hard cap
PUBLIC_DIGEST_HARD_RENDERED_ITEMS = 52
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
    "Business/tech события для тебя": 2,
    "Билеты / Ticket Radar": 2,
    "Футбол": 2,
    "Русскоязычные концерты и стендап UK": 2,
    # IT/business sits near the end of the order, so the visible-item budget was
    # exhausted before it (valid items like a new HQ/office, startup, conference
    # or development came back 'selected_not_published'). Reserve 2 slots so up
    # to 2 real business items always make the issue when they exist.
    "IT и бизнес": 2,
}
PROTECTED_RECOVERY_SECTIONS = frozenset({
    "Свежие новости",
    TODAY_FOCUS_SECTION,
    "Городской радар",
    "Общественный транспорт сегодня",
    "Выходные в GM",
    "Что важно в ближайшие 7 дней",
    "Business/tech события для тебя",
    "Еда, открытия и рынки",
    "Футбол",
    "Русскоязычные концерты и стендап UK",
})


def _load_publish_plan(state_dir: Path) -> dict[str, object]:
    path = state_dir / "publish_plan.json"
    payload = read_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _candidate_publish_plan_status(candidate: dict | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    return str(candidate.get("publish_plan_status") or "").strip()


def _is_publish_plan_must_show(candidate: dict | None) -> bool:
    return bool(
        isinstance(candidate, dict)
        and (
            candidate.get("is_lead")
            or candidate.get("publish_plan_must_show")
            or _candidate_publish_plan_status(candidate) == "must_show"
        )
    )


def _is_publish_plan_protected_budget(candidate: dict | None) -> bool:
    return bool(
        isinstance(candidate, dict)
        and (
            candidate.get("publish_plan_protected_budget")
            or _is_publish_plan_must_show(candidate)
        )
    )


def _apply_publish_plan_to_candidates(candidates: list[dict], publish_plan: dict[str, object]) -> dict[str, object]:
    """Stamp publish-plan status onto candidates before writer decisions.

    The plan is a contract from the selection stage. For P0 we enforce the
    strongest part: ``must_show`` items (lead/protected/transport/russian/
    professional) cannot be silently lost to normal writer caps or quality
    shortcuts.
    """

    plan_items = publish_plan.get("items") if isinstance(publish_plan, dict) else []
    by_fp = {
        str(item.get("fingerprint") or ""): item
        for item in plan_items or []
        if isinstance(item, dict) and str(item.get("fingerprint") or "")
    }
    totals = {
        "loaded": bool(by_fp),
        "items_in_plan": len(by_fp),
        "matched_candidates": 0,
        "must_show_total": 0,
        "show_total": 0,
        "needs_enrichment_total": 0,
        "reserve_total": 0,
        "drop_total": 0,
        "protected_budget_total": 0,
    }
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        fp = str(candidate.get("fingerprint") or "")
        row = by_fp.get(fp)
        if not row:
            if candidate.get("is_lead"):
                candidate["publish_plan_status"] = "must_show"
                candidate["publish_plan_reason"] = "Curator lead; protected even without publish_plan row."
                candidate["publish_plan_must_show"] = True
                candidate["publish_plan_protected_budget"] = True
                candidate["manual_override"] = candidate.get("manual_override") or "force_include"
                totals["must_show_total"] += 1
                totals["protected_budget_total"] += 1
            continue
        status = str(row.get("status") or "").strip()
        candidate["publish_plan_status"] = status
        candidate["publish_plan_reason"] = str(row.get("reason") or "")
        candidate["publish_plan_budget_bucket"] = str(row.get("budget_bucket") or "")
        candidate["publish_plan_protected_budget"] = bool(row.get("protected_budget"))
        totals["matched_candidates"] += 1
        if candidate["publish_plan_protected_budget"]:
            totals["protected_budget_total"] += 1
        key = f"{status}_total"
        if key in totals:
            totals[key] += 1
        if status == "must_show":
            candidate["publish_plan_must_show"] = True
            candidate["manual_override"] = candidate.get("manual_override") or "force_include"
    return totals


def _build_publish_plan_contract_report(
    *,
    candidates: list[dict],
    rendered_fp_set: set[str],
    dropped_candidates: list[dict[str, object]],
    global_budget_dropped: list[dict[str, object]],
    degraded_shrink_dropped: list[dict[str, object]],
    publish_plan_application: dict[str, object],
) -> dict[str, object]:
    reason_by_fp: dict[str, list[str]] = {}
    for row in dropped_candidates:
        fp = str(row.get("fingerprint") or "")
        if not fp:
            continue
        reason_by_fp.setdefault(fp, []).extend(str(r) for r in (row.get("reasons") or []) if str(r).strip())
    for row in [*global_budget_dropped, *degraded_shrink_dropped]:
        fp = str(row.get("fingerprint") or "")
        if not fp:
            continue
        reason = str(row.get("reason") or "").strip()
        if reason:
            reason_by_fp.setdefault(fp, []).append(reason)

    must_show: list[dict[str, object]] = []
    show_missing: list[dict[str, object]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        status = _candidate_publish_plan_status(candidate)
        if status not in {"must_show", "show"}:
            continue
        fp = str(candidate.get("fingerprint") or "")
        rendered = bool(fp and fp in rendered_fp_set)
        row = {
            "fingerprint": fp,
            "title": candidate.get("title") or "",
            "source_label": candidate.get("source_label") or "",
            "primary_block": candidate.get("primary_block") or "",
            "section": PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""), ""),
            "status": status,
            "rendered": rendered,
            "contract_status": "rendered" if rendered else str(candidate.get("publish_plan_contract_status") or "missing"),
            "reason": candidate.get("publish_plan_reason") or "",
            "loss_reasons": reason_by_fp.get(fp) or [],
            "recovery_trace": candidate.get("recovery_trace") or [],
        }
        if status == "must_show":
            must_show.append(row)
        elif not rendered:
            show_missing.append(row)
    missing_must_show = [row for row in must_show if not row["rendered"]]
    return {
        "schema_version": 1,
        "policy": (
            "must_show items must render, be repaired, be replaced in the same block, "
            "or carry an unrecoverable_no_facts reason."
        ),
        "publish_plan_application": publish_plan_application,
        "counts": {
            "must_show_total": len(must_show),
            "must_show_rendered": sum(1 for row in must_show if row["rendered"]),
            "must_show_missing": len(missing_must_show),
            "show_missing": len(show_missing),
        },
        "missing_must_show": missing_must_show[:80],
        "show_missing_examples": show_missing[:40],
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
_TODAY_FOCUS_RECOVERY_SOURCE_BLOCKS = {
    "today_focus",
    "last_24h",
    "city_watch",
}
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
_TODAY_FOCUS_READER_ACTION_RE = re.compile(
    r"\b(?:today|this morning|tonight|tomorrow|this week|deadline|consultation|"
    r"apply|report|check|avoid|use|book|register|appeal|witness|cctv|"
    r"vote|voters?|polls?\s+open|polling\s+station|by-election|election|"
    r"closed|closure|reopen|delays?|diversion|warning|unsafe|danger|"
    r"inspection|cqc|ofsted|requires\s+improvement|inadequate|safeguarding|"
    r"patients?|residents?|tenants?|parents?|children|school|homes?|housing|"
    r"council\s+tax|bins?|strike|appointments?|service|online|email)\b",
    re.IGNORECASE,
)
_TODAY_FOCUS_NO_ACTION_RE = re.compile(
    r"(?:\bpoll:|\bhave\s+your\s+say\b|\banniversary\b|\bchanged\s+manchester\s+forever\b|"
    r"changed\s+.*\s+forever|remembers?|remembered|tribute|survivor|"
    r"moments?\s+from\s+death|look\s+back|throwback|what\s+happened\s+on\s+this\s+day)",
    re.IGNORECASE,
)
_TODAY_FOCUS_HARD_ACTION_RE = re.compile(
    r"\b(?:report|apply|deadline|consultation|closed|closure|reopen|delays?|"
    r"diversion|warning|unsafe|danger|inspection|cqc|ofsted|"
    r"requires\s+improvement|inadequate|safeguarding|appeal|witness|cctv|"
    r"service\s+change|strike|vote|voters?\s+head\s+to\s+the\s+polls|"
    r"polls?\s+open|polling\s+station|by-election|election\s+day)\b",
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
# «Что важно сегодня» is a practical block: a story about the past — a heritage
# retrospective ("24 years ago…"), an old conviction commentary ("murderer…
# serving life") — is not "what a resident should account for today". It only
# qualifies if a concrete current/upcoming hook (this week, pending decision,
# deadline) makes it actionable now.
_TODAY_FOCUS_RETROSPECTIVE_RE = re.compile(
    r"\b(?:\d{1,3}\s+years?\s+ago|decades?\s+ago|back\s+in\s+(?:19|20)\d\d|"
    r"serving\s+(?:a\s+)?life|sentenced\s+to\s+life|jailed\s+for\s+life|"
    r"years?\s+later|murderer|killer)\b",
    re.IGNORECASE,
)
_TODAY_FOCUS_CURRENT_HOOK_RE = re.compile(
    r"\b(?:this\s+week|today|tonight|tomorrow|this\s+morning|pending|awaiting|"
    r"will\s+(?:be\s+)?(?:decid|approv|clos|open|start|vot)|"
    r"to\s+be\s+(?:approved|decided|built|demolished|held)|deadline|"
    r"closes?\s+on|next\s+week|due\s+to\s+(?:open|start|close)|"
    r"expected\s+(?:this|next|to))\b",
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


_TODAY_FOCUS_NATIONAL_RE = re.compile(
    r"\b(?:poll:|have your say|опрос|prime minister|sir keir|keir starmer|starmer|"
    r"downing street|nationwide|across the uk|uk[- ]wide|nationally|parliament|"
    r"westminster|general election|whitehall)\b",
    re.IGNORECASE,
)
_GM_LOCAL_ANCHOR_RE = re.compile(
    r"\b(?:greater manchester|\bgm\b|manchester (?:city )?council|metrolink|tfgm|"
    r"stockport|tameside|trafford|salford|bolton|bury|oldham|rochdale|wigan|"
    r"ashton|wythenshawe|prestwich|altrincham|chorlton|didsbury|hulme|fallowfield|"
    r"эштон|стокпорт|солфорд|болтон|уиган|рочдейл|траффорд|олдем)\b",
    re.IGNORECASE,
)


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
    # Past retrospective / old-conviction commentary without a current hook is
    # not a practical "today" item (neighbour-of-a-murderer piece, «24 years
    # ago…» heritage feature) — keep it out of the morning practical block.
    if _TODAY_FOCUS_RETROSPECTIVE_RE.search(text) and not _TODAY_FOCUS_CURRENT_HOOK_RE.search(text):
        return False
    story_frame = contract.get("story_frame") if isinstance(contract.get("story_frame"), dict) else {}
    why_now = str(story_frame.get("why_now") or "")
    civic_today = (
        story_type in {"civic", "planning", "service_accountability", "local_service_change", "local_cost"}
        and why_now_is_publishable(why_now)
        and _GM_LOCAL_ANCHOR_RE.search(text)
    )
    if tier == "filler" and not (
        (
            story_type == "local_cost"
            and re.search(r"\b(?:flood|water|electric|power|damage|closed|closure|reopen|cost|thousands?)\b", text, re.IGNORECASE)
        )
        or (
            story_type in {"civic", "planning", "service_accountability", "local_service_change"}
            and _TODAY_FOCUS_HARD_ACTION_RE.search(text)
        )
    ):
        return False
    if _TODAY_FOCUS_NO_ACTION_RE.search(text) and not (_TODAY_FOCUS_HARD_ACTION_RE.search(text) or civic_today):
        return False
    if not (_TODAY_FOCUS_READER_ACTION_RE.search(text) or civic_today):
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
    # National politics / polls without a GM hook are not "what matters today
    # in GM" (owner 2026-06-15: the Starmer under-16s social-media ban / a UK
    # poll). A historical/anniversary retrospective only qualifies on a real
    # new phase (the 1996 IRA bomb «99 minutes» piece).
    _contract = _candidate_contract(candidate)
    _topic = str(_contract.get("topic_key") or candidate.get("topic_key") or "")
    if _topic.startswith(("memorial:", "incident:manchester_ira")) and str(_contract.get("anchor_type") or "") != "new_phase":
        return False
    if _TODAY_FOCUS_NATIONAL_RE.search(text) and not _GM_LOCAL_ANCHOR_RE.search(text):
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
    for field in ("story_phase_key", "event_identity_key", "story_identity_key"):
        value = _normalize_text_key(str(c.get(field) or ""))
        if len(value) >= 12 and value not in {"none", "unknown"}:
            return value
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


_DUP_BOILERPLATE_RE = re.compile(
    r"manchestereveningnews|aboutmanchester|placenorthwest|\bgreater\b|\breporter\b|"
    r"correspondent|\beditor\b|\bimages?\b|\bvideo\b|sitemap|\bnews\b|\bwww\b",
    re.IGNORECASE,
)

_INCIDENT_TOPIC_KEYS = {"crime", "incident", "court", "public_safety_after_incident"}
_INCIDENT_EVENT_TYPES = {"crime", "incident", "court", "public_safety", "public_safety_after_incident"}
_INCIDENT_MARKER_ALIASES = {
    "firearm": "firearms",
    "firearms": "firearms",
    "gun": "firearms",
    "guns": "firearms",
    "weapon": "weapons",
    "weapons": "weapons",
    "knife": "knife",
    "knives": "knife",
    "stab": "stabbing",
    "stabbing": "stabbing",
    "stabbings": "stabbing",
    "shot": "shooting",
    "shooting": "shooting",
    "murder": "murder",
    "homicide": "murder",
    "crash": "crash",
    "collision": "crash",
    "fire": "fire",
    "arson": "fire",
    "evacuation": "evacuation",
    "evacuated": "evacuation",
    "trial": "court",
    "court": "court",
    "charged": "charge",
    "charge": "charge",
    "arrest": "arrest",
    "arrested": "arrest",
    "appeal": "appeal",
}
_INCIDENT_MARKER_RE = re.compile(
    r"\b(?:firearms?|guns?|weapons?|knives|knife|stab(?:bed|bing|s)?|shot|shooting|"
    r"murder|homicide|crash|collision|fire|arson|evacuat(?:ed|ion)|trial|court|"
    r"charg(?:ed|e)|arrest(?:ed)?|appeal)\b",
    re.IGNORECASE,
)
_CONCRETE_ENTITY_KEYS = {"people", "venues", "companies", "clubs", "stations"}
_LOCATION_ENTITY_KEYS = {"boroughs", "districts", "councils", "stations", "venues"}
_BROAD_INCIDENT_LOCATIONS = {"greater manchester", "manchester", "gm"}
_GENERIC_INCIDENT_ANCHORS = {
    "police",
    "man",
    "woman",
    "boy",
    "girl",
    "teen",
    "teenager",
    "child",
    "victim",
    "person",
    "people",
    "court",
    "incident",
    "crime",
    "firearms",
    "weapons",
    "stabbing",
    "shooting",
    "crash",
    "fire",
}


def _fresh_evidence_token(value: object) -> str:
    token = _normalize_text_key(str(value or ""))
    token = re.sub(r"\s+", " ", token).strip()
    if len(token) < 4 or token in {"none", "unknown", "n/a"}:
        return ""
    return token


def _fresh_incident_markers(text: str) -> set[str]:
    markers: set[str] = set()
    for match in _INCIDENT_MARKER_RE.finditer(str(text or "")):
        raw = match.group(0).lower()
        stem = re.sub(r"(?:ed|ing|s)$", "", raw)
        markers.add(_INCIDENT_MARKER_ALIASES.get(raw) or _INCIDENT_MARKER_ALIASES.get(stem) or raw)
    return markers


def _fresh_date_tokens(candidate: dict, frame: dict) -> set[str]:
    values = [
        frame.get("when"),
        candidate.get("published_at"),
        candidate.get("updated_at"),
        candidate.get("date_start"),
    ]
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    values.extend([event.get("date_start"), event.get("start_date"), event.get("date")])
    out: set[str] = set()
    for value in values:
        out.update(re.findall(r"\b\d{4}-\d{2}-\d{2}\b", str(value or "")))
    return out


def fresh_dedupe_evidence(row: _SectionRow) -> dict[str, object]:
    c = row.candidate or {}
    entities = c.get("entities")
    if not isinstance(entities, dict):
        from news_digest.pipeline.entity_extraction import extract_entities  # noqa: PLC0415

        entities = extract_entities(c)

    frame = c.get("story_frame") if isinstance(c.get("story_frame"), dict) else {}
    contract = c.get("editorial_contract") if isinstance(c.get("editorial_contract"), dict) else {}
    topic_key = _fresh_evidence_token(c.get("topic_key") or contract.get("topic_key"))
    event_type = _fresh_evidence_token(frame.get("event_type") or contract.get("event_type"))
    anchors: set[str] = set()
    locations: set[str] = set()
    if isinstance(entities, dict):
        for key, values in entities.items():
            if not isinstance(values, list):
                continue
            for value in values:
                token = _fresh_evidence_token(value)
                if not token:
                    continue
                if key in _CONCRETE_ENTITY_KEYS and token not in _GENERIC_INCIDENT_ANCHORS:
                    anchors.add(token)
                if key in _LOCATION_ENTITY_KEYS:
                    locations.add(token)
    for key_field in ("story_identity_key", "event_identity_key", "story_phase_key"):
        token = _fresh_evidence_token(c.get(key_field))
        if token:
            anchors.add(token)
    cluster = c.get("story_cluster") if isinstance(c.get("story_cluster"), dict) else {}
    for key_field in ("cluster_key", "semantic_key", "story_key"):
        token = _fresh_evidence_token(cluster.get(key_field))
        if token:
            anchors.add(token)
    where = _fresh_evidence_token(frame.get("where_exact") or frame.get("where") or c.get("location"))
    if where:
        locations.add(where)
    text = " ".join(
        str(value or "")
        for value in (
            row.title,
            row.line,
            c.get("title"),
            c.get("summary"),
            c.get("lead"),
            c.get("evidence_text"),
            frame.get("what_happened"),
            frame.get("who_affected"),
        )
    )
    markers = _fresh_incident_markers(text)
    return {
        "topic_key": topic_key,
        "event_type": event_type,
        "anchors": anchors,
        "locations": locations,
        "markers": markers,
        "dates": _fresh_date_tokens(c, frame),
    }


def _fresh_is_incident_evidence(evidence: dict[str, object]) -> bool:
    topic_key = str(evidence.get("topic_key") or "")
    event_type = str(evidence.get("event_type") or "")
    markers = evidence.get("markers")
    return topic_key in _INCIDENT_TOPIC_KEYS or event_type in _INCIDENT_EVENT_TYPES or bool(markers)


def _fresh_sets_compatible(left: set[str], right: set[str]) -> bool:
    return not left or not right or bool(left & right)


def _fresh_incident_types_compatible(left: dict[str, object], right: dict[str, object]) -> bool:
    left_type = str(left.get("event_type") or left.get("topic_key") or "")
    right_type = str(right.get("event_type") or right.get("topic_key") or "")
    if left_type and right_type and left_type == right_type:
        return True
    left_markers = left.get("markers") if isinstance(left.get("markers"), set) else set()
    right_markers = right.get("markers") if isinstance(right.get("markers"), set) else set()
    if left_markers and right_markers:
        return bool(left_markers & right_markers)
    broad = {"crime", "incident", "public_safety", "public_safety_after_incident"}
    return bool(left_type in broad or right_type in broad)


def _fresh_incident_match_evidence(left: dict[str, object], right: dict[str, object]) -> dict[str, object]:
    left_anchors = left.get("anchors") if isinstance(left.get("anchors"), set) else set()
    right_anchors = right.get("anchors") if isinstance(right.get("anchors"), set) else set()
    left_locations = left.get("locations") if isinstance(left.get("locations"), set) else set()
    right_locations = right.get("locations") if isinstance(right.get("locations"), set) else set()
    left_markers = left.get("markers") if isinstance(left.get("markers"), set) else set()
    right_markers = right.get("markers") if isinstance(right.get("markers"), set) else set()
    left_dates = left.get("dates") if isinstance(left.get("dates"), set) else set()
    right_dates = right.get("dates") if isinstance(right.get("dates"), set) else set()
    shared_anchors = left_anchors & right_anchors
    shared_locations = left_locations & right_locations
    precise_shared_locations = {
        location for location in shared_locations if location not in _BROAD_INCIDENT_LOCATIONS
    }
    shared_markers = left_markers & right_markers
    compatible = (
        _fresh_sets_compatible(left_dates, right_dates)
        and _fresh_sets_compatible(left_locations, right_locations)
        and _fresh_incident_types_compatible(left, right)
    )
    same_story = bool(compatible and (shared_anchors or (precise_shared_locations and shared_markers)))
    return {
        "same_story": same_story,
        "matched_entities": sorted(shared_anchors),
        "location": sorted(shared_locations),
        "date": sorted(left_dates & right_dates),
        "type": sorted(shared_markers) or [str(left.get("event_type") or left.get("topic_key") or "")],
    }


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
    # Many shared substantive tokens (place + incident specifics) are an
    # unambiguous same-story signal even when one card is much longer and
    # dilutes the Jaccard/overlap ratio — the Oldham Road/Ashton MEN+BBC pair
    # shared 16 tokens but scored 0.44 overlap (owner 2026-06-15 dup). BUT
    # boilerplate shared by ANY two articles from the same outlet (URL-slug
    # fragments, byline words, bare years) must NOT count — otherwise two
    # unrelated MEN crime stories merge on "manchestereveningnews /
    # greater-manchester-news / reporter / 2026" and a real story is suppressed
    # as a duplicate (owner 2026-06-19: Failsworth stabbing eaten by woman-dies
    # -in-park).
    substantive_strong = {
        token for token in strong_common
        if not re.fullmatch(r"https?|\d{1,4}|\d{4}-\d{2}-\d{2}", token)
        and "-" not in token
        and not _DUP_BOILERPLATE_RE.search(token)
    }

    left_ev = fresh_dedupe_evidence(left)
    right_ev = fresh_dedupe_evidence(right)

    if _fresh_is_incident_evidence(left_ev) or _fresh_is_incident_evidence(right_ev):
        evidence = _fresh_incident_match_evidence(left_ev, right_ev)
        if evidence["same_story"]:
            if left.candidate is not None:
                left.candidate["dedupe_merge_reason"] = "fresh_incident_evidence_match"
                left.candidate["dedupe_merge_evidence"] = evidence
            if right.candidate is not None:
                right.candidate["dedupe_merge_reason"] = "fresh_incident_evidence_match"
                right.candidate["dedupe_merge_evidence"] = evidence
            return True
        return False

    if len(substantive_strong) >= 6:
        return True
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
        if not isinstance(candidate, dict):
            continue
        recoverable_today_reserve = (
            not candidate.get("include")
            and is_recoverable_reserve(candidate)
            and str(candidate.get("primary_block") or "") in _TODAY_FOCUS_RECOVERY_SOURCE_BLOCKS
            and _today_focus_candidate_is_eligible(candidate, str(candidate.get("draft_line") or ""))
        )
        if not candidate.get("include") and not recoverable_today_reserve:
            continue
        if candidate.get("reject_reasons") or candidate.get("validation_errors"):
            continue
        if (
            str(candidate.get("editorial_status") or "") == "borderline"
            and str(candidate.get("manual_override") or "") != "force_include"
        ):
            continue
        attach_editorial_contract(candidate)
        if _contract_public_drop_reason(candidate) and str(candidate.get("manual_override") or "") != "force_include":
            continue
        if not candidate.get("source_url") or not candidate.get("source_label"):
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
            if recoverable_today_reserve:
                line = _today_focus_recovery_line(candidate)
                if line:
                    candidate["draft_line_provider"] = "writer_today_focus_recovery"
                    candidate["draft_line_model"] = "deterministic_today_focus_recovery"
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
        # Also group by cluster/topic key so the SAME subject in two sections
        # (e.g. the 1996 IRA bomb in both Fresh and «Что важно сегодня») is
        # collapsed to one card across the whole board, not just within Fresh.
        key = _fresh_related_story_key(row) or _fresh_story_cluster_key(row)
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
        event_dict = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        has_specific_date = bool(event_dict.get("date_start") and event_dict.get("date_confidence") in {"high", "medium"})
        if not has_specific_date:
            return "bookable_activity_filler"
    return ""


def _block_contract_action(candidate: dict, line: str) -> dict[str, str]:
    """Visible block contract enforcement.

    Keep useful material by rerouting it when possible; hold optional filler
    only when it does not satisfy the section's purpose and can be replaced by
    normal same-section recovery.
    """
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    text = " ".join(
        str(value or "")
        for value in (
            line,
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            candidate.get("source_label"),
        )
    )
    if block == "transport" and _LIFT_ESCALATOR_RE.search(text) and not _TRANSPORT_MOVEMENT_RE.search(text):
        return {"action": "hold", "reason": "block_contract:transport_lift_escalator_no_movement"}
    if block == "weekend_activities" and _SOLO_GIG_RE.search(text) and not _WEEKEND_COMMUNITY_RE.search(text):
        return {"action": "reroute", "target_block": "ticket_radar", "reason": "block_contract:weekend_solo_gig_to_ticket_radar"}
    if block == "outside_gm_tickets" and not _is_a_tier_ticket(candidate):
        return {"action": "hold", "reason": "block_contract:outside_gm_non_a_tier"}
    if block == "football" and _football_should_route_to_soft(candidate):
        return {"action": "reroute", "target_block": "city_watch", "reason": "block_contract:football_soft_to_city_watch"}
    if block == "tech_business" and category == "tech_business" and not _BUSINESS_CONCRETE_RE.search(text):
        return {"action": "hold", "reason": "block_contract:business_no_concrete_city_impact"}
    if block == "food_openings" and not _FOOD_CONCRETE_RE.search(text):
        return {"action": "hold", "reason": "block_contract:food_no_opening_market_or_change"}
    return {"action": "keep", "reason": ""}


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
    "Общественный транспорт сегодня",
    "Русскоязычные концерты и стендап UK",
    "Business/tech события для тебя",
}

_MARKET_EVENT_RE = re.compile(
    r"\b(?:car\s*boot|market|markets|makers\s+market|farmer'?s\s+market|"
    r"farmers\s+market|flea\s+market|vintage\s+market|food\s+market|flower\s+festival)\b",
    re.IGNORECASE,
)
_ROUTINE_MARKET_RECURRENCE_RE = re.compile(
    r"\b(?:every|weekly|monthly|each\s+(?:week|month)|"
    r"(?:first|second|third|fourth|last)\s+(?:saturdays?|sundays?|weekends?)|"
    r"кажд(?:ую|ый|ое)|еженедельн|ежемесячн|по\s+(?:субботам|воскресеньям|выходным))\b",
    re.IGNORECASE,
)
_RARE_MARKET_OR_FESTIVAL_RE = re.compile(
    r"\b(?:annual|yearly|once\s+a\s+year|biannual|twice\s+a\s+year|"
    r"festival|food\s+festival|bbq|barbecue|beer\s+festival|street\s+food|"
    r"music|live\s+music|artists?|performance|special|launch|anniversary|"
    r"ежегодн|раз\s+в\s+год|фестиваль|барбекю|уличн\w+\s+ед|живая\s+музык)\b",
    re.IGNORECASE,
)
_SOLD_OUT_EVENT_RE = re.compile(
    r"\b(?:sold\s*out|fully\s*booked|no\s+(?:tickets|spaces|places)\s+(?:left|available)|"
    r"tickets?\s+(?:are\s+)?(?:sold\s*out|unavailable)|распродан[оаы]?|мест\s+нет)\b",
    re.IGNORECASE,
)
_SOLO_GIG_RE = re.compile(
    r"\b(?:gig|concert|live\s+music|tour|dj\s+set|headline\s+show|"
    r"концерт|гастрол|диджей|выступлен)\b",
    re.IGNORECASE,
)
_WEEKEND_COMMUNITY_RE = re.compile(
    r"\b(?:market|fair|festival|family|community|workshop|exhibition|food|makers|"
    r"ярмарк|рынок|фестиваль|семейн|сообществ|мастер-класс|выставк|еда)\b",
    re.IGNORECASE,
)
_TRANSPORT_MOVEMENT_RE = re.compile(
    r"\b(?:cancel|delay|diversion|closure|closed|replacement|suspended|strike|"
    r"metrolink|tram|rail|train|bus|route|line|station|stop|platform|"
    r"отмен|задерж|объезд|закрыт|замещающ|трамва|поезд|автобус|маршрут|линия|станци|остановк)\b",
    re.IGNORECASE,
)
_LIFT_ESCALATOR_RE = re.compile(r"\b(?:lift|lifts|escalator|escalators|лифт|эскалатор)\b", re.IGNORECASE)
_BUSINESS_CONCRETE_RE = re.compile(
    r"\b(?:funding|investment|deal|contract|office|hq|jobs?|appointment|appoints?|"
    r"launch|grant|partnership|screen\s+fund|innovation|startup|"
    r"финансирован|инвестиц|сделк|контракт|офис|назнач|запуск|грант|стартап)\b",
    re.IGNORECASE,
)
_FOOD_CONCRETE_RE = re.compile(
    r"\b(?:opens?|opening|launch|reopen|market|food\s+hall|restaurant|bar|bakery|"
    r"откры|запуск|рынок|фуд-холл|ресторан|бар|пекарн)\b",
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


def _is_a_tier_ticket(candidate: dict | None) -> bool:
    """A top-tier (A) artist in a ticket block must NEVER be trimmed from view
    (owner rule 2026-06-14: "A-artists must not disappear"). Treated as exempt
    so neither the per-section cap nor the global issue budget can drop it —
    even if that means the ticket section grows past its normal cap.

    future_announcements is included: a future A-tier (e.g. The Weeknd, The
    Fratellis) announced for a later date is still an A-tier artist. Without it
    the block guard failed the A-tier check and the item slipped silently into
    manual-review instead of being recognised and held (backlog item 7)."""
    if not isinstance(candidate, dict):
        return False
    if str(candidate.get("primary_block") or "") not in {"ticket_radar", "outside_gm_tickets", "future_announcements"}:
        return False
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    return str(notability.get("tier") or "").upper() == "A"


def _is_budget_exempt_a_tier(candidate: dict | None) -> bool:
    """Every recognised A-tier ticket stays visible, regardless of venue.

    The ticket caps still compact ordinary listings. They must not decide that
    an A-tier artist is less relevant merely because the show is outside GM or
    because the issue has already reached its usual item budget.
    """
    return _is_a_tier_ticket(candidate)


def _is_active_tram_transport(candidate: dict | None) -> bool:
    """Active Metrolink/tram items must not be trimmed by the public budget.

    The owner rule is stronger than the compact transport target: if the run
    found real tram restrictions, every one of them should stay visible.
    Minor bus-stop closures can still be compacted separately.
    """
    if not isinstance(candidate, dict):
        return False
    if str(candidate.get("primary_block") or "") != "transport":
        return False
    if str(candidate.get("transport_mode") or "").strip().lower() == "tram":
        return True
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "draft_line", "evidence_text", "source_label")
    )
    return bool(re.search(r"\b(?:metrolink|tram|trams|трамва[йеия])\b", blob, re.IGNORECASE))


def _is_dated_weekend_event(section_name: str, candidate: dict | None) -> bool:
    """Protected Weekend Inventory must not be cut by public/section caps.

    This is intentionally narrower than "any dated event": ordinary theatre,
    gigs, comedy and Ticketmaster-style listings remain outside this pass.
    """
    return section_name == "Выходные в GM" and is_weekend_inventory_candidate(candidate)


def _is_public_budget_exempt(section_name: str, candidate: dict | None) -> bool:
    if section_name in _PUBLIC_BUDGET_EXEMPT_SECTIONS:
        return True
    if not isinstance(candidate, dict):
        return False
    # venues_tickets no longer gets a blanket budget pass: the main Ticket
    # Radar must count toward the 45-item issue budget. Evergreen markets /
    # recurring drop-ins stay exempt (they answer "what can I do this weekend"
    # and should survive a noisy news morning). A-tier artists are always exempt.
    # Dated weekend events (E4) are exempt too: a confirmed this-weekend date
    # means the reader can act on it, so the budget must not silently cut it.
    return (
        _is_publish_plan_must_show(candidate)
        or (
            _is_publish_plan_protected_budget(candidate)
            and section_name in _PUBLIC_BUDGET_EXEMPT_SECTIONS
        )
        or _is_active_tram_transport(candidate)
        or _is_market_or_recurring_event(candidate)
        or _is_budget_exempt_a_tier(candidate)
        or _is_dated_weekend_event(section_name, candidate)
    )


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
            # the old weekend section cap no longer applies to eligible
            # Weekend Inventory: compression must happen inside the section,
            # not by silently dropping current-weekend inventory.
            if section_name == "Выходные в GM":
                exempt = is_weekend_inventory_candidate(candidate)
            else:
                exempt = bool(
                    isinstance(candidate, dict)
                    and (
                        _is_active_tram_transport(candidate)
                        or _is_market_or_recurring_event(candidate)
                        or _is_budget_exempt_a_tier(candidate)
                    )
                )
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


_MIXED_SCRIPT_WORD_RE = re.compile(
    r"\b(?=[A-Za-zА-Яа-яЁё]*[A-Za-z])(?=[A-Za-zА-Яа-яЁё]*[А-Яа-яЁё])"
    r"[A-Za-zА-Яа-яЁё]{2,}\b"
)


def _mixed_latin_cyrillic_words(value: str) -> list[str]:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return _MIXED_SCRIPT_WORD_RE.findall(text)


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
    # Strip internal feed-qualifier suffixes that leak as English noise in the
    # visible attribution (owner 2026-06-15: "Ticketmaster UK Major Upcoming",
    # "MEN News Sitemap", "BBC Manchester Web").
    label = re.sub(r"^ticketmaster\b.*$", "Ticketmaster", label, flags=re.IGNORECASE)
    label = re.sub(r"\s+(?:news\s+)?sitemap$", "", label, flags=re.IGNORECASE)
    label = re.sub(r"\s+(?:major\s+|manchester\s+|uk\s+)?upcoming$", "", label, flags=re.IGNORECASE)
    label = re.sub(r"\s+web$", "", label, flags=re.IGNORECASE)
    return label.strip() or str(source_label or "").strip()


_SUMMER_MONTHS = frozenset({6, 7, 8})
_HEAVY_SNOW_PATTERN = re.compile(
    r"\b(?:heavy\s+snow|blizzard|snowstorm|snowfall|снегопад|метель|снежная\s+буря)\b",
    re.IGNORECASE,
)
_EXTREME_TEMP_PATTERN = re.compile(r"\b([1-9]\d)\s*°[Cc]\b")
_EVENT_BLOCKS = {"weekend_activities", "next_7_days", "ticket_radar", "outside_gm_tickets", "russian_events", "future_announcements", "professional_events"}
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


# A city/place name is never a music genre — never show it in the genre slot
# (owner 2026-06-16: "Kasabian … Glasgow Green (Glasgow)").
_GENRE_NOT_CITY = {
    "manchester", "liverpool", "london", "greater manchester", "united kingdom", "uk",
    "glasgow", "edinburgh", "newcastle", "cardiff", "newport", "birmingham", "leeds",
    "sheffield", "bristol", "brighton", "thetford", "scarborough", "halifax",
    "delamere", "isle of wight", "salisbury", "nottingham", "leicester", "preston",
    "glasgow green", "delamere forest",
}


def _ticket_genre(candidate: dict) -> str:
    # Prefer the structured Ticketmaster sub-genre, then genre. It is far more
    # accurate than the coarse summary chunk: Lily Allen is subGenre="Pop"
    # (genre="Rock"), Fatboy Slim "Electro Pop" (genre="Pop"), Gorillaz
    # "Alternative Rock". Skip Ticketmaster's no-real-classification placeholders.
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    _skip = {"", "undefined", "other", "unknown", "miscellaneous", "undefined "}
    for key in ("subGenre", "genre"):
        val = re.sub(r"\s+", " ", str(event.get(key) or "")).strip()
        if val.lower() not in _skip and val.lower() not in _GENRE_NOT_CITY:
            return val
    summary = str(candidate.get("summary") or "")
    chunks = [chunk.strip(" .") for chunk in summary.split("|")]
    ignored = _GENRE_NOT_CITY
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
        venue = _ticket_venue(candidate)
        if (
            decision == "hide"
            and ticket_type == "major_upcoming"
            and _parse_ticket_datetime(candidate) is not None
            and (
                _TICKET_MAJOR_VENUE_RE.search(venue)
                or _TICKET_MAJOR_VENUE_RE.search(str(candidate.get("source_label") or ""))
            )
        ):
            decision = "show"
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


def _format_compact_number(value: object) -> str:
    try:
        number = int(value or 0)
    except (TypeError, ValueError):
        return ""
    if number >= 1_000_000:
        return f"{number / 1_000_000:.1f} млн".replace(".0", "")
    if number >= 1_000:
        return f"{round(number / 1000)} тыс."
    return str(number) if number > 0 else ""


def _ticket_notability_proof(candidate: dict) -> str:
    notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
    signals = notability.get("signals") if isinstance(notability.get("signals"), dict) else {}
    signal = str(notability.get("signal") or "").strip()
    spotify_followers = _format_compact_number(signals.get("spotify_followers"))
    spotify_popularity = str(signals.get("spotify_popularity") or "").strip()
    lastfm = _format_compact_number(signals.get("lastfm_listeners"))
    sitelinks = _format_compact_number(notability.get("sitelinks") or signals.get("sitelinks"))
    if signal == "streaming_popularity":
        if spotify_followers:
            return f"Spotify: {spotify_followers} подписчиков"
        if spotify_popularity and spotify_popularity != "0":
            return f"Spotify popularity {spotify_popularity}/100"
        if lastfm:
            return f"Last.fm: {lastfm} слушателей"
    if signal.startswith("wikidata") and sitelinks:
        return f"Wikidata: {sitelinks} языковых страниц"
    if signal == "ticketmaster_attraction":
        return "есть официальная артист-карточка Ticketmaster"
    if signal == "musicbrainz_ticketmaster_identity":
        return "MusicBrainz и Ticketmaster подтверждают артиста"
    if lastfm:
        return f"Last.fm: {lastfm} слушателей"
    if spotify_followers:
        return f"Spotify: {spotify_followers} подписчиков"
    if sitelinks:
        return f"Wikidata: {sitelinks} языковых страниц"
    return ""


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
    # Geography wording follows the authoritative venue scope, not just the
    # block: an outside/nearby venue never says "в GM" even if it slipped into
    # the GM radar (W3 / #0010). Unknown scope falls back to block routing.
    _scope = str(candidate.get("venue_scope") or "").lower()
    if _scope in {"outside", "nearby"}:
        in_gm = False
    elif _scope == "gm":
        in_gm = True
    else:
        in_gm = str(candidate.get("primary_block") or "") != "outside_gm_tickets"
    lineup = re.search(r"\bline[- ]?up\s*=", blob, re.IGNORECASE) or str(notability.get("kind") or "") == "lineup_or_show"
    estate_show = re.search(r"\b(?:estate|open air|open-air|castle|palace|park)\b", blob, re.IGNORECASE)
    arena_show = _TICKET_MAJOR_VENUE_RE.search(venue) or _TICKET_MAJOR_VENUE_RE.search(summary)
    merged = candidate.get("merged_event_dates")
    multi_night = isinstance(merged, list) and len({str(d) for d in merged}) >= 2
    this_week = days is not None and 0 <= days <= 7
    soon = days is not None and 0 <= days <= 14
    where = "в GM" if in_gm else "вне GM"
    # Reason explains WHY it matters with evidence, not machine praise
    # ("крупный артист", "крупная площадка"). A fresh sale is the clearest
    # "act now" reason; notability gets a proof signal from the cache.
    if ticket_type == "presale_soon":
        return "скоро открывается presale"
    if ticket_type in {"on_sale_now", "newly_listed"}:
        return "новая продажа билетов"
    # A festival lineup is a different product from a single headliner.
    if lineup:
        return "фестивальный состав, не один артист"
    if tier == "A":
        # P1-B: give the reader a reason to act (date / venue), not the machine
        # notability signal. Last.fm / Spotify / Wikidata stay in the internal
        # ticket_notability report, never in the published line.
        if days == 0:
            return f"сегодня в {venue}" if venue else "сегодня"
        if this_week:
            return f"{where} на этой неделе"
        if soon:
            return "ближайшая дата тура"
        return f"{venue}: дата впереди" if venue else "UK-дата в радаре"
    if arena_show:
        if multi_night:
            return f"несколько дат в {venue}" if venue else "несколько дат"
        return f"{venue}: дата на этой неделе" if this_week and venue else (f"{venue}: подтверждённая дата" if venue else "подтверждённая дата")
    if estate_show:
        return "open-air концерт на estate-площадке"
    if this_week:
        return f"концерт {where} на этой неделе"
    if soon:
        return "ближайшая дата тура"
    if ticket_type == "major_upcoming":
        return "заметная UK-дата"
    return "билетный повод"


_LINEUP_WRAPPER_RE = re.compile(
    r"\b(?:presents|festival|weekend|day\s+ticket|tickets|vip|hospitality|camping)\b",
    re.IGNORECASE,
)


def _ticket_lineup(candidate: dict) -> list[str]:
    """Main artist names for a festival / multi-act ticket, so the card shows
    the acts that justify it — not just the festival name. Prefers the merged
    ``festival_lineup`` (set when fragments are consolidated in dedupe), then
    the Ticketmaster ``attractions``. Drops promoter / festival-wrapper entries
    ("On the Waterfront presents", "Sky presents", the festival's own name)."""
    merged = candidate.get("festival_lineup")
    raw: list[str] = []
    if isinstance(merged, list) and merged:
        raw = [str(n) for n in merged]
    else:
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        atts = event.get("attractions") if isinstance(event.get("attractions"), list) else []
        raw = [str(a.get("name") or "") for a in atts if isinstance(a, dict)]
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    venue_low = re.sub(r"\s+", " ", str(event.get("venue") or "")).strip().lower()
    event_name_low = re.sub(r"\s+", " ", str(event.get("event_name") or "")).strip().lower()
    names: list[str] = []
    seen: set[str] = set()
    for nm in raw:
        nm = re.sub(r"\s+", " ", nm).strip()
        low = nm.lower()
        if not nm or low in seen or _LINEUP_WRAPPER_RE.search(low):
            continue
        # Drop the festival's own name / the venue masquerading as a performer
        # (owner 2026-06-16: "Состав: Parklife", "Состав: Delamere Forest").
        if len(low) >= 4 and (low == venue_low or low in venue_low or (event_name_low and low in event_name_low)):
            continue
        seen.add(low)
        names.append(nm)
        if len(names) >= 6:
            break
    return names


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
    # Artist name in bold; for festivals show the main lineup (also bold) so the
    # card names the acts that justify it, not just the festival title.
    head = f"<b>{title}</b>"
    lineup = _ticket_lineup(candidate)
    lineup = [n for n in lineup if n.lower() != title.lower()]
    # Only a genuine festival / multi-act lineup gets a "Состав:". A single
    # headliner with a support act (Take That + The Script) must NOT list the
    # support act as if it were a co-headliner.
    _signals = notability.get("signals") if isinstance(notability.get("signals"), dict) else {}
    _is_festival = bool(candidate.get("festival_lineup")) or str(notability.get("kind") or "") == "lineup_or_show"
    if not _is_festival or _signals.get("headliner_resolution") == "primary_headliner_locked":
        lineup = []
    lineup_part = f" Состав: {', '.join(f'<b>{n}</b>' for n in lineup)}." if lineup else ""
    if day_month and venue:
        return f"• {head} — {day_month}{time_part}, {venue}{genre_part}{price_part}.{reason_part}{lineup_part}"
    if day_month:
        return f"• {head} — {day_month}{time_part}{genre_part}{price_part}.{reason_part}{lineup_part}"
    if venue:
        return f"• {head} — {venue}{genre_part}{price_part}.{reason_part}{lineup_part}"
    return f"• {head}{genre_part}{price_part}.{reason_part}{lineup_part}"


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
    summary = re.sub(r"\s+", " ", str(candidate.get("summary") or "")).strip()
    passenger_blob = f"{title} {summary}".lower()
    passenger_effect = re.search(
        r"\b(?:bus|buses|service|services|tram|metrolink|train|rail|station|stop|diversion|diverted|closed|closure|delay|delays|cancelled)\b",
        passenger_blob,
    )
    if not passenger_effect:
        return ""
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
    if "diversion" in passenger_blob or "diverted" in passenger_blob:
        return (
            f"• {operator}: на {location} автобусы идут в объезд из-за работ. "
            "Если едете через этот участок сегодня, проверьте маршрут перед выходом."
        )
    return (
        f"• {operator}: {reason} — {location}. "
        "Проверьте маршрут и время отправления перед выходом."
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
        or str(candidate.get("category") or "") in {"venues_tickets", "football"}
        or str(candidate.get("primary_block") or "") in {"ticket_radar", "outside_gm_tickets", "next_7_days", "future_announcements", "football"}
    ):
        # Football has its own preview format — a civic/court phase lead like
        # "Решение одобрено — Бруно Фернандеш…" is nonsense there.
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
    # Lead with the phase as a natural Russian clause — no machine "Обновление:"
    # marker (owner 2026-06-13: write "предъявлено обвинение…", not
    # "Обновление: предъявлено обвинение").
    label_cap = label[:1].upper() + label[1:]
    match = re.match(r"^(•\s*[^:]{2,45}(?<!\d):\s*)(.+)$", repaired)
    if match:
        return f"{match.group(1)}{label_cap} — {match.group(2)}", ["follow_up_leads_with_change"]
    return f"• {label_cap} — {repaired.removeprefix('• ').strip()}", ["follow_up_leads_with_change"]


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
    event_dt = (
        _weekend_occurrence_datetime(candidate)
        if str(candidate.get("primary_block") or "") == _WEEKEND_BLOCK
        else None
    )
    event_dt = event_dt or _event_structured_datetime(candidate)
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
    road_match = re.search(r"\b(m6|m60|m62|m56|a580)\b", title, flags=re.IGNORECASE)
    if road_match and re.search(r"\b(?:traffic|shut|closed|closure|congestion|delays?|queues?|police incident)\b", lowered):
        road = road_match.group(1).upper()
        return (
            f"• {road}: на участке есть серьёзные ограничения из-за инцидента, возможны очереди и объезды. "
            "Если маршрут идёт через этот коридор сегодня, проверьте карту дорог и заложите больше времени."
        )
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


def _today_focus_recovery_line(candidate: dict) -> str:
    line = _hard_news_recovery_line(candidate)
    if line:
        return line
    title = re.sub(r"\s+", " ", str(candidate.get("title") or "")).strip()
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle")
    )
    lowered = blob.lower()
    if not title or _looks_like_source_chrome(title):
        return ""
    if re.search(r"\boldham\b", lowered) and re.search(r"\bpub\b", lowered) and re.search(r"\bcouncil\b", lowered) and re.search(r"\bdemolish", lowered):
        price = ""
        price_match = re.search(r"£\s?\d+(?:\.\d+)?\s?(?:m|million)?", blob, flags=re.IGNORECASE)
        if price_match:
            price = f" за {price_match.group(0).replace(' ', '')}"
        return (
            f"• Oldham: совет может купить редкий паб{price} и затем снести его; "
            "депутат парламента предупреждает о потере исторических пабов. "
            "Если вам важен район, следите за решением совета."
        )
    if re.search(r"\bbury\b", lowered) and re.search(r"\bschool\b", lowered) and re.search(r"\bchild sex offences?\b|\bsafeguarding\b", lowered):
        school = "St Gabriel's RC High School" if "st gabriel" in lowered else "школе в Bury"
        return (
            f"• Bury: женщину арестовали в {school} по подозрению в сексуальных преступлениях против ребёнка. "
            "Если это ваша школа, следите за обновлениями полиции и администрации."
        )
    if re.search(r"\bconsultation\b", lowered) and re.search(r"\b(?:open|deadline|closing|closes)\b", lowered):
        place = "Greater Manchester"
        borough_match = re.search(r"\b(Oldham|Rochdale|Bury|Bolton|Wigan|Stockport|Salford|Trafford|Tameside|Manchester)\b", blob)
        if borough_match:
            place = borough_match.group(1)
        subject = re.sub(r"\s+[|–—-]\s+.*$", "", title).strip(" .")
        return (
            f"• {place}: открыта консультация — {subject}. "
            "Если это касается вашего района, проверьте сроки и отправьте замечания до закрытия."
        )
    return ""


_SOFT_DRAFT_LINE_ERROR_MARKERS = (
    "draft_line is too short",
    "draft_line must contain at least one complete sentence",
    "draft_line for long-format category needs",
    "draft_line contains bad editorial prose marker",
)

_CORE_SOFT_RECOVERY_BLOCKS = {
    "last_24h",
    "today_focus",
    "city_watch",
    "weekend_activities",
    "next_7_days",
    "openings",
    "tech_business",
    "football",
}


def _only_soft_draft_line_errors(errors: list[str]) -> bool:
    return bool(errors) and all(
        any(marker in error for marker in _SOFT_DRAFT_LINE_ERROR_MARKERS)
        for error in errors
    )


def _core_soft_recovery_allowed(candidate: dict) -> bool:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block in {"ticket_radar", "outside_gm_tickets", "future_announcements"}:
        return False
    return block in _CORE_SOFT_RECOVERY_BLOCKS or category in {
        "media_layer",
        "gmp",
        "council",
        "public_services",
        "tech_business",
        "football",
    }


def _existing_evidence_chars(candidate: dict) -> int:
    return len(
        re.sub(
            r"\s+",
            " ",
            " ".join(str(candidate.get(field) or "") for field in ("summary", "lead", "evidence_text", "practical_angle")),
        ).strip()
    )


def _structured_event_or_ticket_complete(candidate: dict) -> bool:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if category not in {"venues_tickets", "culture_weekly", "russian_speaking_events", "diaspora_events", "professional_events"} and block not in {
        "weekend_activities",
        "ticket_radar",
        "outside_gm_tickets",
        "russian_events",
        "future_announcements",
    }:
        return False
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    has_name = bool(str(event.get("event_name") or candidate.get("title") or "").strip())
    has_date = bool(str(event.get("date_start") or event.get("date") or event.get("date_text") or "").strip())
    has_place = bool(str(event.get("venue") or event.get("borough") or "").strip())
    return has_name and has_date and has_place


_GENERIC_RECOVERY_TAIL_RE = re.compile(
    r"(?:[.;]\s*)?(?:следите\s+за\s+обновлениями[^.]*|"
    r"проверьте\s+(?:детали|обновления|подробности|сроки\s+и\s+детали)[^.]*|"
    r"уточните\s+(?:дату|время|детали|доступность)[^.]*|"
    r"свер(?:ьте|яйте)\s+(?:обновления|детали)[^.]*|"
    r"если\s+хотите\s+попасть[^.]*)\.?\s*$",
    re.IGNORECASE,
)


def _strip_generic_recovery_tail(line: str) -> str:
    text = str(line or "").strip()
    fixed = _GENERIC_RECOVERY_TAIL_RE.sub("", text).rstrip(" ;.")
    return f"{fixed}." if fixed and fixed != text else text


def _recover_soft_draft_line(candidate: dict, line: str, errors: list[str]) -> tuple[str, list[str]]:
    """Recover compact but otherwise safe core cards.

    This is deliberately narrower than a quality bypass: it only handles
    length/sentence-count defects. Factual, numeric, translation, HTML and
    sensitive-story errors still hold the item.
    """
    if not _core_soft_recovery_allowed(candidate) or not _only_soft_draft_line_errors(errors):
        return "", []
    text = re.sub(r"\s+", " ", str(line or "")).strip()
    if not text:
        replacement = _final_replacement_line(candidate)
        if replacement:
            return replacement, ["structured_replacement"]
        return "", []
    if not text.startswith("• "):
        text = f"• {text}"
    if _structured_event_or_ticket_complete(candidate):
        return text, ["short_but_complete"]
    if _existing_evidence_chars(candidate) < 180:
        stripped = _strip_generic_recovery_tail(text)
        if len(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", stripped)).strip()) >= 18:
            return stripped, ["held_thin_evidence"]
        return "", ["held_thin_evidence"]
    return "", ["needs_model_enrichment"]


def _keep_core_card_short(candidate: dict, line: str) -> tuple[str, list[str]]:
    """Deterministic last resort for core cards when model enrichment is
    unavailable or fails: keep the existing line honestly short instead of
    dropping a real story. Auto-repairs editorial-contract (glossary) defects
    and strips any generic recovery tail first. Refuses if a non-soft
    (factual/numeric/translation/sensitive/HTML) error survives — those still
    hold the item. This is the "честно короче" fallback, not a filler stamp.
    """
    if not _core_soft_recovery_allowed(candidate):
        return "", []
    repaired, _repairs = _repair_editorial_contract_line(candidate, line)
    repaired = _strip_generic_recovery_tail(repaired)
    if not repaired.startswith("• "):
        repaired = f"• {repaired.lstrip('• ').strip()}"
    if len(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", repaired)).strip()) < 18:
        return "", []
    residual = _draft_line_quality_errors(candidate, repaired)
    if residual and not _only_soft_draft_line_errors(residual):
        return "", []
    return repaired, ["held_thin_evidence"]


def _core_underflow_sections_for_ticket_throttle(section_counts: dict[str, int], *, show_weekend: bool) -> list[str]:
    underflow: list[str] = []
    for section_name, floor in CORE_EMERGENCY_FLOORS.items():
        if section_name == "Выходные в GM" and not show_weekend:
            continue
        if int(section_counts.get(section_name) or 0) < floor:
            underflow.append(section_name)
    return underflow


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
        # W9: fall back to the on-sale date parsed from the listing's own text
        # (non-Ticketmaster tickets), so "в продаже с …" can render for them too.
        return onsale_datetime_from_blob(candidate)
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
    if re.search(r"\bbarton\s+aerodrome\b", venue, flags=re.IGNORECASE):
        return "Barton Aerodrome"
    if re.search(r"\bmacron\s+stadium\b", venue, flags=re.IGNORECASE):
        return "Macron Stadium"
    venue = re.sub(
        r"\s+\b(?:You|Share|Book\s+now|Tickets?|What's\s+on|Visit|More\s+info)\b\s*$",
        "",
        venue,
        flags=re.IGNORECASE,
    ).strip(" .,-–—|")
    # Address chrome from listing pages («Churchgate Stockport, England,
    # SK1 1YG United Kingdom») is not a venue name — keep the local part.
    venue = re.sub(
        r",?\s*England\b(?:,?\s*[A-Z]{1,2}\d[\dA-Z]?(?:\s*\d[A-Z]{2})?)?(?:\s+United\s+Kingdom)?\s*$",
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
    if not _WEEKEND_SELLER_ADMIN_RE.search(blob):
        return False
    # Seller-admin phrases ("apply for a stall") legitimately contain visitor
    # words like "stall". Strip the matched seller phrases before testing for a
    # genuine visitor signal, so a bare seller page is not misread as a visitor
    # activity just because it says "stall".
    residual = _WEEKEND_SELLER_ADMIN_RE.sub(" ", blob)
    return not _WEEKEND_VISITOR_RE.search(residual)


def _event_venue(candidate: dict) -> str:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    venue = _clean_event_venue_name(str(event.get("venue") or ""))
    source_label = str(candidate.get("source_label") or "")
    generic_venue = {"greater manchester", "manchester", "bury", "rochdale", "salford"}
    if "home" not in source_label.lower():
        generic_venue.add("home")
    if (
        venue
        and venue.lower() not in generic_venue
        and not _event_venue_is_sourceish(candidate, venue)
    ):
        return venue
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
    if venue and venue.lower() not in generic_venue:
        return venue
    return ""


def _weekend_occurrence_datetime(candidate: dict) -> datetime | None:
    structured = _event_structured_datetime(candidate) or _parse_ticket_datetime(candidate)
    occurrence_day = weekend_occurrence_date(candidate)
    if occurrence_day:
        event_time = structured.timetz() if structured else datetime(2000, 1, 1, 12, 0).timetz()
        return datetime.combine(occurrence_day, event_time).replace(tzinfo=now_london().tzinfo)
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    text = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            event.get("date_text"),
            event.get("date_start"),
            event.get("date_end"),
            candidate.get("source_url"),
        )
    )
    dates = sorted(dict.fromkeys(_date_signals(text)))
    weekend_start, weekend_end = current_weekend_window()
    current_weekend_dates = [day for day in dates if weekend_start <= day <= weekend_end]
    if current_weekend_dates:
        chosen = current_weekend_dates[0]
        event_time = structured.timetz() if structured else datetime(2000, 1, 1, 12, 0).timetz()
        return datetime.combine(chosen, event_time).replace(tzinfo=now_london().tzinfo)
    today = now_london().date()
    future_dates = [day for day in dates if day >= today]
    if future_dates and (event.get("is_recurring") or _RECURRING_EVENT_MARKERS.search(text)):
        chosen = future_dates[0]
        event_time = structured.timetz() if structured else datetime(2000, 1, 1, 12, 0).timetz()
        return datetime.combine(chosen, event_time).replace(tzinfo=now_london().tzinfo)
    return structured


_MISROUTED_WEEKEND_MARKET_RE = re.compile(
    r"\b(?:asian\s+food\s+night\s+market|night\s+market|makers?\s+market|"
    r"food\s+market|street\s+food|flea\s+market|vintage\s+market|car\s*boot|"
    r"fair|fayre|festival)\b",
    re.IGNORECASE,
)


def _event_day_for_weekend_rescue(candidate: dict) -> date | None:
    event_dt = _event_structured_datetime(candidate) or _parse_ticket_datetime(candidate)
    return event_dt.date() if event_dt else None


def _is_misrouted_weekend_market_rescue(candidate: dict) -> bool:
    if not isinstance(candidate, dict) or candidate.get("include"):
        return False
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block != "openings" and category != "food_openings":
        return False
    reason = str(candidate.get("reason") or "")
    if not re.search(
        r"cross-day rehash|fingerprint already shipped|already shipped|Без новых фактов",
        reason,
        re.IGNORECASE,
    ):
        return False
    event_day = _event_day_for_weekend_rescue(candidate)
    if not event_day:
        return False
    weekend_start, weekend_end = current_weekend_window()
    if not (weekend_start <= event_day <= weekend_end):
        return False
    rubric = candidate.get("rubric_contract") if isinstance(candidate.get("rubric_contract"), dict) else {}
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    blob = " ".join(
        str(value or "")
        for value in (
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
            candidate.get("source_label"),
            candidate.get("source_url"),
            event.get("event_name"),
        )
    )
    if str(rubric.get("rubric") or "") == "weekend_market":
        return True
    return bool(_MISROUTED_WEEKEND_MARKET_RE.search(blob))


def _rescue_misrouted_weekend_markets(candidates: list[dict], warnings: list[str]) -> dict[str, object]:
    rescued: list[dict[str, object]] = []
    for candidate in candidates:
        if not _is_misrouted_weekend_market_rescue(candidate):
            continue
        previous_block = str(candidate.get("primary_block") or "")
        previous_category = str(candidate.get("category") or "")
        candidate["include"] = True
        candidate["primary_block"] = "weekend_activities"
        candidate["category"] = "culture_weekly"
        candidate["publish_plan_status"] = "show"
        candidate["writer_rescued_weekend_market"] = True
        candidate["reason"] = (
            f"{candidate.get('reason') or ''} | Writer rescue: event-like market "
            "belongs in current weekend inventory."
        ).strip()
        attach_editorial_contract(candidate)
        rescued.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "source_label": candidate.get("source_label"),
                "from_block": previous_block,
                "from_category": previous_category,
                "to_block": "weekend_activities",
                "event_date": str(_event_day_for_weekend_rescue(candidate) or ""),
            }
        )
    if rescued:
        warnings.append(f"Writer rescued {len(rescued)} misrouted current-weekend market item(s).")
    return {"count": len(rescued), "items": rescued}


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
    market_like = bool(_MARKET_EVENT_RE.search(blob))
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
        unique_prices = [price.replace(" ", "") for price in dict.fromkeys(prices[:4])]
        if market_like:
            details.append("вход " + unique_prices[0])
        else:
            details.append("цены: " + ", ".join(unique_prices))
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
    event_dt = _weekend_occurrence_datetime(candidate)
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
    # Minimum publishable weekend card = date + place + type (owner 2026-06-13:
    # "если знает дату, место и тип — публикуется нормально"). The "what's
    # there" detail is a bonus, not a gate. Only hold when we have neither a
    # concrete date nor any detail — a placeless/dateless thin card.
    if not detail_text and not day_month:
        return ""
    sentence = f"{kind}: {detail_text}" if detail_text else kind
    if prefix:
        return f"• {prefix} — {title}: {sentence}. Сверьте часы и условия перед поездкой."
    return f"• {title}: {sentence}. Сверьте часы и условия перед поездкой."


def _repair_weather_line(line: str) -> str:
    text = str(line or "")
    text = re.sub(
        r"(?:—\s*)?(?:перед\s+выходом\s+)?(?:проверьте|посмотрите)\s+"
        r"(?:локальный\s+)?радар(?:\s+по\s+своему\s+району)?\.?",
        "возьмите зонт, если планируете прогулки или пересадки.",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"держите\s+защиту\s+от\s+дождя\s+под\s+рукой",
        "возьмите зонт",
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
        repaired, transport_reasons = repair_transport_line_language(repaired)
        reasons.extend(transport_reasons)
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
    repaired, glossary_reasons = repair_glossary_terms(repaired)
    reasons.extend(glossary_reasons)
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
    "professional_events": "event",
    "food_openings": "event",
    "public_services": "public_services",
}


_RECOVERY_MODEL_MAX_PER_SECTION = 2
_CONTROLLED_ENRICHMENT_MAX_PER_RUN = 4
_RECOVERY_MODEL_MAX_TPM = 27000.0
_RECOVERY_TOKEN_LIMITER = None
_RECOVERY_MODEL_PROMPT = """Ты выпускающий редактор Greater Manchester morning brief.
Нужно восстановить один пункт для блока, который оказался слишком тонким.

Пиши только по переданным фактам. Если фактов не хватает, верни пустую строку и коротко объясни missing_facts.

Верни только JSON:
{"draft_line":"• ...","missing_facts":[]}

Правила:
- строка начинается с "• ";
- русский живой, без кальки и без generic-фраз;
- 120-380 символов;
- сохраняй даты, места, суммы, имена и неопределённость источника;
- не добавляй факты извне;
- для событий обязательно укажи что/когда/где, если это есть в evidence;
- для professional/business объясни конкретную пользу, если она есть в evidence;
- для hard news начни с того, что произошло.
"""


def _writer_recovery_token_limiter():
    global _RECOVERY_TOKEN_LIMITER
    if _RECOVERY_TOKEN_LIMITER is None:
        from news_digest.pipeline.llm_rewrite import _TokenRateLimiter  # noqa: PLC0415

        max_tpm = max(2000.0, float(os.environ.get("WRITER_RECOVERY_MAX_TPM", _RECOVERY_MODEL_MAX_TPM)))
        _RECOVERY_TOKEN_LIMITER = _TokenRateLimiter(max_tpm)
    return _RECOVERY_TOKEN_LIMITER


def _html_to_recovery_text(raw_html: str, *, limit: int = 4500) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", str(raw_html or ""))
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<nav[^>]*>.*?</nav>", " ", text)
    text = re.sub(r"(?is)<footer[^>]*>.*?</footer>", " ", text)
    text = re.sub(r"(?is)<header[^>]*>.*?</header>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def _recovery_evidence_text(candidate: dict) -> tuple[str, dict[str, object]]:
    existing = re.sub(
        r"\s+",
        " ",
        " ".join(
            str(candidate.get(field) or "")
            for field in ("title", "summary", "lead", "evidence_text", "practical_angle")
        ),
    ).strip()
    refetched = str(candidate.get("recovery_refetched_evidence") or "").strip()
    prewrite = str(candidate.get("prewrite_enrichment_text") or "").strip()
    if not prewrite:
        enrichment = candidate.get("prewrite_enrichment") if isinstance(candidate.get("prewrite_enrichment"), dict) else {}
        if enrichment.get("used_refetch") and str(candidate.get("evidence_text") or "").strip():
            prewrite = str(candidate.get("evidence_text") or "").strip()
    if len(prewrite) > len(existing) + 200:
        existing = prewrite
    elif len(refetched) > len(existing) + 200:
        existing = refetched
    report: dict[str, object] = {
        "used_refetch": False,
        "existing_chars": len(existing),
        "refetched_chars": 0,
        "source_url": str(candidate.get("source_url") or ""),
    }
    if len(existing) >= 1200:
        return existing[:4500], report
    url = str(candidate.get("source_url") or "").strip()
    if not url.startswith(("http://", "https://")):
        return existing[:4500], report
    try:
        from news_digest.pipeline.collector.fetch import _fetch_text  # noqa: PLC0415

        fetched = _html_to_recovery_text(_fetch_text(url), limit=4500)
    except Exception as exc:  # noqa: BLE001
        report["refetch_error"] = f"{exc.__class__.__name__}: {exc}"
        return existing[:4500], report
    report["refetched_chars"] = len(fetched)
    if len(fetched) > len(existing) + 200:
        report["used_refetch"] = True
        candidate["recovery_refetched_evidence"] = fetched[:4500]
        return fetched[:4500], report
    return existing[:4500], report


def _model_recover_section_line(candidate: dict, section_name: str, errors: list[str]) -> tuple[str, dict[str, object]]:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    report: dict[str, object] = {
        "attempted": False,
        "status": "skipped_missing_api_key" if not api_key else "skipped",
        "title": candidate.get("title"),
        "section": section_name,
    }
    if not api_key:
        return "", report
    evidence, evidence_report = _recovery_evidence_text(candidate)
    report["evidence"] = evidence_report
    if len(evidence) < 120:
        report["status"] = "not_enough_evidence"
        return "", report
    try:
        from openai import OpenAI  # noqa: PLC0415
        from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415
        from news_digest.pipeline.model_routing import OPENAI_SCORING_MODEL  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001
        report["status"] = "setup_failed"
        report["error"] = f"{exc.__class__.__name__}: {exc}"
        return "", report

    payload = {
        "section": section_name,
        "errors_from_previous_line": errors,
        "candidate": {
            "fingerprint": candidate.get("fingerprint"),
            "title": candidate.get("title"),
            "summary": candidate.get("summary"),
            "lead": candidate.get("lead"),
            "source_label": candidate.get("source_label"),
            "source_url": candidate.get("source_url"),
            "category": candidate.get("category"),
            "primary_block": candidate.get("primary_block"),
            "event": candidate.get("event") if isinstance(candidate.get("event"), dict) else {},
            "professional_event_match": candidate.get("professional_event_match")
            if isinstance(candidate.get("professional_event_match"), dict)
            else {},
            "evidence_text": evidence,
        },
    }
    messages = [
        {"role": "system", "content": _RECOVERY_MODEL_PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    report["attempted"] = True
    try:
        client = OpenAI(api_key=api_key, timeout=35, max_retries=0)
        _writer_recovery_token_limiter().acquire(
            int(sum(len(str(message.get("content") or "")) for message in messages) / 4) + 900
        )
        response = client.chat.completions.create(
            model=OPENAI_SCORING_MODEL,
            messages=messages,
            temperature=0.2,
            max_tokens=900,
            response_format={"type": "json_object"},
        )
        record_call_from_response(
            response=response,
            stage="writer",
            provider="OpenAI",
            model=OPENAI_SCORING_MODEL,
            prompt_name="section_floor_model_recovery",
            messages=messages,
            max_tokens=900,
        )
        raw = str(response.choices[0].message.content or "").strip()
        parsed = json.loads(raw)
    except Exception as exc:  # noqa: BLE001
        report["status"] = "model_failed"
        report["error"] = f"{exc.__class__.__name__}: {exc}"
        return "", report
    line = str(parsed.get("draft_line") or "").strip() if isinstance(parsed, dict) else ""
    if not line:
        report["status"] = "model_returned_no_line"
        if isinstance(parsed, dict):
            report["missing_facts"] = parsed.get("missing_facts") or []
        return "", report
    if not line.startswith("• "):
        line = f"• {line}"
    report["status"] = "model_returned_line"
    report["line_excerpt"] = line[:180]
    return line, report


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
    recovery_metrics: dict | None = None,
) -> tuple[list[str], list[str], list[float], list[str], list[str]]:
    """Top up a thin section up to SECTION_MIN_ITEMS by promoting any
    included/recoverable candidate whose primary_block maps to this section,
    sorted by reader value, using the LLM draft_line if present or a
    deterministic fallback otherwise. Today Focus may also pull eligible
    practical items from Fresh/City reserve; other sections stay same-block.
    Never publishes rejected/manual-review material and never adds the same
    fingerprint twice."""
    target_blocks = [
        block for block, name in PRIMARY_BLOCKS.items() if name == section_name
    ]
    if section_name == TODAY_FOCUS_SECTION:
        target_blocks = [block for block in PRIMARY_BLOCKS if block in _TODAY_FOCUS_RECOVERY_SOURCE_BLOCKS]
    if not target_blocks:
        if recovery_metrics is not None:
            recovery_metrics["still_underflow_reason"] = "no_primary_block_mapping"
        return lines, fps, scores, titles, srcs
    if recovery_metrics is not None:
        recovery_metrics.update(
            {
                "section_below_floor": True,
                "floor_target": min_floor,
                "items_before_recovery": len(lines),
                "include_backup": bool(include_backup),
                "reserve_available": 0,
                "repair_attempts": 0,
                "model_recovery_attempts": 0,
                "model_recovery_inserted": 0,
                "model_recovery_failed": 0,
                "model_recovery_examples": [],
                "short_but_complete": 0,
                "held_thin_evidence": 0,
                "replacements_inserted": 0,
                "still_underflow_reason": "",
            }
        )

    def _allowed_public_pullback(candidate: dict) -> bool:
        if candidate.get("reject_reasons"):
            return False
        if candidate.get("writer_degraded_shrink_held"):
            return False
        if (
            str(candidate.get("editorial_status") or "") == "borderline"
            and str(candidate.get("manual_override") or "") != "force_include"
        ):
            return False
        if candidate.get("include"):
            return True
        if str(candidate.get("manual_override") or "") == "force_include":
            return True
        # A clean capacity-cut backup may be pulled into a thin section. The
        # canonical predicate lives in common.py so rewrite, editor and writer
        # agree on what "recoverable reserve" means; this includes the
        # historical backup_pool_only capacity overflow when it passed all
        # upstream gates.
        if include_backup and is_recoverable_reserve(candidate):
            return True
        return bool(
            include_backup
            and candidate.get("backup_candidate")
            and not candidate.get("backup_pool_only")
        )

    promoted = 0
    pool = [
        c for c in candidates
        if isinstance(c, dict)
        and _allowed_public_pullback(c)
        and str(c.get("primary_block") or "") in target_blocks
        and str(c.get("fingerprint") or "") not in rendered_fps_so_far
        and not c.get("writer_suppressed_from_top_news")
    ]
    if recovery_metrics is not None:
        recovery_metrics["reserve_available"] = len(pool)
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
        if recovery_metrics is not None:
            recovery_metrics["repair_attempts"] = int(recovery_metrics.get("repair_attempts") or 0) + 1
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
            elif section_name == TODAY_FOCUS_SECTION:
                line = _today_focus_recovery_line(c)
                if line:
                    c["draft_line_provider"] = "writer_today_focus_recovery"
                    c["draft_line_model"] = "deterministic_today_focus_recovery"
            elif section_name == "Свежие новости":
                line = _final_replacement_line(c)
        if not line.startswith("• "):
            line = f"• {line}"
        line, repair_reasons = _repair_editorial_contract_line(c, line)
        errors = _draft_line_quality_errors(c, line)
        model_recovery_report: dict[str, object] | None = None
        if (not line.strip("• ").strip() or errors) and recovery_metrics is not None:
            attempts = int(recovery_metrics.get("model_recovery_attempts") or 0)
            if attempts < _RECOVERY_MODEL_MAX_PER_SECTION:
                recovery_metrics["model_recovery_attempts"] = attempts + 1
                model_line, model_recovery_report = _model_recover_section_line(c, section_name, errors)
                examples = recovery_metrics.get("model_recovery_examples")
                if isinstance(examples, list):
                    examples.append(model_recovery_report)
                    del examples[8:]
                if model_line:
                    model_line, model_repairs = _repair_editorial_contract_line(c, model_line)
                    model_errors = _draft_line_quality_errors(c, model_line)
                    if not model_errors:
                        line = model_line
                        errors = []
                        repair_reasons.extend(["model_floor_recovery"] + model_repairs)
                        c["draft_line"] = model_line
                        c["draft_line_provider"] = "writer_model_recovery"
                        c["draft_line_model"] = "gpt-4o-mini"
                        c["writer_model_recovered"] = True
                        recovery_metrics["model_recovery_inserted"] = int(recovery_metrics.get("model_recovery_inserted") or 0) + 1
                    else:
                        errors = model_errors
                        recovery_metrics["model_recovery_failed"] = int(recovery_metrics.get("model_recovery_failed") or 0) + 1
                else:
                    recovery_metrics["model_recovery_failed"] = int(recovery_metrics.get("model_recovery_failed") or 0) + 1
        if not line.strip("• ").strip():
            continue
        if errors:
            recovered_line, recovered_reasons = _recover_soft_draft_line(c, line, errors)
            if recovered_line:
                recovered_line, recovered_repairs = _repair_editorial_contract_line(c, recovered_line)
                recovered_errors = [] if {"short_but_complete", "held_thin_evidence"} & set(recovered_reasons) else _draft_line_quality_errors(c, recovered_line)
                if not recovered_errors:
                    line = recovered_line
                    errors = []
                    repair_reasons.extend(recovered_reasons + recovered_repairs)
                    if recovery_metrics is not None and "short_but_complete" in recovered_reasons:
                        recovery_metrics["short_but_complete"] = int(recovery_metrics.get("short_but_complete") or 0) + 1
                    if recovery_metrics is not None and "held_thin_evidence" in recovered_reasons:
                        recovery_metrics["held_thin_evidence"] = int(recovery_metrics.get("held_thin_evidence") or 0) + 1
                else:
                    errors = recovered_errors
            elif recovery_metrics is not None and "held_thin_evidence" in recovered_reasons:
                recovery_metrics["held_thin_evidence"] = int(recovery_metrics.get("held_thin_evidence") or 0) + 1
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
        if recovery_metrics is not None:
            recovery_metrics["replacements_inserted"] = int(recovery_metrics.get("replacements_inserted") or 0) + 1

    if promoted:
        warnings.append(
            f"Section «{section_name}» topped up with {promoted} item(s) "
            f"to meet floor of {min_floor}."
        )
    if recovery_metrics is not None:
        recovery_metrics["items_after_recovery"] = len(lines)
        if len(lines) < min_floor:
            if not pool:
                reason = "no_reserve_available"
            elif promoted == 0:
                reason = "reserve_failed_quality_or_caps"
            else:
                reason = "reserve_exhausted_before_floor"
            recovery_metrics["still_underflow_reason"] = reason
    return lines, fps, scores, titles, srcs


def _today_focus_loss_trace(
    candidates: list[dict],
    rendered_section_by_fp: dict[str, str],
    dropped_candidates: list[dict[str, object]],
) -> dict[str, object]:
    dropped_by_fp = {
        str(item.get("fingerprint") or ""): item
        for item in dropped_candidates
        if isinstance(item, dict)
    }
    rows: list[dict[str, object]] = []
    counts: dict[str, int] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        block = str(candidate.get("primary_block") or "")
        if block not in _TODAY_FOCUS_RECOVERY_SOURCE_BLOCKS:
            continue
        fp = str(candidate.get("fingerprint") or "")
        draft_line = str(candidate.get("draft_line") or "")
        eligible = _today_focus_candidate_is_eligible(candidate, draft_line)
        if block != "today_focus" and not eligible and not candidate.get("recoverable_reserve"):
            continue
        rendered_section = rendered_section_by_fp.get(fp, "")
        dropped = dropped_by_fp.get(fp) or {}
        if rendered_section == TODAY_FOCUS_SECTION:
            stage = "rendered_today_focus"
            reason = "visible in Today Focus"
        elif rendered_section:
            stage = "visible_elsewhere"
            reason = f"visible in {rendered_section}"
        elif dropped:
            stage = "writer_drop"
            reason = "; ".join(str(item) for item in (dropped.get("reasons") or [])) or "writer dropped candidate"
        elif candidate.get("backup_candidate") or candidate.get("recoverable_reserve"):
            stage = "recoverable_reserve_not_rendered"
            reason = str(candidate.get("rewrite_shortlist_reason") or candidate.get("digest_selection_reason") or "capacity reserve not rendered")
        elif not candidate.get("include"):
            stage = "not_included"
            reason = str(candidate.get("reason") or candidate.get("digest_selection_reason") or "not included")
        elif not draft_line.strip():
            stage = "missing_draft_line"
            reason = "included candidate had no public draft_line"
        else:
            stage = "not_selected_or_capped"
            reason = str(candidate.get("publish_plan_reason") or candidate.get("digest_selection_reason") or "not selected for final section")
        trace = candidate.get("recovery_trace") if isinstance(candidate.get("recovery_trace"), list) else []
        counts[stage] = counts.get(stage, 0) + 1
        rows.append(
            {
                "fingerprint": fp,
                "title": str(candidate.get("title") or "")[:180],
                "source_label": str(candidate.get("source_label") or ""),
                "primary_block": block,
                "category": str(candidate.get("category") or ""),
                "eligible_today_focus": bool(eligible),
                "include": bool(candidate.get("include")),
                "backup_candidate": bool(candidate.get("backup_candidate")),
                "backup_pool_only": bool(candidate.get("backup_pool_only")),
                "recoverable_reserve": bool(candidate.get("recoverable_reserve")),
                "rendered_section": rendered_section,
                "loss_stage": stage,
                "reason": reason,
                "story_type": _candidate_story_type(candidate),
                "reader_action_type": str(candidate.get("reader_action_type") or ""),
                "rewrite_shortlist_status": str(candidate.get("rewrite_shortlist_status") or ""),
                "publish_plan_status": str(candidate.get("publish_plan_status") or ""),
                "recovery_attempted": bool(trace),
                "last_recovery_step": trace[-1] if trace else {},
                "score": round(_section_priority_score(candidate, TODAY_FOCUS_SECTION, draft_line), 3),
            }
        )
    rows.sort(
        key=lambda row: (
            row["loss_stage"] != "rendered_today_focus",
            not row["eligible_today_focus"],
            -float(row["score"] or 0),
            str(row["title"]),
        )
    )
    return {
        "schema_version": 1,
        "policy": (
            "Explains every Today Focus candidate and eligible Fresh/City reserve item: "
            "where it rendered or why it did not."
        ),
        "counts": counts,
        "items": rows[:120],
    }


def _weekend_inventory_loss_trace(
    candidates: list[dict],
    rendered_section_by_fp: dict[str, str],
    dropped_candidates: list[dict[str, object]],
    *,
    show_weekend: bool,
) -> dict[str, object]:
    dropped_by_fp = {
        str(item.get("fingerprint") or ""): item
        for item in dropped_candidates
        if isinstance(item, dict)
    }
    rows: list[dict[str, object]] = []
    counts: dict[str, int] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict) or not is_weekend_inventory_candidate(candidate):
            continue
        fp = str(candidate.get("fingerprint") or "")
        rendered_section = rendered_section_by_fp.get(fp, "")
        dropped = dropped_by_fp.get(fp) or {}
        if not show_weekend:
            stage = "hidden_by_schedule"
            reason = "Weekend section is intentionally hidden Monday-Wednesday."
        elif rendered_section == "Выходные в GM":
            stage = "rendered_weekend"
            reason = "visible in Weekend"
        elif rendered_section:
            stage = "visible_elsewhere"
            reason = f"visible in {rendered_section}"
        elif dropped:
            stage = "writer_drop"
            reason = "; ".join(str(item) for item in (dropped.get("reasons") or [])) or "writer dropped candidate"
        elif candidate.get("backup_candidate") or candidate.get("recoverable_reserve"):
            stage = "recoverable_reserve_not_rendered"
            reason = str(candidate.get("rewrite_shortlist_reason") or candidate.get("digest_selection_reason") or "capacity reserve not rendered")
        elif not candidate.get("include"):
            stage = "not_included"
            reason = str(candidate.get("reason") or candidate.get("digest_selection_reason") or "not included")
        elif not str(candidate.get("draft_line") or "").strip():
            stage = "missing_draft_line"
            reason = "included Weekend Inventory candidate had no public draft_line"
        else:
            stage = "not_selected_or_capped"
            reason = str(candidate.get("publish_plan_reason") or candidate.get("digest_selection_reason") or "not selected for final Weekend section")
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        counts[stage] = counts.get(stage, 0) + 1
        rows.append(
            {
                "fingerprint": fp,
                "title": str(candidate.get("title") or "")[:180],
                "source_label": str(candidate.get("source_label") or ""),
                "source_url": str(candidate.get("source_url") or ""),
                "include": bool(candidate.get("include")),
                "rendered_section": rendered_section,
                "loss_stage": stage,
                "reason": reason,
                "date_start": str(event.get("date_start") or event.get("date") or ""),
                "date_text": str(event.get("date_text") or ""),
                "is_recurring": bool(event.get("is_recurring")),
                "rewrite_shortlist_status": str(candidate.get("rewrite_shortlist_status") or ""),
                "publish_plan_status": str(candidate.get("publish_plan_status") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            row["loss_stage"] != "rendered_weekend",
            str(row["date_start"] or "9999-12-31"),
            str(row["title"]),
        )
    )
    rendered = counts.get("rendered_weekend", 0)
    total = len(rows)
    hidden = counts.get("hidden_by_schedule", 0)
    return {
        "schema_version": 1,
        "show_weekend": show_weekend,
        "policy": (
            "Explains every eligible Weekend Inventory candidate: visible in "
            "Weekend on active days, or why it was missing/hidden."
        ),
        "counts": {
            "eligible": total,
            "rendered": rendered,
            "missing": 0 if not show_weekend else max(0, total - rendered),
            "hidden_by_schedule": hidden,
            **counts,
        },
        "items": rows[:240],
    }


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
    # A trailing «— The SK Lowdown» / «- About Manchester» is listing-site
    # chrome, not the event name — drop it when its words are already covered
    # by the source label.
    label_tokens = set(re.findall(r"[a-zа-яё\d]+", str(candidate.get("source_label") or "").lower()))
    chrome = re.search(r"\s+[—–-]\s+([^—–-]+)$", title)
    if chrome:
        tail_tokens = {t for t in re.findall(r"[a-zа-яё\d]+", chrome.group(1).lower()) if t not in {"the", "a", "an"}}
        if tail_tokens and tail_tokens <= label_tokens:
            title = title[: chrome.start()].rstrip(" .-–—")
    title = title[:120].rstrip(" .-–—")
    # A title that is just the SITE name («The SK Lowdown» with label «SK
    # Lowdown Markets») carries zero event facts — an honest shortfall beats
    # shipping «• The SK Lowdown.» as an event card (0030: show = renderable).
    title_tokens = {t for t in re.findall(r"[a-zа-яё\d]+", title.lower()) if t not in {"the", "a", "an"}}
    if not title_tokens or title_tokens <= label_tokens:
        return ""
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


def _professional_event_priority_score(candidate: dict) -> float:
    match = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
    if not match:
        match = score_professional_event(candidate)
    llm = candidate.get("professional_llm_match") if isinstance(candidate.get("professional_llm_match"), dict) else {}
    verdict = str(llm.get("fit") or candidate.get("score_verdict") or match.get("llm_fit") or "").strip().lower()
    try:
        score = float(candidate.get("score_value") if str(candidate.get("score_source") or "") == "model" else match.get("fit_score"))
    except (TypeError, ValueError):
        score = 0.0
    if verdict == "go":
        score += 1000
    elif verdict == "consider":
        score += 500
    elif verdict == "skip":
        score -= 1000
    level = str(match.get("event_level") or "")
    if level == "major_conference_or_expo":
        score += 18
    elif level == "high_value_professional":
        score += 12
    elif level == "english_practice_networking":
        score += 5
    if match.get("english_practice_value"):
        score += 4
    return score


def _professional_event_access_text(match: dict) -> str:
    label = str(match.get("access_label") or "").strip().lower()
    if label == "free":
        return "Доступ: бесплатно"
    if label == "paid":
        return "Доступ: платно"
    if label == "booking_required":
        return "Доступ: нужна регистрация"
    if label == "unknown":
        return "Доступ: стоимость нужно уточнить"
    fallback = str(match.get("free_access_reason") or "").strip()
    return f"Доступ: {fallback}" if fallback else "Доступ: уточните условия"


def _professional_event_label(level: str) -> str:
    return {
        "major_conference_or_expo": "большая конференция/экспо",
        "high_value_professional": "высокий уровень",
        "english_practice_networking": "английский и нетворк",
    }.get(level, "профессиональное событие")


def _build_professional_event_fallback_line(candidate: dict) -> str:
    match = candidate.get("professional_event_match") if isinstance(candidate.get("professional_event_match"), dict) else {}
    if not match:
        match = score_professional_event(candidate)
        candidate["professional_event_match"] = match
    if not match.get("publish"):
        return ""

    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    title = str(event.get("event_name") or candidate.get("title") or "").strip()
    title = re.sub(r"\s*\|\s*.*$", "", title).strip()
    title = re.sub(r"\s+[—–-]\s*(?:event|events?)\s*$", "", title, flags=re.IGNORECASE).strip()
    title = title[:120].rstrip(" .-–—")
    venue = _event_venue(candidate) or str(event.get("venue") or "").strip()
    event_dt = _event_structured_datetime(candidate) or _parse_ticket_datetime(candidate)
    day_month = _format_ru_day_month(event_dt) if event_dt else ""
    time_part = ""
    if event_dt and event_dt.strftime("%H:%M") not in {"00:00", "12:00"}:
        time_part = f" в {event_dt.strftime('%H:%M')}"
    if not title or (not day_month and not venue):
        return ""

    level = _professional_event_label(str(match.get("event_level") or ""))
    access = _professional_event_access_text(match)
    why = str(match.get("why_this_fits_aleksei") or "").strip()
    if not why:
        gets = match.get("what_he_gets_from_it") if isinstance(match.get("what_he_gets_from_it"), list) else []
        why = "; ".join(str(item) for item in gets[:2] if str(item).strip())
    action = "зарегистрируйтесь" if str(match.get("recommended_action") or "") == "register" else "рассмотрите регистрацию"

    where_when = ""
    if day_month and venue:
        where_when = f"{day_month}{time_part}, {venue}"
    elif day_month:
        where_when = f"{day_month}{time_part}"
    else:
        where_when = venue
    line = f"• {title} — {where_when}. Уровень: {level}; {access}."
    if why:
        line += f" Почему тебе: {why}."
    line += f" Действие: {action}."
    return re.sub(r"\s+", " ", line).strip()


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
    if category in {"culture_weekly", "russian_speaking_events", "diaspora_events", "food_openings"} or block in {"weekend_activities", "next_7_days", "russian_events", "openings"}:
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


def _has_current_weekend_recurring_signal(text: str) -> bool:
    lowered = str(text or "").lower()
    today, weekend_end = current_weekend_window()
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
    if weekend_occurrence_date(candidate):
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
    today, weekend_end = current_weekend_window()
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


_WEEKEND_DUP_STOPWORDS = {
    "the", "and", "for", "with", "festival", "fest", "event", "events",
    "market", "markets", "manchester", "mcr", "greater", "gm", "uk",
}


def _weekend_duplicate_venue(candidate: dict, line: str) -> str:
    venue = _event_venue(candidate)
    blob = " ".join(
        str(value or "")
        for value in (
            venue,
            line,
            candidate.get("title"),
            candidate.get("summary"),
            candidate.get("lead"),
            candidate.get("evidence_text"),
        )
    )
    for pattern in (
        r"\b(The\s+Yard\s+(?:MCR|Manchester)?)\b",
        r"\b(11\s+Bent\s+Street)\b",
        r"\b(Cutting\s+Room\s+Square)\b",
        r"\b(Stockport\s+Market\s+Hall)\b",
        r"\b(Sugden\s+Sports\s+Centre)\b",
    ):
        match = re.search(pattern, blob, flags=re.IGNORECASE)
        if match:
            venue = match.group(1)
            break
    venue = re.sub(r"\bmcr\b", "manchester", venue, flags=re.IGNORECASE)
    return re.sub(r"[^a-z0-9]+", " ", venue.lower()).strip()


def _weekend_duplicate_tokens(candidate: dict, line: str, title: str) -> set[str]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    text = " ".join(
        str(value or "")
        for value in (
            title,
            event.get("event_name"),
            candidate.get("title"),
            line,
        )
    ).lower()
    text = text.replace("bazar", "bazaar")
    return {
        token
        for token in re.findall(r"[a-z0-9]{3,}", text)
        if token not in _WEEKEND_DUP_STOPWORDS
    }


def _weekend_duplicate_date(candidate: dict) -> str:
    occurrence = weekend_occurrence_date(candidate)
    if occurrence:
        return occurrence.isoformat()
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    return str(event.get("date_start") or event.get("date") or "").strip()[:10]


def _collapse_weekend_duplicate_events(
    lines: list[str],
    srcs: list[str],
    fps: list[str],
    scores: list[float],
    titles: list[str],
    candidate_by_fp: dict[str, dict],
) -> tuple[list[str], list[str], list[str], list[float], list[str], list[dict[str, object]]]:
    kept: list[int] = []
    dropped: list[dict[str, object]] = []
    seen: list[tuple[str, str, set[str], int]] = []
    for idx, line in enumerate(lines):
        fp = str(fps[idx] if idx < len(fps) else "")
        candidate = candidate_by_fp.get(fp) or {}
        date_key = _weekend_duplicate_date(candidate)
        venue_key = _weekend_duplicate_venue(candidate, line)
        token_key = _weekend_duplicate_tokens(candidate, line, titles[idx] if idx < len(titles) else "")
        duplicate_of = ""
        if date_key and venue_key and len(token_key) >= 2:
            for seen_date, seen_venue, seen_tokens, seen_idx in seen:
                if seen_date == date_key and seen_venue == venue_key and len(token_key & seen_tokens) >= 2:
                    duplicate_of = str(fps[seen_idx] if seen_idx < len(fps) else "")
                    break
        if duplicate_of:
            dropped.append(
                {
                    "fingerprint": fp,
                    "title": str(candidate.get("title") or (titles[idx] if idx < len(titles) else "")),
                    "category": str(candidate.get("category") or ""),
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "is_lead": bool(candidate.get("is_lead")),
                    "reasons": [f"Duplicate weekend event already rendered ({duplicate_of})."],
                    "duplicate_of": duplicate_of,
                    "recoverable_reserve": False,
                    "story_frame": candidate.get("story_frame") or {},
                    "recovery_trace": candidate.get("recovery_trace") or [],
                }
            )
            continue
        kept.append(idx)
        if date_key and venue_key and token_key:
            seen.append((date_key, venue_key, token_key, idx))
    return (
        [lines[i] for i in kept],
        [srcs[i] if i < len(srcs) else "" for i in kept],
        [fps[i] if i < len(fps) else "" for i in kept],
        [scores[i] if i < len(scores) else 0.0 for i in kept],
        [titles[i] if i < len(titles) else "" for i in kept],
        dropped,
    )


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
    # English-first rewrite produces a source-language editorial score before
    # Russian translation. Treat it as a soft ordering signal only: if the
    # model was unavailable the field is absent and the old deterministic
    # section score remains the source of truth.
    try:
        english_score = float(candidate.get("english_editorial_score"))
    except (TypeError, ValueError):
        english_score = 0.0
    if english_score:
        score += (max(0.0, min(100.0, english_score)) - 50.0) / 4.0
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
    elif section_name == "Business/tech события для тебя":
        return _professional_event_priority_score(candidate)
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
    # Transport lines carry dates/times/route numbers from structured
    # TfGM/Metrolink extraction, not from the article evidence text, so the
    # "number not in evidence" hallucination check would false-positive on a
    # legitimate "до 29 мая" / line number (media item rerouted to transport).
    if str(candidate.get("primary_block") or "") == "transport":
        return []
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
    # Transport dates/times/route numbers come from structured extraction —
    # never strip them as "unsupported" (see _numeric_evidence_errors).
    if str(candidate.get("primary_block") or "") == "transport":
        return line, []
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
    # Removing an age digit from "в возрасте N лет" can orphan/glue the phrase
    # into "в возрастелет" / "в возрасте лет" — drop the empty age phrase.
    repaired = re.sub(r"\bв\s*возрасте\s*лет\b", "", repaired, flags=re.IGNORECASE)
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
    mixed_words = _mixed_latin_cyrillic_words(text)
    if mixed_words:
        errors.append(f"draft_line contains mixed Latin/Cyrillic word: {mixed_words[0]}.")
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
    if block_key in _EVENT_BLOCKS and _SOLD_OUT_EVENT_RE.search(
        " ".join(
            str(value or "")
            for value in (
                text,
                candidate.get("title"),
                candidate.get("summary"),
                candidate.get("lead"),
                candidate.get("evidence_text"),
            )
        )
    ):
        errors.append("sold-out event must not be published.")
    if block_key == "weather" and re.search(r"\b(?:локальн\w+\s+)?радар\b", text, re.IGNORECASE):
        errors.append("weather line must not tell the reader to check a radar.")
    if is_transport_block:
        for issue in transport_public_contract_errors(text):
            if issue == "metrolink_written_as_metro":
                errors.append("Metrolink/tram transport must not be called metro.")
    if is_transport_block and re.search(r"ремонтные работы на остановке [^.]{2,60}\.$", text, re.IGNORECASE):
        errors.append("transport stop works line must explain reader impact/action.")
    if _line_has_conflicting_event_date(candidate, text):
        errors.append("event date in draft_line conflicts with structured event date.")
    if re.search(r"\b(?:тройн\w*\s+ножев\w*\s+ранени|отдельн\w*\s+ножев\w*\s+атак|открыт\w*\s+вывод)", text, re.IGNORECASE):
        errors.append("incident/legal line contains literal translated legal/crime phrasing.")
    for issue in glossary_line_issues(text):
        if (
            is_transport_block
            and issue.startswith("glossary_translate_required:line->")
            and re.search(r"\b[A-Z][A-Za-z' -]{2,40}\s+line\b", text)
        ):
            continue
        errors.append(f"glossary contract violation: {issue}.")
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
        if block_key == "today_focus" and len(normalized) >= 90:
            # Today Focus is a practical pointer block. A clear civic/service
            # update must not be dropped merely because it is one concise
            # sentence; this was the 2026-06-18 send blocker.
            skip_min = True
        # Dated event with no struct venue (extractor gap) still gets a lower
        # floor instead of the full 150 — a complete short listing is not weak.
        min_chars = DATED_EVENT_MIN_CHARS if _has_event_date else LONG_FORMAT_MIN_CHARS
        if not skip_min:
            if len(normalized) < min_chars:
                errors.append(
                    f"draft_line for long-format category needs ≥{min_chars} chars (got {len(normalized)})."
                )
        if sentence_count < LONG_FORMAT_MIN_SENTENCES and block_key not in {"city_watch", "today_focus"} and not (_has_event_date and _event_venue(candidate)):
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


_NO_HEADLINE_FALLBACK_BLOCKS = {
    "last_24h",
    "today_focus",
    "city_watch",
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "professional_events",
}
_NO_HEADLINE_FALLBACK_CATEGORIES = REQUIRE_DRAFT_LINE_CATEGORIES | {
    "venues_tickets",
    "culture_weekly",
    "russian_speaking_events",
    "diaspora_events",
    "professional_events",
}


def _headline_fallback_forbidden(candidate: dict) -> bool:
    """Hard news, events and tickets need a real public line or deterministic
    structured card. A title-only bullet is not recovery; it is a weak visible
    row that hides the missing-facts problem from reports."""
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    return category in _NO_HEADLINE_FALLBACK_CATEGORIES or block in _NO_HEADLINE_FALLBACK_BLOCKS


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
    if block == "today_focus" and not _today_focus_candidate_is_eligible(candidate):
        return "city_watch"
    if _NON_GM_REGIONAL_RE.search(text) and not _GM_TEXT_RE.search(text):
        return "drop_non_gm_regional"
    return ""


def _next_7_market_belongs_in_weekend(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "next_7_days":
        return False
    attach_editorial_contract(candidate)
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    if str(contract.get("event_shape") or "") != "recurring":
        return False
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "source_label"))
    return bool(
        _MARKET_EVENT_RE.search(blob)
        and _ROUTINE_MARKET_RECURRENCE_RE.search(blob)
        and not _RARE_MARKET_OR_FESTIVAL_RE.search(blob)
    )


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


# Mode → scannable Russian/operator prefix. The owner wants every transport
# bullet to lead with the mode so the block scans ("Metrolink: …",
# "Автобусы: …", "National Rail: …", "Дороги: …"). The structured renderer
# always emits these, but the LLM rewrite path sometimes drops the prefix and
# opens with a bare noun ("Остановки …", "Железнодорожные услуги …").
_TRANSPORT_MODE_LABEL = {
    "tram": "Metrolink",
    "bus": "Автобусы",
    "rail": "National Rail",
    "coach": "Автобусы",
    "road": "Дороги",
}

# Heads that already act as a valid mode prefix — leave such lines untouched.
_VALID_TRANSPORT_HEAD_PREFIXES = (
    "metrolink", "автобус", "national rail", "northern", "transpennine",
    "transport for wales", "avanti", "дороги", "дорога", "tfgm",
)


def _infer_transport_label_from_text(text: str) -> str:
    low = re.sub(r"<[^>]+>", " ", str(text or "")).lower()
    if re.search(r"\b(?:metrolink|трамва\w*|tram)\b", low):
        return "Metrolink"
    if re.search(
        r"\b(?:national rail|northern|transpennine|transport for wales|avanti|поезд\w*|железнодорожн\w*|piccadilly|victoria|salford crescent)\b",
        low,
    ):
        return "National Rail"
    if re.search(r"\b(?:m6|m60|m62|m56|объезд|diversion|закрыт\w* дорог\w*|перекрыт\w*)\b", low):
        return "Дороги"
    if re.search(r"\b(?:автобус\w*|остановк\w*|\bbus\b|stagecoach|bee network)\b", low):
        return "Автобусы"
    return ""


def _ensure_transport_mode_prefix(line: str, candidate: dict | None) -> str:
    """Guarantee a transport bullet leads with its mode prefix.

    Preserves lines that already start with a recognised operator label;
    otherwise prepends the mode (from candidate.transport_mode, falling back
    to text inference). Returns the line unchanged when the mode can't be
    classified confidently — better a missing prefix than a wrong one.
    """
    raw = str(line or "")
    if not raw.strip():
        return raw
    match = re.match(r"^(\s*•\s*)(.*)$", raw, flags=re.DOTALL)
    bullet, body = (match.group(1), match.group(2)) if match else ("• ", raw)
    head = body.split(":", 1)[0].strip().lower()
    if head and any(head == p or head.startswith(p) for p in _VALID_TRANSPORT_HEAD_PREFIXES):
        return raw
    label = ""
    if isinstance(candidate, dict):
        mode = str(candidate.get("transport_mode") or "").strip().lower()
        label = _TRANSPORT_MODE_LABEL.get(mode, "")
    if not label:
        label = _infer_transport_label_from_text(body)
    if not label:
        return raw
    # Lower-case a leading Cyrillic common noun so it reads as a clause after
    # the prefix ("Остановки …" → "Автобусы: остановки …"). Latin proper nouns
    # (Piccadilly, Oxford Road) are left capitalised.
    first_word = body.split(" ", 1)[0] if body else ""
    if first_word and re.match(r"[А-ЯЁ][а-яё]", first_word):
        body = first_word[:1].lower() + body[1:]
    return f"{bullet}{label}: {body}"


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


def _a_tier_ticket_trace(
    candidates: list[dict],
    rendered_fingerprints: set[str],
    dropped_candidates: list[dict],
) -> dict[str, object]:
    dropped_by_fp = {
        str(item.get("fingerprint") or ""): item
        for item in dropped_candidates
        if isinstance(item, dict)
    }
    items: list[dict[str, object]] = []
    counts = {"total": 0, "rendered": 0, "not_rendered": 0, "blocked_by_repeat_policy": 0}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        primary_block = str(candidate.get("primary_block") or "")
        if primary_block not in {"ticket_radar", "outside_gm_tickets", "russian_events"} and str(candidate.get("category") or "") != "venues_tickets":
            continue
        notability = candidate.get("ticket_notability") if isinstance(candidate.get("ticket_notability"), dict) else {}
        tier = str(notability.get("tier") or "").strip().upper()
        if tier not in {"A", "PROTECTED"}:
            continue
        fp = str(candidate.get("fingerprint") or "")
        lifecycle = candidate.get("topic_lifecycle_repeat") if isinstance(candidate.get("topic_lifecycle_repeat"), dict) else {}
        calendar_review = lifecycle.get("calendar_repeat_review") if isinstance(lifecycle.get("calendar_repeat_review"), dict) else {}
        dropped = dropped_by_fp.get(fp) or {}
        status = "rendered" if fp in rendered_fingerprints else "not_rendered"
        if dropped:
            status = "writer_dropped"
        elif not candidate.get("include"):
            status = "not_included"
        if calendar_review.get("applies") and not calendar_review.get("allow"):
            counts["blocked_by_repeat_policy"] += 1
        counts["total"] += 1
        if status == "rendered":
            counts["rendered"] += 1
        else:
            counts["not_rendered"] += 1
        items.append(
            {
                "fingerprint": fp,
                "title": str(candidate.get("title") or "")[:180],
                "source_label": str(candidate.get("source_label") or ""),
                "primary_block": primary_block,
                "tier": tier,
                "notability_signal": notability.get("signal"),
                "include": bool(candidate.get("include")),
                "status": status,
                "drop_reasons": dropped.get("reasons") or candidate.get("reject_reasons") or [],
                "calendar_repeat_review": calendar_review,
            }
        )
    return {"counts": counts, "items": items[:80]}


_WEEKEND_FAR_FUTURE_NOTE = "weekend_activities item's event date is beyond"
_WEEKEND_GIG_REROUTE_NOTE = "weekend_solo_gig_to_ticket_radar"


def _weekend_empty_reason(candidates: list[dict], *, show_weekend: bool, weekend_lines: list) -> str:
    """W8 safety valve. When «Выходные в GM» is shown but the weekend product
    contract pruned it empty, return a reason string naming what was removed —
    so an over-enforced empty block is debuggable (and points at source
    coverage) instead of silently vanishing. Empty string when not applicable.
    """
    if not show_weekend or weekend_lines:
        return ""
    demoted = sum(
        1 for c in candidates
        if isinstance(c, dict) and _WEEKEND_FAR_FUTURE_NOTE in str(c.get("reason") or "")
    )
    gigs = sum(
        1 for c in candidates
        if isinstance(c, dict) and _WEEKEND_GIG_REROUTE_NOTE in str(c.get("reason") or "")
    )
    return (
        "«Выходные в GM» пуст после контракта: "
        f"{demoted} far-future/не-эти-выходные демотировано, {gigs} концерт(ов) уведено в билеты. "
        "Проверь покрытие weekend-источников (рынки/ярмарки/фестивали/community)."
    )


_RU_MONTH_WORD = r"(?:январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)[а-яё]*"


def _strip_unsupported_date_tokens(line: str, tokens: list[str]) -> str:
    """Вырезать из строки неподтверждённые числа-даты («20 июля 2026 года»).

    _strip_unsupported_number_phrases целится в денежные/количественные
    формулы; датные фрагменты она не трогает. Для lead-слота фактовая
    честность важнее конкретики: дату без опоры в evidence убираем целиком.
    """
    result = line
    for token in tokens:
        tok = re.escape(str(token))
        result = re.sub(rf"\s*[—–-]?\s*\b{tok}\s+{_RU_MONTH_WORD}(?:\s+\d{{4}})?(?:\s+года?)?", "", result)
        result = re.sub(rf"\s+\b{tok}\b(?=[\s.,;:!?)]|$)", "", result)
    result = re.sub(r"\s{2,}", " ", result)
    result = re.sub(r"\s+([.,;:!?])", r"\1", result)
    return result.strip()


def _produce_slot_line(
    candidate: dict,
    section_name: str,
    *,
    warnings: list[str],
    quality_counts: dict[str, int],
    controlled_enrichment_report: dict[str, object],
    execution: dict[str, object],
    stage: str = "writer",
) -> tuple[str, list[str]]:
    """Произвести строку для планового слота (Этап 3).

    Это бывшая line-production часть цикла write_digest: категория-специфичные
    детерминированные карточки → ремонты → лестница качества (обогащение и
    model-recovery в пределах ОБЩЕГО бюджета попыток из plan_execution).
    Возвращает (line, errors): пустые errors = строка готова к публикации.
    Никаких решений о составе здесь нет — только текст для уже решённого слота.
    """
    from news_digest.pipeline.plan_execution import consume_repair_attempt  # noqa: PLC0415

    block_key = str(candidate.get("primary_block") or "").strip()
    line = str(candidate.get("draft_line") or "").strip()
    title = str(candidate.get("title") or "").strip()
    lead = str(candidate.get("lead") or "").strip()
    summary = str(candidate.get("summary") or "").strip()
    category = str(candidate.get("category") or "").strip()

    if _normalize_text_key(lead) and _normalize_text_key(lead) == _normalize_text_key(summary):
        summary = ""

    english_detected = False
    if category in {"media_layer", "gmp", "public_services", "city_news", "council", "transport", "venues_tickets", "russian_speaking_events", "culture_weekly", "football", "tech_business", "food_openings", "professional_events"}:
        english_fields = [field for field in (lead, summary, title) if _looks_like_untranslated_english(field)]
        if english_fields:
            english_detected = True

    if not line and category == "transport":
        _append_recovery_step(candidate, "transport_card_recovery", "attempted", missing=(candidate.get("story_frame") or {}).get("missing_facts") or [])
        line = _build_transport_fallback_line(candidate)
        if line:
            _append_recovery_step(candidate, "transport_card_recovery", "recovered")
        else:
            _append_recovery_step(candidate, "transport_card_recovery", "held", missing=["transport_impact"])

    if not line and category == "venues_tickets":
        _append_recovery_step(candidate, "ticket_structured_recovery", "attempted", missing=(candidate.get("story_frame") or {}).get("missing_facts") or [])
        line = _build_ticket_fallback_line(candidate)
        if line:
            _append_recovery_step(candidate, "ticket_structured_recovery", "recovered")
        else:
            _append_recovery_step(candidate, "ticket_structured_recovery", "held", missing=["artist_or_date_or_venue_or_notability"])

    if not line and category == "public_services":
        _append_recovery_step(candidate, "public_service_recovery", "attempted")
        line = _build_public_service_fallback_line(candidate)
        _append_recovery_step(candidate, "public_service_recovery", "recovered")

    if not line and category == "professional_events":
        _append_recovery_step(candidate, "professional_event_card", "attempted")
        line = _build_professional_event_fallback_line(candidate)
        if line:
            _append_recovery_step(candidate, "professional_event_card", "recovered")
        else:
            _append_recovery_step(candidate, "professional_event_card", "held", missing=["fit_or_free_access_or_date"])

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
            else:
                _append_recovery_step(candidate, "event_structured_recovery", "held", missing=(candidate.get("story_frame") or {}).get("missing_facts") or [])

    if not line:
        _append_recovery_step(candidate, "official_football_recovery", "attempted")
        line = _build_football_fallback_line(candidate)
        if line:
            _append_recovery_step(candidate, "official_football_recovery", "recovered")

    if not line:
        _append_recovery_step(candidate, "hard_news_recovery", "attempted")
        recovery_line = _hard_news_recovery_line(candidate)
        if recovery_line:
            line = recovery_line
            candidate["draft_line_provider"] = "writer_hard_news_recovery"
            candidate["draft_line_model"] = "deterministic_hard_news_recovery"
            _append_recovery_step(candidate, "hard_news_recovery", "recovered")

    if not line:
        if category in REQUIRE_DRAFT_LINE_CATEGORIES:
            quality_counts["dropped_missing_draft_line"] += 1
            return "", ["missing_required_facts"]
        if _headline_fallback_forbidden(candidate):
            quality_counts["dropped_missing_draft_line"] += 1
            return "", ["missing_required_facts"]
        if english_detected:
            quality_counts["dropped_english_passthrough"] += 1
            return "", ["unrenderable_line"]
        headline = lead or title or summary
        rendered_parts: list[str] = []
        if headline:
            rendered_parts.append(html.escape(headline.rstrip(".")) + ".")
        if _summary_is_useful(summary, headline):
            rendered_parts.append(html.escape(summary.rstrip(".")) + ".")
        line = "• " + " ".join(rendered_parts).strip()

    scrubbed_line, removed_vague_endings = scrub_vague_ending(line)
    if removed_vague_endings:
        line = scrubbed_line
    if _line_claims_future_ticket_sale(candidate, line):
        line = _build_ticket_fallback_line(candidate)
        if not line:
            return "", ["expired_after_plan"]
    line, _repair_reasons = _repair_editorial_contract_line(candidate, line)

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
                else:
                    draft_line_errors = stripped_errors
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
        _append_recovery_step(candidate, "final_replacement", "attempted", missing=draft_line_errors)
        replacement = _final_replacement_line(candidate)
        if replacement and replacement != line:
            replacement, _rr = _repair_editorial_contract_line(candidate, replacement)
            replacement_errors = _draft_line_quality_errors(candidate, replacement)
            if not replacement_errors:
                line = replacement
                draft_line_errors = []
                _append_recovery_step(candidate, "final_replacement", "recovered")
            else:
                _append_recovery_step(candidate, "final_replacement", "held", missing=replacement_errors)
    skip_model_recovery = False
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
        recovered_line, recovered_reasons = _recover_soft_draft_line(candidate, line, draft_line_errors)
        if recovered_line:
            recovered_line, _rp = _repair_editorial_contract_line(candidate, recovered_line)
            recovered_errors = [] if {"short_but_complete", "held_thin_evidence"} & set(recovered_reasons) else _draft_line_quality_errors(candidate, recovered_line)
            if not recovered_errors:
                line = recovered_line
                draft_line_errors = []
                if "short_but_complete" in recovered_reasons:
                    controlled_enrichment_report["short_but_complete"] = int(controlled_enrichment_report.get("short_but_complete") or 0) + 1
                    candidate["draft_line_provider"] = "writer_short_but_complete"
                elif "held_thin_evidence" in recovered_reasons:
                    controlled_enrichment_report["held_thin_evidence"] = int(controlled_enrichment_report.get("held_thin_evidence") or 0) + 1
                    candidate["draft_line_provider"] = "writer_thin_evidence_short"
                else:
                    candidate["draft_line_provider"] = "writer_soft_line_recovery"
                _append_recovery_step(candidate, "draft_line_quality_repair", "recovered", missing=recovered_reasons)
        elif recovered_reasons:
            if "held_thin_evidence" in recovered_reasons:
                skip_model_recovery = True
                controlled_enrichment_report["held_thin_evidence"] = int(controlled_enrichment_report.get("held_thin_evidence") or 0) + 1
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors and not skip_model_recovery:
        # Обогатить-и-переписать моделью — в пределах ОБЩЕГО бюджета выпуска
        # (писатель + редактор + судья делят SHARED_REPAIR_BUDGET_PER_RUN).
        _append_recovery_step(candidate, "must_show_model_recovery", "attempted", missing=draft_line_errors)
        if not consume_repair_attempt(execution):
            model_line, model_recovery_report = "", {"status": "skipped_shared_budget", "attempted": False}
        else:
            controlled_enrichment_report["model_attempts"] = int(controlled_enrichment_report.get("model_attempts") or 0) + 1
            model_line, model_recovery_report = _model_recover_section_line(candidate, section_name, draft_line_errors)
        if model_line:
            model_line, _mr = _repair_editorial_contract_line(candidate, model_line)
            model_errors = _draft_line_quality_errors(candidate, model_line)
            if not model_errors:
                line = model_line
                draft_line_errors = []
                controlled_enrichment_report["model_enriched"] = int(controlled_enrichment_report.get("model_enriched") or 0) + 1
                candidate["draft_line"] = model_line
                candidate["draft_line_provider"] = "writer_must_show_model_recovery"
                candidate["draft_line_model"] = "gpt-4o-mini"
                _append_recovery_step(candidate, "must_show_model_recovery", "recovered")
            else:
                _append_recovery_step(candidate, "must_show_model_recovery", "held", missing=model_errors)
        else:
            _append_recovery_step(
                candidate,
                "must_show_model_recovery",
                "held",
                missing=[str(model_recovery_report.get("status") or "model_recovery_failed")],
            )
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
        kept_line, kept_reasons = _keep_core_card_short(candidate, line)
        if kept_line:
            line = kept_line
            draft_line_errors = []
            controlled_enrichment_report["held_thin_evidence"] = int(controlled_enrichment_report.get("held_thin_evidence") or 0) + 1
            candidate["draft_line"] = kept_line
            candidate["draft_line_provider"] = "writer_core_kept_short"
            _append_recovery_step(candidate, "core_kept_short", "recovered", missing=kept_reasons)
    if category in REQUIRE_DRAFT_LINE_CATEGORIES and draft_line_errors:
        quality_counts["dropped_low_quality"] += 1
        return "", draft_line_errors

    line = re.sub(r",\s*(?:жанр\s+не\s+указан|другой\s+жанр|жанр\s+не\s+определ[её]н|жанр\s+неизвестен)\s*(?=[.!?]|$)", "", line, flags=re.IGNORECASE)
    line = restore_english_toponyms(line)
    if section_name == "Погода":
        line = _repair_weather_line(line)
    return line, []


_REMOVAL_REASON_BY_ERROR_PREFIX = (
    ("draft_line contains number(s) not present", "unsupported_fact"),
    ("missing_required_facts", "missing_required_facts"),
    ("expired", "expired_after_plan"),
    ("stale", "expired_after_plan"),
)


def _removal_reason_from_errors(errors: list[str]) -> str:
    for error in errors:
        text = str(error)
        for prefix, code in _REMOVAL_REASON_BY_ERROR_PREFIX:
            if text.startswith(prefix) or code == text:
                return code
    return "unrenderable_line"


def _render_lead_line(line: str, source_url: str, source_label: str) -> str:
    line = line.lstrip("• ").strip()
    sentences = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)
    if len(sentences) == 2:
        line = f"<b>{sentences[0]}</b> {sentences[1]}"
    else:
        line = f"<b>{line}</b>"
    line = preserve_place_names(line)
    return _attach_source_anchor(line, source_url, source_label)


def write_digest(project_root: Path) -> StageResult:
    """Этап 3: писатель рендерит строго по release_plan.json.

    Состав решён планёркой. Здесь: произвести текст каждого слота; если
    строка бракованная — лестница (обогатить → пересобрать → заменить
    запасным ИЗ ПЛАНА → снять по кодифицированной причине). Результаты
    исполнения — в plan_execution_report.json; сам план не мутируется.
    """
    stage_started = time.monotonic()
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "writer_report.json"

    from news_digest.pipeline.plan_execution import (  # noqa: PLC0415
        init_execution,
        load_plan,
        next_backup,
        normalize_removal_reason,
        plan_slots,
        record_outcome,
        save_execution,
    )

    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])
    candidate_by_fp = {
        str(candidate.get("fingerprint") or ""): candidate
        for candidate in candidates
        if isinstance(candidate, dict)
    }
    plan = load_plan(state_dir)
    errors: list[str] = []
    warnings: list[str] = []
    if not plan or not plan_slots(plan):
        errors.append("release_plan.json is missing or has no slots — run plan-digest before write-digest.")
        write_json(report_path, {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "failed",
            "errors": errors,
            "warnings": warnings,
            "quality_counts": {},
            "section_counts": {},
            "rendered_candidate_fingerprints": [],
        })
        return StageResult(False, "No release plan.", report_path, draft_path)

    execution = init_execution(state_dir, plan)
    quality_counts = {
        "included_candidates": 0,
        "rendered_candidates": 0,
        "blocked_for_quality": 0,
        "held_for_editorial_quality": 0,
        "dropped_missing_draft_line": 0,
        "dropped_ticket_not_selected": 0,
        "dropped_english_passthrough": 0,
        "dropped_low_quality": 0,
        "replaced_from_plan_backup": 0,
        "removed_with_reason": 0,
    }
    controlled_enrichment_report: dict[str, object] = {
        "model_enriched": 0,
        "model_attempts": 0,
        "model_cap": "shared_plan_execution_budget",
        "short_but_complete": 0,
        "held_thin_evidence": 0,
    }
    dropped_candidates: list[dict[str, object]] = []

    ordered_sections = [str(s) for s in plan.get("ordered_sections") or []]
    slots_by_section: dict[str, list[dict]] = {}
    for slot in plan_slots(plan):
        slots_by_section.setdefault(str(slot.get("section") or ""), []).append(slot)
    for section in slots_by_section:
        slots_by_section[section].sort(key=lambda s: int(s.get("position") or 0))

    used_fingerprints: set[str] = set()
    sections_out: dict[str, list[str]] = {}
    section_sources: dict[str, list[str]] = {}
    section_fingerprints: dict[str, list[str]] = {}
    rendered_candidate_fingerprints: list[str] = []
    rendered_section_by_fp: dict[str, str] = {}

    def _finalize_bullet(candidate: dict, line: str) -> str:
        if not line.startswith("• "):
            line = f"• {line}"
        line = preserve_place_names(line)
        line = _attach_source_anchor(
            line,
            str(candidate.get("source_url") or ""),
            str(candidate.get("source_label") or ""),
        )
        return line

    def _try_slot(slot: dict, section_name: str) -> tuple[dict | None, str, str]:
        """Произвести строку слота: основной → цепочка запасных.

        Возвращает (candidate, line, status): status ∈ {shown, replaced, removed}.
        """
        slot_id = str(slot.get("slot_id") or "")
        primary_fp = str(slot.get("primary_fingerprint") or "")
        attempts: list[tuple[str, dict | None]] = [(primary_fp, candidate_by_fp.get(primary_fp))]
        first_errors: list[str] = []
        tried_backup = False
        while attempts:
            fp, candidate = attempts.pop(0)
            if candidate is None or fp in used_fingerprints:
                record_outcome(execution, slot_id, status="", failed_fingerprint=fp, reason="candidate_missing_or_used", stage="writer")
            else:
                quality_counts["included_candidates"] += 1
                line, line_errors = _produce_slot_line(
                    candidate,
                    section_name,
                    warnings=warnings,
                    quality_counts=quality_counts,
                    controlled_enrichment_report=controlled_enrichment_report,
                    execution=execution,
                )
                if line_errors and slot_id == "lead" and line:
                    # Lead-слот не умирает от мягких ошибок формата: главная
                    # история наверху важнее правила «≥150 знаков». Фактовые
                    # ошибки послабления не получают: неподтверждённые числа
                    # вырезает существующий ремонт; если после него остались
                    # только мягкие length-ошибки — строку принимаем.
                    _soft_prefix = "draft_line for long-format category needs"
                    if not all(err.startswith(_soft_prefix) for err in line_errors):
                        _stripped, _strip_repairs = _strip_unsupported_number_phrases(candidate, line)
                        if not (_strip_repairs and _stripped and _stripped != line):
                            _bad_tokens = _numeric_missing_tokens(candidate, line)
                            _stripped = _strip_unsupported_date_tokens(line, _bad_tokens) if _bad_tokens else line
                        if _stripped and _stripped != line:
                            _residual = _draft_line_quality_errors(candidate, _stripped)
                            if all(err.startswith(_soft_prefix) for err in _residual):
                                line, line_errors = _stripped, _residual
                                candidate["draft_line_provider"] = "writer_lead_soft_accept"
                    if line_errors and all(err.startswith(_soft_prefix) for err in line_errors):
                        line_errors = []
                        candidate["draft_line_provider"] = "writer_lead_soft_accept"
                if line and not line_errors:
                    status = "replaced" if tried_backup else "shown"
                    if tried_backup:
                        quality_counts["replaced_from_plan_backup"] += 1
                    record_outcome(
                        execution,
                        slot_id,
                        status=status,
                        final_fingerprint=fp,
                        reason="" if not tried_backup else f"primary_failed:{';'.join(first_errors)[:160]}",
                        stage="writer",
                    )
                    used_fingerprints.add(fp)
                    return candidate, line, status
                if not first_errors:
                    first_errors = list(line_errors) or ["unrenderable_line"]
                record_outcome(
                    execution,
                    slot_id,
                    status="",
                    failed_fingerprint=fp,
                    reason="; ".join(line_errors)[:200] or "empty_line",
                    stage="writer",
                )
                dropped_candidates.append(
                    {
                        "fingerprint": fp,
                        "title": str(candidate.get("title") or ""),
                        "category": str(candidate.get("category") or ""),
                        "primary_block": str(candidate.get("primary_block") or ""),
                        "is_lead": bool(candidate.get("is_lead")),
                        "reasons": line_errors or ["empty_line"],
                        "slot_id": slot_id,
                        "recovery_trace": candidate.get("recovery_trace") or [],
                    }
                )
            backup, backup_fp = next_backup(plan, execution, slot_id, candidate_by_fp, used_fingerprints)
            if backup is None:
                break
            tried_backup = True
            attempts.append((backup_fp, backup))
        removal = normalize_removal_reason(_removal_reason_from_errors(first_errors))
        quality_counts["removed_with_reason"] += 1
        record_outcome(execution, slot_id, status="removed", reason=removal, stage="writer")
        return None, "", "removed"

    # --- Lead ----------------------------------------------------------------
    lead_plan = plan.get("lead") if isinstance(plan.get("lead"), dict) else {}
    lead_line = ""
    if str(lead_plan.get("primary_fingerprint") or ""):
        lead_slot = {
            "slot_id": "lead",
            "primary_fingerprint": str(lead_plan.get("primary_fingerprint") or ""),
            "backup_fingerprints": [str(fp) for fp in lead_plan.get("understudy_fingerprints") or []],
        }
        lead_candidate, lead_raw, lead_status = _try_slot(lead_slot, "Главная история дня")
        if lead_candidate is not None and lead_raw:
            lead_line = _render_lead_line(
                lead_raw,
                str(lead_candidate.get("source_url") or ""),
                str(lead_candidate.get("source_label") or ""),
            )
            fp = str(lead_candidate.get("fingerprint") or "")
            rendered_candidate_fingerprints.append(fp)
            rendered_section_by_fp[fp] = "Главная история дня"
            sections_out["Главная история дня"] = [lead_line]
            section_sources["Главная история дня"] = [str(lead_candidate.get("source_label") or "")]
            section_fingerprints["Главная история дня"] = [fp]
        else:
            warnings.append("Lead slot could not be rendered — plan understudies exhausted; см. plan_execution_report.")

    # --- Обычные слоты в порядке плана ---------------------------------------
    for section_name in ordered_sections:
        if section_name == "Главная история дня":
            continue
        for slot in slots_by_section.get(section_name, []):
            candidate, line, status = _try_slot(slot, section_name)
            if candidate is None or not line:
                continue
            if section_name == "Общественный транспорт сегодня":
                line = _ensure_transport_mode_prefix(line, candidate)
            bullet = _finalize_bullet(candidate, line)
            bullet = _ensure_source_anchor_for_rendered_line(
                bullet,
                str(candidate.get("fingerprint") or ""),
                str(candidate.get("source_label") or ""),
                candidate_by_fp,
            )
            sections_out.setdefault(section_name, []).append(bullet)
            section_sources.setdefault(section_name, []).append(str(candidate.get("source_label") or ""))
            fp = str(candidate.get("fingerprint") or "")
            section_fingerprints.setdefault(section_name, []).append(fp)
            rendered_candidate_fingerprints.append(fp)
            rendered_section_by_fp[fp] = section_name

    if not sections_out.get("Общественный транспорт сегодня"):
        sections_out["Общественный транспорт сегодня"] = [_transport_empty_line(project_root)]
        section_sources["Общественный транспорт сегодня"] = ["TfGM"]
        section_fingerprints["Общественный транспорт сегодня"] = [""]
        warnings.append("Writer added honest empty-transport coverage line.")

    # --- Сборка HTML в порядке плана -----------------------------------------
    rendered: list[str] = [_title_line(), ""]
    section_counts: dict[str, int] = {}
    for section_name in ordered_sections:
        lines = sections_out.get(section_name) or []
        if not lines:
            section_counts[section_name] = 0
            continue
        section_counts[section_name] = len(lines)
        rendered.append(f"<b>{section_name}</b>")
        rendered.extend(lines)
        rendered.append("")

    quality_counts["rendered_candidates"] = len(rendered_candidate_fingerprints)
    rendered_fp_set = set(rendered_candidate_fingerprints)
    a_tier_ticket_trace = _a_tier_ticket_trace(candidates, rendered_fp_set, dropped_candidates)

    save_execution(state_dir, execution)
    draft_path.write_text("\n".join(rendered).strip() + "\n", encoding="utf-8")
    write_json(candidates_path, payload)

    slot_statuses = [str((row or {}).get("status") or "") for row in (execution.get("slots") or {}).values()]
    removed_slots = sum(1 for s in slot_statuses if s == "removed")
    if removed_slots:
        warnings.append(f"{removed_slots} plan slot(s) removed with coded reason — см. plan_execution_report.json.")

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
            "visible_item_count": len(rendered_candidate_fingerprints),
            "plan_execution": {
                "slots_total": len(plan_slots(plan)) + (1 if lead_plan.get("primary_fingerprint") else 0),
                "shown": sum(1 for s in slot_statuses if s == "shown"),
                "replaced": sum(1 for s in slot_statuses if s == "replaced"),
                "removed": removed_slots,
                "repair_attempts_used": int(execution.get("repair_attempts_used") or 0),
            },
            "controlled_enrichment": controlled_enrichment_report,
            "a_tier_ticket_trace": a_tier_ticket_trace,
            "dropped_candidates": dropped_candidates[:120],
            "rendered_candidate_fingerprints": rendered_candidate_fingerprints,
            "rendered_section_by_fingerprint": rendered_section_by_fp,
            "lead": {
                "planned_fingerprint": str(lead_plan.get("primary_fingerprint") or ""),
                "rendered": bool(lead_line),
            },
            "writer_seconds": round(time.monotonic() - stage_started, 2),
        },
    )
    message = f"Draft written by plan: {len(rendered_candidate_fingerprints)} lines, {removed_slots} slot(s) removed."
    return StageResult(not errors, message, report_path, draft_path)
