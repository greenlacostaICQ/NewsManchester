"""Editorial decision cascade — Sprint Quality Fix 1.

Replaces the implicit "either include=True or include=False" reflex of the
older validator with a four-step cascade:

    enrich  →  demote  →  borderline  →  reject

A candidate that fails a freshness/clarity check is no longer dropped
outright. The decision module attaches a structured ``editorial_decision``
field with:

    status            publish | demote | borderline | reject
    why_now           new_today | update_today | happening_today |
                      deadline_soon | ongoing | stale | unclear
    freshness_severity none | soft | hard | expired
    age_days          int | None
    reasons           [str, ...]   non-publishing reasons (machine codes)
    notes             [str, ...]   human-readable explanations

Reject is reserved for material the reader cannot use under any
interpretation (events with a past start date, search/index URLs, etc.).
Everything softer flows through demote (still publishable, ranked lower)
or borderline (held back for manual review). This keeps the
"never block release" guarantee — borderline items skip the digest, the
release ships anyway.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
import re
from typing import Iterable

from news_digest.pipeline.common import now_london


# ── Type taxonomy ────────────────────────────────────────────────────────────

EVENT_CATEGORIES = {"culture_weekly", "venues_tickets", "russian_speaking_events"}
EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "ticket_radar",
    "outside_gm_tickets",
    "russian_events",
    "future_announcements",
}
OPENING_CATEGORIES = {"food_openings"}
NEWS_CATEGORIES = {
    "council",
    "city_news",
    "gmp",
    "public_services",
    "media_layer",
    "tech_business",
    "football",
}
TRANSPORT_CATEGORIES = {"transport"}

# Sprint Fix 1 — Q2 freshness windows (in days) per content kind. The first
# number is "demote after"; the second is "borderline after". Anything past
# the borderline window is held back from the digest entirely. Reasoning:
#   • Openings — a new café is news for ~3 days, then it's just listings.
#   • News    — councils/police statements have a one-week half-life.
#   • Events  — past start date is hard reject (handled by validator).
FRESHNESS_WINDOWS = {
    "opening": (3, 7),
    "news": (7, 14),
    "transport": (3, 7),  # ongoing-disruption logic is handled separately
}

# Sprint Fix 1 — words that hint at a same-source rehash without new facts.
# A rehash that is also stale gets demoted aggressively.
REHASH_CHANGE_TYPES = {"reminder", "same_story_new_facts"}


# ── Status / freshness enums (string constants for JSON friendliness) ────────

STATUS_PUBLISH = "publish"
STATUS_DEMOTE = "demote"
STATUS_BORDERLINE = "borderline"
STATUS_REJECT = "reject"

WHY_NEW_TODAY = "new_today"
WHY_UPDATE_TODAY = "update_today"
WHY_HAPPENING_TODAY = "happening_today"
WHY_DEADLINE_SOON = "deadline_soon"
WHY_ONGOING = "ongoing"
WHY_STALE = "stale"
WHY_UNCLEAR = "unclear"

FRESH_NONE = "none"
FRESH_SOFT = "soft"
FRESH_HARD = "hard"
FRESH_EXPIRED = "expired"


@dataclass(slots=True)
class Decision:
    status: str = STATUS_PUBLISH
    why_now: str = WHY_UNCLEAR
    freshness_severity: str = FRESH_NONE
    age_days: int | None = None
    reasons: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


# ── Date extraction (cheap, multi-source) ────────────────────────────────────

_ISO_DATE_RE = re.compile(r"\b(20\d{2})-(\d{1,2})-(\d{1,2})\b")
_DAY_MONTH_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(?P<month>january|february|march|april|may|june|july|august|"
    r"september|october|november|december)"
    r"(?:\s+(?P<year>20\d{2}))?\b",
    re.IGNORECASE,
)
_SUMMARY_FIELD_RE = re.compile(
    r"\b(?P<field>event_date|public_onsale|published_at)="
    r"(?P<value>20\d{2}-\d{1,2}-\d{1,2})"
)

_MONTH_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Try plain YYYY-MM-DD first.
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    # Fall back to datetime / ISO with TZ.
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _candidate_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field_) or "")
        for field_ in (
            "title",
            "summary",
            "lead",
            "evidence_text",
            "practical_angle",
            "draft_line",
        )
    )


def extract_content_date(candidate: dict, today: date) -> date | None:
    """Best-effort content date — the date the candidate is *about*.

    Source priority:
        1. event.date            (set by event-extraction stage)
        2. summary event_date=…  (Eventbrite/Ticketmaster parsers)
        3. published_at          (when the source published)
        4. first concrete day+month token in title/summary/lead/evidence_text
           — used because the 2026-05-20 failure showed Trof / Golders Green
           carrying the real date in the title only.
    """
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    iso = _parse_iso_date(event.get("date"))
    if iso is not None:
        return iso

    summary = str(candidate.get("summary") or "")
    for match in _SUMMARY_FIELD_RE.finditer(summary):
        if match.group("field") in {"event_date", "public_onsale"}:
            parsed = _parse_iso_date(match.group("value"))
            if parsed is not None:
                return parsed

    blob = _candidate_blob(candidate)

    for match in _ISO_DATE_RE.finditer(blob):
        try:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return date(year, month, day)
        except ValueError:
            continue

    best: date | None = None
    for match in _DAY_MONTH_RE.finditer(blob):
        try:
            day = int(match.group("day"))
        except ValueError:
            continue
        month_name = match.group("month").lower()
        month = _MONTH_NUM[month_name]
        year_token = match.group("year")
        if year_token:
            try:
                candidate_date = date(int(year_token), month, day)
            except ValueError:
                continue
        else:
            try:
                candidate_date = date(today.year, month, day)
            except ValueError:
                continue
            # Year ambiguity: if "in this year" lands far in the past, the
            # writer almost certainly meant next year.
            if candidate_date < today and (today - candidate_date).days > 180:
                try:
                    candidate_date = candidate_date.replace(year=today.year + 1)
                except ValueError:
                    continue
        if best is None or candidate_date < best:
            best = candidate_date

    if best is not None:
        return best

    published_at = candidate.get("published_at")
    return _parse_iso_date(published_at)


def _is_ongoing_disruption(candidate: dict) -> bool:
    """Transport reminders for multi-day works should stay fresh while active."""
    blob = _candidate_blob(candidate).lower()
    if "until" in blob or "ongoing" in blob:
        return True
    summary = str(candidate.get("summary") or "")
    return "end_date=" in summary or "duration=" in summary


# ── Type detection ───────────────────────────────────────────────────────────

def candidate_kind(candidate: dict) -> str:
    """Coarse content-kind bucket used to pick the freshness window."""
    category = str(candidate.get("category") or "")
    block = str(candidate.get("primary_block") or "")
    if category in OPENING_CATEGORIES or block == "openings":
        return "opening"
    if category in TRANSPORT_CATEGORIES or block == "transport":
        return "transport"
    if category in EVENT_CATEGORIES or block in EVENT_BLOCKS:
        return "event"
    if category in NEWS_CATEGORIES:
        return "news"
    return "other"


# ── Why-now classifier ───────────────────────────────────────────────────────

def classify_why_now(candidate: dict, today: date) -> tuple[str, int | None]:
    """Return (why_now tag, age_days). Age is days since the content date.

    A few invariants worth knowing:
      • Events with a known future start date → ``happening_today`` /
        ``deadline_soon`` / ``new_today`` depending on proximity.
      • Past-date events → ``stale`` and freshness_severity=expired upstream.
      • Ongoing-disruption transport entries → ``ongoing`` regardless of age.
    """
    kind = candidate_kind(candidate)
    content_date = extract_content_date(candidate, today)

    if kind == "transport" and _is_ongoing_disruption(candidate):
        return WHY_ONGOING, None

    if content_date is None:
        # No date we could anchor to. Decide later whether to demote/borderline
        # based on type — opening/news without any date is suspicious.
        return WHY_UNCLEAR, None

    age = (today - content_date).days

    # Future content: events, deadlines.
    if age < 0:
        days_ahead = -age
        if days_ahead == 0:
            return WHY_HAPPENING_TODAY, age
        if days_ahead <= 3:
            return WHY_DEADLINE_SOON, age
        return WHY_NEW_TODAY, age

    if age == 0:
        return WHY_NEW_TODAY, age
    if age == 1:
        return WHY_UPDATE_TODAY, age

    # Stale-decision threshold depends on the kind.
    demote_after, borderline_after = FRESHNESS_WINDOWS.get(kind, (7, 14))
    if age > borderline_after:
        return WHY_STALE, age
    if age > demote_after:
        # Still publishable but past prime; signal it.
        return WHY_UPDATE_TODAY, age
    return WHY_UPDATE_TODAY, age


# ── Cascade ──────────────────────────────────────────────────────────────────

def _has_named_place(candidate: dict) -> bool:
    """Soft proxy for "place known" — used by clarity caskcade for non-event
    kinds (planning/property/crime) where reject would be too aggressive."""
    blob = _candidate_blob(candidate)
    # Either an explicit entity is attached, or the blob carries a
    # capitalised proper-noun group + a venue/road token. The default
    # event_quality regex is stricter; here we lean lenient so important
    # news without a perfect address still ships (with a quality_warning).
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    for key in ("venues", "districts", "boroughs", "stations"):
        if entities.get(key):
            return True
    return bool(re.search(
        r"\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)+\s+"
        r"(?:Centre|Center|Arena|Hall|Square|Park|Market|Road|Street|Avenue|Lane|Gallery|Museum|Stadium|Bridge)\b",
        blob,
    ))


def _is_event_kind(candidate: dict) -> bool:
    return candidate_kind(candidate) == "event"


def _clarity_severity(candidate: dict, kind: str) -> tuple[str, list[str]]:
    """Sprint Fix 1 C1+C2 — clarity check that does **not** hard-reject.

    For non-event kinds (planning/property/crime/news) we want to keep the
    candidate around but flag it so it ranks lower and shows up in the
    admin's "what needs enrichment" pile. Pure events keep their existing
    hard gate (handled by the legacy validator).
    """
    if kind in {"event"}:
        # The legacy event_quality gate already covers events — don't
        # double-penalise here.
        return FRESH_NONE, []
    warnings: list[str] = []
    if not _has_named_place(candidate):
        warnings.append("clarity:no_named_place")
    if not warnings:
        return FRESH_NONE, []
    # Borderline only when there is *nothing* — no place AND no district AND
    # no entities at all. Otherwise it's a demote with a warning.
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    has_any_entity = any(entities.get(k) for k in ("boroughs", "districts", "venues", "stations"))
    severity = FRESH_SOFT if has_any_entity else FRESH_HARD
    return severity, warnings


def _is_rehash(candidate: dict) -> bool:
    change_type = str(candidate.get("change_type") or "")
    return change_type in REHASH_CHANGE_TYPES


def decide(candidate: dict, today: date | None = None) -> Decision:
    """Compute an editorial decision for a single candidate.

    Pure function — does not mutate the candidate. Apply the result with
    ``apply_decision`` once the caller has decided to write it back.
    """
    today = today or now_london().date()
    kind = candidate_kind(candidate)
    why_now, age_days = classify_why_now(candidate, today)

    decision = Decision(
        status=STATUS_PUBLISH,
        why_now=why_now,
        age_days=age_days,
    )

    # ── Freshness cascade by kind ────────────────────────────────────────
    demote_after, borderline_after = FRESHNESS_WINDOWS.get(kind, (7, 14))

    if why_now == WHY_ONGOING:
        # Ongoing disruption / persistent reminder — freshness windows do
        # not apply. The transport_fill stage already controls when these
        # roll off (via active_tram_disruptions end_date).
        pass
    elif kind == "event" and age_days is not None and age_days > 0:
        # Event with a past start date — hard reject. The reader can't
        # attend something that has already happened.
        decision.status = STATUS_REJECT
        decision.freshness_severity = FRESH_EXPIRED
        decision.reasons.append("event_date_past")
        decision.notes.append(
            f"Event start date is {age_days} day(s) in the past."
        )
    elif age_days is not None and age_days > borderline_after and kind in {"opening", "news"}:
        decision.status = STATUS_BORDERLINE
        decision.freshness_severity = FRESH_HARD
        decision.reasons.append(f"stale_{kind}_over_{borderline_after}d")
        decision.notes.append(
            f"{kind.title()} is {age_days} days old (> {borderline_after}d borderline)."
        )
    elif age_days is not None and age_days > demote_after and kind in {"opening", "news"}:
        if decision.status == STATUS_PUBLISH:
            decision.status = STATUS_DEMOTE
        decision.freshness_severity = FRESH_SOFT
        decision.reasons.append(f"stale_{kind}_over_{demote_after}d")
        decision.notes.append(
            f"{kind.title()} is {age_days} days old (> {demote_after}d demote)."
        )
    elif why_now == WHY_UNCLEAR and kind in {"opening", "news", "event"}:
        # No date at all for an opening/news/event is suspicious. Demote and
        # warn — the LLM-rewrite stage may yet enrich a real date from the
        # source body (Sprint Fix 1 still ships the demote signal so admin
        # can see where enrichment is missing).
        decision.status = STATUS_DEMOTE
        decision.freshness_severity = FRESH_SOFT
        decision.reasons.append("no_anchor_date")
        decision.notes.append("Could not extract a content date.")

    # ── Rehash penalty (Q3) ──────────────────────────────────────────────
    # Ongoing-disruption reminders are intentionally a rehash by design —
    # transport_fill keeps re-emitting them until end_date. Don't punish.
    if why_now == WHY_ONGOING:
        pass
    elif _is_rehash(candidate) and decision.status == STATUS_PUBLISH:
        decision.status = STATUS_DEMOTE
        decision.reasons.append("same_story_rehash")
        decision.notes.append("Repeated story without a new fact — ranked lower.")
    elif _is_rehash(candidate) and decision.status == STATUS_DEMOTE:
        # Already demoted for staleness; stack the warning so it sorts to
        # the bottom of the digest tail.
        decision.reasons.append("same_story_rehash")

    # ── Clarity cascade (C1/C2) — never reject, only demote/borderline ──
    severity, warnings = _clarity_severity(candidate, kind)
    if warnings:
        decision.reasons.extend(warnings)
        # Hard clarity (no place, no entities) always escalates to borderline
        # — even if the candidate was only demoted for another reason. A
        # story with no usable location AND no anchor date is exactly the
        # generic-property case the 2026-05-20 audit flagged.
        if severity == FRESH_HARD and decision.status in {STATUS_PUBLISH, STATUS_DEMOTE}:
            decision.status = STATUS_BORDERLINE
            decision.notes.append("No identifiable place and no attached entities.")
        elif severity == FRESH_SOFT and decision.status == STATUS_PUBLISH:
            decision.status = STATUS_DEMOTE
            decision.notes.append("Location is unclear; ranked lower pending enrichment.")
        # If already borderline, just record the warning.

    return decision


# ── Apply to candidate dict (mutating) ───────────────────────────────────────

def apply_decision(candidate: dict, decision: Decision) -> None:
    """Write the decision into the candidate dict.

    Layered on top of the existing ``include`` / ``reason`` contract:
      • status=publish    → include unchanged
      • status=demote     → include unchanged; ``editorial_demoted`` = True
      • status=borderline → include=False; ``borderline``=True
      • status=reject     → include=False; reason appended

    Writer + release can read ``editorial_decision`` for richer signals
    (why_now tag, freshness_severity, age_days) without losing the old
    boolean contract used elsewhere.
    """
    candidate["editorial_decision"] = decision.to_dict()
    candidate["why_now"] = decision.why_now

    if decision.status == STATUS_REJECT:
        candidate["include"] = False
        note = "; ".join(decision.notes) or "rejected by editorial cascade"
        existing = str(candidate.get("reason") or "").strip()
        candidate["reason"] = f"{existing} | Editorial: {note}".strip(" |") if existing else f"Editorial: {note}"
        candidate["reject_reasons"] = sorted(set(
            [str(r) for r in candidate.get("reject_reasons") or [] if str(r).strip()]
            + decision.reasons
        ))
    elif decision.status == STATUS_BORDERLINE:
        candidate["include"] = False
        candidate["borderline"] = True
        candidate["editorial_demoted"] = False
        note = "; ".join(decision.notes) or "held for manual review"
        existing = str(candidate.get("reason") or "").strip()
        candidate["reason"] = f"{existing} | Editorial: {note}".strip(" |") if existing else f"Editorial: {note}"
        if decision.reasons:
            candidate["quality_warnings"] = sorted(set(
                [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                + [f"editorial:{r}" for r in decision.reasons]
            ))
    elif decision.status == STATUS_DEMOTE:
        candidate["editorial_demoted"] = True
        candidate["borderline"] = False
        if decision.reasons:
            candidate["quality_warnings"] = sorted(set(
                [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                + [f"editorial:{r}" for r in decision.reasons]
            ))
    else:  # publish
        candidate["editorial_demoted"] = False
        candidate["borderline"] = False


def decide_and_apply(candidate: dict, today: date | None = None) -> Decision:
    decision = decide(candidate, today=today)
    apply_decision(candidate, decision)
    return decision


__all__ = [
    "Decision",
    "STATUS_PUBLISH",
    "STATUS_DEMOTE",
    "STATUS_BORDERLINE",
    "STATUS_REJECT",
    "WHY_NEW_TODAY",
    "WHY_UPDATE_TODAY",
    "WHY_HAPPENING_TODAY",
    "WHY_DEADLINE_SOON",
    "WHY_ONGOING",
    "WHY_STALE",
    "WHY_UNCLEAR",
    "FRESH_NONE",
    "FRESH_SOFT",
    "FRESH_HARD",
    "FRESH_EXPIRED",
    "FRESHNESS_WINDOWS",
    "candidate_kind",
    "classify_why_now",
    "extract_content_date",
    "decide",
    "apply_decision",
    "decide_and_apply",
]
