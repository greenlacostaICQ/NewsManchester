"""Deterministic transport-card fill stage.

Runs after curator-pass and BEFORE llm-rewrite. For every transport
candidate:

1. Extract a structured ``TransportCard`` (see transport_card.py).
2. Persist bounded authoritative tram restrictions independently from the
   candidate's editorial include decision.
3. For included rows, render a Russian Telegram bullet via the deterministic
   templates and write it into ``candidate["draft_line"]``.

The LLM-rewrite stage is then a no-op for transport candidates because
``draft_line`` is already populated. Tier-3 LLM fallback only kicks in
when the extractor failed completely (returned None) — handled inline
by leaving ``draft_line`` empty so the rewrite stage picks it up.

Tram disruptions with a known end_date or duration are persisted to
``data/state/active_tram_disruptions.json``. On subsequent days the
stage:

* Adds new disruptions, updates existing ones.
* Prunes records whose ``end_date`` has passed.
* Injects synthetic "reminder" candidates for every active record that
  is NOT already represented in today's transport candidates.

This keeps long-running Metrolink line closures visible every morning
until the work finishes, so readers don't forget the disruption is
still active. Bus / road / rail disruptions are not persisted — they
are typically short and one-off.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
import re

from news_digest.pipeline.common import (
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.transport_card import (
    TransportCard,
    extract_transport_card,
    render_card,
    render_reminder,
    transport_end_datetime,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


# ── Helpers ───────────────────────────────────────────────────────────────


_MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


def _ru_date_to_iso(ru_date: str, today: date) -> str | None:
    """Convert '1 июня' → 'YYYY-06-01'. Year resolves to the soonest future
    occurrence (or today's year if it's still ahead).
    """
    if not ru_date:
        return None
    m = re.match(r"\s*(\d{1,2})\s+([а-яё]+)\s*$", ru_date, re.IGNORECASE)
    if not m:
        return None
    day = int(m.group(1))
    month_ru = m.group(2).lower()
    month = _MONTHS_RU.get(month_ru)
    if not month:
        return None
    candidate_year = today.year
    try:
        d = date(candidate_year, month, day)
    except ValueError:
        return None
    if d < today:
        try:
            d = date(candidate_year + 1, month, day)
        except ValueError:
            return None
    return d.isoformat()


def _duration_to_end_iso(duration_phrase: str, start: date) -> str | None:
    """Convert 'две недели' / 'три недели' to a concrete end date."""
    if not duration_phrase:
        return None
    weeks_map = {
        "неделю": 1, "две недели": 2, "три недели": 3, "четыре недели": 4,
        "пять недель": 5, "шесть недель": 6, "семь недель": 7, "восемь недель": 8,
    }
    n = weeks_map.get(duration_phrase.strip().lower())
    if not n:
        m = re.match(r"\s*(\d+)\s+недель?\s*$", duration_phrase, re.IGNORECASE)
        if m:
            n = int(m.group(1))
    if not n:
        return None
    return (start + timedelta(weeks=n)).isoformat()


def _disruption_key(card: TransportCard) -> str:
    """Stable identifier so the same Metrolink line works don't accumulate.

    Built from operator + line + segment so two articles about the same
    Bury-line closure collapse into one persisted record.
    """
    parts = [card.operator.lower()]
    if card.line:
        parts.append(re.sub(r"\s+", "-", card.line.lower()))
    if card.segment:
        parts.append(re.sub(r"\s+", "-", card.segment.lower()))
    if not card.line and not card.segment and card.duration_phrase:
        # Network-wide work — fallback key based on duration so we don't
        # surface duplicate "network works" reminders.
        parts.append("network-" + re.sub(r"\s+", "-", card.duration_phrase.lower()))
    return "|".join(parts)


# ── State file management ─────────────────────────────────────────────────


def _load_active(state_dir: Path) -> dict[str, dict]:
    path = state_dir / "active_tram_disruptions.json"
    if not path.exists():
        return {}
    payload = read_json(path, {"records": []})
    out: dict[str, dict] = {}
    for rec in payload.get("records") or []:
        if isinstance(rec, dict) and rec.get("key"):
            out[rec["key"]] = rec
    return out


def _save_active(state_dir: Path, records: dict[str, dict]) -> Path:
    path = state_dir / "active_tram_disruptions.json"
    write_json(path, {
        "last_updated_london": today_london(),
        "records": sorted(records.values(), key=lambda r: r.get("key", "")),
    })
    return path


def _prune_expired(records: dict[str, dict], today: date) -> int:
    """Drop records whose end_date is in the past."""
    dropped = 0
    for key in list(records.keys()):
        end = records[key].get("end_date")
        if not end:
            continue
        try:
            if date.fromisoformat(end) < today:
                del records[key]
                dropped += 1
        except (TypeError, ValueError):
            continue
    return dropped


def _normalize_active_date_ranges(records: dict[str, dict]) -> int:
    """Repair legacy ranges whose already-started first date rolled a year."""
    repaired = 0
    for record in records.values():
        try:
            start = date.fromisoformat(str(record.get("start_date") or ""))
            end = date.fromisoformat(str(record.get("end_date") or ""))
        except ValueError:
            continue
        if start <= end:
            continue
        try:
            same_year_start = start.replace(year=end.year)
        except ValueError:
            continue
        if same_year_start > end:
            same_year_start = same_year_start.replace(year=end.year - 1)
        record["start_date"] = same_year_start.isoformat()
        repaired += 1
    return repaired


def _card_to_record(card: TransportCard, today: date, source_url: str = "") -> dict:
    """Serialize a TransportCard into the persisted record shape, resolving
    Russian date phrases to ISO so pruning works tomorrow."""
    end_iso = _ru_date_to_iso(card.end_date, today)
    start_iso = _ru_date_to_iso(card.start_date, today)
    # A range already in progress used to become 2027-07-13 → 2026-08-02:
    # the generic single-date parser rolled the past start forward a year.
    # Resolve a bounded range as one unit instead.
    if start_iso and end_iso:
        start = date.fromisoformat(start_iso)
        end = date.fromisoformat(end_iso)
        if start > end:
            try:
                same_year_start = start.replace(year=end.year)
            except ValueError:
                same_year_start = start
            start = (
                same_year_start
                if same_year_start <= end
                else same_year_start.replace(year=end.year - 1)
            )
            start_iso = start.isoformat()
    if not end_iso and card.duration_phrase:
        anchor = date.fromisoformat(start_iso) if start_iso else today
        end_iso = _duration_to_end_iso(card.duration_phrase, anchor)
    return {
        "key": _disruption_key(card),
        "mode": card.mode,
        "operator": card.operator,
        "line": card.line,
        "segment": card.segment,
        "start_date_ru": card.start_date,
        "end_date_ru": card.end_date,
        "duration_phrase": card.duration_phrase,
        "start_date": start_iso or "",
        "end_date": end_iso or "",
        "reason": card.reason,
        "alternative": card.alternative,
        "cost_phrase": card.cost_phrase,
        "first_seen": today.isoformat(),
        "last_confirmed": today.isoformat(),
        "source_url": source_url,
    }


_TRAM_LINE_NAMES = (
    "Manchester Airport",
    "Trafford Centre",
    "East Didsbury",
    "Altrincham",
    "Rochdale",
    "Eccles",
    "Bury",
    "Oldham",
)
_TRAM_LINES_PATTERN = "|".join(re.escape(name) for name in _TRAM_LINE_NAMES)
_NO_TRAMS_LINES_RE = re.compile(
    r"\bno\s+trams?\s+(?:would\s+|will\s+|are\s+)?"
    r"(?:operate|run|be\s+available)?\s*(?:on\s+)?(?:the\s+)?"
    r"(?P<lines>[^.;]{1,180}?)\s+lines?\b",
    re.IGNORECASE,
)
_TERMINATING_LINES_RE = re.compile(
    rf"(?P<lines>(?:(?:the\s+)?(?:{_TRAM_LINES_PATTERN})"
    rf"(?:\s*[/,&]|\s+and\s+|\s*)?)+\s+lines?)\s+"
    r"(?:would\s+|will\s+|are\s+)?terminate\s+at\s+"
    r"(?P<terminus>[A-Z][A-Za-z' -]{1,50}?)"
    rf"(?=\s+and\s+the\s+(?:{_TRAM_LINES_PATTERN})\s+line|\s+Replacement\b|[.;,]|$)",
    re.IGNORECASE,
)


def _line_names(text: str) -> list[str]:
    found: list[tuple[int, str]] = []
    for name in _TRAM_LINE_NAMES:
        match = re.search(rf"\b{re.escape(name)}\b", text, re.IGNORECASE)
        if match:
            found.append((match.start(), name))
    return [name for _, name in sorted(found)]


def _extract_tram_service_impacts(text: str) -> list[dict]:
    """Extract every explicitly named line impact from saved source evidence."""
    impacts: list[dict] = []
    seen: set[tuple[str, tuple[str, ...], str]] = set()
    for match in _NO_TRAMS_LINES_RE.finditer(text):
        lines = _line_names(match.group("lines"))
        if lines:
            key = ("no_service", tuple(lines), "")
            if key not in seen:
                impacts.append({"effect": "no_service", "lines": lines})
                seen.add(key)
    for match in _TERMINATING_LINES_RE.finditer(text):
        lines = _line_names(match.group("lines"))
        if lines:
            terminus = match.group("terminus").strip()
            key = ("terminates_at", tuple(lines), terminus.lower())
            if key not in seen:
                impacts.append({
                    "effect": "terminates_at",
                    "lines": lines,
                    "terminus": terminus,
                })
                seen.add(key)
    return impacts


def _history_fact_text(fact: dict) -> str:
    packet = fact.get("evidence_packet") if isinstance(fact.get("evidence_packet"), dict) else {}
    return " ".join(
        str(value or "")
        for value in (
            fact.get("title"),
            fact.get("semantic_text"),
            packet.get("title"),
            packet.get("summary"),
            packet.get("evidence_text"),
        )
    )


_INCIDENT_ANCHOR_DROP = {
    "https", "tfgm", "com", "travel", "updates", "alerts", "tram", "trams",
    "line", "lines", "works", "work", "improvement", "disruption",
}
_MONTHS_EN_LOWER = (
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
)


def _incident_anchor_tokens(record: dict) -> set[str]:
    blob = f"{record.get('source_url', '')} {record.get('line', '')} {record.get('segment', '')}"
    return {
        token for token in re.findall(r"[a-z]{4,}", blob.lower())
        if token not in _INCIDENT_ANCHOR_DROP
    }


def _record_end_matches_text(record: dict, text: str) -> bool:
    try:
        end = date.fromisoformat(str(record.get("end_date") or ""))
    except ValueError:
        return False
    month_en = _MONTHS_EN_LOWER[end.month - 1]
    month_ru = _ISO_MONTH_RU[end.month]
    return bool(re.search(
        rf"\b{end.day}\s+(?:{month_en}|{month_ru})\b",
        text,
        re.IGNORECASE,
    ))


def _recover_active_service_impacts(records: dict[str, dict], facts: list[dict]) -> int:
    """Upgrade legacy one-line records from facts already saved on prior days.

    No live fetch is involved. The richest matching historical evidence wins,
    then the structured result remains in ``active_tram_disruptions.json``.
    """
    recovered = 0
    for record in records.values():
        if record.get("affected_services"):
            continue
        anchors = _incident_anchor_tokens(record)
        existing_lines = {
            name for name in _line_names(f"{record.get('line', '')} {record.get('segment', '')}")
        }
        best_text = ""
        best_impacts: list[dict] = []
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            text = _history_fact_text(fact)
            impacts = _extract_tram_service_impacts(text)
            if not impacts:
                continue
            text_tokens = set(re.findall(r"[a-z]{4,}", text.lower()))
            impact_lines = {line for impact in impacts for line in impact.get("lines", [])}
            same_incident = (
                _record_end_matches_text(record, text)
                and (len(anchors & text_tokens) >= 2 or bool(existing_lines & impact_lines))
            )
            if same_incident and len(impact_lines) > len({
                line for impact in best_impacts for line in impact.get("lines", [])
            }):
                best_text = text
                best_impacts = impacts
        if not best_impacts:
            continue
        record["affected_services"] = best_impacts
        if re.search(r"\breplacement\s+buses\b", best_text, re.IGNORECASE):
            record["alternative"] = "замещающие автобусы между затронутыми остановками"
        elif not record.get("alternative") and re.search(r"\breplacement\s+bus\b", best_text, re.IGNORECASE):
            record["alternative"] = "замещающий автобус"
        recovered += 1
    return recovered


def _recover_active_from_history(state_dir: Path, records: dict[str, dict]) -> int:
    if not any(not rec.get("affected_services") for rec in records.values()):
        return 0
    payload = read_json(state_dir / "published_facts.json", {"facts": []})
    return _recover_active_service_impacts(records, payload.get("facts") or [])


_NEGATED_MOVEMENT_RE = re.compile(
    r"\b(?:trams?|metrolink|services?)\s+(?:are\s+)?not\s+affected\b|"
    r"\bno\s+(?:change|impact)\s+to\s+(?:tram|metrolink|service)",
    re.IGNORECASE,
)


def _persistent_tram_record(candidate: dict, card: TransportCard | None, today: date) -> dict | None:
    """Persist only authoritative, bounded, real tram movement restrictions."""
    if card is None or card.mode != "tram":
        return None
    source_url = str(candidate.get("source_url") or "")
    source_label = str(candidate.get("source_label") or "")
    authoritative = "tfgm.com" in source_url.lower() or source_label.strip().lower() in {"tfgm", "metrolink"}
    if not authoritative:
        return None
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text"))
    if _NEGATED_MOVEMENT_RE.search(blob):
        return None
    from news_digest.pipeline.candidate_validator import transport_movement_impact  # noqa: PLC0415

    if not transport_movement_impact(candidate):
        return None
    record = _card_to_record(card, today, source_url=source_url)
    if not str(record.get("end_date") or ""):
        return None
    impacts = _extract_tram_service_impacts(blob)
    if impacts:
        record["affected_services"] = impacts
    return record


_ISO_MONTH_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
    7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def _iso_to_ru_date(iso: str) -> str:
    if not iso:
        return ""
    try:
        d = date.fromisoformat(iso)
    except (TypeError, ValueError):
        return ""
    return f"{d.day} {_ISO_MONTH_RU[d.month]}"


# TfGM travel-alert slug tokens that are NOT part of the location name.
_TFGM_SLUG_DROP = {
    "tram", "trams", "line", "lines", "metrolink", "bus", "buses", "road",
    "rail", "improvement", "improvements", "works", "work", "closure",
    "closures", "travel", "alert", "alerts", "update", "updates",
    "disruption", "disruptions", "service", "services", "and",
}


def _location_from_tfgm_slug(url: str) -> str:
    """Recover the stop/location from a TfGM travel-alert URL slug.

    ``…/piccadilly-gardens-tram-improvement-works`` → ``Piccadilly Gardens``.
    Used only when the persisted record has no line/segment, so a reminder
    can still tell the reader WHERE the works are.
    """
    slug = (url or "").split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    words: list[str] = []
    for token in slug.split("-"):
        if not token:
            continue
        if token.lower() in _TFGM_SLUG_DROP:
            break  # location is the leading run before the first generic token
        words.append(token)
    return " ".join(w.capitalize() for w in words)


def _record_to_card(rec: dict) -> TransportCard:
    # Prefer the original Russian phrasing if persisted; otherwise rebuild
    # from the ISO date so reminders always show a concrete "до X" tail.
    end_ru = rec.get("end_date_ru") or _iso_to_ru_date(rec.get("end_date") or "")
    start_ru = rec.get("start_date_ru") or _iso_to_ru_date(rec.get("start_date") or "")
    stop_name = ""
    if not (rec.get("line") or rec.get("segment")):
        stop_name = _location_from_tfgm_slug(rec.get("source_url") or "")
    return TransportCard(
        mode=rec.get("mode") or "tram",
        operator=rec.get("operator") or "Metrolink",
        line=rec.get("line") or "",
        segment=rec.get("segment") or "",
        stop_name=stop_name,
        start_date=start_ru,
        end_date=end_ru,
        duration_phrase=rec.get("duration_phrase") or "",
        reason=rec.get("reason") or "",
        alternative=rec.get("alternative") or "",
        cost_phrase=rec.get("cost_phrase") or "",
    )


# ── Synthetic reminder candidate ──────────────────────────────────────────


def _join_ru(values: list[str]) -> str:
    if len(values) < 2:
        return "".join(values)
    return f"{', '.join(values[:-1])} и {values[-1]}"


def _render_service_impacts_reminder(rec: dict) -> str:
    impacts = rec.get("affected_services") or []
    clauses: list[str] = []
    for impact in impacts:
        lines = [str(line) for line in impact.get("lines") or [] if str(line)]
        if not lines:
            continue
        joined = _join_ru(lines)
        if impact.get("effect") == "no_service":
            clauses.append(f"трамваи не ходят по линиям {joined}")
        elif impact.get("effect") == "terminates_at" and impact.get("terminus"):
            noun = "линия" if len(lines) == 1 else "линии"
            verb = "заканчивается" if len(lines) == 1 else "заканчиваются"
            clauses.append(f"{noun} {joined} {verb} на {impact['terminus']}")
    if not clauses:
        return ""
    end_ru = rec.get("end_date_ru") or _iso_to_ru_date(rec.get("end_date") or "")
    head = f"• Metrolink (до {end_ru}): " if end_ru else "• Metrolink: "
    alternative = str(rec.get("alternative") or "")
    tail = ""
    if alternative:
        tail = f"; {alternative}"
    return f"{head}{'; '.join(clauses)}{tail} — сверяйте маршрут перед поездкой."


def _make_reminder_candidate(rec: dict, today_iso: str) -> dict:
    """Build a synthetic candidate for a Metrolink disruption that has
    no fresh article today. Routed to the transport block as a reminder.

    O2 freshness markers:
      - ``data_fetched_at`` = ``last_confirmed`` (falling back to legacy
        ``first_seen``) when authoritative TfGM evidence last confirmed it.
      - ``synthetic_stale`` = True when that confirmation is older than
        :data:`_REMINDER_STALE_DAYS`. Stale reminders still ship (the
        disruption may genuinely be ongoing) but the release report
        flags them so editorial review can ask TfGM whether the closure
        is actually over.
      - ``synthetic_fetch_attempts`` = 0 because reminders are
        synthesised from persisted state rather than fetched live.
    """
    card = _record_to_card(rec)
    line = _render_service_impacts_reminder(rec) or render_reminder(card)
    fp = f"transport-reminder|{rec.get('key', '')}|{today_iso}"
    last_confirmed = str(rec.get("last_confirmed") or rec.get("first_seen") or "")
    data_fetched_at: str | None = last_confirmed or None
    synthetic_stale = False
    if not last_confirmed:
        # No anchor at all ⇒ we have no way to prove the disruption is
        # still current ⇒ flag as stale.
        synthetic_stale = True
    else:
        try:
            today = date.fromisoformat(today_iso)
            seen = date.fromisoformat(last_confirmed)
            if (today - seen).days > _REMINDER_STALE_DAYS:
                synthetic_stale = True
        except (TypeError, ValueError):
            # Unparseable first_seen ⇒ no usable anchor ⇒ treat as stale.
            synthetic_stale = True
            data_fetched_at = None
    return {
        "fingerprint": fp,
        "title": (
            f"[reminder] {rec.get('operator', 'Metrolink')} "
            f"{', '.join(line for impact in rec.get('affected_services') or [] for line in impact.get('lines') or [])}"
        ).strip(),
        "summary": "",
        "lead": "",
        "evidence_text": "",
        "category": "transport",
        "primary_block": "transport",
        "include": True,
        "is_lead": False,
        "source_label": rec.get("operator", "Metrolink"),
        "source_url": rec.get("source_url") or "https://tfgm.com/",
        "published_at": today_iso,
        "published_date_london": today_iso,
        "freshness_status": "stale_synthetic" if synthetic_stale else "reminder",
        "dedupe_decision": "new",
        "change_type": "same_story_new_facts",
        "draft_line": line,
        "draft_line_provider": "transport_fill",
        "draft_line_model": "deterministic_reminder",
        "draft_line_written_at": now_london().isoformat(),
        "reason": "Synthetic reminder for ongoing Metrolink disruption.",
        "transport_reminder": True,
        "transport_mode": "tram",
        # ── O2 freshness markers ─────────────────────────────────────────
        "synthetic": True,
        "data_fetched_at": data_fetched_at,
        "synthetic_stale": synthetic_stale,
        "synthetic_fetch_attempts": 0,
    }


# Reminders synthesised from a `first_seen` older than this threshold
# are flagged as stale. 14 days is a soft sanity bound: routine Metrolink
# improvement works are typically 2-6 weeks, but a disruption record
# that hasn't been re-confirmed by a fresh TfGM article for two full
# weeks deserves a flag so editorial can check whether it's really
# still going. Distinct from the existing 30-day hard cap which drops
# undated records entirely.
_REMINDER_STALE_DAYS = 14


# ── Main stage ────────────────────────────────────────────────────────────


def _minimal_transport_line(candidate: dict) -> str:
    """Last-resort stub when the extractor/renderer give us nothing usable.

    A transport disruption that we *found* but could not parse into a card
    must still publish — the rule is "найдено = опубликовано". We recover a
    WHERE from the TfGM URL slug, else from the title head, and emit a short
    honest bullet that points the reader at the source instead of dropping
    the disruption to a tier-3 LLM that may never run.
    """
    url = str(candidate.get("source_url") or "")
    where = _location_from_tfgm_slug(url)
    if not where:
        title = str(candidate.get("title") or "").strip()
        head = re.split(r"\s+[—–-]\s+", title)[0].strip() if title else ""
        # Drop a trailing generic "Tram Stop" / "Tram" so the WHERE reads clean.
        head = re.sub(r"\s+Tram(\s+Stop)?$", "", head, flags=re.IGNORECASE).strip()
        where = head
    if not where:
        return ""
    return f"• Транспорт: работы в районе {where} — подробности в источнике TfGM."


# 0162: один участок и период — один пассажирский статус. Официальная лента
# оператора (TfGM / Metrolink / National Rail) владеет строкой; медийная
# статья допускается только с самостоятельным воздействием сверх неё.
_OFFICIAL_TRANSPORT_CATEGORIES = frozenset({"transport"})
_SEGMENT_TOKEN_RE = re.compile(
    r"\b(?:eccles|altrincham|trafford\s+centre|bury|rochdale|oldham|ashton|"
    r"didsbury|chorlton|salford\s+quays|media\s*city|piccadilly|victoria|"
    r"deansgate|cornbrook|stalybridge|wigan(?:\s+(?:wallgate|north\s+western))?|"
    r"liverpool(?:\s+lime\s+street)?|st\s+helens(?:\s+junction)?|southport|bolton|"
    r"stockport|prestwich|whitefield|radcliffe|droylsden|newton\s+heath|"
    r"velopark|etihad|airport|wythenshawe|sale|brooklands|timperley)\b",
    re.IGNORECASE,
)
_BROAD_RAIL_ENDPOINT_TOKENS = frozenset({"wigan", "liverpool", "st helens"})


def _transport_blob(candidate: dict) -> str:
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "draft_line", "evidence_text")
    )


def _transport_segment_tokens(candidate: dict) -> frozenset[str]:
    tokens: set[str] = set()
    for match in _SEGMENT_TOKEN_RE.finditer(_transport_blob(candidate)):
        token = match.group(0).lower().replace("  ", " ")
        if token.startswith("wigan"):
            token = "wigan"
        elif token.startswith("liverpool"):
            token = "liverpool"
        elif token.startswith("st helens"):
            token = "st helens"
        tokens.add(token)
    return frozenset(tokens)


def _transport_segment_overlap(first: dict, second: dict) -> frozenset[str]:
    shared = _transport_segment_tokens(first) & _transport_segment_tokens(second)
    # A borough/city name alone is too broad to prove the same rail incident;
    # two shared endpoints are enough. Existing line/station tokens remain
    # specific enough to match on one value (e.g. Eccles line).
    if shared and shared <= _BROAD_RAIL_ENDPOINT_TOKENS and len(shared) < 2:
        return frozenset()
    return shared


# Классы воздействия. Самостоятельным считается воздействие, которого нет
# ни в одном официальном статусе по тому же участку, — а не просто ещё одно
# название места внутри тех же работ.
_IMPACT_CLASS_RE: tuple[tuple[str, "re.Pattern[str]"], ...] = (
    ("no_service", re.compile(r"no\s+trams?|no\s+trains?|нет\s+трамва|нет\s+поезд|suspend", re.IGNORECASE)),
    ("replacement", re.compile(r"replacement\s+bus|замещающ\w*\s+автобус|buses\s+replace", re.IGNORECASE)),
    ("closure", re.compile(r"clos(?:ed|ure)|blocked|закрыт|заблокирован", re.IGNORECASE)),
    ("diversion", re.compile(r"diversion|объезд", re.IGNORECASE)),
    ("delay", re.compile(r"delay|сбой|задержк", re.IGNORECASE)),
    ("works", re.compile(r"engineering\s+work|track\s+(?:replacement|renewal)|roadworks?|ремонтн\w*\s+работ|замена\s+рельс", re.IGNORECASE)),
    ("money", re.compile(r"refund|compensation|fares?\s+(?:rise|increase)|компенсац|возврат\s+денег", re.IGNORECASE)),
    ("strike", re.compile(r"strike|industrial\s+action|забастовк", re.IGNORECASE)),
)


def _transport_impact_classes(candidate: dict) -> frozenset[str]:
    blob = _transport_blob(candidate)
    return frozenset(name for name, pattern in _IMPACT_CLASS_RE if pattern.search(blob))


def _expire_finished_transport(candidates: list[dict]) -> list[dict]:
    """Снять карточки, чьё окно закончилось до времени планирования."""
    now = now_london()
    expired: list[dict] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get("primary_block") or "") != "transport":
            continue
        if not candidate.get("include"):
            continue
        end = transport_end_datetime(candidate, now=now)
        if end is None or end > now:
            continue
        candidate["include"] = False
        candidate["transport_window_ended_at"] = end.isoformat()
        candidate["reason"] = (
            f"Transport: ограничение закончилось в {end.strftime('%H:%M')} — "
            "до времени планирования выпуска."
        )
        expired.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": str(candidate.get("title") or "")[:120],
                "end_time": end.isoformat(),
            }
        )
    return expired


def _collapse_transport_segment_duplicates(candidates: list[dict]) -> list[dict]:
    """Медийный пересказ официального статуса того же участка снимается."""
    active = [
        candidate for candidate in candidates
        if isinstance(candidate, dict)
        and str(candidate.get("primary_block") or "") == "transport"
        and candidate.get("include")
    ]
    official = [
        candidate for candidate in active
        if str(candidate.get("category") or "") in _OFFICIAL_TRANSPORT_CATEGORIES
    ]
    if not official:
        return []
    dropped: list[dict] = []
    for candidate in active:
        if str(candidate.get("category") or "") in _OFFICIAL_TRANSPORT_CATEGORIES:
            continue
        tokens = _transport_segment_tokens(candidate)
        if not tokens:
            continue
        covering = [
            official_card for official_card in official
            if _transport_segment_overlap(official_card, candidate)
        ]
        if not covering:
            continue
        shared: set[str] = set()
        official_impacts: set[str] = set()
        for official_card in covering:
            shared |= set(_transport_segment_overlap(official_card, candidate))
            official_impacts |= set(_transport_impact_classes(official_card))
        # Самостоятельное воздействие — класс последствий, которого нет ни в
        # одном официальном статусе по этому участку. Ещё одно название места
        # внутри тех же работ самостоятельным воздействием не считается.
        if _transport_impact_classes(candidate) - official_impacts:
            continue
        candidate["include"] = False
        candidate["reason"] = (
            "Transport: тот же участок и период уже закрыт официальным "
            f"пассажирским статусом ({', '.join(sorted(shared))}); "
            "самостоятельного воздействия у материала нет."
        )
        dropped.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": str(candidate.get("title") or "")[:120],
                "source_label": candidate.get("source_label"),
                "shared_segments": sorted(shared),
            }
        )
    return dropped


def run_transport_fill(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    report_path = state_dir / "transport_fill_report.json"

    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])

    today = date.fromisoformat(today_london())
    today_iso = today.isoformat()

    active = _load_active(state_dir)
    repaired_date_ranges = _normalize_active_date_ranges(active)
    pruned = _prune_expired(active, today)
    recovered_from_history = _recover_active_from_history(state_dir, active)

    filled = 0
    filled_minimal = 0  # found-but-unparseable, published as a minimal stub
    skipped = 0  # not even a title/slug to anchor a stub
    persisted = 0
    seen_keys_today: set[str] = set()
    fill_details: list[dict] = []

    for c in candidates:
        if not isinstance(c, dict):
            continue
        if str(c.get("primary_block") or "") != "transport":
            continue
        card = extract_transport_card(c)
        persistent_record = _persistent_tram_record(c, card, today)
        persistent_key = ""
        if persistent_record is not None:
            persistent_key = str(persistent_record.get("key") or "")
            if persistent_key in active:
                existing = active[persistent_key]
                for field_name in (
                    "end_date", "start_date", "line", "segment", "reason",
                    "alternative", "cost_phrase", "source_url", "affected_services",
                ):
                    if persistent_record.get(field_name):
                        existing[field_name] = persistent_record[field_name]
                existing["last_confirmed"] = today_iso
            else:
                active[persistent_key] = persistent_record
                persisted += 1
        if not c.get("include"):
            continue
        if persistent_key:
            seen_keys_today.add(persistent_key)
        # Rich-evidence alerts carry the real detail (TfGM JSON description +
        # step-free advice; NRE rail prose). The deterministic template renders
        # from the TITLE and IGNORES that evidence — it dropped the Derker
        # step-free alternative and even printed "Prestwich: нет трамваев" when
        # the description said trams are NOT affected. Send these to the LLM
        # transport rewrite (PROMPT_TRANSPORT) which writes FROM the evidence.
        # Only thin/no-evidence alerts keep the deterministic stub below.
        if len(str(c.get("evidence_text") or "").strip()) >= 120:
            continue

        # If a deterministic draft_line is already present (e.g. from a
        # previous run during the same day), don't overwrite.
        existing_draft = str(c.get("draft_line") or "").strip()
        if existing_draft and str(c.get("draft_line_provider") or "") == "transport_fill":
            if c.get("transport_reminder"):
                if not c.get("dedupe_decision"):
                    c["dedupe_decision"] = "new"
                if not c.get("change_type"):
                    c["change_type"] = "same_story_new_facts"
            continue

        rendered = render_card(card) if card is not None else ""

        # "найдено = опубликовано": a disruption we found but could not parse
        # (no operator, or a card with no usable locator → empty render) still
        # publishes as a minimal stub instead of being dropped to tier 3.
        if not rendered:
            stub = _minimal_transport_line(c)
            if not stub:
                skipped += 1
                fill_details.append({
                    "fingerprint": c.get("fingerprint"),
                    "title": c.get("title"),
                    "status": "skipped_no_card",
                })
                continue
            c["draft_line"] = stub
            c["draft_line_provider"] = "transport_fill"
            c["draft_line_model"] = "minimal_stub"
            c["draft_line_written_at"] = now_london().isoformat()
            if card is not None:
                c["transport_mode"] = card.mode
                c["expected_operator"] = card.operator
            filled += 1
            filled_minimal += 1
            fill_details.append({
                "fingerprint": c.get("fingerprint"),
                "title": c.get("title"),
                "status": "filled_minimal",
                "tier": "3",
            })
            continue

        c["draft_line"] = rendered
        c["draft_line_provider"] = "transport_fill"
        c["draft_line_model"] = "deterministic_template"
        c["draft_line_written_at"] = now_london().isoformat()
        c["transport_mode"] = card.mode
        c["expected_operator"] = card.operator
        filled += 1
        fill_details.append({
            "fingerprint": c.get("fingerprint"),
            "title": c.get("title"),
            "status": "filled",
            "mode": card.mode,
            "tier": "1" if (
                (card.has_line_or_segment or card.has_street_or_stop)
                and (card.has_dates or card.has_reason or card.has_alternative)
            ) else "2",
        })

    # ── Inject reminder candidates for active Metrolink disruptions
    #    that are NOT covered by a fresh article today. ─────────────────
    injected = 0
    for key, rec in active.items():
        if key in seen_keys_today:
            continue
        start_date = str(rec.get("start_date") or "")
        if start_date:
            try:
                if today < date.fromisoformat(start_date):
                    continue
            except ValueError:
                pass
        # Sanity: don't inject reminders for records with no operator.
        if not rec.get("operator"):
            continue
        # If end_date is missing AND first_seen is older than 30 days,
        # treat as stale and drop. Without an end date we can't auto-prune
        # so the cap stops infinite accumulation.
        first_seen = rec.get("first_seen", "")
        try:
            fs_date = date.fromisoformat(first_seen)
            if not rec.get("end_date") and (today - fs_date).days > 30:
                continue
        except (TypeError, ValueError):
            pass

        candidates.append(_make_reminder_candidate(rec, today_iso))
        injected += 1

    # 0162: закончившееся ограничение и смысловой дубль участка снимаются
    # ЗДЕСЬ, до планёрки — в выпуск попадает только то, что ещё действует,
    # и только один текст на участок.
    finished = _expire_finished_transport(candidates)
    segment_duplicates = _collapse_transport_segment_duplicates(candidates)

    candidates_path.write_text(
        __import__("json").dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _save_active(state_dir, active)

    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_iso,
            "stage_status": "complete",
            "filled": filled,
            "filled_minimal": filled_minimal,
            "skipped_no_card": skipped,
            "persisted_tram_disruptions": persisted,
            "injected_reminders": injected,
            "expired_finished": finished,
            "segment_duplicates": segment_duplicates,
            "pruned_expired": pruned,
            "active_tram_count": len(active),
            "recovered_tram_incidents_from_history": recovered_from_history,
            "repaired_tram_date_ranges": repaired_date_ranges,
            "details": fill_details,
        },
    )
    logger.info(
        "transport_fill: filled=%d skipped=%d persisted=%d reminders=%d pruned=%d active=%d",
        filled, skipped, persisted, injected, pruned, len(active),
    )
    return StageResult(True, "Transport fill stage completed.", report_path)
