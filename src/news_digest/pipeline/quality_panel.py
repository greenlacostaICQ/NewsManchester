"""Quality panel — 5 editorial indicators per SENT digest, one row per day.

Replaces the retired quality_scorecard: instead of funnel counters that were
always zero, the panel answers five yes/no questions about the issue the
reader actually received:

  1. all sections filled — no empty section, no bare "•" line;
  2. the lead story («Главная история дня») is present;
  3. zero in-issue repeats of the same story;
  4. zero placeholder phrases («следите за обновлениями» и т.п.);
  5. balance — news lines are not outnumbered by ticket lines.

Observational only: the panel never blocks a release or a send. It is
computed from the sent HTML right after a successful Telegram send and
appended to data/state/quality_panel_history.json. A short weekly summary
goes to Telegram on Sundays.
"""

from __future__ import annotations

from pathlib import Path
import re

from news_digest.pipeline.common import extract_sections, read_json, write_json
from news_digest.pipeline.editorial_contracts import VAGUE_ENDING_MARKERS

LEAD_SECTION = "Главная история дня"

TICKET_SECTIONS = frozenset({
    "Билеты / Ticket Radar",
    "Крупные концерты вне GM",
    "Русскоязычные концерты и стендап UK",
    "Дальние анонсы",
})

# Teaser section: it repeats stories that appear in full later in the same
# issue BY DESIGN, so its lines are excluded from repeat detection.
_TEASER_SECTIONS = frozenset({"Коротко"})

_HREF_RE = re.compile(r'href="([^"]+)"')
_TAG_RE = re.compile(r"<[^>]+>")

HISTORY_FILE = "quality_panel_history.json"
_HISTORY_DAYS_KEPT = 60


def _visible(line: str) -> str:
    return re.sub(r"\s+", " ", _TAG_RE.sub("", line)).strip()


def build_panel_row(html_text: str, run_date: str) -> dict[str, object]:
    sections = extract_sections(html_text)
    empty_sections = [name for name, lines in sections.items() if not lines]
    empty_lines = sum(
        1 for raw in html_text.splitlines() if raw.strip() in {"•", "• "}
    )
    lead_lines = sections.get(LEAD_SECTION) or []
    lead_present = bool([line for line in lead_lines if _visible(line)])

    seen_urls: dict[str, int] = {}
    seen_texts: dict[str, int] = {}
    repeat_examples: list[str] = []
    repeat_lines = 0
    news_lines = 0
    ticket_lines = 0
    placeholder_lines = 0
    placeholder_examples: list[str] = []
    for name, lines in sections.items():
        for line in lines:
            visible = _visible(line)
            if not visible or visible == "•":
                continue
            if name in TICKET_SECTIONS:
                ticket_lines += 1
            else:
                news_lines += 1
            lowered = visible.lower()
            if any(marker in lowered for marker in VAGUE_ENDING_MARKERS):
                placeholder_lines += 1
                if len(placeholder_examples) < 3:
                    placeholder_examples.append(visible[:100])
            if name in _TEASER_SECTIONS:
                continue
            line_is_repeat = False
            normalized = lowered.lstrip("• ").strip()
            if normalized in seen_texts:
                line_is_repeat = True
            seen_texts[normalized] = seen_texts.get(normalized, 0) + 1
            for url in _HREF_RE.findall(line):
                if url in seen_urls and not line_is_repeat:
                    line_is_repeat = True
                seen_urls[url] = seen_urls.get(url, 0) + 1
            if line_is_repeat:
                repeat_lines += 1
                if len(repeat_examples) < 3:
                    repeat_examples.append(visible[:100])

    tickets_dominate = ticket_lines > news_lines
    ok = (
        not empty_sections
        and empty_lines == 0
        and lead_present
        and repeat_lines == 0
        and placeholder_lines == 0
        and not tickets_dominate
    )
    return {
        "date": run_date,
        "ok": ok,
        "empty_sections": empty_sections,
        "empty_lines": empty_lines,
        "lead_present": lead_present,
        "repeat_lines": repeat_lines,
        "repeat_examples": repeat_examples,
        "placeholder_lines": placeholder_lines,
        "placeholder_examples": placeholder_examples,
        "news_lines": news_lines,
        "ticket_lines": ticket_lines,
        "tickets_dominate": tickets_dominate,
    }


def panel_row_line(row: dict[str, object]) -> str:
    """One human-readable line per day — the whole point of the panel."""
    marks = [
        "секции ✓" if not row.get("empty_sections") and not row.get("empty_lines")
        else f"секции ✗ (пустые: {', '.join(map(str, row.get('empty_sections') or [])) or 'пустые строки: ' + str(row.get('empty_lines'))})",
        "лид ✓" if row.get("lead_present") else "лид ✗",
        "повторы 0" if not row.get("repeat_lines") else f"повторы {row.get('repeat_lines')}",
        "заглушки 0" if not row.get("placeholder_lines") else f"заглушки {row.get('placeholder_lines')}",
        f"новости/билеты {row.get('news_lines')}/{row.get('ticket_lines')}"
        + (" ✗" if row.get("tickets_dominate") else ""),
    ]
    verdict = "OK" if row.get("ok") else "ЕСТЬ ЗАМЕЧАНИЯ"
    return f"{row.get('date')}: {'; '.join(marks)} → {verdict}"


def append_panel_row(state_dir: Path, row: dict[str, object]) -> Path:
    path = state_dir / HISTORY_FILE
    history = read_json(path, {"days": []}) if path.exists() else {"days": []}
    days = [d for d in history.get("days") or [] if isinstance(d, dict) and d.get("date") != row.get("date")]
    days.append(row)
    days.sort(key=lambda d: str(d.get("date") or ""))
    write_json(path, {"days": days[-_HISTORY_DAYS_KEPT:]})
    return path


def weekly_panel_summary(state_dir: Path, *, days: int = 7) -> str | None:
    path = state_dir / HISTORY_FILE
    if not path.exists():
        return None
    rows = [d for d in (read_json(path, {"days": []}).get("days") or []) if isinstance(d, dict)]
    week = rows[-days:]
    if not week:
        return None
    ok_days = sum(1 for r in week if r.get("ok"))
    lines = [f"📋 Качество выпусков за неделю: {ok_days} из {len(week)} дней без замечаний"]
    for r in week:
        lines.append(f"• {panel_row_line(r)}")
    cost_line = _weekly_cost_line(state_dir, days=days)
    if cost_line:
        lines.append(cost_line)
    return "\n".join(lines)


def _weekly_cost_line(state_dir: Path, *, days: int = 7) -> str | None:
    """One line of LLM spend — replaces the separate weekly cost message."""
    path = state_dir / "cost_history.json"
    if not path.exists():
        return None
    try:
        import json  # noqa: PLC0415

        history = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(history, list) or not history:
        return None
    week = history[-days:]
    total = sum(float(e.get("total_cost_usd") or 0.0) for e in week if isinstance(e, dict))
    return f"💰 Расходы за {len(week)} дн.: ${total:.2f} (≈${total / len(week):.2f}/день)"
