"""Wave 1 / S4: the visible-HTML contract.

The shipped HTML — not ``writer_report`` counts — is the single source of truth.
Before release promotes ``draft_digest.html`` to ``outgoing/current_digest.html``
this module reparses the draft, reconciles it against the per-section minimums
and the lead contract, runs a *bounded* recovery from the unified recoverable
reserve (S1), rewrites the draft in place, and reports the control assertion
``writer_counts == HTML bullet counts``.

It never aborts the issue (never-block): an unrecoverable shortfall is reported
with a human-readable reason and the issue still ships. The recovery only ever
*appends* bullets to a section that already exists — it never deletes, reorders
or rewrites unrelated lines, and the whole pass is bounded by
``RECOVERY_INSERT_CAP`` so chasing a minimum can never balloon the issue.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from news_digest.pipeline.common import (
    PRIMARY_BLOCKS,
    SECTION_MIN_ITEMS,
    canonical_url_identity,
    extract_sections,
    now_london,
)

LEAD_SECTION = "Главная история дня"
RECOVERY_INSERT_CAP = 8

_HEADING_RE = re.compile(r"<b>([^<]+)</b>")
_HREF_RE = re.compile(r'href="([^"]+)"')


def _html_section_counts(html_text: str) -> dict[str, int]:
    return {section: len(lines) for section, lines in extract_sections(html_text).items()}


def _existing_url_idents(html_text: str) -> set[str]:
    # Match the identity _same_section_reserve_line dedups on (canonical), so a
    # recovered line can never duplicate an item already visible in the issue.
    return {canonical_url_identity(url) for url in _HREF_RE.findall(html_text) if url}


def _section_minimum_active(section: str, html_counts: dict[str, int]) -> bool:
    # Weekend planning is intentionally not rendered Monday-Wednesday. Treating
    # the absent block as a release shortfall created false "broken Weekend"
    # reports and masked the real issue: completeness only on active Weekend days.
    if section == "Выходные в GM" and now_london().weekday() < 3:
        return False
    return section in html_counts or section in {"Погода", "Что важно сегодня", "Свежие новости"}


def _candidate_bullet_line(candidate: dict[str, Any]) -> str:
    """A renderable bullet line for a specific must_show candidate, used to
    recover it when it is missing from the HTML."""
    line = str(candidate.get("draft_line") or "").strip()
    if not line:
        return ""
    if not line.startswith("• "):
        line = f"• {line}"
    url = str(candidate.get("source_url") or "")
    if url and "<a " not in line.lower():
        label = str(candidate.get("source_label") or "источник")
        line = f'{line} <a href="{url}">{label}</a>'
    return line


def _candidate_lead_line(candidate: dict[str, Any]) -> str:
    """A renderable LEAD line (bold first sentence, no bullet) mirroring the
    writer lead format, used to recover a lost curator lead into the lead block
    instead of demoting it to a plain bullet in its primary_block section."""
    line = str(candidate.get("draft_line") or "").strip()
    if not line:
        return ""
    line = line.lstrip("• ").strip()
    sentences = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)
    line = f"<b>{sentences[0]}</b> {sentences[1]}" if len(sentences) == 2 else f"<b>{line}</b>"
    url = str(candidate.get("source_url") or "")
    if url and "<a " not in line.lower():
        label = str(candidate.get("source_label") or "источник")
        line = f'{line} <a href="{url}">{label}</a>'
    return line


def _insert_or_create_lead(html_text: str, lead_line: str) -> tuple[str, int]:
    """Place a recovered lead into the lead block. If the lead heading exists,
    insert the bold line after it; if the writer never rendered a lead (heading
    absent), create the block right after the brief title so the day's main story
    is visible in «Главная история дня» rather than lost or demoted into «Свежие»."""
    if not lead_line.strip().startswith("<b>"):
        return html_text, 0
    lines = html_text.splitlines()
    trailing = "\n" if html_text.endswith("\n") else ""
    for idx, raw in enumerate(lines):
        match = _HEADING_RE.fullmatch(raw.strip())
        if match and match.group(1).strip() == LEAD_SECTION:
            merged = lines[: idx + 1] + [lead_line] + lines[idx + 1 :]
            return "\n".join(merged) + trailing, 1
    for idx, raw in enumerate(lines):
        match = _HEADING_RE.fullmatch(raw.strip())
        if match and match.group(1).strip().startswith("Greater Manchester Brief"):
            merged = lines[: idx + 1] + ["", f"<b>{LEAD_SECTION}</b>", lead_line] + lines[idx + 1 :]
            return "\n".join(merged) + trailing, 1
    return html_text, 0


def insert_bullets_after_section(
    html_text: str, section_heading: str, new_bullets: list[str]
) -> tuple[str, int]:
    """Append ``new_bullets`` after the last existing bullet of the section (or
    directly after its ``<b>heading</b>`` line when it has none). Returns the new
    text and the number inserted. If the section heading is absent, nothing is
    inserted (the caller reports the shortfall instead of fabricating a block).
    """
    bullets = [b for b in new_bullets if str(b).strip().startswith("• ")]
    if not bullets:
        return html_text, 0
    lines = html_text.splitlines()
    head_idx: int | None = None
    for idx, raw in enumerate(lines):
        match = _HEADING_RE.fullmatch(raw.strip())
        if match and match.group(1).strip() == section_heading:
            head_idx = idx
            break
    if head_idx is None:
        return html_text, 0
    insert_at = head_idx + 1
    cursor = head_idx + 1
    while cursor < len(lines):
        stripped = lines[cursor].strip()
        if _HEADING_RE.fullmatch(stripped):
            break
        if stripped.startswith("• "):
            insert_at = cursor + 1
        cursor += 1
    merged = lines[:insert_at] + list(bullets) + lines[insert_at:]
    trailing = "\n" if html_text.endswith("\n") else ""
    return "\n".join(merged) + trailing, len(bullets)


def reconcile_visible_html(
    draft_path: Path,
    candidates: list[dict[str, Any]],
    writer_section_counts: dict[str, int] | None,
    *,
    min_items: dict[str, int] | None = None,
    insert_cap: int = RECOVERY_INSERT_CAP,
) -> dict[str, Any]:
    """Reconcile the shipped draft against the contract and run bounded recovery."""
    # Local import keeps the editor↔release dependency one-directional and avoids
    # an import cycle at module load.
    from news_digest.pipeline.editor import _same_section_reserve_line

    minimums = dict(min_items or SECTION_MIN_ITEMS)
    html_text = draft_path.read_text(encoding="utf-8") if draft_path.exists() else ""
    html_counts = _html_section_counts(html_text)
    rendered_urls = _existing_url_idents(html_text)

    recovered: list[dict[str, Any]] = []
    still_short: list[dict[str, Any]] = []
    inserted_total = 0
    for section, minimum in minimums.items():
        if not _section_minimum_active(section, html_counts):
            continue
        actual = html_counts.get(section, 0)
        if actual >= minimum:
            continue
        new_bullets: list[str] = []
        while actual + len(new_bullets) < minimum and inserted_total < insert_cap:
            # _same_section_reserve_line dedups on rendered_urls and adds the
            # identity it used, so each call returns a fresh recoverable item and
            # never repeats one already visible in the issue.
            line = _same_section_reserve_line(section, candidates, rendered_urls)
            if not line or not line.strip().startswith("• "):
                break
            new_bullets.append(line)
            inserted_total += 1
        if new_bullets:
            html_text, added = insert_bullets_after_section(html_text, section, new_bullets)
            recovered.append(
                {"section": section, "added": added, "from": actual, "to": actual + added, "minimum": minimum}
            )
            actual += added
        if actual < minimum:
            still_short.append(
                {
                    "section": section,
                    "actual": actual,
                    "minimum": minimum,
                    "reason": "no_recoverable_reserve_with_facts",
                }
            )

    # P0-2: the must_show contract — every protected/must_show item must be
    # visible in the HTML or honestly reported. (S4 originally checked only
    # section minimums; this enforces item-level "selected => visible or
    # replaced".) Recover a missing must_show item by inserting its own line.
    must_show_missing: list[dict[str, Any]] = []
    must_show_recovered = 0
    for candidate in candidates:
        if not (candidate.get("publish_plan_must_show") or str(candidate.get("publish_plan_status") or "") == "must_show"):
            continue
        ident = canonical_url_identity(str(candidate.get("source_url") or "")) if candidate.get("source_url") else ""
        if ident and ident in rendered_urls:
            continue  # already visible in the issue
        # A curator lead recovers into the lead block (bold, top of issue), never
        # as a plain bullet demoted into its primary_block section (which both
        # left «Главная история дня» empty and duplicated the story into «Свежие»).
        is_lead = bool(candidate.get("is_lead"))
        section = LEAD_SECTION if is_lead else PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""))
        line = _candidate_lead_line(candidate) if is_lead else _candidate_bullet_line(candidate)
        added = 0
        if line and inserted_total < insert_cap:
            if is_lead:
                html_text, added = _insert_or_create_lead(html_text, line)
            elif section:
                html_text, added = insert_bullets_after_section(html_text, section, [line])
        if added:
            inserted_total += added
            must_show_recovered += added
            if ident:
                rendered_urls.add(ident)
        else:
            must_show_missing.append(
                {
                    "title": str(candidate.get("title") or "")[:80],
                    "section": section or "",
                    "reason": "no_draft_line_or_section_absent_or_cap_reached",
                }
            )

    if inserted_total:
        draft_path.write_text(html_text, encoding="utf-8")
        html_counts = _html_section_counts(html_text)

    # P1: compute every report invariant on the FINAL shipped HTML, never a
    # pre-recovery snapshot — a recovered / must_show line legitimately changes
    # the counts, so a stale lead_visible or divergence would misreport what was
    # actually sent ("report says pass, HTML already different").
    lead_visible = html_counts.get(LEAD_SECTION, 0) >= 1
    divergences = [
        {"section": section, "writer": int(intended or 0), "html": html_counts.get(section, 0)}
        for section, intended in (writer_section_counts or {}).items()
        if int(intended or 0) != html_counts.get(section, 0)
    ]
    # ok = the shipped issue honours the contract: lead visible, no section below
    # its minimum, no must_show item missing — NOT "writer == html" (recovery and
    # intentional editor trims make those differ on purpose).
    contract_ok = lead_visible and not still_short and not must_show_missing

    return {
        "schema_version": 1,
        "enabled": True,
        "html_section_counts": html_counts,
        "lead_visible": lead_visible,
        "control_assertion": {
            "ok": contract_ok,
            "writer_vs_html_divergent_sections": divergences,
            "note": "ok = lead visible + minimums met + must_show present, measured on the shipped HTML.",
        },
        "recovered": recovered,
        "inserted_total": inserted_total,
        "still_under_minimum": still_short,
        "must_show_recovered": must_show_recovered,
        "must_show_missing": must_show_missing,
    }
