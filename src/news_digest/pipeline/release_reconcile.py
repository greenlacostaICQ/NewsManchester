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

from news_digest.pipeline.common import SECTION_MIN_ITEMS, canonical_url_identity, extract_sections

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

    divergences = []
    for section, intended in (writer_section_counts or {}).items():
        shipped = html_counts.get(section, 0)
        if int(intended or 0) != shipped:
            divergences.append({"section": section, "writer": int(intended or 0), "html": shipped})

    lead_visible = html_counts.get(LEAD_SECTION, 0) >= 1

    recovered: list[dict[str, Any]] = []
    still_short: list[dict[str, Any]] = []
    inserted_total = 0
    for section, minimum in minimums.items():
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

    if inserted_total:
        draft_path.write_text(html_text, encoding="utf-8")
        html_counts = _html_section_counts(html_text)

    return {
        "schema_version": 1,
        "enabled": True,
        "html_section_counts": html_counts,
        "lead_visible": lead_visible,
        "control_assertion": {
            "ok": not divergences,
            "writer_vs_html_divergent_sections": divergences,
            "note": "Counts must be measured on the shipped HTML, not writer_report (RC1).",
        },
        "recovered": recovered,
        "inserted_total": inserted_total,
        "still_under_minimum": still_short,
    }
