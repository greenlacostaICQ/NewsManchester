"""Final visible-HTML contract.

The shipped HTML, not ``writer_report`` counts, is the single source of truth.
Writer/editor own content selection and same-block recovery, while release
measures the resulting HTML. The existing curator-lead guard remains until the
separate 0114 judge package replaces it; no ordinary section row is inserted.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from news_digest.pipeline.common import PRIMARY_BLOCKS, SECTION_MIN_ITEMS, canonical_url_identity, extract_sections, now_london

LEAD_SECTION = "Главная история дня"
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


def _candidate_lead_line(candidate: dict[str, Any]) -> str:
    line = str(candidate.get("draft_line") or "").strip().lstrip("• ").strip()
    if not line:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)
    line = f"<b>{sentences[0]}</b> {sentences[1]}" if len(sentences) == 2 else f"<b>{line}</b>"
    url = str(candidate.get("source_url") or "")
    if url and "<a " not in line.lower():
        line = f'{line} <a href="{url}">{str(candidate.get("source_label") or "источник")}</a>'
    return line


def _insert_or_create_lead(html_text: str, lead_line: str) -> tuple[str, int]:
    if not lead_line.strip().startswith("<b>"):
        return html_text, 0
    lines = html_text.splitlines()
    trailing = "\n" if html_text.endswith("\n") else ""
    for idx, raw in enumerate(lines):
        match = _HEADING_RE.fullmatch(raw.strip())
        if match and match.group(1).strip() == LEAD_SECTION:
            return "\n".join(lines[: idx + 1] + [lead_line] + lines[idx + 1 :]) + trailing, 1
    for idx, raw in enumerate(lines):
        match = _HEADING_RE.fullmatch(raw.strip())
        if match and match.group(1).strip().startswith("Greater Manchester Brief"):
            return "\n".join(
                lines[: idx + 1] + ["", f"<b>{LEAD_SECTION}</b>", lead_line] + lines[idx + 1 :]
            ) + trailing, 1
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
    resolved_dispositions: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Measure the final draft without changing public content."""
    minimums = dict(min_items or SECTION_MIN_ITEMS)
    html_text = draft_path.read_text(encoding="utf-8") if draft_path.exists() else ""
    html_counts = _html_section_counts(html_text)
    rendered_urls = _existing_url_idents(html_text)
    resolved_dispositions = dict(resolved_dispositions or {})

    still_short: list[dict[str, Any]] = []
    for section, minimum in minimums.items():
        if not _section_minimum_active(section, html_counts):
            continue
        actual = html_counts.get(section, 0)
        if actual >= minimum:
            continue
        still_short.append(
            {
                "section": section,
                "actual": actual,
                "minimum": minimum,
                "reason": "writer_editor_did_not_supply_eligible_visible_item",
            }
        )

    must_show_missing: list[dict[str, Any]] = []
    must_show_resolved: list[dict[str, Any]] = []
    lead_guard_recovered = 0
    for candidate in candidates:
        if not (candidate.get("publish_plan_must_show") or str(candidate.get("publish_plan_status") or "") == "must_show"):
            continue
        ident = canonical_url_identity(str(candidate.get("source_url") or "")) if candidate.get("source_url") else ""
        if ident and ident in rendered_urls:
            continue
        fingerprint = str(candidate.get("fingerprint") or "")
        if fingerprint in resolved_dispositions:
            must_show_resolved.append(
                {
                    "title": str(candidate.get("title") or "")[:80],
                    "section": LEAD_SECTION if candidate.get("is_lead") else PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""), ""),
                    "disposition": resolved_dispositions[fingerprint],
                }
            )
            continue
        if candidate.get("is_lead"):
            html_text, added = _insert_or_create_lead(html_text, _candidate_lead_line(candidate))
            if added:
                draft_path.write_text(html_text, encoding="utf-8")
                html_counts = _html_section_counts(html_text)
                if ident:
                    rendered_urls.add(ident)
                lead_guard_recovered += added
                continue
        must_show_missing.append(
            {
                "title": str(candidate.get("title") or "")[:80],
                "section": LEAD_SECTION if candidate.get("is_lead") else PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""), ""),
                "reason": "selected_must_show_not_visible_after_writer_editor",
            }
        )

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
        "policy": "report_only_for_sections_with_temporary_curator_lead_guard_pending_0114",
        "recovered": [],
        "inserted_total": lead_guard_recovered,
        "still_under_minimum": still_short,
        "must_show_recovered": lead_guard_recovered,
        "lead_guard_recovered": lead_guard_recovered,
        "must_show_resolved_before_contract": must_show_resolved,
        "must_show_missing": must_show_missing,
    }
