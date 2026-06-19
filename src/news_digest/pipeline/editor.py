from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
import html
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlparse

from news_digest.pipeline.common import (
    LOW_SIGNAL_BLOCKS,
    PRIMARY_BLOCKS,
    REQUIRED_BLOCKS,
    canonical_url_identity,
    extract_sections,
    is_placeholder_practical_angle,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.glossary_qa import glossary_line_issues, repair_glossary_terms


MIN_CITY_PRACTICAL_ANGLE_LENGTH = 40
MAX_WEAK_CITY_CANDIDATE_SHARE = 0.5
PRE_SEND_RUSSIAN_EDITOR_MODEL = "gpt-4o"
PRE_SEND_THIN_EVIDENCE_CHARS = 1200
PRE_SEND_EVIDENCE_MODEL_MAX_CHARS = 18000
PRE_SEND_EDITOR_BATCH_CHAR_BUDGET = 90000
PRE_SEND_EDITOR_MAX_WORKERS = 3

PRE_SEND_RUSSIAN_EDITOR_PROMPT = """Ты выпускающий редактор русского Telegram-дайджеста Greater Manchester.
Тебе дают уже видимые строки выпуска и, если удалось сопоставить строку с кандидатом, evidence по исходной новости.
Исправь русский язык и редакторские дефекты. Если строка непонятная, битая или слишком машинная, пересобери её заново из evidence.

Верни JSON-объект: {"items":[{"index":0,"status":"ok|fixed","line":"...","reason":"..."}]}.

Правила:
- Сохраняй bullet "• " и HTML-теги/ссылки, если они есть.
- Не меняй даты, числа, имена, районы, площадки, источники и ссылки.
- Не добавляй факты вне поля evidence. Если evidence не хватает, убери битую фразу, но не выдумывай возраст/место/причину.
- Исправляй кальку и плохой русский: "защита от дождя", "возрастелет", "в возрасте лет", "перевернулся на крыше", "спасён с высоты", "инцидент был успешно разрешен".
- Погода должна звучать по-человечески: "возьмите зонт", "дождевик", "планируйте пересадки с запасом"; не пиши "защита от дождя".
- Новости должны отвечать: что произошло → кого касается/почему важно сегодня → что читателю делать или понимать.
- Транспорт должен отвечать: что сломано/изменено → какой участок/маршрут → что делать пассажиру.
- Событие/билет должен отвечать: кто/что → когда → где → жанр/тип, если есть в evidence → почему это заметно.
- Футбол и билеты не должны звучать как машинная оценка: убирай "это важная информация" и "интересная информация", заменяй на конкретный смысл из строки.
- Не переводить всё подряд: имена, площадки, компании, AI/API/SaaS/open banking/open space и другие glossary keep-термины оставлять как есть.
- Glossary-нарушения чинить точечно: disruptions/anniversary/inquest/open conclusion переводить, CQC/PBSA/AGM/MDC объяснять внутри строки.
- Если строка нормальная, верни её без изменений со status="ok".
"""

_BAD_RUSSIAN_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"держите\s+защиту\s+от\s+дождя\s+под\s+рукой", re.IGNORECASE), "возьмите зонт"),
    (re.compile(r"защиту\s+от\s+дождя", re.IGNORECASE), "зонт или дождевик"),
    (re.compile(r"\bв\s+возрасте\s*лет\b", re.IGNORECASE), ""),
    (re.compile(r"\bвозрасте\s*лет\b", re.IGNORECASE), ""),
    (re.compile(r"\bвозрастелет\b", re.IGNORECASE), ""),
    (re.compile(r"автомобиль\s+перевернулся\s+на\s+крыше", re.IGNORECASE), "автомобиль перевернулся и оказался на крыше"),
    (re.compile(r"машина\s+перевернулась\s+на\s+крыше", re.IGNORECASE), "машина перевернулась и оказалась на крыше"),
    (re.compile(r"\bспас[её]н\s+с\s+высоты\b", re.IGNORECASE), "снят с высоты спасателями"),
    (re.compile(r"\bспасли\s+с\s+высоты\b", re.IGNORECASE), "сняли с высоты"),
    (re.compile(r"\bинцидент\s+был\s+успешно\s+разреш[её]н\.?", re.IGNORECASE), ""),
    (re.compile(r"\bэто\s+важная\s+информация\s+для\s+", re.IGNORECASE), "это важно для "),
    (re.compile(r"\bэто\s+интересная\s+информация\s+для\s+", re.IGNORECASE), "это может быть полезно для "),
)

_BAD_RUSSIAN_DETECTORS: tuple[re.Pattern[str], ...] = (
    re.compile(r"защит[ау]\s+от\s+дождя", re.IGNORECASE),
    re.compile(r"\b(?:в\s+)?возрасте\s*лет\b", re.IGNORECASE),
    re.compile(r"\bвозрастелет\b", re.IGNORECASE),
    re.compile(r"перевернул[асься]+\s+на\s+крыше", re.IGNORECASE),
    re.compile(r"спас[её]н\s+с\s+высоты", re.IGNORECASE),
    re.compile(r"инцидент\s+был\s+успешно\s+разреш[её]н", re.IGNORECASE),
    re.compile(r"это\s+(?:важная|интересная)\s+информация", re.IGNORECASE),
    # D11 glossary leftovers: English / bad transliteration that must be
    # translated (mirrors the glossary translate-list).
    re.compile(r"\bdisruptions?\b", re.IGNORECASE),
    re.compile(r"\bанниверсар", re.IGNORECASE),
    re.compile(r"\bсубмашин", re.IGNORECASE),
    re.compile(r"\bурбанист", re.IGNORECASE),
    re.compile(r"\bинквест", re.IGNORECASE),
)

# Crime/court/sensitive lines always go through the targeted model pass even
# when no language detector fires — a faithfulness slip there is worst.
_SENSITIVE_LINE_RE = re.compile(
    r"\b(?:полиц|суд|обвин|пригов|осужд|убийств|нож|ножев|погиб|умер|жертв|"
    r"нападени|пострадал|изнасил|коронер|расследован|эвакуир|пожар)\w*",
    re.IGNORECASE,
)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path
    draft_path: Path


def _unique_preserving_order(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for line in lines:
        key = line.strip()
        if key in seen:
            continue
        seen.add(key)
        result.append(line)
    return result


def _is_weak_city_candidate(candidate: dict) -> bool:
    practical_angle = str(candidate.get("practical_angle") or "").strip()
    if is_placeholder_practical_angle(practical_angle):
        return True
    return len(practical_angle) < MIN_CITY_PRACTICAL_ANGLE_LENGTH


def _has_included_candidates_for_section(candidates: list[dict], section_name: str) -> bool:
    return any(
        PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or "")) == section_name
        for candidate in candidates
        if isinstance(candidate, dict) and candidate.get("include")
    )


_RU_MONTHS_GENITIVE = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}
_RELATIVE_DAY_RE = re.compile(
    r"\b(Сегодня|Завтра|Послезавтра)\b\s*,?\s*(\d{1,2})\s+([А-Яа-яё]+)",
    re.IGNORECASE,
)


def _fix_relative_day_label(line: str) -> tuple[str, list[str]]:
    """Recompute «Сегодня/Завтра» against the explicit date in the same line
    and the run date. If the relative word is >2 days off or in the past, drop
    it and keep the reliable explicit date (owner 2026-06-16: «Завтра, 18 июня»
    в выпуске от 16-го)."""
    text = str(line or "")
    match = _RELATIVE_DAY_RE.search(text)
    if not match:
        return text, []
    month = _RU_MONTHS_GENITIVE.get(match.group(3).lower())
    if not month:
        return text, []
    today = now_london().date()
    try:
        event_day = date(today.year, month, int(match.group(2)))
    except ValueError:
        return text, []
    if (event_day - today).days < -182:  # explicit date already rolled into next year
        try:
            event_day = date(today.year + 1, month, int(match.group(2)))
        except ValueError:
            return text, []
    delta = (event_day - today).days
    correct = {0: "Сегодня", 1: "Завтра", 2: "Послезавтра"}.get(delta)
    if correct == match.group(1).capitalize():
        return text, []
    if correct:
        fixed = _RELATIVE_DAY_RE.sub(lambda m: f"{correct}, {m.group(2)} {m.group(3)}", text, count=1)
    else:
        fixed = _RELATIVE_DAY_RE.sub(lambda m: f"{m.group(2)} {m.group(3)}", text, count=1)
    return fixed, ["relative_day_label_fixed"]


def _polish_russian_line_rules(line: str) -> tuple[str, list[str]]:
    fixed = str(line or "")
    reasons: list[str] = []
    fixed, glossary_reasons = repair_glossary_terms(fixed)
    reasons.extend(glossary_reasons)
    for pattern, replacement in _BAD_RUSSIAN_PATTERNS:
        if pattern.search(fixed):
            fixed = pattern.sub(replacement, fixed)
            reasons.append(pattern.pattern)
    fixed, day_reasons = _fix_relative_day_label(fixed)
    reasons.extend(day_reasons)
    fixed = re.sub(r"\s{2,}", " ", fixed)
    fixed = re.sub(r"\s+([,.;:])", r"\1", fixed)
    fixed = re.sub(r"•\s*,\s*", "• ", fixed)
    fixed = re.sub(r"\.\s*\.", ".", fixed)
    fixed = re.sub(
        r"^(•\s+)([а-яё])",
        lambda match: match.group(1) + match.group(2).upper(),
        fixed,
    )
    fixed = fixed.strip()
    return fixed, reasons


def _line_needs_russian_editor(line: str) -> bool:
    text = str(line or "")
    if glossary_line_issues(text):
        return True
    return any(pattern.search(text) for pattern in _BAD_RUSSIAN_DETECTORS)


def _line_preserves_links(original: str, fixed: str) -> bool:
    original_links = re.findall(r"<a\s+[^>]*href=", str(original or ""), flags=re.IGNORECASE)
    fixed_links = re.findall(r"<a\s+[^>]*href=", str(fixed or ""), flags=re.IGNORECASE)
    return len(original_links) == len(fixed_links)


def _line_url_identity(line: str) -> str:
    match = re.search(r'<a\s+[^>]*href="([^"]+)"', str(line or ""), flags=re.IGNORECASE)
    if not match:
        return ""
    return canonical_url_identity(html.unescape(match.group(1)))


def _candidate_index(candidates: list[dict]) -> dict[str, dict]:
    index: dict[str, dict] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        url_key = canonical_url_identity(str(candidate.get("source_url") or ""))
        if url_key:
            index.setdefault(url_key, candidate)
        fp = str(candidate.get("fingerprint") or "").strip()
        if fp:
            index.setdefault(fp, candidate)
    return index


def _clip_text(value: object, limit: int = 900) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _plain_article_text_from_html(html_text: str, title: str = "") -> str:
    from news_digest.pipeline.collector.extract import _clean_long_text, _extract_jsonld_nodes, _strip_evidence_chrome  # noqa: PLC0415

    jsonld_parts: list[str] = []
    for node in _extract_jsonld_nodes(html_text):
        body = _clean_long_text(str(node.get("articleBody") or ""))
        if len(body) >= 200:
            jsonld_parts.append(body)
    article_match = re.search(
        r"<(?:article|main)[^>]*>(.*?)</(?:article|main)>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    candidate_html = article_match.group(1) if article_match else html_text
    title_key = re.sub(r"[^a-z0-9а-яё]+", " ", str(title or "").lower()).strip()
    paragraphs: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"<p[^>]*>(.*?)</p>", candidate_html, flags=re.IGNORECASE | re.DOTALL):
        text = _clean_long_text(match.group(1))
        if len(text) < 35:
            continue
        key = re.sub(r"[^a-z0-9а-яё]+", " ", text.lower()).strip()
        if not key or key in seen or (title_key and key == title_key):
            continue
        seen.add(key)
        paragraphs.append(text)
    return _strip_evidence_chrome(" ".join(jsonld_parts + paragraphs))


def _refetch_candidate_evidence(candidate: dict) -> tuple[str, dict[str, object]]:
    url = str(candidate.get("source_url") or "").strip()
    if not url:
        return "", {"status": "skipped_no_url"}
    try:
        from news_digest.pipeline.collector.fetch import _fetch_text  # noqa: PLC0415

        html_text = _fetch_text(url)
        evidence = _plain_article_text_from_html(html_text, str(candidate.get("title") or ""))
    except Exception as exc:  # noqa: BLE001
        return "", {"status": "failed", "url": url, "error": f"{exc.__class__.__name__}: {exc}"}
    return evidence, {"status": "ok" if evidence else "empty", "url": url, "chars": len(evidence)}


def _editor_refetch_skip_reason(candidate: dict) -> str:
    url = str(candidate.get("source_url") or "").strip()
    category = str(candidate.get("category") or "")
    host = urlparse(url).netloc.lower()
    if category in {"venues_tickets", "ticket_radar"}:
        return "structured_ticket_candidate"
    if "ticketmaster." in host:
        return "ticketmaster_structured_cache"
    if "tfgm.com" in host and "/travel-updates/" in url.lower():
        return "tfgm_ephemeral_alert"
    return ""


def _candidate_full_evidence_text(candidate: dict, refetch_stats: dict[str, object]) -> tuple[str, str]:
    packet = candidate.get("evidence_packet") if isinstance(candidate.get("evidence_packet"), dict) else {}
    parts = [
        candidate.get("evidence_text"),
        packet.get("evidence_text"),
        candidate.get("lead"),
        packet.get("lead"),
        candidate.get("summary"),
        packet.get("summary"),
    ]
    evidence = re.sub(r"\s+", " ", " ".join(str(part or "") for part in parts if part)).strip()
    source = "candidate"
    if len(evidence) < PRE_SEND_THIN_EVIDENCE_CHARS:
        skip_reason = _editor_refetch_skip_reason(candidate)
        if skip_reason:
            refetch_stats["skipped"] = int(refetch_stats.get("skipped") or 0) + 1
            reports = refetch_stats.setdefault("reports", [])
            if isinstance(reports, list) and len(reports) < 30:
                reports.append({
                    "fingerprint": str(candidate.get("fingerprint") or ""),
                    "title": str(candidate.get("title") or "")[:160],
                    "status": "skipped",
                    "reason": skip_reason,
                    "url": str(candidate.get("source_url") or ""),
                    "candidate_evidence_chars": len(evidence),
                })
            return evidence, source
        refetch_stats["attempted"] = int(refetch_stats.get("attempted") or 0) + 1
        refetched, report = _refetch_candidate_evidence(candidate)
        reports = refetch_stats.setdefault("reports", [])
        if isinstance(reports, list) and len(reports) < 30:
            reports.append({
                "fingerprint": str(candidate.get("fingerprint") or ""),
                "title": str(candidate.get("title") or "")[:160],
                **report,
            })
        if refetched and len(refetched) > len(evidence):
            evidence = refetched
            source = "refetched_article"
            refetch_stats["improved"] = int(refetch_stats.get("improved") or 0) + 1
        elif report.get("status") == "failed":
            refetch_stats["failed"] = int(refetch_stats.get("failed") or 0) + 1
        else:
            refetch_stats["empty_or_not_better"] = int(refetch_stats.get("empty_or_not_better") or 0) + 1
    return evidence, source


def _compact_candidate_evidence(candidate: dict | None, refetch_stats: dict[str, object] | None = None) -> dict[str, object]:
    if not isinstance(candidate, dict):
        return {}
    refetch_stats = refetch_stats if refetch_stats is not None else {}
    packet = candidate.get("evidence_packet") if isinstance(candidate.get("evidence_packet"), dict) else {}
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else packet.get("event") if isinstance(packet.get("event"), dict) else {}
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else packet.get("entities") if isinstance(packet.get("entities"), dict) else {}
    story_frame = candidate.get("story_frame") if isinstance(candidate.get("story_frame"), dict) else {}
    evidence_text, evidence_source = _candidate_full_evidence_text(candidate, refetch_stats)
    evidence_full_chars = len(evidence_text)
    evidence_for_model = _clip_text(evidence_text, PRE_SEND_EVIDENCE_MODEL_MAX_CHARS)
    return {
        "fingerprint": str(candidate.get("fingerprint") or packet.get("fingerprint") or ""),
        "category": str(candidate.get("category") or packet.get("category") or ""),
        "primary_block": str(candidate.get("primary_block") or packet.get("primary_block") or ""),
        "source_label": str(candidate.get("source_label") or packet.get("source_label") or ""),
        "source_url": str(candidate.get("source_url") or packet.get("source_url") or ""),
        "title": _clip_text(candidate.get("title") or packet.get("title"), 260),
        "lead": _clip_text(candidate.get("lead") or packet.get("lead"), 500),
        "summary": _clip_text(candidate.get("summary") or packet.get("summary"), 700),
        "practical_angle": _clip_text(candidate.get("practical_angle"), 360),
        "evidence_text": evidence_for_model,
        "evidence_source": evidence_source,
        "evidence_full_chars": evidence_full_chars,
        "evidence_sent_chars": len(evidence_for_model),
        "evidence_truncated_for_model": evidence_full_chars > len(evidence_for_model),
        "event": event,
        "entities": {
            key: value for key, value in entities.items()
            if key in {"boroughs", "districts", "stations", "venues", "clubs", "companies", "people"}
        },
        "story_frame": {
            key: value for key, value in story_frame.items()
            if key in {"news_anchor", "reader_need", "missing_facts", "what_changed", "case_frame"}
        },
    }


def _visible_line_items(
    sections: dict[str, list[str]],
    candidates_by_key: dict[str, dict] | None = None,
    refetch_stats: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    index = 0
    candidates_by_key = candidates_by_key or {}
    for section_name, lines in sections.items():
        for line in lines:
            if not line.strip() or line.strip() == "•":
                continue
            candidate = candidates_by_key.get(_line_url_identity(line))
            evidence = _compact_candidate_evidence(candidate, refetch_stats)
            item: dict[str, object] = {"index": index, "section": section_name, "line": line}
            glossary_issues = glossary_line_issues(line)
            if glossary_issues:
                item["glossary_issues"] = glossary_issues
            if evidence:
                item["evidence"] = evidence
            items.append(item)
            index += 1
    return items


def _batch_editor_items(items: list[dict[str, object]]) -> list[list[dict[str, object]]]:
    batches: list[list[dict[str, object]]] = []
    current: list[dict[str, object]] = []
    current_chars = 0
    for item in items:
        item_chars = len(json.dumps(item, ensure_ascii=False))
        if current and current_chars + item_chars > PRE_SEND_EDITOR_BATCH_CHAR_BUDGET:
            batches.append(current)
            current = []
            current_chars = 0
        current.append(item)
        current_chars += item_chars
    if current:
        batches.append(current)
    return batches


def _call_pre_send_russian_editor_batch(
    client: object,
    items: list[dict[str, object]],
    record_call_from_response: object,
) -> tuple[dict[int, str], dict[str, object]]:
    messages = [
        {"role": "system", "content": PRE_SEND_RUSSIAN_EDITOR_PROMPT},
        {"role": "user", "content": json.dumps({"items": items}, ensure_ascii=False)},
    ]
    max_tokens = min(12000, 300 * len(items) + 1200)
    try:
        response = client.chat.completions.create(
            model=PRE_SEND_RUSSIAN_EDITOR_MODEL,
            messages=messages,
            temperature=0.1,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )
    except Exception as exc:  # noqa: BLE001
        return {}, {"status": "failed", "error": f"{exc.__class__.__name__}: {exc}", "items_sent": len(items)}
    record_call_from_response(
        response=response,
        stage="editor",
        provider="OpenAI",
        model=PRE_SEND_RUSSIAN_EDITOR_MODEL,
        prompt_name="pre_send_russian_editor",
        messages=messages,
        max_tokens=max_tokens,
    )
    raw = str(response.choices[0].message.content or "").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return {}, {"status": "parse_failed", "error": f"{exc.__class__.__name__}: {exc}", "raw_excerpt": raw[:400], "items_sent": len(items)}
    rows = parsed.get("items") if isinstance(parsed, dict) else parsed
    if not isinstance(rows, list):
        return {}, {"status": "parse_failed", "error": "JSON root has no items list", "raw_excerpt": raw[:400], "items_sent": len(items)}
    fixes: dict[int, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            index = int(row.get("index"))
        except (TypeError, ValueError):
            continue
        line = str(row.get("line") or "").strip()
        if line.startswith("• "):
            fixes[index] = line
    return fixes, {"status": "ok", "items_sent": len(items), "items_returned": len(fixes)}


def _call_pre_send_russian_editor(items: list[dict[str, object]], api_key: str) -> tuple[dict[int, str], dict[str, object]]:
    if not items or not api_key:
        return {}, {"status": "skipped_missing_api_key" if not api_key else "skipped_no_items"}
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        return {}, {"status": "skipped_missing_openai_package"}
    from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415

    client = OpenAI(api_key=api_key, timeout=60, max_retries=1)
    fixes: dict[int, str] = {}
    batch_reports: list[dict[str, object]] = []
    batches = _batch_editor_items(items)
    max_workers = max(1, int(os.environ.get("PRE_SEND_EDITOR_MAX_WORKERS", PRE_SEND_EDITOR_MAX_WORKERS)))
    max_workers = min(len(batches), max_workers)
    started = time.monotonic()
    if max_workers <= 1:
        for batch in batches:
            batch_fixes, batch_report = _call_pre_send_russian_editor_batch(client, batch, record_call_from_response)
            fixes.update(batch_fixes)
            batch_reports.append(batch_report)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_call_pre_send_russian_editor_batch, client, batch, record_call_from_response)
                for batch in batches
            ]
            for future in futures:
                batch_fixes, batch_report = future.result()
                fixes.update(batch_fixes)
                batch_reports.append(batch_report)
    failed = [report for report in batch_reports if report.get("status") != "ok"]
    return fixes, {
        "status": "partial_failed" if failed and fixes else "failed" if failed else "ok",
        "items_sent": len(items),
        "items_returned": len(fixes),
        "batch_count": len(batch_reports),
        "max_workers": max_workers,
        "duration_seconds": round(time.monotonic() - started, 3),
        "failed_batches": len(failed),
        "batches": batch_reports[:20],
    }


def _same_section_reserve_line(section_name: str, candidates: list[dict], rendered_urls: set[str]) -> str:
    """A clean, ready public-reserve line for this section, used to replace an
    unrepairable row. Reserve = public_reserve and not backup_pool_only, with a
    draft_line that passes the language check and isn't already on the board."""
    for c in candidates:
        if not isinstance(c, dict):
            continue
        if PRIMARY_BLOCKS.get(str(c.get("primary_block") or "")) != section_name:
            continue
        if not (c.get("public_reserve") and not c.get("backup_pool_only")):
            continue
        line = str(c.get("draft_line") or "").strip()
        if not line:
            continue
        if not line.startswith("• "):
            line = f"• {line}"
        if _line_needs_russian_editor(line):
            continue
        url = str(c.get("source_url") or "")
        ident = canonical_url_identity(url) if url else ""
        if ident and ident in rendered_urls:
            continue
        if "<a " not in line.lower() and url:
            label = str(c.get("source_label") or "источник")
            line = f'{line} <a href="{url}">{label}</a>'
        if ident:
            rendered_urls.add(ident)
        return line
    return ""


def _pre_send_polish_sections(
    sections: dict[str, list[str]],
    warnings: list[str],
    candidates: list[dict] | None = None,
) -> tuple[dict[str, list[str]], dict[str, object]]:
    rule_fixed = 0
    model_fixed = 0
    remaining_bad = 0
    polished: dict[str, list[str]] = {}
    candidates_by_key = _candidate_index(candidates or [])
    for section_name, lines in sections.items():
        new_lines: list[str] = []
        for line in lines:
            fixed, reasons = _polish_russian_line_rules(line)
            if reasons and fixed != line:
                rule_fixed += 1
            new_lines.append(fixed)
        polished[section_name] = new_lines

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    refetch_stats: dict[str, object] = {"attempted": 0, "improved": 0, "failed": 0, "empty_or_not_better": 0, "skipped": 0}
    items = (
        _visible_line_items(polished, candidates_by_key, refetch_stats)
        if api_key
        else _visible_line_items(polished)
    )
    evidence_items = sum(1 for item in items if isinstance(item.get("evidence"), dict) and item.get("evidence"))
    # D12: only the suspicious lines (language detector OR crime/court/sensitive)
    # go to the gpt-4o pass — clean lines keep the rule polish. The model reads
    # the few rows that matter instead of skimming the whole issue.
    suspicious_items = [
        item
        for item in items
        if _line_needs_russian_editor(str(item.get("line") or ""))
        or _SENSITIVE_LINE_RE.search(str(item.get("line") or ""))
    ]
    model_fixes, model_report = _call_pre_send_russian_editor(suspicious_items, api_key)
    model_report["targeted_items"] = len(suspicious_items)
    if model_report.get("status") not in {"ok", "skipped_no_items"}:
        warnings.append(f"Pre-send Russian editor skipped/failed: {model_report.get('status')} {model_report.get('error') or ''}".strip())
    if model_fixes:
        by_index = {int(item["index"]): item for item in items}
        for index, fixed_line in model_fixes.items():
            item = by_index.get(index)
            if not item:
                continue
            original = str(item.get("line") or "")
            if not _line_preserves_links(original, fixed_line):
                continue
            if fixed_line != original:
                item["line"] = fixed_line
                model_fixed += 1
        rebuilt: dict[str, list[str]] = {}
        for item in items:
            rebuilt.setdefault(str(item.get("section") or ""), []).append(str(item.get("line") or ""))
        # Preserve empty sections that had no visible lines.
        for section_name in polished:
            rebuilt.setdefault(section_name, [])
        polished = rebuilt

    # Universal degradation (owner: for everything, always): a row still bad
    # after the repair pass is REPLACED from a clean same-section public
    # reserve, else STRIPPED. The issue always ships; a known-bad row never does.
    replaced = 0
    stripped = 0
    rendered_urls = {
        _line_url_identity(line)
        for lines in polished.values()
        for line in lines
        if line.strip()
    }
    for section_name in list(polished.keys()):
        out: list[str] = []
        for line in polished[section_name]:
            if not line.strip() or not _line_needs_russian_editor(line):
                out.append(line)
                continue
            replacement = _same_section_reserve_line(section_name, candidates or [], rendered_urls)
            if replacement:
                out.append(replacement)
                replaced += 1
            else:
                stripped += 1
                warnings.append(f"Degradation: stripped an unrepairable line in «{section_name}».")
        polished[section_name] = out
    transport_lines = [
        line for line in polished.get("Общественный транспорт сегодня", [])
        if line.strip() and line.strip() != "•"
    ]
    if not transport_lines:
        polished["Общественный транспорт сегодня"] = [
            '• Транспорт: конкретных подтверждённых сбоев в выпуск не попало. '
            'Перед поездкой проверьте страницу статуса TfGM. '
            '<a href="https://tfgm.com/travel-updates">TfGM</a>'
        ]
        warnings.append("Degradation: replaced empty transport block with TfGM status fallback.")

    bad_examples: list[str] = []
    for item in _visible_line_items(polished):
        line = str(item.get("line") or "")
        if _line_needs_russian_editor(line):
            remaining_bad += 1
            if len(bad_examples) < 8:
                bad_examples.append(line[:240])

    return polished, {
        "enabled": True,
        "rules_fixed": rule_fixed,
        "model_fixed": model_fixed,
        "degraded_replaced": replaced,
        "degraded_stripped": stripped,
        "remaining_bad": remaining_bad,
        "bad_examples": bad_examples,
        "model": PRE_SEND_RUSSIAN_EDITOR_MODEL,
        "visible_items": len(items),
        "evidence_items": evidence_items,
        "refetch": refetch_stats,
        "evidence_model_max_chars": PRE_SEND_EVIDENCE_MODEL_MAX_CHARS,
        "thin_evidence_threshold_chars": PRE_SEND_THIN_EVIDENCE_CHARS,
        "model_report": model_report,
    }


def edit_digest(project_root: Path) -> StageResult:
    stage_started = time.monotonic()
    state_dir = project_root / "data" / "state"
    candidates_path = state_dir / "candidates.json"
    draft_path = state_dir / "draft_digest.html"
    report_path = state_dir / "editor_report.json"

    draft_text = draft_path.read_text(encoding="utf-8") if draft_path.exists() else ""
    sections = extract_sections(draft_text)
    payload = read_json(candidates_path, {"candidates": []})
    pipeline_run_id = pipeline_run_id_from(payload)
    included_candidates = [
        candidate
        for candidate in payload.get("candidates", [])
        if isinstance(candidate, dict) and candidate.get("include")
    ]
    # All candidates (incl. public reserves with include=False) so the polish
    # pass can replace an unrepairable row from a same-section reserve.
    all_candidates = [c for c in payload.get("candidates", []) if isinstance(c, dict)]

    errors: list[str] = []
    warnings: list[str] = []

    city_candidates = [
        candidate
        for candidate in included_candidates
        if candidate.get("category") in {"media_layer", "gmp", "public_services", "city_news", "council"}
    ]
    soft_candidates = [
        candidate
        for candidate in included_candidates
        if candidate.get("category") in {"venues_tickets", "ticket_radar", "culture_weekly", "football"}
    ]

    normalized_sections: dict[str, list[str]] = {}
    seen_lines_to_section: dict[str, str] = {}
    duplicate_collisions = 0
    for section_name, lines in sections.items():
        deduped = _unique_preserving_order(lines)
        filtered: list[str] = []
        if len(deduped) < len(lines):
            warnings.append(f"Removed duplicate lines in section {section_name}.")
        for line in deduped:
            key = line.strip()
            previous_section = seen_lines_to_section.get(key)
            if previous_section and previous_section != section_name:
                duplicate_collisions += 1
                warnings.append(
                    f"Removed cross-section duplicate from {section_name}; already present in {previous_section}."
                )
                continue
            seen_lines_to_section[key] = section_name
            filtered.append(line)
        normalized_sections[section_name] = filtered

    if not city_candidates:
        errors.append("No city/public-affairs candidates are included.")
    elif len(soft_candidates) > len(city_candidates) * 2:
        warnings.append(
            "Draft is skewed toward soft items compared with city/public-affairs coverage: "
            f"{len(soft_candidates)} soft vs {len(city_candidates)} city/public-affairs candidate(s)."
        )

    weak_city_candidates = [
        candidate
        for candidate in city_candidates
        if _is_weak_city_candidate(candidate)
    ]
    weak_city_share = len(weak_city_candidates) / len(city_candidates) if city_candidates else 0
    if city_candidates and weak_city_share > MAX_WEAK_CITY_CANDIDATE_SHARE:
        warnings.append(
            "City/public-affairs candidates need editorial rewrite or candidate-level drop: "
            f"({len(weak_city_candidates)}/{len(city_candidates)})."
        )

    normalized_sections, russian_editor_report = _pre_send_polish_sections(normalized_sections, warnings, all_candidates)
    if int(russian_editor_report.get("remaining_bad") or 0) > 0:
        warnings.append(
            "Pre-send Russian editor still sees "
            f"{russian_editor_report.get('remaining_bad')} suspicious line(s); see editor_report.pre_send_russian_editor."
        )

    # "Коротко" больше не требуется — убрана из дайджеста
    required_to_check = [b for b in REQUIRED_BLOCKS if b != "Коротко"]
    for block in required_to_check:
        if block not in normalized_sections:
            if block == "Что важно сегодня":
                warnings.append("Today Focus has no rendered lines after writer/editor pass; omitted «Что важно сегодня» instead of blocking release.")
            else:
                errors.append(f"Required block missing after editor pass: {block}.")

    rendered: list[str] = []
    if draft_text:
        first_line = draft_text.splitlines()[0].strip()
        rendered.append(first_line)
        rendered.append("")
    for section_name, lines in normalized_sections.items():
        real_lines = [line for line in lines if line.strip() and line.strip() != "•"]
        if not real_lines:
            continue
        rendered.append(f"<b>{section_name}</b>")
        rendered.extend(real_lines)
        rendered.append("")
    if rendered:
        draft_path.write_text("\n".join(rendered).strip() + "\n", encoding="utf-8")

    try:
        from news_digest.pipeline.cost_tracker import dump_stage, snapshot, summarise  # noqa: PLC0415
        dump_stage(state_dir, "editor")
        cost_summary = summarise(snapshot(stage="editor"))
    except Exception:  # noqa: BLE001
        cost_summary = {}

    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "failed",
            "errors": errors,
            "warnings": warnings,
            "city_candidate_count": len(city_candidates),
            "soft_candidate_count": len(soft_candidates),
            "weak_city_candidate_count": len(weak_city_candidates),
            "weak_city_candidate_share": round(weak_city_share, 3),
            "pre_send_russian_editor": russian_editor_report,
            "cost_summary": cost_summary,
            "min_city_practical_angle_length": MIN_CITY_PRACTICAL_ANGLE_LENGTH,
            "max_weak_city_candidate_share": MAX_WEAK_CITY_CANDIDATE_SHARE,
            "duplicate_collisions": duplicate_collisions,
            "duration_seconds": round(time.monotonic() - stage_started, 3),
            "draft_path": str(draft_path.resolve()),
        },
    )

    return StageResult(
        not errors,
        "Editor stage completed." if not errors else "Editor stage found blocking issues.",
        report_path,
        draft_path,
    )
