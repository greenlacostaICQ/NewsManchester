from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
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
    is_recoverable_reserve,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
)
from news_digest.pipeline.glossary_qa import glossary_line_issues, repair_glossary_terms
from news_digest.pipeline.transport_language import (
    repair_transport_line_language,
    transport_language_issues,
)


MIN_CITY_PRACTICAL_ANGLE_LENGTH = 40
MAX_WEAK_CITY_CANDIDATE_SHARE = 0.5
PRE_SEND_RUSSIAN_EDITOR_MODEL = "gpt-4o"
PRE_SEND_THIN_EVIDENCE_CHARS = 1200
PRE_SEND_EVIDENCE_MODEL_MAX_CHARS = 18000  # full evidence — kept for sensitive (crime/court) lines
PRE_SEND_EVIDENCE_ROUTINE_MAX_CHARS = 4000  # E2: routine lines (events/tickets/food/business) need far less
# E1: pace the editor by the gpt-4o tokens-per-minute budget instead of running
# sequentially. Batches run concurrently again, but each reserves its estimated
# token cost from a bucket sized to the 30k TPM ceiling (with headroom), so the
# editor is fast AND never breaches the limit. S2 backoff stays as the net.
PRE_SEND_EDITOR_BATCH_CHAR_BUDGET = 60000
PRE_SEND_EDITOR_MAX_WORKERS = 3
PRE_SEND_EDITOR_MAX_TPM = 27000.0
PRE_SEND_EDITOR_MAX_ROUNDS = 2
PRE_SEND_EDITOR_MAX_RETRIES = 4
PRE_SEND_EDITOR_RETRY_CAP_SECONDS = 60.0
_EDITOR_TOKEN_LIMITER = None  # lazily built token bucket (reuses the rewrite-stage pacer)
# E2: keep full evidence whenever the line touches a faithfulness-critical topic.
# _SENSITIVE_LINE_RE (below) covers the Russian rendered line; this covers the
# still-English source evidence/title so a crime/court item is never under-fed.
_EVIDENCE_SENSITIVE_EN = re.compile(
    r"\b(?:murder|kill|stab|knife|shot|shoot|court|charg|convict|sentenc|died|death|dead|"
    r"victim|assault|rape|abus|crash|collision|fire|evacuat|coroner|inquest|missing|arrest)\w*",
    re.IGNORECASE,
)

_EDITOR_TRIMMABLE_SECTIONS = {
    "Билеты / Ticket Radar",
    "Крупные концерты вне GM",
    "Дальние анонсы",
    "Городской радар",
    "Радар по районам",
}

PRE_SEND_RUSSIAN_EDITOR_PROMPT = """Ты выпускающий редактор русского Telegram-дайджеста Greater Manchester.
Тебе дают уже видимые строки выпуска и, если удалось сопоставить строку с кандидатом, evidence по исходной новости.
Исправь русский язык и редакторские дефекты. Если строка непонятная, битая или слишком машинная, пересобери её заново из evidence.

Верни JSON-объект: {"items":[{"index":0,"action":"ok|rewrite|enrich_and_rewrite|replace_needed|strip_only_if_replacement_unavailable","line":"...","reason":"..."}],"block_actions":[{"action":"recover_lead|backfill|trim|move_outside_gm_to_chunk","section":"...","count":1,"reason":"..."}]}.
ОБЯЗАТЕЛЬНО верни ровно один item на КАЖДУЮ присланную строку (по её index). Чистая строка — action="ok".
block_actions верни пустым списком, если блоковых действий не нужно.

Ищи и чини КОНКРЕТНО эти дефекты:
- Английское слово в русском тексте (murder, lineup, line-up, venue, sold out, on sale, headliner, festival) → rewrite, переведи. ИСКЛЮЧЕНИЕ: имена и бренды (Co-op Live, openspace, AI/API/SaaS) — оставить латиницей.
- Латиница, склеенная с русским окончанием (Stockportа, Urmstonе, Rochdaleе) → rewrite, дай корректный русский топоним (Стокпорт, Урмстон, Рочдейл).
- Смысловой дубль: та же история/событие другими словами уже встречалась выше по выпуску → strip_only_if_replacement_unavailable.
- Просроченная дата (дата уже прошла относительно сегодня) → rewrite или replace_needed.
- Шаблон не по теме: "следите за обновлениями полиции или суда" на не-судебной/не-полицейской новости; "если хотите попасть, уточните дату" как наполнитель → rewrite, убери штамп, оставь реальный факт/действие.
- Рассогласование рода/числа/падежа ("бывший медсестра") → rewrite.

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
- Если строка нормальная, верни её без изменений с action="ok".
- Если строку можно исправить по evidence, верни action="rewrite" и новую line.
- Если строку нужно переписать после дотягивания evidence, верни action="enrich_and_rewrite" и новую line, если evidence в payload уже хватает.
- Если строка плохая, но evidence не хватает для честного исправления, верни action="replace_needed".
- Если строка должна исчезнуть и замены нет, верни action="strip_only_if_replacement_unavailable".
- Для тонкого protected-блока верни block_action="backfill"; для потерянного lead — "recover_lead"; для доминирующих optional-блоков — "trim"; для outside-GM, который вытесняет protected, — "move_outside_gm_to_chunk".
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
    # Latin place/word with a glued Russian case ending ("Stockportа",
    # "Urmstonе", "Rochdaleе"). The place-name kept its Latin spelling but a
    # declension ending got appended — strip the ending so it reads as the
    # clean Latin name ("из Stockport") instead of the broken hybrid.
    (re.compile(r"\b([A-Za-z]{3,})[а-яё]+\b"), r"\1"),
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
    # Untranslated English common words that slipped past the rewrite
    # ("murder", "lineup", "venue", "sold out"). Proper names/brands stay
    # Latin and are not in this list, so they are not falsely flagged.
    re.compile(r"\b(?:murder|line-?up|venue|sold\s+out|on\s+sale|headliner)\b", re.IGNORECASE),
    # Latin word glued to a Russian ending ("Stockportа") — belt-and-braces
    # for any hybrid the auto-fix above did not normalise.
    re.compile(r"[A-Za-z]{3,}[а-яё]"),
    # S3: a Cyrillic run glued to Latin ("линияStreet") — the mirror of above.
    re.compile(r"[а-яё]{2,}[A-Za-z]"),
    # S3: a half-translated English title — an English article/preposition
    # immediately followed by a Cyrillic word ("On The линия"). Kept brands stay
    # fully Latin ("The Mill", "The Lowry") so they are not falsely flagged; only
    # the broken mix where one word of the title was translated trips this.
    re.compile(r"\b(?:on|the|in|of|at|for)\s+[а-яё]", re.IGNORECASE),
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
    fixed, transport_reasons = repair_transport_line_language(fixed)
    reasons.extend(transport_reasons)
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
    if transport_language_issues(text):
        return True
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


def _line_story_key(line: str, candidates_by_key: dict[str, dict]) -> str:
    """Cross-section dedup key. Prefer the story cluster so the SAME story
    rendered in two blocks is caught even when the wording differs ("В деле о
    murder Престона Дэви" in Свежие vs "после убийства Престона Дейви" in
    Сегодня). Fall back to the exact line text when no cluster is known, which
    keeps the previous exact-string behaviour and never merges two stories
    that don't share a cluster."""
    url_key = _line_url_identity(line)
    candidate = candidates_by_key.get(url_key) if url_key else None
    story_key = _candidate_story_identity_key(candidate)
    if story_key:
        return story_key
    return line.strip()


def _candidate_story_identity_key(candidate: dict | None) -> str:
    if not isinstance(candidate, dict):
        return ""
    for field in ("story_phase_key", "event_identity_key", "story_identity_key"):
        value = str(candidate.get(field) or "").strip()
        if value:
            return value
    cluster = candidate.get("story_cluster") if isinstance(candidate.get("story_cluster"), dict) else {}
    for field in ("cluster_key", "semantic_key", "story_key"):
        value = str(cluster.get(field) or "").strip()
        if value:
            return f"cluster:{value}"
    cluster_key = candidate.get("story_cluster_key")
    if isinstance(cluster_key, dict):
        value = str(cluster_key.get("cluster_key") or "").strip()
        if value:
            return f"cluster:{value}"
    elif str(cluster_key or "").strip():
        return f"cluster:{str(cluster_key).strip()}"
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    topic = str(contract.get("topic_key") or candidate.get("topic_key") or "").strip()
    if topic:
        return f"topic:{topic}"
    return ""


def _line_story_identity_key(line: str, candidates_by_key: dict[str, dict]) -> str:
    url_key = _line_url_identity(line)
    candidate = candidates_by_key.get(url_key) if url_key else None
    return _candidate_story_identity_key(candidate)


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


def _evidence_is_sensitive(candidate: dict, evidence_text: str = "") -> bool:
    """E2: full editor evidence is reserved for faithfulness-critical lines
    (crime/court/casualty). Checks the Russian rendered line and the English
    title/evidence so neither language slips a sensitive item past the trim."""
    blob = " ".join(str(candidate.get(key) or "") for key in ("draft_line", "lead", "title"))
    if _SENSITIVE_LINE_RE.search(blob):
        return True
    return bool(_EVIDENCE_SENSITIVE_EN.search(blob + " " + (evidence_text or "")[:1500]))


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
    # E2: full evidence only where faithfulness is critical; routine items get a
    # fraction of it (far fewer tokens → faster, cheaper, same quality).
    evidence_cap = (
        PRE_SEND_EVIDENCE_MODEL_MAX_CHARS
        if _evidence_is_sensitive(candidate, evidence_text)
        else PRE_SEND_EVIDENCE_ROUTINE_MAX_CHARS
    )
    evidence_for_model = _clip_text(evidence_text, evidence_cap)
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


def _is_retryable_api_error(exc: Exception) -> bool:
    """A transient error worth a backoff retry (rate limit / 5xx / timeout),
    as opposed to a genuine bad request we should not retry."""
    status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
    if status in {429, 500, 502, 503, 504}:
        return True
    name = exc.__class__.__name__.lower()
    if any(k in name for k in ("ratelimit", "timeout", "apiconnection", "internalserver", "serviceunavailable", "apistatus")):
        return True
    msg = str(exc).lower()
    return any(k in msg for k in ("rate limit", "429", "overloaded", "temporarily unavailable", "timed out"))


def _editor_retry_seconds(exc: Exception, attempt: int) -> float:
    match = re.search(r"try again in ([\d.]+)\s*s", str(exc), flags=re.IGNORECASE)
    if match:
        return min(float(match.group(1)) + 0.5, PRE_SEND_EDITOR_RETRY_CAP_SECONDS)
    return min(2.0 * (2 ** attempt), PRE_SEND_EDITOR_RETRY_CAP_SECONDS)


def _editor_token_limiter():
    """E1: the per-minute token pacer. Reuses the rewrite stage's proven token
    bucket (lazy import keeps it cycle-safe) so the editor runs concurrently yet
    never exceeds the gpt-4o TPM ceiling — fast without 429."""
    global _EDITOR_TOKEN_LIMITER
    if _EDITOR_TOKEN_LIMITER is None:
        from news_digest.pipeline.llm_rewrite import _TokenRateLimiter  # noqa: PLC0415
        _EDITOR_TOKEN_LIMITER = _TokenRateLimiter(PRE_SEND_EDITOR_MAX_TPM)
    return _EDITOR_TOKEN_LIMITER


def _editor_create_with_backoff(client: object, **kwargs: object) -> object:
    """gpt-4o tier-1 caps at 30k TPM; a saturated minute returns 429 with an
    explicit wait. Honour it (capped) and retry instead of dropping the batch —
    an unreviewed line must never be recorded as a clean line (RC5)."""
    last_exc: Exception | None = None
    for attempt in range(PRE_SEND_EDITOR_MAX_RETRIES + 1):
        try:
            return client.chat.completions.create(**kwargs)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= PRE_SEND_EDITOR_MAX_RETRIES or not _is_retryable_api_error(exc):
                raise
            time.sleep(_editor_retry_seconds(exc, attempt))
    assert last_exc is not None
    raise last_exc


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
    # E1: reserve this batch's estimated token cost before firing so concurrent
    # batches stay under the per-minute ceiling (chars/4 input + reserved output).
    est_tokens = sum(len(str(m.get("content") or "")) for m in messages) // 4 + max_tokens
    _editor_token_limiter().acquire(est_tokens)
    try:
        response = _editor_create_with_backoff(
            client,
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
    actions: list[dict[str, object]] = []
    block_actions = parsed.get("block_actions") if isinstance(parsed, dict) else []
    if not isinstance(block_actions, list):
        block_actions = []
    expected_indices = {int(item.get("index")) for item in items if isinstance(item.get("index"), int)}
    returned_indices: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            index = int(row.get("index"))
        except (TypeError, ValueError):
            continue
        line = str(row.get("line") or "").strip()
        action = str(row.get("action") or row.get("status") or "").strip() or "rewrite"
        reason = str(row.get("reason") or "").strip()
        actions.append({"index": index, "action": action, "line": line, "reason": reason})
        returned_indices.add(index)
        if line.startswith("• "):
            fixes[index] = line
    missing_indices = sorted(expected_indices - returned_indices)
    return fixes, {
        "status": "ok",
        "items_sent": len(items),
        "items_returned": len(fixes),
        "actions_returned": len(actions),  # one action per visible line is the contract
        "coverage_complete": not missing_indices and len(returned_indices) >= len(expected_indices),
        "missing_action_indices": missing_indices[:80],
        "actions": actions[:120],
        "block_actions": block_actions[:40],
    }


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
    actions: list[dict[str, object]] = []
    block_actions: list[dict[str, object]] = []
    batches = _batch_editor_items(items)
    max_workers = max(1, int(os.environ.get("PRE_SEND_EDITOR_MAX_WORKERS", PRE_SEND_EDITOR_MAX_WORKERS)))
    max_workers = min(len(batches), max_workers)
    started = time.monotonic()
    if max_workers <= 1:
        for batch in batches:
            batch_fixes, batch_report = _call_pre_send_russian_editor_batch(client, batch, record_call_from_response)
            fixes.update(batch_fixes)
            actions.extend(batch_report.get("actions") or [])
            block_actions.extend(batch_report.get("block_actions") or [])
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
                actions.extend(batch_report.get("actions") or [])
                block_actions.extend(batch_report.get("block_actions") or [])
                batch_reports.append(batch_report)
    failed = [report for report in batch_reports if report.get("status") != "ok"]
    missing_indices: list[int] = []
    for report in batch_reports:
        missing_indices.extend(int(i) for i in (report.get("missing_action_indices") or []) if str(i).isdigit())
    return fixes, {
        "status": "partial_failed" if failed and fixes else "failed" if failed else "ok",
        "items_sent": len(items),
        "items_returned": len(fixes),
        "actions_returned": len(actions),
        "coverage_complete": not missing_indices and len(actions) >= len(items),
        "missing_action_indices": sorted(set(missing_indices))[:120],
        "batch_count": len(batch_reports),
        "max_workers": max_workers,
        "duration_seconds": round(time.monotonic() - started, 3),
        "failed_batches": len(failed),
        "actions": actions[:200],
        "block_actions": block_actions[:80],
        "batches": batch_reports[:20],
    }


# P0-D: recovery may only insert an event into a date-anchored section if the
# event has a concrete occurrence date inside that section's horizon. A no-date
# listing (Black Friar / Boro) or a far-future one (Manchester Psych Festival,
# 5 Sept, in «Выходные») was being re-manufactured into a thin weekend block
# even though validation had dropped it — recovery filled the counter with a
# line that is not actually this weekend. News/city blocks carry no such date
# contract and are unaffected.
_RESERVE_INSERT_EVENT_HORIZON_DAYS: dict[str, int | None] = {
    "Выходные в GM": 3,
    "Что важно в ближайшие 7 дней": 7,
    "Дальние анонсы": None,  # future, any horizon
    "Билеты / Ticket Radar": None,
    "Крупные концерты вне GM": None,
    "Русскоязычные концерты и стендап UK": None,
}


def _reserve_insert_allowed(section_name: str, candidate: dict) -> bool:
    """Whether recovery may insert ``candidate`` into ``section_name`` (P0-D).

    Date-anchored sections require a concrete occurrence date in window; a
    no-date or out-of-window event is not a real entry for that section and
    must not be manufactured to hit a minimum.
    """
    if section_name not in _RESERVE_INSERT_EVENT_HORIZON_DAYS:
        return True
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    raw = str(event.get("date_start") or event.get("date") or "").strip()[:10]
    if not raw:
        return False
    try:
        start = date.fromisoformat(raw)
    except ValueError:
        return False
    try:
        from news_digest.pipeline.event_extraction import event_end_date  # noqa: PLC0415

        end = event_end_date(candidate) or start
    except Exception:  # noqa: BLE001
        end = start
    today = now_london().date()
    if end < today:  # already over
        return False
    horizon = _RESERVE_INSERT_EVENT_HORIZON_DAYS.get(section_name)
    if horizon is not None and start > today + timedelta(days=horizon):
        return False
    return True


def _same_section_reserve_line(
    section_name: str,
    candidates: list[dict],
    rendered_urls: set[str],
    rendered_story_keys: set[str] | None = None,
    replacement_stats: dict[str, object] | None = None,
) -> str:
    """A clean public-reserve line for this section, used to replace an
    unrepairable row.

    First preference is an already clean public reserve draft. If the reserve
    has no line or a weak line, refetch/enrich its evidence and rebuild the
    public line through the writer fallback instead of silently skipping it.
    """
    replacement_stats = replacement_stats if replacement_stats is not None else {}
    for c in candidates:
        if not isinstance(c, dict):
            continue
        if PRIMARY_BLOCKS.get(str(c.get("primary_block") or "")) != section_name:
            continue
        if not is_recoverable_reserve(c):  # S1: unified recoverable pool (public_reserve ∪ capacity-cut board overflow)
            continue
        if not _reserve_insert_allowed(section_name, c):  # P0-D: no no-date / out-of-window events
            continue
        line = str(c.get("draft_line") or "").strip()
        if not line.startswith("• "):
            line = f"• {line}" if line else ""
        if _line_needs_russian_editor(line):
            line = ""
        if not line:
            replacement_stats["enriched_rewrite_attempts"] = int(replacement_stats.get("enriched_rewrite_attempts") or 0) + 1
            c_work = dict(c)
            refetch_stats = replacement_stats.setdefault(
                "refetch",
                {"attempted": 0, "improved": 0, "failed": 0, "empty_or_not_better": 0, "skipped": 0},
            )
            if isinstance(refetch_stats, dict):
                evidence_text, evidence_source = _candidate_full_evidence_text(c_work, refetch_stats)
                if evidence_text:
                    c_work["evidence_text"] = evidence_text
                    packet = c_work.get("evidence_packet") if isinstance(c_work.get("evidence_packet"), dict) else {}
                    packet = dict(packet)
                    packet["evidence_text"] = evidence_text
                    packet["evidence_source"] = evidence_source
                    c_work["evidence_packet"] = packet
            try:
                from news_digest.pipeline.writer import _final_replacement_line  # noqa: PLC0415
            except Exception:  # noqa: BLE001
                replacement_stats["enriched_rewrite_import_failed"] = int(replacement_stats.get("enriched_rewrite_import_failed") or 0) + 1
                continue
            line = _final_replacement_line(c_work)
            if line and not line.startswith("• "):
                line = f"• {line}"
            if line:
                line, _ = _polish_russian_line_rules(line)
            if _line_needs_russian_editor(line):
                replacement_stats["enriched_rewrite_rejected"] = int(replacement_stats.get("enriched_rewrite_rejected") or 0) + 1
                continue
            if line:
                replacement_stats["enriched_rewrite_used"] = int(replacement_stats.get("enriched_rewrite_used") or 0) + 1
            else:
                replacement_stats["enriched_rewrite_empty"] = int(replacement_stats.get("enriched_rewrite_empty") or 0) + 1
                continue
        url = str(c.get("source_url") or "")
        ident = canonical_url_identity(url) if url else ""
        if ident and ident in rendered_urls:
            continue
        story_key = _candidate_story_identity_key(c)
        if rendered_story_keys is not None and story_key and story_key in rendered_story_keys:
            continue
        if "<a " not in line.lower() and url:
            label = str(c.get("source_label") or "источник")
            line = f'{line} <a href="{url}">{label}</a>'
        if ident:
            rendered_urls.add(ident)
        if rendered_story_keys is not None and story_key:
            rendered_story_keys.add(story_key)
        return line
    return ""


def _transport_replacement_for_line(
    line: str,
    candidates_by_key: dict[str, dict],
    rendered_urls: set[str],
) -> str:
    candidate = candidates_by_key.get(_line_url_identity(line))
    if not isinstance(candidate, dict):
        return ""
    try:
        from news_digest.pipeline.writer import _build_transport_fallback_line  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return ""
    replacement = _build_transport_fallback_line(candidate)
    if not replacement:
        return ""
    if not replacement.startswith("• "):
        replacement = f"• {replacement}"
    replacement, _ = _polish_russian_line_rules(replacement)
    url = str(candidate.get("source_url") or "")
    ident = canonical_url_identity(url) if url else ""
    if ident and ident in rendered_urls:
        return ""
    if "<a " not in replacement.lower() and url:
        label = str(candidate.get("source_label") or "источник")
        replacement = f'{replacement} <a href="{url}">{label}</a>'
    if _line_needs_russian_editor(replacement):
        return ""
    if ident:
        rendered_urls.add(ident)
    return replacement


def _transport_status_fallback_line() -> str:
    return (
        '• Транспорт: конкретных подтверждённых сбоев в выпуск не попало. '
        'Перед поездкой проверьте страницу статуса TfGM. '
        '<a href="https://tfgm.com/travel-updates">TfGM</a>'
    )


def _apply_editor_line_actions(
    polished: dict[str, list[str]],
    *,
    items: list[dict[str, object]],
    model_fixes: dict[int, str],
    model_report: dict[str, object],
    candidates: list[dict],
    rendered_urls: set[str],
    rendered_story_keys: set[str],
    warnings: list[str],
    round_no: int,
) -> tuple[dict[str, list[str]], dict[str, object]]:
    action_by_index = {
        int(action.get("index")): action
        for action in (model_report.get("actions") or [])
        if isinstance(action, dict) and action.get("index") is not None and str(action.get("index")).strip()
    }
    stats: dict[str, object] = {
        "round": round_no,
        "model_fixed": 0,
        "model_changes": [],
        "model_requested_replaced": 0,
        "model_requested_stripped": 0,
        "reserve_replacement": {"enriched_rewrite_attempts": 0, "enriched_rewrite_used": 0},
    }
    changes = stats["model_changes"]
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
                stats["model_fixed"] = int(stats["model_fixed"] or 0) + 1
                action = action_by_index.get(index, {})
                if isinstance(changes, list):
                    changes.append(
                        {
                            "round": round_no,
                            "index": index,
                            "section": item.get("section"),
                            "before": original,
                            "after": fixed_line,
                            "reason": action.get("reason") if isinstance(action, dict) else "",
                        }
                    )
        rebuilt: dict[str, list[str]] = {}
        for item in items:
            rebuilt.setdefault(str(item.get("section") or ""), []).append(str(item.get("line") or ""))
        for section_name in polished:
            rebuilt.setdefault(section_name, [])
        polished = rebuilt

    by_index = {int(item["index"]): item for item in items}
    for index, action in action_by_index.items():
        action_name = str(action.get("action") or "").strip()
        if action_name not in {"replace_needed", "strip_only_if_replacement_unavailable"}:
            continue
        item = by_index.get(index)
        if not item:
            continue
        section_name = str(item.get("section") or "")
        original = str(item.get("line") or "")
        if section_name not in polished or not original:
            continue
        replacement = _same_section_reserve_line(
            section_name,
            candidates,
            rendered_urls,
            rendered_story_keys,
            stats["reserve_replacement"] if isinstance(stats.get("reserve_replacement"), dict) else None,
        )
        section_lines = polished.get(section_name) or []
        try:
            pos = section_lines.index(original)
        except ValueError:
            continue
        if replacement:
            section_lines[pos] = replacement
            item["line"] = replacement
            stats["model_requested_replaced"] = int(stats["model_requested_replaced"] or 0) + 1
            if isinstance(changes, list):
                changes.append(
                    {
                        "round": round_no,
                        "index": index,
                        "section": section_name,
                        "before": original,
                        "after": replacement,
                        "reason": action.get("reason") or "Model requested same-section replacement.",
                    }
                )
        else:
            if section_name == "Общественный транспорт сегодня":
                warnings.append(
                    "Final editor requested transport removal, but no replacement was available; "
                    "kept the row for deterministic transport recovery."
                )
                continue
            del section_lines[pos]
            stats["model_requested_stripped"] = int(stats["model_requested_stripped"] or 0) + 1
            warnings.append(f"Final editor requested removal in «{section_name}», but no replacement was available.")
    return polished, stats


def _apply_editor_block_actions(
    polished: dict[str, list[str]],
    *,
    block_actions: list[dict[str, object]],
    candidates: list[dict],
    rendered_urls: set[str],
    rendered_story_keys: set[str],
    warnings: list[str],
) -> tuple[dict[str, list[str]], dict[str, object]]:
    report: dict[str, object] = {"requested": len(block_actions), "applied": 0, "actions": []}
    rows = report["actions"]
    if not isinstance(rows, list):
        return polished, report
    for raw in block_actions:
        if not isinstance(raw, dict):
            continue
        action = str(raw.get("action") or "").strip()
        section = str(raw.get("section") or "").strip()
        count_raw = raw.get("count")
        try:
            count = max(1, min(8, int(count_raw or 1)))
        except (TypeError, ValueError):
            count = 1
        entry = {"action": action, "section": section, "count": count, "reason": str(raw.get("reason") or ""), "applied": 0, "status": ""}
        if action == "recover_lead":
            section = section or "Главная история дня"
            for _ in range(count):
                replacement = _same_section_reserve_line(section, candidates, rendered_urls, rendered_story_keys)
                if not replacement and section == "Главная история дня":
                    replacement = _same_section_reserve_line("Свежие новости", candidates, rendered_urls, rendered_story_keys)
                if not replacement:
                    break
                polished.setdefault(section, []).insert(0, replacement)
                entry["applied"] = int(entry["applied"] or 0) + 1
        elif action == "backfill":
            if not section:
                entry["status"] = "skipped_no_section"
            else:
                for _ in range(count):
                    replacement = _same_section_reserve_line(section, candidates, rendered_urls, rendered_story_keys)
                    if not replacement:
                        break
                    polished.setdefault(section, []).append(replacement)
                    entry["applied"] = int(entry["applied"] or 0) + 1
        elif action in {"trim", "move_outside_gm_to_chunk"}:
            if action == "move_outside_gm_to_chunk" and not section:
                section = "Крупные концерты вне GM"
            if section not in _EDITOR_TRIMMABLE_SECTIONS:
                entry["status"] = "skipped_protected_or_unknown_section"
            else:
                lines = polished.get(section) or []
                floor = 1 if section == "Городской радар" else 0
                removable = max(0, len(lines) - floor)
                take = min(count, removable)
                if take:
                    del lines[-take:]
                    entry["applied"] = take
                    entry["status"] = "trimmed_optional"
        else:
            entry["status"] = "unsupported_action"
        if not entry["status"]:
            entry["status"] = "applied" if int(entry["applied"] or 0) else "no_replacement_available"
        if int(entry["applied"] or 0):
            report["applied"] = int(report["applied"] or 0) + int(entry["applied"] or 0)
        elif action in {"recover_lead", "backfill"}:
            warnings.append(f"Final editor requested {action} for «{section}», but no clean reserve was available.")
        rows.append(entry)
    return polished, report


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
    # The final editor must read the whole visible issue, not only rows that a
    # local regex already suspects. Otherwise bad but regex-clean Russian
    # ("Boltonа", literal English калька, generic business/football filler)
    # can pass untouched while the judge only warns after the fact.
    model_fixes, model_report = _call_pre_send_russian_editor(items, api_key)
    model_report["targeted_items"] = len(items)
    model_report["selection_policy"] = "whole_visible_digest"
    replaced = 0
    stripped = 0
    model_requested_replaced = 0
    model_requested_stripped = 0
    rendered_urls = {
        _line_url_identity(line)
        for lines in polished.values()
        for line in lines
        if line.strip()
    }
    rendered_story_keys = {
        _line_story_identity_key(line, candidates_by_key)
        for lines in polished.values()
        for line in lines
        if line.strip()
    }
    rendered_story_keys.discard("")
    round_reports: list[dict[str, object]] = []
    block_action_reports: list[dict[str, object]] = []
    model_changes: list[dict[str, object]] = []
    polished, round_stats = _apply_editor_line_actions(
        polished,
        items=items,
        model_fixes=model_fixes,
        model_report=model_report,
        candidates=candidates or [],
        rendered_urls=rendered_urls,
        rendered_story_keys=rendered_story_keys,
        warnings=warnings,
        round_no=1,
    )
    model_fixed += int(round_stats.get("model_fixed") or 0)
    model_requested_replaced += int(round_stats.get("model_requested_replaced") or 0)
    model_requested_stripped += int(round_stats.get("model_requested_stripped") or 0)
    replaced += int(round_stats.get("model_requested_replaced") or 0)
    stripped += int(round_stats.get("model_requested_stripped") or 0)
    model_changes.extend(round_stats.get("model_changes") or [])
    if not model_report.get("coverage_complete") and model_report.get("status") == "ok":
        warnings.append("Pre-send Russian editor did not return an action for every visible line; running recovery round.")
    polished, block_report = _apply_editor_block_actions(
        polished,
        block_actions=[row for row in (model_report.get("block_actions") or []) if isinstance(row, dict)],
        candidates=candidates or [],
        rendered_urls=rendered_urls,
        rendered_story_keys=rendered_story_keys,
        warnings=warnings,
    )
    block_action_reports.append(block_report)
    round_reports.append({"round": 1, **model_report})

    # Universal degradation (owner: for everything, always): a row still bad
    # after the repair pass is REPLACED from a clean same-section public
    # reserve, else STRIPPED. The issue always ships; a known-bad row never does.
    for section_name in list(polished.keys()):
        out: list[str] = []
        for line in polished[section_name]:
            if not line.strip() or not _line_needs_russian_editor(line):
                out.append(line)
                continue
            replacement = _same_section_reserve_line(section_name, candidates or [], rendered_urls, rendered_story_keys)
            if not replacement and section_name == "Общественный транспорт сегодня":
                replacement = _transport_replacement_for_line(line, candidates_by_key, rendered_urls)
            if replacement:
                out.append(replacement)
                replaced += 1
            else:
                # A concrete transport row we cannot render is STRIPPED, never
                # swapped for the generic "check TfGM" status line: that line
                # may only stand in for an *empty* block (contract: generic
                # fallback is forbidden when concrete disruption exists). The
                # concrete location-bearing recovery already ran above
                # (_transport_replacement_for_line).
                stripped += 1
                warnings.append(f"Degradation: stripped an unrepairable line in «{section_name}».")
        polished[section_name] = out
    transport_lines = [
        line for line in polished.get("Общественный транспорт сегодня", [])
        if line.strip() and line.strip() != "•"
    ]
    if not transport_lines:
        polished["Общественный транспорт сегодня"] = [_transport_status_fallback_line()]
        warnings.append("Degradation: replaced empty transport block with TfGM status fallback.")

    needs_second_round = (
        bool(api_key)
        and PRE_SEND_EDITOR_MAX_ROUNDS >= 2
        and (
            not bool(model_report.get("coverage_complete"))
            or any(_line_needs_russian_editor(line) for lines in polished.values() for line in lines if line.strip())
        )
    )
    if needs_second_round:
        second_refetch_stats: dict[str, object] = {"attempted": 0, "improved": 0, "failed": 0, "empty_or_not_better": 0, "skipped": 0}
        second_items = _visible_line_items(polished, candidates_by_key, second_refetch_stats)
        second_fixes, second_report = _call_pre_send_russian_editor(second_items, api_key)
        second_report["targeted_items"] = len(second_items)
        second_report["selection_policy"] = "whole_visible_digest_second_round"
        second_report["refetch"] = second_refetch_stats
        polished, second_stats = _apply_editor_line_actions(
            polished,
            items=second_items,
            model_fixes=second_fixes,
            model_report=second_report,
            candidates=candidates or [],
            rendered_urls=rendered_urls,
            rendered_story_keys=rendered_story_keys,
            warnings=warnings,
            round_no=2,
        )
        model_fixed += int(second_stats.get("model_fixed") or 0)
        model_requested_replaced += int(second_stats.get("model_requested_replaced") or 0)
        model_requested_stripped += int(second_stats.get("model_requested_stripped") or 0)
        replaced += int(second_stats.get("model_requested_replaced") or 0)
        stripped += int(second_stats.get("model_requested_stripped") or 0)
        model_changes.extend(second_stats.get("model_changes") or [])
        polished, second_block_report = _apply_editor_block_actions(
            polished,
            block_actions=[row for row in (second_report.get("block_actions") or []) if isinstance(row, dict)],
            candidates=candidates or [],
            rendered_urls=rendered_urls,
            rendered_story_keys=rendered_story_keys,
            warnings=warnings,
        )
        block_action_reports.append(second_block_report)
        round_reports.append({"round": 2, **second_report})
    else:
        round_reports.append({"round": 2, "status": "skipped_not_needed"})

    for section_name in list(polished.keys()):
        out: list[str] = []
        for line in polished[section_name]:
            if not line.strip() or not _line_needs_russian_editor(line):
                out.append(line)
                continue
            replacement = _same_section_reserve_line(section_name, candidates or [], rendered_urls, rendered_story_keys)
            if not replacement and section_name == "Общественный транспорт сегодня":
                replacement = _transport_replacement_for_line(line, candidates_by_key, rendered_urls)
            if replacement:
                out.append(replacement)
                replaced += 1
            else:
                # Strip, don't substitute the generic status line for a concrete
                # row (see first-round note above).
                stripped += 1
                warnings.append(f"Final editor stop-loss: stripped an unrepairable line in «{section_name}».")
        polished[section_name] = out

    # Generic TfGM status line stands in ONLY for an otherwise-empty transport
    # block (honest "no confirmed disruptions"), never beside concrete lines.
    _second_round_transport = [
        line for line in polished.get("Общественный транспорт сегодня", [])
        if line.strip() and line.strip() != "•"
    ]
    if not _second_round_transport and "Общественный транспорт сегодня" in polished:
        polished["Общественный транспорт сегодня"] = [_transport_status_fallback_line()]
        warnings.append("Final editor stop-loss: empty transport block replaced with TfGM status fallback.")

    bad_examples: list[str] = []
    for item in _visible_line_items(polished):
        line = str(item.get("line") or "")
        if _line_needs_russian_editor(line):
            remaining_bad += 1
            if len(bad_examples) < 8:
                bad_examples.append(line[:240])

    # S2: honest coverage. Every visible line must be reviewed at some point — so
    # coverage is complete if ANY round achieved full coverage (each round
    # re-processes the whole visible digest). A skipped/not-needed second round
    # must NOT flip a clean first round to incomplete; only a genuine
    # failed/partial pass with no covering round counts as incomplete (RC5).
    coverage_complete = any(
        bool(r.get("coverage_complete")) and str(r.get("status") or "") in {"ok", "skipped_no_items"}
        for r in round_reports
    )
    if round_reports and not coverage_complete:
        warnings.append(
            "Pre-send editor coverage incomplete after all rounds — unreviewed "
            "lines are flagged for recovery, not counted as clean (S2)."
        )

    return polished, {
        "enabled": True,
        "rules_fixed": rule_fixed,
        "model_fixed": model_fixed,
        "model_changes": model_changes[:120],
        "degraded_replaced": replaced,
        "degraded_stripped": stripped,
        "model_requested_replaced": model_requested_replaced,
        "model_requested_stripped": model_requested_stripped,
        "remaining_bad": remaining_bad,
        "coverage_complete": coverage_complete,
        "bad_examples": bad_examples,
        "model": PRE_SEND_RUSSIAN_EDITOR_MODEL,
        "max_rounds": PRE_SEND_EDITOR_MAX_ROUNDS,
        "rounds": round_reports,
        "block_action_reports": block_action_reports,
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
    final_editor_report_path = state_dir / "final_editor_report.json"

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
    # Index by URL so cross-section dedup can key on the story cluster, not just
    # the exact rendered string (catches one story in two blocks).
    candidates_by_key = _candidate_index(all_candidates)
    # S5: the lead block must win cross-section dedup. Process «Главная история
    # дня» FIRST so it claims its story key, and never drop a line from it — a
    # same-story sibling in a later section (e.g. a mayoral item in Городской
    # радар) is removed instead. Previously section-iteration order decided the
    # winner, so the lead lost to its own sibling and vanished from the HTML.
    lead_heading = "Главная история дня"
    ordered_sections = sorted(sections.items(), key=lambda kv: (kv[0] != lead_heading,))
    for section_name, lines in ordered_sections:
        deduped = _unique_preserving_order(lines)
        filtered: list[str] = []
        if len(deduped) < len(lines):
            warnings.append(f"Removed duplicate lines in section {section_name}.")
        for line in deduped:
            key = _line_story_key(line, candidates_by_key)
            previous_section = seen_lines_to_section.get(key)
            if previous_section and previous_section != section_name and section_name != lead_heading:
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

    report_payload = {
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
    }
    write_json(report_path, report_payload)
    write_json(
        final_editor_report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": report_payload["run_at_london"],
            "run_date_london": report_payload["run_date_london"],
            "stage_status": report_payload["stage_status"],
            "policy": "gpt-4o reads the whole visible digest, applies line-level Russian fixes, and the report stores before/after/reason for changed lines.",
            "pre_send_russian_editor": russian_editor_report,
            "warnings": warnings,
            "errors": errors,
            "draft_path": str(draft_path.resolve()),
        },
    )

    return StageResult(
        not errors,
        "Editor stage completed." if not errors else "Editor stage found blocking issues.",
        report_path,
        draft_path,
    )
