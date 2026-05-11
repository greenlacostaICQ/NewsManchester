"""Editorial curator pass — runs after validate-candidates, before llm-rewrite.

Sees all included candidates at once (~30-40), drops PR fluff / evergreen /
non-GM items, and marks one candidate as is_lead=True (main story of the day).
Uses gpt-4o-mini — decisions are binary, no deep reasoning required.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
import re

from news_digest.pipeline.common import now_london, read_json, today_london, write_json

logger = logging.getLogger(__name__)

OPENAI_BASE_URL = "https://api.openai.com/v1"
OPENAI_MODEL = "gpt-4o-mini"
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_MODEL = "gemini-2.5-flash"

CURATOR_PROMPT = """Ты редакторский куратор дайджеста «Greater Manchester AM Brief».

Тебе даётся список кандидатов. Для каждого прими решение: include true/false и is_lead true/false.

ПРАВИЛА ДРОПА (include: false):
- PR-объявления: только «компания объявила», «партнёрство», «назначение», «признана лучшей» — без конкретных фактов (суммы, даты, адреса)
- Мероприятия осведомлённости без конкретного события («неделя осведомлённости», «месяц X»)
- Дубли темы: если 2+ кандидата об одном и том же — оставь один с лучшим заголовком, остальные дропни
- Evergreen без даты: «лучшие бары», «куда пойти», «гид по», «топ мест» — без конкретной даты
- Tech/бизнес без GM-локации в заголовке или summary

ПРАВИЛА ВКЛЮЧЕНИЯ (include: true):
- Полиция/суды: задержание, приговор, обвинение — с именем/возрастом и местом
- Совет: конкретное решение меняющее что-то для жителей
- Транспорт: реальный сбой с маршрутом и временем
- События: с конкретной датой и площадкой
- Бизнес: открытие/закрытие/инвестиция с суммой £ и GM-адресом
- Еда: открытие нового заведения с названием и районом
- Рынки/ярмарки: конкретная дата, район/площадка, формат рынка или ярмарки
- Футбол: результат матча со счётом или трансфер с суммой

ЛИДЕР (is_lead: true) — ОДИН кандидат:
Выбирай новость с наибольшим влиянием на жителей GM сегодня.
Предпочитай: уголовное дело с исходом > решение совета > крупный транспортный сбой > значимое событие.
Лидом не может быть футбол, еда, evergreen.

Верни ТОЛЬКО JSON-массив без пояснений:
[{"fingerprint": "...", "include": true, "is_lead": false, "reason": "кратко почему"}]"""


_GM_BOROUGHS: tuple[str, ...] = (
    "Manchester", "Salford", "Trafford", "Stockport", "Tameside",
    "Oldham", "Rochdale", "Bury", "Bolton", "Wigan",
)
_CURATOR_PROTECTED_CATEGORIES = {"weather"}
_CURATOR_PROTECTED_BLOCKS = {"weather"}


def _is_curator_protected(candidate: dict) -> bool:
    return (
        str(candidate.get("category") or "") in _CURATOR_PROTECTED_CATEGORIES
        or str(candidate.get("primary_block") or "") in _CURATOR_PROTECTED_BLOCKS
    )


def _infer_borough(candidate: dict) -> str:
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle")
    )
    for borough in _GM_BOROUGHS:
        if re.search(rf"\b{re.escape(borough)}\b", text, flags=re.IGNORECASE):
            return borough
    return ""


_CURATOR_BATCH_SIZE = 20


def _call_curator_batch(batch: list[dict], client: object, model: str) -> list[dict]:
    payload = [
        {
            "fingerprint": c.get("fingerprint", ""),
            "title": c.get("title", ""),
            "summary": (c.get("summary") or "")[:320],
            "lead": (c.get("lead") or "")[:320],
            "evidence_text": (c.get("evidence_text") or "")[:700],
            "practical_angle": (c.get("practical_angle") or "")[:240],
            "category": c.get("category", ""),
            "primary_block": c.get("primary_block", ""),
            "source_label": c.get("source_label", ""),
            "source_url": c.get("source_url", ""),
            "published_at": c.get("published_at", ""),
            "freshness_status": c.get("freshness_status", ""),
            "event_page_type": c.get("event_page_type", ""),
            "borough": c.get("borough") or _infer_borough(c),
        }
        for c in batch
    ]
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": CURATOR_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        temperature=0.1,
        max_tokens=4000,
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.rsplit("```", 1)[0]
    return json.loads(raw.strip())


def _call_curator(candidates: list[dict], api_key: str, base_url: str, model: str) -> list[dict]:
    if not api_key or not candidates:
        return []
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed.")
        return []

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=60)
    results: list[dict] = []
    batches = [candidates[i:i + _CURATOR_BATCH_SIZE] for i in range(0, len(candidates), _CURATOR_BATCH_SIZE)]
    for i, batch in enumerate(batches):
        try:
            logger.info("Curator: batch %d/%d (%d candidates).", i + 1, len(batches), len(batch))
            results.extend(_call_curator_batch(batch, client, model))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Curator call failed: %s", exc)
            return []
    return results


def run_curator_pass(project_root: Path) -> None:
    """Drop PR/evergreen candidates and mark lead story before LLM rewrite."""
    candidates_path = project_root / "data" / "state" / "candidates.json"
    report_path = project_root / "data" / "state" / "curator_report.json"

    if not candidates_path.exists():
        logger.warning("candidates.json not found, skipping curator pass.")
        write_json(report_path, {"status": "skipped", "reason": "missing candidates.json", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    payload = json.loads(candidates_path.read_text(encoding="utf-8"))
    candidates = payload.get("candidates", [])

    included = [
        c
        for c in candidates
        if isinstance(c, dict) and c.get("include") and not _is_curator_protected(c)
    ]
    if not included:
        logger.info("Curator: no included candidates.")
        write_json(report_path, {"status": "skipped", "reason": "no included candidates", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    logger.info("Curator: reviewing %d included candidates.", len(included))

    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    if provider_override == "none":
        logger.info("LLM_PROVIDER=none — skipping curator pass.")
        write_json(report_path, {"status": "skipped", "reason": "LLM_PROVIDER=none", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    base_url = os.environ.get("LLM_BASE_URL") or OPENAI_BASE_URL
    model = os.environ.get("LLM_MODEL") or OPENAI_MODEL
    api_key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENAI_API_KEY", "")

    decisions = _call_curator(included, api_key, base_url, model)

    # Gemini fallback
    if not decisions:
        logger.info("Curator: OpenAI failed, trying Gemini.")
        time.sleep(1)
        decisions = _call_curator(
            included,
            os.environ.get("GEMINI_API_KEY", ""),
            GEMINI_BASE_URL,
            GEMINI_MODEL,
        )

    if not decisions:
        logger.warning("Curator: all providers failed — keeping existing include flags.")
        write_json(report_path, {"status": "skipped", "reason": "all providers failed", "run_at": now_london().isoformat(), "run_date_london": today_london()})
        return

    decision_map = {str(d.get("fingerprint") or ""): d for d in decisions if isinstance(d, dict)}
    dropped = 0
    lead_set = False

    for candidate in candidates:
        if _is_curator_protected(candidate):
            candidate["include"] = True
            candidate["is_lead"] = False
            continue
        fp = str(candidate.get("fingerprint") or "")
        decision = decision_map.get(fp)
        if not decision:
            continue
        if not decision.get("include", True):
            candidate["include"] = False
            candidate["dedupe_decision"] = "drop"
            candidate["reason"] = f"Curator drop: {decision.get('reason', '')}"
            dropped += 1
        if decision.get("is_lead") and not lead_set and candidate.get("include"):
            candidate["is_lead"] = True
            lead_set = True

    candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Curator: dropped %d candidates, lead=%s.", dropped, lead_set)

    write_json(report_path, {
        "run_at": now_london().isoformat(),
        "run_date_london": today_london(),
        "status": "complete",
        "reviewed": len(included),
        "dropped": dropped,
        "lead_set": lead_set,
        "decisions": decisions,
    })
