"""LLM rewrite stage — writes Russian draft_lines to candidates.json.

Provider chain:
  1. Cerebras (qwen-3-235b-a22b-instruct-2507) — primary, generous free tier
  2. Groq (llama-3.3-70b-versatile) — fallback
  3. Rule-based in writer.py — final safety net, always fires if LLM unavailable

Required env vars (set in GitHub Actions Secrets or .env.local):
  CEREBRAS_API_KEY  — console.cerebras.ai (free)
  GROQ_API_KEY      — console.groq.com (free)

Optional overrides:
  LLM_PROVIDER      — force "cerebras" | "groq" | "none"
  LLM_MODEL         — override model name
  LLM_BASE_URL      — override API base URL
  LLM_API_KEY       — override API key (used with LLM_PROVIDER)
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)

CEREBRAS_BASE_URL = "https://api.cerebras.ai/v1"
CEREBRAS_MODEL = "qwen-3-235b-a22b-instruct-2507"

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "llama-3.3-70b-versatile"

SYSTEM_PROMPT = """Ты редактор новостного дайджеста «Greater Manchester AM Brief».

Для каждого переданного кандидата напиши draft_line — 1-2 законченных предложения на русском языке.

Правила:
- draft_line ВСЕГДА начинается с «• »
- Telegram HTML для выделения: <b>текст</b> — НЕ **Markdown**
- Не добавляй ссылки <a href=...>, pipeline добавит их сам
- Первое предложение: конкретный факт (что произошло, что объявлено)
- Второе предложение (необязательно): практический угол для жителя Манчестера
- Для полицейских новостей: первое предложение называет что произошло
- Для событий и билетов: укажи дату словами
- Не используй: «важный сигнал», «заметный кейс», «слот подтверждён»
- Минимум 45 символов
- Весь текст на русском, кроме имён собственных

Верни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "draft_line": "• ..."}]
Никакого markdown, никаких пояснений — только JSON."""


def _call_provider(
    base_url: str,
    api_key: str,
    model: str,
    candidates: list[dict],
    provider_name: str,
    timeout: int = 90,
) -> dict[str, str]:
    """Call one OpenAI-compatible provider. Returns fingerprint→draft_line mapping."""
    if not api_key:
        logger.warning("%s: API key not set, skipping.", provider_name)
        return {}

    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return {}

    user_content = json.dumps(
        [
            {
                "fingerprint": c.get("fingerprint", ""),
                "title": c.get("title", ""),
                "summary": c.get("summary", ""),
                "lead": c.get("lead", ""),
                "category": c.get("category", ""),
                "primary_block": c.get("primary_block", ""),
                "practical_angle": c.get("practical_angle", ""),
                "source_label": c.get("source_label", ""),
            }
            for c in candidates
        ],
        ensure_ascii=False,
    )

    try:
        logger.info(
            "%s: sending %d candidates to %s...", provider_name, len(candidates), model
        )
        client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.3,
            max_tokens=2048,
        )
        raw = response.choices[0].message.content.strip()
        # Strip markdown code fences if model wraps response
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.rsplit("```", 1)[0]
        results = json.loads(raw.strip())
        mapping: dict[str, str] = {}
        for item in results:
            fp = str(item.get("fingerprint") or "").strip()
            dl = str(item.get("draft_line") or "").strip()
            if fp and dl and dl.startswith("• ") and len(dl) >= 10:
                mapping[fp] = dl
        logger.info("%s: got %d valid draft_lines.", provider_name, len(mapping))
        return mapping
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s: failed — %s", provider_name, exc)
        return {}


def run_llm_rewrite(project_root: Path) -> None:
    """Read candidates.json, fill Russian draft_lines for included candidates.

    Falls through silently if no API keys are set or both providers fail —
    writer.py rule-based fallback is the final safety net.
    """
    candidates_path = project_root / "data" / "state" / "candidates.json"

    if not candidates_path.exists():
        logger.warning("candidates.json not found, skipping LLM rewrite.")
        return

    payload = json.loads(candidates_path.read_text(encoding="utf-8"))
    candidates = payload.get("candidates", [])

    to_rewrite = [
        c
        for c in candidates
        if isinstance(c, dict)
        and c.get("include")
        and not str(c.get("draft_line") or "").strip()
    ]

    if not to_rewrite:
        logger.info("LLM rewrite: all included candidates already have draft_lines.")
        return

    logger.info("LLM rewrite: %d candidates need draft_lines.", len(to_rewrite))

    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    model_override = os.environ.get("LLM_MODEL", "").strip()
    base_url_override = os.environ.get("LLM_BASE_URL", "").strip()

    mapping: dict[str, str] = {}

    if provider_override == "none":
        logger.info("LLM_PROVIDER=none — rule-based fallback will handle.")
    elif provider_override and base_url_override and model_override:
        api_key = os.environ.get("LLM_API_KEY", "")
        mapping = _call_provider(
            base_url_override, api_key, model_override, to_rewrite, provider_override
        )
    else:
        # Primary: Cerebras Qwen
        mapping = _call_provider(
            CEREBRAS_BASE_URL,
            os.environ.get("CEREBRAS_API_KEY", ""),
            CEREBRAS_MODEL,
            to_rewrite,
            "Cerebras",
        )
        # Fallback: Groq for candidates still missing
        missing = [
            c
            for c in to_rewrite
            if str(c.get("fingerprint") or "") not in mapping
        ]
        if missing:
            logger.info(
                "Groq fallback: %d candidates still without draft_line.", len(missing)
            )
            time.sleep(1)
            groq_map = _call_provider(
                GROQ_BASE_URL,
                os.environ.get("GROQ_API_KEY", ""),
                GROQ_MODEL,
                missing,
                "Groq",
            )
            mapping.update(groq_map)

    if not mapping:
        logger.info(
            "LLM rewrite: no draft_lines written — rule-based fallback will handle."
        )
        return

    applied = 0
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "").strip()
        if fp in mapping:
            candidate["draft_line"] = mapping[fp]
            applied += 1

    payload["candidates"] = candidates
    candidates_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("LLM rewrite: applied %d draft_lines.", applied)
