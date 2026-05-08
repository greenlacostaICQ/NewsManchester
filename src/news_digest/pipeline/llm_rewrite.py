"""LLM rewrite stage — writes Russian draft_lines to candidates.json.

Provider chain:
  1. Gemini 2.0 Flash — primary, best quality
  2. Groq Qwen3-32B — fallback, reliable infra, good Russian quality
  3. Groq Llama-3.3-70B — emergency fallback on same Groq infra
  4. Rule-based in writer.py — final safety net, always fires if LLM unavailable

Required env vars (set in GitHub Actions Secrets or .env.local):
  GEMINI_API_KEY    — aistudio.google.com (free)
  GROQ_API_KEY      — console.groq.com (free)

Optional overrides:
  LLM_PROVIDER      — force "gemini" | "cerebras" | "groq" | "none"
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

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
# Primary: Qwen3-32B — best Russian quality, 100+ languages, reliable Groq infra
GROQ_PRIMARY_MODEL = "qwen3-32b"
# Fallback: Llama 3.3 70B — proven fallback on same Groq infra
GROQ_FALLBACK_MODEL = "llama-3.3-70b-versatile"

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_MODEL = "gemini-2.0-flash"

SYSTEM_PROMPT = """Ты редактор новостного дайджеста «Greater Manchester AM Brief».

Для каждого кандидата напиши draft_line — 2-3 предложения на русском с достаточным контекстом чтобы читатель понял суть без перехода по ссылке.

ФОРМАТ:
- Начинай ВСЕГДА с «• »
- Telegram HTML: <b>текст</b> — НЕ Markdown
- Без ссылок <a href=...> — pipeline добавит сам
- Максимум 280 символов на весь draft_line
- Весь текст на русском, кроме имён собственных и названий мест

СОДЕРЖАНИЕ:
- Первое предложение: ТОЛЬКО факт — что произошло, кто конкретно, где конкретно. Без оценок.
- Называй КОНКРЕТНЫЕ имена и места из summary/title: не «туристическая достопримечательность», а её название; не «местный житель», а возраст/должность; не «агентство», а его название.
- Второе предложение: контекст или подробности — число жертв, сумма, причина, хронология. Не повторяй первое предложение другими словами.
- Третье предложение (только если есть практическое следствие для жителя): маршрут закрыт, участок работает до X, цены вырастут.
- Для событий и билетов: обязательно укажи дату, место и о чём оно — коротко своими словами если название непонятное.
- Для полиции: кто (возраст/должность), что именно, где.
- Для IT/бизнес: только запуск, инвестиция, открытие или закрытие. Сумма если есть.
- Для футбола: счёт матча или сумма трансфера — одна строка с деталью.
- Погода: температура + главное явление. Без советов.

СТРОГО ЗАПРЕЩЕНО:
«заранее проверьте», «держите в планах», «уточните заранее», «сверьте»,
«важный сигнал», «заметный кейс», «это не X, а Y», «слот подтверждён»,
«если вам подходит», «если голосуете», «если следите», «если собираетесь»,
любые объяснения почему новость включена в дайджест,
расплывчатые фразы: «туристическая достопримечательность», «местный житель», «одна из организаций», «появилось обновление»,
бытовые английские слова вместо русских: «forecast»→«прогноз», «pop-up»→«временный магазин», «highlights»→«лучшие моменты».
IT-термины, названия мест и брендов оставляй по-английски: Digital, AI, CEO, startup, Manchester United.

СПЕЦИАЛЬНЫЕ ПРАВИЛА:
- Суд/полиция: обязательно назови кто (должность/возраст) и что конкретно произошло.
- Погода: только температура + главное явление дня. Без советов («лучше взять зонт» — запрещено).
- Футбол: только мужские команды (Man Utd, Man City). Только результат матча или факт трансфера с деталью (счёт, сумма, откуда).
  Пресс-конференции, подкасты, фотосессии, составы, женские команды — пропусти (верни пустой draft_line «»).
- IT/бизнес: только инвестиции, открытия, закрытия компаний. Кадровые назначения и профили людей — пропусти.
- События: дата + место + одно предложение о чём это, своими словами.

ПРИМЕРЫ правильно:
• В Bolton полиция задержала 17-летнего водителя после погони, пострадавшая девушка в больнице.
• У Northern до конца четверга изменены поздневечерние рейсы через Manchester Piccadilly.
• В HOME с 7 по 14 мая — фильм «The North» о пешем походе по Шотландии.
• 9–16°C, сильный дождь после обеда.
• Manchester United подписал полузащитника из Atletico Madrid за £45 млн.
• Asterix Health получила £2.1 млн на цифровой сервис первичной медпомощи.
• В Dunham Massey 10 мая — традиционный фестиваль May Day с выбором «Королевы роз».

ПРИМЕРЫ неправильно (запрещено):
• Туристическая достопримечательность в Greater Manchester пострадала от вандализма. ← нет конкретики
• Появилось новое судебное обновление. ← пустая строка
• Суд по делу об убийстве продолжается. ← что за дело, кто?
• С 2026 года проходит неделя осведомлённости. ← непонятно когда именно

Верни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "draft_line": "• ..."}]
Никакого markdown, никаких пояснений — только JSON."""


BATCH_SIZE = 20  # keep each request well under Groq's 12k TPM free-tier limit


def _call_provider_batch(
    base_url: str,
    api_key: str,
    model: str,
    candidates: list[dict],
    provider_name: str,
    timeout: int = 90,
) -> dict[str, str]:
    """Call one provider in batches of BATCH_SIZE. Returns fingerprint→draft_line."""
    if not api_key:
        logger.warning("%s: API key not set, skipping.", provider_name)
        return {}

    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return {}

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
    mapping: dict[str, str] = {}

    batches = [candidates[i: i + BATCH_SIZE] for i in range(0, len(candidates), BATCH_SIZE)]
    logger.info("%s: %d candidates → %d batch(es) of ≤%d.", provider_name, len(candidates), len(batches), BATCH_SIZE)

    for batch_idx, batch in enumerate(batches, start=1):
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
                for c in batch
            ],
            ensure_ascii=False,
        )
        try:
            logger.info("%s: batch %d/%d — sending %d candidates to %s...",
                        provider_name, batch_idx, len(batches), len(batch), model)
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
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.rsplit("```", 1)[0]
            results = json.loads(raw.strip())
            batch_hits = 0
            for item in results:
                fp = str(item.get("fingerprint") or "").strip()
                dl = str(item.get("draft_line") or "").strip()
                if fp and dl and dl.startswith("• ") and len(dl) >= 15:
                    mapping[fp] = dl
                    batch_hits += 1
            logger.info("%s: batch %d/%d → %d draft_lines.", provider_name, batch_idx, len(batches), batch_hits)
            if batch_idx < len(batches):
                time.sleep(1)  # small pause between batches
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s: batch %d/%d failed — %s", provider_name, batch_idx, len(batches), exc)

    logger.info("%s: total %d valid draft_lines.", provider_name, len(mapping))
    return mapping


# Keep old name as alias for backward compat
_call_provider = _call_provider_batch


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
        # Primary: Gemini 2.0 Flash (best quality)
        mapping = _call_provider(
            GEMINI_BASE_URL,
            os.environ.get("GEMINI_API_KEY", ""),
            GEMINI_MODEL,
            to_rewrite,
            "Gemini",
        )
        # Fallback 1: Groq Qwen3-32B (reliable, good Russian)
        missing = [
            c
            for c in to_rewrite
            if str(c.get("fingerprint") or "") not in mapping
        ]
        if missing:
            logger.info("Groq-Qwen3 fallback: %d candidates still without draft_line.", len(missing))
            time.sleep(1)
            qwen_map = _call_provider(
                GROQ_BASE_URL,
                os.environ.get("GROQ_API_KEY", ""),
                GROQ_PRIMARY_MODEL,
                missing,
                "Groq-Qwen3",
            )
            mapping.update(qwen_map)
        # Fallback 2: Groq Llama (emergency)
        missing = [
            c
            for c in to_rewrite
            if str(c.get("fingerprint") or "") not in mapping
        ]
        if missing:
            logger.info("Groq-Llama fallback: %d candidates still without draft_line.", len(missing))
            time.sleep(1)
            llama_map = _call_provider(
                GROQ_BASE_URL,
                os.environ.get("GROQ_API_KEY", ""),
                GROQ_FALLBACK_MODEL,
                missing,
                "Groq-Llama",
            )
            mapping.update(llama_map)

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
