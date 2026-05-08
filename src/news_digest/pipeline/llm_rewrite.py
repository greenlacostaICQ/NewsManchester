"""LLM rewrite stage — writes Russian draft_lines to candidates.json.

Provider chain:
  1. OpenAI gpt-4o-mini — primary, paid, reliable (~$0.15/1M input tokens)
  2. Gemini 2.0 Flash — fallback, free tier
  3. Groq Llama-3.3-70B — emergency fallback, free tier
  4. Rule-based in writer.py — final safety net, always fires if LLM unavailable

Required env vars (set in GitHub Actions Secrets or .env.local):
  OPENAI_API_KEY    — platform.openai.com (paid, gpt-4o-mini)
  GEMINI_API_KEY    — aistudio.google.com (free)
  GROQ_API_KEY      — console.groq.com (free)

Optional overrides:
  LLM_PROVIDER      — force "gemini" | "groq" | "openai" | "none"
  LLM_MODEL         — override model name
  LLM_BASE_URL      — override API base URL
  LLM_API_KEY       — override API key (used with LLM_PROVIDER)
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path

logger = logging.getLogger(__name__)

OPENAI_BASE_URL = "https://api.openai.com/v1"
OPENAI_MODEL = "gpt-4o-mini"  # cheapest OpenAI model, ~$0.15/1M input tokens

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_MODEL = "gemini-2.0-flash"

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_FALLBACK_MODEL = "llama-3.3-70b-versatile"

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
- Второе предложение — ТОЛЬКО если добавляет существенный контекст которого нет в первом: число жертв, сумма, причина, хронология. Если первое предложение уже полное — не добавляй второе.
- Третье предложение — ТОЛЬКО если есть конкретное практическое следствие для жителя: маршрут закрыт, участок работает до X, цены вырастут. Не добавляй если очевидно.
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


BATCH_SIZE = 20      # default — used for OpenAI and Gemini
GROQ_BATCH_SIZE = 8  # Groq free tier: 6000 TPM; 8 candidates ≈ 2700 tokens safely

FIX_TRANSLATE_SYSTEM = """Переведи строку новостного дайджеста на русский язык.
Названия людей, мест, брендов, компаний, IT-терминов оставляй по-английски.
Строка начинается с «• » и не превышает 280 символов.
Верни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "draft_line": "• ..."}]
Никакого markdown, никаких пояснений — только JSON."""


def _cyrillic_ratio(text: str) -> float:
    non_space = re.sub(r"\s", "", text)
    if not non_space:
        return 1.0
    return len(re.findall(r"[а-яёА-ЯЁ]", text)) / len(non_space)


_EN_FUNCTION_WORDS = frozenset({
    # articles / determiners
    "the", "a", "an",
    # prepositions
    "of", "in", "at", "on", "by", "as", "to", "up",
    "for", "with", "from", "into", "onto", "out",
    "after", "before", "during", "following", "across", "about",
    "ahead", "alongside", "within", "against", "despite",
    # conjunctions
    "and", "or", "but",
    # pronouns / determiners
    "their", "they", "this", "that", "which", "who", "its", "our",
    # auxiliary / common verbs
    "is", "are", "was", "were", "be", "been", "have", "has", "had",
    "will", "would", "could", "should", "may", "might",
    "said", "says", "makes", "made", "gets", "got",
    "signed", "confirmed", "announced", "opened", "closed",
    "donated", "makes", "joining", "leaves", "joins",
})


def _needs_translation_fix(draft_line: str) -> bool:
    """True only when the line reads as English prose, not just contains brand names."""
    text = str(draft_line or "").strip()
    if not text or _cyrillic_ratio(text) >= 0.5:
        return False
    lowercase_words = re.findall(r"[a-z][a-z''-]+", text)
    hits = sum(1 for w in lowercase_words if w in _EN_FUNCTION_WORDS)
    return hits >= 2


def _call_provider_batch(
    base_url: str,
    api_key: str,
    model: str,
    candidates: list[dict],
    provider_name: str,
    timeout: int = 90,
    batch_size: int = BATCH_SIZE,
    system_prompt: str = SYSTEM_PROMPT,
) -> dict[str, str]:
    """Call one provider in batches. Returns fingerprint→draft_line."""
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

    batches = [candidates[i: i + batch_size] for i in range(0, len(candidates), batch_size)]
    logger.info("%s: %d candidates → %d batch(es) of ≤%d.", provider_name, len(candidates), len(batches), batch_size)

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
                    {"role": "system", "content": system_prompt},
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
        # Primary: OpenAI gpt-4o-mini (paid, reliable, cheapest OpenAI model)
        mapping = _call_provider(
            OPENAI_BASE_URL,
            os.environ.get("OPENAI_API_KEY", ""),
            OPENAI_MODEL,
            to_rewrite,
            "OpenAI",
        )
        # Fallback 1: Gemini 2.0 Flash (free tier)
        missing = [
            c
            for c in to_rewrite
            if str(c.get("fingerprint") or "") not in mapping
        ]
        if missing:
            logger.info("Gemini fallback: %d candidates still without draft_line.", len(missing))
            time.sleep(1)
            gemini_map = _call_provider(
                GEMINI_BASE_URL,
                os.environ.get("GEMINI_API_KEY", ""),
                GEMINI_MODEL,
                missing,
                "Gemini",
            )
            mapping.update(gemini_map)
        # Fallback 2: Groq Llama (emergency, free tier)
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
                batch_size=GROQ_BATCH_SIZE,
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

    # Fix pass: re-translate draft_lines that are still mostly English
    to_fix = [
        c for c in candidates
        if isinstance(c, dict)
        and c.get("include")
        and _needs_translation_fix(str(c.get("draft_line") or ""))
    ]
    if not to_fix:
        return

    logger.info("LLM fix pass: %d draft_lines are English-dominant, re-translating.", len(to_fix))

    fix_candidates = [
        {"fingerprint": c.get("fingerprint", ""), "draft_line": c.get("draft_line", "")}
        for c in to_fix
    ]

    if provider_override == "none":
        logger.info("LLM_PROVIDER=none — skipping fix pass.")
        return

    fix_mapping: dict[str, str] = {}
    if provider_override and base_url_override and model_override:
        api_key = os.environ.get("LLM_API_KEY", "")
        fix_mapping = _call_provider_batch(
            base_url_override, api_key, model_override, fix_candidates,
            provider_override, system_prompt=FIX_TRANSLATE_SYSTEM,
        )
    else:
        fix_mapping = _call_provider_batch(
            OPENAI_BASE_URL, os.environ.get("OPENAI_API_KEY", ""), OPENAI_MODEL,
            fix_candidates, "OpenAI-fix", system_prompt=FIX_TRANSLATE_SYSTEM,
        )
        still_missing = [c for c in fix_candidates if c["fingerprint"] not in fix_mapping]
        if still_missing:
            fix_mapping.update(_call_provider_batch(
                GEMINI_BASE_URL, os.environ.get("GEMINI_API_KEY", ""), GEMINI_MODEL,
                still_missing, "Gemini-fix", system_prompt=FIX_TRANSLATE_SYSTEM,
            ))

    fixed = 0
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "").strip()
        if fp in fix_mapping and not _needs_translation_fix(fix_mapping[fp]):
            candidate["draft_line"] = fix_mapping[fp]
            fixed += 1

    if fixed:
        candidates_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("LLM fix pass: fixed %d English draft_lines.", fixed)
