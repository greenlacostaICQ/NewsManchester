"""LLM rewrite stage — writes Russian draft_lines to candidates.json.

Provider chain:
  1. DeepSeek deepseek-chat — primary, paid, cheap (~$0.27/1M input)
  2. OpenAI gpt-4o-mini    — backup, paid (~$0.15/1M input)
  3. Groq Llama-3.3-70B    — emergency fallback, free tier
  4. Rule-based in writer.py — final safety net, always fires if LLM unavailable

Required env vars (set in GitHub Actions Secrets or .env.local):
  DEEPSEEK_API_KEY  — platform.deepseek.com (paid, deepseek-chat)
  OPENAI_API_KEY    — platform.openai.com (paid, gpt-4o-mini)
  GROQ_API_KEY      — console.groq.com (free)

Optional overrides:
  LLM_PROVIDER      — force "deepseek" | "openai" | "groq" | "none"
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

from news_digest.pipeline.common import now_london

logger = logging.getLogger(__name__)

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_MODEL = "deepseek-chat"  # alias for current production V3.x

OPENAI_BASE_URL = "https://api.openai.com/v1"
OPENAI_MODEL = "gpt-4o-mini"  # cheapest OpenAI model, ~$0.15/1M input tokens

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_FALLBACK_MODEL = "llama-3.3-70b-versatile"

_PROMPT_FOOTER = (
    '\nВерни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "draft_line": "• ..."}]\n'
    "Никакого markdown, никаких пояснений — только JSON."
)

_ANTI_HALLUCINATION = (
    "АНТИ-ВЫМЫСЕЛ: каждое имя, должность, число, сумма £, дата, адрес в твоём тексте ДОЛЖНЫ буквально присутствовать "
    "в title/summary/lead/evidence_text. Нет в evidence — нет в draft_line. Запрещены конструкции «по словам экспертов», "
    "«как ожидается», «вероятно», «по предварительным данным» — если этого нет в evidence.\n\n"
)

_LONG_FORMAT_RULES = (
    "ФОРМАТ: «• », Telegram HTML, без ссылок, без markdown. 350–500 символов, 2–3 коротких предложения.\n"
    "СТРУКТУРА:\n"
    "1) Первое предложение — главный факт: кто (имя/возраст/должность), что сделал/произошло, где конкретно (район/улица/площадка GM).\n"
    "2) Второе предложение — ключевая деталь из evidence_text: сумма, число, причина, имя пострадавшего/обвиняемого, дата вступления в силу, последствие. То, ради чего человек читает.\n"
    "3) Третье предложение (по желанию, если данные есть) — что это значит для жителя GM: с какой даты меняется, кого затронет, чем кончится.\n\n"
    "АНТИ-ПОВТОР: второе предложение НЕ должно пересказывать первое другими словами. "
    "Если в evidence_text нет новых фактов для второго предложения — оставь одно предложение, не добавляй воду.\n\n"
)

PROMPT_TRANSPORT = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для транспортных сбоев.\n\n"
    "ФОРМАТ: «• », Telegram HTML, без ссылок, максимум 160 символов, весь текст на русском кроме названий линий/операторов.\n\n"
    "ОПЕРАТОР — БЕРИ ИЗ TITLE, НЕ ИЗ SOURCE_LABEL.\n"
    "Title вида «TransPennine Express: Disruption between A and B» → оператор = «TransPennine Express», НЕ «National Rail».\n"
    "Title вида «Ashton/Eccles Lines - Minor Delay» → оператор = «Metrolink» (для tfgm.com линий) или «TfGM» (для bus stops).\n\n"
    "ПРАВИЛО: оператор + что именно + маршрут/линия (если в title) + когда заканчивается (если в evidence).\n"
    "ПЕРЕВОДИ ВСЁ: «Disruption» → «сбой/задержки», «Minor Delay» → «небольшие задержки», «Bus Stop Closure» → «закрытие остановки», «Improvement Works» → «ремонтные работы».\n"
    "Английские слова в финальной строке (кроме названий линий) — ЗАПРЕЩЕНО.\n\n"
    "«• У Northern до конца четверга отменены поздние рейсы через Manchester Piccadilly.»\n"
    "«• Metrolink: задержки на линии Ashton/Eccles из-за неисправности трамвая.»\n"
    "«• TransPennine Express: сбой между Selby и Hull — ремонт пути.»\n\n"
    "Без «проверьте заранее», без «следите за обновлениями»."
    + _PROMPT_FOOTER
)

PROMPT_CITY_NEWS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для городских новостей GM "
    "(полиция, советы, NHS, происшествия, мэрия).\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ПИШИ ПО EVIDENCE_TEXT, НЕ ПЕРЕВОДОМ TITLE. В evidence обычно есть имена пострадавших, возраст, район, причина, дата суда — это и есть содержание для второго предложения.\n\n"
    "ПОЛИЦИЯ/СУДЫ: «• Moss Side: 34-летний Адриан Браун, отец троих детей, зарезан на улице в южном Манчестере. Полиция назвала имя жертвы, мать заявила «они забрали моего мальчика»; задержанных пока нет. Это второе ножевое убийство в районе за месяц.»\n"
    "СОВЕТ/МЭРИЯ: что меняется, с какой даты, для кого. «• Манчестер: совет утвердил план обязательной лицензии для съёмного жилья в 8 районах с 1 июля. Лицензия стоит £1175 на пять лет и распространяется на ~22 000 квартир в Cheetham, Levenshulme, Moss Side и Longsight. Цель — поднять стандарты после жалоб на сырость и плесень.»\n"
    "NHS/СЛУЖБЫ: конкретный факт + цифра + срок. Без PR-языка («важный шаг», «значимая инициатива») и без «появилось обновление», «заметный кейс».\n\n"
    "ЗАПРЕЩЕНО: «туристическая достопримечательность», «местный житель», «одна из организаций», «заранее проверьте», размытые формулировки. Если в evidence_text нет ни одной конкретной детали — лучше короче, чем выдумывать.\n"
    "НАЗВАНИЯ: если в title есть название организации, компании, учреждения (слово с заглавной буквы) — оно ОБЯЗАНО присутствовать в draft_line. «Одна из организаций», «местная компания», «один из советов» — ЗАПРЕЩЕНО."
    + _PROMPT_FOOTER
)

PROMPT_EVENTS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для событий и культуры GM.\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ОБЯЗАТЕЛЬНО: дата + площадка + о чём событие своими словами, опираясь на evidence_text.\n"
    "Структура: 1) что именно + где + когда; 2) кто играет/ставит/курирует + о чём это (из evidence); 3) цена/билеты/доступность, если есть в evidence.\n\n"
    "«• В HOME 10–14 мая — танцевальный спектакль Akram Khan «Outwitting the Devil» о миграции и мифологии. Постановка для шести танцоров, премьера в Лондоне получила 4 звезды у Guardian. Билеты от £15, скидки для студентов.»\n"
    "«• В Dunham Massey 10 мая — May Day фестиваль с выбором Королевы роз и народными танцами. Вход свободный для членов National Trust, для остальных — £8 со взрослого. Парковка ограничена, рекомендуют приехать до 11:00.»\n\n"
    "МУЗЫКА: артист + площадка + дата + жанр/формат, только если жанр/формат есть в evidence; не выдумывай жанр.\n\n"
    "ЗАПРЕЩЕНО: «не пропустите», «обязательно посетите», «захватывающий», «уникальный», даты без конкретного места."
    + _PROMPT_FOOTER
)

PROMPT_DIASPORA_EVENTS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для русскоязычных концертов, стендапа и diaspora events в UK.\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ОБЯЗАТЕЛЬНО: артист/комик или название события + город + площадка + дата. Если есть фаза продаж — прямо скажи «билеты уже в продаже» или «продажи стартуют ...».\n"
    "Пиши как полезный early warning: человеку важно узнать заранее, а не в день концерта. London/Liverpool/Manchester можно оставлять в этом блоке, если событие русскоязычное или от diaspora-промоутера.\n\n"
    "«• Manchester Academy 24 марта — концерт Би-2 от EventCartel. На странице указаны двери в 19:00, curfew в 23:00 и билеты £69.75 плюс сбор; это официальный листинг площадки. Если планируете идти, лучше брать заранее: такие туры редко получают много северных дат.»\n"
    "«• The Comedy Store Manchester 23 октября — русскоязычный стендап от UK Stand-Up Club. В описании указан конкретный комик, время начала и возрастное ограничение; билеты идут через Eventbrite/EventFirst. Это формат, который часто появляется вне Ticketmaster.»\n\n"
    "ЗАПРЕЩЕНО: добавлять биографию артиста, песни, политический контекст или статус «крупный артист», если этого нет в evidence_text."
    + _PROMPT_FOOTER
)

PROMPT_BUSINESS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для бизнеса, еды, открытий и рынков GM.\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ИСПОЛЬЗУЙ EVIDENCE_TEXT. В нём — конкретные имена, должности, суммы, адреса. Если в evidence есть «Chief Nursing Officer Duncan Burton», в твоём тексте должно быть «главный руководитель медсестринской службы Дункан Бертон», не безымянное «главного медсестры». ПЕРЕВОДИ ДОЛЖНОСТИ ПО РОДУ: мужское имя → мужской род, женское → женский.\n\n"
    "IT/БИЗНЕС: инвестиция с суммой £, открытие/закрытие компании с GM-локацией. Структура: 1) кто получил/инвестировал/открыл + где в GM; 2) сумма и куда пойдёт + сколько сотрудников/раундов до; 3) что это значит для рынка/региона, если в evidence есть. Кадровые назначения без сюжета — пропусти (верни \"\").\n"
    "«• Salford-стартап Heliex получил £3.2 млн от Aviva Ventures на расширение в Сингапур и Гонконг. Компания делает турбины для рекуперации тепла на промпредприятиях; за три года выручка выросла с £400k до £2.1 млн. Новый раунд — пятый, с 2019 года Heliex привлекла £8.5 млн.»\n\n"
    "ЕДА/ОТКРЫТИЯ: название + тип заведения + район GM + дата. Не перевод заголовка: объясни, что реально открывается, кто стоит за проектом, что в меню/чем выделяется. Если в evidence есть имя шефа, цена, концепция — добавь.\n"
    "«• На Thomas Street в Northern Quarter с 13 мая — корейский ресторан Seoulful, проект бывшего шефа Hawksmoor Ли Уильямса. В меню — bibimbap, KFC-стиль курица и натуральные вина по £6 за бокал. Открытие совпадает с запуском восьми новых заведений в NQ за месяц.»\n\n"
    "РЫНКИ/ЯРМАРКИ: название + район/площадка + дата/время + что продают, сколько участников, кому идёт сбор.\n"
    "«• В Prestwich 10 мая, 10:00–16:00 — Makers Market у Longfield Centre. Около 50 независимых продавцов: керамика, мыло, выпечка, винтаж. Часть выручки идёт на ремонт детской площадки в St Mary's Park.»\n\n"
    "ЗАПРЕЩЕНО: профили людей без цифр, PR-события без конкретных данных, компании без GM-адреса.\n"
    "НАЗВАНИЯ: если в title есть название компании, ресторана, бренда, стартапа (слово с заглавной буквы) — оно ОБЯЗАНО присутствовать в draft_line. «Технологическая компания», «местный ресторан», «один из стартапов» — ЗАПРЕЩЕНО."
    + _PROMPT_FOOTER
)

PROMPT_FOOTBALL = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line только для Man Utd и Man City.\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ПРИНИМАЙ — верни заполненный draft_line:\n"
    "• Результат матча со счётом: 1) счёт + соперник + турнир; 2) кто забил/удалён + минута; 3) что это значит для турнирной таблицы/серии.\n"
    "  «• Man City 2–1 Arsenal в АПЛ на «Этихаде». Голы Холанда (34') и де Брёйне (87') с пенальти; Сака отквитал на 70'. После 32 туров City — третьи, отрыв от Liverpool сократился до 4 очков.»\n"
    "• Трансфер с суммой и клубом: фигурант + сумма + контракт + откуда; ради чего; что значит для состава.\n"
    "  «• Man Utd подписал Кассерру из Sporting за £38 млн на пять лет. 23-летний португалец — опорный полузащитник, проведёт первый матч после паузы на сборные. Подписание закрывает дыру после ухода Каземиро в Al-Nassr.»\n"
    "• Официальный анонс матча, реакция тренера/игрока с конкретной деталью, травма/возвращение.\n\n"
    "ПРОПУСКАЙ — верни draft_line \"\": пресс-конференции без новых фактов, подкасты, донации, награды без имени, "
    "Under-18/Under-21, женские команды, matchday programme, фото-галереи, Community события, мерч и kit launches."
    + _PROMPT_FOOTER
)

_CATEGORY_TO_PROMPT: dict[str, str] = {
    "transport": PROMPT_TRANSPORT,
    "gmp": PROMPT_CITY_NEWS,
    "media_layer": PROMPT_CITY_NEWS,
    "council": PROMPT_CITY_NEWS,
    "public_services": PROMPT_CITY_NEWS,
    "city_news": PROMPT_CITY_NEWS,
    "culture_weekly": PROMPT_EVENTS,
    "venues_tickets": PROMPT_EVENTS,
    "russian_speaking_events": PROMPT_DIASPORA_EVENTS,
    "food_openings": PROMPT_BUSINESS,
    "tech_business": PROMPT_BUSINESS,
    "football": PROMPT_FOOTBALL,
}


BATCH_SIZE = 20      # default — used for OpenAI and Gemini
GROQ_BATCH_SIZE = 8  # Groq free tier: 6000 TPM; 8 candidates ≈ 2700 tokens safely

FIX_TRANSLATE_SYSTEM = """Переведи строку новостного дайджеста на русский язык.
Названия людей, мест, брендов, компаний, IT-терминов оставляй по-английски.
Строка начинается с «• » и не превышает 280 символов.
Верни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "draft_line": "• ..."}]
Никакого markdown, никаких пояснений — только JSON."""

REPAIR_DRAFT_SYSTEM = """Ты senior editor городского morning brief.
Исправь слабые draft_line на нормальные русские пункты: самодостаточно, понятно, без канцелярита.

ФОРМАТ:
- строка начинается с «• »
- для категорий media_layer/gmp/council/public_services/food_openings/tech_business/culture_weekly/venues_tickets/russian_speaking_events/football: 350–500 символов, 2–3 коротких предложения
- для transport — 90–160 символов, одна строка
- без ссылок и markdown
- только факты из title/summary/lead/evidence_text/source_label/source_url/published_at

АНТИ-ВЫМЫСЕЛ: каждое имя, число, сумма £, дата, адрес должны буквально присутствовать в evidence_text/title. Запрещены «по словам экспертов», «как ожидается», «вероятно», если их нет в evidence.

СТРУКТУРА длинного формата:
1) Главный факт: кто, что, где конкретно.
2) Ключевая деталь из evidence: сумма/имя/причина/дата.
3) (опционально) что это значит для жителя GM.

АНТИ-ПОВТОР: второе предложение НЕ должно пересказывать первое другими словами. Если новых фактов нет — оставь одно предложение.
НАЗВАНИЯ: если в title есть название компании/организации/ресторана — оно ОБЯЗАНО быть в draft_line.

Если данных мало, всё равно сделай лучший самодостаточный пункт из имеющихся фактов. Не возвращай пустую строку.

Верни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "draft_line": "• ..."}]
Никакого markdown, никаких пояснений — только JSON."""

_REPAIR_BAD_MARKERS = (
    "forecast",
    "live alert",
    "attractions",
    "highlights",
    "опубликовал важное обновление",
    "появилось новое обновление",
    "футбольное обновление",
    "подробности уточняйте",
    "подробности ниже",
)


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


_LONG_FORMAT_CATEGORIES_FOR_REPAIR = {
    "media_layer", "gmp", "council", "public_services",
    "food_openings", "tech_business", "culture_weekly",
    "venues_tickets", "russian_speaking_events", "football",
}


def _needs_quality_repair(candidate: dict) -> bool:
    line = str(candidate.get("draft_line") or "").strip()
    if not line:
        return False
    category = str(candidate.get("category") or "")
    primary_block = str(candidate.get("primary_block") or "")
    if category not in _LONG_FORMAT_CATEGORIES_FOR_REPAIR | {"transport"}:
        return False
    normalized = re.sub(r"\s+", " ", line)
    lowered = normalized.lower()
    sentence_count = len(re.findall(r"[.!?]", normalized))
    # Long-format card: must hit ≥150 chars AND ≥2 sentences. Anything
    # shorter is still a headline and will be blocked by the writer.
    if category in _LONG_FORMAT_CATEGORIES_FOR_REPAIR:
        if len(normalized) < 150 or sentence_count < 2:
            return True
    if len(normalized) < 90 and (category == "food_openings" or primary_block in {"weekend_activities", "next_7_days", "ticket_radar"}):
        return True
    if any(marker in lowered for marker in _REPAIR_BAD_MARKERS):
        return True
    if _needs_translation_fix(line):
        return True
    # Bare opening lines like "X opens — date" are exactly what made the
    # food section feel like translated headlines rather than edited copy.
    if category == "food_openings" and sentence_count < 1:
        return True
    return False


def _call_provider_batch(
    base_url: str,
    api_key: str,
    model: str,
    candidates: list[dict],
    provider_name: str,
    timeout: int = 90,
    batch_size: int = BATCH_SIZE,
    system_prompt: str = PROMPT_CITY_NEWS,
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
                    "evidence_text": c.get("evidence_text", ""),
                    "category": c.get("category", ""),
                    "primary_block": c.get("primary_block", ""),
                    "practical_angle": c.get("practical_angle", ""),
                    "source_label": c.get("source_label", ""),
                    "source_url": c.get("source_url", ""),
                    "published_at": c.get("published_at", ""),
                    "freshness_status": c.get("freshness_status", ""),
                    "borough": c.get("borough", ""),
                    "current_draft_line": c.get("draft_line", ""),
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
                max_tokens=8192,
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
                    mapping[fp] = (dl, provider_name, model)
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


def _call_with_fallback(
    candidates: list[dict],
    prompt: str,
    provider_override: str,
    base_url_override: str,
    model_override: str,
    label_suffix: str = "",
) -> dict[str, str]:
    """Call provider chain with a specific prompt, return fingerprint→draft_line."""
    if not candidates:
        return {}
    if provider_override == "none":
        return {}
    if provider_override and base_url_override and model_override:
        return _call_provider_batch(
            base_url_override, os.environ.get("LLM_API_KEY", ""),
            model_override, candidates, provider_override + label_suffix,
            system_prompt=prompt,
        )
    mapping = _call_provider_batch(
        DEEPSEEK_BASE_URL, os.environ.get("DEEPSEEK_API_KEY", ""), DEEPSEEK_MODEL,
        candidates, f"DeepSeek{label_suffix}", system_prompt=prompt,
    )
    missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
    if missing:
        time.sleep(1)
        mapping.update(_call_provider_batch(
            OPENAI_BASE_URL, os.environ.get("OPENAI_API_KEY", ""), OPENAI_MODEL,
            missing, f"OpenAI{label_suffix}", system_prompt=prompt,
        ))
    missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
    if missing:
        time.sleep(1)
        mapping.update(_call_provider_batch(
            GROQ_BASE_URL, os.environ.get("GROQ_API_KEY", ""), GROQ_FALLBACK_MODEL,
            missing, f"Groq{label_suffix}", batch_size=GROQ_BATCH_SIZE, system_prompt=prompt,
        ))
    return mapping


def run_llm_rewrite(project_root: Path) -> None:
    """Read candidates.json, fill Russian draft_lines for included candidates."""
    candidates_path = project_root / "data" / "state" / "candidates.json"
    if not candidates_path.exists():
        logger.warning("candidates.json not found, skipping LLM rewrite.")
        return

    payload = json.loads(candidates_path.read_text(encoding="utf-8"))
    candidates = payload.get("candidates", [])

    # Rewrite EVERY included candidate each run, not only ones missing a
    # draft_line. Caching draft_lines between runs meant a one-time fallback
    # to Gemini/Groq Llama (during an OpenAI timeout, say) would freeze a
    # weak draft_line into state forever — and later runs with healthy
    # OpenAI quietly skipped them. With ~50-80 candidates/day this costs
    # roughly $0.02/day on gpt-4o-mini but guarantees today's text actually
    # came from today's primary model.
    to_rewrite = [
        c for c in candidates
        if isinstance(c, dict) and c.get("include")
    ]
    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    model_override = os.environ.get("LLM_MODEL", "").strip()
    base_url_override = os.environ.get("LLM_BASE_URL", "").strip()

    if provider_override == "none":
        logger.info("LLM_PROVIDER=none — skipping rewrite.")
        return

    if not to_rewrite:
        logger.info("LLM rewrite: all included candidates already have draft_lines.")
    else:
        logger.info("LLM rewrite: %d candidates need draft_lines.", len(to_rewrite))

        # Group by prompt type and call each group separately
        groups: dict[str, list[dict]] = {}
        for c in to_rewrite:
            prompt = _CATEGORY_TO_PROMPT.get(str(c.get("category") or ""), PROMPT_CITY_NEWS)
            groups.setdefault(prompt, []).append(c)

        mapping: dict[str, str] = {}
        for prompt, group in groups.items():
            logger.info("LLM rewrite: calling group of %d candidates.", len(group))
            mapping.update(_call_with_fallback(group, prompt, provider_override, base_url_override, model_override))

        run_iso = now_london().isoformat()
        applied = 0
        for candidate in candidates:
            fp = str(candidate.get("fingerprint") or "").strip()
            if fp in mapping:
                line, prov, model_name = mapping[fp]
                candidate["draft_line"] = line
                candidate["draft_line_provider"] = prov
                candidate["draft_line_model"] = model_name
                candidate["draft_line_written_at"] = run_iso
                applied += 1

        candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("LLM rewrite: applied %d draft_lines.", applied)

    # Fix pass: re-translate draft_lines still mostly in English
    to_fix = [
        c for c in candidates
        if isinstance(c, dict) and c.get("include")
        and _needs_translation_fix(str(c.get("draft_line") or ""))
    ]
    if to_fix:
        logger.info("LLM fix pass: %d English-dominant draft_lines, re-translating.", len(to_fix))
        fix_candidates = [{"fingerprint": c.get("fingerprint", ""), "draft_line": c.get("draft_line", "")} for c in to_fix]
        fix_mapping = _call_with_fallback(fix_candidates, FIX_TRANSLATE_SYSTEM, provider_override, base_url_override, model_override, label_suffix="-fix")

        run_iso = now_london().isoformat()
        fixed = 0
        for candidate in candidates:
            fp = str(candidate.get("fingerprint") or "").strip()
            if fp in fix_mapping:
                line, prov, model_name = fix_mapping[fp]
                if not _needs_translation_fix(line):
                    candidate["draft_line"] = line
                    candidate["draft_line_provider"] = prov
                    candidate["draft_line_model"] = model_name
                    candidate["draft_line_written_at"] = run_iso
                    fixed += 1

        if fixed:
            candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("LLM fix pass: fixed %d English draft_lines.", fixed)

    to_repair = [
        c for c in candidates
        if isinstance(c, dict) and c.get("include") and _needs_quality_repair(c)
    ]
    if not to_repair:
        return

    logger.info("LLM repair pass: %d weak draft_lines, rewriting editorially.", len(to_repair))
    repair_mapping = _call_with_fallback(
        to_repair,
        REPAIR_DRAFT_SYSTEM,
        provider_override,
        base_url_override,
        model_override,
        label_suffix="-repair",
    )

    repaired = 0
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "").strip()
        if fp not in repair_mapping:
            continue
        replacement, prov, model_name = repair_mapping[fp]
        if replacement and replacement.startswith("• ") and len(re.sub(r"\s+", " ", replacement)) >= 70:
            candidate["draft_line"] = replacement
            candidate["draft_line_provider"] = prov
            candidate["draft_line_model"] = model_name
            candidate["draft_line_written_at"] = run_iso
            repaired += 1

    if repaired:
        candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("LLM repair pass: repaired %d weak draft_lines.", repaired)
