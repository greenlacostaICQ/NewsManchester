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

import calendar
from dataclasses import dataclass
import json
import logging
import os
import re
import time
from datetime import date, timedelta
from pathlib import Path

from news_digest.pipeline.common import now_london, pipeline_run_id_from, today_london, write_json

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


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
    "ФОРМАТ: «• », Telegram HTML, без ссылок, без markdown. 250–450 символов, 2–3 коротких предложения.\n"
    "СТРУКТУРА:\n"
    "1) Первое предложение — главный факт: кто (имя/возраст/должность), что сделал/произошло, где конкретно (район/улица/площадка GM).\n"
    "2) Второе предложение — ключевая деталь из evidence_text: сумма, число, причина, имя пострадавшего/обвиняемого, дата вступления в силу, последствие. То, ради чего человек читает.\n"
    "3) Третье предложение (по желанию, если данные есть) — что это значит для жителя GM: с какой даты меняется, кого затронет, чем кончится.\n"
    "ЕСЛИ данных мало — пиши короче (250–300 символов), не добавляй воду. Лучше точный короткий пункт, чем раздутый пустой.\n\n"
)

PROMPT_TRANSPORT = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line только для сбоев общественного транспорта (Metrolink, bus, rail, coach).\n\n"
    "ФОРМАТ: «• », Telegram HTML, без ссылок, максимум 180 символов, весь текст на русском кроме названий линий/операторов/остановок.\n\n"
    "ОПЕРАТОР — БЕРИ ИЗ TITLE, НЕ ИЗ SOURCE_LABEL.\n"
    "Title вида «TransPennine Express: Disruption between A and B» → оператор = «TransPennine Express», НЕ «National Rail».\n"
    "Title вида «Ashton/Eccles Lines - Minor Delay» → оператор = «Metrolink».\n\n"
    "ЧЕТЫРЕ ОБЯЗАТЕЛЬНЫХ ЭЛЕМЕНТА в пункте (если есть в evidence):\n"
    "1) Что не работает (задержки/отмены/объезд);\n"
    "2) Между какими станциями или на какой линии;\n"
    "3) До какого времени/даты;\n"
    "4) Альтернатива (замещающий автобус, объезд).\n"
    "Если какого-то элемента нет в evidence — пропусти, не выдумывай.\n\n"
    "ПЕРЕВОДИ ВСЁ: «Disruption» → «сбой/задержки», «Minor Delay» → «небольшие задержки», «Bus Stop Closure» → «закрытие остановки», «Improvement Works» → «ремонтные работы», «Replacement Bus» → «замещающий автобус».\n"
    "Английские слова в финальной строке (кроме названий линий/остановок) — ЗАПРЕЩЕНО.\n\n"
    "«• Metrolink: с 17 мая по 1 июня нет трамваев на линии Bury между Bury Interchange и Crumpsall. Замещающий автобус с теми же остановками.»\n"
    "«• Northern: задержки до 30 мин на маршрутах через Manchester Victoria — сигнальная неисправность.»\n"
    "«• TransPennine Express: сбой между Selby и Hull — ремонт пути, ориентировочно до 18:00.»\n\n"
    "Без «проверьте заранее», без «следите за обновлениями»."
    + _PROMPT_FOOTER
)

PROMPT_CITY_NEWS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для городских новостей GM "
    "(полиция, советы, NHS, происшествия, мэрия, городское развитие, наука).\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ПИШИ ПО EVIDENCE_TEXT, НЕ ПЕРЕВОДОМ TITLE. В evidence обычно есть имена пострадавших, возраст, район, причина, дата суда — это и есть содержание для второго предложения.\n\n"
    "ПОЛИЦИЯ/СУДЫ: «• Moss Side: 34-летний Адриан Браун, отец троих детей, зарезан на улице в южном Манчестере. Полиция назвала имя жертвы, мать заявила «они забрали моего мальчика»; задержанных пока нет. Это второе ножевое убийство в районе за месяц.»\n"
    "СОВЕТ/МЭРИЯ: что меняется, с какой даты, для кого. «• Манчестер: совет утвердил план обязательной лицензии для съёмного жилья в 8 районах с 1 июля. Лицензия стоит £1175 на пять лет и распространяется на ~22 000 квартир в Cheetham, Levenshulme, Moss Side и Longsight. Цель — поднять стандарты после жалоб на сырость и плесень.»\n"
    "NHS/СЛУЖБЫ: конкретный факт + цифра + срок. Без PR-языка («важный шаг», «значимая инициатива») и без «появилось обновление», «заметный кейс».\n\n"
    "ТЕРМИНОЛОГИЯ — переводи точно, не буквально:\n"
    "«mural» = настенная роспись/мурал — НЕ «граффити» (граффити — несанкционированные надписи, это разные понятия).\n"
    "«climate-ready» / «climate-resilient countries» = страны, адаптированные к климатическим изменениям — НЕ «страны, готовые к изменению климата».\n"
    "«sponge park» = парк-губка (специально спроектированный для впитывания ливневой воды, защита от подтоплений) — объяснение включи в текст.\n"
    "«OnlyFans creator» = модель/автор OnlyFans — НЕ «создательница OnlyFans» (создатель платформы — другой человек).\n"
    "«National League → League Two» = из Национальной лиги (5-й дивизион, полупрофессиональный) в League Two (4-й дивизион, профессиональная EFL) — поясни кратко.\n\n"
    "ЗАПРЕЩЕНО: «туристическая достопримечательность», «местный житель», «одна из организаций», «заранее проверьте», «привлечёт внимание», размытые формулировки. Если в evidence_text нет ни одной конкретной детали — пиши короче, не выдумывай."
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
    "ПЕРЕВОДИ ВСЕ НАРИЦАТЕЛЬНЫЕ ТЕРМИНЫ — кроме имён собственных, названий площадок и устоявшихся культурных понятий (punk, jazz, hip-hop, opera):\n"
    "«booking fee» → «сбор при покупке», «under-30s» → «до 30 лет», «claimants» → «получатели пособий», "
    "«guided writing session» → «занятие с ведущим», «book club» → «книжный клуб», "
    "«soft refreshments» → «лёгкие угощения», «life drawing» → «рисование с натуры», "
    "«in residence» / «artist in residence» → «художник-резидент», "
    "«mild horror» → «мягкий хоррор», «flashes» → «световые вспышки», «toggle» → «настройка отключения».\n\n"
    "ЗАПРЕЩЕНО: «не пропустите», «обязательно посетите», «захватывающий», «уникальный», даты без конкретного места, "
    "CTA без конкретики («уточните даты», «билеты и даты уточняйте»)."
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
    "ЕДА/ОТКРЫТИЯ: название + тип заведения + район GM + дата открытия (только если есть в evidence). Не перевод заголовка: объясни, что реально открывается, кто стоит за проектом, что в меню/чем выделяется. Если даты открытия в evidence нет — пиши «уже работает» или «недавно открылся», не выдумывай дату.\n"
    "«• На Thomas Street в Northern Quarter с 13 мая — корейский ресторан Seoulful, проект бывшего шефа Hawksmoor Ли Уильямса. В меню — bibimbap, KFC-стиль курица и натуральные вина по £6 за бокал. Открытие совпадает с запуском восьми новых заведений в NQ за месяц.»\n\n"
    "РЫНКИ/ЯРМАРКИ: название + район/площадка + КОНКРЕТНАЯ дата ближайшего проведения + что продают.\n"
    "ЕСЛИ в summary есть поле NEXT_OCCURRENCE — используй его как точную дату ближайшего рынка. Не пиши «каждую третью субботу» — пиши конкретную дату: «в субботу 16 мая», «в воскресенье 17 мая».\n"
    "«• В Prestwich, суббота 10 мая, 10:00–16:00 — Makers Market у Longfield Centre. Около 50 независимых продавцов: керамика, мыло, выпечка, винтаж. Вход свободный.»\n\n"
    "ПЕРЕВОДИ: «takeaway» → «навынос», «booking fee» → «сбор при покупке».\n\n"
    "ЗАПРЕЩЕНО: профили людей без цифр, PR-события без конкретных данных, компании без GM-адреса, "
    "рынки без конкретной даты, \"каждую субботу\", \"каждое воскресенье\" (вместо этого — конкретная дата)."
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
- для категорий media_layer/gmp/council/public_services/food_openings/tech_business/culture_weekly/venues_tickets/russian_speaking_events/football: 250–450 символов, 2–3 коротких предложения
- для transport — 90–180 символов, одна строка
- без ссылок и markdown
- только факты из title/summary/lead/evidence_text/source_label/source_url/published_at

АНТИ-ВЫМЫСЕЛ: каждое имя, число, сумма £, дата, адрес должны буквально присутствовать в evidence_text/title. Запрещены «по словам экспертов», «как ожидается», «вероятно», если их нет в evidence.

СТРУКТУРА длинного формата:
1) Главный факт: кто, что, где конкретно.
2) Ключевая деталь из evidence: сумма/имя/причина/дата.
3) (опционально) что это значит для жителя GM.

ЕСЛИ данных мало — пиши 250–300 символов с реальной фактурой. Лучше точный короткий пункт, чем раздутый пустой.

ЗАПРЕЩЕНЫ окончания-заглушки: «обогатит», «центр притяжения», «новая достопримечательность», «другие детали не сообщаются», «подробности не раскрываются», «решение вступило в силу», «остаётся нерешённой», «уточняйте», «привлечёт внимание».

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
    # PR filler endings — LLM padding to hit char minimum
    "обогатит",
    "центр притяжения",
    "новая достопримечательность",
    "другие детали не сообщаются",
    "подробности не раскрываются",
    "остаётся нерешённой",
    "привлечёт внимание",
    "вступило в силу.",      # standalone finisher ("Решение вступило в силу.")
    "билеты и даты уточняйте",
    "время и дату уточняйте",
    "дату и время уточняйте",
    "уточните даты",
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
    prompt_name: str = "unknown",
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
            from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415
            record_call_from_response(
                response=response,
                stage="llm_rewrite",
                provider=provider_name.split("-", 1)[0],
                model=model,
                prompt_name=prompt_name,
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
    prompt_name: str = "unknown",
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
            system_prompt=prompt, prompt_name=prompt_name,
        )
    mapping = _call_provider_batch(
        DEEPSEEK_BASE_URL, os.environ.get("DEEPSEEK_API_KEY", ""), DEEPSEEK_MODEL,
        candidates, f"DeepSeek{label_suffix}", system_prompt=prompt, prompt_name=prompt_name,
    )
    missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
    if missing:
        time.sleep(1)
        mapping.update(_call_provider_batch(
            OPENAI_BASE_URL, os.environ.get("OPENAI_API_KEY", ""), OPENAI_MODEL,
            missing, f"OpenAI{label_suffix}", system_prompt=prompt, prompt_name=prompt_name,
        ))
    missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
    if missing:
        time.sleep(1)
        mapping.update(_call_provider_batch(
            GROQ_BASE_URL, os.environ.get("GROQ_API_KEY", ""), GROQ_FALLBACK_MODEL,
            missing, f"Groq{label_suffix}", batch_size=GROQ_BATCH_SIZE, system_prompt=prompt,
            prompt_name=prompt_name,
        ))
    return mapping


# ---------------------------------------------------------------------------
# Recurring-event date enrichment
# ---------------------------------------------------------------------------

_ORDINAL_MAP = {"first": 1, "1st": 1, "second": 2, "2nd": 2,
                "third": 3, "3rd": 3, "fourth": 4, "4th": 4, "last": -1}
_WEEKDAY_MAP = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                "friday": 4, "saturday": 5, "sunday": 6}
_RUSSIAN_WEEKDAY = ["понедельник", "вторник", "среда", "четверг",
                    "пятница", "суббота", "воскресенье"]
_RUSSIAN_WEEKDAY_ACCUS = ["понедельник", "вторник", "среду", "четверг",
                          "пятницу", "субботу", "воскресенье"]
_RUSSIAN_MONTHS = ["января", "февраля", "марта", "апреля", "мая", "июня",
                   "июля", "августа", "сентября", "октября", "ноября", "декабря"]

_RECURRING_PATTERN = re.compile(
    r'\b(first|1st|second|2nd|third|3rd|fourth|4th|last)\s+'
    r'(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+'
    r'(?:of\s+)?(?:each|every)\s+month\b',
    re.IGNORECASE,
)


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date | None:
    """Return the nth occurrence of weekday (0=Mon) in given year/month.

    n=-1 means last occurrence.
    """
    if n == -1:
        # Last occurrence: start from end of month
        last_day = calendar.monthrange(year, month)[1]
        d = date(year, month, last_day)
        while d.weekday() != weekday:
            d -= timedelta(days=1)
        return d
    count = 0
    d = date(year, month, 1)
    while d.month == month:
        if d.weekday() == weekday:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)
    return None


def _next_occurrence_from_pattern(text: str, from_date: date) -> str | None:
    """Detect 'third Saturday of each month' patterns and return a formatted date string."""
    m = _RECURRING_PATTERN.search(text)
    if not m:
        return None
    ordinal = _ORDINAL_MAP[m.group(1).lower()]
    weekday = _WEEKDAY_MAP[m.group(2).lower()]

    # Try current month first, then next month
    for delta_months in (0, 1):
        month = from_date.month + delta_months
        year = from_date.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        occurrence = _nth_weekday_of_month(year, month, weekday, ordinal)
        if occurrence and occurrence >= from_date:
            day_name = _RUSSIAN_WEEKDAY_ACCUS[occurrence.weekday()]
            month_name = _RUSSIAN_MONTHS[occurrence.month - 1]
            return f"{day_name}, {occurrence.day} {month_name} {occurrence.year}"
    return None


def _enrich_recurring_events(candidates: list[dict]) -> None:
    """For food_openings/culture candidates with recurring schedules,
    compute the next concrete date and inject it into the summary field
    so the LLM can use an exact date rather than 'каждую третью субботу'.
    """
    today = now_london().date()
    for c in candidates:
        if not isinstance(c, dict) or not c.get("include"):
            continue
        if c.get("category") not in {"food_openings", "culture_weekly"}:
            continue
        text = f"{c.get('title', '')} {c.get('summary', '')} {c.get('lead', '')}"
        next_date = _next_occurrence_from_pattern(text, today)
        if next_date and "NEXT_OCCURRENCE" not in str(c.get("summary", "")):
            c["summary"] = (str(c.get("summary") or "")).rstrip() + f" NEXT_OCCURRENCE: {next_date}."
            logger.debug("Enriched recurring event '%s' with next date: %s", c.get("title", ""), next_date)


def run_llm_rewrite(project_root: Path) -> StageResult:
    """Read candidates.json, fill Russian draft_lines for included candidates."""
    report_path = project_root / "data" / "state" / "llm_rewrite_report.json"
    candidates_path = project_root / "data" / "state" / "candidates.json"
    if not candidates_path.exists():
        logger.warning("candidates.json not found, skipping LLM rewrite.")
        write_json(
            report_path,
            {
                "pipeline_run_id": "",
                "run_at_london": now_london().isoformat(),
                "run_date_london": today_london(),
                "stage_status": "failed",
                "errors": ["Missing data/state/candidates.json."],
                "warnings": [],
            },
        )
        return StageResult(False, "Missing candidates.json.", report_path)

    payload = json.loads(candidates_path.read_text(encoding="utf-8"))
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])

    # Rewrite EVERY included candidate each run, not only ones missing a
    # draft_line. Caching draft_lines between runs meant a one-time fallback
    # to Gemini/Groq Llama (during an OpenAI timeout, say) would freeze a
    # weak draft_line into state forever — and later runs with healthy
    # OpenAI quietly skipped them. With ~50-80 candidates/day this costs
    # roughly $0.02/day on gpt-4o-mini but guarantees today's text actually
    # came from today's primary model.
    _enrich_recurring_events(candidates)

    to_rewrite = [
        c for c in candidates
        if isinstance(c, dict) and c.get("include")
        and str(c.get("category") or "") != "weather"  # handcrafted line, no LLM needed
    ]
    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    model_override = os.environ.get("LLM_MODEL", "").strip()
    base_url_override = os.environ.get("LLM_BASE_URL", "").strip()
    errors: list[str] = []
    warnings: list[str] = []
    applied = 0
    fixed = 0
    repaired = 0

    if provider_override == "none":
        logger.info("LLM_PROVIDER=none — skipping rewrite.")
        warnings.append("LLM_PROVIDER=none — rewrite stage skipped; writer/release gates will decide publishability.")
        write_json(
            report_path,
            {
                "pipeline_run_id": pipeline_run_id,
                "run_at_london": now_london().isoformat(),
                "run_date_london": today_london(),
                "stage_status": "degraded",
                "errors": errors,
                "warnings": warnings,
                "included_for_rewrite": len(to_rewrite),
                "applied": 0,
                "fixed": 0,
                "repaired": 0,
            },
        )
        return StageResult(True, "LLM rewrite disabled; continuing with writer/release gates.", report_path)

    if not to_rewrite:
        logger.info("LLM rewrite: all included candidates already have draft_lines.")
    else:
        logger.info("LLM rewrite: %d candidates need draft_lines.", len(to_rewrite))

        # Group by prompt type and call each group separately.
        # For prompts that need current date (business/events/diaspora — recurring
        # markets, date computation), inject TODAY_DATE header at the top.
        _DATE_AWARE_PROMPTS = {PROMPT_BUSINESS, PROMPT_EVENTS, PROMPT_DIASPORA_EVENTS}
        _today = today_london()

        def _with_date_header(prompt: str) -> str:
            if prompt in _DATE_AWARE_PROMPTS:
                return f"TODAY_DATE={_today}\n\n" + prompt
            return prompt

        groups: dict[str, list[dict]] = {}
        for c in to_rewrite:
            prompt = _CATEGORY_TO_PROMPT.get(str(c.get("category") or ""), PROMPT_CITY_NEWS)
            groups.setdefault(prompt, []).append(c)

        from news_digest.pipeline.prompts_meta import prompt_name_for  # noqa: PLC0415
        mapping: dict[str, str] = {}
        for prompt, group in groups.items():
            logger.info("LLM rewrite: calling group of %d candidates.", len(group))
            mapping.update(_call_with_fallback(
                group, _with_date_header(prompt), provider_override, base_url_override, model_override,
                prompt_name=prompt_name_for(prompt),
            ))

        run_iso = now_london().isoformat()
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
        fix_mapping = _call_with_fallback(fix_candidates, FIX_TRANSLATE_SYSTEM, provider_override, base_url_override, model_override, label_suffix="-fix", prompt_name="fix_translate")

        run_iso = now_london().isoformat()
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
    if to_repair:
        logger.info("LLM repair pass: %d weak draft_lines, rewriting editorially.", len(to_repair))
        repair_mapping = _call_with_fallback(
            to_repair,
            REPAIR_DRAFT_SYSTEM,
            provider_override,
            base_url_override,
            model_override,
            label_suffix="-repair",
            prompt_name="repair_draft",
        )

        run_iso = now_london().isoformat()
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

    missing_after = [
        c for c in to_rewrite
        if not str(c.get("draft_line") or "").strip()
    ]
    weak_after = [
        c for c in to_rewrite
        if str(c.get("draft_line") or "").strip() and _needs_quality_repair(c)
    ]
    successful = len(to_rewrite) - len(missing_after)
    if to_rewrite and successful < len(to_rewrite):
        warnings.append(
            f"LLM rewrite yield low after provider fallback: {successful}/{len(to_rewrite)} draft_lines written."
        )
    if weak_after:
        warnings.append(f"{len(weak_after)} draft_line(s) still look weak after repair.")

    from news_digest.pipeline.cost_tracker import dump_stage, snapshot, summarise  # noqa: PLC0415
    from news_digest.pipeline.prompts_meta import snapshot as prompts_snapshot  # noqa: PLC0415
    state_dir = project_root / "data" / "state"
    dump_stage(state_dir, "llm_rewrite")
    cost_summary = summarise(snapshot(stage="llm_rewrite"))
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not warnings else "degraded",
            "errors": errors,
            "warnings": warnings,
            "included_for_rewrite": len(to_rewrite),
            "applied": applied,
            "fixed": fixed,
            "repaired": repaired,
            "cost_summary": cost_summary,
            "prompt_versions": prompts_snapshot(),
            "missing_after": [
                {
                    "fingerprint": c.get("fingerprint"),
                    "title": c.get("title"),
                    "category": c.get("category"),
                    "primary_block": c.get("primary_block"),
                }
                for c in missing_after[:30]
            ],
            "weak_after": [
                {
                    "fingerprint": c.get("fingerprint"),
                    "title": c.get("title"),
                    "category": c.get("category"),
                    "primary_block": c.get("primary_block"),
                }
                for c in weak_after[:30]
            ],
        },
    )
    return StageResult(
        True,
        "LLM rewrite completed." if not warnings else "LLM rewrite completed with degraded yield/quality.",
        report_path,
    )
