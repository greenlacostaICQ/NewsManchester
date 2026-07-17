"""LLM rewrite stage — writes Russian draft_lines to candidates.json.

Default model route:
  1. OpenAI gpt-4o-mini           — source-language board judge + fact cards
  2. OpenAI gpt-4o-mini           — direct Russian draft from fact cards + evidence
  3. Legacy category rewrite      — mini-only full-evidence fallback for selected misses
  4. Lead-only gpt-4o fallback    — single visible lead item, never the broad pool

Required env vars (set in GitHub Actions Secrets or .env.local):
  OPENAI_API_KEY    — platform.openai.com
  DEEPSEEK_API_KEY  — platform.deepseek.com

Optional overrides:
  LLM_PROVIDER      — force "deepseek" | "openai" | "groq" | "none"
  LLM_MODEL         — override model name
  LLM_BASE_URL      — override API base URL
  LLM_API_KEY       — override API key (used with LLM_PROVIDER)
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
import hashlib
import html as html_lib
import json
import logging
import math
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

from news_digest.pipeline.common import PRIMARY_BLOCKS, now_london, pipeline_run_id_from, read_json, today_london, write_json
from news_digest.pipeline.model_routing import (
    DEEPSEEK_BASE_URL,
    DEEPSEEK_MODEL,
    GROQ_BASE_URL,
    GROQ_FALLBACK_MODEL,
    OPENAI_BASE_URL,
    OPENAI_REWRITE_MODEL,
    chat_completion_options_for_route,
    resolve_model_route,
    route_snapshot,
    sdk_retries_for_route,
)
from news_digest.pipeline.reader_value import reader_value_score
from news_digest.pipeline.story_intelligence import apply_story_intelligence, section_board_score
from news_digest.pipeline.weekend_inventory import is_weekend_inventory_candidate

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


OPENAI_MODEL = OPENAI_REWRITE_MODEL

ProviderMapping = dict[str, tuple[str, str, str]]
EnglishCardMapping = dict[str, tuple[dict[str, object], str, str]]
_ACTIVE_TRANSLATION_GLOSSARY: list[dict[str, str]] = []

REWRITE_SHORTLIST_VERSION = 2
# Per-block recall cap = how many candidates the DeepSeek board judge gets to
# see and rank. Editorial blocks (news/weekend/civic/professional) get a wide
# net so the model — not a deterministic rule cap — picks the best from real
# competition (last_24h had 112 candidates but the model only saw 12).
# Catalog blocks (tickets, transport) stay narrow: they are ranked by
# deterministic tier/lifecycle, the model adds nothing there.
REWRITE_SHORTLIST_CAPS_BY_BLOCK: dict[str, int] = {
    "transport": 99,          # rules: show every real tram/rail restriction
    "lead_story": 4,
    "today_focus": 8,
    "last_24h": 18,           # was 12 — biggest news pool, give the model recall
    "city_watch": 15,         # was 8
    "weekend_activities": 16,  # was 10
    "next_7_days": 14,        # was 8
    "ticket_radar": 8,        # catalog: deterministic sale/tier ranking
    "future_announcements": 14,  # was 4 — far too tight on 88 candidates
    "outside_gm_tickets": 4,  # catalog: A-tier notability ranking, not the model
    "russian_events": 12,     # was 6 — show all valid after dedupe
    "openings": 10,           # was 6
    "tech_business": 10,      # was 5
    "professional_events": 3,  # CV-approved personal rail; keep compact.
    "football": 6,            # was 3
}
REWRITE_SHORTLIST_DEFAULT_CAP = 8

# Source-language board judge gets a wider per-block board first. The final
# Russian writer is capped later, after DeepSeek has assigned scores/decisions.
# The board judge is the CHEAP model (DeepSeek-pro on compact cards), so a wider
# board buys recall cheaply; the expensive Russian writing stays capped at 42.
REWRITE_RANKING_BOARD_MAX = 90
# #9 Soft global ceiling on how many candidates we write in Russian per run.
# Items above the ceiling are not deleted — they stay as backup reserve.
# Raised 42→50 (2026-07-07): per-section floors starve on a busy morning when the
# global cap is too tight — Свежие/Еда shipped below their minimums most of the
# week because there was no board room left after events/tickets. The per-section
# floors do the shaping; the global cap only needs to be wide enough not to fight
# them.
REWRITE_TRANSLATION_BOARD_MAX = 50

# 0001-class reserve pre-write: sections that keep shipping under floor with
# `no_recoverable_reserve_with_facts` get a bounded rewrite quota for their
# recoverable reserve, so recovery has renderable lines (~8 extra lines/day).
REPAIR_DRAFT_MAX_ITEMS_DEFAULT = 8
TRANSLATION_MEMORY_VERSION = 1
TRANSLATION_MEMORY_MAX_ENTRIES = 2500
TRANSLATION_MEMORY_TTL_DAYS = 45
ENGLISH_CARD_MEMORY_VERSION = 1
ENGLISH_CARD_MEMORY_MAX_ENTRIES = 2500
ENGLISH_CARD_MEMORY_TTL_DAYS = 45
TOKEN_BUDGET_HISTORY_VERSION = 1
TOKEN_BUDGET_MIN_SAMPLES = 3
TOKEN_BUDGET_MARGIN = 1.35
TOKEN_BUDGET_RESPONSE_BUFFER = 256
TOKEN_HISTORY_MAX_SAMPLES = 240
_ACTIVE_TOKEN_BUDGET_HISTORY: dict[str, object] = {}
_REWRITE_BLOCK_FLOOR = 3
_REWRITE_BLOCK_FLOORS: dict[str, int] = {
    "last_24h": 7,
    "weekend_activities": 8,
}
_FRESH_GLOBAL_PROTECTED_TARGET = 12
TODAY_PRACTICAL_TRANSLATION_RESERVE = 3

_TODAY_PRACTICAL_ACTIVE_RE = re.compile(
    r"\b(?:m6|m60|m62|m56|a580|traffic|roadworks?|road\s+closed|motorway|"
    r"shut|closed|closure|delays?|queues?|congestion|diversion|warning|"
    r"unsafe|danger|appeal|witness|cctv|deadline|consultation|reopen|"
    r"inspection|cqc|ofsted|requires\s+improvement|inadequate|safeguarding|"
    r"school\s+closed|service\s+change|strike|airport|polls?\s+open|"
    r"polling\s+station|by-election)\b",
    re.IGNORECASE,
)
_TODAY_PRACTICAL_SOFT_RE = re.compile(
    r"\b(?:charity|fundrais|tribute|anniversary|opening|restaurant|deli|"
    r"concert|gig|ticket|festival|market|poll:|have\s+your\s+say|"
    r"changed\s+.*\s+forever|look\s+back)\b",
    re.IGNORECASE,
)
_TODAY_PRACTICAL_LOCAL_RE = re.compile(
    r"\b(?:greater manchester|manchester|m6|m60|m62|m56|a580|stockport|"
    r"tameside|trafford|salford|bolton|bury|oldham|rochdale|wigan|"
    r"prestwich|altrincham|wythenshawe|airport|metrolink|tfgm)\b",
    re.IGNORECASE,
)

_EVENT_DATE_TEXT_RE = re.compile(
    r"\b(?:\d{1,2}(?:st|nd|rd|th)?\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|"
    r"apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|"
    r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|"
    r"января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)|"
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?|"
    r"\d{4}-\d{2}-\d{2}|today|tomorrow|tonight|this\s+week|сегодня|завтра|послезавтра)\b",
    re.IGNORECASE,
)
_COST_GUARD_EVENT_BLOCKS = {
    "weekend_activities",
    "next_7_days",
    "future_announcements",
    "russian_events",
    "professional_events",
    "ticket_radar",
    "outside_gm_tickets",
}
_COST_GUARD_EVENT_CATEGORIES = {
    "culture_weekly",
    "food_openings",
    "russian_speaking_events",
    "diaspora_events",
    "professional_events",
    "venues_tickets",
}

_PROMPT_FOOTER = (
    '\nВерни ТОЛЬКО JSON-объект: {"items": [{"fingerprint": "...", "decision": "write|needs_enrichment|skip", '
    '"draft_line": "• ...", "missing_facts": []}]}\n'
    "Если не можешь написать без домыслов, decision=\"needs_enrichment\" или \"skip\" и missing_facts объясняет, чего нет. "
    "Пустая draft_line без decision/missing_facts запрещена. Никакого markdown, никаких пояснений — только JSON."
)

_ANTI_HALLUCINATION = (
    "АНТИ-ВЫМЫСЕЛ: каждое имя, должность, число, сумма £, дата, адрес в твоём тексте ДОЛЖНЫ буквально присутствовать "
    "в title/summary/lead/evidence_text. Нет в evidence — нет в draft_line. Запрещены конструкции «по словам экспертов», "
    "«как ожидается», «вероятно», «по предварительным данным» — если этого нет в evidence.\n\n"
    "ОПОРА ДЛЯ ТЕКСТА: если в item есть rewrite_packet, сначала используй его как карту фактов: кто, что, где, когда, "
    "что изменилось, почему сейчас, действие читателя. Но rewrite_packet НЕ заменяет источник: если для нормальной строки "
    "нужна деталь, бери её из title/summary/lead/evidence_text. Нельзя выбрасывать хороший факт только потому, что он не попал "
    "в rewrite_packet; нельзя добавлять факт, которого нет ни в packet, ни в evidence. Если rewrite_packet.allowed_numbers "
    "передан, используй только числа из этого списка или буквально видимые в evidence.\n\n"
    "ПО УМОЛЧАНИЮ — ПИШИ. Пустая draft_line — это КРАЙНЯЯ мера, а НЕ безопасный выбор. "
    "Если в evidence_text есть хотя бы один конкретный факт (имя, число, сумма £, дата, район/адрес, решение, исход) "
    "ИЛИ осмысленного текста ≥150 символов — ПУСТАЯ СТРОКА ЗАПРЕЩЕНА и decision=\"skip\" запрещён. Материала достаточно: "
    "извлеки главный факт + одну деталь и напиши пункт. Длинный evidence (как репортаж о суде на 1000+ символов) — это "
    "ОБЯЗАТЕЛЬНО к написанию, а не повод пропустить.\n"
    "Пустую строку (draft_line=\"\") возвращай ТОЛЬКО когда evidence реально пуст: короче ~120 символов осмысленного текста "
    "ИЛИ чистый тизер/анонс/paywall-stub без единого факта («One of Manchester's most iconic…», «Details to follow», "
    "«приобретено северное мероприятие» без названия/сумм/имён). "
    "ЗАПРЕЩЕНО возвращать пустоту потому, что текст длинный и лень его разбирать, или потому что «без домысла будет не идеально» — "
    "не домысливай факты, но обязательно напиши из того, что в evidence есть.\n\n"
    "НЕ ЗАВЫШАЙ СТАТУС СУБЪЕКТА: пиши роль человека ровно так, как в источнике. Ребёнок остаётся ребёнком, жертва — жертвой, "
    "местный житель — жителем, подозреваемый — подозреваемым (не «преступник», пока нет приговора). НЕ приписывай известность "
    "или экспертность («известный артист», «эксперт», «звезда»), если этого нет в evidence. Пример ошибки: 8-летнюю погибшую "
    "девочку, которая любила выступать, нельзя называть «известной артисткой» — она ребёнок, который любил выступать.\n\n"
    "ВРЕМЯ ОТНОСИТЕЛЬНО СЕГОДНЯ: сверяй даты с today_date. О дате в прошлом пиши в прошедшем/настоящем времени "
    "(«билеты уже в продаже», «прошёл 3 июня»), НЕ в будущем («поступят в продажу 3 июня», когда 3 июня уже прошло).\n\n"
    "ПОЯСНЯЙ НЕЗНАКОМОЕ: если в evidence есть короткое пояснение, кто или что это (напр. «popular Dutch DJ», должность, расшифровка "
    "аббревиатуры) — добавь его при ПЕРВОМ упоминании незнакомого имени/аббревиатуры (ANOTR — голландский диджей; UMIST — бывший "
    "технический университет). Бери пояснение ТОЛЬКО из evidence, не выдумывай. Читатель не должен гуглить, кто это.\n\n"
    "СОЧУВСТВЕННЫЕ ШТАМПЫ: не вставляй пустые соболезнования без новостной нагрузки («ушла слишком рано», «мысли с семьёй», "
    "«любили все, кто знал») — они не несут факта. НО если цитата принадлежит ключевой фигуре события ИЛИ сама по себе является "
    "новостью (официальное заявление, слова центрального участника) — оставь её. Режется пустая эмоция от периферийных людей, "
    "а не значимая цитата.\n\n"
)

ENGLISH_CARD_SYSTEM = """You are the English-first board judge for Greater Manchester AM Brief.
Your job is NOT to translate. Judge publishability, then build a compact English fact card and a readable English reader card from the supplied source evidence.

Use only title, summary, lead, evidence_text, event, entities, story_frame, source_label, dates and glossary_terms supplied in the JSON. Do not browse. Do not invent missing facts.
If glossary_terms are present, follow them for terminology and naming. Glossary terms do not add facts; they only control wording.

Return ONLY a JSON object in this shape: {"items": [...]}. The items array must contain objects with:
{
  "fingerprint": "...",
  "rubric": "transport|event|ticket|market|council_planning|hard_news|civic|business|football|human_interest|other",
  "fact_card": {
    "what_happened": "...",
    "where": "...",
    "when": "...",
    "who_affected": "...",
    "why_now": "...",
    "reader_value": "...",
    "reader_action": "...",
    "missing_facts": []
  },
  "reader_card": "One concise English digest bullet without the bullet marker.",
  "editorial_score": 0-100,
  "selection_hint": "publish|backup|weak",
  "board_decision": "publish|backup|reject",
  "board_confidence": 0.0-1.0,
  "suggested_block": "transport|today_focus|last_24h|next_7_days|weekend_activities|future_announcements|business|football|short_actions|other",
  "reason_codes": ["..."],
  "needs_gpt4o_escalation": false,
  "missing_facts": []
}

Decision rules:
- publish: local, fresh/useful, enough facts for a self-contained Telegram line.
- backup: potentially useful but weaker than the board, repetitive, or missing secondary facts.
- reject: PR-only, stale, non-Greater-Manchester, duplicate, expired, or too thin to write without inventing facts.
- Mark needs_gpt4o_escalation=true for borderline civic/legal/safety stories, protected lanes, low confidence, or lead-story contenders.

Rubric-specific requirements:
- transport: explain what is disrupted, where/which line, when, who is affected, and what the reader should do. If the affected section is not named, say that the operator/source has not named a specific section.
- event/ticket/market: explain who/what, city/venue, event date or sale date, genre/type if present, and why it is useful to plan.
- council_planning: explain the decision/proposal/status, location, next step/deadline if present, and local impact.
- hard_news: explain what happened, where, when if present, current police/court/emergency status, and public impact. Do not require a reader action.
- civic: explain who spoke/acted, where the Greater Manchester connection is, why it matters now. Do not require a practical action.
- human_interest: keep only if there is a new local fact beyond motivation/profile.

Important: missing date/place/action does not automatically mean weak. Judge against the rubric. A civic speech, tribute, death, or court update may be useful without a direct reader action.
Reader card style: factual, concise, source-faithful, 1-2 sentences, no hype, no "source says", no vague "this is important".
"""

FINAL_TRANSLATE_SYSTEM = """You are the Russian writer for Greater Manchester AM Brief.
Write the visible Telegram bullet directly in Russian from the supplied fact_card, source_evidence and structured fields.

The English reader_card is only a service note from the board judge. Do NOT translate it literally. Use it only to understand the chosen angle. The visible output must be natural Russian written from facts.
Preserve every date, place, line, venue, amount, status and uncertainty from english_fact_card/source_evidence/event/story_frame. Do not add facts from outside the supplied payload.
source_evidence is the safety net for selected items: use it to restore concrete dates, venues, amounts, roles and status, not to invent a new story angle.
If glossary_terms are present, follow them for terminology and naming. Glossary terms do not add facts; they only control wording.
If story_frame.case_frame is present, preserve the case stage, roles and unknowns exactly: do not turn "no charges" into "charged", do not turn a witness/father/driver into a suspect, and do not invent a verdict.

Return ONLY a JSON object:
{"items": [{"fingerprint": "...", "draft_line": "• ...", "subject": "...", "roles_preserved": true, "numbers_preserved": true, "untranslated_tokens": []}]}

Rules:
- Start every line with "• ".
- Write natural Russian, not literal machine translation and not a калька from English.
- Keep proper names, venue names, English transport line names and artist names as source names.
- If the English card says a fact is unspecified, keep that honesty in Russian.
- Do not add generic filler such as "это важный сигнал", "следите за обновлениями", "может привлечь внимание".
- Transport must answer: what is disrupted, who is affected, what to do.
- Events/tickets must answer: who/what, where, when, why it is useful to know.
- Hard news must start with what happened, not with "police are working at the scene".
"""

_LONG_FORMAT_RULES = (
    "ФОРМАТ: «• », Telegram HTML, без ссылок, без markdown. 250–450 символов, 2–3 коротких предложения.\n"
    "СТРУКТУРА:\n"
    "1) Первое предложение — главный факт: кто (имя/возраст/должность), что сделал/произошло, где конкретно (район/улица/площадка GM).\n"
    "2) Второе предложение — ключевая деталь из evidence_text: сумма, число, причина, имя пострадавшего/обвиняемого, дата вступления в силу, последствие. То, ради чего человек читает.\n"
    "3) Третье предложение (по желанию, если данные есть) — что это значит для жителя GM: с какой даты меняется, кого затронет, чем кончится.\n"
    "ЕСЛИ данных мало — пиши короче (250–300 символов), не добавляй воду. Лучше точный короткий пункт, чем раздутый пустой.\n\n"
)

PROMPT_TRANSPORT = (
    "Ты редактор Greater Manchester AM Brief. ВНИМАНИЕ: основной транспортный рендер выполняется детерминированным шаблоном "
    "в transport_fill. Ты вызываешься ТОЛЬКО для тех алёртов, где структурный экстрактор не справился — это значит формат "
    "необычный (driver shortage, tunnel inspection, signal box upgrade, special event closures и т.п.).\n\n"
    "ФОРМАТ: «• », Telegram HTML, без ссылок, максимум 180 символов, текст на русском кроме названий линий/операторов/остановок.\n\n"
    "ОПЕРАТОР — ВСЕГДА ИЗ TITLE, никогда «TfGM:».\n"
    "  TransPennine Express → «TransPennine Express:»\n"
    "  Northern → «Northern:»\n"
    "  Metrolink / Bury Line / Ashton Lines → «Metrolink:»\n"
    "  Bus Stop Closure / Road Closure с упоминанием bus services → «Автобусы:»\n\n"
    "ПИШИ ТОЛЬКО ПО ФАКТАМ ИЗ EVIDENCE. Если evidence короткое (< 100 символов) и в нём нет ни линии, ни улицы, ни остановки, "
    "ни маршрута — верни draft_line=\"\". Лучше пустая строка, чем выдуманный объезд или фейковая дата. "
    "Заглушка-шаблон по title подключится автоматически на стадии writer.\n\n"
    "🚫 НЕ ИНВЕРТИРУЙ СМЫСЛ. Утверждение и отрицание бери БУКВАЛЬНО из evidence. "
    "Если в тексте сказано «services will not be affected», «trams continue to run», «no disruption to services», "
    "«service running normally» — значит транспорт ХОДИТ, и писать «не работает / нет трамваев» ЗАПРЕЩЕНО. "
    "Пример (РЕАЛЬНАЯ ОШИБКА): evidence «improvement works at Prestwich; tram services will NOT be affected» → "
    "ПРАВИЛЬНО: «• Metrolink: на остановке Prestwich идут работы до 19 августа; движение трамваев не нарушено.» "
    "НЕВЕРНО: «трамваи не работают». «Не работает / нет трамваев» пиши ТОЛЬКО если в evidence явно "
    "«no trams / suspended / not operate / replaced by buses».\n\n"
    "ЕСЛИ ИНФОРМАЦИИ ХВАТАЕТ — структура: «• {Оператор}: {что не работает} {где} — {причина}; {альтернатива если есть}.»\n\n"
    "ПЕРЕВОДИ: «Disruption» → «сбой/задержки», «Minor Delay» → «небольшие задержки», «Improvement Works» → «ремонтные работы», "
    "«Replacement Bus» → «замещающий автобус». Английские слова кроме названий — ЗАПРЕЩЕНО.\n\n"
    "Примеры:\n"
    "«• Metrolink: 18 мая нет трамваев на Manchester Airport Line с 22:00 до закрытия — учения экстренных служб.»\n"
    "«• Northern: задержки до 30 мин на маршрутах через Manchester Victoria — сигнальная неисправность.»\n\n"
    "Без «проверьте заранее», без «следите за обновлениями»."
    + _PROMPT_FOOTER
)

PROMPT_CITY_NEWS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для городских новостей GM "
    "(полиция, советы, NHS, происшествия, мэрия, городское развитие, наука).\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ОБЯЗАТЕЛЬНАЯ СТРУКТУРА КАРТОЧКИ — три слоя в этом порядке:\n"
    "  1) ЛИД-ФАКТ (первое предложение) — кто/что/где/исход одной фразой. "
    "     Глагол действия, а не описания: «отклонил», «приговорил», «открывает», "
    "     «тушили», «исследование показало». Не цитата, не имя жительницы, не описание эмоций.\n"
    "  2) ДЕТАЛИ (1-2 предложения) — имена, цифры, район, дата, что/кто против/за.\n"
    "  3) ЧТО ДАЛЬШЕ или ПОЧЕМУ ВАЖНО (одна фраза) — апелляция, дата суда, "
    "     следующее голосование, рекомендация. Если в evidence этого нет — опусти.\n\n"
    "ОБЯЗАТЕЛЬНЫЕ ПОЛЯ ПО ТИПУ НОВОСТИ:\n"
    "  • Пожар/incident: продолжительность ИЛИ время начала + место (объект) + число расчётов + пострадавшие.\n"
    "    Пример лида: «🔥 Heywood: пожарные тушили историческую мельницу 6 часов. На месте 8 расчётов, никто не пострадал.»\n"
    "  • Планирование/совет: что решено + кто против/за + что дальше.\n"
    "    Пример лида: «🏗️ Trafford: советники отклонили склад Wain Estates в Carrington — план требовал вырубки 10 000+ деревьев.»\n"
    "  • Политический тупик/совет без руководства: объясни проблему простыми словами: кто не может договориться, какое решение заблокировано, сколько это может стоить или чем грозит, следующая дата.\n"
    "    Пример: «Oldham: совет снова не смог выбрать руководство после выборов; без мэра нельзя назначить лидера и комитеты. Если тупик не снимут, вмешательство комиссара может стоить до £1,200 в день; следующий раунд назначен на 15 июня.»\n"
    "  • Retail / локальная услуга: не пиши как property/listing. Объясни изменение для района: что закрывается, кто приходит вместо, где, когда, почему это заметно жителям.\n"
    "    Пример: «Hale Barns: Asda на Hale Barns Square закроется, а Waitrose планирует открыть магазин на этом месте осенью 2026 года. Для жителей это смена основного супермаркета в центре района; точная дата закрытия зависит от перехода аренды.»\n"
    "  • Change-of-use / бывшее здание: первым предложением пиши текущее решение, а старые даты только как фон.\n"
    "    Пример: «Standish: бывший Windsor House на Wigan Road переоборудуют в детский дом на 6 мест. Здание закрылось как care home в конце 2024 года; теперь Millennium Care использует его для размещения детей под опекой.»\n"
    "  • Наука/исследование: вывод исследования простыми словами одной фразой.\n"
    "    Пример лида: «🧠 Manchester: учёные UoM показали, что алкогольная зависимость нарушает способность мозга формировать новые ассоциации.»\n"
    "    Если в evidence нет конкретного вывода — лучше вернуть пустую draft_line, чем «исследование показало что-то про алкоголь».\n"
    "  • Дорожные работы/планирование: что и сколько стоит + что меняется + сроки.\n"
    "    Пример лида: «🚧 Rochdale: на Sudden junction начались работы за £5 млн — установка интеллектуальной системы сигналов и перекладка дороги под велодорожку.»\n\n"
    "ПИШИ ПО EVIDENCE_TEXT, НЕ ПЕРЕВОДОМ TITLE. В evidence обычно есть имена пострадавших, возраст, район, причина, дата суда — это и есть содержание для второго предложения.\n\n"
    "ПОЛИЦИЯ/СУДЫ: «• Moss Side: 34-летний Адриан Браун, отец троих детей, зарезан на улице в южном Манчестере. Полиция назвала имя жертвы, мать заявила «они забрали моего мальчика»; задержанных пока нет. Это второе ножевое убийство в районе за месяц.»\n"
    "СОВЕТ/МЭРИЯ: что меняется, с какой даты, для кого. «• Манчестер: совет утвердил план обязательной лицензии для съёмного жилья в 8 районах с 1 июля. Лицензия стоит £1175 на пять лет и распространяется на ~22 000 квартир в Cheetham, Levenshulme, Moss Side и Longsight. Цель — поднять стандарты после жалоб на сырость и плесень.»\n"
    "NHS/СЛУЖБЫ: конкретный факт + цифра + срок. Без PR-языка («важный шаг», «значимая инициатива») и без «появилось обновление», «заметный кейс».\n\n"
    "ТЕРМИНОЛОГИЯ — переводи точно, не буквально:\n"
    "«mural» = настенная роспись/мурал — НЕ «граффити» (граффити — несанкционированные надписи, это разные понятия).\n"
    "«climate-ready» / «climate-resilient countries» = страны, адаптированные к климатическим изменениям — НЕ «страны, готовые к изменению климата».\n"
    "«sponge park» = парк-губка (специально спроектированный для впитывания ливневой воды, защита от подтоплений) — объяснение включи в текст.\n"
    "«OnlyFans creator» = модель/автор OnlyFans — НЕ «создательница OnlyFans» (создатель платформы — другой человек).\n"
    "«National League → League Two» = из Национальной лиги (5-й дивизион, полупрофессиональный) в League Two (4-й дивизион, профессиональная EFL) — поясни кратко.\n\n"
    "ЗАПРЕЩЕНО НАЧИНАТЬ КАРТОЧКУ С:\n"
    "  - прямой цитаты в кавычках («Жительница была в ужасе...»);\n"
    "  - имени жительницы/местного жителя без должности или новости («Madeeha Sheikh заявила...»);\n"
    "  - описания эмоций («Многие были встревожены...», «Местные жители возмущены...»);\n"
    "  - фразы «местный житель», «одна из организаций», «туристическая достопримечательность».\n"
    "  Если в evidence_text нет ни одной конкретной детали — пиши короче, не выдумывай.\n"
    "ЗАПРЕЩЕНО ТАКЖЕ: «заранее проверьте», «привлечёт внимание», размытые формулировки."
    + _PROMPT_FOOTER
)

PROMPT_EVENTS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для событий и культуры GM.\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ЧИТАТЕЛЬ ХОЧЕТ ОДНОЗНАЧНО ПОНЯТЬ: что, когда, где. Цена и бронь — приятно, но НЕ обязательно.\n\n"
    "ОБЯЗАТЕЛЬНО в первой части draft_line:\n"
    "  1) Название события (или артист) и/или площадка.\n"
    "  2) КОНКРЕТНЫЙ временной маркер — выбирай по типу события:\n"
    "     - ТОЧЕЧНОЕ событие (один день): «в субботу 23 мая в 19:00».\n"
    "       Пример: «в субботу 23 мая в 19:00 в Deaf Institute — концерт ...».\n"
    "     - ФЕСТИВАЛЬ/ВЫСТАВКА 2+ дня: «идёт до 24 мая» или «с 15 по 24 мая».\n"
    "       Пример: «Manchester Jazz Festival идёт до 24 мая на First Street — ...».\n"
    "       Если ближайший конкретный концерт серии — назови дату: «headliner-концерты 22–23 мая».\n"
    "     - ПОВТОРЯЮЩЕЕСЯ событие (event.is_recurring=true): пиши «каждое воскресенье до сентября»,\n"
    "       «каждую субботу в 10:00», «работает постоянно», «работает по выходным».\n"
    "       Не пиши «с 5 апреля» когда сезон уже идёт — это сбивает читателя.\n"
    "       Пример: «Burnage RFC car boot — каждое воскресенье до конца августа, 6:00 для продавцов».\n"
    "  3) Место с borough/районом если есть в event.borough — «в Eccles», «в Stockport»,\n"
    "     «в центре Manchester».\n\n"
    "ОПЦИОНАЛЬНО (используй ТОЛЬКО если есть в evidence_text или event-полях): цена/free, ссылка-кто продаёт, "
    "артист/режиссёр, возрастной ценз, особенности.\n"
    "Если цены нет в evidence — НЕ выдумывай цифру. Просто опусти. То же для букинга.\n\n"
    "ПРИМЕРЫ ПО ТРЁМ ШАБЛОНАМ:\n"
    "«• В Deaf Institute 22 мая в 19:00 — концерт Lily Moore (rock). Билеты от Ticketmaster. (точечное)»\n"
    "«• Manchester Jazz Festival идёт до 24 мая на First Street + другие площадки города. Open-weekend (15–17 мая) "
    "бесплатный, headliner-концерты 22–23 мая платные через Creative Tourist. (фестиваль)»\n"
    "«• Burnage RFC car boot — каждое воскресенье 10 мая – 30 августа, 6:00 для продавцов и 8:00 для покупателей. "
    "Вход для покупателей бесплатный, для машин £15. (повторяющееся)»\n"
    "«• Alcotraz Penitentiary в центре Manchester — иммерсивный бар, работает постоянно. Билеты от £40, бронь на сайте. (постоянный)»\n\n"
    "МУЗЫКА: артист + площадка + дата + жанр/формат, только если жанр/формат есть в evidence; не выдумывай жанр.\n\n"
    "ПЕРЕВОДИ ВСЕ НАРИЦАТЕЛЬНЫЕ ТЕРМИНЫ — кроме имён собственных, названий площадок и устоявшихся культурных понятий (punk, jazz, hip-hop, opera):\n"
    "«booking fee» → «сбор при покупке», «under-30s» → «до 30 лет», «claimants» → «получатели пособий», "
    "«guided writing session» → «занятие с ведущим», «book club» → «книжный клуб», "
    "«soft refreshments» → «лёгкие угощения», «life drawing» → «рисование с натуры», "
    "«in residence» / «artist in residence» → «художник-резидент», "
    "«mild horror» → «мягкий хоррор», «flashes» → «световые вспышки», «toggle» → «настройка отключения».\n\n"
    "ЗАПРЕЩЕНО: «не пропустите», «обязательно посетите», «захватывающий», «уникальный», даты без конкретного места, "
    "CTA без конкретики («уточните даты», «билеты и даты уточняйте»), «с 22 мая» если событие постоянное "
    "или продолжается (всегда уточняй: постоянно / каждое воскресенье / до конца DD)."
    + _PROMPT_FOOTER
)

PROMPT_DIASPORA_EVENTS = (
    "Ты редактор дайджеста «Greater Manchester AM Brief». Пиши draft_line для русскоязычных концертов, стендапа и diaspora events в UK.\n\n"
    + _LONG_FORMAT_RULES
    + _ANTI_HALLUCINATION
    + "ОБЯЗАТЕЛЬНО: артист/комик или название события + город + площадка + конкретный временной маркер.\n"
    "Временной маркер — выбирай по типу:\n"
    "  - точечное: «23 октября в 19:00»;\n"
    "  - тур/несколько дат: «4 октября в Manchester, 5 октября в Liverpool»;\n"
    "  - повторяющееся (event.is_recurring=true): «каждую субботу в 20:00», «работает постоянно».\n"
    "Если есть фаза продаж — прямо скажи «билеты уже в продаже» или «продажи стартуют ...».\n"
    "Пиши как полезный early warning: человеку важно узнать заранее, а не в день концерта. London/Liverpool/Manchester можно оставлять в этом блоке, если событие русскоязычное или от diaspora-промоутера.\n\n"
    "«• Manchester Academy 24 марта в 19:00 — концерт Би-2 от EventCartel. На странице указаны двери в 19:00, curfew в 23:00 и билеты £69.75 плюс сбор. Если планируете идти, лучше брать заранее: такие туры редко получают много северных дат.»\n"
    "«• The Comedy Store Manchester 23 октября — русскоязычный стендап от UK Stand-Up Club. В описании указан конкретный комик, время начала и возрастное ограничение; билеты идут через Eventbrite/EventFirst.»\n\n"
    "ЗАПРЕЩЕНО: добавлять биографию артиста, песни, политический контекст или статус «крупный артист», если этого нет в evidence_text. "
    "«с 22 мая» если событие постоянное или повторяющееся — пиши «постоянно работает» или «каждую субботу»."
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
    + "ПРИНИМАЙ — верни заполненный draft_line. Лучше короткая карточка чем пустота. Если в evidence есть хотя бы одна конкретная деталь (имя игрока, цитата, дата матча, соперник, тренер, минута, счёт, сумма £) — пиши:\n"
    "• Результат матча: 1) счёт + соперник + турнир; 2) кто забил/удалён + минута; 3) что это значит.\n"
    "  «• Man City 2–1 Arsenal в АПЛ на «Этихаде». Голы Холанда (34') и де Брёйне (87') с пенальти; Сака отквитал на 70'. После 32 туров City — третьи, отрыв от Liverpool сократился до 4 очков.»\n"
    "• Трансфер: фигурант + сумма + контракт + откуда + ради чего + что значит для состава.\n"
    "  «• Man Utd подписал Кассерру из Sporting за £38 млн на пять лет. 23-летний португалец — опорный полузащитник, проведёт первый матч после паузы на сборные. Подписание закрывает дыру после ухода Каземиро.»\n"
    "• Анонс матча: соперник + турнир + дата + что на кону.\n"
    "  «• Man City — Real Madrid в 1/8 ЛЧ во вторник 18 февраля в 20:00 на «Этихаде». Первый матч в Манчестере; ответный 11 марта в Мадриде. Гвардиола без де Брёйне (травма).»\n"
    "• Реакция игрока / тренера с КОНКРЕТНОЙ цитатой или фактом из evidence — даже если она про настроение или будущее.\n"
    "  «• Мбёмо доволен переходом в Man Utd: «Это лучший выбор для моей карьеры — играть на «Олд Траффорде» под Аморимом». Француз подписал контракт на 5 лет, дебют в воскресенье против Crystal Palace.»\n"
    "• Травма/возвращение: имя + диагноз/тип + сроки + что значит для состава.\n"
    "  «• Холанд пропустит до месяца — растяжение икроножной мышцы на тренировке. Без него в атаке Гвардиолы остаются Доку и Хаалер. Пропустит Ливерпуль и матч ЛЧ.»\n"
    "• Назначения / уходы / контракты: фигурант + позиция + детали из evidence.\n\n"
    "ПРОПУСКАЙ — верни draft_line \"\":\n"
    "  - Title/evidence настолько пустые, что нет НИ ОДНОЙ конкретной детали (только заголовок-тизер без любых имён/цифр/дат).\n"
    "  - Чисто рекламные / пиар-карточки без новости: «купите мерч», «kit launch», «matchday programme», donate/award/community без имён.\n"
    "  - Under-18 / Under-21 / женские команды (отдельная лига).\n"
    "  - Фото-галереи и видео-нарезки без текста.\n\n"
    "ВАЖНО: бенч-комментарии, цитаты игроков, превью противника, обзоры формы — ВСЁ ЭТО ПОДХОДИТ если в evidence есть конкретика. Не отбраковывай только потому что это «не результат матча».\n"
    "Если сомневаешься — пиши короче (250 символов), но пиши, не возвращай пустоту."
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


BATCH_SIZE = 20      # default — used for OpenAI/DeepSeek
GROQ_BATCH_SIZE = 3  # Groq free tier TPM is tight once long prompts are included.

# Speed: batches and prompt-groups run concurrently ("fan out the API calls")
# so the stage's wall-clock is ~the slowest batch, not the sum. Two global
# throttles tame that fan-out:
#   • _API_SEMAPHORE   — caps *concurrent* in-flight calls (socket pressure).
#   • _API_RATE_LIMITER — caps the *rate* of new calls per minute.
# The semaphore alone is NOT enough: OpenAI throttles on requests/tokens-per-
# MINUTE, not on concurrency, so 8 batches firing in the same instant still
# tripped a 429 storm (2026-06-10: 131×429, ~820s of advertised backoff to
# rewrite just 47 items). The rate limiter spreads request *starts* so the
# burst stays under the tier ceiling. Tune via LLM_REWRITE_MAX_CONCURRENCY /
# LLM_REWRITE_MAX_RPM after watching a run.
try:
    _REWRITE_API_CONCURRENCY = max(1, int(os.environ.get("LLM_REWRITE_MAX_CONCURRENCY", "8") or "8"))
except ValueError:
    _REWRITE_API_CONCURRENCY = 8
_API_SEMAPHORE = threading.Semaphore(_REWRITE_API_CONCURRENCY)

try:
    _REWRITE_MAX_RPM = max(1.0, float(os.environ.get("LLM_REWRITE_MAX_RPM", "60") or "60"))
except ValueError:
    _REWRITE_MAX_RPM = 60.0


class _RateLimiter:
    """Token bucket that paces API request *starts* to <= max_rpm per minute.

    Shared across every category group/batch thread in the single
    llm-rewrite process, so the concurrent fan-out can no longer hit OpenAI
    in one synchronized burst. ``burst`` lets a few calls start warm before
    pacing kicks in. Thread-safe.
    """

    def __init__(self, max_rpm: float, burst: int) -> None:
        self._rate_per_sec = max_rpm / 60.0
        self._capacity = float(max(1, burst))
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity,
                    self._tokens + (now - self._updated) * self._rate_per_sec,
                )
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate_per_sec
            time.sleep(wait)


# Burst < concurrency on purpose: a few calls start warm, the rest are paced
# so the 8 batches never fire in the same instant.
_API_RATE_LIMITER = _RateLimiter(_REWRITE_MAX_RPM, burst=min(_REWRITE_API_CONCURRENCY, 4))

# The REAL ceiling on a Tier-1 org is tokens-per-minute (TPM), not requests:
# e.g. gpt-4o = 30k TPM. Pacing requests isn't enough — one big request can
# blow it. This limiter paces by estimated tokens (input + reserved output) so
# we stay just under the ceiling and never trip the 429 storm that killed the
# 2026-06-11..13 runs. Default leaves headroom under 30k; tune via env.
try:
    _REWRITE_MAX_TPM = max(2000.0, float(os.environ.get("LLM_REWRITE_MAX_TPM", "27000") or "27000"))
except ValueError:
    _REWRITE_MAX_TPM = 27000.0


class _TokenRateLimiter:
    """Token bucket measured in LLM tokens per minute (not requests).

    Each call acquires its estimated cost; the bucket refills at max_tpm/60 per
    second. Thread-safe. A request larger than the whole budget is clamped so
    it can still proceed instead of waiting forever.
    """

    def __init__(self, max_tpm: float) -> None:
        self._rate = max_tpm / 60.0
        self._capacity = float(max_tpm)
        self._available = self._capacity
        self._updated = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, cost: float) -> None:
        cost = max(0.0, min(float(cost), self._capacity))
        while True:
            with self._lock:
                now = time.monotonic()
                self._available = min(self._capacity, self._available + (now - self._updated) * self._rate)
                self._updated = now
                if self._available >= cost:
                    self._available -= cost
                    return
                wait = (cost - self._available) / self._rate
            time.sleep(max(wait, 0.0))


_API_TOKEN_LIMITER = _TokenRateLimiter(_REWRITE_MAX_TPM)


def _estimate_request_tokens(messages: list, max_tokens: int) -> int:
    """Rough token cost for the TPM limiter: input chars/4 + reserved output.
    Deliberately on the high side so we stay under the ceiling."""
    chars = sum(len(str(m.get("content") or "")) for m in messages)
    return chars // 4 + int(max_tokens)


def _env_int(name: str, default: int, *, minimum: int = 1, maximum: int = 100000) -> int:
    try:
        value = int(os.environ.get(name, str(default)) or default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _json_token_estimate(value: object) -> int:
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return max(1, len(text) // 4)


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * max(0.0, min(1.0, p))
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return ordered[lo]
    weight = rank - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight


def _safe_float(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _token_budget_history_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "llm_token_budget_history.json"


def _load_token_budget_history(project_root: Path) -> dict:
    path = _token_budget_history_path(project_root)
    if not path.exists():
        return {"schema_version": TOKEN_BUDGET_HISTORY_VERSION, "entries": {}}
    payload = read_json(path, {"schema_version": TOKEN_BUDGET_HISTORY_VERSION, "entries": {}})
    if not isinstance(payload, dict):
        return {"schema_version": TOKEN_BUDGET_HISTORY_VERSION, "entries": {}}
    if not isinstance(payload.get("entries"), dict):
        payload["entries"] = {}
    payload["schema_version"] = TOKEN_BUDGET_HISTORY_VERSION
    return payload


def _token_budget_key(prompt_name: str, model: str) -> str:
    return f"{prompt_name or 'unknown'}::{model or 'unknown'}"


def _max_tokens_for_batch(prompt_name: str, model: str, batch_len: int, default_max_tokens: int) -> tuple[int, str]:
    entries = _ACTIVE_TOKEN_BUDGET_HISTORY.get("entries") if isinstance(_ACTIVE_TOKEN_BUDGET_HISTORY, dict) else {}
    entry = entries.get(_token_budget_key(prompt_name, model)) if isinstance(entries, dict) else None
    if not isinstance(entry, dict):
        return default_max_tokens, "default_formula"
    samples = int(entry.get("sample_count") or 0)
    truncated = int(entry.get("truncated_responses") or 0)
    p95_per_item = _safe_float(entry.get("p95_completion_tokens_per_item"))
    if samples < TOKEN_BUDGET_MIN_SAMPLES or truncated or p95_per_item <= 0:
        return default_max_tokens, "history_warmup"
    recommended = int(math.ceil(p95_per_item * max(1, batch_len) * TOKEN_BUDGET_MARGIN + TOKEN_BUDGET_RESPONSE_BUFFER))
    # Do not make the first measured tightening too aggressive. One bad day
    # should not collapse a batch into truncation; acceptance requires zero
    # length-finish responses before future runs keep tightening.
    bounded = max(512, min(default_max_tokens, recommended))
    return bounded, "history_p95"


def _response_token_diagnostics(response: object, *, max_tokens: int, batch_len: int) -> dict[str, object]:
    usage = getattr(response, "usage", None)
    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0) if usage else 0
    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0) if usage else 0
    total_tokens = int(getattr(usage, "total_tokens", 0) or 0) if usage else prompt_tokens + completion_tokens
    finish_reasons: list[str] = []
    for choice in getattr(response, "choices", []) or []:
        reason = str(getattr(choice, "finish_reason", "") or "").strip()
        if reason:
            finish_reasons.append(reason)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "completion_tokens_per_item": round(completion_tokens / max(1, batch_len), 3) if completion_tokens else 0,
        "max_tokens": int(max_tokens),
        "finish_reasons": finish_reasons,
        "truncated": any(reason == "length" for reason in finish_reasons),
    }


def _token_aware_batches(
    candidates: list[dict],
    *,
    batch_size: int,
    system_prompt: str,
    item_builder: Callable[[list[dict]], list[dict]],
    today_date: str = "",
    max_items_env: str = "LLM_REWRITE_MAX_BATCH_ITEMS",
    token_budget_env: str = "LLM_REWRITE_BATCH_TOKEN_BUDGET",
    default_token_budget: int = 6200,
) -> list[list[dict]]:
    """Pack candidates by estimated input tokens, not by raw item count."""
    if not candidates:
        return []
    max_items = min(
        max(1, batch_size),
        _env_int(max_items_env, min(8, max(1, batch_size)), minimum=1, maximum=max(1, batch_size)),
    )
    token_budget = _env_int(token_budget_env, default_token_budget, minimum=1200, maximum=30000)
    prompt_overhead = max(1, len(system_prompt) // 4) + 300
    if today_date:
        prompt_overhead += 20
    batches: list[list[dict]] = []
    current: list[dict] = []
    current_tokens = prompt_overhead
    for candidate in candidates:
        item_tokens = _json_token_estimate(item_builder([candidate]))
        candidate["rewrite_estimated_item_tokens"] = item_tokens
        too_many_items = len(current) >= max_items
        too_many_tokens = current and current_tokens + item_tokens > token_budget
        if too_many_items or too_many_tokens:
            batches.append(current)
            current = []
            current_tokens = prompt_overhead
        current.append(candidate)
        current_tokens += item_tokens
    if current:
        batches.append(current)
    return batches


def _summarise_provider_batch_diagnostics(diagnostics: list[dict]) -> dict:
    if not diagnostics:
        return {
            "batch_count": 0,
            "sent": 0,
            "accepted": 0,
            "truncated_responses": 0,
            "timeout_errors": 0,
        }

    def _duration_stats(key: str, rows: list[dict]) -> dict[str, float]:
        values = [_safe_float(row.get(key)) for row in rows if _safe_float(row.get(key)) > 0]
        return {
            "total": round(sum(values), 3),
            "p50": round(_percentile(values, 0.50), 3),
            "p95": round(_percentile(values, 0.95), 3),
            "max": round(max(values) if values else 0.0, 3),
        }

    def _aggregate(rows: list[dict]) -> dict:
        sent = sum(int(row.get("sent") or 0) for row in rows)
        accepted = sum(int(row.get("accepted") or 0) for row in rows)
        returned = sum(int(row.get("returned_items") or 0) for row in rows)
        completion_per_item = [
            _safe_float(row.get("completion_tokens_per_item"))
            for row in rows
            if _safe_float(row.get("completion_tokens_per_item")) > 0
        ]
        max_tokens = [int(row.get("max_tokens") or 0) for row in rows if int(row.get("max_tokens") or 0) > 0]
        return {
            "batch_count": len(rows),
            "sent": sent,
            "returned_items": returned,
            "accepted": accepted,
            "accepted_rate": round(accepted / sent, 3) if sent else 0,
            "errors": sum(1 for row in rows if row.get("error")),
            "timeout_errors": sum(1 for row in rows if "timeout" in str(row.get("error") or "").lower()),
            "truncated_responses": sum(1 for row in rows if row.get("truncated")),
            "queue_wait_seconds": _duration_stats("queue_wait_seconds", rows),
            "api_seconds": _duration_stats("api_seconds", rows),
            "duration_seconds": _duration_stats("duration_seconds", rows),
            "completion_tokens_per_item": {
                "p50": round(_percentile(completion_per_item, 0.50), 3),
                "p95": round(_percentile(completion_per_item, 0.95), 3),
                "max": round(max(completion_per_item) if completion_per_item else 0.0, 3),
            },
            "max_tokens": {
                "p50": round(_percentile([float(v) for v in max_tokens], 0.50), 1),
                "p95": round(_percentile([float(v) for v in max_tokens], 0.95), 1),
                "max": max(max_tokens) if max_tokens else 0,
            },
        }

    by_prompt: dict[str, dict] = {}
    for prompt_name in sorted({str(row.get("prompt_name") or "unknown") for row in diagnostics}):
        by_prompt[prompt_name] = _aggregate([row for row in diagnostics if str(row.get("prompt_name") or "unknown") == prompt_name])
    by_provider: dict[str, dict] = {}
    for provider in sorted({str(row.get("provider") or "unknown") for row in diagnostics}):
        by_provider[provider] = _aggregate([row for row in diagnostics if str(row.get("provider") or "unknown") == provider])
    summary = _aggregate(diagnostics)
    summary["by_prompt"] = by_prompt
    summary["by_provider"] = by_provider
    return summary


def _update_token_budget_history(project_root: Path, diagnostics: list[dict]) -> dict:
    history = _load_token_budget_history(project_root)
    entries = history.setdefault("entries", {})
    if not isinstance(entries, dict):
        history["entries"] = {}
        entries = history["entries"]
    updated_keys: set[str] = set()
    for row in diagnostics:
        if row.get("error"):
            continue
        sent = int(row.get("sent") or 0)
        completion_tokens = int(row.get("completion_tokens") or 0)
        if sent <= 0 or completion_tokens <= 0:
            continue
        key = _token_budget_key(str(row.get("prompt_name") or "unknown"), str(row.get("model") or "unknown"))
        entry = entries.setdefault(key, {"samples_completion_tokens_per_item": []})
        samples = entry.setdefault("samples_completion_tokens_per_item", [])
        if not isinstance(samples, list):
            samples = []
            entry["samples_completion_tokens_per_item"] = samples
        truncated_samples = entry.setdefault("samples_truncated", [])
        if not isinstance(truncated_samples, list):
            truncated_samples = []
            entry["samples_truncated"] = truncated_samples
        samples.append(round(completion_tokens / sent, 3))
        truncated_samples.append(1 if row.get("truncated") else 0)
        if len(samples) > TOKEN_HISTORY_MAX_SAMPLES:
            del samples[:-TOKEN_HISTORY_MAX_SAMPLES]
        if len(truncated_samples) > TOKEN_HISTORY_MAX_SAMPLES:
            del truncated_samples[:-TOKEN_HISTORY_MAX_SAMPLES]
        entry["prompt_name"] = str(row.get("prompt_name") or "unknown")
        entry["model"] = str(row.get("model") or "unknown")
        entry["sample_count"] = len(samples)
        entry["p50_completion_tokens_per_item"] = round(_percentile([float(v) for v in samples], 0.50), 3)
        entry["p95_completion_tokens_per_item"] = round(_percentile([float(v) for v in samples], 0.95), 3)
        entry["max_completion_tokens_per_item"] = round(max(float(v) for v in samples), 3)
        entry["truncated_responses"] = sum(int(v or 0) for v in truncated_samples)
        entry["last_seen_london"] = now_london().isoformat()
        updated_keys.add(key)
    history["schema_version"] = TOKEN_BUDGET_HISTORY_VERSION
    history["updated_at_london"] = now_london().isoformat()
    write_json(_token_budget_history_path(project_root), history)
    return {
        "path": str(_token_budget_history_path(project_root).relative_to(project_root)),
        "updated_prompts": len(updated_keys),
        "entries": len(entries),
        "min_samples_before_tightening": TOKEN_BUDGET_MIN_SAMPLES,
        "margin": TOKEN_BUDGET_MARGIN,
    }


def _jittered_sleep(base: float) -> None:
    """Sleep ``base`` + up to ``base`` random jitter so batches that hit a
    429/timeout in the same instant don't retry in lockstep."""
    time.sleep(base + random.uniform(0.0, base))

FIX_TRANSLATE_SYSTEM = """Переведи строку новостного дайджеста на русский язык.
Названия людей, мест, брендов, компаний, IT-терминов оставляй по-английски.
Строка начинается с «• » и не превышает 280 символов.
Верни ТОЛЬКО JSON-объект: {"items": [{"fingerprint": "...", "draft_line": "• ..."}]}
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

ПО ТИПАМ:
- council deadlock / council vote: объясни, что заблокировано, кто/какой орган не договорился, стоимость/последствие и следующую дату.
- retail closure / takeover: что закрывается, кто заменяет, где, когда, что меняется для жителей района.
- change-of-use / former building: текущее решение первым; старую дату закрытия давай только как фон, не как новость.
- transport: не оставляй «небольшие задержки» без линии/участка, если они есть в title/summary; если участок не указан источником, прямо скажи «TfGM не уточнил участок».

ЖЁСТКОЕ ПРАВИЛО ДЛИНЫ (long-format категории): минимум ДВА самостоятельных предложения и ≥150 символов. Первое — главный факт; второе — конкретная деталь из evidence_text (имя/сумма/дата суда/адрес/последствие). НЕ упаковывай всё в одно длинное предложение — раздели на два. Бери вторую деталь из evidence_text, она почти всегда там есть.
ЕСЛИ в evidence_text реально только заголовок без второй фактуры — НЕ выдумывай и НЕ добавляй заглушку; верни draft_line пустой строкой "", такой пункт честно не пойдёт в выпуск (это пробел обогащения, а не слабый текст).
ЕСЛИ данных мало, но они есть — пиши 250–300 символов с реальной фактурой. Лучше точный короткий пункт, чем раздутый пустой.

ЗАПРЕЩЕНЫ окончания-заглушки: «обогатит», «центр притяжения», «новая достопримечательность», «другие детали не сообщаются», «подробности не раскрываются», «решение вступило в силу», «остаётся нерешённой», «уточняйте», «привлечёт внимание».

Верни ТОЛЬКО JSON-объект: {"items": [{"fingerprint": "...", "draft_line": "• ..."}]}
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


def _writer_quality_errors(candidate: dict, line: str) -> list[str]:
    from news_digest.pipeline.writer import _draft_line_quality_errors  # noqa: PLC0415

    return _draft_line_quality_errors(candidate, line)


def _skip_llm_for_manual_review(candidate: dict) -> bool:
    """Do not spend model calls on items the writer will hold anyway."""
    if candidate.get("include"):
        return False
    return (
        str(candidate.get("editorial_status") or "") == "borderline"
        and str(candidate.get("manual_override") or "") != "force_include"
    )


_MARKET_EVENT_RE = re.compile(
    r"\b(?:car\s*boot|market|markets|makers\s+market|farmer'?s\s+market|"
    r"farmers\s+market|flea\s+market|vintage\s+market|food\s+market|flower\s+festival)\b",
    re.IGNORECASE,
)


def _is_market_or_recurring_event(candidate: dict) -> bool:
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "source_label")
    )
    if str(protected.get("lane") or "") in {"weekend_market", "recurring_market"}:
        return True
    if str(contract.get("event_shape") or candidate.get("event_shape") or "") == "recurring" and _MARKET_EVENT_RE.search(text):
        return True
    return bool(event.get("is_recurring") and _MARKET_EVENT_RE.search(text))


def _uses_deterministic_writer(candidate: dict) -> bool:
    """Skip LLM where writer has a safer structured template.

    Ticket cards are high-volume and already carry venue/date/genre fields
    from source APIs. Sending all of them through rewrite caused the 20-minute
    failure mode: OpenAI timed out, then weaker fallback models wrote visible
    copy. Markets/recurring protected events use occurrence-first templates,
    which are safer than free-form prose for "this weekend" planning.
    """
    category = str(candidate.get("category") or "")
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    text = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text"))
    has_title = bool(str(candidate.get("title") or "").strip())
    has_event_date = bool(
        str(event.get("date_start") or event.get("start_date") or event.get("date") or "").strip()
        or re.search(r"\bevent_date\s*=\s*\d{4}-\d{2}-\d{2}", text, re.IGNORECASE)
    )
    has_venue = bool(
        str(event.get("venue") or event.get("location") or candidate.get("venue") or "").strip()
        or re.search(r"\b(?:venue|location)\s*=", text, re.IGNORECASE)
    )
    if category == "venues_tickets" and has_title and has_event_date and has_venue:
        return True
    if (
        category in {"culture_weekly", "food_openings", "russian_speaking_events", "diaspora_events"}
        and _is_market_or_recurring_event(candidate)
        and bool(event.get("is_event"))
        and has_title
        and (has_event_date or bool(event.get("is_recurring")))
    ):
        return True
    return False


def _append_reason(candidate: dict, note: str) -> None:
    existing = str(candidate.get("reason") or "").strip()
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note


def _set_digest_selection_verdict(candidate: dict, verdict: str, reason: str) -> None:
    """Every candidate must leave rewrite selection with a product verdict.

    This deliberately mirrors product language, not internal pipeline state:
    selected / reserve / needs_enrichment / drop. A "pending" limbo means the
    next stage cannot explain why a useful event vanished.
    """
    candidate["digest_selection_verdict"] = verdict
    candidate["digest_selection_reason"] = reason
    if reason:
        _append_reason(candidate, f"Selection: {reason}")


def _needs_selection_enrichment(candidate: dict) -> bool:
    if _has_nothing_to_write_from(candidate):
        return True
    missing = candidate.get("english_missing_facts")
    if isinstance(missing, list) and missing:
        return True
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    event_like = block in {
        "weekend_activities",
        "next_7_days",
        "future_announcements",
        "russian_events",
        "professional_events",
        "openings",
    } or category in {"culture_weekly", "russian_speaking_events", "diaspora_events", "professional_events"}
    if event_like and event.get("is_event"):
        has_date = bool(str(event.get("date_start") or event.get("date") or "").strip())
        has_place_or_booking = bool(str(event.get("venue") or event.get("booking_url") or candidate.get("source_url") or "").strip())
        return not (has_date and has_place_or_booking)
    # News/civic card with only a headline-length blurb: the full article was
    # not pulled, so it cannot be written well. This is "the news is weak →
    # enrich and rewrite", not just "nothing at all" — enrichment must fire on
    # thin material, not only on empty material.
    news_like = block in {"last_24h", "city_watch", "today_focus", "tech_business"} or category in {
        "media_layer", "gmp", "council", "public_services", "tech_business",
    }
    if news_like:
        text = " ".join(str(candidate.get(f) or "") for f in ("evidence_text", "summary", "lead")).strip()
        return len(text) < 160
    return False


def _professional_cv_approved(candidate: dict) -> bool:
    if str(candidate.get("primary_block") or "") != "professional_events":
        return False
    llm = candidate.get("professional_llm_match") if isinstance(candidate.get("professional_llm_match"), dict) else {}
    return str(llm.get("fit") or candidate.get("score_verdict") or "").strip().lower() in {"go", "consider"}


def _professional_cv_priority(candidate: dict) -> float:
    llm = candidate.get("professional_llm_match") if isinstance(candidate.get("professional_llm_match"), dict) else {}
    verdict = str(llm.get("fit") or candidate.get("score_verdict") or "").strip().lower()
    try:
        score = float(candidate.get("score_value") if str(candidate.get("score_source") or "") == "model" else llm.get("score"))
    except (TypeError, ValueError):
        score = 0.0
    if verdict == "go":
        return 1000.0 + score
    if verdict == "consider":
        return 500.0 + score
    if verdict == "skip":
        return -1000.0
    return -500.0


def _rewrite_shortlist_priority(candidate: dict) -> tuple[float, float, float, str]:
    apply_story_intelligence(candidate)
    lead_bonus = 1000.0 if candidate.get("is_lead") else 0.0
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    protected_bonus = 250.0 if protected.get("protected") else 0.0
    if str(candidate.get("primary_block") or "") == "professional_events":
        return (
            _professional_cv_priority(candidate),
            float(section_board_score(candidate)),
            float(reader_value_score({**candidate, "included": True})),
            str(candidate.get("title") or ""),
        )
    board_score = candidate.get("english_editorial_score")
    try:
        board_score_bonus = float(board_score)
    except (TypeError, ValueError):
        board_score_bonus = 0.0
    decision = str(candidate.get("english_board_decision") or candidate.get("english_selection_hint") or "").lower()
    if decision == "publish":
        board_score_bonus += 25.0
    elif decision == "backup":
        board_score_bonus -= 40.0
    elif decision == "reject":
        board_score_bonus -= 200.0
    return (
        lead_bonus + protected_bonus + board_score_bonus,
        float(section_board_score(candidate)),
        float(reader_value_score({**candidate, "included": True})),
        str(candidate.get("title") or ""),
    )


def _must_translate_before_cap(candidate: dict) -> bool:
    apply_story_intelligence(candidate)
    if candidate.get("is_lead"):
        return True
    if candidate.get("today_practical_translation_reserve"):
        return True
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    tier = str(contract.get("publish_tier") or candidate.get("publish_tier") or "")
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    if block in {"transport", "today_focus"}:
        return True
    if block == "weekend_activities" and _is_actionable_weekend_candidate(candidate):
        return True
    # Protected quota for official/community venue events: a dated card with a
    # venue must not be cut by the soft translation-board cap (owner 2026-06-15:
    # next_7 afisha недобирается, official venue events терялись из-за soft max).
    if block in {"next_7_days", "future_announcements"}:
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        event_day = _candidate_event_day(candidate)
        if event_day and event_day >= date.fromisoformat(today_london()) and str(event.get("venue") or "").strip():
            return True
    if category in {"media_layer", "council", "gmp", "public_services", "city_news"} and tier in {"must_include", "strong"}:
        return True
    if protected.get("protected") and category != "venues_tickets":
        return True
    return False


def _today_practical_translation_candidate(candidate: dict) -> bool:
    apply_story_intelligence(candidate)
    if candidate.get("reject_reasons") or candidate.get("validation_errors"):
        return False
    block = str(candidate.get("primary_block") or "")
    if block not in {"last_24h", "city_watch"}:
        return False
    category = str(candidate.get("category") or "")
    if category not in {"media_layer", "gmp", "council", "public_services", "city_news"}:
        return False
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if event.get("is_event"):
        return False
    text = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle", "source_label")
    )
    if _TODAY_PRACTICAL_SOFT_RE.search(text):
        return False
    return bool(_TODAY_PRACTICAL_ACTIVE_RE.search(text) and _TODAY_PRACTICAL_LOCAL_RE.search(text))


def _mark_today_practical_translation_reserve(candidates: list[dict]) -> list[dict[str, object]]:
    ranked = sorted(
        (candidate for candidate in candidates if _today_practical_translation_candidate(candidate)),
        key=_rewrite_shortlist_priority,
        reverse=True,
    )
    protected: list[dict[str, object]] = []
    for candidate in ranked[:TODAY_PRACTICAL_TRANSLATION_RESERVE]:
        candidate["today_practical_translation_reserve"] = True
        protected.append(
            {
                "fingerprint": candidate.get("fingerprint") or "",
                "title": candidate.get("title") or "",
                "source_label": candidate.get("source_label") or "",
                "primary_block": candidate.get("primary_block") or "",
                "category": candidate.get("category") or "",
                "score": reader_value_score({**candidate, "included": True}),
                "reason": "Protected practical Today Focus reserve before Russian writing.",
            }
        )
    return protected


def _candidate_event_day(candidate: dict) -> date | None:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    for value in (event.get("date_start"), event.get("date")):
        text = str(value or "").strip()
        if not text:
            continue
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            continue
    return None


def _is_actionable_weekend_candidate(candidate: dict) -> bool:
    # Weekend-inventory items (markets/fairs/festivals in the weekend block) are
    # always actionable, but so is any card whose concrete event date lands on
    # the upcoming weekend, or which recurs every weekend. The caller already
    # gates on block == "weekend_activities", so this must judge the date, not
    # re-check the block (which would drop dated weekend events).
    if is_weekend_inventory_candidate(candidate):
        return True
    today = date.fromisoformat(today_london())
    days_to_sat = (5 - today.weekday()) % 7
    start = today + timedelta(days=days_to_sat)
    end = start + timedelta(days=1)
    event_day = _candidate_event_day(candidate)
    if event_day and start <= event_day <= end:
        return True
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text")).lower()
    return bool(re.search(r"\b(?:every|weekly|saturdays?|sundays?|weekend|каждую\s+субботу|каждое\s+воскресенье)\b", blob))


def _apply_rewrite_shortlist(candidates: list[dict], to_rewrite: list[dict]) -> tuple[list[dict], dict[str, object]]:
    """Select the English-scored candidates worth paying to translate.

    This is the cutover from "translate the broad included pool" to
    "judge/score first, translate only the publishable shortlist". Items
    held back are not deleted: they are marked as backup candidates so the
    release report and backup_pool can explain what was not translated.
    """
    today_practical_reserve = _mark_today_practical_translation_reserve(to_rewrite)
    groups: dict[str, list[dict]] = {}
    for candidate in to_rewrite:
        block = str(candidate.get("primary_block") or "")
        groups.setdefault(block, []).append(candidate)

    selected_ids: set[int] = set()
    held: list[dict[str, object]] = []
    uncapped_selected: list[dict[str, object]] = []
    caps: dict[str, int] = {}
    for block, group in groups.items():
        cap = REWRITE_SHORTLIST_CAPS_BY_BLOCK.get(block, REWRITE_SHORTLIST_DEFAULT_CAP)
        caps[block] = cap
        ranked = sorted(group, key=_rewrite_shortlist_priority, reverse=True)
        protected_group = [candidate for candidate in ranked if _must_translate_before_cap(candidate)]
        protected_ids = {id(item) for item in protected_group}
        normal_group = [candidate for candidate in ranked if id(candidate) not in protected_ids]
        for candidate in protected_group:
            selected_ids.add(id(candidate))
            candidate["rewrite_shortlist_status"] = "selected_uncapped"
            _set_digest_selection_verdict(
                candidate,
                "selected",
                "Protected item selected before size limits.",
            )
            uncapped_selected.append(
                {
                    "fingerprint": candidate.get("fingerprint") or "",
                    "title": candidate.get("title") or "",
                    "source_label": candidate.get("source_label") or "",
                    "category": candidate.get("category") or "",
                    "primary_block": block,
                    "reason": "Protected hard news / transport / today-focus item; not capped before rewrite.",
                }
            )
        for candidate in normal_group[:cap]:
            selected_ids.add(id(candidate))
            candidate["rewrite_shortlist_status"] = "selected"
            _set_digest_selection_verdict(
                candidate,
                "selected",
                f"Selected in {block or 'unknown'} shortlist before writing.",
            )
        for candidate in normal_group[cap:]:
            candidate["include"] = False
            candidate["backup_candidate"] = True
            candidate["backup_pool_only"] = True
            candidate["public_reserve"] = False
            candidate["rewrite_shortlist_status"] = "backup_before_rewrite"
            candidate["rewrite_shortlist_reason"] = f"Outside pre-rewrite shortlist for {block or 'unknown'}."
            verdict = "needs_enrichment" if _needs_selection_enrichment(candidate) else "reserve"
            _set_digest_selection_verdict(candidate, verdict, candidate["rewrite_shortlist_reason"])
            from news_digest.pipeline.plan_digest import _backup_eligible  # noqa: PLC0415

            candidate["recoverable_reserve"] = _backup_eligible(candidate)[0]  # зеркало: пригодность в запасные плана
            held.append(
                {
                    "fingerprint": candidate.get("fingerprint") or "",
                    "title": candidate.get("title") or "",
                    "source_label": candidate.get("source_label") or "",
                    "category": candidate.get("category") or "",
                    "primary_block": block,
                    "section_board_score": candidate.get("section_board_score"),
                    "reader_value_score": reader_value_score({**candidate, "included": True}),
                    "reason": candidate["rewrite_shortlist_reason"],
                }
            )

    selected = [candidate for candidate in to_rewrite if id(candidate) in selected_ids]

    # First ceiling: a wider source-language ranking board. DeepSeek should
    # see the realistic competition inside each block before the final Russian
    # writing cut. Overflow is demoted to backup reserve, not deleted.
    board_overflow = 0
    if len(selected) > REWRITE_RANKING_BOARD_MAX:
        def _never_drop(c: dict) -> bool:
            return (
                bool(c.get("is_lead"))
                or str(c.get("primary_block") or "") in {"transport", "today_focus"}
                or bool(c.get("today_practical_translation_reserve"))
                or is_weekend_inventory_candidate(c)
                or _professional_cv_approved(c)
            )

        keep_ids: set[int] = {id(c) for c in selected if _never_drop(c)}
        # Fresh hard-news was still allowed to fall out of the global board
        # after the per-block cap. Keep the strongest Fresh items above the
        # generic 45-item ceiling so a crowded event/ticket morning cannot
        # starve actual city news.
        fresh_protected = [
            c for c in selected
            if str(c.get("primary_block") or "") == "last_24h"
            and _must_translate_before_cap(c)
        ]
        for c in sorted(fresh_protected, key=_rewrite_shortlist_priority, reverse=True)[:_FRESH_GLOBAL_PROTECTED_TARGET]:
            keep_ids.add(id(c))
        # Per-block floor: keep the top _REWRITE_BLOCK_FLOOR of each block.
        by_block: dict[str, list[dict]] = {}
        for c in selected:
            by_block.setdefault(str(c.get("primary_block") or ""), []).append(c)
        for _block, items in by_block.items():
            floor = _REWRITE_BLOCK_FLOORS.get(_block, _REWRITE_BLOCK_FLOOR)
            for c in sorted(items, key=_rewrite_shortlist_priority, reverse=True)[:floor]:
                keep_ids.add(id(c))
        # Fill the rest of the budget by global priority.
        room = REWRITE_RANKING_BOARD_MAX - len(keep_ids)
        if room > 0:
            rest = sorted(
                (c for c in selected if id(c) not in keep_ids),
                key=_rewrite_shortlist_priority,
                reverse=True,
            )
            keep_ids.update(id(c) for c in rest[:room])
        for candidate in selected:
            if id(candidate) in keep_ids:
                continue
            candidate["include"] = False
            candidate["backup_candidate"] = True
            candidate["backup_pool_only"] = True
            candidate["public_reserve"] = False
            candidate["rewrite_shortlist_status"] = "backup_ranking_board_cap"
            candidate["rewrite_shortlist_reason"] = (
                f"Outside DeepSeek ranking board (soft max {REWRITE_RANKING_BOARD_MAX})."
            )
            verdict = "needs_enrichment" if _needs_selection_enrichment(candidate) else "reserve"
            _set_digest_selection_verdict(candidate, verdict, candidate["rewrite_shortlist_reason"])
            from news_digest.pipeline.plan_digest import _backup_eligible  # noqa: PLC0415

            candidate["recoverable_reserve"] = _backup_eligible(candidate)[0]  # зеркало: пригодность в запасные плана
            selected_ids.discard(id(candidate))
            board_overflow += 1
            held.append(
                {
                    "fingerprint": candidate.get("fingerprint") or "",
                    "title": candidate.get("title") or "",
                    "source_label": candidate.get("source_label") or "",
                    "category": candidate.get("category") or "",
                    "primary_block": str(candidate.get("primary_block") or ""),
                    "score_value": candidate.get("score_value"),
                    "score_source": candidate.get("score_source") or "not model scored",
                    "score_scope": candidate.get("score_scope"),
                    "score_verdict": candidate.get("score_verdict"),
                    "reason": candidate["rewrite_shortlist_reason"],
                }
            )
        selected = [candidate for candidate in to_rewrite if id(candidate) in selected_ids]

    return selected, {
        "schema_version": REWRITE_SHORTLIST_VERSION,
        "enabled": True,
        "input_candidates": len(to_rewrite),
        "selected_for_rewrite": len(selected),
        "held_for_backup": len(held),
        "board_overflow": board_overflow,
        "board_max": REWRITE_RANKING_BOARD_MAX,
        "final_russian_board_max": REWRITE_TRANSLATION_BOARD_MAX,
        "caps_by_block": caps,
        "uncapped_selected": len(uncapped_selected),
        "uncapped_examples": uncapped_selected[:20],
        "today_practical_translation_reserve": today_practical_reserve,
        "held_examples": held[:40],
    }


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

# Hard-news categories where a skip on a story that carries full facts is
# never acceptable. On 2026-06-03 8 of 12 missing draft_lines were these,
# with 1100–1500 chars of evidence — the model just chose decision=skip in a
# batch. The forcing pass re-runs them one at a time.
_FORCE_WRITE_CATEGORIES = {
    "media_layer", "gmp", "council", "public_services", "tech_business", "city_news", "football",
}

# Single-item forcing prompt: skip is removed as an option. The batch prompt
# lets the model lazily return decision=skip; a one-item call with this prompt
# cannot. Used only for rich-evidence (≥400 chars) hard-news misses.
FORCE_WRITE_SYSTEM = """Ты редактор Greater Manchester AM Brief. Тебе дают ОДНУ новость с полным текстом фактов (evidence_text). Твоя ЕДИНСТВЕННАЯ задача — написать русский пункт draft_line по этим фактам.

ВАЖНО: в evidence_text фактов ДОСТАТОЧНО — это не тизер. «skip», «needs_enrichment» и пустая строка ЗАПРЕЩЕНЫ. Ты обязан написать пункт.

ФОРМАТ: строка начинается с «• », 150–400 символов, 2–3 коротких предложения, без ссылок и markdown.
СТРУКТУРА:
1) Главный факт: кто/что/где конкретно (район/улица GM), глагол действия.
2) Ключевая деталь из evidence_text: имя, сумма £, число, причина, дата суда, последствие.
3) (если есть) что это значит для жителя GM.

АНТИ-ВЫМЫСЕЛ: каждое имя, число, сумма, дата, адрес должны буквально быть в evidence_text/title. Ничего не выдумывай — но из длинного evidence всегда можно взять минимум два реальных факта.

Верни ТОЛЬКО JSON-объект: {"items": [{"fingerprint": "...", "draft_line": "• ..."}]}
"""


def _has_nothing_to_write_from(candidate: dict) -> bool:
    """True when the source gave only a headline (empty/near-empty
    evidence_text + summary + lead) and there is no structured event to
    render deterministically. Such items cannot be written by any model
    and must be treated as an enrichment gap, not a generation failure."""
    text = " ".join(
        str(candidate.get(f) or "") for f in ("evidence_text", "summary", "lead")
    ).strip()
    ev = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    structured_event = bool(
        ev.get("is_event") and (ev.get("date_start") or ev.get("date")) and ev.get("venue")
    )
    return len(text) < 40 and not structured_event


def _event_like_without_date(candidate: dict) -> bool:
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block not in _COST_GUARD_EVENT_BLOCKS and category not in _COST_GUARD_EVENT_CATEGORIES:
        return False
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if str(event.get("date_start") or event.get("date") or "").strip():
        return False
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle")
    )
    if _EVENT_DATE_TEXT_RE.search(blob):
        return False
    event_like = bool(event.get("is_event")) or bool(
        re.search(
            r"\b(?:event|events?|ticket|tickets?|concert|gig|show|festival|market|"
            r"workshop|webinar|meetup|comedy|stand-?up|exhibition|performance|"
            r"событи[ея]|билет|концерт|фестивал|маркет|рынок|мастер-?класс|"
            r"выставк|спектакл|стендап)\b",
            blob,
            re.IGNORECASE,
        )
    )
    return event_like


def _cost_after_quality_skip_reason(candidate: dict) -> str:
    """Return why an item should not consume another model call.

    P0/P1 quality rules already decide which no-date / no-fact items are not
    safe to render. This guard makes the cost layer obey that decision before
    the Board Judge, final translation, repair, or forcing passes spend tokens.
    """
    if str(candidate.get("manual_override") or "") == "force_include":
        return ""
    if _has_nothing_to_write_from(candidate):
        return "No source facts after enrichment; hold for enrichment instead of model rewrite."
    if _event_like_without_date(candidate):
        return "Event-like candidate has no actionable date after enrichment; hold before model rewrite."
    reject_reasons = candidate.get("reject_reasons")
    if isinstance(reject_reasons, list) and reject_reasons:
        return "Candidate already carries reject reasons; do not spend model calls on a line that should not publish."
    if str(candidate.get("reject_reason") or "").strip():
        return "Candidate already carries a reject reason; do not spend model calls on a line that should not publish."
    return ""


def _apply_cost_after_quality_guard(to_rewrite: list[dict]) -> tuple[list[dict], dict[str, object]]:
    selected: list[dict] = []
    held: list[dict[str, object]] = []
    for candidate in to_rewrite:
        reason = _cost_after_quality_skip_reason(candidate)
        if not reason:
            selected.append(candidate)
            continue
        candidate["include"] = False
        candidate["backup_candidate"] = True
        candidate["backup_pool_only"] = True
        candidate["public_reserve"] = False
        candidate["recoverable_reserve"] = False
        candidate["rewrite_shortlist_status"] = "held_cost_after_quality"
        candidate["rewrite_shortlist_reason"] = reason
        verdict = "needs_enrichment" if (
            "No source facts" in reason or "no actionable date" in reason
        ) else "drop"
        _set_digest_selection_verdict(candidate, verdict, reason)
        held.append(
            {
                "fingerprint": candidate.get("fingerprint") or "",
                "title": candidate.get("title") or "",
                "source_label": candidate.get("source_label") or "",
                "category": candidate.get("category") or "",
                "primary_block": candidate.get("primary_block") or "",
                "reason": reason,
            }
        )
    return selected, {
        "schema_version": 1,
        "enabled": True,
        "input_candidates": len(to_rewrite),
        "selected_for_model": len(selected),
        "held_before_model": len(held),
        "held_examples": held[:40],
    }


def _cap_repair_targets(to_repair: list[dict], *, max_items: int | None = None) -> tuple[list[dict], dict[str, object]]:
    cap = max_items if max_items is not None else _env_int(
        "LLM_REPAIR_DRAFT_MAX_ITEMS",
        REPAIR_DRAFT_MAX_ITEMS_DEFAULT,
        minimum=1,
        maximum=40,
    )
    if len(to_repair) <= cap:
        return to_repair, {
            "schema_version": 1,
            "enabled": True,
            "max_items": cap,
            "input_candidates": len(to_repair),
            "selected_for_repair": len(to_repair),
            "held_after_cap": 0,
            "held_examples": [],
        }
    ranked = sorted(to_repair, key=_rewrite_shortlist_priority, reverse=True)
    selected = ranked[:cap]
    held = ranked[cap:]
    for candidate in held:
        candidate["llm_repair_skipped_reason"] = (
            f"Outside repair cap ({cap}); writer/release recovery will handle or drop."
        )
    return selected, {
        "schema_version": 1,
        "enabled": True,
        "max_items": cap,
        "input_candidates": len(to_repair),
        "selected_for_repair": len(selected),
        "held_after_cap": len(held),
        "held_examples": [
            {
                "fingerprint": candidate.get("fingerprint") or "",
                "title": candidate.get("title") or "",
                "category": candidate.get("category") or "",
                "primary_block": candidate.get("primary_block") or "",
                "reason": candidate.get("llm_repair_skipped_reason") or "",
            }
            for candidate in held[:30]
        ],
    }


def _force_write_evidence_floor(candidate: dict) -> int:
    category = str(candidate.get("category") or "")
    source = str(candidate.get("source_label") or "")
    if category == "football" and source in {"Manchester United", "Manchester City"}:
        return 40
    return 400


_SOFT_REPAIR_ERROR_MARKERS = (
    "draft_line is too short",
    "draft_line must contain at least one complete sentence",
    "draft_line for long-format category needs",
    "commercial/retail item needs opening/access/useful local impact",
    "old official/public-service item needs a concrete new public reason",
)

_SOFT_REPAIR_VISIBLE_BLOCKS = {
    "last_24h",
    "today_focus",
    "city_watch",
    "weekend_activities",
    "next_7_days",
    "openings",
    "tech_business",
    "football",
}

_SOFT_REPAIR_EXCLUDED_BLOCKS = {
    "ticket_radar",
    "outside_gm_tickets",
    "future_announcements",
}


def _hard_repair_errors(candidate: dict, line: str) -> list[str]:
    """Return only defects worth spending an LLM repair call on.

    Shortness, sentence count, and general style are writer/editor concerns.
    The repair route is reserved for broken structure, English passthrough,
    unsupported facts, bad markup, or product-critical copy invariants.
    """
    errors = _writer_quality_errors(candidate, line)
    return [
        error for error in errors
        if not any(marker in error for marker in _SOFT_REPAIR_ERROR_MARKERS)
    ]


def _soft_repair_errors(candidate: dict, line: str) -> list[str]:
    errors = _writer_quality_errors(candidate, line)
    return [
        error for error in errors
        if any(marker in error for marker in _SOFT_REPAIR_ERROR_MARKERS)
    ]


def _should_repair_soft_writer_errors(candidate: dict, errors: list[str]) -> bool:
    """Repair short/one-sentence cards only after the visible board.

    The broad pool can contain hundreds of ticket/catalog rows; repairing every
    compact line would reintroduce the latency blow-up. Core digest sections are
    different: a short mini-written card must be expanded before writer, not
    silently dropped and replaced by tickets.
    """
    if not errors:
        return False
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    if block in _SOFT_REPAIR_EXCLUDED_BLOCKS:
        return False
    if block in _SOFT_REPAIR_VISIBLE_BLOCKS:
        return True
    return category in {"media_layer", "gmp", "council", "public_services", "tech_business", "football"}


def _needs_quality_repair(candidate: dict) -> bool:
    line = str(candidate.get("draft_line") or "").strip()
    if not line:
        return False
    category = str(candidate.get("category") or "")
    primary_block = str(candidate.get("primary_block") or "")
    if category not in _LONG_FORMAT_CATEGORIES_FOR_REPAIR | {"transport"}:
        return False
    writer_errors = _hard_repair_errors(candidate, line)
    if writer_errors:
        return True
    normalized = re.sub(r"\s+", " ", line)
    lowered = normalized.lower()
    # A complete dated event card (real event + date) is allowed to be concise:
    # the writer accepts it at the lower DATED_EVENT floor, so re-flagging it
    # "weak" here only churns repairs and inflates weak_after for cards that
    # will publish fine (David Gray, Grand Soul Day Party on 2026-06-03). The
    # writer_errors check above is already authoritative for these.
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    has_event_date = bool(event.get("is_event") and str(event.get("date_start") or event.get("date") or "").strip())
    if any(marker in lowered for marker in _REPAIR_BAD_MARKERS):
        return True
    if _needs_translation_fix(line):
        return True
    return False


def _diagnostic_excerpt(text: str, limit: int = 700) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()[:limit]


def _parse_provider_results(
    raw: str,
    batch: list[dict],
    provider_name: str,
    model: str,
    prompt_name: str,
    batch_idx: int,
    total_batches: int,
) -> tuple[ProviderMapping, dict]:
    expected = {str(c.get("fingerprint") or "").strip(): c for c in batch}
    rejected_counts = {
        "bad_item_shape": 0,
        "missing_fingerprint": 0,
        "unknown_fingerprint": 0,
        "empty_draft_line": 0,
        "empty_draft_line_with_reason": 0,
        "missing_bullet": 0,
        "too_short": 0,
        "duplicate_fingerprint": 0,
    }
    diagnostic = {
        "provider": provider_name,
        "model": model,
        "prompt_name": prompt_name,
        "batch_index": batch_idx,
        "batch_count": total_batches,
        "sent": len(batch),
        "returned_items": 0,
        "accepted": 0,
        "rejected_counts": rejected_counts,
        "rejected_examples": [],
        "missing_candidates": [],
    }

    cleaned = str(raw or "").strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
        cleaned = cleaned.rsplit("```", 1)[0]

    try:
        results = json.loads(cleaned.strip())
    except json.JSONDecodeError as exc:
        diagnostic["parse_error"] = f"{exc.__class__.__name__}: {exc}"
        diagnostic["raw_excerpt"] = _diagnostic_excerpt(cleaned)
        return {}, diagnostic

    if isinstance(results, dict):
        for key in ("items", "results", "draft_lines"):
            if isinstance(results.get(key), list):
                diagnostic["coerced_from_object_key"] = key
                results = results[key]
                break
        else:
            diagnostic["parse_error"] = "JSON root is an object, not a list."
            diagnostic["raw_excerpt"] = _diagnostic_excerpt(cleaned)
            return {}, diagnostic

    if not isinstance(results, list):
        diagnostic["parse_error"] = f"JSON root is {type(results).__name__}, not a list."
        diagnostic["raw_excerpt"] = _diagnostic_excerpt(cleaned)
        return {}, diagnostic

    mapping: ProviderMapping = {}

    def _example(reason: str, item: object, fp: str = "", draft_line: str = "") -> None:
        examples = diagnostic["rejected_examples"]
        if len(examples) >= 5:
            return
        examples.append(
            {
                "reason": reason,
                "fingerprint": fp,
                "draft_line_excerpt": _diagnostic_excerpt(draft_line, limit=180),
                "item_excerpt": _diagnostic_excerpt(json.dumps(item, ensure_ascii=False), limit=240),
            }
        )

    for item in results:
        diagnostic["returned_items"] += 1
        if not isinstance(item, dict):
            rejected_counts["bad_item_shape"] += 1
            _example("bad_item_shape", item)
            continue
        fp = str(item.get("fingerprint") or "").strip()
        dl = str(item.get("draft_line") or "").strip()
        decision = str(item.get("decision") or "").strip()
        missing_facts = item.get("missing_facts") if isinstance(item.get("missing_facts"), list) else []
        if not fp:
            rejected_counts["missing_fingerprint"] += 1
            _example("missing_fingerprint", item, fp, dl)
            continue
        if fp not in expected:
            rejected_counts["unknown_fingerprint"] += 1
            _example("unknown_fingerprint", item, fp, dl)
            continue
        if not dl:
            if decision or missing_facts:
                rejected_counts["empty_draft_line_with_reason"] += 1
                _example("empty_draft_line_with_reason", item, fp, dl)
            else:
                rejected_counts["empty_draft_line"] += 1
                _example("empty_draft_line", item, fp, dl)
            continue
        if not dl.startswith("• "):
            rejected_counts["missing_bullet"] += 1
            _example("missing_bullet", item, fp, dl)
            continue
        if len(dl) < 15:
            rejected_counts["too_short"] += 1
            _example("too_short", item, fp, dl)
            continue
        if fp in mapping:
            rejected_counts["duplicate_fingerprint"] += 1
        mapping[fp] = (dl, provider_name, model)

    diagnostic["accepted"] = len(mapping)
    missing = [fp for fp in expected if fp not in mapping]
    diagnostic["missing_candidates"] = [
        {
            "fingerprint": fp,
            "title": expected[fp].get("title"),
            "category": expected[fp].get("category"),
            "primary_block": expected[fp].get("primary_block"),
        }
        for fp in missing[:8]
    ]
    if diagnostic["accepted"] < diagnostic["sent"]:
        diagnostic["raw_excerpt"] = _diagnostic_excerpt(cleaned)
    return mapping, diagnostic


def _parse_english_card_results(
    raw: str,
    batch: list[dict],
    provider_name: str,
    model: str,
    batch_idx: int,
    total_batches: int,
) -> tuple[EnglishCardMapping, dict]:
    expected = {str(c.get("fingerprint") or ""): c for c in batch if c.get("fingerprint")}
    diagnostic = {
        "provider": provider_name,
        "model": model,
        "prompt_name": "english_cards",
        "batch_index": batch_idx,
        "batch_count": total_batches,
        "sent": len(batch),
        "returned_items": 0,
        "accepted": 0,
        "rejected_counts": {
            "bad_item_shape": 0,
            "missing_fingerprint": 0,
            "unknown_fingerprint": 0,
            "missing_reader_card": 0,
            "duplicate_fingerprint": 0,
            "parse_error": 0,
        },
        "rejected_examples": [],
        "missing_candidates": [],
    }
    cleaned = str(raw or "").strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
        cleaned = cleaned.rsplit("```", 1)[0]
    try:
        results = json.loads(cleaned.strip())
    except json.JSONDecodeError as exc:
        diagnostic["rejected_counts"]["parse_error"] += 1
        diagnostic["parse_error"] = f"{exc.__class__.__name__}: {exc}"
        diagnostic["raw_excerpt"] = _diagnostic_excerpt(cleaned)
        return {}, diagnostic
    if isinstance(results, dict):
        for key in ("items", "results", "cards"):
            if isinstance(results.get(key), list):
                results = results[key]
                break
    if not isinstance(results, list):
        diagnostic["parse_error"] = f"JSON root is {type(results).__name__}, not a list."
        diagnostic["raw_excerpt"] = _diagnostic_excerpt(cleaned)
        return {}, diagnostic

    mapping: EnglishCardMapping = {}

    def _example(reason: str, item: object, fp: str = "") -> None:
        examples = diagnostic["rejected_examples"]
        if len(examples) >= 5:
            return
        examples.append(
            {
                "reason": reason,
                "fingerprint": fp,
                "item_excerpt": _diagnostic_excerpt(json.dumps(item, ensure_ascii=False), limit=260),
            }
        )

    for item in results:
        diagnostic["returned_items"] += 1
        if not isinstance(item, dict):
            diagnostic["rejected_counts"]["bad_item_shape"] += 1
            _example("bad_item_shape", item)
            continue
        fp = str(item.get("fingerprint") or "").strip()
        if not fp:
            diagnostic["rejected_counts"]["missing_fingerprint"] += 1
            _example("missing_fingerprint", item, fp)
            continue
        if fp not in expected:
            diagnostic["rejected_counts"]["unknown_fingerprint"] += 1
            _example("unknown_fingerprint", item, fp)
            continue
        reader_card = re.sub(r"\s+", " ", str(item.get("reader_card") or "")).strip()
        if len(reader_card) < 20:
            diagnostic["rejected_counts"]["missing_reader_card"] += 1
            _example("missing_reader_card", item, fp)
            continue
        fact_card = item.get("fact_card") if isinstance(item.get("fact_card"), dict) else {}
        score_raw = item.get("editorial_score")
        try:
            editorial_score = int(float(score_raw))
        except (TypeError, ValueError):
            editorial_score = int(min(100, max(0, section_board_score(expected[fp], str(expected[fp].get("primary_block") or "")))))
        card = {
            "rubric": str(item.get("rubric") or "other").strip() or "other",
            "fact_card": fact_card,
            "reader_card": reader_card,
            "editorial_score": max(0, min(100, editorial_score)),
            "selection_hint": str(item.get("selection_hint") or "publish").strip() or "publish",
            "board_decision": str(item.get("board_decision") or item.get("selection_hint") or "publish").strip() or "publish",
            "board_confidence": item.get("board_confidence"),
            "suggested_block": str(item.get("suggested_block") or "").strip(),
            "reason_codes": item.get("reason_codes") if isinstance(item.get("reason_codes"), list) else [],
            "needs_gpt4o_escalation": bool(item.get("needs_gpt4o_escalation")),
            "missing_facts": item.get("missing_facts") if isinstance(item.get("missing_facts"), list) else [],
        }
        if fp in mapping:
            diagnostic["rejected_counts"]["duplicate_fingerprint"] += 1
        mapping[fp] = (card, provider_name, model)

    diagnostic["accepted"] = len(mapping)
    missing = [fp for fp in expected if fp not in mapping]
    diagnostic["missing_candidates"] = [
        {
            "fingerprint": fp,
            "title": expected[fp].get("title"),
            "category": expected[fp].get("category"),
            "primary_block": expected[fp].get("primary_block"),
        }
        for fp in missing[:8]
    ]
    if diagnostic["accepted"] < diagnostic["sent"]:
        diagnostic["raw_excerpt"] = _diagnostic_excerpt(cleaned)
    return mapping, diagnostic


def _call_english_card_provider_batch(
    base_url: str,
    api_key: str,
    model: str,
    candidates: list[dict],
    provider_name: str,
    timeout: int = 60,
    batch_size: int = 8,
    diagnostics: list[dict] | None = None,
) -> EnglishCardMapping:
    if not candidates:
        return {}
    if not api_key:
        logger.warning("%s: API key not set, skipping English cards.", provider_name)
        return {}
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return {}

    client = OpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=timeout,
        max_retries=0 if provider_name.lower().startswith("openai") else sdk_retries_for_route(provider=provider_name, model=model, base_url=base_url),
    )
    mapping: EnglishCardMapping = {}
    batches = _token_aware_batches(
        candidates,
        batch_size=batch_size,
        system_prompt=ENGLISH_CARD_SYSTEM,
        item_builder=_english_card_batch_items,
        max_items_env="LLM_ENGLISH_CARD_MAX_BATCH_ITEMS",
        token_budget_env="LLM_ENGLISH_CARD_BATCH_TOKEN_BUDGET",
        default_token_budget=5400,
    )
    logger.info(
        "%s English cards: %d candidates → %d token-aware batch(es), max_items≤%d.",
        provider_name,
        len(candidates),
        len(batches),
        batch_size,
    )

    def _send_once(batch: list[dict], batch_idx: int, attempt: str) -> EnglishCardMapping:
        user_payload = {"today_date": today_london(), "candidates": _english_card_batch_items(batch)}
        messages = [
            {"role": "system", "content": ENGLISH_CARD_SYSTEM},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ]
        started_at = now_london().isoformat()
        t0 = time.monotonic()
        queue_wait_seconds = 0.0
        api_seconds = 0.0
        try:
            default_max_tokens = min(8192, 420 * len(batch) + 1400)
            max_tokens, max_tokens_source = _max_tokens_for_batch("english_cards", model, len(batch), default_max_tokens)
            with _API_SEMAPHORE:
                queue_t0 = time.monotonic()
                _API_RATE_LIMITER.acquire()
                estimated_request_tokens = _estimate_request_tokens(messages, max_tokens)
                _API_TOKEN_LIMITER.acquire(estimated_request_tokens)
                queue_wait_seconds = time.monotonic() - queue_t0
                api_t0 = time.monotonic()
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=0.1,
                    max_tokens=max_tokens,
                    **chat_completion_options_for_route(provider=provider_name, model=model, base_url=base_url),
                )
                api_seconds = time.monotonic() - api_t0
            from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415
            record_call_from_response(
                response=response,
                stage="llm_rewrite",
                provider=provider_name.split("-", 1)[0],
                model=model,
                prompt_name="english_cards",
                messages=messages,
                max_tokens=max_tokens,
            )
            raw = response.choices[0].message.content.strip()
            batch_mapping, diagnostic = _parse_english_card_results(raw, batch, provider_name, model, batch_idx, len(batches))
            diagnostic["attempt"] = attempt
            diagnostic["started_at"] = started_at
            diagnostic["finished_at"] = now_london().isoformat()
            diagnostic["duration_seconds"] = round(time.monotonic() - t0, 3)
            diagnostic["queue_wait_seconds"] = round(queue_wait_seconds, 3)
            diagnostic["api_seconds"] = round(api_seconds, 3)
            diagnostic["estimated_request_tokens"] = estimated_request_tokens
            diagnostic["max_tokens_source"] = max_tokens_source
            diagnostic.update(_response_token_diagnostics(response, max_tokens=max_tokens, batch_len=len(batch)))
            if diagnostics is not None:
                diagnostics.append(diagnostic)
            return batch_mapping
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s English cards: batch %s/%d (%s) failed — %s", provider_name, batch_idx, len(batches), attempt, exc)
            if diagnostics is not None:
                diagnostics.append(
                    {
                        "provider": provider_name,
                        "model": model,
                        "prompt_name": "english_cards",
                        "batch_index": batch_idx,
                        "batch_count": len(batches),
                        "attempt": attempt,
                        "sent": len(batch),
                        "returned_items": 0,
                        "accepted": 0,
                        "error": f"{exc.__class__.__name__}: {exc}",
                        "started_at": started_at,
                        "finished_at": now_london().isoformat(),
                        "duration_seconds": round(time.monotonic() - t0, 3),
                        "queue_wait_seconds": round(queue_wait_seconds, 3),
                        "api_seconds": round(api_seconds, 3),
                        "max_tokens": max_tokens if "max_tokens" in locals() else 0,
                        "max_tokens_source": max_tokens_source if "max_tokens_source" in locals() else "",
                        "estimated_request_tokens": estimated_request_tokens if "estimated_request_tokens" in locals() else 0,
                        "truncated": False,
                    }
                )
            return {}

    def _process_batch(batch_idx: int, batch: list[dict]) -> EnglishCardMapping:
        result = _send_once(batch, batch_idx, "initial")
        missing = [c for c in batch if str(c.get("fingerprint") or "") not in result]
        if missing and len(missing) > 1:
            split_size = max(1, min(4, len(missing) // 2 or 1))
            for split_idx in range(0, len(missing), split_size):
                split = missing[split_idx: split_idx + split_size]
                _jittered_sleep(0.4)
                result.update(_send_once(split, batch_idx, f"split_{split_idx // split_size + 1}"))
        elif missing:
            _jittered_sleep(0.4)
            result.update(_send_once(missing, batch_idx, "single_retry"))
        return result

    if len(batches) <= 1:
        for batch_idx, batch in enumerate(batches, start=1):
            mapping.update(_process_batch(batch_idx, batch))
    else:
        max_workers = min(len(batches), _REWRITE_API_CONCURRENCY)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_process_batch, batch_idx, batch)
                for batch_idx, batch in enumerate(batches, start=1)
            ]
            for future in futures:
                mapping.update(future.result())
    return mapping


def _call_english_cards_with_fallback(
    candidates: list[dict],
    provider_override: str,
    base_url_override: str,
    model_override: str,
    diagnostics: list[dict] | None = None,
) -> EnglishCardMapping:
    if not candidates or provider_override == "none":
        return {}
    route = resolve_model_route(
        "english_cards",
        provider_override=provider_override,
        base_url_override=base_url_override,
        model_override=model_override,
    )
    from news_digest.pipeline import provider_health  # noqa: PLC0415
    mapping: EnglishCardMapping = {}
    missing = list(candidates)
    escalation_pending: list[dict] = []
    for step in route:
        if not missing and not escalation_pending:
            break
        if provider_health.is_dead(step.provider):
            logger.info("Skipping %s English cards — circuit breaker tripped earlier this run.", step.provider_label)
            continue
        if escalation_pending and step.provider != "openai":
            escalation_pending = []
        if (
            escalation_pending
            and not missing
            and step.priority <= 1
            and not (provider_override or base_url_override or model_override)
        ):
            # A strong-model escalation should go to the surgical gpt-4o lead
            # step, not through another cheap full-list judge.
            continue
        previous_escalation = list(escalation_pending)
        request_candidates = _fallback_request_candidates(
            list(missing),
            step_priority=step.priority,
            provider=step.provider,
            model=step.model,
            provider_override=provider_override,
            base_url_override=base_url_override,
            model_override=model_override,
        )
        if escalation_pending and step.priority > 1:
            seen_request = {str(c.get("fingerprint") or "") for c in request_candidates}
            request_candidates.extend(
                c for c in escalation_pending
                if _is_lead_candidate(c) and str(c.get("fingerprint") or "") not in seen_request
            )
        if not request_candidates:
            logger.info(
                "Skipping %s English cards fallback for %d non-lead miss(es).",
                step.provider_label,
                len(missing),
            )
            missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
            continue
        step_mapping = (
            _call_english_card_provider_batch(
                step.base_url,
                step.api_key,
                step.model,
                request_candidates,
                step.provider_label,
                timeout=step.timeout_seconds or 60,
                batch_size=1 if step.priority > 1 and not (provider_override or base_url_override or model_override) else (step.batch_size or 8),
                diagnostics=diagnostics,
            )
        )
        mapping.update(step_mapping)
        if step_mapping:
            provider_health.record_success(step.provider)
        else:
            provider_health.record_failure(step.provider)
        if step.role in {"board_judge_mini_primary", "board_judge_mini_reserve", "board_ranker_deepseek_pro_primary"}:
            next_escalation = [
                c for c in candidates
                if (
                    str(c.get("fingerprint") or "") in step_mapping
                    and bool(step_mapping[str(c.get("fingerprint") or "")][0].get("needs_gpt4o_escalation"))
                )
            ]
            existing = {str(c.get("fingerprint") or "") for c in next_escalation}
            next_escalation.extend(
                c for c in previous_escalation
                if _is_lead_candidate(c) and str(c.get("fingerprint") or "") not in existing
            )
            escalation_pending = next_escalation
        else:
            escalation_pending = []
        missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
    return mapping


def _apply_english_cards_to_candidates(
    candidates: list[dict],
    english_card_mapping: EnglishCardMapping,
    run_iso: str,
) -> int:
    applied = 0
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "").strip()
        if fp not in english_card_mapping:
            continue
        card, prov, model_name = english_card_mapping[fp]
        fact_card = card.get("fact_card") if isinstance(card.get("fact_card"), dict) else {}
        candidate["english_rubric"] = card.get("rubric") or "other"
        candidate["english_fact_card"] = fact_card
        candidate["english_reader_card"] = card.get("reader_card") or ""
        candidate["english_editorial_score"] = card.get("editorial_score")
        candidate["english_selection_hint"] = card.get("selection_hint") or "publish"
        candidate["english_board_decision"] = card.get("board_decision") or candidate["english_selection_hint"]
        candidate["english_board_confidence"] = card.get("board_confidence")
        candidate["english_suggested_block"] = card.get("suggested_block") or ""
        candidate["english_board_reason_codes"] = card.get("reason_codes") or []
        candidate["english_needs_gpt4o_escalation"] = bool(card.get("needs_gpt4o_escalation"))
        candidate["english_missing_facts"] = card.get("missing_facts") or []
        candidate["english_card_provider"] = prov
        candidate["english_card_model"] = model_name
        candidate["english_card_written_at"] = run_iso
        applied += 1
    return applied


def _is_protected_rewrite_candidate(candidate: dict) -> bool:
    """Items that must get extra rewrite recovery before they can disappear."""
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    reason_codes = protected.get("reason_codes") if isinstance(protected.get("reason_codes"), list) else []
    if block in {"last_24h", "today_focus", "transport"}:
        return True
    if category in {"gmp", "public_services"}:
        return True
    if protected.get("protected") and any(
        str(code) in {"transport", "public_safety", "council_decision", "hard_news"}
        for code in reason_codes
    ):
        return True
    return False


def _is_lead_candidate(candidate: dict) -> bool:
    return bool(candidate.get("is_lead"))


def _default_route_allows_step(
    *,
    step_priority: int,
    provider: str,
    model: str,
    provider_override: str,
    base_url_override: str,
    model_override: str,
) -> bool:
    if provider_override or base_url_override or model_override:
        return True
    if step_priority <= 1:
        return True
    return provider.lower() == "openai" and model == OPENAI_REWRITE_MODEL


def _fallback_request_candidates(
    candidates: list[dict],
    *,
    step_priority: int,
    provider: str,
    model: str,
    provider_override: str,
    base_url_override: str,
    model_override: str,
) -> list[dict]:
    if not _default_route_allows_step(
        step_priority=step_priority,
        provider=provider,
        model=model,
        provider_override=provider_override,
        base_url_override=base_url_override,
        model_override=model_override,
    ):
        return []
    if provider_override or base_url_override or model_override or step_priority <= 1:
        return list(candidates)
    return [candidate for candidate in candidates if _is_lead_candidate(candidate)]


def _rewrite_packet(candidate: dict) -> dict[str, object]:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    frame = candidate.get("story_frame") if isinstance(candidate.get("story_frame"), dict) else {}
    entities = candidate.get("entities") if isinstance(candidate.get("entities"), dict) else {}
    boroughs = candidate.get("boroughs") if isinstance(candidate.get("boroughs"), list) else []
    where = (
        frame.get("where_exact")
        or event.get("venue")
        or (boroughs[0] if boroughs else "")
        or candidate.get("borough")
        or ""
    )
    when = (
        frame.get("when")
        or event.get("date_text")
        or event.get("date_start")
        or event.get("date")
        or candidate.get("published_at")
        or ""
    )
    people = entities.get("people") if isinstance(entities.get("people"), list) else []
    venues = entities.get("venues") if isinstance(entities.get("venues"), list) else []
    evidence_blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "evidence_text", "practical_angle")
    )
    allowed_numbers = sorted(set(
        token.strip()
        for token in re.findall(
            r"(?:£\s?\d[\d,]*(?:\.\d+)?|\d[\d,]*(?:\.\d+)?\s?(?:%|miles?|mph|years?|months?|days?|hours?|минут|час(?:а|ов)?|лет|дн(?:я|ей)?)|\d{1,2}:\d{2}|\d{4})",
            evidence_blob,
            flags=re.IGNORECASE,
        )
        if token.strip()
    ))
    return {
        "what": frame.get("what_happened") or candidate.get("title") or "",
        "where": where,
        "when": when,
        "who": frame.get("who_affected") or (people[0] if people else ""),
        "venue": event.get("venue") or (venues[0] if venues else ""),
        "event_date": event.get("date_start") or event.get("date") or "",
        "change_type": candidate.get("change_type") or "",
        "change_phase": candidate.get("change_phase") or "",
        "why_now": candidate.get("why_now") or "",
        "reader_action_type": candidate.get("reader_action_type") or "",
        "practical_angle": candidate.get("practical_angle") or "",
        "allowed_numbers": allowed_numbers[:30],
        "do_not_use_literal_phrases": [
            "тройное ножевое ранение",
            "отдельные ножевые атаки",
            "открытые выводы",
        ],
    }


def _rewrite_batch_items(batch: list[dict]) -> list[dict]:
    return [
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
            "entities": c.get("entities", {}),
            "event": c.get("event", {}),
            "rewrite_packet": _rewrite_packet(c),
            "expected_operator": c.get("expected_operator", ""),
            "transport_mode": c.get("transport_mode", ""),
            "current_draft_line": c.get("draft_line", ""),
        }
        for c in batch
    ]


def _english_card_batch_items(batch: list[dict]) -> list[dict]:
    """Compact source-language payload for English fact/reader cards.

    This is the enrichment handoff: the model sees already-fetched source
    evidence, structured event/entity fields, and story frames. It does not
    browse and must explicitly mark missing facts instead of inventing them.
    """
    items: list[dict] = []
    for c in batch:
        event = c.get("event") if isinstance(c.get("event"), dict) else {}
        frame = c.get("story_frame") if isinstance(c.get("story_frame"), dict) else {}
        evidence_limit = 3600 if _is_selected_or_reserve_for_enriched_context(c) else 1000
        items.append(
            {
                "fingerprint": c.get("fingerprint", ""),
                "title": c.get("title", ""),
                "summary": c.get("summary", ""),
                "lead": c.get("lead", ""),
                "evidence_text": str(c.get("evidence_text") or "")[:evidence_limit],
                "category": c.get("category", ""),
                "primary_block": c.get("primary_block", ""),
                "source_label": c.get("source_label", ""),
                "source_url": c.get("source_url", ""),
                "published_at": c.get("published_at", ""),
                "freshness_status": c.get("freshness_status", ""),
                "borough": c.get("borough", ""),
                "entities": c.get("entities", {}),
                "event": event,
                "story_frame": frame,
                "rewrite_packet": _rewrite_packet(c),
                "reader_value_score": reader_value_score(c),
                "section_board_score": section_board_score(c, str(c.get("primary_block") or "")),
                "glossary_terms": _glossary_terms_for_candidate(c),
            }
        )
    return items


def _is_selected_or_reserve_for_enriched_context(candidate: dict) -> bool:
    status = str(candidate.get("rewrite_shortlist_status") or "")
    verdict = str(candidate.get("digest_selection_verdict") or "")
    block = str(candidate.get("primary_block") or "")
    return bool(
        status.startswith("selected")
        or verdict in {"selected", "reserve", "needs_enrichment"}
        or candidate.get("is_lead")
        or block in {"russian_events", "professional_events", "weekend_activities", "next_7_days"}
    )


def _plain_source_text(raw_html: str, *, limit: int = 4500) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", str(raw_html or ""))
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<nav[^>]*>.*?</nav>", " ", text)
    text = re.sub(r"(?is)<footer[^>]*>.*?</footer>", " ", text)
    text = re.sub(r"(?is)<header[^>]*>.*?</header>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def _needs_prewrite_enrichment(candidate: dict) -> bool:
    if _needs_selection_enrichment(candidate):
        return True
    block = str(candidate.get("primary_block") or "")
    evidence = re.sub(
        r"\s+",
        " ",
        " ".join(str(candidate.get(field) or "") for field in ("summary", "lead", "evidence_text")).strip(),
    )
    if len(evidence) >= 1200:
        return False
    return bool(
        candidate.get("is_lead")
        or block in {"last_24h", "today_focus", "weekend_activities", "next_7_days", "russian_events", "professional_events", "openings", "tech_business"}
    )


def _enrich_before_board(candidates: list[dict], *, max_items: int = 24) -> dict[str, object]:
    """Refetch full source text for selected/reserve items before model writing.

    This is a recovery-first step: if a useful item only has a title/snippet, we
    try to give the board judge and Russian writer real evidence before asking
    them to write or decide.
    """
    report: dict[str, object] = {
        "attempted": 0,
        "enriched": 0,
        "skipped": 0,
        "failed": 0,
        "examples": [],
    }
    try:
        from news_digest.pipeline.collector.fetch import _fetch_text  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001
        report["setup_error"] = f"{exc.__class__.__name__}: {exc}"
        return report

    for candidate in candidates:
        if int(report["attempted"]) >= max_items:
            break
        if not isinstance(candidate, dict) or not _needs_prewrite_enrichment(candidate):
            report["skipped"] = int(report["skipped"]) + 1
            continue
        url = str(candidate.get("source_url") or "").strip()
        if not url.startswith(("http://", "https://")):
            report["skipped"] = int(report["skipped"]) + 1
            continue
        existing = re.sub(
            r"\s+",
            " ",
            " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead", "evidence_text")).strip(),
        )
        report["attempted"] = int(report["attempted"]) + 1
        try:
            fetched = _plain_source_text(_fetch_text(url), limit=4500)
        except Exception as exc:  # noqa: BLE001
            report["failed"] = int(report["failed"]) + 1
            examples = report["examples"]
            if isinstance(examples, list) and len(examples) < 8:
                examples.append(
                    {
                        "fingerprint": candidate.get("fingerprint") or "",
                        "title": candidate.get("title") or "",
                        "status": "failed",
                        "error": f"{exc.__class__.__name__}: {exc}",
                    }
                )
            continue
        if len(fetched) <= len(existing) + 200:
            report["skipped"] = int(report["skipped"]) + 1
            continue
        candidate["evidence_text"] = fetched[:4500]
        candidate["prewrite_enrichment"] = {
            "used_refetch": True,
            "existing_chars": len(existing),
            "refetched_chars": len(fetched),
            "source_url": url,
        }
        report["enriched"] = int(report["enriched"]) + 1
        examples = report["examples"]
        if isinstance(examples, list) and len(examples) < 8:
            examples.append(
                {
                    "fingerprint": candidate.get("fingerprint") or "",
                    "title": candidate.get("title") or "",
                    "status": "enriched",
                    "existing_chars": len(existing),
                    "refetched_chars": len(fetched),
                }
            )
    return report


def _selected_evidence_text(candidate: dict, *, limit: int = 3000) -> str:
    shortlist_status = str(candidate.get("rewrite_shortlist_status") or "")
    protected = candidate.get("protected_lane")
    selected = (
        shortlist_status.startswith("selected")
        or shortlist_status == "writer_deterministic"
        or bool(candidate.get("is_lead"))
        or bool(protected)
        or bool(candidate.get("public_reserve"))
        or bool(candidate.get("backup_candidate"))
    )
    if not selected:
        return ""
    return _memory_text_digest(candidate.get("evidence_text"), limit=limit)


def _translation_batch_items(batch: list[dict]) -> list[dict]:
    """Direct Russian-writing payload.

    The English card is only a service fact/angle note. The visible bullet is
    written directly in Russian from the fact card, structured fields and richer
    selected-item evidence.
    """
    items: list[dict] = []
    for c in batch:
        fact_card = c.get("english_fact_card") if isinstance(c.get("english_fact_card"), dict) else {}
        items.append(
            {
                "fingerprint": c.get("fingerprint", ""),
                "category": c.get("category", ""),
                "primary_block": c.get("primary_block", ""),
                "source_label": c.get("source_label", ""),
                "title": c.get("title", ""),
                "english_rubric": c.get("english_rubric", ""),
                "english_reader_card": c.get("english_reader_card", ""),
                "english_fact_card": fact_card,
                "source_evidence": _selected_evidence_text(c),
                "event": c.get("event") if isinstance(c.get("event"), dict) else {},
                "story_frame": c.get("story_frame") if isinstance(c.get("story_frame"), dict) else {},
                "glossary_terms": _glossary_terms_for_candidate(c),
            }
        )
    return items


def _translation_memory_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "translation_memory.json"


def _english_card_memory_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "english_card_memory.json"


def _translation_glossary_path(project_root: Path) -> Path:
    return project_root / "data" / "translation_glossary.json"


def _load_translation_glossary(project_root: Path) -> list[dict[str, str]]:
    path = _translation_glossary_path(project_root)
    if not path.exists():
        return []
    payload = read_json(path)
    terms = payload.get("terms") if isinstance(payload, dict) else []
    if not isinstance(terms, list):
        return []
    cleaned: list[dict[str, str]] = []
    for term in terms:
        if not isinstance(term, dict):
            continue
        match = str(term.get("match") or "").strip()
        ru = str(term.get("ru") or "").strip()
        if not match or not ru:
            continue
        cleaned.append({"match": match, "ru": ru, "note": str(term.get("note") or "").strip()})
    return cleaned


def _glossary_terms_for_candidate(candidate: dict, *, limit: int = 12) -> list[dict[str, str]]:
    if not _ACTIVE_TRANSLATION_GLOSSARY:
        return []
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in (
            "title",
            "summary",
            "lead",
            "evidence_text",
            "english_reader_card",
            "source_label",
            "primary_block",
            "category",
        )
    ).lower()
    matched: list[dict[str, str]] = []
    seen: set[str] = set()
    for term in _ACTIVE_TRANSLATION_GLOSSARY:
        key = str(term.get("match") or "").lower()
        if not key or key in seen:
            continue
        if key in blob:
            matched.append(term)
            seen.add(key)
        if len(matched) >= limit:
            break
    return matched


def _memory_text_digest(value: object, *, limit: int = 1800) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def _candidate_content_hash(candidate: dict) -> str:
    """Fact hash for reusable model work across repeated event/listing pages.

    Backlog 8.3: includes prompt_version + structured story facts (not just
    truncated evidence text) so a prompt change or a materially changed hard-
    news fact (casualty count, court stage) invalidates reuse even when the
    change sits past the evidence-text truncation point (3200 chars)."""
    from news_digest.pipeline.inventory import INVENTORY_SCHEMA_VERSION, evidence_cache_extra_fields  # noqa: PLC0415
    from news_digest.pipeline.prompts_meta import PROMPT_REGISTRY_VERSION  # noqa: PLC0415

    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    payload = {
        "category": str(candidate.get("category") or ""),
        "primary_block": str(candidate.get("primary_block") or ""),
        "title": _memory_text_digest(candidate.get("title"), limit=700),
        "summary": _memory_text_digest(candidate.get("summary"), limit=900),
        "lead": _memory_text_digest(candidate.get("lead"), limit=900),
        "evidence_text": _memory_text_digest(candidate.get("source_evidence") or candidate.get("evidence_text"), limit=3200),
        "published_at": str(candidate.get("published_at") or ""),
        "event_name": str(event.get("name") or candidate.get("event_name") or ""),
        "event_date": str(event.get("date_start") or event.get("date") or candidate.get("event_date") or ""),
        "event_venue": str(event.get("venue") or candidate.get("venue") or ""),
        "rewrite_packet": _rewrite_packet(candidate),
        "prompt_version": PROMPT_REGISTRY_VERSION,
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "story_facts": evidence_cache_extra_fields(candidate),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _translation_memory_signature(candidate: dict) -> str:
    """Stable signature for "same facts, safe to reuse Russian".

    Fingerprint alone is not enough: recurring events and ticket pages can keep
    the same URL while the next occurrence/date changes. The signature includes
    compact source facts plus structured event/rewrite packets, so changed facts
    force a fresh English/translation pass.
    """
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    signature_payload = {
        "content_hash": _candidate_content_hash(candidate),
        "category": str(candidate.get("category") or ""),
        "primary_block": str(candidate.get("primary_block") or ""),
        "title": _memory_text_digest(candidate.get("title"), limit=700),
        "summary": _memory_text_digest(candidate.get("summary"), limit=900),
        "lead": _memory_text_digest(candidate.get("lead"), limit=900),
        "evidence_text": _memory_text_digest(candidate.get("evidence_text"), limit=2200),
        "published_at": str(candidate.get("published_at") or ""),
        "event_date": str(event.get("date_start") or event.get("date") or ""),
        "event_venue": str(event.get("venue") or ""),
        "rewrite_packet": _rewrite_packet(candidate),
        "english_reader_card": _memory_text_digest(candidate.get("english_reader_card"), limit=900),
    }
    raw = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_english_card_memory(project_root: Path) -> dict:
    path = _english_card_memory_path(project_root)
    if not path.exists():
        return {"schema_version": ENGLISH_CARD_MEMORY_VERSION, "entries": {}}
    payload = read_json(path, {"schema_version": ENGLISH_CARD_MEMORY_VERSION, "entries": {}})
    if not isinstance(payload, dict):
        return {"schema_version": ENGLISH_CARD_MEMORY_VERSION, "entries": {}}
    if not isinstance(payload.get("entries"), dict):
        payload["entries"] = {}
    payload["schema_version"] = ENGLISH_CARD_MEMORY_VERSION
    return payload


def _prune_english_card_memory(memory: dict) -> dict:
    entries = memory.get("entries")
    if not isinstance(entries, dict):
        memory["entries"] = {}
        return memory
    today = date.fromisoformat(today_london())
    kept: dict[str, dict] = {}
    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        last_seen_raw = str(entry.get("last_seen_london") or entry.get("updated_at_london") or "")[:10]
        try:
            age_days = (today - date.fromisoformat(last_seen_raw)).days
        except ValueError:
            age_days = ENGLISH_CARD_MEMORY_TTL_DAYS + 1
        if age_days <= ENGLISH_CARD_MEMORY_TTL_DAYS:
            kept[str(key)] = entry
    if len(kept) > ENGLISH_CARD_MEMORY_MAX_ENTRIES:
        rows = sorted(
            kept.items(),
            key=lambda item: str(item[1].get("last_seen_london") or item[1].get("updated_at_london") or ""),
            reverse=True,
        )
        kept = dict(rows[:ENGLISH_CARD_MEMORY_MAX_ENTRIES])
    memory["entries"] = kept
    return memory


def _apply_english_card_memory(candidates: list[dict], memory: dict) -> list[dict]:
    entries = memory.get("entries") if isinstance(memory.get("entries"), dict) else {}
    reused: list[dict] = []
    now_iso = now_london().isoformat()
    for candidate in candidates:
        if str(candidate.get("english_reader_card") or "").strip():
            continue
        content_hash = _candidate_content_hash(candidate)
        entry = entries.get(f"content:{content_hash}")
        if not isinstance(entry, dict) or str(entry.get("content_hash") or "") != content_hash:
            continue
        reader_card = str(entry.get("english_reader_card") or "").strip()
        if len(reader_card) < 20:
            continue
        candidate["english_rubric"] = str(entry.get("english_rubric") or "other")
        candidate["english_reader_card"] = reader_card
        candidate["english_fact_card"] = entry.get("english_fact_card") if isinstance(entry.get("english_fact_card"), dict) else {}
        candidate["english_card_provider"] = "english_card_memory"
        candidate["english_card_model"] = str(entry.get("model") or "")
        candidate["english_card_written_at"] = now_iso
        candidate["english_card_memory_hit"] = True
        entry["last_seen_london"] = today_london()
        entry["reuse_count"] = int(entry.get("reuse_count") or 0) + 1
        reused.append(
            {
                "fingerprint": candidate.get("fingerprint"),
                "title": candidate.get("title"),
                "primary_block": candidate.get("primary_block"),
                "category": candidate.get("category"),
                "model": entry.get("model"),
            }
        )
    return reused


def _update_english_card_memory(candidates: list[dict], memory: dict) -> int:
    entries = memory.setdefault("entries", {})
    if not isinstance(entries, dict):
        memory["entries"] = {}
        entries = memory["entries"]
    updated = 0
    now_iso = now_london().isoformat()
    for candidate in candidates:
        reader_card = str(candidate.get("english_reader_card") or "").strip()
        if len(reader_card) < 20 or str(candidate.get("category") or "") == "weather":
            continue
        content_hash = _candidate_content_hash(candidate)
        key = f"content:{content_hash}"
        entries[key] = {
            "content_hash": content_hash,
            "fingerprint": str(candidate.get("fingerprint") or ""),
            "title": str(candidate.get("title") or ""),
            "english_rubric": str(candidate.get("english_rubric") or "other"),
            "english_reader_card": reader_card,
            "english_fact_card": candidate.get("english_fact_card") if isinstance(candidate.get("english_fact_card"), dict) else {},
            "provider": str(candidate.get("english_card_provider") or ""),
            "model": str(candidate.get("english_card_model") or ""),
            "updated_at_london": now_iso,
            "last_seen_london": today_london(),
            "reuse_count": int((entries.get(key) or {}).get("reuse_count") or 0),
        }
        updated += 1
    _prune_english_card_memory(memory)
    return updated


def _load_translation_memory(project_root: Path) -> dict:
    path = _translation_memory_path(project_root)
    if not path.exists():
        return {"schema_version": TRANSLATION_MEMORY_VERSION, "entries": {}}
    payload = read_json(path)
    if not isinstance(payload, dict):
        return {"schema_version": TRANSLATION_MEMORY_VERSION, "entries": {}}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
    payload["schema_version"] = TRANSLATION_MEMORY_VERSION
    return payload


def _prune_translation_memory(memory: dict) -> dict:
    entries = memory.get("entries")
    if not isinstance(entries, dict):
        memory["entries"] = {}
        return memory
    today = date.fromisoformat(today_london())
    kept: dict[str, dict] = {}
    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        last_seen_raw = str(entry.get("last_seen_london") or entry.get("updated_at_london") or "")[:10]
        try:
            age_days = (today - date.fromisoformat(last_seen_raw)).days
        except ValueError:
            age_days = TRANSLATION_MEMORY_TTL_DAYS + 1
        if age_days <= TRANSLATION_MEMORY_TTL_DAYS:
            kept[str(key)] = entry
    if len(kept) > TRANSLATION_MEMORY_MAX_ENTRIES:
        rows = sorted(
            kept.items(),
            key=lambda item: str(item[1].get("last_seen_london") or item[1].get("updated_at_london") or ""),
            reverse=True,
        )
        kept = dict(rows[:TRANSLATION_MEMORY_MAX_ENTRIES])
    memory["entries"] = kept
    return memory


# 0042: blocks whose cards are legitimately short (tickets, weekend planning,
# transport, weather, russian/food listings) skip the prose length bar.
_CACHE_SHORT_OK_BLOCKS = {
    "ticket_radar",
    "outside_gm_tickets",
    "weekend_activities",
    "future_announcements",
    "russian_events",
    "weather",
    "transport",
    "openings",
}
_CACHE_PROSE_MIN_CHARS = 140


def _cached_line_meets_contract(candidate: dict, draft_line: str) -> bool:
    """0042: a cached Russian line may only be reused if it still meets the
    CURRENT structural contract for its block. The signature match already
    guards against changed facts (stale date ⇒ different signature ⇒ no hit),
    so this deliberately checks only STRUCTURAL quality, not evidence-derived
    facts: a full `_draft_line_quality_errors` here would falsely reject a good
    cached line whenever today's re-fetched evidence is thinner than the day the
    line was written (its numbers/entities would look "unsupported").

    Rejects (⇒ cache miss ⇒ regenerate): mixed Latin/Cyrillic words, a generic
    call-to-action tail, and — for prose blocks only — a stub that is too short
    or single-sentence. Short-by-design blocks skip the length bar.
    """
    from news_digest.pipeline.writer import _mixed_latin_cyrillic_words  # noqa: PLC0415
    from news_digest.pipeline.editorial_contracts import scrub_vague_ending  # noqa: PLC0415

    text = re.sub(r"\s+", " ", str(draft_line or "")).strip()
    if not text:
        return False
    if _mixed_latin_cyrillic_words(text):
        return False
    _, removed = scrub_vague_ending(draft_line)
    if removed:
        return False
    if str(candidate.get("primary_block") or "") in _CACHE_SHORT_OK_BLOCKS:
        return True
    # Prose block: a real card, not a one-sentence stub.
    if len(text) < _CACHE_PROSE_MIN_CHARS:
        return False
    return len(re.findall(r"[.!?]", text)) >= 2


def _apply_translation_memory(candidates: list[dict], memory: dict) -> list[dict]:
    entries = memory.get("entries") if isinstance(memory.get("entries"), dict) else {}
    reused: list[dict] = []
    now_iso = now_london().isoformat()
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "").strip()
        if not fp or str(candidate.get("draft_line") or "").strip():
            continue
        content_hash = _candidate_content_hash(candidate)
        signature = _translation_memory_signature(candidate)
        candidates_entries = []
        fp_entry = entries.get(fp)
        content_entry = entries.get(f"content:{content_hash}")
        if isinstance(fp_entry, dict):
            candidates_entries.append(fp_entry)
        if isinstance(content_entry, dict) and content_entry is not fp_entry:
            candidates_entries.append(content_entry)
        entry = next(
            (item for item in candidates_entries if signature == str(item.get("signature") or "")),
            None,
        )
        if not isinstance(entry, dict):
            continue
        draft_line = str(entry.get("draft_line") or "").strip()
        if not draft_line.startswith("• "):
            continue
        # 0042: quality gate — do not serve a cached line that no longer meets
        # the block's contract (too short for a prose block, banned generic
        # tail, mixed-script). It becomes a cache miss and goes to the LLM.
        if not _cached_line_meets_contract(candidate, draft_line):
            candidate["translation_memory_gate"] = "contract_fail"
            continue
        candidate["draft_line"] = draft_line
        candidate["draft_line_provider"] = "translation_memory"
        candidate["draft_line_model"] = str(entry.get("model") or "")
        candidate["draft_line_written_at"] = now_iso
        candidate["translation_memory_hit"] = True
        candidate["english_reader_card"] = candidate.get("english_reader_card") or entry.get("english_reader_card") or ""
        entry["last_seen_london"] = today_london()
        entry["reuse_count"] = int(entry.get("reuse_count") or 0) + 1
        reused.append(
            {
                "fingerprint": fp,
                "content_hash": content_hash[:12],
                "title": candidate.get("title"),
                "primary_block": candidate.get("primary_block"),
                "category": candidate.get("category"),
                "model": entry.get("model"),
            }
        )
    return reused


def _update_translation_memory(candidates: list[dict], memory: dict) -> int:
    entries = memory.setdefault("entries", {})
    if not isinstance(entries, dict):
        memory["entries"] = {}
        entries = memory["entries"]
    updated = 0
    now_iso = now_london().isoformat()
    for candidate in candidates:
        fp = str(candidate.get("fingerprint") or "").strip()
        draft_line = str(candidate.get("draft_line") or "").strip()
        if not fp or not draft_line.startswith("• "):
            continue
        if str(candidate.get("category") or "") == "weather":
            continue
        provider = str(candidate.get("draft_line_provider") or "")
        if provider in {"writer_deterministic_pending"}:
            continue
        content_hash = _candidate_content_hash(candidate)
        entry_payload = {
            "signature": _translation_memory_signature(candidate),
            "content_hash": content_hash,
            "fingerprint": fp,
            "draft_line": draft_line,
            "english_reader_card": str(candidate.get("english_reader_card") or ""),
            "source_label": str(candidate.get("source_label") or ""),
            "category": str(candidate.get("category") or ""),
            "primary_block": str(candidate.get("primary_block") or ""),
            "provider": provider,
            "model": str(candidate.get("draft_line_model") or ""),
            "updated_at_london": now_iso,
            "last_seen_london": today_london(),
            "reuse_count": int((entries.get(fp) or {}).get("reuse_count") or 0),
        }
        entries[fp] = dict(entry_payload)
        content_key = f"content:{content_hash}"
        content_reuse = int((entries.get(content_key) or {}).get("reuse_count") or 0)
        entries[content_key] = dict(entry_payload, reuse_count=content_reuse)
        updated += 1
    _prune_translation_memory(memory)
    return updated


def _rewrite_inventory_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "rewrite_inventory.json"


def _rewrite_status_for_candidate(candidate: dict) -> str:
    if not candidate.get("include"):
        return "not_included"
    category = str(candidate.get("category") or "")
    if category == "weather":
        return "non_llm_weather"
    if candidate.get("translation_memory_hit"):
        return "translation_memory"
    shortlist_status = str(candidate.get("rewrite_shortlist_status") or "")
    if shortlist_status == "writer_deterministic":
        return "writer_deterministic"
    if shortlist_status == "backup_before_translation":
        return "backup_before_translation"
    provider = str(candidate.get("draft_line_provider") or "")
    if provider:
        if provider == "transport_fill":
            return "transport_fill"
        if provider == "writer_deterministic_pending":
            return "writer_deterministic"
        if provider.startswith("DeepSeek"):
            return "llm_deepseek"
        if provider.startswith("OpenAI"):
            return "llm_openai"
        return "draft_line_other"
    if str(candidate.get("english_reader_card") or "").strip():
        return "english_card_no_russian"
    return "missing_draft_line"


def _build_rewrite_inventory(candidates: list[dict]) -> dict[str, object]:
    sections: dict[str, dict[str, object]] = {}
    for block, heading in PRIMARY_BLOCKS.items():
        sections[block] = {
            "heading": heading,
            "total": 0,
            "included": 0,
            "statuses": {},
            "categories": {},
            "examples_missing": [],
        }
    sections.setdefault("unknown", {"heading": "Unknown", "total": 0, "included": 0, "statuses": {}, "categories": {}, "examples_missing": []})
    totals = {
        "candidates": 0,
        "included": 0,
        "with_draft_line": 0,
        "translation_memory": 0,
        "llm": 0,
        "writer_deterministic": 0,
        "backup_before_translation": 0,
        "missing_draft_line": 0,
    }
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        totals["candidates"] += 1
        block = str(candidate.get("primary_block") or "unknown")
        row = sections.setdefault(
            block,
            {"heading": PRIMARY_BLOCKS.get(block, block), "total": 0, "included": 0, "statuses": {}, "categories": {}, "examples_missing": []},
        )
        row["total"] = int(row.get("total") or 0) + 1
        category = str(candidate.get("category") or "unknown")
        categories = row["categories"]
        assert isinstance(categories, dict)
        categories[category] = int(categories.get(category) or 0) + 1
        if candidate.get("include"):
            totals["included"] += 1
            row["included"] = int(row.get("included") or 0) + 1
        status = _rewrite_status_for_candidate(candidate)
        statuses = row["statuses"]
        assert isinstance(statuses, dict)
        statuses[status] = int(statuses.get(status) or 0) + 1
        if str(candidate.get("draft_line") or "").strip():
            totals["with_draft_line"] += 1
        if status == "translation_memory":
            totals["translation_memory"] += 1
        elif status in {"llm_deepseek", "llm_openai"}:
            totals["llm"] += 1
        elif status == "writer_deterministic":
            totals["writer_deterministic"] += 1
        elif status == "backup_before_translation":
            totals["backup_before_translation"] += 1
        elif status in {"missing_draft_line", "english_card_no_russian"}:
            totals["missing_draft_line"] += 1
            examples = row["examples_missing"]
            assert isinstance(examples, list)
            if len(examples) < 8:
                examples.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": candidate.get("title"),
                        "category": candidate.get("category"),
                        "source_label": candidate.get("source_label"),
                    }
                )
    return {
        "schema_version": 1,
        "run_date_london": today_london(),
        "created_at_london": now_london().isoformat(),
        "totals": totals,
        "sections": sections,
    }


def _section_selection_report_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "section_selection_report.json"


def _enrichment_report_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "enrichment_report.json"


def _section_ranking_report_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "section_ranking_report.json"


_ENRICH_MAX_PER_RUN = 15


def _enrich_thin_candidates_inplace(to_rewrite: list[dict]) -> dict[str, object]:
    """Phase 6 ACTION: for thin selected candidates (headline-only evidence)
    pull the full article and replace evidence_text so the writer has real
    facts to write from. Bounded per run and fully fault-tolerant — a refetch
    failure leaves the candidate as-is and never blocks the rewrite."""
    report: dict[str, object] = {
        "attempted": 0, "enriched": 0, "failed": 0, "skipped_no_url": 0, "examples": [],
    }
    targets = [c for c in to_rewrite if isinstance(c, dict) and _needs_selection_enrichment(c)]
    if not targets:
        return report
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415
    from news_digest.pipeline.collector.extract import _extract_paragraph_evidence  # noqa: PLC0415
    from news_digest.pipeline.collector.fetch import _fetch_text  # noqa: PLC0415

    def _enrich_one(candidate: dict) -> tuple[dict, str]:
        url = str(candidate.get("source_url") or "").strip()
        if not url:
            return candidate, "__no_url__"
        try:
            return candidate, _extract_paragraph_evidence(_fetch_text(url), str(candidate.get("title") or ""))
        except Exception:  # noqa: BLE001 - one bad refetch must not block the run
            return candidate, "__failed__"

    batch = targets[:_ENRICH_MAX_PER_RUN]
    with ThreadPoolExecutor(max_workers=min(8, len(batch))) as executor:
        for candidate, full in executor.map(_enrich_one, batch):
            report["attempted"] = int(report["attempted"]) + 1
            if full == "__no_url__":
                report["skipped_no_url"] = int(report["skipped_no_url"]) + 1
                continue
            if full == "__failed__":
                report["failed"] = int(report["failed"]) + 1
                continue
            existing = str(candidate.get("evidence_text") or "")
            if full and len(full) > len(existing) + 80:
                candidate["evidence_text"] = full[:6000]
                candidate["enriched_from_source"] = True
                report["enriched"] = int(report["enriched"]) + 1
                if len(report["examples"]) < 12:
                    report["examples"].append({
                        "title": str(candidate.get("title") or "")[:80],
                        "primary_block": str(candidate.get("primary_block") or ""),
                        "chars_before": len(existing),
                        "chars_after": len(full[:6000]),
                    })
    return report


def _build_enrichment_report(candidates: list[dict], action: dict[str, object] | None = None) -> dict[str, object]:
    """Phase 6: list every candidate that selection marked needs_enrichment,
    with the facts it is missing, plus what the enrichment ACTION actually
    pulled — so the enrichment step is auditable end to end."""
    items: list[dict[str, object]] = []
    by_block: dict[str, int] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get("digest_selection_verdict") or "") != "needs_enrichment":
            continue
        block = str(candidate.get("primary_block") or "unknown")
        by_block[block] = by_block.get(block, 0) + 1
        missing = candidate.get("missing_facts") if isinstance(candidate.get("missing_facts"), list) else []
        items.append(
            {
                "fingerprint": str(candidate.get("fingerprint") or ""),
                "primary_block": block,
                "title": str(candidate.get("title") or "")[:120],
                "source_url": str(candidate.get("source_url") or ""),
                "missing_facts": [str(m)[:80] for m in missing[:8]],
                "reason": str(candidate.get("digest_selection_reason") or ""),
            }
        )
    return {
        "run_date_london": today_london(),
        "needs_enrichment_count": len(items),
        "by_block": by_block,
        "action": action or {},
        "items": items[:200],
    }


_PUBLISH_PLAN_BUDGET_BUCKETS = {
    "lead_story": "core_news",
    "last_24h": "core_news",
    "city_watch": "core_news",
    "today_focus": "today",
    "transport": "transport",
    "weekend_activities": "weekend",
    "next_7_days": "week",
    "professional_events": "professional",
    "russian_events": "russian",
    "ticket_radar": "gm_tickets",
    "outside_gm_tickets": "outside_gm",
    "openings": "food_openings",
    "tech_business": "business",
    "football": "football",
    "future_announcements": "optional_soft",
}


def _finalize_digest_selection_verdicts(candidates: list[dict]) -> dict[str, object]:
    """Fill missing product verdicts and produce a compact selection report."""
    sections: dict[str, dict[str, object]] = {}
    totals = {"selected": 0, "reserve": 0, "needs_enrichment": 0, "drop": 0, "pending": 0}
    pending_dedupe_left: list[dict[str, object]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        block = str(candidate.get("primary_block") or "unknown")
        row = sections.setdefault(
            block,
            {
                "heading": PRIMARY_BLOCKS.get(block, block),
                "total": 0,
                "selected": 0,
                "reserve": 0,
                "needs_enrichment": 0,
                "drop": 0,
                "examples": [],
            },
        )
        row["total"] = int(row.get("total") or 0) + 1
        verdict = str(candidate.get("digest_selection_verdict") or "").strip()
        if verdict not in totals:
            if candidate.get("include"):
                verdict = "selected"
                reason = "Included for the current digest."
            elif candidate.get("backup_candidate"):
                verdict = "needs_enrichment" if _needs_selection_enrichment(candidate) else "reserve"
                reason = (
                    "Held as backup because it needs more facts before writing."
                    if verdict == "needs_enrichment"
                    else "Held as same-block backup for recovery."
                )
            else:
                verdict = "drop"
                reason = str(candidate.get("reason") or "Not selected for today's digest.")
            candidate["digest_selection_verdict"] = verdict
            candidate["digest_selection_reason"] = reason
        totals[verdict] += 1
        row[verdict] = int(row.get(verdict) or 0) + 1
        reason = str(candidate.get("digest_selection_reason") or candidate.get("reason") or "")
        examples = row["examples"]
        assert isinstance(examples, list)
        if len(examples) < 8 and verdict != "selected":
            examples.append(
                {
                    "verdict": verdict,
                    "title": candidate.get("title"),
                    "source_label": candidate.get("source_label"),
                    "reason": reason,
                }
            )
        if "pending dedupe" in str(candidate.get("reason") or "").lower():
            pending_dedupe_left.append(
                {
                    "title": candidate.get("title"),
                    "source_label": candidate.get("source_label"),
                    "primary_block": block,
                    "reason": candidate.get("reason"),
                }
            )
    totals["pending"] = len(pending_dedupe_left)
    return {
        "schema_version": 1,
        "run_date_london": today_london(),
        "created_at_london": now_london().isoformat(),
        "policy": "Every candidate must have selected / reserve / needs_enrichment / drop. Pending dedupe is not a valid final state.",
        "totals": totals,
        "pending_dedupe_left": pending_dedupe_left[:40],
        "sections": sections,
    }


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
    today_date: str = "",
    diagnostics: list[dict] | None = None,
    item_builder: Callable[[list[dict]], list[dict]] | None = None,
) -> ProviderMapping:
    """Call one provider in batches. Returns fingerprint→draft_line.

    ``today_date``, when set, is injected into the user payload (NOT the
    system prompt) as ``{"today_date": ..., "candidates": [...]}`` so the
    system prefix stays byte-stable across days and DeepSeek / OpenAI can
    cache it (DeepSeek ``prompt_cache_hit_tokens``, OpenAI
    ``prompt_tokens_details.cached_tokens``). Only date-aware prompts
    pass a non-empty value.
    """
    if not candidates:
        return {}
    if not api_key:
        logger.warning("%s: API key not set, skipping.", provider_name)
        return {}

    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return {}

    # SDK retries are a balance between two failure modes we've actually hit:
    #   • max_retries=1 let a single slow response kill a whole news batch
    #     (2026-05-29: two city_news batches lost to APITimeoutError).
    #   • max_retries=4 silently AMPLIFIED the 429 storm — every rate-limited
    #     batch retried up to 4× with the server's 7-15s backoff, turning a
    #     burst into minutes of stalling (2026-06-10).
    # 2 keeps one timeout retry without the 4× 429 amplification. The
    # _API_RATE_LIMITER now prevents most 429s upfront, and our own recovery
    # ladder (same batch → split → per item) + the DeepSeek last-resort step
    # catch any residual misses.
    client = OpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=timeout,
        max_retries=0 if provider_name.lower().startswith("openai") else sdk_retries_for_route(provider=provider_name, model=model, base_url=base_url),
    )
    mapping: ProviderMapping = {}

    batches = _token_aware_batches(
        candidates,
        batch_size=batch_size,
        system_prompt=system_prompt,
        today_date=today_date,
        item_builder=item_builder if item_builder is not None else _rewrite_batch_items,
    )
    logger.info(
        "%s: %d candidates → %d token-aware batch(es), max_items≤%d.",
        provider_name,
        len(candidates),
        len(batches),
        batch_size,
    )

    def _send_once(batch: list[dict], batch_idx: int, attempt: str) -> ProviderMapping:
        batch_items = item_builder(batch) if item_builder is not None else _rewrite_batch_items(batch)
        if today_date:
            user_payload: object = {"today_date": today_date, "candidates": batch_items}
        else:
            user_payload = batch_items
        user_content = json.dumps(user_payload, ensure_ascii=False)
        # #10: record every action's timing (start/end/duration), not just
        # stage boundaries — so we can see where the rewrite minutes actually go.
        _started_at = now_london().isoformat()
        _t0 = time.monotonic()
        _queue_wait_seconds = 0.0
        _api_seconds = 0.0
        try:
            logger.info(
                "%s: batch %s/%d (%s) — sending %d candidates to %s...",
                provider_name,
                batch_idx,
                len(batches),
                attempt,
                len(batch),
                model,
            )
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ]
            # Right-size the output reservation to the batch: OpenAI counts
            # max_tokens against the per-minute ceiling, so reserving a flat
            # 8192 for a handful of short cards wasted ~3/4 of the budget and
            # tripped the TPM 429 storm. ~512 tokens/card + buffer is ample for
            # 350-450 char Russian cards without risking truncation.
            default_max_tokens = min(8192, 512 * len(batch) + 1024)
            max_tokens, max_tokens_source = _max_tokens_for_batch(prompt_name, model, len(batch), default_max_tokens)
            # Three-stage throttle: semaphore caps concurrency, the rate limiter
            # paces *when* calls start, and the token limiter keeps tokens-per-
            # minute under OpenAI's real ceiling (the actual cause of the 429s).
            with _API_SEMAPHORE:
                _queue_t0 = time.monotonic()
                _API_RATE_LIMITER.acquire()
                estimated_request_tokens = _estimate_request_tokens(messages, max_tokens)
                _API_TOKEN_LIMITER.acquire(estimated_request_tokens)
                _queue_wait_seconds = time.monotonic() - _queue_t0
                _api_t0 = time.monotonic()
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=0.3,
                    max_tokens=max_tokens,
                    **chat_completion_options_for_route(provider=provider_name, model=model, base_url=base_url),
                )
                _api_seconds = time.monotonic() - _api_t0
            from news_digest.pipeline.cost_tracker import record_call_from_response  # noqa: PLC0415
            record_call_from_response(
                response=response,
                stage="llm_rewrite",
                provider=provider_name.split("-", 1)[0],
                model=model,
                prompt_name=prompt_name,
                messages=messages,
                max_tokens=max_tokens,
            )
            raw = response.choices[0].message.content.strip()
            batch_mapping, batch_diagnostic = _parse_provider_results(
                raw=raw,
                batch=batch,
                provider_name=provider_name,
                model=model,
                prompt_name=prompt_name,
                batch_idx=batch_idx,
                total_batches=len(batches),
            )
            batch_diagnostic["attempt"] = attempt
            batch_diagnostic["started_at"] = _started_at
            batch_diagnostic["finished_at"] = now_london().isoformat()
            batch_diagnostic["duration_seconds"] = round(time.monotonic() - _t0, 3)
            batch_diagnostic["queue_wait_seconds"] = round(_queue_wait_seconds, 3)
            batch_diagnostic["api_seconds"] = round(_api_seconds, 3)
            batch_diagnostic["estimated_request_tokens"] = estimated_request_tokens
            batch_diagnostic["max_tokens_source"] = max_tokens_source
            batch_diagnostic.update(_response_token_diagnostics(response, max_tokens=max_tokens, batch_len=len(batch)))
            if diagnostics is not None:
                diagnostics.append(batch_diagnostic)
            logger.info(
                "%s: batch %s/%d (%s) → %d/%d draft_lines.",
                provider_name,
                batch_idx,
                len(batches),
                attempt,
                len(batch_mapping),
                len(batch),
            )
            if len(batch_mapping) < len(batch):
                logger.info(
                    "%s: batch %s/%d (%s) rejected_counts=%s",
                    provider_name,
                    batch_idx,
                    len(batches),
                    attempt,
                    batch_diagnostic.get("rejected_counts", {}),
                )
            return batch_mapping
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s: batch %s/%d (%s) failed — %s", provider_name, batch_idx, len(batches), attempt, exc)
            if diagnostics is not None:
                diagnostics.append(
                    {
                        "provider": provider_name,
                        "model": model,
                        "prompt_name": prompt_name,
                        "batch_index": batch_idx,
                        "batch_count": len(batches),
                        "attempt": attempt,
                        "sent": len(batch),
                        "returned_items": 0,
                        "accepted": 0,
                        "error": f"{exc.__class__.__name__}: {exc}",
                        "started_at": _started_at,
                        "finished_at": now_london().isoformat(),
                        "duration_seconds": round(time.monotonic() - _t0, 3),
                        "queue_wait_seconds": round(_queue_wait_seconds, 3),
                        "api_seconds": round(_api_seconds, 3),
                        "max_tokens": max_tokens if "max_tokens" in locals() else 0,
                        "max_tokens_source": max_tokens_source if "max_tokens_source" in locals() else "",
                        "estimated_request_tokens": estimated_request_tokens if "estimated_request_tokens" in locals() else 0,
                        "truncated": False,
                    }
                )
            return {}

    def _process_batch(batch_idx: int, batch: list[dict]) -> ProviderMapping:
        """Full recovery ladder for ONE batch (unchanged behaviour):
        initial → retry_same_batch → split → protected per-item recovery.
        Runs in its own thread; the global semaphore bounds API concurrency.
        """
        batch_result: ProviderMapping = {}
        batch_result.update(_send_once(batch, batch_idx, "initial"))
        missing = [c for c in batch if str(c.get("fingerprint") or "") not in batch_result]
        if missing and len(missing) > 1:
            split_size = max(1, min(3, len(missing) // 2 or 1))
            for split_idx in range(0, len(missing), split_size):
                split = missing[split_idx: split_idx + split_size]
                _jittered_sleep(0.5)
                split_mapping = _send_once(split, batch_idx, f"split_{split_idx // split_size + 1}")
                batch_result.update(split_mapping)
            missing = [c for c in missing if str(c.get("fingerprint") or "") not in batch_result]
        protected_missing = [c for c in missing if _is_protected_rewrite_candidate(c)]
        for item in protected_missing:
            _jittered_sleep(0.5)
            item_mapping = _send_once([item], batch_idx, "protected_item_recovery")
            batch_result.update(item_mapping)
        return batch_result

    # Fan the batches out concurrently instead of one-after-another. The
    # global _API_SEMAPHORE caps how many actually hit the API at once, so
    # wall-clock ≈ slowest batch rather than the sum of all batches.
    if len(batches) <= 1:
        for batch_idx, batch in enumerate(batches, start=1):
            mapping.update(_process_batch(batch_idx, batch))
    else:
        max_workers = min(len(batches), _REWRITE_API_CONCURRENCY)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_process_batch, batch_idx, batch)
                for batch_idx, batch in enumerate(batches, start=1)
            ]
            for future in futures:
                mapping.update(future.result())

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
    route_name: str = "rewrite",
    today_date: str = "",
    diagnostics: list[dict] | None = None,
    batch_size_override: int | None = None,
    item_builder: Callable[[list[dict]], list[dict]] | None = None,
) -> ProviderMapping:
    """Call provider chain with a specific prompt, return fingerprint→draft_line."""
    if not candidates:
        return {}
    if provider_override == "none":
        return {}
    route = resolve_model_route(
        route_name,
        provider_override=provider_override,
        base_url_override=base_url_override,
        model_override=model_override,
    )
    from news_digest.pipeline import provider_health  # noqa: PLC0415
    mapping: ProviderMapping = {}
    missing = list(candidates)
    for step in route:
        if not missing:
            break
        if provider_health.is_dead(step.provider):
            logger.info(
                "Skipping %s — circuit breaker tripped earlier this run.",
                step.provider_label,
            )
            continue
        if mapping:
            time.sleep(1)
        request_candidates = _fallback_request_candidates(
            missing,
            step_priority=step.priority,
            provider=step.provider,
            model=step.model,
            provider_override=provider_override,
            base_url_override=base_url_override,
            model_override=model_override,
        )
        if not request_candidates:
            logger.info(
                "Skipping %s fallback for %d non-lead miss(es) on route %s.",
                step.provider_label,
                len(missing),
                route_name,
            )
            break
        before = len(mapping)
        mapping.update(
            _call_provider_batch(
                step.base_url,
                step.api_key,
                step.model,
                request_candidates,
                f"{step.provider_label}{label_suffix}",
                timeout=step.timeout_seconds or 90,
                batch_size=(
                    1
                    if step.priority > 1 and not (provider_override or base_url_override or model_override)
                    else (batch_size_override or step.batch_size or BATCH_SIZE)
                ),
                system_prompt=prompt,
                prompt_name=prompt_name,
                today_date=today_date,
                diagnostics=diagnostics,
                item_builder=item_builder,
            )
        )
        if len(mapping) > before:
            provider_health.record_success(step.provider)
        else:
            provider_health.record_failure(step.provider)
        missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
        if (
            missing
            and step.provider == "openai"
            and step.priority == 1
            and route_name in {"rewrite", "events_rewrite"}
            and prompt_name not in {"force_write", "repair_draft"}
        ):
            retry_before = len(mapping)
            retry_candidates = [c for c in missing if _is_lead_candidate(c) or _is_protected_rewrite_candidate(c)]
            if not retry_candidates:
                logger.info("OpenAI rewrite retry skipped for %d optional miss(es).", len(missing))
                continue
            logger.info(
                "OpenAI rewrite retry: %d protected/lead miss(es) after primary batch; retrying one-by-one before fallback.",
                len(retry_candidates),
            )
            mapping.update(
                _call_provider_batch(
                    step.base_url,
                    step.api_key,
                    step.model,
                    retry_candidates,
                    f"{step.provider_label}{label_suffix}-retry",
                    timeout=step.timeout_seconds or 90,
                    batch_size=1,
                    system_prompt=prompt,
                    prompt_name=f"{prompt_name}_retry",
                    today_date=today_date,
                    diagnostics=diagnostics,
                    item_builder=item_builder,
                )
            )
            if len(mapping) > retry_before:
                provider_health.record_success(step.provider)
            else:
                provider_health.record_failure(step.provider)
            missing = [c for c in candidates if str(c.get("fingerprint") or "") not in mapping]
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


def _rank_report_path(project_root: Path) -> Path:
    return project_root / "data" / "state" / "rank_digest_report.json"


def run_rank_digest(project_root: Path) -> StageResult:
    """Этап 3: рейтинговая половина старого llm-rewrite, отдельной стадией.

    Runs the source-language boards (DeepSeek english cards), assigns
    ``digest_selection_verdict`` per candidate, warms the ticket notability
    cache, and persists everything into candidates.json. Writes NO Russian
    text and makes NO slot decisions: plan-digest owns composition, the
    writing half of llm-rewrite owns prose.
    """
    report_path = _rank_report_path(project_root)
    candidates_path = project_root / "data" / "state" / "candidates.json"
    _stage_t0 = time.monotonic()
    global _ACTIVE_TRANSLATION_GLOSSARY, _ACTIVE_TOKEN_BUDGET_HISTORY
    _ACTIVE_TRANSLATION_GLOSSARY = _load_translation_glossary(project_root)
    _ACTIVE_TOKEN_BUDGET_HISTORY = _load_token_budget_history(project_root)

    if not candidates_path.exists():
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
    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    model_override = os.environ.get("LLM_MODEL", "").strip()
    base_url_override = os.environ.get("LLM_BASE_URL", "").strip()
    errors: list[str] = []
    warnings: list[str] = []
    provider_batch_diagnostics: list[dict] = []

    _enrich_recurring_events(candidates)

    def _already_deterministic(c: dict) -> bool:
        line = str(c.get("draft_line") or "").strip()
        prov = str(c.get("draft_line_provider") or "")
        return bool(line and prov == "transport_fill")

    deterministic_writer_items: list[dict[str, object]] = []
    for c in candidates:
        if (
            isinstance(c, dict)
            and c.get("include")
            and str(c.get("category") or "") != "weather"
            and not _already_deterministic(c)
            and not _skip_llm_for_manual_review(c)
            and _uses_deterministic_writer(c)
        ):
            c["draft_line"] = ""
            c["draft_line_provider"] = "writer_deterministic_pending"
            c["draft_line_model"] = ""
            c["rewrite_shortlist_status"] = "writer_deterministic"
            _set_digest_selection_verdict(
                c,
                "selected",
                "Structured writer template will produce the visible line.",
            )
            deterministic_writer_items.append(
                {
                    "fingerprint": c.get("fingerprint") or "",
                    "title": c.get("title") or "",
                    "category": c.get("category") or "",
                    "primary_block": c.get("primary_block") or "",
                }
            )

    to_rank = [
        c for c in candidates
        if isinstance(c, dict) and c.get("include")
        and str(c.get("category") or "") != "weather"
        and not _already_deterministic(c)
        and not _skip_llm_for_manual_review(c)
        and not _uses_deterministic_writer(c)
    ]
    skipped_manual_review = sum(
        1 for c in candidates
        if isinstance(c, dict) and c.get("include") and _skip_llm_for_manual_review(c)
    )

    enrichment_action: dict[str, object] = {}
    cost_after_quality_guard: dict[str, object] = {
        "schema_version": 1,
        "enabled": False,
        "input_candidates": len(to_rank),
        "selected_for_model": len(to_rank),
        "held_before_model": 0,
        "held_examples": [],
    }
    rewrite_shortlist: dict[str, object] = {
        "schema_version": REWRITE_SHORTLIST_VERSION,
        "enabled": False,
        "input_candidates": len(to_rank),
        "selected_for_rewrite": len(to_rank),
        "held_for_backup": 0,
    }
    prewrite_enrichment_report: dict[str, object] = {"attempted": 0, "enriched": 0, "skipped": 0, "failed": 0}
    english_cards_applied = 0
    english_card_memory_reused: list[dict] = []
    english_card_memory = _load_english_card_memory(project_root)

    if provider_override == "none":
        warnings.append("LLM_PROVIDER=none — ranking boards skipped; verdicts finalized deterministically.")
    elif to_rank:
        enrichment_action = _enrich_thin_candidates_inplace(to_rank)
        to_rank, cost_after_quality_guard = _apply_cost_after_quality_guard(to_rank)
        to_rank, rewrite_shortlist = _apply_rewrite_shortlist(candidates, to_rank)
        prewrite_enrichment_report = _enrich_before_board(to_rank)
        english_card_memory_reused = _apply_english_card_memory(to_rank, english_card_memory)
        english_card_request = [c for c in to_rank if not str(c.get("english_reader_card") or "").strip()]
        english_card_mapping = _call_english_cards_with_fallback(
            english_card_request,
            provider_override,
            base_url_override,
            model_override,
            diagnostics=provider_batch_diagnostics,
        )
        english_cards_applied = _apply_english_cards_to_candidates(candidates, english_card_mapping, now_london().isoformat())
        candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Ticket notability warm (moved out of writer): plan-digest needs the
    # scores to make watch/public decisions before any prose exists.
    from news_digest.pipeline.ticket_notability import enrich_ticket_notability, prefetch_notability  # noqa: PLC0415

    ticket_notability_cache = project_root / "data" / "state" / "ticket_notability_cache.json"
    notability_prefetch = prefetch_notability(candidates, ticket_notability_cache)
    notability_scored = 0
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        if str(candidate.get("category") or "") != "venues_tickets" and str(candidate.get("primary_block") or "") not in {
            "ticket_radar",
            "outside_gm_tickets",
            "russian_events",
        }:
            continue
        notability = enrich_ticket_notability(candidate, ticket_notability_cache)
        candidate["ticket_notability"] = {
            "artist": notability.artist,
            "kind": notability.kind,
            "tier": notability.tier,
            "confidence": notability.confidence,
            "signal": notability.signal,
            "wikidata_id": notability.wikidata_id,
            "sitelinks": notability.sitelinks,
            "headliners": list(notability.headliners),
            "signals": notability.signals or {},
        }
        notability_scored += 1

    section_selection_report = _finalize_digest_selection_verdicts(candidates)
    write_json(_section_selection_report_path(project_root), section_selection_report)
    write_json(_enrichment_report_path(project_root), _build_enrichment_report(candidates, enrichment_action))
    write_json(
        _section_ranking_report_path(project_root),
        {"run_date_london": today_london(), **rewrite_shortlist},
    )
    candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    from news_digest.pipeline.cost_tracker import dump_stage, snapshot, summarise  # noqa: PLC0415

    dump_stage(project_root / "data" / "state", "rank_digest")
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": now_london().isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete" if not errors else "degraded",
            "errors": errors,
            "warnings": warnings,
            "ranked_pool": len(to_rank),
            "skipped_manual_review": skipped_manual_review,
            "deterministic_writer_items": {
                "count": len(deterministic_writer_items),
                "examples": deterministic_writer_items[:40],
            },
            "cost_after_quality_guard": cost_after_quality_guard,
            "rewrite_shortlist": rewrite_shortlist,
            "prewrite_enrichment": prewrite_enrichment_report,
            "english_cards": {
                "applied": english_cards_applied,
                "reused_from_content_cache": len(english_card_memory_reused),
            },
            "ticket_notability": {
                "prefetched": notability_prefetch if isinstance(notability_prefetch, (int, dict)) else {},
                "scored": notability_scored,
            },
            "selection_totals": section_selection_report.get("totals", {}),
            "rank_seconds": round(time.monotonic() - _stage_t0, 2),
            "cost_summary": summarise(snapshot(stage="rank_digest")),
            "english_card_route": route_snapshot().get("english_cards", []),
            "provider_batch_diagnostics": provider_batch_diagnostics,
        },
    )
    return StageResult(True, "Rank digest completed.", report_path)


def run_llm_rewrite(project_root: Path) -> StageResult:
    """Read candidates.json, fill Russian draft_lines for included candidates."""
    report_path = project_root / "data" / "state" / "llm_rewrite_report.json"
    candidates_path = project_root / "data" / "state" / "candidates.json"
    _stage_t0 = time.monotonic()  # #10: total wall-clock of the rewrite stage
    global _ACTIVE_TRANSLATION_GLOSSARY, _ACTIVE_TOKEN_BUDGET_HISTORY
    _ACTIVE_TRANSLATION_GLOSSARY = _load_translation_glossary(project_root)
    _ACTIVE_TOKEN_BUDGET_HISTORY = _load_token_budget_history(project_root)

    def _prompt_versions() -> list[dict[str, str]]:
        from news_digest.pipeline.prompts_meta import snapshot as prompts_snapshot  # noqa: PLC0415

        return prompts_snapshot()

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
                "prompt_versions": _prompt_versions(),
                "model_route": route_snapshot().get("rewrite", []),
                "translation_glossary": {
                    "enabled": bool(_ACTIVE_TRANSLATION_GLOSSARY),
                    "terms": len(_ACTIVE_TRANSLATION_GLOSSARY),
                },
            },
        )
        return StageResult(False, "Missing candidates.json.", report_path)

    payload = json.loads(candidates_path.read_text(encoding="utf-8"))
    pipeline_run_id = pipeline_run_id_from(payload)
    candidates = payload.get("candidates", [])

    # Этап 3: состав решён планёркой (plan-digest). Пишущая половина даёт
    # прозу ровно основным и запасным из release_plan.json и никого не
    # переотбирает. Rank-digest уже проставил вердикты и доски.
    def _already_deterministic(c: dict) -> bool:
        line = str(c.get("draft_line") or "").strip()
        prov = str(c.get("draft_line_provider") or "")
        return bool(line and prov == "transport_fill")

    release_plan = read_json(project_root / "data" / "state" / "release_plan.json", {})
    plan_write_fps: set[str] = set()
    for slot in release_plan.get("slots") or []:
        if not isinstance(slot, dict):
            continue
        fp = str(slot.get("primary_fingerprint") or "").strip()
        if fp:
            plan_write_fps.add(fp)
        for backup_fp in slot.get("backup_fingerprints") or []:
            if str(backup_fp or "").strip():
                plan_write_fps.add(str(backup_fp).strip())
    _lead_plan = release_plan.get("lead") if isinstance(release_plan.get("lead"), dict) else {}
    for fp in [_lead_plan.get("primary_fingerprint"), *(_lead_plan.get("understudy_fingerprints") or [])]:
        if str(fp or "").strip():
            plan_write_fps.add(str(fp).strip())

    deterministic_writer_items = [
        {
            "fingerprint": c.get("fingerprint") or "",
            "title": c.get("title") or "",
            "category": c.get("category") or "",
            "primary_block": c.get("primary_block") or "",
        }
        for c in candidates
        if isinstance(c, dict) and str(c.get("rewrite_shortlist_status") or "") == "writer_deterministic"
    ]
    skipped_manual_review = sum(
        1 for c in candidates
        if isinstance(c, dict) and c.get("include") and _skip_llm_for_manual_review(c)
    )
    if plan_write_fps:
        to_rewrite = [
            c for c in candidates
            if isinstance(c, dict)
            and str(c.get("fingerprint") or "") in plan_write_fps
            and str(c.get("category") or "") != "weather"
            and not _already_deterministic(c)
            and not _skip_llm_for_manual_review(c)
            and not _uses_deterministic_writer(c)
        ]
    else:
        # Fail-soft (never-block): отсутствие плана — это ошибка порядка
        # стадий, а не причина не выпустить утренний дайджест. Пишем всем
        # selected-вердиктам, verify-digest-plan отчитается о расхождении.
        to_rewrite = [
            c for c in candidates
            if isinstance(c, dict) and c.get("include")
            and str(c.get("digest_selection_verdict") or "") == "selected"
            and str(c.get("category") or "") != "weather"
            and not _already_deterministic(c)
            and not _skip_llm_for_manual_review(c)
            and not _uses_deterministic_writer(c)
        ]
    reserve_prewrite: list[dict] = []
    provider_override = os.environ.get("LLM_PROVIDER", "").lower().strip()
    model_override = os.environ.get("LLM_MODEL", "").strip()
    base_url_override = os.environ.get("LLM_BASE_URL", "").strip()
    errors: list[str] = []
    warnings: list[str] = []
    # Editorial soft warnings (weak draft_line, repair rejected, small
    # yield gap) — recorded for the audit trail but MUST NOT push
    # stage_status to "degraded" because the writer interprets that as
    # a structural failure and starts degraded_shrink.
    soft_warnings: list[str] = []
    provider_batch_diagnostics: list[dict] = []
    repair_rejections: list[dict] = []
    rewrite_shortlist: dict[str, object] = {
        "schema_version": REWRITE_SHORTLIST_VERSION,
        "enabled": False,
        "input_candidates": len(to_rewrite),
        "selected_for_rewrite": len(to_rewrite),
        "held_for_backup": 0,
        "reserve_prewrite": len(reserve_prewrite),
    }
    prewrite_enrichment_report: dict[str, object] = {
        "attempted": 0,
        "enriched": 0,
        "skipped": 0,
        "failed": 0,
    }
    post_board_translation_cut: dict[str, object] = {
        "schema_version": 1,
        "enabled": False,
        "input_candidates": 0,
        "selected_for_russian": 0,
        "held_for_backup": 0,
    }
    cost_after_quality_guard: dict[str, object] = {
        "schema_version": 1,
        "enabled": False,
        "input_candidates": len(to_rewrite),
        "selected_for_model": len(to_rewrite),
        "held_before_model": 0,
        "held_examples": [],
    }
    repair_cap_report: dict[str, object] = {
        "schema_version": 1,
        "enabled": False,
        "max_items": REPAIR_DRAFT_MAX_ITEMS_DEFAULT,
        "input_candidates": 0,
        "selected_for_repair": 0,
        "held_after_cap": 0,
        "held_examples": [],
    }
    if deterministic_writer_items:
        candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    applied = 0
    fixed = 0
    repaired = 0
    english_cards_applied = 0
    translation_memory_reused: list[dict] = []
    translation_memory_updated = 0
    translation_memory = _load_translation_memory(project_root)
    english_card_memory_reused: list[dict] = []
    english_card_memory_updated = 0
    english_card_memory = _load_english_card_memory(project_root)

    if provider_override == "none":
        logger.info("LLM_PROVIDER=none — skipping rewrite.")
        warnings.append("LLM_PROVIDER=none — rewrite stage skipped; writer/release gates will decide publishability.")
        if deterministic_writer_items:
            candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
                "cost_after_quality_guard": cost_after_quality_guard,
                "rewrite_shortlist": rewrite_shortlist,
                "deterministic_writer_items": {
                    "count": len(deterministic_writer_items),
                    "examples": deterministic_writer_items[:40],
                },
                "skipped_manual_review": skipped_manual_review,
                "applied": 0,
                "fixed": 0,
                "repaired": 0,
                "repair_cap": repair_cap_report,
                "prompt_versions": _prompt_versions(),
                "model_route": route_snapshot().get("rewrite", []),
            },
        )
        return StageResult(True, "LLM rewrite disabled; continuing with writer/release gates.", report_path)

    enrichment_action: dict[str, object] = {}
    if not to_rewrite:
        logger.info("LLM rewrite: all included candidates already have draft_lines.")
    else:
        original_rewrite_count = len(to_rewrite)
        # Обогащение тонких кандидатов перед письмом остаётся (правило
        # «сначала обогати»): писать из реальных фактов, не из заголовка.
        enrichment_action = _enrich_thin_candidates_inplace(to_rewrite)
        if int(enrichment_action.get("enriched") or 0):
            logger.info("Enrichment: pulled full text for %d thin candidate(s).", enrichment_action["enriched"])
        run_iso = now_london().isoformat()
        logger.info(
            "LLM rewrite (plan-driven): %d prose target(s) from release_plan.",
            len(to_rewrite),
        )
        translation_memory_reused = _apply_translation_memory(to_rewrite, translation_memory)
        if translation_memory_reused:
            candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            to_rewrite = [c for c in to_rewrite if not str(c.get("draft_line") or "").strip()]
            logger.info(
                "Translation memory: reused %d draft_line(s); %d candidate(s) still need LLM.",
                len(translation_memory_reused),
                len(to_rewrite),
            )

        # English-first path:
        # 1) build compact English fact/reader cards from enriched evidence;
        # 2) translate those final English cards to Russian;
        # 3) if either model layer misses an item, fall back to the legacy
        #    category prompt with full evidence. This preserves "release
        #    always goes" while moving normal quality work before translation.
        _DATE_AWARE_PROMPTS = {PROMPT_BUSINESS, PROMPT_EVENTS, PROMPT_DIASPORA_EVENTS}
        _EVENTS_PROMPTS = {PROMPT_EVENTS, PROMPT_DIASPORA_EVENTS}
        _today = today_london()

        missing_card_candidates = [c for c in to_rewrite if not str(c.get("english_reader_card") or "").strip()]
        if missing_card_candidates:
            missing_card_mapping = _call_english_cards_with_fallback(
                missing_card_candidates,
                provider_override,
                base_url_override,
                model_override,
                diagnostics=provider_batch_diagnostics,
            )
            additional_cards = _apply_english_cards_to_candidates(candidates, missing_card_mapping, now_london().isoformat())
            english_cards_applied += additional_cards
            if additional_cards:
                candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                logger.info("English-first rewrite: applied %d missing English reader cards.", additional_cards)

        english_ready = [c for c in to_rewrite if str(c.get("english_reader_card") or "").strip()]
        legacy_rewrite_pool = [c for c in to_rewrite if not str(c.get("english_reader_card") or "").strip()]

        from news_digest.pipeline.prompts_meta import prompt_name_for  # noqa: PLC0415
        mapping: ProviderMapping = {}

        if english_ready:
            logger.info("Direct Russian write: %d selected fact cards.", len(english_ready))
            mapping.update(
                _call_with_fallback(
                    english_ready,
                    FINAL_TRANSLATE_SYSTEM,
                    provider_override,
                    base_url_override,
                    model_override,
                    prompt_name="final_translate",
                    route_name="final_translate",
                    today_date=_today,
                    diagnostics=provider_batch_diagnostics,
                    item_builder=_translation_batch_items,
                )
            )
            translated_missing = [
                c for c in english_ready
                if str(c.get("fingerprint") or "") not in mapping
            ]
            if translated_missing:
                logger.info(
                    "Direct Russian write missed %d item(s); falling back to legacy category prompts.",
                    len(translated_missing),
                )
                legacy_rewrite_pool.extend(translated_missing)

        groups: dict[str, list[dict]] = {}
        for c in legacy_rewrite_pool:
            prompt = _CATEGORY_TO_PROMPT.get(str(c.get("category") or ""), PROMPT_CITY_NEWS)
            groups.setdefault(prompt, []).append(c)

        def _rewrite_group(prompt: str, group: list[dict]) -> ProviderMapping:
            logger.info("LLM rewrite: calling group of %d candidates.", len(group))
            today_for_group = _today if prompt in _DATE_AWARE_PROMPTS else ""
            if prompt == PROMPT_TRANSPORT:
                route_for_group = "transport_rewrite"  # cheap mini — short structured translation
            elif prompt in _EVENTS_PROMPTS:
                route_for_group = "events_rewrite"
            else:
                route_for_group = "rewrite"
            return _call_with_fallback(
                group, prompt, provider_override, base_url_override, model_override,
                prompt_name=prompt_name_for(prompt),
                route_name=route_for_group,
                today_date=today_for_group,
                diagnostics=provider_batch_diagnostics,
            )

        # Run the independent prompt-groups (city / events / business /
        # football) concurrently. The global _API_SEMAPHORE still bounds total
        # API concurrency across all groups, so this only removes the dead
        # wait between groups — it does not increase the request rate.
        group_items = list(groups.items())
        if group_items:
            if len(group_items) <= 1:
                for prompt, group in group_items:
                    mapping.update(_rewrite_group(prompt, group))
            else:
                with ThreadPoolExecutor(max_workers=len(group_items)) as executor:
                    futures = [executor.submit(_rewrite_group, prompt, group) for prompt, group in group_items]
                    for future in futures:
                        mapping.update(future.result())

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
        and not _skip_llm_for_manual_review(c)
        and _needs_translation_fix(str(c.get("draft_line") or ""))
    ]
    if to_fix:
        logger.info("LLM fix pass: %d English-dominant draft_lines, re-translating.", len(to_fix))
        fix_candidates = [{"fingerprint": c.get("fingerprint", ""), "draft_line": c.get("draft_line", "")} for c in to_fix]
        fix_mapping = _call_with_fallback(
            fix_candidates,
            FIX_TRANSLATE_SYSTEM,
            provider_override,
            base_url_override,
            model_override,
            label_suffix="-fix",
            prompt_name="fix_translate",
            diagnostics=provider_batch_diagnostics,
        )

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
        if isinstance(c, dict) and c.get("include")
        and not _skip_llm_for_manual_review(c)
        and _needs_quality_repair(c)
    ]
    to_repair, repair_cap_report = _cap_repair_targets(to_repair)
    if repair_cap_report.get("held_after_cap"):
        soft_warnings.append(
            "Repair cap: "
            f"{repair_cap_report['held_after_cap']} hard-defect draft_line(s) left for deterministic recovery/drop."
        )
    if to_repair:
        logger.info("LLM repair pass: %d hard-defect draft_lines, rewriting editorially.", len(to_repair))
        repair_mapping = _call_with_fallback(
            to_repair,
            REPAIR_DRAFT_SYSTEM,
            provider_override,
            base_url_override,
            model_override,
            label_suffix="-repair",
            prompt_name="repair_draft",
            route_name="repair",
            diagnostics=provider_batch_diagnostics,
        )

        run_iso = now_london().isoformat()
        for candidate in candidates:
            fp = str(candidate.get("fingerprint") or "").strip()
            if fp not in repair_mapping:
                continue
            replacement, prov, model_name = repair_mapping[fp]
            quality_errors = _writer_quality_errors(candidate, replacement)
            if replacement and not quality_errors:
                candidate["draft_line"] = replacement
                candidate["draft_line_provider"] = prov
                candidate["draft_line_model"] = model_name
                candidate["draft_line_written_at"] = run_iso
                repaired += 1
            else:
                repair_rejections.append(
                    {
                        "fingerprint": candidate.get("fingerprint"),
                        "title": candidate.get("title"),
                        "category": candidate.get("category"),
                        "primary_block": candidate.get("primary_block"),
                        "provider": prov,
                        "model": model_name,
                        "quality_errors": quality_errors,
                        "draft_line_excerpt": _diagnostic_excerpt(replacement, limit=240),
                    }
                )

        if repaired:
            candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("LLM repair pass: repaired %d hard-defect draft_lines.", repaired)

    # Forcing pass: rich-evidence hard news that the batch passes skipped. The
    # batch prompt lets the model return decision=skip on a story it could
    # easily write (8 of 12 misses on 2026-06-03 carried 1100–1500 chars of
    # evidence). Re-run those ONE AT A TIME with a prompt that forbids skip —
    # a single-item call cannot hide a lazy skip inside a batch of three.
    force_targets = [
        c for c in to_rewrite
        if isinstance(c, dict)
        and not str(c.get("draft_line") or "").strip()
        and not _has_nothing_to_write_from(c)
        and str(c.get("category") or "") in _FORCE_WRITE_CATEGORIES
        and len(re.sub(r"\s+", " ", str(c.get("evidence_text") or c.get("summary") or c.get("lead") or "")).strip()) >= _force_write_evidence_floor(c)
    ]
    forced = 0
    if force_targets:
        logger.info("LLM forcing pass: %d rich-evidence misses, single-item forced write.", len(force_targets))
        force_mapping = _call_with_fallback(
            force_targets,
            FORCE_WRITE_SYSTEM,
            provider_override,
            base_url_override,
            model_override,
            label_suffix="-force",
            prompt_name="force_write",
            route_name="repair",
            diagnostics=provider_batch_diagnostics,
            batch_size_override=1,
        )
        run_iso = now_london().isoformat()
        for candidate in candidates:
            fp = str(candidate.get("fingerprint") or "").strip()
            if fp not in force_mapping or str(candidate.get("draft_line") or "").strip():
                continue
            replacement, prov, model_name = force_mapping[fp]
            if replacement and not _writer_quality_errors(candidate, replacement):
                candidate["draft_line"] = replacement
                candidate["draft_line_provider"] = prov
                candidate["draft_line_model"] = model_name
                candidate["draft_line_written_at"] = run_iso
                forced += 1
        if forced:
            candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("LLM forcing pass: recovered %d draft_lines.", forced)

    all_missing = [
        c for c in to_rewrite
        if not str(c.get("draft_line") or "").strip()
    ]
    # Separate "the model failed" from "there was nothing to write from".
    # On 2026-05-28 all 4 missing draft_lines had evidence_text="" — the
    # source (The Mill paywall column, a bare film/tour listing) gave
    # only a headline, so every provider returned empty. That is an
    # ENRICHMENT gap, not unstable generation, and must not drag the
    # yield metric or flip stage_status to degraded.
    no_source_text = [c for c in all_missing if _has_nothing_to_write_from(c)]
    no_source_fps = {id(c) for c in no_source_text}
    missing_after = [c for c in all_missing if id(c) not in no_source_fps]
    weak_after = [
        c for c in to_rewrite
        if str(c.get("draft_line") or "").strip() and _needs_quality_repair(c)
    ]
    # Yield is measured only over items that actually had source text to
    # work with; no-source-text items are reported separately.
    rewriteable = len(to_rewrite) - len(no_source_text)
    successful = rewriteable - len(missing_after)
    # Only treat yield as structurally bad if we are below 90% — anything
    # above is a normal-day result and must NOT trigger writer
    # degraded_shrink (the trigger that held Manchester Academy tickets
    # at reader_value 800+ on 2026-05-27).
    yield_low = bool(rewriteable) and successful < max(1, int(rewriteable * 0.9))
    if rewriteable and successful < rewriteable:
        msg = f"LLM rewrite yield low after provider fallback: {successful}/{rewriteable} draft_lines written."
        # Low yield is DIAGNOSTIC only. It must stay in soft_warnings so it never
        # flips stage_status to "degraded": on 2026-05-29 a 20% miss (16/79) set
        # degraded → writer degraded_shrink HELD 13 + dropped 16 good items and the
        # digest collapsed to 34 visible. Items without a draft_line simply can't
        # render; the ones that DID rewrite must all ship (owner rule: never
        # withhold, give the full picture). yield_low is kept for the audit trail.
        soft_warnings.append(msg)
        _ = yield_low
    if no_source_text:
        soft_warnings.append(
            f"{len(no_source_text)} item(s) had no source text to write from "
            "(headline-only / paywalled source) — enrichment gap, not a generation failure."
        )
    if weak_after:
        soft_warnings.append(f"{len(weak_after)} draft_line(s) still look weak after repair.")
    # Last-resort provider visibility: items OpenAI could not write and that
    # the DeepSeek fallback caught. These ship (better than vanishing) but the
    # phrasing is weaker, so they must be auditable rather than silent.
    last_resort_writes = [
        {
            "fingerprint": c.get("fingerprint"),
            "title": c.get("title"),
            "category": c.get("category"),
            "primary_block": c.get("primary_block"),
            "provider": c.get("draft_line_provider"),
        }
        for c in to_rewrite
        if str(c.get("draft_line_provider") or "").lower().startswith("deepseek")
    ]
    if last_resort_writes:
        soft_warnings.append(
            f"{len(last_resort_writes)} draft_line(s) written by DeepSeek last-resort fallback "
            "(OpenAI missed them) — shipped to avoid vanishing, phrasing may be weaker."
        )
    if repair_rejections:
        soft_warnings.append(
            f"Repair pass rejected {len(repair_rejections)} replacement(s) that still failed writer quality gate."
        )
    english_card_memory_updated = _update_english_card_memory(candidates, english_card_memory)
    write_json(_english_card_memory_path(project_root), english_card_memory)
    translation_memory_updated = _update_translation_memory(candidates, translation_memory)
    write_json(_translation_memory_path(project_root), translation_memory)
    write_json(_enrichment_report_path(project_root), _build_enrichment_report(candidates, enrichment_action))
    candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    rewrite_inventory = _build_rewrite_inventory(candidates)
    write_json(_rewrite_inventory_path(project_root), rewrite_inventory)
    diagnostics_summary = _summarise_provider_batch_diagnostics(provider_batch_diagnostics)
    token_budget_history_summary = _update_token_budget_history(project_root, provider_batch_diagnostics)

    from news_digest.pipeline.cost_tracker import dump_stage, snapshot, summarise  # noqa: PLC0415
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
            "soft_warnings": soft_warnings,
            "included_for_rewrite": len(to_rewrite),
            "cost_after_quality_guard": cost_after_quality_guard,
            "rewrite_shortlist": rewrite_shortlist,
            "prewrite_enrichment": prewrite_enrichment_report,
            "post_board_translation_cut": post_board_translation_cut,
            "deterministic_writer_items": {
                "count": len(deterministic_writer_items),
                "examples": deterministic_writer_items[:40],
            },
            "skipped_manual_review": skipped_manual_review,
            "english_first": {
                "enabled": True,
                "deterministic_pre_board_before_judge": True,
                "pre_board_input": original_rewrite_count if "original_rewrite_count" in locals() else len(to_rewrite),
                "board_judge_input": int(rewrite_shortlist.get("selected_for_rewrite") or len(to_rewrite)),
                "cards_applied": english_cards_applied,
                "cards_reused_from_content_cache": len(english_card_memory_reused),
                "selected_for_translation": len(to_rewrite),
                "cards_missing_on_selected": sum(1 for c in to_rewrite if not str(c.get("english_reader_card") or "").strip()),
                "visible_russian_write": "direct_from_fact_card_and_evidence",
                "policy": "Deterministic per-block pre-board builds a wider source-language ranking board; DeepSeek scores that board; only then the final Russian writing board is cut. Source-language fact cards are service data only, and visible Russian is written directly from facts/evidence.",
            },
            "english_card_memory": {
                "enabled": True,
                "schema_version": ENGLISH_CARD_MEMORY_VERSION,
                "reused": len(english_card_memory_reused),
                "updated": english_card_memory_updated,
                "entries": len(english_card_memory.get("entries") or {}),
                "examples": english_card_memory_reused[:20],
                "policy": "Reuse English fact/reader cards by content hash only; changed facts, date, venue, or evidence force a fresh card.",
            },
            "translation_memory": {
                "enabled": True,
                "schema_version": TRANSLATION_MEMORY_VERSION,
                "reused": len(translation_memory_reused),
                "updated": translation_memory_updated,
                "entries": len(translation_memory.get("entries") or {}),
                "examples": translation_memory_reused[:20],
                "policy": "Reuse Russian draft_line when compact fact signature matches; content-hash entries allow safe reuse across repeated event URLs/fingerprints.",
            },
            "translation_glossary": {
                "enabled": bool(_ACTIVE_TRANSLATION_GLOSSARY),
                "terms": len(_ACTIVE_TRANSLATION_GLOSSARY),
                "path": str(_translation_glossary_path(project_root).relative_to(project_root)),
            },
            "rewrite_inventory": {
                "path": str(_rewrite_inventory_path(project_root).relative_to(project_root)),
                "totals": rewrite_inventory.get("totals", {}),
            },
            "release_plan_targets": {
                "plan_loaded": bool(plan_write_fps),
                "prose_targets": len(to_rewrite),
            },
            "applied": applied,
            "fixed": fixed,
            "repaired": repaired,
            "repair_cap": repair_cap_report,
            "repair_policy": "hard_defects_only; soft short/style issues are left for writer recovery and whole-digest final editor",
            "rewrite_seconds": round(time.monotonic() - _stage_t0, 2),
            "cost_summary": cost_summary,
            "diagnostics_summary": diagnostics_summary,
            "token_budget_history": token_budget_history_summary,
            "batching_strategy": {
                "token_aware": True,
                "english_card_token_budget": _env_int("LLM_ENGLISH_CARD_BATCH_TOKEN_BUDGET", 5400, minimum=1200, maximum=30000),
                "rewrite_token_budget": _env_int("LLM_REWRITE_BATCH_TOKEN_BUDGET", 6200, minimum=1200, maximum=30000),
                "short_card_target_batch": "6-8 items when token budget allows; heavy items split automatically.",
            },
            "concurrency_policy": {
                "current_max_concurrency": _REWRITE_API_CONCURRENCY,
                "policy": "Do not raise concurrency until token reservation p95 tightening is stable with truncated_responses=0.",
            },
            "batch_api_policy": {
                "morning_digest": "disabled",
                "nightly_use_only": True,
                "reason": "Batch API is suited to overnight non-urgent work; the 08:00 digest requires online completion.",
            },
            "prompt_versions": _prompt_versions(),
            "model_route": route_snapshot().get("rewrite", []),
            "english_card_route": route_snapshot().get("english_cards", []),
            "final_translate_route": route_snapshot().get("final_translate", []),
            "provider_batch_diagnostics": provider_batch_diagnostics,
            "repair_rejections": repair_rejections[:30],
            "last_resort_writes": last_resort_writes,
            "missing_after": [
                {
                    "fingerprint": c.get("fingerprint"),
                    "title": c.get("title"),
                    "category": c.get("category"),
                    "primary_block": c.get("primary_block"),
                }
                for c in missing_after[:30]
            ],
            "no_source_text": [
                {
                    "fingerprint": c.get("fingerprint"),
                    "title": c.get("title"),
                    "category": c.get("category"),
                    "primary_block": c.get("primary_block"),
                }
                for c in no_source_text[:30]
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
        "LLM rewrite completed."
        if not warnings
        else "LLM rewrite completed with degraded yield/quality.",
        report_path,
    )
