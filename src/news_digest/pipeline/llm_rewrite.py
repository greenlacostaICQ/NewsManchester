"""LLM rewrite stage — writes Russian draft_lines to candidates.json.

Default model route:
  1. OpenAI gpt-4o-mini    — quality rewrite primary
  2. Rule-based in writer.py — final safety net for structured tickets/events/transport

Required env vars (set in GitHub Actions Secrets or .env.local):
  OPENAI_API_KEY    — platform.openai.com (paid, gpt-4o-mini)

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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

from news_digest.pipeline.common import now_london, pipeline_run_id_from, today_london, write_json
from news_digest.pipeline.model_routing import (
    DEEPSEEK_BASE_URL,
    DEEPSEEK_MODEL,
    GROQ_BASE_URL,
    GROQ_FALLBACK_MODEL,
    OPENAI_BASE_URL,
    OPENAI_REWRITE_MODEL,
    resolve_model_route,
    route_snapshot,
)
from news_digest.pipeline.reader_value import reader_value_score
from news_digest.pipeline.story_intelligence import apply_story_intelligence, section_board_score

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StageResult:
    ok: bool
    message: str
    report_path: Path


OPENAI_MODEL = OPENAI_REWRITE_MODEL

ProviderMapping = dict[str, tuple[str, str, str]]

REWRITE_SHORTLIST_VERSION = 2
REWRITE_SHORTLIST_CAPS_BY_BLOCK: dict[str, int] = {
    "transport": 99,
    "lead_story": 4,
    "today_focus": 6,
    "last_24h": 10,
    "city_watch": 8,
    "weekend_activities": 10,
    "next_7_days": 8,
    "ticket_radar": 8,
    "future_announcements": 4,
    "outside_gm_tickets": 4,
    "russian_events": 6,
    "openings": 6,
    "tech_business": 5,
    "football": 3,
}
REWRITE_SHORTLIST_DEFAULT_CAP = 6

# #9 Soft global ceiling on how many candidates we translate per run. Even
# with per-block caps, the protected/uncapped lane could push the board to 76+
# (2026-06-03), which bloated the digest with thin cards AND slowed rewrite.
# This is a SOFT cap, not a hard one: we keep a per-block floor so no section
# is starved (the failure mode the owner flagged — "новости отсеялись, доп не
# перевёл, провал"), and the lead / transport / today_focus core is never cut.
# Items above the ceiling are not deleted — they stay as backup reserve.
REWRITE_TRANSLATION_BOARD_MAX = 45
_REWRITE_BLOCK_FLOOR = 3

_PROMPT_FOOTER = (
    '\nВерни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "decision": "write|needs_enrichment|skip", '
    '"draft_line": "• ...", "missing_facts": []}]\n'
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
    "в rewrite_packet; нельзя добавлять факт, которого нет ни в packet, ни в evidence.\n\n"
    "ПРАВИЛО ПУСТОТЫ: если evidence_text короче ~120 символов осмысленного текста, ИЛИ содержит только тизер/анонс без сути "
    "(«One of Manchester's most iconic…», «Details to follow», paywall-stub, «приобретено северное мероприятие» без названия "
    "сделки/сумм/имён), ИЛИ ты понимаешь, что для самодостаточного пункта нужно бы что-то домыслить — верни draft_line=\"\". "
    "Лучше пустая строка, чем туманная карточка вида «Совет призывает сказать нет нелегальному кредитованию» без сути.\n"
    "НО НЕ НАОБОРОТ: если evidence_text длинный (≥400 символов) и в нём есть конкретные факты (имена, цифры, район, дата, причина, "
    "решение) — ПУСТАЯ СТРОКА ЗАПРЕЩЕНА, decision=\"skip\" запрещён. Материала достаточно, твоя работа — извлечь главный факт и одну "
    "деталь и написать пункт. Skip оправдан только когда evidence реально пустой/тизерный, а не когда тебе лень разбирать длинный текст.\n\n"
    "ПОЯСНЯЙ НЕЗНАКОМОЕ: если в evidence есть короткое пояснение, кто или что это (напр. «popular Dutch DJ», должность, расшифровка "
    "аббревиатуры) — добавь его при ПЕРВОМ упоминании незнакомого имени/аббревиатуры (ANOTR — голландский диджей; UMIST — бывший "
    "технический университет). Бери пояснение ТОЛЬКО из evidence, не выдумывай. Читатель не должен гуглить, кто это.\n\n"
    "СОЧУВСТВЕННЫЕ ШТАМПЫ: не вставляй пустые соболезнования без новостной нагрузки («ушла слишком рано», «мысли с семьёй», "
    "«любили все, кто знал») — они не несут факта. НО если цитата принадлежит ключевой фигуре события ИЛИ сама по себе является "
    "новостью (официальное заявление, слова центрального участника) — оставь её. Режется пустая эмоция от периферийных людей, "
    "а не значимая цитата.\n\n"
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

# Speed: batches and prompt-groups run concurrently (canonical "fan out the
# API calls" pattern) instead of one-after-another, so the rewrite stage's
# wall-clock is the SLOWEST single batch, not the sum of all of them. This
# does NOT change per-batch behaviour — timeout, max_retries, batch_size and
# the DeepSeek last-resort step are all untouched. A single global semaphore
# caps how many requests hit the API at once across every group/batch/thread,
# so we get the speed-up without tripping OpenAI rate limits (429). 8 is safe
# for Tier 1/2; override with LLM_REWRITE_MAX_CONCURRENCY if the tier allows.
try:
    _REWRITE_API_CONCURRENCY = max(1, int(os.environ.get("LLM_REWRITE_MAX_CONCURRENCY", "8") or "8"))
except ValueError:
    _REWRITE_API_CONCURRENCY = 8
_API_SEMAPHORE = threading.Semaphore(_REWRITE_API_CONCURRENCY)

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

ПО ТИПАМ:
- council deadlock / council vote: объясни, что заблокировано, кто/какой орган не договорился, стоимость/последствие и следующую дату.
- retail closure / takeover: что закрывается, кто заменяет, где, когда, что меняется для жителей района.
- change-of-use / former building: текущее решение первым; старую дату закрытия давай только как фон, не как новость.
- transport: не оставляй «небольшие задержки» без линии/участка, если они есть в title/summary; если участок не указан источником, прямо скажи «TfGM не уточнил участок».

ЖЁСТКОЕ ПРАВИЛО ДЛИНЫ (long-format категории): минимум ДВА самостоятельных предложения и ≥150 символов. Первое — главный факт; второе — конкретная деталь из evidence_text (имя/сумма/дата суда/адрес/последствие). НЕ упаковывай всё в одно длинное предложение — раздели на два. Бери вторую деталь из evidence_text, она почти всегда там есть.
ЕСЛИ в evidence_text реально только заголовок без второй фактуры — НЕ выдумывай и НЕ добавляй заглушку; верни draft_line пустой строкой "", такой пункт честно не пойдёт в выпуск (это пробел обогащения, а не слабый текст).
ЕСЛИ данных мало, но они есть — пиши 250–300 символов с реальной фактурой. Лучше точный короткий пункт, чем раздутый пустой.

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


def _writer_quality_errors(candidate: dict, line: str) -> list[str]:
    from news_digest.pipeline.writer import _draft_line_quality_errors  # noqa: PLC0415

    return _draft_line_quality_errors(candidate, line)


def _skip_llm_for_manual_review(candidate: dict) -> bool:
    """Do not spend model calls on items the writer will hold anyway."""
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
    if category == "venues_tickets":
        return True
    if category in {"culture_weekly", "food_openings", "russian_speaking_events", "diaspora_events"} and _is_market_or_recurring_event(candidate):
        return True
    return False


def _append_reason(candidate: dict, note: str) -> None:
    existing = str(candidate.get("reason") or "").strip()
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note


def _rewrite_shortlist_priority(candidate: dict) -> tuple[float, float, float, str]:
    apply_story_intelligence(candidate)
    lead_bonus = 1000.0 if candidate.get("is_lead") else 0.0
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    protected_bonus = 250.0 if protected.get("protected") else 0.0
    return (
        lead_bonus + protected_bonus,
        float(section_board_score(candidate)),
        float(reader_value_score({**candidate, "included": True})),
        str(candidate.get("title") or ""),
    )


def _must_translate_before_cap(candidate: dict) -> bool:
    apply_story_intelligence(candidate)
    if candidate.get("is_lead"):
        return True
    block = str(candidate.get("primary_block") or "")
    category = str(candidate.get("category") or "")
    contract = candidate.get("editorial_contract") if isinstance(candidate.get("editorial_contract"), dict) else {}
    tier = str(contract.get("publish_tier") or candidate.get("publish_tier") or "")
    protected = candidate.get("protected_lane") if isinstance(candidate.get("protected_lane"), dict) else {}
    if block in {"transport", "today_focus"}:
        return True
    if category in {"media_layer", "council", "gmp", "public_services", "city_news"} and tier in {"must_include", "strong"}:
        return True
    if protected.get("protected") and category != "venues_tickets":
        return True
    return False


def _apply_rewrite_shortlist(candidates: list[dict], to_rewrite: list[dict]) -> tuple[list[dict], dict[str, object]]:
    """Select the English-scored candidates worth paying to translate.

    This is the cutover from "translate the broad included pool" to
    "judge/score first, translate only the publishable shortlist". Items
    held back are not deleted: they are marked as backup candidates so the
    release report and backup_pool can explain what was not translated.
    """
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
        for candidate in normal_group[cap:]:
            candidate["include"] = False
            candidate["backup_candidate"] = True
            candidate["rewrite_shortlist_status"] = "backup_before_rewrite"
            candidate["rewrite_shortlist_reason"] = f"Outside pre-rewrite shortlist for {block or 'unknown'}."
            _append_reason(candidate, candidate["rewrite_shortlist_reason"])
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

    # #9 Soft global ceiling. Trim the board to REWRITE_TRANSLATION_BOARD_MAX
    # by priority, but guarantee: (a) the never-drop core (lead / transport /
    # today_focus) always stays, and (b) every block that had material keeps
    # at least _REWRITE_BLOCK_FLOOR items — so a section is never starved.
    # Overflow is demoted to backup reserve (not deleted), exactly like held.
    board_overflow = 0
    if len(selected) > REWRITE_TRANSLATION_BOARD_MAX:
        def _never_drop(c: dict) -> bool:
            return bool(c.get("is_lead")) or str(c.get("primary_block") or "") in {"transport", "today_focus"}

        keep_ids: set[int] = {id(c) for c in selected if _never_drop(c)}
        # Per-block floor: keep the top _REWRITE_BLOCK_FLOOR of each block.
        by_block: dict[str, list[dict]] = {}
        for c in selected:
            by_block.setdefault(str(c.get("primary_block") or ""), []).append(c)
        for _block, items in by_block.items():
            for c in sorted(items, key=_rewrite_shortlist_priority, reverse=True)[:_REWRITE_BLOCK_FLOOR]:
                keep_ids.add(id(c))
        # Fill the rest of the budget by global priority.
        room = REWRITE_TRANSLATION_BOARD_MAX - len(keep_ids)
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
            candidate["rewrite_shortlist_status"] = "backup_board_cap"
            candidate["rewrite_shortlist_reason"] = (
                f"Outside global translation board (soft max {REWRITE_TRANSLATION_BOARD_MAX})."
            )
            _append_reason(candidate, candidate["rewrite_shortlist_reason"])
            selected_ids.discard(id(candidate))
            board_overflow += 1
            held.append(
                {
                    "fingerprint": candidate.get("fingerprint") or "",
                    "title": candidate.get("title") or "",
                    "source_label": candidate.get("source_label") or "",
                    "category": candidate.get("category") or "",
                    "primary_block": str(candidate.get("primary_block") or ""),
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
        "board_max": REWRITE_TRANSLATION_BOARD_MAX,
        "caps_by_block": caps,
        "uncapped_selected": len(uncapped_selected),
        "uncapped_examples": uncapped_selected[:20],
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
    "media_layer", "gmp", "council", "public_services", "tech_business", "city_news",
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

Верни ТОЛЬКО JSON-массив: [{"fingerprint": "...", "draft_line": "• ..."}]
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


def _needs_quality_repair(candidate: dict) -> bool:
    line = str(candidate.get("draft_line") or "").strip()
    if not line:
        return False
    category = str(candidate.get("category") or "")
    primary_block = str(candidate.get("primary_block") or "")
    if category not in _LONG_FORMAT_CATEGORIES_FOR_REPAIR | {"transport"}:
        return False
    writer_errors = _writer_quality_errors(candidate, line)
    if writer_errors:
        return True
    normalized = re.sub(r"\s+", " ", line)
    lowered = normalized.lower()
    sentence_count = len(re.findall(r"[.!?]", normalized))
    # A complete dated event card (real event + date) is allowed to be concise:
    # the writer accepts it at the lower DATED_EVENT floor, so re-flagging it
    # "weak" here only churns repairs and inflates weak_after for cards that
    # will publish fine (David Gray, Grand Soul Day Party on 2026-06-03). The
    # writer_errors check above is already authoritative for these.
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    has_event_date = bool(event.get("is_event") and str(event.get("date_start") or event.get("date") or "").strip())
    # Long-format card: must hit ≥150 chars AND ≥2 sentences. Anything
    # shorter is still a headline and will be blocked by the writer.
    if category in _LONG_FORMAT_CATEGORIES_FOR_REPAIR and not has_event_date:
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
) -> ProviderMapping:
    """Call one provider in batches. Returns fingerprint→draft_line.

    ``today_date``, when set, is injected into the user payload (NOT the
    system prompt) as ``{"today_date": ..., "candidates": [...]}`` so the
    system prefix stays byte-stable across days and DeepSeek / OpenAI can
    cache it (DeepSeek ``prompt_cache_hit_tokens``, OpenAI
    ``prompt_tokens_details.cached_tokens``). Only date-aware prompts
    pass a non-empty value.
    """
    if not api_key:
        logger.warning("%s: API key not set, skipping.", provider_name)
        return {}

    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return {}

    # Let the SDK absorb transient slowness with exponential backoff — this
    # is what high-volume OpenAI users rely on. max_retries=1 (the old value)
    # meant a single slow response killed a whole batch of news; on
    # 2026-05-29 two city_news batches vanished to APITimeoutError that way.
    # 4 retries with backoff makes OpenAI itself resilient before we ever
    # need the visible recovery ladder (same batch → split → per item) or the
    # DeepSeek last-resort step.
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout, max_retries=4)
    mapping: ProviderMapping = {}

    batches = [candidates[i: i + batch_size] for i in range(0, len(candidates), batch_size)]
    logger.info("%s: %d candidates → %d batch(es) of ≤%d.", provider_name, len(candidates), len(batches), batch_size)

    def _send_once(batch: list[dict], batch_idx: int, attempt: str) -> ProviderMapping:
        batch_items = _rewrite_batch_items(batch)
        if today_date:
            user_payload: object = {"today_date": today_date, "candidates": batch_items}
        else:
            user_payload = batch_items
        user_content = json.dumps(user_payload, ensure_ascii=False)
        # #10: record every action's timing (start/end/duration), not just
        # stage boundaries — so we can see where the rewrite minutes actually go.
        _started_at = now_london().isoformat()
        _t0 = time.monotonic()
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
            max_tokens = 8192
            # Global limiter: caps concurrent API calls across all parallel
            # groups/batches so the fan-out never exceeds the rate limit.
            with _API_SEMAPHORE:
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=0.3,
                    max_tokens=max_tokens,
                )
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
        if missing:
            time.sleep(0.5)
            retry_mapping = _send_once(missing, batch_idx, "retry_same_batch")
            batch_result.update(retry_mapping)
            missing = [c for c in missing if str(c.get("fingerprint") or "") not in retry_mapping]
        if missing and len(missing) > 1:
            split_size = max(1, min(3, len(missing) // 2 or 1))
            for split_idx in range(0, len(missing), split_size):
                split = missing[split_idx: split_idx + split_size]
                time.sleep(0.5)
                split_mapping = _send_once(split, batch_idx, f"split_{split_idx // split_size + 1}")
                batch_result.update(split_mapping)
            missing = [c for c in missing if str(c.get("fingerprint") or "") not in batch_result]
        protected_missing = [c for c in missing if _is_protected_rewrite_candidate(c)]
        for item in protected_missing:
            time.sleep(0.5)
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
        before = len(mapping)
        mapping.update(
            _call_provider_batch(
                step.base_url,
                step.api_key,
                step.model,
                missing,
                f"{step.provider_label}{label_suffix}",
                timeout=step.timeout_seconds or 90,
                batch_size=batch_size_override or step.batch_size or BATCH_SIZE,
                system_prompt=prompt,
                prompt_name=prompt_name,
                today_date=today_date,
                diagnostics=diagnostics,
            )
        )
        if len(mapping) > before:
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


def run_llm_rewrite(project_root: Path) -> StageResult:
    """Read candidates.json, fill Russian draft_lines for included candidates."""
    report_path = project_root / "data" / "state" / "llm_rewrite_report.json"
    candidates_path = project_root / "data" / "state" / "candidates.json"
    _stage_t0 = time.monotonic()  # #10: total wall-clock of the rewrite stage

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

    # Don't overwrite draft_lines that the deterministic transport_fill
    # stage already produced (provider="transport_fill"). LLM tier-3 only
    # fires for transport candidates the extractor couldn't handle —
    # those leave draft_line empty so the filter below still grabs them.
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
            deterministic_writer_items.append(
                {
                    "fingerprint": c.get("fingerprint") or "",
                    "title": c.get("title") or "",
                    "source_label": c.get("source_label") or "",
                    "category": c.get("category") or "",
                    "primary_block": c.get("primary_block") or "",
                    "reason": "Structured ticket/market/event template; skipped LLM rewrite for speed and consistency.",
                }
            )

    to_rewrite = [
        c for c in candidates
        if isinstance(c, dict) and c.get("include")
        and str(c.get("category") or "") != "weather"  # handcrafted line, no LLM needed
        and not _already_deterministic(c)
        and not _skip_llm_for_manual_review(c)
        and not _uses_deterministic_writer(c)
    ]
    skipped_manual_review = sum(
        1 for c in candidates
        if isinstance(c, dict) and c.get("include") and _skip_llm_for_manual_review(c)
    )
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
    }
    if deterministic_writer_items:
        candidates_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    applied = 0
    fixed = 0
    repaired = 0

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
                "rewrite_shortlist": rewrite_shortlist,
                "deterministic_writer_items": {
                    "count": len(deterministic_writer_items),
                    "examples": deterministic_writer_items[:40],
                },
                "skipped_manual_review": skipped_manual_review,
                "applied": 0,
                "fixed": 0,
                "repaired": 0,
                "prompt_versions": _prompt_versions(),
                "model_route": route_snapshot().get("rewrite", []),
            },
        )
        return StageResult(True, "LLM rewrite disabled; continuing with writer/release gates.", report_path)

    if not to_rewrite:
        logger.info("LLM rewrite: all included candidates already have draft_lines.")
    else:
        original_rewrite_count = len(to_rewrite)
        to_rewrite, rewrite_shortlist = _apply_rewrite_shortlist(candidates, to_rewrite)
        if rewrite_shortlist["held_for_backup"]:
            # Holding lower-priority candidates in backup before
            # translation is NORMAL cost control, not a failure. It must
            # NOT push stage_status to "degraded" — that flipped the
            # writer into degraded_shrink on 2026-05-28 and made the
            # report say "генерация работала нестабильно" at 92% yield.
            soft_warnings.append(
                "Rewrite shortlist: "
                f"{rewrite_shortlist['held_for_backup']} candidate(s) held in backup before translation."
            )
        logger.info(
            "LLM rewrite: %d/%d candidates selected for GPT rewrite; %d held in backup.",
            len(to_rewrite),
            original_rewrite_count,
            rewrite_shortlist["held_for_backup"],
        )

        # Group by prompt type and call each group separately.
        # TODAY_DATE is passed via the user payload (not the system prompt)
        # so the system prefix is byte-stable across days and DeepSeek /
        # OpenAI prompt caching can reuse it on day N+1. Only date-aware
        # prompts get a non-empty today_date — others stay on the legacy
        # bare-list payload shape.
        _DATE_AWARE_PROMPTS = {PROMPT_BUSINESS, PROMPT_EVENTS, PROMPT_DIASPORA_EVENTS}
        _EVENTS_PROMPTS = {PROMPT_EVENTS, PROMPT_DIASPORA_EVENTS}
        _today = today_london()

        groups: dict[str, list[dict]] = {}
        for c in to_rewrite:
            prompt = _CATEGORY_TO_PROMPT.get(str(c.get("category") or ""), PROMPT_CITY_NEWS)
            groups.setdefault(prompt, []).append(c)

        from news_digest.pipeline.prompts_meta import prompt_name_for  # noqa: PLC0415
        mapping: ProviderMapping = {}

        def _rewrite_group(prompt: str, group: list[dict]) -> ProviderMapping:
            logger.info("LLM rewrite: calling group of %d candidates.", len(group))
            today_for_group = _today if prompt in _DATE_AWARE_PROMPTS else ""
            route_for_group = "events_rewrite" if prompt in _EVENTS_PROMPTS else "rewrite"
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
            logger.info("LLM repair pass: repaired %d weak draft_lines.", repaired)

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
        and len(re.sub(r"\s+", " ", str(c.get("evidence_text") or "")).strip()) >= 400
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
            route_name="rewrite",
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
            "rewrite_shortlist": rewrite_shortlist,
            "deterministic_writer_items": {
                "count": len(deterministic_writer_items),
                "examples": deterministic_writer_items[:40],
            },
            "skipped_manual_review": skipped_manual_review,
            "applied": applied,
            "fixed": fixed,
            "repaired": repaired,
            "rewrite_seconds": round(time.monotonic() - _stage_t0, 2),
            "cost_summary": cost_summary,
            "prompt_versions": _prompt_versions(),
            "model_route": route_snapshot().get("rewrite", []),
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
