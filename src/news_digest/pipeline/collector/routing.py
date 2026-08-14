"""Block-routing logic: freshness, today_focus promotion, ticket horizon.

`_freshness_status` and `_resolve_primary_block` decide where a fresh
candidate lands. `_promote_to_today_focus` (and helpers) is the
"pull-up" pass that ensures Что важно сегодня has substantive material.
`_adjust_ticket_radar_block` classifies ticket opportunities.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
import re

from news_digest.pipeline.common import now_london
from news_digest.pipeline.editorial_contracts import classify_ticket_type, ticket_venue

from .dates import _parse_datetime_value
from .filters import _has_gm_token
from .sources import SourceDef
from news_digest.pipeline.block_policy import block_policy


def _freshness_status(source: SourceDef, published_at: str | None) -> str:
    """Classify a candidate's publication time relative to today's window.

    Semantically, 'fresh_24h' means 'happened yesterday or today (London)'
    rather than the literal 'within the last 24 hours' — at any scan time
    items published since yesterday midnight London count as fresh, so an
    item from yesterday afternoon is fresh whether the digest runs at
    08:00 or 18:00. The label is kept for backward compatibility with
    downstream report fields (`fresh_last_24h_count` etc.).
    """

    if source.primary_block != "last_24h":
        return "not_applicable"
    if not published_at:
        return "unknown"
    published_dt = _parse_datetime_value(published_at)
    if published_dt is None:
        return "unknown"
    now = now_london()
    if published_dt > now + timedelta(minutes=5):
        return "future"
    yesterday_midnight = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if published_dt >= yesterday_midnight:
        return "fresh_24h"
    return "stale"


def _resolve_primary_block(source: SourceDef, published_at: str | None) -> str:
    """Decide which block a candidate lands in based on source config + freshness.

    Previously stale items from last_24h-sources defaulted to ``today_focus``,
    which is the WRONG direction — "Что важно сегодня" should be today's
    fresh news, not yesterday's leftovers. Stale items now go to
    ``city_watch`` (the catch-all radar block) so today_focus stays
    reserved for promotion-pass output.
    """
    if source.primary_block != "last_24h":
        return source.primary_block
    if _freshness_status(source, published_at) == "fresh_24h":
        return "last_24h"
    return "city_watch"


_TODAY_FOCUS_KEYWORDS: tuple[str, ...] = (
    "strike",
    "industrial action",
    "walkout",
    "closure",
    "closed",
    "cancel",
    "fire",
    "blaze",
    "smoke",
    "warning",
    "evacuat",
    "police",
    "gmp",
    "stab",
    "charged",
    "arrest",
    "court",
    "election",
    "council",
    "mayor",
    "metrolink",
    "airport",
    "piccadilly",
    "victoria",
    "weather warning",
)


def _today_facing_practical_angle(candidate: dict) -> str:
    """Return a today-facing practical angle for a promoted city candidate.

    Avoids the "Включать только…" placeholder that writer.py drops as
    insufficiently actionable, and tailors the message to the topic.
    """

    blob = (
        f"{str(candidate.get('title') or '')} {str(candidate.get('summary') or '')}"
    ).lower()
    if any(token in blob for token in ("strike", "industrial action", "walkout", "cancel", "closure", "closed")):
        return "Если это касается вашего сервиса или маршрута сегодня, уточнить статус заранее."
    if any(token in blob for token in ("fire", "blaze", "smoke", "warning", "evacuat", "windows closed")):
        return "Проверить, остаётся ли предупреждение активным сегодня и касается ли оно района."
    if any(token in blob for token in ("police", "gmp", "stab", "charged", "arrest", "court")):
        return "Сверить с официальным policing update; учесть, как это влияет на район сегодня."
    if any(token in blob for token in ("election", "council", "mayor")):
        return "Учитывать, что сегодня это влияет на повестку городской политики."
    if any(token in blob for token in ("train", "metrolink", "airport", "bus", "rail")):
        return "Проверить, влияет ли это на поездки сегодня перед выходом."
    return "Сверить, остаётся ли история актуальной для сегодняшнего дня перед публикацией."


# 0159: Today наполняется по смыслу действия, а не по совпадению ключевых
# слов. Кандидат попадает в блок, только если сегодняшнее действие читателя
# следует из одного из пяти классов и в карточке есть место, затронутые люди
# и само действие. Ключевые слова ниже остаются лишь ранжированием.
_TODAY_ACTION_CLASS_RE: tuple[tuple[str, "re.Pattern[str]"], ...] = (
    (
        "restriction",
        re.compile(
            r"\b(?:clos(?:ed|ures?|ing)|shut|suspend(?:ed)?|cancel(?:led|s)?|"
            r"restrict(?:ed|ion)s?|cordon|evacuat\w*|diversion|"
            r"road\s*works?|strike|industrial action|walkout|no\s+access|"
            r"ban\s+on|bans?\s+(?:come|comes|take|takes)\s+into\s+force|"
            r"lanes?\s+(?:closed|shut|blocked)|traffic\s+(?:stopped|held)|"
            r"facing\s+delays|severe\s+delays)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "deadline",
        re.compile(
            r"\b(?:deadline|last\s+(?:day|chance)|clos(?:es|ing)\s+(?:today|on|at)|"
            r"applications?\s+clos\w*|consultation\s+(?:closes|ends|deadline)|"
            r"must\s+(?:apply|register|respond)\s+by|expires?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "service_change",
        re.compile(
            r"\b(?:new\s+rules?|comes?\s+into\s+force|takes?\s+effect|from\s+today|"
            r"changes?\s+to\s+(?:collections?|services?|opening|charges?|parking|fares?)|"
            r"service\s+change|bin\s+collections?|opening\s+hours?|reopen(?:s|ed|ing)?|"
            r"charges?\s+(?:rise|increase|apply)|timetable\s+change)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "weather_impact",
        re.compile(
            # 0191: «flooding» без погодного контекста — метафора. По ней
            # рейд с изъятием GBL («a man involved in flooding GM with…»)
            # получал класс weather_impact.
            r"\b(?:weather\s+warning|amber\s+warning|yellow\s+warning|red\s+warning|"
            r"met\s+office|flood\s+(?:warning|alert|risk)|flooded\s+\w+|"
            r"flooding\s+(?:of|expected|forecast|possible)|"
            r"heatwave|storm\s+\w+|ice\s+warning|"
            r"heavy\s+(?:rain|snow)|high\s+winds?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "active_safety",
        re.compile(
            r"\b(?:urge[ds]?\s+to\s+avoid|avoid\s+the\s+area|ongoing\s+incident|"
            r"live\s+incident|remains?\s+at\s+the\s+scene|manhunt|on\s+the\s+run|"
            r"do\s+not\s+approach|contamination|recall(?:ed)?|outbreak|"
            r"safety\s+(?:warning|alert)|unsafe)\b",
            re.IGNORECASE,
        ),
    ),
)

_TODAY_AFFECTED_PEOPLE_RE = re.compile(
    r"\b(?:residents?|passengers?|commuters?|drivers?|pupils?|students?|parents?|"
    r"patients?|customers?|shoppers?|tenants?|households?|families|staff|workers?|"
    r"visitors?|motorists?|travellers?|locals|anyone|people(?:\s+(?:in|living|who))?)\b",
    re.IGNORECASE,
)


# 0191: аудиторию можно назвать не только словом «residents». Если карточка
# называет затронутый общественный объект или маршрут, действие читателя всё
# равно конкретное («улицы оцеплены» = людям туда нельзя).
_TODAY_AFFECTED_PLACE_RE = re.compile(
    r"\b(?:street|streets|road|roads|motorway|lane|junction|line|route|"
    r"station|stop|tram|bus|train|school|schools|college|hospital|"
    r"library|park|playground|tip|recycling\s+centre|leisure\s+centre|"
    r"pool|surgery|clinic|market|car\s+park)\b",
    re.IGNORECASE,
)
# …но только пока действие ещё длится. «Closed since 2018» и судебная фаза
# прошедшего инцидента — это не сегодняшнее действие: по ним в Today
# приезжали реставрация ратуши и суд по прошлогоднему bomb hoax.
_TODAY_FINISHED_ACTION_RE = re.compile(
    r"\bsince\s+20\d{2}\b|\bmonths?\s+away\b|\byears?\s+ago\b|"
    r"\b(?:charged|convicted|sentenced|jailed|pleaded|in\s+court|trial)\b",
    re.IGNORECASE,
)
_TODAY_PLACE_AUDIENCE_CLASSES = frozenset({"restriction", "active_safety", "service_change"})


def _today_blob(candidate: dict) -> str:
    """Только собственный текст карточки.

    ``evidence_text`` тянет за собой чужую обвязку страницы (блоки «External
    links», чужие заголовки) — по ней «M60 lanes shut» приезжало в карточку
    про суд. Для решения о блоке берём только редакционные поля.
    """
    return " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle")
    )


def _today_action_class(candidate: dict) -> str:
    """Класс сегодняшнего действия или пустая строка."""
    blob = _today_blob(candidate)
    for name, pattern in _TODAY_ACTION_CLASS_RE:
        if pattern.search(blob):
            return name
    return ""


def _today_focus_native_fit(candidate: dict) -> tuple[bool, str]:
    """(годен для Today, причина отказа). Место + люди + действие обязательны."""
    action_class = _today_action_class(candidate)
    if not action_class:
        return False, "no_today_action"
    blob = _today_blob(candidate)
    if not _has_gm_token(blob.lower()):
        return False, "no_place"
    if _TODAY_FINISHED_ACTION_RE.search(blob):
        return False, "finished_action"
    # Summary and lead are deliberately compact.  The action and place above
    # must still be present in the card's own fields (so a foreign page sidebar
    # cannot create a Today story), but the article body may identify who is
    # affected just after the truncation boundary.
    affected_blob = " ".join((blob, str(candidate.get("evidence_text") or "")[:1200]))
    if not _TODAY_AFFECTED_PEOPLE_RE.search(affected_blob):
        # 0191: затронутый общественный объект или маршрут называет аудиторию
        # не хуже слова «residents» — но только у длящегося действия.
        named_place = (
            action_class in _TODAY_PLACE_AUDIENCE_CLASSES
            and _TODAY_AFFECTED_PLACE_RE.search(blob)
        )
        if not named_place:
            return False, "no_affected_people"
    return True, action_class


def _today_focus_score(candidate: dict) -> int:
    """Score a candidate's fitness for today_focus promotion.

    Higher = more important. Items must score at least 15 to be promoted,
    so we don't escalate generic stories.
    """

    score = 0
    if candidate.get("freshness_status") == "fresh_24h":
        score += 100
    elif candidate.get("freshness_status") == "stale":
        score += 5  # tolerate stale only if topical
    if candidate.get("category") == "gmp":
        score += 25
    elif candidate.get("category") == "public_services":
        score += 15
    blob = (
        f"{str(candidate.get('title') or '')} {str(candidate.get('summary') or '')}"
    ).lower()
    for keyword in _TODAY_FOCUS_KEYWORDS:
        if keyword in blob:
            score += 10
            break
    if _has_gm_token(blob):
        score += 5
    return score


_AWARENESS_TOKENS = re.compile(
    r"\b(awareness week|awareness month|mental health awareness|deaf awareness|"
    r"cancer awareness|heart awareness|diabetes awareness|stroke awareness|"
    r"week \d{4}|month \d{4})\b",
    re.IGNORECASE,
)


def _is_awareness_item(candidate: dict) -> bool:
    blob = f"{candidate.get('title', '')} {candidate.get('summary', '')}".lower()
    return bool(_AWARENESS_TOKENS.search(blob))


_TODAY_FOCUS_NORMAL_SCORE = 15
_TODAY_FOCUS_FAILSAFE_SCORE = 5  # accept weaker candidates rather than ship empty


def _promote_to_today_focus(candidates: list[dict]) -> None:
    """Ensure 'Что важно сегодня' has at least _TODAY_FOCUS_TARGET substantive items.

    Substantive = not an awareness press release and not the auto-skip
    "Включать только…" placeholder. Routine GMMH/NHS press releases
    that just happened to be tagged today_focus by their source aren't
    counted as enough — we still pull in real news on top.

    Two-pass promotion:

    1. NORMAL pass: pull candidates scoring ≥ 15 (fresh_24h news with
       GM/topical signals). Fills the bulk of the block on a normal day.

    2. FAIL-SAFE pass: if today_focus would still ship empty or with
       only 1 item after the normal pass, lower the bar to score ≥ 5
       and promote the best available media_layer/gmp/council item.
       Better a slightly off-target news in "Что важно сегодня" than
       an empty block that breaks the required-block invariant.

    0159: обе волны идут по одному смысловому шлюзу
    ``_today_focus_native_fit`` — ограничение, срок, изменение услуги,
    погодное воздействие или активная проблема безопасности, плюс место
    и затронутые люди. Балл только упорядочивает уже пригодных: подбор
    по ключевым словам давал две негодные карточки в день и пустой блок.
    """

    # Шлюз применяется и к нативным карточкам блока — иначе полнота считается
    # по строкам, которые сам блок не пропустил бы.
    _demote_unfit_native_today(candidates)

    target = int(block_policy("today_focus").get("min") or 0)
    substantive = _today_focus_substantive(candidates)
    if len(substantive) >= target:
        return

    promoted_fingerprints = {
        str(c.get("fingerprint") or "") for c in substantive
    }

    def _do_promote(threshold: int, slots: int) -> int:
        pool = []
        for c in candidates:
            if not isinstance(c, dict):
                continue
            if not c.get("include"):  # only promote items that will actually publish
                continue
            if c.get("category") not in {"media_layer", "gmp", "council", "public_services"}:
                continue
            if c.get("primary_block") not in {"last_24h", "city_watch"}:
                continue
            if c.get("promoted_to_today_focus"):
                continue
            if str(c.get("fingerprint") or "") in promoted_fingerprints:
                continue
            fit, verdict = _today_focus_native_fit(c)
            if not fit:
                c["today_focus_reject_reason"] = verdict
                continue
            c["today_action_class"] = verdict
            pool.append(c)
        pool.sort(key=_today_focus_score, reverse=True)
        promoted_count = 0
        for c in pool:
            if promoted_count >= slots:
                break
            if _today_focus_score(c) < threshold:
                break  # pool is sorted; nothing below either
            fp = str(c.get("fingerprint") or "")
            c["primary_block"] = "today_focus"
            c["promoted_to_today_focus"] = True
            c["practical_angle"] = _today_facing_practical_angle(c)
            existing = str(c.get("reason") or "").strip()
            note = (
                f"Promoted to today_focus: сегодняшнее действие "
                f"({c.get('today_action_class')}), место и затронутые люди есть."
            )
            c["reason"] = f"{existing} | {note}".strip(" |") if existing else note
            if fp:
                promoted_fingerprints.add(fp)
            promoted_count += 1
        return promoted_count

    # Pass 1 — normal threshold.
    needed = target - len(substantive)
    _do_promote(_TODAY_FOCUS_NORMAL_SCORE, needed)

    # Pass 2 — fail-safe. Recount substantive (promotion may have added some).
    substantive = _today_focus_substantive(candidates)
    if len(substantive) >= target:
        return
    needed = target - len(substantive)
    _do_promote(_TODAY_FOCUS_FAILSAFE_SCORE, needed)


_NEXT_7_PRACTICAL_CHANGE_RE = re.compile(
    r"\b(?:clos(?:e|ed|es|ing|ure)|shut|suspend(?:ed|s|ing)?|service\s+change|"
    r"route\s+change|diversion|roadworks?|works?\s+(?:start|begin)|"
    r"deadline|applications?\s+close|consultation\s+(?:closes|ends)|"
    r"vote|ballot|hearing|decision|takes?\s+effect|comes?\s+into\s+force|"
    r"fare|price|charge|fee|restriction|access\s+change|"
    r"закры|перекры|приостан|изменени[ея]\s+маршрут|работы\s+нач|"
    r"срок|дедлайн|голосован|вступ(?:ит|ает)\s+в\s+силу)\b",
    re.IGNORECASE,
)
_NEXT_7_LEISURE_RE = re.compile(
    r"\b(?:concert|gig|festival|market|fair|fete|show|exhibition|workshop|"
    r"screening|comedy|stand[-\s]?up|концерт|фестивал|ярмарк|выставк|стендап)\b",
    re.IGNORECASE,
)
_NEXT_7_SOURCE_CATEGORIES = {
    "media_layer", "gmp", "public_services", "council", "transport",
}
_NEXT_7_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
_NEXT_7_RELATIVE_WEEKDAY_RE = re.compile(
    r"\b(?:(?P<modifier>next|this|on|from|starting|starts?|begins?)\s+)"
    r"(?P<weekday>monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
    re.IGNORECASE,
)


def _next_7_reference_day(candidate: dict) -> date:
    """Publication day anchors relative weekday prose when it is available."""
    for field in ("published_at", "published_date_london"):
        parsed = _parse_datetime_value(str(candidate.get(field) or ""))
        if parsed is not None:
            return parsed.date()
    return now_london().date()


def _next_7_relative_weekday_date(candidate: dict, blob: str) -> date | None:
    """Resolve a stated future weekday without turning an ended range into news.

    ``from Monday to Thursday`` is anchored by Monday — the date on which the
    practical change starts. If Monday has already passed by the morning run,
    the item belongs in Today/current status rather than being mislabelled as a
    future Thursday change.
    """
    match = _NEXT_7_RELATIVE_WEEKDAY_RE.search(blob)
    if not match:
        return None
    reference = _next_7_reference_day(candidate)
    weekday = _NEXT_7_WEEKDAYS[match.group("weekday").lower()]
    delta = (weekday - reference.weekday()) % 7
    modifier = str(match.group("modifier") or "").lower()
    if modifier == "next" and delta == 0:
        delta = 7
    resolved = reference + timedelta(days=delta)
    return resolved if resolved >= now_london().date() else None


def _next_7_structured_date(candidate: dict) -> date | None:
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    for value in (
        event.get("date_start"),
        event.get("date"),
        candidate.get("effective_date"),
        candidate.get("deadline_date"),
    ):
        raw = str(value or "").strip()[:10]
        if not raw:
            continue
        try:
            return datetime.fromisoformat(raw).date()
        except ValueError:
            continue
    blob = " ".join(str(candidate.get(field) or "") for field in ("title", "summary", "lead"))
    for raw in re.findall(r"\b20\d{2}-\d{2}-\d{2}\b", blob):
        try:
            return datetime.fromisoformat(raw).date()
        except ValueError:
            continue
    # 0191: практическое изменение почти никогда не приходит с ISO-датой —
    # его называют прозой («set to close from Monday», «closes on 31 July»).
    # Пока принимались только структурные даты, роутер Next7 не срабатывал ни
    # разу: на пуле 28.07 все 7 практических карточек отсеивались здесь.
    # Берём ближайшую будущую дату из собственных полей карточки; окно D+2…D+7
    # проверяет вызывающий.
    try:
        from news_digest.pipeline.candidate_validator import _explicit_dates_from_blob  # noqa: PLC0415
    except Exception:  # noqa: BLE001 — карточка не должна падать из-за даты
        return None
    own_text = {
        "title": candidate.get("title"),
        "summary": candidate.get("summary"),
        "lead": candidate.get("lead"),
    }
    today = now_london().date()
    future = sorted(day for day in _explicit_dates_from_blob(own_text) if day >= today)
    if future:
        return future[0]
    return _next_7_relative_weekday_date(candidate, blob)


def route_future_practical_change(candidate: dict) -> bool:
    """Route a real D+2…D+7 practical change into Next 7.

    This is a producer contract, not a minimum-filler: ordinary crime and
    leisure never qualify merely because the section is short.
    """
    if not isinstance(candidate, dict) or not candidate.get("include"):
        return False
    if str(candidate.get("category") or "") not in _NEXT_7_SOURCE_CATEGORIES:
        return False
    if str(candidate.get("primary_block") or "") not in {
        "last_24h", "city_watch", "transport", "future_announcements",
    }:
        return False
    blob = " ".join(
        str(candidate.get(field) or "")
        for field in ("title", "summary", "lead", "practical_angle", "what_happened")
    )
    if _NEXT_7_LEISURE_RE.search(blob) or not _NEXT_7_PRACTICAL_CHANGE_RE.search(blob):
        return False
    change_day = _next_7_structured_date(candidate)
    if change_day is None:
        return False
    days_out = (change_day - now_london().date()).days
    if not 2 <= days_out <= 7:
        return False
    candidate["primary_block"] = "next_7_days"
    candidate["next_7_practical_change"] = True
    candidate["next_7_effective_date"] = change_day.isoformat()
    candidate["next_7_route_reason"] = f"future_practical_change_d{days_out}"
    existing = str(candidate.get("reason") or "").strip()
    note = f"Routed to next_7_days: practical change takes effect in {days_out} day(s)."
    candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
    return True


def _demote_unfit_native_today(candidates: list[dict]) -> list[dict]:
    """Нативная Today-карточка проходит тот же шлюз, что и повышенная.

    0170: источник может отдать карточку прямо в `today_focus`
    (`data/sources.toml`: GMMH). Такая карточка обходила
    `_today_focus_native_fit`, а `_today_focus_substantive` считала её
    полноценной без проверки действия, места и затронутых людей — блок
    выглядел заполненным, а на деле вёз пресс-релиз. Непригодная карточка
    уезжает в `city_watch` ДО подсчёта полноты; из выпуска она не теряется.
    """
    demoted: list[dict] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get("primary_block") or "") != "today_focus":
            continue
        # Шлюз проходят ВСЕ карточки блока, включая уже помеченные как
        # повышенные: флаг мог остаться в state от прошлого прогона, и тогда
        # пропуск проверки вернул бы ровно ту дыру, которую закрываем.
        fit, verdict = _today_focus_native_fit(candidate)
        if fit:
            candidate["today_action_class"] = verdict
            continue
        candidate["primary_block"] = "city_watch"
        candidate["today_focus_reject_reason"] = verdict
        existing = str(candidate.get("reason") or "").strip()
        note = f"Demoted from today_focus to city_watch: {verdict}."
        candidate["reason"] = f"{existing} | {note}".strip(" |") if existing else note
        demoted.append(candidate)
    return demoted


def _today_focus_substantive(candidates: list[dict]) -> list[dict]:
    """Today_focus items that are real news (not awareness/PR boilerplate).

    0170: считаются только публикуемые карточки. Непубликуемая строка
    заполняла счётчик полноты, и блок выглядел набранным, хотя в выпуск
    из него не уходило ничего.
    """
    return [
        c for c in candidates
        if isinstance(c, dict)
        and c.get("primary_block") == "today_focus"
        and c.get("include")
        and not str(c.get("practical_angle") or "").startswith("Включать только")
        and not _is_awareness_item(c)
    ]


_TRANSIT_DISRUPTION_RE = re.compile(
    r'\b(no\s+trams?|trams?\s+(not|won\'t)\s+(run|operate)|line\s+closure|'
    r'metrolink\s+(suspended|closed|disruption|closure|replacement|works)|'
    r'replacement\s+bus\s+service|track\s+replacement|'
    r'two\s+weeks?|several\s+weeks?)\b',
    re.IGNORECASE,
)
_TRANSIT_SUBJECT_RE = re.compile(
    r'\b(metrolink|trams?|bee\s+network|northern|transpennine)\b',
    re.IGNORECASE,
)
_TRANSIT_ROUTE_SPECIFICITY_RE = re.compile(
    r"\b(?:bury|rochdale|oldham|eccles|ashton|airport|trafford\s+park|"
    r"east\s+didsbury|altrincham)\s+line\b|"
    r"\bbetween\s+[A-Z][A-Za-z' -]{2,}\s+and\s+[A-Z][A-Za-z' -]{2,}\b|"
    r"\b(?:victoria|piccadilly|crumpsall|rochdale\s+town\s+centre|bury\s+interchange)\b",
    re.IGNORECASE,
)


def _reroute_media_transit_to_transport(candidates: list[dict]) -> None:
    """Move media_layer/city_news articles about Metrolink/transit closures to transport block.

    The TfGM live-alerts feed only covers currently-active alerts. Planned multi-day
    closures (e.g. "no trams on Bury line for two weeks") often surface first via
    media sources (The Manc, BBC Manchester) in the media_layer category. This pass
    detects them and moves them to the transport block so they sit alongside live alerts.
    """
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("include"):
            continue
        if candidate.get("primary_block") not in {"last_24h", "city_watch"}:
            continue
        if candidate.get("category") not in {"media_layer", "city_news"}:
            continue
        blob = (
            f"{str(candidate.get('title') or '')} "
            f"{str(candidate.get('summary') or '')} "
            f"{str(candidate.get('lead') or '')} "
            f"{str(candidate.get('evidence_text') or '')}"
        )
        if (
            _TRANSIT_DISRUPTION_RE.search(blob)
            and _TRANSIT_SUBJECT_RE.search(blob)
            and _TRANSIT_ROUTE_SPECIFICITY_RE.search(blob)
        ):
            candidate["primary_block"] = "transport"
            existing_reason = str(candidate.get("reason") or "").strip()
            note = "Rerouted media_layer transit disruption to transport block."
            candidate["reason"] = (
                f"{existing_reason} | {note}".strip(" |") if existing_reason else note
            )


_TICKET_DATE_PATTERN = re.compile(
    r"\b(?:mon|tue|wed|thu|fri|sat|sun)?\s*"
    r"(\d{1,2})\s+"
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"
    r"\s+(20\d{2})",
    re.IGNORECASE,
)
_TICKET_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_TICKET_HORIZON_DAYS = 540
_TICKET_RADAR_STALE_ONSALE_DAYS = 14

# Matches "event_date=YYYY-MM-DD" or "public_onsale=YYYY-MM-DD" in Ticketmaster summary fields.
_SUMMARY_ISODATE_PATTERN = re.compile(
    r'\b(event_date|public_onsale)=(\d{4}-\d{2}-\d{2})'
)


def _parse_summary_field_date(summary: str, field: str) -> datetime | None:
    """Extract a date value from the structured summary field (e.g. 'event_date=2026-10-05')."""
    for m in _SUMMARY_ISODATE_PATTERN.finditer(summary):
        if m.group(1) == field:
            try:
                d = datetime.strptime(m.group(2), "%Y-%m-%d")
                return d.replace(tzinfo=now_london().tzinfo)
            except ValueError:
                return None
    return None


def _ticket_event_max_date(title: str) -> datetime | None:
    """Return the latest event date mentioned in a venues/tickets title.

    Used to demote items whose first show is months away, so the
    Билеты / Ticket Radar block focuses on near-term on-sale moments.
    """

    latest: datetime | None = None
    for match in _TICKET_DATE_PATTERN.finditer(str(title or "")):
        day_str, month_str, year_str = match.group(1), match.group(2).lower()[:3], match.group(3)
        month = _TICKET_MONTHS.get(month_str)
        if month is None:
            continue
        try:
            candidate = datetime(int(year_str), month, int(day_str), 12, 0, tzinfo=now_london().tzinfo)
        except ValueError:
            continue
        if latest is None or candidate > latest:
            latest = candidate
    return latest


def _adjust_ticket_radar_block(candidate: dict) -> None:
    """Classify ticket items and demote only non-actionable calendar filler.

    Major venues get a 540-day radar horizon. Regular upcoming events at
    non-major venues move to normal event blocks unless they are close enough
    for planning. Older public-sale metadata still belongs in Ticket Radar
    when the event itself is upcoming: the writer must say "already on sale",
    not pretend the past sale date is future.
    """

    if candidate.get("primary_block") != "ticket_radar":
        return

    summary = str(candidate.get("summary") or "")
    today_dt = now_london()
    ticket_type = classify_ticket_type(candidate)
    onsale_dt = _parse_summary_field_date(summary, "public_onsale")
    event_dt = _parse_summary_field_date(summary, "event_date")

    if (
        onsale_dt is not None
        and onsale_dt < today_dt - timedelta(days=3)
        and ticket_type in {"major_upcoming", "regular_upcoming"}
    ):
        candidate["ticket_type"] = "old_public_sale"
        existing_reason = str(candidate.get("reason") or "").strip()
        note = (
            f"Ticket public sale opened {(today_dt - onsale_dt).days} day(s) ago; "
            "kept in ticket_radar as already-on-sale coverage."
        )
        candidate["reason"] = f"{existing_reason} | {note}".strip(" |") if existing_reason else note
        return

    if "ticket_signal=onsale" in summary.lower():
        if onsale_dt is not None and onsale_dt < today_dt:
            # The on-sale window already opened. Keep very recent on-sales in
            # Ticket Radar, but move older ones out so the block stays about
            # new opportunities rather than a stale event calendar.
            age_days = (today_dt - onsale_dt).days
            if age_days <= 3:
                candidate["ticket_type"] = "on_sale_now"
                return
            if event_dt is None or event_dt < today_dt - timedelta(days=1):
                candidate["include"] = False
                candidate["reason"] = (
                    "Onsale date is in the past and event date has passed or is missing."
                )
            else:
                days_out = (event_dt - today_dt).days
                candidate["primary_block"] = "future_announcements"
                candidate["ticket_type"] = "old_onsale"
                if age_days > _TICKET_RADAR_STALE_ONSALE_DAYS:
                    candidate["editorial_status"] = "borderline"
                    candidate["quality_warnings"] = sorted(set(
                        [str(r) for r in candidate.get("quality_warnings") or [] if str(r).strip()]
                        + [f"ticket_old_onsale:{age_days}d"]
                    ))
                existing_reason = str(candidate.get("reason") or "").strip()
                note = (
                    f"Onsale opened {age_days} day(s) ago; event ~{days_out} day(s) away, "
                    "moved out of ticket_radar."
                )
                candidate["reason"] = f"{existing_reason} | {note}".strip(" |") if existing_reason else note
        else:
            candidate["ticket_type"] = ticket_type
        return

    title = str(candidate.get("title") or "")
    latest = _ticket_event_max_date(title)
    if latest is None:
        return
    if latest < today_dt - timedelta(days=1):
        candidate["include"] = False
        candidate["reason"] = (
            "Ticket radar candidate excluded because all dated occurrences are in the past."
        )
        return
    days_out = (latest - today_dt).days
    if days_out > _TICKET_HORIZON_DAYS:
        candidate["primary_block"] = "future_announcements"
        existing_reason = str(candidate.get("reason") or "").strip()
        note = f"Demoted from ticket_radar: earliest date is ~{days_out} day(s) away."
        candidate["reason"] = f"{existing_reason} | {note}".strip(" |") if existing_reason else note
        return

    # Non-onsale upcoming events only deserve the Ticket Radar slot if they
    # are major enough. Small/unknown venues stay in normal event planning
    # blocks, so Ticket Radar reads like "act on tickets" rather than "all
    # gigs in date order".
    candidate["ticket_type"] = ticket_type
    if ticket_type == "regular_upcoming":
        # 0160: Next7 — недосуговый блок. Обычный будущий билет уезжает в
        # Future независимо от близости даты; досуг живёт в Weekend/Ticket/Future.
        candidate["primary_block"] = "future_announcements"
        existing_reason = str(candidate.get("reason") or "").strip()
        venue = ticket_venue(candidate)
        note = f"Regular upcoming ticket at non-major venue ({venue}); moved out of ticket_radar."
        candidate["reason"] = f"{existing_reason} | {note}".strip(" |") if existing_reason else note
