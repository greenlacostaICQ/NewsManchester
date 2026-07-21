"""Этап 3: планёрка. Состав выпуска решается здесь один раз.

Детерминированная стадия между rank-digest и llm-rewrite: без сети и без
моделей. Читает кандидатов с вердиктами/баллами (rank-digest), историю
публикаций (repeat-политика) и день недели; выпускает неизменяемый
слот-план ``release_plan.json``:

* слоты по разделам в порядке выпуска + цепочка запасных на слот;
* главная история + 2 дублёра из-под публичной границы отбора;
* лимиты разделов и бюджета выпуска применяются здесь и только здесь;
* повторы гасятся здесь (а не вырезаются из готового HTML на release);
* недобор ниже минимума фиксируется честной причиной в самом плане.

После записи плана writer только исполняет слоты, editor правит слова, pre-send
repair executor может заменить слот запасным ИЗ ПЛАНА или снять его по
кодифицированной причине, а verify только сверяет результат.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
from pathlib import Path
import re

from news_digest.pipeline.common import (
    PRIMARY_BLOCKS,
    SECTION_MAX_ITEMS,
    SECTION_MAX_PER_SOURCE,
    SECTION_MIN_ITEMS,
    fingerprint_for_candidate,
    now_london,
    pipeline_run_id_from,
    read_json,
    today_london,
    write_json,
    write_json_atomic,
)
from news_digest.pipeline.plan_execution import plan_path
from news_digest.pipeline.ticket_notability import a_tier_ticket_policy, is_a_tier_ticket

logger = logging.getLogger(__name__)

PLAN_SCHEMA_VERSION = 1
PLAN_POLICY_VERSION = 1

# Глубина цепочки запасных на слот: 3 для стержневых разделов, 2 остальным,
# 0 синтетике (Погода/Транспорт — их производит детерминированный контур).
SPINE_SECTIONS = frozenset({"Свежие новости", "Что важно сегодня", "Городской радар", "Еда, открытия и рынки"})
BACKUP_DEPTH_SPINE = 3
BACKUP_DEPTH_DEFAULT = 2
SYNTHETIC_SECTIONS = frozenset({"Погода", "Общественный транспорт сегодня"})
LEAD_UNDERSTUDY_COUNT = 2
LEAD_UNDERSTUDY_SOURCE_BLOCKS = ("today_focus", "last_24h", "city_watch")

# Бюджет выпуска — те же константы, что жили в writer (переехали сюда).
PLAN_PUBLIC_MAX_VISIBLE_ITEMS = 40
PLAN_PUBLIC_HARD_RENDERED_ITEMS = 52

_MINOR_BUS_STOP_MAX = 3


@dataclass
class StageResult:
    ok: bool
    message: str
    report_path: Path


def _planned_transport_status_candidate(state_dir: Path) -> tuple[dict | None, str]:
    """Create the neutral transport card only from a fresh, complete TfGM scan.

    It is admitted only when no concrete transport card survives planning, so it
    can never replace or sit beside a specific restriction.
    """
    report = read_json(state_dir / "collector_report.json", {})
    if str(report.get("run_date_london") or "") != today_london():
        return None, "collector_report_not_fresh"
    transport = (report.get("categories") or {}).get("transport") or {}
    if not isinstance(transport, dict) or not transport.get("checked"):
        return None, "transport_scan_not_checked"
    if transport.get("usable_for_release") is False:
        return None, "transport_scan_incomplete"
    health = [row for row in transport.get("source_health") or [] if isinstance(row, dict)]
    tfgm = next((row for row in health if str(row.get("name") or "").strip().lower() == "tfgm"), None)
    if not tfgm or tfgm.get("errors") or not (tfgm.get("fetched") or tfgm.get("not_modified")):
        return None, "tfgm_fresh_check_failed"
    incomplete = [
        row for row in health
        if row.get("errors") or not (row.get("fetched") or row.get("not_modified"))
    ]
    if incomplete:
        return None, "transport_scan_incomplete"

    candidate = {
        "title": f"TfGM transport status check {today_london()}",
        "category": "transport",
        "summary": "TfGM был успешно проверен; отдельной подтверждённой карточки об ограничении для выпуска нет.",
        "source_url": "https://tfgm.com/travel-updates",
        "source_label": "TfGM",
        "primary_block": "transport",
        "include": True,
        "validated": True,
        "digest_selection_verdict": "selected",
        "dedupe_decision": "new",
        "freshness_status": "fresh_24h",
        "published_at": str(report.get("run_at_london") or now_london().isoformat()),
        "published_date_london": today_london(),
        "source_health": "dated",
        "synthetic": True,
        "synthetic_stale": False,
        "transport_status_fallback": True,
        "plan_fallback_only": True,
        "draft_line": (
            "• TfGM: утренняя проверка завершена; отдельного подтверждённого ограничения "
            "для карточки нет. Перед поездкой проверьте актуальный статус своего маршрута."
        ),
        "reason": "Plan-owned transport fallback after a fresh complete TfGM scan.",
    }
    candidate["fingerprint"] = fingerprint_for_candidate(candidate)
    return candidate, ""


def _ordered_sections(show_weekend: bool) -> list[str]:
    return [
        "Погода",
        "Главная история дня",
        "Свежие новости",
        "Общественный транспорт сегодня",
        "Что важно сегодня",
        "Футбол",
        *(["Выходные в GM"] if show_weekend else []),
        "Городской радар",
        "Что важно в ближайшие 7 дней",
        "Еда, открытия и рынки",
        "IT и бизнес",
        "Business/tech события для тебя",
        "Дальние анонсы",
        "Билеты / Ticket Radar",
        "Крупные концерты вне GM",
        "Русскоязычные концерты и стендап UK",
    ]


def _blob(candidate: dict) -> str:
    return str(candidate.get("draft_line") or "").strip()


def _published_at(candidate: dict) -> str:
    return str(candidate.get("published_at") or "")


def _story_key(candidate: dict) -> str:
    if is_a_tier_ticket(candidate):
        from news_digest.pipeline.ticket_notability import ticket_artist_name  # noqa: PLC0415

        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        artist = re.sub(r"[^a-z0-9]+", " ", ticket_artist_name(candidate).lower()).strip()
        venue = re.sub(r"[^a-z0-9]+", " ", str(event.get("venue") or "").lower()).strip()
        event_day = str(event.get("date_start") or event.get("date") or "")[:10]
        if artist and venue and event_day:
            return f"ticket:{artist}:{venue}:{event_day}"
    from news_digest.pipeline.editor import _candidate_story_identity_key  # noqa: PLC0415

    return _candidate_story_identity_key(candidate)


def _collapse_a_tier_event_runs(candidates: list[dict]) -> dict[str, object]:
    """Conserve one public A-tier card per recognised artist.

    Ticket feeds repeat the same tour across sellers, dates and cities. The
    public unit is the recognised artist, not every feed row. Prefer a Greater
    Manchester event, then a nearby one, then the nearest outside-GM date.
    This A-tier-only identity rule does not collapse ordinary ticket events.
    """
    from news_digest.pipeline.ticket_notability import ticket_artist_name  # noqa: PLC0415

    recognised_artists: set[str] = set()
    grouped: dict[str, list[tuple[date, dict]]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict) or not is_a_tier_ticket(candidate):
            continue
        # These fields are owned by this stage. Clear them so a manual rerun of
        # plan-digest is deterministic instead of treating yesterday's local
        # plan decision as upstream evidence.
        candidate.pop("a_tier_collapsed_into", None)
        if str(candidate.get("a_tier_policy_status") or "").startswith("collapsed_"):
            candidate.pop("a_tier_policy_status", None)
            candidate.pop("a_tier_policy_reason", None)
    for candidate in candidates:
        if not isinstance(candidate, dict) or not is_a_tier_ticket(candidate):
            continue
        event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
        artist = re.sub(r"[^a-z0-9]+", " ", ticket_artist_name(candidate).lower()).strip()
        if artist:
            recognised_artists.add(artist)
        eligible, _ = a_tier_ticket_policy(candidate)
        if not eligible:
            continue
        try:
            event_day = date.fromisoformat(str(event.get("date_start") or event.get("date") or "")[:10])
        except ValueError:
            continue
        if artist:
            grouped.setdefault(artist, []).append((event_day, candidate))

    collapsed = 0
    conserved_artists = 0
    for rows in grouped.values():
        rows.sort(key=lambda item: (item[0], str(item[1].get("fingerprint") or "")))
        conserved_artists += 1

        def _geography_priority(candidate: dict) -> int:
            scope = str(candidate.get("venue_scope") or "").strip().lower()
            block = str(candidate.get("primary_block") or "")
            if scope == "gm" or (not scope and block == "ticket_radar"):
                return 0
            if scope == "nearby":
                return 1
            if block in {"ticket_radar", "future_announcements", "next_7_days"} and scope != "outside":
                return 1
            return 2

        def _survivor_key(item: tuple[date, dict]) -> tuple:
            candidate = item[1]
            return (
                _geography_priority(candidate),
                item[0],
                0 if candidate.get("include") else 1,
                -_source_authority(candidate),
                str(candidate.get("fingerprint") or ""),
            )

        survivor = min(rows, key=_survivor_key)[1]
        survivor_event = survivor.get("event") if isinstance(survivor.get("event"), dict) else {}
        survivor_venue = re.sub(r"[^a-z0-9]+", " ", str(survivor_event.get("venue") or "").lower()).strip()

        def _normalised_venue(candidate: dict) -> str:
            event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
            return re.sub(r"[^a-z0-9]+", " ", str(event.get("venue") or "").lower()).strip()

        same_venue_dates = sorted({
            item[0].isoformat()
            for item in rows
            if _normalised_venue(item[1]) == survivor_venue
        })
        if len(same_venue_dates) > 1:
            survivor["merged_event_dates"] = same_venue_dates
        survivor_fp = str(survivor.get("fingerprint") or "")
        for _, candidate in rows:
            if candidate is survivor:
                continue
            candidate["a_tier_collapsed_into"] = survivor_fp
            candidate["a_tier_policy_status"] = "collapsed_same_a_tier_artist"
            candidate["a_tier_policy_reason"] = "one_public_card_per_a_tier_artist"
            collapsed += 1
    return {
        "recognised_artists": len(recognised_artists),
        "eligible_artists": conserved_artists,
        "collapsed_rows": collapsed,
    }


def _source_authority(candidate: dict) -> int:
    from news_digest.pipeline.source_selection import source_score  # noqa: PLC0415

    try:
        return int(source_score(str(candidate.get("source_label") or ""), str(candidate.get("category") or "")))
    except Exception:  # noqa: BLE001
        return 0


def _order_key(candidate: dict, score: float) -> tuple:
    """Стабильный порядок: балл ↓ → авторитет источника ↓ → свежесть ↓ → fingerprint ↑."""
    return (
        -float(score),
        -_source_authority(candidate),
        # ISO datetime сортируется лексикографически; пустые в конец.
        "" if _published_at(candidate) else "~",
        _published_at(candidate)[::-1] if _published_at(candidate) else "",
        str(candidate.get("fingerprint") or ""),
    )


def _sorted_pool(pool: list[dict], section: str) -> list[dict]:
    from news_digest.pipeline.writer import _section_priority_score  # noqa: PLC0415

    scored = []
    for candidate in pool:
        try:
            score = float(_section_priority_score(candidate, section, _blob(candidate)))
        except Exception:  # noqa: BLE001
            score = float(candidate.get("reader_value_score") or 0)
        candidate["plan_score_snapshot"] = round(score, 3)
        scored.append((candidate, score))
    scored.sort(key=lambda pair: _order_key(pair[0], pair[1]))
    return [candidate for candidate, _ in scored]


def _must_show(candidate: dict, repeat_allowed: bool) -> bool:
    block = str(candidate.get("primary_block") or "")
    if is_a_tier_ticket(candidate):
        return True
    if block == "weekend_activities":
        from news_digest.pipeline.weekend_inventory import is_weekend_inventory_candidate  # noqa: PLC0415

        if is_weekend_inventory_candidate(candidate):
            return True
    if candidate.get("is_lead") or block == "transport":
        return True
    if block == "russian_events":
        return repeat_allowed
    if block == "professional_events":
        llm = candidate.get("professional_llm_match") if isinstance(candidate.get("professional_llm_match"), dict) else {}
        return str(llm.get("fit") or "").strip().lower() in {"go", "consider"}
    return False


def _backup_render_path(candidate: dict) -> str:
    """Запасной допускается только с доказуемым путём к строке."""
    if _blob(candidate):
        return "has_prose"
    if str(candidate.get("rewrite_shortlist_status") or "") == "writer_deterministic":
        return "deterministic"
    category = str(candidate.get("category") or "")
    if category in {"transport", "venues_tickets", "public_services", "professional_events"}:
        return "deterministic"
    event = candidate.get("event") if isinstance(candidate.get("event"), dict) else {}
    if event.get("is_event") and str(event.get("event_name") or candidate.get("title") or "").strip():
        return "deterministic"
    # Остальным строку может дописать пишущая половина llm-rewrite —
    # план назначает их запасными, rewrite получает их fingerprint.
    if str(candidate.get("evidence_text") or candidate.get("summary") or "").strip():
        return "model_write"
    return ""


def _backup_eligible(candidate: dict) -> tuple[bool, str]:
    """Порт recoverable_reserve_eligible: только capacity-hold, не брак."""
    if not isinstance(candidate, dict):
        return False, "not_a_candidate"
    if not candidate.get("validated", False):
        return False, "not_validated"
    if str(candidate.get("digest_selection_verdict") or "") == "drop":
        return False, "verdict_drop"
    if candidate.get("synthetic_stale"):
        return False, "stale_synthetic"
    if str(candidate.get("freshness_status") or "") == "stale":
        return False, "stale"
    if candidate.get("source_trial"):
        return False, "source_trial"
    if candidate.get("manual_review_hold") or candidate.get("held_for_manual_review"):
        return False, "manual_review"
    if str(candidate.get("dedupe_decision") or "") in {"drop", "duplicate"}:
        return False, "duplicate"
    if candidate.get("reject_reasons"):
        return False, "rejected"
    render_path = _backup_render_path(candidate)
    if not render_path:
        return False, "no_render_path"
    return True, render_path


def _admission_verdict(candidate: dict, previous_by_fp: dict[str, dict]) -> tuple[str, str]:
    """Порт цепочки исключений писателя + repeat-политика (переехала с release).

    Возвращает (decision, reason): decision ∈ {ok, out}.
    Роутинговые переезды мутируют primary_block ДО вызова (см. _apply_routing).
    """
    from news_digest.pipeline.writer import (  # noqa: PLC0415
        _contract_public_drop_reason,
        _is_expired_event_candidate,
        _is_outside_current_weekend_candidate,
    )

    if candidate.get("validation_errors"):
        return "out", "validation_errors"
    if not candidate.get("source_url") or not candidate.get("source_label"):
        return "out", "missing_source_reference"
    contract_drop = _contract_public_drop_reason(candidate)
    if contract_drop and candidate.get("manual_override") != "force_include":
        return "out", f"contract_drop:{contract_drop}"
    if str(candidate.get("primary_block") or "") == "last_24h" and not _published_at(candidate).strip():
        return "out", "last_24h_without_published_at"
    if _is_outside_current_weekend_candidate(candidate):
        return "out", "outside_current_weekend"
    if _is_expired_event_candidate(candidate, _blob(candidate)):
        return "out", "expired_event"
    # Повторы решаются ЗДЕСЬ, один раз — release больше не режет готовый HTML.
    previous = previous_by_fp.get(str(candidate.get("fingerprint") or ""))
    if previous is not None:
        from news_digest.pipeline.repeat_policy import visible_repeat_verdict  # noqa: PLC0415

        verdict = visible_repeat_verdict(candidate, previous)
        if not verdict.allow:
            return "out", f"repeat_blocked:{verdict.reason}"
    return "ok", ""


def _apply_routing(candidate: dict, warnings: list[str]) -> str:
    """Роутинговые переезды между блоками (только на планёрке).

    Возвращает "" или причину out.
    """
    from news_digest.pipeline.writer import (  # noqa: PLC0415
        _next_7_market_belongs_in_weekend,
        _section_event_timing_decision,
        _top_news_route_or_drop,
    )

    top_news_route = _top_news_route_or_drop(candidate)
    if top_news_route == "city_watch":
        candidate["primary_block"] = "city_watch"
    if _next_7_market_belongs_in_weekend(candidate):
        candidate["primary_block"] = "weekend_activities"
    elif top_news_route == "drop_non_gm_regional" and candidate.get("manual_override") != "force_include":
        return "non_gm_regional"
    timing_decision, timing_reason = _section_event_timing_decision(candidate)
    if timing_decision == "move_future":
        candidate["primary_block"] = "future_announcements"
    elif timing_decision == "move_next_7":
        candidate["primary_block"] = "next_7_days"
    elif timing_decision == "hold" and is_a_tier_ticket(candidate):
        warnings.append(
            f"A-tier сохранён вопреки timing-hold: {str(candidate.get('title') or '')[:100]} ({timing_reason})."
        )
    elif timing_decision == "hold":
        return f"event_timing:{timing_reason}"
    return ""


def _ticket_public_ok(candidate: dict) -> tuple[bool, str]:
    if is_a_tier_ticket(candidate):
        return True, ""
    from news_digest.pipeline.writer import _ticket_watch_decision  # noqa: PLC0415

    decision = _ticket_watch_decision(candidate)
    if str(decision.get("decision") or "") == "show":
        return True, ""
    reasons = [str(r) for r in decision.get("reasons") or [] if str(r).strip()]
    return False, "ticket_watch:" + ("; ".join(reasons)[:160] or "below_public_threshold")


def _collapse_weekend_duplicates(pool: list[dict], demoted: list[tuple[dict, str]]) -> list[dict]:
    from news_digest.pipeline.writer import (  # noqa: PLC0415
        _weekend_duplicate_date,
        _weekend_duplicate_venue,
    )

    seen: dict[tuple[str, str], dict] = {}
    kept: list[dict] = []
    for candidate in pool:  # pool уже отсортирован по силе
        venue = _weekend_duplicate_venue(candidate, _blob(candidate))
        day = _weekend_duplicate_date(candidate)
        key = (venue, day)
        if venue and day and key in seen:
            demoted.append((candidate, "duplicate_weekend_event"))
            continue
        if venue and day:
            seen[key] = candidate
        kept.append(candidate)
    return kept


def run_plan_digest(project_root: Path) -> StageResult:
    state_dir = project_root / "data" / "state"
    report_path = state_dir / "plan_digest_report.json"
    candidates_path = state_dir / "candidates.json"
    payload = read_json(candidates_path, {})
    candidates = payload.get("candidates", []) if isinstance(payload, dict) else []
    pipeline_run_id = pipeline_run_id_from(payload if isinstance(payload, dict) else {})
    if not candidates:
        write_json(report_path, {
            "pipeline_run_id": pipeline_run_id,
            "run_date_london": today_london(),
            "stage_status": "failed",
            "errors": ["candidates.json is missing or empty."],
        })
        return StageResult(False, "No candidates for planning.", report_path)

    from news_digest.pipeline.editorial_contracts import attach_editorial_contract  # noqa: PLC0415
    from news_digest.pipeline.writer import (  # noqa: PLC0415
        PUBLIC_SECTION_RESERVED_MIN,
        _fresh_hard_news_can_bypass_source_cap,
        _is_minor_bus_stop_line,
        _is_public_budget_exempt,
        _today_focus_candidate_is_eligible,
        _transport_line_priority,
    )

    warnings: list[str] = []
    london_now = now_london()
    show_weekend = london_now.weekday() >= 3

    published_facts = read_json(state_dir / "published_facts.json", {})

    def _pre_send_previous(fact: dict) -> dict | None:
        """Вид факта ДО сегодняшней отправки.

        В проде планёрка работает до send и сегодняшних фактов в истории нет.
        Replay-снапшоты коммитятся ПОСЛЕ отправки — сегодняшняя запись там уже
        есть, и без этой поправки план блокировал бы собственный выпуск как
        «повтор». Если last_published == сегодня: откатываем к first_published
        (было раньше — обычный повтор), либо факт вовсе не существовал.
        """
        today = today_london()
        last = str(fact.get("last_published_day_london") or "")
        if last != today:
            return fact
        first = str(fact.get("first_published_day_london") or "")
        if first and first < today:
            rolled = dict(fact)
            rolled["last_published_day_london"] = first
            count = int(fact.get("published_count") or 1)
            rolled["published_count"] = max(1, count - 1)
            return rolled
        return None

    previous_by_fp: dict[str, dict] = {}
    for fact in published_facts.get("facts") or []:
        if not isinstance(fact, dict) or not fact.get("fingerprint"):
            continue
        rolled = _pre_send_previous(fact)
        if rolled is not None:
            previous_by_fp[str(fact.get("fingerprint") or "")] = rolled

    # Страховка для offline-replay и деградаций rank-этапа: если у билетного
    # кандидата нет notability-штампа, добираем его из локального кэша.
    # Сетевые промахи глотаем: в replay сеть закрыта на уровне сокета.
    notability_cache = state_dir / "ticket_notability_cache.json"
    if notability_cache.exists():
        from news_digest.pipeline.ticket_notability import enrich_ticket_notability  # noqa: PLC0415

        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if isinstance(candidate.get("ticket_notability"), dict) and candidate["ticket_notability"].get("tier"):
                continue
            if str(candidate.get("category") or "") != "venues_tickets" and str(candidate.get("primary_block") or "") not in {
                "ticket_radar",
                "outside_gm_tickets",
                "russian_events",
            }:
                continue
            try:
                notability = enrich_ticket_notability(candidate, notability_cache)
            except Exception:  # noqa: BLE001 — offline replay: сеть закрыта
                continue
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
                "event_owner": notability.event_owner,
            }

    a_tier_identity = _collapse_a_tier_event_runs(candidates)

    # --- Допуск и роутинг --------------------------------------------------
    out_rows: list[dict] = []
    eligible_a_tier_fps: set[str] = set()
    pools: dict[str, list[dict]] = {heading: [] for heading in PRIMARY_BLOCKS.values()}
    backup_pools: dict[str, list[dict]] = {heading: [] for heading in PRIMARY_BLOCKS.values()}

    def _mark_out(candidate: dict, reason: str) -> None:
        candidate["publish_plan_status"] = "drop"
        candidate["publish_plan_reason"] = reason
        out_rows.append(
            {
                "fingerprint": candidate.get("fingerprint") or "",
                "title": str(candidate.get("title") or "")[:120],
                "section": PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""), ""),
                "reason": reason,
            }
        )

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        verdict = str(candidate.get("digest_selection_verdict") or "")
        included = bool(candidate.get("include"))
        a_tier_eligible, a_tier_reason = a_tier_ticket_policy(candidate)
        if a_tier_eligible:
            candidate["a_tier_policy_status"] = "must_show"
            candidate["a_tier_policy_reason"] = a_tier_reason
            included = True
        elif is_a_tier_ticket(candidate):
            candidate["a_tier_policy_status"] = "ineligible"
            candidate["a_tier_policy_reason"] = a_tier_reason
        if candidate.get("a_tier_collapsed_into"):
            _mark_out(candidate, "a_tier_same_artist_collapsed")
            continue
        if is_a_tier_ticket(candidate) and not a_tier_eligible:
            _mark_out(candidate, f"a_tier_ineligible:{a_tier_reason}")
            continue
        if not included and verdict not in {"reserve", "selected"}:
            continue  # drop/needs_enrichment: не участвует ни в слотах, ни в запасе
        attach_editorial_contract(candidate)
        route_out = _apply_routing(candidate, warnings)
        section = PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""))
        if not section:
            _mark_out(candidate, f"unknown_primary_block:{candidate.get('primary_block')!r}")
            continue
        if route_out:
            _mark_out(candidate, route_out)
            continue
        decision, reason = _admission_verdict(candidate, previous_by_fp)
        if decision == "out":
            _mark_out(candidate, reason)
            continue
        if str(candidate.get("primary_block") or "") in {"ticket_radar", "outside_gm_tickets"}:
            ok, ticket_reason = _ticket_public_ok(candidate)
            if not ok:
                # watch-режим: билет остаётся в служебном инвентаре, не в выпуске
                _mark_out(candidate, ticket_reason)
                continue
        if a_tier_eligible:
            eligible_a_tier_fps.add(str(candidate.get("fingerprint") or ""))
        if included and (verdict != "reserve" or a_tier_eligible):
            pools[section].append(candidate)
        else:
            eligible, render_path = _backup_eligible(candidate)
            if eligible:
                candidate["plan_render_path"] = render_path
                backup_pools[section].append(candidate)
            else:
                _mark_out(candidate, f"backup_ineligible:{render_path or 'no_render_path'}")

    transport_section = "Общественный транспорт сегодня"
    if not pools.get(transport_section):
        transport_fallback, fallback_reason = _planned_transport_status_candidate(state_dir)
        if transport_fallback is not None:
            attach_editorial_contract(transport_fallback)
            candidates.append(transport_fallback)
            pools[transport_section].append(transport_fallback)
            warnings.append(
                "Планёрка создала один synthetic transport fallback после свежей полной проверки TfGM."
            )
        elif fallback_reason:
            warnings.append(
                f"Transport fallback не запланирован: {fallback_reason}; неполный скан не маскируется статусной строкой."
            )

    # --- Сортировка пулов и планёрочные решения ----------------------------
    for section in list(pools):
        pools[section] = _sorted_pool(pools[section], section)
    for section in list(backup_pools):
        backup_pools[section] = _sorted_pool(backup_pools[section], section)

    demoted: list[tuple[dict, str]] = []
    if show_weekend:
        pools["Выходные в GM"] = _collapse_weekend_duplicates(pools["Выходные в GM"], demoted)

    # Одна история — один слот во всём выпуске (включая lead).
    seen_story_keys: set[str] = set()

    # --- Lead --------------------------------------------------------------
    lead_candidate = next(
        (c for pool in pools.values() for c in pool if c.get("is_lead")),
        None,
    )
    lead_promoted = False
    if lead_candidate is None:
        for source_block in ("today_focus", "last_24h"):
            section = PRIMARY_BLOCKS[source_block]
            for candidate in pools.get(section) or []:
                from news_digest.pipeline.curator import _is_weak_lead  # noqa: PLC0415

                if not _is_weak_lead(candidate):
                    lead_candidate = candidate
                    lead_candidate["is_lead"] = True
                    lead_promoted = True
                    warnings.append("Куратор не дал lead — планёрка повысила сильнейшую новость дня.")
                    break
            if lead_candidate is not None:
                break
    if lead_candidate is not None:
        for pool in pools.values():
            if lead_candidate in pool:
                pool.remove(lead_candidate)
        seen_story_keys.add(_story_key(lead_candidate))

    # Дублёры главной — из-под публичной границы (из запасных пулов),
    # чтобы не замораживать сильные новости штатного выпуска.
    lead_understudies: list[str] = []
    for source_block in LEAD_UNDERSTUDY_SOURCE_BLOCKS:
        section = PRIMARY_BLOCKS[source_block]
        for candidate in backup_pools.get(section) or []:
            if len(lead_understudies) >= LEAD_UNDERSTUDY_COUNT:
                break
            if not _blob(candidate) and candidate.get("plan_render_path") != "model_write":
                continue
            key = _story_key(candidate)
            if key in seen_story_keys:
                continue
            lead_understudies.append(str(candidate.get("fingerprint") or ""))
            seen_story_keys.add(key)
        if len(lead_understudies) >= LEAD_UNDERSTUDY_COUNT:
            break

    # --- Сегодня/Свежие ----------------------------------------------------
    # Today is purpose-owned: only items routed there upstream may enter it.
    # An honest shortfall is reported below instead of borrowing Fresh/City.
    today_board = {"policy": "native_today_only", "from_fresh": 0, "from_city_watch": 0}

    # --- Межсекционный дедуп историй ---------------------------------------
    for section in _ordered_sections(show_weekend):
        pool = pools.get(section)
        if not pool:
            continue
        kept: list[dict] = []
        for candidate in pool:
            key = _story_key(candidate)
            if key and key in seen_story_keys:
                demoted.append((candidate, "duplicate_story_cross_section"))
                continue
            if key:
                seen_story_keys.add(key)
            kept.append(candidate)
        pools[section] = kept

    # --- Пер-секционные лимиты ---------------------------------------------
    def _demote(candidate: dict, reason: str) -> None:
        demoted.append((candidate, reason))

    planned: dict[str, list[dict]] = {}
    for section in _ordered_sections(show_weekend):
        pool = list(pools.get(section) or [])
        if section == "Еда, открытия и рынки" and len(pool) > 1:
            # The three public Food slots should show two independent live
            # sources whenever two survived upstream checks.
            visible_window = min(SECTION_MAX_ITEMS.get(section, len(pool)), len(pool))
            first_source = str(pool[0].get("source_label") or "")
            if first_source and all(
                str(candidate.get("source_label") or "") == first_source
                for candidate in pool[:visible_window]
            ):
                different_index = next(
                    (
                        index
                        for index, candidate in enumerate(pool[visible_window:], start=visible_window)
                        if str(candidate.get("source_label") or "")
                        and str(candidate.get("source_label") or "") != first_source
                    ),
                    None,
                )
                if different_index is not None:
                    pool.insert(1, pool.pop(different_index))
        if section == "Общественный транспорт сегодня" and pool:
            pool.sort(key=lambda c: -_transport_line_priority(_blob(c), float(c.get("plan_score_snapshot") or 0)))
            minor_seen = 0
            filtered: list[dict] = []
            for candidate in pool:
                if _is_minor_bus_stop_line(_blob(candidate)):
                    minor_seen += 1
                    if minor_seen > _MINOR_BUS_STOP_MAX:
                        _demote(candidate, "minor_bus_stop_over_cap")
                        continue
                filtered.append(candidate)
            pool = filtered
        per_source_cap = SECTION_MAX_PER_SOURCE.get(section)
        if per_source_cap and pool:
            src_counts: dict[str, int] = {}
            filtered = []
            skipped: list[dict] = []
            for candidate in pool:
                src = str(candidate.get("source_label") or "")
                if (
                    src_counts.get(src, 0) >= per_source_cap
                    and not _is_public_budget_exempt(section, candidate)
                    and not (
                        section == "Свежие новости"
                        and _fresh_hard_news_can_bypass_source_cap(candidate, _blob(candidate))
                    )
                ):
                    skipped.append(candidate)
                    continue
                src_counts[src] = src_counts.get(src, 0) + 1
                filtered.append(candidate)
            min_items = SECTION_MIN_ITEMS.get(section, 0)
            if not min_items or len(filtered) >= min_items or len(pool) < min_items:
                for candidate in skipped:
                    _demote(candidate, "per_source_cap")
                pool = filtered
        cap = SECTION_MAX_ITEMS.get(section)
        if cap and pool:
            counted = 0
            trimmed: list[dict] = []
            for candidate in pool:
                if _is_public_budget_exempt(section, candidate):
                    trimmed.append(candidate)
                    continue
                if counted >= cap:
                    _demote(candidate, "section_cap")
                    continue
                counted += 1
                trimmed.append(candidate)
            pool = trimmed
        planned[section] = pool

    # --- Бюджет выпуска -----------------------------------------------------
    ordered = _ordered_sections(show_weekend)

    def _counted(section: str, pool: list[dict]) -> int:
        return sum(0 if _is_public_budget_exempt(section, c) else 1 for c in pool)

    visible_count = 0
    for index, section in enumerate(ordered):
        pool = planned.get(section) or []
        if not pool:
            continue
        reserved_later = 0
        for later in ordered[index + 1:]:
            later_pool = planned.get(later) or []
            if later in PUBLIC_SECTION_RESERVED_MIN:
                reserved_later += min(PUBLIC_SECTION_RESERVED_MIN[later], _counted(later, later_pool))
        remaining = PLAN_PUBLIC_MAX_VISIBLE_ITEMS - visible_count - reserved_later
        if section in PUBLIC_SECTION_RESERVED_MIN:
            remaining += min(PUBLIC_SECTION_RESERVED_MIN[section], _counted(section, pool))
        kept: list[dict] = []
        counted_kept = 0
        for candidate in pool:
            if _is_public_budget_exempt(section, candidate):
                kept.append(candidate)
                continue
            if counted_kept >= max(0, remaining):
                _demote(candidate, "issue_budget")
                continue
            counted_kept += 1
            kept.append(candidate)
        planned[section] = kept
        visible_count += counted_kept

    # Жёсткий предел на отрисованные не-исключённые строки.
    hard_counted = 0
    for section in ordered:
        pool = planned.get(section) or []
        kept = []
        for candidate in pool:
            if _is_public_budget_exempt(section, candidate):
                kept.append(candidate)
                continue
            if hard_counted >= PLAN_PUBLIC_HARD_RENDERED_ITEMS:
                _demote(candidate, "hard_render_cap")
                continue
            hard_counted += 1
            kept.append(candidate)
        planned[section] = kept

    # Понижённые — в начало очереди запасных своего раздела (они сильнее).
    for candidate, reason in demoted:
        candidate["publish_plan_reason"] = reason
        section = PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""))
        if not section:
            continue
        eligible, render_path = _backup_eligible(candidate)
        if eligible:
            candidate["plan_render_path"] = render_path
            backup_pools.setdefault(section, []).insert(0, candidate)
        else:
            _mark_out(candidate, f"{reason};backup_ineligible")

    # --- Слоты и цепочки запасных -------------------------------------------
    slots: list[dict] = []
    sections_summary: dict[str, dict] = {}
    used_backup_fps = set(lead_understudies)
    for section in ordered:
        pool = planned.get(section) or []
        block_key = next((k for k, v in PRIMARY_BLOCKS.items() if v == section), section)
        depth = 0 if section in SYNTHETIC_SECTIONS else (
            BACKUP_DEPTH_SPINE if section in SPINE_SECTIONS else BACKUP_DEPTH_DEFAULT
        )
        queue = [
            c for c in backup_pools.get(section) or []
            if str(c.get("fingerprint") or "") not in used_backup_fps
            and _story_key(c) not in seen_story_keys
        ]
        # Недобор до минимума закрывает сама планёрка: повышаем сильнейших
        # пригодных запасных в основные слоты. Это НЕ ремонт после вёрстки —
        # это нормальное решение состава до написания текстов.
        minimum_floor = SECTION_MIN_ITEMS.get(section, 0)
        promoted_here = 0
        while minimum_floor and len(pool) < minimum_floor and queue and section not in SYNTHETIC_SECTIONS:
            promoted = queue.pop(0)
            promoted_fp = str(promoted.get("fingerprint") or "")
            if not promoted_fp or promoted_fp in used_backup_fps:
                continue
            key = _story_key(promoted)
            if key and key in seen_story_keys:
                continue
            used_backup_fps.add(promoted_fp)
            if key:
                seen_story_keys.add(key)
            promoted["publish_plan_reason"] = "Повышен планёркой из резерва: недобор раздела до минимума."
            pool.append(promoted)
            promoted_here += 1
        if promoted_here:
            warnings.append(f"Планёрка повысила {promoted_here} запасных в «{section}» до минимума {minimum_floor}.")
        section_slots: list[str] = []
        for position, candidate in enumerate(pool, start=1):
            fp = str(candidate.get("fingerprint") or "")
            chain: list[str] = []
            while queue and len(chain) < depth:
                backup = queue.pop(0)
                backup_fp = str(backup.get("fingerprint") or "")
                if not backup_fp or backup_fp in used_backup_fps:
                    continue
                used_backup_fps.add(backup_fp)
                backup["publish_plan_status"] = "reserve"
                backup["publish_plan_reason"] = f"Запасной слота {block_key}-{position:02d}"
                chain.append(backup_fp)
            repeat_allowed = True
            previous = previous_by_fp.get(fp)
            if previous is not None:
                from news_digest.pipeline.repeat_policy import visible_repeat_verdict  # noqa: PLC0415

                repeat_allowed = visible_repeat_verdict(candidate, previous).allow
            required = _must_show(candidate, repeat_allowed)
            slot_id = f"{block_key}-{position:02d}"
            candidate["publish_plan_status"] = "must_show" if required else "show"
            candidate["publish_plan_reason"] = str(candidate.get("digest_selection_reason") or "Слот плана.")
            candidate["plan_slot_id"] = slot_id
            candidate["plan_section"] = section
            slots.append(
                {
                    "slot_id": slot_id,
                    "section": section,
                    "block": block_key,
                    "position": position,
                    "primary_fingerprint": fp,
                    "backup_fingerprints": chain,
                    "required": required,
                    "must_show": required,
                    "selection_reason": str(candidate.get("digest_selection_reason") or "")[:200],
                    "score_snapshot": candidate.get("plan_score_snapshot"),
                    "story_key": _story_key(candidate),
                    "prose_source": (
                        "synthetic" if section in SYNTHETIC_SECTIONS
                        else ("model" if _blob(candidate) or candidate.get("plan_render_path") == "model_write" else "deterministic")
                    ),
                    "source_label": str(candidate.get("source_label") or ""),
                    "title": str(candidate.get("title") or "")[:140],
                }
            )
            section_slots.append(slot_id)
        minimum = SECTION_MIN_ITEMS.get(section, 0)
        shortfall = None
        if minimum and len(pool) < minimum and not (section == "Выходные в GM" and not show_weekend):
            section_reasons = [row["reason"] for row in out_rows if row.get("section") == section][:5]
            shortfall = {
                "planned": len(pool),
                "minimum": minimum,
                "reason": "; ".join(dict.fromkeys(section_reasons)) or "pool_exhausted_after_upstream_gates",
            }
            warnings.append(f"Недобор в «{section}»: {len(pool)}/{minimum} — {shortfall['reason'][:160]}")
        sections_summary[section] = {
            "block": block_key,
            "min": minimum,
            "max": SECTION_MAX_ITEMS.get(section),
            "planned": len(pool),
            "slots": section_slots,
            "backups_available": len(backup_pools.get(section) or []),
            "expected_shortfall": shortfall,
        }

    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "policy_version": PLAN_POLICY_VERSION,
        "pipeline_run_id": pipeline_run_id,
        "run_date_london": today_london(),
        "created_at_london": london_now.isoformat(),
        "weekday": london_now.weekday(),
        "show_weekend": show_weekend,
        "policy": (
            "Состав решён один раз. Writer исполняет план, editor правит только слова, "
            "pre-send repair executor заменяет слот только его запасным или снимает по "
            "кодифицированной причине; verify только сверяет. План неизменяем."
        ),
        "budget": {
            "max_visible_items": PLAN_PUBLIC_MAX_VISIBLE_ITEMS,
            "hard_rendered_items": PLAN_PUBLIC_HARD_RENDERED_ITEMS,
            "exempt_sections": sorted(
                {"Общественный транспорт сегодня", "Русскоязычные концерты и стендап UK", "Business/tech события для тебя"}
            ),
        },
        "ordered_sections": ordered,
        "lead": {
            "primary_fingerprint": str((lead_candidate or {}).get("fingerprint") or ""),
            "understudy_fingerprints": lead_understudies,
            "promoted_by_plan": lead_promoted,
            "title": str((lead_candidate or {}).get("title") or "")[:140],
        },
        "sections": sections_summary,
        "slots": slots,
        "today_focus_board": today_board,
        "out_sample": out_rows[:200],
        "totals": {
            "slots": len(slots),
            "backups_assigned": sum(len(s.get("backup_fingerprints") or []) for s in slots),
            "out": len(out_rows),
            "demoted_to_backup": len(demoted),
        },
        "a_tier_conservation": {
            "recognised": int(a_tier_identity.get("recognised_artists") or 0),
            "recognised_rows": sum(
                1 for candidate in candidates
                if isinstance(candidate, dict) and is_a_tier_ticket(candidate)
            ),
            "eligible": len(eligible_a_tier_fps),
            "planned": len(
                eligible_a_tier_fps
                & {str(slot.get("primary_fingerprint") or "") for slot in slots}
            ),
            "missing_from_plan": sorted(
                eligible_a_tier_fps
                - {str(slot.get("primary_fingerprint") or "") for slot in slots}
            ),
            "identity": a_tier_identity,
            "policy": (
                "One public card per recognised A-tier artist, preferring a "
                "Greater Manchester event. The survivor is decided before "
                "timing/watch/cap/repeat; only hard event validity may exclude it."
            ),
        },
        "warnings": warnings,
    }
    write_json_atomic(plan_path(state_dir), plan)
    # Совместимое зеркало для ночного контура (inventory/story_intelligence
    # читают publish_plan.json). Это НЕ второй источник решений: файл — прямая
    # проекция слот-плана, пишется только здесь.
    legacy_items = []
    legacy_totals = {"must_show": 0, "show": 0, "reserve": 0, "needs_enrichment": 0, "drop": 0}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        status = str(candidate.get("publish_plan_status") or "drop")
        if status not in legacy_totals:
            status = "drop"
        legacy_totals[status] += 1
        legacy_items.append(
            {
                "fingerprint": candidate.get("fingerprint") or "",
                "title": candidate.get("title") or "",
                "primary_block": candidate.get("primary_block") or "",
                "section": PRIMARY_BLOCKS.get(str(candidate.get("primary_block") or ""), ""),
                "status": status,
                "reason": str(candidate.get("publish_plan_reason") or ""),
                "is_lead": bool(candidate.get("is_lead")),
                "protected_budget": bool(candidate.get("publish_plan_status") == "must_show"),
            }
        )
    write_json_atomic(
        state_dir / "publish_plan.json",
        {
            "schema_version": 2,
            "mirror_of": "release_plan.json",
            "run_date_london": today_london(),
            "pipeline_run_id": pipeline_run_id,
            "totals": legacy_totals,
            "items": legacy_items,
        },
    )
    write_json_atomic(candidates_path, payload)
    write_json(
        report_path,
        {
            "pipeline_run_id": pipeline_run_id,
            "run_at_london": london_now.isoformat(),
            "run_date_london": today_london(),
            "stage_status": "complete",
            "errors": [],
            "warnings": warnings,
            "totals": plan["totals"],
            "lead": plan["lead"],
            "sections": {
                name: {k: row[k] for k in ("planned", "min", "max", "expected_shortfall")}
                for name, row in sections_summary.items()
            },
        },
    )
    logger.info(
        "Plan digest: %d slots, %d backups, %d out, lead=%s.",
        len(slots),
        plan["totals"]["backups_assigned"],
        len(out_rows),
        "yes" if lead_candidate else "NO",
    )
    return StageResult(True, f"Release plan fixed: {len(slots)} slots.", report_path)
