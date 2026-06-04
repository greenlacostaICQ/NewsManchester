from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env_file(PROJECT_ROOT / ".env.local")

from news_digest.assembly.demo_digest import build_demo_digest
from news_digest.bot.service import DigestBotService
from news_digest.config.settings import load_settings
from news_digest.delivery.telegram import TelegramClient
from news_digest.delivery.telegram import TelegramTransportError
from news_digest.jobs.send_demo_digest import send_demo_digest
from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.collector import collect_digest, initialize_collector_state
from news_digest.pipeline.common import SECTION_MAX_ITEMS, SECTION_MIN_ITEMS, read_json, write_json
from news_digest.pipeline.city_trends import (
    build_weekly_city_rollup,
    weekly_city_rollup_text,
)
from news_digest.pipeline.dedupe import dedupe_candidates, initialize_candidates_state
from news_digest.pipeline.editor import edit_digest
from news_digest.pipeline.history import ensure_history_files, record_delivery_artifacts
from news_digest.pipeline.llm_rewrite import run_llm_rewrite
from news_digest.pipeline.release import build_release, initialize_release_inputs
from news_digest.pipeline.writer import write_digest
from news_digest.state.store import StateStore

LONDON_TZ = ZoneInfo("Europe/London")
REQUIRED_RELEASE_GATE_VERSION = 3


def _runtime_state_dir() -> Path:
    return Path.home() / ".mnewsdigest" / "data" / "state"


def _delivery_state_paths(settings) -> list[Path]:
    paths: list[Path] = []
    for path in [settings.state_dir / "delivery_state.json", _runtime_state_dir() / "delivery_state.json"]:
        if path not in paths:
            paths.append(path)
    return paths


def _read_delivery_state(path: Path) -> dict:
    if not path.exists():
        return {"last_delivery_at": None, "last_delivery_day_london": None, "targets": [], "source_path": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"last_delivery_at": None, "last_delivery_day_london": None, "targets": [], "source_path": None}


def _delivered_today_payload(settings) -> dict | None:
    today_london = datetime.now(LONDON_TZ).strftime("%Y-%m-%d")
    for path in _delivery_state_paths(settings):
        payload = _read_delivery_state(path)
        if payload.get("last_delivery_day_london") == today_london:
            payload = dict(payload)
            payload["delivery_state_path"] = str(path)
            return payload
    return None


def _load_store_and_client() -> tuple:
    settings = load_settings(PROJECT_ROOT)
    client = TelegramClient(settings.telegram_bot_token)
    store = StateStore(settings.state_dir, settings.archive_dir)
    return settings, client, store


def _effective_targets(primary_target: str | None, subscribers: list[str]) -> list[str]:
    targets: list[str] = []
    seen: set[str] = set()
    for candidate in [primary_target, *subscribers]:
        if not candidate:
            continue
        value = str(candidate)
        if value in seen:
            continue
        seen.add(value)
        targets.append(value)
    return targets


def _release_gate_error_for_file(path: Path) -> str | None:
    state_dir = PROJECT_ROOT / "data" / "state"
    outgoing_path = (PROJECT_ROOT / "data" / "outgoing" / "current_digest.html").resolve()
    resolved_path = path.resolve()
    if resolved_path != outgoing_path:
        return None

    report_path = state_dir / "release_report.json"
    if not report_path.exists():
        return "release_report.json is missing"
    report = read_json(report_path, {})
    today = datetime.now(LONDON_TZ).strftime("%Y-%m-%d")
    if report.get("release_decision") != "pass":
        return f"release gate did not pass: {report.get('message') or report.get('errors')}"
    if int(report.get("release_gate_version") or 0) < REQUIRED_RELEASE_GATE_VERSION:
        return "release_report was produced by an old gate version"
    if report.get("run_date_london") != today:
        return f"release_report is stale: {report.get('run_date_london')} != {today}"
    output_path = str(report.get("output_path") or "")
    if output_path:
        try:
            if Path(output_path).resolve() != resolved_path:
                return "release_report output_path does not match requested file"
        except OSError:
            return "release_report output_path is invalid"
    text = resolved_path.read_text(encoding="utf-8")
    if f"Greater Manchester Brief — {today}," not in text:
        return "current_digest.html does not contain today's digest header"
    return None


def cmd_bot_info() -> int:
    settings = load_settings(PROJECT_ROOT)
    client = TelegramClient(settings.telegram_bot_token)
    result = client.get_me()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_get_updates() -> int:
    settings, client, store = _load_store_and_client()
    offset = store.get_last_update_id()
    try:
        result = client.get_updates(offset=None if offset is None else offset + 1)
    except TelegramTransportError as exc:
        result = {
            "ok": False,
            "status": "deferred",
            "reason": "telegram_transport_unavailable",
            "error": str(exc),
        }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok", True) is not False else 0


def cmd_process_updates() -> int:
    settings, client, store = _load_store_and_client()
    try:
        result = _process_pending_updates(settings, client, store)
    except TelegramTransportError as exc:
        result = {
            "processed_updates": 0,
            "handled_messages": 0,
            "replies_sent": 0,
            "subscribers": store.list_subscribers(),
            "status": "deferred",
            "reason": "telegram_transport_unavailable",
            "error": str(exc),
        }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _process_pending_updates(settings, client, store) -> dict[str, object]:
    offset = store.get_last_update_id()
    updates = client.get_updates(offset=None if offset is None else offset + 1)
    latest_digest_path = settings.project_root / "data" / "outgoing" / "current_digest.html"
    service = DigestBotService(client, store, latest_digest_path)
    result = service.process_updates(updates)
    return {
        "processed_updates": result.processed_updates,
        "handled_messages": result.handled_messages,
        "replies_sent": result.replies_sent,
        "subscribers": store.list_subscribers(),
    }


def cmd_poll_updates(interval_seconds: int) -> int:
    settings, client, store = _load_store_and_client()
    print(
        f"Starting Telegram polling loop with interval {interval_seconds}s. Press Ctrl+C to stop.",
        flush=True,
    )
    try:
        while True:
            try:
                result = _process_pending_updates(settings, client, store)
            except TelegramTransportError as exc:
                result = {
                    "processed_updates": 0,
                    "handled_messages": 0,
                    "replies_sent": 0,
                    "subscribers": store.list_subscribers(),
                    "status": "deferred",
                    "reason": "telegram_transport_unavailable",
                    "error": str(exc),
                }
            print(json.dumps(result, ensure_ascii=False), flush=True)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Stopped Telegram polling loop.", flush=True)
        return 0


def cmd_render_demo() -> int:
    issue = build_demo_digest()
    print(issue.render_text())
    return 0


def cmd_send_demo() -> int:
    settings = load_settings(PROJECT_ROOT)
    result = send_demo_digest(settings)
    print(
        f"Demo digest sent to {result['target']}. Archived to {result['archive_path']}.",
    )
    return 0


def _rendered_candidates_for_delivery() -> list[dict]:
    state_dir = PROJECT_ROOT / "data" / "state"
    writer_report = read_json(state_dir / "writer_report.json", {})
    rendered_fingerprints = {
        str(item).strip()
        for item in writer_report.get("rendered_candidate_fingerprints", [])
        if str(item).strip()
    }
    if not rendered_fingerprints:
        print(
            "Warning: writer_report has no rendered_candidate_fingerprints; "
            "published_facts.json was not updated.",
            file=sys.stderr,
        )
        return []

    candidates_payload = read_json(state_dir / "candidates.json", {"candidates": []})
    rendered_candidates = [
        candidate
        for candidate in candidates_payload.get("candidates", [])
        if isinstance(candidate, dict)
        and candidate.get("include")
        and not candidate.get("validation_errors")
        and str(candidate.get("fingerprint") or "").strip() in rendered_fingerprints
    ]
    if not rendered_candidates:
        print(
            "Warning: no candidates matched rendered_candidate_fingerprints; "
            "published_facts.json was not updated.",
            file=sys.stderr,
        )
    return rendered_candidates


def cmd_send_file(file_path: str, parse_mode: str | None, force: bool) -> int:
    settings, client, store = _load_store_and_client()
    resolved_path = Path(file_path).resolve()
    gate_error = _release_gate_error_for_file(resolved_path)
    if gate_error:
        raise RuntimeError(f"Refusing to send current_digest.html: {gate_error}. Run build-digest successfully first.")
    text = resolved_path.read_text(encoding="utf-8")
    effective_parse_mode = parse_mode
    if effective_parse_mode is None and resolved_path.suffix.lower() == ".html":
        effective_parse_mode = "HTML"
    delivered_payload = _delivered_today_payload(settings)
    if delivered_payload and not force:
        print(
            "Digest was already delivered today; skipping duplicate send "
            f"(delivery_state={delivered_payload.get('delivery_state_path')}, "
            f"source_path={delivered_payload.get('source_path')})."
        )
        return 0
    targets = _effective_targets(settings.telegram_target, store.list_subscribers())
    if not targets:
        raise RuntimeError(
            "Нет ни одного получателя. Укажите TELEGRAM_TARGET или подпишите хотя бы один чат через /subscribe."
        )

    message_ids: list[int] = []
    for target in targets:
        results = client.send_text_in_chunks(target, text, parse_mode=effective_parse_mode)
        for result in results:
            mid = (result.get("result") or {}).get("message_id") if isinstance(result, dict) else None
            if isinstance(mid, int):
                message_ids.append(mid)
    store.mark_delivery(targets, str(resolved_path), message_ids=message_ids)
    runtime_state_dir = _runtime_state_dir()
    if runtime_state_dir != settings.state_dir and runtime_state_dir.exists():
        StateStore(runtime_state_dir, settings.archive_dir).mark_delivery(
            targets, str(resolved_path), message_ids=message_ids
        )
    record_delivery_artifacts(PROJECT_ROOT, resolved_path, _rendered_candidates_for_delivery())
    print(f"Sent file {file_path} to {len(targets)} target(s): {', '.join(targets)}.")
    return 0


# ── Helpers for the human-readable admin warnings report ──────────────────
#
# This report is written for a NEWSROOM EDITOR, not a developer.
# Rules:
#   • No pipeline-stage names ("писатель", "куратор", "writer", "curator").
#     Say "автоматическая проверка" or just describe the effect.
#   • No code identifiers (no _normalize_money, no writer.py).
#   • Lead with the news headline, then the reason. The editor cares about
#     "what did I almost lose" — not about how the system found it.
#   • When a problem class is already fixed in code, label it
#     "Уже исправлено — повторится в следующий раз должно уйти" so the
#     editor doesn't think they need to act.

# Set of root-cause IDs that were patched in this PR (#5) and won't repeat
# after the next pipeline run. The report renders them as informational
# rather than actionable.
_ALREADY_FIXED_CAUSES: set[str] = {"money_format"}


def _explain_source_failure(detail: str) -> str:
    """Translate the raw fetch error into a one-sentence Russian summary.

    Honest: no "уже исправлено" tag based on error type alone — that was
    misleading when fixes deployed but the underlying CI block persisted.
    Day-counter context is added separately by _source_streak_tag.
    """
    d = (detail or "").lower()
    if "http 403" in d:
        return "сайт заблокировал нашего бота (Cloudflare / WAF)"
    if "http 404" in d:
        return "страница исчезла или переехала"
    if "http 405" in d:
        return "сайт отверг наш запрос (405 Method Not Allowed — обычно бот-защита)"
    if "http 429" in d:
        return "превышен лимит запросов (429 — нужен backoff)"
    if "http 5" in d:
        return "сервер источника вернул ошибку 5xx — сайт временно сломан на их стороне"
    if "timeout" in d or "timed out" in d:
        return "сайт не отвечает (либо у них даун, либо молча режут нас)"
    if "no candidate links" in d:
        return "сайт ответил, но парсер не нашёл ссылок на материалы"
    if ("errno" in d and ("not known" in d or "nodename" in d)) or "dns" in d:
        return "домен не существует — переехал или закрыт"
    return detail[:100]


def _translate_health_signal(sig: dict) -> str:
    """Render a digest_health signal in plain Russian.

    Signals come from release.py:_evaluate_digest_health with English
    `detail` strings. We map by `name` (stable) and prefer our Russian
    rewrite; if a new signal type appears we fall back to the raw detail.
    """
    name = str(sig.get("name") or "")
    detail = str(sig.get("detail") or "")
    if name == "too_few_items":
        # "Only N item(s) rendered — below the 12-item hard floor."
        m = re.search(r"\d+", detail)
        n = m.group(0) if m else "?"
        return f"Вышло мало опубликованных пунктов: {n} (жёсткий минимум 12, цель 14–45)."
    if name == "few_items":
        m = re.search(r"\d+", detail)
        n = m.group(0) if m else "?"
        return f"Пунктов меньше нормы: {n} (цель — 14–45 опубликованных пункта)."
    if name == "too_many_items":
        m = re.search(r"\d+", detail)
        n = m.group(0) if m else "?"
        return f"Выпуск раздут: {n} опубликованных пунктов (цель — 14–45). Нужно ужесточить отбор."
    if name == "weather_empty":
        return "Раздел погоды пустой — Met Office не отвечает."
    if name == "transport_empty":
        return "Транспортный раздел пустой — TfGM/Metrolink молчат."
    if name == "all_news_thin":
        # "All news sections thin: 24h=X, today=Y, radar=Z — possible coverage breakdown."
        m_24h = re.search(r"24h=(\d+)", detail)
        m_today = re.search(r"today=(\d+)", detail)
        m_radar = re.search(r"radar=(\d+)", detail)
        n24 = m_24h.group(1) if m_24h else "?"
        nt = m_today.group(1) if m_today else "?"
        nr = m_radar.group(1) if m_radar else "?"
        return (f"Все новостные разделы тонкие: «За 24 часа» — {n24}, "
                f"«Что важно сегодня» — {nt}, «Городской радар» — {nr}. "
                f"Возможный сбой сбора.")
    if name == "events_without_dates":
        m = re.search(r"(\d+)/(\d+)", detail)
        nodate = m.group(1) if m else "?"
        total = m.group(2) if m else "?"
        return (f"У {nodate} из {total} событий нет даты — читатель не сможет "
                f"спланировать поход.")
    if name == "high_semantic_duplicates":
        m = re.search(r"\d+", detail)
        n = m.group(0) if m else "?"
        return f"{n} семантических дубликатов отброшено — сегодня необычно много шума в источниках."
    if name == "low_writer_yield":
        m = re.search(r"(\d+) of (\d+).*?(\d+%)", detail)
        if m:
            return (f"Из {m.group(2)} принятых кандидатов в выпуск попало {m.group(1)} "
                    f"({m.group(3)}) — проверки качества режут слишком много.")
        return "Проверки качества режут слишком много материалов — фильтры стоит ослабить."
    # Fallback for any new signal type we haven't translated yet
    return f"{name}: {detail[:140]}"


def _humanize_quality_warning(raw: str) -> str:
    text = str(raw or "").strip()
    lowered = text.lower()
    if lowered.startswith("property_borderline:"):
        missing = lowered.split(":", 1)[1]
        bits = []
        if "decision_or_action" in missing:
            bits.append("не ясно, что именно произошло: продажа, заявка, решение совета или просто витрина")
        if "specific_location" in missing:
            bits.append("не хватает конкретного места: название здания, улица или узнаваемая площадка")
        return "Недвижимость/планирование: " + "; ".join(bits or ["не хватает конкретики"])
    if lowered.startswith("crime_borderline:"):
        missing = lowered.split(":", 1)[1]
        bits = []
        if "what_happened" in missing:
            bits.append("не хватает ясного описания, что именно произошло")
        if "who_affected" in missing:
            bits.append("не понятно, кого это касается")
        if "where" in missing:
            bits.append("не хватает места")
        if "why_now" in missing:
            bits.append("не ясно, почему это важно сегодня")
        return "Инцидент/суд/полиция: " + "; ".join(bits or ["не хватает конкретики"])
    if lowered.startswith("why_now_unclear"):
        return "Не ясно, почему это должно попасть именно в сегодняшний утренний выпуск"
    if lowered.startswith("event_schema_missing:"):
        missing = lowered.split(":", 1)[1].replace(",", ", ")
        return f"Событие неполное: не хватает {missing}"
    if lowered.startswith("ticket_old_onsale:"):
        return "Билеты уже давно в продаже; нет нового повода показывать их сегодня"
    return text


def _borderline_verdict(item: dict) -> str:
    warnings = [str(w).lower() for w in (item.get("quality_warnings") or [])]
    joined = " ".join(warnings)
    if "event_schema_missing:no_date" in joined or "date_in_body" in joined:
        return "похоже на ошибку извлечения данных — проверить, была ли дата в тексте материала"
    if "ticket_old_onsale" in joined or "why_now_unclear" in joined:
        return "скорее правильно удержано — нет нового повода для сегодняшнего выпуска"
    if "property_borderline" in joined or "crime_borderline" in joined:
        return "спорно — нужна ручная редакторская проверка конкретики"
    if "source_thin" in joined or "weak_value" in joined:
        return "скорее правильно удержано — мало пользы или фактов для читателя"
    return "спорно — причина удержания требует проверки"


def _humanize_source_reason(raw: str) -> str:
    reason = str(raw or "").strip()
    lowered = reason.lower()
    if "intra-batch topic duplicate" in lowered or "same story kept from stronger source" in lowered:
        return "дубликат темы: такую же историю взяли из более сильного источника"
    if "curator drop" in lowered and "лондон" in lowered:
        return "не Greater Manchester: событие относится к Лондону"
    if "not greater manchester" in lowered or "не относится к gm" in lowered:
        return "не Greater Manchester"
    if "no_date" in lowered or "without date" in lowered:
        return "не нашли рабочую дату события"
    if "regular_upcoming_non_major" in lowered:
        return "обычный небольшой концерт или событие, не уровень билетного блока"
    if "held for manual review" in lowered or "borderline editorial status" in lowered:
        return "удержано для ручной проверки: фактов недостаточно для уверенной публикации"
    if "stale" in lowered:
        return "устаревшее или без нового повода"
    return reason[:140]


def _source_name_human(name: str) -> str:
    text = str(name or "")
    return (
        text
        .replace("BBC Manchester public safety fallback", "BBC Manchester, резервный источник по происшествиям")
    )


def _source_counts_phrase(row: dict) -> str:
    found = int(row.get("raw_count", row.get("candidate_count", 0)) or 0)
    passed = int(row.get("accepted_count", row.get("curated_count", 0)) or 0)
    published = int(row.get("rendered_count", 0) or 0)
    return f"нашли {found}, прошло отбор {passed}, опубликовано {published}"


def _section_shape_rows(writer_report: dict) -> list[dict[str, object]]:
    counts = (writer_report.get("section_counts") or {}) if isinstance(writer_report, dict) else {}
    names = sorted(set(counts) | set(SECTION_MAX_ITEMS) | set(SECTION_MIN_ITEMS))
    rows: list[dict[str, object]] = []
    for name in names:
        actual = int(counts.get(name) or 0)
        max_items = SECTION_MAX_ITEMS.get(name)
        min_items = SECTION_MIN_ITEMS.get(name)
        status = "в норме"
        if max_items is not None and actual > max_items:
            status = "выше лимита"
        elif min_items is not None and actual < min_items:
            status = "ниже минимума"
        rows.append(
            {
                "section": name,
                "actual": actual,
                "min": min_items,
                "max": max_items,
                "status": status,
            }
        )
    return rows


def _section_name_human(section: str) -> str:
    return {
        "Билеты / Ticket Radar": "Билеты и концерты",
    }.get(str(section or ""), str(section or "Без названия"))


def _humanize_borough_flag(flag: str) -> str:
    text = str(flag or "")
    m = re.search(r"В\s+(\d+)\s+GM borough\(s\)\s+ноль видимых пунктов:\s*(.+)", text)
    if m:
        count = int(m.group(1))
        noun = "районе" if count == 1 else "районах"
        boroughs = m.group(2).strip().rstrip(".")
        return f"В {count} {noun} Greater Manchester нет опубликованных пунктов: {boroughs}."
    return (
        text
        .replace("GM borough(s)", "районах Greater Manchester")
        .replace("видимых пунктов", "опубликованных пунктов")
        .replace("rendered", "опубликованных")
    )


def _ticket_type_human(ticket_type: str) -> str:
    return {
        "event_this_week": "событие на этой неделе",
        "on_sale_now": "сейчас в продаже",
        "presale_soon": "скоро пресейл/старт продаж",
        "newly_listed": "новые листинги",
        "major_upcoming": "крупные будущие концерты",
        "regular_upcoming": "обычные будущие события",
        "old_onsale": "старые продажи, демотированы",
        "old_public_sale": "старые публичные продажи",
        "unknown": "тип не распознан",
    }.get(str(ticket_type or ""), str(ticket_type or "неизвестно"))


def _ticketmaster_rows(source_status: dict) -> list[dict]:
    return [
        row for row in (source_status.get("sources") or [])
        if isinstance(row, dict) and "ticketmaster" in str(row.get("name") or "").lower()
    ]


def _support_top_issues(
    *,
    rendered: int,
    health_level: str,
    health_signals: list,
    writer_report: dict,
    transport_coverage: dict,
    quality_scorecard: dict,
    source_status: dict,
    synthetic_freshness: dict,
    prompt_drift: list,
    cost_summary: dict,
    warnings: list[str],
    suspicious_rejects: list,
    suspicious_published: list,
    borderline_queue: dict,
    event_miss_review: dict | None = None,
    source_anomalies: list | None = None,
    dead_parsers: list | None = None,
) -> list[tuple[str, str]]:
    issues: list[tuple[int, str, str]] = []
    event_miss_counts = ((event_miss_review or {}).get("counts") or {})
    critical_event_misses = int(event_miss_counts.get("critical_misses") or 0)
    if critical_event_misses:
        issues.append((
            110,
            f"Под подозрением на потерю при отборе/сборке: {critical_event_misses} событий (метрика для авторазбора, не для ручного чтения утром).",
            "Это НЕ опубликованные пункты и не подтверждённые потери — это кандидаты в дедупе/писателе, которые стоит перепроверить автоматически.",
        ))
    if rendered > 45:
        issues.append((
            100,
            f"Выпуск раздут: {rendered} пунктов при норме 14–45.",
            "Смотри «Что раздуло выпуск»: проблема в сумме секций, а не в одном отдельном пункте.",
        ))
    if any(str(w).lower().startswith("llm rewrite was degraded") for w in warnings):
        issues.append((
            95,
            "Генерация текста работала нестабильно.",
            "Часть материалов не получила нормальный финальный текст; слабые пункты должны удерживаться, а не публиковаться.",
        ))
    if not transport_coverage.get("metrolink_checked"):
        issues.append((
            90,
            "Metrolink не проверен как отдельный источник.",
            "Автобусы/TfGM и rail проверены, но по трамваям нет отдельного подтверждения.",
        ))
    ticket_types = ((quality_scorecard.get("today") or {}).get("ticket_types") or {})
    unknown_tickets = int((ticket_types.get("unknown") or {}).get("fetched") or 0)
    if unknown_tickets:
        issues.append((
            85,
            f"Билетный блок плохо классифицирует события: {unknown_tickets} материалов с типом «не распознан».",
            "Починить определение типа билета: старт продаж, пресейл, новый анонс или обычный календарь.",
        ))
    if int((synthetic_freshness or {}).get("stale_count") or 0) > 0:
        issues.append((
            80,
            f"Есть устаревшие служебные карточки: {synthetic_freshness.get('stale_count')}.",
            "Не доверять погоде или транспортной заглушке без проверки времени обновления.",
        ))
    anomalies = source_anomalies or []
    if anomalies:
        names = ", ".join(str(a.get("name") or "") for a in anomalies[:3])
        more = f" и ещё {len(anomalies) - 3}" if len(anomalies) > 3 else ""
        issues.append((
            78,
            f"Источники резко просели против своей нормы: {names}{more}.",
            "Источник раньше стабильно давал материалы, а сегодня почти ничего — обычно это сломавшийся парсер или смена вёрстки сайта.",
        ))
    dead = dead_parsers or []
    if dead:
        names = ", ".join(str(d.get("name") or "") for d in dead[:3])
        more = f" и ещё {len(dead) - 3}" if len(dead) > 3 else ""
        issues.append((
            77,
            f"Источники качаются, но парсер не достаёт ничего всю неделю: {names}{more}.",
            "Сайт отвечает (200), но из него не вытаскивается ни одного пункта — нужен отдельный парсер под этот источник.",
        ))
    if prompt_drift:
        issues.append((
            75,
            f"Промпты изменились без явного обновления версии: {len(prompt_drift)}.",
            "Поднять версию промпта или откатить незапланированное изменение.",
        ))
    unknown_models = cost_summary.get("unknown_priced_models") or []
    if unknown_models:
        issues.append((
            70,
            f"Учёт стоимости не знает цену моделей: {', '.join(str(m) for m in unknown_models)}.",
            "Добавить цены в таблицу стоимости, иначе стоимость дня может быть занижена.",
        ))
    if suspicious_published:
        issues.append((
            68,
            f"Самопроверка считает {len(suspicious_published)} опубликованных пункт(ов) подозрительными.",
            "Открыть блок «Что зря прошло в выпуск» и усилить gate.",
        ))
    if suspicious_rejects:
        issues.append((
            60,
            f"Есть {len(suspicious_rejects)} возможных ложных отклонений.",
            "Проверить «Возможно зря отклонили» и решить: чинить extraction или оставить отказ.",
        ))
    borderline_count = int(((borderline_queue or {}).get("counts") or {}).get("borderline") or 0)
    if borderline_count >= 20:
        issues.append((
            55,
            f"Слишком много спорных материалов: {borderline_count}.",
            "Разобрать топ причин borderline; вероятно, часть правил слишком широко матчится.",
        ))
    if int((source_status.get("counts") or {}).get("failed") or 0):
        issues.append((
            40,
            f"Не ответили источники: {(source_status.get('counts') or {}).get('failed')}.",
            "Проверить только если повторяется несколько дней или это ключевой источник.",
        ))
    issues.sort(key=lambda row: -row[0])
    return [(title, action) for _, title, action in issues[:3]]


def _diaspora_verdict_human(verdict: str) -> str:
    return {
        "checked_empty": "источники проверены, подходящих событий не найдено",
        "fetched_but_filtered": "события нашлись, но все отсеялись фильтрами",
        "accepted_not_rendered": "события прошли отбор, но не попали в финальный выпуск",
        "rendered": "русскоязычные события опубликованы",
        "not_checked": "источники не были проверены",
    }.get(str(verdict or ""), str(verdict or "неизвестно"))


def _event_miss_bucket(item: dict) -> str:
    verdict = str(item.get("verdict") or "")
    reason = str(item.get("reason") or "").lower()
    if verdict == "dedupe_lost_event":
        return "вероятная ошибка дедупликации"
    if verdict == "writer_dropped_event":
        return "ошибка генерации текста"
    if "без новых фактов" in reason or "no new facts" in reason or "no_change" in reason:
        return "вероятно корректно отклонено"
    if verdict in {"selected_but_not_published", "rejected_high_value_event"}:
        return "нужно продуктово решить"
    if verdict == "covered_by_rendered_duplicate":
        return "покрыто другим опубликованным пунктом"
    return "требует проверки"


def _event_miss_plain_reason(item: dict) -> str:
    verdict = str(item.get("verdict") or "")
    kept = str(item.get("kept_title") or "").strip()
    if verdict == "dedupe_lost_event":
        if kept:
            return f"похоже на ложный дубль с «{kept[:70]}»"
        return "дедупликация сняла событие как дубль"
    if verdict == "writer_dropped_event":
        return "событие было найдено, но финальный текст не прошёл проверку"
    if verdict == "selected_but_not_published":
        return "событие прошло отбор, но не вошло в финальный выпуск"
    reason = str(item.get("reason") or "").strip()
    return _humanize_source_reason(reason) if reason else "точная причина в JSON-отчёте"


def _event_miss_summary(event_miss_review: dict) -> dict[str, object]:
    items = list(event_miss_review.get("critical_misses") or [])
    counts = event_miss_review.get("counts") or {}
    total = int(counts.get("critical_misses") or len(items))
    buckets: dict[str, int] = {}
    for item in items:
        bucket = _event_miss_bucket(item if isinstance(item, dict) else {})
        buckets[bucket] = buckets.get(bucket, 0) + 1
    priority = {
        "вероятная ошибка дедупликации": 0,
        "ошибка генерации текста": 1,
        "нужно продуктово решить": 2,
        "требует проверки": 3,
        "вероятно корректно отклонено": 4,
        "покрыто другим опубликованным пунктом": 5,
    }
    top = sorted(
        [item for item in items if isinstance(item, dict)],
        key=lambda item: (
            priority.get(_event_miss_bucket(item), 99),
            int(item.get("days_out") if item.get("days_out") is not None else 999),
            -int(item.get("score") or 0),
        ),
    )[:3]
    return {"total": total, "shown_total": len(items), "buckets": buckets, "top": top}


def _russian_counted_phrase(count: int, one: str, few: str, many: str) -> str:
    number = abs(int(count))
    if number % 100 in {11, 12, 13, 14}:
        noun = many
    elif number % 10 == 1:
        noun = one
    elif number % 10 in {2, 3, 4}:
        noun = few
    else:
        noun = many
    return f"{count} {noun}"


def _compact_section_pressure(writer_report: dict) -> list[str]:
    rows = _section_shape_rows(writer_report)
    relevant = [
        row for row in rows
        if int(row.get("actual") or 0) >= 3
        and str(row.get("section") or "") not in {"Погода", "Главная история дня", "Общественный транспорт сегодня"}
    ]
    relevant.sort(key=lambda row: (-int(row.get("actual") or 0), str(row.get("section") or "")))
    out: list[str] = []
    for row in relevant[:5]:
        max_items = row.get("max")
        max_part = f", лимит секции {max_items}" if max_items is not None else ""
        out.append(f"{_section_name_human(str(row.get('section') or ''))}: {row.get('actual')}{max_part}")
    return out


def _transport_source_line(label: str, checked: bool, transport_coverage: dict) -> str:
    if checked:
        return f"• {label}: проверен."
    return f"• {label}: не проверен отдельно — транспортная картина неполная."


def _source_health_compact(source_status: dict) -> list[str]:
    counts = source_status.get("counts") or {}
    total_sources = sum(
        int(counts.get(key) or 0)
        for key in ("ok", "failed", "partial", "empty", "stale")
    )
    lines = [
        (
            f"Проверено источников: {total_sources}. Статусы: работают {counts.get('ok', 0)}, "
            f"не ответили {counts.get('failed', 0)}, пустые {counts.get('empty', 0)}, "
            f"без новых материалов {counts.get('stale', 0)}."
        )
    ]
    for status, label in (("failed", "Не ответили"), ("empty", "Пустые"), ("stale", "Без новых материалов")):
        rows = [
            row for row in (source_status.get("sources") or [])
            if isinstance(row, dict) and row.get("status") == status
        ]
        if rows and (status == "failed" or len(rows) <= 4):
            names = ", ".join(_source_name_human(str(row.get("name") or "")) for row in rows[:4])
            suffix = f" и ещё {len(rows) - 4}" if len(rows) > 4 else ""
            lines.append(f"{label}: {names}{suffix}.")
    if int(counts.get("zero_yield") or 0):
        lines.append(
            f"Отдельная метрика: {counts.get('zero_yield')} источника сработали, "
            "но ничего не дали в финальный выпуск; это пересекается со статусами выше."
        )
        rows = [
            row for row in (source_status.get("sources") or [])
            if isinstance(row, dict)
            and int(row.get("raw_count") or row.get("candidate_count") or 0) > 0
            and int(row.get("rendered_count") or 0) == 0
        ]
        for row in rows[:3]:
            human = row.get("human_funnel") if isinstance(row.get("human_funnel"), dict) else {}
            one_line = str(human.get("one_line") or "").strip()
            if one_line:
                lines.append(f"Воронка: {one_line}")
    return lines


def _humanize_llm_warning(warning: str) -> str:
    text = str(warning or "").strip()
    lowered = text.lower()
    m = re.search(r"(\d+)\s*/\s*(\d+)\s+draft_lines\s+written", text)
    if "yield low" in lowered and m:
        return f"модель написала {m.group(1)} из {m.group(2)} текстов; часть материалов осталась без нормального финального текста"
    if "weak" in lowered and "draft_line" in lowered:
        return "после автоматического ремонта остались слабые тексты; они должны быть удержаны или проверены"
    if "provider fallback" in lowered:
        return "основной маршрут модели был нестабилен, использовался запасной провайдер"
    return text[:140]


def _support_actions(
    *,
    rendered: int,
    event_summary: dict,
    transport_coverage: dict,
    ticket_types: dict,
    writer_report: dict,
    warnings: list[str],
    borderline_count: int,
) -> list[str]:
    actions: list[str] = []
    buckets = event_summary.get("buckets") or {}
    if int(buckets.get("вероятная ошибка дедупликации") or 0):
        actions.append("Авторазбор: проверить ложные дубли событий, потому что они могут скрывать концерты и weekend events.")
    if int(buckets.get("ошибка генерации текста") or 0):
        actions.append("Авторазбор: проверить события, снятые writer; возможно, факты были, но текст не собрался.")
    if int((ticket_types.get("unknown") or {}).get("fetched") or 0):
        actions.append("Разобрать билетные карточки с нераспознанным типом; такие не должны публиковаться автоматически.")
    if int((ticket_types.get("old_public_sale") or {}).get("published") or 0):
        actions.append("Убрать старые продажи из Ticket Radar, если нет нового повода для читателя.")
    if not transport_coverage.get("metrolink_checked"):
        actions.append("Сделать Metrolink отдельной проверкой, не прятать его внутри TfGM.")
    degraded = any(str(w).lower().startswith("llm rewrite was degraded") for w in warnings)
    shrink = writer_report.get("degraded_shrink") or {}
    if degraded:
        if int(shrink.get("dropped_count") or 0):
            actions.append("Авторазбор: сверить, что осторожный режим удержал слабые пункты, а не важные концерты/события.")
        else:
            actions.append("Авторазбор: генерация была нестабильной, но осторожный режим ничего не снял.")
    if borderline_count >= 20:
        actions.append("Авторазбор: сгруппировать удержанные материалы по причинам и найти слишком широкие правила.")
    if rendered > 45:
        actions.append("Отдельно принять global budget для выпуска: сейчас секции по отдельности могут быть нормальными, а выпуск раздут.")
    return actions[:5]


def _build_product_support_text(report: dict, writer_report: dict) -> str:
    run_date = report.get("run_date_london") or ""
    health = report.get("digest_health") or {}
    health_level = str(health.get("risk_level") or "healthy")
    icon = {"healthy": "✅", "at_risk": "🟡", "unhealthy": "🔴"}.get(health_level, "⚠️")
    qc = (writer_report.get("quality_counts") or {}) if isinstance(writer_report, dict) else {}
    included = int(qc.get("included_candidates") or 0)
    rendered = int(qc.get("rendered_candidates") or 0)
    dropped = (
        int(qc.get("dropped_missing_draft_line") or 0)
        + int(qc.get("dropped_english_passthrough") or 0)
        + int(qc.get("dropped_low_quality") or 0)
        + int(qc.get("blocked_for_quality") or 0)
    )
    warnings = [str(w) for w in (report.get("warnings") or [])]
    source_status = report.get("source_status") or {}
    transport_coverage = report.get("transport_coverage") or {}
    quality_scorecard = report.get("quality_scorecard") or {}
    today_quality = (quality_scorecard.get("today") or {}) if isinstance(quality_scorecard, dict) else {}
    ticket_types = today_quality.get("ticket_types") or {}
    event_summary = _event_miss_summary(report.get("event_miss_review") or {})
    borderline_queue = report.get("borderline_queue") or {}
    borderline_count = int((borderline_queue.get("counts") or {}).get("borderline") or 0)
    llm_report = read_json(PROJECT_ROOT / "data" / "state" / "llm_rewrite_report.json", {})
    degraded = any(str(w).lower().startswith("llm rewrite was degraded") for w in warnings)
    shrink = writer_report.get("degraded_shrink") or {}

    if report.get("release_decision") == "pass":
        if rendered > 45:
            header = f"{icon} Выпуск {run_date}: отправлен с риском — опубликовано {rendered} пунктов (цель 14–45)"
        else:
            header = f"{icon} Выпуск {run_date}: отправлен — опубликовано {rendered} пунктов"
    else:
        header = f"⛔ Выпуск {run_date}: НЕ отправлен — проверь release_report.json"

    lines: list[str] = [header, ""]
    lines.append("📌 Вердикт")
    if rendered > 45:
        lines.append("Выпуск слишком длинный: читателю трудно отделить важное от второстепенного.")
    elif degraded:
        lines.append("Объём выпуска нормальный, но есть риск качества: генерация текста работала нестабильно.")
    elif int(event_summary.get("total") or 0):
        lines.append("Выпуск отправлен, но есть возможные пропуски событий или билетов.")
    else:
        lines.append("Критичных продуктовых проблем не найдено.")
    lines.append(f"Опубликовано: {rendered}; прошло первичный редакционный отбор: {included}.")
    lines.append("")

    # 📊 Honest end-to-end funnel from ONE cohort. The earlier version
    # mixed two populations: it showed included→rendered (a small set)
    # but pulled the breakdown from final_loss_check, which counts the
    # full collected pool (~600). That made the numbers (dedupe 140,
    # rejected 274) dwarf the stated "lost 75". Now every number is the
    # real cohort: collected → included → sent to text → published, and
    # the "no text" line is the real LLM miss count, not writer drops.
    llm_rewrite = llm_report if isinstance(llm_report, dict) else {}
    sent_to_text = int(llm_rewrite.get("included_for_rewrite") or 0)
    no_text = len(llm_rewrite.get("missing_after") or [])
    weak_text = len(llm_rewrite.get("weak_after") or [])
    held_backup = int(((llm_rewrite.get("rewrite_shortlist") or {}).get("held_for_backup")) or 0)
    backup_counts = ((report.get("backup_pool") or {}).get("counts") or {})
    lines.append("📊 Воронка дня (по одному и тому же набору)")
    lines.append(f"• Прошло редакционный отбор: {included}.")
    if sent_to_text:
        lines.append(f"• Отправлено на генерацию текста: {sent_to_text} (остальные — готовый текст или придержаны в резерве: {held_backup}).")
    lines.append(f"• Опубликовано: {rendered}.")
    if dropped:
        lines.append(f"• Снято на этапе писателя (нет текста / англ. / низкое качество): {dropped}.")
    if no_text:
        lines.append(f"• Модель решила пропустить (нет/мало фактуры в источнике): {no_text} из {sent_to_text}.")
    if weak_text:
        lines.append(f"• Текст написан, но слабый после ремонта: {weak_text}.")
    lines.append("Примечание: разбор «дубли / отклонено правилами» считается по всему собранному пулу (~сотни), а не по этому набору — детали в JSON (final_loss_check).")
    lines.append("")

    issues = _support_top_issues(
        rendered=rendered,
        health_level=health_level,
        health_signals=health.get("signals") or [],
        writer_report=writer_report,
        transport_coverage=transport_coverage,
        quality_scorecard=quality_scorecard,
        source_status=source_status,
        synthetic_freshness=report.get("synthetic_freshness") or {},
        prompt_drift=report.get("prompt_drift") or [],
        cost_summary=report.get("cost_summary") or {},
        warnings=warnings,
        suspicious_rejects=(report.get("reject_review") or {}).get("suspiciously_rejected") or [],
        suspicious_published=(report.get("published_review") or {}).get("suspiciously_published") or [],
        borderline_queue=borderline_queue,
        source_anomalies=report.get("source_anomalies") or [],
        dead_parsers=report.get("dead_parsers") or [],
        event_miss_review=report.get("event_miss_review") or {},
    )
    if issues:
        lines.append("🚨 Главное сегодня")
        for idx, (title, action) in enumerate(issues, start=1):
            lines.append(f"{idx}. {title}")
            lines.append(f"   Что значит: {action}")
        lines.append("")

    pressure = _compact_section_pressure(writer_report)
    if pressure:
        if rendered > 45:
            lines.append("📐 Что раздуло выпуск")
        else:
            lines.append("📐 Состав выпуска")
        for item in pressure:
            lines.append(f"• {item}.")
        if rendered > 45:
            lines.append("Проблема: сумма секций делает выпуск слишком длинным. Полная разбивка сохранена в JSON.")
        else:
            lines.append("Объём в норме; проблема сегодня не длина выпуска, а пропуски событий и нестабильная генерация.")
        lines.append("")

    if ticket_types or _ticketmaster_rows(source_status):
        lines.append("🎟️ Билеты и события")
        total_ticket_found = 0
        total_ticket_published = 0
        if ticket_types:
            for ticket_type in ("event_this_week", "on_sale_now", "presale_soon", "newly_listed", "major_upcoming", "regular_upcoming", "old_public_sale", "old_onsale", "unknown"):
                counts = ticket_types.get(ticket_type)
                if not counts:
                    continue
                total_ticket_found += int(counts.get("fetched") or 0)
                total_ticket_published += int(counts.get("published") or 0)
                lines.append(
                    f"• {_ticket_type_human(ticket_type)}: найдено {counts.get('fetched', 0)}, "
                    f"опубликовано {counts.get('published', 0)}."
                )
        if total_ticket_found and not total_ticket_published:
            lines.append("Итог: билетный блок пустой не потому, что ничего не найдено; фильтры/проверки не дали ни одной карточке пройти в выпуск.")
        ticket_rows = _ticketmaster_rows(source_status)
        if ticket_rows:
            zero_published = sum(1 for row in ticket_rows if int(row.get("raw_count") or 0) and not int(row.get("rendered_count") or 0))
            lines.append(f"• Ticketmaster-источников без публикаций: {zero_published}; детали по каждому источнику в JSON.")
        lines.append("")

    if int(event_summary.get("total") or 0):
        lines.append("⚠️ Возможные пропуски событий")
        buckets = event_summary.get("buckets") or {}
        bucket_text = ", ".join(f"{name}: {count}" for name, count in sorted(buckets.items()))
        shown_total = int(event_summary.get("shown_total") or 0)
        shown_tail = f"; в Telegram показан топ-{min(shown_total, 3)}" if shown_total else ""
        lines.append(
            f"Система пометила {_russian_counted_phrase(int(event_summary.get('total') or 0), 'возможный пропуск', 'возможных пропуска', 'возможных пропусков')}{shown_tail}. "
            "Это не значит, что все они точно должны были выйти."
        )
        if bucket_text:
            lines.append(f"В первых {shown_total} примерах из JSON: {bucket_text}.")
        for item in event_summary.get("top") or []:
            days_out = item.get("days_out")
            when = "сегодня" if days_out == 0 else f"через {days_out} дн." if isinstance(days_out, int) else "дата рядом"
            lines.append(f"• {str(item.get('title') or '')[:80]} ({when})")
            lines.append(f"  Вердикт: {_event_miss_bucket(item)} — {_event_miss_plain_reason(item)}.")
        lines.append("Вывод: это автозадача для event-dedupe/writer, а не список для ручного чтения утром.")
        lines.append("")

    backup_pool = report.get("backup_pool") or {}
    backup_active = int((backup_pool.get("counts") or {}).get("active") or 0)
    if backup_active:
        backup_items = backup_pool.get("items") or []
        future_blocks = {"weekend_activities", "next_7_days", "future_announcements",
                         "ticket_radar", "outside_gm_tickets", "russian_events"}
        future_count = sum(1 for it in backup_items if str(it.get("primary_block") or "") in future_blocks)
        news_count = len(backup_items) - future_count
        lines.append("🗂 В резерве (не «за 24 часа»)")
        lines.append(
            f"Всего {backup_active}: это в основном будущие события/билеты "
            f"(~{future_count} — концерты, ярмарки, выставки на ближайшие недели), "
            f"а не сегодняшние новости."
        )
        if news_count:
            lines.append(
                f"Из них ~{news_count} — новости в коротком резерве на добор, если завтра секция окажется тонкой; "
                "у каждой свой TTL, устаревшие удаляются автоматически."
            )
        lines.append("")

    if borderline_count:
        lines.append("🟨 Удержано для проверки")
        lines.append(f"Удержано {_russian_counted_phrase(borderline_count, 'материал', 'материала', 'материалов')}; полный список скрыт из Telegram и сохранён в JSON.")
        # Prefer the per-reason histogram computed by release.py
        # _borderline_queue (E16 audit fix). Falls back to scanning
        # quality_warnings if older release_report shape is read.
        by_reason = ((borderline_queue.get("counts") or {}).get("by_reason") or {}) if isinstance(borderline_queue, dict) else {}
        reason_counter: dict[str, int] = {}
        if isinstance(by_reason, dict) and by_reason:
            for raw_reason, count in by_reason.items():
                reason_counter[_humanize_quality_warning(str(raw_reason))] = (
                    reason_counter.get(_humanize_quality_warning(str(raw_reason)), 0) + int(count or 0)
                )
        else:
            for item in (borderline_queue.get("items") or []):
                for warning in item.get("quality_warnings") or []:
                    reason = _humanize_quality_warning(str(warning))
                    reason_counter[reason] = reason_counter.get(reason, 0) + 1
        for reason, count in sorted(reason_counter.items(), key=lambda kv: -kv[1])[:3]:
            lines.append(f"• {reason}: {count}.")
        lines.append("Смысл: это карантин на ручную проверку, а не выброшенные новости; если сюда попадает важный материал — значит правило отбора слишком широкое и его надо сузить.")
        for item in (borderline_queue.get("items") or [])[:3]:
            title = str(item.get("title") or "").strip()
            lines.append(f"• Пример: {title[:85]}")
        lines.append("")

    transport_verdict = str(transport_coverage.get("verdict") or "")
    if transport_verdict:
        lines.append("🚋 Транспорт")
        lines.append(_transport_source_line("TfGM", bool(transport_coverage.get("tfgm_checked")), transport_coverage))
        lines.append(_transport_source_line("Metrolink", bool(transport_coverage.get("metrolink_checked")), transport_coverage))
        lines.append(_transport_source_line("National Rail", bool(transport_coverage.get("national_rail_checked")), transport_coverage))
        if transport_verdict == "disruptions_rendered":
            lines.append(
                f"Найдено ограничений/сбоев: {transport_coverage.get('disruptions_found', 0)}, "
                f"опубликовано: {transport_coverage.get('disruptions_rendered', 0)}."
            )
        elif transport_verdict == "checked_no_disruptions":
            lines.append("Серьёзных ограничений не найдено.")
        elif transport_verdict == "found_not_rendered":
            lines.append("Ограничения найдены, но не опубликованы — это нужно проверить.")
        if not transport_coverage.get("metrolink_checked"):
            lines.append("Смысл: транспортные ограничения показаны, но отдельного подтверждения по трамваям нет.")
        lines.append("")

    if degraded or shrink:
        lines.append("🤖 Генерация текста")
        # Which model(s) actually ran, and did the DeepSeek last-resort kick in.
        routes = llm_report.get("model_route") or []
        if isinstance(routes, list) and routes:
            primary = next((r for r in routes if int(r.get("priority") or 9) == 1), routes[0])
            primary_name = f"{primary.get('provider_label') or primary.get('provider')} {primary.get('model')}".strip()
            lines.append(f"Основная модель: {primary_name}.")
            last_resort = [r for r in routes if str(r.get("role") or "").endswith("last_resort")]
            lr_writes = llm_report.get("last_resort_writes") or []
            if last_resort:
                lr_name = f"{last_resort[0].get('provider_label') or last_resort[0].get('provider')} {last_resort[0].get('model')}".strip()
                if lr_writes:
                    lines.append(f"Резерв {lr_name}: включался, дописал {len(lr_writes)} материалов, где основная модель вернула пустоту.")
                else:
                    lines.append(f"Резерв {lr_name}: подключён, но не понадобился (основная модель справилась).")
        applied = llm_report.get("applied")
        total = llm_report.get("included_for_rewrite")
        if applied is not None and total is not None:
            missing = max(0, int(total or 0) - int(applied or 0))
            lines.append(f"Материалов без финального текста: {missing} (из {total} отправленных на генерацию; счётчик дописанных включает recovery-проходы).")
        weak_after = llm_report.get("weak_after") or []
        if weak_after:
            lines.append(f"После ремонта текста всё ещё слабых карточек: {len(weak_after)}.")
        if llm_report.get("warnings"):
            lines.append(f"Предупреждение: {_humanize_llm_warning(str((llm_report.get('warnings') or [''])[0]))}.")
        if shrink:
            lines.append(
                f"Осторожный режим: удержано {int(shrink.get('dropped_count') or 0)} низкоприоритетных пунктов; "
                "список сохранён в writer_report.degraded_shrink."
            )
        lines.append("")

    if source_status.get("sources"):
        lines.append("📡 Источники")
        failed = int((source_status.get("counts") or {}).get("failed") or 0)
        if not failed:
            lines.append("• Критичных падений источников нет.")
        for line in _source_health_compact(source_status):
            lines.append(f"• {line}")
        lines.append("Смысл: «без вклада» не значит плохой источник; многие источники резервные или сегодня не дали материала уровня выпуска.")
        lines.append("")

    actions = _support_actions(
        rendered=rendered,
        event_summary=event_summary,
        transport_coverage=transport_coverage,
        ticket_types=ticket_types,
        writer_report=writer_report,
        warnings=warnings,
        borderline_count=borderline_count,
    )
    if actions:
        lines.append("🔧 Что система должна разобрать до завтра")
        for idx, action in enumerate(actions, start=1):
            lines.append(f"{idx}. {action}")
        lines.append("")

    lines.append("Технические списки, ID, source funnel и полные очереди сохранены в JSON-отчётах, не в Telegram.")
    return "\n".join(lines).rstrip()


def _source_streak_tag(source_name: str, today_iso: str) -> str:
    """Look up how many days in a row this source has been failing.

    Reads data/state/daily_index/*.jsonl files (last 14 days) and checks
    whether the source had ANY successful candidate ingest on each day.
    Returns a Russian tag like "новая проблема", "падает 3 дня подряд",
    "падает неделю — пора отключить".
    """
    snapshot_dir = PROJECT_ROOT / "data" / "state" / "daily_index"
    if not snapshot_dir.exists():
        return ""
    try:
        today = datetime.strptime(today_iso, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return ""
    # Look back up to 14 days; count consecutive days where the source
    # produced ZERO records in the snapshot (means it failed every fetch).
    streak = 0
    for back in range(0, 14):
        day = today - timedelta(days=back)
        path = snapshot_dir / f"{day.isoformat()}.jsonl"
        if not path.exists():
            # No snapshot for that day — can't tell, stop counting.
            break
        had_success = False
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if rec.get("source_label") == source_name:
                had_success = True
                break
        if had_success:
            break
        streak += 1
    if streak <= 1:
        return "новая проблема (первый день)"
    if streak <= 3:
        return f"падает {streak} дня подряд"
    if streak <= 6:
        return f"падает {streak} дней подряд"
    return f"падает {streak} дней подряд — пора отключить источник"


# Classification of writer/curator suspicious-reject reasons into
# human-friendly groups. Each group has:
#   label — what to call it for an editor
#   plain — one-sentence "что произошло" in plain Russian
#   editor_action — what the editor (not developer) should do, or None
#                   if the system handles it automatically next time
_REJECT_GROUPS = {
    "money_format": {
        "label": "Карточки с £-суммами не прошли автопроверку",
        "plain": (
            "Система переводит «£150m» → «£150 млн», потом сверяет с оригиналом и не "
            "находит точную форму. До сегодняшнего фикса считала это «выдуманной суммой» "
            "и выкидывала карточку, хотя сумма реальная."
        ),
        "editor_action": None,  # auto-fixed
    },
    "evidence_thin": {
        "label": "У статьи был только заголовок-тизер без сути",
        "plain": (
            "Источник прислал «What on earth is going on in flat 3203?» — и всё. "
            "Без paywall-доступа карточка была бы вода."
        ),
        "editor_action": (
            "Если такой источник встречается каждый день — стоит подумать, "
            "оставлять ли его в реестре."
        ),
    },
    "missing_draft": {
        "label": "LLM не смог написать текст",
        "plain": "LLM посмотрел кандидата и решил, что фактов слишком мало для самодостаточного пункта.",
        "editor_action": (
            "Если есть пример где LLM зря промолчал — пришли, ослаблю правила."
        ),
    },
    "bad_prose": {
        "label": "Готовая карточка содержала клише («не пропустите», «уточняйте»)",
        "plain": "Сработал стоп-словарь редактуры, который мы вместе собирали.",
        "editor_action": "Если фраза не клише — скажи, уберу из словаря.",
    },
    "evergreen_with_date": {
        "label": "Событие могло быть отклонено как «без даты»",
        "plain": (
            "Дата могла быть не в заголовке, а ниже в тексте или в метаданных. "
            "Если это повторяется на одном источнике, нужно чинить извлечение дат для него."
        ),
        "editor_action": "системная задача — проверить извлечение дат из полного текста для этого источника; от тебя действий не требуется.",
    },
    "other": {
        "label": "Прочие отказы",
        "plain": "Не попали ни в один типовой шаблон.",
        "editor_action": "Если их много — пришли пример, разберусь руками.",
    },
}


def _classify_reject_reason(text: str) -> str:
    t = (text or "").lower()
    if "pound amount" in t or "£" in t:
        return "money_format"
    if "padded from thin evidence" in t or ("thin" in t and "evidence" in t):
        return "evidence_thin"
    if "missing draft_line" in t:
        return "missing_draft"
    if "bad editorial prose" in t:
        return "bad_prose"
    if "evergreen" in t:
        return "evergreen_with_date"
    return "other"


def _group_suspicious_rejects(rejects: list) -> dict:
    """Group writer/curator suspicious rejects by root cause + carry titles."""
    out: dict[str, dict] = {}
    for r in rejects:
        if r.get("stage") == "writer":
            raw_reason = "; ".join(str(x) for x in (r.get("reasons") or []))
        else:
            raw_reason = r.get("why_flagged") or r.get("reason") or ""
        cause = _classify_reject_reason(raw_reason)
        bucket = out.setdefault(cause, {
            **_REJECT_GROUPS[cause],
            "id": cause,
            "count": 0,
            "examples": [],
        })
        bucket["count"] += 1
        title = str(r.get("title") or "").strip() or "(без заголовка)"
        # Trim "as part of £150m improvement works" tails that just repeat
        # the £ amount we already flagged. Keep first 90 chars of headline.
        bucket["examples"].append(title[:90])
    return out


def _humanize_writer_reason(text: str) -> str:
    """Translate a writer-drop reason into a one-line plain explanation."""
    t = (text or "").lower()
    if "pound amount" in t:
        return "не прошёл автопроверку £-суммы (формат различался — уже исправлено)"
    if "padded from thin evidence" in t:
        return "у статьи только тизер без фактов, текст бы получился вода"
    if "missing draft_line" in t:
        return "LLM не смог написать самодостаточный пункт"
    if "bad editorial prose" in t:
        return "сработал стоп-словарь редактуры"
    if "long-format category" in t:
        return "карточка вышла короче минимума для этой темы"
    if "too short" in t:
        return "карточка короче 45 символов"
    if "english" in t or "untranslated" in t:
        return "карточка осталась на английском"
    return (text or "причина не записана")[:120]


def _section_drops(section_name: str) -> list[tuple[str, str]]:
    """Pull writer-dropped candidates aimed at `section_name`.

    Returns list of (title, plain_reason).
    """
    writer_state_path = PROJECT_ROOT / "data" / "state" / "writer_report.json"
    if not writer_state_path.exists():
        return []
    try:
        wr = json.loads(writer_state_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    block_to_section = {
        "weather": "Погода",
        "transport": "Общественный транспорт сегодня",
        "today_focus": "Что важно сегодня",
        "last_24h": "Свежие новости",
        "lead_story": "Главная история дня",
        "city_watch": "Городской радар",
        "weekend_activities": "Выходные в GM",
        "next_7_days": "Что важно в ближайшие 7 дней",
        "future_announcements": "Дальние анонсы",
        "ticket_radar": "Билеты / Ticket Radar",
        "outside_gm_tickets": "Крупные концерты вне GM",
        "russian_events": "Русскоязычные концерты и стендап UK",
        "openings": "Еда, открытия и рынки",
        "tech_business": "IT и бизнес",
        "football": "Футбол",
        "district_radar": "Радар по районам",
    }
    section_to_block = {v: k for k, v in block_to_section.items()}
    target_block = section_to_block.get(section_name)
    if not target_block:
        return []
    out: list[tuple[str, str]] = []
    for d in (wr.get("dropped_candidates") or []):
        if d.get("primary_block") != target_block:
            continue
        title = str(d.get("title") or "")[:85]
        reasons = "; ".join(str(x) for x in (d.get("reasons") or []))
        out.append((title, _humanize_writer_reason(reasons)))
    return out


def cmd_send_warnings() -> int:
    """Post a short admin message to Telegram if release_report flagged
    lost leads or section underflow. Opt-out with WARNINGS_TO_TELEGRAM=0.

    Skips silently when:
      - WARNINGS_TO_TELEGRAM=0 (kill switch)
      - release_report.json is missing
      - there are no lost_leads and no section_underflow
    """
    if os.environ.get("WARNINGS_TO_TELEGRAM", "1").strip() in {"0", "false", "False", ""}:
        print("Warnings-to-Telegram disabled (WARNINGS_TO_TELEGRAM=0). Skipping.")
        return 0

    report_path = PROJECT_ROOT / "data" / "state" / "release_report.json"
    if not report_path.exists():
        print(f"No release_report.json at {report_path}. Skipping.")
        return 0

    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"Could not read release_report.json: {exc}. Skipping.")
        return 0

    lost_leads = report.get("lost_leads") or []
    section_underflow = report.get("section_underflow") or []
    health = report.get("digest_health") or {}
    health_level = str(health.get("risk_level") or "healthy")
    health_signals = health.get("signals") or []
    summary = report.get("after_run_summary") or {}
    source_status = report.get("source_status") or {}
    transport_coverage = report.get("transport_coverage") or {}
    diaspora_diagnostics = report.get("diaspora_diagnostics") or {}
    borderline_queue = report.get("borderline_queue") or {}
    quality_scorecard = report.get("quality_scorecard") or {}
    feedback_capture = report.get("feedback_capture") or {}
    synthetic_freshness = report.get("synthetic_freshness") or {}
    cost_summary = report.get("cost_summary") or {}
    prompt_drift = report.get("prompt_drift") or []
    reject_review = report.get("reject_review") or {}
    published_review = report.get("published_review") or {}
    city_intelligence = report.get("city_intelligence") or {}
    event_miss_review = report.get("event_miss_review") or {}
    cross_day_recurrence = report.get("cross_day_recurrence") or {}
    event_completeness = report.get("event_completeness") or {}
    news_lead_quality = report.get("news_lead_quality") or {}
    post_publish_judge = report.get("post_publish_judge") or {}
    borough_coverage = city_intelligence.get("borough_coverage") or {}
    borough_skew_flags = [
        str(flag) for flag in (borough_coverage.get("skew_flags") or []) if str(flag).strip()
    ]
    suspicious_rejects = reject_review.get("suspiciously_rejected") or []
    suspicious_published = published_review.get("suspiciously_published") or []
    src_counts = source_status.get("counts") or {}
    failed_sources = [
        s for s in (source_status.get("sources") or []) if s.get("status") == "failed"
    ]

    # Trigger when there is anything worth surfacing.
    cross_day_blocked = int(((cross_day_recurrence.get("counts") or {}).get("blocked") or 0))
    ec_counts = event_completeness.get("counts") or {}
    event_incomplete = int(ec_counts.get("missing_date") or 0) + int(ec_counts.get("missing_venue") or 0)
    lead_counts = news_lead_quality.get("counts") or {}
    bad_leads = int(lead_counts.get("quote_lead") or 0) + int(lead_counts.get("narrative_lead") or 0)
    judge_signals = list((post_publish_judge.get("drift") or {}).get("signals") or [])
    has_signal = (
        report.get("release_decision") != "pass"
        or bool(report.get("warnings"))
        or bool(lost_leads)
        or bool(section_underflow)
        or health_level != "healthy"
        or bool(suspicious_rejects)
        or bool(suspicious_published)
        or bool(borough_skew_flags)
        or transport_coverage.get("verdict") in {"found_not_rendered", "not_checked", "partially_checked"}
        or diaspora_diagnostics.get("verdict") in {"checked_empty", "fetched_but_filtered", "accepted_not_rendered"}
        or bool((borderline_queue.get("items") or []))
        or int(((event_miss_review.get("counts") or {}).get("critical_misses") or 0)) > 0
        or bool(failed_sources)
        or int((synthetic_freshness or {}).get("stale_count") or 0) > 0
        or bool(prompt_drift)
        or bool((cost_summary or {}).get("unknown_priced_models") or [])
        or cross_day_blocked > 0
        or event_incomplete > 0
        or bad_leads > 0
        or bool(judge_signals)
    )
    if not has_signal:
        print("Healthy run with no signals. Nothing to alert on.")
        return 0

    if os.environ.get("SUPPORT_REPORT_LEGACY", "0").strip() not in {"1", "true", "True"}:
        writer_report = read_json(PROJECT_ROOT / "data" / "state" / "writer_report.json", {})
        text = _build_product_support_text(report, writer_report if isinstance(writer_report, dict) else {})
        settings, client, store = _load_store_and_client()
        targets = _effective_targets(settings.telegram_target, store.list_subscribers())
        if not targets:
            print("No Telegram targets configured. Skipping admin warnings.")
            return 0
        for target in targets:
            client.send_text_in_chunks(target, text, parse_mode=None)
        print(f"Sent warnings to {len(targets)} target(s).")
        return 0

    run_date = report.get("run_date_london") or ""
    release_decision = str(report.get("release_decision") or "").strip()
    release_errors = [str(e) for e in (report.get("errors") or []) if str(e).strip()]
    icon = {"healthy": "✅", "at_risk": "🟡", "unhealthy": "🔴"}.get(health_level, "⚠️")
    useful = summary.get("useful_items", "?")
    writer_report = read_json(PROJECT_ROOT / "data" / "state" / "writer_report.json", {})
    qc = (writer_report.get("quality_counts") or {}) if isinstance(writer_report, dict) else {}
    included = int(qc.get("included_candidates") or 0)
    rendered = int(qc.get("rendered_candidates") or 0)
    dropped = (
        int(qc.get("dropped_missing_draft_line") or 0)
        + int(qc.get("dropped_english_passthrough") or 0)
        + int(qc.get("dropped_low_quality") or 0)
        + int(qc.get("blocked_for_quality") or 0)
    )

    # ── Заголовок ─────────────────────────────────────────────────
    if release_decision == "pass":
        if health_level == "at_risk" and rendered > 45:
            header = f"{icon} Выпуск {run_date}: отправлен, но слишком длинный — {rendered} пунктов (норма 14–45)"
        else:
            header = f"{icon} Выпуск {run_date}: отправлен — {useful} пунктов"
    else:
        reason = f": {release_errors[0]}" if release_errors else ""
        header = f"⛔ Выпуск {run_date} НЕ отправлен — release gate {release_decision or 'unknown'}{reason}"
    lines: list[str] = [header, ""]

    if rendered or included or dropped:
        lines.append("📌 ИТОГ ВЫПУСКА")
        lines.append(f"  • Читатель увидел: {rendered} пунктов.")
        lines.append(f"  • После редакционного отбора осталось: {included} материалов.")
        lines.append(f"  • На финальной проверке снято: {dropped} материалов с плохим/пустым текстом.")
        lines.append("  • Норма для утреннего выпуска: 14–45 пункта.")
        lines.append("")

    top_issues = _support_top_issues(
        rendered=rendered,
        health_level=health_level,
        health_signals=health_signals,
        writer_report=writer_report,
        transport_coverage=transport_coverage,
        quality_scorecard=quality_scorecard,
        source_status=source_status,
        synthetic_freshness=synthetic_freshness,
        prompt_drift=prompt_drift,
        cost_summary=cost_summary,
        warnings=[str(w) for w in (report.get("warnings") or [])],
        suspicious_rejects=suspicious_rejects,
        suspicious_published=suspicious_published,
        borderline_queue=borderline_queue,
        source_anomalies=report.get("source_anomalies") or [],
        dead_parsers=report.get("dead_parsers") or [],
        event_miss_review=event_miss_review,
    )
    if top_issues:
        lines.append("🚨 ГЛАВНОЕ СЕГОДНЯ")
        for idx, (title, action) in enumerate(top_issues, start=1):
            lines.append(f"  {idx}. {title}")
            lines.append(f"     Что делать: {action}")
        lines.append("")

    section_rows = _section_shape_rows(writer_report)
    if section_rows:
        lines.append("📐 СЕКЦИИ И ЛИМИТЫ")
        lines.append("Показывает, какие блоки раздувают или просаживают выпуск:")
        for row in sorted(section_rows, key=lambda r: (-int(r["actual"]), str(r["section"]))):
            actual = int(row["actual"])
            if actual == 0 and row["min"] is None and row["max"] is None:
                continue
            min_part = f"минимум {row['min']}" if row["min"] is not None else "без минимума"
            max_part = f"лимит {row['max']}" if row["max"] is not None else "без лимита"
            lines.append(f"  • {_section_name_human(str(row['section']))}: {actual} ({min_part}, {max_part}) — {row['status']}.")
        lines.append("")

    today_quality = (quality_scorecard.get("today") or {}) if isinstance(quality_scorecard, dict) else {}
    ticket_types = today_quality.get("ticket_types") or {}
    ticket_rows = _ticketmaster_rows(source_status)
    if ticket_types or ticket_rows:
        lines.append("🎟️ БИЛЕТЫ И КОНЦЕРТЫ")
        if ticket_types:
            for ticket_type, counts in sorted(ticket_types.items()):
                lines.append(
                    f"  • {_ticket_type_human(ticket_type)}: найдено {counts.get('fetched', 0)}, "
                    f"опубликовано {counts.get('published', 0)}."
                )
        if ticket_rows:
            lines.append("  Источники билетов:")
            for row in ticket_rows[:6]:
                reasons = row.get("reject_reasons") or {}
                reason_txt = ""
                if reasons:
                    reason, count = sorted(reasons.items(), key=lambda kv: -int(kv[1]))[0]
                    reason_txt = f"; причина: {_humanize_source_reason(reason)} ({count})"
                lines.append(f"    • {_source_name_human(str(row.get('name') or ''))}: {_source_counts_phrase(row)}{reason_txt}.")
        lines.append("")

    event_miss_items = event_miss_review.get("critical_misses") or []
    if event_miss_items:
        lines.append("🎭 ВАЖНЫЕ СОБЫТИЯ, КОТОРЫЕ НЕ ДОШЛИ")
        lines.append("Система нашла событие/билет с датой в ближайшие дни, но оно не попало к читателю:")
        for item in event_miss_items[:8]:
            title = str(item.get("title") or "").strip() or "(без заголовка)"
            source = str(item.get("source_label") or "").strip()
            verdict = str(item.get("verdict") or "")
            days_out = item.get("days_out")
            when = "сегодня" if days_out == 0 else f"через {days_out} дн." if isinstance(days_out, int) else "дата рядом"
            kept = str(item.get("kept_title") or "").strip()
            source_tail = f" — {source}" if source else ""
            lines.append(f"  • {title[:90]}{source_tail} ({when})")
            if verdict == "dedupe_lost_event" and kept:
                lines.append(f"    Что случилось: дедупликация сочла дублем другого материала: {kept[:100]}.")
            elif verdict == "selected_but_not_published":
                lines.append("    Что случилось: материал прошёл отбор, но выпал на финальных лимитах секции.")
            elif verdict == "writer_dropped_event":
                lines.append("    Что случилось: материал снял writer на финальной проверке текста.")
            else:
                reason = str(item.get("reason") or "").strip()
                lines.append(f"    Что случилось: {reason[:140] if reason else 'точная причина в release_report.event_miss_review'}.")
        if len(event_miss_items) > 8:
            lines.append(f"  …и ещё {len(event_miss_items) - 8}.")
        lines.append("  Это критический сигнал: выпуск отправлен, но эти причины нужно разобрать до следующего запуска.")
        lines.append("")

    # ── Что могли пропустить (главное для редактора) ──────────────
    suspicious_groups = _group_suspicious_rejects(suspicious_rejects) if suspicious_rejects else {}
    actionable_groups = {k: v for k, v in suspicious_groups.items()
                         if v.get("editor_action") is not None and k not in _ALREADY_FIXED_CAUSES}
    auto_fixed_groups = {k: v for k, v in suspicious_groups.items()
                         if k in _ALREADY_FIXED_CAUSES}

    if suspicious_groups:
        lines.append("📰 ВОЗМОЖНО ЗРЯ ОТКЛОНИЛИ")
        lines.append("Материалы, которые система увидела, но не опубликовала; причина отказа выглядит спорной:")
        lines.append("")
        # Show actionable first — что реально требует внимания
        for cause_id, info in sorted(actionable_groups.items(), key=lambda kv: -kv[1]["count"]):
            lines.append(f"  «{info['label']}» — {info['count']} шт.")
            lines.append(f"    Что произошло: {info['plain']}")
            for ex in info["examples"][:3]:
                lines.append(f"    • {ex}")
            if len(info["examples"]) > 3:
                lines.append(f"    • …ещё {len(info['examples']) - 3}")
            if info.get("editor_action"):
                lines.append(f"    Следующий шаг: {info['editor_action']}")
            lines.append("")
        # Auto-fixed last — informational
        for cause_id, info in sorted(auto_fixed_groups.items(), key=lambda kv: -kv[1]["count"]):
            lines.append(f"  «{info['label']}» — {info['count']} шт. (✅ уже исправлено в коде)")
            lines.append(f"    Что произошло: {info['plain']}")
            for ex in info["examples"][:3]:
                lines.append(f"    • {ex}")
            if len(info["examples"]) > 3:
                lines.append(f"    • …ещё {len(info['examples']) - 3}")
            lines.append("    Что от тебя нужно: ничего, со следующего выпуска не повторится.")
            lines.append("")

    if suspicious_published:
        lines.append("🚫 ЧТО ЗРЯ ПРОШЛО В ВЫПУСК")
        lines.append("Видимые пункты, которые выглядят устаревшими или неуместными для daily:")
        for item in suspicious_published[:8]:
            title = str(item.get("title") or "").strip() or "(без заголовка)"
            source = str(item.get("source_label") or "").strip()
            reasons = "; ".join(str(r) for r in (item.get("reasons") or []))
            tail = f" — {source}" if source else ""
            lines.append(f"  • {title[:90]}{tail}")
            lines.append(f"    Почему: {reasons[:160]}")
        if len(suspicious_published) > 8:
            lines.append(f"  …и ещё {len(suspicious_published) - 8}.")
        lines.append("")

    if borderline_queue.get("items"):
        lines.append("🟨 СПОРНЫЕ МАТЕРИАЛЫ — НЕ ОПУБЛИКОВАНЫ")
        lines.append("Система удержала их, потому что фактов недостаточно для уверенной публикации:")
        for item in (borderline_queue.get("items") or [])[:8]:
            title = str(item.get("title") or "").strip() or "(без заголовка)"
            warnings_txt = "; ".join(_humanize_quality_warning(str(w)) for w in (item.get("quality_warnings") or []))
            lines.append(f"  • {title[:90]}")
            if warnings_txt:
                lines.append(f"    Почему: {warnings_txt[:180]}")
            lines.append(f"    Вердикт: {_borderline_verdict(item)}.")
        if len(borderline_queue.get("items") or []) > 8:
            lines.append(f"  …и ещё {len(borderline_queue.get('items') or []) - 8}.")
        lines.append("  Технические ID скрыты из Telegram; для ручного включения они сохранены в JSON-отчёте.")
        lines.append("")

    if post_publish_judge.get("eval") or judge_signals or post_publish_judge.get("drift", {}).get("status") in {"warming_up", "ok"}:
        judge_eval = post_publish_judge.get("eval") or {}
        judge_drift = post_publish_judge.get("drift") or {}
        judge_status = str(post_publish_judge.get("status") or "")
        baseline_days = int(judge_drift.get("baseline_days") or 0)
        lines.append("🧪 ОЦЕНКА КАЧЕСТВА (POST-PUBLISH JUDGE)")
        if judge_status == "ok" and judge_eval:
            lines.append(
                "  • Сегодняшние оценки (по шкале 0–5): "
                f"факт {judge_eval.get('factuality', '?')}, "
                f"новизна {judge_eval.get('novelty', '?')}, "
                f"разнообразие источников {judge_eval.get('source_diversity', '?')}, "
                f"плотность сигнала {judge_eval.get('signal_density', '?')}, "
                f"связность {judge_eval.get('coherence', '?')}."
            )
            judge_notes = str(judge_eval.get("notes") or "").strip()
            if judge_notes:
                lines.append(f"  • Что заметил судья: {judge_notes}")
        elif judge_status:
            lines.append(
                f"  • Сегодня судью не запустить: {post_publish_judge.get('reason') or judge_status}."
            )
        if baseline_days < 14:
            lines.append(
                f"  • Базовая линия ещё накапливается: {baseline_days} дней из 14 "
                f"минимально нужных. Дрейф пока не считается."
            )
        if judge_signals:
            lines.append(
                "  • Просадки vs 30-дневной нормы (минимум 1σ ниже среднего):"
            )
            axis_label = {
                "factuality": "факт",
                "novelty": "новизна",
                "source_diversity": "разнообразие источников",
                "signal_density": "плотность сигнала",
                "coherence": "связность",
            }
            for sig in judge_signals:
                axis = str(sig.get("axis") or "")
                pretty = axis_label.get(axis, axis)
                lines.append(
                    f"    – {pretty}: сегодня {sig.get('today')}, "
                    f"30-дневная норма {sig.get('baseline_mean')} "
                    f"(σ={sig.get('baseline_sigma')}, "
                    f"−{sig.get('sigmas_below_baseline')}σ)."
                )
            lines.append(
                "  Если просадка реальная — посмотри какие карточки попали "
                "сегодня и не сделал ли rewrite шаг назад."
            )
        lines.append("")

    lead_issues = news_lead_quality.get("issues") or []
    if lead_issues:
        lines.append("📰 НОВОСТИ БЕЗ ЛИДА-ФАКТА")
        quote_n = int(lead_counts.get("quote_lead") or 0)
        narr_n = int(lead_counts.get("narrative_lead") or 0)
        checked_n = int(lead_counts.get("checked") or 0)
        lines.append(
            f"Из {checked_n} опубликованных новостей: начинаются с цитаты — {quote_n}, "
            f"с описания жительницы/жителя — {narr_n}. Читатель не понимает что произошло "
            f"из первой фразы."
        )
        for row in lead_issues[:6]:
            title = str(row.get("title") or "").strip()
            issue = str(row.get("issue") or "")
            detail = str(row.get("detail") or "")
            label = (
                "цитата вместо факта" if issue == "quote_lead"
                else "narrative-лид вместо факта"
            )
            lines.append(f"  • {title[:90]}")
            lines.append(f"    Проблема: {label}. {detail}")
        if len(lead_issues) > 6:
            lines.append(f"  …и ещё {len(lead_issues) - 6} новостей.")
        lines.append("")

    ec_issues = event_completeness.get("issues") or []
    if ec_issues:
        lines.append("📅 СОБЫТИЯ БЕЗ ДАТЫ ИЛИ МЕСТА")
        missing_date_n = int(ec_counts.get("missing_date") or 0)
        missing_venue_n = int(ec_counts.get("missing_venue") or 0)
        checked_n = int(ec_counts.get("checked") or 0)
        lines.append(
            f"Из {checked_n} опубликованных событий: без даты — {missing_date_n}, "
            f"без места — {missing_venue_n}. Это значит rewrite потерял базовую "
            f"информацию, читателю непонятно когда/где."
        )
        for row in ec_issues[:6]:
            title = str(row.get("title") or "").strip()
            issue = str(row.get("issue") or "")
            detail = str(row.get("detail") or "")
            lines.append(f"  • {title[:90]}")
            label = "нет даты в карточке" if issue == "missing_date" else "нет места в карточке"
            lines.append(f"    Проблема: {label}. {detail}")
        if len(ec_issues) > 6:
            lines.append(f"  …и ещё {len(ec_issues) - 6} событий с похожей проблемой.")
        lines.append("")

    cross_day_blocked_list = cross_day_recurrence.get("blocked") or []
    if cross_day_blocked_list:
        lines.append("🔁 ПОВТОРЫ ПО ФИГУРАНТАМ")
        lines.append(
            f"Снято материалов потому что та же фигурант(а) уже была в выпуске: "
            f"{len(cross_day_blocked_list)}."
        )
        for row in cross_day_blocked_list[:6]:
            today_name = str(row.get("matched_person_today") or "").strip()
            prev_day = str(row.get("previous_published_day") or "").strip()
            prev_title = str(row.get("previous_title") or "").strip()
            source = str(row.get("source_label") or "").strip()
            today_title = str(row.get("title") or "").strip()
            source_tail = f" ({source})" if source else ""
            who = today_name or "тот же сюжет"
            when = f"уже был {prev_day}" if prev_day else "уже публиковался"
            if prev_title:
                lines.append(f"  • «{today_title[:90]}»{source_tail}")
                lines.append(f"    {who}: {when} как «{prev_title[:90]}».")
            else:
                lines.append(f"  • «{today_title[:90]}»{source_tail} — {who}, {when}.")
        if len(cross_day_blocked_list) > 6:
            lines.append(f"  …и ещё {len(cross_day_blocked_list) - 6} материал(ов).")
        lines.append(
            "Если по факту это была НОВАЯ информация (имя обвиняемого, "
            "дата суда, цифра ущерба) — значит детектор недо-увидел "
            "новый факт; напиши пример, добавим в тест."
        )
        lines.append("")

    if quality_scorecard.get("today"):
        today_q = quality_scorecard.get("today") or {}
        lines.append("📊 КОНТРОЛЬ КАЧЕСТВА")
        lines.append(
            f"  • Система просмотрела {today_q.get('full_count', 0)} материалов; "
            f"в выпуск попали {today_q.get('visible_count', 0)}."
        )
        lines.append(
            f"  • После финальной самопроверки: устаревших/неуместных — {today_q.get('suspicious_published', 0)}, "
            f"непонятных — {today_q.get('unclear_visible', 0)}, повторов — {today_q.get('repeat_visible', 0)}."
        )
        top_sources = today_q.get("top_sources") or []
        if top_sources:
            compact = ", ".join(
                f"{row.get('source_label')} {int(float(row.get('share') or 0) * 100)}%"
                for row in top_sources[:3]
            )
            lines.append(f"  • Топ источников: {compact}.")
        if feedback_capture:
            lines.append(
                f"  • Ручная оценка пользы: {feedback_capture.get('labelled_items', 0)} пунктов оценено; "
                f"{feedback_capture.get('pending_items', 0)} можно оценить позже для будущей персонализации."
            )
        lines.append("")

    if lost_leads:
        lines.append("⚠️ ГЛАВНЫЕ НОВОСТИ ДНЯ, КОТОРЫЕ ВЫПАЛИ")
        lines.append("Эти статьи были выбраны как главные, но не попали в выпуск:")
        for ll in lost_leads[:5]:
            title = str(ll.get("title") or "").strip() or "(без заголовка)"
            reasons = "; ".join(str(r) for r in (ll.get("reasons") or []))
            lines.append(f"  • {title[:90]}")
            lines.append(f"    Почему: {_humanize_writer_reason(reasons)}")
        if len(lost_leads) > 5:
            lines.append(f"  …и ещё {len(lost_leads) - 5}.")
        lines.append("")

    if section_underflow:
        lines.append("📉 ТОНКИЕ СЕКЦИИ")
        lines.append("В этих разделах вышло меньше пунктов, чем нужно:")
        lines.append("")
        for su in section_underflow:
            name = su.get("section")
            actual = su.get("actual", 0)
            minimum = su.get("minimum", 0)
            dropped = su.get("dropped_by_writer", 0)
            lines.append(f"  «{_section_name_human(str(name))}» — {actual} из минимума {minimum}")
            if dropped:
                word = "карточку" if dropped == 1 else ("карточки" if 2 <= dropped <= 4 else "карточек")
                lines.append(f"    Выбросило {dropped} {word}:")
                for title, reason in _section_drops(name)[:3]:
                    lines.append(f"    • {title}")
                    lines.append(f"      Почему: {reason}")
            else:
                lines.append("    Источники по этой теме принесли мало материала.")
            lines.append("")

    if borough_skew_flags:
        lines.append("🏙️ ПОКРЫТИЕ GM")
        lines.append("Распределение по районам Greater Manchester сегодня неровное:")
        for flag in borough_skew_flags[:4]:
            lines.append(f"  • {_humanize_borough_flag(flag)}")
        visible_rows = [
            row for row in (borough_coverage.get("boroughs") or [])
            if isinstance(row, dict) and int(row.get("rendered_count") or 0) > 0
        ]
        if visible_rows:
            visible_rows.sort(key=lambda row: (-int(row.get("rendered_count") or 0), str(row.get("borough") or "")))
            layout = ", ".join(
                f"{row.get('borough')}: {int(row.get('rendered_count') or 0)}"
                for row in visible_rows[:6]
            )
            lines.append(f"    Раскладка опубликованных пунктов: {layout}.")
        lines.append("")

    transport_verdict = str(transport_coverage.get("verdict") or "")
    if transport_verdict in {"disruptions_rendered", "found_not_rendered", "checked_no_disruptions", "partially_checked", "not_checked"}:
        lines.append("🚋 ТРАНСПОРТ: ЧТО ПРОВЕРЕНО")
        if transport_verdict == "disruptions_rendered":
            lines.append(
                f"  • Найдено ограничений/сбоев: {transport_coverage.get('disruptions_found', 0)}, "
                f"опубликовано: {transport_coverage.get('disruptions_rendered', 0)}."
            )
        elif transport_verdict == "checked_no_disruptions":
            lines.append("  • Транспортные источники проверены; серьёзных ограничений или сбоев не найдено.")
        elif transport_verdict == "found_not_rendered":
            lines.append(
                f"  • Найдено {transport_coverage.get('disruptions_found', 0)} транспортных ограничений, "
                "но в выпуск ничего не попало — нужно проверить финальные правила публикации."
            )
        elif transport_verdict == "partially_checked":
            lines.append("  • Транспорт проверен частично: часть источников недоступна.")
        else:
            lines.append("  • Транспортные источники не были проверены — нельзя честно писать, что сбоев нет.")
        flags = []
        if transport_coverage.get("tfgm_checked"):
            flags.append("TfGM")
        if transport_coverage.get("metrolink_checked"):
            flags.append("Metrolink")
        if transport_coverage.get("national_rail_checked"):
            flags.append("National Rail")
        if flags:
            lines.append(f"    Источники: {', '.join(flags)}.")
        missing_flags = []
        if not transport_coverage.get("tfgm_checked"):
            missing_flags.append("TfGM")
        if not transport_coverage.get("metrolink_checked"):
            missing_flags.append("Metrolink")
        if not transport_coverage.get("national_rail_checked"):
            missing_flags.append("National Rail")
        if missing_flags:
            lines.append(f"    Не проверено явно: {', '.join(missing_flags)}.")
        lines.append("")

    if synthetic_freshness:
        lines.append("🧯 СВЕЖЕСТЬ СЛУЖЕБНЫХ КАРТОЧЕК")
        stale = int(synthetic_freshness.get("stale_count") or 0)
        total_synth = int(synthetic_freshness.get("total") or 0)
        lines.append(f"  • Служебных карточек: {total_synth}; устаревших: {stale}.")
        for item in (synthetic_freshness.get("items") or [])[:5]:
            label = item.get("source_label") or "unknown"
            fetched_at = item.get("data_fetched_at") or "нет времени обновления"
            state = "устарело" if item.get("synthetic_stale") else "свежее"
            lines.append(f"  • {label}: {state}, данные от {fetched_at}.")
        lines.append("")

    semantic = summary or {}
    if semantic.get("semantic_dedup_enabled") is not None:
        lines.append("🧬 ПОХОЖИЕ НОВОСТИ И ДУБЛИ")
        lines.append(
            f"  • Включён: {'да' if semantic.get('semantic_dedup_enabled') else 'нет'}; "
            f"внутри дня снято {semantic.get('semantic_intra_drops', 0)}, "
            f"между днями снято {semantic.get('semantic_cross_day_drops', 0)}, "
            f"спорных пар {semantic.get('semantic_borderline', 0)}, "
            f"возвращено защитным правилом {semantic.get('semantic_restored_by_guard', 0)}."
        )
        lines.append("")

    if cost_summary or prompt_drift or any(str(w).lower().startswith("llm rewrite was degraded") for w in (report.get("warnings") or [])):
        lines.append("🤖 ГЕНЕРАЦИЯ, ПРОМПТЫ И СТОИМОСТЬ")
        if any(str(w).lower().startswith("llm rewrite was degraded") for w in (report.get("warnings") or [])):
            lines.append("  • Генерация текста: аварийный режим — часть пунктов добрана запасной логикой.")
        if cost_summary:
            lines.append(
                f"  • Стоимость запуска: ${float(cost_summary.get('total_cost_usd') or 0):.4f}; "
                f"вызовов моделей: {cost_summary.get('total_calls', 0)}."
            )
            unknown_models = cost_summary.get("unknown_priced_models") or []
            if unknown_models:
                lines.append(f"  • Без цены в cost monitor: {', '.join(str(m) for m in unknown_models)}.")
        if prompt_drift:
            lines.append(f"  • Изменения промптов: {len(prompt_drift)} промпт(ов) поменялись без явного обновления версии.")
        else:
            lines.append("  • Изменения промптов: не обнаружены.")
        lines.append("")

    if diaspora_diagnostics.get("verdict") in {"checked_empty", "fetched_but_filtered", "accepted_not_rendered"}:
        lines.append("🎭 РУССКОЯЗЫЧНЫЙ БЛОК")
        verdict = str(diaspora_diagnostics.get("verdict") or "")
        lines.append(f"  • Итог: {_diaspora_verdict_human(verdict)}.")
        lines.append(
            f"  • Нашли {diaspora_diagnostics.get('raw_count', 0)} материалов, "
            f"прошло отбор {diaspora_diagnostics.get('accepted_count', 0)}, "
            f"опубликовано {diaspora_diagnostics.get('rendered_count', 0)}."
        )
        for row in (diaspora_diagnostics.get("sources") or [])[:5]:
            name = str(row.get("name") or "")
            detail = str(row.get("detail") or row.get("status") or "")
            reasons = row.get("reject_reasons") or {}
            lines.append(f"  • {_source_name_human(name)}: {_source_counts_phrase(row)}.")
            if reasons:
                top_reason = sorted(reasons.items(), key=lambda kv: -int(kv[1]))[0]
                lines.append(f"    Почему не вошло: {_humanize_source_reason(top_reason[0])} ({top_reason[1]}).")
            elif detail:
                lines.append(f"    Статус: {_explain_source_failure(detail) if 'no candidate links' in detail.lower() else detail[:160]}.")
        lines.append("")

    if source_status.get("sources"):
        counts = source_status.get("counts") or {}
        lines.append("📡 ЗДОРОВЬЕ ИСТОЧНИКОВ")
        lines.append(
            f"  • Работают: {counts.get('ok', 0)}, не ответили: {counts.get('failed', 0)}, "
            f"пустые: {counts.get('empty', 0)}, без новых материалов: {counts.get('stale', 0)}, "
            f"без вклада в выпуск: {counts.get('zero_yield', 0)}."
        )
        for status, label in (("failed", "Не ответили"), ("empty", "Пустые"), ("stale", "Без новых материалов")):
            status_rows = [
                row for row in source_status.get("sources") or []
                if isinstance(row, dict) and row.get("status") == status
            ]
            if status_rows:
                names = ", ".join(_source_name_human(str(row.get("name") or "")) for row in status_rows[:4])
                suffix = f" и ещё {len(status_rows) - 4}" if len(status_rows) > 4 else ""
                lines.append(f"  • {label}: {names}{suffix}.")
        lines.append("")
        rows = [
            row for row in source_status.get("sources") or []
            if isinstance(row, dict)
            and int(row.get("raw_count") or row.get("candidate_count") or 0) > 0
            and int(row.get("rendered_count") or 0) == 0
        ]
        if rows:
            lines.append("🧪 ИСТОЧНИКИ БЕЗ ВКЛАДА В ВЫПУСК")
            lines.append("Эти источники дали материалы, но в финальный выпуск ничего не попало:")
            for row in rows[:8]:
                human = row.get("human_funnel") if isinstance(row.get("human_funnel"), dict) else {}
                if human:
                    lines.append(f"  • {_source_name_human(str(row.get('name') or ''))}:")
                    for part in (human.get("template") or [])[:7]:
                        lines.append(f"    {part}.")
                    lines.append(f"    Вывод: {human.get('conclusion')}.")
                    lines.append(f"    Что делать: {human.get('action')}.")
                    continue
                reasons = row.get("reject_reasons") or {}
                reason_txt = ""
                if reasons:
                    reason, count = sorted(reasons.items(), key=lambda kv: -int(kv[1]))[0]
                    reason_txt = f"; причина: {_humanize_source_reason(reason)} ({count})"
                lines.append(f"  • {_source_name_human(str(row.get('name') or ''))}: {_source_counts_phrase(row)}{reason_txt}.")
            if len(rows) > 8:
                lines.append(f"  …и ещё {len(rows) - 8}.")
            lines.append("  Полная таблица сохранена в техническом JSON-отчёте.")
            lines.append("")

    # ── Источники: что не работало ────────────────────────────────
    if failed_sources:
        lines.append("🔌 ИСТОЧНИКИ КОТОРЫЕ НЕ ОТВЕТИЛИ")
        any_fixed = False
        today_iso = report.get("run_date_london") or ""
        chronic_count = 0
        for s in failed_sources[:10]:
            name = s.get("name") or ""
            detail = str(s.get("detail") or "")
            plain = _explain_source_failure(detail)
            streak_tag = _source_streak_tag(name, today_iso) if today_iso else ""
            tail = f" ({streak_tag})" if streak_tag else ""
            if streak_tag and "пора отключить" in streak_tag:
                chronic_count += 1
            lines.append(f"  • {_source_name_human(str(name))} — {plain}{tail}")
        if len(failed_sources) > 10:
            lines.append(f"  …и ещё {len(failed_sources) - 10}.")
        if chronic_count:
            lines.append(f"    {chronic_count} источник(ов) падают неделю+ — стоит отключить и найти замену.")
        else:
            lines.append("    Если повторяется 3+ дня — стоит проверить, не закрыли ли сайт совсем.")
        lines.append("")

    # ── Качество выпуска (если что-то не в норме) ──────────────────
    if health_level != "healthy" and health_signals:
        level_human = {"at_risk": "🟡 под риском", "unhealthy": "🔴 слабый выпуск"}.get(health_level, health_level)
        lines.append(f"⚖️ ОЦЕНКА КАЧЕСТВА: {level_human}")
        for sig in health_signals:
            lines.append(f"  • {_translate_health_signal(sig)}")
        lines.append("")

    # ── Если кроме «всё в норме» сказать нечего — короткий happy summary
    if not (suspicious_groups or lost_leads or section_underflow
            or failed_sources or borough_skew_flags
            or report.get("warnings")
            or prompt_drift
            or int((synthetic_freshness or {}).get("stale_count") or 0) > 0
            or bool((cost_summary or {}).get("unknown_priced_models") or [])
            or transport_coverage.get("verdict") in {"found_not_rendered", "not_checked", "partially_checked"}
            or diaspora_diagnostics.get("verdict") in {"checked_empty", "fetched_but_filtered", "accepted_not_rendered"}
            or borderline_queue.get("items")
            or (health_level != "healthy" and health_signals)):
        lines.append("Всё прошло чисто — никаких проблем не обнаружено.")

    text = "\n".join(lines).rstrip()

    settings, client, store = _load_store_and_client()
    targets = _effective_targets(settings.telegram_target, store.list_subscribers())
    if not targets:
        print("No Telegram targets configured. Skipping admin warnings.")
        return 0
    for target in targets:
        client.send_text_in_chunks(target, text, parse_mode=None)
    print(f"Sent warnings to {len(targets)} target(s).")
    return 0


def cmd_send_weekly_cost() -> int:
    """Send a 7-day cost summary to Telegram from data/state/cost_history.json.
    Intended to be called on Sundays in CI."""
    history_path = PROJECT_ROOT / "data" / "state" / "cost_history.json"
    if not history_path.exists():
        print(f"No cost_history.json at {history_path}. Nothing to summarise.")
        return 0
    try:
        history = json.loads(history_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"Could not read cost_history.json: {exc}. Skipping.")
        return 0
    if not isinstance(history, list) or not history:
        print("cost_history.json empty. Nothing to summarise.")
        return 0

    last7 = history[-7:]
    total = sum(float(e.get("total_cost_usd") or 0.0) for e in last7)
    by_provider: dict[str, float] = {}
    by_stage: dict[str, float] = {}
    for e in last7:
        for p, info in (e.get("by_provider") or {}).items():
            by_provider[p] = by_provider.get(p, 0.0) + float(info.get("cost_usd") or 0.0)
        for s, info in (e.get("by_stage") or {}).items():
            by_stage[s] = by_stage.get(s, 0.0) + float(info.get("cost_usd") or 0.0)
    days = len(last7)
    avg = total / days if days else 0.0

    lines = [f"💰 Weekly cost — last {days} day(s)", f"Total: ${total:.4f}  •  Avg/day: ${avg:.4f}"]
    if by_stage:
        lines.append("By stage:")
        for stage, cost in sorted(by_stage.items(), key=lambda x: -x[1]):
            lines.append(f"• {stage}: ${cost:.4f}")
    if by_provider:
        lines.append("By provider:")
        for prov, cost in sorted(by_provider.items(), key=lambda x: -x[1]):
            lines.append(f"• {prov}: ${cost:.4f}")
    lines.append("\nDay-by-day:")
    for e in last7:
        lines.append(f"• {e.get('run_date_london')}: ${float(e.get('total_cost_usd') or 0.0):.4f}")
    text = "\n".join(lines)

    settings, client, store = _load_store_and_client()
    targets = _effective_targets(settings.telegram_target, store.list_subscribers())
    if not targets:
        print("No Telegram targets. Skipping weekly cost summary.")
        return 0
    for target in targets:
        client.send_text_in_chunks(target, text, parse_mode=None)
    print(f"Sent weekly cost summary to {len(targets)} target(s).")
    return 0


def cmd_weekly_city_rollup() -> int:
    state_dir = PROJECT_ROOT / "data" / "state"
    rollup = build_weekly_city_rollup(state_dir)
    print(json.dumps(rollup, ensure_ascii=False, indent=2))
    return 0 if _weekly_city_rollup_errors_are_non_blocking(rollup) else 1


def cmd_send_weekly_city_rollup() -> int:
    state_dir = PROJECT_ROOT / "data" / "state"
    rollup = build_weekly_city_rollup(state_dir)
    text = weekly_city_rollup_text(rollup)

    settings, client, store = _load_store_and_client()
    targets = _effective_targets(settings.telegram_target, store.list_subscribers())
    if not targets:
        print("No Telegram targets. Skipping weekly city rollup.")
        return 0
    for target in targets:
        client.send_text_in_chunks(target, text, parse_mode=None)
    print(f"Sent weekly city rollup to {len(targets)} target(s).")
    return 0 if _weekly_city_rollup_errors_are_non_blocking(rollup) else 1


def _weekly_city_rollup_errors_are_non_blocking(rollup: dict) -> bool:
    errors = [str(err) for err in rollup.get("errors") or []]
    return not errors or errors == ["city_intelligence_history.json is empty"]


def cmd_post_publish_judge() -> int:
    """S6: score today's published digest, append to digest_evals.jsonl,
    print the drift report. Exits 0 even on judge failure — the gate
    never blocks for the judge's sake. The drift output is also written
    into release_report.json so cmd_send_warnings can surface it.
    """
    from news_digest.pipeline.post_publish_judge import evaluate_today  # noqa: PLC0415

    pipeline_run_id = ""
    release_report_path = PROJECT_ROOT / "data" / "state" / "release_report.json"
    if release_report_path.exists():
        try:
            existing_report = json.loads(release_report_path.read_text(encoding="utf-8"))
            pipeline_run_id = str(existing_report.get("pipeline_run_id") or "")
        except (OSError, json.JSONDecodeError):
            existing_report = {}
    else:
        existing_report = {}

    result = evaluate_today(PROJECT_ROOT, pipeline_run_id=pipeline_run_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))

    # Stamp drift into release_report.json so send-warnings can read it
    # without re-running the judge. Idempotent: only adds fields, doesn't
    # touch anything else.
    if isinstance(existing_report, dict):
        existing_report["post_publish_judge"] = result
        try:
            release_report_path.write_text(
                json.dumps(existing_report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            print(f"Warning: could not write post_publish_judge into release_report.json: {exc}")
    return 0


def cmd_delivered_today() -> int:
    settings, _, store = _load_store_and_client()
    payload = _delivered_today_payload(settings) or store.get_last_delivery()
    today_london = datetime.now(LONDON_TZ).strftime("%Y-%m-%d")
    delivered_day = payload.get("last_delivery_day_london")
    delivered = delivered_day == today_london
    print(
        json.dumps(
            {
                "today_london": today_london,
                "last_delivery_day_london": delivered_day,
                "delivered_today": delivered,
                "source_path": payload.get("source_path"),
                "targets": payload.get("targets", []),
                "delivery_state_path": payload.get("delivery_state_path") or str(settings.state_dir / "delivery_state.json"),
                "project_root": str(settings.project_root),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if delivered else 1


def cmd_digest_status() -> int:
    """Single-view diagnostic: did today's digest actually ship?

    Reads delivery_state, release_report (latest run, could be fail) and
    last_passed_release_report (most recent successful gate) and prints
    a coherent picture so a stale fail in release_report.json doesn't
    look like 'today failed' when delivery actually happened.
    """

    state_dir = PROJECT_ROOT / "data" / "state"
    today_london = datetime.now(LONDON_TZ).strftime("%Y-%m-%d")

    def _load(path):
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    delivery = _load(state_dir / "delivery_state.json") or {}
    release_latest = _load(state_dir / "release_report.json") or {}
    release_passed = _load(state_dir / "last_passed_release_report.json") or {}
    published_facts = _load(state_dir / "published_facts.json") or {}
    bot_state = _load(state_dir / "bot_state.json") or {}

    delivered_today = delivery.get("last_delivery_day_london") == today_london
    last_pass_today = release_passed.get("run_date_london") == today_london

    print(json.dumps({
        "today_london": today_london,
        "delivered_today": delivered_today,
        "last_delivery_at": delivery.get("last_delivery_at"),
        "last_delivery_targets": delivery.get("targets", []),
        "last_passed_gate_today": last_pass_today,
        "last_passed_gate_at": release_passed.get("run_at_london"),
        "latest_gate_decision": release_latest.get("release_decision"),
        "latest_gate_at": release_latest.get("run_at_london"),
        "latest_gate_errors_count": len(release_latest.get("errors", [])),
        "published_facts_count": len(published_facts.get("facts", [])),
        "published_facts_last_updated": published_facts.get("last_updated_london"),
        "subscribers_count": len(bot_state.get("subscribers", [])),
    }, ensure_ascii=False, indent=2))
    return 0 if delivered_today else 1


def cmd_build_digest() -> int:
    result = build_release(PROJECT_ROOT)
    # Also surface gate errors so CI logs show exactly what blocked release
    from news_digest.pipeline.common import read_json  # noqa: PLC0415
    report = read_json(result.report_path, {})
    payload = {
        "ok": result.ok,
        "message": result.message,
        "errors": report.get("errors", []),
        "report_path": str(result.report_path),
        "output_path": str(result.output_path),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


def cmd_mark_pipeline_failed(stage: str) -> int:
    state_dir = PROJECT_ROOT / "data" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    now_london = datetime.now(LONDON_TZ)
    report_path = state_dir / "release_report.json"
    candidates_report = read_json(state_dir / "candidates.json", {})
    report_payload = {
        "release_gate_version": REQUIRED_RELEASE_GATE_VERSION,
        "pipeline_run_id": str(candidates_report.get("pipeline_run_id") or ""),
        "run_at_london": now_london.isoformat(),
        "run_date_london": now_london.strftime("%Y-%m-%d"),
        "release_decision": "fail",
        "message": "Digest pipeline stopped before release gate.",
        "errors": [f"Pipeline stage failed before build-digest: {stage}."],
        "failed_stage": stage,
        "inputs": {
            "collector_report": str((state_dir / "collector_report.json").resolve()),
            "candidates": str((state_dir / "candidates.json").resolve()),
            "curator_report": str((state_dir / "curator_report.json").resolve()),
            "writer_report": str((state_dir / "writer_report.json").resolve()),
            "editor_report": str((state_dir / "editor_report.json").resolve()),
            "draft_digest": str((state_dir / "draft_digest.html").resolve()),
        },
        "output_path": str((PROJECT_ROOT / "data" / "outgoing" / "current_digest.html").resolve()),
    }
    write_json(report_path, report_payload)
    print(json.dumps({"ok": True, "report_path": str(report_path), "failed_stage": stage}, ensure_ascii=False, indent=2))
    return 0


def cmd_init_build_state(overwrite: bool) -> int:
    paths = initialize_release_inputs(PROJECT_ROOT, overwrite=overwrite)
    initialize_collector_state(PROJECT_ROOT, overwrite=overwrite)
    initialize_candidates_state(PROJECT_ROOT, overwrite=overwrite)
    ensure_history_files(PROJECT_ROOT / "data" / "state")
    print(
        json.dumps(
            {
                "initialized": {name: str(path) for name, path in paths.items()},
                "overwrite": overwrite,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _stage_payload(result) -> dict[str, object]:
    payload = {"ok": result.ok, "message": result.message, "report_path": str(result.report_path)}
    if hasattr(result, "draft_path"):
        payload["draft_path"] = str(result.draft_path)
    return payload


def cmd_collect_digest() -> int:
    report_path = PROJECT_ROOT / "data" / "state" / "collector_report.json"
    candidates_path = PROJECT_ROOT / "data" / "state" / "candidates.json"
    force_collect = str(os.environ.get("FORCE_COLLECT") or "").strip().lower() in {"1", "true", "yes"}
    if not force_collect and report_path.exists() and candidates_path.exists():
        report = read_json(report_path, {})
        run_at_str = report.get("run_at_london", "")
        if run_at_str:
            try:
                from datetime import timezone
                run_at = datetime.fromisoformat(run_at_str)
                age_hours = (datetime.now(run_at.tzinfo or timezone.utc) - run_at).total_seconds() / 3600
                if age_hours < 12:
                    candidates = read_json(candidates_path, {})
                    print(json.dumps({
                        "skipped": True,
                        "reason": f"Collect already ran {age_hours:.1f}h ago — reusing existing candidates.",
                        "run_at_london": run_at_str,
                        "candidates_path": str(candidates_path),
                        "candidate_count": len(candidates.get("candidates", [])),
                    }, ensure_ascii=False, indent=2))
                    return 0
            except Exception:
                pass
    result = collect_digest(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


def cmd_dedupe_digest() -> int:
    result = dedupe_candidates(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


def cmd_validate_candidates() -> int:
    result = validate_candidates(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


def cmd_curator_pass() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from news_digest.pipeline.curator import run_curator_pass  # noqa: PLC0415
    run_curator_pass(PROJECT_ROOT)
    print(json.dumps({"ok": True, "message": "Curator pass complete."}, ensure_ascii=False))
    return 0


def cmd_transport_fill() -> int:
    """Deterministic transport-card rendering. Runs between curator-pass
    and llm-rewrite so LLM only sees the few odd transport alerts that
    don't fit the standard TfGM / Metrolink / National Rail templates.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from news_digest.pipeline.transport_fill import run_transport_fill  # noqa: PLC0415
    result = run_transport_fill(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False))
    return 0 if result.ok else 1


def cmd_llm_rewrite() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = run_llm_rewrite(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False))
    return 0 if result.ok else 1


def cmd_prompt_versions() -> int:
    from news_digest.pipeline.prompts_meta import (  # noqa: PLC0415
        PROMPT_REGISTRY_VERSION,
        snapshot,
        validate_registry,
    )

    errors = validate_registry()
    print(
        json.dumps(
            {
                "prompt_registry_version": PROMPT_REGISTRY_VERSION,
                "prompt_versions": snapshot(),
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if not errors else 1


def cmd_model_routing() -> int:
    from news_digest.pipeline.model_routing import route_snapshot  # noqa: PLC0415

    print(json.dumps(route_snapshot(), ensure_ascii=False, indent=2))
    return 0


def cmd_pipeline_config() -> int:
    from news_digest.pipeline.model_routing import route_snapshot  # noqa: PLC0415
    from news_digest.pipeline.prompts_meta import (  # noqa: PLC0415
        PROMPT_REGISTRY_VERSION,
        snapshot,
        validate_registry,
    )

    prompt_errors = validate_registry()
    print(
        json.dumps(
            {
                "pipeline": [
                    "collect-digest",
                    "dedupe-digest",
                    "validate-candidates",
                    "curator-pass",
                    "transport-fill",
                    "llm-rewrite",
                    "write-digest",
                    "edit-digest",
                    "build-digest",
                ],
                "required_release_gate_version": REQUIRED_RELEASE_GATE_VERSION,
                "prompt_registry_version": PROMPT_REGISTRY_VERSION,
                "prompt_versions": snapshot(),
                "prompt_errors": prompt_errors,
                "model_routing": route_snapshot(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if not prompt_errors else 1


def cmd_cost_summary() -> int:
    report = read_json(PROJECT_ROOT / "data" / "state" / "release_report.json", {})
    cost_summary = report.get("cost_summary") or {}
    payload = {
        "run_date_london": report.get("run_date_london"),
        "release_decision": report.get("release_decision"),
        "cost_summary": cost_summary,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if cost_summary else 1


def cmd_reader_value_validation() -> int:
    from news_digest.pipeline.reader_value import (  # noqa: PLC0415
        evaluate_reader_value_labels,
        write_reader_value_validation_report,
    )

    report = evaluate_reader_value_labels(PROJECT_ROOT)
    report_path = write_reader_value_validation_report(PROJECT_ROOT)
    payload = {
        "report_path": str(report_path.resolve()),
        "errors": report.get("errors", []),
        "summary": report.get("summary", {}),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not payload["errors"] else 1


def cmd_model_bakeoff(dry_run: bool, limit: int | None) -> int:
    from news_digest.pipeline.model_bakeoff import run_model_bakeoff  # noqa: PLC0415

    report = run_model_bakeoff(PROJECT_ROOT, dry_run=dry_run, limit=limit)
    payload = {
        "report_path": report.get("report_path"),
        "dry_run": report.get("dry_run"),
        "validation_errors": report.get("validation_errors") or [],
        "validation_set": report.get("validation_set") or {},
        "models": [
            {
                "provider": model.get("provider"),
                "model": model.get("model"),
                "status": model.get("status"),
                "metrics": model.get("metrics") or {},
                "diagnostic": model.get("diagnostic") or {},
            }
            for model in report.get("models") or []
        ],
        "promotion_recommendation": report.get("promotion_recommendation") or {},
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    failed = bool(payload["validation_errors"]) or any(
        str(model.get("status") or "") == "failed" for model in payload["models"]
    )
    return 1 if failed else 0


def cmd_write_digest() -> int:
    os.environ.setdefault("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "1")
    result = write_digest(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


def cmd_edit_digest() -> int:
    result = edit_digest(PROJECT_ROOT)
    payload = _stage_payload(result)
    report = read_json(result.report_path, {})
    payload["errors"] = report.get("errors", [])
    payload["warnings"] = report.get("warnings", [])
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


def cmd_discover_sources(seeds: list[str] | None = None) -> int:
    from news_digest.pipeline.source_discovery import write_discovery_report

    path = write_discovery_report(PROJECT_ROOT, seeds=seeds or None)
    payload = read_json(path, {})
    recommendations = payload.get("recommendations") if isinstance(payload.get("recommendations"), list) else []
    print(f"Source discovery report: {path.relative_to(PROJECT_ROOT)}")
    print(f"Кандидатов: {len(recommendations)}")
    print("Как проверять: открыть example_urls, добавить SourceDef с trial = true, смотреть trial funnel 3-7 дней.")
    for item in recommendations[:12]:
        if not isinstance(item, dict):
            continue
        print("")
        print(str(item.get("recommended_name") or item.get("url") or "Candidate"))
        print(f"URL: {item.get('url') or ''}")
        print(f"Тип: {item.get('report_category_guess') or ''} / {item.get('primary_block_guess') or ''}")
        print(f"Как найдено: {item.get('reason') or item.get('kind') or ''}")
        examples = item.get("example_urls") if isinstance(item.get("example_urls"), list) else []
        if examples:
            print("Примеры:")
            for example in examples[:3]:
                print(f"  - {example}")
        source_def = item.get("recommended_source_def") if isinstance(item.get("recommended_source_def"), dict) else {}
        if source_def:
            print("Trial SourceDef:")
            print(
                "  "
                + ", ".join(
                    f"{key}={source_def.get(key)!r}"
                    for key in ("name", "url", "source_type", "report_category", "primary_block", "trial", "max_candidates")
                    if key in source_def
                )
            )
        checks = item.get("how_to_check") if isinstance(item.get("how_to_check"), list) else []
        if checks:
            print("Проверка:")
            for step in checks[:4]:
                print(f"  - {step}")
    if len(recommendations) > 12:
        print(f"\nЕщё кандидатов: {len(recommendations) - 12}. Полный список в {path.relative_to(PROJECT_ROOT)}")
    return 0


def cmd_repair_dead_parsers() -> int:
    from news_digest.pipeline.dead_parser_repair import write_dead_parser_repair_report

    path = write_dead_parser_repair_report(PROJECT_ROOT)
    payload = read_json(path, {})
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local runner for the Manchester digest MVP.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bot-info", help="Check that the bot token works.")
    subparsers.add_parser(
        "get-updates",
        help="Show raw Telegram updates. Useful for discovering the target chat id after starting the bot.",
    )
    subparsers.add_parser(
        "process-updates",
        help="Process pending Telegram commands like /start, /latest and /subscribe.",
    )
    subparsers.add_parser(
        "delivered-today",
        help="Exit 0 if a digest was already delivered today in Europe/London.",
    )
    subparsers.add_parser(
        "digest-status",
        help=(
            "One-view diagnostic: was today actually delivered, when was "
            "the last successful gate, what does the latest gate say. "
            "Use this when release_report.json shows fail but delivery "
            "happened earlier in the day."
        ),
    )
    subparsers.add_parser(
        "build-digest",
        help="Promote a staged draft digest to outgoing/current_digest.html only if all release gates pass.",
    )
    subparsers.add_parser(
        "collect-digest",
        help="Validate broad scan coverage in collector_report.json.",
    )
    subparsers.add_parser(
        "dedupe-digest",
        help="Apply repeat handling and write dedupe_memory.json.",
    )
    subparsers.add_parser(
        "validate-candidates",
        help="Validate source quality and publishability for candidates.",
    )
    subparsers.add_parser(
        "curator-pass",
        help="Editorial curator: drop PR/evergreen candidates and mark lead story.",
    )
    subparsers.add_parser(
        "transport-fill",
        help="Deterministic transport-card rendering and active Metrolink reminders.",
    )
    subparsers.add_parser(
        "llm-rewrite",
        help="Write Russian draft_lines via quality rewrite route with resilient fallback.",
    )
    subparsers.add_parser(
        "prompt-versions",
        help="Print registered prompt versions and content hashes.",
    )
    subparsers.add_parser(
        "model-routing",
        help="Print default model routing policy for scoring, curation, rewrite, and fallback.",
    )
    subparsers.add_parser(
        "pipeline-config",
        help="Print pipeline stages, release gate version, prompt registry and model routing.",
    )
    subparsers.add_parser(
        "cost-summary",
        help="Print the latest release_report per-run LLM cost summary.",
    )
    subparsers.add_parser(
        "reader-value-validation",
        help="Validate reader-value scoring against the manual historical label set.",
    )
    bakeoff_parser = subparsers.add_parser(
        "model-bakeoff",
        help=(
            "Offline English judge bake-off on manual labels. Not part of "
            "the morning pipeline; compares deterministic stub plus configured "
            "DeepSeek/OpenAI routes when API keys are present."
        ),
    )
    bakeoff_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call model APIs; validate labels and print configured routes.",
    )
    bakeoff_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional number of validation labels to evaluate.",
    )
    subparsers.add_parser(
        "write-digest",
        help="Write staged draft_digest.html from include=true validated candidates.",
    )
    subparsers.add_parser(
        "edit-digest",
        help="Run editor/balancer checks on draft_digest.html.",
    )
    discover_parser = subparsers.add_parser(
        "discover-sources",
        help="Probe seed sites for RSS/sitemap/news/event/consultation source candidates.",
    )
    discover_parser.add_argument(
        "seeds",
        nargs="*",
        help="Optional seed URLs. Defaults to the built-in GM council/transport/event seeds.",
    )
    subparsers.add_parser(
        "repair-dead-parsers",
        help="Probe release_report.dead_parsers and suggest concrete extractor repairs.",
    )
    mark_failed_parser = subparsers.add_parser(
        "mark-pipeline-failed",
        help=argparse.SUPPRESS,
    )
    mark_failed_parser.add_argument("stage", help="Pipeline stage that failed before build-digest.")
    init_build_parser = subparsers.add_parser(
        "init-build-state",
        help="Create or refresh today's collector/candidates/draft template files for the staged digest pipeline.",
    )
    init_build_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing staged files with fresh templates for today.",
    )
    poll_parser = subparsers.add_parser(
        "poll-updates",
        help="Keep polling Telegram for bot commands in a loop.",
    )
    poll_parser.add_argument(
        "--interval-seconds",
        type=int,
        default=15,
        help="How often to poll Telegram for new updates.",
    )
    subparsers.add_parser("render-demo", help="Render the demo digest to stdout.")
    subparsers.add_parser("send-demo", help="Send the demo digest to Telegram.")
    send_file_parser = subparsers.add_parser(
        "send-file",
        help="Send a prepared digest file to Telegram.",
    )
    send_file_parser.add_argument("file_path", help="Path to the text or HTML-formatted file.")
    send_file_parser.add_argument(
        "--parse-mode",
        choices=["HTML"],
        default=None,
        help="Optional Telegram parse mode. Defaults to HTML for .html files.",
    )
    send_file_parser.add_argument(
        "--force",
        action="store_true",
        help="Send even if a digest was already delivered today.",
    )
    subparsers.add_parser(
        "send-warnings",
        help=(
            "Post an admin alert to Telegram if release_report.json flagged "
            "lost leads or section underflow. Opt-out via WARNINGS_TO_TELEGRAM=0."
        ),
    )
    subparsers.add_parser(
        "send-weekly-cost",
        help="Send a 7-day LLM cost summary to Telegram from cost_history.json.",
    )
    subparsers.add_parser(
        "weekly-city-rollup",
        help="Build and print the 7-day city intelligence rollup.",
    )
    subparsers.add_parser(
        "send-weekly-city-rollup",
        help="Build and send the 7-day city intelligence rollup to Telegram.",
    )
    subparsers.add_parser(
        "post-publish-judge",
        help=(
            "Score today's already-published digest on 5 quality axes "
            "(factuality, novelty, source_diversity, signal_density, "
            "coherence) via gpt-4o-mini and append to digest_evals.jsonl. "
            "Computes drift vs 30-day baseline. Never blocks the pipeline."
        ),
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "bot-info":
        return cmd_bot_info()
    if args.command == "get-updates":
        return cmd_get_updates()
    if args.command == "process-updates":
        return cmd_process_updates()
    if args.command == "delivered-today":
        return cmd_delivered_today()
    if args.command == "digest-status":
        return cmd_digest_status()
    if args.command == "build-digest":
        return cmd_build_digest()
    if args.command == "collect-digest":
        return cmd_collect_digest()
    if args.command == "dedupe-digest":
        return cmd_dedupe_digest()
    if args.command == "validate-candidates":
        return cmd_validate_candidates()
    if args.command == "curator-pass":
        return cmd_curator_pass()
    if args.command == "transport-fill":
        return cmd_transport_fill()
    if args.command == "llm-rewrite":
        return cmd_llm_rewrite()
    if args.command == "prompt-versions":
        return cmd_prompt_versions()
    if args.command == "model-routing":
        return cmd_model_routing()
    if args.command == "pipeline-config":
        return cmd_pipeline_config()
    if args.command == "cost-summary":
        return cmd_cost_summary()
    if args.command == "reader-value-validation":
        return cmd_reader_value_validation()
    if args.command == "model-bakeoff":
        return cmd_model_bakeoff(args.dry_run, args.limit)
    if args.command == "write-digest":
        return cmd_write_digest()
    if args.command == "edit-digest":
        return cmd_edit_digest()
    if args.command == "discover-sources":
        return cmd_discover_sources(args.seeds)
    if args.command == "repair-dead-parsers":
        return cmd_repair_dead_parsers()
    if args.command == "mark-pipeline-failed":
        return cmd_mark_pipeline_failed(args.stage)
    if args.command == "init-build-state":
        return cmd_init_build_state(args.overwrite)
    if args.command == "poll-updates":
        return cmd_poll_updates(args.interval_seconds)
    if args.command == "render-demo":
        return cmd_render_demo()
    if args.command == "send-demo":
        return cmd_send_demo()
    if args.command == "send-file":
        return cmd_send_file(args.file_path, args.parse_mode, args.force)
    if args.command == "send-warnings":
        return cmd_send_warnings()
    if args.command == "send-weekly-cost":
        return cmd_send_weekly_cost()
    if args.command == "weekly-city-rollup":
        return cmd_weekly_city_rollup()
    if args.command == "send-weekly-city-rollup":
        return cmd_send_weekly_city_rollup()
    if args.command == "post-publish-judge":
        return cmd_post_publish_judge()

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
