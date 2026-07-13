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

from news_digest.config.settings import load_settings
from news_digest.delivery.telegram import TelegramClient
from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.collector import collect_digest, initialize_collector_state
from news_digest.pipeline.common import SECTION_MAX_ITEMS, SECTION_MIN_ITEMS, read_json, today_london, write_json
from news_digest.pipeline.dedupe import dedupe_candidates, initialize_candidates_state
from news_digest.pipeline.editor import edit_digest
from news_digest.pipeline.history import ensure_history_files, record_delivery_artifacts
from news_digest.pipeline.llm_rewrite import run_llm_rewrite
from news_digest.pipeline.release import build_release, flush_stage_observability, initialize_release_inputs
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
    store = StateStore(settings.state_dir)
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
    if report.get("release_decision") not in {"pass", "ship_degraded"}:
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
    # The send gate blocks ONLY on technical consistency (built+promoted digest,
    # matching date/header, no hard release failure). "ship_degraded" is allowed:
    # it means the issue was promoted and visible quality problems were repaired,
    # replaced or honestly marked instead of silently cancelling delivery.
    return None


def cmd_bot_info() -> int:
    settings = load_settings(PROJECT_ROOT)
    client = TelegramClient(settings.telegram_bot_token)
    result = client.get_me()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _rendered_candidates_for_delivery(sent_path: Path) -> list[dict]:
    """Candidates whose canonical URL is visible in the SENT html.

    Publication history must reflect exactly what the READER saw — nothing
    more, nothing less. The old writer-fingerprint source had both failure
    modes on real issues: lines inserted after the writer (must_show recovery,
    pre-send top-ups) never reached published_facts, so «Скамейка» repeated
    3+ issues with zero history; and writer-rendered lines later stripped by
    the editor/judge were recorded as published although they never aired
    (4 phantom entries on 2026-07-13 alone), teaching repeat policy to block
    their legitimate first showing. The send flow records right after a
    successful send of this same file, so the file always exists here.
    """
    from news_digest.pipeline.common import canonical_url_identity  # noqa: PLC0415

    state_dir = PROJECT_ROOT / "data" / "state"
    sent_idents: set[str] = set()
    if sent_path.exists():
        sent_html = sent_path.read_text(encoding="utf-8")
        sent_idents = {
            canonical_url_identity(url)
            for url in re.findall(r'<a\b[^>]*href=["\']([^"\']+)["\']', sent_html, flags=re.IGNORECASE)
        }
        sent_idents.discard("")
    if not sent_idents:
        print(
            "Warning: sent digest has no source links; published_facts.json was not updated.",
            file=sys.stderr,
        )
        return []

    candidates_payload = read_json(state_dir / "candidates.json", {"candidates": []})
    rendered_candidates = [
        candidate
        for candidate in candidates_payload.get("candidates", [])
        if isinstance(candidate, dict)
        and str(candidate.get("source_url") or "").strip()
        and canonical_url_identity(str(candidate["source_url"])) in sent_idents
    ]
    if not rendered_candidates:
        print(
            "Warning: no candidates matched the sent digest links; "
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
        StateStore(runtime_state_dir).mark_delivery(
            targets, str(resolved_path), message_ids=message_ids
        )
    record_delivery_artifacts(PROJECT_ROOT, resolved_path, _rendered_candidates_for_delivery(resolved_path))
    if resolved_path.name == "current_digest.html":
        # Quality panel: 5 editorial indicators per SENT issue, one row/day.
        from news_digest.pipeline.quality_panel import (  # noqa: PLC0415
            append_panel_row,
            build_panel_row,
            panel_row_line,
        )

        row = build_panel_row(text, today_london())
        append_panel_row(PROJECT_ROOT / "data" / "state", row)
        print(f"Quality panel: {panel_row_line(row)}")
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


def _source_name_human(name: str) -> str:
    text = str(name or "")
    return (
        text
        .replace("BBC Manchester public safety fallback", "BBC Manchester, резервный источник по происшествиям")
    )


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
    source_status: dict,
    synthetic_freshness: dict,
    prompt_drift: list,
    cost_summary: dict,
    warnings: list[str],
    suspicious_rejects: list,
    suspicious_published: list,
    borderline_queue: dict,
    source_anomalies: list | None = None,
    dead_parsers: list | None = None,
) -> list[tuple[str, str]]:
    issues: list[tuple[int, str, str]] = []
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
    transport_coverage: dict,
    writer_report: dict,
    warnings: list[str],
    borderline_count: int,
) -> list[str]:
    actions: list[str] = []
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


def _llm_speed_summary_lines(llm_report: dict) -> list[str]:
    if not isinstance(llm_report, dict):
        return []
    summary = llm_report.get("diagnostics_summary") or {}
    if not isinstance(summary, dict) or not summary.get("batch_count"):
        return []
    queue = summary.get("queue_wait_seconds") or {}
    api = summary.get("api_seconds") or {}
    completion = summary.get("completion_tokens_per_item") or {}
    lines = [
        (
            "• Скорость ИИ: "
            f"запросов {summary.get('batch_count', 0)}, "
            f"текст получен для {summary.get('accepted', 0)}/{summary.get('sent', 0)} пунктов, "
            f"ожидание p95 {float(queue.get('p95') or 0):.1f}s, "
            f"ответ модели p95 {float(api.get('p95') or 0):.1f}s."
        )
    ]
    truncated = int(summary.get("truncated_responses") or 0)
    timeouts = int(summary.get("timeout_errors") or 0)
    if completion or truncated or timeouts:
        lines.append(
            "• Контроль длины ответа: "
            f"p95 на пункт {float(completion.get('p95') or 0):.1f}, "
            f"обрезанных ответов {truncated}, ошибок по времени {timeouts}."
        )
    english_memory = llm_report.get("english_card_memory") or {}
    translation_memory = llm_report.get("translation_memory") or {}
    if isinstance(english_memory, dict) or isinstance(translation_memory, dict):
        lines.append(
            "• Повторное использование: "
            f"служебных факт-карточек {int((english_memory or {}).get('reused') or 0)}, "
            f"русских строк {int((translation_memory or {}).get('reused') or 0)}."
        )
    return lines


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
    lines.append("")

    recovery = writer_report.get("recovery_controller") if isinstance(writer_report, dict) else {}
    recovery_totals = recovery.get("totals") if isinstance(recovery, dict) else {}
    if isinstance(recovery_totals, dict) and int(recovery_totals.get("section_below_floor") or 0):
        lines.append("🧩 Восстановление тонких блоков")
        lines.append(
            f"Блоков ниже минимума: {recovery_totals.get('section_below_floor', 0)}; "
            f"запасных материалов найдено: {recovery_totals.get('reserve_available', 0)}; "
            f"вставлено замен: {recovery_totals.get('replacements_inserted', 0)}."
        )
        if int(recovery_totals.get("model_recovery_attempts") or 0):
            lines.append(
                f"ИИ-дописка запасных материалов: попыток {recovery_totals.get('model_recovery_attempts', 0)}, "
                f"успешно вставлено {recovery_totals.get('model_recovery_inserted', 0)}, "
                f"не получилось {recovery_totals.get('model_recovery_failed', 0)}."
            )
        if int(recovery_totals.get("still_underflow") or 0):
            lines.append(
                f"Остались тонкими: {recovery_totals.get('still_underflow', 0)} блока; "
                "причины по каждому блоку сохранены в writer_report."
            )
        lines.append("")

    speed_lines = _llm_speed_summary_lines(llm_rewrite)
    if speed_lines:
        lines.append("⏱️ Скорость генерации")
        lines.extend(speed_lines)
        lines.append("")

    issues = _support_top_issues(
        rendered=rendered,
        health_level=health_level,
        health_signals=health.get("signals") or [],
        writer_report=writer_report,
        transport_coverage=transport_coverage,
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

    ticket_rows = _ticketmaster_rows(source_status)
    if ticket_rows:
        lines.append("🎟️ Билеты и события")
        zero_published = sum(1 for row in ticket_rows if int(row.get("raw_count") or 0) and not int(row.get("rendered_count") or 0))
        lines.append(f"• Ticketmaster-источников без публикаций: {zero_published}; детали по каждому источнику в JSON.")
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
        transport_coverage=transport_coverage,
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


def cmd_send_warnings() -> int:
    """Telegram alert ONLY when today's issue did not reach the reader.

    Contract (owner 2026-07-12): the reader-facing digest is the only daily
    Telegram message. The full technical report is printed to stdout and
    lives in the Actions log; when the release gate failed or the send did
    not happen, a short alert goes to Telegram with the first real error.
    Before this, the logic was inverted: a delivered issue produced an
    ~80-line report in the chat, while a blocked issue (2026-07-11,
    ticket_radar_over_cap) was skipped silently by the delivery guard.
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

    run_date = str(report.get("run_date_london") or "").strip()
    delivery_state = read_json(PROJECT_ROOT / "data" / "state" / "delivery_state.json", {})
    delivered_day = str(delivery_state.get("last_delivery_day_london") or "").strip() if isinstance(delivery_state, dict) else ""
    delivery_status = str(delivery_state.get("status") or "").strip() if isinstance(delivery_state, dict) else ""
    today = today_london()
    delivered_today = run_date == today and delivered_day == today and delivery_status == "delivered"

    # Full technical report — Actions log only; nobody reads it in Telegram.
    writer_report = read_json(PROJECT_ROOT / "data" / "state" / "writer_report.json", {})
    print(_build_product_support_text(report, writer_report if isinstance(writer_report, dict) else {}))

    if delivered_today:
        print("Digest delivered — Telegram stays silent; the report above lives in the Actions log.")
        return 0

    errors = [str(e) for e in (report.get("errors") or []) if str(e).strip()]
    reason = errors[0] if errors else (
        f"release_report за {run_date or '—'}, последняя доставка {delivered_day or '—'} (status={delivery_status or '—'})"
    )
    alert = (
        f"⛔ Выпуск {today} НЕ дошёл до читателя.\n"
        f"Причина: {reason}\n"
        "Полный разбор — в Actions-логе шага send-warnings (release_report.json)."
    )
    settings, client, store = _load_store_and_client()
    targets = _effective_targets(settings.telegram_target, store.list_subscribers())
    if not targets:
        print("No Telegram targets configured. Skipping alert.")
        return 0
    for target in targets:
        client.send_text_in_chunks(target, alert, parse_mode=None)
    print(f"Sent not-delivered alert to {len(targets)} target(s).")
    return 0


def cmd_send_weekly_quality() -> int:
    """Send the 7-day quality panel summary to Telegram. Sundays in CI."""
    from news_digest.pipeline.quality_panel import weekly_panel_summary  # noqa: PLC0415

    text = weekly_panel_summary(PROJECT_ROOT / "data" / "state")
    if not text:
        print("No quality_panel_history.json rows yet. Nothing to summarise.")
        return 0
    settings, client, store = _load_store_and_client()
    targets = _effective_targets(settings.telegram_target, store.list_subscribers())
    if not targets:
        print("No Telegram targets. Skipping weekly quality summary.")
        return 0
    for target in targets:
        client.send_text_in_chunks(target, text, parse_mode=None)
    print(f"Sent weekly quality summary to {len(targets)} target(s).")
    return 0


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


def cmd_pre_send_quality_judge(dry_run: bool) -> int:
    """Run the strong-model quality judge before Telegram send.

    The judge is a CONTENT check with a repair executor: it tries repair from
    facts, same-block replacement, then honest degradation before Telegram.
    It still must NEVER block the send; only technical send-consistency
    (built+promoted digest, matching date/marker, no hard release failure) may
    block, and that lives in the send-file gate. So always exit 0; loudly warn
    on issues.
    """
    from news_digest.pipeline.pre_send_quality_judge import evaluate_pre_send_quality  # noqa: PLC0415

    result = evaluate_pre_send_quality(PROJECT_ROOT, dry_run=dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get("can_send") is not True:
        print(
            "WARNING: pre-send quality judge flagged content issues "
            f"({result.get('decision') or 'repair_required'}); shipping anyway "
            "(degradation, never block). See pre_send_quality_report.json.",
            file=sys.stderr,
        )
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


def _flush_obs(stage: str) -> None:
    """Cancel-proof observability flush after a stage. Guarded: a flush error
    must never fail the stage or block the issue."""
    try:
        flush_stage_observability(PROJECT_ROOT, stage)
    except Exception as exc:  # noqa: BLE001
        print(f"observability flush failed ({stage}): {exc}", file=sys.stderr)


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
                    _flush_obs("collect")
                    return 0
            except Exception:
                pass
    result = collect_digest(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    _flush_obs("collect")
    return 0 if result.ok else 1


def cmd_build_inventory() -> int:
    """Backlog 8.1/8.2: persist schema-versioned inventory records from the
    current candidates.json. Additive and non-breaking — writes only under
    data/state/inventory/, never touches the hot path."""
    from news_digest.pipeline.inventory import build_inventory_record, merge_inventory  # noqa: PLC0415
    from news_digest.pipeline.prompts_meta import PROMPT_REGISTRY_VERSION  # noqa: PLC0415
    from news_digest.pipeline.common import write_json_atomic  # noqa: PLC0415

    state_dir = PROJECT_ROOT / "data" / "state"
    payload = read_json(state_dir / "candidates.json", {"candidates": []})
    candidates = [c for c in payload.get("candidates", []) if isinstance(c, dict)]
    by_category: dict[str, list[dict]] = {}
    for candidate in candidates:
        category = str(candidate.get("category") or "unknown")
        by_category.setdefault(category, []).append(
            build_inventory_record(candidate, prompt_version=PROMPT_REGISTRY_VERSION)
        )
    written: dict[str, dict[str, int]] = {}
    for category, records in by_category.items():
        total = merge_inventory(state_dir, category, records)
        written[category] = {"new": len(records), "total": total, "render_ready": sum(1 for r in records if r.get("render_ready"))}
    report = {
        "schema_version": 1,
        "run_at_london": datetime.now(LONDON_TZ).isoformat(),
        "source": "candidates.json",
        "categories": written,
        "total_records": sum(v["total"] for v in written.values()),
    }
    write_json_atomic(state_dir / "inventory_refresh_report.json", report)
    print(json.dumps({"ok": True, **report}, ensure_ascii=False, indent=2))
    return 0


def cmd_collect_inventory(wave: str) -> int:
    """Backlog 8.4: one night wave collects ONLY into inventory (upsert),
    never candidates.json. The 08:00 build is untouched, so a night job can
    never block or corrupt the morning release."""
    from news_digest.pipeline.collector.core import SOURCES, _collect_single_source  # noqa: PLC0415
    from news_digest.pipeline.inventory import (  # noqa: PLC0415
        BREAKING_CHECK_CATEGORIES,
        NIGHT_WAVES,
        build_inventory_record,
        merge_inventory,
        prewrite_stable_inventory_candidate,
    )
    from news_digest.pipeline.entity_extraction import enrich_candidates_entities  # noqa: PLC0415
    from news_digest.pipeline.event_extraction import enrich_candidates_events  # noqa: PLC0415
    from news_digest.pipeline.prompts_meta import PROMPT_REGISTRY_VERSION  # noqa: PLC0415
    from news_digest.pipeline.common import write_json_atomic  # noqa: PLC0415

    if wave == "breaking":
        categories = BREAKING_CHECK_CATEGORIES
    else:
        categories = NIGHT_WAVES.get(wave)
    if not categories:
        print(json.dumps({"ok": False, "error": f"unknown wave '{wave}'", "known": sorted(NIGHT_WAVES) + ["breaking"]}, ensure_ascii=False))
        return 1

    state_dir = PROJECT_ROOT / "data" / "state"
    sources = [s for s in SOURCES if s.report_category in categories]
    per_category: dict[str, list[dict]] = {}
    run_log: list[dict] = []
    started = time.monotonic()
    for source in sources:
        try:
            health, source_candidates = _collect_single_source(source)
        except Exception as exc:  # noqa: BLE001
            run_log.append({"wave": wave, "source": source.name, "category": source.report_category, "error": str(exc), "found": 0})
            continue
        # 0066a: per-card night enrichment only. Corpus-level dedupe/clusters
        # stay in the morning path because a single wave is not the whole corpus.
        enrich_candidates_entities(source_candidates)
        enrich_candidates_events(source_candidates)
        prewritten = 0
        for candidate in source_candidates:
            if isinstance(candidate, dict) and prewrite_stable_inventory_candidate(candidate):
                prewritten += 1
        records = [build_inventory_record(c, prompt_version=PROMPT_REGISTRY_VERSION) for c in source_candidates if isinstance(c, dict)]
        per_category.setdefault(source.report_category, []).extend(records)
        run_log.append({
            "wave": wave,
            "source": source.name,
            "category": source.report_category,
            "checked": bool(health.get("checked")),
            "fetched": bool(health.get("fetched")),
            "found": len(source_candidates),
            "enriched": sum(1 for c in source_candidates if isinstance(c, dict) and c.get("include")),
            "errors": len(health.get("errors") or []),
            "fact_ready": sum(1 for r in records if str(r.get("quality_status") or "") in {"ready", "needs_text"}),
            "prewritten": prewritten,
            "render_ready": sum(1 for r in records if r.get("render_ready")),
        })
    merged: dict[str, int] = {}
    for category, records in per_category.items():
        merged[category] = merge_inventory(state_dir, category, records)
    # Append this wave's run log (never overwrites earlier waves).
    with (state_dir / "inventory_run_log.jsonl").open("a", encoding="utf-8") as handle:
        for row in run_log:
            handle.write(json.dumps({**row, "run_at_london": datetime.now(LONDON_TZ).isoformat()}, ensure_ascii=False) + "\n")
    summary = {
        "ok": True,
        "wave": wave,
        "categories": sorted(categories),
        "sources_polled": len(sources),
        "merged_totals": merged,
        "duration_seconds": round(time.monotonic() - started, 2),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def cmd_dedupe_digest() -> int:
    result = dedupe_candidates(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    _flush_obs("dedupe")
    return 0 if result.ok else 1


def cmd_validate_candidates() -> int:
    result = validate_candidates(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    _flush_obs("validate")
    return 0 if result.ok else 1


def cmd_curator_pass() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from news_digest.pipeline.curator import run_curator_pass  # noqa: PLC0415
    run_curator_pass(PROJECT_ROOT)
    print(json.dumps({"ok": True, "message": "Curator pass complete."}, ensure_ascii=False))
    _flush_obs("curator")
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
    _flush_obs("transport_fill")
    return 0 if result.ok else 1


def cmd_llm_rewrite() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = run_llm_rewrite(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False))
    _flush_obs("llm_rewrite")
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
                    "pre-send-quality-judge",
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


def cmd_write_digest() -> int:
    os.environ.setdefault("NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP", "1")
    result = write_digest(PROJECT_ROOT)
    print(json.dumps(_stage_payload(result), ensure_ascii=False, indent=2))
    _flush_obs("write")
    return 0 if result.ok else 1


def cmd_edit_digest() -> int:
    result = edit_digest(PROJECT_ROOT)
    payload = _stage_payload(result)
    report = read_json(result.report_path, {})
    payload["errors"] = report.get("errors", [])
    payload["warnings"] = report.get("warnings", [])
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    _flush_obs("edit")
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
        "build-inventory",
        help="Persist schema-versioned inventory records from candidates.json (backlog 8.1/8.2).",
    )
    collect_inventory_parser = subparsers.add_parser(
        "collect-inventory",
        help="Run one night wave into inventory only, never candidates.json (backlog 8.4).",
    )
    collect_inventory_parser.add_argument(
        "--wave",
        required=True,
        help="Night wave: events | tickets | pro_food_russian | live_news | breaking.",
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
    subparsers.add_parser(
        "write-digest",
        help="Write staged draft_digest.html from include=true validated candidates.",
    )
    subparsers.add_parser(
        "edit-digest",
        help="Run editor/balancer checks on draft_digest.html.",
    )
    pre_send_parser = subparsers.add_parser(
        "pre-send-quality-judge",
        help=(
            "Required strong-model final editor before Telegram send. "
            "Reads current_digest.html plus compact rendered evidence and "
            "writes pre_send_quality_report.json."
        ),
    )
    pre_send_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and write a dry-run report without calling the model.",
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
        "send-weekly-quality",
        help="Send the 7-day quality panel summary (+ cost line) to Telegram from quality_panel_history.json.",
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
    if args.command == "delivered-today":
        return cmd_delivered_today()
    if args.command == "digest-status":
        return cmd_digest_status()
    if args.command == "build-digest":
        return cmd_build_digest()
    if args.command == "collect-digest":
        return cmd_collect_digest()
    if args.command == "build-inventory":
        return cmd_build_inventory()
    if args.command == "collect-inventory":
        return cmd_collect_inventory(args.wave)
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
    if args.command == "write-digest":
        return cmd_write_digest()
    if args.command == "edit-digest":
        return cmd_edit_digest()
    if args.command == "pre-send-quality-judge":
        return cmd_pre_send_quality_judge(args.dry_run)
    if args.command == "discover-sources":
        return cmd_discover_sources(args.seeds)
    if args.command == "repair-dead-parsers":
        return cmd_repair_dead_parsers()
    if args.command == "mark-pipeline-failed":
        return cmd_mark_pipeline_failed(args.stage)
    if args.command == "init-build-state":
        return cmd_init_build_state(args.overwrite)
    if args.command == "send-file":
        return cmd_send_file(args.file_path, args.parse_mode, args.force)
    if args.command == "send-warnings":
        return cmd_send_warnings()
    if args.command == "send-weekly-quality":
        return cmd_send_weekly_quality()
    if args.command == "post-publish-judge":
        return cmd_post_publish_judge()

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
