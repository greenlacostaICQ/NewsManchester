from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from news_digest.assembly.demo_digest import build_demo_digest
from news_digest.bot.service import DigestBotService
from news_digest.config.settings import load_settings
from news_digest.delivery.telegram import TelegramClient
from news_digest.delivery.telegram import TelegramTransportError
from news_digest.jobs.send_demo_digest import send_demo_digest
from news_digest.pipeline.candidate_validator import validate_candidates
from news_digest.pipeline.collector import collect_digest, initialize_collector_state
from news_digest.pipeline.common import read_json, write_json
from news_digest.pipeline.dedupe import dedupe_candidates, initialize_candidates_state
from news_digest.pipeline.editor import edit_digest
from news_digest.pipeline.history import ensure_history_files, record_delivery_artifacts
from news_digest.pipeline.llm_rewrite import run_llm_rewrite
from news_digest.pipeline.release import build_release, initialize_release_inputs
from news_digest.pipeline.writer import write_digest
from news_digest.state.store import StateStore

LONDON_TZ = ZoneInfo("Europe/London")


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

    for target in targets:
        client.send_text_in_chunks(target, text, parse_mode=effective_parse_mode)
    store.mark_delivery(targets, str(resolved_path))
    runtime_state_dir = _runtime_state_dir()
    if runtime_state_dir != settings.state_dir and runtime_state_dir.exists():
        StateStore(runtime_state_dir, settings.archive_dir).mark_delivery(targets, str(resolved_path))
    record_delivery_artifacts(PROJECT_ROOT, resolved_path, _rendered_candidates_for_delivery())
    print(f"Sent file {file_path} to {len(targets)} target(s): {', '.join(targets)}.")
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
    report_payload = {
        "run_at_london": now_london.isoformat(),
        "run_date_london": now_london.strftime("%Y-%m-%d"),
        "release_decision": "fail",
        "message": "Digest pipeline stopped before release gate.",
        "errors": [f"Pipeline stage failed before build-digest: {stage}."],
        "failed_stage": stage,
        "published_facts_updated": False,
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
    if report_path.exists() and candidates_path.exists():
        report = read_json(report_path, {})
        run_at_str = report.get("run_at_london", "")
        if run_at_str:
            try:
                from datetime import timezone
                run_at = datetime.fromisoformat(run_at_str)
                age_hours = (datetime.now(run_at.tzinfo or timezone.utc) - run_at).total_seconds() / 3600
                if age_hours < 12:
                    print(json.dumps({
                        "skipped": True,
                        "reason": f"Collect already ran {age_hours:.1f}h ago — reusing existing candidates.",
                        "run_at_london": run_at_str,
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


def cmd_llm_rewrite() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run_llm_rewrite(PROJECT_ROOT)
    print(json.dumps({"ok": True, "message": "LLM rewrite stage complete."}, ensure_ascii=False))
    return 0


def cmd_write_digest() -> int:
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
        "llm-rewrite",
        help="Write Russian draft_lines via OpenAI → Gemini → Groq Llama provider chain.",
    )
    subparsers.add_parser(
        "write-digest",
        help="Write staged draft_digest.html from include=true validated candidates.",
    )
    subparsers.add_parser(
        "edit-digest",
        help="Run editor/balancer checks on draft_digest.html.",
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
    if args.command == "llm-rewrite":
        return cmd_llm_rewrite()
    if args.command == "write-digest":
        return cmd_write_digest()
    if args.command == "edit-digest":
        return cmd_edit_digest()
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

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
