#!/usr/bin/env python3
"""Offline replay of a past day's publish pipeline (write → edit → build).

Every morning run commits its full state to git ("chore: digest state
YYYY-MM-DD"), including data/state/candidates.json (with draft_line already
written) and the actually-sent data/outgoing/current_digest.html. This script
takes that snapshot, re-runs the deterministic publish stages in a sandbox —
no network, no LLM, no Telegram — and compares the rebuilt digest against the
one readers actually received.

Usage:
    python3 scripts/replay_day.py 2026-07-09
    python3 scripts/replay_day.py 2026-07-09 --keep
    python3 scripts/replay_day.py 2026-07-09 --sandbox /tmp/replay
    python3 scripts/replay_day.py --golden          # all golden + ordinary days

Workflow this enables: fix a bug → replay the day it shipped on → see the
defect gone in the rebuilt HTML while it is still visible in the sent one →
replay a few ordinary days to confirm nothing else moved.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import difflib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CODE_ROOT = Path(os.environ.get("NEWS_DIGEST_REPLAY_CODE_ROOT", PROJECT_ROOT))
SRC_DIR = CODE_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Days with defects that shipped to readers (expectations measured on the
# real sent HTML, see --golden) plus ordinary days that shipped clean.
GOLDEN_DAYS = [
    "2026-06-27", "2026-07-02", "2026-07-07", "2026-07-09",
    "2026-07-12", "2026-07-15",
]
# 2026-06-30 deliberately excluded: prod itself did not ship that morning
# (the committed current_digest.html is the stale 06-29 file), so it is not
# an "ordinary clean day" fixture.
ORDINARY_DAYS = ["2026-06-29", "2026-07-01", "2026-07-03", "2026-07-05", "2026-07-06", "2026-07-08"]

# What must be visible in the SENT artifact of each golden day. Replay output
# is reported next to it: if current code no longer produces the defect, that
# is the fix working, not a harness failure.
GOLDEN_EXPECTATIONS: dict[str, dict[str, object]] = {
    "2026-06-27": {"boilerplate_endings_min": 1},
    "2026-07-02": {"lead_status": "missing"},
    "2026-07-07": {"lead_status": "empty", "max_blank_run_min": 2},
    "2026-07-09": {"max_blank_run_min": 2, "boilerplate_endings_min": 1},
}

# Env that could reach the outside world. Scrubbed before stages run; the
# socket block below is the hard guarantee, this just makes skips explicit.
NETWORK_ENV_KEYS = [
    "OPENAI_API_KEY",
    "DEEPSEEK_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "TELEGRAM_ADMIN_CHAT_ID",
    "NRE_USERNAME",
    "NRE_PASSWORD",
    "NEWS_DIGEST_TICKET_NOTABILITY_LOOKUP",
]

MASTHEAD_RE = re.compile(r"^<b>Greater Manchester Brief — (\d{4}-\d{2}-\d{2}), (\d{2}:\d{2})</b>$")
HEADER_RE = re.compile(r"^<b>([^<>]+)</b>$")
LEAD_TITLE = "Главная история дня"
REPLAY_SANDBOX_MARKER = ".news_digest_replay_sandbox"
STALE_REPLAY_SECONDS = 24 * 60 * 60
REPLAY_TEMP_NAME_RE = re.compile(r"^replay_\d{4}-\d{2}-\d{2}_[A-Za-z0-9_-]+$")


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def find_state_commit(day: str) -> str:
    sha = _git("log", "--format=%H", "-1", "--fixed-strings", "--grep", f"digest state {day}").strip()
    if not sha:
        raise SystemExit(f"No 'chore: digest state {day}' commit found in git history.")
    return sha


def _restore_pre_send_history(sha: str, sandbox: Path) -> str:
    """Use publication history from immediately before the replayed send.

    Daily state commits are written after Telegram delivery, so their
    ``published_facts.json`` already contains the issue being replayed. Feeding
    that file back into write/edit/build makes the issue dedupe against itself
    and turns a product replay into a false 30-line mini-issue.
    """
    parent = _git("rev-parse", f"{sha}^").strip()
    history_path = sandbox / "data" / "state" / "published_facts.json"
    try:
        history = _git("show", f"{parent}:data/state/published_facts.json")
    except subprocess.CalledProcessError:
        history = json.dumps({"last_updated_london": None, "facts": []})
    history_path.write_text(history, encoding="utf-8")
    return parent


def extract_snapshot(sha: str, sandbox: Path) -> tuple[str, str]:
    """Materialize the day's data/state (+ data/validation if present) into
    the sandbox and return the sent HTML plus the pre-send history commit."""
    paths = ["data/state"]
    if _git("ls-tree", "-d", sha, "data/validation").strip():
        paths.append("data/validation")
    archive = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "archive", sha, *paths],
        check=True,
        capture_output=True,
    ).stdout
    subprocess.run(["tar", "-x", "-C", str(sandbox)], input=archive, check=True)
    if not (sandbox / "data" / "state" / "candidates.json").exists():
        raise SystemExit(f"Snapshot {sha[:8]} has no data/state/candidates.json — cannot replay.")
    history_sha = _restore_pre_send_history(sha, sandbox)
    (sandbox / "data" / "outgoing").mkdir(parents=True, exist_ok=True)
    try:
        sent = _git("show", f"{sha}:data/outgoing/current_digest.html")
    except subprocess.CalledProcessError:
        raise SystemExit(f"Snapshot {sha[:8]} has no sent current_digest.html — nothing to compare against.")
    (sandbox / "sent_digest.html").write_text(sent, encoding="utf-8")
    return sent, history_sha


def freeze_environment(day: str, sent_html: str, sha: str) -> str:
    """Scrub network credentials and freeze pipeline time to the moment the
    real digest was rendered. Returns the frozen ISO timestamp."""
    import os

    for key in NETWORK_ENV_KEYS:
        os.environ.pop(key, None)
    os.environ["WARNINGS_TO_TELEGRAM"] = "0"

    match = MASTHEAD_RE.match(sent_html.splitlines()[0].strip()) if sent_html else None
    if match and match.group(1) == day:
        fake_now = f"{day}T{match.group(2)}:00"
    else:
        # Masthead date != replay day means prod shipped nothing that morning
        # (stale file committed); freeze to the commit time instead.
        commit_iso = _git("show", "-s", "--format=%aI", sha).strip()
        fake_now = f"{day}T{commit_iso[11:19]}" if commit_iso else f"{day}T08:00:00"
    os.environ["NEWS_DIGEST_FAKE_NOW"] = fake_now
    return fake_now


def block_network() -> None:
    """Hard offline guarantee: any attempt to open a connection raises."""

    def _blocked(*_args, **_kwargs):
        raise RuntimeError("network access blocked: replay_day runs offline")

    socket.socket.connect = _blocked  # type: ignore[method-assign]
    socket.create_connection = _blocked  # type: ignore[assignment]
    socket.getaddrinfo = _blocked  # type: ignore[assignment]
    # curl_cffi performs DNS/connect below Python's socket monkeypatch. Patch
    # the project's single wrapper too, otherwise a reserve refetch can escape
    # the replay sandbox and wait through real network retries.
    from news_digest.pipeline.collector import fetch  # noqa: PLC0415

    fetch._fetch_text_curl_cffi = _blocked


def run_stages(sandbox: Path) -> list[dict[str, object]]:
    from news_digest.pipeline.editor import edit_digest
    from news_digest.pipeline.release import build_release
    from news_digest.pipeline.writer import write_digest

    results: list[dict[str, object]] = []
    for name, fn in (("write-digest", write_digest), ("edit-digest", edit_digest), ("build-digest", build_release)):
        started = time.monotonic()
        try:
            result = fn(sandbox)
            row = {"stage": name, "ok": bool(result.ok), "message": str(result.message)}
        except Exception as exc:  # noqa: BLE001
            row = {"stage": name, "ok": False, "message": f"{exc.__class__.__name__}: {exc}"}
        row["seconds"] = round(time.monotonic() - started, 1)
        results.append(row)
        draft_path = sandbox / "data" / "state" / "draft_digest.html"
        if row["ok"] and draft_path.exists():
            artifact_name = name.replace("-", "_")
            shutil.copy2(draft_path, sandbox / f"replay_after_{artifact_name}.html")
        if not row["ok"]:
            break
    return results


def analyze_digest(html_text: str) -> dict[str, object]:
    """Section/bullet counts plus detectors for the known shipped defects:
    blank-line runs, missing/empty lead block, boilerplate endings."""
    from news_digest.pipeline.editor import _EMPTY_ENDING_RE, _strip_editor_tags

    lines = html_text.splitlines()
    sections: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    blank_run = 0
    max_blank_run = 0
    blank_runs_2plus = 0
    boilerplate = 0

    for line in lines:
        stripped = line.strip()
        if not stripped:
            blank_run += 1
            if blank_run == 2:
                blank_runs_2plus += 1
            max_blank_run = max(max_blank_run, blank_run)
            continue
        blank_run = 0
        if MASTHEAD_RE.match(stripped):
            continue
        header = HEADER_RE.match(stripped)
        if header:
            # A bold-only line opens a new section (the lead body is bold too,
            # but it carries a trailing link/text so it doesn't match HEADER_RE).
            current = {"title": header.group(1), "bullets": 0, "content_lines": 0}
            sections.append(current)
            continue
        if current is not None:
            current["content_lines"] = int(current["content_lines"]) + 1
            if stripped.startswith("• "):
                current["bullets"] = int(current["bullets"]) + 1
        if stripped.startswith("• "):
            body = re.sub(r"\s*<a\s+[^>]*>.*?</a>\s*$", "", stripped, flags=re.IGNORECASE | re.DOTALL)
            if _EMPTY_ENDING_RE.search(_strip_editor_tags(body)):
                boilerplate += 1

    lead = next((s for s in sections if s["title"] == LEAD_TITLE), None)
    if lead is None:
        lead_status = "missing"
    elif int(lead["content_lines"]) == 0:
        lead_status = "empty"
    else:
        lead_status = "ok"

    return {
        "sections": [{"title": s["title"], "bullets": s["bullets"]} for s in sections],
        "section_count": len(sections),
        "bullet_total": sum(int(s["bullets"]) for s in sections),
        "max_blank_run": max_blank_run,
        "blank_runs_2plus": blank_runs_2plus,
        "lead_status": lead_status,
        "boilerplate_endings": boilerplate,
    }


def diff_digests(sent: str, replayed: str, sandbox: Path) -> dict[str, object]:
    def normalize(text: str) -> list[str]:
        out = []
        for line in text.splitlines():
            out.append(MASTHEAD_RE.sub(r"<b>Greater Manchester Brief — \1, HH:MM</b>", line.strip()))
        return out

    sent_lines, replay_lines = normalize(sent), normalize(replayed)
    diff = list(difflib.unified_diff(sent_lines, replay_lines, "sent", "replayed", lineterm=""))
    (sandbox / "replay_diff.txt").write_text("\n".join(diff), encoding="utf-8")
    added = sum(1 for d in diff if d.startswith("+") and not d.startswith("+++"))
    removed = sum(1 for d in diff if d.startswith("-") and not d.startswith("---"))
    same = sum(1 for a, b in zip(sent_lines, replay_lines) if a == b)
    return {
        "identical": sent_lines == replay_lines,
        "lines_only_in_sent": removed,
        "lines_only_in_replay": added,
        "matching_lines": same,
        "diff_file": str(sandbox / "replay_diff.txt"),
    }


def check_golden_expectations(day: str, sent_metrics: dict[str, object]) -> list[str]:
    failures: list[str] = []
    expected = GOLDEN_EXPECTATIONS.get(day, {})
    for key, want in expected.items():
        if key == "lead_status" and sent_metrics["lead_status"] != want:
            failures.append(f"lead_status: expected {want}, got {sent_metrics['lead_status']}")
        if key == "max_blank_run_min" and int(sent_metrics["max_blank_run"]) < int(want):
            failures.append(f"max_blank_run: expected >= {want}, got {sent_metrics['max_blank_run']}")
        if key == "boilerplate_endings_min" and int(sent_metrics["boilerplate_endings"]) < int(want):
            failures.append(f"boilerplate_endings: expected >= {want}, got {sent_metrics['boilerplate_endings']}")
    return failures


def describe_sent_context_drift(
    sent_metrics: dict[str, object], replay_metrics: dict[str, object] | None
) -> list[str]:
    """Describe sent-vs-replay drift without treating it as a regression.

    Daily snapshots contain post-send mutations, while replay executes current
    code. The project regression signal is baseline-replay versus changed-code
    replay; the sent artifact is context and golden-defect evidence only.
    """
    if not replay_metrics:
        return ["replayed HTML is missing"]
    sent_bullets = int(sent_metrics.get("bullet_total") or 0)
    replay_bullets = int(replay_metrics.get("bullet_total") or 0)
    allowed_gap = max(3, round(sent_bullets * 0.15))
    failures: list[str] = []
    if abs(sent_bullets - replay_bullets) > allowed_gap:
        failures.append(
            f"bullet_total drift {sent_bullets}->{replay_bullets} exceeds {allowed_gap}"
        )
    if sent_metrics.get("lead_status") == "ok" and replay_metrics.get("lead_status") != "ok":
        failures.append("sent lead was visible but replay lead is missing/empty")
    return failures


@contextmanager
def replay_sandbox(day: str, sandbox_root: Path | None, keep: bool):
    """Yield a replay sandbox and clean up temporary runs by default."""
    if sandbox_root is not None:
        sandbox = sandbox_root / day
        sandbox.mkdir(parents=True, exist_ok=True)
        yield sandbox
        return
    if keep:
        sandbox = Path(tempfile.mkdtemp(prefix=f"replay_{day}_"))
        yield sandbox
        return
    with tempfile.TemporaryDirectory(prefix=f"replay_{day}_") as temp_dir:
        sandbox = Path(temp_dir)
        (sandbox / REPLAY_SANDBOX_MARKER).write_text(str(PROJECT_ROOT), encoding="utf-8")
        yield sandbox


def cleanup_stale_replay_sandboxes(*, now: float | None = None) -> int:
    """Remove only abandoned, script-owned default sandboxes older than 24h."""
    cutoff = (time.time() if now is None else now) - STALE_REPLAY_SECONDS
    removed = 0
    for path in Path(tempfile.gettempdir()).iterdir():
        if not path.is_dir() or REPLAY_TEMP_NAME_RE.fullmatch(path.name) is None:
            continue
        marker = path / REPLAY_SANDBOX_MARKER
        try:
            owned = marker.read_text(encoding="utf-8") == str(PROJECT_ROOT)
            stale = max(path.stat().st_mtime, marker.stat().st_mtime) < cutoff
        except OSError:
            continue
        if owned and stale:
            shutil.rmtree(path, ignore_errors=True)
            if not path.exists():
                removed += 1
    return removed


def replay_one(day: str, sandbox: Path) -> dict[str, object]:
    sha = find_state_commit(day)

    sent_html, history_sha = extract_snapshot(sha, sandbox)
    fake_now = freeze_environment(day, sent_html, sha)
    block_network()

    started = time.monotonic()
    stages = run_stages(sandbox)
    total_seconds = round(time.monotonic() - started, 1)

    replay_path = sandbox / "data" / "outgoing" / "current_digest.html"
    replayed_html = replay_path.read_text(encoding="utf-8") if replay_path.exists() else ""

    report: dict[str, object] = {
        "day": day,
        "commit": sha,
        "history_commit": history_sha,
        "frozen_now": fake_now,
        "sandbox": str(sandbox),
        "stages": stages,
        "total_seconds": total_seconds,
        "stages_ok": all(s["ok"] for s in stages) and len(stages) == 3,
        "sent_metrics": analyze_digest(sent_html),
        "replay_metrics": analyze_digest(replayed_html) if replayed_html else None,
        "diff": diff_digests(sent_html, replayed_html, sandbox) if replayed_html else None,
    }
    report["golden_failures"] = check_golden_expectations(day, report["sent_metrics"])
    report["sent_context_drift"] = describe_sent_context_drift(
        report["sent_metrics"], report["replay_metrics"]
    )
    (sandbox / "replay_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report


def print_report(report: dict[str, object]) -> None:
    day = report["day"]
    print(f"\n=== Replay {day} (commit {str(report['commit'])[:8]}, frozen at {report['frozen_now']}) ===")
    for stage in report["stages"]:
        mark = "ok" if stage["ok"] else "FAIL"
        print(f"  {stage['stage']:<14} {mark:<5} {stage['seconds']}s  {stage['message']}")
    print(f"  total: {report['total_seconds']}s  sandbox: {report['sandbox']}")

    sent = report["sent_metrics"]
    replayed = report["replay_metrics"]
    print(f"  {'metric':<22} {'sent':>8} {'replayed':>10}")
    for key in ("section_count", "bullet_total", "max_blank_run", "blank_runs_2plus", "lead_status", "boilerplate_endings"):
        rv = replayed[key] if replayed else "—"
        print(f"  {key:<22} {str(sent[key]):>8} {str(rv):>10}")
    if report["diff"]:
        d = report["diff"]
        if d["identical"]:
            print("  diff vs sent: identical")
        else:
            print(
                f"  diff vs sent: {d['lines_only_in_sent']} lines only in sent, "
                f"{d['lines_only_in_replay']} only in replay → {d['diff_file']}"
            )
    if report["golden_failures"]:
        print(f"  GOLDEN CHECK FAILED: {'; '.join(report['golden_failures'])}")
    elif report["day"] in GOLDEN_EXPECTATIONS:
        print("  golden check: known defects confirmed in sent artifact")
    if report["sent_context_drift"]:
        print(f"  sent context drift: {'; '.join(report['sent_context_drift'])}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("day", nargs="?", help="Date to replay, YYYY-MM-DD")
    parser.add_argument("--golden", action="store_true", help="Replay all golden + ordinary days")
    parser.add_argument("--sandbox", type=Path, default=None, help="Directory for sandboxes (default: system temp)")
    parser.add_argument("--keep", action="store_true", help="Keep temporary sandboxes for inspection")
    args = parser.parse_args()

    if not args.golden and not args.day:
        parser.error("pass a date (YYYY-MM-DD) or --golden")

    if args.sandbox is None:
        stale_removed = cleanup_stale_replay_sandboxes()
        if stale_removed:
            print(f"Removed {stale_removed} abandoned replay sandbox(es) older than 24h.")

    days = GOLDEN_DAYS + ORDINARY_DAYS if args.golden else [args.day]
    failures: list[str] = []
    for day in days:
        with replay_sandbox(day, args.sandbox, args.keep) as sandbox:
            report = replay_one(day, sandbox)
            print_report(report)
            if not report["stages_ok"]:
                failures.append(f"{day}: stage failure")
            if report["golden_failures"]:
                failures.append(f"{day}: golden expectations not met")
            if report["total_seconds"] > 300:
                failures.append(f"{day}: replay took {report['total_seconds']}s (> 5 min budget)")

    if failures:
        print("\nFAILURES:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print(f"\nAll {len(days)} day(s) replayed with pre-send history restored.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
