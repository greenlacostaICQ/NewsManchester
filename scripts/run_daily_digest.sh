#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTGOING_FILE="$PROJECT_ROOT/data/outgoing/current_digest.html"

detect_python() {
  local candidates=(
    "${PYTHON_BIN:-}"
    "/opt/homebrew/bin/python3"
    "/usr/local/bin/python3"
    "$(command -v python3 2>/dev/null || true)"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

PYTHON_BIN="$(detect_python || true)"

if [[ -z "$PYTHON_BIN" ]]; then
  echo "No compatible python3 executable found"
  exit 1
fi

if [[ ! -f "$PROJECT_ROOT/.env.local" ]]; then
  echo ".env.local is missing"
  exit 1
fi

set -a
source "$PROJECT_ROOT/.env.local"
set +a

run_stage() {
  local stage="$1"
  if ! "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_local_digest.py" "$stage"; then
    "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_local_digest.py" mark-pipeline-failed "$stage" >/dev/null 2>&1 || true
    echo "Stage '$stage' failed; see data/state/*_report.json"
    exit 1
  fi
}

# Full pipeline chain — must run in this order before the gate.
# Each stage owns its own report file; the gate (build-digest) is the only
# stage allowed to promote draft_digest.html to outgoing/current_digest.html.
run_stage collect-digest
run_stage dedupe-digest
run_stage validate-candidates
run_stage curator-pass
run_stage llm-rewrite
run_stage auto-edit-digest
run_stage write-digest
run_stage edit-digest

if ! "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_local_digest.py" build-digest; then
  echo "Build digest failed; see data/state/release_report.json"
  exit 1
fi

if [[ ! -f "$OUTGOING_FILE" ]]; then
  echo "Outgoing digest file is missing: $OUTGOING_FILE"
  exit 1
fi

TODAY_LONDON="$(TZ=Europe/London date '+%F')"
FILE_DAY_LONDON="$(TZ=Europe/London date -r "$OUTGOING_FILE" '+%F')"

if [[ "$FILE_DAY_LONDON" != "$TODAY_LONDON" ]]; then
  echo "Outgoing digest is stale: file date $FILE_DAY_LONDON, today $TODAY_LONDON"
  exit 1
fi

if "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_local_digest.py" delivered-today >/dev/null 2>&1; then
  echo "Digest was already delivered today; skipping duplicate send"
  exit 0
fi

"$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_local_digest.py" send-file "$OUTGOING_FILE" --parse-mode HTML
