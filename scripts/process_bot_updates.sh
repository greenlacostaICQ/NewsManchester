#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK_DIR="$PROJECT_ROOT/data/state/process-updates.lock"

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

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "process-updates is already running"
  exit 0
fi

cleanup() {
  rmdir "$LOCK_DIR" >/dev/null 2>&1 || true
}

trap cleanup EXIT

set -a
source "$PROJECT_ROOT/.env.local"
set +a

"$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_local_digest.py" process-updates
