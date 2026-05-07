#!/bin/bash
# Phase 1: Deterministic collection pipeline.
# Runs collect → dedupe → validate. After this, candidates.json contains
# all validated candidates with include/exclude decisions but only
# mechanical draft_line placeholders. An agent (Codex / Antigravity)
# should rewrite draft_line fields before running Phase 2.
#
# Usage:
#   bash scripts/run_pipeline_collect.sh
#
# If you don't need agentic text quality, use run_daily_digest.sh instead
# — it runs the full pipeline end-to-end with deterministic text.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

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
    echo "Stage '$stage' failed; see data/state/*_report.json"
    exit 1
  fi
}

run_stage collect-digest
run_stage dedupe-digest
run_stage validate-candidates

echo "Phase 1 complete. candidates.json is ready for agentic rewrite."
echo "Next: agent rewrites draft_line fields, then run scripts/run_pipeline_publish.sh"
