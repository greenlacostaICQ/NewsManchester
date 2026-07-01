#!/bin/bash
# Backlog 8.4: night inventory waves. Fired by launchd at 00:30 / 02:00 /
# 03:30 / 06:15 / 07:45 (Europe/London). The wave is derived from the current
# time so a single plist can drive all waves. Each wave collects ONLY into
# data/state/inventory/*.jsonl (upsert) — it never writes candidates.json, so
# it can never block or corrupt the 08:00 release.
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

if [[ -f "$PROJECT_ROOT/.env.local" ]]; then
  set -a
  source "$PROJECT_ROOT/.env.local"
  set +a
fi

# Wave selection by London wall-clock. A launchd time can drift a minute or
# two, so match on the hour with a widened minute window rather than an exact
# HH:MM. An unrecognised firing is a no-op (logged), never an error.
NOW_HM="$(TZ=Europe/London date '+%H:%M')"
HOUR="${NOW_HM%%:*}"
MIN="10#${NOW_HM##*:}"

WAVE=""
case "$HOUR" in
  00) WAVE="events" ;;
  02) WAVE="tickets" ;;
  03) WAVE="pro_food_russian" ;;
  06) WAVE="live_news" ;;
  07) [[ "$MIN" -ge 30 ]] && WAVE="breaking" ;;
esac

if [[ -z "$WAVE" ]]; then
  echo "run_night_inventory: no wave mapped for $NOW_HM (London) — nothing to do."
  exit 0
fi

echo "run_night_inventory: firing wave '$WAVE' at $NOW_HM (London)."
exec "$PYTHON_BIN" "$PROJECT_ROOT/scripts/run_local_digest.py" collect-inventory --wave "$WAVE"
