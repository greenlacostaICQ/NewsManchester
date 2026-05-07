#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNTIME_ROOT="$HOME/.mnewsdigest"
BOT_PLIST_SRC="$PROJECT_ROOT/ops/launchd/com.mnewsdigest.bot-updates.plist"
DAILY_PLIST_SRC="$PROJECT_ROOT/ops/launchd/com.mnewsdigest.daily.plist"
BOT_PLIST_DST="$HOME/Library/LaunchAgents/com.mnewsdigest.bot-updates.plist"
DAILY_PLIST_DST="$HOME/Library/LaunchAgents/com.mnewsdigest.daily.plist"

mkdir -p "$HOME/Library/LaunchAgents"
bash "$PROJECT_ROOT/scripts/sync_runtime_bundle.sh"

: > "$RUNTIME_ROOT/data/state/launchd.stdout.log"
: > "$RUNTIME_ROOT/data/state/launchd.stderr.log"
: > "$RUNTIME_ROOT/data/state/bot-updates.stdout.log"
: > "$RUNTIME_ROOT/data/state/bot-updates.stderr.log"

launchctl bootout "gui/$(id -u)" "$DAILY_PLIST_DST" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$BOT_PLIST_DST" >/dev/null 2>&1 || true
rm -f "$DAILY_PLIST_DST" "$BOT_PLIST_DST"
sed "s|__RUNTIME_ROOT__|$RUNTIME_ROOT|g" "$DAILY_PLIST_SRC" > "$DAILY_PLIST_DST"
sed "s|__RUNTIME_ROOT__|$RUNTIME_ROOT|g" "$BOT_PLIST_SRC" > "$BOT_PLIST_DST"
launchctl enable "gui/$(id -u)/com.mnewsdigest.daily" >/dev/null 2>&1 || true
launchctl enable "gui/$(id -u)/com.mnewsdigest.bot-updates" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$DAILY_PLIST_DST"
launchctl bootstrap "gui/$(id -u)" "$BOT_PLIST_DST"
launchctl kickstart -k "gui/$(id -u)/com.mnewsdigest.daily" >/dev/null 2>&1 || true
launchctl kickstart -k "gui/$(id -u)/com.mnewsdigest.bot-updates" >/dev/null 2>&1 || true

echo "Loaded launchd job:"
echo "  $DAILY_PLIST_DST"
echo "  $BOT_PLIST_DST"
