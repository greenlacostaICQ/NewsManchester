#!/bin/bash
set -euo pipefail

DAILY_PLIST_DST="$HOME/Library/LaunchAgents/com.mnewsdigest.daily.plist"
BOT_PLIST_DST="$HOME/Library/LaunchAgents/com.mnewsdigest.bot-updates.plist"

launchctl bootout "gui/$(id -u)" "$DAILY_PLIST_DST" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$BOT_PLIST_DST" >/dev/null 2>&1 || true
rm -f "$DAILY_PLIST_DST" "$BOT_PLIST_DST"
echo "Removed launchd jobs:"
echo "  $DAILY_PLIST_DST"
echo "  $BOT_PLIST_DST"
