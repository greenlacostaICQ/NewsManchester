#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNTIME_ROOT="${HOME}/.mnewsdigest"

warn() {
  echo "sync_runtime_bundle: $*" >&2
}

die() {
  warn "$*"
  exit 1
}

copy_required() {
  local src="$1"
  local dst="$2"
  cp "$src" "$dst" || die "Failed to copy $src -> $dst"
}

copy_if_possible() {
  local src="$1"
  local dst="$2"
  if ! cp "$src" "$dst" 2>/dev/null; then
    warn "skipped copy $src -> $dst"
    return 1
  fi
}

assert_runtime_match() {
  local src="$1"
  local dst="$2"

  if [[ ! -f "$src" ]]; then
    die "source file missing for parity check: $src"
  fi

  if [[ ! -f "$dst" ]]; then
    die "runtime file missing after sync: $dst"
  fi

  if ! diff -q "$src" "$dst" >/dev/null; then
    die "runtime drift detected: $dst does not match $src"
  fi
}

mkdir -p \
  "$RUNTIME_ROOT/scripts" \
  "$RUNTIME_ROOT/src" \
  "$RUNTIME_ROOT/data" \
  "$RUNTIME_ROOT/data/outgoing" \
  "$RUNTIME_ROOT/data/state" \
  "$RUNTIME_ROOT/data/archive"

# --- Required files: fail hard if these don't copy ---
[[ -f "$PROJECT_ROOT/.env.local" ]] || die ".env.local missing in workspace"
copy_required "$PROJECT_ROOT/.env.local" "$RUNTIME_ROOT/.env.local"

rsync -a --delete "$PROJECT_ROOT/src/" "$RUNTIME_ROOT/src/" \
  || die "rsync src/ failed"

copy_required "$PROJECT_ROOT/data/sources.toml" "$RUNTIME_ROOT/data/sources.toml"

copy_required "$PROJECT_ROOT/scripts/run_local_digest.py" "$RUNTIME_ROOT/scripts/run_local_digest.py"
copy_required "$PROJECT_ROOT/scripts/run_daily_digest.sh" "$RUNTIME_ROOT/scripts/run_daily_digest.sh"
copy_required "$PROJECT_ROOT/scripts/process_bot_updates.sh" "$RUNTIME_ROOT/scripts/process_bot_updates.sh"

chmod +x \
  "$RUNTIME_ROOT/scripts/run_daily_digest.sh" \
  "$RUNTIME_ROOT/scripts/process_bot_updates.sh"

# --- State files: sync published_facts + dedupe_memory workspace → runtime.
# These are the cross-day dedup history. Without sync, runtime and workspace
# diverge and items get re-published or wrongly deduped.
for state_file in published_facts.json dedupe_memory.json; do
  if [[ -f "$PROJECT_ROOT/data/state/$state_file" ]]; then
    copy_required \
      "$PROJECT_ROOT/data/state/$state_file" \
      "$RUNTIME_ROOT/data/state/$state_file"
  fi
done

# bot_state: only copy if runtime doesn't have it yet (subscribers live in runtime)
if [[ -f "$PROJECT_ROOT/data/state/bot_state.json" && ! -f "$RUNTIME_ROOT/data/state/bot_state.json" ]]; then
  copy_if_possible "$PROJECT_ROOT/data/state/bot_state.json" "$RUNTIME_ROOT/data/state/bot_state.json"
fi

# Do not sync data/outgoing/current_digest.html workspace → runtime.
# Runtime must only serve a digest that its own release gate promoted.
# Copying a stale workspace artifact here can make /latest and send-file
# publish old HTML after a failed build.

assert_runtime_match \
  "$PROJECT_ROOT/src/news_digest/pipeline/collector/sources.py" \
  "$RUNTIME_ROOT/src/news_digest/pipeline/collector/sources.py"
assert_runtime_match \
  "$PROJECT_ROOT/data/sources.toml" \
  "$RUNTIME_ROOT/data/sources.toml"
assert_runtime_match \
  "$PROJECT_ROOT/scripts/run_local_digest.py" \
  "$RUNTIME_ROOT/scripts/run_local_digest.py"
assert_runtime_match \
  "$PROJECT_ROOT/scripts/run_daily_digest.sh" \
  "$RUNTIME_ROOT/scripts/run_daily_digest.sh"
assert_runtime_match \
  "$PROJECT_ROOT/scripts/process_bot_updates.sh" \
  "$RUNTIME_ROOT/scripts/process_bot_updates.sh"
assert_runtime_match \
  "$PROJECT_ROOT/data/state/published_facts.json" \
  "$RUNTIME_ROOT/data/state/published_facts.json"
assert_runtime_match \
  "$PROJECT_ROOT/data/state/dedupe_memory.json" \
  "$RUNTIME_ROOT/data/state/dedupe_memory.json"

echo "Synced runtime bundle to $RUNTIME_ROOT"
