#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/app"
LOCK_DIR="/tmp/checker-refresh.lock"
ENV_FILE="$ROOT_DIR/.cron_env.sh"

log() {
  printf '[checker-cron] %s\n' "$*"
}

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  log "Refresh gia' in corso, salto questa esecuzione."
  exit 0
fi

cleanup() {
  rmdir "$LOCK_DIR"
}

trap cleanup EXIT

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

cd "$ROOT_DIR"
log "Avvio refresh programmato del DuckDB."
"$ROOT_DIR/.venv/bin/python" main.py refresh-db --allow-missing-remote-db
log "Refresh programmato completato."
