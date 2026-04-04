#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/app"
CRON_ENV_FILE="$ROOT_DIR/.cron_env.sh"
CRON_FILE="/etc/cron.d/checker-refresh"
DEFAULT_CRON_SCHEDULE="0 5 * * *"

log() {
  printf '[checker-entrypoint] %s\n' "$*"
}

write_cron_env_file() {
  "$ROOT_DIR/.venv/bin/python" - <<'PY'
import os
import shlex
from pathlib import Path

env_file = Path("/app/.cron_env.sh")
ignored = {"PWD", "OLDPWD", "SHLVL", "_"}
lines = ["#!/usr/bin/env bash"]
for key, value in sorted(os.environ.items()):
    if key in ignored:
        continue
    lines.append(f"export {key}={shlex.quote(value)}")
env_file.write_text("\n".join(lines) + "\n")
PY
  chmod 600 "$CRON_ENV_FILE"
}

configure_cron() {
  local schedule="${CHECKER_CRON_SCHEDULE:-$DEFAULT_CRON_SCHEDULE}"

  cat >"$CRON_FILE" <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
${schedule} root $ROOT_DIR/docker/cron-refresh.sh >> /proc/1/fd/1 2>> /proc/1/fd/2
EOF

  chmod 0644 "$CRON_FILE"
}

start_internal_cron() {
  if [[ "${ENABLE_INTERNAL_CRON:-1}" != "1" ]]; then
    log "Cron interno disabilitato."
    return
  fi

  write_cron_env_file
  configure_cron
  cron
  log "Cron interno avviato con schedule '${CHECKER_CRON_SCHEDULE:-$DEFAULT_CRON_SCHEDULE}' (UTC)."
}

start_internal_cron
log "Avvio processo principale: $*"
exec "$@"
