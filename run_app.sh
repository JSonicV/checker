#!/usr/bin/env bash

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_FILE="${APP_FILE:-src/app/app.py}"
AWS_CLI_BIN="${AWS_CLI_BIN:-aws}"
VENV_BIN_DIR="${VENV_BIN_DIR:-$ROOT_DIR/.venv/bin}"
RUN_COLLECTORS=0

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "$VENV_BIN_DIR/python" ]]; then
    PYTHON_BIN="$VENV_BIN_DIR/python"
  elif [[ -x "$VENV_BIN_DIR/python3" ]]; then
    PYTHON_BIN="$VENV_BIN_DIR/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

if [[ -z "${STREAMLIT_BIN:-}" ]]; then
  if [[ -x "$VENV_BIN_DIR/streamlit" ]]; then
    STREAMLIT_BIN="$VENV_BIN_DIR/streamlit"
  else
    STREAMLIT_BIN="streamlit"
  fi
fi

usage() {
  cat <<'EOF'
Usage: ./run_app.sh [--collect] [--help]

Options:
  --collect  Esegue src/collector.py e src/pod_collector.py prima di avviare l'app.
  --help             Mostra questo messaggio.

Env utili:
  PYTHON_BIN         Interprete Python da usare per i collector e come fallback per Streamlit.
  STREAMLIT_BIN      Binario Streamlit da usare. Default: .venv/bin/streamlit se presente
  AWS_CLI_BIN        Binario AWS CLI da usare per aws sso login. Default: aws
  AWS_PROFILE        Se impostato, viene passato ad aws sso login --profile <profile>
  APP_FILE           Entry point Streamlit. Default: src/app/app.py
  VENV_BIN_DIR       Directory bin del virtualenv. Default: .venv/bin
EOF
}

while (($# > 0)); do
  case "$1" in
    --collect)
      RUN_COLLECTORS=1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'Argomento non riconosciuto: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

log() {
  printf '[run_app] %s\n' "$*"
}

ensure_command() {
  local cmd="$1"
  if [[ "$cmd" == */* ]]; then
    if [[ -x "$cmd" ]]; then
      return 0
    fi
  elif command -v "$cmd" >/dev/null 2>&1; then
    return 0
  fi

  printf 'Comando non trovato: %s\n' "$cmd" >&2
  exit 1
}

aws_login() {
  ensure_command "$AWS_CLI_BIN"

  if [[ -n "${AWS_PROFILE:-}" ]]; then
    log "Credenziali AWS non valide o scadute. Avvio '$AWS_CLI_BIN sso login --profile $AWS_PROFILE'."
    "$AWS_CLI_BIN" sso login --profile "$AWS_PROFILE"
  else
    log "Credenziali AWS non valide o scadute. Avvio '$AWS_CLI_BIN sso login'."
    "$AWS_CLI_BIN" sso login
  fi
}

is_aws_login_error() {
  local output_file="$1"
  grep -Eiq \
    'aws sso login|UnauthorizedSSOTokenError|SSOTokenLoadError|Error loading SSO Token|The SSO session associated with this profile has expired|Token has expired and refresh failed|ExpiredToken|Unable to locate credentials|NoCredentialsError|UnrecognizedClientException|AccessDenied' \
    "$output_file"
}

run_python_script() {
  local script_path="$1"
  local label="$2"
  local tmp_output

  tmp_output="$(mktemp)"
  log "Eseguo $label: $script_path"

  if (
    cd "$ROOT_DIR" &&
    "$PYTHON_BIN" "$script_path"
  ) > >(tee "$tmp_output") 2> >(tee -a "$tmp_output" >&2); then
    rm -f "$tmp_output"
    return 0
  fi

  if is_aws_login_error "$tmp_output"; then
    rm -f "$tmp_output"
    aws_login
    tmp_output="$(mktemp)"
    log "Ritento $label dopo aws sso login."
    if (
      cd "$ROOT_DIR" &&
      "$PYTHON_BIN" "$script_path"
    ) > >(tee "$tmp_output") 2> >(tee -a "$tmp_output" >&2); then
      rm -f "$tmp_output"
      return 0
    fi
  fi

  rm -f "$tmp_output"
  return 1
}

start_streamlit() {
  ensure_command "$STREAMLIT_BIN"
  cd "$ROOT_DIR"
  exec "$STREAMLIT_BIN" run "$APP_FILE"
}

ensure_command "$PYTHON_BIN"

if ((RUN_COLLECTORS)); then
  run_python_script "src/collector.py" "cost collector"
  run_python_script "src/pod_collector.py" "pod collector"
fi

log "Avvio Streamlit."
start_streamlit
