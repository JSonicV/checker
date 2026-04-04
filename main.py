import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
DEFAULT_APP_FILE = "src/app/app.py"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from db_artifact import download_remote_db, has_remote_db_artifact, upload_remote_db
from runtime_config import get_db_path, get_remote_db_uri


def log(message: str) -> None:
    print(f"[checker] {message}")


def build_env(extra_env: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH", "").strip()
    src_path = str(SRC_DIR)
    env["PYTHONPATH"] = (
        f"{src_path}{os.pathsep}{pythonpath}" if pythonpath else src_path
    )
    if extra_env:
        env.update(extra_env)
    return env


def resolve_python_bin() -> str:
    configured_python = os.environ.get("PYTHON_BIN", "").strip()
    return configured_python or sys.executable


def resolve_streamlit_command(app_file: str) -> list[str]:
    configured_streamlit = os.environ.get("STREAMLIT_BIN", "").strip()
    if configured_streamlit:
        return [configured_streamlit, "run", app_file]
    return [resolve_python_bin(), "-m", "streamlit", "run", app_file]


def run_python_script(script_path: str, extra_env: dict[str, str] | None = None) -> None:
    command = [resolve_python_bin(), str(ROOT_DIR / script_path)]
    log(f"Eseguo {' '.join(command)}")
    completed = subprocess.run(
        command,
        cwd=ROOT_DIR,
        env=build_env(extra_env),
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def run_collectors(extra_env: dict[str, str] | None = None) -> None:
    run_python_script("src/collector.py", extra_env=extra_env)
    run_python_script("src/pod_collector.py", extra_env=extra_env)


def maybe_download_db(*, allow_missing: bool) -> None:
    if not has_remote_db_artifact():
        return

    remote_uri = get_remote_db_uri()
    log(f"Scarico DuckDB da {remote_uri} -> {get_db_path()}")
    download_remote_db(allow_missing=allow_missing)


def maybe_upload_db() -> None:
    if not has_remote_db_artifact():
        return

    remote_uri = get_remote_db_uri()
    log(f"Carico DuckDB da {get_db_path()} -> {remote_uri}")
    upload_remote_db()


def build_refresh_work_db_path(live_db_path: Path) -> Path:
    return live_db_path.with_name(f"{live_db_path.stem}.refresh{live_db_path.suffix}")


def prepare_refresh_work_db(
    live_db_path: Path, *, allow_missing_remote_db: bool
) -> Path:
    work_db_path = build_refresh_work_db_path(live_db_path)
    if work_db_path.exists():
        work_db_path.unlink()

    if has_remote_db_artifact():
        remote_uri = get_remote_db_uri()
        log(f"Scarico DuckDB di lavoro da {remote_uri} -> {work_db_path}")
        download_remote_db(
            allow_missing=allow_missing_remote_db,
            local_path=work_db_path,
        )
        if work_db_path.exists():
            return work_db_path

    if live_db_path.exists():
        log(f"Clono il DuckDB live in working copy: {live_db_path} -> {work_db_path}")
        work_db_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(live_db_path, work_db_path)
        return work_db_path

    work_db_path.parent.mkdir(parents=True, exist_ok=True)
    return work_db_path


def promote_refresh_work_db(work_db_path: Path, live_db_path: Path) -> None:
    if not work_db_path.exists():
        raise FileNotFoundError(f"Working copy DuckDB non trovata: {work_db_path}")

    live_db_path.parent.mkdir(parents=True, exist_ok=True)
    work_db_path.replace(live_db_path)

    if has_remote_db_artifact():
        remote_uri = get_remote_db_uri()
        log(f"Carico DuckDB aggiornato da {live_db_path} -> {remote_uri}")
        upload_remote_db(local_path=live_db_path)


def command_collect(_args: argparse.Namespace) -> int:
    run_collectors()
    return 0


def command_download_db(args: argparse.Namespace) -> int:
    maybe_download_db(allow_missing=args.allow_missing)
    return 0


def command_upload_db(_args: argparse.Namespace) -> int:
    maybe_upload_db()
    return 0


def command_refresh_db(args: argparse.Namespace) -> int:
    live_db_path = get_db_path()
    work_db_path = prepare_refresh_work_db(
        live_db_path, allow_missing_remote_db=args.allow_missing_remote_db
    )

    try:
        run_collectors(extra_env={"DUCKDB_PATH": str(work_db_path)})
        promote_refresh_work_db(work_db_path, live_db_path)
    finally:
        if work_db_path.exists():
            work_db_path.unlink()
    return 0


def command_dashboard(args: argparse.Namespace) -> int:
    if not args.skip_db_download:
        maybe_download_db(allow_missing=args.allow_missing_remote_db)

    if args.collect:
        run_collectors()
        if args.upload_db_after_collect:
            maybe_upload_db()

    command = resolve_streamlit_command(args.app_file)
    log(f"Avvio dashboard con {' '.join(command)}")
    os.chdir(ROOT_DIR)
    os.execvpe(command[0], command, build_env())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Runtime CLI per Checker.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser(
        "collect", help="Esegue i collector senza sincronizzazione S3."
    )
    collect_parser.set_defaults(handler=command_collect)

    download_parser = subparsers.add_parser(
        "download-db",
        help="Scarica il DuckDB da S3 se DUCKDB_S3_URI e' configurato.",
    )
    download_parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Non fallire se l'oggetto remoto non esiste ancora.",
    )
    download_parser.set_defaults(handler=command_download_db)

    upload_parser = subparsers.add_parser(
        "upload-db",
        help="Carica il DuckDB su S3 se DUCKDB_S3_URI e' configurato.",
    )
    upload_parser.set_defaults(handler=command_upload_db)

    refresh_parser = subparsers.add_parser(
        "refresh-db",
        help="Scarica il DB remoto, esegue i collector e ricarica il file su S3.",
    )
    refresh_parser.add_argument(
        "--allow-missing-remote-db",
        action="store_true",
        help="Consente il bootstrap se l'oggetto remoto non esiste ancora.",
    )
    refresh_parser.set_defaults(handler=command_refresh_db)

    dashboard_parser = subparsers.add_parser(
        "dashboard",
        help="Scarica opzionalmente il DuckDB remoto e avvia Streamlit.",
    )
    dashboard_parser.add_argument(
        "--app-file",
        default=DEFAULT_APP_FILE,
        help=f"Entry point Streamlit. Default: {DEFAULT_APP_FILE}",
    )
    dashboard_parser.add_argument(
        "--collect",
        action="store_true",
        help="Esegue i collector prima di avviare la dashboard.",
    )
    dashboard_parser.add_argument(
        "--upload-db-after-collect",
        action="store_true",
        help="Se usato con --collect, carica su S3 il DB aggiornato al termine.",
    )
    dashboard_parser.add_argument(
        "--skip-db-download",
        action="store_true",
        help="Salta il download da S3 prima dell'avvio.",
    )
    dashboard_parser.add_argument(
        "--allow-missing-remote-db",
        action="store_true",
        help="Non fallire se il DB remoto non esiste ancora.",
    )
    dashboard_parser.set_defaults(handler=command_dashboard)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
