import os
from pathlib import Path


DEFAULT_DB_NAME = "database.duckdb"


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_database_name(default: str = DEFAULT_DB_NAME) -> str:
    configured_name = os.environ.get("DUCKDB_DATABASE", default).strip()
    return configured_name or default


def get_database_dir() -> Path:
    configured_dir = os.environ.get("DUCKDB_LOCAL_DIR", "").strip()
    if configured_dir:
        return Path(configured_dir).expanduser()
    return get_project_root() / "db"


def get_db_path(database: str | None = None) -> Path:
    configured_path = os.environ.get("DUCKDB_PATH", "").strip()
    if configured_path:
        return Path(configured_path).expanduser()

    database_name = database or get_database_name()
    return get_database_dir() / database_name


def ensure_db_parent(database: str | None = None) -> Path:
    db_path = get_db_path(database)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


def get_aws_region() -> str | None:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    return region.strip() if region else None


def get_remote_db_uri() -> str | None:
    uri = os.environ.get("DUCKDB_S3_URI", "").strip()
    return uri or None


def get_db_mtime_ns(database: str | None = None) -> int:
    db_path = get_db_path(database)
    try:
        return db_path.stat().st_mtime_ns
    except FileNotFoundError:
        return 0
