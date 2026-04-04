from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from runtime_config import ensure_db_parent, get_aws_region, get_remote_db_uri


MISSING_OBJECT_CODES = {"404", "NoSuchKey", "NotFound"}


def has_remote_db_artifact() -> bool:
    return get_remote_db_uri() is not None


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"URI S3 non valido: {uri}")

    bucket_and_key = uri[len("s3://") :]
    bucket, separator, key = bucket_and_key.partition("/")
    if not bucket or not separator or not key:
        raise ValueError(f"URI S3 non valido: {uri}")
    return bucket, key


def get_s3_client():
    region = get_aws_region()
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")


def download_remote_db(
    database: str | None = None,
    *,
    allow_missing: bool = False,
    remote_uri: str | None = None,
    local_path: str | Path | None = None,
) -> Path:
    if local_path is not None:
        db_path = Path(local_path).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        db_path = ensure_db_parent(database)
    uri = remote_uri or get_remote_db_uri()
    if uri is None:
        return db_path

    bucket, key = parse_s3_uri(uri)
    tmp_path = db_path.with_suffix(f"{db_path.suffix}.download")

    try:
        get_s3_client().download_file(bucket, key, str(tmp_path))
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if allow_missing and error_code in MISSING_OBJECT_CODES:
            if tmp_path.exists():
                tmp_path.unlink()
            return db_path
        raise

    tmp_path.replace(db_path)
    return db_path


def upload_remote_db(
    database: str | None = None,
    *,
    remote_uri: str | None = None,
    local_path: str | Path | None = None,
) -> Path:
    if local_path is not None:
        db_path = Path(local_path).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        db_path = ensure_db_parent(database)
    uri = remote_uri or get_remote_db_uri()
    if uri is None:
        return db_path
    if not db_path.exists():
        raise FileNotFoundError(f"File DuckDB locale non trovato: {db_path}")

    bucket, key = parse_s3_uri(uri)
    get_s3_client().upload_file(str(db_path), bucket, key)
    return db_path
