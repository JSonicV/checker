import gzip
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from duckdb_client import get_duckdb_client
from utils import accounts_map, roles_arn_map


load_dotenv()

DUCKDB_DATABASE = os.environ.get("DUCKDB_DATABASE", "database.duckdb")
POD_MONTHLY_TABLE_NAME = os.environ.get("DUCKDB_POD_TABLE", "pod_monthly_trend")
POD_DAILY_TABLE_NAME = os.environ.get("DUCKDB_POD_DAILY_TABLE", "pod_daily_trend")
AWS_REGION = os.environ.get(
    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")
)
POD_S3_BUCKET = os.environ.get("POD_S3_BUCKET", "statistics-master-eu-central-1")
POD_S3_PREFIX = os.environ.get("POD_S3_PREFIX", "daily_source_totals").strip("/")
POD_SNAPSHOT_FILENAME = os.environ.get("POD_SNAPSHOT_FILENAME", "snapshots.json")
POD_BOOTSTRAP_START_YEAR = int(os.environ.get("POD_BOOTSTRAP_START_YEAR", "2024"))
POD_AWS_ROLE_SESSION_NAME = os.environ.get(
    "POD_AWS_ROLE_SESSION_NAME", "PodCollectorSession"
)
PARTITION_YEAR_PATTERN = re.compile(r"(?:^|/)year=(\d{4})(?:/|$)")
REQUIRED_SNAPSHOT_COLUMNS = ("date", "tenant", "source_backend", "pods", "onboarded")


@dataclass(frozen=True)
class TenantSource:
    source_key: str
    path_template_env: str
    legacy_path_env: str
    bucket_env: str
    prefix_env: str
    filename_env: str


TENANT_SOURCES = (
    TenantSource(
        "digiwatt",
        "POD_DIGIWATT_PATH_TEMPLATE",
        "POD_DIGIWATT_PATHS",
        "POD_DIGIWATT_S3_BUCKET",
        "POD_DIGIWATT_S3_PREFIX",
        "POD_DIGIWATT_SNAPSHOT_FILENAME",
    ),
    TenantSource(
        "fastweb",
        "POD_FASTWEB_PATH_TEMPLATE",
        "POD_FASTWEB_PATHS",
        "POD_FASTWEB_S3_BUCKET",
        "POD_FASTWEB_S3_PREFIX",
        "POD_FASTWEB_SNAPSHOT_FILENAME",
    ),
    TenantSource(
        "sinapsi",
        "POD_SINAPSI_PATH_TEMPLATE",
        "POD_SINAPSI_PATHS",
        "POD_SINAPSI_S3_BUCKET",
        "POD_SINAPSI_S3_PREFIX",
        "POD_SINAPSI_SNAPSHOT_FILENAME",
    ),
)


def month_start(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.to_period("M").to_timestamp()


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not is_s3_path(uri):
        raise ValueError(f"Path S3 non valido: {uri}")

    bucket_and_key = uri[len("s3://") :]
    bucket, separator, key = bucket_and_key.partition("/")
    if not bucket or not separator or not key:
        raise ValueError(f"URI S3 non valido: {uri}")
    return bucket, key


def assume_role(role_arn: str, session_name: str = POD_AWS_ROLE_SESSION_NAME) -> dict:
    sts_client = boto3.client("sts", region_name=AWS_REGION)
    resp = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    creds = resp["Credentials"]
    return {
        "aws_access_key_id": creds["AccessKeyId"],
        "aws_secret_access_key": creds["SecretAccessKey"],
        "aws_session_token": creds["SessionToken"],
        "region_name": AWS_REGION,
    }


def get_source_account_env_name(source: TenantSource) -> str:
    return f"POD_{source.source_key.upper()}_AWS_ACCOUNT"


def get_source_role_arn_env_name(source: TenantSource) -> str:
    return f"POD_{source.source_key.upper()}_AWS_ROLE_ARN"


def detect_account_name_from_bucket(bucket: str) -> str | None:
    for account_name, account_id in accounts_map.items():
        if account_id in bucket:
            return account_name
    return None


def get_source_account_name(source: TenantSource) -> str | None:
    explicit_account_name = os.environ.get(get_source_account_env_name(source))
    if explicit_account_name:
        return explicit_account_name

    if source.source_key in roles_arn_map:
        return source.source_key

    return detect_account_name_from_bucket(get_source_bucket(source))


def get_source_role_arn(source: TenantSource) -> str | None:
    explicit_role_arn = os.environ.get(get_source_role_arn_env_name(source))
    if explicit_role_arn:
        return explicit_role_arn

    account_name = get_source_account_name(source)
    if account_name is None:
        return None
    if account_name not in roles_arn_map:
        raise ValueError(
            f"Account AWS non configurato per source '{source.source_key}': {account_name}"
        )
    return roles_arn_map[account_name]["infra"]


def get_s3_client(source: TenantSource | None = None):
    if source is None:
        return boto3.client("s3", region_name=AWS_REGION)

    role_arn = get_source_role_arn(source)
    if role_arn is None:
        return boto3.client("s3", region_name=AWS_REGION)

    print(
        f"AssumeRole per source={source.source_key}: role_arn={role_arn}",
        file=sys.stderr,
    )
    return boto3.client("s3", **assume_role(role_arn))


def get_source_bucket(source: TenantSource) -> str:
    return os.environ.get(source.bucket_env, POD_S3_BUCKET)


def get_source_prefix(source: TenantSource) -> str:
    return os.environ.get(source.prefix_env, POD_S3_PREFIX).strip("/")


def get_source_snapshot_filename(source: TenantSource) -> str:
    return os.environ.get(source.filename_env, POD_SNAPSHOT_FILENAME)


def get_default_path_template(source: TenantSource) -> str:
    source_bucket = get_source_bucket(source)
    source_prefix = get_source_prefix(source)
    source_filename = get_source_snapshot_filename(source)
    return f"s3://{source_bucket}/{source_prefix}/year={{year}}/{source_filename}"


def get_path_template(source: TenantSource) -> str:
    template = os.environ.get(
        source.path_template_env,
        os.environ.get(source.legacy_path_env, get_default_path_template(source)),
    )
    if "{year}" not in template:
        raise ValueError(
            "Il template path deve contenere '{year}': "
            f"{source.path_template_env}/{source.legacy_path_env} -> {template}"
        )
    return template


def build_input_paths_by_source(years: list[int]) -> dict[str, list[str]]:
    input_paths: dict[str, list[str]] = {}
    for source in TENANT_SOURCES:
        template = get_path_template(source)
        source_bucket = get_source_bucket(source)
        source_prefix = get_source_prefix(source)
        source_filename = get_source_snapshot_filename(source)
        input_paths[source.source_key] = [
            template.format(
                year=year,
                source=source.source_key,
                source_key=source.source_key,
                bucket=source_bucket,
                prefix=source_prefix,
                filename=source_filename,
            )
            for year in years
        ]
    return input_paths


def maybe_decompress(
    raw_bytes: bytes, path: str, content_encoding: str | None = None
) -> str:
    is_gzip_payload = (
        path.endswith(".gz")
        or (content_encoding or "").lower() == "gzip"
        or raw_bytes.startswith(b"\x1f\x8b")
    )
    if is_gzip_payload:
        raw_bytes = gzip.decompress(raw_bytes)
    return raw_bytes.decode("utf-8")


def read_json_payload(path: str, s3_client, source_key: str | None = None) -> dict:
    bucket: str | None = None
    key: str | None = None
    source_label = source_key or "unknown"
    try:
        if is_s3_path(path):
            bucket, key = parse_s3_uri(path)
            print(
                "Lettura snapshot S3:"
                f" source={source_label}, bucket={bucket}, key={key}, path={path}",
                file=sys.stderr,
            )
            response = s3_client.get_object(Bucket=bucket, Key=key)
            raw_content = maybe_decompress(
                response["Body"].read(),
                path,
                content_encoding=response.get("ContentEncoding"),
            )
        else:
            print(
                f"Lettura snapshot locale: source={source_label}, path={path}",
                file=sys.stderr,
            )
            with open(path, "rb") as file_handle:
                raw_content = maybe_decompress(file_handle.read(), path)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if bucket is not None and key is not None:
            print(
                "Errore S3 leggendo snapshot:"
                f" source={source_label}, bucket={bucket}, key={key}, path={path},"
                f" code={error_code or 'unknown'}",
                file=sys.stderr,
            )
        if error_code in {"NoSuchKey", "404"}:
            raise FileNotFoundError(f"File snapshot non trovato: {path}") from exc
        raise

    payload = json.loads(raw_content)
    if not isinstance(payload, dict):
        raise ValueError(
            f"Il file '{path}' deve contenere un oggetto JSON al top level."
        )
    return payload


def get_partition_year(path: str) -> int | None:
    match = PARTITION_YEAR_PATTERN.search(path)
    if match is None:
        return None
    return int(match.group(1))


def flatten_snapshot_payload(
    payload: dict,
    source_path: str,
    source: TenantSource,
) -> list[dict]:
    partition_year = get_partition_year(source_path)
    records: list[dict] = []

    for raw_date, snapshot_rows in payload.items():
        parsed_date = pd.to_datetime(raw_date, utc=True, errors="coerce")
        if pd.isna(parsed_date):
            raise ValueError(f"Data non valida '{raw_date}' nel file '{source_path}'")

        snapshot_date = parsed_date.tz_convert(None).normalize()
        if partition_year is not None and snapshot_date.year != partition_year:
            raise ValueError(
                f"Data {snapshot_date.date()} non coerente con partizione year={partition_year} in '{source_path}'"
            )

        if not isinstance(snapshot_rows, list):
            raise ValueError(
                f"Il valore associato a '{raw_date}' nel file '{source_path}' deve essere una lista."
            )

        for row in snapshot_rows:
            if not isinstance(row, dict):
                raise ValueError(
                    f"Ogni snapshot in '{source_path}' deve essere un oggetto JSON."
                )

            raw_tenant = row.get("tenant")
            if raw_tenant is None or str(raw_tenant).strip() == "":
                raise ValueError(
                    f"Snapshot senza tenant nel file '{source_path}' alla data '{raw_date}'."
                )

            total_value = row.get("total", row.get("pods"))
            if total_value is None:
                raise ValueError(
                    f"Snapshot senza total/pods nel file '{source_path}' alla data '{raw_date}'."
                )
            onboarded_value = row.get("onboarded")
            if onboarded_value is None:
                raise ValueError(
                    f"Snapshot senza onboarded nel file '{source_path}' alla data '{raw_date}'."
                )

            numeric_total = pd.to_numeric(total_value, errors="coerce")
            if pd.isna(numeric_total):
                raise ValueError(
                    f"Valore total/pods non numerico nel file '{source_path}' alla data '{raw_date}'."
                )
            if numeric_total < 0:
                raise ValueError(
                    f"Valore total/pods negativo nel file '{source_path}' alla data '{raw_date}'."
                )
            if numeric_total % 1 != 0:
                raise ValueError(
                    f"Valore total/pods non intero nel file '{source_path}' alla data '{raw_date}'."
                )
            numeric_onboarded = pd.to_numeric(onboarded_value, errors="coerce")
            if pd.isna(numeric_onboarded):
                raise ValueError(
                    f"Valore onboarded non numerico nel file '{source_path}' alla data '{raw_date}'."
                )
            if numeric_onboarded < 0:
                raise ValueError(
                    f"Valore onboarded negativo nel file '{source_path}' alla data '{raw_date}'."
                )
            if numeric_onboarded % 1 != 0:
                raise ValueError(
                    f"Valore onboarded non intero nel file '{source_path}' alla data '{raw_date}'."
                )
            numeric_onboarded = min(numeric_onboarded, numeric_total)

            records.append(
                {
                    "date": snapshot_date,
                    "tenant": str(raw_tenant).strip(),
                    "source_backend": source.source_key,
                    "pods": int(numeric_total),
                    "onboarded": int(numeric_onboarded),
                }
            )

    return records


def load_snapshot_df(
    input_paths_by_source: dict[str, list[str]],
    skip_missing_leading_paths: bool = False,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    source_by_key = {source.source_key: source for source in TENANT_SOURCES}
    records: list[dict] = []
    loaded_paths_by_source = {
        source_key: [] for source_key in input_paths_by_source.keys()
    }

    for source_key, input_paths in input_paths_by_source.items():
        source = source_by_key[source_key]
        s3_client = get_s3_client(source)
        found_snapshot_for_source = False
        for path in input_paths:
            try:
                payload = read_json_payload(path, s3_client, source_key=source_key)
            except FileNotFoundError:
                if skip_missing_leading_paths and not found_snapshot_for_source:
                    print(
                        "Snapshot non trovato durante bootstrap,"
                        f" salto path iniziale per source={source_key}: {path}",
                        file=sys.stderr,
                    )
                    continue
                raise
            found_snapshot_for_source = True
            loaded_paths_by_source[source_key].append(path)
            records.extend(flatten_snapshot_payload(payload, path, source))

        if skip_missing_leading_paths and not found_snapshot_for_source:
            print(
                "Nessuno snapshot trovato per source="
                f"{source_key} negli anni richiesti; source ignorata.",
                file=sys.stderr,
            )

    if not records:
        return pd.DataFrame(columns=list(REQUIRED_SNAPSHOT_COLUMNS)), loaded_paths_by_source
    return (
        pd.DataFrame.from_records(records, columns=list(REQUIRED_SNAPSHOT_COLUMNS)),
        loaded_paths_by_source,
    )


def normalize_snapshot_df(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df.empty:
        return pd.DataFrame(columns=list(REQUIRED_SNAPSHOT_COLUMNS))

    missing = [
        column
        for column in REQUIRED_SNAPSHOT_COLUMNS
        if column not in snapshot_df.columns
    ]
    if missing:
        raise ValueError(
            "Colonne mancanti negli snapshot pod: "
            + ", ".join(missing)
            + ". Attese: date, tenant, source_backend, pods, onboarded."
        )

    normalized = snapshot_df.loc[:, list(REQUIRED_SNAPSHOT_COLUMNS)].copy()
    normalized["date"] = (
        pd.to_datetime(normalized["date"], utc=True, errors="coerce")
        .dt.tz_convert(None)
        .dt.normalize()
    )
    normalized["tenant"] = normalized["tenant"].astype("string").str.strip()
    normalized["source_backend"] = (
        normalized["source_backend"].astype("string").str.strip()
    )
    normalized["pods"] = pd.to_numeric(normalized["pods"], errors="coerce")
    normalized["onboarded"] = pd.to_numeric(normalized["onboarded"], errors="coerce")
    normalized = normalized.dropna(
        subset=["date", "tenant", "source_backend", "pods", "onboarded"]
    )
    normalized = normalized[
        (normalized["tenant"] != "") & (normalized["source_backend"] != "")
    ]

    if normalized.empty:
        return pd.DataFrame(columns=list(REQUIRED_SNAPSHOT_COLUMNS))

    normalized["pods"] = normalized["pods"].astype("int64")
    normalized["onboarded"] = normalized["onboarded"].astype("int64")
    normalized["onboarded"] = normalized[["onboarded", "pods"]].min(axis=1)
    normalized = normalized.sort_values(
        ["date", "tenant", "source_backend"]
    ).reset_index(drop=True)

    conflicts = normalized.groupby(["date", "tenant"], as_index=False).agg(
        distinct_pods=("pods", "nunique"),
        distinct_onboarded=("onboarded", "nunique"),
    )
    conflicts = conflicts[
        (conflicts["distinct_pods"] > 1) | (conflicts["distinct_onboarded"] > 1)
    ]
    if not conflicts.empty:
        sample = conflicts.iloc[0]
        raise ValueError(
            "Trovati snapshot duplicati con valori diversi per la stessa coppia date/tenant: "
            f"date={sample['date'].date()} tenant={sample['tenant']}"
        )

    duplicates = (
        normalized.groupby(["date", "tenant"], as_index=False)
        .size()
        .rename(columns={"size": "count_rows"})
    )
    duplicates = duplicates[duplicates["count_rows"] > 1]
    if not duplicates.empty:
        sample = duplicates.iloc[0]
        raise ValueError(
            "Trovati più snapshot per la stessa coppia date/tenant: "
            f"date={sample['date'].date()} tenant={sample['tenant']}. "
            "Controlla i file annuali configurati."
        )

    return normalized.reset_index(drop=True)


def build_daily_rows(
    snapshot_df: pd.DataFrame,
    updated_at: datetime,
    seed_totals: dict[str, int] | None = None,
    seed_onboarded: dict[str, int] | None = None,
) -> list[tuple]:
    if snapshot_df.empty:
        return []

    seed_totals = seed_totals or {}
    seed_onboarded = seed_onboarded or {}
    daily_df = snapshot_df.sort_values(["tenant", "date"]).copy()
    previous_total_values = daily_df.groupby("tenant")["pods"].shift(1)
    previous_onboarded_values = daily_df.groupby("tenant")["onboarded"].shift(1)

    first_rows = daily_df.groupby("tenant").cumcount().eq(0)
    previous_total_values.loc[first_rows] = (
        daily_df.loc[first_rows, "tenant"].map(seed_totals).fillna(0)
    )
    previous_onboarded_values.loc[first_rows] = (
        daily_df.loc[first_rows, "tenant"].map(seed_onboarded).fillna(0)
    )
    daily_df["daily_delta"] = daily_df["pods"] - previous_total_values.fillna(0)
    daily_df["daily_onboarded_delta"] = daily_df[
        "onboarded"
    ] - previous_onboarded_values.fillna(0)

    return [
        (
            row.date.date(),
            row.tenant,
            row.source_backend,
            int(row.daily_delta),
            int(row.pods),
            int(row.daily_onboarded_delta),
            int(row.onboarded),
            updated_at,
        )
        for row in daily_df.itertuples(index=False)
    ]


def build_monthly_rows(
    snapshot_df: pd.DataFrame,
    updated_at: datetime,
    seed_totals: dict[str, int] | None = None,
    seed_onboarded: dict[str, int] | None = None,
) -> list[tuple]:
    if snapshot_df.empty:
        return []

    seed_totals = seed_totals or {}
    seed_onboarded = seed_onboarded or {}
    monthly_df = snapshot_df.sort_values(["tenant", "date"]).copy()
    monthly_df["month_start"] = monthly_df["date"].apply(month_start)
    monthly_df = (
        monthly_df.groupby(["tenant", "month_start"], group_keys=False)
        .tail(1)
        .sort_values(["tenant", "month_start"])
        .reset_index(drop=True)
    )

    previous_total_values = monthly_df.groupby("tenant")["pods"].shift(1)
    previous_onboarded_values = monthly_df.groupby("tenant")["onboarded"].shift(1)
    first_rows = monthly_df.groupby("tenant").cumcount().eq(0)
    previous_total_values.loc[first_rows] = (
        monthly_df.loc[first_rows, "tenant"].map(seed_totals).fillna(0)
    )
    previous_onboarded_values.loc[first_rows] = (
        monthly_df.loc[first_rows, "tenant"].map(seed_onboarded).fillna(0)
    )
    monthly_df["monthly_delta"] = monthly_df["pods"] - previous_total_values.fillna(0)
    monthly_df["monthly_onboarded_delta"] = monthly_df[
        "onboarded"
    ] - previous_onboarded_values.fillna(0)

    return [
        (
            row.month_start.date(),
            row.tenant,
            row.source_backend,
            int(row.monthly_delta),
            int(row.pods),
            int(row.monthly_onboarded_delta),
            int(row.onboarded),
            updated_at,
        )
        for row in monthly_df.itertuples(index=False)
    ]


def get_latest_loaded_date(duckdb) -> pd.Timestamp | None:
    latest_date = duckdb.get_latest_date(POD_DAILY_TABLE_NAME)
    if latest_date is not None:
        return pd.Timestamp(latest_date).normalize()

    latest_month = duckdb.get_latest_month(POD_MONTHLY_TABLE_NAME)
    if latest_month is not None:
        return month_start(latest_month)
    return None


def get_target_years(duckdb, current_year: int) -> tuple[list[int], bool]:
    latest_loaded_date = get_latest_loaded_date(duckdb)
    if latest_loaded_date is None:
        return list(range(POD_BOOTSTRAP_START_YEAR, current_year + 1)), True
    return [current_year], False


def get_previous_day_values(
    duckdb,
    table_name: str,
    current_year_start: pd.Timestamp,
    value_column: str,
) -> dict[str, int]:
    previous_day = (current_year_start - pd.Timedelta(days=1)).date()
    df = duckdb.execute(
        f"""
        SELECT tenant, {value_column} AS metric_value
        FROM {table_name}
        WHERE date = ?
        """,
        [previous_day],
    )
    if df.empty:
        return {}
    return {
        tenant: int(value) for tenant, value in zip(df["tenant"], df["metric_value"])
    }


def get_previous_month_values(
    duckdb,
    table_name: str,
    current_year_month_start: pd.Timestamp,
    value_column: str,
) -> dict[str, int]:
    previous_month = (current_year_month_start - pd.DateOffset(months=1)).date()
    df = duckdb.execute(
        f"""
        SELECT tenant, {value_column} AS metric_value
        FROM {table_name}
        WHERE month_start = ?
        """,
        [previous_month],
    )
    if df.empty:
        return {}
    return {
        tenant: int(value) for tenant, value in zip(df["tenant"], df["metric_value"])
    }


def replace_all_rows(duckdb, table_name: str, rows: list[tuple]) -> None:
    duckdb.execute(f"DELETE FROM {table_name}")
    if rows:
        duckdb.insert_many(table_name, rows)


def replace_rows_in_range(
    duckdb,
    table_name: str,
    date_column: str,
    start_value,
    end_value,
    rows: list[tuple],
) -> None:
    duckdb.execute(
        f"""
        DELETE FROM {table_name}
        WHERE {date_column} >= ? AND {date_column} <= ?
        """,
        [start_value, end_value],
    )
    if rows:
        duckdb.insert_many(table_name, rows)


def summarize_loaded_sources(input_paths_by_source: dict[str, list[str]]) -> str:
    parts = []
    for source in TENANT_SOURCES:
        parts.append(
            f"{source.source_key}={len(input_paths_by_source[source.source_key])}"
        )
    return ", ".join(parts)


def main() -> None:
    if sys.argv[1:]:
        raise ValueError(
            "Il pod collector non accetta argomenti posizionali. "
            "Configura eventualmente i template via env."
        )

    duckdb = get_duckdb_client(DUCKDB_DATABASE)
    duckdb.create_pod_trend_table(POD_MONTHLY_TABLE_NAME)
    duckdb.create_pod_daily_trend_table(POD_DAILY_TABLE_NAME)

    current_year = datetime.now(UTC).year
    target_years, is_bootstrap = get_target_years(duckdb, current_year)
    input_paths_by_source = build_input_paths_by_source(target_years)

    raw_snapshot_df, loaded_paths_by_source = load_snapshot_df(
        input_paths_by_source,
        skip_missing_leading_paths=is_bootstrap,
    )
    snapshot_df = normalize_snapshot_df(raw_snapshot_df)
    if snapshot_df.empty:
        print("Nessuno snapshot pod valido disponibile.")
        return

    run_ts = datetime.now(UTC)
    current_year_start = pd.Timestamp(year=current_year, month=1, day=1)
    current_year_end = pd.Timestamp(year=current_year, month=12, day=31)
    current_year_month_start = month_start(current_year_start)
    current_year_month_end = month_start(current_year_end)

    if is_bootstrap:
        daily_rows = build_daily_rows(snapshot_df, run_ts)
        monthly_rows = build_monthly_rows(snapshot_df, run_ts)
        replace_all_rows(duckdb, POD_DAILY_TABLE_NAME, daily_rows)
        replace_all_rows(duckdb, POD_MONTHLY_TABLE_NAME, monthly_rows)
        load_mode = "bootstrap"
    else:
        daily_seed_totals = get_previous_day_values(
            duckdb, POD_DAILY_TABLE_NAME, current_year_start, "total_pods"
        )
        daily_seed_onboarded = get_previous_day_values(
            duckdb, POD_DAILY_TABLE_NAME, current_year_start, "onboarded_pods"
        )
        monthly_seed_totals = get_previous_month_values(
            duckdb, POD_MONTHLY_TABLE_NAME, current_year_month_start, "total_pods"
        )
        monthly_seed_onboarded = get_previous_month_values(
            duckdb,
            POD_MONTHLY_TABLE_NAME,
            current_year_month_start,
            "onboarded_pods",
        )
        daily_rows = build_daily_rows(
            snapshot_df,
            run_ts,
            seed_totals=daily_seed_totals,
            seed_onboarded=daily_seed_onboarded,
        )
        monthly_rows = build_monthly_rows(
            snapshot_df,
            run_ts,
            seed_totals=monthly_seed_totals,
            seed_onboarded=monthly_seed_onboarded,
        )
        replace_rows_in_range(
            duckdb,
            POD_DAILY_TABLE_NAME,
            "date",
            current_year_start.date(),
            current_year_end.date(),
            daily_rows,
        )
        replace_rows_in_range(
            duckdb,
            POD_MONTHLY_TABLE_NAME,
            "month_start",
            current_year_month_start.date(),
            current_year_month_end.date(),
            monthly_rows,
        )
        load_mode = "refresh"

    min_date = snapshot_df["date"].min().date()
    max_date = snapshot_df["date"].max().date()
    monthly_summary = duckdb.execute(
        f"""
        SELECT
            month_start,
            tenant,
            source_backend,
            monthly_delta,
            total_pods,
            monthly_onboarded_delta,
            onboarded_pods
        FROM {POD_MONTHLY_TABLE_NAME}
        ORDER BY month_start DESC, tenant
        LIMIT 20
        """
    )
    daily_summary = duckdb.execute(
        f"""
        SELECT
            date,
            tenant,
            source_backend,
            daily_delta,
            total_pods,
            daily_onboarded_delta,
            onboarded_pods
        FROM {POD_DAILY_TABLE_NAME}
        ORDER BY date DESC, tenant
        LIMIT 20
        """
    )

    print(
        "POD collector completato:"
        f" modalita={load_mode}, anni={target_years[0]}->{target_years[-1]},"
        f" file per sorgente [{summarize_loaded_sources(loaded_paths_by_source)}],"
        f" {len(snapshot_df)} snapshot tenant/giorno,"
        f" {len(monthly_rows)} righe aggiornate su '{POD_MONTHLY_TABLE_NAME}',"
        f" {len(daily_rows)} righe aggiornate su '{POD_DAILY_TABLE_NAME}'."
        f" Intervallo snapshot letto: {min_date} -> {max_date}."
    )
    print(monthly_summary)
    print(daily_summary)


if __name__ == "__main__":
    main()
