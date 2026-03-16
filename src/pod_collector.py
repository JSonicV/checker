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
PARTITION_YEAR_PATTERN = re.compile(r"(?:^|/)year=(\d{4})(?:/|$)")
REQUIRED_SNAPSHOT_COLUMNS = ("date", "tenant", "source_backend", "pods")


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
    # TenantSource(
    #     "fastweb",
    #     "POD_FASTWEB_PATH_TEMPLATE",
    #     "POD_FASTWEB_PATHS",
    #     "POD_FASTWEB_S3_BUCKET",
    #     "POD_FASTWEB_S3_PREFIX",
    #     "POD_FASTWEB_SNAPSHOT_FILENAME",
    # ),
    # TenantSource(
    #     "sinapsi",
    #     "POD_SINAPSI_PATH_TEMPLATE",
    #     "POD_SINAPSI_PATHS",
    #     "POD_SINAPSI_S3_BUCKET",
    #     "POD_SINAPSI_S3_PREFIX",
    #     "POD_SINAPSI_SNAPSHOT_FILENAME",
    # ),
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


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


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
    return (
        f"s3://{source_bucket}/{source_prefix}/year={{year}}/{source_filename}"
    )


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


def read_json_payload(path: str, s3_client) -> dict:
    try:
        if is_s3_path(path):
            bucket, key = parse_s3_uri(path)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            raw_content = maybe_decompress(
                response["Body"].read(),
                path,
                content_encoding=response.get("ContentEncoding"),
            )
        else:
            with open(path, "rb") as file_handle:
                raw_content = maybe_decompress(file_handle.read(), path)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
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

            records.append(
                {
                    "date": snapshot_date,
                    "tenant": str(raw_tenant).strip(),
                    "source_backend": source.source_key,
                    "pods": int(numeric_total),
                }
            )

    return records


def load_snapshot_df(input_paths_by_source: dict[str, list[str]]) -> pd.DataFrame:
    s3_client = get_s3_client()
    source_by_key = {source.source_key: source for source in TENANT_SOURCES}
    records: list[dict] = []

    for source_key, input_paths in input_paths_by_source.items():
        source = source_by_key[source_key]
        for path in input_paths:
            payload = read_json_payload(path, s3_client)
            records.extend(flatten_snapshot_payload(payload, path, source))

    if not records:
        return pd.DataFrame(columns=list(REQUIRED_SNAPSHOT_COLUMNS))
    return pd.DataFrame.from_records(records, columns=list(REQUIRED_SNAPSHOT_COLUMNS))


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
            + ". Attese: date, tenant, source_backend, pods."
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
    normalized = normalized.dropna(subset=["date", "tenant", "source_backend", "pods"])
    normalized = normalized[
        (normalized["tenant"] != "") & (normalized["source_backend"] != "")
    ]

    if normalized.empty:
        return pd.DataFrame(columns=list(REQUIRED_SNAPSHOT_COLUMNS))

    normalized["pods"] = normalized["pods"].astype("int64")
    normalized = normalized.sort_values(
        ["date", "tenant", "source_backend"]
    ).reset_index(drop=True)

    conflicts = (
        normalized.groupby(["date", "tenant"], as_index=False)["pods"]
        .nunique()
        .rename(columns={"pods": "distinct_pods"})
    )
    conflicts = conflicts[conflicts["distinct_pods"] > 1]
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
) -> list[tuple]:
    if snapshot_df.empty:
        return []

    seed_totals = seed_totals or {}
    daily_df = snapshot_df.sort_values(["tenant", "date"]).copy()
    previous_values = daily_df.groupby("tenant")["pods"].shift(1)

    first_rows = daily_df.groupby("tenant").cumcount().eq(0)
    previous_values.loc[first_rows] = (
        daily_df.loc[first_rows, "tenant"].map(seed_totals).fillna(0)
    )
    daily_df["daily_delta"] = daily_df["pods"] - previous_values.fillna(0)

    return [
        (
            row.date.date(),
            row.tenant,
            row.source_backend,
            int(row.daily_delta),
            int(row.pods),
            updated_at,
        )
        for row in daily_df.itertuples(index=False)
    ]


def build_monthly_rows(
    snapshot_df: pd.DataFrame,
    updated_at: datetime,
    seed_totals: dict[str, int] | None = None,
) -> list[tuple]:
    if snapshot_df.empty:
        return []

    seed_totals = seed_totals or {}
    monthly_df = snapshot_df.sort_values(["tenant", "date"]).copy()
    monthly_df["month_start"] = monthly_df["date"].apply(month_start)
    monthly_df = (
        monthly_df.groupby(["tenant", "month_start"], group_keys=False)
        .tail(1)
        .sort_values(["tenant", "month_start"])
        .reset_index(drop=True)
    )

    previous_values = monthly_df.groupby("tenant")["pods"].shift(1)
    first_rows = monthly_df.groupby("tenant").cumcount().eq(0)
    previous_values.loc[first_rows] = (
        monthly_df.loc[first_rows, "tenant"].map(seed_totals).fillna(0)
    )
    monthly_df["monthly_delta"] = monthly_df["pods"] - previous_values.fillna(0)

    return [
        (
            row.month_start.date(),
            row.tenant,
            row.source_backend,
            int(row.monthly_delta),
            int(row.pods),
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


def get_previous_day_totals(
    duckdb,
    table_name: str,
    current_year_start: pd.Timestamp,
) -> dict[str, int]:
    previous_day = (current_year_start - pd.Timedelta(days=1)).date()
    df = duckdb.execute(
        f"""
        SELECT tenant, total_pods
        FROM {table_name}
        WHERE date = ?
        """,
        [previous_day],
    )
    if df.empty:
        return {}
    return dict(zip(df["tenant"], df["total_pods"]))


def get_previous_month_totals(
    duckdb,
    table_name: str,
    current_year_month_start: pd.Timestamp,
) -> dict[str, int]:
    previous_month = (current_year_month_start - pd.DateOffset(months=1)).date()
    df = duckdb.execute(
        f"""
        SELECT tenant, total_pods
        FROM {table_name}
        WHERE month_start = ?
        """,
        [previous_month],
    )
    if df.empty:
        return {}
    return dict(zip(df["tenant"], df["total_pods"]))


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

    raw_snapshot_df = load_snapshot_df(input_paths_by_source)
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
        daily_seed_totals = get_previous_day_totals(
            duckdb, POD_DAILY_TABLE_NAME, current_year_start
        )
        monthly_seed_totals = get_previous_month_totals(
            duckdb, POD_MONTHLY_TABLE_NAME, current_year_month_start
        )
        daily_rows = build_daily_rows(
            snapshot_df, run_ts, seed_totals=daily_seed_totals
        )
        monthly_rows = build_monthly_rows(
            snapshot_df, run_ts, seed_totals=monthly_seed_totals
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
            total_pods
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
            total_pods
        FROM {POD_DAILY_TABLE_NAME}
        ORDER BY date DESC, tenant
        LIMIT 20
        """
    )

    print(
        "POD collector completato:"
        f" modalita={load_mode}, anni={target_years[0]}->{target_years[-1]},"
        f" file per sorgente [{summarize_loaded_sources(input_paths_by_source)}],"
        f" {len(snapshot_df)} snapshot tenant/giorno,"
        f" {len(monthly_rows)} righe aggiornate su '{POD_MONTHLY_TABLE_NAME}',"
        f" {len(daily_rows)} righe aggiornate su '{POD_DAILY_TABLE_NAME}'."
        f" Intervallo snapshot letto: {min_date} -> {max_date}."
    )
    print(monthly_summary)
    print(daily_summary)


if __name__ == "__main__":
    main()
