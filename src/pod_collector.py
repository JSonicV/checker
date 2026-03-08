import os
from datetime import UTC, datetime

import pandas as pd
from dotenv import load_dotenv

from duckdb_client import get_duckdb_client


load_dotenv()

DUCKDB_DATABASE = os.environ.get("DUCKDB_DATABASE", "database.duckdb")
POD_TABLE_NAME = os.environ.get("DUCKDB_POD_TABLE", "pod_monthly_trend")
LOOKBACK_MONTHS = 12


def month_start(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.to_period("M").to_timestamp()


def build_mock_postgres_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    companies = pd.DataFrame(
        [
            {"company_id": 1, "tenant": "digiwatt"},
            {"company_id": 2, "tenant": "fastweb_prod"},
            {"company_id": 3, "tenant": "sinapsi_prod"},
        ]
    )

    users = pd.DataFrame(
        [
            {"user_id": 101, "company_id": 1},
            {"user_id": 102, "company_id": 1},
            {"user_id": 201, "company_id": 2},
            {"user_id": 202, "company_id": 2},
            {"user_id": 301, "company_id": 3},
        ]
    )

    houses = pd.DataFrame(
        [
            {"house_id": 1001, "user_id": 101},
            {"house_id": 1002, "user_id": 101},
            {"house_id": 1003, "user_id": 102},
            {"house_id": 2001, "user_id": 201},
            {"house_id": 2002, "user_id": 202},
            {"house_id": 3001, "user_id": 301},
            {"house_id": 3002, "user_id": 301},
        ]
    )

    sources = pd.DataFrame(
        [
            {"source_id": "pg-1", "house_id": 1001, "created_at": "2025-03-11T10:00:00Z"},
            {"source_id": "pg-2", "house_id": 1002, "created_at": "2025-05-20T08:00:00Z"},
            {"source_id": "pg-3", "house_id": 1003, "created_at": "2025-08-05T12:15:00Z"},
            {"source_id": "pg-4", "house_id": 2001, "created_at": "2025-04-02T18:05:00Z"},
            {"source_id": "pg-5", "house_id": 2002, "created_at": "2025-09-22T09:30:00Z"},
            {"source_id": "pg-6", "house_id": 3001, "created_at": "2025-07-14T14:20:00Z"},
            {"source_id": "pg-7", "house_id": 3002, "created_at": "2025-12-03T07:45:00Z"},
        ]
    )

    return companies, users, houses, sources


def build_mock_postgres_events() -> pd.DataFrame:
    companies, users, houses, sources = build_mock_postgres_tables()
    sources["created_at"] = pd.to_datetime(sources["created_at"], utc=True)

    joined = (
        companies.merge(users, on="company_id", how="inner")
        .merge(houses, on="user_id", how="inner")
        .merge(sources, on="house_id", how="inner")
    )

    return pd.DataFrame(
        {
            "tenant": joined["tenant"],
            "created_at": joined["created_at"],
            "source_backend": "postgres",
            "delta": 1,
        }
    )


def build_mock_s3_events() -> pd.DataFrame:
    # Mock parquet rows. "delta" permette di simulare sia acquisizione (+1) sia perdita (-1).
    events = pd.DataFrame(
        [
            {"tenant": "fastweb_dev", "created_at": "2025-04-10T11:00:00Z", "delta": 1},
            {"tenant": "fastweb_dev", "created_at": "2025-08-13T10:25:00Z", "delta": 1},
            {"tenant": "fastweb_staging", "created_at": "2025-06-01T16:30:00Z", "delta": 1},
            {"tenant": "fastweb_staging", "created_at": "2025-11-18T09:45:00Z", "delta": 1},
            {"tenant": "fastweb_staging", "created_at": "2026-01-07T13:15:00Z", "delta": -1},
        ]
    )
    events["created_at"] = pd.to_datetime(events["created_at"], utc=True)
    events["source_backend"] = "s3_parquet"
    return events


def build_mock_events() -> pd.DataFrame:
    events = pd.concat([build_mock_postgres_events(), build_mock_s3_events()], ignore_index=True)
    events["month_start"] = events["created_at"].apply(month_start)
    return events


def get_start_month(duckdb, table_name: str, current_month: pd.Timestamp) -> pd.Timestamp:
    latest_month = duckdb.get_latest_month(table_name)
    lookback_start = current_month - pd.DateOffset(months=LOOKBACK_MONTHS - 1)
    if latest_month is None:
        return lookback_start

    latest_month = month_start(latest_month)
    return max(latest_month, lookback_start)


def get_previous_month_totals(
    duckdb, table_name: str, month_value: pd.Timestamp
) -> dict[str, int]:
    prev_month = (month_value - pd.DateOffset(months=1)).date()
    df = duckdb.execute(
        f"""
        SELECT tenant, total_pods
        FROM {table_name}
        WHERE month_start = ?
        """,
        [prev_month],
    )
    if df.empty:
        return {}
    return dict(zip(df["tenant"], df["total_pods"]))


def compute_rows(
    events: pd.DataFrame,
    duckdb,
    table_name: str,
    start_month: pd.Timestamp,
    current_month: pd.Timestamp,
) -> list[tuple]:
    filtered_events = events[events["month_start"] <= current_month].copy()
    tenants = sorted(filtered_events["tenant"].unique().tolist())
    if not tenants:
        return []

    backend_by_tenant = (
        filtered_events.sort_values("created_at")
        .drop_duplicates(subset=["tenant"])
        .set_index("tenant")["source_backend"]
        .to_dict()
    )

    monthly_delta = (
        filtered_events.groupby(["tenant", "month_start"], as_index=False)["delta"].sum()
    )
    delta_lookup = {
        (row.tenant, row.month_start): int(row.delta)
        for row in monthly_delta.itertuples(index=False)
    }

    previous_totals = get_previous_month_totals(duckdb, table_name, start_month)
    base_totals_df = (
        filtered_events[filtered_events["month_start"] < start_month]
        .groupby("tenant", as_index=False)["delta"]
        .sum()
    )
    base_totals = {
        row.tenant: int(row.delta)
        for row in base_totals_df.itertuples(index=False)
    }

    running_totals = {
        tenant: int(previous_totals.get(tenant, base_totals.get(tenant, 0)))
        for tenant in tenants
    }

    rows = []
    updated_at = datetime.now(UTC)
    month_range = pd.date_range(start=start_month, end=current_month, freq="MS")

    for month_value in month_range:
        for tenant in tenants:
            delta = int(delta_lookup.get((tenant, month_value), 0))
            running_totals[tenant] = running_totals[tenant] + delta

            rows.append(
                (
                    month_value.date(),
                    tenant,
                    backend_by_tenant[tenant],
                    delta,
                    int(running_totals[tenant]),
                    updated_at,
                )
            )

    return rows


def main() -> None:
    duckdb = get_duckdb_client(DUCKDB_DATABASE)
    duckdb.create_pod_trend_table(POD_TABLE_NAME)

    today = datetime.now(UTC).date()
    current_month = month_start(today)
    start_month = get_start_month(duckdb, POD_TABLE_NAME, current_month)

    events = build_mock_events()
    rows = compute_rows(events, duckdb, POD_TABLE_NAME, start_month, current_month)
    if not rows:
        print("Nessun evento pod disponibile.")
        return

    duckdb.insert_many(POD_TABLE_NAME, rows)

    summary = duckdb.execute(
        f"""
        SELECT
            month_start,
            tenant,
            source_backend,
            monthly_delta,
            total_pods
        FROM {POD_TABLE_NAME}
        WHERE month_start >= ?
        ORDER BY month_start, tenant
        """,
        [start_month.date()],
    )

    print(
        f"POD collector completato: {len(rows)} righe upsert su '{POD_TABLE_NAME}' "
        f"({start_month.date()} -> {current_month.date()})."
    )
    print(summary)


if __name__ == "__main__":
    main()
