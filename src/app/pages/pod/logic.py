from dataclasses import dataclass

import pandas as pd
import streamlit as st

from duckdb_client import get_duckdb_client
try:
    from app.page_shared import safe_div
except ModuleNotFoundError:
    from page_shared import safe_div


@dataclass
class PodTableState:
    months_desc: list[pd.Timestamp]
    tenants: list[str]
    matrix: pd.DataFrame
    total_series: pd.Series
    total_delta: pd.Series
    total_pct: pd.Series
    tenant_delta: dict[str, pd.Series]
    tenant_pct: dict[str, pd.Series]


@st.cache_data(show_spinner=False)
def load_data(db_name: str, table_name: str, months: int = 12):
    client = get_duckdb_client(db_name)
    return client.get_pod_monthly_trend(table_name, months=months)


def build_table_state(pod_df: pd.DataFrame, months: int = 12) -> PodTableState | None:
    pod_df = pod_df.copy()
    pod_df["month_start"] = pd.to_datetime(
        pod_df["month_start"], errors="coerce"
    ).dt.to_period("M").dt.to_timestamp()
    pod_df["total_pods"] = pd.to_numeric(pod_df["total_pods"], errors="coerce")
    pod_df = pod_df.dropna(subset=["month_start", "tenant", "total_pods"])
    if pod_df.empty:
        return None

    tenant_month = (
        pod_df.groupby(["month_start", "tenant"], as_index=False)["total_pods"]
        .max()
        .sort_values("month_start")
    )
    if tenant_month.empty:
        return None

    latest_month = tenant_month["month_start"].max()
    months_asc = pd.date_range(end=latest_month, periods=months, freq="MS")
    months_desc = list(months_asc[::-1])
    tenants = sorted(tenant_month["tenant"].unique().tolist())

    matrix = tenant_month.pivot(
        index="month_start",
        columns="tenant",
        values="total_pods",
    ).reindex(index=months_asc, columns=tenants)
    matrix = matrix.ffill().fillna(0)

    total_series = matrix.sum(axis=1)
    total_prev = total_series.shift(1)
    total_delta = total_series - total_prev
    total_pct = pd.Series(
        [safe_div(curr - prev, prev) for curr, prev in zip(total_series, total_prev)],
        index=total_series.index,
    )

    tenant_delta = {}
    tenant_pct = {}
    for tenant in tenants:
        series = matrix[tenant]
        prev = series.shift(1)
        tenant_delta[tenant] = series - prev
        tenant_pct[tenant] = pd.Series(
            [
                safe_div(curr - prev_value, prev_value)
                for curr, prev_value in zip(series, prev)
            ],
            index=series.index,
        )

    return PodTableState(
        months_desc=months_desc,
        tenants=tenants,
        matrix=matrix,
        total_series=total_series,
        total_delta=total_delta,
        total_pct=total_pct,
        tenant_delta=tenant_delta,
        tenant_pct=tenant_pct,
    )
