from dataclasses import dataclass

import pandas as pd
import streamlit as st

from duckdb_client import get_duckdb_client

try:
    from app.page_shared import safe_div
except ModuleNotFoundError:
    from page_shared import safe_div


POD_METRIC_COLUMNS = {
    "total": "total_pods",
    "onboarded": "onboarded_pods",
}


@dataclass
class PodWindowRow:
    label: str
    total_value: float
    total_reference_value: float
    total_delta: float
    total_pct: float | object
    tenant_value: dict[str, float]
    tenant_reference_value: dict[str, float]
    tenant_delta: dict[str, float]
    tenant_pct: dict[str, float | object]


@dataclass
class PodTableState:
    metric: str
    months_desc: list[pd.Timestamp]
    tenants: list[str]
    matrix: pd.DataFrame
    reference_matrix: pd.DataFrame
    total_series: pd.Series
    total_reference_series: pd.Series
    total_delta: pd.Series
    total_pct: pd.Series
    tenant_delta: dict[str, pd.Series]
    tenant_pct: dict[str, pd.Series]
    last_30_days: PodWindowRow | None


@st.cache_data(show_spinner=False)
def load_monthly_data(
    db_name: str, table_name: str, months: int = 12, _db_cache_buster: int = 0
):
    client = get_duckdb_client(db_name)
    try:
        return client.get_pod_monthly_trend(table_name, months=months)
    finally:
        client.close()


@st.cache_data(show_spinner=False)
def load_daily_data(
    db_name: str, table_name: str, days: int = 60, _db_cache_buster: int = 0
):
    client = get_duckdb_client(db_name)
    try:
        return client.get_pod_daily_trend(table_name, days=days)
    finally:
        client.close()


def _resolve_metric_column(metric: str) -> str:
    try:
        return POD_METRIC_COLUMNS[metric]
    except KeyError as exc:
        raise ValueError(f"Metrica pod non supportata: {metric}") from exc


def _normalize_monthly_df(pod_monthly_df: pd.DataFrame) -> pd.DataFrame:
    pod_monthly_df = pod_monthly_df.copy()
    pod_monthly_df["month_start"] = pd.to_datetime(
        pod_monthly_df["month_start"], errors="coerce"
    ).dt.to_period("M").dt.to_timestamp()
    for column in ("total_pods", "onboarded_pods"):
        if column not in pod_monthly_df.columns:
            pod_monthly_df[column] = pd.NA
        pod_monthly_df[column] = pd.to_numeric(pod_monthly_df[column], errors="coerce")
    return pod_monthly_df.dropna(subset=["month_start", "tenant", "total_pods"])


def _normalize_daily_df(pod_daily_df: pd.DataFrame) -> pd.DataFrame:
    if pod_daily_df is None or pod_daily_df.empty:
        return pd.DataFrame(
            columns=["date", "tenant", "total_pods", "onboarded_pods"]
        )

    pod_daily_df = pod_daily_df.copy()
    pod_daily_df["date"] = pd.to_datetime(
        pod_daily_df["date"], errors="coerce"
    ).dt.normalize()
    for column in ("total_pods", "onboarded_pods"):
        if column not in pod_daily_df.columns:
            pod_daily_df[column] = pd.NA
        pod_daily_df[column] = pd.to_numeric(pod_daily_df[column], errors="coerce")
    return pod_daily_df.dropna(subset=["date", "tenant", "total_pods"])


def _build_last_days_row(
    pod_daily_df: pd.DataFrame,
    tenants: list[str],
    value_column: str,
    window_days: int = 30,
    label: str = "Ultimi 30 giorni",
) -> PodWindowRow | None:
    if pod_daily_df.empty:
        return None

    columns = ["total_pods"]
    if value_column != "total_pods":
        columns.append(value_column)

    tenant_date = (
        pod_daily_df.groupby(["date", "tenant"], as_index=False)[columns]
        .max()
        .sort_values("date")
    )
    if tenant_date.empty:
        return None

    daily_matrix = tenant_date.pivot(
        index="date",
        columns="tenant",
        values=value_column,
    ).sort_index()
    reference_matrix = tenant_date.pivot(
        index="date",
        columns="tenant",
        values="total_pods",
    ).sort_index()

    daily_matrix = daily_matrix.reindex(columns=tenants).ffill().fillna(0)
    reference_matrix = reference_matrix.reindex(columns=tenants).ffill().fillna(0)
    if daily_matrix.empty:
        return None

    latest_date = daily_matrix.index.max()
    baseline_date = latest_date - pd.Timedelta(days=window_days)

    current_values = daily_matrix.loc[latest_date]
    current_reference_values = reference_matrix.loc[latest_date]
    baseline_slice = daily_matrix[daily_matrix.index <= baseline_date]
    if baseline_slice.empty:
        baseline_values = pd.Series(0, index=tenants, dtype="float64")
    else:
        baseline_values = baseline_slice.iloc[-1]

    delta_values = current_values - baseline_values
    pct_values = pd.Series(
        [
            safe_div(delta_values.at[tenant], baseline_values.at[tenant])
            for tenant in tenants
        ],
        index=tenants,
    )

    total_value = float(current_values.sum())
    total_reference_value = float(current_reference_values.sum())
    total_baseline = float(baseline_values.sum())
    total_delta = total_value - total_baseline
    total_pct = safe_div(total_delta, total_baseline)

    return PodWindowRow(
        label=label,
        total_value=total_value,
        total_reference_value=total_reference_value,
        total_delta=total_delta,
        total_pct=total_pct,
        tenant_value={tenant: float(current_values.at[tenant]) for tenant in tenants},
        tenant_reference_value={
            tenant: float(current_reference_values.at[tenant]) for tenant in tenants
        },
        tenant_delta={tenant: float(delta_values.at[tenant]) for tenant in tenants},
        tenant_pct={tenant: pct_values.at[tenant] for tenant in tenants},
    )


def build_table_state(
    pod_monthly_df: pd.DataFrame,
    pod_daily_df: pd.DataFrame | None = None,
    months: int = 12,
    metric: str = "total",
) -> PodTableState | None:
    value_column = _resolve_metric_column(metric)
    pod_monthly_df = _normalize_monthly_df(pod_monthly_df)
    if pod_monthly_df.empty:
        return None
    pod_monthly_df = pod_monthly_df.dropna(subset=[value_column])
    if pod_monthly_df.empty:
        return None

    pod_daily_df = _normalize_daily_df(pod_daily_df)
    pod_daily_df = pod_daily_df.dropna(subset=[value_column])

    columns = ["total_pods"]
    if value_column != "total_pods":
        columns.append(value_column)

    tenant_month = (
        pod_monthly_df.groupby(["month_start", "tenant"], as_index=False)[columns]
        .max()
        .sort_values("month_start")
    )
    if tenant_month.empty:
        return None

    latest_month = tenant_month["month_start"].max()
    months_asc = pd.date_range(end=latest_month, periods=months, freq="MS")
    months_desc = list(months_asc[::-1])
    monthly_tenants = tenant_month["tenant"].unique().tolist()
    daily_tenants = (
        pod_daily_df["tenant"].dropna().unique().tolist()
        if not pod_daily_df.empty
        else []
    )
    tenants = sorted(set(monthly_tenants).union(daily_tenants))

    matrix = tenant_month.pivot(
        index="month_start",
        columns="tenant",
        values=value_column,
    ).reindex(index=months_asc, columns=tenants)
    matrix = matrix.ffill()

    reference_matrix = tenant_month.pivot(
        index="month_start",
        columns="tenant",
        values="total_pods",
    ).reindex(index=months_asc, columns=tenants)
    reference_matrix = reference_matrix.ffill()

    total_series = matrix.sum(axis=1)
    total_reference_series = reference_matrix.sum(axis=1)
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

    ordering_last_30_days = _build_last_days_row(
        pod_daily_df,
        tenants,
        value_column="total_pods",
        window_days=30,
    )
    current_month_values = reference_matrix.loc[latest_month].to_dict()
    last_30_day_deltas = (
        ordering_last_30_days.tenant_delta
        if ordering_last_30_days is not None
        else {}
    )
    tenants = sorted(
        tenants,
        key=lambda tenant: (
            -(
                float(current_month_values.get(tenant))
                if pd.notna(current_month_values.get(tenant))
                else float("-inf")
            ),
            -float(last_30_day_deltas.get(tenant, 0.0)),
            tenant.casefold(),
        ),
    )
    matrix = matrix.reindex(columns=tenants)
    reference_matrix = reference_matrix.reindex(columns=tenants)

    last_30_days = _build_last_days_row(
        pod_daily_df,
        tenants,
        value_column=value_column,
        window_days=30,
    )

    return PodTableState(
        metric=metric,
        months_desc=months_desc,
        tenants=tenants,
        matrix=matrix,
        reference_matrix=reference_matrix,
        total_series=total_series,
        total_reference_series=total_reference_series,
        total_delta=total_delta,
        total_pct=total_pct,
        tenant_delta=tenant_delta,
        tenant_pct=tenant_pct,
        last_30_days=last_30_days,
    )
