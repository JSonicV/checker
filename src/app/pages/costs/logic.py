import re

import pandas as pd
import streamlit as st

from duckdb_client import get_duckdb_client
try:
    from app.page_shared import format_month_label, safe_div
except ModuleNotFoundError:
    from page_shared import format_month_label, safe_div


TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ROW_DEFS = [
    ("mtd", "delta_prev", "pct_prev"),
    ("prev_mtd", "delta_prev2", "pct_prev2"),
    ("prev2_mtd", "delta_prev3", "pct_prev3"),
    ("prev3_mtd", "delta_prev4", "pct_prev4"),
    ("prev4_mtd", "delta_prev5", "pct_prev5"),
    ("avg6", "delta_avg6", "pct_avg6"),
    ("avg12", "delta_avg12", "pct_avg12"),
]
ROW_LABELS = {
    "mtd": "Mese corrente",
    "prev_mtd": "Mese precedente",
    "prev2_mtd": "2 mesi fa",
    "prev3_mtd": "3 mesi fa",
    "prev4_mtd": "4 mesi fa",
    "avg6": "Media 6M",
    "avg12": "Media 12M",
}


@st.cache_data(show_spinner=False)
def load_data(db_name: str, table_name: str, _db_cache_buster: int):
    client = get_duckdb_client(db_name)
    try:
        return client.get_services_metrics(table_name)
    finally:
        client.close()


@st.cache_data(show_spinner=False)
def load_data_for_anchor(
    db_name: str, table_name: str, anchor_date: str, _db_cache_buster: int
):
    client = get_duckdb_client(db_name)
    try:
        return client.get_services_metrics(table_name, anchor_date=anchor_date)
    finally:
        client.close()


@st.cache_data(show_spinner=False)
def load_data_for_month(
    db_name: str, table_name: str, month_start: str, _db_cache_buster: int
):
    client = get_duckdb_client(db_name)
    try:
        return client.get_services_metrics_for_month(table_name, month_start=month_start)
    finally:
        client.close()


@st.cache_data(show_spinner=False)
def load_available_month_anchors(db_name: str, table_name: str, _db_cache_buster: int):
    client = get_duckdb_client(db_name)
    try:
        return client.get_available_month_anchors(table_name)
    finally:
        client.close()


def is_valid_table_name(table_name: str) -> bool:
    return bool(TABLE_NAME_PATTERN.match(table_name))


def normalize_costs_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    return df


def normalize_month_anchors(month_anchors: pd.DataFrame) -> pd.DataFrame:
    if month_anchors.empty:
        return month_anchors

    month_anchors = month_anchors.copy()
    month_anchors["month_start"] = pd.to_datetime(
        month_anchors["month_start"], errors="coerce"
    ).dt.date
    month_anchors["anchor_date"] = pd.to_datetime(
        month_anchors["anchor_date"], errors="coerce"
    ).dt.date
    return month_anchors.dropna(subset=["account", "month_start", "anchor_date"])


def get_accounts(df: pd.DataFrame) -> list[str]:
    if "account" not in df.columns:
        return []
    return sorted(df["account"].dropna().unique().tolist())


def get_metric_cols(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if col not in {"date", "account", "service"}]


def build_period_options(account_months: pd.DataFrame) -> tuple[list[str], dict[str, str]]:
    period_options = ["current"]
    month_to_anchor = {}
    if account_months.empty:
        return period_options, month_to_anchor

    latest_month = account_months.iloc[0]["month_start"]
    previous_months = account_months[account_months["month_start"] < latest_month]
    for row in previous_months.itertuples(index=False):
        month_key = row.month_start.isoformat()
        period_options.append(f"month:{month_key}")
        month_to_anchor[month_key] = row.anchor_date.isoformat()
    return period_options, month_to_anchor


def period_label(option: str) -> str:
    if option == "current":
        return "Stato attuale"
    month_key = option.split(":", 1)[1]
    month_start = pd.to_datetime(month_key, errors="coerce")
    if pd.isna(month_start):
        return option
    return format_month_label(month_start.date())


def build_row_labels(anchor_date) -> dict[str, str]:
    labels = dict(ROW_LABELS)
    anchor_ts = pd.to_datetime(anchor_date, errors="coerce")
    if pd.isna(anchor_ts):
        return labels

    anchor_month = anchor_ts.to_period("M").to_timestamp()
    month_metrics = [
        ("mtd", 0),
        ("prev_mtd", 1),
        ("prev2_mtd", 2),
        ("prev3_mtd", 3),
        ("prev4_mtd", 4),
    ]
    for metric, months_back in month_metrics:
        month_start = anchor_month - pd.DateOffset(months=months_back)
        labels[metric] = format_month_label(month_start.date())
    return labels


def build_total_series(df: pd.DataFrame, metric_cols: list[str]) -> pd.Series:
    sums = {}
    for col in metric_cols:
        if pd.api.types.is_numeric_dtype(df[col]):
            sums[col] = df[col].sum()

    totals = {metric: sums.get(metric, pd.NA) for metric in metric_cols}

    if "delta_day" in metric_cols and {"cost", "prev_day"} <= sums.keys():
        totals["delta_day"] = sums["cost"] - sums["prev_day"]
    if "delta_week" in metric_cols and {"cost", "prev_week"} <= sums.keys():
        totals["delta_week"] = sums["cost"] - sums["prev_week"]
    if "delta_7d" in metric_cols and {"cost", "avg7"} <= sums.keys():
        totals["delta_7d"] = sums["cost"] - sums["avg7"]
    if "delta_30d" in metric_cols and {"cost", "avg30"} <= sums.keys():
        totals["delta_30d"] = sums["cost"] - sums["avg30"]
    if "delta_prev" in metric_cols and {"mtd", "prev_mtd"} <= sums.keys():
        totals["delta_prev"] = sums["mtd"] - sums["prev_mtd"]
    if "delta_prev2" in metric_cols and {"mtd", "prev2_mtd"} <= sums.keys():
        totals["delta_prev2"] = sums["prev_mtd"] - sums["prev2_mtd"]
    if "delta_prev3" in metric_cols and {"mtd", "prev3_mtd"} <= sums.keys():
        totals["delta_prev3"] = sums["prev2_mtd"] - sums["prev3_mtd"]
    if "delta_prev4" in metric_cols and {"mtd", "prev4_mtd"} <= sums.keys():
        totals["delta_prev4"] = sums["prev3_mtd"] - sums["prev4_mtd"]
    if "delta_prev5" in metric_cols and {"mtd", "prev5_mtd"} <= sums.keys():
        totals["delta_prev5"] = sums["prev4_mtd"] - sums["prev5_mtd"]
    if "delta_avg6" in metric_cols and {"mtd", "avg6"} <= sums.keys():
        totals["delta_avg6"] = sums["mtd"] - sums["avg6"]
    if "delta_avg12" in metric_cols and {"mtd", "avg12"} <= sums.keys():
        totals["delta_avg12"] = sums["mtd"] - sums["avg12"]

    if "pct_7d" in metric_cols and {"cost", "avg7"} <= sums.keys():
        totals["pct_7d"] = safe_div(sums["cost"] - sums["avg7"], sums["avg7"])
    if "pct_week" in metric_cols and {"cost", "prev_week"} <= sums.keys():
        totals["pct_week"] = safe_div(
            sums["cost"] - sums["prev_week"], sums["prev_week"]
        )
    if "pct_prev" in metric_cols and {"mtd", "prev_mtd"} <= sums.keys():
        totals["pct_prev"] = safe_div(sums["mtd"] - sums["prev_mtd"], sums["prev_mtd"])
    if "pct_prev2" in metric_cols and {"mtd", "prev2_mtd"} <= sums.keys():
        totals["pct_prev2"] = safe_div(
            sums["prev_mtd"] - sums["prev2_mtd"], sums["prev2_mtd"]
        )
    if "pct_prev3" in metric_cols and {"mtd", "prev3_mtd"} <= sums.keys():
        totals["pct_prev3"] = safe_div(
            sums["prev2_mtd"] - sums["prev3_mtd"], sums["prev3_mtd"]
        )
    if "pct_prev4" in metric_cols and {"mtd", "prev4_mtd"} <= sums.keys():
        totals["pct_prev4"] = safe_div(
            sums["prev3_mtd"] - sums["prev4_mtd"], sums["prev4_mtd"]
        )
    if "pct_prev5" in metric_cols and {"mtd", "prev5_mtd"} <= sums.keys():
        totals["pct_prev5"] = safe_div(
            sums["prev4_mtd"] - sums["prev5_mtd"], sums["prev5_mtd"]
        )
    if "pct_avg6" in metric_cols and {"mtd", "avg6"} <= sums.keys():
        totals["pct_avg6"] = safe_div(sums["mtd"] - sums["avg6"], sums["avg6"])
    if "pct_avg12" in metric_cols and {"mtd", "avg12"} <= sums.keys():
        totals["pct_avg12"] = safe_div(sums["mtd"] - sums["avg12"], sums["avg12"])

    return pd.Series(totals)


def build_account_matrix(
    account_df: pd.DataFrame, metric_cols: list[str]
) -> tuple[pd.DataFrame, list[str], pd.Series]:
    if account_df.empty:
        empty_rows = (
            account_df.set_index("service")
            if "service" in account_df.columns
            else pd.DataFrame(index=pd.Index([], name="service"))
        )
        return empty_rows, [], pd.Series(dtype="object")

    service_rows = account_df.set_index("service")
    if service_rows.index.has_duplicates:
        service_rows = service_rows.groupby(level=0).first()

    if "mtd" in service_rows.columns:
        service_order = (
            service_rows["mtd"].sort_values(ascending=False, na_position="last").index.tolist()
        )
    else:
        service_order = sorted(account_df["service"].dropna().unique().tolist())

    totals = build_total_series(account_df, metric_cols)
    return service_rows, service_order, totals
