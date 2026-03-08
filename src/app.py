import html
import os
import re
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from duckdb_client import get_duckdb_client


DEFAULT_DB_NAME = os.environ.get("DUCKDB_DATABASE", "database.duckdb")
DEFAULT_TABLE = os.environ.get("DUCKDB_TABLE", "costs")
DEFAULT_POD_TABLE = os.environ.get("DUCKDB_POD_TABLE", "pod_monthly_trend")
PAGE_COSTS = "Costi"
PAGE_POD = "Pod"
NAV_PAGE_KEY = "active_page"
MONTH_NAMES_IT = {
    1: "gennaio",
    2: "febbraio",
    3: "marzo",
    4: "aprile",
    5: "maggio",
    6: "giugno",
    7: "luglio",
    8: "agosto",
    9: "settembre",
    10: "ottobre",
    11: "novembre",
    12: "dicembre",
}


def get_db_path(db_name: str) -> Path:
    return Path(__file__).resolve().parent.parent / "db" / db_name


@st.cache_data(show_spinner=False)
def load_data(db_name: str, table_name: str):
    client = get_duckdb_client(db_name)
    return client.get_services_metrics(table_name)


@st.cache_data(show_spinner=False)
def load_data_for_anchor(db_name: str, table_name: str, anchor_date: str):
    client = get_duckdb_client(db_name)
    return client.get_services_metrics(table_name, anchor_date=anchor_date)


@st.cache_data(show_spinner=False)
def load_available_month_anchors(db_name: str, table_name: str):
    client = get_duckdb_client(db_name)
    return client.get_available_month_anchors(table_name)


@st.cache_data(show_spinner=False)
def load_pod_data(db_name: str, table_name: str, months: int = 12):
    client = get_duckdb_client(db_name)
    return client.get_pod_monthly_trend(table_name, months=months)


def safe_div(numerator, denominator):
    if denominator in (0, None) or pd.isna(denominator):
        return pd.NA
    return numerator / denominator


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


def format_month_label(month_start) -> str:
    return f"{MONTH_NAMES_IT[month_start.month].capitalize()} {month_start.year}"


def inject_navigation_styles() -> None:
    st.markdown(
        """
        <style>
        html, body, .stApp {
            background-color: #ECEFF3;
            color: #1F2933;
        }
        .stApp h1,
        .stApp h2,
        .stApp h3,
        .stApp p,
        .stApp label,
        .stApp li {
            color: #1F2933;
        }
        section[data-testid="stSidebar"] {
            position: relative;
            background-color: #ECEFF3 !important;
            color: #1F2933;
        }
        section[data-testid="stSidebar"] > div {
            background-color: #ECEFF3 !important;
            box-shadow: 8px 0 20px rgba(15, 23, 42, 0.16);
            border-right: 1px solid #D6DCE4;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button {
            border-radius: 10px;
            min-height: 42px;
            border: 1px solid #BFC5CD !important;
            background: #FFFFFF !important;
            color: #1F2933 !important;
            font-weight: 600;
            transition: none;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button *,
        section[data-testid="stSidebar"] div[data-testid="stButton"] button span {
            color: inherit !important;
            fill: currentColor !important;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="secondary"] {
            background: #FFFFFF !important;
            color: #1F2933 !important;
            border-color: #BFC5CD !important;
            transition: background-color 0.18s ease-in-out, border-color 0.18s ease-in-out, color 0.18s ease-in-out;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="secondary"]:hover {
            border-color: #8D99A8 !important;
            background: #EEF2F6 !important;
            color: #1F2933 !important;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] {
            background: #1F2933 !important;
            border-color: #1F2933 !important;
            color: #FFFFFF !important;
            transition: none !important;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"]:hover {
            background: #1F2933 !important;
            border-color: #1F2933 !important;
            color: #FFFFFF !important;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] *,
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] span {
            color: #FFFFFF !important;
            fill: #FFFFFF !important;
        }
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="secondary"] *,
        section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="secondary"] span {
            color: #1F2933 !important;
            fill: #1F2933 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_navigation() -> str:
    if NAV_PAGE_KEY not in st.session_state:
        st.session_state[NAV_PAGE_KEY] = PAGE_COSTS

    active_page = st.session_state[NAV_PAGE_KEY]

    st.sidebar.markdown("### Pagine")
    costs_clicked = st.sidebar.button(
        PAGE_COSTS,
        key="nav-costs",
        width="stretch",
        type="primary" if active_page == PAGE_COSTS else "secondary",
    )
    pod_clicked = st.sidebar.button(
        PAGE_POD,
        key="nav-pod",
        width="stretch",
        type="primary" if active_page == PAGE_POD else "secondary",
    )

    new_page = active_page
    if costs_clicked:
        new_page = PAGE_COSTS
    elif pod_clicked:
        new_page = PAGE_POD

    if new_page != active_page:
        st.session_state[NAV_PAGE_KEY] = new_page
        st.rerun()

    return new_page


def main() -> None:
    st.set_page_config(page_title="AWS Costs Dashboard", layout="wide")
    inject_navigation_styles()
    selected_page = render_sidebar_navigation()

    if selected_page == PAGE_POD:
        st.title("Pod")
        db_path = get_db_path(DEFAULT_DB_NAME)
        if not db_path.exists():
            st.error(f"File DuckDB non trovato: {db_path}")
            return

        try:
            pod_df = load_pod_data(DEFAULT_DB_NAME, DEFAULT_POD_TABLE, months=12)
        except Exception:
            st.info(
                "Tabella pod non disponibile. Esegui `python3 src/pod_collector.py` "
                "per generare i dati mock."
            )
            return

        if pod_df.empty:
            st.info("Nessun dato pod disponibile negli ultimi 12 mesi.")
            return

        pod_df["month_start"] = pd.to_datetime(
            pod_df["month_start"], errors="coerce"
        ).dt.to_period("M").dt.to_timestamp()
        pod_df["total_pods"] = pd.to_numeric(pod_df["total_pods"], errors="coerce")
        pod_df = pod_df.dropna(subset=["month_start", "tenant", "total_pods"])
        if pod_df.empty:
            st.info("Dati pod non validi.")
            return

        tenant_month = (
            pod_df.groupby(["month_start", "tenant"], as_index=False)["total_pods"]
            .max()
            .sort_values("month_start")
        )
        if tenant_month.empty:
            st.info("Nessun dato pod aggregabile.")
            return

        latest_month = tenant_month["month_start"].max()
        months_asc = pd.date_range(end=latest_month, periods=12, freq="MS")
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
                [safe_div(curr - prev_value, prev_value) for curr, prev_value in zip(series, prev)],
                index=series.index,
            )

        def format_number(num, decimals=0, sign=False):
            if pd.isna(num):
                return ""
            fmt = f"{'+' if sign else ''},.{decimals}f"
            raw = format(num, fmt)
            return raw.replace(",", "X").replace(".", ",").replace("X", ".")

        def get_trend_class(delta_value, pct_value):
            if not pd.isna(pct_value):
                if pct_value < 0:
                    return "neg"
                if pct_value > 0:
                    return "pos"
                return ""
            if pd.isna(delta_value):
                return ""
            if delta_value < 0:
                return "neg"
            if delta_value > 0:
                return "pos"
            return ""

        def format_pod_cell(value, delta, pct):
            main = format_number(value, decimals=0)
            if not main:
                return ""

            trend_class = get_trend_class(delta, pct)
            symbol_html = (
                f'<span class="sym {trend_class}">●</span>'
                if trend_class
                else '<span class="sym">●</span>'
            )
            delta_str = format_number(delta, decimals=0, sign=True) if not pd.isna(delta) else ""
            pct_str = (
                f"{format_number(pct * 100, decimals=1, sign=True)}%"
                if not pd.isna(pct)
                else ""
            )
            if not delta_str and not pct_str:
                return f'<div class="cell"><div class="main">{main}</div></div>'

            if trend_class:
                delta_html = (
                    f'<span class="{trend_class}">{delta_str}</span>' if delta_str else ""
                )
                pct_html = f'<span class="{trend_class}">{pct_str}</span>' if pct_str else ""
            else:
                delta_html = f"<span>{delta_str}</span>" if delta_str else ""
                pct_html = f"<span>{pct_str}</span>" if pct_str else ""

            sub_parts = []
            if delta_html:
                sub_parts.append(delta_html)
            if pct_html:
                sub_parts.append(f"({pct_html})")
            sub_html = " ".join(sub_parts)

            return (
                '<div class="cell">'
                f'<div class="main">{symbol_html} {main}</div>'
                f'<div class="sub">{sub_html}</div>'
                "</div>"
            )

        st.markdown(
            """
            <style>
            .pod-table-wrap {
                overflow-x: auto;
                max-width: 100%;
                box-shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
                border-radius: 12px;
            }
            table.pod-table {
                border-collapse: separate;
                border-spacing: 0;
                width: max-content;
                min-width: 100%;
                background: #ECEFF3;
                border-radius: 12px;
            }
            table.pod-table th,
            table.pod-table td {
                padding: 8px 10px;
                border-bottom: 1px solid #ECEFF3;
                color: #1F2933;
                vertical-align: top;
                white-space: nowrap;
                background: #ECEFF3;
                text-align: right;
                border-right: 1px solid #BFC5CD;
            }
            table.pod-table th {
                font-weight: 600;
                color: #6B7280;
            }
            table.pod-table th.month-col,
            table.pod-table td.month-col {
                position: sticky;
                left: 0;
                z-index: 3;
                background: #FFFFFF;
                min-width: 190px;
                text-align: left;
            }
            table.pod-table th.total-col,
            table.pod-table td.total-col {
                position: sticky;
                left: 190px;
                z-index: 2;
                background: #FFFFFF;
                min-width: 150px;
            }
            table.pod-table td .cell {
                white-space: normal;
                line-height: 1.45;
            }
            table.pod-table td .main {
                font-size: 15px;
                font-weight: 600;
                color: #1F2933;
            }
            table.pod-table td .sub {
                font-size: 12px;
                font-weight: 500;
                color: #6B7280;
                margin-top: 4px;
            }
            table.pod-table td .sym {
                font-size: 15px;
                line-height: 1;
                color: #6B7280;
            }
            table.pod-table td .sym.pos {
                color: #E06C75;
            }
            table.pod-table td .sym.neg {
                color: #3BA776;
            }
            table.pod-table td .sub .pos {
                color: inherit;
            }
            table.pod-table td .sub .neg {
                color: inherit;
            }
            table.pod-table tr.row-white td,
            table.pod-table tr.row-white th {
                background: #FFFFFF;
            }
            table.pod-table tr.row-white th.month-col,
            table.pod-table tr.row-white td.month-col,
            table.pod-table tr.row-white th.total-col,
            table.pod-table tr.row-white td.total-col {
                background: #FFFFFF;
            }
            table.pod-table tr.row-grey td,
            table.pod-table tr.row-grey th {
                background: #ECEFF3;
            }
            table.pod-table tr.row-grey th.month-col,
            table.pod-table tr.row-grey td.month-col,
            table.pod-table tr.row-grey th.total-col,
            table.pod-table tr.row-grey td.total-col {
                background: #FFFFFF;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        header_cells = [
            '<th class="month-col">Mese</th>',
            '<th class="total-col">Total</th>',
        ] + [f"<th>{html.escape(tenant)}</th>" for tenant in tenants]
        header_row = f"<tr>{''.join(header_cells)}</tr>"

        body_rows = []
        for idx, month_value in enumerate(months_desc):
            row_class = "row-white" if idx % 2 == 0 else "row-grey"
            month_label = html.escape(format_month_label(month_value.date()))

            row_cells = [
                f'<th class="month-col">{month_label}</th>',
                (
                    '<td class="total-col">'
                    f"{format_pod_cell(total_series.at[month_value], total_delta.at[month_value], total_pct.at[month_value])}"
                    "</td>"
                ),
            ]

            for tenant in tenants:
                row_cells.append(
                    "<td>"
                    f"{format_pod_cell(matrix.at[month_value, tenant], tenant_delta[tenant].at[month_value], tenant_pct[tenant].at[month_value])}"
                    "</td>"
                )

            body_rows.append(f'<tr class="{row_class}">{"".join(row_cells)}</tr>')

        table_html = (
            '<div class="pod-table-wrap">'
            '<table class="pod-table">'
            f"<thead>{header_row}</thead>"
            f"<tbody>{''.join(body_rows)}</tbody>"
            "</table>"
            "</div>"
        )
        st.markdown(table_html, unsafe_allow_html=True)
        return

    st.title("AWS Costs Dashboard")

    db_name = DEFAULT_DB_NAME
    table_name = DEFAULT_TABLE

    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
        st.error("Table name non valido.")
        return

    db_path = get_db_path(db_name)
    if not db_path.exists():
        st.error(f"File DuckDB non trovato: {db_path}")
        return

    df = load_data(db_name, table_name)
    if df.empty:
        st.info("Nessun dato disponibile per la query.")
        return

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    month_anchors = load_available_month_anchors(db_name, table_name)
    if not month_anchors.empty:
        month_anchors["month_start"] = pd.to_datetime(
            month_anchors["month_start"], errors="coerce"
        ).dt.date
        month_anchors["anchor_date"] = pd.to_datetime(
            month_anchors["anchor_date"], errors="coerce"
        ).dt.date
        month_anchors = month_anchors.dropna(
            subset=["account", "month_start", "anchor_date"]
        )

    accounts = sorted(df["account"].dropna().unique().tolist())

    st.markdown(
        """
        <style>
        .table-wrap {
            overflow-x: auto;
            max-width: 100%;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
            border-radius: 12px;
        }
        .account-sep {
            margin-bottom: 72px;
        }
        .table-updated-at {
            margin: 0;
            display: flex;
            align-items: center;
            min-height: 38px;
            font-size: 12px;
            font-weight: 500;
            color: #6B7280;
            line-height: 1.2;
        }
        div[data-testid="stHorizontalBlock"]:has(.table-updated-at):has(div[data-testid="stDownloadButton"]) {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            gap: 12px;
            margin: 4px 0 6px 4px;
        }
        div[data-testid="stHorizontalBlock"]:has(.table-updated-at):has(div[data-testid="stDownloadButton"]) > div[data-testid="stColumn"] {
            display: flex;
            align-items: center;
        }
        div[data-testid="stHorizontalBlock"]:has(.table-updated-at):has(div[data-testid="stDownloadButton"]) > div[data-testid="stColumn"]:first-child {
            flex: 1 1 auto;
            min-width: 0;
        }
        div[data-testid="stHorizontalBlock"]:has(.table-updated-at):has(div[data-testid="stDownloadButton"]) > div[data-testid="stColumn"]:first-child > div[data-testid="stVerticalBlock"] {
            display: flex;
            justify-content: center;
            width: 100%;
        }
        div[data-testid="stHorizontalBlock"]:has(.table-updated-at):has(div[data-testid="stDownloadButton"]) > div[data-testid="stColumn"]:last-child {
            flex: 0 0 auto;
            width: auto;
        }
        table.costs-table {
            border-collapse: separate;
            border-spacing: 0;
            width: max-content;
            min-width: 100%;
            background: #ECEFF3;
            border-radius: 12px;
        }
        table.costs-table th,
        table.costs-table td {
            padding: 8px 10px;
            border-bottom: 1px solid #ECEFF3;
            color: #1F2933;
            vertical-align: top;
            white-space: nowrap;
            background: #ECEFF3;
            text-align: right;
            border-right: 1px solid #BFC5CD;
        }
        table.costs-table th {
            font-weight: 600;
            background: #ECEFF3;
            color: #6B7280;
        }
        table.costs-table th.metric-col,
        table.costs-table td.metric-col {
            text-align: left;
        }
        table.costs-table td .cell {
            white-space: normal;
            line-height: 1.45;
        }
        table.costs-table td .main {
            font-size: 15px;
            font-weight: 600;
            color: #1F2933;
        }
        table.costs-table td .main .dec {
            font-size: 12px;
            font-weight: 500;
        }
        table.costs-table td .sub {
            font-size: 12px;
            font-weight: 500;
            color: #6B7280;
            margin-top: 4px;
        }
        table.costs-table td .sub .pos {
            color: inherit;
        }
        table.costs-table td .sub .neg {
            color: inherit;
        }
        table.costs-table td .sym.pos {
            color: #E06C75;
        }
        table.costs-table td .sym.neg {
            color: #3BA776;
        }
        table.costs-table td .sym {
            font-size: 15px;
            line-height: 1;
        }
        table.costs-table tr.row-current td .sub .pos {
            color: inherit;
        }
        table.costs-table tr.row-current td .sub .neg {
            color: inherit;
        }
        table.costs-table tr.row-current td .sym.pos {
            color: #D64545;
        }
        table.costs-table tr.row-current td .sym.neg {
            color: #1F9D55;
        }
        table.costs-table th.metric-col,
        table.costs-table td.metric-col {
            position: sticky;
            left: 0;
            z-index: 3;
            background: #FFFFFF;
            min-width: 180px;
        }
        table.costs-table th.total-col,
        table.costs-table td.total-col {
            position: sticky;
            left: 180px;
            z-index: 2;
            background: #FFFFFF;
            min-width: 140px;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) {
            display: flex;
            flex-direction: row;
            justify-content: flex-end;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) > div[data-testid="stElementContainer"]:has(div[data-testid="stSelectbox"]) {
            flex: 0 0 170px;
            width: 170px;
            min-width: 170px;
            max-width: 170px;
            margin-bottom: 0;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) > div[data-testid="stElementContainer"]:has(div[data-testid="stDownloadButton"]) {
            flex: 0 0 auto;
            margin-bottom: 0;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stSelectbox"] label,
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stSelectbox"] [data-baseweb="select"] *,
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stSelectbox"] [data-baseweb="select"] input {
            color: #FFFFFF !important;
            -webkit-text-fill-color: #FFFFFF !important;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stSelectbox"] [data-baseweb="select"] svg {
            color: #FFFFFF !important;
            fill: #FFFFFF !important;
        }
        div[data-testid="stDownloadButton"] {
            display: flex;
            justify-content: flex-end;
        }
        div[data-testid="stDownloadButton"] button,
        div[data-testid="stDownloadButton"] button span {
            color: #FFFFFF !important;
        }
        div[data-testid="stDownloadButton"] button * {
            color: #FFFFFF !important;
        }
        table.costs-table tr.row-current td,
        table.costs-table tr.row-current th {
            padding-top: 12px;
            padding-bottom: 12px;
        }
        table.costs-table tr.row-current td .main {
            font-size: 16px;
        }
        table.costs-table tr.row-current td .sub {
            font-size: 12px;
        }
        table.costs-table tr.row-highlight td,
        table.costs-table tr.row-highlight th {
            background: #ECEFF3;
        }
        table.costs-table tr.row-highlight th.metric-col,
        table.costs-table tr.row-highlight td.metric-col {
            background: #FFFFFF;
        }
        table.costs-table tr.row-highlight th.total-col,
        table.costs-table tr.row-highlight td.total-col {
            background: #FFFFFF;
        }
        table.costs-table tr.row-white td,
        table.costs-table tr.row-white th {
            background: #FFFFFF;
        }
        table.costs-table tr.row-white th.metric-col,
        table.costs-table tr.row-white td.metric-col {
            background: #FFFFFF;
        }
        table.costs-table tr.row-white th.total-col,
        table.costs-table tr.row-white td.total-col {
            background: #FFFFFF;
        }
        table.costs-table tr.row-grey td,
        table.costs-table tr.row-grey th {
            background: #ECEFF3;
        }
        table.costs-table tr.row-grey th.metric-col,
        table.costs-table tr.row-grey td.metric-col {
            background: #ECEFF3;
        }
        table.costs-table tr.row-grey th.total-col,
        table.costs-table tr.row-grey td.total-col {
            background: #ECEFF3;
        }
        table.costs-table tr.row-grey td,
        table.costs-table tr.row-grey th {
            border-bottom-color: #FFFFFF;
        }
        table.costs-table tr:nth-child(even):not(.row-white) td,
        table.costs-table tr:nth-child(even):not(.row-white) th {
            background: #ECEFF3;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    metric_cols = [
        col for col in df.columns if col not in {"date", "account", "service"}
    ]
    if not metric_cols:
        st.info("Nessuna metrica disponibile.")
        return

    row_defs = [
        ("mtd", "delta_prev", "pct_prev"),
        ("prev_mtd", "delta_prev2", "pct_prev2"),
        ("prev2_mtd", "delta_prev3", "pct_prev3"),
        ("prev3_mtd", "delta_prev4", "pct_prev4"),
        ("prev4_mtd", "delta_prev5", "pct_prev5"),
        ("avg6", "delta_avg6", "pct_avg6"),
        ("avg12", "delta_avg12", "pct_avg12"),
    ]

    row_labels = {
        "mtd": "Mese corrente",
        "prev_mtd": "Mese precedente",
        "prev2_mtd": "2 mesi fa",
        "prev3_mtd": "3 mesi fa",
        "prev4_mtd": "4 mesi fa",
        "avg6": "Media 6M",
        "avg12": "Media 12M",
    }

    def symbol(val):
        if pd.isna(val) or val == 0:
            return "●"
        return "●"

    def format_number(num, decimals=2, sign=False):
        if pd.isna(num):
            return ""
        fmt = f"{'+' if sign else ''},.{decimals}f"
        raw = format(num, fmt)
        return raw.replace(",", "X").replace(".", ",").replace("X", ".")

    def format_cell(value, delta, pct, invert_colors=True):
        if pd.isna(value):
            return ""
        main_value = format_number(value)
        if "," in main_value:
            int_part, dec_part = main_value.split(",", 1)
            main = (
                f'{int_part}<span class="dec">,</span>'
                f'<span class="dec">{dec_part}</span>'
            )
        else:
            main = main_value
        if delta is None and pct is None:
            return f'<div class="cell"><div class="main">{main}</div></div>'

        def cls(pct_val):
            if pd.isna(pct_val):
                return ""
            if pct_val < 0:
                return "neg" if invert_colors else "pos"
            return "pos" if invert_colors else "neg"

        delta_str = "" if pd.isna(delta) else format_number(delta, sign=True)
        pct_str = (
            ""
            if pd.isna(pct)
            else f"{format_number(pct * 100, decimals=1, sign=True)}%"
        )
        delta_class = cls(pct)
        pct_class = cls(pct)
        symbol_class = cls(pct)
        delta_symbol = symbol(delta) if not pd.isna(delta) else symbol(pct)
        delta_html = (
            f'<span class="{delta_class}">{delta_str}</span>' if delta_str else ""
        )
        pct_html = f'<span class="{pct_class}">{pct_str}</span>' if pct_str else ""
        symbol_html = (
            f'<span class="sym {symbol_class}">{delta_symbol}</span>'
            if symbol_class
            else delta_symbol
        )
        return (
            '<div class="cell">'
            f'<div class="main">{symbol_html} {main}</div>'
            f'<div class="sub">{delta_html} ({pct_html})</div>'
            "</div>"
        )

    def format_cell_text(value, pct):
        if pd.isna(value):
            return ""
        main = format_number(value)
        if pct is None or pd.isna(pct):
            return main
        pct_str = f"{format_number(pct * 100, decimals=1, sign=True)}%"
        return f"{main}\n{pct_str}"

    def build_excel_bytes(columns, value_rows, pct_rows):
        try:
            import xlsxwriter
        except ImportError:
            return None

        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {"in_memory": True})
        sheet = workbook.add_worksheet("Costs")

        header_fmt = workbook.add_format(
            {
                "bold": True,
                "font_color": "#1F2933",
                "bg_color": "#ECEFF3",
                "border": 1,
            }
        )
        base_fmt = workbook.add_format(
            {
                "font_color": "#1F2933",
                "valign": "top",
                "text_wrap": True,
                "border": 1,
            }
        )
        metric_fmt = workbook.add_format(
            {
                "bold": True,
                "font_color": "#1F2933",
                "valign": "vcenter",
                "border": 1,
            }
        )
        main_fmt = workbook.add_format({"font_color": "#1F2933"})
        up_fmt = workbook.add_format({"font_color": "#D64545"})
        down_fmt = workbook.add_format({"font_color": "#1F9D55"})

        for col_idx, col_name in enumerate(columns):
            sheet.write(0, col_idx, col_name, header_fmt)
        if columns:
            sheet.set_column(0, 0, 24)
        if len(columns) > 1:
            sheet.set_column(1, 1, 16)
        if len(columns) > 2:
            sheet.set_column(2, len(columns) - 1, 16)

        for row_idx, row_values in enumerate(value_rows, start=1):
            sheet.set_row(row_idx, 34)
            row_pcts = pct_rows[row_idx - 1]
            for col_idx, col_name in enumerate(columns):
                cell_text = row_values.get(col_name, "")
                if col_name == "Metric":
                    sheet.write(row_idx, col_idx, cell_text, metric_fmt)
                    continue

                pct_val = row_pcts.get(col_name, pd.NA)
                if (
                    cell_text
                    and "\n" in cell_text
                    and pct_val is not None
                    and not pd.isna(pct_val)
                ):
                    main_text, pct_text = cell_text.split("\n", 1)
                    pct_fmt = (
                        up_fmt if pct_val > 0 else down_fmt if pct_val < 0 else main_fmt
                    )
                    sheet.write_rich_string(
                        row_idx,
                        col_idx,
                        main_fmt,
                        main_text,
                        "\n",
                        pct_fmt,
                        pct_text,
                        base_fmt,
                    )
                else:
                    sheet.write(row_idx, col_idx, cell_text, base_fmt)

        sheet.freeze_panes(1, 1)
        workbook.close()
        output.seek(0)
        return output.getvalue()

    def build_account_matrix(account_df: pd.DataFrame):
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
                service_rows["mtd"]
                .sort_values(ascending=False, na_position="last")
                .index.tolist()
            )
        else:
            service_order = sorted(account_df["service"].dropna().unique().tolist())

        totals = build_total_series(account_df, metric_cols)
        return service_rows, service_order, totals

    def build_download_payload(account_df: pd.DataFrame):
        if account_df.empty:
            return ["Metric", "Total"], [], []

        service_rows, service_order, totals = build_account_matrix(account_df)
        columns = ["Metric", "Total"] + service_order
        value_rows = []
        pct_rows = []
        for metric, delta_col, pct_col in row_defs:
            label = row_labels.get(metric, metric)
            row_values = {
                "Metric": label,
                "Total": format_cell_text(totals.get(metric), totals.get(pct_col)),
            }
            row_pcts = {
                "Metric": pd.NA,
                "Total": totals.get(pct_col),
            }
            for service in service_order:
                if service not in service_rows.index:
                    row_values[service] = ""
                    row_pcts[service] = pd.NA
                else:
                    row = service_rows.loc[service]
                    row_values[service] = format_cell_text(
                        row.get(metric), row.get(pct_col)
                    )
                    row_pcts[service] = row.get(pct_col)
            value_rows.append(row_values)
            pct_rows.append(row_pcts)

        return columns, value_rows, pct_rows

    for account in accounts:
        base_account_df = df[df["account"] == account]
        if base_account_df.empty:
            continue

        account_months = month_anchors[month_anchors["account"] == account]
        account_months = account_months.sort_values("month_start", ascending=False)

        period_options = ["current"]
        month_to_anchor = {}
        if not account_months.empty:
            latest_month = account_months.iloc[0]["month_start"]
            previous_months = account_months[
                account_months["month_start"] < latest_month
            ]
            for row in previous_months.itertuples(index=False):
                month_key = row.month_start.isoformat()
                period_options.append(f"month:{month_key}")
                month_to_anchor[month_key] = row.anchor_date.isoformat()

        def period_label(option: str) -> str:
            if option == "current":
                return "Stato attuale"
            month_key = option.split(":", 1)[1]
            month_start = pd.to_datetime(month_key, errors="coerce")
            if pd.isna(month_start):
                return option
            return format_month_label(month_start.date())

        st.markdown(f"**Account:** {account} (in USD)")

        toolbar_left, toolbar_right = st.columns([1, 1])
        with toolbar_right:
            selected_period = st.selectbox(
                "Periodo",
                options=period_options,
                format_func=period_label,
                key=f"csv-period-{account}",
                label_visibility="collapsed",
            )

            selected_anchor_date = None
            if selected_period == "current":
                selected_account_df = base_account_df
                file_suffix = "stato_attuale"
                if not account_months.empty:
                    selected_anchor_date = account_months.iloc[0]["anchor_date"]
            else:
                month_key = selected_period.split(":", 1)[1]
                selected_anchor_date = month_to_anchor.get(month_key)
                if selected_anchor_date:
                    historical_df = load_data_for_anchor(
                        db_name, table_name, selected_anchor_date
                    )
                    selected_account_df = historical_df[
                        historical_df["account"] == account
                    ]
                else:
                    selected_account_df = pd.DataFrame(columns=base_account_df.columns)
                file_suffix = month_key[:7]

            download_columns, download_values, download_pcts = build_download_payload(
                selected_account_df
            )

            excel_bytes = build_excel_bytes(
                download_columns, download_values, download_pcts
            )
            if excel_bytes is None:
                fallback_df = pd.DataFrame(download_values, columns=download_columns)
                csv_bytes = fallback_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Scarica CSV",
                    data=csv_bytes,
                    file_name=f"costs_{account}_{file_suffix}.csv",
                    mime="text/csv",
                )
                st.caption(
                    "Installa `xlsxwriter` per export Excel con percentuali colorate."
                )
            else:
                st.download_button(
                    label="Scarica Excel",
                    data=excel_bytes,
                    file_name=f"costs_{account}_{file_suffix}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        selected_update_text = "-"
        selected_anchor_ts = pd.to_datetime(selected_anchor_date, errors="coerce")
        if not pd.isna(selected_anchor_ts):
            selected_update_text = selected_anchor_ts.strftime("%d/%m/%Y")
        with toolbar_left:
            st.markdown(
                (
                    '<div class="table-updated-at">'
                    f"Dati aggiornati al: {html.escape(selected_update_text)}"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

        selected_service_rows, selected_services, selected_totals = (
            build_account_matrix(selected_account_df)
        )

        header_cells = [
            '<th class="metric-col">Metric (MTD)</th>',
            '<th class="total-col">Total</th>',
        ] + [f"<th>{html.escape(service)}</th>" for service in selected_services]
        header_row = f"<tr>{''.join(header_cells)}</tr>"

        body_rows = []
        for metric, delta_col, pct_col in row_defs:
            label = html.escape(row_labels.get(metric, metric))
            invert_colors = True
            total_cell = format_cell(
                selected_totals.get(metric),
                selected_totals.get(delta_col),
                selected_totals.get(pct_col),
                invert_colors=invert_colors,
            )
            row_classes = []
            if metric == "mtd":
                row_classes.append("row-current")
                row_classes.append("row-white")
            if metric in {"prev_mtd", "prev2_mtd", "prev3_mtd", "prev4_mtd"}:
                row_classes.append("row-highlight row-grey")
            if metric in {"prev2_mtd", "prev4_mtd"}:
                row_classes.append("row-grey")
            if metric in {"avg6", "avg12"}:
                row_classes.append("row-white")
            row_class = f' class="{" ".join(row_classes)}"' if row_classes else ""
            row_cells = [
                f'<th class="metric-col">{label}</th>',
                f'<td class="total-col">{total_cell}</td>',
            ]
            for service in selected_services:
                if service not in selected_service_rows.index:
                    cell = ""
                else:
                    row = selected_service_rows.loc[service]
                    cell = format_cell(
                        row.get(metric),
                        row.get(delta_col),
                        row.get(pct_col),
                        invert_colors=invert_colors,
                    )
                row_cells.append(f"<td>{cell}</td>")
            body_rows.append(f"<tr{row_class}>{''.join(row_cells)}</tr>")

        table_html = (
            '<div class="table-wrap">'
            '<table class="costs-table">'
            f"<thead>{header_row}</thead>"
            f"<tbody>{''.join(body_rows)}</tbody>"
            "</table>"
            "</div>"
        )
        st.markdown(table_html, unsafe_allow_html=True)
        st.markdown('<div class="account-sep"></div>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()
