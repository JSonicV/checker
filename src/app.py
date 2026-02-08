import html
import os
import re
from pathlib import Path

import pandas as pd
import streamlit as st

from duckdb_client import get_duckdb_client


DEFAULT_DB_NAME = os.environ.get("DUCKDB_DATABASE", "database.duckdb")
DEFAULT_TABLE = os.environ.get("DUCKDB_TABLE", "costs")


def get_db_path(db_name: str) -> Path:
    return Path(__file__).resolve().parent.parent / "db" / db_name


@st.cache_data(show_spinner=False)
def load_data(db_name: str, table_name: str):
    client = get_duckdb_client(db_name)
    return client.get_services_metrics(table_name)


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


def main() -> None:
    st.set_page_config(page_title="AWS Costs Dashboard", layout="wide")
    st.title("AWS Costs Dashboard")

    db_name = st.sidebar.text_input("DuckDB file", DEFAULT_DB_NAME)
    table_name = st.sidebar.text_input("Table name", DEFAULT_TABLE)

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

    accounts = sorted(df["account"].dropna().unique().tolist())
    services = sorted(df["service"].dropna().unique().tolist())

    account_filter = st.sidebar.multiselect("Account", accounts, default=accounts)
    service_filter = st.sidebar.multiselect("Service", services, default=services)

    filtered = df[
        df["account"].isin(account_filter) & df["service"].isin(service_filter)
    ]

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
        .stApp span,
        .stApp div,
        .stApp li {
            color: #1F2933;
        }
        .stSidebar {
            color: #1F2933;
        }
        .table-wrap {
            overflow-x: auto;
            max-width: 100%;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
            border-radius: 12px;
        }
        .account-sep {
            margin-bottom: 72px;
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
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stDownloadButton"]) {
            align-items: flex-end;
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
        col for col in filtered.columns if col not in {"date", "account", "service"}
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

    def format_cell_text(value, delta, pct):
        if pd.isna(value):
            return ""
        main = format_number(value)
        if delta is None and pct is None:
            return main
        delta_str = "" if pd.isna(delta) else format_number(delta, sign=True)
        pct_str = (
            ""
            if pd.isna(pct)
            else f"{format_number(pct * 100, decimals=1, sign=True)}%"
        )
        delta_symbol = symbol(delta) if not pd.isna(delta) else symbol(pct)
        return f"{main}\n{delta_symbol} {delta_str} ({pct_str})"

    for account in account_filter:
        account_df = filtered[filtered["account"] == account]
        if account_df.empty:
            continue

        col_left, col_right = st.columns([6, 2])
        with col_left:
            st.markdown(f"**Account:** {account} (in USD)")

        service_rows = account_df.set_index("service")
        if service_rows.index.has_duplicates:
            service_rows = service_rows.groupby(level=0).first()

        if "mtd" in service_rows.columns:
            services = (
                service_rows["mtd"]
                .sort_values(ascending=False, na_position="last")
                .index.tolist()
            )
        else:
            services = sorted(account_df["service"].dropna().unique().tolist())

        totals = build_total_series(account_df, metric_cols)

        download_rows = []
        for metric, delta_col, pct_col in row_defs:
            label = row_labels.get(metric, metric)
            row_dict = {
                "Metric": label,
                "Total": format_cell_text(
                    totals.get(metric), totals.get(delta_col), totals.get(pct_col)
                ),
            }
            for service in services:
                if service not in service_rows.index:
                    row_dict[service] = ""
                else:
                    row = service_rows.loc[service]
                    row_dict[service] = format_cell_text(
                        row.get(metric), row.get(delta_col), row.get(pct_col)
                    )
            download_rows.append(row_dict)

        download_df = pd.DataFrame(download_rows)
        download_df = download_df[["Metric", "Total"] + services]
        csv_bytes = download_df.to_csv(index=False).encode("utf-8")
        with col_right:
            st.download_button(
                label="Scarica CSV",
                data=csv_bytes,
                file_name=f"costs_{account}.csv",
                mime="text/csv",
            )

        header_cells = [
            '<th class="metric-col">Metric (MTD)</th>',
            '<th class="total-col">Total</th>',
        ] + [f"<th>{html.escape(service)}</th>" for service in services]
        header_row = f"<tr>{''.join(header_cells)}</tr>"

        body_rows = []
        for metric, delta_col, pct_col in row_defs:
            label = html.escape(row_labels.get(metric, metric))
            invert_colors = True
            total_cell = format_cell(
                totals.get(metric),
                totals.get(delta_col),
                totals.get(pct_col),
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
            for service in services:
                if service not in service_rows.index:
                    cell = ""
                else:
                    row = service_rows.loc[service]
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
