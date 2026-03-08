import html

import pandas as pd
import streamlit as st

try:
    from app.page_shared import format_month_label, get_db_path
    from app.pages.pod.css import inject_styles
    from app.pages.pod.logic import build_table_state, load_data
except ModuleNotFoundError:
    from page_shared import format_month_label, get_db_path
    from pages.pod.css import inject_styles
    from pages.pod.logic import build_table_state, load_data


def render_page(db_name: str, table_name: str) -> None:
    st.title("Pod")

    db_path = get_db_path(db_name)
    if not db_path.exists():
        st.error(f"File DuckDB non trovato: {db_path}")
        return

    try:
        pod_df = load_data(db_name, table_name, months=12)
    except Exception:
        st.info(
            "Tabella pod non disponibile. Esegui `python3 src/pod_collector.py` "
            "per generare i dati mock."
        )
        return

    if pod_df.empty:
        st.info("Nessun dato pod disponibile negli ultimi 12 mesi.")
        return

    table_state = build_table_state(pod_df, months=12)
    if table_state is None:
        st.info("Dati pod non validi.")
        return

    inject_styles()

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

    def format_cell(value, delta, pct):
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
            delta_html = f'<span class="{trend_class}">{delta_str}</span>' if delta_str else ""
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

    header_cells = [
        '<th class="month-col">Mese</th>',
        '<th class="total-col">Total</th>',
    ] + [f"<th>{html.escape(tenant)}</th>" for tenant in table_state.tenants]
    header_row = f"<tr>{''.join(header_cells)}</tr>"

    body_rows = []
    for idx, month_value in enumerate(table_state.months_desc):
        row_class = "row-white" if idx % 2 == 0 else "row-grey"
        month_label = html.escape(format_month_label(month_value.date()))

        row_cells = [
            f'<th class="month-col">{month_label}</th>',
            (
                '<td class="total-col">'
                f"{format_cell(table_state.total_series.at[month_value], table_state.total_delta.at[month_value], table_state.total_pct.at[month_value])}"
                "</td>"
            ),
        ]

        for tenant in table_state.tenants:
            row_cells.append(
                "<td>"
                f"{format_cell(table_state.matrix.at[month_value, tenant], table_state.tenant_delta[tenant].at[month_value], table_state.tenant_pct[tenant].at[month_value])}"
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
