import html

import pandas as pd
import streamlit as st

try:
    from app.page_shared import format_month_label, get_db_cache_buster, get_db_path
    from app.pages.pod.css import inject_styles
    from app.pages.pod.logic import (
        build_table_state,
        load_daily_data,
        load_monthly_data,
    )
except ModuleNotFoundError:
    from page_shared import format_month_label, get_db_cache_buster, get_db_path
    from pages.pod.css import inject_styles
    from pages.pod.logic import build_table_state, load_daily_data, load_monthly_data


def render_page(db_name: str, monthly_table_name: str, daily_table_name: str) -> None:
    st.title("Pod")

    db_path = get_db_path(db_name)
    if not db_path.exists():
        st.error(f"File DuckDB non trovato: {db_path}")
        return

    db_cache_buster = get_db_cache_buster(db_name)

    try:
        pod_monthly_df = load_monthly_data(
            db_name,
            monthly_table_name,
            months=12,
            _db_cache_buster=db_cache_buster,
        )
    except Exception:
        st.info(
            "Tabella pod non disponibile. Esegui `python3 src/pod_collector.py` "
            "per generare i dati."
        )
        return

    if pod_monthly_df.empty:
        st.info("Nessun dato pod disponibile negli ultimi 12 mesi.")
        return

    try:
        pod_daily_df = load_daily_data(
            db_name,
            daily_table_name,
            days=60,
            _db_cache_buster=db_cache_buster,
        )
    except Exception:
        pod_daily_df = pd.DataFrame(
            columns=["date", "tenant", "total_pods", "onboarded_pods"]
        )

    def set_metric(next_metric: str) -> None:
        st.session_state["pod_metric_selector"] = next_metric

    metric = st.session_state.setdefault("pod_metric_selector", "total")
    active_switch_key = "pod_metric_total" if metric == "total" else "pod_metric_onboarded"
    inactive_switch_key = (
        "pod_metric_onboarded" if metric == "total" else "pod_metric_total"
    )
    inject_styles(active_switch_key, inactive_switch_key)
    with st.container(
        key="pod-switch",
        horizontal=True,
        horizontal_alignment="left",
        gap=None,
    ):
        st.button(
            "total",
            key="pod_metric_total",
            type="primary" if metric == "total" else "secondary",
            on_click=set_metric,
            args=("total",),
        )
        st.button(
            "onboarded",
            key="pod_metric_onboarded",
            type="primary" if metric == "onboarded" else "secondary",
            on_click=set_metric,
            args=("onboarded",),
        )
    metric = st.session_state["pod_metric_selector"]

    table_state = build_table_state(
        pod_monthly_df,
        pod_daily_df,
        months=12,
        metric=metric,
    )
    if table_state is None:
        st.info("Dati pod non validi.")
        return

    def format_number(num, decimals=0, sign=False):
        if pd.isna(num):
            return ""
        rounded = round(float(num), decimals)
        if rounded == 0:
            return "0"
        fmt = f"{'+' if sign else ''},.{decimals}f"
        raw = format(num, fmt)
        return raw.replace(",", "X").replace(".", ",").replace("X", ".")

    def format_pct_value(pct):
        if pd.isna(pct):
            return ""
        pct_display = pct * 100
        decimals = 0 if abs(pct_display) >= 1 else 1
        return f"{format_number(pct_display, decimals=decimals, sign=True)}%"

    def format_share_pct_value(value, total_value):
        if pd.isna(value) or pd.isna(total_value) or total_value <= 0:
            return ""
        share_pct = (value / total_value) * 100
        decimals = 0 if abs(share_pct) >= 1 else 1
        return f"{format_number(share_pct, decimals=decimals)}%"

    def get_trend_class(delta_value, pct_value):
        if not pd.isna(delta_value):
            if delta_value > 0:
                return "pos"
            if delta_value < 0:
                return "neg"
            return "neutral"
        if not pd.isna(pct_value):
            if pct_value > 0:
                return "pos"
            if pct_value < 0:
                return "neg"
            return "neutral"
        return ""

    def get_onboarded_share_class(value, total_value):
        if pd.isna(value) or pd.isna(total_value) or total_value <= 0:
            return "neutral"
        share = value / total_value
        if share >= 0.75:
            return "pos"
        if share >= 0.30:
            return "warn"
        return "neg"

    def format_value_cell(value, delta, pct, reference_value=None):
        if pd.isna(value):
            return '<div class="cell"><div class="main">-</div></div>'

        main = format_number(value, decimals=0)
        if not main:
            return ""

        trend_class = get_trend_class(delta, pct)
        show_symbol = trend_class in {"pos", "neg"}
        symbol_html = f'<span class="sym {trend_class}">●</span>' if show_symbol else ""
        delta_str = (
            format_number(delta, decimals=0, sign=True) if not pd.isna(delta) else ""
        )
        pct_str = format_pct_value(pct)
        if not delta_str and not pct_str:
            return f'<div class="cell"><div class="main">{main}</div></div>'

        if trend_class:
            delta_html = (
                f'<span class="{trend_class}">{delta_str}</span>' if delta_str else ""
            )
            pct_html = (
                f'<span class="{trend_class}">{pct_str}</span>' if pct_str else ""
            )
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
            f'<div class="main">{f"{symbol_html} " if symbol_html else ""}{main}</div>'
            f'<div class="sub">{sub_html}</div>'
            "</div>"
        )

    def format_onboarded_month_cell(value, delta, total_value):
        if pd.isna(value):
            return '<div class="cell"><div class="main">-</div></div>'

        share_main = format_share_pct_value(value, total_value)
        onboarded_str = format_number(value, decimals=0)
        total_str = format_number(total_value, decimals=0) if not pd.isna(total_value) else ""
        delta_str = (
            format_number(delta, decimals=0, sign=True) if not pd.isna(delta) else ""
        )

        symbol_class = get_onboarded_share_class(value, total_value)
        show_symbol = share_main != ""
        symbol_html = f'<span class="sym {symbol_class}">●</span>' if show_symbol else ""

        if share_main:
            main_html = (
                f'<div class="main">{f"{symbol_html} " if symbol_html else ""}{share_main}</div>'
            )
        else:
            main_html = '<div class="main">-</div>'

        sub_parts = []
        if onboarded_str or total_str:
            ratio_bits = [bit for bit in (onboarded_str, total_str) if bit]
            if ratio_bits:
                sub_parts.append("/".join(ratio_bits))
        if delta_str:
            sub_parts.append(f"({delta_str})")
        sub_html = " ".join(sub_parts)

        if not sub_html:
            return f'<div class="cell">{main_html}</div>'

        return (
            '<div class="cell">'
            f"{main_html}"
            f'<div class="sub">{sub_html}</div>'
            "</div>"
        )

    def format_window_delta_cell(delta, pct):
        main = format_number(delta, decimals=0, sign=True) if not pd.isna(delta) else ""
        if not main:
            return ""

        trend_class = get_trend_class(delta, pct)
        show_symbol = trend_class in {"pos", "neg"}
        symbol_html = f'<span class="sym {trend_class}">●</span>' if show_symbol else ""

        pct_str = format_pct_value(pct)
        pct_html = f'<span class="{trend_class}">{pct_str}</span>' if pct_str else ""
        sub_html = f'<div class="sub">({pct_html})</div>' if pct_html else ""

        return (
            '<div class="cell">'
            f'<div class="main">{f"{symbol_html} " if symbol_html else ""}{main}</div>'
            f"{sub_html}"
            "</div>"
        )

    header_cells = [
        '<th class="month-col">mese</th>',
        '<th class="total-col">total</th>',
    ] + [f"<th>{html.escape(tenant.lower())}</th>" for tenant in table_state.tenants]
    header_row = f"<tr>{''.join(header_cells)}</tr>"

    body_rows = []
    if table_state.last_30_days is not None:
        last_30 = table_state.last_30_days
        total_window_html = format_window_delta_cell(
            last_30.total_delta,
            last_30.total_pct,
        )

        last_30_cells = [
            f'<th class="month-col">{html.escape(last_30.label)}</th>',
            '<td class="total-col">' f"{total_window_html}" "</td>",
        ]
        for tenant in table_state.tenants:
            cell_html = format_window_delta_cell(
                last_30.tenant_delta.get(tenant, pd.NA),
                last_30.tenant_pct.get(tenant, pd.NA),
            )
            last_30_cells.append(f"<td>{cell_html}</td>")
        body_rows.append(f'<tr class="row-white">{"".join(last_30_cells)}</tr>')

    for idx, month_value in enumerate(table_state.months_desc):
        is_current_month = idx == 0
        row_class = "row-white" if is_current_month else "row-grey"
        if is_current_month:
            month_label = "Mese corrente"
        else:
            month_label = html.escape(format_month_label(month_value.date()))

        total_reference_value = (
            table_state.total_reference_series.at[month_value]
            if metric == "onboarded"
            else None
        )
        row_cells = [
            f'<th class="month-col">{month_label}</th>',
            (
                '<td class="total-col">'
                f"{format_onboarded_month_cell(table_state.total_series.at[month_value], table_state.total_delta.at[month_value], total_reference_value) if metric == 'onboarded' else format_value_cell(table_state.total_series.at[month_value], table_state.total_delta.at[month_value], table_state.total_pct.at[month_value], reference_value=total_reference_value)}"
                "</td>"
            ),
        ]

        for tenant in table_state.tenants:
            reference_value = (
                table_state.reference_matrix.at[month_value, tenant]
                if metric == "onboarded"
                else None
            )
            row_cells.append(
                "<td>"
                f"{format_onboarded_month_cell(table_state.matrix.at[month_value, tenant], table_state.tenant_delta[tenant].at[month_value], reference_value) if metric == 'onboarded' else format_value_cell(table_state.matrix.at[month_value, tenant], table_state.tenant_delta[tenant].at[month_value], table_state.tenant_pct[tenant].at[month_value], reference_value=reference_value)}"
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
