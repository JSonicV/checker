import html
from io import BytesIO

import pandas as pd
import streamlit as st

try:
    from app.page_shared import get_db_path
    from app.pages.costs.css import inject_styles
    from app.pages.costs.logic import (
        ROW_DEFS,
        ROW_LABELS,
        build_account_matrix,
        build_period_options,
        get_accounts,
        get_metric_cols,
        is_valid_table_name,
        load_available_month_anchors,
        load_data,
        load_data_for_anchor,
        normalize_costs_dataframe,
        normalize_month_anchors,
        period_label,
    )
except ModuleNotFoundError:
    from page_shared import get_db_path
    from pages.costs.css import inject_styles
    from pages.costs.logic import (
        ROW_DEFS,
        ROW_LABELS,
        build_account_matrix,
        build_period_options,
        get_accounts,
        get_metric_cols,
        is_valid_table_name,
        load_available_month_anchors,
        load_data,
        load_data_for_anchor,
        normalize_costs_dataframe,
        normalize_month_anchors,
        period_label,
    )


def render_page(db_name: str, table_name: str) -> None:
    st.title("AWS Costs Dashboard")

    if not is_valid_table_name(table_name):
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

    df = normalize_costs_dataframe(df)
    month_anchors = normalize_month_anchors(
        load_available_month_anchors(db_name, table_name)
    )
    accounts = get_accounts(df)

    inject_styles()

    metric_cols = get_metric_cols(df)
    if not metric_cols:
        st.info("Nessuna metrica disponibile.")
        return

    def symbol(_val):
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

    def build_download_payload(account_df: pd.DataFrame):
        if account_df.empty:
            return ["Metric", "Total"], [], []

        service_rows, service_order, totals = build_account_matrix(account_df, metric_cols)
        columns = ["Metric", "Total"] + service_order
        value_rows = []
        pct_rows = []
        for metric, _delta_col, pct_col in ROW_DEFS:
            label = ROW_LABELS.get(metric, metric)
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
        period_options, month_to_anchor = build_period_options(account_months)

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
                    selected_account_df = historical_df[historical_df["account"] == account]
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

        selected_service_rows, selected_services, selected_totals = build_account_matrix(
            selected_account_df, metric_cols
        )

        header_cells = [
            '<th class="metric-col">Metric (MTD)</th>',
            '<th class="total-col">Total</th>',
        ] + [f"<th>{html.escape(service)}</th>" for service in selected_services]
        header_row = f"<tr>{''.join(header_cells)}</tr>"

        body_rows = []
        for metric, delta_col, pct_col in ROW_DEFS:
            label = html.escape(ROW_LABELS.get(metric, metric))
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
