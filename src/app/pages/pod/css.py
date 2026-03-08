import streamlit as st


def inject_styles() -> None:
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
