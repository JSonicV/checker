import streamlit as st


def inject_styles() -> None:
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
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) > div[data-testid="stElementContainer"]:has(div[data-testid="stButton"]) {
            flex: 0 0 auto;
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
        div[data-testid="stButton"],
        div[data-testid="stDownloadButton"] {
            display: flex;
            justify-content: flex-end;
        }
        div[data-testid="stButton"] button,
        div[data-testid="stButton"] button span,
        div[data-testid="stDownloadButton"] button,
        div[data-testid="stDownloadButton"] button span {
            color: #FFFFFF !important;
        }
        div[data-testid="stButton"] button *,
        div[data-testid="stDownloadButton"] button * {
            color: #FFFFFF !important;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="secondary"] {
            background: #FFFFFF !important;
            border: 1px solid #D1D5DB !important;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="secondary"],
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="secondary"] *,
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="secondary"] span {
            color: #9CA3AF !important;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="primary"] {
            background: #000000 !important;
            border: 1px solid #000000 !important;
        }
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="primary"],
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="primary"] *,
        div[data-testid="stVerticalBlock"]:has(div[data-testid="stSelectbox"]):has(div[data-testid="stDownloadButton"]) div[data-testid="stButton"] button[kind="primary"] span {
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
