import os
import sys
from pathlib import Path

import streamlit as st

SRC_DIR = Path(__file__).resolve().parent.parent
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

try:
    from app.layout import (
        PAGE_COSTS,
        PAGE_POD,
        inject_layout_styles,
        render_sidebar_navigation,
    )
    from app.pages.costs.ui import render_page as render_costs_page
    from app.pages.pod.ui import render_page as render_pod_page
except ModuleNotFoundError:
    from layout import (
        PAGE_COSTS,
        PAGE_POD,
        inject_layout_styles,
        render_sidebar_navigation,
    )
    from pages.costs.ui import render_page as render_costs_page
    from pages.pod.ui import render_page as render_pod_page


DEFAULT_DB_NAME = os.environ.get("DUCKDB_DATABASE", "database.duckdb")
DEFAULT_COSTS_TABLE = os.environ.get("DUCKDB_TABLE", "costs")
DEFAULT_POD_TABLE = os.environ.get("DUCKDB_POD_TABLE", "pod_monthly_trend")


def main() -> None:
    st.set_page_config(page_title="AWS Costs Dashboard", layout="wide")
    inject_layout_styles()

    selected_page = render_sidebar_navigation()

    if selected_page == PAGE_POD:
        render_pod_page(DEFAULT_DB_NAME, DEFAULT_POD_TABLE)
        return

    if selected_page == PAGE_COSTS:
        render_costs_page(DEFAULT_DB_NAME, DEFAULT_COSTS_TABLE)
        return

    st.error("Pagina non riconosciuta.")


if __name__ == "__main__":
    main()
