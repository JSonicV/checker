import streamlit as st


PAGE_COSTS = "Costi"
PAGE_POD = "Pod"
NAV_PAGE_KEY = "active_page"


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
