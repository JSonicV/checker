import streamlit as st


def inject_layout_styles() -> None:
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
