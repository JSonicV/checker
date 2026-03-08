from pathlib import Path

import pandas as pd


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
    return Path(__file__).resolve().parents[2] / "db" / db_name


def safe_div(numerator, denominator):
    if denominator in (0, None) or pd.isna(denominator):
        return pd.NA
    return numerator / denominator


def format_month_label(month_start) -> str:
    return f"{MONTH_NAMES_IT[month_start.month].capitalize()} {month_start.year}"
