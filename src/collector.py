import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, UTC
from duckdb_client import get_duckdb_client
from aws_costs_client import get_aws_costs_client
from utils import accounts_map

load_dotenv()
# AWS_ROLE_ARN_COSTS_DIGIWATT = os.environ["AWS_ROLE_ARN_COSTS_DIGIWATT"]

# ----------------------------
# CONFIGURAZIONE
# ----------------------------
# COSTS_ROLE_ARN = AWS_ROLE_ARN_COSTS_DIGIWATT
# REGION = "eu-central-1"
DUCKDB_DATABASE = os.environ.get("DUCKDB_DATABASE", "database.duckdb")
TABLE_NAME = "aws_costs"
COSTS_BACKFILL_MONTHS = int(os.environ.get("COSTS_BACKFILL_MONTHS", "12"))

# Intervallo dei dati: ultimi 7 giorni
# END_DATE = datetime.now(UTC).date()
# START_DATE = END_DATE - timedelta(days=31)
# print(START_DATE)


def shift_month_start(value, months_delta: int):
    month_index = (value.year * 12) + (value.month - 1) + months_delta
    year, month_zero_based = divmod(month_index, 12)
    return value.replace(year=year, month=month_zero_based + 1, day=1)


def main() -> None:
    duckdb = get_duckdb_client(DUCKDB_DATABASE)
    try:
        duckdb.create_table(TABLE_NAME)
        duckdb.create_service_map()
        duckdb.create_costs_view()

        today = datetime.now(UTC).date()
        month_start = today.replace(day=1)
        query_end = today + timedelta(days=1)
        backfill_start = shift_month_start(month_start, -COSTS_BACKFILL_MONTHS)

        for account in accounts_map.keys():
            costs_client = get_aws_costs_client(account)
            latest_date = duckdb.get_latest_date(TABLE_NAME, account=account)
            start_candidate = (
                latest_date.date() - timedelta(days=1)
                if latest_date
                else backfill_start
            )
            start = min(start_candidate, month_start)
            stop = query_end
            print(account, start, stop)
            costs = costs_client.get_records(start, stop, format="tuple")
            duckdb.insert_many(TABLE_NAME, costs)

        summary = duckdb.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                MIN(date) AS min_date,
                MAX(date) AS max_date
            FROM {TABLE_NAME}
            """
        )
        duckdb.checkpoint()
        print(
            "Cost collector completato:"
            f" database={duckdb.db_path},"
            f" accounts={len(accounts_map)},"
            f" righe={int(summary.iloc[0]['total_rows']) if not summary.empty else 0},"
            f" intervallo={summary.iloc[0]['min_date']}->{summary.iloc[0]['max_date']}"
        )
    finally:
        duckdb.close()


if __name__ == "__main__":
    main()
