from dotenv import load_dotenv
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
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
DUCKDB_DATABASE = "database.duckdb"
TABLE_NAME = "aws_costs"

# Intervallo dei dati: ultimi 7 giorni
# END_DATE = datetime.now(UTC).date()
# START_DATE = END_DATE - timedelta(days=31)
# print(START_DATE)


duckdb = get_duckdb_client(DUCKDB_DATABASE)
duckdb.create_table(TABLE_NAME)
duckdb.create_service_map()
duckdb.create_costs_view()

for account in accounts_map.keys():
    costs_client = get_aws_costs_client(account)
    latest_date = duckdb.get_latest_date(TABLE_NAME, account=account)
    if not latest_date or latest_date.date() < datetime.now(UTC).date() - timedelta(days=1):
        start = (
            latest_date.date() - timedelta(days=1)
            if latest_date
            else (datetime.now(UTC) - relativedelta(months=13)).date()
        )
        stop = datetime.now(UTC).date()
        print(account, start, stop)
        costs = costs_client.get_records(start, stop, format="tuple")
        duckdb.insert_many(TABLE_NAME, costs)
print(duckdb.read_table("costs"))
# print(duckdb.get_services_metrics(TABLE_NAME))
