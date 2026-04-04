import duckdb
import pandas as pd
from utils import service_map
from runtime_config import ensure_db_parent


class DuckDBClient:
    def __init__(self, database):
        self.db_path = ensure_db_parent(database)
        self.conn = duckdb.connect(str(self.db_path))

    def create_table(self, table_name):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date DATE,
                account VARCHAR,
                service VARCHAR,
                amount DOUBLE,
                UNIQUE(date, account, service)
            )
        """)

    def create_pod_trend_table(self, table_name):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                month_start DATE,
                tenant VARCHAR,
                source_backend VARCHAR,
                monthly_delta BIGINT,
                total_pods BIGINT,
                monthly_onboarded_delta BIGINT,
                onboarded_pods BIGINT,
                updated_at TIMESTAMP,
                UNIQUE(month_start, tenant)
            )
        """)

    def create_pod_daily_trend_table(self, table_name):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date DATE,
                tenant VARCHAR,
                source_backend VARCHAR,
                daily_delta BIGINT,
                total_pods BIGINT,
                daily_onboarded_delta BIGINT,
                onboarded_pods BIGINT,
                updated_at TIMESTAMP,
                UNIQUE(date, tenant)
            )
        """)

    def execute(self, query, params=None):
        if params is None:
            return self.conn.execute(query).df()
        return self.conn.execute(query, params).df()

    def read_table(self, table_name, **kwargs):
        columns = kwargs.get("columns", "*")
        query = f"SELECT {columns} FROM {table_name}"
        return self.execute(query)

    def get_latest_date(self, table_name, account=None):
        if account:
            query = f"SELECT MAX(date) as latest_date FROM {table_name} WHERE account = ?"
            df = self.execute(query, [account])
        else:
            query = f"SELECT MAX(date) as latest_date FROM {table_name}"
            df = self.execute(query)
        return None if pd.isna(df.iloc[0, 0]) else df.iloc[0, 0]

    def get_latest_month(self, table_name):
        query = f"SELECT MAX(month_start) as latest_month FROM {table_name}"
        df = self.execute(query)
        return None if pd.isna(df.iloc[0, 0]) else df.iloc[0, 0]

    def insert_many(self, table_name, values):
        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """
        columns_df = self.execute(query)
        columns = ", ".join(columns_df["column_name"].tolist())

        placeholders = ", ".join(["?"] * len(columns_df))
        query = (
            f"INSERT OR REPLACE INTO {table_name}({columns}) VALUES ({placeholders})"
        )
        self.conn.executemany(query, values)

    def create_service_map(self):
        query = """
            CREATE TABLE IF NOT EXISTS service_map (
                raw VARCHAR PRIMARY KEY,
                label VARCHAR
            );
        """
        self.execute(query)
        self.insert_many("service_map", list(service_map.items()))

    def create_costs_view(self):
        query = """
            CREATE OR REPLACE VIEW costs AS
            SELECT
                c.date,
                c.account,
                COALESCE(m.label, c.service) AS service,
                c.amount
            FROM aws_costs c
            LEFT JOIN service_map m
                ON m.raw = c.service;
        """
        self.execute(query)

    def get_services_metrics(self, table_name, anchor_date=None):
        if anchor_date is None:
            last_cte = f"SELECT MAX(date) AS last_date FROM {table_name}"
            params = None
        else:
            last_cte = "SELECT CAST(? AS DATE) AS last_date"
            params = [anchor_date]

        query = f"""
            WITH last AS (
                {last_cte}
            ),
            params AS (
                SELECT
                    last_date,
                    date_trunc('month', last_date) AS month_start,
                    EXTRACT(day FROM last_date) AS dom
                FROM last
            ),
            mtd AS (
                SELECT account, service, SUM(amount) AS mtd
                FROM {table_name}, params
                WHERE date BETWEEN month_start AND last_date
                GROUP BY account, service
            ),
            prev_mtd AS (
                SELECT account, service, SUM(amount) AS prev_mtd
                FROM {table_name}, params
                WHERE date BETWEEN CAST(month_start - INTERVAL '1 month' AS DATE)
                    AND CAST(
                        LEAST(
                            last_day(month_start - INTERVAL '1 month'),
                            (month_start - INTERVAL '1 month') + (dom - 1) * INTERVAL '1 day'
                        ) AS DATE
                    )
                GROUP BY account, service
            ),
            prev2_mtd AS (
                SELECT account, service, SUM(amount) AS prev2_mtd
                FROM {table_name}, params
                WHERE date BETWEEN CAST(month_start - INTERVAL '2 months' AS DATE)
                    AND CAST(
                        LEAST(
                            last_day(month_start - INTERVAL '2 months'),
                            (month_start - INTERVAL '2 months') + (dom - 1) * INTERVAL '1 day'
                        ) AS DATE
                    )
                GROUP BY account, service
            ),
            prev3_mtd AS (
                SELECT account, service, SUM(amount) AS prev3_mtd
                FROM {table_name}, params
                WHERE date BETWEEN CAST(month_start - INTERVAL '3 months' AS DATE)
                    AND CAST(
                        LEAST(
                            last_day(month_start - INTERVAL '3 months'),
                            (month_start - INTERVAL '3 months') + (dom - 1) * INTERVAL '1 day'
                        ) AS DATE
                    )
                GROUP BY account, service
            ),
            prev4_mtd AS (
                SELECT account, service, SUM(amount) AS prev4_mtd
                FROM {table_name}, params
                WHERE date BETWEEN CAST(month_start - INTERVAL '4 months' AS DATE)
                    AND CAST(
                        LEAST(
                            last_day(month_start - INTERVAL '4 months'),
                            (month_start - INTERVAL '4 months') + (dom - 1) * INTERVAL '1 day'
                        ) AS DATE
                    )
                GROUP BY account, service
            ),
            prev5_mtd AS (
                SELECT account, service, SUM(amount) AS prev5_mtd
                FROM {table_name}, params
                WHERE date BETWEEN CAST(month_start - INTERVAL '5 months' AS DATE)
                    AND CAST(
                        LEAST(
                            last_day(month_start - INTERVAL '5 months'),
                            (month_start - INTERVAL '5 months') + (dom - 1) * INTERVAL '1 day'
                        ) AS DATE
                    )
                GROUP BY account, service
            ),
            avg6 AS (
                SELECT account, service, AVG(month_sum) AS avg6
                FROM (
                    SELECT
                        account,
                        service,
                        date_trunc('month', date) AS m,
                        SUM(amount) AS month_sum
                    FROM {table_name}, params
                    WHERE date >= month_start - INTERVAL '6 months'
                        AND date < month_start
                        AND EXTRACT(day FROM date) <= dom
                    GROUP BY account, service, m
                ) t
                GROUP BY account, service
            ),
            avg12 AS (
                SELECT account, service, AVG(month_sum) AS avg12
                FROM (
                    SELECT
                        account,
                        service,
                        date_trunc('month', date) AS m,
                        SUM(amount) AS month_sum
                    FROM {table_name}, params
                    WHERE date >= month_start - INTERVAL '12 months'
                        AND date < month_start
                        AND EXTRACT(day FROM date) <= dom
                    GROUP BY account, service, m
                ) t
                GROUP BY account, service
            )
            SELECT
                mtd.account,
                mtd.service,
                mtd.mtd,
                prev_mtd.prev_mtd,
                prev2_mtd.prev2_mtd,
                prev3_mtd.prev3_mtd,
                prev4_mtd.prev4_mtd,
                prev5_mtd.prev5_mtd,
                avg6.avg6,
                avg12.avg12,
                (mtd.mtd - prev_mtd.prev_mtd) AS delta_prev,
                (prev_mtd.prev_mtd - prev2_mtd.prev2_mtd) AS delta_prev2,
                (prev2_mtd.prev2_mtd - prev3_mtd.prev3_mtd) AS delta_prev3,
                (prev3_mtd.prev3_mtd - prev4_mtd.prev4_mtd) AS delta_prev4,
                (prev4_mtd.prev4_mtd - prev5_mtd.prev5_mtd) AS delta_prev5,
                (mtd.mtd - avg6.avg6) AS delta_avg6,
                (mtd.mtd - avg12.avg12) AS delta_avg12,
                (mtd.mtd - prev_mtd.prev_mtd) / NULLIF(prev_mtd.prev_mtd, 0) AS pct_prev,
                (prev_mtd.prev_mtd - prev2_mtd.prev2_mtd) / NULLIF(prev2_mtd.prev2_mtd, 0) AS pct_prev2,
                (prev2_mtd.prev2_mtd - prev3_mtd.prev3_mtd) / NULLIF(prev3_mtd.prev3_mtd, 0) AS pct_prev3,
                (prev3_mtd.prev3_mtd - prev4_mtd.prev4_mtd) / NULLIF(prev4_mtd.prev4_mtd, 0) AS pct_prev4,
                (prev4_mtd.prev4_mtd - prev5_mtd.prev5_mtd) / NULLIF(prev5_mtd.prev5_mtd, 0) AS pct_prev5,
                (mtd.mtd - avg6.avg6) / NULLIF(avg6.avg6, 0) AS pct_avg6,
                (mtd.mtd - avg12.avg12) / NULLIF(avg12.avg12, 0) AS pct_avg12
            FROM mtd
            LEFT JOIN prev_mtd ON prev_mtd.account = mtd.account AND prev_mtd.service = mtd.service
            LEFT JOIN prev2_mtd ON prev2_mtd.account = mtd.account AND prev2_mtd.service = mtd.service
            LEFT JOIN prev3_mtd ON prev3_mtd.account = mtd.account AND prev3_mtd.service = mtd.service
            LEFT JOIN prev4_mtd ON prev4_mtd.account = mtd.account AND prev4_mtd.service = mtd.service
            LEFT JOIN prev5_mtd ON prev5_mtd.account = mtd.account AND prev5_mtd.service = mtd.service
            LEFT JOIN avg6     ON avg6.account = mtd.account AND avg6.service = mtd.service
            LEFT JOIN avg12    ON avg12.account = mtd.account AND avg12.service = mtd.service
            ORDER BY mtd.account, mtd.service;
        """
        return self.execute(query, params=params)

    def get_services_metrics_for_month(self, table_name, month_start):
        query = f"""
            WITH params AS (
                SELECT CAST(? AS DATE) AS month_start
            ),
            mtd AS (
                SELECT account, service, SUM(amount) AS mtd
                FROM {table_name}, params
                WHERE date >= month_start
                    AND date < month_start + INTERVAL '1 month'
                GROUP BY account, service
            ),
            prev_mtd AS (
                SELECT account, service, SUM(amount) AS prev_mtd
                FROM {table_name}, params
                WHERE date >= month_start - INTERVAL '1 month'
                    AND date < month_start
                GROUP BY account, service
            ),
            prev2_mtd AS (
                SELECT account, service, SUM(amount) AS prev2_mtd
                FROM {table_name}, params
                WHERE date >= month_start - INTERVAL '2 months'
                    AND date < month_start - INTERVAL '1 month'
                GROUP BY account, service
            ),
            prev3_mtd AS (
                SELECT account, service, SUM(amount) AS prev3_mtd
                FROM {table_name}, params
                WHERE date >= month_start - INTERVAL '3 months'
                    AND date < month_start - INTERVAL '2 months'
                GROUP BY account, service
            ),
            prev4_mtd AS (
                SELECT account, service, SUM(amount) AS prev4_mtd
                FROM {table_name}, params
                WHERE date >= month_start - INTERVAL '4 months'
                    AND date < month_start - INTERVAL '3 months'
                GROUP BY account, service
            ),
            prev5_mtd AS (
                SELECT account, service, SUM(amount) AS prev5_mtd
                FROM {table_name}, params
                WHERE date >= month_start - INTERVAL '5 months'
                    AND date < month_start - INTERVAL '4 months'
                GROUP BY account, service
            ),
            avg6 AS (
                SELECT account, service, AVG(month_sum) AS avg6
                FROM (
                    SELECT
                        account,
                        service,
                        date_trunc('month', date) AS m,
                        SUM(amount) AS month_sum
                    FROM {table_name}, params
                    WHERE date >= month_start - INTERVAL '6 months'
                        AND date < month_start
                    GROUP BY account, service, m
                ) t
                GROUP BY account, service
            ),
            avg12 AS (
                SELECT account, service, AVG(month_sum) AS avg12
                FROM (
                    SELECT
                        account,
                        service,
                        date_trunc('month', date) AS m,
                        SUM(amount) AS month_sum
                    FROM {table_name}, params
                    WHERE date >= month_start - INTERVAL '12 months'
                        AND date < month_start
                    GROUP BY account, service, m
                ) t
                GROUP BY account, service
            )
            SELECT
                mtd.account,
                mtd.service,
                mtd.mtd,
                prev_mtd.prev_mtd,
                prev2_mtd.prev2_mtd,
                prev3_mtd.prev3_mtd,
                prev4_mtd.prev4_mtd,
                prev5_mtd.prev5_mtd,
                avg6.avg6,
                avg12.avg12,
                (mtd.mtd - prev_mtd.prev_mtd) AS delta_prev,
                (prev_mtd.prev_mtd - prev2_mtd.prev2_mtd) AS delta_prev2,
                (prev2_mtd.prev2_mtd - prev3_mtd.prev3_mtd) AS delta_prev3,
                (prev3_mtd.prev3_mtd - prev4_mtd.prev4_mtd) AS delta_prev4,
                (prev4_mtd.prev4_mtd - prev5_mtd.prev5_mtd) AS delta_prev5,
                (mtd.mtd - avg6.avg6) AS delta_avg6,
                (mtd.mtd - avg12.avg12) AS delta_avg12,
                (mtd.mtd - prev_mtd.prev_mtd) / NULLIF(prev_mtd.prev_mtd, 0) AS pct_prev,
                (prev_mtd.prev_mtd - prev2_mtd.prev2_mtd) / NULLIF(prev2_mtd.prev2_mtd, 0) AS pct_prev2,
                (prev2_mtd.prev2_mtd - prev3_mtd.prev3_mtd) / NULLIF(prev3_mtd.prev3_mtd, 0) AS pct_prev3,
                (prev3_mtd.prev3_mtd - prev4_mtd.prev4_mtd) / NULLIF(prev4_mtd.prev4_mtd, 0) AS pct_prev4,
                (prev4_mtd.prev4_mtd - prev5_mtd.prev5_mtd) / NULLIF(prev5_mtd.prev5_mtd, 0) AS pct_prev5,
                (mtd.mtd - avg6.avg6) / NULLIF(avg6.avg6, 0) AS pct_avg6,
                (mtd.mtd - avg12.avg12) / NULLIF(avg12.avg12, 0) AS pct_avg12
            FROM mtd
            LEFT JOIN prev_mtd ON prev_mtd.account = mtd.account AND prev_mtd.service = mtd.service
            LEFT JOIN prev2_mtd ON prev2_mtd.account = mtd.account AND prev2_mtd.service = mtd.service
            LEFT JOIN prev3_mtd ON prev3_mtd.account = mtd.account AND prev3_mtd.service = mtd.service
            LEFT JOIN prev4_mtd ON prev4_mtd.account = mtd.account AND prev4_mtd.service = mtd.service
            LEFT JOIN prev5_mtd ON prev5_mtd.account = mtd.account AND prev5_mtd.service = mtd.service
            LEFT JOIN avg6     ON avg6.account = mtd.account AND avg6.service = mtd.service
            LEFT JOIN avg12    ON avg12.account = mtd.account AND avg12.service = mtd.service
            ORDER BY mtd.account, mtd.service;
        """
        return self.execute(query, params=[month_start])

    def get_available_month_anchors(self, table_name):
        query = f"""
            SELECT
                account,
                CAST(date_trunc('month', date) AS DATE) AS month_start,
                CAST(MAX(date) AS DATE) AS anchor_date
            FROM {table_name}
            GROUP BY account, date_trunc('month', date)
            ORDER BY account, month_start DESC;
        """
        return self.execute(query)

    def get_pod_monthly_trend(self, table_name, months=12):
        query = f"""
            SELECT
                month_start,
                tenant,
                source_backend,
                monthly_delta,
                total_pods,
                monthly_onboarded_delta,
                onboarded_pods,
                updated_at
            FROM {table_name}
            WHERE month_start >= CAST(
                date_trunc('month', current_date) - INTERVAL '{months - 1} months'
                AS DATE
            )
            ORDER BY month_start, tenant;
        """
        return self.execute(query)

    def get_pod_daily_trend(self, table_name, days=60):
        query = f"""
            SELECT
                date,
                tenant,
                source_backend,
                daily_delta,
                total_pods,
                daily_onboarded_delta,
                onboarded_pods,
                updated_at
            FROM {table_name}
            WHERE date >= CAST(current_date - INTERVAL '{days - 1} days' AS DATE)
            ORDER BY date, tenant;
        """
        return self.execute(query)

    def checkpoint(self):
        self.conn.execute("CHECKPOINT")

    def close(self):
        self.conn.close()


def get_duckdb_client(database):
    return DuckDBClient(database)
