# Load data to postgres

import pandas as pd
from sqlalchemy import create_engine, text
import os

# Lets use 'localhost' when running locally, and 'postgres' when run inside Docker

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_CONN = f"postgresql://airflow:airflow@{DB_HOST}:5432/airflow"
# DB_CONN = "postgresql://airflow:airflow@postgres:5432/airflow"

def get_engine():
    return create_engine(DB_CONN)

def create_staging_schema(engine):
    """Create staging schema and tables if they don't exist"""
    statements = [
        """CREATE SCHEMA IF NOT EXISTS staging""",
        """
        CREATE TABLE IF NOT EXISTS staging.employees (
            employee_id     VARCHAR PRIMARY KEY,
            first_name      VARCHAR,
            last_name       VARCHAR,
            email           VARCHAR,
            country_code    VARCHAR(2),
            department      VARCHAR,
            hire_date       DATE,
            salary_usd      NUMERIC(12,2),
            is_active       BOOLEAN,
            created_at      TIMESTAMP,
            _loaded_at      TIMESTAMP DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.pay_periods (
            period_id       VARCHAR PRIMARY KEY,
            period_start    DATE,
            period_end      DATE,
            pay_date        DATE,
            period_year     INT,
            period_month    INT,
            _loaded_at      TIMESTAMP DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS staging.transactions (
            transaction_id  VARCHAR PRIMARY KEY,
            employee_id     VARCHAR,
            period_id       VARCHAR,
            gross_pay_usd   NUMERIC(12,2),
            deductions_usd  NUMERIC(12,2),
            net_pay_usd     NUMERIC(12,2),
            currency_code   VARCHAR(3),
            status          VARCHAR,
            processed_at    TIMESTAMP,
            _loaded_at      TIMESTAMP DEFAULT NOW()
        )
        """
    ]
    # SQLAlchemy 2.x requires text() wrapper for raw SQL strings
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    print("Staging schema and tables created.")

def load_csv(filepath, table_name, engine):
    df = pd.read_csv(filepath)

    # Add _loaded_at metadata column
    from datetime import datetime
    df['_loaded_at'] = datetime.utcnow()

    # Drop with CASCADE first to remove dependent views
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS staging.{table_name} CASCADE"))

    df.to_sql(
        table_name,
        engine,
        schema="staging",
        if_exists="replace",
        index=False
    )
    print(f"  Loaded {len(df)} rows into staging.{table_name}")

if __name__ == "__main__":
    # Check Docker is running and postgres container is up
    engine = get_engine()

    print("Creating staging schema...")
    create_staging_schema(engine)

    data_dir = os.environ.get("DATA_OUTPUT_DIR",
           os.path.join(os.path.dirname(__file__), "..", "data"))
    print("Loading CSV files...")
    load_csv(os.path.join(data_dir, "employees.csv"),   "employees",   engine)
    load_csv(os.path.join(data_dir, "pay_periods.csv"), "pay_periods", engine)
    load_csv(os.path.join(data_dir, "transactions.csv"),"transactions", engine)
    print("\nAll data loaded successfully!")