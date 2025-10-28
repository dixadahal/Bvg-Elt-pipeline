# /opt/airflow/dags/ingest_to_staging.py

import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection using Docker Compose service name
engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

# List of raw tables to move into staging
raw_tables = [
    "agency",
    "calendar",
    "calendar_dates",
    "routes",
    "shapes",
    "stops",
    "stop_times",
    "transfers",
    "trips",
]

# Ensure staging schema exists
engine.execute("CREATE SCHEMA IF NOT EXISTS staging;")

# Ingest each table
for table in raw_tables:
    print(f"Loading table '{table}' from raw -> staging...")
    # Read from raw schema
    df = pd.read_sql(f"SELECT * FROM raw.{table}", engine)

    # Write to staging schema, replacing existing table
    df.to_sql(table, engine, schema="staging", if_exists="replace", index=False)

    print(f"'{table}' loaded successfully into staging schema.")

print("All raw tables ingested into staging successfully!")
