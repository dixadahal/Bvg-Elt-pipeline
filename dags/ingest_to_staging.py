# /opt/airflow/dags/ingest_to_staging.py

import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection
engine = create_engine('postgresql+psycopg2://airflow:airflow@assign-postgres-1:5432/airflow')

# List of tables to move from raw -> staging
tables = [
    "agency",
    "calendar",
    "calendar_dates",
    "routes",
    "shapes",
    "stops",
    "stop_times",
    "transfers",
    "trips"
]

for table in tables:
    print(f"Loading {table} from raw to staging...")
    # Read from raw schema
    df = pd.read_sql(f"SELECT * FROM raw.{table}", engine)
    
    # Write to staging schema
    df.to_sql(table, engine, schema='staging', if_exists='replace', index=False)
    
    print(f"{table} loaded to staging schema.")

print("All tables ingested to staging successfully!")
