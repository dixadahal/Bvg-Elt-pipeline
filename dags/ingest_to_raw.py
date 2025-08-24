import os
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
landing_dir = "/data_lake/landing"

# Ensure schema exists
engine.execute("CREATE SCHEMA IF NOT EXISTS raw;")

for file_name in os.listdir(landing_dir):
    if file_name.lower().endswith(".txt"):  # safe lowercase check
        file_path = os.path.join(landing_dir, file_name)
        df = pd.read_csv(file_path, sep=",")
        table_name = os.path.splitext(file_name)[0].lower()
        df.to_sql(table_name, engine, schema="raw", if_exists="replace", index=False)
        print(f"{table_name} loaded to raw schema")
