from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

raw_tables = ["agency", "calendar", "calendar_dates", "routes", "shapes", 
              "stop_times", "stops", "transfers", "trips"]

for table in raw_tables:
    engine.execute(f"""
        CREATE TABLE IF NOT EXISTS staging.{table} AS
        SELECT * FROM raw.{table} WHERE 1=0;
    """)
    print(f"{table} staging table created")
