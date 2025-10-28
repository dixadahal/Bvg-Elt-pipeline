from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from datetime import timedelta
import logging

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_data_quality_checks():
    """Run simple SQL data quality checks directly in Airflow."""
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )

    checks = {
        "row_count_check": "SELECT COUNT(*) FROM warehouse.ridership_fact;",
        "null_station_check": "SELECT COUNT(*) FROM warehouse.ridership_fact WHERE station_id IS NULL;",
        "null_route_check": "SELECT COUNT(*) FROM warehouse.ridership_fact WHERE route_id IS NULL;",
        "null_date_check": "SELECT COUNT(*) FROM warehouse.ridership_fact WHERE date_id IS NULL;",
    }

    for name, query in checks.items():
        result = engine.execute(query).fetchone()[0]
        logging.info(f"{name}: {result}")
        if "row_count" in name and result == 0:
            raise ValueError("❌ Data quality failed: ridership_fact is empty.")
        elif "null_" in name and result > 0:
            raise ValueError(f"❌ Data quality failed: {result} NULL values in {name}.")

    logging.info("✅ All data quality checks passed successfully!")


with DAG(
    dag_id="data_quality_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["data_quality"],
) as dag:

    run_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
    )
