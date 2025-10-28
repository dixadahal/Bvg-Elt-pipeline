from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'email': ['dahaldixa98@gmail.com'],  # your email
    'email_on_failure': True,             # send email if task fails
    'email_on_retry': False,
}


with DAG(
    dag_id='gtfs_ingestion',
    default_args=default_args,
    description='GTFS Data Ingestion DAG with Warehouse Load and Data Quality Checks',
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 17),
    catchup=False,
    tags=['gtfs'],
) as dag:

    # Step 1: Load raw GTFS (network/download → prone to failure)
    def load_raw():
        subprocess.run(["python", "/opt/airflow/dags/ingest_to_raw.py"], check=True)

    # Step 2: Create staging tables (usually stable, no extra retries needed)
    def create_staging():
        subprocess.run(["python", "/opt/airflow/dags/create_staging.py"], check=True)

    # Step 3: Ingest data into staging (I/O, sometimes fails)
    def ingest_to_staging():
        subprocess.run(["python", "/opt/airflow/dags/ingest_to_staging.py"], check=True)

    # Step 4: Load warehouse (joins/transformations, stable)
    def load_warehouse():
        subprocess.run(["python", "/opt/airflow/dags/load_warehouse.py"], check=True)

    # Step 5: Data quality checks
    def run_data_quality():
        result = subprocess.run(
            ["python", "/opt/airflow/dags/data_quality_checks.py"],
            capture_output=True,
            text=True
        )
        print("=== DATA QUALITY STDOUT ===")
        print(result.stdout)
        print("=== DATA QUALITY STDERR ===")
        print(result.stderr)
        if result.returncode != 0:
            raise Exception("Data quality checks failed!")

    # Define tasks (custom retries for unstable steps)
    t1 = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw,
        retries=5,  # More retries since download may fail
        retry_delay=timedelta(minutes=2),
    )

    t2 = PythonOperator(
        task_id='create_staging',
        python_callable=create_staging
    )

    t3 = PythonOperator(
        task_id='ingest_to_staging',
        python_callable=ingest_to_staging,
        retries=4,  # Some retries, e.g. DB might be busy
        retry_delay=timedelta(minutes=3),
    )

    t4 = PythonOperator(
        task_id='load_warehouse',
        python_callable=load_warehouse
    )

    t5 = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_data_quality
    )

    # DAG dependencies
    t1 >> t2 >> t3 >> t4 >> t5
