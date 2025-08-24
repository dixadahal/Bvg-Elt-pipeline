from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define DAG
with DAG(
    dag_id='gtfs_ingestion',
    default_args=default_args,
    description='GTFS Data Ingestion DAG with Warehouse Load',
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 17),
    catchup=False,
    tags=['gtfs'],
) as dag:

    # Step 1: Load raw GTFS
    def load_raw():
        subprocess.run(["python", "/opt/airflow/dags/ingest_to_raw.py"], check=True)

    # Step 2: Create staging tables
    def create_staging():
        subprocess.run(["python", "/opt/airflow/dags/create_staging.py"], check=True)

    # Step 3: Ingest data into staging
    def ingest_to_staging():
        subprocess.run(["python", "/opt/airflow/dags/ingest_to_staging.py"], check=True)

    # Step 4: Load warehouse (dimensions + fact)
    def load_warehouse():
        subprocess.run(["python", "/opt/airflow/dags/load_warehouse.py"], check=True)

    # Define tasks
    t1 = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw
    )

    t2 = PythonOperator(
        task_id='create_staging',
        python_callable=create_staging
    )

    t3 = PythonOperator(
        task_id='ingest_to_staging',
        python_callable=ingest_to_staging
    )

    t4 = PythonOperator(
        task_id='load_warehouse',
        python_callable=load_warehouse
    )

    # Set task dependencies
    t1 >> t2 >> t3 >> t4
