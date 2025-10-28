# -------------------------------
# Test Email DAG (added at end)
# -------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'email': ['dahaldixa98@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

def fail_task():
    raise Exception("❌ This is a test failure for email alert verification!")

with DAG(
    dag_id='email_test_dag',
    default_args=default_args,
    description='A simple DAG to test email notifications',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'email'],
) as dag:

    test_email_alert = PythonOperator(
        task_id='trigger_email',
        python_callable=fail_task,
    )
