from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
}

def run_transformation():
    from processing.bronze_to_silver.bronze_to_silver import run_bronze_to_silver
    run_bronze_to_silver()

with DAG(
    dag_id='bronze_to_silver',
    default_args=default_args,
    description='Daily Bronze to Silver transformation',
    schedule_interval='0 9 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['transformation', 'silver']
) as dag:

    transform = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=run_transformation
    )