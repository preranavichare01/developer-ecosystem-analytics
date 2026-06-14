from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def run_pypi_ingestion():
    from ingestion.pypi.pypi_ingester import run_pypi_ingestion
    run_pypi_ingestion()

with DAG(
    dag_id='pypi_ingestion',
    default_args=default_args,
    description='Weekly PyPI download stats ingestion',
    schedule_interval='0 8 * * 1',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'pypi', 'bronze']
) as dag:

    ingest = PythonOperator(
        task_id='ingest_pypi_downloads',
        python_callable=run_pypi_ingestion
    )