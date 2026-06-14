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

def run_github_ingestion():
    from ingestion.github.github_ingester import run_github_ingestion
    run_github_ingestion()

with DAG(
    dag_id='github_ingestion',
    default_args=default_args,
    description='Daily GitHub repo metrics ingestion',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'github', 'bronze']
) as dag:

    ingest = PythonOperator(
        task_id='ingest_github_repos',
        python_callable=run_github_ingestion
    )