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

def run_hn_ingestion():
    from ingestion.hackernews.hn_ingester import run_hn_ingestion
    run_hn_ingestion()

with DAG(
    dag_id='hackernews_ingestion',
    default_args=default_args,
    description='Daily HackerNews stories ingestion',
    schedule_interval='0 7 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ingestion', 'hackernews', 'bronze']
) as dag:

    ingest = PythonOperator(
        task_id='ingest_hn_stories',
        python_callable=run_hn_ingestion
    )