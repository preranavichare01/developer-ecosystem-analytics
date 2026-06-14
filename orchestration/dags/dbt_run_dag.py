from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
}

with DAG(
    dag_id='dbt_gold_layer',
    default_args=default_args,
    description='Daily dbt Silver to Gold transformation',
    schedule_interval='0 10 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['transformation', 'gold', 'dbt']
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/transformation/dev_ecosystem && dbt run --profiles-dir /opt/airflow/config',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/transformation/dev_ecosystem && dbt test --profiles-dir /opt/airflow/config',
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/transformation/dev_ecosystem && dbt snapshot --profiles-dir /opt/airflow/config',
    )

    dbt_run >> dbt_test >> dbt_snapshot