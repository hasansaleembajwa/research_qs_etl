from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from your_etl_script import run_etl  # Import your main ETL function

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'research_questions_etl',
    default_args=default_args,
    description='ETL process for research questions',
    schedule_interval=timedelta(days=1),
)

extract_transform_load = PythonOperator(
    task_id='extract_transform_load',
    python_callable=run_etl,
    dag=dag,
)

run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /path/to/dbt/project && dbt run',
    dag=dag,
)

extract_transform_load >> run_dbt