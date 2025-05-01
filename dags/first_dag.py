from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator   

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'first_dag',
    default_args=default_args,
    schedule=timedelta(hours=1),
    catchup=False 
)

run_script = BashOperator(
    task_id='run_script',
    bash_command='python3 /opt/airflow/python_scripts/main.py',
    dag=dag,
)
