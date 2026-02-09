from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings for the DAG
default_args = {
    'owner': 'aabidh',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'retail_pipeline_dag',
    default_args=default_args,
    schedule='@daily',  # Run once a day
    catchup=False                # Don't run for past dates
)

# Task 1: Run extract.py
t1 = BashOperator(
    task_id='extract',
    bash_command='python3 ~/retail_pipeline/scripts/extract.py',
    dag=dag,
)

# Task 2: Run transform.py
t2 = BashOperator(
    task_id='transform',
    bash_command='python3 ~/retail_pipeline/scripts/transform.py',
    dag=dag,
)

# Task 3: Run load.py
t3 = BashOperator(
    task_id='load',
    bash_command='python3 ~/retail_pipeline/scripts/load.py',
    dag=dag,
)

# Define the Order
t1 >> t2 >> t3