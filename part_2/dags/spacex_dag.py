from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess


# define the dag coordinator arguments 
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# initialise the dag itself 
# this pipelie includes the script for moving and transforming our data to AWS Redshift along with some basic tests
dag = DAG(
    'spacex_pipeline_with_separate_tests',
    default_args=default_args,
    description='Pipeline with separate test scripts',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# run the first script that send data to S3
run_pipeline = BashOperator(
    task_id='run_send_to_S3',
    bash_command='python /path/to/send_to_S3.py',
    dag=dag,
)

# Task to copy data from S3 to Redshift with validation
copy_to_redshift = BashOperator(
    task_id='copy_to_redshift',
    bash_command='python /path/to/redshift_copy_script.py',
    dag=dag,
)


#run the schema and count tests
run_tests = BashOperator(
    task_id='run_tests',
    bash_command='python /path/to/tests/test_row_counts.py && python /path/to/tests/test_schema.py',
    dag=dag,
)

# run the dbt models to produce the analytical tables
run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command='dbt run --project-dir /path/to/part_2/dbt',
    dag=dag,
)

# run the dbt preconfigured tests 
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command='dbt test --project-dir /path/to/part_2/dbt',
    dag=dag,
)


run_pipeline >> copy_to_redshift >> run_tests >> run_dbt_models >> run_dbt_tests