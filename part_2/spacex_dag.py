from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

# Importing the functions from your existing script
from part_2.scripts.send_to_s3 import save_spacex_launches, create_launches_dataframe, process_fairings_and_links
from part_2.scripts.send_to_s3 import process_cores, process_failures, process_payloads, validate_and_clean, upload_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'spacex_pipeline',
    default_args=default_args,
    description='A pipeline for processing SpaceX launches data',
    schedule_interval='@daily',  # Runs daily, you can customize this
    start_date=days_ago(1),
    catchup=False
)

# Task 1: Fetch SpaceX launches data
def fetch_data():
    data = save_spacex_launches()
    return data

fetch_data_task = PythonOperator(
    task_id='fetch_spacex_data',
    python_callable=fetch_data,
    dag=dag
)

# Task 2: Process the data and validate it
def process_and_validate_data():
    # Load and process data
    data = save_spacex_launches()
    launches_df, details_df = create_launches_dataframe(data)
    fairings_df, links_df = process_fairings_and_links(data)
    cores_df = process_cores(data)
    failures_df = process_failures(data)
    payloads_df = process_payloads(data)

    # Validate and clean dataframes
    launches_df = validate_and_clean(launches_df, 'launch_id')
    details_df = validate_and_clean(details_df, 'launch_id')
    cores_df = validate_and_clean(cores_df, 'launch_id')
    fairings_df = validate_and_clean(fairings_df, 'launch_id')
    links_df = validate_and_clean(links_df, 'launch_id')
    failures_df = validate_and_clean(failures_df, 'launch_id')
    payloads_df = validate_and_clean(payloads_df, 'payload_id')

    return {
        'launches_df': launches_df,
        'details_df': details_df,
        'cores_df': cores_df,
        'fairings_df': fairings_df,
        'links_df': links_df,
        'failures_df': failures_df,
        'payloads_df': payloads_df
    }

process_data_task = PythonOperator(
    task_id='process_and_validate_data',
    python_callable=process_and_validate_data,
    dag=dag
)

# Task 3: Upload processed data to S3
def upload_data_to_s3(**context):
    dataframes = context['ti'].xcom_pull(task_ids='process_and_validate_data')
    processed_bucket_name = "sequra-spacex-preprocessed"

    # Upload each dataframe to S3
    for file_name, df in dataframes.items():
        object_name = f"{file_name}.csv"
        success = upload_to_s3(df, processed_bucket_name, object_name, 'text/csv')
        if not success:
            raise ValueError(f"Failed to upload {file_name}")

upload_data_task = PythonOperator(
    task_id='upload_data_to_s3',
    python_callable=upload_data_to_s3,
    provide_context=True,
    dag=dag
)

# Defining task dependencies
fetch_data_task >> process_data_task >> upload_data_task
