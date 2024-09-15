import json
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from typing import List, Dict, Any
import requests


# Import functions from the processing script
from transform_json import (
    load_json_data,
    create_launches_dataframe,
    process_fairings_and_links,
    process_cores,
    process_failures,
    process_payloads
)

def save_spacex_launches():
    # URL of the SpaceX API
    url = "https://api.spacexdata.com/v5/launches/"

    try:
        # Send GET request to the API
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Parse the JSON response
        launches_data = response.json()

        # Define the file name
        file_name = "spacex_launches.json"

        # Save the JSON data to a file in the local directory
        with open(file_name, 'w') as file:
            json.dump(launches_data, file, indent=4)

        print(f"Successfully saved SpaceX launches data to {os.path.abspath(file_name)}")

    except requests.RequestException as e:
        print(f"Error fetching data from the API: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except IOError as e:
        print(f"Error writing to file: {e}")

    return launches_data


def validate_and_clean(df: pd.DataFrame, primary_key: str) -> pd.DataFrame:
    """
    Validate and clean a DataFrame based on a primary key.
    
    :param df: DataFrame to validate and clean
    :param primary_key: Column name to use as primary key
    :return: Cleaned DataFrame
    """
    # Drop rows with null values in the primary key
    df.dropna(subset=[primary_key], inplace=True)

    
    return df

def upload_to_s3(data: Any, bucket_name: str, object_name: str, content_type: str) -> bool:
    """
    Upload data to an S3 bucket

    :param data: Data to upload (can be DataFrame or raw data)
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name
    :param content_type: Content type of the data
    :return: True if file was uploaded, else False
    """
    # Create S3 client with explicit credentials
    session = boto3.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    s3_client = session.client('s3')

    try:
        # Prepare the data
        if isinstance(data, pd.DataFrame):
            body = data.to_csv(index=False)
        elif isinstance(data, (dict, list)):
            body = json.dumps(data)
        else:
            body = str(data)
        
        # Upload the file
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=body,
            ContentType=content_type
        )
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False
    else:
        print(f"Data uploaded successfully to {bucket_name}/{object_name}")
        return True

def main():
    # Load data
    # data = load_json_data('spacex_launches.json')
    data = save_spacex_launches()
    
    # Upload raw JSON to S3
    raw_bucket_name = "sequra-spacex-raw"
    upload_to_s3(data, raw_bucket_name, 'spacex_launches_raw.json', 'application/json')
    
    # Process dataframes
    launches_df, details_df = create_launches_dataframe(data)
    fairings_df, links_df = process_fairings_and_links(data)
    cores_df = process_cores(data)
    failures_df = process_failures(data)
    payloads_df = process_payloads(data)
    
    # Validate and clean dataframes
    # By utilising this validation strtegy we are exsuring there are no missing orws with primary keys
    launches_df = validate_and_clean(launches_df, 'launch_id')
    details_df = validate_and_clean(details_df, 'launch_id')
    cores_df = validate_and_clean(cores_df, 'launch_id')
    fairings_df = validate_and_clean(fairings_df, 'launch_id')
    links_df = validate_and_clean(links_df, 'launch_id')
    failures_df = validate_and_clean(failures_df, 'launch_id')
    payloads_df = validate_and_clean(payloads_df, 'payload_id')
    
    print('All dataframes validated and cleaned!')

    # Upload processed dataframes to S3
    processed_bucket_name = "sequra-spacex-preprocessed"
    dataframes = {
        'launches_data.csv': launches_df,
        'details_data.csv': details_df,
        'cores_data.csv': cores_df,
        'fairings_data.csv': fairings_df,
        'links_data.csv': links_df,
        'failures_data.csv': failures_df,
        'payloads_data.csv': payloads_df
    }

    for object_name, df in dataframes.items():
        # this is a basic implementation designed for 1 iteration of the workflow
        # in case we want to run the pipeline on a daily basis we should be saving intermidiate results
        # to the daily folders instead of root directory of the bucket.
        success = upload_to_s3(df, processed_bucket_name, object_name, 'text/csv')
        if not success:
            print(f"Failed to upload {object_name}")

if __name__ == "__main__":
    main()