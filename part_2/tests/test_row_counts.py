import pandas as pd
import boto3
import psycopg2
import os

def get_redshift_connection():
    return psycopg2.connect(
        host=os.getenv('REDSHIFT_HOST'),
        port=os.getenv('REDSHIFT_PORT', '5439'),
        dbname=os.getenv('REDSHIFT_DB'),
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )

def get_s3_row_count(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    return len(df)

def get_redshift_row_count(conn, table):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        return cursor.fetchone()[0]

def test_row_counts():
    # Define the tables and their corresponding S3 keys
    tables = {
        'launches': 'launches_data.csv',
        'cores': 'cores_data.csv'
    }
    
    conn = get_redshift_connection()
    
    for table, s3_key in tables.items():
        s3_count = get_s3_row_count('sequra-spacex-preprocessed', s3_key)
        redshift_count = get_redshift_row_count(conn, table)
        assert s3_count == redshift_count, f"Row count mismatch for table '{table}': S3({s3_count}) vs Redshift({redshift_count})"
        print(f"Row count test passed for table '{table}': S3({s3_count}) == Redshift({redshift_count})")
    
    conn.close()

if __name__ == "__main__":
    test_row_counts()
    print("All row count tests passed.")
