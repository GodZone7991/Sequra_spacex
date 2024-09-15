import psycopg2
import pandas as pd
import os

def get_redshift_schema(conn, table_name):
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name.lower()}';
    """
    df = pd.read_sql(query, conn)
    return df.set_index('column_name')['data_type'].to_dict()

def get_csv_schema(file_path):
    df = pd.read_csv(file_path, nrows=0)
    return df.dtypes.apply(lambda x: str(x)).to_dict()

def test_schema():
    # Redshift connection details
    redshift_host = os.getenv('REDSHIFT_HOST')
    redshift_port = os.getenv('REDSHIFT_PORT', '5439')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')

    # Expected CSV files
    csv_files = {
        'launches': 'path/to/spacex_launches_preprocessed/launches_data.csv',
        'cores': 'path/to/spacex_launches_preprocessed/cores_data.csv'
    }

    # Connect to Redshift
    conn = psycopg2.connect(
        host=redshift_host,
        port=redshift_port,
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password
    )

    try:
        for table, csv_path in csv_files.items():
            redshift_schema = get_redshift_schema(conn, table)
            csv_schema = get_csv_schema(csv_path)

            # Compare columns
            if set(redshift_schema.keys()) != set(csv_schema.keys()):
                missing_in_redshift = set(csv_schema.keys()) - set(redshift_schema.keys())
                missing_in_csv = set(redshift_schema.keys()) - set(csv_schema.keys())
                raise AssertionError(
                    f"Schema mismatch in table '{table}':\n"
                    f"Missing in Redshift: {missing_in_redshift}\n"
                    f"Missing in CSV: {missing_in_csv}"
                )

            # Optionally compare data types
            for column in redshift_schema:
                redshift_type = redshift_schema[column].lower()
                csv_type = csv_schema[column].lower()

                # Simplistic type mapping, adjust as needed
                type_mapping = {
                    'integer': ['int', 'bigint'],
                    'double precision': ['float', 'float64'],
                    'character varying': ['object', 'string'],
                    'timestamp without time zone': ['datetime64[ns]']
                }

                matched = False
                for redshift_base, csv_types in type_mapping.items():
                    if redshift_type.startswith(redshift_base) and csv_type in csv_types:
                        matched = True
                        break
                if not matched:
                    raise AssertionError(
                        f"Data type mismatch for column '{column}' in table '{table}': "
                        f"Redshift type '{redshift_type}' vs CSV type '{csv_type}'"
                    )

            print(f"Schema test passed for table '{table}'.")

        conn.close()

    except Exception as e:
        print(f"Schema test failed: {e}")
        conn.close()
        raise

if __name__ == "__main__":
    test_schema()
    print("Schema tests passed successfully.")