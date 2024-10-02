# redshift_copy_script.py
import psycopg2
import os

def check_table_exists(cursor, table_name):
    """
    Check if a table exists in Redshift.
    """
    cursor.execute(f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '{table_name}');")
    return cursor.fetchone()[0]

def get_row_count(cursor, table_name):
    """
    Get the row count of a Redshift table.
    """
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    return cursor.fetchone()[0]

def copy_to_redshift():
    # Redshift connection details
    redshift_host = os.getenv('REDSHIFT_HOST')
    redshift_port = os.getenv('REDSHIFT_PORT', '5439')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')

    # Tables and COPY commands
    table_copy_commands = {
        "launches": """
            COPY launches
            FROM 's3://sequra-spacex-preprocessed/launches_data.csv'
            IAM_ROLE 'arn:aws:iam::886436958512:role/RedshiftS3AccessRole'
            CSV
            IGNOREHEADER 1
            TIMEFORMAT 'auto'
            EMPTYASNULL
            NULL AS '\\N';
        """,
        "cores": """
            COPY cores
            FROM 's3://sequra-spacex-preprocessed/cores_data.csv'
            IAM_ROLE 'arn:aws:iam::886436958512:role/RedshiftS3AccessRole'
            CSV
            IGNOREHEADER 1
            TIMEFORMAT 'auto'
            EMPTYASNULL
            NULL AS '\\N';
        """
    }

    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password
        )
        cursor = conn.cursor()

        for table_name, copy_command in table_copy_commands.items():
            # Pre-COPY: Check if table exists
            if not check_table_exists(cursor, table_name):
                raise Exception(f"Table {table_name} does not exist in Redshift.")

            # Execute the COPY command
            cursor.execute(copy_command)
            print(f"Executed COPY command for table: {table_name}")

            # Post-COPY: Check if the table exists and has more than 1 row
            if check_table_exists(cursor, table_name):
                row_count = get_row_count(cursor, table_name)
                if row_count > 1:
                    print(f"Table {table_name} exists and has {row_count} rows.")
                else:
                    raise Exception(f"Table {table_name} has insufficient rows: {row_count}")
            else:
                raise Exception(f"Table {table_name} does not exist after COPY command.")

        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully copied to Redshift and verified.")

    except Exception as e:
        print(f"Error during Redshift operations: {e}")
        if conn:
            conn.rollback()
            cursor.close()
            conn.close()
        raise

if __name__ == "__main__":
    copy_to_redshift()