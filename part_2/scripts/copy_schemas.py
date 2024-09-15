# redshift_copy_script.py

import psycopg2
import os

def copy_to_redshift():
    # Redshift connection details
    redshift_host = os.getenv('REDSHIFT_HOST')
    redshift_port = os.getenv('REDSHIFT_PORT', '5439')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')

    # COPY commands
    copy_commands = [
        """
        COPY launches
        FROM 's3://sequra-spacex-preprocessed/launches_data.csv'
        IAM_ROLE 'arn:aws:iam::886436958512:role/RedshiftS3AccessRole'
        CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto'
        EMPTYASNULL
        NULL AS '\\N';
        """,
        """
        COPY cores
        FROM 's3://sequra-spacex-preprocessed/cores_data.csv'
        IAM_ROLE 'arn:aws:iam::886436958512:role/RedshiftS3AccessRole'
        CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto'
        EMPTYASNULL
        NULL AS '\\N';
        """
    ]

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

        # Execute COPY commands
        for command in copy_commands:
            cursor.execute(command)
            print(f"Executed COPY command: {command.strip().split()[1]}")

        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully copied to Redshift.")

    except Exception as e:
        print(f"Error copying data to Redshift: {e}")
        if conn:
            conn.rollback()
            cursor.close()
            conn.close()
        raise

if __name__ == "__main__":
    copy_to_redshift()
