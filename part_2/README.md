# Sequra

The structure of  the part 2 is the following:

The following is the structure of the repository:

```plaintext
part_2/
├── dags/
│   └── spacex_dag.py
├── dbt/
│   ├── models/
│   ├── dbt_project.yaml
│   └── profiles.yaml
├── scripts/
│   ├── copy_schemas.py
│   ├── send_to_s3.py
│   └── transform_json.py
├── tests/
│   ├── test_row_counts.py
│   └── test_schema.py
└── README.md
```

Explanation of the Project Structure:
dags/spacex_dag.py: Contains the Airflow DAG used for orchestrating the SpaceX data pipeline.

dbt/models/: Directory for dbt models used to define data transformations.

dbt/dbt_project.yaml & profiles.yaml: dbt configuration files for project structure and connection.

scripts/: Python scripts for data pipeline tasks:

copy_schemas.py: Copies schema information.
send_to_s3.py: Sends data to an S3 bucket.
transform_json.py: Transforms JSON into a structured format.

tests/: Unit tests to ensure data quality:
test_row_counts.py: Checks row count consistency.
test_schema.py: Validates schema compliance.

