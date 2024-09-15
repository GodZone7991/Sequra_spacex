# Sequra

The following section contains an architectural diagram of the solution for the assignment.

<img width="1262" alt="image" src="https://github.com/user-attachments/assets/ac3f325c-c7c8-4599-bf87-8e820843a52a">

This diagram captures the data flow utilized in the solution. The design choices aim to create a robust, scalable, and maintainable solution.

Key Design Considerations
**1. Why Use COPY Command Instead of Direct Ingestion?

During the development of this solution, I encountered several challenges with direct connections to Redshift, which influenced the decision to use the COPY command:

- **Network and Security Configurations: Redshift clusters are typically deployed within a VPC (Virtual Private Cloud), requiring proper network setups, including security group and VPC configurations. Establishing and maintaining direct connectivity with Redshift can be complex.

- **Performance: Even after configuring the Redshift Connector, I observed that the data upload speed to Redshift was significantly slower compared to uploading to S3. This is particularly concerning as the dataset grows, which could lead to performance bottlenecks.

Given these challenges, I opted to store the data in S3 first and then load it into Redshift using the COPY command. This approach provides a more reliable and faster method for data ingestion, particularly for large datasets.

**2. Storing Processed and Unprocessed Files in S3

Although the problem statement could be solved with a limited dataset (from the initial JSON files), I opted to use S3 as a data lake to store both processed and unprocessed data. This decision provides several advantages:

- **Scalability: As the data team matures and new data requirements emerge, it becomes easier to normalize the incoming JSON data and store the results in a structured format, such as a star schema. This eliminates the need to continually adjust the pipeline for each new data request.

- **Cost Efficiency: S3 operates on a pay-per-use model and offers virtually unlimited storage capacity. By storing both raw and processed data, we can maintain scalability while ensuring that the additional storage costs remain manageable.

This setup allows the pipeline to scale as the data grows and makes it more adaptable to future data requirements.

**3. Running Tests in Two Steps

In the diagram, you’ll notice tests are run at two stages:

- **Step 1: Basic tests are run when importing the data into Redshift, where we compare schemas and row counts to ensure data integrity.
- **Step 2: More advanced tests are executed via DBT after the data is loaded. These tests leverage DBT’s standardized testing capabilities, allowing us to reuse tests across multiple tables and ETL processes.
The reason for splitting the tests into two stages is to avoid overloading the DAG during data ingestion. Running lightweight checks early in the process ensures quick feedback, while DBT provides more thorough testing in later stages, ensuring data quality across transformations.

**4. Airflow DAG Orchestration**
The data pipeline is orchestrated using Airflow, which coordinates each step from data ingestion to transformation and loading. Key aspects of the DAG setup include:

- **Task Dependencies:** Each task is defined in a modular way, ensuring that failures in one step do not propagate to other stages. For example, data is only loaded into Redshift after successful uploads to S3.
- **Scheduling:** The DAG runs daily, ensuring that the data is regularly updated. The schedule interval can be adjusted based on business needs.
- **Error Handling & Monitoring:** Airflow handles errors by retrying failed tasks up to a certain threshold, and logs are captured for monitoring. For additional visibility, Airflow UI is used for task-level monitoring and alerting.



**5. ETL vs ELT Approach**

This pipeline might seem like its following the ELT approach since the transfomations on the graph are inidcated to happen after the data is uploaded to Redshift. However that refers to the future logic of transforming data to fit buisness usecase. The original peipline used in the assignment is of ETL type


**6. Future Scalability & Enhancements**
As the data pipeline scales, several enhancements can be made:

- **Real-time Ingestion:** The current solution handles batch ingestion, but for real-time use cases, we could introduce streaming data ingestion using tools like AWS Kinesis or Apache Kafka.
- **Data Partitioning:** As the dataset grows, partitioning the data in S3 and Redshift can further optimize performance. We could also consider using Redshift Spectrum for querying larger datasets directly from S3.

**7. Cost Considerations**
The architecture is designed with cost efficiency in mind:

- **S3 Pay-As-You-Go:** S3’s pay-per-use model ensures we only pay for the storage we use, which can scale up or down based on the amount of data.
- **Redshift Scalability:** Redshift’s ability to auto-scale enables us to handle fluctuating data volumes without over-provisioning resources.
- **Efficient Data Transfer:** By loading data into S3 first, we minimize data transfer costs and avoid potential throttling or high costs associated with direct Redshift ingestion.
