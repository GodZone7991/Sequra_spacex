# Sequra

The following section contains an architectural diagram of the solution for the assignment.

<img width="1262" alt="image" src="https://github.com/user-attachments/assets/ac3f325c-c7c8-4599-bf87-8e820843a52a">

This diagram captures the data flow utilized in the solution. The design choices aim to create a robust, scalable, and maintainable solution.

Key Design Considerations
1. Why Use COPY Command Instead of Direct Ingestion?

During the development of this solution, I encountered several challenges with direct connections to Redshift, which influenced the decision to use the COPY command:

Network and Security Configurations: Redshift clusters are typically deployed within a VPC (Virtual Private Cloud), requiring proper network setups, including security group and VPC configurations. Establishing and maintaining direct connectivity with Redshift can be complex.

Performance: Even after configuring the Redshift Connector, I observed that the data upload speed to Redshift was significantly slower compared to uploading to S3. This is particularly concerning as the dataset grows, which could lead to performance bottlenecks.

Given these challenges, I opted to store the data in S3 first and then load it into Redshift using the COPY command. This approach provides a more reliable and faster method for data ingestion, particularly for large datasets.

2. Storing Processed and Unprocessed Files in S3

Although the problem statement could be solved with a limited dataset (from the initial JSON files), I opted to use S3 as a data lake to store both processed and unprocessed data. This decision provides several advantages:

Scalability: As the data team matures and new data requirements emerge, it becomes easier to normalize the incoming JSON data and store the results in a structured format, such as a star schema. This eliminates the need to continually adjust the pipeline for each new data request.

Cost Efficiency: S3 operates on a pay-per-use model and offers virtually unlimited storage capacity. By storing both raw and processed data, we can maintain scalability while ensuring that the additional storage costs remain manageable.

This setup allows the pipeline to scale as the data grows and makes it more adaptable to future data requirements.

3. Running Tests in Two Steps

In the diagram, you’ll notice tests are run at two stages:

Step 1: Basic tests are run when importing the data into Redshift, where we compare schemas and row counts to ensure data integrity.
Step 2: More advanced tests are executed via DBT after the data is loaded. These tests leverage DBT’s standardized testing capabilities, allowing us to reuse tests across multiple tables and ETL processes.
The reason for splitting the tests into two stages is to avoid overloading the DAG during data ingestion. Running lightweight checks early in the process ensures quick feedback, while DBT provides more thorough testing in later stages, ensuring data quality across transformations.



