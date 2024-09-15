# Sequra

The following part contains an architectual diagram of the solution to the test. 

<img width="1262" alt="image" src="https://github.com/user-attachments/assets/ac3f325c-c7c8-4599-bf87-8e820843a52a">

The attempt was to capture the dataflow used in the assignment and try and make the solution as robust as possible. 

A few points of considerations in case there are questions regarding design choices.

- 1. Why use copy instead of diret ingestions
 
While working on this solution I ran into a few issues with direct conntction to RedShift
 - Network and Security Configurations: Redshift clusters are usually deployed in a VPC, and accessing them requires proper network setup, including security groups and VPC settings.
 - Even after properly configuring the Redshift Connector I noticed that compared to S3 the upload speed was significanlty slower on my which providing the increase in the datasize may present challanges

Due to the resons above I decided to save data in the S3 bucket as it probide a more reliable and quicker data transfer.

- 2. Storing of processed and unprocessed files on S3.

Although the goal of this test was defined in a way that can be solved with a limited amount of data from initial json, I belived using S3 as a datalake and storing both processed and unprocessed results brings more advantages. As the team matures instead of adjusting the pipeline every time we need a new piece of data its easier to normalise the json and store the results as separate tables under the star schema. 

S3 is designed as a pay per use mode and supports virtually unlimited storage, so using it to store the normalised schema at the cost of storing more data that requred matches the requrement for providing the scalable solution 


- 3. Running Tests in two steps

In the digram we run tests at multiple stages. First when we import the data to Redshift by comparing the schemas and the row counts and later after the imports with the DBT. 

The resons behind this is in order to not overstress the DAG (by running only the basic checks) and leveraging dbt by using standartised tests that can be recycled on multiple tables and ETLS.




