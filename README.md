# Project-Data-Pipeline-for-Recruitment-Start-Up

## Introduction
This project is to build a near real-time ETL pipeline for a recruitment company to calculate the total number of clicks, candidates applying, and the qualified and unqualified people for jobs on the recruitment website by combining programming language and frameworks like Python, Apache Spark(PySpark, SparkSQL), Docker.
<br>
![test (1)](https://github.com/DuyDoan233/Project-Data-Pipeline-for-Recruitment-Start-Up/assets/101572443/8a0af314-8eca-4aa3-98c9-b0903fe2a8ff)

## Dataset
Since it mainly focuses on building the ETL pipeline so the dataset used for processing is faked from the customer's schema.
- [CassandraDB (DataLake)]()
- [MySQLDB (Data Warehouse)]()

## Process description
- Fake Data process
  - Since this project only received schema and a limited amount of data from the customer, in order to increase the size of the data to test ETL scripts as well as simulate the process of a near real-time ETL pipeline, I built a Python script ([Fake_data_process.py](##)) to fake randomly generate from 1 to 10 records after 'Time = Script running time + 5s sleep'.
- ETL process
  - Since the processing selection is near real-time (batch processing), the first is to compare the latest update time of the data inside the Data Lake and the Data Warehouse.
  - If the condition is False, the comparison will continue.
  - If the condition is True, then it will get the latest values and trigger the ETL script to filter and the job_id, publisher_id, campaign_id, group_id, ts, custom_track, bid values from there in MySQLBD.<br>
![test](https://github.com/DuyDoan233/Project-Data-Pipeline-for-Recruitment-Start-Up/assets/101572443/6d390861-1f15-4a4d-8f13-fbd8afb4f99b)

## Requirements
- Since it simulates an actual project, so we use some programming\tools\framework such as:
  - Python: Use to build the Fake_data_process.py script to combine with ETL pipeline to simulate the near real-time process.
  - Apache Spark: Framework to build the ETL script (Including PySpark & SparkSQL)
  - Docker: Install Cassandra(as DataLake), MySQL(as Data Warehouse).
    - Cassandra: raw data would store in a keyspace named 'datalake'.
    - MySQL: processed data would store in the database named 'DW', the shorten of Data Warehouse.
  - DataGrip: is a IDE for SQL, I using it to connect and process data with MySQL and Cassandra.
