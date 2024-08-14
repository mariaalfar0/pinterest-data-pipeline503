# Pinterest Data Pipeline

An end-to-end data processing pipeline that ingests data from an API simulating users posting on Pinterest, cleans and processes the data into batch and stream pipelines and stores them into a data lake and PostgreSQL database respectively.

Technologies used:
Python -> Programming
Apache Kafka -> Data Ingestion
Apache Spark (Pyspark) -> Batch Data Processing
Apache Spark Streaming (Pyspark) -> Stream Data Processing
Apache Airflow -> Orchestrating Batch Processing Job
AWS S3/Boto3 -> Storage of Batch Data
PostgreSQL -> Storage of Stream Data

### Installation instructions
Installation:

Clone the directory using:

git clone https://github.com/mariaalfar0/pinterest-data-pipeline503

### Usage instructions

Initialise EC2 instance using SSH client

Navigate to confluent-7.2.0/bin/

Initialise server

In a new terminal, navigate to and run user_posting_emulation_streaming.py

In databricks, run kinesis_integration notebook

### File structure of the project

 ┣ .gitignore
 ┣ 0affe2a66fdf-key-pair.pem
 ┣ 0affe2a66fdf_dag.py
 ┣ databricks_mount.py
 ┣ databricks_queries.py
 ┣ dataframe_transforms.py
 ┣ db_creds.yaml
 ┣ kinesis_integration.py
 ┣ README.md
 ┣ user_posting_emulation.py
 ┗ user_posting_emulation_streaming.py

### License information

N/A