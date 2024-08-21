## NOTE might want to add a high level description of how the project was built for your own reference.

Images I would add:

- MWAA Airflow image of DAG runs
- Image of all your API endpoints
- Image of REST proxy running on the EC2
- Image of `kafka-rest.properties`
- Image of `kafka.properties`
- Image of Databricks
- Image of data in the Kinesis stream

# Pinterest Data Pipeline

An end-to-end data processing pipeline that ingests data from an API simulating users posting on Pinterest, cleans and processes the data into batch and stream pipelines and stores them into a data lake and PostgreSQL database respectively.

Technologies used:
- Python -> Programming
- Apache Kafka -> Data Ingestion
Apache Spark (Pyspark) -> Batch Data Processing
Apache Spark Streaming (Pyspark) -> Stream Data Processing
Apache Airflow -> Orchestrating Batch Processing Job
AWS S3/Boto3 -> Storage of Batch Data
PostgreSQL -> Storage of Stream Data

### Installation instructions
Installation:


### NOTE adding requirements.txt
\can use `pip install pipreqs`
Running `pipreqs` will create a `requirements.txt`
If there is already a `requirements.txt` run `pipreqs --force`

To install requirements run `pip install -r /path/to/requirements.txt`

Clone the directory using:

git clone https://github.com/mariaalfar0/pinterest-data-pipeline503

## Dependencies


- Python 3.8+

### Usage instructions

Initialise EC2 instance using SSH client

Navigate to `confluent-7.2.0/bin/`

Initialise server

In a new terminal, navigate to and run user_posting_emulation_streaming.py

In databricks, run kinesis_integration notebook

### File structure of the project
```
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
```

### Architecture of the project

![CloudPinterestPipeline](https://github.com/user-attachments/assets/6a9df0ba-2989-47bd-a677-f5f3a9c7a0e7)


