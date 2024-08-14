# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

geo_bucket = "s3://user-0affe2a66fdf-bucket/topics/0affe2a66fdf.geo/partition=0/"
pin_bucket = "s3://user-0affe2a66fdf-bucket/topics/0affe2a66fdf.pin/partition=0/"
geo_user = "s3://user-0affe2a66fdf-bucket/topics/0affe2a66fdf.user/partition=0/"

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0affe2a66fdf-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/0affe2a66fdf"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

display(dbutils.fs.ls("/mnt/0affe2a66fdf/topics/0affe2a66fdf.geo/partition=0"))
