# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
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

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, DoubleType

# Define a streaming schema using StructType
pin_streaming_schema = StructType([
    StructField("category", StringType(), True),
    StructField("description", StringType(), True),
    StructField("downloaded", LongType(), True),
    StructField("follower_count", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("index", LongType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("title", StringType(), True),
    StructField("unique_id", StringType(), True)
])

geo_streaming_schema = StructType([
    StructField("country", StringType(), True),
    StructField("ind", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

user_streaming_schema = StructType([
    StructField("age", StringType(), True),
    StructField("date_joined", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("ind", LongType(), True),
    StructField("last_name", StringType(), True),
])


# COMMAND ----------

pin_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affe2a66fdf-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

pin_df = pin_df \
    .selectExpr("CAST(data AS STRING) as message") \
    .select(functions.from_json(functions.col("message"), pin_streaming_schema).alias("json")) \
    .select("json.*")

display(pin_df)

# COMMAND ----------

geo_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affe2a66fdf-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

geo_df = geo_df \
    .selectExpr("CAST(data AS STRING) as message") \
    .select(functions.from_json(functions.col("message"), geo_streaming_schema).alias("json")) \
    .select("json.*")

display(geo_df)


# COMMAND ----------

user_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affe2a66fdf-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load() \


user_df = user_df \
    .selectExpr("CAST(data AS STRING) as message") \
    .select(functions.from_json(functions.col("message"), user_streaming_schema).alias("json")) \
    .select("json.*")

display(user_df)    

# COMMAND ----------

# MAGIC %run ./dataframe_transforms

# COMMAND ----------

cleaned_df_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affe2a66fdf_pin_table")

cleaned_df_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affe2a66fdf_geo_table")

cleaned_df_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affe2a66fdf_user_table")
