# Databricks notebook source
# from pyspark.sql.functions import *
# from pyspark.sql import DataFrame

# # File location and type
# # Asterisk(*) indicates reading all the content of the specified file that have .json extension
# geo_file_location = "/mnt/0affe2a66fdf/topics/0affe2a66fdf.geo/partition=0/*.json" 
# pin_file_location = "/mnt/0affe2a66fdf/topics/0affe2a66fdf.pin/partition=0/*.json" 
# user_file_location = "/mnt/0affe2a66fdf/topics/0affe2a66fdf.user/partition=0/*.json" 
# file_type = "json"
# # Ask Spark to infer the schema
# infer_schema = "true"
# # Read in JSONs from mounted S3 bucket
# df_pin = spark.read.format(file_type) \
# .option("inferSchema", infer_schema) \
# .load(pin_file_location)

# df_geo = spark.read.format(file_type) \
# .option("inferSchema", infer_schema) \
# .load(geo_file_location)

# df_user = spark.read.format(file_type) \
# .option("inferSchema", infer_schema) \
# .load(user_file_location)
# # Display Spark dataframe to check its content
# #display(df)

# COMMAND ----------

# Clean pin data

# Replace missing values with None
cleaned_df_pin = pin_df.replace({'': None})
cleaned_df_pin = cleaned_df_pin.na.drop
# Make follower_count contain only numbers and cast to int 
cleaned_df_pin = pin_df.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", cleaned_df_pin["follower_count"].cast("int"))
cleaned_df_pin.printSchema()
# Cast downloaded to boolean
cleaned_df_pin = cleaned_df_pin.withColumn("downloaded", regexp_replace("downloaded", "1", "True"))
cleaned_df_pin = cleaned_df_pin.withColumn("downloaded", regexp_replace("downloaded", "0", "False"))
cleaned_df_pin = cleaned_df_pin.withColumn("downloaded", cleaned_df_pin["downloaded"].cast("boolean"))

# Clean save_location to only include path
cleaned_df_pin = cleaned_df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# Rename index column
cleaned_df_pin = cleaned_df_pin.withColumnRenamed("index", "ind")

# Reorder columns
cleaned_df_pin = cleaned_df_pin.select("ind", "unique_id", "title", "description", "follower_count",
                                       "poster_name", "tag_list", "is_image_or_video", "image_src", 
                                       "save_location", "downloaded")

#cleaned_df_pin.printSchema()
cleaned_df_pin.display()

# COMMAND ----------

# Clean geospatial data

cleaned_df_geo = geo_df.withColumn("coordinates", array("latitude", "longitude"))
cleaned_df_geo = cleaned_df_geo.drop("latitude", "longitude")
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")
cleaned_df_geo.display()


# COMMAND ----------

# Clean user data

cleaned_df_user = user_df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")
cleaned_df_user.display()

