# Databricks notebook source
# Clean pin data

# Replace missing values with None
cleaned_df_pin = df_pin.replace({'': None})
cleaned_df_pin = cleaned_df_pin.na.drop
# Make follower_count contain only numbers and cast to int 
cleaned_df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
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

cleaned_df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
cleaned_df_geo = cleaned_df_geo.drop("latitude", "longitude")
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")
cleaned_df_geo.show()


# COMMAND ----------

# Clean user data

cleaned_df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")
cleaned_df_user.show()

