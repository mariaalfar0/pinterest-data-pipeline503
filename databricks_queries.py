# Databricks notebook source
# Find the most popular Pinterest category people post to based on their country.

# Combine pin & geographical data
pin_geo = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_pin["ind"] == cleaned_df_geo["ind"], how="left")

# Re-format and re-label category column
pin_geo = pin_geo.withColumn("save_location", regexp_replace("save_location", "/data/", ""))
pin_geo = pin_geo.withColumnRenamed("save_location", "category")

# Select only category and country columns
category_country = pin_geo["category", "country"]
grouped_category_country = category_country.groupBy("country", "category").agg({"category": "count"}).alias("category_count")
grouped_category_country = grouped_category_country.orderBy("country", ascending=True)
final_category_country = grouped_category_country.groupBy("country").agg(first("category"),max("category_count.count(category)")).show();   


# COMMAND ----------

# Task 5: Find how many posts each category had between 2018 and 2022.

# Your query should return a DataFrame that contains the following columns:

# post_year, a new column that contains only the year from the timestamp column
# category
# category_count, a new column containing the desired query output

from pyspark.sql.functions import year

category_by_year = pin_geo["category", "timestamp"]
category_by_year = category_by_year.withColumn("post_year", year("timestamp"))

grouped_category_by_year = category_by_year.groupBy("post_year", "category").agg({"category": "count"}).alias("category_count")
grouped_category_by_year = grouped_category_by_year.na.drop()
final_grouped_category_by_year = grouped_category_by_year.groupBy("post_year").agg(first("category"),max("category_count.count(category)")).show()

# COMMAND ----------

# Task 6
# Step 1: For each country find the user with the most followers.

# Join all three data sources together
temp = cleaned_df_pin.withColumnRenamed("ind", "index")
pin_user = temp.join(cleaned_df_user, temp["index"] == cleaned_df_user["ind"], how="left")
pin_user_geo = pin_user.join(cleaned_df_geo, pin_user["ind"] == cleaned_df_geo["ind"], how="left")

# Select necessary columns
followers_by_country = pin_user_geo["ind", "unique_id", "user_name", "follower_count"]
grouped_followers_by_country = followers_by_country.groupBy("country").agg(first("user_name"),max("follower_count"))
grouped_followers_by_country = grouped_followers_by_country.na.drop()

# Step 2: Based on the above query, find the country with the user with most followers.
country_most_followers = grouped_followers_by_country.orderBy("max(follower_count)", ascending = False).limit(1).show()



# COMMAND ----------

# Task 7: What is the most popular category people post to based on the following age groups:
# - 18-24; 25-35; 36-50; +50
# Your query should return a DataFrame that contains the following columns:
# - age_group, a new column based on the original age column
# - category
# - category_count, a new column containing the desired query output

pin_user = temp.join(cleaned_df_user, temp["index"] == cleaned_df_user["ind"], how="left")
pin_user = pin_user.withColumn("save_location", regexp_replace("save_location", "/data/", ""))
pin_user = pin_user.withColumnRenamed("save_location", "category")

age_cat_df = pin_user.withColumn("age_group", 
                                 when((pin_user["age"] >= 18) & (pin_user["age"] < 24), "18-24")
                                 .when((pin_user["age"] >= 25) & (pin_user["age"] < 35), "25-35")
                                 .when((pin_user["age"] >= 36) & (pin_user["age"] < 50), "36-50")
                                 .otherwise("+50"))
age_cat_df = age_cat_df.groupBy("age_group", "category").agg(count("category")).orderBy("age_group", "category", ascending=True)
age_cat_df = age_cat_df.groupBy("age_group").agg(first("category"),max("count(category)")).show()

# COMMAND ----------

# Task 8: What is the median follower count for users in the following age groups:
# - 18-24; 25-35; 36-50; +50
# Your query should return a DataFrame that contains the following columns:
# - age_group, a new column based on the original age column
# - median_follower_count, a new column containing the desired query output

from pyspark.sql.functions import expr

age_follower_df = pin_user["age", "follower_count"]

# Age group classification
age_follower_df = pin_user.withColumn("age_group", 
                                      when((col("age") >= 18) & (col("age") < 24), "18-24")
                                      .when((col("age") >= 25) & (col("age") < 35), "25-35")
                                      .when((col("age") >= 36) & (col("age") < 50), "36-50")
                                      .otherwise("+50"))

age_follower_df = age_follower_df.groupBy("age_group").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))
display(age_follower_df)


# COMMAND ----------

# Task 9: Find how many users have joined between 2015 and 2020.

# Your query should return a DataFrame that contains the following columns:

# post_year, a new colun that contains only the year from the timestamp column
# number_users_joined, a new column containing the desired query output

from pyspark.sql.functions import year

result_df = cleaned_df_user.withColumn("post_year", year("date_joined"))
result_df = result_df.groupBy("post_year").agg(count("post_year")).alias("number_users_joined")
display(result_df)

# COMMAND ----------

# Task 10: Find the median follower count of users have joined between 2015 and 2020.

# Your query should return a DataFrame that contains the following columns:

# post_year, a new column that contains only the year from the timestamp column
# median_follower_count, a new column containing the desired query output

year_follower_df = pin_user["follower_count", "date_joined"]
year_follower_df = year_follower_df.withColumn("post_year", year("date_joined"))
year_follower_df = year_follower_df.groupBy("post_year").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count")).show()



# COMMAND ----------

# Task 11: Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.

# Your query should return a DataFrame that contains the following columns:

# age_group, a new column based on the original age column
# post_year, a new column that contains only the year from the timestamp column
# median_follower_count, a new column containing the desired query output

age_year_follower_df = pin_user["age", "follower_count", "date_joined"]
age_year_follower_df = age_year_follower_df.withColumn("age_group", 
                                      when((col("age") >= 18) & (col("age") < 24), "18-24")
                                      .when((col("age") >= 25) & (col("age") < 35), "25-35")
                                      .when((col("age") >= 36) & (col("age") < 50), "36-50")
                                      .otherwise("+50"))
age_year_follower_df = age_year_follower_df.withColumn("post_year", year("date_joined"))
age_year_follower_df = age_year_follower_df.groupBy("post_year", "age_group").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count")).orderBy("post_year").show()
