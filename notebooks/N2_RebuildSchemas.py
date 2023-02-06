# Databricks notebook source
#Directories

dir_bronze = "/tmp/Abdus/Bronze/"

dir_silver = "/tmp/Abdus/Silver/"

dir_gold = "/tmp/Abdus/Gold/"

# COMMAND ----------

# MAGIC %run /Repos/abdus.choudhury@qualyfi.co.uk/steven-repo/final_notebooks/SchemaCreation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Empty Bronze DataFrames

# COMMAND ----------

from pyspark.sql.types import *

empty_df = sc.emptyRDD()

bronze_trips_df = spark.createDataFrame(empty_df, bronze_trips_schema)
bronze_payments_df = spark.createDataFrame(empty_df, bronze_payments_schema)
bronze_riders_df = spark.createDataFrame(empty_df, bronze_riders_schema)
bronze_stations_df = spark.createDataFrame(empty_df, bronze_stations_schema)

bronze_trips_df.write.format("delta").mode("overwrite").save(dir_bronze + "trips")
bronze_payments_df.write.format("delta").mode("overwrite").save(dir_bronze + "payments")
bronze_riders_df.write.format("delta").mode("overwrite").save(dir_bronze + "riders")
bronze_stations_df.write.format("delta").mode("overwrite").save(dir_bronze + "stations")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Empty Silver DataFrames

# COMMAND ----------

silver_trips_df = spark.createDataFrame(empty_df, silver_trips_schema)
silver_payments_df = spark.createDataFrame(empty_df, silver_payments_schema)
silver_riders_df = spark.createDataFrame(empty_df, silver_riders_schema)
silver_stations_df = spark.createDataFrame(empty_df, silver_stations_schema)

silver_trips_df.write.format("delta").mode("overwrite").save(dir_silver + "trips")
silver_payments_df.write.format("delta").mode("overwrite").save(dir_silver + "payments")
silver_riders_df.write.format("delta").mode("overwrite").save(dir_silver + "riders")
silver_stations_df.write.format("delta").mode("overwrite").save(dir_silver + "stations")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Empty Gold DataFrames

# COMMAND ----------


