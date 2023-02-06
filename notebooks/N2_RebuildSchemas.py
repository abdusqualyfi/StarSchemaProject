# Databricks notebook source
#Directories
main_folder = "/tmp/Abdus/"

dir_bronze = "/tmp/Abdus/Bronze/"

dir_silver = "/tmp/Abdus/Silver/"

dir_gold = "/tmp/Abdus/Gold/"

# COMMAND ----------

dbutils.fs.mkdirs(main_folder)

# COMMAND ----------

# MAGIC %run /Repos/abdus.choudhury@qualyfi.co.uk/StarSchemaProject/notebooks/N0_SchemaCreation

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

gold_trips_df = spark.createDataFrame(empty_df, gold_trips_schema)
gold_payments_df = spark.createDataFrame(empty_df, gold_payments_schema)
gold_riders_df = spark.createDataFrame(empty_df, gold_riders_schema)
gold_stations_df = spark.createDataFrame(empty_df, gold_stations_schema)
gold_bikes_df = spark.createDataFrame(empty_df, gold_bikes_schema)
gold_dates_df = spark.createDataFrame(empty_df, gold_dates_schema)
gold_times_df = spark.createDataFrame(empty_df, gold_times_schema)

gold_trips_df.write.format("delta").mode("overwrite").save(dir_gold + "fact_trips")
gold_payments_df.write.format("delta").mode("overwrite").save(dir_gold + "fact_payments")
gold_riders_df.write.format("delta").mode("overwrite").save(dir_gold + "dim_riders")
gold_stations_df.write.format("delta").mode("overwrite").save(dir_gold + "dim_stations")
gold_bikes_df.write.format("delta").mode("overwrite").save(dir_gold + "dim_bikes")
gold_dates_df.write.format("delta").mode("overwrite").save(dir_gold + "dim_dates")
gold_times_df.write.format("delta").mode("overwrite").save(dir_gold + "dim_times")
