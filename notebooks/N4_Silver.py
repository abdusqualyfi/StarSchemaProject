# Databricks notebook source
# MAGIC %md
# MAGIC ##Silver Schema

# COMMAND ----------

# MAGIC %run /Repos/abdus.choudhury@qualyfi.co.uk/StarSchemaProject/notebooks/N0_SchemaCreation

# COMMAND ----------

#Directories
main_folder = "/tmp/Abdus/"

# COMMAND ----------

from pyspark.sql.functions import col, cast, unix_timestamp, from_unixtime, date_format, to_date
from pyspark.sql.types import *

#Open Bronze folder delta
bronze_trips_df = spark.read.format("delta").load(main_folder + "Bronze/trips")
bronze_payments_df = spark.read.format("delta").load(main_folder + "Bronze/payments")
bronze_riders_df = spark.read.format("delta").load(main_folder + "Bronze/riders")
bronze_stations_df = spark.read.format("delta").load(main_folder + "Bronze/stations")

# COMMAND ----------

#New defined schema for silver

bronze_trips_df2 =  bronze_trips_df.withColumn("started_at", unix_timestamp(col("started_at"), 'dd/MM/yyyy HH:mm').cast("timestamp")) \
                                .withColumn("ended_at", unix_timestamp(col("ended_at"), 'dd/MM/yyyy HH:mm').cast("timestamp"))

silver_trips_df = bronze_trips_df2.select(*(bronze_trips_df2[c].cast(silver_trips_schema[i].dataType) for i, c in enumerate(bronze_trips_df2.columns)))
silver_payments_df = bronze_payments_df.select(*(bronze_payments_df[c].cast(silver_payments_schema[i].dataType) for i, c in enumerate(bronze_payments_df.columns)))
silver_riders_df = bronze_riders_df.select(*(bronze_riders_df[c].cast(silver_riders_schema[i].dataType) for i, c in enumerate(bronze_riders_df.columns)))
silver_stations_df = bronze_stations_df.select(*(bronze_stations_df[c].cast(silver_stations_schema[i].dataType) for i, c in enumerate(bronze_stations_df.columns)))

# COMMAND ----------

#Save to delta in Silver folder
silver_trips_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/trips")
silver_payments_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/payments")
silver_riders_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/riders")
silver_stations_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/stations")
