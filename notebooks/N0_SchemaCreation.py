# Databricks notebook source
# MAGIC %md
# MAGIC ##Bronze Schema Creation

# COMMAND ----------

from pyspark.sql.types import *

#Trip
bronze_trips_header = ['trip_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_id', 'end_station_id', 'rider_id']
bronze_trips_schema = StructType([StructField(col, StringType(), True) for col in bronze_trips_header])

#Payments
bronze_payments_header = ['payment_id', 'date', 'amount', 'rider_id']
bronze_payments_schema = StructType([StructField(col, StringType(), True) for col in bronze_payments_header])

#Riders
bronze_riders_header = ['rider_id', 'first', 'last', 'address', 'birthday', 'account_start', 'account_end', 'is_member'] 
bronze_riders_schema = StructType([StructField(col, StringType(), True) for col in bronze_riders_header])

#Stations
bronze_stations_header = ['station_id', 'name', 'longitude', 'latitude'] 
bronze_stations_schema = StructType([StructField(col, StringType(), True) for col in bronze_stations_header])


# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Schema Creation

# COMMAND ----------

silver_trips_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("rider_id", IntegerType(), True)
])

silver_payments_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount", FloatType(), True),
    StructField("rider_id", IntegerType(), True)
])

silver_riders_schema = StructType([
    StructField("rider_id", IntegerType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", DateType(), True),
    StructField("account_start", DateType(), True),
    StructField("account_end", DateType(), True),
    StructField("is_member", BooleanType(), True)
])

silver_stations_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC ##Gold Schema Creation

# COMMAND ----------

gold_trips_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("rider_id", IntegerType(), True),
    StructField("bike_id", IntegerType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("started_date_id", IntegerType(), True),
    StructField("started_time_id", IntegerType(), True),
    StructField("ended_date_id", IntegerType(), True),
    StructField("ended_time_id", IntegerType(), True),
    StructField("trip_duration", IntegerType(), True),
    StructField("rider_age", IntegerType(), True)
])

gold_payments_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("rider_id", IntegerType(), True),
    StructField("date_id", IntegerType(), True),
    StructField("amount", FloatType(), True)
])
gold_riders_schema = StructType([
    StructField("rider_id", IntegerType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", DateType(), True),
    StructField("account_start", DateType(), True),
    StructField("account_end", DateType(), True),
    StructField("is_member", BooleanType(), True)
])

gold_stations_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True)
])

gold_bikes_schema = StructType([
    StructField("bike_id", IntegerType(), True),
    StructField("rideable_type", StringType(), True)  
])

gold_dates_schema = StructType([
    StructField("date_id", IntegerType(), True),
    StructField("date", DateType(), True)    
])

gold_times_schema = StructType([
    StructField("time_id", IntegerType(), True),
    StructField("time", StringType(), True)    
])
