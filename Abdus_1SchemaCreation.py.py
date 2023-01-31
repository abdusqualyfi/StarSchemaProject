# Databricks notebook source
import zipfile, subprocess, glob

#Directories - ensure / is at the end
main_folder = "/tmp/Abdus/" #This folder has to exist first for it to work
zip_location_folder = "/dbfs/tmp/landing/"
zip_output_folder = "/dbfs/tmp/Abdus/landing"
dbfs_directory = 'dbfs:/tmp/Abdus/landing/'

#Delete everything in destination folder
dbutils.fs.rm(main_folder, True)

#Extract all zips in a given location
zip_files = glob.glob("/dbfs/tmp/landing/*.zip")
for zip_file in zip_files:
    extract_to_dir = "/dbfs/tmp/Abdus/landing"
    subprocess.call(["unzip", "-d", extract_to_dir, zip_file])

#Check directory exists and has the extract csv files in it
print(dbutils.fs.ls('/tmp/Abdus/landing'))

# COMMAND ----------

from pyspark.sql.types import *
#Assign to dataframes

#Trip
bronze_trip_header = ['trip_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_id', 'end_station_id', 'rider_id']
bronze_trip_schema = StructType([StructField(col, StringType(), True) for col in bronze_trip_header])
bronze_trip_df = spark.read.option("header", "false").schema(bronze_trip_schema).csv(dbfs_directory +  'trips.csv')
#display(bronze_trip_df)

#Payments
bronze_payments_header = ['payment_id', 'date', 'amount', 'rider_id']
bronze_payments_schema = StructType([StructField(col, StringType(), True) for col in bronze_payments_header])
bronze_payments_df = spark.read.option("header", "false").schema(bronze_payments_schema).csv(dbfs_directory +  'payments.csv')
#display(bronze_payments_df)

#Riders
bronze_riders_header = ['rider_id', 'first', 'last', 'address', 'birthday', 'account_start', 'account_end', 'is_member'] 
bronze_riders_schema = StructType([StructField(col, StringType(), True) for col in bronze_riders_header])
bronze_riders_df = spark.read.option("header", "false").schema(bronze_riders_schema).csv(dbfs_directory +  'riders.csv')
#display(bronze_riders_df)

#Stations
bronze_stations_header = ['station_id', 'name', 'longitude', 'latitude'] 
bronze_stations_schema = StructType([StructField(col, StringType(), True) for col in bronze_stations_header])
bronze_stations_df = spark.read.option("header", "false").schema(bronze_stations_schema).csv(dbfs_directory +  'stations.csv')
#display(bronze_stations_df)


# COMMAND ----------

#Save to delta in Bronze folder
bronze_trip_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/trips")
bronze_payments_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/payments")
bronze_riders_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/riders")
bronze_stations_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/stations")
#########################################################################END OF SCHEMA CREATION#########################################################################

# COMMAND ----------

#Open Bronze folder delta
bronze_to_silverdf_trip = spark.read.format("delta").load(main_folder + "Bronze/trips")
display(bronze_to_silverdf_trip)

bronze_to_silverdf_payments = spark.read.format("delta").load(main_folder + "Bronze/payments")
#display(bronze_to_silverdf_payments)

bronze_to_silverdf_riders = spark.read.format("delta").load(main_folder + "Bronze/riders")
#display(bronze_to_silverdf_riders)

bronze_to_silverdf_stations = spark.read.format("delta").load(main_folder + "Bronze/stations")
#display(bronze_to_silverdf_stations)

# COMMAND ----------

#Show datatypes
print("Trip schema")
bronze_to_silverdf_trip.printSchema()

print("Payments schema")
bronze_to_silverdf_payments.printSchema()

print("Riders schema")
bronze_to_silverdf_riders.printSchema()

print("Stations schema")
bronze_to_silverdf_stations.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, cast, unix_timestamp, from_unixtime, date_format, to_date
#New defined schema for silver

#Silver Trips DF
silver_trips_df = bronze_to_silverdf_trip.withColumn("trip_id", bronze_to_silverdf_trip["trip_id"].cast("string")) \
                                        .withColumn("rideable_type", bronze_to_silverdf_trip["rideable_type"].cast("string")) \
                                        .withColumn("started_at", unix_timestamp(bronze_to_silverdf_trip["started_at"], "dd/MM/yyyy HH:mm").cast("timestamp")) \
                                        .withColumn("ended_at", unix_timestamp(bronze_to_silverdf_trip["ended_at"], "dd/MM/yyyy HH:mm").cast("timestamp")) \
                                        .withColumn("start_station_id", bronze_to_silverdf_trip["start_station_id"].cast("string")) \
                                        .withColumn("end_station_id", bronze_to_silverdf_trip["end_station_id"].cast("string")) \
                                        .withColumn("rider_id", bronze_to_silverdf_trip["rider_id"].cast("integer"))

#Silver Payments DF
silver_payments_df = bronze_to_silverdf_payments.withColumn("payment_id", bronze_to_silverdf_payments["payment_id"].cast("integer")) \
                                        .withColumn("date", bronze_to_silverdf_payments["date"].cast("date")) \
                                        .withColumn("amount", bronze_to_silverdf_payments["amount"].cast("float")) \
                                        .withColumn("rider_id", bronze_to_silverdf_payments["rider_id"].cast("integer"))

#Silver Riders DF
silver_riders_df = bronze_to_silverdf_riders.withColumn("rider_id", bronze_to_silverdf_riders["rider_id"].cast("integer")) \
                                        .withColumn("first", bronze_to_silverdf_riders["first"].cast("string")) \
                                        .withColumn("last", bronze_to_silverdf_riders["last"].cast("string")) \
                                        .withColumn("address", bronze_to_silverdf_riders["address"].cast("string")) \
                                        .withColumn("birthday", bronze_to_silverdf_riders["birthday"].cast("date")) \
                                        .withColumn("account_start", bronze_to_silverdf_riders["account_start"].cast("date")) \
                                        .withColumn("account_end", bronze_to_silverdf_riders["account_end"].cast("date")) \
                                        .withColumn("is_member", bronze_to_silverdf_riders["is_member"].cast("boolean"))

#Silver Stations DF
silver_stations_df = bronze_to_silverdf_stations.withColumn("station_id", bronze_to_silverdf_stations["station_id"].cast("string")) \
                                        .withColumn("name", bronze_to_silverdf_stations["name"].cast("string")) \
                                        .withColumn("longitude", bronze_to_silverdf_stations["longitude"].cast("float")) \
                                        .withColumn("latitude", bronze_to_silverdf_stations["latitude"].cast("float"))



# COMMAND ----------

display(silver_trips_df)

display(silver_payments_df)

display(silver_riders_df)

display(silver_stations_df)

# COMMAND ----------

#Show datatypes
print("Trip schema")
silver_trips_df.printSchema()

print("Payments schema")
silver_payments_df.printSchema()

print("Riders schema")
silver_riders_df.printSchema()

print("Stations schema")
silver_stations_df.printSchema()

# COMMAND ----------

#Save to delta in Silver folder
silver_trips_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/trips")
silver_payments_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/payments")
silver_riders_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/riders")
silver_stations_df.write.format("delta").mode("overwrite").save(main_folder + "Silver/stations")
#########################################################################END OF BRONZE NOTEBOOK#########################################################################

# COMMAND ----------

#Open Silver folder delta
silver_to_golddf_trips = spark.read.format("delta").load(main_folder + "Silver/trips")
display(silver_to_golddf_trips)

silver_to_golddf_payments = spark.read.format("delta").load(main_folder + "Silver/payments")
#display(silver_to_golddf_payments)

silver_to_golddf_riders = spark.read.format("delta").load(main_folder + "Silver/riders")
#display(silver_to_golddf_riders)

silver_to_golddf_stations = spark.read.format("delta").load(main_folder + "Silver/stations")
#display(silver_to_golddf_stations)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import split

#Create dimension tables

#Create Bike table
bike_window = Window.orderBy("rideable_type")

bike_dim_df = silver_to_golddf_trips.select("rideable_type").distinct() \
            .withColumn("bike_id", F.row_number().over(bike_window))  \
            .select("bike_id", "rideable_type")

#display(bike_dim_df)
#print(bike_df)

#Create date and time table (combine started_at and ended_at from trip table)
dates_started_df = silver_to_golddf_trips.select("started_at")
dates_ended_df = silver_to_golddf_trips.select("ended_at").withColumnRenamed("ended_at", "started_at")

dates_combined_df = dates_started_df.union(dates_ended_df).distinct()
dates_combined_df = dates_combined_df.withColumn("started_at", col("started_at").cast("string"))

dates_df_datetime = dates_combined_df.withColumn("date", split(dates_combined_df["started_at"], " ")[0])
dates_df_datetime = dates_df_datetime.withColumn("time", split(col("started_at"), " ")[1].substr(0,5)).select("date", "time")

#dates table (combine trip and payment dates)
payments_date_df = silver_to_golddf_payments.select("date")
trips_date_df = dates_df_datetime.select("date")
pt_dates_combined_df = payments_date_df.union(trips_date_df).distinct()

dates_window = Window.orderBy("date")
dates_dim_df = pt_dates_combined_df.select("date") \
            .withColumn("date_id", F.row_number().over(dates_window)) \
            .select("date_id", "date")

#time table
time_window = Window.orderBy("time")
times_dim_df = dates_df_datetime.select("time").distinct() \
            .withColumn("time_id", F.row_number().over(time_window)) \
            .select("time_id", "time")

display(bike_dim_df)
display(dates_dim_df)
display(times_dim_df)


# COMMAND ----------

#Establish joins and relationships

#payment fact table
payment_fact_df = silver_to_golddf_payments.join(dates_dim_df, on="date", how="left").select("payment_id", "rider_id", "date_id", "amount")

#trip fact table
trip_fact_df = silver_to_golddf_trips.join(bike_dim_df, on="rideable_type", how="left") \
                .select("trip_id", "rider_id", "bike_id", "started_at", "ended_at", "start_station_id", "end_station_id")


#Create date and time table (combine started_at and ended_at from trip table)
trip_fact_df = trip_fact_df.withColumn("started_at", col("started_at").cast("string"))
trip_fact_df = trip_fact_df.withColumn("ended_at", col("ended_at").cast("string"))

trip_fact_df = trip_fact_df.withColumn("started_date", split(trip_fact_df["started_at"], " ")[0])
trip_fact_df = trip_fact_df.withColumn("started_time", split(col("started_at"), " ")[1].substr(0,5))

trip_fact_df = trip_fact_df.withColumn("ended_date", split(trip_fact_df["ended_at"], " ")[0])
trip_fact_df2 = trip_fact_df.withColumn("ended_time", split(col("ended_at"), " ")[1].substr(0,5))

trip_fact_df_sd = trip_fact_df2.join(dates_dim_df, trip_fact_df2.started_date == dates_dim_df.date, how="left") \
                .withColumnRenamed("date_id", "started_date_id") \
                .drop("date") \
                .join(dates_dim_df, trip_fact_df2.ended_date == dates_dim_df.date, how="left") \
                .withColumnRenamed("date_id", "ended_date_id") \
                .drop("date")

trip_fact_df_st = trip_fact_df_sd.join(times_dim_df, trip_fact_df_sd.started_time == times_dim_df.time, how="left") \
                .withColumnRenamed("time_id", "started_time_id") \
                .drop("time") \
                .join(times_dim_df, trip_fact_df_sd.ended_time == times_dim_df.time, how="left") \
                .withColumnRenamed("time_id", "ended_time_id") \
                .drop("time")
                
                

display(trip_fact_df_st)
#display(trip_fact_df2)
#display(dates_dim_df)

# COMMAND ----------

from pyspark.sql.functions import datediff
#print(silver_to_golddf_trips)
#print(trip_fact_df2) #cast started_at and ended_at as timestamp

trip_fact_df_st2 = trip_fact_df_st.withColumn("trip_duration", (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at")))/60) \
                    .select("trip_id", "rider_id", "bike_id", "start_station_id", "end_station_id", "started_date_id", "started_time_id", "ended_date_id", "ended_time_id", "trip_duration", "started_date")    

rider_bday = silver_to_golddf_riders.select("rider_id", "birthday")

trip_fact_df_st3 = trip_fact_df_st2.join(rider_bday, on="rider_id", how="left")


trip_fact_fin = trip_fact_df_st3.withColumn("rider_age", (datediff(col("started_date"), col("birthday"))/365).cast("int")) \
                .select("trip_id", "rider_id", "bike_id", "start_station_id", "end_station_id", "started_date_id", "started_time_id", "ended_date_id", "ended_time_id", "trip_duration", "rider_age")

print(trip_fact_fin)
display(trip_fact_fin)

# COMMAND ----------

#Save to delta in Gold folder

#facts
trip_fact_fin.write.format("delta").mode("overwrite").save(main_folder + "Gold/fact_trips")
payment_fact_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/fact_payments")

#dim
silver_to_golddf_riders.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_riders")
silver_to_golddf_stations.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_stations")
bike_dim_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_bikes")
dates_dim_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_dates")
times_dim_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_times")
#########################################################################END OF SILVER NOTEBOOK#########################################################################
