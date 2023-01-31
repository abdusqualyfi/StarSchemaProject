# Databricks notebook source
import zipfile, subprocess, glob

#Delete everything in destination folder
personal_landing_folder = "/tmp/Abdus/landing/"
personal_bronze_folder = "/tmp/Abdus/Bronze/"
dbutils.fs.rm(personal_landing_folder, True)
dbutils.fs.rm(personal_bronze_folder, True)

#Extract all zips in a given location
landing_location_zips = "/dbfs/tmp/landing/*.zip"
zip_files = glob.glob(landing_location_zips)
for zip_file in zip_files:
    extract_to_dir = "/dbfs/tmp/Abdus/landing"
    subprocess.call(["unzip", "-d", extract_to_dir, zip_file])

#Individual extraction of Zip files
#print(subprocess.call("unzip /dbfs/tmp/landing/trips.zip -d /dbfs/tmp/Abdus/landing", shell=True))
#print(subprocess.call("unzip /dbfs/tmp/landing/riders.zip -d /dbfs/tmp/Abdus/landing", shell=True))
#print(subprocess.call("unzip /dbfs/tmp/landing/stations.zip -d /dbfs/tmp/Abdus/landing", shell=True))
#print(subprocess.call("unzip /dbfs/tmp/landing/payments.zip -d /dbfs/tmp/Abdus/landing", shell=True))

#Move and Make Directories
#dbutils.fs.mv('/tmp/Abdus/landing/stations.zip', '/tmp/landing/stations.zip')
#dbutils.fs.mkdirs('/tmp/Abdus/Silver')
#dbutils.fs.mkdirs('/tmp/Abdus/Gold')


#Landing Directories
landing_payments = 'dbfs:/tmp/landing/payments.zip'
landing_riders = 'dbfs:/tmp/landing/riders.zip'
landing_stations = 'dbfs:/tmp/landing/stations.zip'
landing_trips = 'dbfs:/tmp/landing/trips.zip'

#Unzip files to personal landing folder
#with zipfile.ZipFile('/tmp/landing/payments.zip', "r") as zip_ref:
#    zip_ref.extractall("dbfs:/tmp/Abdus/landing")

#Check directory exists and has the extract csv files in it
print(dbutils.fs.ls('/tmp/Abdus/landing'))

#CSV file directories
csv_payments = 'dbfs:/tmp/Abdus/landing/payments.csv'
csv_riders = 'dbfs:/tmp/Abdus/landing/riders.csv'
csv_stations = 'dbfs:/tmp/Abdus/landing/stations.csv'
csv_trips = 'dbfs:/tmp/Abdus/landing/trips.csv'

# COMMAND ----------

from pyspark.sql.types import *
#Assign to dataframes

#Trip
bronze_trip_header = ['trip_id', 'rideable_type', 'started_at', 'ended_at', 'start_station_id', 'end_station_id', 'rider_id']
bronze_trip_schema = StructType([StructField(col, StringType(), True) for col in bronze_trip_header])
bronze_trip_df = spark.read.option("header", "false").schema(bronze_trip_schema).csv(csv_trips)
#display(bronze_trip_df)

#Payments
bronze_payments_header = ['payment_id', 'date', 'amount', 'rider_id']
bronze_payments_schema = StructType([StructField(col, StringType(), True) for col in bronze_payments_header])
bronze_payments_df = spark.read.option("header", "false").schema(bronze_payments_schema).csv(csv_payments)
#display(bronze_payments_df)

#Riders
bronze_riders_header = ['rider_id', 'first', 'last', 'address', 'birthday', 'account_start', 'account_end', 'is_member']
bronze_riders_schema = StructType([StructField(col, StringType(), True) for col in bronze_riders_header])
bronze_riders_df = spark.read.option("header", "false").schema(bronze_riders_schema).csv(csv_riders)
#display(bronze_riders_df)

#Stations
bronze_stations_header = ['station_id', 'name', 'longitude', 'latitude']
bronze_stations_schema = StructType([StructField(col, StringType(), True) for col in bronze_stations_header])
bronze_stations_df = spark.read.option("header", "false").schema(bronze_stations_schema).csv(csv_stations)
#display(bronze_stations_df)


# COMMAND ----------

#Save to delta in Bronze folder
bronze_trip_df.write.format("delta").mode("overwrite").save("/tmp/Abdus/Bronze/trip")
bronze_payments_df.write.format("delta").mode("overwrite").save("/tmp/Abdus/Bronze/payments")
bronze_riders_df.write.format("delta").mode("overwrite").save("/tmp/Abdus/Bronze/riders")
bronze_stations_df.write.format("delta").mode("overwrite").save("/tmp/Abdus/Bronze/stations")

# COMMAND ----------

#Open Bronze folder delta
bronze_to_silverdf_trip = spark.read.format("delta").load("/tmp/Abdus/Bronze/trip")
display(bronze_to_silverdf_trip)

bronze_to_silverdf_payments = spark.read.format("delta").load("/tmp/Abdus/Bronze/payments")
#display(bronze_to_silverdf_payments)

bronze_to_silverdf_riders = spark.read.format("delta").load("/tmp/Abdus/Bronze/riders")
#display(bronze_to_silverdf_riders)

bronze_to_silverdf_stations = spark.read.format("delta").load("/tmp/Abdus/Bronze/stations")
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
