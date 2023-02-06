# Databricks notebook source
# MAGIC %md
# MAGIC ##Extract Zips From GitHub Repo

# COMMAND ----------

#Directories - ensure / is at the end
main_folder = "/tmp/Abdus/" #This folder has to exist first for it to work
zip_location_folder = "/dbfs/tmp/Abdus/github/"
zip_output_folder = "/dbfs/tmp/Abdus/landing"
github_location_folder = "/tmp/Abdus/github/"
csv_location_folder = "/tmp/Abdus/landing/"


#get files from GitHub
!wget "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/payments.zip" -P "/dbfs/tmp/Abdus/github/"
!wget "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/riders.zip" -P "/dbfs/tmp/Abdus/github/"
!wget "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/stations.zip" -P "/dbfs/tmp/Abdus/github/"
!wget "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/trips.zip" -P "/dbfs/tmp/Abdus/github/"

# COMMAND ----------

import zipfile, subprocess, glob

#Extract all zips in a given location
#zip_files = glob.glob("/dbfs/tmp/abdusgithub/*.zip")
zip_files = glob.glob(zip_location_folder + "*.zip")
for zip_file in zip_files:
    extract_to_dir = zip_output_folder
    subprocess.call(["unzip", "-d", extract_to_dir, zip_file])

#Check directory exists and has the extract csv files in it
print(dbutils.fs.ls('/tmp/Abdus/landing'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze Schema

# COMMAND ----------

# MAGIC %run /Repos/abdus.choudhury@qualyfi.co.uk/StarSchemaProject/notebooks/N0_SchemaCreation

# COMMAND ----------

from pyspark.sql.types import *

#Assign to dataframes
bronze_trips_df = spark.read.format("csv").load(csv_location_folder + "trips.csv", schema = bronze_trips_schema)
bronze_payments_df = spark.read.format("csv").load(csv_location_folder + "payments.csv", schema = bronze_payments_schema)
bronze_riders_df = spark.read.format("csv").load(csv_location_folder + "riders.csv", schema = bronze_riders_schema)
bronze_stations_df = spark.read.format("csv").load(csv_location_folder + "stations.csv", schema = bronze_stations_schema)

# COMMAND ----------

#Save to delta in Bronze folder
bronze_trips_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/trips")
bronze_payments_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/payments")
bronze_riders_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/riders")
bronze_stations_df.write.format("delta").mode("overwrite").save(main_folder + "Bronze/stations")

# COMMAND ----------

#delete github and landing folder
dbutils.fs.rm(github_location_folder, True)
dbutils.fs.rm(csv_location_folder, True)
