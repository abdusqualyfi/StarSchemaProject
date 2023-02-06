# Databricks notebook source
# MAGIC %md
# MAGIC ##Extract zips from GitHub Repo

# COMMAND ----------

#Directories - ensure / is at the end
main_folder = "/tmp/Abdus/" #This folder has to exist first for it to work
zip_location_folder = "/dbfs/tmp/Abdus/github/"
zip_output_folder = "/dbfs/tmp/Abdus/landing/"
dbfs_directory = "dbfs:/tmp/Abdus/landing/"

zip_payments = "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/payments.zip"
zip_riders = "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/riders.zip"
zip_stations = "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/stations.zip"
zip_trips = "https://github.com/abdusqualyfi/StarSchemaProject/raw/main/files/trips.zip"

#Delete everything in destination folder
dbutils.fs.rm(main_folder, True)
dbutils.fs.mkdirs(main_folder)

#get files from GitHub
!wget zip_payments -P zip_location_folder
!wget zip_riders -P zip_location_folder
!wget zip_stations -P zip_location_folder
!wget zip_trips -P zip_location_folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##Extract zip files to Landing

# COMMAND ----------

import zipfile, subprocess, glob

#Extract all zips in a given location
zip_files = glob.glob(zip_location_folder + "*.zip")
for zip_file in zip_files:
    extract_to_dir = zip_output_folder
    subprocess.call(["unzip", "-d", extract_to_dir, zip_file])
