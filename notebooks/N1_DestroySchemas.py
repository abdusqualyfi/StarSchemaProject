# Databricks notebook source
# MAGIC %md
# MAGIC ##Destroy main folder "Abdus"

# COMMAND ----------

main_folder = "/tmp/Abdus/"
dbutils.fs.rm(main_folder, True)
