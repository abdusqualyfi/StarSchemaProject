# Databricks notebook source
dbutils.fs.mv('/tmp/Abdus/landing/stations.zip', '/tmp/landing/stations.zip')

#dbutils.fs.mkdirs('/tmp/Abdus/Silver')

#dbutils.fs.mkdirs('/tmp/Abdus/Gold')

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType ([
    StructField("MessageId", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("DateOfBirth", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Email", StringType(), True),
])

# COMMAND ----------

# File location and type
file_location = "/tmp/Abdus/Bronze/trips.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "trips_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `trips_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "trips_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
