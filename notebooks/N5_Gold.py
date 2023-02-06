# Databricks notebook source
# MAGIC %md
# MAGIC ##Gold Schema

# COMMAND ----------

#Directories
main_folder = "/tmp/Abdus/"

# COMMAND ----------

#Open Silver folder delta
silver_trips_df = spark.read.format("delta").load(main_folder + "Silver/trips")
silver_payments_df = spark.read.format("delta").load(main_folder + "Silver/payments")
silver_riders_df = spark.read.format("delta").load(main_folder + "Silver/riders")
silver_stations_df = spark.read.format("delta").load(main_folder + "Silver/stations")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Dimension tables

# COMMAND ----------

# MAGIC %md
# MAGIC Bike table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, split, unix_timestamp

bike_window = Window.orderBy("rideable_type")

gold_bikes_df = silver_trips_df.select("rideable_type").distinct() \
            .withColumn("bike_id", F.row_number().over(bike_window))  \
            .select("bike_id", "rideable_type")

# COMMAND ----------

# MAGIC %md
# MAGIC Date table

# COMMAND ----------

#Name started_at and ended_at as started_at to allow union join
dates_started_df = silver_trips_df.select("started_at")
dates_ended_df = silver_trips_df.select("ended_at").withColumnRenamed("ended_at", "started_at")

#Union join to one column
dates_combined_df = dates_started_df.union(dates_ended_df).distinct()
dates_combined_df = dates_combined_df.withColumn("started_at", col("started_at").cast("string"))

#Split dates to date and time
dates_df_datetime = dates_combined_df.withColumn("date", split(dates_combined_df["started_at"], " ")[0])
dates_df_datetime = dates_df_datetime.withColumn("time", split(col("started_at"), " ")[1].substr(0,5)).select("date", "time")

#Add dates from payments table
payments_date_df = silver_payments_df.select("date")
trips_date_df = dates_df_datetime.select("date")
pt_dates_combined_df = payments_date_df.union(trips_date_df).distinct()

#Create dates table
dates_window = Window.orderBy("date")
gold_dates_df = pt_dates_combined_df.select("date") \
            .withColumn("date_id", F.row_number().over(dates_window)) \
            .select("date_id", "date")

gold_dates_df = gold_dates_df.withColumn("date", col("date").cast("date"))

# COMMAND ----------

# MAGIC %md
# MAGIC Time table

# COMMAND ----------

time_window = Window.orderBy("time")
gold_times_df = dates_df_datetime.select("time").distinct() \
            .withColumn("time_id", F.row_number().over(time_window)) \
            .select("time_id", "time")

display(gold_bikes_df)
display(gold_dates_df)
display(gold_times_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Establish Joins and Move Columns

# COMMAND ----------

#Join date to payment table
gold_payments_df = silver_payments_df.join(gold_dates_df, on="date", how="left").select("payment_id", "rider_id", "date_id", "amount")#gold_payments_df

# COMMAND ----------

#Join bike to trip table
trip_fact_df = silver_trips_df.join(gold_bikes_df, on="rideable_type", how="left") \
                .select("trip_id", "rider_id", "bike_id", "started_at", "ended_at", "start_station_id", "end_station_id")#gold_trips_df

#Splits started_at and ended_at to date and time
trip_fact_df2 = trip_fact_df.withColumn("started_at", col("started_at").cast("string")) \
                            .withColumn("ended_at", col("ended_at").cast("string"))

trip_fact_df3 = trip_fact_df2.withColumn("started_date", split(trip_fact_df2["started_at"], " ")[0]) \
                            .withColumn("started_time", split(col("started_at"), " ")[1].substr(0,5))

trip_fact_df4 = trip_fact_df3.withColumn("ended_date", split(trip_fact_df3["ended_at"], " ")[0]) \
                            .withColumn("ended_time", split(col("ended_at"), " ")[1].substr(0,5))

#Join date to trip table
trip_fact_df5 = trip_fact_df4.join(gold_dates_df, trip_fact_df4.started_date == gold_dates_df.date, how="left") \
                .withColumnRenamed("date_id", "started_date_id") \
                .drop("date") \
                .join(gold_dates_df, trip_fact_df4.ended_date == gold_dates_df.date, how="left") \
                .withColumnRenamed("date_id", "ended_date_id") \
                .drop("date")

#Join time to trip table
trip_fact_df6 = trip_fact_df5.join(gold_times_df, trip_fact_df5.started_time == gold_times_df.time, how="left") \
                .withColumnRenamed("time_id", "started_time_id") \
                .drop("time") \
                .join(gold_times_df, trip_fact_df5.ended_time == gold_times_df.time, how="left") \
                .withColumnRenamed("time_id", "ended_time_id") \
                .drop("time")

#Select relevant columnns
trip_fact_df7 = trip_fact_df6.withColumn("trip_duration", (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at")))/60) \
                    .select("trip_id", "rider_id", "bike_id", "start_station_id", "end_station_id", "started_date_id", "started_time_id", "ended_date_id", "ended_time_id", "trip_duration", "started_date")    

#Get age from birthday
rider_bday = silver_riders_df.select("rider_id", "birthday")
trip_fact_df8 = trip_fact_df7.join(rider_bday, on="rider_id", how="left")
gold_trips_df = trip_fact_df8.withColumn("rider_age", (datediff(col("started_date"), col("birthday"))/365).cast("int")) \
                .select("trip_id", "rider_id", "bike_id", "start_station_id", "end_station_id", "started_date_id", "started_time_id", "ended_date_id", "ended_time_id", "trip_duration", "rider_age")

gold_trips_df = gold_trips_df.withColumn('trip_duration', col('trip_duration').cast('int'))

# COMMAND ----------

#Adjust variables to gold as these were not changed
gold_riders_df = silver_riders_df

gold_stations_df = silver_stations_df

# COMMAND ----------

#Save to delta in Gold folder

#facts
gold_trips_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/fact_trips")
gold_payments_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/fact_payments")

#dim
gold_riders_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_riders")
gold_stations_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_stations")
gold_bikes_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_bikes")
gold_dates_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_dates")
gold_times_df.write.format("delta").mode("overwrite").save(main_folder + "Gold/dim_times")
