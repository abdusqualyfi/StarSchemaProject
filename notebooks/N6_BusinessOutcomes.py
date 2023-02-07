# Databricks notebook source
# MAGIC %md
# MAGIC ##Business Outcomes

# COMMAND ----------

#Directories
main_folder = "/tmp/Abdus/"

#Variables
#1A) timeSpentPerRide_date
#1B) timeSpentPerRide_time
#1C) timeSpentPerRide_StartStation
#1D) timeSpentPerRide_EndStation
#1E) timeSpentPerRide_RiderAge
#1F) timeSpentPerRide_RiderType
#2A) moneySpentPerRide_perMonth
#2B) moneySpentPerRide_perQuarter
#2C) moneySpentPerRide_perYear
#2D) moneySpentPerRiderAge
#3A) memberAvgTripMonth
#3B) memberAvgTripMinutes

# COMMAND ----------

#Open Gold folder delta
gold_trips_df = spark.read.format("delta").load(main_folder + "Gold/fact_trips")
gold_payments_df = spark.read.format("delta").load(main_folder + "Gold/fact_payments")
gold_riders_df = spark.read.format("delta").load(main_folder + "Gold/dim_riders")
gold_stations_df = spark.read.format("delta").load(main_folder + "Gold/dim_stations")
gold_bikes_df = spark.read.format("delta").load(main_folder + "Gold/dim_bikes")
gold_dates_df = spark.read.format("delta").load(main_folder + "Gold/dim_dates")
gold_times_df = spark.read.format("delta").load(main_folder + "Gold/dim_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ##1) Analyse how much time is spent per ride

# COMMAND ----------

# MAGIC %md
# MAGIC 1A) Based on date and time factors such as day of week

# COMMAND ----------

from pyspark.sql.functions import avg, col, date_format, round, substring,  sum, quarter, when, year

timeSpentPerRide_date = gold_trips_df.join(gold_dates_df, gold_trips_df.started_date_id == gold_dates_df.date_id, how="left") \
                            .withColumnRenamed("date", "trip_start_date") \
                            .drop("date_id")

timeSpentPerRide_date = timeSpentPerRide_date.withColumn("day_of_week", date_format(timeSpentPerRide_date["trip_start_date"], "E"))

timeSpentPerRide_date = timeSpentPerRide_date.groupBy("day_of_week").agg(avg("trip_duration").alias("avg_trip_duration")).orderBy("day_of_week")
timeSpentPerRide_date = timeSpentPerRide_date.withColumn("avg_trip_duration", round(timeSpentPerRide_date["avg_trip_duration"], 2))
display(timeSpentPerRide_date)

# COMMAND ----------

# MAGIC %md
# MAGIC 1B) Based on date and time factors such as time of day

# COMMAND ----------

timeSpentPerRide_time = gold_trips_df.join(gold_times_df, gold_trips_df.started_time_id == gold_times_df.time_id, how="left") \
                            .withColumnRenamed("time", "trip_start_time") \
                            .drop("time_id")

timeSpentPerRide_time = timeSpentPerRide_time.withColumn("trip_start_time", substring(timeSpentPerRide_time["trip_start_time"], 1, 2))

timeSpentPerRide_time = timeSpentPerRide_time.groupBy("trip_start_time").agg(avg("trip_duration").alias("avg_trip_duration")).orderBy("trip_start_time")
timeSpentPerRide_time = timeSpentPerRide_time.withColumn("avg_trip_duration", round(timeSpentPerRide_time["avg_trip_duration"], 2))
display(timeSpentPerRide_time)

# COMMAND ----------

# MAGIC %md
# MAGIC 1C) Based on which station is the starting station

# COMMAND ----------

timeSpentPerRide_StartStation = gold_trips_df.join(gold_stations_df, gold_trips_df.start_station_id == gold_stations_df.station_id, how="left") \
                            .withColumnRenamed("name", "trip_start_station") \
                            .drop("station_id")

timeSpentPerRide_StartStation = timeSpentPerRide_StartStation.groupBy("trip_start_station").agg(avg("trip_duration").alias("avg_trip_duration")).orderBy("trip_start_station")
timeSpentPerRide_StartStation = timeSpentPerRide_StartStation.withColumn("avg_trip_duration", round(timeSpentPerRide_StartStation["avg_trip_duration"], 2))
display(timeSpentPerRide_StartStation)

# COMMAND ----------

# MAGIC %md
# MAGIC 1D) Based on which station is the ending station

# COMMAND ----------

timeSpentPerRide_EndStation = gold_trips_df.join(gold_stations_df, gold_trips_df.end_station_id == gold_stations_df.station_id, how="left") \
                            .withColumnRenamed("name", "trip_end_station") \
                            .drop("station_id")

timeSpentPerRide_EndStation = timeSpentPerRide_EndStation.groupBy("trip_end_station").agg(avg("trip_duration").alias("avg_trip_duration")).orderBy("trip_end_station")
timeSpentPerRide_EndStation = timeSpentPerRide_EndStation.withColumn("avg_trip_duration", round(timeSpentPerRide_EndStation["avg_trip_duration"], 2))
display(timeSpentPerRide_EndStation)

# COMMAND ----------

# MAGIC %md
# MAGIC 1E) Based on age of the rider at time of the ride

# COMMAND ----------

timeSpentPerRide_RiderAge = gold_trips_df.groupBy("rider_age").agg(avg("trip_duration").alias("avg_trip_duration")).orderBy("rider_age")
timeSpentPerRide_RiderAge = timeSpentPerRide_RiderAge.withColumn("avg_trip_duration", round(timeSpentPerRide_RiderAge["avg_trip_duration"], 2))
display(timeSpentPerRide_RiderAge)

# COMMAND ----------

# MAGIC %md
# MAGIC 1F) Based on whether the rider is a a member or a casual rider

# COMMAND ----------

timeSpentPerRide_RiderType = gold_trips_df.join(gold_riders_df, on="rider_id", how="left") \
                            .drop("rider_id")

timeSpentPerRide_RiderType = timeSpentPerRide_RiderType.groupBy("is_member").agg(avg("trip_duration").alias("avg_trip_duration")).orderBy("is_member")
timeSpentPerRide_RiderType = timeSpentPerRide_RiderType.withColumn("avg_trip_duration", round(timeSpentPerRide_RiderType["avg_trip_duration"], 2))
display(timeSpentPerRide_RiderType)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2) Analyse how much money is spent

# COMMAND ----------

# MAGIC %md
# MAGIC 2A) Per month

# COMMAND ----------

moneySpentPerRide = gold_payments_df.join(gold_dates_df, on="date_id", how="left") \
                            .withColumnRenamed("date", "payment_date") \
                            .drop("date_id")

moneySpentPerRide_month = moneySpentPerRide.withColumn("month", date_format(moneySpentPerRide["payment_date"], "MM-yyyy"))
moneySpentPerRide_month = moneySpentPerRide_month.groupBy("month").agg(sum("amount").alias("monthly_amount")).orderBy("month")

moneySpentPerRide_month2 = moneySpentPerRide_month.withColumn("month", substring(moneySpentPerRide_month["month"], 1, 2))
moneySpentPerRide_month2 = moneySpentPerRide_month2.groupBy("month").agg(avg("monthly_amount").alias("avg_monthly_amount")).orderBy("month")

moneySpentPerRide_perMonth = moneySpentPerRide_month2.withColumn("avg_monthly_amount", round(moneySpentPerRide_month2["avg_monthly_amount"], 2))
display(moneySpentPerRide_perMonth)

# COMMAND ----------

# MAGIC %md
# MAGIC 2B) Per quarter

# COMMAND ----------

moneySpentPerRide_quarter = moneySpentPerRide_perMonth.withColumn("month", col("month").cast("int"))
moneySpentPerRide_quarter = moneySpentPerRide_quarter.withColumn("quarter", when(moneySpentPerRide_quarter["month"].between(1, 3), 1) \
                                                                             .when(moneySpentPerRide_quarter["month"].between(4, 6), 2) \
                                                                             .when(moneySpentPerRide_quarter["month"].between(7, 9), 3) \
                                                                             .otherwise(4))

moneySpentPerRide_quarter = moneySpentPerRide_quarter.groupBy("quarter").agg(sum("avg_monthly_amount").alias("quarterly_amount")).orderBy("quarter")
moneySpentPerRide_perQuarter = moneySpentPerRide_quarter.withColumn("quarterly_amount", round(moneySpentPerRide_quarter["quarterly_amount"], 2))
display(moneySpentPerRide_perQuarter)

# COMMAND ----------

# MAGIC %md
# MAGIC 2C) Per year

# COMMAND ----------

moneySpentPerRide_year = moneySpentPerRide.withColumn("year", date_format(moneySpentPerRide["payment_date"], "yyyy"))
moneySpentPerRide_year = moneySpentPerRide_year.groupBy("year").agg(sum("amount").alias("yearly_amount")).orderBy("year")
moneySpentPerRide_perYear = moneySpentPerRide_year.withColumn("yearly_amount", round(moneySpentPerRide_year["yearly_amount"], 2))
display(moneySpentPerRide_perYear)

# COMMAND ----------

# MAGIC %md
# MAGIC 2D) Per member, based on the age of the rider at account start

# COMMAND ----------

moneySpentPerRiderAge = gold_payments_df.join(gold_riders_df, on="rider_id", how="left") \
                            .drop("rider_id")

moneySpentPerRiderAge = moneySpentPerRiderAge.withColumn("age_at_creation", year(moneySpentPerRiderAge["account_start"]) - year(moneySpentPerRiderAge["birthday"]))

moneySpentPerRiderAge = moneySpentPerRiderAge.groupBy("age_at_creation").agg(sum("amount").alias("amount_per_age")).orderBy("age_at_creation")
moneySpentPerRiderAge = moneySpentPerRiderAge.withColumn("amount_per_age", round(moneySpentPerRiderAge["amount_per_age"], 2))

display(moneySpentPerRiderAge)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) EXTRA CREDIT - Analyse how much money is spent per member

# COMMAND ----------

# MAGIC %md
# MAGIC 3A) Based on how many rides the rider averages per month

# COMMAND ----------

memberTrip = gold_trips_df.select("rider_id", "started_date_id")
memberRiders = gold_riders_df.select("rider_id", "is_member")
memberPayment = gold_payments_df.select("rider_id", "amount")

memberAvgTripMonth = memberTrip.join(memberRiders, on="rider_id", how="left")
memberAvgTripMonth = memberAvgTripMonth.filter(col("is_member") == True)

memberAvgTripMonth = memberAvgTripMonth.join(memberPayment, on="rider_id", how="left")

memberAvgTripMonth = memberAvgTripMonth.join(gold_dates_df, memberAvgTripMonth.started_date_id == gold_dates_df.date_id, how="left") \
                            .withColumnRenamed("date", "month") \
                            .drop("date_id")

memberAvgTripMonth = memberAvgTripMonth.withColumn("month", date_format(memberAvgTripMonth["month"], "MMMM"))

memberAvgTripMonth = memberAvgTripMonth.groupBy("rider_id", "month").agg(sum("amount").alias("total_amount")).orderBy("rider_id")

display(memberAvgTripMonth)

# COMMAND ----------

# MAGIC %md
# MAGIC 3B) Based on how many minutes the rider spends on a bike per month

# COMMAND ----------

memberTripMins = gold_trips_df.select("rider_id", "started_date_id", "trip_duration")

memberAvgTripMinutes = memberTripMins.join(memberRiders, on="rider_id", how="left")
memberAvgTripMinutes = memberAvgTripMinutes.filter(col("is_member") == True)

memberAvgTripMinutes = memberAvgTripMinutes.join(memberPayment, on="rider_id", how="left")

memberAvgTripMinutes = memberAvgTripMinutes.join(gold_dates_df, memberAvgTripMinutes.started_date_id == gold_dates_df.date_id, how="left") \
                            .withColumnRenamed("date", "month") \
                            .drop("date_id")

memberAvgTripMinutes = memberAvgTripMinutes.withColumn("month", date_format(memberAvgTripMinutes["month"], "MMMM")) \
                            .drop("started_date_id", "is_member")

memberAvgTripMinutes = memberAvgTripMinutes.groupBy("rider_id", "month", "trip_duration").agg(sum("amount").alias("total_amount")).orderBy("rider_id")
memberAvgTripMinutes = memberAvgTripMinutes.groupBy("rider_id", "month", "total_amount").agg(sum("trip_duration").alias("trip_duration")).orderBy("rider_id")
#Rider_ID 9658 duplicate

display(memberAvgTripMinutes)
