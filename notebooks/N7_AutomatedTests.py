# Databricks notebook source
# MAGIC %md
# MAGIC ##Automated Tests

# COMMAND ----------

# MAGIC %md
# MAGIC Asserts on Schema

# COMMAND ----------

# MAGIC %run /Repos/abdus.choudhury@qualyfi.co.uk/StarSchemaProject/notebooks/N0_SchemaCreation

# COMMAND ----------

assert gold_trips_df.schema == gold_trips_schema, "Schema mismatch on: Trips table"
assert gold_payments_df.schema == gold_payments_schema, "Schema mismatch on: Payments table"
assert gold_riders_df.schema == gold_riders_schema, "Schema mismatch on: Riders table"
assert gold_stations_df.schema == gold_stations_schema, "Schema mismatch on: Stations table"
assert gold_bikes_df.schema == gold_bikes_schema, "Schema mismatch on: Bikes table"
assert gold_dates_df.schema == gold_dates_schema, "Schema mismatch on: Dates table"
assert gold_times_df.schema == gold_times_schema, "Schema mismatch on: Times table"

# COMMAND ----------

# MAGIC %md
# MAGIC Asserts on Business Outcomes

# COMMAND ----------

# MAGIC %run /Repos/abdus.choudhury@qualyfi.co.uk/StarSchemaProject/notebooks/N6_BusinessOutcomes

# COMMAND ----------

from pyspark.sql.functions import col

#Q1A) timeSpentPerRide_date
assert timeSpentPerRide_date.count() == 7, "Incorrect number of rows in Q1A, there should only be 7 rows for 7 days"

#Q1B) timeSpentPerRide_time
q1b_count = timeSpentPerRide_time.filter(col("trip_start_time") < 24).count()
q1b_total = timeSpentPerRide_time.count()
assert q1b_count == q1b_total, "Incorrect value found in Q1B, 23 should be the max value in this column"

#Q1C) timeSpentPerRide_StartStation
assert timeSpentPerRide_StartStation.count() == 74, "Incorrect number of rows in Q1C, expecting 74"

#Q1D) timeSpentPerRide_EndStation
assert timeSpentPerRide_EndStation.count() == 67, "Incorrect number of rows in Q1D, expecting 67"

#Q1E) timeSpentPerRide_RiderAge
q1e_count = timeSpentPerRide_RiderAge.filter(col("rider_age") < 5).count()
assert q1e_count == 0, "Incorrect value found in Q1E, found rider younger than 5"

#Q1F) timeSpentPerRide_RiderType


#Q2A) moneySpentPerRide_perMonth


#Q2B) moneySpentPerRide_perQuarter


#Q2C) moneySpentPerRide_perYear


#Q2D) moneySpentPerRiderAge


#Q3A) memberAvgTripMonth


#Q3B) memberAvgTripMinutes



