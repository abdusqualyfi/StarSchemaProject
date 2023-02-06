# Databricks notebook source
# MAGIC %md
# MAGIC ##Automated Tests

# COMMAND ----------

# MAGIC %run /Repos/abdus.choudhury@qualyfi.co.uk/StarSchemaProject/notebooks/N6_BusinessOutcomes

# COMMAND ----------

#Asserts

#Bike count
assert gold_bikes_df.count() == 3, "Incorrect number of rows in bikes table"

#Bike docked_bike check
#assert gold_bikes_df[1] == "docked_bike", "Incorrect value in bikes table, expecting 'docked_bike'"


#Deliberate fail check
assert gold_dates_df.count() == 123, "Incorrect number of rows in dates table"
