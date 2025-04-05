# Databricks notebook source
# DBTITLE 1,IMPORT CONNECTION
# MAGIC %run "./nyc_taxi_conn"

# COMMAND ----------

# DBTITLE 1,CREATE A DATABASE
# MAGIC %sql
# MAGIC CREATE DATABASE gold

# COMMAND ----------

# MAGIC %md
# MAGIC ####READ AND WRITE TRIP-TYPE TO GOLD

# COMMAND ----------

# DBTITLE 1,READING TRIP TYPE FROM SILVER
df_type_gold = spark.read.format('parquet') \
    .option('header', True) \
        .option('inferSchema', True) \
            .load(f'{silver}/trip-type')

# display(df_type_gold)

# COMMAND ----------

# DBTITLE 1,WRITING TRIP TYPE TO GOLD AS DELTA TABLE
df_type_gold.write.format('delta') \
    .option('mode', 'overwrite') \
        .option('path', f'{gold}/trip-type') \
            .saveAsTable('gold.trip_type')

# COMMAND ----------

# MAGIC %md
# MAGIC ####READ AND WRITE TRIP-ZONE TO GOLD

# COMMAND ----------

# DBTITLE 1,READ TRIP ZONE FROM SILVER
df_zone_gold = spark.read.format('parquet') \
    .option('header', True) \
        .option('inferSchema', True) \
            .load(f'{silver}/trip-zone')

# COMMAND ----------

# DBTITLE 1,WRITE TRIP ZONE TO GOLD AS DELTA TABLE
df_zone_gold.write.format('delta') \
    .option('moder', 'overwrite') \
        .option('path', f'{gold}/trip-zone') \
            .saveAsTable('gold.trip_zone')


# COMMAND ----------

# MAGIC %md
# MAGIC ####READ AND WRITE TRIP-DATA (2024) TO GOLD

# COMMAND ----------

# DBTITLE 1,READ TRIP DATA (2024) FROM SILVER
df_data_gold = spark.read.format('parquet') \
    .option('header', True) \
        .option('inferSchema', True) \
            .load(f'{silver}/trip-data')

# COMMAND ----------

# DBTITLE 1,WRITE TRIP DATA (2024) TO GOLD AS DELTA TABLE
df_data_gold.write.format('delta') \
    .option('mode', 'overwrite') \
        .option('path', f'{gold}/trip-data') \
            .saveAsTable('gold.trip_data')

# COMMAND ----------

# MAGIC %md
# MAGIC #QUERY DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ####ALTER TABLE DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip_zone
# MAGIC SET Borough = 'EMR'
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.trip_zone
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone WHERE LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ####VERSIONING

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ####TIME TRAVEL

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE gold.trip_zone TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone