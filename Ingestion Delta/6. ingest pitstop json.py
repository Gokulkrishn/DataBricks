# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder}/{v_file_date}/pit_stops.json")

# COMMAND ----------

final_df = add_ingestion_date(pit_stops_df)\
            .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

overwrite_partition(final_df,"f1_processed","pit_stops","race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*) from f1_processed.pit_stops group by race_id;

# COMMAND ----------


