# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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

lap_time_df = spark.read.csv(f"{raw_folder}/{v_file_date}/lap_times/lap_times_split*")

# COMMAND ----------


lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_time_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder}/{v_file_date}/lap_times/*.csv")

# COMMAND ----------

final_df = add_ingestion_date(lap_time_df)\
          .withColumnRenamed("driverId", "driver_id") \
          .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

overwrite_partition(final_df,"f1_processed","lap_times","race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*) from f1_processed.lap_times group by race_id;

# COMMAND ----------


