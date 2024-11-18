# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

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
.json(f"{raw_folder}/pit_stops.json")

# COMMAND ----------

final_df = add_ingestion_date(pit_stops_df)\
            .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder}/pit_stops")

# COMMAND ----------

spark.read.parquet(f"{processed_folder}/pit_stops").display()

# COMMAND ----------


