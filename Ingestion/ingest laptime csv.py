# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_time_df = spark.read.csv("/mnt/databrickudemy/raw/lap_times/lap_times_split*")

# COMMAND ----------

lap_time_df.show(2)

# COMMAND ----------


lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_time_df = spark.read.schema(lap_times_schema).csv("/mnt/databrickudemy/raw/lap_times/*.csv")

# COMMAND ----------

lap_time_df.show(2)

# COMMAND ----------

final_df = lap_time_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/databrickudemy/processed/lap_times")

# COMMAND ----------

spark.read.parquet("/mnt/databrickudemy/processed/lap_times").display(2)

# COMMAND ----------


