# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-03-28")
v_file_date = dbutils.widgets.get("p_file_date")  

# COMMAND ----------

v_file_date

# COMMAND ----------

qualifying_df = spark.read.option("multiline",True).json(f"{raw_folder}/{v_file_date}/qualifying/")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiline",True).json(f"{raw_folder}/{v_file_date}/qualifying/")

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

overwrite_partition(final_df, "f1_processed","qualifying", "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*) from f1_processed.qualifying group by race_id

# COMMAND ----------


