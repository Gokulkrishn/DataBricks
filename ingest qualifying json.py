# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_df = spark.read.option("multiline",True).json("/mnt/databrickudemy/raw/qualifying/")

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

qualifying_df = spark.read.schema(qualifying_schema).option("multiline",True).json("/mnt/databrickudemy/raw/qualifying/")

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/databrickudemy/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet('/mnt/databrickudemy/processed/qualifying'))

# COMMAND ----------


