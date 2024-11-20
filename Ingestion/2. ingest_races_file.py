# Databricks notebook source
from pyspark.sql.types import StructField,ShortType,IntegerType, StringType, DoubleType, StructType,DateType
from pyspark.sql.functions import col, lit, concat, to_timestamp

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{raw_folder}/races.csv").show(2)

# COMMAND ----------

race_schema = StructType(fields=[
  StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

race_df = spark.read.format("csv").schema(race_schema).option("header", "true").load(f"{raw_folder}/races.csv")

# COMMAND ----------

race_df.show(2)

# COMMAND ----------

race_renamed_df = race_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("year","race_year")

# COMMAND ----------

race_newcol_df = add_ingestion_date(race_renamed_df).withColumn("race_timestamp",to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

race_newcol_df.show(1)

# COMMAND ----------

race_final_df = race_newcol_df.select("race_id","race_year","round","circuit_id","name","race_timestamp","ingestion_date")

# COMMAND ----------

dbutils.fs.rm(f"{processed_folder}/races", True)

# COMMAND ----------

# race_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder}/races")

# COMMAND ----------

race_final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------


