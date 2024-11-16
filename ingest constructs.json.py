# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json("/mnt/databrickudemy/raw/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

constructor_final_df = constructor_dropped_df\
                                  .withColumnRenamed("constructorId","constructor_id")\
                                  .withColumnRenamed("constructorRef","constructor_ref")\
                                  .withColumn("ingestion_date",current_timestamp())                                  

# COMMAND ----------

constructor_final_df.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Write file to parquet form

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/databrickudemy/processed/constructor")

# COMMAND ----------

spark.read.parquet("/mnt/databrickudemy/processed/constructor").display(1)

# COMMAND ----------


