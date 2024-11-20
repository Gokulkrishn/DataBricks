# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder}/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df)\
                                  .withColumnRenamed("constructorId","constructor_id")\
                                  .withColumnRenamed("constructorRef","constructor_ref")\
                                  .withColumn("ingestion_date",current_timestamp())                                  

# COMMAND ----------

constructor_final_df.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Write file to parquet form

# COMMAND ----------

dbutils.fs.rm(f"{processed_folder}/constructor", True)

# COMMAND ----------

constructor_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructor;

# COMMAND ----------


